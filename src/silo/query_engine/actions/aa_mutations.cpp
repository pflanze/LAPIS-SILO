#include "silo/query_engine/actions/aa_mutations.h"

#include <cmath>
#include <map>
#include <utility>
#include <vector>

#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <nlohmann/json.hpp>

#include "silo/common/aa_symbols.h"
#include "silo/database.h"
#include "silo/query_engine/operator_result.h"
#include "silo/query_engine/query_parse_exception.h"
#include "silo/query_engine/query_result.h"
#include "silo/storage/aa_store.h"
#include "silo/storage/database_partition.h"

using silo::query_engine::OperatorResult;

namespace {

std::pair<std::vector<size_t>, std::vector<size_t>> preFilterBitmaps(
   const silo::AAStore& aa_store,
   std::vector<OperatorResult>& bitmap_filter
) {
   std::vector<size_t> bitmap_filters_to_evaluate;
   std::vector<size_t> full_bitmap_filters_to_evaluate;
   for (size_t i = 0; i < aa_store.partitions.size(); ++i) {
      const silo::AAStorePartition& aa_store_partition = aa_store.partitions.at(i);
      OperatorResult& filter = bitmap_filter[i];
      const size_t cardinality = filter->cardinality();
      if (cardinality == 0) {
         continue;
      }
      if (cardinality == aa_store_partition.sequence_count) {
         full_bitmap_filters_to_evaluate.push_back(i);
      } else {
         if (filter.isMutable()) {
            filter->runOptimize();
         }
         bitmap_filters_to_evaluate.push_back(i);
      }
   }
   return {bitmap_filters_to_evaluate, full_bitmap_filters_to_evaluate};
}

}  // namespace

namespace silo::query_engine::actions {

AAMutations::AAMutations(std::string aa_sequence_name, double min_proportion)
    : aa_sequence_name(std::move(aa_sequence_name)),
      min_proportion(min_proportion) {}

std::array<std::vector<uint32_t>, AAMutations::MUTATION_SYMBOL_COUNT> AAMutations::
   calculateMutationsPerPosition(
      const AAStore& aa_store,
      std::vector<OperatorResult>& bitmap_filter
   ) {
   const size_t sequence_length = aa_store.reference_sequence.length();

   std::vector<size_t> bitmap_filters_to_evaluate;
   std::vector<size_t> full_bitmap_filters_to_evaluate;
   std::tie(bitmap_filters_to_evaluate, full_bitmap_filters_to_evaluate) =
      preFilterBitmaps(aa_store, bitmap_filter);

   std::array<std::vector<uint32_t>, MUTATION_SYMBOL_COUNT> count_of_mutations_per_position;
   for (auto& vec : count_of_mutations_per_position) {
      vec.resize(sequence_length);
   }
   static constexpr int POSITIONS_PER_PROCESS = 300;
   const tbb::blocked_range<uint32_t> range(
      0, sequence_length, /*grain_size=*/POSITIONS_PER_PROCESS
   );
   tbb::parallel_for(range.begin(), range.end(), [&](uint32_t pos) {
      for (const size_t partition_index : bitmap_filters_to_evaluate) {
         const OperatorResult& filter = bitmap_filter[partition_index];
         const silo::AAStorePartition& aa_store_partition = aa_store.partitions[partition_index];

         for (const auto symbol : VALID_MUTATION_SYMBOLS) {
            if (aa_store_partition.positions[pos].symbol_whose_bitmap_is_flipped != symbol) {
               count_of_mutations_per_position[static_cast<uint32_t>(symbol)][pos] +=
                  filter->and_cardinality(
                     aa_store_partition.positions[pos].bitmaps[static_cast<uint32_t>(symbol)]
                  );
            } else {
               count_of_mutations_per_position[static_cast<uint32_t>(symbol)][pos] +=
                  filter->andnot_cardinality(
                     aa_store_partition.positions[pos].bitmaps[static_cast<uint32_t>(symbol)]
                  );
            }
         }
      }
      // For these partitions, we have full bitmaps. Do not need to bother with AND cardinality
      for (const size_t partition_index : full_bitmap_filters_to_evaluate) {
         const silo::AAStorePartition& aa_store_partition = aa_store.partitions[partition_index];

         for (const auto symbol : VALID_MUTATION_SYMBOLS) {
            if (aa_store_partition.positions[pos].symbol_whose_bitmap_is_flipped != symbol) {
               count_of_mutations_per_position[static_cast<uint32_t>(symbol)][pos] +=
                  aa_store_partition.positions[pos]
                     .bitmaps[static_cast<uint32_t>(symbol)]
                     .cardinality();
            } else {
               count_of_mutations_per_position[static_cast<uint32_t>(symbol)][pos] +=
                  aa_store_partition.sequence_count - aa_store_partition.positions[pos]
                                                         .bitmaps[static_cast<uint32_t>(symbol)]
                                                         .cardinality();
            }
         }
      }
   });
   return count_of_mutations_per_position;
}

QueryResult AAMutations::execute(
   const Database& database,
   std::vector<OperatorResult> bitmap_filter
) const {
   using roaring::Roaring;
   CHECK_SILO_QUERY(
      database.aa_sequences.contains(aa_sequence_name),
      "Database does not contain the amino acid sequence with name: '" + aa_sequence_name + "'"
   )

   const AAStore& aa_store = database.aa_sequences.at(aa_sequence_name);

   const size_t sequence_length = aa_store.reference_sequence.length();

   std::array<std::vector<uint32_t>, MUTATION_SYMBOL_COUNT> count_of_mutations_per_position =
      calculateMutationsPerPosition(aa_store, bitmap_filter);

   std::vector<QueryResultEntry> mutation_proportions;
   {
      for (size_t pos = 0; pos < sequence_length; ++pos) {
         uint32_t total = 0;
         for (auto& count_per_position : count_of_mutations_per_position) {
            total += count_per_position[pos];
         }
         if (total == 0) {
            continue;
         }
         const auto threshold_count =
            static_cast<uint32_t>(std::ceil(static_cast<double>(total) * min_proportion) - 1);

         const auto symbol_in_reference_genome =
            toAASymbol(aa_store.reference_sequence.at(pos)).value();

         for (const auto symbol : VALID_MUTATION_SYMBOLS) {
            if (symbol_in_reference_genome != symbol) {
               const uint32_t count =
                  count_of_mutations_per_position[static_cast<size_t>(symbol)][pos];
               if (count > threshold_count) {
                  const double proportion = static_cast<double>(count) / static_cast<double>(total);
                  const std::map<
                     std::string,
                     std::optional<std::variant<std::string, int32_t, double>>>
                     fields{
                        {"position",
                         AA_SYMBOL_REPRESENTATION[static_cast<size_t>(symbol_in_reference_genome)] +
                            std::to_string(pos + 1) +
                            AA_SYMBOL_REPRESENTATION[static_cast<size_t>(symbol)]},
                        {"proportion", proportion},
                        {"count", static_cast<int32_t>(count)}};
                  mutation_proportions.push_back({fields});
               }
            }
         }
      }
   }

   return {mutation_proportions};
}

void from_json(const nlohmann::json& json, std::unique_ptr<AAMutations>& action) {
   CHECK_SILO_QUERY(
      json.contains("sequenceName") && json["sequenceName"].is_string(),
      "AminoAcideMutations action must have the field sequenceName:string"
   )
   const std::string aa_sequence_name = json["sequenceName"].get<std::string>();
   double min_proportion = AAMutations::DEFAULT_MIN_PROPORTION;
   if (json.contains("minProportion")) {
      min_proportion = json["minProportion"].get<double>();
      if (min_proportion <= 0 || min_proportion > 1) {
         throw QueryParseException(
            "Invalid proportion: minProportion must be in interval (0.0, 1.0]"
         );
      }
   }
   action = std::make_unique<AAMutations>(aa_sequence_name, min_proportion);
}

}  // namespace silo::query_engine::actions
