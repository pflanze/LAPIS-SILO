#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_for_each.h>
#include <cmath>
#include <silo/common/PerfEvent.hpp>
#include "silo/query_engine/query_engine.h"

uint64_t silo::executeCount(
   const silo::Database& /*database*/,
   std::vector<silo::BooleanExpressionResult>& partition_filters
) {
   std::atomic<uint32_t> count = 0;
   tbb::parallel_for_each(partition_filters.begin(), partition_filters.end(), [&](auto& filter) {
      count += filter.getAsConst()->cardinality();
      filter.free();
   });
   return count;
}
// TODO(someone): reduce cognitive complexity
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::vector<silo::MutationProportion> silo::executeMutations(
   const silo::Database& database,
   std::vector<silo::BooleanExpressionResult>& partition_filters,
   double proportion_threshold,
   std::ostream& performance_file
) {
   using roaring::Roaring;

   std::vector<uint32_t> count_of_nucleotide_symbols_a_at_position(silo::GENOME_LENGTH);
   std::vector<uint32_t> count_of_nucleotide_symbols_c_at_position(silo::GENOME_LENGTH);
   std::vector<uint32_t> count_of_nucleotide_symbols_g_at_position(silo::GENOME_LENGTH);
   std::vector<uint32_t> count_of_nucleotide_symbols_t_at_position(silo::GENOME_LENGTH);
   std::vector<uint32_t> count_of_gaps_at_position(silo::GENOME_LENGTH);

   std::vector<unsigned> partition_filters_to_evaluate;
   std::vector<unsigned> full_partition_filters_to_evaluate;
   for (unsigned i = 0; i < database.partitions.size(); ++i) {
      const silo::DatabasePartition& dbp = database.partitions[i];
      silo::BooleanExpressionResult filter = partition_filters[i];
      const Roaring& bitmap = *filter.getAsConst();
      // TODO(taepper) check naive run_compression
      const unsigned card = bitmap.cardinality();
      if (card == 0) {
         continue;
      }
      if (card == dbp.sequenceCount) {
         full_partition_filters_to_evaluate.push_back(i);
      } else {
         if (filter.mutable_res) {
            filter.mutable_res->runOptimize();
         }
         partition_filters_to_evaluate.push_back(i);
      }
   }

   int64_t microseconds = 0;
   {
      BlockTimer const timer(microseconds);
      static constexpr int POSITIONS_PER_PROCESS = 300;
      tbb::blocked_range<uint32_t> const range(
         0, silo::GENOME_LENGTH, /*grain_size=*/POSITIONS_PER_PROCESS
      );
      // TODO(someone): reduce cognitive complexity
      // NOLINTNEXTLINE(readability-function-cognitive-complexity)
      tbb::parallel_for(range.begin(), range.end(), [&](uint32_t pos) {
         for (unsigned const partition_index : partition_filters_to_evaluate) {
            const silo::DatabasePartition& database_partition =
               database.partitions[partition_index];
            silo::BooleanExpressionResult const filter = partition_filters[partition_index];
            const Roaring& bitmap = *filter.getAsConst();

            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::A) {
               count_of_nucleotide_symbols_a_at_position[pos] += bitmap.and_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::A]
               );
            } else {
               count_of_nucleotide_symbols_a_at_position[pos] += bitmap.andnot_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::A]
               );
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::C) {
               count_of_nucleotide_symbols_c_at_position[pos] += bitmap.and_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::C]
               );
            } else {
               count_of_nucleotide_symbols_c_at_position[pos] += bitmap.andnot_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::C]
               );
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::G) {
               count_of_nucleotide_symbols_g_at_position[pos] += bitmap.and_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::G]
               );
            } else {
               count_of_nucleotide_symbols_g_at_position[pos] += bitmap.andnot_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::G]
               );
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::T) {
               count_of_nucleotide_symbols_t_at_position[pos] += bitmap.and_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::T]
               );
            } else {
               count_of_nucleotide_symbols_t_at_position[pos] += bitmap.andnot_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::T]
               );
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::GAP) {
               count_of_gaps_at_position[pos] += bitmap.and_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::GAP]
               );
            } else {
               count_of_gaps_at_position[pos] += bitmap.andnot_cardinality(
                  database_partition.seq_store.positions[pos].bitmaps[silo::GENOME_SYMBOL::GAP]
               );
            }
         }
         /// For these partitions, we have full bitmaps. Do not need to bother with AND cardinality
         for (unsigned const partition_index : full_partition_filters_to_evaluate) {
            const silo::DatabasePartition& database_partition =
               database.partitions[partition_index];
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::A) {
               count_of_nucleotide_symbols_a_at_position[pos] +=
                  database_partition.seq_store.positions[pos]
                     .bitmaps[silo::GENOME_SYMBOL::A]
                     .cardinality();
            } else {
               count_of_nucleotide_symbols_a_at_position[pos] +=
                  database_partition.sequenceCount - database_partition.seq_store.positions[pos]
                                                        .bitmaps[silo::GENOME_SYMBOL::A]
                                                        .cardinality();
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::C) {
               count_of_nucleotide_symbols_c_at_position[pos] +=
                  database_partition.seq_store.positions[pos]
                     .bitmaps[silo::GENOME_SYMBOL::C]
                     .cardinality();
            } else {
               count_of_nucleotide_symbols_c_at_position[pos] +=
                  database_partition.sequenceCount - database_partition.seq_store.positions[pos]
                                                        .bitmaps[silo::GENOME_SYMBOL::C]
                                                        .cardinality();
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::G) {
               count_of_nucleotide_symbols_g_at_position[pos] +=
                  database_partition.seq_store.positions[pos]
                     .bitmaps[silo::GENOME_SYMBOL::G]
                     .cardinality();
            } else {
               count_of_nucleotide_symbols_g_at_position[pos] +=
                  database_partition.sequenceCount - database_partition.seq_store.positions[pos]
                                                        .bitmaps[silo::GENOME_SYMBOL::G]
                                                        .cardinality();
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::T) {
               count_of_nucleotide_symbols_t_at_position[pos] +=
                  database_partition.seq_store.positions[pos]
                     .bitmaps[silo::GENOME_SYMBOL::T]
                     .cardinality();
            } else {
               count_of_nucleotide_symbols_t_at_position[pos] +=
                  database_partition.sequenceCount - database_partition.seq_store.positions[pos]
                                                        .bitmaps[silo::GENOME_SYMBOL::T]
                                                        .cardinality();
            }
            if (database_partition.seq_store.positions[pos].flipped_bitmap != silo::GENOME_SYMBOL::GAP) {
               count_of_gaps_at_position[pos] += database_partition.seq_store.positions[pos]
                                                    .bitmaps[silo::GENOME_SYMBOL::GAP]
                                                    .cardinality();
            } else {
               count_of_gaps_at_position[pos] +=
                  database_partition.sequenceCount - database_partition.seq_store.positions[pos]
                                                        .bitmaps[silo::GENOME_SYMBOL::GAP]
                                                        .cardinality();
            }
         }
      });
   }
   performance_file << "pos_calculation\t" << std::to_string(microseconds) << std::endl;

   for (unsigned i = 0; i < database.partitions.size(); ++i) {
      partition_filters[i].free();
   }

   std::vector<silo::MutationProportion> mutation_proportions;
   microseconds = 0;
   {
      BlockTimer const timer(microseconds);
      for (unsigned pos = 0; pos < silo::GENOME_LENGTH; ++pos) {
         uint32_t const total = count_of_nucleotide_symbols_a_at_position[pos] +
                                count_of_nucleotide_symbols_c_at_position[pos] +
                                count_of_nucleotide_symbols_g_at_position[pos] +
                                count_of_nucleotide_symbols_t_at_position[pos] +
                                count_of_gaps_at_position[pos];
         if (total == 0) {
            continue;
         }
         auto const threshold_count =
            static_cast<uint32_t>(std::ceil(static_cast<double>(total) * proportion_threshold) - 1);

         char const pos_ref = database.global_reference[0].at(pos);
         if (pos_ref != 'A') {
            const uint32_t tmp = count_of_nucleotide_symbols_a_at_position[pos];
            if (tmp > threshold_count) {
               double const proportion = static_cast<double>(tmp) / static_cast<double>(total);
               mutation_proportions.push_back({pos_ref, pos, 'A', proportion, tmp});
            }
         }
         if (pos_ref != 'C') {
            const uint32_t tmp = count_of_nucleotide_symbols_c_at_position[pos];
            if (tmp > threshold_count) {
               double const proportion = static_cast<double>(tmp) / static_cast<double>(total);
               mutation_proportions.push_back({pos_ref, pos, 'C', proportion, tmp});
            }
         }
         if (pos_ref != 'G') {
            const uint32_t tmp = count_of_nucleotide_symbols_g_at_position[pos];
            if (tmp > threshold_count) {
               double const proportion = static_cast<double>(tmp) / static_cast<double>(total);
               mutation_proportions.push_back({pos_ref, pos, 'G', proportion, tmp});
            }
         }
         if (pos_ref != 'T') {
            const uint32_t tmp = count_of_nucleotide_symbols_t_at_position[pos];
            if (tmp > threshold_count) {
               double const proportion = static_cast<double>(tmp) / static_cast<double>(total);
               mutation_proportions.push_back({pos_ref, pos, 'T', proportion, tmp});
            }
         }
         /// This should always be the case. For future-proof-ness (gaps in reference), keep this
         /// check in.
         if (pos_ref != '-') {
            const uint32_t tmp = count_of_gaps_at_position[pos];
            if (tmp > threshold_count) {
               double const proportion = static_cast<double>(tmp) / static_cast<double>(total);
               mutation_proportions.push_back({pos_ref, pos, '-', proportion, tmp});
            }
         }
      }
   }
   performance_file << "Proportion_calculation\t" << std::to_string(microseconds) << std::endl;

   return mutation_proportions;
}