#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <roaring/roaring.hh>

#include "silo/common/aa_symbols.h"
#include "silo/common/nucleotide_symbols.h"
#include "silo/common/symbol_map.h"
#include "silo/storage/position.h"

namespace boost::serialization {
class access;
}  // namespace boost::serialization

namespace silo {
template <typename SymbolType>
class Position;
class ZstdFastaTableReader;

struct SequenceStoreInfo {
   uint32_t sequence_count;
   uint64_t size;
   size_t n_bitmaps_size;
};

template <typename SymbolType>
class SequenceStorePartition {
   friend class boost::serialization::access;

   template <class Archive>
   void serialize(Archive& archive, [[maybe_unused]] const uint32_t version) {
      // clang-format off
      archive & indexing_differences_to_reference_sequence;
      for(auto& position : positions){
            archive & position;
      }
      archive & missing_symbol_bitmaps;
      archive & sequence_count;
      // clang-format on
   }

  public:
   const std::vector<typename SymbolType::Symbol>& reference_sequence;
   std::vector<std::pair<size_t, typename SymbolType::Symbol>>
      indexing_differences_to_reference_sequence;
   std::vector<Position<SymbolType>> positions;
   std::vector<roaring::Roaring> missing_symbol_bitmaps;
   uint32_t sequence_count = 0;

  private:
   void fillIndexes(const std::vector<std::optional<std::string>>& genomes);

   void addSymbolsToPositions(
      const size_t& position,
      SymbolMap<SymbolType, std::vector<uint32_t>>& ids_per_symbol_for_current_position,
      const size_t number_of_sequences
   );

   void fillNBitmaps(const std::vector<std::optional<std::string>>& genomes);

   void optimizeBitmaps();

  public:
   explicit SequenceStorePartition(
      const std::vector<typename SymbolType::Symbol>& reference_sequence
   );

   [[nodiscard]] size_t computeSize() const;

   [[nodiscard]] const roaring::Roaring* getBitmap(
      size_t position,
      typename SymbolType::Symbol symbol
   ) const;

   [[nodiscard]] SequenceStoreInfo getInfo() const;

   size_t fill(silo::ZstdFastaTableReader& input);

   void interpret(const std::vector<std::optional<std::string>>& genomes);
};

template <typename SymbolType>
class SequenceStore {
  public:
   std::vector<typename SymbolType::Symbol> reference_sequence;
   std::deque<SequenceStorePartition<SymbolType>> partitions;

   explicit SequenceStore(std::vector<typename SymbolType::Symbol> reference_sequence);

   SequenceStorePartition<SymbolType>& createPartition();
};

}  // namespace silo

template <>
struct [[maybe_unused]] fmt::formatter<silo::SequenceStoreInfo> : fmt::formatter<std::string> {
   [[maybe_unused]] static auto format(
      silo::SequenceStoreInfo sequence_store_info,
      format_context& ctx
   ) -> decltype(ctx.out());
};
