#ifndef SILO_STRING_COLUMN_H
#define SILO_STRING_COLUMN_H

#include <deque>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <roaring/roaring.hh>

#include "silo/common/bidirectional_map.h"
#include "silo/common/string.h"

namespace boost::serialization {
struct access;
}

namespace silo::storage::column {

class StringColumnPartition {
   friend class boost::serialization::access;

   template <class Archive>
   [[maybe_unused]] void serialize(Archive& archive, const uint32_t /* version */) {
      // clang-format off
      archive& values;
      // clang-format on
   }

   std::vector<common::String<silo::common::STRING_SIZE>> values;
   silo::common::BidirectionalMap<std::string>& lookup;

  public:
   explicit StringColumnPartition(silo::common::BidirectionalMap<std::string>& lookup);

   [[nodiscard]] const std::vector<common::String<silo::common::STRING_SIZE>>& getValues() const;

   void insert(const std::string& value);

   [[nodiscard]] std::optional<common::String<silo::common::STRING_SIZE>> embedString(
      const std::string& string
   ) const;

   [[nodiscard]] inline std::string lookupValue(common::String<silo::common::STRING_SIZE> string
   ) const {
      return string.toString(lookup);
   }
};

class StringColumn {
   friend class boost::serialization::access;

  private:
   template <class Archive>
   [[maybe_unused]] void serialize(Archive& archive, const uint32_t /* version */) {
      // clang-format off
      archive& lookup;
      archive& partitions;
      // clang-format on
      // TODO sync lookups
   }

   std::unique_ptr<silo::common::BidirectionalMap<std::string>> lookup;
   // Need container with pointer stability, because database partitions point into this
   std::deque<StringColumnPartition> partitions;

  public:
   StringColumn();

   StringColumnPartition& createPartition();

   [[nodiscard]] std::optional<common::String<silo::common::STRING_SIZE>> embedString(
      const std::string& string
   ) const;
};

}  // namespace silo::storage::column

#endif  // SILO_STRING_COLUMN_H
