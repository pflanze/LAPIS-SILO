#ifndef SILO_PATTERN_SEARCH_H
#define SILO_PATTERN_SEARCH_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "silo/query_engine/filter_expressions/expression.h"

namespace silo {
class Database;
class DatabasePartition;
enum class NUCLEOTIDE_SYMBOL : char;

namespace query_engine {
namespace operators {
class Operator;
}  // namespace operators
}  // namespace query_engine
}  // namespace silo

namespace silo::query_engine::filter_expressions {

struct PatternSearch : public Expression {
   std::optional<std::string> nuc_sequence_name;
   uint32_t position;
   std::vector<NUCLEOTIDE_SYMBOL> pattern;

   explicit PatternSearch(
      std::optional<std::string> nuc_sequence_name,
      uint32_t position,
      std::vector<NUCLEOTIDE_SYMBOL> pattern
   );

   std::string toString(const Database& database) const override;

   [[nodiscard]] std::unique_ptr<silo::query_engine::operators::Operator> compile(
      const Database& database,
      const DatabasePartition& database_partition,
      AmbiguityMode mode
   ) const override;
};

// NOLINTNEXTLINE(readability-identifier-naming)
void from_json(const nlohmann::json& json, std::unique_ptr<PatternSearch>& filter);

}  // namespace silo::query_engine::filter_expressions

#endif  // SILO_PATTERN_SEARCH_H