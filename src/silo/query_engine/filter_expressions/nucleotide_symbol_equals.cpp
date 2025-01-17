#include "silo/query_engine/filter_expressions/nucleotide_symbol_equals.h"

#include <array>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "silo/common/nucleotide_symbols.h"
#include "silo/config/database_config.h"
#include "silo/database.h"
#include "silo/query_engine/filter_expressions/and.h"
#include "silo/query_engine/filter_expressions/expression.h"
#include "silo/query_engine/filter_expressions/negation.h"
#include "silo/query_engine/filter_expressions/or.h"
#include "silo/query_engine/operators/bitmap_selection.h"
#include "silo/query_engine/operators/complement.h"
#include "silo/query_engine/operators/index_scan.h"
#include "silo/query_engine/operators/operator.h"
#include "silo/query_engine/query_parse_exception.h"
#include "silo/storage/database_partition.h"

using silo::Nucleotide;

namespace {

const std::array<std::vector<Nucleotide::Symbol>, Nucleotide::COUNT> AMBIGUITY_NUC_SYMBOLS{{
   {Nucleotide::Symbol::GAP},
   {Nucleotide::Symbol::A,
    Nucleotide::Symbol::R,
    Nucleotide::Symbol::M,
    Nucleotide::Symbol::W,
    Nucleotide::Symbol::D,
    Nucleotide::Symbol::H,
    Nucleotide::Symbol::V,
    Nucleotide::Symbol::N},
   {Nucleotide::Symbol::C,
    Nucleotide::Symbol::Y,
    Nucleotide::Symbol::M,
    Nucleotide::Symbol::S,
    Nucleotide::Symbol::B,
    Nucleotide::Symbol::H,
    Nucleotide::Symbol::V,
    Nucleotide::Symbol::N},
   {Nucleotide::Symbol::G,
    Nucleotide::Symbol::R,
    Nucleotide::Symbol::K,
    Nucleotide::Symbol::S,
    Nucleotide::Symbol::B,
    Nucleotide::Symbol::D,
    Nucleotide::Symbol::V,
    Nucleotide::Symbol::N},
   {Nucleotide::Symbol::T,
    Nucleotide::Symbol::Y,
    Nucleotide::Symbol::K,
    Nucleotide::Symbol::W,
    Nucleotide::Symbol::B,
    Nucleotide::Symbol::D,
    Nucleotide::Symbol::H,
    Nucleotide::Symbol::N},
   {Nucleotide::Symbol::R},
   {Nucleotide::Symbol::Y},
   {Nucleotide::Symbol::S},
   {Nucleotide::Symbol::W},
   {Nucleotide::Symbol::K},
   {Nucleotide::Symbol::M},
   {Nucleotide::Symbol::B},
   {Nucleotide::Symbol::D},
   {Nucleotide::Symbol::H},
   {Nucleotide::Symbol::V},
   {Nucleotide::Symbol::N},
}};
}  // namespace

namespace silo::query_engine::filter_expressions {

NucleotideSymbolEquals::NucleotideSymbolEquals(
   std::optional<std::string> nuc_sequence_name,
   uint32_t position,
   std::optional<Nucleotide::Symbol> value
)
    : nuc_sequence_name(std::move(nuc_sequence_name)),
      position(position),
      value(value) {}

std::string NucleotideSymbolEquals::toString(const silo::Database& /*database*/) const {
   const std::string nuc_sequence_name_prefix =
      nuc_sequence_name ? nuc_sequence_name.value() + ":" : "";
   const char symbol_char = value.has_value() ? Nucleotide::symbolToChar(*value) : '.';
   return nuc_sequence_name_prefix + std::to_string(position + 1) + std::to_string(symbol_char);
}

std::unique_ptr<silo::query_engine::operators::Operator> NucleotideSymbolEquals::compile(
   const silo::Database& database,
   const silo::DatabasePartition& database_partition,
   Expression::AmbiguityMode mode
) const {
   const std::string nuc_sequence_name_or_default =
      nuc_sequence_name.value_or(database.database_config.default_nucleotide_sequence);
   CHECK_SILO_QUERY(
      database.nuc_sequences.contains(nuc_sequence_name_or_default),
      "Database does not contain the nucleotide sequence with name: '" +
         nuc_sequence_name_or_default + "'"
   )
   const auto& seq_store_partition =
      database_partition.nuc_sequences.at(nuc_sequence_name_or_default);
   if (position >= seq_store_partition.reference_sequence.size()) {
      throw QueryParseException(
         "NucleotideEquals position is out of bounds '" + std::to_string(position + 1) + "' > '" +
         std::to_string(seq_store_partition.reference_sequence.size()) + "'"
      );
   }
   const Nucleotide::Symbol nucleotide_symbol =
      value.value_or(seq_store_partition.reference_sequence.at(position));
   if (mode == UPPER_BOUND) {
      auto symbols_to_match = AMBIGUITY_NUC_SYMBOLS.at(static_cast<uint32_t>(nucleotide_symbol));
      std::vector<std::unique_ptr<Expression>> symbol_filters;
      std::transform(
         symbols_to_match.begin(),
         symbols_to_match.end(),
         std::back_inserter(symbol_filters),
         [&](silo::Nucleotide::Symbol symbol) {
            return std::make_unique<NucleotideSymbolEquals>(
               nuc_sequence_name_or_default, position, symbol
            );
         }
      );
      return Or(std::move(symbol_filters)).compile(database, database_partition, NONE);
   }
   if (nucleotide_symbol == Nucleotide::SYMBOL_MISSING) {
      SPDLOG_TRACE(
         "Filtering for '{}' at position {}",
         Nucleotide::symbolToChar(Nucleotide::SYMBOL_MISSING),
         position
      );
      return std::make_unique<operators::BitmapSelection>(
         seq_store_partition.missing_symbol_bitmaps.data(),
         seq_store_partition.missing_symbol_bitmaps.size(),
         operators::BitmapSelection::CONTAINS,
         position
      );
   }
   if (seq_store_partition.positions[position].isSymbolFlipped(nucleotide_symbol)) {
      SPDLOG_TRACE(
         "Filtering for flipped symbol '{}' at position {}",
         Nucleotide::symbolToChar(nucleotide_symbol),
         position
      );
      return std::make_unique<operators::Complement>(
         std::make_unique<operators::IndexScan>(
            seq_store_partition.getBitmap(position, nucleotide_symbol),
            database_partition.sequence_count
         ),
         database_partition.sequence_count
      );
   }
   if (seq_store_partition.positions[position].isSymbolDeleted(nucleotide_symbol)) {
      SPDLOG_TRACE(
         "Filtering for deleted symbol '{}' at position {}",
         Nucleotide::symbolToChar(nucleotide_symbol),
         position
      );
      std::vector<Nucleotide::Symbol> symbols =
         std::vector<Nucleotide::Symbol>(Nucleotide::SYMBOLS.begin(), Nucleotide::SYMBOLS.end());
      // NOLINTNEXTLINE(bugprone-unused-return-value)
      (void)std::remove(symbols.begin(), symbols.end(), nucleotide_symbol);
      std::vector<std::unique_ptr<filter_expressions::Expression>> symbol_filters;
      std::transform(
         symbols.begin(),
         symbols.end(),
         std::back_inserter(symbol_filters),
         [&](Nucleotide::Symbol symbol) {
            return std::make_unique<Negation>(std::make_unique<NucleotideSymbolEquals>(
               nuc_sequence_name_or_default, position, symbol
            ));
         }
      );
      return And(std::move(symbol_filters)).compile(database, database_partition, NONE);
   }
   SPDLOG_TRACE(
      "Filtering for symbol '{}' at position {}",
      Nucleotide::symbolToChar(nucleotide_symbol),
      position
   );
   return std::make_unique<operators::IndexScan>(
      seq_store_partition.getBitmap(position, nucleotide_symbol), database_partition.sequence_count
   );
}

// NOLINTNEXTLINE(readability-identifier-naming)
void from_json(const nlohmann::json& json, std::unique_ptr<NucleotideSymbolEquals>& filter) {
   CHECK_SILO_QUERY(
      json.is_object() && json.contains("position"),
      "The field 'position' is required in a NucleotideEquals expression"
   )
   CHECK_SILO_QUERY(
      json["position"].is_number_unsigned() && json["position"].get<uint32_t>() > 0,
      "The field 'position' in a NucleotideEquals expression needs to be an unsigned "
      "integer greater than 0"
   )
   CHECK_SILO_QUERY(
      json.contains("symbol"), "The field 'symbol' is required in a NucleotideEquals expression"
   )
   CHECK_SILO_QUERY(
      json["symbol"].is_string(),
      "The field 'symbol' in a NucleotideEquals expression needs to be a string"
   )
   std::optional<std::string> nuc_sequence_name;
   if (json.contains("sequenceName")) {
      nuc_sequence_name = json["sequenceName"].get<std::string>();
   }
   const uint32_t position = json["position"].get<uint32_t>() - 1;
   const std::string& nucleotide_symbol = json["symbol"];

   CHECK_SILO_QUERY(
      nucleotide_symbol.size() == 1, "The string field 'symbol' must be exactly one character long"
   )
   const std::optional<Nucleotide::Symbol> nuc_value =
      Nucleotide::charToSymbol(nucleotide_symbol.at(0));
   CHECK_SILO_QUERY(
      nuc_value.has_value() || nucleotide_symbol.at(0) == '.',
      "The string field 'symbol' must be either a valid nucleotide symbol or the '.' symbol."
   )

   filter = std::make_unique<NucleotideSymbolEquals>(nuc_sequence_name, position, nuc_value);
}

}  // namespace silo::query_engine::filter_expressions
