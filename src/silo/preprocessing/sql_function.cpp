#include "silo/preprocessing/sql_function.h"

#include <spdlog/spdlog.h>

#include "silo/common/pango_lineage.h"
#include "silo/storage/reference_genomes.h"
#include "silo/zstdfasta/zstd_compressor.h"

using duckdb::BinaryExecutor;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::ExpressionState;
using duckdb::LogicalType;
using duckdb::string_t;
using duckdb::StringVector;
using duckdb::UnaryExecutor;
using duckdb::Value;
using duckdb::Vector;

silo::CustomSqlFunction::CustomSqlFunction(std::string function_name)
    : function_name(std::move(function_name)) {}

silo::UnaliasPangoLineage::UnaliasPangoLineage(
   std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup
)
    : CustomSqlFunction("pango_lineage_unalias"),
      pango_lineage_alias_lookup(std::move(pango_lineage_alias_lookup)) {}

void silo::UnaliasPangoLineage::applyTo(Connection& connection) const {
   const std::function<void(DataChunk&, ExpressionState&, Vector&)> unalias_pango_lineage_wrapper =
      [&](DataChunk& args, ExpressionState& /*state*/, Vector& result) {
         UnaryExecutor::Execute<string_t, string_t>(
            args.data[0],
            result,
            args.size(),
            [&](const string_t unaliased_pango_lineage) {
               const std::string compressed =
                  pango_lineage_alias_lookup
                     ->aliasPangoLineage({unaliased_pango_lineage.GetString()})
                     .value;

               return StringVector::AddString(
                  result, compressed.data(), static_cast<uint32_t>(compressed.size())
               );
            }
         );
      };

   connection.CreateVectorizedFunction(
      function_name, {LogicalType::VARCHAR}, LogicalType::VARCHAR, unalias_pango_lineage_wrapper
   );
}

std::string silo::UnaliasPangoLineage::generateSqlStatement(const std::string& column_name_in_data
) const {
   return function_name + "(" + column_name_in_data + ")";
}

silo::CompressSequence::CompressSequence(
   const std::string& sequence_name,
   const std::map<std::string, std::string>& reference
)
    : CustomSqlFunction("compress_" + sequence_name) {
   SPDLOG_DEBUG("CompressSequence - initialize with reference_genomes for '{}'", function_name);
   for (const auto& [name, sequence] : reference) {
      SPDLOG_TRACE("CompressSequence - Creating compressor for '{}'", name);
      compressors.emplace(name, silo::ZstdCompressor(sequence));
   }
}

void silo::CompressSequence::applyTo(Connection& connection) const {
   const std::function<void(DataChunk&, ExpressionState&, Vector&)> compressor_wrapper =
      [&](DataChunk& args, ExpressionState& /*state*/, Vector& result) {
         BinaryExecutor::Execute<string_t, string_t, string_t>(
            args.data[0],
            args.data[1],
            result,
            args.size(),
            [&](const string_t uncompressed, const string_t segment_name) {
               auto compressor = compressors.at(segment_name.GetString());
               std::string_view const compressed =
                  compressor.local().compress(uncompressed.GetData(), uncompressed.GetSize());

               return StringVector::AddStringOrBlob(
                  result, compressed.data(), static_cast<uint32_t>(compressed.size())
               );
            }
         );
      };

   connection.CreateVectorizedFunction(
      function_name,
      {LogicalType::VARCHAR, LogicalType::VARCHAR},
      LogicalType::BLOB,
      compressor_wrapper
   );
}
std::string silo::CompressSequence::generateSqlStatement(
   const std::string& column_name_in_data,
   const std::string& sequence_name
) const {
   return function_name + "(" + column_name_in_data + ", '" + sequence_name + "')";
}
