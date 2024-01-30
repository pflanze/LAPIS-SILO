#include "silo/preprocessing/sql_function.h"

#include "silo/common/pango_lineage.h"

silo::CustomSqlFunction::CustomSqlFunction(std::string function_name)
    : function_name(std::move(function_name)) {}

silo::UnaliasPangoLineage::UnaliasPangoLineage(
   std::optional<std::filesystem::path> alias_key_file,
   const std::string& function_name
)
    : CustomSqlFunction(function_name) {
   if (alias_key_file.has_value()) {
      pango_lineage_alias_lookup = PangoLineageAliasLookup::readFromFile(alias_key_file.value());
   }
}

void silo::UnaliasPangoLineage::applyTo(duckdb::Connection& connection) const {
   const std::function<void(duckdb::DataChunk&, duckdb::ExpressionState&, duckdb::Vector&)>
      unalias_pango_lineage_wrapper =
         [&](duckdb::DataChunk& args, duckdb::ExpressionState&, duckdb::Vector& result) {
            duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
               args.data[0],
               result,
               args.size(),
               [&](const duckdb::string_t unaliased_pango_lineage) {
                  const std::string compressed =
                     pango_lineage_alias_lookup
                        .aliasPangoLineage({unaliased_pango_lineage.GetString()})
                        .value;

                  return duckdb::StringVector::AddString(
                     result, compressed.data(), static_cast<uint32_t>(compressed.size())
                  );
               }
            );
         };

   connection.CreateVectorizedFunction(
      function_name,
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::VARCHAR,
      unalias_pango_lineage_wrapper
   );
}
std::string silo::UnaliasPangoLineage::getFunctionName() const {
   return function_name;
}
