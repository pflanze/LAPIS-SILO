#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>

#include <duckdb.hpp>

#include <oneapi/tbb/enumerable_thread_specific.h>
#include "silo/storage/pango_lineage_alias.h"
#include "silo/zstdfasta/zstd_compressor.h"

namespace silo {

class ZstdCompressor;

class CustomSqlFunction {
  public:
   explicit CustomSqlFunction(std::string function_name);

   virtual void applyTo(duckdb::Connection& connection) const = 0;

  protected:
   std::string function_name;
};

class UnaliasPangoLineage : public CustomSqlFunction {
  public:
   explicit UnaliasPangoLineage(std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup
   );

   void applyTo(duckdb::Connection& connection) const override;

   [[nodiscard]] std::string generateSqlStatement(const std::string& column_name_in_data) const;

  private:
   std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup;
};

class CompressSequence : public CustomSqlFunction {
  public:
   CompressSequence(
      const std::string& sequence_name,
      const std::map<std::string, std::string>& reference
   );

   void applyTo(duckdb::Connection& connection) const override;

   std::string generateSqlStatement(
      const std::string& column_name_in_data,
      const std::string& sequence_name
   ) const;

  private:
   std::unordered_map<std::string_view, tbb::enumerable_thread_specific<silo::ZstdCompressor>>
      compressors;
};

}  // namespace silo