#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include <duckdb.hpp>

#include "silo/storage/pango_lineage_alias.h"

namespace silo {
class CustomSqlFunction {
  public:
   CustomSqlFunction(std::string function_name);

   virtual void applyTo(duckdb::Connection& connection) const = 0;
   virtual std::string getFunctionName() const = 0;

  protected:
   std::string function_name;
};

class UnaliasPangoLineage : public CustomSqlFunction {
  public:
   UnaliasPangoLineage(
      std::optional<std::filesystem::path> alias_key_file,
      const std::string& function_name
   );

   void applyTo(duckdb::Connection& connection) const override;
   std::string getFunctionName() const override;

  private:
   PangoLineageAliasLookup pango_lineage_alias_lookup;
};

}  // namespace silo