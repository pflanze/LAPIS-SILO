#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <duckdb.hpp>
#include "silo/preprocessing/sql_function.h"
#include "silo/storage/pango_lineage_alias.h"
#include "silo/preprocessing/preprocessing_config.h"

namespace silo {

class ZstdFastaTable;
class ReferenceGenomes;

namespace preprocessing {

class Partitions;

class PreprocessingDatabase {
  public:
   static constexpr std::string_view COMPRESS_NUC = "compressNuc";
   static constexpr std::string_view COMPRESS_AA = "compressAA";
   std::vector<std::shared_ptr<CustomSqlFunction>> registered_functions_;

  private:
   duckdb::DuckDB duck_db;
   duckdb::Connection connection;

  public:
   PreprocessingDatabase(
      const std::string& backing_file,
      const std::vector<std::shared_ptr<CustomSqlFunction>>& registered_functions
   );

   static std::unique_ptr<PreprocessingDatabase> create(
      const preprocessing::PreprocessingConfig& preprocessing_config
   );

   duckdb::Connection& getConnection();

   Partitions getPartitionDescriptor();

   static void registerSequences(const silo::ReferenceGenomes& reference_genomes);

   std::unique_ptr<duckdb::MaterializedQueryResult> query(std::string sql_query);

   ZstdFastaTable generateSequenceTableFromFasta(
      const std::string& table_name,
      const std::string& reference_sequence,
      const std::string& filename
   );

   ZstdFastaTable generateSequenceTableFromZstdFasta(
      const std::string& table_name,
      const std::string& reference_sequence,
      const std::string& filename
   );
};

std::vector<std::string> extractStringListValue(
   duckdb::MaterializedQueryResult& result,
   size_t row,
   size_t column
);

}  // namespace preprocessing
}  // namespace silo