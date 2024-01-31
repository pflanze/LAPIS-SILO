#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <duckdb.hpp>
#include "silo/storage/pango_lineage_alias.h"
#include "silo/preprocessing/preprocessing_config.h"

namespace silo {

class ZstdFastaTable;
class ReferenceGenomes;
class UnaliasPangoLineage;
class CompressSequence;

namespace preprocessing {

class Partitions;

class PreprocessingDatabase {
  public:
   std::unique_ptr<CompressSequence> compress_nucleotide_function;
   std::unique_ptr<CompressSequence> compress_amino_acid_function;
   std::unique_ptr<UnaliasPangoLineage> unalias_pango_lineage_function;

  private:
   duckdb::DuckDB duck_db;
   duckdb::Connection connection;

  public:
   PreprocessingDatabase(
      const std::string& backing_file,
      std::shared_ptr<ReferenceGenomes> reference_genomes,
      std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup
   );

   duckdb::Connection& getConnection();

   Partitions getPartitionDescriptor();

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