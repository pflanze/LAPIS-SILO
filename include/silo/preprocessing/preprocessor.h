#pragma once

#include "silo/config/database_config.h"
#include "silo/preprocessing/preprocessing_config.h"
#include "silo/preprocessing/preprocessing_database.h"

namespace silo {
class Database;
class PangoLineageAliasLookup;
class ReferenceGenomes;

namespace preprocessing {

class Preprocessor {
   PreprocessingConfig preprocessing_config;
   PreprocessingDatabase preprocessing_db;
   config::DatabaseConfig database_config;
   std::shared_ptr<ReferenceGenomes> reference_genomes;
   std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup;

  public:
   Preprocessor(
      preprocessing::PreprocessingConfig preprocessing_config,
      config::DatabaseConfig database_config,
      std::shared_ptr<ReferenceGenomes> reference_genomes,
      std::shared_ptr<PangoLineageAliasLookup> pango_lineage_alias_lookup
   );

   Database preprocess();

  private:
   void buildTablesFromNdjsonInput(const std::filesystem::path& file_name);
   void buildMetadataTableFromFile(const std::filesystem::path& metadata_filename);

   void buildPartitioningTable();
   void buildPartitioningTableByColumn(const std::string& partition_by_field);
   void buildEmptyPartitioning();

   void createSequenceViews();
   void createPartitionedSequenceTables();
   void createPartitionedTableForSequence(
      const std::string& sequence_name,
      const std::string& reference_sequence,
      const std::filesystem::path& filename,
      const std::string& table_prefix
   );

   Database buildDatabase(
      const preprocessing::Partitions& partition_descriptor,
      const ReferenceGenomes& reference_genomes,
      const std::string& order_by_clause,
      const silo::PangoLineageAliasLookup& alias_key,
      const std::filesystem::path& intermediate_results_directory
   );
};
}  // namespace preprocessing
}  // namespace silo
