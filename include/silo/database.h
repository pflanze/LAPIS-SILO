#ifndef SILO_DATABASE_H
#define SILO_DATABASE_H

#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "silo/storage/dictionary.h"
#include "silo/storage/metadata_store.h"
#include "silo/storage/sequence_store.h"
#include "silo/storage/database_partition.h"

namespace silo {

struct Partition {
   std::string name;
   uint32_t count;
   std::vector<Chunk> chunks;
};

struct Partitions {
   std::vector<Partition> partitions;
};

struct PangoLineageCount {
   std::string pango_lineage;
   uint32_t count;
};

struct PangoLineageCounts {
   std::vector<PangoLineageCount> pango_lineage_counts;
};

struct DatabaseInfo;
struct DetailedDatabaseInfo;
struct BitmapSizePerSymbol;
struct BitmapContainerSize;

struct PreprocessingConfig;

class Database {
  public:
   const std::string working_directory;
   std::vector<std::string> global_reference;
   std::vector<DatabasePartition> partitions;
   std::unique_ptr<PangoLineageCounts> pango_descriptor;
   std::unique_ptr<Partitions> partition_descriptor;
   std::unique_ptr<Dictionary> dict;

   Database();

   explicit Database(const std::string& directory);

   void preprocessing(const PreprocessingConfig& config);

   void build(
      const std::string& partition_name_prefix,
      const std::string& metadata_file_suffix,
      const std::string& sequence_file_suffix
   );

   virtual silo::DatabaseInfo getDatabaseInfo() const;

   virtual DetailedDatabaseInfo detailedDatabaseInfo() const;
   void finalizeBuild();

   [[maybe_unused]] void flipBitmaps();

   [[maybe_unused]] void indexAllNucleotideSymbolsN();

   [[maybe_unused]] void naiveIndexAllNucleotideSymbolsN();

   [[maybe_unused]] void saveDatabaseState(const std::string& save_directory);

   [[maybe_unused]] [[maybe_unused]] void loadDatabaseState(const std::string& save_directory);

   [[nodiscard]] const std::unordered_map<std::string, std::string>& getAliasKey() const;

  private:
   std::unordered_map<std::string, std::string> alias_key;
   BitmapSizePerSymbol calculateBitmapSizePerSymbol() const;
   BitmapContainerSize calculateBitmapContainerSizePerGenomeSection(uint32_t section_length) const;
};

unsigned fillSequenceStore(SequenceStore& sequence_store, std::istream& input_file);

unsigned fillMetadataStore(
   MetadataStore& meta_store,
   std::istream& input_file,
   const std::unordered_map<std::string, std::string>& alias_key,
   const Dictionary& dict
);

void savePangoLineageCounts(
   const PangoLineageCounts& pango_lineage_counts,
   std::ostream& output_file
);

PangoLineageCounts loadPangoLineageCounts(std::istream& input_stream);

void savePartitions(const Partitions& partitions, std::ostream& output_file);

Partitions loadPartitions(std::istream& input_file);

std::string resolvePangoLineageAlias(
   const std::unordered_map<std::string, std::string>& alias_key,
   const std::string& pango_lineage
);

std::string buildChunkName(unsigned partition, unsigned chunk);

}  // namespace silo

#endif  // SILO_DATABASE_H
