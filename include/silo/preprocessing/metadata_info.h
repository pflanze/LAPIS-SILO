#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include "silo/config/database_config.h"

namespace silo::preprocessing {

class PreprocessingDatabase;

struct MetadataName {
   std::string name;
};

struct MetadataWithType {
   std::string name;
   config::ValueType type;
};

struct MetadataWithPath {
   std::string name;
   config::ValueType type;
   std::string path_in_file;
};

struct MetadataForSelect {
   std::string name;
   config::ValueType type;
   std::string path_in_file;
   std::string sql_select_expression;
};

enum class MetadataFileType { TSV, NDJSON };

class MetadataInfo {
   static constexpr std::string_view POSSIBLE_NDJSON_TOPLEVEL_INSERTION_NAME =
      "nucleotideInsertions";
   static constexpr std::string_view POSSIBLE_NDJSON_TOPLEVEL_AA_INSERTION_NAME =
      "aminoAcidInsertions";

      std::unordered_map<std::string, std::string>
         metadata_selects;

   MetadataInfo(std::unordered_map<std::string, std::string> metadata_selects);

  public:
   static MetadataInfo validateFromMetadataFile(
      const std::filesystem::path& metadata_file,
      const silo::config::DatabaseConfig& database_config,
      const silo::preprocessing::PreprocessingDatabase& preprocessing_database

   );

   static MetadataInfo validateFromNdjsonFile(
      const std::filesystem::path& ndjson_file,
      const silo::config::DatabaseConfig& database_config,
      const silo::preprocessing::PreprocessingDatabase& preprocessing_database
   );

   static std::vector<MetadataName> getMetadataNamesFromFile(
      const std::filesystem::path& metadata_file,
      MetadataFileType metadata_file_type
   );
   static std::vector<MetadataName> getMetadataNamesFromNdjson(
      const std::filesystem::path& ndjson_file
   );
   static std::vector<MetadataName> getMetadataNamesFromTsvFile(
      const std::filesystem::path& tsv_file
   );
   static std::vector<MetadataName> addInsertionsFromTopLevel(
      const std::vector<MetadataName>& metadata_names,
      const std::filesystem::path& ndjson_file
   );
   static std::vector<MetadataName> getValidMetadataNames(
      const std::vector<MetadataName>& metadata_names,
      const silo::config::DatabaseConfig& database_config
   );

   static std::vector<MetadataWithType> getTypesForMetadata(
      const std::vector<MetadataName>& metadata_names,
      const silo::config::DatabaseConfig& database_config
   );
   static std::vector<MetadataWithPath> getMetadataWithPath(
      const std::vector<MetadataWithType>& metadata_with_type,
      const std::filesystem::path& metadata_file
   );
   static std::vector<MetadataForSelect> getMetadataForSelect(
      const std::vector<MetadataWithPath>& metadata_with_path,
      const silo::preprocessing::PreprocessingDatabase& preprocessing_database
   );

   std::vector<std::string> getMetadataFields() const;

   std::vector<std::string> getMetadataSelects() const;
};
}  // namespace silo::preprocessing
