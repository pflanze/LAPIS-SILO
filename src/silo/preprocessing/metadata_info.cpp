#include "silo/preprocessing/metadata_info.h"

#include <spdlog/spdlog.h>
#include <boost/algorithm/string/join.hpp>
#include <duckdb.hpp>

#include "silo/config/database_config.h"
#include "silo/preprocessing/preprocessing_database.h"
#include "silo/preprocessing/preprocessing_exception.h"
#include "silo/preprocessing/sql_function.h"

namespace {

void logMetadataFields(std::unordered_map<std::string, std::string>& validated_metadata_fields) {
   std::string metadata_field_string;
   for (const auto& [field_name, select] : validated_metadata_fields) {
      metadata_field_string += "'";
      metadata_field_string += field_name;
      metadata_field_string += "' with selection '";
      metadata_field_string += select;
      metadata_field_string += "',";
   }
   SPDLOG_TRACE("Found metadata fields: " + metadata_field_string);
}

std::unordered_map<std::string, std::string> validateFieldsAgainstConfig(
   const std::unordered_map<std::string, std::string>& found_metadata_fields,
   const silo::config::DatabaseConfig& database_config,
   const silo::preprocessing::PreprocessingDatabase& preprocessing_database
) {
   std::vector<std::string> config_metadata_names = database_config.getMetadataNames();
   std::vector<std::string> column_names = found_metadata_fields | boost::adaptors::map_keys;

   const std::set<std::string> config_metadata_names_set(
      config_metadata_names.begin(), config_metadata_names.end()
   );
   const std::set<std::string> column_names_set(column_names.begin(), column_names.end());

   std::set<std::string> not_in_config;
   std::set_difference(
      column_names_set.begin(),
      column_names_set.end(),
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      std::inserter(not_in_config, not_in_config.begin())
   );
   if (not_in_config.size() > 0) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "The metadata fields '{}' which are contained in the input are not contained in the "
         "database config.",
         boost::join(not_in_config, ", ")
      ));
   }

   std::set<std::string> not_in_column_names;
   std::set_difference(
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      column_names_set.begin(),
      column_names_set.end(),
      std::inserter(not_in_column_names, not_in_column_names.begin())
   );
   if (not_in_column_names.size() > 0) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "The metadata fields '{}' which are contained in the database config are not contained in "
         "the input.",
         boost::join(not_in_column_names, ", ")
      ));
   }

   std::set<std::string> in_config_and_column_names;
   std::set_intersection(
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      column_names_set.begin(),
      column_names_set.end(),
      std::inserter(in_config_and_column_names, in_config_and_column_names.begin())
   );

   for (const auto& [field_name, access_path] : validated_metadata_fields) {
      const auto metadata = database_config.getMetadata(field_name);
      if (metadata->type == silo::config::ValueType::PANGOLINEAGE) {
         validated_metadata_fields.at(field_name) =
            preprocessing_database.unalias_pango_lineage_function->generateSqlStatement(access_path
            );
      }
   }

   logMetadataFields(validated_metadata_fields);
   return validated_metadata_fields;
}

void detectInsertionLists(
   const std::filesystem::path& ndjson_file,
   std::unordered_map<std::string, std::string>& metadata_fields_to_validate
) {
   duckdb::DuckDB duck_db(nullptr);
   duckdb::Connection connection(duck_db);
   auto top_level_entries =
      connection.Query(fmt::format("SELECT * FROM '{}' LIMIT 0", ndjson_file.string()));
   for (size_t idx = 0; idx < top_level_entries->ColumnCount(); idx++) {
      const std::string& top_level_entry = top_level_entries->ColumnName(idx);
      if (top_level_entry == "nucleotideInsertions" || top_level_entry == "aminoAcidInsertions") {
         auto contained_insertions = connection.Query(
            fmt::format("SELECT {}.* FROM '{}' LIMIT 0", top_level_entry, ndjson_file.string())
         );
         if (contained_insertions->ColumnCount() == 0) {
            metadata_fields_to_validate[top_level_entry] = "''";
         }
         if (contained_insertions->ColumnCount() == 1) {
            metadata_fields_to_validate[top_level_entry] = fmt::format(
               "list_string_agg({}.{})", top_level_entry, contained_insertions->ColumnName(0)
            );
         }

         std::vector<std::string> list_transforms;
         for (size_t idx2 = 0; idx2 < contained_insertions->ColumnCount(); idx2++) {
            const std::string& sequence_name = contained_insertions->ColumnName(idx2);
            list_transforms.push_back(fmt::format(
               "list_transform({0}.{1}, x ->'{1}:' || x)", top_level_entry, sequence_name
            ));
         }
         metadata_fields_to_validate[top_level_entry] =
            "list_string_agg(flatten([" + boost::join(list_transforms, ",") + "]))";
      }
   }
}

}  // namespace

namespace silo::preprocessing {

MetadataInfo::MetadataInfo(std::unordered_map<std::string, std::string> metadata_selects)
    : metadata_selects(std::move(metadata_selects)) {}

MetadataInfo MetadataInfo::validateFromMetadataFile(
   const std::filesystem::path& metadata_file,
   const silo::config::DatabaseConfig& database_config,
   const silo::preprocessing::PreprocessingDatabase& preprocessing_database
) {
   std::vector<MetadataName> metadata_names =
      getMetadataNamesFromFile(metadata_file, MetadataFileType::TSV);
   std::vector<MetadataName> valid_metadata_names =
      getValidMetadataNames(metadata_names, database_config);
   std::vector<MetadataWithType> metadata_with_type =
      getTypesForMetadata(valid_metadata_names, database_config);
   std::vector<MetadataWithPath> metadata_with_path =
      getMetadataWithPath(metadata_with_type, metadata_file);
   std::vector<MetadataForSelect> metadata_for_select =
      getMetadataForSelect(metadata_with_path, preprocessing_database);

   std::unordered_map<std::string, std::string> file_metadata_fields;
   for (size_t idx = 0; idx < result->ColumnCount(); idx++) {
      const auto column_name = result->ColumnName(idx);
      file_metadata_fields[column_name] = fmt::format(R"("{}")", column_name);
   }

   const std::unordered_map<std::string, std::string> validated_metadata_fields =
      validateFieldsAgainstConfig(file_metadata_fields, database_config, preprocessing_database);

   return {validated_metadata_fields};
}

MetadataInfo MetadataInfo::validateFromNdjsonFile(
   const std::filesystem::path& ndjson_file,
   const silo::config::DatabaseConfg& database_config,
   const silo::preprocessing::PreprocessingDatabase& preprocessing_database
) {
   duckdb::DuckDB duck_db(nullptr);
   duckdb::Connection connection(duck_db);

   auto result = connection.Query(fmt::format(
      "SELECT json_keys(metadata) "
      "FROM read_json_auto(\"{}\") LIMIT 1; ",
      ndjson_file.string()
   ));
   if (result->HasError()) {
      throw silo::preprocessing::PreprocessingException(
         "Preprocessing exception when retrieving the field 'metadata', "
         "duckdb threw with error: " +
         result->GetError()
      );
   }
   if (result->RowCount() == 0) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "File {} is empty, which must not be empty at this point", ndjson_file.string()
      ));
   }
   if (result->RowCount() > 1) {
      throw silo::preprocessing::PreprocessingException(
         "Internal exception, expected Row Count=1, actual " + std::to_string(result->RowCount())
      );
   }

   std::unordered_map<std::string, std::string> metadata_fields_to_validate;
   for (const std::string& metadata_field : preprocessing::extractStringListValue(*result, 0, 0)) {
      metadata_fields_to_validate[metadata_field] = "metadata.\"" + metadata_field + "\"";
   }
   detectInsertionLists(ndjson_file, metadata_fields_to_validate);

   const std::unordered_map<std::string, std::string> validated_metadata_fields =
      validateFieldsAgainstConfig(
         metadata_fields_to_validate, database_config, preprocessing_database
      );

   return {validated_metadata_fields};
}

std::vector<std::string> MetadataInfo::getMetadataFields() const {
   std::vector<std::string> ret;
   for (const auto& [field, _] : metadata_selects) {
      ret.emplace_back("\"" + field + "\"");
   }
   return ret;
}

std::vector<std::string> MetadataInfo::getMetadataSelects() const {
   std::vector<std::string> ret;
   for (const auto& [field, select] : metadata_selects) {
      ret.emplace_back(fmt::format(R"({} as "{}")", select, field));
   }
   return ret;
}

MetadataFileType getMetadataFileType(const std::filesystem::path& metadata_file) {
   if (metadata_file.extension() == ".tsv") {
      return MetadataFileType::TSV;
   }
   if (metadata_file.extension() == ".ndjson") {
      return MetadataFileType::NDJSON;
   }
   throw std::runtime_error("Unknown metadata file type: " + metadata_file.string());
};

std::vector<MetadataName> MetadataInfo::getMetadataNamesFromFile(
   const std::filesystem::path& metadata_file,
   MetadataFileType metadata_file_type
) {
   if (!std::filesystem::exists(metadata_file)) {
      throw silo::preprocessing::PreprocessingException(
         fmt::format("The specified input file {} does not exist.", metadata_file.string())
      );
   }
   if (std::filesystem::is_empty(metadata_file)) {
      throw silo::preprocessing::PreprocessingException(
         fmt::format("The specified input file {} is empty.", metadata_file.string())
      );
   }

   switch (metadata_file_type) {
      case MetadataFileType::TSV: {
         return getMetadataNamesFromTsvFile(metadata_file);
      }
      case MetadataFileType::NDJSON: {
         return getMetadataNamesFromNdjson(metadata_file);
      }
   }
}

std::vector<MetadataName> MetadataInfo::getMetadataNamesFromTsvFile(
   const std::filesystem::path& tsv_file
) {
   duckdb::DuckDB duck_db(nullptr);
   duckdb::Connection connection(duck_db);

   auto result = connection.Query(fmt::format("SELECT * FROM '{}' LIMIT 0", tsv_file.string()));

   std::vector<MetadataName> column_names;
   for (size_t idx = 0; idx < result->ColumnCount(); idx++) {
      column_names.emplace_back(result->ColumnName(idx));
   }
   return column_names;
}

std::vector<MetadataName> MetadataInfo::getMetadataNamesFromNdjson(
   const std::filesystem::path& ndjson_file
) {
   duckdb::DuckDB duck_db(nullptr);
   duckdb::Connection connection(duck_db);

   auto result = connection.Query(fmt::format(
      "SELECT json_keys(metadata) "
      "FROM read_json_auto(\"{}\") LIMIT 1; ",
      ndjson_file.string()
   ));
   if (result->HasError()) {
      throw silo::preprocessing::PreprocessingException(
         "Preprocessing exception when retrieving the field 'metadata', "
         "duckdb threw with error: " +
         result->GetError()
      );
   }
   if (result->RowCount() == 0) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "File {} is empty, which must not be empty at this point", ndjson_file.string()
      ));
   }
   if (result->RowCount() > 1) {
      throw silo::preprocessing::PreprocessingException(
         "Internal exception, expected Row Count=1, actual " + std::to_string(result->RowCount())
      );
   }

   std::vector<MetadataName> metadata_names;
   for (const std::string& metadata_field : preprocessing::extractStringListValue(*result, 0, 0)) {
      metadata_names.emplace_back(metadata_field);
   }

   const auto metadata_names_with_insertions =
      addInsertionsFromTopLevel(metadata_names, ndjson_file);

   return metadata_names_with_insertions;
}
std::vector<MetadataName> MetadataInfo::addInsertionsFromTopLevel(
   const std::vector<MetadataName>& metadata_names,
   const std::filesystem::path& ndjson_file
) {
   duckdb::DuckDB duck_db(nullptr);
   duckdb::Connection connection(duck_db);

   auto top_level_entries =
      connection.Query(fmt::format("SELECT * FROM '{}' LIMIT 0", ndjson_file.string()));

   std::vector<MetadataName> metadata_names_with_insertions = metadata_names;
   for (size_t idx = 0; idx < top_level_entries->ColumnCount(); idx++) {
      const std::string& top_level_entry = top_level_entries->ColumnName(idx);
      if (top_level_entry == POSSIBLE_NDJSON_TOPLEVEL_AA_INSERTION_NAME || top_level_entry == POSSIBLE_NDJSON_TOPLEVEL_INSERTION_NAME) {
         metadata_names_with_insertions.emplace_back(top_level_entry);
      }
   }

   return metadata_names_with_insertions;
}
std::vector<MetadataName> MetadataInfo::getValidMetadataNames(
   const std::vector<MetadataName>& metadata_names,
   const config::DatabaseConfig& database_config
) {
   std::vector<std::string> config_metadata_names = database_config.getMetadataNames();
   const std::set<std::string> config_metadata_names_set(
      config_metadata_names.begin(), config_metadata_names.end()
   );
   std::set<std::string> column_names_set;
   for (const auto& metadata_name : metadata_names) {
      column_names_set.insert(metadata_name.name);
   }

   std::set<std::string> not_in_config;
   std::set_difference(
      column_names_set.begin(),
      column_names_set.end(),
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      std::inserter(not_in_config, not_in_config.begin())
   );
   if (!not_in_config.empty()) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "The metadata fields '{}' which are contained in the input are not contained in the "
         "database config.",
         boost::join(not_in_config, ", ")
      ));
   }

   std::set<std::string> not_in_column_names;
   std::set_difference(
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      column_names_set.begin(),
      column_names_set.end(),
      std::inserter(not_in_column_names, not_in_column_names.begin())
   );
   if (!not_in_column_names.empty()) {
      throw silo::preprocessing::PreprocessingException(fmt::format(
         "The metadata fields '{}' which are contained in the database config are not contained in "
         "the input.",
         boost::join(not_in_column_names, ", ")
      ));
   }

   std::set<std::string> in_config_and_column_names;
   std::set_intersection(
      config_metadata_names_set.begin(),
      config_metadata_names_set.end(),
      column_names_set.begin(),
      column_names_set.end(),
      std::inserter(in_config_and_column_names, in_config_and_column_names.begin())
   );

   std::vector<MetadataName> valid_metadata_names;
   for (const auto& metadata_name : in_config_and_column_names) {
      valid_metadata_names.emplace_back(metadata_name);
   }

   return valid_metadata_names;
}

}  // namespace silo::preprocessing
