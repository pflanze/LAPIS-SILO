#pragma once

#include <filesystem>

namespace silo::config {
struct DatabaseConfig;
}  // namespace silo::config

namespace silo::preprocessing {

class MetadataValidator {
  public:
   virtual void validateMedataFile(
      const std::filesystem::path& metadata_file,
      const silo::config::DatabaseConfig& database_config
   ) const;
};

}  // namespace silo::preprocessing
