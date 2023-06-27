#ifndef SILO_SRC_SILO_STORAGE_CSV_READER_H_
#define SILO_SRC_SILO_STORAGE_CSV_READER_H_

#include <filesystem>
#include <string>
#include <vector>

#include <csv.hpp>

namespace silo::preprocessing {

class MetadataReader {
  public:
   static std::vector<std::string> getColumn(
      const std::filesystem::path& metadata_path,
      const std::string& column_name
   );

   static csv::CSVReader getReader(const std::filesystem::path& metadata_path);
};

class MetadataWriter {
  private:
   std::unique_ptr<std::ostream> out_stream;

  public:
   MetadataWriter(std::unique_ptr<std::ostream> out_stream);

   void writeHeader(const csv::CSVReader& csv_reader);

   void writeRow(const csv::CSVRow& row);
};

}  // namespace silo::preprocessing

#endif  // SILO_SRC_SILO_STORAGE_CSV_READER_H_