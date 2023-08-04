#ifndef SILO_PREPROCESSING_CONFIG_H
#define SILO_PREPROCESSING_CONFIG_H

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>

#include <fmt/core.h>

namespace silo::preprocessing {
struct PartitionChunk;
struct Partitions;

struct InputDirectory {
   std::string directory;
};
const InputDirectory DEFAULT_INPUT_DIRECTORY = {"./"};

struct OutputDirectory {
   std::string directory;
};
const OutputDirectory DEFAULT_OUTPUT_DIRECTORY = {"./output/"};

struct MetadataFilename {
   std::string filename;
};
const MetadataFilename DEFAULT_METADATA_FILENAME = {"metadata.tsv"};

struct PangoLineageDefinitionFilename {
   std::optional<std::string> filename;
};

struct NucleotideSequencePrefix {
   std::string prefix;
};
const NucleotideSequencePrefix DEFAULT_NUCLEOTIDE_SEQUENCE_PREFIX = {"nuc_"};

struct GenePrefix {
   std::string prefix;
};
const GenePrefix DEFAULT_GENE_PREFIX = {"gene_"};

struct PartitionsFolder {
   std::string folder;
};
const PartitionsFolder DEFAULT_PARTITIONS_FOLDER = {"partitions/"};

struct SortedPartitionsFolder {
   std::string folder;
};
const SortedPartitionsFolder DEFAULT_SORTED_PARTITIONS_FOLDER = {"partitions_sorted/"};

struct SerializedStateFolder {
   std::string folder;
};
const SerializedStateFolder DEFAULT_SERIALIZED_STATE_FOLDER = {"serialized_state/"};

struct ReferenceGenomeFilename {
   std::string filename;
};
const ReferenceGenomeFilename DEFAULT_REFERENCE_GENOME_FILENAME = {"reference-genomes.json"};

class PreprocessingConfig {
   friend class fmt::formatter<silo::preprocessing::PreprocessingConfig>;

   std::filesystem::path input_directory;
   std::optional<std::filesystem::path> pango_lineage_definition_file;
   std::filesystem::path metadata_file;
   std::filesystem::path partition_folder;
   std::filesystem::path sorted_partition_folder;
   std::filesystem::path serialization_folder;
   std::filesystem::path reference_genome_file;
   std::string nucleotide_sequence_prefix;
   std::string gene_prefix;

  public:
   explicit PreprocessingConfig();

   explicit PreprocessingConfig(
      const InputDirectory& input_directory_,
      const OutputDirectory& output_directory_,
      const MetadataFilename& metadata_filename_,
      const PangoLineageDefinitionFilename& pango_lineage_definition_filename_,
      const PartitionsFolder& partition_folder_,
      const SortedPartitionsFolder& sorted_partition_folder_,
      const SerializedStateFolder& serialization_folder_,
      const ReferenceGenomeFilename& reference_genome_filename_,
      const NucleotideSequencePrefix& nucleotide_sequence_prefix_,
      const GenePrefix& gene_prefix_
   );

   [[nodiscard]] std::optional<std::filesystem::path> getPangoLineageDefinitionFilename() const;

   [[nodiscard]] std::filesystem::path getReferenceGenomeFilename() const;

   [[nodiscard]] std::filesystem::path getMetadataInputFilename() const;

   [[nodiscard]] std::unordered_map<silo::preprocessing::PartitionChunk, std::filesystem::path>
   getMetadataPartitionFilenames(const silo::preprocessing::Partitions& partitions) const;

   [[nodiscard]] std::filesystem::path getMetadataPartitionFilename(
      uint32_t partition,
      uint32_t chunk
   ) const;

   [[nodiscard]] std::filesystem::path getMetadataSortedPartitionFilename(
      uint32_t partition,
      uint32_t chunk
   ) const;

   [[nodiscard]] std::filesystem::path getNucFilename(std::string_view nuc_name) const;

   [[nodiscard]] std::unordered_map<silo::preprocessing::PartitionChunk, std::filesystem::path>
   getNucPartitionFilenames(
      std::string_view nuc_name,
      const silo::preprocessing::Partitions& partitions
   ) const;

   [[nodiscard]] std::filesystem::path getNucPartitionFilename(
      std::string_view nuc_name,
      uint32_t partition,
      uint32_t chunk
   ) const;

   [[nodiscard]] std::filesystem::path getNucSortedPartitionFilename(
      std::string_view nuc_name,
      uint32_t partition,
      uint32_t chunk
   ) const;

   [[nodiscard]] std::filesystem::path getGeneFilename(std::string_view gene_name) const;

   [[nodiscard]] std::unordered_map<silo::preprocessing::PartitionChunk, std::filesystem::path>
   getGenePartitionFilenames(
      std::string_view gene_name,
      const silo::preprocessing::Partitions& partitions
   ) const;

   [[nodiscard]] std::filesystem::path getGenePartitionFilename(
      std::string_view gene_name,
      uint32_t partition,
      uint32_t chunk
   ) const;

   [[nodiscard]] std::filesystem::path getGeneSortedPartitionFilename(
      std::string_view gene_name,
      uint32_t partition,
      uint32_t chunk
   ) const;
};

std::filesystem::path createPath(
   const std::filesystem::path& directory,
   const std::string& filename
);

}  // namespace silo::preprocessing

template <>
struct [[maybe_unused]] fmt::formatter<silo::preprocessing::PreprocessingConfig>
    : fmt::formatter<std::string> {
   [[maybe_unused]] static auto format(
      const silo::preprocessing::PreprocessingConfig& preprocessing_config,
      format_context& ctx
   ) -> decltype(ctx.out());
};

#endif  // SILO_PREPROCESSING_CONFIG_H
