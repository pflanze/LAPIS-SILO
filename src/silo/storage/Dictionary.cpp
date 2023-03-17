#include <silo/common/silo_symbols.h>
#include <silo/database.h>
#include <silo/storage/Dictionary.h>
#include <cassert>
#include <iostream>

// TODO(someone): reduce cognitive complexity
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void Dictionary::updateDictionary(
   std::istream& metadata_file,
   const std::unordered_map<std::string, std::string>& alias_key
) {
   // Parse header. Assert order EPI, PANGO, DATE, REGION, COUNTRY, then fill additional columns
   {
      std::string header;
      if (!getline(metadata_file, header, '\n')) {
         std::cerr << "Failed to read header line. Abort." << std::endl;
         return;
      }
      std::stringstream header_in(header);
      std::string col_name;
      if (!getline(header_in, col_name, '\t') || col_name != "gisaid_epi_isl") {
         std::cerr << "Expected 'gisaid_epi_isl' as first column in metadata." << std::endl;
         return;
      }
      if (!getline(header_in, col_name, '\t') || col_name != "pango_lineage") {
         std::cerr << "Expected 'pango_lineage' as first column in metadata." << std::endl;
         return;
      }
      if (!getline(header_in, col_name, '\t') || col_name != "date") {
         std::cerr << "Expected 'date' as first column in metadata." << std::endl;
         return;
      }
      if (!getline(header_in, col_name, '\t') || col_name != "region") {
         std::cerr << "Expected 'region' as first column in metadata." << std::endl;
         return;
      }
      if (!getline(header_in, col_name, '\t') || col_name != "country") {
         std::cerr << "Expected 'country' as first column in metadata." << std::endl;
         return;
      }
      while (!header_in.eof()) {
         getline(header_in, col_name, '\t');
         additional_columns_lookup.push_back(col_name);
         additional_columns_dictionary[col_name] = additional_columns_count++;
      }
   }

   while (true) {
      std::string pango_lineage_raw;
      std::string epi_isl;
      std::string region;
      std::string country;
      std::string division;
      if (!getline(metadata_file, epi_isl, '\t')) {
         break;
      }
      if (!getline(metadata_file, pango_lineage_raw, '\t')) {
         break;
      }
      metadata_file.ignore(LONG_MAX, '\t');
      if (!getline(metadata_file, region, '\t')) {
         break;
      }
      if (!getline(metadata_file, country, '\t')) {
         break;
      }
      if (!getline(metadata_file, division, '\n')) {
         break;
      }

      /// Deal with pango_lineage alias:
      std::string const pango_lineage =
         silo::resolvePangoLineageAlias(alias_key, pango_lineage_raw);

      if (!pango_lineage_dictionary.contains(pango_lineage)) {
         pango_lineage_lookup.push_back(pango_lineage);
         pango_lineage_dictionary[pango_lineage] = pango_lineage_count++;
      }
      if (!region_dictionary.contains(region)) {
         region_lookup.push_back(region);
         region_dictionary[region] = region_count++;
      }
      if (!country_dictionary.contains(country)) {
         country_lookup.push_back(country);
         country_dictionary[country] = country_count++;
      }
      if (!general_dictionary.contains(division)) {
         general_lookup.push_back(division);
         general_dictionary[division] = general_count++;
      }
   }
}

void Dictionary::saveDictionary(std::ostream& dictionary_file) const {
   dictionary_file << "pango_lineage_count\t" << pango_lineage_count << '\n';
   dictionary_file << "region_count\t" << region_count << '\n';
   dictionary_file << "country_count\t" << country_count << '\n';
   dictionary_file << "additional_columns_count\t" << additional_columns_count << '\n';
   dictionary_file << "dict_count\t" << general_count << '\n';
   for (uint32_t pango_lineage_id = 0; pango_lineage_id < pango_lineage_count; ++pango_lineage_id) {
      dictionary_file << pango_lineage_lookup[pango_lineage_id] << '\t' << pango_lineage_id << '\n';
   }
   for (uint32_t region_lookup_id = 0; region_lookup_id < region_count; ++region_lookup_id) {
      dictionary_file << region_lookup[region_lookup_id] << '\t' << region_lookup_id << '\n';
   }
   for (uint32_t country_lookup_id = 0; country_lookup_id < country_count; ++country_lookup_id) {
      dictionary_file << country_lookup[country_lookup_id] << '\t' << country_lookup_id << '\n';
   }
   for (uint32_t columns_id = 0; columns_id < additional_columns_count; ++columns_id) {
      dictionary_file << additional_columns_lookup[columns_id] << '\t' << columns_id << '\n';
   }
   for (uint64_t general_id = 0; general_id < general_count; ++general_id) {
      dictionary_file << general_lookup[general_id] << '\t' << general_id << '\n';
   }
}

// TODO(someone): reduce cognitive complexity
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Dictionary Dictionary::loadDictionary(std::istream& dictionary_file) {
   Dictionary dictionary;

   std::string read_string;
   uint32_t pango_count;
   uint32_t region_count;
   uint32_t country_count;
   uint32_t col_count;
   uint32_t dict_count;

   if (!getline(dictionary_file, read_string, '\t')) {
      std::cerr << "Invalid dict-header1a." << std::endl;
      return dictionary;
   }
   if (!getline(dictionary_file, read_string, '\n')) {
      std::cerr << "Invalid dict-header1b." << std::endl;
      return dictionary;
   }
   pango_count = atoi(read_string.c_str());

   if (!getline(dictionary_file, read_string, '\t')) {
      std::cerr << "Invalid dict-header2a." << std::endl;
      return dictionary;
   }
   if (!getline(dictionary_file, read_string, '\n')) {
      std::cerr << "Invalid dict-header2b." << std::endl;
      return dictionary;
   }
   region_count = atoi(read_string.c_str());

   if (!getline(dictionary_file, read_string, '\t')) {
      std::cerr << "Invalid dict-header3a." << std::endl;
      return dictionary;
   }
   if (!getline(dictionary_file, read_string, '\n')) {
      std::cerr << "Invalid dict-header3b." << std::endl;
      return dictionary;
   }
   country_count = atoi(read_string.c_str());

   if (!getline(dictionary_file, read_string, '\t')) {
      std::cerr << "Invalid dict-header4a." << std::endl;
      return dictionary;
   }
   if (!getline(dictionary_file, read_string, '\n')) {
      std::cerr << "Invalid dict-header4b." << std::endl;
      return dictionary;
   }
   col_count = atoi(read_string.c_str());

   if (!getline(dictionary_file, read_string, '\t')) {
      std::cerr << "Invalid dict-header5a." << std::endl;
      return dictionary;
   }
   if (!getline(dictionary_file, read_string, '\n')) {
      std::cerr << "Invalid dict-header5b." << std::endl;
      return dictionary;
   }
   dict_count = atoi(read_string.c_str());

   dictionary.pango_lineage_count = pango_count;
   dictionary.pango_lineage_lookup.resize(pango_count);
   dictionary.region_count = region_count;
   dictionary.region_lookup.resize(region_count);
   dictionary.country_count = country_count;
   dictionary.country_lookup.resize(country_count);
   dictionary.additional_columns_count = col_count;
   dictionary.additional_columns_lookup.resize(col_count);
   dictionary.general_count = dict_count;
   dictionary.general_lookup.resize(dict_count);
   std::string id_str;
   for (uint32_t i = 0; i < pango_count; ++i) {
      if (!getline(dictionary_file, read_string, '\t')) {
         std::cerr << "Unexpected end of file. Expected pango_lineage_count:" << pango_count
                   << " many lineages in the dict file. No read_string" << std::endl;
         return dictionary;
      }
      if (!getline(dictionary_file, id_str, '\n')) {
         std::cerr << "Unexpected end of file. Expected pango_lineage_count:" << pango_count
                   << " many lineages in the dict file. No lookup_id" << std::endl;
         return dictionary;
      }
      uint32_t const lookup_id = atoi(id_str.c_str());
      dictionary.pango_lineage_lookup[lookup_id] = read_string;
      dictionary.pango_lineage_dictionary[read_string] = lookup_id;
   }
   for (uint32_t i = 0; i < region_count; ++i) {
      if (!getline(dictionary_file, read_string, '\t')) {
         std::cerr << "Unexpected end of file. Expected region_count:" << region_count
                   << " many regions in the dict file. No read_string" << std::endl;
         return dictionary;
      }
      if (!getline(dictionary_file, id_str, '\n')) {
         std::cerr << "Unexpected end of file. Expected region_count:" << region_count
                   << " many regions in the dict file. No lookup_id" << std::endl;
         return dictionary;
      }
      uint32_t const lookup_id = atoi(id_str.c_str());
      dictionary.region_lookup[lookup_id] = read_string;
      dictionary.region_dictionary[read_string] = lookup_id;
   }
   for (uint32_t i = 0; i < country_count; ++i) {
      if (!getline(dictionary_file, read_string, '\t')) {
         std::cerr << "Unexpected end of file. Expected country_count:" << country_count
                   << " many countries in the dict file. No read_string" << std::endl;
         return dictionary;
      }
      if (!getline(dictionary_file, id_str, '\n')) {
         std::cerr << "Unexpected end of file. Expected country_count:" << country_count
                   << " many countries in the dict file. No lookup_id" << std::endl;
         return dictionary;
      }
      uint32_t const lookup_id = atoi(id_str.c_str());
      dictionary.country_lookup[lookup_id] = read_string;
      dictionary.country_dictionary[read_string] = lookup_id;
   }
   for (uint32_t i = 0; i < col_count; ++i) {
      if (!getline(dictionary_file, read_string, '\t')) {
         std::cerr << "Unexpected end of file. Expected additional_columns_count:" << col_count
                   << " many columns in the dict file. No read_string" << std::endl;
         return dictionary;
      }
      if (!getline(dictionary_file, id_str, '\n')) {
         std::cerr << "Unexpected end of file. Expected additional_columns_count:" << col_count
                   << " many columns in the dict file. No lookup_id" << std::endl;
         return dictionary;
      }
      uint32_t const lookup_id = atoi(id_str.c_str());
      dictionary.additional_columns_lookup[lookup_id] = read_string;
      dictionary.additional_columns_dictionary[read_string] = lookup_id;
   }
   for (uint64_t i = 0; i < dict_count; ++i) {
      if (!getline(dictionary_file, read_string, '\t')) {
         std::cerr << "Unexpected end of file. Expected dict_count:" << dict_count
                   << " many lookups in the dict file. No read_string" << std::endl;
         return dictionary;
      }
      if (!getline(dictionary_file, id_str, '\n')) {
         std::cerr << "Unexpected end of file. Expected dict_count:" << dict_count
                   << " many lookups in the dict file. No id" << std::endl;
         return dictionary;
      }
      uint64_t const id64 = atoi(id_str.c_str());
      dictionary.general_lookup[id64] = read_string;
      dictionary.general_dictionary[read_string] = id64;
   }
   return dictionary;
}

uint32_t Dictionary::getPangoLineageIdInLookup(const std::string& pango_lineage) const {
   if (pango_lineage_dictionary.contains(pango_lineage)) {
      return pango_lineage_dictionary.at(pango_lineage);
   }
   return UINT32_MAX;
}

std::string error_string = "NOID";

const std::string& Dictionary::getPangoLineage(uint32_t pango_lineage_id_in_lookup) const {
   if (pango_lineage_id_in_lookup == UINT32_MAX) {
      return error_string;
   }
   assert(pango_lineage_id_in_lookup < pango_lineage_count);
   return pango_lineage_lookup[pango_lineage_id_in_lookup];
}

uint32_t Dictionary::getCountryIdInLookup(const std::string& country) const {
   if (country_dictionary.contains(country)) {
      return country_dictionary.at(country);
   }
   return UINT32_MAX;
}

const std::string& Dictionary::getCountry(uint32_t country_id_in_lookup) const {
   if (country_id_in_lookup == UINT32_MAX) {
      return error_string;
   }
   assert(country_id_in_lookup < country_count);
   return country_lookup[country_id_in_lookup];
}

uint32_t Dictionary::getRegionIdInLookup(const std::string& region) const {
   if (region_dictionary.contains(region)) {
      return region_dictionary.at(region);
   }
   return UINT32_MAX;
}

const std::string& Dictionary::getRegion(uint32_t region_lookup_id) const {
   if (region_lookup_id == UINT32_MAX) {
      return error_string;
   }
   assert(region_lookup_id < region_count);
   return region_lookup[region_lookup_id];
}

uint64_t Dictionary::getIdInGeneralLookup(const std::string& region_id_in_lookup) const {
   if (general_dictionary.contains(region_id_in_lookup)) {
      return general_dictionary.at(region_id_in_lookup);
   }
   return UINT64_MAX;
}

[[maybe_unused]] const std::string& Dictionary::getGeneralLookup(uint64_t general_id_in_lookup
) const {
   if (general_id_in_lookup == UINT64_MAX) {
      return error_string;
   }
   assert(general_id_in_lookup < general_count);
   return general_lookup[general_id_in_lookup];
}

uint32_t Dictionary::getColumnIdInLookup(const std::string& column_name) const {
   if (additional_columns_dictionary.contains(column_name)) {
      return additional_columns_dictionary.at(column_name);
   }
   return UINT32_MAX;
}

[[maybe_unused]] const std::string& Dictionary::getColumn(uint32_t column_id_in_lookup) const {
   if (column_id_in_lookup == UINT32_MAX) {
      return error_string;
   }
   assert(column_id_in_lookup < additional_columns_count);
   return additional_columns_lookup[column_id_in_lookup];
}
uint32_t Dictionary::getPangoLineageCount() const {
   return pango_lineage_count;
}
uint32_t Dictionary::getCountryCount() const {
   return country_count;
}
uint32_t Dictionary::getRegionCount() const {
   return region_count;
}