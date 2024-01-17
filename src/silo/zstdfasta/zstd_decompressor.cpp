#include "silo/zstdfasta/zstd_decompressor.h"

#include <memory>
#include <string>
#include <utility>

namespace silo {

ZstdDecompressor::~ZstdDecompressor() {
   ZSTD_freeDDict(zstd_dictionary);
   ZSTD_freeDCtx(zstd_context);
}

ZstdDecompressor::ZstdDecompressor(std::string_view dictionary_string) {
   zstd_dictionary = ZSTD_createDDict(dictionary_string.data(), dictionary_string.length());
   zstd_context = ZSTD_createDCtx();
}

ZstdDecompressor::ZstdDecompressor(ZstdDecompressor&& other) noexcept {
   this->zstd_context = std::exchange(other.zstd_context, nullptr);
   this->zstd_dictionary = std::exchange(other.zstd_dictionary, nullptr);
}

ZstdDecompressor& ZstdDecompressor::operator=(ZstdDecompressor&& other) noexcept {
   std::swap(this->zstd_context, other.zstd_context);
   std::swap(this->zstd_dictionary, other.zstd_dictionary);
   return *this;
}

void ZstdDecompressor::decompress(const std::string& input, std::string& output) {
   auto size_or_error_code = ZSTD_decompress_usingDDict(
      zstd_context, output.data(), output.length(), input.data(), input.size(), zstd_dictionary
   );
   if (ZSTD_isError(size_or_error_code)) {
      const std::string error_name = ZSTD_getErrorName(size_or_error_code);
      throw std::runtime_error(
         "Error '" + error_name + "' in dependency when decompressing using zstd."
      );
   }
}

void ZstdDecompressor::decompress(
   const char* input_data,
   size_t input_length,
   char* output_data,
   size_t output_length
) {
   ZSTD_decompress_usingDDict(
      zstd_context, output_data, output_length, input_data, input_length, zstd_dictionary
   );
}

}  // namespace silo