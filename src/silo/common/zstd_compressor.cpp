#include "silo/common/zstd_compressor.h"

#include <string_view>

namespace silo {

ZstdCompressor::~ZstdCompressor() {
   ZSTD_freeCDict(zstd_dictionary);
   ZSTD_freeCCtx(zstd_context);
}

ZstdCompressor::ZstdCompressor(std::string_view dictionary_string) {
   size_bound = ZSTD_compressBound(dictionary_string.size());
   zstd_dictionary = ZSTD_createCDict(dictionary_string.data(), dictionary_string.length(), 2);
   zstd_context = ZSTD_createCCtx();
}

ZstdCompressor::ZstdCompressor(ZstdCompressor&& other) {
   this->zstd_context = std::exchange(other.zstd_context, nullptr);
   this->zstd_dictionary = std::exchange(other.zstd_dictionary, nullptr);
   this->size_bound = other.size_bound;
}

ZstdCompressor& ZstdCompressor::operator=(ZstdCompressor&& other) {
   std::swap(this->zstd_context, other.zstd_context);
   std::swap(this->zstd_dictionary, other.zstd_dictionary);
   std::swap(this->size_bound, other.size_bound);
   return *this;
}

size_t ZstdCompressor::compress(const std::string& input, std::string& output) {
   return ZSTD_compress_usingCDict(
      zstd_context, output.data(), output.size(), input.data(), input.size(), zstd_dictionary
   );
}

size_t ZstdCompressor::compress(
   const char* input_data,
   size_t input_size,
   char* output_data,
   size_t output_size
) {
   return ZSTD_compress_usingCDict(
      zstd_context, output_data, output_size, input_data, input_size, zstd_dictionary
   );
}

size_t ZstdCompressor::getSizeBound() const {
   return size_bound;
}

}  // namespace silo