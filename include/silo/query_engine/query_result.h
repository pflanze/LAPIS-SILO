#ifndef SILO_QUERY_ENGINE_RESULT_H
#define SILO_QUERY_ENGINE_RESULT_H

#include <string>

namespace silo::response {
struct AggregationResult {
   int64_t count;
};

struct MutationProportion {
   std::string mutation;
   double proportion;
   int64_t count;
};

struct ErrorResult {
   std::string error;
   std::string message;
};
}  // namespace silo::response

#endif  // SILO_QUERY_ENGINE_RESULT_H