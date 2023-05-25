#include "silo/query_engine/operators/union.h"

#include <roaring/roaring.hh>
#include <vector>

#include "silo/query_engine/operators/operator.h"

namespace silo::query_engine::operators {

Union::Union(std::vector<std::unique_ptr<Operator>>&& children)
    : children(std::move(children)) {}

Union::~Union() noexcept = default;

std::string Union::toString() const {
   std::string res = "(" + children[0]->toString();
   for (unsigned i = 1; i < children.size(); ++i) {
      const auto& child = children[i];
      res += " | " + child->toString();
   }
   res += ")";
   return res;
}

Type Union::type() const {
   return UNION;
}

OperatorResult Union::evaluate() const {
   const unsigned size_of_children = children.size();
   std::vector<const roaring::Roaring*> union_tmp(size_of_children);
   std::vector<OperatorResult> child_res(size_of_children);
   for (unsigned i = 0; i < size_of_children; i++) {
      child_res[i] = children[i]->evaluate();
      const roaring::Roaring& const_bitmap = *child_res[i];
      union_tmp[i] = &const_bitmap;
   }
   auto* result =
      new roaring::Roaring(roaring::Roaring::fastunion(union_tmp.size(), union_tmp.data()));
   return OperatorResult(result);
}

}  // namespace silo::query_engine::operators
