#include "silo/query_engine/operators/index_scan.h"

#include <gtest/gtest.h>
#include <roaring/roaring.hh>

using silo::query_engine::operators::IndexScan;

TEST(OperatorIndexScan, evaluateShouldReturnCorrectValues) {
   const roaring::Roaring test_bitmap(roaring::Roaring({1, 3}));
   const IndexScan under_test(&test_bitmap, 5);
   ASSERT_EQ(*under_test.evaluate(), roaring::Roaring({1, 3}));
}

TEST(OperatorIndexScan, correctTypeInfo) {
   const roaring::Roaring test_bitmap({1, 2, 3});

   const IndexScan under_test(&test_bitmap, 5);

   ASSERT_EQ(under_test.type(), silo::query_engine::operators::INDEX_SCAN);
}
