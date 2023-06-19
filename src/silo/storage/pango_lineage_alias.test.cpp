#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

#include "silo/storage/pango_lineage_alias.h"

namespace {

struct TestParameter {
   std::string input;
   std::string expected_result;
};

class ResolveAliasTestFixture : public ::testing::TestWithParam<TestParameter> {
  protected:
   const silo::PangoLineageAliasLookup alias_map = silo::PangoLineageAliasLookup(
      std::unordered_map<std::string, std::string>{{"X", "A"}, {"XY", "A.1"}}
   );
};

TEST_P(ResolveAliasTestFixture, shouldReturnExpectedResolvedAlias) {
   const auto test_parameter = GetParam();

   const auto result = alias_map.resolvePangoLineageAlias(test_parameter.input);

   ASSERT_EQ(result, test_parameter.expected_result);
}

// NOLINTNEXTLINE(readability-identifier-length)
INSTANTIATE_TEST_SUITE_P(
   ResolveAliasTest,
   ResolveAliasTestFixture,
   ::testing::Values(
      TestParameter{"", ""},
      TestParameter{"SomeNotListedAlias", "SomeNotListedAlias"},
      TestParameter{"X", "A"},
      TestParameter{"XY", "A.1"},
      TestParameter{"X.1.1", "A.1.1"},
      TestParameter{"XYX.1.1", "XYX.1.1"},
      TestParameter{".X", ".X"}
   )
);

TEST(PangoLineageAliasLookup, readFromFile) {
   auto under_test = silo::PangoLineageAliasLookup::readFromFile("testBaseData/pango_alias.txt");

   ASSERT_EQ(under_test.resolvePangoLineageAlias("C"), "B.1.1.1");
   ASSERT_EQ(under_test.resolvePangoLineageAlias("EP"), "B.1.1.529.2.75.3.1.1.4");
}

}  // namespace
