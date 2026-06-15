// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/types.h"

#include <gtest/gtest.h>

namespace kafka {
namespace {

// Base of the reserved range, taken from the source of truth rather than
// hard-coded, so these stay correct if the base ever moves.
constexpr int16_t base = static_cast<int16_t>(redpanda_api_key_base());

// Standard tables pass Base=0 and are indexed directly by key: the span must
// cover slot 0 through the max key.
static_assert(api_table_span<0, 0>() == 1);
static_assert(api_table_span<0, 5>() == 6);
static_assert(
  api_table_span<0, 2, 0, 5>() == 6); // order-independent (max wins)

// Reserved-range tables pass redpanda_api_key_base and are rebased: the table
// is sized to the offset span (key - base), not the five-digit key value, and
// tolerates gaps in the offset range.
static_assert(api_table_span<base, base>() == 1);
static_assert(api_table_span<base, static_cast<int16_t>(base + 2)>() == 3);
static_assert(
  api_table_span<base, base, static_cast<int16_t>(base + 2)>() == 3);

// Mirror a few cases at runtime so the contract surfaces as a test case; the
// consteval calls fold to constants.
TEST(ApiTableSpan, SizesTableToRebasedOffsetSpan) {
    EXPECT_EQ((api_table_span<0, 5>()), 6U);
    EXPECT_EQ((api_table_span<base, base>()), 1U);
    EXPECT_EQ(
      (api_table_span<base, base, static_cast<int16_t>(base + 2)>()), 3U);
}

} // namespace
} // namespace kafka
