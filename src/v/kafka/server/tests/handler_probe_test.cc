/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/protocol/types.h"
#include "kafka/server/handlers/handler_probe.h"
#include "test_utils/test.h"

// Reserved Redpanda-range keys (e.g. DescribeRedpandaRoles at 15000) resolve
// via handler_for_key but must not index the dense, max-key-sized _probes
// vector. get_probe routes them to the small rebased _custom_probes vector,
// indexed by key - redpanda_api_key_base. This guards against the
// out-of-bounds access that would otherwise occur.
//
// Runs as a coroutine so handler_probe_manager has a seastar reactor for its
// metric registration.
TEST_CORO(HandlerProbe, get_probe_reserved_range_key) {
    kafka::handler_probe_manager mgr;

    const auto roles_key = kafka::api_key(
      kafka::redpanda_api_key_base() /* DescribeRedpandaRoles */);

    // Stable identity: the same reserved-range key always maps to the same
    // probe instance (and the lookup does not go out of bounds / crash).
    EXPECT_EQ(&mgr.get_probe(roles_key), &mgr.get_probe(roles_key));

    // A reserved-range probe is distinct from a standard-range probe.
    EXPECT_NE(&mgr.get_probe(roles_key), &mgr.get_probe(kafka::api_key(0)));

    co_return;
}
