/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/flex_versions.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/handlers.h"

#include <boost/test/unit_test.hpp>

template<kafka::KafkaApiHandlerAny H>
void check_any_vs_static() {
    BOOST_TEST_INFO("Testing " << H::api::name);
    auto hopt = kafka::handler_for_key(H::api::key);
    BOOST_REQUIRE(hopt.has_value());
    auto h = *hopt;
    BOOST_CHECK_EQUAL(h->min_supported(), H::min_supported);
    BOOST_CHECK_EQUAL(h->max_supported(), H::max_supported);
    BOOST_CHECK_EQUAL(h->key(), H::api::key);
    BOOST_CHECK_EQUAL(h->name(), H::api::name);
}

template<typename... Ts>
void check_all_types(kafka::type_list<Ts...>) {
    (check_any_vs_static<Ts>(), ...);
}

BOOST_AUTO_TEST_CASE(handler_all_types) {
    check_all_types(kafka::request_types{});
}

BOOST_AUTO_TEST_CASE(handler_handler_for_key) {
    // key too low
    BOOST_CHECK(!kafka::handler_for_key(kafka::api_key(-1)).has_value());
    // key too high
    const auto max_key = kafka::max_api_key(kafka::request_types{});
    BOOST_CHECK(
      !kafka::handler_for_key(kafka::api_key(max_key + 1)).has_value());
    // last key should be present
    BOOST_CHECK(kafka::handler_for_key(kafka::api_key(max_key)).has_value());
    // 34 is AlterReplicaLogDirs which we don't currently support, use it as a
    // test case for handlers which fall in the valid range but we don't support
    BOOST_CHECK(!kafka::handler_for_key(kafka::api_key(34)).has_value());
}

BOOST_AUTO_TEST_CASE(handler_custom_redpanda_types) {
    // Every custom (Redpanda-range) handler must resolve via handler_for_key.
    check_all_types(kafka::redpanda_request_types{});
}

BOOST_AUTO_TEST_CASE(handler_custom_range_and_gap) {
    // DescribeRedpandaRoles (the reserved base key) resolves.
    BOOST_CHECK(
      kafka::handler_for_key(kafka::redpanda_api_key_base).has_value());
    // The first key past the standard range (a gap below the reserved base)
    // misses.
    BOOST_CHECK(
      !kafka::handler_for_key(kafka::api_key(kafka::max_api_key() + 1))
         .has_value());
    // An unused key inside the reserved range misses.
    BOOST_CHECK(!kafka::handler_for_key(
                   kafka::api_key(kafka::redpanda_api_key_base() + 1))
                   .has_value());
    // max_api_key() must remain the standard max, unaffected by custom keys.
    BOOST_CHECK_LT(
      kafka::api_key(kafka::max_api_key()), kafka::redpanda_api_key_base());
}

BOOST_AUTO_TEST_CASE(max_redpanda_api_key_spans_reserved_range) {
    // The reserved max is derived from redpanda_request_types and must sit at
    // or above the reserved base, well past the standard range.
    BOOST_CHECK_GE(
      kafka::api_key(kafka::max_redpanda_api_key()),
      kafka::redpanda_api_key_base);
    BOOST_CHECK_LT(
      kafka::api_key(kafka::max_api_key()), kafka::redpanda_api_key_base);
}

BOOST_AUTO_TEST_CASE(is_reserved_redpanda_api_key_range) {
    using kafka::api_key;
    using kafka::is_reserved_redpanda_api_key;
    // The reserved base and max are inside the range.
    BOOST_CHECK(is_reserved_redpanda_api_key(kafka::redpanda_api_key_base));
    BOOST_CHECK(
      is_reserved_redpanda_api_key(api_key(kafka::max_redpanda_api_key())));
    // Just below the base (the standard/reserved gap) is not reserved.
    BOOST_CHECK(!is_reserved_redpanda_api_key(
      api_key(kafka::redpanda_api_key_base() - 1)));
    // Just above the reserved max is not reserved.
    BOOST_CHECK(!is_reserved_redpanda_api_key(
      api_key(kafka::max_redpanda_api_key() + 1)));
    // Standard-range and invalid keys are not reserved.
    BOOST_CHECK(!is_reserved_redpanda_api_key(api_key(0)));
    BOOST_CHECK(!is_reserved_redpanda_api_key(api_key(kafka::max_api_key())));
    BOOST_CHECK(!is_reserved_redpanda_api_key(api_key(-1)));
}

BOOST_AUTO_TEST_CASE(flex_versions_custom_range) {
    using kafka::flex_versions;
    // DescribeRedpandaRoles is in schema and flexible from v0.
    BOOST_CHECK(flex_versions::is_api_in_schema(kafka::redpanda_api_key_base));
    BOOST_CHECK(
      flex_versions::is_flexible_request(
        kafka::redpanda_api_key_base, kafka::api_version(0)));
    // An unused reserved-range key is not in schema.
    BOOST_CHECK(!flex_versions::is_api_in_schema(
      kafka::api_key(kafka::redpanda_api_key_base() + 1)));
}
