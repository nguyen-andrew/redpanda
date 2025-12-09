// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// #include "pandaproxy/schema_registry/test/compatibility_avro.h"

#include "pandaproxy/error.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/avro_payloads.h"
#include "pandaproxy/schema_registry/test/client_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/boost_fixture.h"

#include <seastar/util/bool_class.hh>

#include <limits>
#include <optional>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

// static ss::logger avro_field_name_test_log("avro_field_name_test");

SEASTAR_THREAD_TEST_CASE(test_bad_field_name_with_dots) {
    // Parsing Canonical Form requires fields to be ordered:
    // name, type, fields, symbols, items, values, size
    pps::schema_definition schema{
      R"({"type":"record","namespace":"com.redpanda.examples.avro","name":"ClickEvent","fields":[{"name":"foo.bar.baz","type":"long"}]})",
    // pps::schema_definition schema{
    //   R"({
    //         "type": "record",
    //         "namespace": "com.redpanda.examples.avro",
    //         "name": "ClickEvent",
    //         "fields": [
    //             {"name": "foo.bar.baz", "type": "long"}
    //         ]
    //       })",
      pps::schema_type::avro};
    pps::sharded_store s;
    BOOST_REQUIRE_EXCEPTION(
        pps::make_avro_schema_definition(
            s,
            {pps::subject("subject"),
             {schema.shared_raw(), pps::schema_type::avro}})
            .get(),
        pps::exception,
        [](const auto& ex) {
          return ex.code() == pps::error_code::schema_invalid;
        });

    // auto valid = pps::make_avro_schema_definition(
    //                s,
    //                {pps::subject("subject"),
    //                 {schema.shared_raw(), pps::schema_type::avro}})
    //                .get();
    // static_assert(
    //   std::
    //     is_same_v<std::decay_t<decltype(valid)>, pps::avro_schema_definition>,
    //   "schema is an avro_schema_definition");
    // pps::schema_definition avro_conversion{valid};
    // ASSERT_EQ(schema, avro_conversion);
    // ASSERT_EQ(valid.name(), "myrecord");
}

// FIXTURE_TEST(
//   schema_registry_post_subjects_subject_version, pandaproxy_test_fixture) {
//     using namespace std::chrono_literals;

//     info("Connecting client");
//     auto client = make_schema_reg_client();

//     {
//         info("Post a schema as key (expect schema_id=1)");
//         auto res = post_schema(
//           client, pps::subject{"test-key"}, avro_int_payload);
//         BOOST_REQUIRE_EQUAL(
//           res.headers.result(), boost::beast::http::status::ok);
//         BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
//         BOOST_REQUIRE_EQUAL(
//           res.headers.at(boost::beast::http::field::content_type),
//           to_header_value(ppj::serialization_format::schema_registry_v1_json));
//     }

//     {
//         info("Repost a schema as key (expect schema_id=1)");
//         auto res = post_schema(
//           client, pps::subject{"test-key"}, avro_int_payload);
//         BOOST_REQUIRE_EQUAL(
//           res.headers.result(), boost::beast::http::status::ok);
//         BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
//         BOOST_REQUIRE_EQUAL(
//           res.headers.at(boost::beast::http::field::content_type),
//           to_header_value(ppj::serialization_format::schema_registry_v1_json));
//     }

//     {
//         info("Repost a schema as value (expect schema_id=1)");
//         auto res = post_schema(
//           client, pps::subject{"test-value"}, avro_int_payload);
//         BOOST_REQUIRE_EQUAL(
//           res.headers.result(), boost::beast::http::status::ok);
//         BOOST_REQUIRE_EQUAL(res.body, R"({"id":1})");
//         BOOST_REQUIRE_EQUAL(
//           res.headers.at(boost::beast::http::field::content_type),
//           to_header_value(ppj::serialization_format::schema_registry_v1_json));
//     }

//     {
//         info("Post a new schema as key (expect schema_id=2)");
//         auto res = post_schema(
//           client, pps::subject{"test-key"}, avro_long_payload);
//         BOOST_REQUIRE_EQUAL(
//           res.headers.result(), boost::beast::http::status::ok);
//         BOOST_REQUIRE_EQUAL(res.body, R"({"id":2})");
//         BOOST_REQUIRE_EQUAL(
//           res.headers.at(boost::beast::http::field::content_type),
//           to_header_value(ppj::serialization_format::schema_registry_v1_json));
//     }
// }