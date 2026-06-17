/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"
#include "serde/rw/rw.h"

#include <gtest/gtest.h>

namespace cluster_link::model::tests {

TEST(test_model, test_no_leak_private_data) {
    scram_credentials creds{
      .username = "user", .password = "pass", .mechanism = "SCRAM-SHA-256"};

    auto creds_str = fmt::format("{}", creds);
    // verify password does not get printed
    EXPECT_TRUE(creds_str.contains("password: ****"));

    connection_config config_files{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_file_path{"cert.pem"},
      .key = tls_file_path{"key.pem"},
      .ca = tls_file_path{"ca.pem"},
      .client_id = "client-id"};

    auto fmt = fmt::format("{}", config_files);
    // verify password does not get printed
    EXPECT_TRUE(fmt.contains("password: ****"));

    connection_config config_values{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_value{"cert.pem"},
      .key = tls_value{"key.pem"},
      .ca = tls_value{"ca.pem"},
      .client_id = "client-id"};
    auto values_fmt = fmt::format("{}", config_values);
    // verify password does not get printed
    EXPECT_TRUE(fmt.contains("password: ****"));
    // verify key is not printed
    EXPECT_TRUE(values_fmt.contains("key: {value: ****}"));
}
TEST(test_model, role_sync_config_serde_round_trip) {
    link_configuration cfg;
    cfg.role_sync_cfg.is_enabled = enabled_t::no;
    cfg.role_sync_cfg.task_interval = std::chrono::seconds{45};
    cfg.role_sync_cfg.role_name_filters.emplace_back(
      resource_name_filter_pattern{
        .pattern_type = filter_pattern_type::prefix,
        .filter = filter_type::include,
        .pattern = "analytics-"});

    auto buf = serde::to_iobuf(cfg.copy());
    auto decoded = serde::from_iobuf<link_configuration>(std::move(buf));

    EXPECT_EQ(decoded, cfg);
    ASSERT_EQ(decoded.role_sync_cfg.role_name_filters.size(), 1);
    EXPECT_EQ(decoded.role_sync_cfg.is_enabled, enabled_t::no);
    EXPECT_EQ(
      decoded.role_sync_cfg.get_task_interval(), std::chrono::seconds{45});
}
} // namespace cluster_link::model::tests
