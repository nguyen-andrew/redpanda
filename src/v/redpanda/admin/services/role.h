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

#pragma once

#include "proto/redpanda/core/admin/v2/security.proto.h"
// #include "redpanda/admin/proxy/client.h"

namespace admin::security {

class role_service_impl : public proto::admin::security::role_service {
public:
  role_service_impl() = default;

  // POST /v1/security/roles
  // Create a role
  // References:
  // - https://google.aip.dev/133
  seastar::future<proto::admin::security::create_role_response> create_role(serde::pb::rpc::context, proto::admin::security::create_role_request) override;

private:
    // proto::admin::security::role self_role() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
