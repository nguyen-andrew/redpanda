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

class oidc_service_impl : public proto::admin::security::oidc_service {
public:
  oidc_service_impl() = default;

  // GET /v1/security/oidc/whoami
  // Get information about the current authenticated user
  // References:
  // - https://google.aip.dev/136
  seastar::future<proto::admin::security::oidc_who_am_i_response> oidc_who_am_i(serde::pb::rpc::context, proto::admin::security::oidc_who_am_i_request) override;
  // POST /v1/security/oidc/keys/cache_invalidate
  // Reload the OIDC keys from the identity provider.
  // Flush the JWK cache and force reload keys.
  // References:
  // - https://google.aip.dev/136
  seastar::future<proto::admin::security::refresh_oidc_keys_response> refresh_oidc_keys(serde::pb::rpc::context, proto::admin::security::refresh_oidc_keys_request) override;

private:
    // proto::admin::security::user self_user() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
