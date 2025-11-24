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

#include "base/vlog.h"
#include "redpanda/admin/services/oidc.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin::security {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger oidclog{"admin_api_server/oidc_service"};

} // namespace

// GET /v1/security/oidc/whoami
// Get information about the current authenticated user
// References:
// - https://google.aip.dev/136
seastar::future<proto::admin::security::oidc_who_am_i_response> oidc_service_impl::oidc_who_am_i(serde::pb::rpc::context ctx, proto::admin::security::oidc_who_am_i_request req) {
    (void)ctx;
    vlog(oidclog.info, "oidc_who_am_i: {}", req);
    proto::admin::security::oidc_who_am_i_response resp;
    // Populate the response with the current authenticated user's information
    co_return resp;
}

// POST /v1/security/oidc/keys/cache_invalidate
// Reload the OIDC keys from the identity provider.
// Flush the JWK cache and force reload keys.
// References:
// - https://google.aip.dev/136
seastar::future<proto::admin::security::refresh_oidc_keys_response> oidc_service_impl::refresh_oidc_keys(serde::pb::rpc::context ctx, proto::admin::security::refresh_oidc_keys_request req) {
    (void)ctx;
    vlog(oidclog.info, "refresh_oidc_keys: {}", req);
    proto::admin::security::refresh_oidc_keys_response resp;
    // Perform the cache invalidation and key reload
    co_return resp;
}

} // namespace admin::security
