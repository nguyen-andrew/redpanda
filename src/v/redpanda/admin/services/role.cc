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
#include "redpanda/admin/services/role.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin::security {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger rolelog{"admin_api_server/role_service"};

} // namespace

// POST /v1/security/roles
// Create a role
// References:
// - https://google.aip.dev/133
seastar::future<proto::admin::security::create_role_response> role_service_impl::create_role(serde::pb::rpc::context ctx, proto::admin::security::create_role_request req) {
    (void)ctx;
    vlog(rolelog.info, "create_role: {}", req);
    proto::admin::security::create_role_response resp;
    resp.set_role(std::move(req.get_role()));
    co_return resp;
}

// GET /v1/security/roles/{role}
// Get role
// References:
// - https://google.aip.dev/131
seastar::future<proto::admin::security::get_role_response> role_service_impl::get_role(serde::pb::rpc::context ctx, proto::admin::security::get_role_request req) {
    (void)ctx;
    vlog(rolelog.info, "get_role: {}", req);
    proto::admin::security::get_role_response resp;
    // Populate the response with the role details
    co_return resp;
}

// GET /v1/security/roles
// List roles
// References:
// - https://google.aip.dev/132
seastar::future<proto::admin::security::list_roles_response> role_service_impl::list_roles(serde::pb::rpc::context ctx, proto::admin::security::list_roles_request req) {
    (void)ctx;
    vlog(rolelog.info, "list_roles: {}", req);
    proto::admin::security::list_roles_response resp;
    // Populate the response with the list of roles
    co_return resp;
}

// DELETE /v1/security/roles/{role}
// Remove a role by name
// References:
// - https://google.aip.dev/135
seastar::future<proto::admin::security::delete_role_response> role_service_impl::delete_role(serde::pb::rpc::context ctx, proto::admin::security::delete_role_request req) {
    (void)ctx;
    vlog(rolelog.info, "delete_role: {}", req);
    proto::admin::security::delete_role_response resp;
    // Perform the deletion logic here
    co_return resp;
}

} // namespace admin::security
