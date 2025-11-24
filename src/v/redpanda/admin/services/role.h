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

#include "proto/redpanda/core/admin/v2/security/role.proto.h"
// #include "redpanda/admin/proxy/client.h"

namespace admin::security {

class role_service_impl : public proto::admin::security::role::role_service {
public:
  role_service_impl() = default;

  // POST /v1/security/roles
  // Create a role
  // References:
  // - https://google.aip.dev/133
  seastar::future<proto::admin::security::role::create_role_response> create_role(serde::pb::rpc::context, proto::admin::security::role::create_role_request) override;
  // GET /v1/security/roles/{role}
  // Get role
  // References:
  // - https://google.aip.dev/131
  seastar::future<proto::admin::security::role::get_role_response> get_role(serde::pb::rpc::context, proto::admin::security::role::get_role_request) override;
  // GET /v1/security/roles
  // List roles
  // References:
  // - https://google.aip.dev/132
  seastar::future<proto::admin::security::role::list_roles_response> list_roles(serde::pb::rpc::context, proto::admin::security::role::list_roles_request) override;
  // GET /v1/security/users/roles
  // List roles for the current user
  // References:
  // - https://google.aip.dev/132
  seastar::future<proto::admin::security::role::list_roles_for_current_user_response> list_roles_for_current_user(serde::pb::rpc::context, proto::admin::security::role::list_roles_for_current_user_request) override;
  // DELETE /v1/security/roles/{role}
  // Remove a role by name
  // References:
  // - https://google.aip.dev/135
  seastar::future<proto::admin::security::role::delete_role_response> delete_role(serde::pb::rpc::context, proto::admin::security::role::delete_role_request) override;
  // PUT /v1/security/roles/{role}
  // Update role
  // References:
  // - https://google.aip.dev/134
  seastar::future<proto::admin::security::role::update_role_response> update_role(serde::pb::rpc::context, proto::admin::security::role::update_role_request) override;


private:
    // proto::admin::security::role::role self_role() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
