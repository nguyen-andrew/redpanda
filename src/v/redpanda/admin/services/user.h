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

#include "proto/redpanda/core/admin/v2/security/user.proto.h"
// #include "redpanda/admin/proxy/client.h"

namespace admin::security {

// An admin service that provides some reflection capabilities
// for the admin API, such as the version of Redpanda as well as
// what requests are available.
class user_service_impl : public proto::admin::security::user::user_service {
public:
  user_service_impl() = default;

  // POST /v1/security/users
  // Create user
  seastar::future<proto::admin::security::user::create_user_response> create_user(serde::pb::rpc::context, proto::admin::security::user::create_user_request) override;
  // GET /v1/security/users
  // List users
  seastar::future<proto::admin::security::user::list_users_response> list_users(serde::pb::rpc::context, proto::admin::security::user::list_users_request) override;
  // DELETE /v1/security/users/{user}
  // Delete user
  seastar::future<proto::admin::security::user::delete_user_response> delete_user(serde::pb::rpc::context, proto::admin::security::user::delete_user_request) override;
  // PUT /v1/security/users/{user}
  // Update user
  seastar::future<proto::admin::security::user::update_user_response> update_user(serde::pb::rpc::context, proto::admin::security::user::update_user_request) override;


private:
    // proto::admin::security::user::user self_user() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
