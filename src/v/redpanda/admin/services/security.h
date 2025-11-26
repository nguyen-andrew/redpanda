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

class security_service_impl : public proto::admin::security_service {
public:
  security_service_impl() = default;

  seastar::future<proto::admin::create_scram_credential_response> create_scram_credential(serde::pb::rpc::context, proto::admin::create_scram_credential_request) override;
  seastar::future<proto::admin::get_scram_credentials_response> get_scram_credentials(serde::pb::rpc::context, proto::admin::get_scram_credentials_request) override;
  seastar::future<proto::admin::list_scram_credentials_response> list_scram_credentials(serde::pb::rpc::context, proto::admin::list_scram_credentials_request) override;
  seastar::future<proto::admin::update_scram_credential_response> update_scram_credential(serde::pb::rpc::context, proto::admin::update_scram_credential_request) override;
  seastar::future<proto::admin::delete_scram_credential_response> delete_scram_credential(serde::pb::rpc::context, proto::admin::delete_scram_credential_request) override;

  seastar::future<proto::admin::create_role_response> create_role(serde::pb::rpc::context, proto::admin::create_role_request) override;
  seastar::future<proto::admin::get_role_response> get_role(serde::pb::rpc::context, proto::admin::get_role_request) override;
  seastar::future<proto::admin::list_roles_response> list_roles(serde::pb::rpc::context, proto::admin::list_roles_request) override;
  seastar::future<proto::admin::update_role_response> update_role(serde::pb::rpc::context, proto::admin::update_role_request) override;
  seastar::future<proto::admin::delete_role_response> delete_role(serde::pb::rpc::context, proto::admin::delete_role_request) override;
  seastar::future<proto::admin::enumerate_current_user_roles_response> enumerate_current_user_roles(serde::pb::rpc::context, proto::admin::enumerate_current_user_roles_request) override;

  seastar::future<proto::admin::resolve_oidc_identity_response> resolve_oidc_identity(serde::pb::rpc::context, proto::admin::resolve_oidc_identity_request) override;
  seastar::future<proto::admin::refresh_oidc_keys_response> refresh_oidc_keys(serde::pb::rpc::context, proto::admin::refresh_oidc_keys_request) override;
  seastar::future<proto::admin::revoke_credentials_response> revoke_credentials(serde::pb::rpc::context, proto::admin::revoke_credentials_request) override;

  seastar::future<proto::admin::generate_security_report_response> generate_security_report(serde::pb::rpc::context, proto::admin::generate_security_report_request) override;


private:
    // proto::admin::user self_user() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
