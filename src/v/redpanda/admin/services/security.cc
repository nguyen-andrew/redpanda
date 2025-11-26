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
#include "redpanda/admin/services/security.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin::security {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger securitylog{"admin_api_server/security_service"};

} // namespace

seastar::future<proto::admin::create_scram_credential_response> 
security_service_impl::create_scram_credential(serde::pb::rpc::context ctx, proto::admin::create_scram_credential_request req) {
    (void)ctx;
    vlog(securitylog.info, "create_user: {}", req);
    proto::admin::create_scram_credential_response resp;
    co_return resp;
}

seastar::future<proto::admin::list_scram_credentials_response> 
security_service_impl::list_scram_credentials(serde::pb::rpc::context ctx, proto::admin::list_scram_credentials_request req) {
    (void)ctx;
    vlog(securitylog.info, "list_scram_credentials: {}", req);
    proto::admin::list_scram_credentials_response resp;
    co_return resp;
}

seastar::future<proto::admin::update_scram_credential_response> 
security_service_impl::update_scram_credential(serde::pb::rpc::context ctx, proto::admin::update_scram_credential_request req) {
    (void)ctx;
    vlog(securitylog.info, "update_scram_credential: {}", req);
    proto::admin::update_scram_credential_response resp;
    co_return resp;
}

seastar::future<proto::admin::delete_scram_credential_response> 
security_service_impl::delete_scram_credential(serde::pb::rpc::context ctx, proto::admin::delete_scram_credential_request req) {
    (void)ctx;
    vlog(securitylog.info, "delete_scram_credential: {}", req);
    proto::admin::delete_scram_credential_response resp;
    co_return resp;
}

} // namespace admin::security
