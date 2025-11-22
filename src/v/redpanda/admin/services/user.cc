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

#include "redpanda/admin/services/user.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin::security {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger userlog{"admin_api_server/user_service"};

} // namespace

seastar::future<proto::admin::security::create_user_response> 
user_service_impl::create_user(serde::pb::rpc::context ctx, proto::admin::security::create_user_request req) {
    (void)ctx;
    proto::admin::security::create_user_response resp;
    resp.set_user(std::move(req.get_user()));
    co_return resp;
}

} // namespace admin::security
