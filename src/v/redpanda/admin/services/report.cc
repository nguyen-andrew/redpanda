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
#include "redpanda/admin/services/report.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin::security {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger reportlog{"admin_api_server/report_service"};

} // namespace

// GET /v1/security/report
// Get comprehensive security report for all interfaces
// References:
// - https://google.aip.dev/136
seastar::future<proto::admin::security::report::generate_security_report_response> report_service_impl::generate_security_report(serde::pb::rpc::context ctx, proto::admin::security::report::generate_security_report_request req) {
    (void)ctx;
    vlog(reportlog.info, "generate_security_report: {}", req);
    proto::admin::security::report::generate_security_report_response resp;
    // Populate the response with the security report details
    co_return resp;
}

} // namespace admin::security
