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

#include "proto/redpanda/core/admin/v2/security/report.proto.h"
// #include "redpanda/admin/proxy/client.h"

namespace admin::security {

class report_service_impl : public proto::admin::security::report::report_service {
public:
  report_service_impl() = default;

  // GET /v1/security/report
  // Get comprehensive security report for all interfaces
  // References:
  // - https://google.aip.dev/136
  seastar::future<proto::admin::security::report::generate_security_report_response> generate_security_report(serde::pb::rpc::context, proto::admin::security::report::generate_security_report_request) override;

private:
    // proto::admin::security::report::user self_user() const;

    // admin::proxy::client _proxy_client;
    // std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin::security
