/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/oidc_principal_mapping_applicator.h"

#include "security/group_range.h"
#include "security/logger.h"
#include "security/oidc_principal_mapping.h"

#include <boost/algorithm/string.hpp>
#include <rapidjson/pointer.h>

namespace security::oidc {


result<acl_principal> principal_mapping_rule_apply(
  const principal_mapping_rule& mapping, const jwt& jwt) {
    auto claim = jwt.claim<std::string_view>(mapping.claim());
    if (claim.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    auto principal = mapping.mapping().apply(claim.value());
    if (principal.value_or("").empty()) {
        return errc::jwt_invalid_principal;
    }

    return {principal_type::user, std::move(principal).value()};
}

result<group_range>
group_policy_apply(const group_claim_policy& policy, ss::lw_shared_ptr<const jwt> jwt) {
    return group_range{jwt, policy};
    // const auto& p = policy.group_pointer();
    // auto list_claim_sv = jwt->claim<chunked_vector<std::string_view>>(p);
    // if (list_claim_sv) {
    //     vlog(
    //       seclog.trace,
    //       "Group claim found as string list: {}",
    //       list_claim_sv.value());
    //     chunked_vector<ss::sstring> list_claim;
    //     list_claim.reserve(list_claim_sv.value().size());
    //     std::ranges::transform(
    //         list_claim_sv.value(),
    //         std::back_inserter(list_claim),
    //         [](std::string_view sv) { return ss::sstring{sv}; }
    //     );
    //     return group_range(std::move(list_claim), policy);
    // }

    // auto string_claim = jwt->claim<std::string_view>(p);
    // if (!string_claim) {
    //     return group_range{};
    // }

    // vlog(seclog.trace, "Group claim found as string: {}", string_claim.value());
    // return group_range(
    //   ss::sstring{string_claim.value()}, policy.nested_behavior());
}

} // namespace security::oidc
