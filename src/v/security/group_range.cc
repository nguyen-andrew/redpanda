/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "security/group_range.h"

// #include "container/chunked_vector.h"
// #include "security/acl.h"
// #include "security/config.h"
// #include "security/jwt.h"
// #include "security/oidc_principal_mapping.h"

// #include <seastar/core/sstring.hh>
// #include <seastar/util/variant_utils.hh>

// #include <fmt/format.h>

// #include <iterator>
// #include <optional>
// #include <string_view>
// #include <variant>

namespace security {

group_range::group_range(
  ss::lw_shared_ptr<const oidc::jwt> jwt, const oidc::group_claim_policy& policy) {
    const auto& p = policy.group_pointer();
    auto list_claim = jwt->claim<chunked_vector<std::string_view>>(p);
    if (list_claim) {
        _storage = jwt_list_state{
          .jwt = jwt,
          .policy = policy,
          .list_claim = std::move(list_claim.value())};
        return;
    }

    auto string_claim = jwt->claim<std::string_view>(p);
    if (string_claim) {
        _storage = jwt_string_state{
          .jwt = jwt,
          .policy = policy,
          .string_claim = string_claim.value()};
        return;
    }
}

group_range::group_range(
  chunked_vector<security::acl_principal> materialized_groups)
  : _storage{materialized_state{.materialized_groups = std::move(
      materialized_groups)}} {}


group_range group_range::copy() const {
    return ss::visit(_storage,
        [](std::monostate const&) -> group_range {
            return group_range{};
        },
        [](const jwt_list_state& state) -> group_range {
            return {state.jwt, state.policy};
        },
        [](const jwt_string_state& state) -> group_range {
            return {state.jwt, state.policy};
        },
        [](const materialized_state& state) -> group_range {
            return group_range(state.materialized_groups.copy());
        }
    );
}

bool group_range::empty() const noexcept {
        return ss::visit(_storage,
            [](std::monostate const&) -> bool {
                return true;
            },
            [](const jwt_list_state& state) -> bool {
                return state.list_claim.empty();
            },
            [](const jwt_string_state& state) -> bool {
                // TODO: Is this correct? What if the string is just whitespace?
                return state.string_claim.empty();
            },
            [](const materialized_state& state) -> bool {
                return state.materialized_groups.empty();
            }
        );
    }

}

} // namespace security