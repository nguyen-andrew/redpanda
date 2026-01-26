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

#include "container/chunked_vector.h"
#include "security/acl.h"
#include "security/config.h"
#include "security/jwt.h"
#include "security/oidc_principal_mapping.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

#include <cstddef>
#include <iterator>
#include <optional>
#include <string_view>
#include <variant>

namespace security {

/// \brief Lazy range for ACL group principals
///
/// This class provides a lazy view over group principals that defers
/// parsing and transformation until iteration. This is critical for
/// authentication performance since groups are often not needed:
/// - Superusers skip group checks entirely
/// - Empty ACLs skip group checks
/// - Principal matches often short-circuit before groups
///
/// The range supports multiple source types:
/// - Comma-separated string (lazy split on iteration)
/// - Pre-split array of string_view (from JWT array claims)
/// - Empty (no groups)
///
/// Example usage:
/// \code
///   auto groups = group_range("admin,users,readers", behavior);
///   for (const auto& g : groups) {  // Only materializes on iteration
///       if (check_acl(g)) break;    // Can short-circuit early
///   }
/// \endcode
class group_range {
private:
    struct jwt_list_state {
        ss::lw_shared_ptr<const oidc::jwt> jwt;
        oidc::group_claim_policy policy;
        chunked_vector<std::string_view> list_claim;
    };

    struct jwt_string_state {
        ss::lw_shared_ptr<const oidc::jwt> jwt;
        oidc::group_claim_policy policy;
        std::string_view string_claim;
    };

    struct materialized_state {
        chunked_vector<security::acl_principal> materialized_groups;
    };

public:
    using value_type = security::acl_principal;

    // Default constructor creates an empty range with no allocation
    group_range() = default;

    group_range(ss::lw_shared_ptr<const oidc::jwt> jwt, const oidc::group_claim_policy& policy);

    explicit group_range(chunked_vector<security::acl_principal> materialized_groups);

    group_range(group_range&&) = default;
    group_range& operator=(group_range&&) = default;
    ~group_range() = default;

    group_range(const group_range&) = delete;
    group_range& operator=(const group_range&) = delete;

    /// \brief Explicitly copy this group_range
    [[nodiscard]] group_range copy() const;

    /// \brief Check if this group_range is empty (has no groups)
    [[nodiscard]] bool empty() const noexcept;


    /// \warning Iterator lifetime is tied to the group_range.
    /// Moving or destroying the group_range invalidates all iterators.
    struct iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = security::acl_principal;
        using difference_type   = std::ptrdiff_t;
        using reference         = security::acl_principal;

        struct jwt_list_it_state {
            const jwt_list_state* jwt_list_state = nullptr;
            std::size_t index = 0;
        };

        struct jwt_string_it_state {
            // const jwt_string_state* jwt_string_state = nullptr;
            const jwt_string_state& jwt_string_state;
            // Invariant: remaining should not have leading or trailing commas
            std::string_view remaining;
            // Invariant: current should not have leading or trailing whitespaces
            std::string_view current{};
            // bool at_end = false;

            jwt_string_it_state(const struct jwt_string_state& state,
                                std::string_view rem)
              : jwt_string_state(state), remaining(rem) {
                skip_leading(remaining, ",");
                skip_trailing(remaining, ",");
                // Prime the first token into current
                advance();
              }

            explicit jwt_string_it_state(
              const struct jwt_string_state& state)
              : jwt_string_it_state(state, state.string_claim) {}

            static void skip_leading(std::string_view& str, std::string_view skip_chars) {
                auto first = str.find_first_not_of(skip_chars);
                if (first == std::string_view::npos) {
                    // Here, str is either all commas or empty
                    str = {};
                } else {
                    // first is the index of the first character we want to keep.
                    str.remove_prefix(first);
                }
            }

            static void skip_trailing(std::string_view& str, std::string_view skip_chars) {
                auto last = str.find_last_not_of(skip_chars);
                if (last == std::string_view::npos) {
                    // Here, str is either all commas or empty
                    str = {};
                } else {
                    // last is the index of the last character we want to keep.
                    str.remove_suffix(str.size() - last - 1);
                }
            }

            void advance() noexcept {
                current = {};

                while (!remaining.empty()) {
                    auto next_comma = remaining.find(',');
                    if (next_comma == std::string_view::npos) {
                        current = remaining;
                        remaining = {};
                    } else {
                        current = remaining.substr(0, next_comma);
                        remaining.remove_prefix(next_comma + 1);
                    }

                    // Invariant: remaining has no leading/trailing commas
                    skip_leading(remaining, ",");

                    // Invariant: current has no leading/trailing whitespaces
                    skip_leading(current, " \t\r\n");
                    skip_trailing(current, " \t\r\n");
                    // Skip empty/blank fields
                    if (current.empty()) {
                        // Loop continues, and remaining is smaller than before
                        continue;
                    } else {
                        return; // Found a valid current token
                    }
                }
            }

            bool at_end() const noexcept {
                return current.empty() && remaining.empty();
            }
        };

        struct materialized_it_state {
            const materialized_state* materialized_state = nullptr;
            std::size_t index = 0;
        };

        using type = std::variant<std::monostate, 
                                  jwt_list_it_state, 
                                  jwt_string_it_state, 
                                  materialized_it_state>;

        type _it_state;

        reference operator*() const noexcept;

        iterator& operator++() noexcept {
            ss::visit(_it_state,
                [](std::monostate&) noexcept {
                    // UB to increment empty/end iterator - no-op
                },
                [](jwt_list_it_state& s) noexcept {
                    ++s.index;
                },
                [](jwt_string_it_state& s) noexcept {
                    s.advance();
                },
                [](materialized_it_state& s) noexcept {
                    ++s.index;
                }
            );
            return *this;
        }

        iterator operator++(int) noexcept {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(iterator const& a, iterator const& b) noexcept {
            return std::visit([&](auto const& x, auto const& y) noexcept {
                using T1 = std::decay_t<decltype(x)>;
                using T2 = std::decay_t<decltype(y)>;

                if constexpr (!std::is_same_v<T1, T2>) {
                    return false; // Different variant alternatives can't be equal
                } else if constexpr (std::is_same_v<T1, std::monostate>) {
                    return true; // All empty iterators are equal
                } else if constexpr (std::is_same_v<T1, jwt_list_it_state>) {
                    return x.jwt_list_state == y.jwt_list_state && x.index == y.index;
                } else if constexpr (std::is_same_v<T1, jwt_string_it_state>) {
                    // If both at end, they're equal
                    if (x.at_end() && y.at_end()) {return true;}
                    if (x.at_end() != y.at_end()) {return false;}
                    
                    // Both not at end - compare remaining position
                    return x.remaining.data() == y.remaining.data()
                        && x.remaining.size() == y.remaining.size() 
                        && x.jwt_string_state.policy.nested_behavior() == y.jwt_string_state.policy.nested_behavior();
                } else if constexpr (std::is_same_v<T1, materialized_it_state>) {
                    return x.materialized_state == y.materialized_state && x.index == y.index;
                }
            }, a._it_state, b._it_state);
        }

        friend bool operator!=(iterator const& a, iterator const& b) noexcept {
            return !(a == b);
        }
    };

    // Delete rvalue overloads to prevent dangerous patterns
    iterator begin() const && = delete;   // Prevents: group_range(...).begin()
    iterator end() const && = delete;

    iterator begin() const & noexcept {
        return ss::visit(_storage,
            [](std::monostate const&) -> iterator {
                std::cout << "*** MONOSTATE" << std::endl;
                return {std::monostate{}};
            },
            [](const jwt_list_state& state) -> iterator {
                std::cout << "*** STRING VEC" << std::endl;
                return {iterator::jwt_list_it_state{ .jwt_list_state = &state, .index=0 }};
            },
            [](const jwt_string_state& state) -> iterator {
                std::cout << "*** CSV" << std::endl;
                return {iterator::jwt_string_it_state{state}};
            },
            [](const materialized_state& state) -> iterator {
                std::cout << "*** MATERIALIZED" << std::endl;
                return {iterator::materialized_it_state{ .materialized_state = &state, .index=0 }};
            }
        );
    }

    iterator end() const & noexcept {
        return ss::visit(_storage,
            [](std::monostate const&) -> iterator {
                return {std::monostate{}};
            },
            [](const jwt_list_state& state) -> iterator {
                return {iterator::jwt_list_it_state{ .jwt_list_state = &state, .index= state.list_claim.size() }};
            },
            [](const jwt_string_state& state) -> iterator {
                return {iterator::jwt_string_it_state{state, {}}};
            },
            [](const materialized_state& state) -> iterator {
                std::cout << "*** MATERIALIZED (END), v.size() = " << state.materialized_groups.size() << std::endl;
                return {iterator::materialized_it_state{ .materialized_state = &state, .index=state.materialized_groups.size() }};
            }
        );
    }

private:
    std::variant<std::monostate, jwt_list_state, jwt_string_state, materialized_state> _storage;
};

} // namespace security

// Formatter for group_range to enable logging
template<>
struct fmt::formatter<security::group_range> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const security::group_range& range, FormatContext& ctx) const {
        auto out = ctx.out();
        *out++ = '[';
        bool first = true;
        for (const auto& group : range) {
            if (!first) {
                *out++ = ',';
                *out++ = ' ';
            }
            first = false;
            out = fmt::format_to(out, "{}", group);
        }
        *out++ = ']';
        return out;
    }
};
