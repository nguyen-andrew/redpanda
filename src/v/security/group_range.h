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

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

#include <iterator>
#include <optional>
#include <string_view>
#include <variant>

namespace security {

// TODO: move this?
inline acl_principal
apply_nested_group_policy(std::string_view user, oidc::nested_group_behavior b) {
    switch (b) {
    case oidc::nested_group_behavior::none:
        return acl_principal{principal_type::group, ss::sstring{user}};
    case oidc::nested_group_behavior::suffix: {
        auto pos = user.find_last_of('/');
        if (pos == std::string_view::npos) {
            return acl_principal{principal_type::group, ss::sstring{user}};
        }
        return acl_principal{
          principal_type::group, ss::sstring{user.substr(pos + 1)}};
    }
    }
}

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
public:
    using value_type = security::acl_principal;

    // Default constructor creates an empty range with no allocation
    group_range() = default;

    // Own a vector of group names.
    explicit group_range(chunked_vector<ss::sstring> groups, oidc::nested_group_behavior behavior)
    : _storage(std::move(groups)), _behavior(behavior) {}

    // Own a comma-separated list.
    explicit group_range(ss::sstring comma_separated, oidc::nested_group_behavior behavior)
    : _storage(std::move(comma_separated)), _behavior(behavior) {}

    explicit group_range(chunked_vector<security::acl_principal> materialized_groups)
    : _storage(std::move(materialized_groups)) {}

    group_range(group_range&&) = default;
    group_range& operator=(group_range&&) = default;
    ~group_range() = default;

    group_range(const group_range&) = delete;
    group_range& operator=(const group_range&) = delete;

    /// \brief Explicitly copy this group_range
    [[nodiscard]] group_range copy() const {
        return ss::visit(_storage,
            [](std::monostate const&) -> group_range {
                return group_range{};
            },
            [this](chunked_vector<ss::sstring> const& v) -> group_range {
                return group_range(v.copy(), _behavior);
            },
            [this](ss::sstring const& s) -> group_range {
                return group_range(s, _behavior);
            },
            [](chunked_vector<security::acl_principal> const& v) -> group_range {
                return group_range(v.copy());
            }
        );
    }

    /// \brief Check if this group_range is empty (has no groups)
    [[nodiscard]] bool empty() const noexcept {
        return ss::visit(_storage,
            [](std::monostate const&) -> bool {
                return true;
            },
            [](chunked_vector<ss::sstring> const& v) -> bool {
                return v.empty();
            },
            [](ss::sstring const& s) -> bool {
                return s.empty();
            },
            [](chunked_vector<security::acl_principal> const& v) -> bool {
                return v.empty();
            }
        );
    }


    /// \warning Iterator lifetime is tied to the group_range.
    /// Moving or destroying the group_range invalidates all iterators.
    struct iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = security::acl_principal;
        using difference_type   = std::ptrdiff_t;
        using reference         = security::acl_principal;

        struct vec_state {
            oidc::nested_group_behavior _behavior;
            const chunked_vector<ss::sstring>* v = nullptr;
            std::size_t index = 0;
        };

        struct split_state {
            oidc::nested_group_behavior _behavior;
            std::string_view remaining;
            std::string_view current;
            bool at_end = false;

            void advance() noexcept {
                if (at_end) {
                    return;
                }

                if (remaining.empty()) {
                    // Final element after trailing comma (empty string)
                    current = {};
                    at_end = true;
                    return;
                }

                const auto next_comma = remaining.find(',');
                if (next_comma == std::string_view::npos) {
                    current = remaining;
                    remaining = {};
                } else {
                    current = remaining.substr(0, next_comma);
                    remaining.remove_prefix(next_comma + 1);
                }
            }
        };

        struct materialized_state {
            const chunked_vector<security::acl_principal>* v = nullptr;
            std::size_t index = 0;
        };

        using type = std::variant<std::monostate, vec_state, split_state, materialized_state>;

        type _it_state;

        reference operator*() const noexcept {
            return ss::visit(_it_state,
                [](std::monostate const&) -> security::acl_principal {
                    // UB to dereference empty/end iterator
                    __builtin_unreachable();
                },
                [](vec_state const& s) -> security::acl_principal {
                    return apply_nested_group_policy(s.v->at(s.index), s._behavior);
                },
                [](split_state const& s) -> security::acl_principal {
                    // In split mode, current is always the token for this position.
                    // (If we're at end, deref is undefined like normal iterators.)
                    return apply_nested_group_policy(s.current, s._behavior);
                },
                [](materialized_state const& s) -> security::acl_principal {
                    return s.v->at(s.index);
                }
            );
        }

        iterator& operator++() noexcept {
            ss::visit(_it_state,
                [](std::monostate&) noexcept {
                    // UB to increment empty/end iterator - no-op
                },
                [](vec_state& s) noexcept {
                    ++s.index;
                },
                [](split_state& s) noexcept {
                    s.advance();
                },
                [](materialized_state& s) noexcept {
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
                } else if constexpr (std::is_same_v<T1, vec_state>) {
                    return x.v == y.v && x.index == y.index && x._behavior == y._behavior;
                } else if constexpr (std::is_same_v<T1, split_state>) {
                    // If both at end, they're equal
                    if (x.at_end && y.at_end) {return true;}
                    if (x.at_end != y.at_end) {return false;}
                    
                    // Both not at end - compare remaining position
                    return x.remaining.data() == y.remaining.data()
                        && x.remaining.size() == y.remaining.size() 
                        && x._behavior == y._behavior;
                } else if constexpr (std::is_same_v<T1, materialized_state>) {
                    auto eq =( x.v == y.v && x.index == y.index);
                    std::cout << (eq ? "EQUAL" : "NOT EQUAL") << std::endl;
                    std::cout << "x.index: " << x.index << ", y.index: " << y.index << std::endl;
                    return eq;
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
            [this](chunked_vector<ss::sstring> const& v) -> iterator {
                std::cout << "*** STRING VEC" << std::endl;
                // return { ._it_state=iterator::vec_state{ .v=&v, .index=0 }, .behavior=_behavior };
                return {iterator::vec_state{ ._behavior=_behavior, .v=&v, .index=0 }};
            },
            [this](ss::sstring const& s) -> iterator {
                std::cout << "*** CSV" << std::endl;
                iterator::split_state ss{ ._behavior=_behavior, .remaining = s, .current = {}, .at_end = false };
                // Prime the first token into ss.current
                ss.advance();
                // return { ._it_state=ss, .behavior=_behavior };
                return {ss};
            },
            [](chunked_vector<security::acl_principal> const& v) -> iterator {
                std::cout << "*** MATERIALIZED" << std::endl;
                return {iterator::materialized_state{ .v=&v, .index=0 }};
            }
        );
    }

    iterator end() const & noexcept {
        return ss::visit(_storage,
            [](std::monostate const&) -> iterator {
                return {std::monostate{}};
            },
            [this](chunked_vector<ss::sstring> const& v) -> iterator {
                return {iterator::vec_state{ ._behavior=_behavior, .v=&v, .index=v.size() }};
            },
            [this](ss::sstring const&) -> iterator {
                return {iterator::split_state{._behavior=_behavior, .remaining = {}, .current = {}, .at_end = true}};
            },
            [](chunked_vector<security::acl_principal> const& v) -> iterator {
                std::cout << "*** MATERIALIZED (END), v.size() = " << v.size() << std::endl;
                return {iterator::materialized_state{ .v=&v, .index=v.size() }};
            }
        );
    }

private:
    std::variant<std::monostate, chunked_vector<ss::sstring>, ss::sstring, chunked_vector<security::acl_principal>> _storage;
    oidc::nested_group_behavior _behavior = oidc::nested_group_behavior::none;
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
