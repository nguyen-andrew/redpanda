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

#include <iterator>
#include <optional>
#include <string_view>
#include <variant>

namespace security {

acl_principal
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
/// - Already materialized chunked_vector<acl_principal>
/// - Empty (no groups)
///
/// Example usage:
/// \code
///   auto groups = group_range::from_string("admin,users,readers", behavior);
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
    explicit group_range(const ss::sstring& comma_separated, oidc::nested_group_behavior behavior) {
        // Ensure leading comma for simpler parsing logic.
        if (comma_separated.empty() || comma_separated.front() != ',') {
            _storage = ss::make_sstring(",", comma_separated);
        } else {
            _storage = comma_separated;
        }
        _behavior = behavior;
    }

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
                // A string with only "," represents empty (one empty element from empty input)
                return s.size() == 1 && s[0] == ',';
            }
        );
    }


    struct iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = security::acl_principal;
        using difference_type   = std::ptrdiff_t;
        using reference         = security::acl_principal&;

        struct vec_state {
            const chunked_vector<ss::sstring>* v = nullptr;
            std::size_t index = 0;
        };

        // Split state assumes the backing string starts with a comma
        // - "," -> [""]
        // - ",a," -> ["a", ""]
        // - ",," -> ["", ""]
        // - ",a,,b" -> ["a", "", "b"]
        // - ",a,,b," -> ["a", "", "b", ""]
        struct split_state {
            const ss::sstring* backing = nullptr; // for identity checks in operator==
            std::string_view remaining;
            std::string_view current;

            void advance() noexcept {
                if (remaining.empty()) {
                    return;
                }

                const auto next_comma = remaining.find(',', 1);
                if (next_comma == std::string_view::npos) {
                    current = remaining;
                    remaining = {};
                } else {
                    // Consume up to next comma
                    current = remaining.substr(0, next_comma);
                    remaining.remove_prefix(next_comma);
                }
                // Remove leading comma from current
                current.remove_prefix(1);
            }
        };

        using type = std::variant<std::monostate, vec_state, split_state>;

        type _it_state;
        oidc::nested_group_behavior _behavior;
        mutable security::acl_principal _current_principal;

        reference operator*() const noexcept {
            auto group_str = ss::visit(_it_state,
                [](std::monostate const&) -> std::string_view {
                    // UB to dereference empty/end iterator
                    __builtin_unreachable();
                },
                [](vec_state const& s) -> std::string_view {
                    return s.v->at(s.index);
                },
                [](split_state const& s) -> std::string_view {
                    // In split mode, current is always the token for this position.
                    // (If we're at end, deref is undefined like normal iterators.)
                    return s.current;
                }
            );

            _current_principal = apply_nested_group_policy(group_str, _behavior);
            return _current_principal;
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
            // Iterators with different behaviors produce different values, so not equal
            if (a._behavior != b._behavior) {
                return false;
            }

            return std::visit([&](auto const& x, auto const& y) noexcept {
                using T1 = std::decay_t<decltype(x)>;
                using T2 = std::decay_t<decltype(y)>;

                if constexpr (!std::is_same_v<T1, T2>) {
                    return false; // Different variant alternatives can't be equal
                } else if constexpr (std::is_same_v<T1, std::monostate>) {
                    return true; // All empty iterators are equal
                } else if constexpr (std::is_same_v<T1, vec_state>) {
                    return x.v == y.v && x.index == y.index;
                } else if constexpr (std::is_same_v<T1, split_state>) {
                    // Here both are split_state
                    // Iterators are equal if they point to the same backing string
                    // and have the same remaining portion to parse
                    return x.backing == y.backing
                        && x.remaining.data() == y.remaining.data()
                        && x.remaining.size() == y.remaining.size();
                }
            }, a._it_state, b._it_state);
        }

        friend bool operator!=(iterator const& a, iterator const& b) noexcept {
            return !(a == b);
        }
    };

    iterator begin() const noexcept {
        auto it_state = ss::visit(_storage,
            [](std::monostate const&) -> iterator::type {
                return std::monostate{};
            },
            [](chunked_vector<ss::sstring> const& v) -> iterator::type {
                // return { ._it_state=iterator::vec_state{ .v=&v, .index=0 }, .behavior=_behavior };
                return iterator::vec_state{ .v=&v, .index=0 };
            },
            [](ss::sstring const& s) -> iterator::type {
                iterator::split_state ss;
                ss.backing = &s;
                ss.remaining = std::string_view(s);
                ss.current = {};
                // Prime the first token into ss.current
                ss.advance();
                // return { ._it_state=ss, .behavior=_behavior };
                return ss;
            }
        );

        return iterator{. _it_state = it_state, ._behavior = _behavior};
    }

    iterator end() const noexcept {
        auto it_state = ss::visit(_storage,
            [](std::monostate const&) -> iterator::type {
                return std::monostate{};
            },
            [](chunked_vector<ss::sstring> const& v) -> iterator::type {
                return iterator::vec_state{ .v=&v, .index=v.size() };
            },
            [](ss::sstring const& s) -> iterator::type {
                iterator::split_state ss;
                ss.backing = &s;
                ss.remaining = {};  // Empty remaining indicates end
                ss.current = {};
                return ss;
            }
        );
        return iterator{. _it_state = it_state, ._behavior = _behavior};
    }

private:
    std::variant<std::monostate, chunked_vector<ss::sstring>, ss::sstring> _storage;
    oidc::nested_group_behavior _behavior{oidc::nested_group_behavior::none};
};

} // namespace security
