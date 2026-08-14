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

#include <fmt/format.h>

#include <cstdio>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace demo {

/// Every line the demos emit starts with '|' so run_all.sh can recover
/// the transcript with grep '^|'. stderr is unbuffered, so the lines
/// survive the scenario that ends in std::terminate.
template<typename... Args>
void say(fmt::format_string<Args...> fmt_str, Args&&... args) {
    auto line = fmt::format(fmt_str, std::forward<Args>(args)...);
    std::fprintf(stderr, "| %s\n", line.c_str());
}

/// A section header in the transcript. Scenarios call this at points
/// where they have control, to group the lines that follow under one
/// heading with blank lines around it.
inline void phase(const std::string& text) {
    std::fprintf(stderr, "|\n| ==== %s ====\n|\n", text.c_str());
}

namespace detail {
inline int unwind_event_count = 0;
} // namespace detail

/// Number of instrumented destructions that ran while an exception was
/// unwinding (std::uncaught_exceptions() > 0). Each one is direct evidence
/// that a live throw passed through the enclosing frame.
inline int unwind_events() { return detail::unwind_event_count; }
inline void reset_unwind_events() { detail::unwind_event_count = 0; }

enum class dtor_mode {
    log_only,
    /// Model of a guard-style destructor: it throws when the introspection
    /// API says no exception is in flight. Safe on the ordinary stack,
    /// fatal in coroutine frame teardown after a real suspension.
    throw_when_clear,
};

/// Logs its construction, moves, and destruction. At destruction it also
/// prints what std::uncaught_exceptions() and std::current_exception()
/// report at that moment, which is the only view of the world a destructor
/// has for deciding whether an exception is in flight.
///
/// Each move-construction bumps a generation counter that shows up in the
/// label: "param" is the object the scenario created, "param#1" was
/// move-constructed from it, "param#2" from that, and so on. Passing a
/// named object into a coroutine by value produces a three-object chain:
/// the named original, the by-value parameter object the caller
/// materializes for the call, and the copy the coroutine makes in its
/// frame (the one the body actually uses).
class instrumented {
public:
    explicit instrumented(
      std::string name, dtor_mode mode = dtor_mode::log_only)
      : _name(std::move(name))
      , _mode(mode) {
        event(
          "instrumented()",
          _mode == dtor_mode::throw_when_clear
            ? "constructed (mode: throw_when_clear)"
            : "constructed");
    }

    instrumented(instrumented&& other) noexcept
      : _name(other._name)
      , _mode(other._mode)
      , _gen(other._gen + 1) {
        other._moved_from = true;
        event(
          "instrumented(&&)",
          fmt::format(
            "move-constructed from {}; {} is now moved-from",
            other.label(),
            other.label()));
    }

    instrumented(const instrumented&) = delete;
    instrumented& operator=(const instrumented&) = delete;
    instrumented& operator=(instrumented&&) = delete;

    void note_alive() const {
        event("note_alive()", "the coroutine body uses this object");
    }

    /// "name" for the originally constructed object, "name#N" after N
    /// move-constructions.
    std::string label() const {
        return _gen == 0 ? _name : fmt::format("{}#{}", _name, _gen);
    }

    ~instrumented() noexcept(false) {
        const int uncaught = std::uncaught_exceptions();
        const bool has_current = std::current_exception() != nullptr;
        if (uncaught > 0) {
            ++detail::unwind_event_count;
        }
        event(
          "~instrumented()",
          fmt::format(
            "{}uncaught_exceptions={} current_exception={}",
            _moved_from ? "[moved-from] " : "",
            uncaught,
            has_current ? "non-null" : "null"));
        if (_mode == dtor_mode::throw_when_clear && !_moved_from) {
            if (uncaught == 0 && !has_current) {
                event("~instrumented()", "no exception visible here; throwing");
                throw std::runtime_error(
                  fmt::format("thrown by ~{}", label()));
            }
            event("~instrumented()", "exception in flight; standing down");
        }
    }

private:
    /// One transcript line in the shape "<label>.<member>()  <detail>",
    /// so every object line names both the object and the special member
    /// function that printed it.
    void event(std::string_view member, std::string_view detail) const {
        say("{:<30} {}", fmt::format("{}.{}", label(), member), detail);
    }

    std::string _name;
    dtor_mode _mode;
    int _gen = 0;
    bool _moved_from = false;
};

} // namespace demo
