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

// Parts 1 through 4 of the guided tour in README.md: what a coroutine
// frame is, the three exception-introspection windows a destructor can
// run in, where a coroutine's locals and by-value parameters land among
// those windows, and why a destructor that throws "when nothing is in
// flight" is recoverable as a local but fatal as a parameter. The
// scenarios below appear in the same order the README walks them. Run
// with --scenario, or run everything via run_all.sh.

#include "base/seastarx.h"
#include "instrumented.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp_options.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/log.hh>

#include <boost/program_options.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

// Part 1: suspending destroys nothing. The caller runs while the
// coroutine is suspended, and the local's destructor fires only when the
// body finishes and the local leaves scope.
ss::future<> coro_holds_local_across_suspension() {
    demo::instrumented local("local");
    demo::say("coro: suspending; the local stays alive");
    co_await ss::sleep(std::chrono::milliseconds(1));
    demo::say("coro: resumed; the local is still alive");
}

ss::future<> scenario_suspension_not_scope_exit() {
    demo::phase("call the coroutine; it runs eagerly until its first suspension");
    auto fut = coro_holds_local_across_suspension();
    demo::phase("suspended: the caller runs while the local stays alive");
    demo::say("caller: I have control and no destructor has run");
    co_await std::move(fut);
    demo::say("caller: coroutine finished");
}

// Part 1: a by-value parameter is copied into the frame (three objects
// total: the caller's named original, the parameter object itself, and
// the frame copy the body uses). A coroutine that never suspends runs
// its whole life, frame teardown included, inside the call itself: the
// frame copy is destroyed before the caller even receives the future.
ss::future<> coro_completes_without_suspending(demo::instrumented param) {
    param.note_alive();
    demo::say("coro: completing without ever suspending");
    co_return;
}

ss::future<> scenario_no_suspension() {
    demo::phase(
      "call a coroutine that never suspends: its whole life happens inside the call");
    demo::instrumented arg("param");
    auto fut = coro_completes_without_suspending(std::move(arg));
    demo::phase("back in the caller: the frame and its copy are already gone");
    co_await std::move(fut);
}

// Part 2: no coroutines. The three exception contexts a destructor can
// run in, plus the baseline that makes window 3 the blind one: it reads
// exactly like "no exception ever happened".
ss::future<> scenario_windows() {
    demo::phase("baseline: plain scope exit, no exception anywhere");
    {
        demo::instrumented b("baseline-local");
    }
    demo::phase("window 1: destroyed while an exception unwinds the scope");
    try {
        demo::instrumented x("w1-local");
        throw std::runtime_error("boom");
    } catch (const std::exception&) {
        demo::phase("window 2: destroyed inside the catch handler");
        {
            demo::instrumented y("w2-local");
        }
    }
    demo::phase("window 3: destroyed after the handler exited");
    {
        demo::instrumented z("w3-local");
    }
    demo::say("note: window 3 reads exactly like the baseline");
    co_return;
}

/// A failure that arrives only after the awaiting coroutine has really
/// suspended, so the resume runs on the reactor as a task, not inside
/// the original call.
ss::future<> fail_after_real_suspension() {
    co_await ss::sleep(std::chrono::milliseconds(1));
    demo::phase("the failure arrives after a real suspension");
    demo::say("helper: resumed; throwing");
    throw std::runtime_error("boom");
}

// Part 3: one coroutine holding the same type as a local and as a
// by-value parameter, failing after a real suspension. The local is
// destroyed during unwinding (window 1); the frame copy of the parameter
// is destroyed by frame teardown (window 3), after the implicit
// catch-all already stored the exception into the result future.
ss::future<> coro_local_vs_param(demo::instrumented param) {
    param.note_alive();
    demo::instrumented local("local");
    demo::say("coro: suspending on a future that will fail");
    co_await fail_after_real_suspension();
    demo::say("coro: never reached");
}

ss::future<> scenario_local_vs_param() {
    demo::phase("the caller builds the argument and calls the coroutine");
    demo::instrumented arg("param");
    auto f = co_await ss::coroutine::as_future(
      coro_local_vs_param(std::move(arg)));
    demo::phase("the caller finds out");
    demo::say("caller: coroutine failed, received as data: failed={}", f.failed());
    f.ignore_ready_future();
    demo::say("caller: the local unwound; the param was destroyed in the blind window");
}

// Part 4: the throwing destructor as a coroutine LOCAL. Scope exit
// happens inside the implicit catch-all's territory, so the throw
// becomes an ordinary failed future and the process is fine.
ss::future<> coro_throwing_dtor_local() {
    co_await ss::sleep(std::chrono::milliseconds(1));
    demo::instrumented local("local", demo::dtor_mode::throw_when_clear);
    demo::say("coro: local goes out of scope now, with nothing in flight");
}

ss::future<> scenario_throwing_dtor_local() {
    demo::phase(
      "a guard object as a coroutine LOCAL: its throw becomes a failed future");
    try {
        co_await coro_throwing_dtor_local();
    } catch (const std::exception& e) {
        demo::say("caller: caught the dtor throw as a failed future: {}", e.what());
    }
    demo::say("caller: the process is fine");
}

// Part 4: the same throwing destructor as a by-value PARAMETER, with the
// body failing after a real suspension. Teardown runs below the implicit
// catch-all (the stored exception is invisible to introspection) and the
// dtor's throw escapes into the reactor's noexcept task boundary:
// std::terminate, SIGABRT, exit code 134.
ss::future<> coro_throwing_dtor_param(demo::instrumented param) {
    param.note_alive();
    demo::say("coro: suspending on a future that will fail");
    co_await fail_after_real_suspension();
    demo::say("coro: never reached");
}

ss::future<> scenario_throwing_dtor_param() {
    demo::phase(
      "a guard object as a by-value PARAMETER: expect std::terminate");
    demo::instrumented arg("param", demo::dtor_mode::throw_when_clear);
    co_await coro_throwing_dtor_param(std::move(arg));
    demo::say("caller: never reached");
}

// Part 4, the boundary of the fatal shape: if the body throws BEFORE any
// suspension, teardown (and the dtor's throw) happens inside the
// original call, where a plain try/catch can still absorb it. The
// guaranteed-fatal version needs a real suspension first.
ss::future<> coro_throws_before_suspending(demo::instrumented param) {
    param.note_alive();
    demo::say("coro: throwing before any suspension");
    throw std::runtime_error("boom");
    co_return;
}

ss::future<> scenario_throwing_dtor_param_no_suspension() {
    demo::phase(
      "the same guard parameter, but the body throws before any suspension");
    try {
        demo::instrumented arg("param", demo::dtor_mode::throw_when_clear);
        auto fut = coro_throws_before_suspending(std::move(arg));
        demo::say("caller: never reached");
        co_await std::move(fut);
    } catch (const std::exception& e) {
        demo::say("caller: caught during the call itself: {}", e.what());
    }
    demo::say("caller: a plain try/catch absorbed it; the process is fine");
}

struct scenario {
    std::string_view name;
    ss::future<> (*fn)();
};

constexpr std::array scenarios{
  scenario{"suspension-not-scope-exit", scenario_suspension_not_scope_exit},
  scenario{"no-suspension", scenario_no_suspension},
  scenario{"windows", scenario_windows},
  scenario{"local-vs-param", scenario_local_vs_param},
  scenario{"throwing-dtor-local", scenario_throwing_dtor_local},
  scenario{"throwing-dtor-param", scenario_throwing_dtor_param},
  scenario{
    "throwing-dtor-param-no-suspension",
    scenario_throwing_dtor_param_no_suspension},
};

ss::future<int> run_scenario(std::string name) {
    const auto* it = std::ranges::find(scenarios, name, &scenario::name);
    if (it == scenarios.end()) {
        demo::say("unknown scenario '{}'", name);
        co_return 1;
    }
    co_await it->fn();
    co_return 0;
}

} // namespace

int main(int ac, char** av) {
    std::string scenario_name;
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help", "list scenarios")(
      "scenario", po::value<std::string>(&scenario_name), "scenario to run");
    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);
    if (vm.count("help") != 0 || scenario_name.empty()) {
        std::cout << desc << "scenarios:\n";
        for (const auto& s : scenarios) {
            std::cout << "  " << s.name << "\n";
        }
        return 1;
    }

    ss::app_template::seastar_options sscfg;
    sscfg.smp_opts.smp.set_value(1);
    sscfg.smp_opts.memory_allocator = ss::memory_allocator::standard;
    sscfg.reactor_opts.overprovisioned.set_value();
    sscfg.log_opts.default_log_level.set_value(ss::log_level::warn);
    ss::app_template app(std::move(sscfg));
    ss::sstring prog_name = "lifetime_windows_demo";
    std::array<char*, 1> args = {prog_name.data()};
    return app.run(args.size(), args.data(), [&scenario_name] {
        return run_scenario(scenario_name);
    });
}
