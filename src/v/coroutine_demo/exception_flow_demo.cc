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

// Part 5 of the guided tour in README.md: where an exception LIVES on its
// way from a throw to the code that handles it. Through a chain of
// coroutines it alternates between data (an exception_ptr in a future) and
// a live throw (rethrown at each co_await), once per layer. Through a
// .then() chain, and through ss::coroutine::as_future, it stays data the
// whole way. The witness is demo::unwind_events(): every instrumented
// destruction that runs while an exception is unwinding counts one live
// throw passing through that frame.

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

/// The shared failure source: throws after a real suspension. Its own
/// local unwinds when the throw happens, so every scenario starts with
/// exactly one unwind event at the origin.
ss::future<> origin_fails() {
    demo::instrumented local("origin-local");
    co_await ss::sleep(std::chrono::milliseconds(1));
    demo::phase("the origin resumes and throws");
    throw std::runtime_error("boom");
}

/// A pure forwarding layer: co_await the level below and nothing else.
/// Each layer's local can only be destroyed by unwinding if the exception
/// was re-thrown as a live throw inside this layer's frame.
ss::future<> layer(int n) {
    demo::instrumented local(fmt::format("layer{}-local", n));
    if (n == 1) {
        co_await origin_fails();
    } else {
        co_await layer(n - 1);
    }
}

// One failure, three coroutine layers: the exception is stored as data in
// each layer's awaited future, then rethrown live at that layer's
// co_await. Four unwind events: the origin plus one per layer.
ss::future<> scenario_per_layer_rethrow() {
    demo::reset_unwind_events();
    demo::phase(
      "build four nested coroutines; they run eagerly down to the origin and suspend");
    auto f = co_await ss::coroutine::as_future(layer(3));
    demo::phase("what the caller saw");
    demo::say("caller: chain failed, received as data: failed={}", f.failed());
    f.ignore_ready_future();
    demo::say(
      "unwind events observed: {} (origin plus one per coroutine layer)",
      demo::unwind_events());
}

// The same failure through a .then() chain: every continuation is skipped
// and the exception moves between futures as data. One unwind event total,
// at the origin.
ss::future<> scenario_then_chain() {
    demo::reset_unwind_events();
    demo::phase(
      "the same failure through a .then() chain: continuations are skipped");
    co_await origin_fails()
      .then([] { demo::say("then-1: continuation ran"); })
      .then([] { demo::say("then-2: continuation ran"); })
      .then([] { demo::say("then-3: continuation ran"); })
      .then_wrapped([](ss::future<> f) {
          demo::say(
            "consumer: received failed={} as data; no continuation ran",
            f.failed());
          f.ignore_ready_future();
      });
    demo::say(
      "unwind events observed: {} (origin only; forwarding added none)",
      demo::unwind_events());
}

// The same failure consumed with ss::coroutine::as_future: the awaiting
// coroutine receives the failed future as a value, so nothing is rethrown
// in this frame and its local is destroyed on the normal path.
ss::future<> scenario_as_future() {
    demo::reset_unwind_events();
    demo::phase(
      "the same failure consumed as a value with ss::coroutine::as_future");
    demo::instrumented local("consumer-local");
    auto f = co_await ss::coroutine::as_future(origin_fails());
    demo::say(
      "consumer: received failed={} as data; nothing was rethrown here",
      f.failed());
    f.ignore_ready_future();
    demo::say(
      "unwind events observed: {} (origin only; this layer stayed on the value path)",
      demo::unwind_events());
}

struct scenario {
    std::string_view name;
    ss::future<> (*fn)();
};

constexpr std::array scenarios{
  scenario{"per-layer-rethrow", scenario_per_layer_rethrow},
  scenario{"then-chain", scenario_then_chain},
  scenario{"as-future", scenario_as_future},
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
    ss::sstring prog_name = "exception_flow_demo";
    std::array<char*, 1> args = {prog_name.data()};
    return app.run(args.size(), args.data(), [&scenario_name] {
        return run_scenario(scenario_name);
    });
}
