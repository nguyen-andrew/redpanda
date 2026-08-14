load("//bazel:build.bzl", "redpanda_cc_binary")

redpanda_cc_binary(
    name = "lifetime_windows_demo",
    testonly = True,
    srcs = [
        "instrumented.h",
        "lifetime_windows_demo.cc",
    ],
    deps = [
        "//src/v/base",
        "@boost//:program_options",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_binary(
    name = "exception_flow_demo",
    testonly = True,
    srcs = [
        "exception_flow_demo.cc",
        "instrumented.h",
    ],
    deps = [
        "//src/v/base",
        "@boost//:program_options",
        "@fmt",
        "@seastar",
    ],
)
