load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "user_proto",
    srcs = ["user.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
        "@googleapis//google/api:field_behavior_proto",
        "@googleapis//google/api:resource_proto",
        "@protobuf//:field_mask_proto",
    ],
)

redpanda_proto_library(
    name = "user_redpanda_proto",
    protos = [
        ":user_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde/protobuf:field_mask",
    ],
)

proto_library(
    name = "role_proto",
    srcs = ["role.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
        "@googleapis//google/api:field_behavior_proto",
        "@googleapis//google/api:resource_proto",
        "@protobuf//:field_mask_proto",
    ],
)

redpanda_proto_library(
    name = "role_redpanda_proto",
    protos = [
        ":role_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde/protobuf:field_mask",
    ],
)

proto_library(
    name = "oidc_proto",
    srcs = ["oidc.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
        "@googleapis//google/api:field_behavior_proto",
        "@googleapis//google/api:resource_proto",
        "@protobuf//:field_mask_proto",
        "@protobuf//:timestamp_proto",
    ],
)

redpanda_proto_library(
    name = "oidc_redpanda_proto",
    protos = [
        ":oidc_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde/protobuf:field_mask",
        "@abseil-cpp//absl/time:time",
    ],
)