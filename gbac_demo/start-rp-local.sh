#!/bin/bash

mkdir -p ~/workspace/redpanda/gbac_demo/data

# Suppress known UBSan false positives (e.g. OpenSSL function pointer casts).
export UBSAN_OPTIONS="halt_on_error=0:suppressions=$HOME/workspace/redpanda/ubsan_suppressions.txt"

# Pass --debug to apply --config=debugger (builds with debug symbols for lldb).
BAZEL_CONFIG=()
if [[ "$1" == "--debug" ]]; then
  BAZEL_CONFIG=(--config=debugger)
fi

# Run the bazel command
# bazel run //src/v/redpanda -- \
RP_BOOTSTRAP_USER=admin:admin bazel run "${BAZEL_CONFIG[@]}" //src/v/redpanda -- \
  -c1 \
  -m 2G \
  --redpanda-cfg ~/workspace/redpanda/gbac_demo/rpconfig.yaml
# bazel run --config=debug //src/v/redpanda -- \
#   -c1 \
#   -m 2G \
#   --redpanda-cfg ~/workspace/redpanda/local_rp/rpconfig.yaml