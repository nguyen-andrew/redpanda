#!/usr/bin/env bash
# Shared config for the role-sync TOI demo kit.
set -euo pipefail

REDPANDA_DIR="${REDPANDA_DIR:-$HOME/workspace/redpanda}"
KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$KIT_DIR/.run"

RPK="${RPK:-$REDPANDA_DIR/bazel-bin/src/go/rpk/cmd/rpk/rpk_/rpk}"
REDPANDA_BIN="$REDPANDA_DIR/bazel-bin/src/v/redpanda/redpanda"

# Port offsets from redpanda defaults (kafka 9092, admin 9644, sr 8081, ...).
# Change these two to move ALL demo ports; everything else derives from them.
# 20000/30000 avoid the host's Confluent test stack on 9092/8081.
SRC_OFFSET="${SRC_OFFSET:-20000}"
DST_OFFSET="${DST_OFFSET:-30000}"
SRC_KAFKA=127.0.0.1:$((9092 + SRC_OFFSET)); SRC_ADMIN=127.0.0.1:$((9644 + SRC_OFFSET))
DST_KAFKA=127.0.0.1:$((9092 + DST_OFFSET)); DST_ADMIN=127.0.0.1:$((9644 + DST_OFFSET))
LINK_NAME=demo-link
# The role filter is "include prefix in-scope": ROLE syncs; the out-of-scope
# role is created on the shadow during the demo, and sync never touches it.
ROLE=in-scope-role
OUT_OF_SCOPE_DST_ROLE=out-scope-dst-role

TOPIC=pageviews
LINK_USER=link-user
LINK_PASS=link-pass
SASL_OPTS=(-X user=admin -X pass=admin -X sasl.mechanism=SCRAM-SHA-256)
ALICE_OPTS=(-X user=alice -X pass=demo-pass -X sasl.mechanism=SCRAM-SHA-256)

# Section heading colors, shared by watch-src.sh and watch-dst.sh so the
# same section carries the same color in both windows. Shadow-only sections
# use SECTION_EXTRA: a colored heading means "has a twin in the other window".
# True-black text on fixed-RGB 256-palette backgrounds: terminal themes
# remap the base 16 ANSI colors (making some chips dark), but the 256-color
# cube renders the same bright hues everywhere.
SECTION_ACL=$'\e[38;5;16;48;5;220m'     # black on gold
SECTION_ROLES=$'\e[38;5;16;48;5;51m'    # black on bright cyan
SECTION_MEMBERS=$'\e[38;5;16;48;5;213m' # black on light magenta
SECTION_MSGS=$'\e[38;5;16;48;5;118m'    # black on bright green
SECTION_EXTRA=$'\e[38;5;16;48;5;253m'   # black on light gray: shadow-only, no twin
SECTION_OFF=$'\e[0m'

die() { echo "FATAL: $*" >&2; exit 1; }

# Admin-credentialed helpers (SASL is enabled on both clusters).
src_rpk() { "$RPK" "$@" -X brokers="$SRC_KAFKA" -X admin.hosts="$SRC_ADMIN" "${SASL_OPTS[@]}"; }
dst_rpk() { "$RPK" "$@" -X brokers="$DST_KAFKA" -X admin.hosts="$DST_ADMIN" "${SASL_OPTS[@]}"; }

# Act as alice: her authorization comes only from RedpandaRole:$ROLE.
alice_src() { "$RPK" "$@" -X brokers="$SRC_KAFKA" -X admin.hosts="$SRC_ADMIN" "${ALICE_OPTS[@]}"; }
alice_dst() { "$RPK" "$@" -X brokers="$DST_KAFKA" -X admin.hosts="$DST_ADMIN" "${ALICE_OPTS[@]}"; }

wait_ready() { # wait_ready <admin_addr> <timeout_s>
  local addr=$1 timeout=$2 t=0
  # NB: an until-loop's exit status is its last BODY command, so the timeout
  # check must be an if (status 0 when not yet timed out), not `[ ] && return`.
  until curl -sf "http://$addr/v1/status/ready" >/dev/null 2>&1; do
    if [ "$t" -ge "$timeout" ]; then return 1; fi
    sleep 1; t=$((t + 1))
  done
  return 0
}

link_role_status() { # one line: Roles Migrator Task state + reason
  dst_rpk shadow status "$LINK_NAME" 2>&1 | grep "Roles Migrator" || true
}
