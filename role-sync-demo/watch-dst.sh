#!/usr/bin/env bash
# Live view of SHADOW-cluster state (the SHADOW watch window).
# Sections and order are IDENTICAL to watch-src.sh so the two windows can be
# matched visually section-for-section, plus one shadow-only section at the
# bottom: the link's sync-task lines relevant to this demo. Each section is
# labeled with the rpk command that produces it (run against the SHADOW).
# Output is composed off-screen then painted, to keep refresh flicker low.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

section() { # section <color> <title command>
  printf '\n%s$ %s%s\n' "$1" "$2" "$SECTION_OFF"
}

while true; do
  frame=$(
    echo "=== SHADOW cluster · all commands against the shadow · 1s refresh ==="

    section "$SECTION_ACL" "rpk security acl list"
    dst_rpk security acl list 2>&1 || true

    section "$SECTION_ROLES" "rpk security role list"
    dst_rpk security role list 2>&1 || true

    section "$SECTION_MEMBERS" "rpk security role describe $ROLE    (membership)"
    # Real output either way: the PRINCIPALS block when the role exists,
    # rpk's own error when it doesn't.
    if out=$(dst_rpk security role describe "$ROLE" 2>&1); then
      printf '%s\n' "$out" | sed -n '/PRINCIPALS/,$p'
    else
      printf '%s\n' "$out"
    fi

    section "$SECTION_MSGS" "rpk topic consume $TOPIC --offset start   (last 5; 1s snapshot)"
    timeout 1 "$RPK" topic consume "$TOPIC" --offset start -f '%o: %v\n' \
      -X brokers="$DST_KAFKA" "${SASL_OPTS[@]}" 2>/dev/null | tail -5 || true

    echo
    echo "======================================================================"

    section "$SECTION_EXTRA" "rpk shadow status $LINK_NAME        (this demo's sync tasks + topics)"
    if out=$(dst_rpk shadow status "$LINK_NAME" 2>&1); then
      printf '%s\n' "$out" | grep -E "^NAME +BROKER-ID|Roles Migrator|Security Migrator"
      printf '%s\n' "$out" | sed -n '/^TOPICS/,/^$/p'
    else
      printf '%s\n' "$out"
    fi

    section "$SECTION_EXTRA" "rpk shadow describe $LINK_NAME --print-role   (the filter)"
    if out=$(dst_rpk shadow describe "$LINK_NAME" --print-role 2>&1); then
      printf '%s\n' "$out" | tail -n +3
    else
      printf '%s\n' "$out"
    fi
  )
  clear
  printf '%s\n' "$frame"
  sleep 1
done
