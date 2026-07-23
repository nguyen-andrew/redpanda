#!/usr/bin/env bash
# Live view of SOURCE-cluster state (the SOURCE watch window).
# Sections and order are IDENTICAL to watch-dst.sh so the two windows can be
# matched visually section-for-section; each is labeled with the rpk command
# that produces it (run against the SOURCE). Output is composed off-screen
# then painted, to keep refresh flicker low.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

section() { # section <color> <title command>
  printf '\n%s$ %s%s\n' "$1" "$2" "$SECTION_OFF"
}

while true; do
  frame=$(
    echo "=== SOURCE cluster · all commands against the source · 1s refresh ==="

    section "$SECTION_ACL" "rpk security acl list"
    src_rpk security acl list 2>&1 || true

    section "$SECTION_ROLES" "rpk security role list"
    src_rpk security role list 2>&1 || true

    section "$SECTION_MEMBERS" "rpk security role describe $ROLE    (membership)"
    # Real output either way: the PRINCIPALS block when the role exists,
    # rpk's own error when it doesn't.
    if out=$(src_rpk security role describe "$ROLE" 2>&1); then
      printf '%s\n' "$out" | sed -n '/PRINCIPALS/,$p'
    else
      printf '%s\n' "$out"
    fi

    section "$SECTION_MSGS" "rpk topic consume $TOPIC --offset start   (last 5; 1s snapshot)"
    timeout 1 "$RPK" topic consume "$TOPIC" --offset start -f '%o: %v\n' \
      -X brokers="$SRC_KAFKA" "${SASL_OPTS[@]}" 2>/dev/null | tail -5 || true

    echo
    echo "======================================================================"
  )
  clear
  printf '%s\n' "$frame"
  sleep 1
done
