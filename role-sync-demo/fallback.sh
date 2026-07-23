#!/usr/bin/env bash
# Play the recorded demo. Use when the live demo misbehaves:
# Ctrl-C out of demo.sh, then run this in the same window and narrate over it.
# Touches nothing on the clusters.
set -euo pipefail
KIT="$(cd "$(dirname "$0")" && pwd)"
exec asciinema play "$KIT/demo.cast"
