#!/usr/bin/env bash
# Record the fallback cast: one clean auto-paced demo run.
set -euo pipefail
KIT="$(cd "$(dirname "$0")" && pwd)"
"$KIT/reset.sh"
# 8s per step leaves room to narrate each beat over the playback.
asciinema rec --overwrite \
  -c "env AUTO_DELAY=8 $KIT/demo.sh --auto" \
  "$KIT/demo.cast"
echo "recorded $KIT/demo.cast — check with: asciinema play $KIT/demo.cast"
