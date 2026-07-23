#!/usr/bin/env bash
# EDITOR shim for `rpk shadow update`: deletes the role_sync_options block
# from the config rpk hands us (used by reset to revert to the pre-demo,
# no-role-sync link state).
set -euo pipefail
tmpfile=$1
awk '
  /^role_sync_options:/ { skip = 1; next }
  skip && /^[^ ]/ { skip = 0 }
  !skip { print }
' "$tmpfile" > "$tmpfile.new"
mv "$tmpfile.new" "$tmpfile"
