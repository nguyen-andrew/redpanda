#!/usr/bin/env bash
# EDITOR shim for `rpk shadow update`: replaces the role_sync_options block
# in the config rpk hands us with lib/role-sync-options.yaml (the live
# "enable role sync" demo step shows that file right before running this).
set -euo pipefail
tmpfile=$1
here="$(cd "$(dirname "$0")" && pwd)"
awk '
  /^role_sync_options:/ { skip = 1; next }
  skip && /^[^ ]/ { skip = 0 }
  !skip { print }
' "$tmpfile" > "$tmpfile.new"
cat "$here/role-sync-options.yaml" >>"$tmpfile.new"
mv "$tmpfile.new" "$tmpfile"
