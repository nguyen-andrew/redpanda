#!/usr/bin/env bash
# Return to the pre-demo state from ANY point (completed or aborted run):
# role sync unconfigured on the link, demo roles absent everywhere, staged
# ACL in place on both clusters.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
cd "$KIT_DIR"

# 1. Revert the link: remove role_sync_options (beat 2 re-adds it live).
EDITOR="$KIT_DIR/lib/editor-remove-role-sync.sh" \
  dst_rpk shadow update "$LINK_NAME" >/dev/null 2>&1 || true

# 2. Demo roles: with role sync unconfigured, deletions cannot race the
#    migrator, so delete directly on both sides.
src_rpk security role delete "$ROLE" --no-confirm >/dev/null 2>&1 || true
dst_rpk security role delete "$ROLE" --no-confirm >/dev/null 2>&1 || true
dst_rpk security role delete "$OUT_OF_SCOPE_DST_ROLE" --no-confirm >/dev/null 2>&1 || true

# 3. Restore the staged SOURCE state: the in-scope role (with members) and
#    its ACL. rpk role delete cascades to the role's ACLs, so the ACL must
#    be recreated too; ACL sync re-mirrors it to the shadow (create-only,
#    so no delete ever propagates). With role sync unconfigured (step 1),
#    the recreated role stays source-only, as staging intends.
src_rpk security role create "$ROLE" >/dev/null 2>&1 || true
src_rpk security role assign "$ROLE" --principal User:alice >/dev/null 2>&1 || true
src_rpk security acl create \
  --allow-principal "RedpandaRole:$ROLE" --operation write --topic pageviews \
  >/dev/null 2>&1 || true
t=0
until dst_rpk security acl list 2>/dev/null | grep -q "RedpandaRole:$ROLE"; do
  if [ "$t" -ge 25 ]; then echo "ACL not re-mirrored; run ./stage.sh provision"; exit 1; fi
  sleep 1; t=$((t + 1))
done

# 4. Empty the demo topic on both sides (messages accumulate across runs
#    and the watch windows display them). Delete+recreate is deterministic.
for side in src dst; do
  ${side}_rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  ${side}_rpk topic create "$TOPIC" -p 1 >/dev/null 2>&1 || true
  ${side}_rpk topic list 2>/dev/null | grep -q "$TOPIC" \
    || { echo "topic $TOPIC missing ($side) after reset"; exit 1; }
done

# 5. Verify the full pre-demo state: role sync off; role on the source
#    only; ACL on both; shadow clean of demo roles.
dst_rpk shadow describe "$LINK_NAME" --print-role 2>/dev/null | grep -q "in-scope" \
  && { echo "role sync still configured on the link"; exit 1; }
src_rpk security role list 2>/dev/null | grep -q "$ROLE" \
  || { echo "staged role missing on source; run ./stage.sh provision"; exit 1; }
dst_rpk security role list 2>/dev/null | grep -q "$ROLE" \
  && { echo "shadow still has $ROLE"; exit 1; }
for side in src dst; do
  ${side}_rpk security acl list 2>/dev/null | grep -q "RedpandaRole:$ROLE" \
    || { echo "staged ACL missing ($side); run ./stage.sh provision"; exit 1; }
done
dst_rpk security role list 2>/dev/null | grep -q "$OUT_OF_SCOPE_DST_ROLE" \
  && { echo "shadow still has $OUT_OF_SCOPE_DST_ROLE"; exit 1; }
echo "reset: role sync off, role staged on source only, ACL on both, shadow clean"
