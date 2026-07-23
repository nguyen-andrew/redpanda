#!/usr/bin/env bash
# Stage the role-sync TOI demo: build, start SOURCE + SHADOW, provision, link.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

# Generates a plain single-node config and launches the redpanda binary
# directly. Deliberately NOT tools/dev_cluster: it embeds cluster properties
# (incl. the developmental-features master switch) in redpanda.yaml, which
# crashes startup on current dev (see README "Known dev-branch issue").
gen_config() { # gen_config <name> <port_offset>
  local name=$1 offset=$2
  local dir="$RUN_DIR/$name"
  mkdir -p "$dir/data"
  # Minimal on purpose. Defaults cover the rest: advertised_* fall back to
  # the bound listeners (node_config.h), empty seed_servers is the default
  # and bootstraps a single-node cluster, and pandaproxy/schema_registry
  # services only start if their sections exist, so omitting them means no
  # extra listeners at all. developer_mode stays: it skips production host
  # checks, so the kit works on untuned machines.
  cat >"$dir/rpconfig.yaml" <<EOF
redpanda:
  developer_mode: true
  data_directory: $dir/data
  rpc_server:
    address: 127.0.0.1
    port: $((33145 + offset))
  kafka_api:
    - address: 127.0.0.1
      port: $((9092 + offset))
  admin:
    address: 127.0.0.1
    port: $((9644 + offset))
EOF
}

start_cluster() { # start_cluster <name> <port_offset>
  local name=$1 offset=$2
  rm -rf "$RUN_DIR/$name/data"
  gen_config "$name" "$offset"
  export UBSAN_OPTIONS="halt_on_error=0:suppressions=$REDPANDA_DIR/ubsan_suppressions.txt"
  RP_BOOTSTRAP_USER=admin:admin setsid nohup \
    "$REDPANDA_BIN" \
    --redpanda-cfg "$RUN_DIR/$name/rpconfig.yaml" \
    -c1 -m 1G --overprovisioned \
    >"$RUN_DIR/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/$name.pid"
}

provision() {
  echo "== enabling shadow linking + SASL (both clusters) =="
  for side in src dst; do
    ${side}_rpk cluster config set enable_shadow_linking true
    ${side}_rpk cluster config set superusers "['admin']"
    ${side}_rpk cluster config set enable_sasl true
  done
  echo "== provisioning identities (both clusters) =="
  for who in alice bob; do
    src_rpk security user create "$who" -p demo-pass --mechanism SCRAM-SHA-256 || true
    dst_rpk security user create "$who" -p demo-pass --mechanism SCRAM-SHA-256 || true
  done
  echo "== provisioning the link principal on the SOURCE (cluster DESCRIBE only) =="
  src_rpk security user create "$LINK_USER" -p "$LINK_PASS" --mechanism SCRAM-SHA-256 || true
  src_rpk security acl create \
    --allow-principal "User:$LINK_USER" --operation describe --cluster || true
  echo "== provisioning the demo topic (both clusters) =="
  src_rpk topic create "$TOPIC" -p 1 2>/dev/null || true
  dst_rpk topic create "$TOPIC" -p 1 2>/dev/null || true
  echo "== provisioning the in-scope role + demo ACL on the SOURCE =="
  # The role pre-exists on the source with alice as its only member: beat 2
  # shows it converge the moment role sync is enabled, then adds bob live to
  # show membership updates flowing. The ACL is mirrored by ACL sync.
  src_rpk security role create "$ROLE" 2>/dev/null || true
  src_rpk security role assign "$ROLE" --principal User:alice || true
  src_rpk security acl create \
    --allow-principal "RedpandaRole:$ROLE" --operation write --topic "$TOPIC" || true
  echo "== creating shadow link on the shadow cluster =="
  mkdir -p "$RUN_DIR"
  sed "s/SRC_KAFKA_ADDR/$SRC_KAFKA/" "$KIT_DIR/link.yaml" >"$RUN_DIR/link.gen.yaml"
  if dst_rpk shadow list 2>/dev/null | grep -q "$LINK_NAME"; then
    echo "link $LINK_NAME already exists, skipping create"
  else
    dst_rpk shadow create --config-file "$RUN_DIR/link.gen.yaml" --no-confirm \
      || die "shadow create failed"
  fi
  sleep 8   # one sync interval + slack
  link_role_status | grep -qi "active" || die "role sync task not active:
$(link_role_status)"
  echo "== waiting for ACL sync to mirror the demo ACL =="
  t=0
  until dst_rpk security acl list 2>/dev/null | grep -q "RedpandaRole:$ROLE"; do
    if [ "$t" -ge 30 ]; then die "ACL not mirrored to shadow after 30s"; fi
    sleep 1; t=$((t + 1))
  done
  echo "== staged: link up, ACL mirrored; role sync NOT configured (beat 2 enables it) =="
}

up() {
  for addr in "$SRC_ADMIN" "$DST_ADMIN"; do
    if curl -sf "http://$addr/v1/status/ready" >/dev/null 2>&1; then
      die "a demo cluster is already running ($addr); use stage.sh down first"
    fi
  done
  echo "== building (bazel) =="
  (cd "$REDPANDA_DIR" && bazel build //src/v/redpanda:redpanda //:rpk)
  echo "== starting clusters =="
  start_cluster source "$SRC_OFFSET"
  start_cluster shadow "$DST_OFFSET"
  wait_ready "$SRC_ADMIN" 120 || die "source not ready (see $RUN_DIR/source.log)"
  wait_ready "$DST_ADMIN" 120 || die "shadow not ready (see $RUN_DIR/shadow.log)"
  echo "== clusters ready =="
  provision
}

down() {
  for name in source shadow; do
    if [ -f "$RUN_DIR/$name.pid" ]; then
      kill -- -"$(cat "$RUN_DIR/$name.pid")" 2>/dev/null || true
      rm -f "$RUN_DIR/$name.pid"
    fi
  done
  echo "clusters stopped"
}

status() {
  for pair in "source:$SRC_ADMIN" "shadow:$DST_ADMIN"; do
    name=${pair%%:*}; addr=${pair#*:}
    if curl -sf "http://$addr/v1/status/ready" >/dev/null 2>&1; then
      echo "$name: ready ($addr)"
    else
      echo "$name: NOT ready ($addr)"
    fi
  done
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) status ;;
  provision) provision ;;
  *) die "usage: stage.sh [up|down|status|provision]" ;;
esac
