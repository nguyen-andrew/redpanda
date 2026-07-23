# Role Sync TOI demo kit

Presentation and live demo for the v26.2 Shadow Linking Role Sync TOI
session: an 8-slide HTML deck plus a scripted two-cluster demo with a
recorded fallback. See `REHEARSAL.md` for the talk track, beats, and the
pre-session checklist.

## Layout

- `deck.html` - the slide deck (self-contained; open full screen in a
  browser; navigate with arrow keys). Built from `deck/deck.src.html` +
  `deck/assets/` by `python3 deck/build.py`. Never edit `deck.html` directly.
- `stage.sh up|down|status|provision` - build binaries, run both clusters,
  provision users/ACLs, create the shadow link. `up` is the full path;
  `provision` re-runs just the provisioning against live clusters
  (idempotent).
- `watch-src.sh` / `watch-dst.sh` - live views of source and shadow state
  with identical section layouts (ACLs, roles, membership, topic messages),
  color-coordinated per section across the two windows (shared palette in
  lib/common.sh; a colored heading means it has a twin, shadow-only
  sections are neutral white); the shadow one adds the link's two
  relevant sync-task lines, the TOPICS section (shows "No topics are being
  shadowed"), and the role sync filter at the bottom. reset.sh empties the
  demo topic on both sides so message lists start clean each run. Each section is labeled with the rpk command that
  produces it; run them in two terminal windows next to the demo window.
- `demo.sh [--auto]` - four beats: produce as alice (authorized on the
  source, denied on the shadow), enable role sync live (beat 2, via
  `rpk shadow update` with the `lib/editor-add-role-sync.sh` EDITOR shim —
  the staged role converges, the same alice produce now succeeds, and a
  live bob assignment on the source syncs through), the authority gotcha +
  out-of-scope role, and the source-side delete.
  Default: each Enter reveals and runs the next command. `--auto` runs
  unattended with `AUTO_DELAY` (default 2s) pacing and asserts convergence,
  scoping, and the deny/allow flip; used for rehearsal checks and
  recording. `reset.sh` reverts the link with the matching remove shim.
- `fallback.sh` - plays the recorded demo. If the live demo misbehaves,
  Ctrl-C out of demo.sh and run this in the same window.
- `reset.sh` - returns to the pre-demo state (role absent everywhere).
- `record.sh` - records the fallback cast (`demo.cast`) from a clean state
  at `AUTO_DELAY=3`. Re-record at your presentation terminal size; casts
  replay at the recorded dimensions.
- `lib/common.sh` - ports, paths, `src_rpk`/`dst_rpk` helpers.
- `link.yaml` - the shadow link config (role sync: 5s interval, wildcard
  include filter).

## Topology

Two single-node clusters from this checkout's dev build. `stage.sh`
generates a plain node config per cluster (`.run/<name>/rpconfig.yaml`, all
listeners on 127.0.0.1) and launches `bazel-bin/src/v/redpanda/redpanda`
directly. Deliberately not `tools/dev_cluster`: see the known dev-branch
issue below. Cluster-level settings (`enable_shadow_linking`) are applied
via `rpk cluster config set` at provision time, never embedded in the node
config.

- SOURCE: kafka 127.0.0.1:29092, admin 29644 (port offset 20000)
- SHADOW: kafka 127.0.0.1:39092, admin 39644 (port offset 30000)

Offsets avoid the Confluent test stack on this host (9092/8081). To move
all demo ports, change `SRC_OFFSET`/`DST_OFFSET` in `lib/common.sh` (or set
them as env vars); every port, and the link bootstrap address (generated
into `.run/link.gen.yaml` at provision time), derives from those two. Keep
offsets at or below about 32000: the rpc base port is 33145 and ports past
65535 fail node config validation. Data,
logs, and pidfiles live under `.run/`. `REDPANDA_DIR` (default
`~/workspace/redpanda`) and `RPK` are overridable via env.

## Prerequisites

- This repo checkout with bazel working (`//src/v/redpanda:redpanda`, `//:rpk`)
- `asciinema` (`sudo apt-get install -y asciinema`) for the fallback
  recording and playback
- Three terminal windows (VS Code split terminals work fine): demo,
  source watch, shadow watch

## Known dev-branch issue (re-verified on c45abdf3f0, 2026-07-21)

Cluster starts crash at startup with an assert in `base_property.cc` when
the node's redpanda.yaml embeds the developmental-features master switch
(`enable_developmental_unrecoverable_data_corrupting_features`), which
`tools/dev_cluster.py` writes unconditionally. Validating that assignment in
`config_manager::preload -> load_legacy` reads the property's current value
("cannot be changed once enabled") before the config store is ready
(assert introduced by 029d793085, "add usable_before_ready flag"). Plain
node configs without embedded cluster properties, like start-rp-local.sh's
rpconfig.yaml, are unaffected; so are CI/ducktape clusters.

**This kit is not affected**: it generates plain node configs and launches
the redpanda binary directly, precisely to avoid this bug, and runs on
unpatched dev. The one-line fix that unblocks dev_cluster (mark the master
switch `usable_before_ready::yes`) is kept here as
`dev-assert-workaround.patch` for reference until it is fixed upstream.

## Simplifications vs production

- SASL/SCRAM is ON for both clusters (no TLS): admin is the superuser, the
  kit's rpk helpers carry admin credentials, and the link authenticates as
  `link-user`, whose only grant is cluster DESCRIBE — the production story.
  alice acts with her own credentials; her access comes only from the role
  (beats 1-2 produce to the `pageviews` topic, staged on both clusters).
- ACL sync is enabled, scoped to the demo role's principal so the shadow's
  ACL list stays clean (production DR would typically mirror all ACLs); the
  demo ACL is created on the SOURCE at staging time and mirrored by the
  Security Migrator before the session (for demo pacing; note ACL sync
  creates/updates only, it never deletes on the shadow). Role sync is scoped to
  `include prefix in-scope`. `in-scope-role` (member: alice) is staged on
  the source so beat 2 shows it converge the moment sync is enabled, then
  bob is assigned live to show membership updates flowing;
  `out-scope-dst-role` (beat 3, on the shadow) is created live to demo the
  filter boundary.
- Sync intervals are 5s for demo pacing; the default is 30s.
- Single-node clusters, 1 core, 1G each.
- `enable_shadow_linking` is set to true on both clusters by `stage.sh`.
