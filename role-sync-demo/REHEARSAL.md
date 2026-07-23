# Role Sync TOI: Rehearsal Sheet

10:00 total. Target 8:30 of content, leaving about 1:30 for questions.
Beat 4 of the demo is the flex buffer if questions matter more; the bob
assign in beat 2 is the second cut candidate.

## Run of show

| # | Slide | Time | Cumulative |
|---|-------|------|------------|
| 1 | Title | 0:15 | 0:15 |
| 2 | The gap: inert ACLs | 0:55 | 1:10 |
| 3 | What role sync does | 1:05 | 2:15 |
| 4 | How it reads roles | 0:45 | 3:00 |
| 5 | Config: opting in | 0:40 | 3:40 |
| 6 | Demo divider, switch to terminal | 3:25 | 7:05 |
| 7 | Task states and triage | 0:55 | 8:00 |
| 8 | Boundaries and close | 0:30 | 8:30 |

## Talk track

**1. Title.** "v26.2 extends Shadow Linking to sync RBAC roles. I will cover
why, what it does, a quick demo, and how to troubleshoot it."

**2. The gap.** "In 25.3, Shadow Linking synced ACLs but deliberately not
roles or credentials. An ACL granted to RedpandaRole:analysts syncs fine, but
on the shadow it is inert: the role does not exist, so it grants nobody
anything. After failover, requests that worked on the source get denied. Role
sync closes exactly this gap."

**3. What role sync does.** "It is a full mirror within a filter scope:
create, update, delete. The key mental model: for in-scope roles the source
is authoritative. Add a member on the shadow and the next sync strips it.
Delete an in-scope role on the shadow and it comes back. Roles outside the
filter are invisible to the feature, so the filter is your blast radius
control. Three boundaries: Redpanda to Redpanda only, credentials are
deliberately not synced (pre-provision identities; SCRAM export was cut for
security reasons, happy to expand in Q&A), and it is opt-in: empty filters
sync nothing."

**4. How it reads roles.** "Roles are not part of the Kafka data model, so
there is nothing standard to read. We added a Redpanda-specific Kafka API,
DescribeRedpandaRoles, key 15000 in a reserved range upstream will never
allocate. Why a Kafka API and not the Admin API: the Admin API is not
guaranteed reachable across clusters and is not exposed in BYOC; the Kafka
listener is the surface the link already uses. Authorization rides cluster
DESCRIBE, the same gate as DescribeAcls, so the link principal needs zero new
permissions. Consequence: a Confluent or Apache Kafka source cannot answer
key 15000, so the role sync task parks LINK_UNAVAILABLE while the rest of the
link keeps replicating. If you are migrating from Confluent via Shadow Link,
plan to provision RBAC fresh on the destination; the parked task is expected
and harmless."

**5. Config.** "Per link, a role_sync_options block: interval, paused, and
name filters. Empty filters sync nothing. LITERAL star is the only wildcard;
under PREFIX, star is just a character. EXCLUDE beats INCLUDE. Configured via
rpk shadow on self-hosted; cloud links reject the block for now. The demo
uses a prefix filter instead of the wildcard, so you can see the scope
boundary work."

**6. Demo.** See beats below.

**7. Task states.** "One task status is the whole triage story, read from
GetShadowLink, the task named Roles Migrator Task. ACTIVE includes the
no-filters case. LINK_UNAVAILABLE is a recoverable park: RBAC off on the
shadow, source unreachable, missing DESCRIBE, or a non-Redpanda source; it
self-heals when the condition clears. FAULTED means the source returned an
error or an apply failed. PAUSED is only ever user-driven. Match on state;
the reason text is a hint, not a contract. There are no dedicated metrics
yet; this status is the observability surface."

**8. Close.** "Roles land, so role-bound ACLs actually work at failover.
Still out of scope: SCRAM by design, cloud rpk config, dedicated metrics,
serverless. Identities are still yours to provision. Details in the TOI doc
and ENG-898."

## Demo beats (2:40)

Three terminal windows. UPPER RIGHT runs `./watch-src.sh`, LOWER RIGHT runs
`./watch-dst.sh`. The two watches have IDENTICAL sections in the same order
(ACLs, roles, membership, topic messages), with matching heading colors
across the two windows (shadow-only sections are neutral white: colored =
has a twin), so they can be matched visually line for line; the shadow one adds two extra sections at the bottom: the
demo's sync task lines plus the TOPICS section ("No topics are being
shadowed" — the caption for the diverging message lists), and the role
sync filter (so "include prefix in-scope" is on screen while you narrate
the boundary). The SOURCE window's
ACL section also shows the link principal's cluster DESCRIBE grant —
slide 4's zero-new-permissions point, if anyone asks. It is not mirrored:
the demo's ACL sync filter is scoped to RedpandaRole:in-scope-role for a
clean shadow view (production DR would typically use a match-all filter). Every section is labeled with the rpk
command that produces it. LEFT runs `./demo.sh`: mutations only, each Enter
reveals and runs the next command.

1. **Meet alice: authorized here, denied there (0:50).** LEFT: produce to
   pageviews as alice against the SOURCE. "alice is a member of
   in-scope-role, and this ACL grants that role WRITE on pageviews — so
   this works." Then the exact same produce against the SHADOW: DENIED,
   TOPIC_AUTHORIZATION_FAILED on screen. "The shadow has the very same ACL
   — ACL sync mirrored it — but there is no role behind it: role sync is
   not configured (empty filter section, bottom of the shadow window).
   The ACL grants to nobody. This is slide 2's inert state — this is what
   failover would look like today."
2. **Enable role sync; alice's access follows (1:20).** LEFT: `cat` the
   six-line config being added (lib/role-sync-options.yaml): "This is the
   whole change: role_sync_options, include prefix in-scope, 5-second
   interval for demo pacing. rpk shadow update is editor-based; the EDITOR
   here is a small script that splices exactly that block in, so it is one
   command on stage." Run the update; the filter section flips to match
   the file, and the PRE-EXISTING role converges with alice within
   seconds. Then the SAME alice produce against the SHADOW — now it
   succeeds. "Nobody touched the role, nobody touched the ACL. Enabling
   sync made authorization real on the shadow. That is the whole feature."
   Then assign bob ON THE SOURCE: he appears in both membership sections
   within a sync interval. "And it keeps flowing: membership changes on
   the source sync through." Close by pointing at the message sections: "and notice the messages did
   NOT converge — role sync moved authorization, not data. Topic shadowing
   is its own opt-in, and it is off: 'No topics are being shadowed', right
   there in the shadow window."
3. **In scope vs out of scope, back to back (0:45).** LEFT: assign charlie
   ON THE SHADOW. He appears in the shadow window's membership — and never
   in the source window's — then the next sync strips him. "For in-scope
   roles the source wins." Then LEFT: create `out-scope-dst-role` ON THE
   SHADOW. "No in-scope prefix: sync never reads, changes, or deletes it.
   It just stays."
4. **Delete, in scope only (0:30).** LEFT: delete on the source. Both
   windows drop in-scope-role; out-scope-dst-role is still sitting in the
   shadow window untouched. "In scope: a full mirror. Out of scope:
   invisible." Note: rpk role delete also removes the role's ACLs, so the
   SOURCE window's ACL section empties too, while the shadow keeps its
   mirrored copy (ACL sync never deletes). If anyone notices, that is the
   answer; reset.sh restores the source ACL between runs. Skippable if
   running long; the close slide states it.

Reality note: in steady state the task reason reads "Roles Migrator Task has
started" rather than the per-sync counts. The migrator does produce a
"Synced roles: N created, N updated, N deleted, N failures" reason after
every cycle, but the task runner drops same-state transitions entirely
(task.cc change_state returns early when state is unchanged), so the counts
only surface when the state actually changes (for example recovering from
LINK_UNAVAILABLE to ACTIVE, or going FAULTED). The convergence proof on
stage is therefore the watch pane, not the reason text. Consistent with
"reason is a hint, not a contract".

## Session flow (the physical sequence)

Terminal setup: three ordinary terminal windows (VS Code terminal splits
work fine), all in the kit directory. LEFT: idle, ready for `./demo.sh`.
UPPER RIGHT: run `./watch-src.sh` (source state). LOWER RIGHT: run
`./watch-dst.sh` (shadow state). Start both watches before the talk and
leave them running.

1. Before: clusters ready (`./stage.sh status`), `./reset.sh`, terminals
   arranged as above, deck full screen on slide 1. Share the full screen.
2. Slides 1 to 5 in the browser; stop on the red Demo divider (slide 6).
3. Alt-Tab to the terminals. Run `./demo.sh` in the LEFT window; Enter
   through the beats, pointing at the RIGHT window for each convergence.
4. When it prints the closing "done" line, Alt-Tab back to the browser
   (still on the divider) and press the right arrow: slide 7, then slide 8.
   Leave slide 8 up during Q&A.
5. On any fallback trigger: in the LEFT window press Ctrl-C, then run
   `./fallback.sh` and narrate over the playback. Same return path after.
6. After: a completed run leaves state clean; `./reset.sh` before any
   re-run (no-op when clean, and the fix after a mid-demo Ctrl-C).
   `./stage.sh down` when done with the clusters.

## Fallback decision rule

Switch to the recording (Ctrl-C in the demo window, then `./fallback.sh`)
if ANY of:
- a wait exceeds about 20 seconds,
- either cluster is unhealthy (`./stage.sh status`),
- any command errors twice.

No debugging on stage. The cast is short; narrate over it.

## Pre-session checklist (morning of)

1. `./stage.sh down && ./stage.sh up` (fresh clusters, link ACTIVE)
2. `./reset.sh`
3. `AUTO_DELAY=0 ./demo.sh --auto` once; must end clean
4. `./reset.sh` again
5. Re-record the fallback at your presentation terminal size: `./record.sh`
6. Arrange the three terminal windows; start `./watch-src.sh` and
   `./watch-dst.sh` in the right-hand ones
7. Open `deck.html` full screen in the browser; check slide 1 renders
8. Silence notifications

## Q&A ammo

- **Why not SCRAM credentials?** At rest a SCRAM credential is verifier
  material: salt, stored key, server key, iterations. Exporting it enables
  offline dictionary attack and, with one eavesdropped exchange,
  impersonation (RFC 5802 section 9). A superuser password reset is noisy; an
  export is silent. Upstream Kafka stripped exactly this from KIP-554 in
  2020, rebuffed a 2021 re-request for the DR use case, and KIP-1061 /
  KAFKA-17063 has stalled since 2024 even with encrypted keys. CORE-16457,
  16458, 16459 are resolved Won't Do. Competitors (Confluent, MSK Replicator,
  MM2) all keep auth per-cluster.
- **Why key 15000?** Reserved Redpanda range in the Kafka protocol key space;
  upstream will never allocate there, so no collision with future Kafka APIs.
- **Interval?** Default 30s, configurable per link, no enforced floor (demo
  uses 5s).
- **Scale?** Fleet p99 is about 90 roles. Tested at 5,000 roles through a
  full create, update, delete lifecycle, plus 50x100 and 5x1000 membership
  shapes; fleet-scale syncs complete in seconds. Fetch is a single complete
  snapshot: deletes never fire on partial data.
- **Ordering vs ACL sync?** Independent tasks, no ordering dependency. A
  role-bound ACL becomes functional once both have converged.
- **Does ACL sync delete like role sync does?** No. The security migrator
  only creates and updates ACLs on the shadow (no deletion logic in
  security_migrator.cc; verified empirically). Role sync is the full mirror
  with deletes; ACL sync is additive. Deleting an ACL on the source leaves
  the shadow copy in place.
- **Deleting a role deletes its ACLs.** rpk role delete always cascades to
  the role's ACLs (DeleteAcls is hard-coded, the help text says so). On the
  shadow, the roles migrator deletes only the role, so the mirrored ACL
  survives there.
- **How do I enable role sync on an existing link?** `rpk shadow update`,
  which opens the link config in your editor; add role_sync_options and
  save (the demo scripts this with an EDITOR shim for pacing). Config can
  also be set at link creation, and the same path reverts it.
- **Cloud?** Control plane does not expose role_sync_options yet; rpk rejects
  it for cloud links. Serverless is out of scope for Shadow Linking.
- **Observability?** No dedicated metrics yet. SecuritySyncHealth was
  designed but deferred. GetShadowLink task status is the surface.
