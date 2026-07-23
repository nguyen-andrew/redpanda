#!/usr/bin/env bash
# The demo beats: ACTIONS only. All cluster state is on display in
# watch-src.sh / watch-dst.sh; the shadow-side commands here (beat 3 and
# the beat-2 link update) are deliberate. alice_src/alice_dst run with
# alice's credentials — her authorization comes only from the role.
# Enter-driven by default; --auto runs unattended (AUTO_DELAY seconds
# between steps) and asserts convergence, scoping, and authorization.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
cd "$KIT_DIR"

AUTO=false; [ "${1:-}" = "--auto" ] && AUTO=true
AUTO_DELAY="${AUTO_DELAY:-2}"
BOLD=$'\e[1m'; RED=$'\e[31m'; DIM=$'\e[2m'; OFF=$'\e[0m'

say()  { echo; echo "${DIM}# $*${OFF}"; }
step() { # step <command shown, then run (must succeed)>
  echo
  echo "${BOLD}${RED}\$${OFF}${BOLD} $*${OFF}"
  if $AUTO; then sleep "$AUTO_DELAY"; else read -r; fi
  eval "$@"
}
step_fail() { # step_fail <command shown, then run — DENIAL is the point>
  echo
  echo "${BOLD}${RED}\$${OFF}${BOLD} $*${OFF}"
  if $AUTO; then sleep "$AUTO_DELAY"; else read -r; fi
  if eval "$@"; then
    if $AUTO; then echo "CHECK FAILED: expected denial, but it succeeded"; exit 1; fi
  else
    if $AUTO; then echo "${DIM}(check ok: denied as expected)${OFF}"; fi
  fi
  true
}
wait_for() { # wait_for <timeout_s> <success predicate...>
  local timeout=$1; shift
  local t=0
  until eval "$@" >/dev/null 2>&1; do
    if [ "$t" -ge "$timeout" ]; then echo "TIMEOUT waiting for: $*"; return 1; fi
    sleep 1; t=$((t + 1))
  done
  echo "${DIM}(converged in ${t}s)${OFF}"
}
check() { # check <description> <predicate...> — auto-mode assertions
  $AUTO || return 0
  local desc=$1; shift
  if eval "$@" >/dev/null 2>&1; then
    echo "${DIM}(check ok: ${desc})${OFF}"
  else
    echo "CHECK FAILED: $desc"; exit 1
  fi
}

say "BEAT 1 — meet alice. On the SOURCE she is a member of '$ROLE', and the"
say "ACL authorizes that role to write to '$TOPIC'. Acting as alice:"
step "echo 'page-view-1' | alice_src topic produce $TOPIC"
say "Works. The SHADOW already has that ACL — mirrored by ACL sync — but NO"
say "role behind it: role sync is NOT configured (filter section, bottom of"
say "the SHADOW window). The exact same action against the SHADOW:"
step_fail "echo 'page-view-2' | alice_dst topic produce $TOPIC"
say "Denied. The mirrored ACL grants to a role that doesn't exist here —"
say "the inert state from slide 2. This is what failover would look like."
check "role staged on source" "src_rpk security role list | grep -q $ROLE"
check "alice is the staged member" "src_rpk security role describe $ROLE | grep -q alice"
check "bob not a member yet" "! src_rpk security role describe $ROLE | grep -q bob"
check "role absent on shadow" "! dst_rpk security role list | grep -q $ROLE"
check "ACL present on source" "src_rpk security acl list | grep -q RedpandaRole:$ROLE"
check "ACL mirrored to shadow" "dst_rpk security acl list | grep -q RedpandaRole:$ROLE"
check "role sync not configured yet" "! dst_rpk shadow describe $LINK_NAME --print-role | grep -q in-scope"
if ! $AUTO; then echo "${DIM}(press Enter to start beat 2)${OFF}"; read -r; fi

say "BEAT 2 — role sync is opt-in. This is the config we're adding to the link:"
step "cat lib/role-sync-options.yaml"
say "(rpk shadow update is editor-based; EDITOR here is a script that"
say "splices that block into the link config — one command on stage.)"
step "EDITOR=lib/editor-add-role-sync.sh dst_rpk shadow update $LINK_NAME"
wait_for 25 "dst_rpk security role list | grep -q $ROLE"
say "...the PRE-EXISTING role converged, alice's membership included,"
say "without anyone touching the role. Same alice action on the SHADOW:"
step "echo 'page-view-3' | alice_dst topic produce $TOPIC"
say "Authorized. The mirrored ACL finally has a role behind it — exactly"
say "what a failover needs. And it keeps flowing: membership changes on"
say "the source sync too."
step "src_rpk security role assign $ROLE --principal User:bob"
wait_for 20 "dst_rpk security role describe $ROLE | grep -q bob"
say "...bob appears in BOTH membership sections. One more look at the"
say "message sections: they did"
say "NOT converge. Role sync moved authorization, not data — topic"
say "shadowing is its own opt-in, and it's off ('No topics are being"
say "shadowed', SHADOW window)."

say "BEAT 3 — the gotcha: edit an IN-SCOPE role ON THE SHADOW; the source wins"
step "dst_rpk security role assign $ROLE --principal User:charlie"
wait_for 20 "! dst_rpk security role describe $ROLE | grep -q charlie"
say "...charlie stripped. Now the contrast: create an OUT-OF-SCOPE role on"
say "the shadow — no in-scope prefix, so sync never reads, changes, or"
say "deletes it. It just stays."
step "dst_rpk security role create $OUT_OF_SCOPE_DST_ROLE"
check "out-of-scope shadow role present" "dst_rpk security role list | grep -q $OUT_OF_SCOPE_DST_ROLE"

say "BEAT 4 — full mirror IN SCOPE: delete on the source; '$ROLE' vanishes"
say "from the shadow — while '$OUT_OF_SCOPE_DST_ROLE' is never touched."
step "src_rpk security role delete $ROLE --no-confirm"
wait_for 20 "! dst_rpk security role list | grep -q '^$ROLE\$'"
check "out-of-scope shadow role survived" "dst_rpk security role list | grep -q $OUT_OF_SCOPE_DST_ROLE"

say "done — opt-in; in scope, a full mirror that makes authorization real"
say "at failover; out of scope, invisible."
