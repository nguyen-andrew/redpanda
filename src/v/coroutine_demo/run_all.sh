#!/usr/bin/env bash
# Builds both demo binaries, runs every scenario, and checks two things per
# scenario: the exit code, and the transcript (the lines starting with
# '| ') against the committed golden file. The goldens are the verified
# expected transcripts; the README tells the story of how they were
# predicted first and where the prediction had to be corrected.
set -uo pipefail

cd "$(dirname "$0")"

echo "building demo binaries..."
if ! bazel build //src/v/coroutine_demo:lifetime_windows_demo \
        //src/v/coroutine_demo:exception_flow_demo; then
    echo "build failed" >&2
    exit 1
fi
bindir="$(bazel info bazel-bin)/src/v/coroutine_demo"

# The terminate scenario aborts on purpose; don't write a core file.
ulimit -c 0

pass=0
fail=0

run_one() {
    local binary="$1" scenario="$2" expected_exit="$3"
    local out actual_exit transcript diff_out problems=""
    out="$("${bindir}/${binary}" --scenario "${scenario}" 2>&1)"
    actual_exit=$?
    transcript="$(grep '^|' <<<"${out}" || true)"
    if [[ "${actual_exit}" -ne "${expected_exit}" ]]; then
        problems+="  exit code: expected ${expected_exit}, got ${actual_exit}"$'\n'
    fi
    if ! diff_out="$(diff -u "golden/${scenario}.txt" <(printf '%s\n' "${transcript}"))"; then
        problems+="  transcript differs from golden/${scenario}.txt:"$'\n'"${diff_out}"$'\n'
    fi
    if [[ -z "${problems}" ]]; then
        echo "PASS  ${scenario} (exit ${actual_exit})"
        ((pass += 1))
    else
        echo "FAIL  ${scenario}"
        printf '%s' "${problems}"
        ((fail += 1))
    fi
}

run_one lifetime_windows_demo suspension-not-scope-exit 0
run_one lifetime_windows_demo no-suspension 0
run_one lifetime_windows_demo windows 0
run_one lifetime_windows_demo local-vs-param 0
run_one lifetime_windows_demo throwing-dtor-local 0
run_one lifetime_windows_demo throwing-dtor-param 134
run_one lifetime_windows_demo throwing-dtor-param-no-suspension 0
run_one exception_flow_demo per-layer-rethrow 0
run_one exception_flow_demo then-chain 0
run_one exception_flow_demo as-future 0

echo
echo "${pass} passed, ${fail} failed"
[[ "${fail}" -eq 0 ]]
