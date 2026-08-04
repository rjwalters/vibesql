#!/usr/bin/env bash
# test-merge-pr-rate-limit-fallback.sh - Unit tests for the GraphQL-layer
# unavailability (rate-limit / NWO-resolution) fallback in merge-pr.sh (#4447).
#
# GitHub's `enablePullRequestAutoMerge` mutation is GraphQL-only. When the
# shared credential's GraphQL quota is exhausted, the mutation attempt fails
# with a rate-limit-shaped error — or, on the native `loom-daemon forge
# auto-merge` path, the *prior* step (resolving the repo NWO via
# `gh repo view`, itself a GraphQL call) fails first with:
#
#     Failed to enable auto-merge for PR #4439: could not resolve repository NWO
#
# This is a TRANSIENT, environmental condition (unlike #3763's permanent
# repo-level "Allow auto-merge" setting) — REST has a separate quota and
# typically still has headroom, so the existing wait-for-checks/immediate-merge
# REST path (#3820) is a safe degrade. Before this fix, neither error string
# matched the "is in clean status" nor "is in unstable status" greps, so the
# script fell through to the generic terminal error and aborted, even though a
# plain synchronous REST merge would have succeeded immediately (the observed
# failure: 2026-07-29, PR #4439, 0/5000 GraphQL remaining while REST had ~4000
# left; re-running without --auto succeeded instantly).
#
# This test exercises:
#   1. The trigger-string matcher (grep) fires on all documented GraphQL
#      outage signatures and stays mutually exclusive with the CLEAN/UNSTABLE
#      and #3763 auto-merge-disabled matchers.
#   2. The mergeability-gated decision policy: immediate merge when already
#      mergeable, else degrade to the #3820 wait path (never re-attempt the
#      doomed mutation).
#   3. Source wiring: the classifier block exists, is placed after the #3763
#      block and before the CLEAN/UNSTABLE greps, logs the AC's exact one-line
#      reason, and calls _wait_for_checks_then_sync_merge on the not-yet-
#      mergeable branch.
#
# Usage:
#   ./.loom/scripts/tests/test-merge-pr-rate-limit-fallback.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPERS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MERGE_PR_SRC="$HELPERS_DIR/merge-pr.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$expected" == "$actual" ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Expected: '$expected'"
        echo "    Actual:   '$actual'"
    fi
}

# --- Fixture error strings ---
# The exact live failure (native loom-daemon path, GraphQL repo_nwo() lookup
# fails under quota exhaustion before the mutation even runs).
nwo_error="Failed to enable auto-merge for PR #4439: could not resolve repository NWO"

# Fixtures that MUST match the new #4447 classifier (name:value pairs, name
# first so failure messages stay readable without indirect var expansion,
# which shellcheck's SC2034 cannot follow through this codebase's target
# shell=bash config).
positive_fixtures=(
    "nwo_error:$nwo_error"
    "rate_limit_error:gh: API rate limit exceeded for user ID 1 (HTTP 403)"
    "submitted_too_quickly_error:gh: You have exceeded a secondary rate limit and have been temporarily blocked from content creation. Please retry your request again later. was submitted too quickly (HTTP 403)"
    'graphql_typed_error:{"type":"RATE_LIMITED","message":"API rate limit exceeded"}'
)

# Fixtures for the other classifiers, to prove mutual exclusivity (#4447's
# matcher must NOT fire on these).
negative_fixtures=(
    "disabled_error:Failed to enable auto-merge for PR #26: gh: Auto merge is not allowed for this repository"
    "clean_error:gh: Pull request Pull request is in clean status (enablePullRequestAutoMerge)"
    "unstable_error:gh: Pull request Pull request is in unstable status (enablePullRequestAutoMerge)"
)

# --- Test the trigger-string matcher ---
echo "Testing the GraphQL-unavailability substring matcher shape (#4447)..."

_rlf_matches() {
    echo "$1" | grep -Eq "API rate limit|rate limit exceeded|RATE_LIMITED|was submitted too quickly|could not resolve repository NWO"
}

for entry in "${positive_fixtures[@]}"; do
    fixture_name="${entry%%:*}"
    fixture_value="${entry#*:}"
    TESTS_RUN=$((TESTS_RUN + 1))
    if _rlf_matches "$fixture_value"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: matcher fires on $fixture_name"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: matcher missed $fixture_name ('$fixture_value')"
    fi
done

for entry in "${negative_fixtures[@]}"; do
    fixture_name="${entry%%:*}"
    fixture_value="${entry#*:}"
    TESTS_RUN=$((TESTS_RUN + 1))
    if _rlf_matches "$fixture_value"; then
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: matcher fired on $fixture_name (false positive)"
    else
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: matcher does NOT fire on $fixture_name"
    fi
done

# Conversely, the existing #3763 matcher must not fire on the new
# GraphQL-outage fixture (CLEAN/UNSTABLE are already proven disjoint above).
TESTS_RUN=$((TESTS_RUN + 1))
if echo "$nwo_error" | grep -q "Auto merge is not allowed for this repository"; then
    TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #3763 matcher fired on the NWO-resolution error (false positive)"
else
    TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #3763 matcher does NOT fire on the NWO-resolution error"
fi

# --- Test the mergeability-gated decision policy ---
echo ""
echo "Testing the GraphQL-unavailability mergeability policy (#4447)..."

# Returns "merge" (immediate synchronous merge), "wait" (degrade to the #3820
# wait-then-merge path), mirroring the merge-pr.sh callsite predicate.
_rlf_decision() {
    local mergeable="$1"
    if [[ "$mergeable" == "true" ]]; then
        echo "merge"
    else
        echo "wait"
    fi
}

_mergeable_of() {
    echo "$1" | jq -r '.mergeable // empty'
}

pr_mergeable='{"number":4439,"mergeable":true,"merged":false}'
assert_eq "merge" "$(_rlf_decision "$(_mergeable_of "$pr_mergeable")")" \
  "#4447: GraphQL outage + already-mergeable PR -> immediate synchronous merge"

pr_pending_checks='{"number":4439,"mergeable":null,"merged":false}'
assert_eq "wait" "$(_rlf_decision "$(_mergeable_of "$pr_pending_checks")")" \
  "#4447: GraphQL outage + mergeable unknown (checks pending) -> degrade to wait-then-merge"

pr_not_yet_computed='{"number":4439,"merged":false}'
assert_eq "wait" "$(_rlf_decision "$(_mergeable_of "$pr_not_yet_computed")")" \
  "#4447: GraphQL outage + no .mergeable field -> degrade to wait-then-merge"

# --- Assert merge-pr.sh source actually wires the #4447 fallback ---
echo ""
echo "Testing merge-pr.sh source wiring (#4447)..."

if grep -Eq 'grep -Eq "API rate limit\|rate limit exceeded\|RATE_LIMITED\|was submitted too quickly\|could not resolve repository NWO"' "$MERGE_PR_SRC"; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: merge-pr.sh greps for the GraphQL-unavailability error signatures"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: merge-pr.sh missing the #4447 rate-limit/NWO grep"
fi

# The AC's exact one-line log message must be present verbatim.
if grep -q 'auto-merge enablement unavailable (GraphQL rate limit) — degrading to immediate/wait merge' "$MERGE_PR_SRC"; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: merge-pr.sh logs the AC's exact degrade reason"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: merge-pr.sh missing the AC's one-line degrade log message"
fi

# Anchor on the actual classifier block (from its grep to the next classifier
# below it — the CLEAN check) to assert its internals, mirroring the #3763
# test's awk anchoring technique.
_rlf_block="$(awk '/grep -Eq "API rate limit\|rate limit exceeded\|RATE_LIMITED\|was submitted too quickly\|could not resolve repository NWO"/{f=1} f; /grep -q "is in clean status"/{exit}' "$MERGE_PR_SRC")"

if echo "$_rlf_block" | grep -q 'forge_get_pr_nocache'; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #4447 fallback re-fetches PR state via forge_get_pr_nocache (uncached)"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #4447 fallback must re-fetch via forge_get_pr_nocache"
fi

if echo "$_rlf_block" | grep -q '_wait_for_checks_then_sync_merge'; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #4447 fallback degrades to the #3820 _wait_for_checks_then_sync_merge path"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #4447 fallback must call _wait_for_checks_then_sync_merge on the not-yet-mergeable branch"
fi

if echo "$_rlf_block" | grep -q 'AUTO_MERGE=false' && echo "$_rlf_block" | grep -q 'AUTO_MERGE_OK=true'; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #4447 fallback flips to the synchronous-merge path (AUTO_MERGE=false, AUTO_MERGE_OK=true)"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #4447 fallback must flip AUTO_MERGE=false / AUTO_MERGE_OK=true"
fi

# The fallback must NEVER re-attempt the doomed GraphQL mutation itself
# (forge_auto_merge / loom-daemon forge auto-merge) — it degrades to REST.
if echo "$_rlf_block" | grep -Eq 'forge_auto_merge "\$REPO_NWO"|loom-daemon forge auto-merge'; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #4447 fallback must NOT re-attempt the GraphQL auto-merge mutation"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #4447 fallback does not re-attempt the GraphQL auto-merge mutation"
fi

# Ordering: the #4447 fallback must be inserted AFTER the #3763 auto-merge-
# disabled block and BEFORE the CLEAN/UNSTABLE greps (per curator guidance).
_amd_line=$(grep -n 'grep -q "Auto merge is not allowed for this repository"' "$MERGE_PR_SRC" | head -1 | cut -d: -f1)
_rlf_line=$(grep -n 'grep -Eq "API rate limit' "$MERGE_PR_SRC" | head -1 | cut -d: -f1)
_clean_line=$(grep -n 'grep -q "is in clean status"' "$MERGE_PR_SRC" | head -1 | cut -d: -f1)
if [[ -n "$_amd_line" ]] && [[ -n "$_rlf_line" ]] && [[ -n "$_clean_line" ]] \
   && [[ "$_amd_line" -lt "$_rlf_line" ]] && [[ "$_rlf_line" -lt "$_clean_line" ]]; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #4447 fallback is placed after #3763 and before the CLEAN/UNSTABLE greps"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #4447 fallback ordering wrong (amd=$_amd_line rlf=$_rlf_line clean=$_clean_line)"
fi

# --- Regression: the #3763 and #3820 tests' key invariants must still hold ---
# (Full re-execution is left to the CI job that runs both files; here we just
# assert this new block did not clobber the earlier ones' source anchors.)
echo ""
echo "Testing #3763/#3820 blocks are still intact (no regression from #4447)..."

if grep -q 'grep -q "Auto merge is not allowed for this repository"' "$MERGE_PR_SRC"; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #3763 grep is still present"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #3763 grep missing (regression)"
fi

if grep -q 'REPO_AUTO_MERGE_ALLOWED="\$(forge_check_auto_merge_allowed' "$MERGE_PR_SRC"; then
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: #3820 proactive probe is still present"
else
    TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: #3820 proactive probe missing (regression)"
fi

# --- Summary ---
echo ""
echo "────────────────────────────────"
echo "Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"

if [[ $TESTS_FAILED -gt 0 ]]; then
    exit 1
fi
exit 0
