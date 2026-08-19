#!/usr/bin/env bash
# test-worktree-existing-dir-drift-check.sh — Tests for the drift check on the
# "worktree directory already exists, registered with git" fast path (#6257)
#
# Regression coverage for the incident on #5609: a Judge session reused an
# existing builder worktree (`.loom/worktrees/issue-5609`) that was one commit
# behind the PR's actual pushed tip, had ~230 lines of uncommitted stale WIP
# sitting in the working tree, and whose local branch's upstream tracking ref
# was wrongly set to `origin/main` instead of `origin/feature/issue-5609`.
#
# Root cause: worktree.sh's "worktree directory already exists" fast path
# (`if git worktree list | grep -q "$WORKTREE_PATH"`) only ever compared the
# worktree's HEAD to BASE_REF (the default branch) to decide whether to
# "preserve existing work" or reset a stale worktree — it never fetched or
# compared against the branch's OWN upstream (origin/$BRANCH_NAME), and never
# touched upstream tracking at all. This is a completely different code path
# from the "local branch exists, no worktree dir yet" reuse path (#6095/#6100,
# covered by test-worktree-local-branch-upstream-tracking.sh) — that fix never
# ran here, so a worktree left with stale HEAD and/or wrong upstream tracking
# was silently "preserved" and handed straight to a Judge/Doctor session with
# no signal that it no longer matched the branch's actual pushed tip.
#
# Coverage:
#   1. Worktree one commit behind the pushed branch tip, wrong upstream
#      (origin/main), AND uncommitted changes (the exact incident shape):
#      worktree.sh warns about the drift, corrects the upstream, and does NOT
#      destroy the uncommitted work (still preserved for the caller).
#   2. Worktree with a local commit ahead of the pushed tip (unpushed work)
#      and no uncommitted changes: no false-positive "may be stale" warning.
#   3. Worktree already correctly synced (HEAD matches origin's tip, upstream
#      already correct, no uncommitted changes): no-op, no warning of any
#      kind (regression guard against false positives on the common case).
#
# Pattern follows test-worktree-local-branch-upstream-tracking.sh: throwaway
# bare origin + repo in a mktemp dir, copy worktree.sh + lib/, but here the
# worktree itself is first materialized via a REAL `./.loom/scripts/worktree.sh
# <N>` call (so the fast path under test — "directory already exists,
# registered with git" — actually fires on the second invocation, exactly as
# it would for a reused builder worktree), then mutated to the drift shape
# under test before invoking worktree.sh a second time.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKTREE_SH="$SCRIPTS_DIR/worktree.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1)); echo -e "  ${GREEN}PASS${NC}: $1"; }
fail() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); echo -e "  ${RED}FAIL${NC}: $1"; }

# Build a throwaway repo with a `feature/issue-<n>` branch pushed to origin,
# then materialize its worktree via a real (first) `worktree.sh <n>` call —
# so the worktree is registered with git exactly as an earlier
# worktree.sh/Builder pass would have left it, with correct tracking. Echoes
# "<repo-path> <worktree-relative-path>".
#
# Resolves the mktemp root to its physical path (pwd -P): on macOS /tmp is a
# symlink to /private/tmp, and worktree.sh's orphan-cleanup compares `git
# worktree list` paths (physical) against a resolved path — a symlinked temp
# root would make the just-registered worktree look unregistered and get
# spuriously deleted, defeating the point of this reuse-path test (mirrors
# test-worktree-sentinel-reinvoke.sh's TMP_ROOT handling).
setup_repo_with_worktree() {
    local name="$1"
    local issue="$2"
    local tmp
    tmp=$(cd "$(mktemp -d /tmp/loom-wtdrift.XXXXXX)" && pwd -P)
    git init -q -b main "$tmp/origin.git" --bare
    git init -q -b main "$tmp/$name"
    (
        cd "$tmp/$name"
        git config user.email t@t
        git config user.name t
        git commit --allow-empty -q -m init
        git remote add origin "$tmp/origin.git"
        git push -q origin main
        mkdir -p .loom/scripts/lib .loom/hooks
        cp "$WORKTREE_SH" .loom/scripts/worktree.sh
        if [[ -d "$SCRIPTS_DIR/lib" ]]; then
            cp -R "$SCRIPTS_DIR"/lib/* .loom/scripts/lib/ 2>/dev/null || true
        fi
        chmod +x .loom/scripts/worktree.sh

        git checkout -q -b "feature/issue-$issue"
        echo "builder-work" > work.txt
        git add work.txt
        git commit -q -m "builder work"
        git push -q -u origin "feature/issue-$issue"
        git checkout -q main

        # First (real) worktree.sh invocation - materializes and registers
        # the worktree exactly as a Builder pass would.
        ./.loom/scripts/worktree.sh "$issue" >/dev/null 2>&1
    )
    echo "$tmp/$name .loom/worktrees/issue-$issue"
}

# Push one more commit to origin/feature/issue-<n> WITHOUT touching the
# existing worktree (which already has that branch checked out) — via a
# throwaway second clone, simulating a later push from a different
# session/worktree.
push_followup_commit() {
    local repo="$1"
    local issue="$2"
    local origin
    origin="$(git -C "$repo" remote get-url origin)"
    local clone_dir
    clone_dir=$(mktemp -d /tmp/loom-wtdrift-clone.XXXXXX)
    git clone -q "$origin" "$clone_dir" >/dev/null 2>&1
    (
        cd "$clone_dir"
        git config user.email t@t
        git config user.name t
        git checkout -q "feature/issue-$issue"
        echo "later-push" > later.txt
        git add later.txt
        git commit -q -m "later push from another session"
        git push -q origin "feature/issue-$issue"
    )
    rm -rf "$clone_dir"
}

cleanup_repo() {
    local repo="$1"
    [[ -z "$repo" ]] && return 0
    rm -rf "$(dirname "$repo")"
}

# --- Test 1: behind pushed tip + wrong upstream + uncommitted changes (the incident) ---
echo "Test 1: worktree one commit behind pushed tip, wrong upstream, uncommitted WIP -> worktree.sh warns and corrects upstream without destroying WIP"
read -r REPO WT_REL <<< "$(setup_repo_with_worktree incident 301)"
WT="$REPO/$WT_REL"

# Simulate: another session pushed a follow-up commit this worktree never saw.
push_followup_commit "$REPO" 301

# Simulate: upstream tracking somehow got mis-set to origin/main (the #6095
# incident shape) after the worktree was created correctly.
git -C "$WT" branch --set-upstream-to=origin/main feature/issue-301

# Simulate: stale uncommitted WIP sitting in the working tree.
echo "stale-wip-line" >> "$WT/work.txt"

OUT_LOG="/tmp/wtdrift-incident.$$"
(
    cd "$REPO"
    ./.loom/scripts/worktree.sh 301 >"$OUT_LOG" 2>&1 || { echo "FAILED"; cat "$OUT_LOG"; }
)

if grep -qi "may be stale" "$OUT_LOG"; then
    pass "worktree.sh warns that the worktree may be stale"
else
    fail "worktree.sh did not warn about staleness"
    cat "$OUT_LOG"
fi

if grep -qi "uncommitted changes" "$OUT_LOG"; then
    pass "worktree.sh flags the uncommitted changes alongside the drift warning"
else
    fail "worktree.sh did not mention the uncommitted changes in its drift warning"
fi

WT_UPSTREAM=$(git -C "$WT" rev-parse --abbrev-ref 'feature/issue-301@{u}' 2>/dev/null || echo "")
if [[ "$WT_UPSTREAM" == "origin/feature/issue-301" ]]; then
    pass "worktree.sh corrected the upstream to origin/feature/issue-301 (was origin/main)"
else
    fail "worktree's upstream is '$WT_UPSTREAM', expected 'origin/feature/issue-301'"
fi

if grep -q "stale-wip-line" "$WT/work.txt"; then
    pass "uncommitted WIP was NOT destroyed by the drift check"
else
    fail "uncommitted WIP was lost"
fi

WT_HEAD=$(git -C "$WT" rev-parse HEAD)
ORIGIN_TIP=$(git -C "$REPO" rev-parse origin/feature/issue-301)
if [[ "$WT_HEAD" != "$ORIGIN_TIP" ]]; then
    pass "worktree.sh did not silently pull/reset HEAD on its own (still behind, as expected for a warn-only check)"
else
    fail "worktree.sh unexpectedly moved HEAD to the remote tip"
fi
cleanup_repo "$REPO"
rm -f "$OUT_LOG"

# --- Test 2: local commit ahead of pushed tip, no uncommitted changes -> no false positive ---
echo ""
echo "Test 2: worktree has an unpushed local commit ahead of origin's tip, no uncommitted changes -> no 'may be stale' false positive"
read -r REPO WT_REL <<< "$(setup_repo_with_worktree ahead 302)"
WT="$REPO/$WT_REL"

# Add a local commit in the worktree that has NOT been pushed.
(
    cd "$WT"
    echo "unpushed-local-commit" > unpushed.txt
    git add unpushed.txt
    git commit -q -m "unpushed local work"
)

OUT_LOG="/tmp/wtdrift-ahead.$$"
(
    cd "$REPO"
    ./.loom/scripts/worktree.sh 302 >"$OUT_LOG" 2>&1 || { echo "FAILED"; cat "$OUT_LOG"; }
)

if grep -qi "may be stale" "$OUT_LOG"; then
    fail "worktree.sh false-positive warned about staleness for a worktree that is genuinely AHEAD, not behind"
    cat "$OUT_LOG"
else
    pass "worktree.sh did not false-positive warn for a worktree ahead of origin (unpushed local commit)"
fi
if grep -qi "preserving existing work" "$OUT_LOG"; then
    pass "worktree.sh still reports preserving the existing (ahead) work"
else
    fail "worktree.sh did not report preserving the ahead work"
fi
cleanup_repo "$REPO"
rm -f "$OUT_LOG"

# --- Test 3: already correctly synced -> pure no-op, no warnings at all ---
echo ""
echo "Test 3: worktree already matches origin's tip, upstream already correct, no uncommitted changes -> no-op"
read -r REPO WT_REL <<< "$(setup_repo_with_worktree synced 303)"
WT="$REPO/$WT_REL"

# Nothing mutated - this worktree is exactly as worktree.sh's first
# invocation left it: HEAD == origin/feature/issue-303, upstream already
# correct, clean tree.

OUT_LOG="/tmp/wtdrift-synced.$$"
(
    cd "$REPO"
    ./.loom/scripts/worktree.sh 303 >"$OUT_LOG" 2>&1 || { echo "FAILED"; cat "$OUT_LOG"; }
)

if grep -qi "may be stale\|correcting to\|has no upstream" "$OUT_LOG"; then
    fail "worktree.sh printed a drift/correction warning for an already-synced worktree"
    cat "$OUT_LOG"
else
    pass "worktree.sh made no drift/correction noise for the already-synced case"
fi
WT_UPSTREAM=$(git -C "$WT" rev-parse --abbrev-ref 'feature/issue-303@{u}' 2>/dev/null || echo "")
if [[ "$WT_UPSTREAM" == "origin/feature/issue-303" ]]; then
    pass "worktree's upstream remains origin/feature/issue-303 (unaffected)"
else
    fail "worktree's upstream is '$WT_UPSTREAM', expected unchanged 'origin/feature/issue-303'"
fi
cleanup_repo "$REPO"
rm -f "$OUT_LOG"

# --- Summary ---
echo ""
echo "Tests run: $TESTS_RUN, Passed: $TESTS_PASSED, Failed: $TESTS_FAILED"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
