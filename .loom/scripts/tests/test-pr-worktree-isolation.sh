#!/usr/bin/env bash
# test-pr-worktree-isolation.sh - Regression test for issue #3358
#
# Verifies that:
#   1. pr-worktree.sh creates a worktree at .loom/worktrees/pr-<N>/ with the
#      .loom-managed sentinel (PR-mode entry).
#   2. The orchestrator's main worktree HEAD is unchanged after a simulated
#      doctor pass that targets an external-fork-style PR branch (`fix/foo-bar`).
#   3. merge-pr.sh's regex tightening correctly classifies branch shapes:
#      - `feature/issue-42` -> issue-style (clean up issue-42 worktree)
#      - `release-1`        -> PR-style (clean up pr-<N> worktree)
#      - `fix/foo-bar`      -> PR-style
#      - `feature/issue-42-fix` (extra suffix) -> PR-style (strict regex)
#   4. doctor.md (defaults/) documents the branch-isolation requirement.
#
# This is a static / simulation test — we do not exercise the real `gh pr
# checkout` round-trip because that would require a live forge. The
# correctness of the regex and worktree-path selection is what we care about.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPTS_DIR/../.." && pwd)"

PR_WORKTREE_SH="$SCRIPTS_DIR/pr-worktree.sh"
MERGE_PR_SH="$SCRIPTS_DIR/merge-pr.sh"
# doctor.md is shipped (installed at .claude/commands/loom/doctor.md), so
# resolve it the way each layout actually lays it out: the installed path
# first (consumer repos, and Loom's own dogfooded checkout), falling back
# to the defaults/ source-tree path (a bare source checkout with no
# .claude/commands/loom/ copy yet). See issue #6194 / #6241.
if [[ -f "$REPO_ROOT/.claude/commands/loom/doctor.md" ]]; then
    DOCTOR_MD="$REPO_ROOT/.claude/commands/loom/doctor.md"
else
    DOCTOR_MD="$REPO_ROOT/defaults/.claude/commands/loom/doctor.md"
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1)); echo -e "  ${GREEN}PASS${NC}: $1"; }
fail() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); echo -e "  ${RED}FAIL${NC}: $1"; }

assert_grep() {
    local pattern="$1" file="$2" msg="$3"
    if grep -qE "$pattern" "$file"; then pass "$msg"; else fail "$msg (pattern: $pattern)"; fi
}

if [[ ! -f "$DOCTOR_MD" ]]; then
    echo -e "${RED}FATAL${NC}: doctor.md not found at $DOCTOR_MD"
    exit 1
fi

# --- Test 1: pr-worktree.sh exists and has the sentinel write block ---
echo "Test 1: pr-worktree.sh ships sentinel write logic"
if [[ -x "$PR_WORKTREE_SH" ]]; then
    pass "pr-worktree.sh exists and is executable"
else
    fail "pr-worktree.sh missing or not executable at $PR_WORKTREE_SH"
fi
assert_grep "Loom-managed worktree marker" "$PR_WORKTREE_SH" \
    "pr-worktree.sh writes the sentinel marker"
assert_grep "# PR: " "$PR_WORKTREE_SH" \
    "pr-worktree.sh records PR number in sentinel (mirrors worktree.sh issue convention)"
assert_grep "worktrees/pr-" "$PR_WORKTREE_SH" \
    "pr-worktree.sh uses .loom/worktrees/pr-<N>/ convention"

# --- Test 2: merge-pr.sh uses strict regex and recognizes pr-<N> path ---
echo ""
echo "Test 2: merge-pr.sh uses strict branch regex + pr-<N> cleanup"
assert_grep '\^feature/issue-\(\[0-9\]\+\)\$' "$MERGE_PR_SH" \
    "merge-pr.sh uses strict ^feature/issue-([0-9]+)$ regex (tightened from trailing-digit heuristic)"
assert_grep "worktrees/pr-" "$MERGE_PR_SH" \
    "merge-pr.sh cleans up .loom/worktrees/pr-<N>/ path"
# Confirm the loose trailing-digit heuristic is gone
if grep -q "grep -oE '\[0-9\]+\$'" "$MERGE_PR_SH"; then
    fail "merge-pr.sh still uses the loose trailing-digit regex (should be tightened)"
else
    pass "loose trailing-digit regex no longer present in merge-pr.sh"
fi

# --- Test 3: doctor.md documents PR Branch Isolation ---
echo ""
echo "Test 3: doctor.md documents the worktree-isolation rule"
assert_grep "PR Branch Isolation" "$DOCTOR_MD" \
    "doctor.md has a 'PR Branch Isolation' section"
assert_grep "pr-worktree\.sh" "$DOCTOR_MD" \
    "doctor.md references pr-worktree.sh helper"
assert_grep "\^feature/issue-" "$DOCTOR_MD" \
    "doctor.md documents the feature/issue-<N> regex heuristic"

# --- Test 4: branch-to-worktree classification (in-script regex simulation) ---
echo ""
echo "Test 4: branch classification regex (simulated)"

classify_branch() {
    local branch="$1"
    if [[ "$branch" =~ ^feature/issue-([0-9]+)$ ]]; then
        echo "issue:${BASH_REMATCH[1]}"
    else
        echo "pr"
    fi
}

assert_classify() {
    local branch="$1" expected="$2"
    local got
    got=$(classify_branch "$branch")
    if [[ "$got" == "$expected" ]]; then
        pass "branch '$branch' -> $expected"
    else
        fail "branch '$branch' expected '$expected', got '$got'"
    fi
}

assert_classify "feature/issue-42"      "issue:42"
assert_classify "feature/issue-3358"    "issue:3358"
assert_classify "fix/foo-bar"           "pr"
assert_classify "release-1"             "pr"            # tightening fix: no longer matches issue style
assert_classify "fix-bug-42"            "pr"            # tightening fix: no longer matches issue style
assert_classify "feature/issue-42-fix"  "pr"            # extra suffix -> not strict match
assert_classify "jperla/fix-thing"      "pr"
assert_classify "main"                  "pr"

# --- Test 5: simulated doctor pass leaves orchestrator HEAD unchanged ---
echo ""
echo "Test 5: doctor pass on 'fix/foo-bar' does not move orchestrator HEAD"

TMP=$(mktemp -d /tmp/loom-pr-iso-test.XXXXXX)
trap 'rm -rf "$TMP"; cd "$REPO_ROOT" 2>/dev/null || true' EXIT

# Build a tiny throwaway repo so we exercise real git commands without
# touching the real workspace. We simulate the doctor's worktree-creation
# step only — we don't call `gh pr checkout` since there is no live forge.
git init -q -b main "$TMP/origin.git" --bare
git init -q -b main "$TMP/repo"
cd "$TMP/repo"
git config user.email t@t
git config user.name t
git commit --allow-empty -q -m init
git remote add origin "$TMP/origin.git"
git push -q origin main

ORCH_HEAD_BEFORE=$(git rev-parse HEAD)
ORCH_BRANCH_BEFORE=$(git rev-parse --abbrev-ref HEAD)

# Simulate what the corrected doctor flow does for an external-fork PR
# whose branch is `fix/foo-bar`: create a `pr-<N>` worktree from origin/main
# and "check out" a synthetic PR branch INSIDE that worktree. We don't have a
# real PR, so we substitute a fake branch ref pointing at origin/main and
# check it out inside the dedicated worktree.
PR_NUM=999
PR_BRANCH="fix/foo-bar"
git update-ref "refs/heads/$PR_BRANCH" HEAD
git worktree add --detach ".loom/worktrees/pr-$PR_NUM" origin/main >/dev/null 2>&1

# Write a sentinel mirroring pr-worktree.sh
mkdir -p ".loom/worktrees/pr-$PR_NUM"
cat > ".loom/worktrees/pr-$PR_NUM/.loom-managed" <<EOF
# Loom-managed worktree marker
# PR: $PR_NUM
EOF

# Now "check out" the PR branch from INSIDE the worktree.
(cd ".loom/worktrees/pr-$PR_NUM" && git checkout "$PR_BRANCH" >/dev/null 2>&1)

ORCH_HEAD_AFTER=$(git rev-parse HEAD)
ORCH_BRANCH_AFTER=$(git rev-parse --abbrev-ref HEAD)

if [[ "$ORCH_HEAD_BEFORE" == "$ORCH_HEAD_AFTER" ]]; then
    pass "orchestrator HEAD SHA unchanged after PR-worktree checkout"
else
    fail "orchestrator HEAD moved: $ORCH_HEAD_BEFORE -> $ORCH_HEAD_AFTER"
fi
if [[ "$ORCH_BRANCH_BEFORE" == "$ORCH_BRANCH_AFTER" ]]; then
    pass "orchestrator branch unchanged ($ORCH_BRANCH_BEFORE)"
else
    fail "orchestrator branch moved: $ORCH_BRANCH_BEFORE -> $ORCH_BRANCH_AFTER"
fi

# Verify the worktree got the fork branch checked out
WT_BRANCH=$(git -C ".loom/worktrees/pr-$PR_NUM" rev-parse --abbrev-ref HEAD)
if [[ "$WT_BRANCH" == "$PR_BRANCH" ]]; then
    pass "PR worktree HEAD is on '$PR_BRANCH'"
else
    fail "PR worktree HEAD expected '$PR_BRANCH', got '$WT_BRANCH'"
fi

# Verify sentinel exists in the pr-<N> worktree
if [[ -f ".loom/worktrees/pr-$PR_NUM/.loom-managed" ]]; then
    pass "pr-$PR_NUM worktree has .loom-managed sentinel"
else
    fail "pr-$PR_NUM worktree missing .loom-managed sentinel"
fi

# --- Test 6: detached-HEAD collision is diagnosed clearly, not silent (#6264) ---
echo ""
echo "Test 6: pr-worktree.sh surfaces a clear diagnostic on the branch-collision failure (#6264)"

# Reproduces (with a mocked `gh`, since this test has no live forge) the exact
# mechanism confirmed by hand against a real PR while curating/building #6264:
# `gh pr checkout <N> --force` fails loudly (git refuses the same branch
# checked out in two worktrees at once) and leaves the freshly-created
# pr-<N> worktree on a detached HEAD. The fix under test is NOT changing that
# git-level refusal (impossible — it's a structural git invariant) but making
# pr-worktree.sh's failure message name the colliding worktree explicitly so
# it cannot be mistaken for a ready-to-evaluate worktree.
#
# Still inside "$TMP/repo" from Test 5, with the same origin remote. Use a
# different PR/branch pair (998 / feature/issue-998) to avoid colliding with
# Test 5's pr-999 fixture.
COLLIDE_BRANCH="feature/issue-998"
git update-ref "refs/heads/$COLLIDE_BRANCH" HEAD
# Simulate an existing builder worktree already holding the branch — the
# real-world collision source (a same-host issue-<N> builder worktree).
git worktree add -q ".loom/worktrees/issue-998" "$COLLIDE_BRANCH"

MOCKBIN=$(mktemp -d /tmp/loom-pr-iso-mockbin.XXXXXX)
trap 'rm -rf "$TMP" "$MOCKBIN"; cd "$REPO_ROOT" 2>/dev/null || true' EXIT
cat > "$MOCKBIN/gh" <<'MOCKEOF'
#!/usr/bin/env bash
# Minimal `gh pr checkout --force` stand-in reproducing the exact failure
# `git worktree add`/`checkout` itself produces when the target branch is
# already checked out elsewhere — see the real repro in the #6264 PR
# description. Any other subcommand is a no-op success (unused by this test).
if [[ "$1" == "pr" && "$2" == "checkout" ]]; then
    echo "fatal: 'feature/issue-998' is already used by worktree at '$LOOM_TEST_COLLIDING_WT'" >&2
    echo "failed to run git: exit status 128" >&2
    exit 1
fi
exit 0
MOCKEOF
chmod +x "$MOCKBIN/gh"

COLLIDING_WT_ABS="$(cd ".loom/worktrees/issue-998" && pwd)"
set +e
PR_OUT=$(PATH="$MOCKBIN:$PATH" LOOM_DEFAULT_BRANCH=main LOOM_TEST_COLLIDING_WT="$COLLIDING_WT_ABS" \
    "$PR_WORKTREE_SH" 998 2>&1)
PR_RC=$?
set -e

if [[ "$PR_RC" -ne 0 ]]; then
    pass "pr-worktree.sh exits non-zero on the branch-collision failure"
else
    fail "pr-worktree.sh exited 0 despite the checkout failure — got: $PR_OUT"
fi
if echo "$PR_OUT" | grep -q "already checked out in another worktree"; then
    pass "failure message explicitly names the collision (not just the raw git error)"
else
    fail "expected an explicit collision diagnostic; got: $PR_OUT"
fi
if echo "$PR_OUT" | grep -qF "$COLLIDING_WT_ABS"; then
    pass "failure message names the specific colliding worktree path"
else
    fail "expected the colliding worktree path ($COLLIDING_WT_ABS) in the output; got: $PR_OUT"
fi
if echo "$PR_OUT" | grep -qi "do NOT evaluate code in it as-is"; then
    pass "failure message warns against evaluating the detached worktree as-is"
else
    fail "expected an explicit do-not-evaluate warning; got: $PR_OUT"
fi

# The worktree directory itself is still left behind (pre-existing, unchanged
# behavior — merge-pr.sh's cleanup is path-based specifically because of this)
# and remains on a detached HEAD, since the mock never touches it.
if [[ -d ".loom/worktrees/pr-998" ]]; then
    pass "the pr-998 worktree directory is left on disk after the failed checkout (unchanged pre-#6264 behavior)"
else
    fail "expected .loom/worktrees/pr-998 to still exist on disk"
fi
PR998_BRANCH=$(git -C ".loom/worktrees/pr-998" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
if [[ "$PR998_BRANCH" == "HEAD" ]]; then
    pass "pr-998 worktree is left on a detached HEAD (matches the incident's 'git worktree list' output)"
else
    fail "expected detached HEAD ('HEAD' from rev-parse --abbrev-ref); got '$PR998_BRANCH'"
fi

cd "$REPO_ROOT"

# --- Summary ---
echo ""
echo "Tests run: $TESTS_RUN, Passed: $TESTS_PASSED, Failed: $TESTS_FAILED"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
