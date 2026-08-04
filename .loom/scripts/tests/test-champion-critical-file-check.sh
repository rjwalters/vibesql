#!/usr/bin/env bash
# test-champion-critical-file-check.sh - Regression tests for the Critical File
# Exclusion Check false negative on PR #4611 (#4613).
#
# Champion's criterion #3 (`champion-pr-merge.md` "Critical File Exclusion
# Check") is prose an LLM instance reads and executes, not a standalone
# script (same situation as test-dependency-parse.sh) — so this file mirrors
# the documented check-loop in a local function and pins the shipped
# markdown's exact commands with `assert_doc_contains`, catching drift
# between the two.
#
# Incident recap: on PR #4611 (117 changed files), a concurrent Champion
# evaluation posted "no critical-file changes" despite the PR removing
# `.github/workflows/gitea-integration.yml` — a direct match for the
# documented `.github/workflows/` critical pattern. `gh pr view --json files`
# was confirmed (empirically, against the live PR) to silently truncate at
# 100 files with no error, while the paginated REST endpoint
# (`gh api repos/{owner}/{repo}/pulls/<n>/files --paginate`) returns the full
# set. The fix (#4613):
#   1. Switches criterion #3's FILES command (and the criterion #2 evidence-
#      gathering command) from `gh pr view --json files` to the paginated
#      REST endpoint.
#   2. Makes explicit that "no critical-file changes" / "No critical files
#      modified" must never be asserted in a comment without the check-loop
#      having actually just run over the full file list.
#
# This file asserts:
#   1. The mirrored critical-file check-loop correctly FAILS when a critical
#      file is present anywhere in a 100+-file list, including past index 100
#      (the position a naive 100-item cap would have dropped).
#   2. The mirrored loop correctly PASSes on a large all-clean file list.
#   3. The shipped markdown no longer contains the truncating
#      `gh pr view <number> --json files` invocation for either the
#      criterion #3 FILES command or the criterion #2 evidence-gathering
#      command, and does contain the paginated replacement.
#
# Usage:
#   ./.loom/scripts/tests/test-champion-critical-file-check.sh

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TEST_DIR/.." && pwd)"
DEFAULTS_DIR="$(cd "$SCRIPTS_DIR/.." && pwd)"
CHAMPION_MD="$DEFAULTS_DIR/.claude/commands/loom/champion-pr-merge.md"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

assert_eq() {
    local expected="$1" actual="$2" msg="$3"
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

# Pin a literal snippet as present verbatim in a doc file — catches drift
# between this test's mirrored function and the shipped markdown.
assert_doc_contains() {
    local file="$1" needle="$2" msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -qF -- "$needle" "$file"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg (missing literal in $file: $needle)"
    fi
}

# Pin a literal snippet's ABSENCE from a doc file — catches a regression back
# to the truncating command.
assert_doc_lacks() {
    local file="$1" needle="$2" msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -qF -- "$needle" "$file"; then
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg (found stale/truncating literal in $file: $needle)"
    else
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    fi
}

# =====================================================================
# champion-pr-merge.md criterion #3's critical-file check-loop, mirrored
# verbatim (pattern list + matching loop) from defaults/.claude/commands/
# loom/champion-pr-merge.md.
# =====================================================================
CRITICAL_PATTERNS=(
    "Cargo.toml"
    "loom-daemon/Cargo.toml"
    "loom-api/Cargo.toml"
    "package.json"
    ".github/workflows/"
    ".sql"
    "migration"
)

champion_critical_file_check() {
    # Reads a newline-separated file list on stdin. Echoes "FAIL: <file>" for
    # the first critical-pattern match found, or "PASS" if none match —
    # mirrors the doc's exit-on-first-match loop.
    local file
    while IFS= read -r file; do
        [ -z "$file" ] && continue
        for pattern in "${CRITICAL_PATTERNS[@]}"; do
            if [[ "$file" == *"$pattern"* ]]; then
                echo "FAIL: $file"
                return 0
            fi
        done
    done
    echo "PASS"
}

# Build a synthetic 117-changed-file payload matching PR #4611's shape: the
# critical file (a removed `.github/workflows/*.yml`) sits at position 1 in
# REST tree order but must survive regardless of position, so this fixture
# also covers it appearing past a naive 100-file cutoff (position 101).
build_fixture_files() {
    local critical_position="$1"  # 1-based line number to place the critical file at
    local total="$2"
    local i
    for ((i = 1; i <= total; i++)); do
        if [ "$i" -eq "$critical_position" ]; then
            echo ".github/workflows/gitea-integration.yml"
        else
            echo "src/module_$i/file_$i.rs"
        fi
    done
}

echo "--- champion_critical_file_check: catches a critical file at any position in 100+ files ---"

# Position 1 (matches the actual PR #4611 REST ordering).
fixture="$(build_fixture_files 1 117)"
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: .github/workflows/gitea-integration.yml" "$out" \
    "critical file at position 1 of 117 is caught"

# Position 101 — past a naive 100-file cutoff (the confirmed truncation point
# of \`gh pr view --json files\`), the exact blind spot this fix closes.
fixture="$(build_fixture_files 101 117)"
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: .github/workflows/gitea-integration.yml" "$out" \
    "critical file at position 101 of 117 (past a naive 100-item cap) is still caught"

# Position 117 (last file).
fixture="$(build_fixture_files 117 117)"
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: .github/workflows/gitea-integration.yml" "$out" \
    "critical file at the very last position of 117 is caught"

echo
echo "--- champion_critical_file_check: clean 100+ file list passes ---"

fixture="$(build_fixture_files 0 150)"  # position 0 = never place a critical file
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "PASS" "$out" "150 non-critical files pass with no false positive"

echo
echo "--- Doc pins: shipped markdown uses the paginated REST endpoint, not the truncating gh pr view field ---"

assert_doc_contains "$CHAMPION_MD" \
    'FILES=$(gh api "repos/{owner}/{repo}/pulls/<number>/files" --paginate --jq '"'"'.[].filename'"'"')' \
    "criterion #3 FILES command ships the paginated REST endpoint"

assert_doc_contains "$CHAMPION_MD" \
    'gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/files" --paginate --jq' \
    "criterion #2 evidence-gathering command ships the paginated REST endpoint"

assert_doc_lacks "$CHAMPION_MD" \
    'FILES=$(gh pr view <number> --json files --jq -r' \
    "criterion #3 FILES command no longer uses the truncating gh pr view --json files field"

assert_doc_lacks "$CHAMPION_MD" \
    'gh pr view "$PR_NUMBER" --json files --jq'"'"'.files[] | "\(.additions)+/\(.deletions)- \(.path)"'"'" \
    "criterion #2 evidence-gathering command no longer uses the truncating gh pr view --json files field"

assert_doc_contains "$CHAMPION_MD" \
    "#4613" \
    "champion-pr-merge.md documents the #4613 regression that motivated this fix"

echo
echo "Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
