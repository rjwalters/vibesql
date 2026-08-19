#!/usr/bin/env bash
# test-check-defaults-version-bump.sh - Smoke tests for
# check-defaults-version-bump.sh (#5874, #6480).
#
# Constructs a throwaway local git repo with a `defaults/` tree and a
# `VERSION` file, then exercises:
#   (a) no defaults/ change              -> PASS
#   (b) defaults/ change + VERSION bump  -> PASS
#   (c) defaults/ change, no bump        -> FAIL, lists the changed files
#   (d) defaults/ change + PR_BODY marker -> PASS
#   (e) defaults/ change + commit-message marker -> PASS
#   (f) non-defaults/ change only        -> PASS (nothing to check)
# Plus usage/arg-handling checks:
#   - missing --base -> exit 2
#   - unknown base ref -> exit 2
#   - --help prints usage
# Plus custom-watch-path checks (#6480):
#   - --paths: change under a custom watched path, no bump -> FAIL
#   - --paths: change outside the watched paths only        -> PASS
#   - VERSION_BUMP_WATCH_PATHS env var behaves like --paths
#   - no --paths / no env var: default behavior unchanged (covered by the
#     original (a)-(f) cases above, which never set either)
#
# Usage:
#   ./.loom/scripts/tests/test-check-defaults-version-bump.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPERS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SCRIPT="$HELPERS_DIR/check-defaults-version-bump.sh"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() {
    TESTS_RUN=$((TESTS_RUN + 1))
    TESTS_PASSED=$((TESTS_PASSED + 1))
    echo -e "  ${GREEN}PASS${NC}: $1"
}

fail() {
    TESTS_RUN=$((TESTS_RUN + 1))
    TESTS_FAILED=$((TESTS_FAILED + 1))
    echo -e "  ${RED}FAIL${NC}: $1"
}

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/test-defaults-version-bump.XXXXXX")"
# shellcheck disable=SC2329  # invoked indirectly via the EXIT trap below
cleanup() { rm -rf "$WORKDIR" 2>/dev/null || true; }
trap cleanup EXIT

export GIT_AUTHOR_NAME="test" GIT_AUTHOR_EMAIL="test@example.com"
export GIT_COMMITTER_NAME="test" GIT_COMMITTER_EMAIL="test@example.com"

REPO="$WORKDIR/repo"

# Fresh repo with a base commit carrying defaults/foo.md, install.sh (a
# stand-in for a consumer repo's own installable surface), and VERSION=1.0.0,
# tagged "base". Callers add more commits on top and diff against "base".
make_fixture() {
    rm -rf "$REPO"
    git init --quiet "$REPO"
    git -C "$REPO" checkout -q -b main
    mkdir -p "$REPO/defaults/scripts"
    echo "hello" > "$REPO/defaults/scripts/foo.md"
    echo "#!/bin/sh" > "$REPO/install.sh"
    echo "1.0.0" > "$REPO/VERSION"
    echo "unrelated" > "$REPO/README.md"
    git -C "$REPO" add -A
    git -C "$REPO" commit -q -m "base"
    git -C "$REPO" tag base
}

# -------- Test 1: script exists and is executable --------
echo "Test 1: script exists and is executable"
if [[ -x "$SCRIPT" ]]; then
    pass "check-defaults-version-bump.sh is executable"
else
    fail "check-defaults-version-bump.sh is missing or not executable: $SCRIPT"
    echo "FAILED: $TESTS_FAILED/$TESTS_RUN"
    exit 1
fi

# -------- Test 2: no defaults/ change -> PASS --------
echo "Test 2: no defaults/ change passes"
make_fixture
echo "more" >> "$REPO/README.md"
git -C "$REPO" commit -q -am "readme only"
out="$(cd "$REPO" && "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "no defaults/ change exits 0"
else
    fail "no defaults/ change expected exit 0, got $rc. Output: $out"
fi
if printf '%s' "$out" | grep -qi "OK"; then
    pass "no defaults/ change prints OK"
else
    fail "no defaults/ change missing OK message. Got: $out"
fi

# -------- Test 3: defaults/ change + VERSION bump -> PASS --------
echo "Test 3: defaults/ change with a VERSION bump passes"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
echo "1.0.1" > "$REPO/VERSION"
git -C "$REPO" commit -q -am "bump"
out="$(cd "$REPO" && "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "defaults/ + VERSION bump exits 0"
else
    fail "defaults/ + VERSION bump expected exit 0, got $rc. Output: $out"
fi

# -------- Test 4: defaults/ change without a VERSION bump -> FAIL --------
echo "Test 4: defaults/ change without a VERSION bump fails"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "prompt change, no bump"
err_out="$(cd "$REPO" && "$SCRIPT" --base base 2>&1 >/dev/null)"
rc=0
( cd "$REPO" && "$SCRIPT" --base base >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 1 ]]; then
    pass "unbumped defaults/ change exits 1"
else
    fail "unbumped defaults/ change expected exit 1, got $rc"
fi
if printf '%s' "$err_out" | grep -q "defaults/scripts/foo.md"; then
    pass "failure output lists the changed file"
else
    fail "failure output missing changed file. Got: $err_out"
fi
if printf '%s' "$err_out" | grep -q "version.sh bump patch"; then
    pass "failure output suggests the version bump remediation"
else
    fail "failure output missing version bump remediation. Got: $err_out"
fi
if printf '%s' "$err_out" | grep -q "loom:no-surface-change"; then
    pass "failure output mentions the no-surface-change marker escape hatch"
else
    fail "failure output missing marker escape hatch. Got: $err_out"
fi

# -------- Test 5: defaults/ change + PR_BODY marker -> PASS --------
echo "Test 5: defaults/ change with a PR_BODY marker passes"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "doc typo fix"
out="$(cd "$REPO" && PR_BODY='fixes a typo

<!-- loom:no-surface-change -->' "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "PR_BODY marker exits 0"
else
    fail "PR_BODY marker expected exit 0, got $rc. Output: $out"
fi

# -------- Test 6: defaults/ change + commit-message marker -> PASS --------
echo "Test 6: defaults/ change with a commit-message marker passes"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "doc typo fix

<!-- loom:no-surface-change -->"
out="$(cd "$REPO" && "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "commit-message marker exits 0"
else
    fail "commit-message marker expected exit 0, got $rc. Output: $out"
fi

# -------- Test 7: missing --base -> exit 2 --------
echo "Test 7: missing --base exits 2"
make_fixture
rc=0
( cd "$REPO" && "$SCRIPT" >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 2 ]]; then
    pass "missing --base exits 2"
else
    fail "missing --base expected exit 2, got $rc"
fi

# -------- Test 8: unknown base ref -> exit 2 --------
echo "Test 8: unknown --base ref exits 2"
make_fixture
rc=0
( cd "$REPO" && "$SCRIPT" --base does-not-exist >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 2 ]]; then
    pass "unknown --base ref exits 2"
else
    fail "unknown --base ref expected exit 2, got $rc"
fi

# -------- Test 9: --help prints usage and exits 0 --------
echo "Test 9: --help prints usage and exits 0"
help_out="$("$SCRIPT" --help 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "--help exit code is 0"
else
    fail "--help expected exit 0, got $rc"
fi
if printf '%s' "$help_out" | grep -qi "Usage"; then
    pass "--help mentions Usage"
else
    fail "--help did not mention Usage. Got: $help_out"
fi

# -------- Test 10: --paths, change under a watched path, no bump -> FAIL --------
echo "Test 10: --paths custom watch, change under it without a bump fails"
make_fixture
echo "changed" >> "$REPO/install.sh"
git -C "$REPO" commit -q -am "install.sh change, no bump"
rc=0
err_out="$(cd "$REPO" && "$SCRIPT" --base base --paths install.sh 2>&1 >/dev/null)" || true
( cd "$REPO" && "$SCRIPT" --base base --paths install.sh >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 1 ]]; then
    pass "--paths install.sh: unbumped change under watched path exits 1"
else
    fail "--paths install.sh: expected exit 1, got $rc"
fi
if printf '%s' "$err_out" | grep -q "install.sh"; then
    pass "--paths install.sh: failure output lists the changed file"
else
    fail "--paths install.sh: failure output missing changed file. Got: $err_out"
fi
if printf '%s' "$err_out" | grep -q "install.sh"; then
    pass "--paths install.sh: failure output names the actual watched path, not 'defaults/'"
else
    fail "--paths install.sh: failure output missing watched-path name. Got: $err_out"
fi

# -------- Test 11: --paths, change outside the watched paths -> PASS --------
echo "Test 11: --paths custom watch, change outside it passes"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "defaults/ change, not watched"
out="$(cd "$REPO" && "$SCRIPT" --base base --paths install.sh 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "--paths install.sh: change outside watched paths exits 0"
else
    fail "--paths install.sh: expected exit 0, got $rc. Output: $out"
fi

# -------- Test 12: --paths with a VERSION bump -> PASS --------
echo "Test 12: --paths custom watch, change under it with a bump passes"
make_fixture
echo "changed" >> "$REPO/install.sh"
echo "1.0.1" > "$REPO/VERSION"
git -C "$REPO" commit -q -am "install.sh change, bumped"
out="$(cd "$REPO" && "$SCRIPT" --base base --paths install.sh 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "--paths install.sh: bumped change under watched path exits 0"
else
    fail "--paths install.sh: expected exit 0, got $rc. Output: $out"
fi

# -------- Test 13: VERSION_BUMP_WATCH_PATHS env var, change under it, no bump -> FAIL --------
echo "Test 13: VERSION_BUMP_WATCH_PATHS env var, unbumped change under it fails"
make_fixture
echo "changed" >> "$REPO/install.sh"
git -C "$REPO" commit -q -am "install.sh change via env var, no bump"
rc=0
( cd "$REPO" && VERSION_BUMP_WATCH_PATHS="install.sh" "$SCRIPT" --base base >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 1 ]]; then
    pass "VERSION_BUMP_WATCH_PATHS=install.sh: unbumped change exits 1"
else
    fail "VERSION_BUMP_WATCH_PATHS=install.sh: expected exit 1, got $rc"
fi

# -------- Test 14: VERSION_BUMP_WATCH_PATHS env var, change outside it -> PASS --------
echo "Test 14: VERSION_BUMP_WATCH_PATHS env var, change outside it passes"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "defaults/ change, not watched via env var"
out="$(cd "$REPO" && VERSION_BUMP_WATCH_PATHS="install.sh" "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "VERSION_BUMP_WATCH_PATHS=install.sh: change outside watched paths exits 0"
else
    fail "VERSION_BUMP_WATCH_PATHS=install.sh: expected exit 0, got $rc. Output: $out"
fi

# -------- Test 15: --paths overrides VERSION_BUMP_WATCH_PATHS -------
echo "Test 15: --paths takes precedence over VERSION_BUMP_WATCH_PATHS"
make_fixture
echo "changed" >> "$REPO/defaults/scripts/foo.md"
git -C "$REPO" commit -q -am "defaults/ change, --paths should still watch it"
out="$(cd "$REPO" && VERSION_BUMP_WATCH_PATHS="install.sh" "$SCRIPT" --base base --paths defaults/ 2>&1 >/dev/null)"
rc=0
( cd "$REPO" && VERSION_BUMP_WATCH_PATHS="install.sh" "$SCRIPT" --base base --paths defaults/ >/dev/null 2>&1 ) || rc=$?
if [[ "$rc" -eq 1 ]]; then
    pass "--paths defaults/ overrides VERSION_BUMP_WATCH_PATHS=install.sh"
else
    fail "--paths defaults/ expected to override env var and exit 1, got $rc"
fi

# -------- Test 16: default behavior with no --paths / no env var unchanged -------
echo "Test 16: default behavior (no --paths, no env var) matches original defaults/-only behavior"
make_fixture
echo "changed" >> "$REPO/install.sh"
git -C "$REPO" commit -q -am "install.sh change only, default watch is defaults/"
out="$(cd "$REPO" && "$SCRIPT" --base base 2>&1)"
rc=$?
if [[ "$rc" -eq 0 ]]; then
    pass "default watch (defaults/): install.sh-only change exits 0 (not watched by default)"
else
    fail "default watch (defaults/): expected exit 0, got $rc. Output: $out"
fi

# -------- Summary --------
echo ""
echo "Results: $TESTS_PASSED/$TESTS_RUN passed"
if [[ "$TESTS_FAILED" -gt 0 ]]; then
    echo -e "${RED}FAILED${NC}: $TESTS_FAILED test(s) failed"
    exit 1
fi
echo -e "${GREEN}OK${NC}: all tests passed"
exit 0
