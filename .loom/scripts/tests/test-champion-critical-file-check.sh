#!/usr/bin/env bash
# test-champion-critical-file-check.sh - Regression tests for the Critical File
# Exclusion Check false negative on PR #4611 (#4613), the version-only diff
# carve-out that fixes the permanent auto-merge block on `scripts/version.sh
# bump`'s mechanical commit (#6147), and the durable critical-file hold that
# replaces the endless-retry rejection on a genuine critical-file match
# (#6879).
#
# Champion's criterion #3 (`champion-pr-merge.md` "Critical File Exclusion
# Check") is prose an LLM instance reads and executes, not a standalone
# script (same situation as test-dependency-parse.sh) — so this file mirrors
# the documented check-loop (and, since #6147, the `version_only_diff()`
# carve-out, and since #6879, the durable-hold labeling) in local functions
# and pins the shipped markdown's exact commands with `assert_doc_contains`,
# catching drift between the two.
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
# Second incident recap (#6879): a critical-file FAIL routed through the same
# generic "Transient failures — keep loom:pr, retry next tick" template as
# label-check/size-check/ci-status, even though nothing about a diff's
# critical-file-ness clears on its own. A fleet PR was re-evaluated and
# re-rejected ~59 times over 36 hours this way (200-300s/tick), invisible to
# the operator queue the whole time (no loom:operator* label). The fix routes
# criterion #3's FAIL through its own durable hold instead — `loom:operator`
# + a `<!-- champion:critical-file-hold -->` marker, mirroring criterion #2's
# merge-risk hold — released only when a later push narrows the diff so it no
# longer matches any critical-file pattern.
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
#   4. (#6879) A fresh critical-file FAIL applies the loom:operator label and
#      posts the champion:critical-file-hold marker comment exactly once; a
#      repeated FAIL against an unchanged file set is idempotent (no
#      duplicate label/comment); and a later PASS (diff no longer touches a
#      critical file) clears the label and posts a one-time cleared notice.
#
# Usage:
#   ./.loom/scripts/tests/test-champion-critical-file-check.sh

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TEST_DIR/.." && pwd)"

# Two `..` reaches repo-root/.claude/commands/loom for an INSTALLED copy
# (SCRIPTS_DIR is .loom/scripts there); one `..` reaches defaults/.claude/
# commands/loom when running inside this source repo (SCRIPTS_DIR is
# defaults/scripts) -- the two layouts differ in depth, so probe both rather
# than hard-coding one (#6725).
if [[ -d "$SCRIPTS_DIR/../../.claude/commands/loom" ]]; then
    PROMPT_DIR="$(cd "$SCRIPTS_DIR/../../.claude/commands/loom" && pwd)"
else
    PROMPT_DIR="$(cd "$SCRIPTS_DIR/../.claude/commands/loom" && pwd)"
fi
CHAMPION_MD="$PROMPT_DIR/champion-pr-merge.md"

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
    "migrations/"
    "_migration.py"
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

# =====================================================================
# Version-only diff carve-out (#6147), mirrored verbatim from criterion #3's
# `version_only_diff()` in champion-pr-merge.md. The doc's version reads the
# diff via `gh api .../pulls/<n>/files --jq '... | .patch'`; this mirror
# takes the patch text directly (stdin) so it can be exercised against fixed
# fixtures without a live PR.
# =====================================================================
version_only_diff_from_patch() {
    local file="$1"
    local pattern
    case "$file" in
        package.json|mcp-loom/package.json|mcp-loom/package-lock.json)
            pattern='^[+-][[:space:]]*"version":[[:space:]]*"[0-9]+\.[0-9]+\.[0-9]+",?[[:space:]]*$'
            ;;
        loom-daemon/Cargo.toml|loom-api/Cargo.toml|Cargo.lock)
            pattern='^[+-]version = "[0-9]+\.[0-9]+\.[0-9]+"[[:space:]]*$'
            ;;
        *)
            return 1
            ;;
    esac

    local bad_lines
    bad_lines=$(grep -E '^[+-]' | grep -vE '^(\+\+\+|---)' | grep -vE "$pattern")
    [ -z "$bad_lines" ]
}

# champion_critical_file_check, extended to apply the version-only carve-out.
# $2 (optional, per-invocation) supplies a patch-lookup function name; when a
# critical-pattern match is one of the 6 version-bearing files, that function
# is called as `"$patch_lookup_fn" "$file"` and piped into
# version_only_diff_from_patch to decide PASS vs FAIL for that file alone.
champion_critical_file_check_with_carveout() {
    local patch_lookup_fn="$1"
    local file
    while IFS= read -r file; do
        [ -z "$file" ] && continue
        for pattern in "${CRITICAL_PATTERNS[@]}"; do
            if [[ "$file" == *"$pattern"* ]]; then
                if "$patch_lookup_fn" "$file" | version_only_diff_from_patch "$file"; then
                    echo "PASS (version-only carve-out): $file"
                else
                    echo "FAIL: $file"
                    return 0
                fi
                continue 2
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
echo "--- champion_critical_file_check: 'migration' pattern false positive on docs/migration/ (#5723) ---"

# docs/migration/*.md is a real, intentional repo convention (this repo's own
# CLAUDE.md links to docs/migration/v0.10.0-shepherd-deprecation.md) — it must
# NOT be treated as a critical database-migration file.
fixture=$'README.md\ndocs/migration/v0.10.0-shepherd-deprecation.md\ndocs/migration/daemon-state-consumers.md'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "PASS" "$out" "docs/migration/*.md files pass (not treated as critical migration files)"

# Genuine database migration files must still be caught.
fixture=$'src/lib.rs\ndb/migrations/003_add_column.sql'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: db/migrations/003_add_column.sql" "$out" \
    "a file inside a */migrations/* directory is still caught"

fixture=$'src/lib.rs\npolls/migrations/0001_initial.py'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: polls/migrations/0001_initial.py" "$out" \
    "a nested (Django-style) migrations/ .py file is still caught"

# Root-level `migrations/` directories must be caught too — the pattern has no
# leading `/`, so it is not restricted to nested directories. A leading-slash
# form ("/migrations/") silently missed these, which is Alembic's and
# Flask-Migrate's actual default `alembic init migrations` output layout
# (`migrations/versions/*.py` at the repo root) — a non-`.sql` migration script
# there would have bypassed the critical-file safety net entirely (#5723).
fixture=$'src/lib.rs\nmigrations/0001_initial.py'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: migrations/0001_initial.py" "$out" \
    "a root-level migrations/ .py file is caught (no leading-slash requirement)"

fixture=$'src/lib.rs\nmigrations/versions/0001_add.py'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: migrations/versions/0001_add.py" "$out" \
    "Alembic/Flask-Migrate's default root-level migrations/versions/*.py layout is caught"

fixture=$'src/lib.rs\nbackend/0001_initial_migration.py'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "FAIL: backend/0001_initial_migration.py" "$out" \
    "a *_migration.py single-file migration script is still caught"

# Edge case (explicitly decided, see #5723): a doc file whose name merely
# contains "migration" as a substring with no directory/suffix convention
# match (no "migrations/" dir, no "_migration.py" suffix) is NOT a database
# migration file and must PASS, same as docs/migration/*.md above.
fixture="docs/migration-notes.md"
out="$(printf '%s\n' "$fixture" | champion_critical_file_check)"
assert_eq "PASS" "$out" \
    "docs/migration-notes.md (bare 'migration' substring, no directory/suffix convention) passes"

echo
echo "--- version_only_diff_from_patch: real PR #6118 version-bump diff shapes carve out cleanly (#6147) ---"

# Fixture patch bodies copied verbatim (patch-line shape) from PR #6118's
# actual diff for each of the 6 version-bearing files.
pr6118_patch_loom_api_cargo_toml=$'@@ -1,6 +1,6 @@\n [package]\n name = "loom-api"\n-version = "0.18.38"\n+version = "0.18.39"\n edition = "2021"\n description = "External REST API for Loom analytics data access"\n '
pr6118_patch_loom_daemon_cargo_toml=$'@@ -1,6 +1,6 @@\n [package]\n name = "loom-daemon"\n-version = "0.18.38"\n+version = "0.18.39"\n edition = "2021"\n \n [dependencies]'
pr6118_patch_cargo_lock=$'@@ -1247,7 +1247,7 @@ checksum = "0ceec5bc11778974d1bcb055b18002eba7f4b3518b6a0081b3af5f21666da9ad"\n \n [[package]]\n name = "loom-api"\n-version = "0.18.38"\n+version = "0.18.39"\n dependencies = [\n "anyhow",\n "axum",\n@@ -1265,7 +1265,7 @@ dependencies = [\n \n [[package]]\n name = "loom-daemon"\n-version = "0.18.38"\n+version = "0.18.39"\n dependencies = [\n "anyhow",'
pr6118_patch_package_json=$'@@ -1,6 +1,6 @@\n {\n   "name": "loom",\n-  "version": "0.18.38",\n+  "version": "0.18.39",\n   "description": "AI-powered development orchestration...",\n   "type": "module",'
pr6118_patch_mcp_package_json=$'@@ -1,6 +1,6 @@\n {\n   "name": "@loom/mcp",\n-  "version": "0.18.38",\n+  "version": "0.18.39",\n   "description": "Unified MCP server for Loom",\n   "type": "module",'
pr6118_patch_mcp_package_lock_json=$'@@ -1,12 +1,12 @@\n {\n   "name": "@loom/mcp",\n-  "version": "0.18.38",\n+  "version": "0.18.39",\n   "lockfileVersion": 3,\n   "requires": true,\n   "packages": {\n     "": {\n       "name": "@loom/mcp",\n-      "version": "0.18.38",\n+      "version": "0.18.39",\n       "dependencies": {'

if printf '%s\n' "$pr6118_patch_loom_api_cargo_toml" | version_only_diff_from_patch "loom-api/Cargo.toml"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's loom-api/Cargo.toml diff is recognized as version-only"

if printf '%s\n' "$pr6118_patch_loom_daemon_cargo_toml" | version_only_diff_from_patch "loom-daemon/Cargo.toml"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's loom-daemon/Cargo.toml diff is recognized as version-only"

if printf '%s\n' "$pr6118_patch_cargo_lock" | version_only_diff_from_patch "Cargo.lock"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's Cargo.lock diff (two [[package]] version bumps) is recognized as version-only"

if printf '%s\n' "$pr6118_patch_package_json" | version_only_diff_from_patch "package.json"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's package.json diff is recognized as version-only"

if printf '%s\n' "$pr6118_patch_mcp_package_json" | version_only_diff_from_patch "mcp-loom/package.json"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's mcp-loom/package.json diff is recognized as version-only"

if printf '%s\n' "$pr6118_patch_mcp_package_lock_json" | version_only_diff_from_patch "mcp-loom/package-lock.json"; then
    r=0; else r=1; fi
assert_eq "0" "$r" "PR #6118's mcp-loom/package-lock.json diff (two version lines) is recognized as version-only"

echo
echo "--- version_only_diff_from_patch: a real (non-version) change to a critical file still fails (#6147) ---"

# A genuine dependency bump alongside the version line — the carve-out must
# NOT apply; this file still fails criterion #3 as before.
real_dep_change_cargo_toml=$'@@ -1,7 +1,7 @@\n [package]\n name = "loom-api"\n-version = "0.18.38"\n+version = "0.18.39"\n edition = "2021"\n \n [dependencies]\n-anyhow = "1.0"\n+anyhow = "1.1"'
if printf '%s\n' "$real_dep_change_cargo_toml" | version_only_diff_from_patch "loom-api/Cargo.toml"; then
    r=0; else r=1; fi
assert_eq "1" "$r" \
    "a Cargo.toml diff with a real dependency-version change (not just the package version) still fails the carve-out"

# A new field added alongside the version bump in package.json.
real_new_field_package_json=$'@@ -1,7 +1,8 @@\n {\n   "name": "loom",\n-  "version": "0.18.38",\n+  "version": "0.18.39",\n+  "private": true,\n   "description": "...",\n   "type": "module",'
if printf '%s\n' "$real_new_field_package_json" | version_only_diff_from_patch "package.json"; then
    r=0; else r=1; fi
assert_eq "1" "$r" \
    "a package.json diff with a new field alongside the version bump still fails the carve-out"

# A critical file NOT in the 6-file allowlist is never eligible for the
# carve-out, even with a version-only-shaped diff — e.g. a hypothetical
# some-crate/Cargo.toml.
version_only_shaped_other_toml=$'@@ -1,3 +1,3 @@\n [package]\n-version = "1.2.3"\n+version = "1.2.4"'
if printf '%s\n' "$version_only_shaped_other_toml" | version_only_diff_from_patch "some-crate/Cargo.toml"; then
    r=0; else r=1; fi
assert_eq "1" "$r" \
    "a critical Cargo.toml outside the 6-file allowlist is never eligible for the carve-out, even with a version-only-shaped diff"

echo
echo "--- champion_critical_file_check_with_carveout: full check-loop integration (#6147) ---"

# Patch-lookup stub used by the integration tests below: dispatches by
# filename to the fixtures already defined.
patch_lookup_pr6118_clean() {
    case "$1" in
        loom-api/Cargo.toml) printf '%s\n' "$pr6118_patch_loom_api_cargo_toml" ;;
        loom-daemon/Cargo.toml) printf '%s\n' "$pr6118_patch_loom_daemon_cargo_toml" ;;
        Cargo.lock) printf '%s\n' "$pr6118_patch_cargo_lock" ;;
        package.json) printf '%s\n' "$pr6118_patch_package_json" ;;
        mcp-loom/package.json) printf '%s\n' "$pr6118_patch_mcp_package_json" ;;
        mcp-loom/package-lock.json) printf '%s\n' "$pr6118_patch_mcp_package_lock_json" ;;
        *) printf '' ;;
    esac
}

fixture=$'defaults/scripts/merge-pr.sh\nloom-api/Cargo.toml\nloom-daemon/Cargo.toml\nCargo.lock\npackage.json\nmcp-loom/package.json\nmcp-loom/package-lock.json'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check_with_carveout patch_lookup_pr6118_clean)"
# The loop only calls version_only_diff on files that match a CRITICAL_PATTERNS
# entry in the first place; per the current pattern list that is
# loom-api/Cargo.toml, loom-daemon/Cargo.toml, package.json, and
# mcp-loom/package.json (Cargo.lock and mcp-loom/package-lock.json don't match
# any pattern substring today, so they never even reach version_only_diff —
# harmless, and version_only_diff still recognizes them defensively in case
# CRITICAL_PATTERNS is ever extended to cover lockfiles). The overall result
# must have no FAIL line and end on the loop's final "PASS".
last_line="$(printf '%s' "$out" | tail -1)"
fail_count="$(printf '%s\n' "$out" | grep -c '^FAIL:' || true)"
assert_eq "PASS" "$last_line" \
    "a PR #6118-shaped file list (6 version-only critical files + one non-critical substantive file) ends on overall PASS"
assert_eq "0" "$fail_count" \
    "a PR #6118-shaped file list (6 version-only critical files + one non-critical substantive file) produces zero FAIL lines"
assert_eq "PASS (version-only carve-out): loom-api/Cargo.toml
PASS (version-only carve-out): loom-daemon/Cargo.toml
PASS (version-only carve-out): package.json
PASS (version-only carve-out): mcp-loom/package.json
PASS" "$out" \
    "the 4 files that match a CRITICAL_PATTERNS entry (loom-api/Cargo.toml, loom-daemon/Cargo.toml, package.json, mcp-loom/package.json) each pass via the version-only carve-out"

# Same file list, but loom-api/Cargo.toml now carries a real dependency
# change too — the whole check must fail again, on that file.
patch_lookup_pr6118_dirty() {
    case "$1" in
        loom-api/Cargo.toml) printf '%s\n' "$real_dep_change_cargo_toml" ;;
        loom-daemon/Cargo.toml) printf '%s\n' "$pr6118_patch_loom_daemon_cargo_toml" ;;
        Cargo.lock) printf '%s\n' "$pr6118_patch_cargo_lock" ;;
        package.json) printf '%s\n' "$pr6118_patch_package_json" ;;
        mcp-loom/package.json) printf '%s\n' "$pr6118_patch_mcp_package_json" ;;
        mcp-loom/package-lock.json) printf '%s\n' "$pr6118_patch_mcp_package_lock_json" ;;
        *) printf '' ;;
    esac
}

fixture=$'defaults/scripts/merge-pr.sh\nloom-api/Cargo.toml\nloom-daemon/Cargo.toml\nCargo.lock\npackage.json\nmcp-loom/package.json\nmcp-loom/package-lock.json'
out="$(printf '%s\n' "$fixture" | champion_critical_file_check_with_carveout patch_lookup_pr6118_dirty)"
assert_eq "FAIL: loom-api/Cargo.toml" "$out" \
    "the same file list still fails criterion #3 when one version-bearing file also carries a real dependency change"

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
echo "--- Doc pins: shipped markdown no longer uses the bare 'migration' substring pattern (#5723) ---"

assert_doc_lacks "$CHAMPION_MD" \
    '"migration"' \
    "CRITICAL_PATTERNS array no longer contains the bare 'migration' substring pattern"

assert_doc_lacks "$CHAMPION_MD" \
    '`*migration*` - database migration files' \
    "prose critical-file-patterns bullet list no longer contains the bare *migration* pattern"

assert_doc_lacks "$CHAMPION_MD" \
    '"/migrations/"' \
    "CRITICAL_PATTERNS array no longer uses the leading-slash form that missed root-level migrations/ dirs"

assert_doc_contains "$CHAMPION_MD" \
    '"migrations/"' \
    "CRITICAL_PATTERNS array ships the narrower migrations/ directory pattern"

assert_doc_contains "$CHAMPION_MD" \
    '"_migration.py"' \
    "CRITICAL_PATTERNS array ships the narrower _migration.py suffix pattern"

assert_doc_contains "$CHAMPION_MD" \
    "#5723" \
    "champion-pr-merge.md documents the #5723 docs/migration/ false-positive fix"

echo
echo "--- Doc pins: shipped markdown ships the version-only diff carve-out (#6147) ---"

assert_doc_contains "$CHAMPION_MD" \
    'version_only_diff() {' \
    "criterion #3 defines the version_only_diff() carve-out function"

assert_doc_contains "$CHAMPION_MD" \
    'package.json|mcp-loom/package.json|mcp-loom/package-lock.json)' \
    "version_only_diff() case-matches the 3 JSON version-bearing files exactly (not by substring)"

assert_doc_contains "$CHAMPION_MD" \
    'loom-daemon/Cargo.toml|loom-api/Cargo.toml|Cargo.lock)' \
    "version_only_diff() case-matches the 3 TOML-style version-bearing files exactly (not by substring)"

assert_doc_contains "$CHAMPION_MD" \
    'pattern='"'"'^[+-][[:space:]]*"version":[[:space:]]*"[0-9]+\.[0-9]+\.[0-9]+",?[[:space:]]*$'"'" \
    "version_only_diff() ships the JSON version-line pattern"

assert_doc_contains "$CHAMPION_MD" \
    'pattern='"'"'^[+-]version = "[0-9]+\.[0-9]+\.[0-9]+"[[:space:]]*$'"'" \
    "version_only_diff() ships the TOML version-line pattern"

assert_doc_contains "$CHAMPION_MD" \
    'if version_only_diff "$file" <number>; then' \
    "criterion #3's check-loop calls version_only_diff before failing on a critical-pattern match"

assert_doc_contains "$CHAMPION_MD" \
    "PASS (version-only carve-out)" \
    "criterion #3's check-loop emits the carve-out PASS line so it can be reused verbatim in a Champion comment"

assert_doc_contains "$CHAMPION_MD" \
    "#6147" \
    "champion-pr-merge.md documents the #6147 version-only carve-out fix"

assert_doc_contains "$CHAMPION_MD" \
    "Verified against PR #6118 (#6147)" \
    "champion-pr-merge.md records verification against the real PR #6118 diff shape"

# =====================================================================
# Durable critical-file hold (#6879): a critical-file FAIL is a one-way
# terminal state (nothing about a diff's critical-file-ness changes without a
# human decision or a later push that narrows the diff), so criterion #3 now
# applies its own durable hold (`loom:operator` + `<!-- champion:critical-
# file-hold -->`) instead of routing through the shared "Transient failures"
# template. Mirrored here (same rationale as the mirrors above: the criterion
# is prose an LLM instance executes, not a standalone script) from
# `champion-pr-merge.md`'s "Safety Criteria → 3. Critical File Exclusion
# Check → Durable hold on FAIL" subsection.
#
# Unlike criterion #2's merge-risk hold, this one needs no sticky-hold
# precheck machinery: the check-loop above is a deterministic file-pattern
# match, not a judgment call, so the FAIL/PASS verdict recomputed fresh every
# tick IS the release signal — "last marker wins" is enough state to track.
# =====================================================================

assert_contains() {
    local haystack="$1" needle="$2" msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -qF -- "$needle" <<<"$haystack"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Missing '$needle' in:"
        sed 's/^/      /' <<<"$haystack"
    fi
}

assert_lacks() {
    local haystack="$1" needle="$2" msg="$3"
    TESTS_RUN=$((TESTS_RUN + 1))
    if grep -qF -- "$needle" <<<"$haystack"; then
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}FAIL${NC}: $msg"
        echo "    Unexpectedly found '$needle' in:"
        sed 's/^/      /' <<<"$haystack"
    else
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}PASS${NC}: $msg"
    fi
}

# STATE_FILE holds exactly one line: "none" | "held" | "cleared" — the state
# implied by whichever of the two markers was posted LAST, mirroring the
# doc's "last comment matching either marker prefix" lookup without needing
# real timestamps (a deterministic check-loop makes this sufficient — see the
# doc's own rationale for skipping criterion #2's sticky-hold machinery).
hold_state_get() {
    local f="$1"
    [[ -f "$f" ]] && cat "$f" || echo "none"
}
hold_state_set() {
    printf '%s' "$2" >"$1"
}

# LABEL_FILE tracks whether loom:operator is currently applied ("1"/"0").
label_get() {
    local f="$1"
    [[ -f "$f" ]] && cat "$f" || echo "0"
}
label_set() {
    printf '%s' "$2" >"$1"
}

# Simulates one Champion tick of criterion #3's durable-hold logic, mirroring
# the doc's FAIL / CURRENTLY_HELD branches verbatim. Reads a newline-separated
# file list on stdin (same input shape as champion_critical_file_check).
# Emits one ACTION line per observable forge effect (comment posted, label
# added/removed) so a test can assert on them without a live PR.
champion_critical_file_hold_tick() {
    local hold_state_file="$1" label_file="$2"
    local result currently_held

    result="$(champion_critical_file_check)"

    case "$(hold_state_get "$hold_state_file")" in
        held) currently_held=true ;;
        *) currently_held=false ;;
    esac

    if [[ "$result" == FAIL:* ]]; then
        if [[ "$currently_held" == true ]]; then
            echo "HOLD:stands"
        else
            echo "COMMENT:champion:critical-file-hold"
            hold_state_set "$hold_state_file" "held"
        fi
        echo "LABEL_ADD:loom:operator"
        label_set "$label_file" "1"
    elif [[ "$currently_held" == true ]]; then
        echo "COMMENT:champion:critical-file-hold-cleared"
        echo "LABEL_REMOVE:loom:operator"
        hold_state_set "$hold_state_file" "cleared"
        label_set "$label_file" "0"
    else
        echo "PASS:no-hold"
    fi
}

echo
echo "--- critical-file hold: label + marker applied on first rejection (#6879) ---"

HS="$(mktemp)"
LF="$(mktemp)"
rm -f "$HS" "$LF"

critical_fixture=$'src/lib.rs\n.github/workflows/new-ci-job.yml'
out="$(printf '%s\n' "$critical_fixture" | champion_critical_file_hold_tick "$HS" "$LF")"
assert_contains "$out" "COMMENT:champion:critical-file-hold" \
    "a fresh critical-file FAIL posts the champion:critical-file-hold marker comment"
assert_contains "$out" "LABEL_ADD:loom:operator" \
    "a fresh critical-file FAIL adds the loom:operator label"
assert_eq "held" "$(hold_state_get "$HS")" \
    "hold state is recorded as held after the first rejection"
assert_eq "1" "$(label_get "$LF")" \
    "loom:operator is applied after the first rejection"

echo
echo "--- critical-file hold: no duplicate label/comment on a repeated rejection with an unchanged file set (idempotency, #6879) ---"

out2="$(printf '%s\n' "$critical_fixture" | champion_critical_file_hold_tick "$HS" "$LF")"
assert_contains "$out2" "HOLD:stands" \
    "a repeated FAIL with an existing hold is recognized as still-held"
comment_count="$(grep -c '^COMMENT:' <<<"$out2" || true)"
assert_eq "0" "$comment_count" \
    "a repeated FAIL with an existing hold posts NO new comment (idempotency guard)"
assert_eq "held" "$(hold_state_get "$HS")" \
    "hold state remains held (unchanged) across the repeated rejection"
assert_eq "1" "$(label_get "$LF")" \
    "loom:operator remains applied (label add is idempotent) across the repeated rejection"

echo
echo "--- critical-file hold: label/marker cleared when a later push no longer touches a critical file (#6879) ---"

clean_fixture=$'src/lib.rs\nsrc/other.rs'
out3="$(printf '%s\n' "$clean_fixture" | champion_critical_file_hold_tick "$HS" "$LF")"
assert_contains "$out3" "COMMENT:champion:critical-file-hold-cleared" \
    "a PASS after a prior hold posts the champion:critical-file-hold-cleared marker comment"
assert_contains "$out3" "LABEL_REMOVE:loom:operator" \
    "a PASS after a prior hold removes the loom:operator label"
assert_eq "cleared" "$(hold_state_get "$HS")" \
    "hold state is recorded as cleared once the diff no longer matches a critical pattern"
assert_eq "0" "$(label_get "$LF")" \
    "loom:operator is removed once the diff no longer matches a critical pattern"

out4="$(printf '%s\n' "$clean_fixture" | champion_critical_file_hold_tick "$HS" "$LF")"
assert_eq "PASS:no-hold" "$out4" \
    "an ordinary PASS with no prior hold produces no forge side effects (no re-clearing an already-cleared hold)"

out5="$(printf '%s\n' "$critical_fixture" | champion_critical_file_hold_tick "$HS" "$LF")"
assert_contains "$out5" "COMMENT:champion:critical-file-hold" \
    "a fresh critical-file touch after a cleared hold starts a NEW hold episode (fresh comment)"
assert_eq "held" "$(hold_state_get "$HS")" \
    "hold state re-enters held for the new episode"

rm -f "$HS" "$LF"

echo
echo "--- Doc pins: shipped markdown ships the durable critical-file hold (#6879) ---"

assert_doc_contains "$CHAMPION_MD" \
    'HOLD_MARKER="<!-- champion:critical-file-hold -->"' \
    "criterion #3 defines its own durable-hold marker, distinct from criterion #2's champion:merge-risk-hold"

assert_doc_contains "$CHAMPION_MD" \
    'CLEARED_MARKER="<!-- champion:critical-file-hold-cleared -->"' \
    "criterion #3 defines a distinct cleared-marker for the release path"

assert_doc_contains "$CHAMPION_MD" \
    'gh pr edit "$PR_NUMBER" --add-label "loom:operator"' \
    "criterion #3's durable-hold path applies loom:operator (#5502) on FAIL"

assert_doc_contains "$CHAMPION_MD" \
    'gh pr edit "$PR_NUMBER" --remove-label "loom:operator"' \
    "criterion #3's release path removes loom:operator once the diff no longer touches a critical file"

assert_doc_contains "$CHAMPION_MD" \
    "#6879" \
    "champion-pr-merge.md documents the #6879 durable critical-file-hold fix"

assert_doc_lacks "$CHAMPION_MD" \
    '- **critical-file**: the sorted, comma-joined list of touched critical file paths.' \
    "critical-file is no longer keyed through the shared transient-failure REASON_KEY table — it has its own durable-hold path (#6879)"

assert_doc_lacks "$CHAMPION_MD" \
    'label-check | size-check | critical-file |' \
    "the shared CRITERION_KEY slug comment no longer lists critical-file among the transient-failure criteria (#6879)"

echo
echo "Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
