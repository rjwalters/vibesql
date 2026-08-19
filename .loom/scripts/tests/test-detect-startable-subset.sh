#!/usr/bin/env bash
# test-detect-startable-subset.sh - Tests for detect-startable-subset.sh
# (issue #5664, "recurred after closure").
#
# Champion's dependency handling only ever answered questions about the WHOLE
# issue ("is the blocker closed", "is there a cycle"). A proposal can declare a
# `## Startable Subset` naming part of its work that does not depend on an open
# blocker at all -- an explicit split point an architect stated so a Builder
# could land the unblocked half first. This suite covers the pure parsing
# function directly, and the CLI wrapper black-box via `--body-file` (no forge
# stub needed for that path).
#
# Hermetic: no network, no live forge, no tokens.
#
# Usage:
#   ./.loom/scripts/tests/test-detect-startable-subset.sh

set -uo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$TEST_DIR/.." && pwd)"
DEFAULTS_DIR="$(cd "$SCRIPTS_DIR/.." && pwd)"
DSS="$SCRIPTS_DIR/detect-startable-subset.sh"
CHAMPION_PROMO_MD="$DEFAULTS_DIR/.claude/commands/loom/champion-issue-promo.md"

# shellcheck source=/dev/null
source "$DSS"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1)); echo -e "  ${GREEN}PASS${NC}: $1"; }
fail() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); echo -e "  ${RED}FAIL${NC}: $1"; }

assert_eq() {
    local expected="$1" actual="$2" msg="$3"
    if [[ "$expected" == "$actual" ]]; then
        pass "$msg"
    else
        fail "$msg"
        echo "    Expected: '$expected'"
        echo "    Actual:   '$actual'"
    fi
}

assert_contains() {
    local haystack="$1" needle="$2" msg="$3"
    if [[ "$haystack" == *"$needle"* ]]; then
        pass "$msg"
    else
        fail "$msg"
        echo "    Expected to contain: '$needle'"
        echo "    Actual: '$haystack'"
    fi
}

assert_true() { if "$@"; then pass "${!#}"; else fail "${!#}"; fi; }
assert_false() { if "$@"; then fail "${!#}"; else pass "${!#}"; fi; }

assert_doc_contains() {
    local file="$1" needle="$2" msg="$3"
    if grep -qF -- "$needle" "$file"; then
        pass "$msg"
    else
        fail "$msg (missing literal in $file: $needle)"
    fi
}

# =====================================================================
# Unit tests: extract_startable_subset / has_startable_subset
# =====================================================================

echo "--- extract_startable_subset: the declared section, verbatim ---"

# shellcheck disable=SC2016  # literal text, not an expansion
BODY_WITH_SUBSET='## Summary
A proposal that hard-depends on #1 for most of its scope.

## Startable Subset

The comparator and mutation tests need only `warmup/01_netlist.v`, which is
already published upstream -- independent of the blocked RTL deliverable.

## Dependencies
- [ ] #1'

out="$(extract_startable_subset "$BODY_WITH_SUBSET")"
assert_contains "$out" "comparator and mutation tests" "the subset section body is captured"
assert_contains "$out" 'warmup/01_netlist.v' "the named file is captured"
assert_eq "0" "$(printf '%s\n' "$out" | grep -c '## Dependencies' || true)" \
    "the next heading (## Dependencies) is NOT captured"

out="$(extract_startable_subset '## Startable Subset (independent of #3)
Do X and Y.

### Files
- warmup/01_netlist.v

## Dependencies
- [ ] #3')"
assert_contains "$out" "Do X and Y." "a heading with trailing parenthetical text still matches"
assert_contains "$out" "### Files" "a DEEPER subsection (###) stays inside the captured section"
assert_contains "$out" "warmup/01_netlist.v" "content under the deeper subsection is captured"

out="$(extract_startable_subset '## Summary
No startable subset section here at all.')"
assert_eq "" "$out" "a body with no Startable Subset heading yields nothing"

out="$(extract_startable_subset '## Startable Subset

## Dependencies
- [ ] #3')"
assert_eq "" "$out" "an EMPTY Startable Subset section (heading with no body) yields nothing"

out="$(extract_startable_subset '### Startable Subset
Deeper heading depth still matches (### as well as ##).
## Next section
unrelated')"
assert_contains "$out" "Deeper heading depth" "### (not just ##) is recognized as a heading depth"
assert_eq "0" "$(printf '%s\n' "$out" | grep -c 'unrelated' || true)" \
    "capture still stops at the next heading of equal-or-shallower depth"

out="$(extract_startable_subset '## STARTABLE SUBSET
Case-insensitive heading match.')"
assert_contains "$out" "Case-insensitive" "the heading match is case-insensitive"

echo
echo "--- has_startable_subset ---"

assert_true has_startable_subset "$BODY_WITH_SUBSET" "a body with a non-empty section reports true"
assert_false has_startable_subset "## Summary
Nothing here." "a body with no section reports false"
assert_false has_startable_subset "## Startable Subset

## Dependencies" "a body with an EMPTY section reports false (whitespace-only is not a declaration)"

# =====================================================================
# Black-box: --body-file (no forge stub needed for this path)
# =====================================================================

echo
echo "--- CLI: --body-file ---"

TMP_BODY="$(mktemp)"
trap 'rm -f "$TMP_BODY"' EXIT

printf '%s' "$BODY_WITH_SUBSET" > "$TMP_BODY"
OUT="$("$DSS" --issue 3 --body-file "$TMP_BODY")"; RC=$?
assert_eq "0" "$RC" "exit 0 when a subset is declared"
assert_contains "$OUT" "STARTABLE_SUBSET" "STARTABLE_SUBSET marker present"
assert_contains "$OUT" "comparator and mutation tests" "the subset text is printed"

printf '%s' '## Summary
No subset.' > "$TMP_BODY"
OUT="$("$DSS" --issue 3 --body-file "$TMP_BODY")"; RC=$?
assert_eq "1" "$RC" "exit 1 when no subset is declared"
assert_contains "$OUT" "NO_STARTABLE_SUBSET" "NO_STARTABLE_SUBSET marker present"

echo
echo "--- CLI: argument validation ---"

OUT="$("$DSS" --repo o/r 2>&1)"; RC=$?
assert_eq "2" "$RC" "missing --issue exits 2"

OUT="$("$DSS" --issue notanumber --repo o/r 2>&1)"; RC=$?
assert_eq "2" "$RC" "non-numeric --issue exits 2"

OUT="$("$DSS" --issue 3 --body-file /nonexistent/path 2>&1)"; RC=$?
assert_eq "2" "$RC" "a missing --body-file exits 2"

# =====================================================================
# Doc pins: the Champion prose actually calls the detector
# =====================================================================

echo
echo "--- Doc pins: champion-issue-promo.md wires the carve-out ---"

assert_doc_contains "$CHAMPION_PROMO_MD" "detect-startable-subset.sh" \
    "champion-issue-promo.md invokes detect-startable-subset.sh"
assert_doc_contains "$CHAMPION_PROMO_MD" "Startable Subset" \
    "the convention (## Startable Subset) is documented in the role file"
assert_doc_contains "$CHAMPION_PROMO_MD" "Part of #" \
    "Champion's promotion guidance directs the Builder to the partial-increment convention"

echo
echo "Results: $TESTS_PASSED/$TESTS_RUN passed, $TESTS_FAILED failed"
[[ $TESTS_FAILED -eq 0 ]] || exit 1
