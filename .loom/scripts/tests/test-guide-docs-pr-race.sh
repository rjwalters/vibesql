#!/usr/bin/env bash
# test-guide-docs-pr-race.sh - Regression suite: does the existing
# single-writer discipline for Guide's Document Maintenance phase (Step 1's
# open-docs-PR check + Step 5's uncached OPEN_DOCS_PR_RECHECK, #5573/#5615)
# actually bound a multi-host race on the SAME debounce-eligible delta to
# exactly one `gh pr create` (Issue #6327).
#
# ## Why this suite exists
#
# Issue #6327 was filed observing N-host duplicate `docs: Guide document
# maintenance update` PRs and initially asked for a generic lease/claim
# primitive for this phase -- "the way sweeps now have it" (#6165). Live
# forge-history verification (see the issue's "Verified corrections" +
# "Curator Enhancement" sections) found that premise incomplete: the
# lock+recheck combination this suite exercises was ALREADY implemented
# (`docs-guide-lock.sh`, #5573, same-host mkdir lock; the Step 5 uncached
# recheck, #5615, cross-host TOCTOU narrowing) before this issue was ever
# filed. What was missing was a REGRESSION TEST that actually simulates the
# interleaving and proves the combination holds, rather than asserting it by
# code-inspection/comments alone -- exactly the gap
# `test-sweep-lease-fence-race.sh` (#6315) closed for the analogous
# sweep-side lease fencing check. This suite mirrors that test's shape:
# extract the REAL command lines from the prompt, run them against a shared
# stubbed forge state across a sequence of simulated host attempts, and
# assert the aggregate `gh pr create` count.
#
# ## Why this is a separate suite from test-docs-guide-lock.sh
#
# `test-docs-guide-lock.sh` already unit-tests `docs-guide-lock.sh` itself
# (acquire/release/staleness reaping) in isolation. That is necessary but not
# sufficient: `docs-guide-lock.sh` is explicitly SAME-HOST ONLY (see its own
# header comment and guide.md's Step 1 "#5615 GAP" note) -- it can never, by
# itself, prove anything about a CROSS-HOST race, which is exactly the shape
# #6327 was filed against. This suite does not re-test the lock; it tests
# the Step 1 check + Step 5 recheck PAIR that guide.md documents as the
# cross-host mitigation, using a harness that models two-or-more INDEPENDENT
# hosts (no shared lock file) contending for the same forge state.
#
# ## Harness shape
#
# A fake shared "forge" is a JSON array of open docs-maintenance PRs in a
# temp file. `simulate_tick <host>` runs the EXACT Step 1 line (extracted
# verbatim from guide.md, using `$GH_READ`) and, if it finds no open PR, the
# EXACT Step 5 recheck line (also extracted verbatim, using bare `gh` per
# the #5615 "deliberately uncached" requirement) against that shared store.
# Only if BOTH checks come back empty does the simulated tick "create" a PR
# -- appending an entry to the shared store and to `created.log`. Ticks are
# invoked in an explicit, documented order that models a specific timeline
# of forge-visible events (identical technique to
# `test-sweep-lease-fence-race.sh`'s `simulate_sweep_attempt` sequencing --
# this is not real OS-level concurrency, it is a deterministic reconstruction
# of the sequence of forge reads/writes that a genuine race produces).
#
# ## Scope (read before assuming this "proves no race is possible")
#
# This suite proves the REALISTIC race shape guide.md's own Step 1 comment
# describes as what the recheck narrows the window DOWN TO: two-or-more hosts
# that both pass Step 1 near-simultaneously (both see "no open PR"), but
# whose Step 5 rechecks are NOT the same forge read -- i.e. the winner's
# `gh pr create` has already landed on the forge by the time a loser's
# recheck runs. Every Guide tick does substantial work (rendering doc bodies,
# git operations) between Step 1 and Step 5, so in practice two hosts'
# Step-5 moments are separated by real wall-clock time even when their
# Step-1 checks were nearly simultaneous -- this is the scenario the #5615
# fix actually targets, and the one the issue's own "Verified corrections"
# analysis found no counter-evidence against in live forge history.
# It deliberately does NOT claim to prove atomicity for the case where two
# hosts' Step 5 rechecks both read the SAME pre-create forge state (an
# exact simultaneous double-recheck) -- guide.md's own Step 1 comment
# already documents that residual gap ("narrowing... not a hard guarantee").
# Closing that residual sliver, if it is ever observed live, is exactly the
# "the new regression test finds the existing lock+recheck insufficient"
# trigger condition #6327 names for building a real lease primitive --
# which this suite does not attempt, per the issue's explicit instruction
# not to build one preemptively.
#
# Usage:
#   ./.loom/scripts/tests/test-guide-docs-pr-race.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# guide.md is shipped (installed at .claude/commands/loom/guide.md), so
# resolve it the way each layout actually lays it out: the installed path
# first (consumer repos, and Loom's own dogfooded checkout), falling back
# to the defaults/ source-tree path (a bare source checkout with no
# .claude/commands/loom/ copy yet). See issue #6194 / #6241.
if [[ -f "$REPO_ROOT/.claude/commands/loom/guide.md" ]]; then
    GUIDE_MD="$REPO_ROOT/.claude/commands/loom/guide.md"
else
    GUIDE_MD="$REPO_ROOT/defaults/.claude/commands/loom/guide.md"
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_PASSED=$((TESTS_PASSED + 1)); echo -e "  ${GREEN}PASS${NC}: $1"; }
fail() { TESTS_RUN=$((TESTS_RUN + 1)); TESTS_FAILED=$((TESTS_FAILED + 1)); echo -e "  ${RED}FAIL${NC}: $1"; }

assert_eq() {
    local actual="$1" expected="$2" msg="$3"
    if [[ "$actual" == "$expected" ]]; then pass "$msg"; else fail "$msg (got '$actual', expected '$expected')"; fi
}

assert_grep() {
    local pattern="$1" file="$2" msg="$3"
    if grep -qE "$pattern" "$file"; then pass "$msg"; else fail "$msg (missing pattern: $pattern)"; fi
}

if [[ ! -f "$GUIDE_MD" ]]; then
    echo -e "${RED}FATAL${NC}: guide.md not found at $GUIDE_MD"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo -e "${RED}FATAL${NC}: jq is required for this suite"
    exit 2
fi

# ---------------------------------------------------------------------------
# Test 1: the two guard lines exist as documented (Step 1 cached-OK read,
# Step 5 deliberately-uncached read) -- sanity check before we extract and
# execute them below.
# ---------------------------------------------------------------------------
echo "Test 1: guide.md defines the Step 1 check and Step 5 uncached recheck"

assert_grep 'OPEN_DOCS_PR=\$\("\$GH_READ" pr list --state open --search "head:docs/guide-update"' "$GUIDE_MD" \
    "Step 1's open-docs-PR check uses \$GH_READ (may be cached)"
assert_grep 'OPEN_DOCS_PR_RECHECK=\$\(gh pr list --state open --search "head:docs/guide-update"' "$GUIDE_MD" \
    "Step 5's recheck uses bare gh (deliberately uncached, #5615)"
assert_grep '#6327 CORRECTED UNDERSTANDING' "$GUIDE_MD" \
    "guide.md documents the #6327 corrected understanding near the Step 1 lock/recheck block"

# ---------------------------------------------------------------------------
# Extract the two guard lines VERBATIM so this suite can never silently drift
# from the actual prompt text (mirrors test-guide-work-log-debounce.sh's
# JQ_EXPR extraction style).
# ---------------------------------------------------------------------------
STEP1_LINE="$(grep -m1 '^OPEN_DOCS_PR=\$("\$GH_READ" pr list' "$GUIDE_MD")"
STEP5_LINE="$(grep -m1 '^  OPEN_DOCS_PR_RECHECK=\$(gh pr list' "$GUIDE_MD")"

if [[ -z "$STEP1_LINE" || -z "$STEP5_LINE" ]]; then
    echo -e "${RED}FATAL${NC}: could not extract Step 1 / Step 5 guard lines from guide.md"
    exit 2
fi

# ---------------------------------------------------------------------------
# Harness: a shared fake-forge store of open docs-maintenance PRs, and a stub
# `gh` that answers exactly the query shape both guard lines issue.
# ---------------------------------------------------------------------------
STUB_DIR="$(mktemp -d)"
trap 'rm -rf "$STUB_DIR" 2>/dev/null || true' EXIT

STORE="$STUB_DIR/open-prs.json"
CREATED_LOG="$STUB_DIR/created.log"
EVENT_LOG="$STUB_DIR/events.log"

cat > "$STUB_DIR/gh" <<'STUB'
#!/usr/bin/env bash
# Minimal stub: only handles the exact `pr list --state open --search
# "head:docs/guide-update" --json number --jq FILTER` query both the Step 1
# check and the Step 5 recheck issue. STORE points at the shared fake-forge
# JSON array of open docs PRs (each {"number": N}).
STORE="${LOOM_TEST_STORE:?stub gh: LOOM_TEST_STORE not set}"
if [[ "$1" == "pr" && "$2" == "list" ]]; then
  shift 2
  filter=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --jq) filter="$2"; shift 2 ;;
      *) shift ;;
    esac
  done
  jq -c "$filter" "$STORE"
  exit 0
fi
echo "stub gh: unhandled args: $*" >&2
exit 3
STUB
chmod +x "$STUB_DIR/gh"

export LOOM_TEST_STORE="$STORE"
export PATH="$STUB_DIR:$PATH"
export GH_READ="gh"   # matches guide.md's fallback when gh-cached is absent

reset_state() {
    echo '[]' > "$STORE"
    : > "$CREATED_LOG"
    : > "$EVENT_LOG"
}

next_pr_number() {
    # 1000 + however many PRs (open or historically created) already exist,
    # so numbers never collide across a scenario's whole timeline.
    local n
    n=$(( 1000 + $(wc -l < "$CREATED_LOG" | tr -d '[:space:]') ))
    echo "$n"
}

# simulate_tick <host>
#
# Runs the REAL Step 1 line, then (only if it found nothing) the REAL Step 5
# line, both eval'd verbatim against the shared stub. On a genuine pass of
# BOTH checks, "creates" a PR: appends it to the shared store (making it
# visible to every subsequent simulate_tick call) and to created.log (the
# side effect this suite counts). Logs which guard (if any) caused a skip.
simulate_tick() {
    local host="$1"
    local OPEN_DOCS_PR="" OPEN_DOCS_PR_RECHECK=""

    eval "$STEP1_LINE"
    if [[ -n "$OPEN_DOCS_PR" ]]; then
        echo "$host: Step 1 found open PR #$OPEN_DOCS_PR -- skip" >> "$EVENT_LOG"
        return
    fi

    eval "$STEP5_LINE"
    if [[ -n "$OPEN_DOCS_PR_RECHECK" ]]; then
        echo "$host: Step 5 recheck found open PR #$OPEN_DOCS_PR_RECHECK -- discard local commit, skip" >> "$EVENT_LOG"
        return
    fi

    local n
    n="$(next_pr_number)"
    jq --argjson n "$n" '. + [{"number": $n}]' "$STORE" > "$STORE.tmp" && mv "$STORE.tmp" "$STORE"
    echo "$host" >> "$CREATED_LOG"
    echo "$host: Step 1 and Step 5 both empty -- created PR #$n" >> "$EVENT_LOG"
}

created_count() { wc -l < "$CREATED_LOG" | tr -d '[:space:]'; }
created_contents() { cat "$CREATED_LOG"; }

# ============================================================================
# Scenario 1: sequential (non-racing) ticks -- host B's tick starts only
# after host A's whole tick (including its Step 1 check through PR creation)
# has already landed on the forge. This is the common case (staggered role-
# runner cadence) and the cheapest one for Step 1 alone to handle -- Step 5
# never even needs to fire for host B.
# ============================================================================
echo ""
echo "--- Scenario 1: sequential ticks -- Step 1 alone is enough ---"
reset_state
simulate_tick host-A
simulate_tick host-B
assert_eq "1" "$(created_count)" "(1) exactly one PR created across two sequential (non-racing) ticks"
assert_eq "host-A" "$(created_contents)" "(1) the first host's tick is the one that created the PR"
assert_grep "host-B: Step 1 found open PR" "$EVENT_LOG" \
    "(1) the second host's Step 1 check alone caught the already-open PR (Step 5 never needed to fire)"

# ============================================================================
# Scenario 2: genuine two-host race -- both hosts pass Step 1 while the
# store is still empty (interleaved starts), but host A reaches Step 5 (and
# creates) BEFORE host B's own Step 5 recheck runs -- the realistic shape
# the #5615 fix targets (near-simultaneous starts, staggered finishes, see
# "Scope" above). Modeled by NOT resetting the store between host A's full
# tick and host B's Step-1-already-passed continuation.
# ============================================================================
echo ""
echo "--- Scenario 2: interleaved starts, staggered finishes -- host A wins ---"
reset_state

# Both hosts' Step 1 checks run back-to-back while the store is still empty
# (this models the near-simultaneous start -- store genuinely has nothing
# open yet for EITHER read).
OPEN_DOCS_PR=""; eval "$STEP1_LINE"; STEP1_A="$OPEN_DOCS_PR"
OPEN_DOCS_PR=""; eval "$STEP1_LINE"; STEP1_B="$OPEN_DOCS_PR"
assert_eq "" "$STEP1_A" "(2) host A's Step 1 check finds nothing (store still empty at start)"
assert_eq "" "$STEP1_B" "(2) host B's Step 1 check ALSO finds nothing (genuine interleaved start, not sequential)"

# Host A now completes the rest of its tick first: Step 5 recheck (still
# empty) + create.
OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
assert_eq "" "$OPEN_DOCS_PR_RECHECK" "(2) host A's Step 5 recheck also finds nothing -- proceeds to create"
n="$(next_pr_number)"; jq --argjson n "$n" '. + [{"number": $n}]' "$STORE" > "$STORE.tmp" && mv "$STORE.tmp" "$STORE"
echo "host-A" >> "$CREATED_LOG"
echo "host-A: Step 1 and Step 5 both empty -- created PR #$n" >> "$EVENT_LOG"

# Host B's Step 5 recheck runs AFTER host A's create has landed on the
# (shared) store -- this is the #5615 mitigation actually firing.
OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
assert_eq "$n" "$OPEN_DOCS_PR_RECHECK" \
    "(2) host B's Step 5 recheck now sees host A's just-created PR (the uncached #5615 recheck catching the race)"

assert_eq "1" "$(created_count)" "(2) exactly one PR created across the whole race (not zero, not two)"
assert_eq "host-A" "$(created_contents)" "(2) the surviving create belongs to the winner (host A) only"

# ============================================================================
# Scenario 3: same race, reversed winner -- proves the outcome depends on
# forge-visible ordering, not any host-identity bias baked into the checks.
# ============================================================================
echo ""
echo "--- Scenario 3: interleaved starts, staggered finishes -- host B wins ---"
reset_state

OPEN_DOCS_PR=""; eval "$STEP1_LINE"; STEP1_A="$OPEN_DOCS_PR"
OPEN_DOCS_PR=""; eval "$STEP1_LINE"; STEP1_B="$OPEN_DOCS_PR"
assert_eq "" "$STEP1_A" "(3) host A's Step 1 check finds nothing"
assert_eq "" "$STEP1_B" "(3) host B's Step 1 check also finds nothing"

# This time host B finishes first.
OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
n="$(next_pr_number)"; jq --argjson n "$n" '. + [{"number": $n}]' "$STORE" > "$STORE.tmp" && mv "$STORE.tmp" "$STORE"
echo "host-B" >> "$CREATED_LOG"
echo "host-B: Step 1 and Step 5 both empty -- created PR #$n" >> "$EVENT_LOG"

OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
assert_eq "$n" "$OPEN_DOCS_PR_RECHECK" "(3) host A's Step 5 recheck now sees host B's just-created PR"

assert_eq "1" "$(created_count)" "(3) exactly one PR created (not zero, not two) -- winner determined by finish order, not identity"
assert_eq "host-B" "$(created_contents)" "(3) the surviving create belongs to the winner (host B) only"

# ============================================================================
# Scenario 4: three-host interleave on the same debounce-eligible delta --
# the acceptance criteria's "≥2 dispatchers" case, exercised at N=3. All
# three pass Step 1 while the store is empty; only the first to reach Step 5
# succeeds, the other two's rechecks both catch the winner's PR.
# ============================================================================
echo ""
echo "--- Scenario 4: three-host interleave -- still exactly one winner ---"
reset_state

for h in host-A host-B host-C; do
    OPEN_DOCS_PR=""; eval "$STEP1_LINE"
    if [[ -n "$OPEN_DOCS_PR" ]]; then
        fail "(4) $h's Step 1 check unexpectedly found an open PR before any host has created one"
    else
        pass "(4) $h's Step 1 check finds nothing (store still empty)"
    fi
done

# host-B reaches Step 5 first among the three (arbitrary finish order).
OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
n="$(next_pr_number)"; jq --argjson n "$n" '. + [{"number": $n}]' "$STORE" > "$STORE.tmp" && mv "$STORE.tmp" "$STORE"
echo "host-B" >> "$CREATED_LOG"
echo "host-B: Step 1 and Step 5 both empty -- created PR #$n" >> "$EVENT_LOG"

# host-A and host-C's Step 5 rechecks both run after host-B's create landed.
for h in host-A host-C; do
    OPEN_DOCS_PR_RECHECK=""; eval "$STEP5_LINE"
    assert_eq "$n" "$OPEN_DOCS_PR_RECHECK" "(4) $h's Step 5 recheck sees host-B's already-created PR and would skip"
done

assert_eq "1" "$(created_count)" "(4) exactly one PR created across a 3-host interleave (not zero, not three)"
assert_eq "host-B" "$(created_contents)" "(4) the surviving create belongs to the single winner (host-B) only"

# ============================================================================
echo ""
echo "================================"
echo "Tests run:    $TESTS_RUN"
echo -e "Tests passed: ${GREEN}${TESTS_PASSED}${NC}"
if ((TESTS_FAILED > 0)); then
    echo -e "Tests failed: ${RED}${TESTS_FAILED}${NC}"
    exit 1
fi
echo "All tests passed"
exit 0
