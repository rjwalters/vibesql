#!/usr/bin/env bash
# test-urgent-flip-guard.sh - Regression test for issue #5643
#
# The Guide's "Maximum Urgent: 3 Issues" triage used to recompute the top 3
# from scratch every 15-30 minute tick. More than one Guide ticks against the
# same forge at once (this host's daemon role runner, other fleet hosts'
# daemons, a manual /loom:guide session), so two ticks that ranked the same
# boundary-case issue differently each overwrote the other's decision.
# Observed: `loom:urgent` flipped on #5565 seven times in ~2.5 hours at exactly
# Guide-tick cadence, which regenerated WORK_PLAN.md's Urgent section every
# time and spawned 12 `docs: Guide document maintenance update` PRs in ~8.5h.
#
# The fix has two halves, and this suite covers both:
#
#   A. urgent-flip-guard.sh — forge-backed hysteresis in front of every
#      loom:urgent write. It reads the issue's own label-event history (the
#      only decision record every fleet host shares — a local mkdir lock like
#      docs-guide-lock.sh is same-host only, #5615) and refuses a write that
#      reverses a decision younger than the cooldown, or that re-promotes an
#      already-flapping issue.
#
#   B. guide.md's incumbency rule — the urgent set is EDITED, not recomputed:
#      ties keep the incumbent, so two ticks reading the same forge state
#      reach the same answer.
#
# Test 8 is the headline regression: it replays #5565's real 2026-08-07
# timeline through the guard and asserts that the 7 observed flips collapse to
# exactly 1 accepted label write.
#
# Hermetic: no forge/network calls. The guard's LOOM_URGENT_GUARD_EVENTS_FILE
# and LOOM_URGENT_GUARD_NOW seams supply the label history and the clock.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPTS_DIR/../.." && pwd)"

GUARD_SH="$SCRIPTS_DIR/urgent-flip-guard.sh"
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

assert_grep() {
    local pattern="$1" file="$2" msg="$3"
    if grep -qE "$pattern" "$file"; then pass "$msg"; else fail "$msg (missing pattern: $pattern)"; fi
}

summarize_and_exit() {
    echo ""
    echo "================================"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}${TESTS_PASSED}${NC}"
    if [[ $TESTS_FAILED -gt 0 ]]; then
        echo -e "Tests failed: ${RED}${TESTS_FAILED}${NC}"
        exit 1
    fi
    echo "All tests passed"
    exit 0
}

if [[ ! -x "$GUARD_SH" ]]; then
    fail "urgent-flip-guard.sh missing or not executable at $GUARD_SH"
    summarize_and_exit
fi
if [[ ! -f "$GUIDE_MD" ]]; then
    fail "guide.md not found at $GUIDE_MD"
    summarize_and_exit
fi

SANDBOX="$(mktemp -d)"
cleanup() { rm -rf "$SANDBOX"; }
trap cleanup EXIT

# --- helpers -----------------------------------------------------------------

# ISO-8601 -> epoch, portable across GNU date (Linux CI) and BSD date (macOS).
iso_epoch() {
    date -u -d "$1" +%s 2>/dev/null || date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$1" +%s 2>/dev/null
}

# Write a GitHub-issue-events-shaped fixture from "<iso> <labeled|unlabeled>"
# lines on stdin. Unrelated events are interleaved so the guard's label filter
# is exercised, not just its date math.
make_events() { # <output-path>
    local out="$1" first=1
    printf '[\n' > "$out"
    printf '  {"event":"commented","created_at":"2026-01-01T00:00:00Z"},\n' >> "$out"
    printf '  {"event":"labeled","label":{"name":"tier:goal-supporting"},"created_at":"2026-01-01T00:00:00Z"}' >> "$out"
    first=0
    while read -r ts kind; do
        [[ -n "$ts" ]] || continue
        [[ "$first" -eq 1 ]] || printf ',\n' >> "$out"
        printf '  {"event":"%s","label":{"name":"loom:urgent"},"created_at":"%s"}' "$kind" "$ts" >> "$out"
        first=0
    done
    printf '\n]\n' >> "$out"
}

# Run the guard against a fixture at a fixed "now"; echo the verdict line.
guard_check() { # <events-file> <now-epoch> <issue> <add|remove> [extra env assignments...]
    local events="$1" now="$2" issue="$3" dir="$4"; shift 4
    env LOOM_URGENT_GUARD_EVENTS_FILE="$events" LOOM_URGENT_GUARD_NOW="$now" "$@" \
        "$GUARD_SH" check "$issue" "$dir" 2>/dev/null
}

guard_rc() { # same args as guard_check; echo the exit code
    local events="$1" now="$2" issue="$3" dir="$4"; shift 4
    local rc=0
    env LOOM_URGENT_GUARD_EVENTS_FILE="$events" LOOM_URGENT_GUARD_NOW="$now" "$@" \
        "$GUARD_SH" check "$issue" "$dir" >/dev/null 2>&1 || rc=$?
    echo "$rc"
}

expect_verdict() { # <events> <now> <issue> <dir> <expected-rc> <expected-reason> <msg>
    local events="$1" now="$2" issue="$3" dir="$4" want_rc="$5" want_reason="$6" msg="$7"
    local out rc
    out="$(guard_check "$events" "$now" "$issue" "$dir")"
    rc="$(guard_rc "$events" "$now" "$issue" "$dir")"
    if [[ "$rc" == "$want_rc" ]] && [[ "$out" == *"reason=$want_reason"* ]]; then
        pass "$msg"
    else
        fail "$msg (rc=$rc want=$want_rc; verdict='$out' want reason=$want_reason)"
    fi
}

NOW_ISO="2026-08-07T23:00:00Z"
NOW="$(iso_epoch "$NOW_ISO")"
if ! [[ "$NOW" =~ ^[0-9]+$ ]]; then
    fail "could not convert an ISO-8601 stamp to epoch with either GNU or BSD date"
    summarize_and_exit
fi

mins_ago() { echo "$(( NOW - ($1 * 60) ))"; }
iso_mins_ago() { # minutes -> ISO stamp, portable
    local secs=$(( $1 * 60 ))
    date -u -d "@$(( NOW - secs ))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || date -u -r "$(( NOW - secs ))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null
}

# --- Test 1: argument and configuration validation ---------------------------
echo "Test 1: argument and configuration validation"

rc=0; "$GUARD_SH" >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "missing command -> exit 2"; else fail "expected exit 2 for missing command, got $rc"; fi

rc=0; "$GUARD_SH" bogus 12 >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "unknown command -> exit 2"; else fail "expected exit 2 for unknown command, got $rc"; fi

rc=0; "$GUARD_SH" --help >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "0" ]]; then pass "--help -> exit 0"; else fail "expected exit 0 for --help, got $rc"; fi

rc=0; "$GUARD_SH" check notanumber add >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "non-numeric issue number -> exit 2"; else fail "expected exit 2 for a non-numeric issue, got $rc"; fi

rc=0; "$GUARD_SH" check 5565 sideways >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "invalid direction -> exit 2"; else fail "expected exit 2 for an invalid direction, got $rc"; fi

rc=0; LOOM_URGENT_FLIP_COOLDOWN_SECS=abc "$GUARD_SH" check 5565 add >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "non-numeric LOOM_URGENT_FLIP_COOLDOWN_SECS -> exit 2"; else fail "expected exit 2 for a bad cooldown value, got $rc"; fi

rc=0; LOOM_URGENT_FLAP_THRESHOLD=-1 "$GUARD_SH" check 5565 add >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "2" ]]; then pass "negative LOOM_URGENT_FLAP_THRESHOLD -> exit 2"; else fail "expected exit 2 for a bad flap threshold, got $rc"; fi

# --- Test 2: a first-ever promotion is never blocked -------------------------
echo ""
echo "Test 2: an issue with no loom:urgent history is freely promotable"
EMPTY="$SANDBOX/empty.json"
: | make_events "$EMPTY"
expect_verdict "$EMPTY" "$NOW" 5565 add 0 "no-history" \
    "no prior loom:urgent events -> add is allowed"
expect_verdict "$EMPTY" "$NOW" 5565 remove 0 "no-history" \
    "no prior loom:urgent events -> remove is allowed (no decision to reverse)"

# --- Test 3: THE FLAP — reversing a fresh decision is suppressed -------------
echo ""
echo "Test 3: a write that reverses a decision inside the cooldown is suppressed"
FRESH_ADD="$SANDBOX/fresh-add.json"
printf '%s labeled\n' "$(iso_mins_ago 31)" | make_events "$FRESH_ADD"

expect_verdict "$FRESH_ADD" "$NOW" 5565 remove 1 "reversal-within-cooldown" \
    "labeled 31 min ago (one tick) -> remove is suppressed (THE #5643 FLAP)"
expect_verdict "$FRESH_ADD" "$NOW" 5565 add 0 "no-recent-conflict" \
    "labeled 31 min ago -> a same-direction add is NOT a reversal and is allowed"

FRESH_REMOVE="$SANDBOX/fresh-remove.json"
printf '%s unlabeled\n' "$(iso_mins_ago 31)" | make_events "$FRESH_REMOVE"
expect_verdict "$FRESH_REMOVE" "$NOW" 5565 add 1 "reversal-within-cooldown" \
    "unlabeled 31 min ago -> re-promotion is suppressed (the other half of the flap)"

# --- Test 4: a REAL priority change still displaces a holder ----------------
# Acceptance criterion: the urgent set must not become permanently frozen. The
# cooldown gates RECENCY, not merit, so a decision older than the cooldown is
# freely reversible.
echo ""
echo "Test 4: a decision older than the cooldown stays freely reversible"
OLD_ADD="$SANDBOX/old-add.json"
printf '%s labeled\n' "$(iso_mins_ago 200)" | make_events "$OLD_ADD"
expect_verdict "$OLD_ADD" "$NOW" 5630 remove 0 "no-recent-conflict" \
    "labeled 200 min ago (> 3h cooldown) -> a holder can still be demoted"

# An explicit operator override is available for the fresh case too.
rc="$(guard_rc "$FRESH_ADD" "$NOW" 5565 remove LOOM_URGENT_FLIP_COOLDOWN_SECS=0)"
if [[ "$rc" == "0" ]]; then
    pass "LOOM_URGENT_FLIP_COOLDOWN_SECS=0 lets an operator override a fresh decision"
else
    fail "expected the cooldown override to allow the write, got rc=$rc"
fi

# --- Test 5: flap freeze, and its deliberate asymmetry -----------------------
# Four loom:urgent events inside the 24h window == demonstrably flapping. The
# freeze applies to `add` ONLY: freezing `remove` too could strand a 4th
# loom:urgent label and break guide.md's "max 3" cap, which this guard must
# leave untouched.
echo ""
echo "Test 5: a flapping issue is frozen against re-promotion (add only)"
FLAPPING="$SANDBOX/flapping.json"
{
    printf '%s labeled\n' "$(iso_mins_ago 1200)"
    printf '%s unlabeled\n' "$(iso_mins_ago 1140)"
    printf '%s labeled\n' "$(iso_mins_ago 1080)"
    printf '%s unlabeled\n' "$(iso_mins_ago 1020)"
    printf '%s labeled\n' "$(iso_mins_ago 960)"
} | make_events "$FLAPPING"

expect_verdict "$FLAPPING" "$NOW" 5565 add 1 "flapping" \
    "5 loom:urgent events in 24h -> add is frozen"
expect_verdict "$FLAPPING" "$NOW" 5565 remove 0 "no-recent-conflict" \
    "the freeze exempts remove, so the 'max 3' cap can always be restored"

# Self-healing: once the events age out of the flap window, promotion resumes.
LATER=$(( NOW + 90000 ))
rc="$(guard_rc "$FLAPPING" "$LATER" 5565 add)"
if [[ "$rc" == "0" ]]; then
    pass "the freeze self-heals once the events age out of the flap window"
else
    fail "expected the flap freeze to expire with the window, got rc=$rc"
fi

# --- Test 6: fail-closed on an unreadable history ---------------------------
echo ""
echo "Test 6: an unreadable label history suppresses the write (fail closed)"
expect_verdict "$SANDBOX/does-not-exist.json" "$NOW" 5565 add 1 "history-unreadable" \
    "unreadable history -> SUPPRESS, never a silent ALLOW"

rc=0
env LOOM_URGENT_GUARD_EVENTS_FILE="$SANDBOX/does-not-exist.json" LOOM_URGENT_GUARD_NOW="$NOW" \
    "$GUARD_SH" status 5565 >/dev/null 2>&1 || rc=$?
if [[ "$rc" == "1" ]]; then pass "status on an unreadable history -> exit 1"; else fail "expected exit 1, got $rc"; fi

# --- Test 7: status reports the history it acted on -------------------------
echo ""
echo "Test 7: status emits a machine-readable summary"
STATUS_JSON="$(env LOOM_URGENT_GUARD_EVENTS_FILE="$FLAPPING" LOOM_URGENT_GUARD_NOW="$NOW" \
    "$GUARD_SH" status 5565 2>/dev/null)"
if printf '%s\n' "$STATUS_JSON" | jq -e '.issue == 5565 and .urgent_state == "present" and .flapping == true and .events_in_flap_window == 5' >/dev/null 2>&1; then
    pass "status reports issue, derived label state, flap count and flapping verdict"
else
    fail "status JSON did not match expectations: $STATUS_JSON"
fi

# --- Test 8: THE REGRESSION — replay #5565's real 2026-08-07 timeline -------
# Seven observed loom:urgent flips in ~2.5 hours. Replay each attempted flip in
# order against the guard, appending only the ones it allows, and skipping any
# attempt that is a no-op against the state the surviving history implies (the
# Guide's own safety check would never issue it). With the guard in place the
# whole storm must collapse to a single label write.
echo ""
echo "Test 8: replaying #5565's real 7-flip timeline yields exactly 1 write (THE #5643 REGRESSION)"

REPLAY="$SANDBOX/replay.json"
# Seed: the label was applied well before the storm (older than the cooldown),
# so the storm's first removal is legitimately allowed.
HISTORY_LINES="2026-08-07T17:00:00Z labeled"
printf '%s\n' "$HISTORY_LINES" | make_events "$REPLAY"
STATE="present"
ACCEPTED=0
SUPPRESSED=0
NOOPS=0

replay_attempt() { # <iso> <add|remove>
    local ts="$1" dir="$2" now_epoch rc
    # A direction that matches the current state is a no-op the Guide would
    # never issue — not a flip.
    if { [[ "$dir" == "add" && "$STATE" == "present" ]] || [[ "$dir" == "remove" && "$STATE" == "absent" ]]; }; then
        NOOPS=$((NOOPS + 1))
        return
    fi
    now_epoch="$(iso_epoch "$ts")"
    rc="$(guard_rc "$REPLAY" "$now_epoch" 5565 "$dir")"
    if [[ "$rc" == "0" ]]; then
        ACCEPTED=$((ACCEPTED + 1))
        if [[ "$dir" == "add" ]]; then
            HISTORY_LINES="$HISTORY_LINES
$ts labeled"
            STATE="present"
        else
            HISTORY_LINES="$HISTORY_LINES
$ts unlabeled"
            STATE="absent"
        fi
        printf '%s\n' "$HISTORY_LINES" | make_events "$REPLAY"
    else
        SUPPRESSED=$((SUPPRESSED + 1))
    fi
}

replay_attempt 2026-08-07T20:13:58Z remove
replay_attempt 2026-08-07T20:41:02Z add
replay_attempt 2026-08-07T20:44:42Z remove
replay_attempt 2026-08-07T22:03:21Z add
replay_attempt 2026-08-07T22:04:39Z remove
replay_attempt 2026-08-07T22:18:48Z add
replay_attempt 2026-08-07T22:49:37Z remove

if [[ "$ACCEPTED" == "1" ]]; then
    pass "7 attempted flips collapse to 1 accepted loom:urgent write (suppressed=$SUPPRESSED, no-ops=$NOOPS)"
else
    fail "expected exactly 1 accepted write across the replayed storm, got $ACCEPTED (suppressed=$SUPPRESSED, no-ops=$NOOPS)"
fi

if [[ "$SUPPRESSED" -ge 3 ]]; then
    pass "the guard actively suppressed $SUPPRESSED of the storm's reversals"
else
    fail "expected the guard to suppress >=3 reversals, got $SUPPRESSED"
fi

# --- Test 9: guide.md wiring — no unguarded loom:urgent writes --------------
echo ""
echo "Test 9: guide.md routes every loom:urgent write through the guard"

assert_grep 'urgent-flip-guard\.sh check' "$GUIDE_MD" \
    "guide.md invokes urgent-flip-guard.sh"
assert_grep 'urgent-flip-guard\.sh check <number> add' "$GUIDE_MD" \
    "guide.md documents the add-direction check"
assert_grep 'urgent-flip-guard\.sh check <weakest> remove' "$GUIDE_MD" \
    "guide.md documents the remove-direction check on the demoted incumbent"

# Every `gh issue edit ... loom:urgent` line must be preceded (within the same
# snippet) by a guard invocation. Checked structurally: no such write may
# appear before the first guard call in the file, and each one must have a
# guard call within the 12 lines above it.
UNGUARDED=0
while IFS=: read -r lineno _; do
    [[ -n "$lineno" ]] || continue
    start=$(( lineno > 12 ? lineno - 12 : 1 ))
    if ! sed -n "${start},${lineno}p" "$GUIDE_MD" | grep -q 'urgent-flip-guard\.sh check'; then
        UNGUARDED=$((UNGUARDED + 1))
        echo "    unguarded loom:urgent write at guide.md:$lineno"
    fi
done < <(grep -n 'gh issue edit .*label "loom:urgent"' "$GUIDE_MD")

if [[ "$UNGUARDED" -eq 0 ]]; then
    pass "no unguarded 'gh issue edit ... loom:urgent' example remains in guide.md"
else
    fail "$UNGUARDED loom:urgent write example(s) in guide.md are not preceded by a guard call"
fi

# --- Test 10: guide.md's incumbency rule ------------------------------------
echo ""
echo "Test 10: guide.md replaces from-scratch recomputation with an incumbency rule"

assert_grep '#5643 BUG, DO NOT REINTRODUCE' "$GUIDE_MD" \
    "the from-scratch-recomputation bug is recorded so it is not reintroduced"
assert_grep 'urgency_rank\(\)' "$GUIDE_MD" \
    "a deterministic urgency_rank() ladder is defined"
assert_grep 'strictly_outranks\(\)' "$GUIDE_MD" \
    "a strictly_outranks() comparison is defined"

# The tie-break MUST favour the incumbent — a `-le` here would reopen the flap
# for exactly the same-rank issues #5565 flapped over.
if grep -q 'strictly_outranks() { \[ "$(urgency_rank "$1")" -lt "$(urgency_rank "$2")" \]; }' "$GUIDE_MD"; then
    pass "strictly_outranks uses a STRICT comparison (ties keep the incumbent)"
else
    fail "strictly_outranks must compare with -lt; a non-strict comparison reopens the #5643 flap"
fi

assert_grep '[Dd]emote first' "$GUIDE_MD" \
    "the swap is documented demote-first (a suppressed demotion must not strand a 4th urgent label)"

# --- Test 11: the "max 3" invariant is untouched ----------------------------
echo ""
echo "Test 11: the documented 'max 3 urgent' invariant is unchanged"
assert_grep 'NEVER have more than 3 issues marked' "$GUIDE_MD" \
    "guide.md still states the hard cap of 3 urgent issues"
assert_grep 'Max 3 urgent.*non-negotiable' "$GUIDE_MD" \
    "the Working Style cap bullet is intact"

summarize_and_exit
