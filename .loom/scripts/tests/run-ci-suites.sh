#!/usr/bin/env bash
# run-ci-suites.sh — run the CI-wired shell test suites for
# defaults/scripts/tests/ (issue #4455).
#
# Runs every suite listed in ci-wired.txt, one at a time, capturing pass/fail
# and per-suite wall-clock time. Prints a summary and exits non-zero if any
# wired suite fails. The wired/excluded partition invariant is enforced first
# via check-ci-suite-manifest.sh, so a fresh unlisted suite is a hard failure
# here (it cannot silently slip into an unwired pool).
#
# Usage:
#   run-ci-suites.sh                 # run the whole wired set
#   run-ci-suites.sh --plan          # print the RUN/SKIP plan and exit (runs nothing)
#   run-ci-suites.sh --print-candidates
#                                    # print the live-daemon guard's derived pid-file
#                                    # candidate list and exit (runs nothing)
#   LOOM_CI_SUITE_TIMEOUT=180 …      # per-suite timeout in seconds (default 1200)
#
# ## Live-daemon guard (#6386)
#
# The daemon-lifecycle suites (LIVE_DAEMON_GUARDED_SUITES below) execute the
# REAL loom-daemon-{start,stop,update,quiesce}.sh and loom-daemon-watchdog.sh —
# they `kill`, `rm -f` pid files, and `launchctl bootout` / `systemctl --user
# disable` whatever their invocations resolve. Every one of them sandboxes
# itself, but that is a per-case property: it takes exactly ONE case that
# forgets one pin (or one script that ignores one of them) to reach a live
# daemon. That is not hypothetical — #6386 was an Auditor running THIS script
# from a fleet host's live checkout, whose stop suite SIGTERM'd the fleet's
# authoritative dispatcher and left it down for 11 hours.
#
# So: when a daemon pid file exists on this host, those suites are SKIPPED
# (loudly, and reported in the summary), not run. CI runners have no daemon and
# no pid file, so the wired set is unaffected there — this only fires on the
# hosts where the blast radius is real. A skip is NOT a failure: the run still
# exits 0 if everything else passes, and the summary names what went
# unvalidated so the gap is never invisible.
#
# The trigger is EXISTENCE, not liveness: the lifecycle suites `rm -f`
# whichever pid file they resolve, so even a stale one is host state they must
# not silently delete (it is also the operator's evidence of how the daemon
# last exited).
#
#   LOOM_CI_ALLOW_DAEMON_SUITES=1    run them anyway (deliberate operator override)
#   LOOM_CI_DAEMON_PIDFILE_CANDIDATES=<path>[:<path>…]|none
#                                    TEST-ONLY seam: replace the derived
#                                    candidate list (`none` = no candidates at
#                                    all), so the guard's own regression suite
#                                    can assert BOTH directions regardless of
#                                    whether its host runs a real daemon. See
#                                    test-run-ci-suites-daemon-guard.sh.
#
# A manifest entry containing a `/` is a suite outside this directory,
# resolved relative to the repo root (tests/hooks/…, #4769; defaults/hooks/
# tests/…, #4451) rather than SCRIPT_DIR; its log file name has the `/`
# replaced with `_` so it stays a flat /tmp path. The default per-suite
# timeout was raised from 120s to 1200s in #4769 to cover
# tests/hooks/test-guard-destructive.sh (531 assertions, observed up to ~14
# min / 850s wall-clock on a loaded dev machine — still hermetic, just large
# — so the ceiling keeps real headroom above that peak).
#
# Exit 0 = all wired suites passed; 1 = one or more failed / manifest invalid.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
WIRED_MANIFEST="$SCRIPT_DIR/ci-wired.txt"
PER_SUITE_TIMEOUT="${LOOM_CI_SUITE_TIMEOUT:-1200}"

PLAN_ONLY=false
PRINT_CANDIDATES=false
for arg in "$@"; do
    case "$arg" in
        --plan) PLAN_ONLY=true ;;
        --print-candidates) PRINT_CANDIDATES=true ;;
        *) echo "run-ci-suites.sh: unknown option '$arg'" >&2; exit 1 ;;
    esac
done

# ---------- live-daemon guard (#6386) ----------
# Host-mutating suites: each one drives the real daemon lifecycle scripts.
LIVE_DAEMON_GUARDED_SUITES="test-loom-daemon-start.sh test-loom-daemon-stop.sh test-loom-daemon-update.sh test-loom-daemon-quiesce.sh test-loom-daemon-watchdog.sh"

# guard_repo_root_from / live_daemon_pidfile_candidates / live_daemon_pidfiles_present
# — extracted to a shared lib (#6528) so nextest-daemon-guard.sh (the
# equivalent guard for the Rust `daemon-integration` nextest group) reuses the
# exact same pid-file detection instead of a second, driftable copy. See
# defaults/scripts/lib/live-daemon-guard.sh for the full function docs.
# shellcheck source=../lib/live-daemon-guard.sh
source "$REPO_ROOT/defaults/scripts/lib/live-daemon-guard.sh"

# --print-candidates: the derived candidate list and nothing else. The guard's
# resolution is otherwise only observable through its RUN/SKIP decision, which
# depends on whether the host running the test happens to have a daemon — this
# seam lets the guard's own regression suite (and an operator debugging a
# surprising skip) assert the resolution directly. Runs no suite.
if [[ "$PRINT_CANDIDATES" == "true" ]]; then
    live_daemon_pidfile_candidates | awk 'NF' | sort -u
    exit 0
fi

LIVE_DAEMON_EVIDENCE="$(live_daemon_pidfiles_present)"
SKIP_DAEMON_SUITES=false
if [[ -n "$LIVE_DAEMON_EVIDENCE" ]]; then
    if [[ "${LOOM_CI_ALLOW_DAEMON_SUITES:-}" =~ ^(1|true|yes|on)$ ]]; then
        echo "!!! LOOM_CI_ALLOW_DAEMON_SUITES is set — running the daemon-lifecycle suites anyway" >&2
        echo "$LIVE_DAEMON_EVIDENCE" | sed 's/^/      /' >&2
    else
        SKIP_DAEMON_SUITES=true
    fi
fi

# 1) Fail fast if the manifest partition invariant is broken.
if ! bash "$SCRIPT_DIR/check-ci-suite-manifest.sh"; then
    echo "::error::CI-suite manifest invariant failed — fix ci-wired.txt / ci-excluded.txt" >&2
    exit 1
fi

# Returns 0 when <suite> must be skipped because this host has a daemon pid file.
suite_is_daemon_guarded() {
    local candidate name="${1##*/}"
    [[ "$SKIP_DAEMON_SUITES" == "true" ]] || return 1
    for candidate in $LIVE_DAEMON_GUARDED_SUITES; do
        [[ "$name" == "$candidate" ]] && return 0
    done
    return 1
}

if [[ "$SKIP_DAEMON_SUITES" == "true" ]]; then
    {
        echo
        echo "############################################################"
        echo "!!! LIVE DAEMON DETECTED ON THIS HOST — daemon-lifecycle suites SKIPPED (#6386)"
        echo "$LIVE_DAEMON_EVIDENCE" | sed 's/^/      /'
        echo "    Skipping: $LIVE_DAEMON_GUARDED_SUITES"
        echo "    These suites run the REAL loom-daemon-{start,stop,update,quiesce}.sh and can"
        echo "    kill/bootout a live daemon if any single case's sandbox is incomplete — that is"
        echo "    #6386 (an 11h fleet-dispatcher outage caused by exactly this script's Auditor run)."
        echo "    Run them on a host with no daemon, or override with LOOM_CI_ALLOW_DAEMON_SUITES=1."
        echo "############################################################"
        echo
    } >&2
fi

# Resolve a GNU/BSD-agnostic timeout wrapper (optional — plain bash if absent).
timeout_cmd=""
if command -v timeout >/dev/null 2>&1; then
    timeout_cmd="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
    timeout_cmd="gtimeout"
fi

mapfile -t suites < <(sed -E 's/#.*$//' "$WIRED_MANIFEST" | awk 'NF { print $1 }')

passed=0
failed=0
skipped=0
failed_names=()
skipped_names=()
total_start=$(date +%s)

# --plan: report what WOULD run (and what the live-daemon guard removed), then
# exit without executing a single suite. This is the seam the guard's own
# regression test drives — asserting the skip decision without paying for (or
# risking) a full suite run.
if [[ "$PLAN_ONLY" == "true" ]]; then
    for suite in "${suites[@]}"; do
        if suite_is_daemon_guarded "$suite"; then
            printf 'SKIP  %s (live-daemon guard, #6386)\n' "$suite"
        else
            printf 'RUN   %s\n' "$suite"
        fi
    done
    exit 0
fi

printf '\n=== Running %d CI-wired shell suites (timeout %ss each) ===\n\n' \
    "${#suites[@]}" "$PER_SUITE_TIMEOUT"

for suite in "${suites[@]}"; do
    if suite_is_daemon_guarded "$suite"; then
        printf 'SKIP  %-52s     (live-daemon guard, #6386)\n' "$suite"
        skipped=$((skipped + 1)); skipped_names+=("$suite"); continue
    fi
    if [[ "$suite" == */* ]]; then
        path="$REPO_ROOT/$suite"
    else
        path="$SCRIPT_DIR/$suite"
    fi
    log_name="${suite//\//_}"
    if [[ ! -f "$path" ]]; then
        echo "FAIL  $suite (missing file)"
        failed=$((failed + 1)); failed_names+=("$suite"); continue
    fi
    start=$(date +%s)
    if [[ -n "$timeout_cmd" ]]; then
        "$timeout_cmd" "$PER_SUITE_TIMEOUT" bash "$path" >"/tmp/ci-suite-$log_name.log" 2>&1
    else
        bash "$path" >"/tmp/ci-suite-$log_name.log" 2>&1
    fi
    rc=$?
    dur=$(( $(date +%s) - start ))
    if [[ "$rc" -eq 0 ]]; then
        printf 'PASS  %-52s %3ss\n' "$suite" "$dur"
        passed=$((passed + 1))
    else
        printf 'FAIL  %-52s %3ss (exit %s)\n' "$suite" "$dur" "$rc"
        failed=$((failed + 1)); failed_names+=("$suite")
        echo "----- last 40 lines of $suite -----"
        tail -40 "/tmp/ci-suite-$log_name.log"
        echo "----- end $suite -----"
    fi
done

total_dur=$(( $(date +%s) - total_start ))
printf '\n=== Summary: %d passed, %d failed, %d skipped of %d wired suites in %ss ===\n' \
    "$passed" "$failed" "$skipped" "${#suites[@]}" "$total_dur"

if [[ "$skipped" -ne 0 ]]; then
    printf 'Skipped (live-daemon guard, #6386 — NOT validated on this host): %s\n' \
        "${skipped_names[*]}" >&2
fi

if [[ "$failed" -ne 0 ]]; then
    printf 'Failed suites: %s\n' "${failed_names[*]}" >&2
    exit 1
fi
exit 0
