#!/usr/bin/env bash
# detect-startable-subset.sh - Detects a stated "startable subset" carve-out in
# a proposal's body (issue #5664, "recurred after closure").
#
# THE FAILURE MODE THIS EXISTS FOR
#
# Champion's dependency handling (the epic-aware sub-check, the dependency-cycle
# gate, and classify-dependency-block.sh's defer/un-escalate split) all answer
# questions about the WHOLE issue: is #N still open, is there a cycle, has the
# blocker closed. None of them can see a dependency that only covers PART of an
# issue's work.
#
# An architect proposal can be filed with an explicit split point: "comparator +
# mutation tests only need warmup/01_netlist.v, independent of the blocked
# deliverable" is a declaration that part of the issue is buildable RIGHT NOW,
# regardless of when the blocker closes. Nothing previously read that
# declaration. The whole issue was evaluated (and, once repeatedly rejected,
# deferred or escalated) as a single unit, discarding the stated split and
# parking work that was never actually blocked.
#
# THE CONVENTION
#
# A proposal states a startable subset with a `## Startable Subset` (or
# `### Startable Subset`) heading in its body, followed by prose naming which
# part of the work does not depend on the open blocker(s). Anyone who declares a
# dependency (an Architect proposal, a Curator enhancement, a human) can add
# this section; Champion only reads it, never writes it.
#
#   ## Startable Subset
#
#   The comparator and mutation tests need only `warmup/01_netlist.v`, which is
#   already published upstream -- independent of the blocked RTL deliverable.
#
# This script answers exactly one question, as a testable unit instead of prose
# judgement inline in the role file:
#
#   "Does this issue's body declare a startable subset, and if so, what is it?"
#
# It does NOT decide whether the declared subset is actually independent of the
# blocker (that remains Champion's own judgement, the same way "no obvious
# blockers or dependencies" already is) -- it only makes the declaration
# machine-readable so Champion's criterion-2 evaluation, the Step 4
# dependency-timing gate, and the Pass 0 self-healing re-scan can all detect it
# the same way, rather than three independent readings of free text.
#
# Usage:
#   detect-startable-subset.sh --issue <N> [--repo <owner/repo>] [options]
#
# Options:
#   --issue <N>          Issue number (required).
#   --repo <nwo>          owner/repo of --issue (default: the current repo's
#                        origin).
#   --body-file <path>   Read the body from this file instead of fetching the
#                        issue (used by tests, and by callers that already hold
#                        the body in a variable).
#   --no-cache           Read via plain `gh` instead of the `gh-cached` wrapper.
#   --help,-h            Show this help.
#
# Output (stdout, marker lines -- parseable by the Champion prose):
#   Found:
#     STARTABLE_SUBSET
#     <the subset text, one or more lines, verbatim from the section body>
#   Not found:
#     NO_STARTABLE_SUBSET
#
# Exit codes:
#   0 - a startable subset is declared
#   1 - no startable subset section (or an empty one)
#   2 - error (bad arguments, issue unreadable, missing jq)
#
# BOUNDED COST. One cached read of the issue body (or zero, with --body-file).
# No forge writes -- this script is strictly read-only; it never comments and
# never changes labels. The pure parsing function below is directly
# sourceable and unit-tested by tests/test-detect-startable-subset.sh, exactly
# like detect-dependency-cycle.sh's parse_dependency_refs.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- output helpers ----
if [[ -t 2 ]]; then
    RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
    RED=''; YELLOW=''; NC=''
fi
err()  { echo -e "${RED}ERROR: $1${NC}" >&2; }
warn() { echo -e "${YELLOW}WARNING: $1${NC}" >&2; }

show_help() {
    sed -n '2,64p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# ---- defaults ----
ISSUE=""
REPO_NWO=""
BODY_FILE=""
NO_CACHE=0
GH_READ="${GH_READ:-gh}"

# =====================================================================
# Pure helper (sourceable and unit-tested directly by
# tests/test-detect-startable-subset.sh -- no stubs, no forge)
# =====================================================================

# extract_startable_subset <body>
#
# Emits the body of the FIRST `## Startable Subset` (any heading depth `##`+,
# case-insensitive, tolerant of trailing text on the heading line, e.g.
# "## Startable Subset (independent of #3)") section: every line from just
# after that heading up to -- but not including -- the next heading of the
# same or shallower depth, or end of body. Emits nothing if no such heading
# exists.
extract_startable_subset() {
    printf '%s\n' "$1" | awk '
        function lower(s) { return tolower(s) }
        {
            line = $0
            if (!capturing) {
                stripped = line
                sub(/^[[:space:]]+/, "", stripped)
                if (stripped ~ /^#{2,6}[[:space:]]+/) {
                    heading_text = stripped
                    sub(/^#{2,6}[[:space:]]+/, "", heading_text)
                    if (lower(heading_text) ~ /^startable subset/) {
                        capturing = 1
                        depth = 0
                        n = match(stripped, /^#+/)
                        depth = RLENGTH
                        next
                    }
                }
                next
            }
            # Already capturing: stop at the next heading of the same or
            # shallower depth (a deeper subsection, e.g. "### Files", stays
            # inside the captured text).
            stripped = line
            sub(/^[[:space:]]+/, "", stripped)
            if (stripped ~ /^#+[[:space:]]/) {
                n = match(stripped, /^#+/)
                if (RLENGTH <= depth) { capturing = 0; next }
            }
            print line
        }
    '
}

# has_startable_subset <body>
# True (0) when extract_startable_subset returns non-blank text.
has_startable_subset() {
    local text
    text="$(extract_startable_subset "$1")"
    [[ -n "${text//[[:space:]]/}" ]]
}

# =====================================================================
# repo resolution (same shape as detect-dependency-cycle.sh's _resolve_repo)
# =====================================================================
_resolve_repo() {
    local url
    url="$(git remote get-url origin 2>/dev/null || true)"
    if [[ -n "$url" ]]; then
        url="${url%.git}"
        url="$(printf '%s' "$url" | sed -E 's#^.*[:/]([^/]+/[^/]+)$#\1#')"
        if [[ "$url" == */* ]]; then
            printf '%s\n' "$url"
            return 0
        fi
    fi
    gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null
}

# =====================================================================
# main
# =====================================================================
main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --issue)      ISSUE="${2:-}"; shift 2 ;;
            --repo)       REPO_NWO="${2:-}"; shift 2 ;;
            --body-file)  BODY_FILE="${2:-}"; shift 2 ;;
            --no-cache)   NO_CACHE=1; shift ;;
            --help|-h)    show_help; exit 0 ;;
            *)            err "Unexpected argument: $1"; show_help >&2; exit 2 ;;
        esac
    done

    if [[ -z "$ISSUE" ]]; then
        err "Usage: detect-startable-subset.sh --issue <N> [--repo <owner/repo>] [--body-file <path>]"
        exit 2
    fi
    if ! [[ "$ISSUE" =~ ^[0-9]+$ ]]; then
        err "--issue must be a number (got: $ISSUE)"
        exit 2
    fi
    if [[ -n "$BODY_FILE" && ! -f "$BODY_FILE" ]]; then
        err "--body-file not found: $BODY_FILE"
        exit 2
    fi

    local body
    if [[ -n "$BODY_FILE" ]]; then
        body="$(cat "$BODY_FILE")"
    else
        GH_READ="gh"
        if [[ "$NO_CACHE" -eq 0 ]]; then
            local ghc="$SCRIPT_DIR/gh-cached"
            if [[ -x "$ghc" ]] && "$ghc" --version >/dev/null 2>&1; then GH_READ="$ghc"; fi
        fi
        if [[ -z "$REPO_NWO" ]]; then
            REPO_NWO="$(_resolve_repo)"
        fi
        if [[ -z "$REPO_NWO" ]]; then
            err "could not determine the repo - pass --repo <owner/repo>"
            exit 2
        fi
        body="$("$GH_READ" issue view "$ISSUE" --repo "$REPO_NWO" --json body --jq '.body // ""' 2>/dev/null)"
        if [[ -z "${body+x}" ]]; then
            err "could not read $REPO_NWO#$ISSUE"
            exit 2
        fi
    fi

    if has_startable_subset "$body"; then
        echo "STARTABLE_SUBSET"
        extract_startable_subset "$body"
        exit 0
    fi
    echo "NO_STARTABLE_SUBSET"
    exit 1
}

# Only run main when executed directly (not when sourced by tests or by
# classify-dependency-block.sh for the pure helpers above).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
