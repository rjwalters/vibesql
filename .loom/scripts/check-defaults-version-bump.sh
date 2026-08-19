#!/usr/bin/env bash
# check-defaults-version-bump.sh - Fail a PR that changes the watched
# surface (defaults/ by default) without either bumping VERSION or
# declaring an explicit no-surface-change marker (#5874, #6480).
#
# Why this exists: every file under defaults/ is copied into every
# consumer's installed .loom/{scripts,hooks,roles,docs,bin}/ +
# .claude/commands/loom/ surfaces at install time -- and is NOT refreshed by
# a `git pull` (that drift is what resync-installed.sh exists to remediate,
# per #3777). The only mechanical signal a consumer has that its installed
# copies are behind is a VERSION comparison: install.sh's own currency check,
# /repo:update-tools, and a fleet's `fleet-resync.sh --dry-run` all key off
# it. If a PR changes defaults/ without bumping VERSION, that signal silently
# lies -- exactly what happened when PR #5846 changed five role prompts plus
# a doc without touching VERSION: 23 fleet repos all reported
# "v0.18.0 -> v0.18.0" (current) while 59-85 installed surfaces per repo were
# actually stale.
#
# This is deliberately NOT trying to force semantic-version inflation on
# every doc typo or test-only edit under defaults/ -- an explicit marker lets
# an author declare "this change does not alter installed behavior" without a
# version bump.
#
# Consumer-repo use case (#6480): any repo shaped like Loom -- one that ships
# its own installable surface via an install.sh that copies files into other
# repos, keying its own currency checks off its own VERSION (e.g.
# rjwalters/repo's Repo Skills, rjwalters/anvil, rjwalters/squad) -- can
# reuse this exact script to gate its own VERSION on its own surface. Point
# --paths (or VERSION_BUMP_WATCH_PATHS) at that repo's installable paths
# instead of defaults/; everything else (the VERSION file name, marker text,
# exit codes) stays the same.
#
# Usage:
#   check-defaults-version-bump.sh --base <ref> [--head <ref>]
#                                   [--paths <p1> [<p2>...]]
#     --base <ref>   Git ref/sha to diff FROM (the PR's base commit, e.g. a
#                     fetched base sha, or origin/main for a local check).
#                     Required.
#     --head <ref>   Git ref/sha to diff TO. Defaults to HEAD.
#     --paths <p1> [<p2>...]
#                     One or more pathspecs to watch (repeatable -- each
#                     --paths flag may take multiple path arguments, and the
#                     flag itself may be passed more than once; all given
#                     paths accumulate). Defaults to `defaults/` so Loom's
#                     own CI job and every existing caller are unaffected.
#                     Also settable via the VERSION_BUMP_WATCH_PATHS
#                     environment variable (a whitespace-separated list),
#                     which --paths overrides when both are given.
#   check-defaults-version-bump.sh --help
#
# No-surface-change marker: a PR whose body OR whose HEAD-reachable commit
# messages (between --base and --head) contain the literal string
#     <!-- loom:no-surface-change -->
# is exempt even when a watched path changed and VERSION did not. Pass the PR
# body via the PR_BODY environment variable (GitHub Actions:
# `env: PR_BODY: ${{ github.event.pull_request.body }}`); the commit-message
# path needs no extra plumbing beyond --base/--head.
#
# Exit codes:
#   0 - nothing under the watched paths changed in the diff, OR VERSION was
#       also changed, OR the no-surface-change marker is present.
#   1 - a watched path changed, VERSION was not, and no marker is present.
#   2 - bad usage (missing/invalid --base or --head).

set -euo pipefail

MARKER='<!-- loom:no-surface-change -->'

BASE=""
HEAD="HEAD"
WATCH_PATHS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      BASE="${2:-}"
      shift 2
      ;;
    --head)
      HEAD="${2:-}"
      shift 2
      ;;
    --paths)
      shift
      while [[ $# -gt 0 && "$1" != --* ]]; do
        WATCH_PATHS+=("$1")
        shift
      done
      ;;
    --help|-h)
      sed -n '2,63p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "check-defaults-version-bump: unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$BASE" ]]; then
  echo "check-defaults-version-bump: --base <ref> is required." >&2
  exit 2
fi

if ! git rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null; then
  echo "check-defaults-version-bump: base ref '$BASE' not found (not fetched?)." >&2
  exit 2
fi

if ! git rev-parse --verify --quiet "${HEAD}^{commit}" >/dev/null; then
  echo "check-defaults-version-bump: head ref '$HEAD' not found." >&2
  exit 2
fi

# --paths on the command line wins; otherwise fall back to the
# VERSION_BUMP_WATCH_PATHS env var (whitespace-separated); otherwise the
# original hardcoded default of defaults/, so every existing caller
# (Loom's own CI job included) is byte-for-byte unchanged.
if [[ ${#WATCH_PATHS[@]} -eq 0 ]]; then
  if [[ -n "${VERSION_BUMP_WATCH_PATHS:-}" ]]; then
    # shellcheck disable=SC2206  # intentional word-splitting on a
    # whitespace-separated path list
    WATCH_PATHS=($VERSION_BUMP_WATCH_PATHS)
  else
    WATCH_PATHS=("defaults/")
  fi
fi

WATCHED_DESC="${WATCH_PATHS[*]}"

# Deliberately a direct two-ref diff, not a merge-base-narrowed one -- this
# script is invoked from a shallow single-commit-fetch CI checkout where the
# base and head shallow histories may not share enough depth to resolve a
# merge-base. A direct diff answers the question this check actually cares
# about ("does applying head's changes touch a watched path without touching
# VERSION") without requiring ancestry.
CHANGED_FILES="$(git diff --name-only "$BASE" "$HEAD" -- "${WATCH_PATHS[@]}" 2>/dev/null || true)"

if [[ -z "$CHANGED_FILES" ]]; then
  echo "check-defaults-version-bump: OK — no changes under watched path(s) ($WATCHED_DESC) in this diff."
  exit 0
fi

VERSION_CHANGED="$(git diff --name-only "$BASE" "$HEAD" -- VERSION 2>/dev/null || true)"

if [[ -n "$VERSION_CHANGED" ]]; then
  echo "check-defaults-version-bump: OK — watched path(s) ($WATCHED_DESC) changed and VERSION was bumped."
  exit 0
fi

# --- no-surface-change marker check -----------------------------------------

if [[ -n "${PR_BODY:-}" ]] && grep -qF "$MARKER" <<<"$PR_BODY"; then
  echo "check-defaults-version-bump: OK — no-surface-change marker found in the PR body."
  exit 0
fi

if git log --format=%B "${BASE}..${HEAD}" 2>/dev/null | grep -qF "$MARKER"; then
  echo "check-defaults-version-bump: OK — no-surface-change marker found in a commit message."
  exit 0
fi

echo "check-defaults-version-bump: FAIL — watched path(s) ($WATCHED_DESC) changed without a VERSION bump:" >&2
echo "" >&2
echo "$CHANGED_FILES" | sed 's/^/  /' >&2
echo "" >&2
echo "Every file under defaults/ is copied into consumers' installed" >&2
echo ".loom/{scripts,hooks,roles,docs,bin}/ + .claude/commands/loom/ surfaces at" >&2
echo "install time -- NOT refreshed by a git pull (#3777). VERSION is the only" >&2
echo "mechanical signal consumers have that those copies are stale, so a" >&2
echo "watched-path change must bump it (at minimum the patch component):" >&2
echo "    ./scripts/version.sh bump patch" >&2
echo "" >&2
echo "If this change genuinely does not alter installed behavior (e.g. a" >&2
echo "comment, a test-only edit, a typo fix), declare that explicitly instead" >&2
echo "of bumping VERSION -- add this exact marker to the PR body or to a commit" >&2
echo "message in this PR:" >&2
echo "    $MARKER" >&2
exit 1
