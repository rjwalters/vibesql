#!/usr/bin/env bash
# Loom PR Worktree Helper - Create a dedicated review worktree for a PR.
#
# Usage:
#   ./.loom/scripts/pr-worktree.sh <PR_NUMBER>
#
# Despite the name suggesting "external-fork / ad-hoc only" (the original,
# narrower use case, #3358), this is ALSO the script Judge/Doctor use for an
# ordinary Loom-issue PR (branch `feature/issue-<N>`) whenever no builder
# `issue-<N>` worktree already exists locally at review time — see
# `defaults/.claude/commands/loom/judge.md`'s "Worktree-Aware Code Access"
# section (#6264). In that shape the intended/documented case for
# `feature/issue-<N>` branches is `worktree.sh <ISSUE_NUMBER>` (below); this
# script's own worktree/branch-name mismatch (a `pr-<N>` directory holding a
# `feature/issue-<N>` branch) is exactly why a leftover review worktree could
# be missed by `merge-pr.sh`'s cleanup — fixed there, not here, by #6264.
#
# Typical uses:
#   - External-fork PRs (e.g., jperla/loom:fix/claude-code-2.1-compat)
#   - Ad-hoc branch names that don't include a Loom issue number
#   - A Judge/Doctor review of a Loom-issue PR with no local builder worktree
#
# For Loom-issue PRs whose branch IS `feature/issue-<N>` AND a builder
# worktree may already exist, prefer:
#   ./.loom/scripts/worktree.sh <ISSUE_NUMBER>
#
# What it does:
#   1. Fetches the PR's branch into a local tracking branch via `gh pr checkout`
#      INSIDE the new worktree (not in the orchestrator's main worktree)
#   2. Creates .loom/worktrees/pr-<PR_NUMBER>/ on a placeholder branch first,
#      then runs `gh pr checkout` from inside it so the PR branch is only
#      ever checked out in the dedicated worktree
#   3. Writes a `.loom-managed` sentinel so merge-pr.sh / loom-clean will
#      remove the worktree on PR merge
#
# Known failure mode (#6264, reproduced): if the PR's branch is ALREADY
# checked out in another local worktree (most commonly this same PR's
# `issue-<N>` builder worktree — git refuses the same branch in two
# worktrees at once), step 2's `gh pr checkout --force` fails loudly
# (non-zero exit, git's "already used by worktree" error) and this script
# exits 1 — but the worktree directory it already created is left behind on
# a detached HEAD (pinned at the base branch, not the PR). Callers MUST
# check this script's exit code rather than assuming the printed path is
# ready to evaluate; merge-pr.sh's cleanup no longer depends on the branch
# actually having switched (#6264 — it checks by path, not by branch state).
#
# Exit codes:
#   0 = success (worktree exists at the expected path, PR branch checked out)
#   1 = failure (error printed; the worktree directory may still exist,
#       possibly on a detached HEAD if the PR-branch checkout itself failed)
#   2 = invalid arguments

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_error() { echo -e "${RED}ERROR: $1${NC}" >&2; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }

show_help() {
    cat <<'EOF'
Loom PR Worktree Helper

Usage: ./.loom/scripts/pr-worktree.sh <PR_NUMBER>

Creates an isolated worktree at .loom/worktrees/pr-<PR_NUMBER>/ for a PR
whose branch doesn't fit the `feature/issue-<N>` convention (typically
external-fork PRs). The PR's branch is checked out inside the worktree —
never in the orchestrator's main worktree.

For Loom-issue PRs (branch = feature/issue-<N>), use worktree.sh instead.

Exit codes:
  0 = worktree ready at .loom/worktrees/pr-<PR_NUMBER>/
  1 = failure
  2 = invalid arguments
EOF
}

if [[ $# -eq 0 ]] || [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    show_help
    [[ $# -eq 0 ]] && exit 2 || exit 0
fi

PR_NUMBER="$1"
if ! [[ "$PR_NUMBER" =~ ^[0-9]+$ ]]; then
    print_error "PR number must be numeric (got: '$PR_NUMBER')"
    exit 2
fi

# Resolve the main repo root even when invoked from a worktree.
GIT_COMMON_DIR=$(git rev-parse --git-common-dir 2>/dev/null) || {
    print_error "Not in a git repository"
    exit 1
}
REPO_ROOT=$(cd "$(dirname "$GIT_COMMON_DIR")" && pwd)

# Shared worktree-root resolver (#3530). Redirects the worktree base to an
# external volume when LOOM_WORKTREE_ROOT / worktree.root is configured;
# otherwise returns "$REPO_ROOT/.loom/worktrees" unchanged.
# shellcheck source=lib/worktree-root.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/worktree-root.sh"
WORKTREE_ROOT_DIR="$(loom_worktree_root "$REPO_ROOT")"

# Shared default-branch resolver (#3549). Detects the repo's default branch so
# the PR worktree bases on origin/<default> rather than a hardcoded origin/main
# (which fails on master-default repos). Resolve against the main repo context.
# shellcheck source=lib/default-branch.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/default-branch.sh"
if ! DEFAULT_BRANCH="$(cd "$REPO_ROOT" && loom_default_branch)"; then
    print_error "Could not determine the default branch. Set LOOM_DEFAULT_BRANCH or run: git remote set-head origin -a"
    exit 1
fi

WORKTREE_PATH="$WORKTREE_ROOT_DIR/pr-$PR_NUMBER"

# If the worktree already exists, treat it as reusable. The doctor may
# re-enter for the same PR across multiple iterations.
if [[ -d "$WORKTREE_PATH" ]]; then
    if git -C "$REPO_ROOT" worktree list | grep -q "$WORKTREE_PATH"; then
        print_info "PR worktree already exists at $WORKTREE_PATH (reusing)"
        # Refresh the PR branch in case upstream pushed new commits.
        if (cd "$WORKTREE_PATH" && gh pr checkout "$PR_NUMBER" --force >/dev/null 2>&1); then
            print_success "Refreshed PR branch in existing worktree"
        else
            print_warning "Could not refresh PR branch (continuing with existing checkout)"
        fi
        echo "$WORKTREE_PATH"
        exit 0
    else
        print_error "Directory exists but is not a registered worktree: $WORKTREE_PATH"
        print_info "Remove it and retry: rm -rf '$WORKTREE_PATH'"
        exit 1
    fi
fi

print_info "Creating PR worktree for PR #$PR_NUMBER..."
print_info "  Path: $WORKTREE_PATH"

# Create the worktree on a detached HEAD of origin/<default-branch>, then run
# `gh pr checkout` from inside it. This avoids ever touching the
# orchestrator's main worktree HEAD.
mkdir -p "$WORKTREE_ROOT_DIR"

# Fetch origin/<default-branch> so we have something to base the worktree on.
git -C "$REPO_ROOT" fetch origin "$DEFAULT_BRANCH" >/dev/null 2>&1 || \
    print_warning "Could not fetch origin/$DEFAULT_BRANCH (continuing)"

# Use --detach so we don't create a stale branch ref. `gh pr checkout` will
# switch to the PR's branch once we cd into the worktree.
if ! git -C "$REPO_ROOT" worktree add --detach "$WORKTREE_PATH" "origin/$DEFAULT_BRANCH" 2>/dev/null; then
    print_error "Failed to create worktree at $WORKTREE_PATH"
    exit 1
fi

# Write the sentinel BEFORE any PR mutation so merge-pr.sh / loom-clean
# recognize it as Loom-managed even if `gh pr checkout` fails midway.
# Mirrors the heredoc shape used by worktree.sh:761-768 but records the PR
# number instead of the issue number.
cat > "$WORKTREE_PATH/.loom-managed" <<EOF
# Loom-managed worktree marker
# Created by .loom/scripts/pr-worktree.sh
# PR: $PR_NUMBER
# Removing this file makes Loom treat the worktree as user-owned and refuse
# to clean it up automatically.
EOF

# Now check out the PR branch from inside the new worktree. Capture output
# (rather than discarding it) so a failure can be diagnosed instead of just
# reported as opaque — in particular the #6264 collision case below.
CHECKOUT_OUTPUT=""
if ! CHECKOUT_OUTPUT="$(cd "$WORKTREE_PATH" && gh pr checkout "$PR_NUMBER" --force 2>&1)"; then
    print_error "Failed to run 'gh pr checkout $PR_NUMBER' in $WORKTREE_PATH"
    print_warning "$CHECKOUT_OUTPUT"
    # #6264: the most common cause is the PR's branch already being checked
    # out in ANOTHER worktree (git structurally refuses the same branch in
    # two worktrees at once) — most often this same PR's own issue-<N>
    # builder worktree. When that's the case, name the colliding worktree
    # explicitly rather than leaving the caller to guess from the raw git
    # error above; this worktree is left behind on a detached HEAD either
    # way (git already refused the checkout before touching HEAD here).
    if echo "$CHECKOUT_OUTPUT" | grep -qi "already used by worktree"; then
        COLLIDING_WT="$(echo "$CHECKOUT_OUTPUT" | grep -oE "worktree at '[^']*'" | sed -E "s/worktree at '(.*)'/\1/" | head -1)"
        print_error "The PR's branch is already checked out in another worktree${COLLIDING_WT:+ ($COLLIDING_WT)} — this is the #6264 detached-HEAD collision. $WORKTREE_PATH now sits on a detached HEAD instead of the PR branch; do NOT evaluate code in it as-is."
        if [[ -n "$COLLIDING_WT" ]]; then
            print_info "Prefer reusing the colliding worktree directly instead of retrying here: cd '$COLLIDING_WT'"
        fi
    else
        print_info "The worktree was created but the PR branch is not checked out."
        print_info "You can retry: cd '$WORKTREE_PATH' && gh pr checkout $PR_NUMBER"
    fi
    exit 1
fi

# Symlink .mcp.json so MCP servers work in the PR worktree (same pattern
# as worktree.sh).
if [[ -f "$REPO_ROOT/.mcp.json" && ! -e "$WORKTREE_PATH/.mcp.json" ]]; then
    ln -s "$REPO_ROOT/.mcp.json" "$WORKTREE_PATH/.mcp.json" 2>/dev/null || true
fi

print_success "PR worktree ready at $WORKTREE_PATH"
echo "$WORKTREE_PATH"
