#!/usr/bin/env bash
# spawn-worker.sh - Runtime-dispatch seam in front of the worker spawn path.
#
# This thin dispatcher is the single indirection point that turns "the worker
# is always Claude Code" into "the worker is whichever runtime adapter the
# operator selected". Claude Code is adapter #1 (`spawn-claude.sh`); a future
# Codex adapter slots in as `spawn-codex.sh` behind the same seam with no
# caller change. It mirrors the multi-runtime fork's `spawn-worker.sh` so the
# two trees converge (epic #4167, Phase 1).
#
# Zero behavior change: with no `LOOM_RUNTIME` env and no `runtimes.default`
# in `.loom/config.json`, this execs `spawn-claude.sh` with the args
# verbatim — byte-for-byte the same worker Loom spawned before this seam
# existed. Existing callers that invoke `spawn-claude.sh` directly are
# unaffected; migrating them to `spawn-worker.sh` is a follow-up.
#
# Runtime resolution (standard precedence, highest first):
#   1. LOOM_RUNTIME env var (non-empty).
#   2. `.loom/config.json` -> runtimes.default.
#   3. Built-in default: "claude".
#
# Dispatch: exec "$SCRIPT_DIR/spawn-<runtime>.sh" "$@" — all args forwarded
# verbatim, and the runner's exit code is passed through by exec.
#
# Failure: an unknown runtime (no matching `spawn-<runtime>.sh` on disk) exits
# 78 (EX_CONFIG) with a message naming the resolved runtime, where it was
# resolved from (env vs config vs default), and the runners actually present.
#
# Sweep/role-runner scheduling priority (issue #4233): applied inside
# `spawn-<runtime>.sh` (currently only `spawn-claude.sh`), NOT here — this
# dispatcher only ever `exec`s into a runner, which preserves the pid, so a
# runner-level `nice`/`taskpolicy` re-exec still covers every process this
# script itself would otherwise have spawned. Deliberately not duplicated at
# this layer to avoid a double-apply; a future `spawn-<runtime>.sh` adopting a
# new runtime should apply its own priority policy the same way
# `spawn-claude.sh` does, not rely on this dispatcher for it.
#
# Usage:
#   .loom/scripts/spawn-worker.sh -p "your prompt"
#   LOOM_RUNTIME=claude .loom/scripts/spawn-worker.sh --use-wrapper -p "..."
#
# Env vars:
#   LOOM_RUNTIME     Selects the runtime adapter (highest precedence). An empty
#                    value is treated as unset (falls through to config/default).
#   LOOM_WORKSPACE   Override repo-root detection (used only to locate
#                    `.loom/config.json`).

set -euo pipefail

# --- Logging helpers (match loom convention) ---
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[$(date -u '+%Y-%m-%dT%H:%M:%SZ')]${NC} $*" >&2; }
log_warn() { echo -e "${YELLOW}[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] WARN${NC} $*" >&2; }
log_error() { echo -e "${RED}[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] ERROR${NC} $*" >&2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Repo root resolution (handles worktrees) ---
# Only used to locate `.loom/config.json`. Mirrors spawn-claude.sh:
#   1. Trust LOOM_WORKSPACE if set.
#   2. Else derive from `git rev-parse --git-common-dir` (works in worktrees).
#   3. Else fall back relative to this script (.loom/scripts -> repo root).
_resolve_workspace() {
    if [[ -n "${LOOM_WORKSPACE:-}" ]]; then
        printf '%s\n' "$LOOM_WORKSPACE"
        return
    fi

    local git_common_dir
    if git_common_dir="$(git rev-parse --git-common-dir 2>/dev/null)"; then
        if [[ ! "$git_common_dir" = /* ]]; then
            git_common_dir="$(cd "$git_common_dir" && pwd)"
        fi
        printf '%s\n' "$(dirname "$git_common_dir")"
        return
    fi

    cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

# --- List the spawn-<runtime>.sh runners present on disk (for messages) ---
# Echoes a space-separated list of runtime names (the `<runtime>` slug), with
# the dispatcher itself (`worker`) excluded.
_available_runtimes() {
    local f name
    local found=()
    for f in "$SCRIPT_DIR"/spawn-*.sh; do
        [[ -e "$f" ]] || continue
        name="$(basename "$f")"
        name="${name#spawn-}"
        name="${name%.sh}"
        [[ "$name" == "worker" ]] && continue
        found+=("$name")
    done
    printf '%s' "${found[*]:-<none>}"
}

# --- Resolve the runtime (env > config > default) ---
RUNTIME=""
RUNTIME_SOURCE=""

if [[ -n "${LOOM_RUNTIME:-}" ]]; then
    RUNTIME="$LOOM_RUNTIME"
    RUNTIME_SOURCE="env (LOOM_RUNTIME)"
else
    _cfg_runtime=""
    _resolver_lib="$SCRIPT_DIR/lib/config-resolver.sh"
    if [[ -f "$_resolver_lib" ]]; then
        # shellcheck source=./lib/config-resolver.sh
        source "$_resolver_lib"
        _workspace="$(_resolve_workspace)"
        # loom_config_get soft-fails to the default (here "") on a missing
        # config file, a missing `runtimes` block, or a missing `jq` — so a
        # bare install with no runtimes config resolves to "claude" below.
        _cfg_runtime="$(loom_config_get "$_workspace" "runtimes.default" "")"
    fi

    if [[ -n "$_cfg_runtime" ]]; then
        RUNTIME="$_cfg_runtime"
        RUNTIME_SOURCE="config (runtimes.default)"
    else
        RUNTIME="claude"
        RUNTIME_SOURCE="default"
    fi
fi

# --- Dispatch ---
RUNNER="$SCRIPT_DIR/spawn-$RUNTIME.sh"
if [[ ! -f "$RUNNER" ]]; then
    log_error "Unknown runtime '$RUNTIME' (resolved from $RUNTIME_SOURCE):"
    log_error "no runner found at $RUNNER."
    log_error "Available runtimes on disk: $(_available_runtimes)."
    log_error "Set LOOM_RUNTIME, or .loom/config.json -> runtimes.default, to a"
    log_error "runtime that has a matching spawn-<runtime>.sh runner."
    exit 78  # EX_CONFIG
fi

log_info "spawn-worker: runtime=$RUNTIME (from $RUNTIME_SOURCE) -> $(basename "$RUNNER")"
echo "# LOOM_RUNTIME_RESOLVED runtime=$RUNTIME" >&2
exec "$RUNNER" "$@"
