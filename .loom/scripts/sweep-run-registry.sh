#!/usr/bin/env bash

# sweep-run-registry.sh - Stable per-sweep-run identity + lightweight peer registry.
#
# Purpose (#3768): give a single `/loom:sweep` invocation ONE stable run id that
# is fixed for the whole sweep, rather than the historical `sweep-$$` — which is
# the PID of each Bash *subshell* and therefore varies within a single sweep
# across tool calls. That instability meant:
#   - concurrent sweeps could not tell their own checkpoints apart from a peer's,
#   - the main-clean baseline path (a fixed constant) was clobbered when a second
#     sweep re-snapshotted it mid-run of the first.
#
# This helper provides:
#   - `new`     — generate + register a stable run id once, at sweep start.
#   - `peers`   — list OTHER live registered sweeps (dead-PID entries are pruned),
#                 so Stage -1 can print a loud, NON-BLOCKING peer-/sweep warning.
#   - `cleanup` — remove this run's own entry (and prune dead peers) at sweep end.
#   - `list`    — dump all registry entries (debug).
#
# Per-run transients (#4450): a run's registry entry is not its only RUN_ID-keyed
# file — `/loom:sweep` also writes a main-clean baseline at
# `.loom/sweep-checkpoint/main-clean-baseline-<RUN_ID>.txt`. Both have the same
# lifetime (one sweep invocation), so whenever this helper removes a registry
# entry — `cleanup` for the run's own entry, or a dead-PID prune for a peer's —
# it removes that run's baseline too. Without this the baselines accumulated
# forever (200+ dead files observed). Bulk pruning of baselines left behind by a
# SIGKILLed sweep is `loom-daemon clean`'s job.
#
# That reaping is only safe if "dead" is judged correctly. Before #4691 it was
# not: the recorded PID was the script's literal `$PPID`, which under an agent
# harness is the ONE-SHOT `<shell> -c …` process spawned for that single tool
# call and reaped seconds later. Every registered run therefore looked dead to
# the very next peer scan, which pruned a LIVE run's entry and deleted its
# main-clean baseline mid-sweep — and, since the entry vanished before the
# baseline was even written, left that baseline orphaned forever. One root cause,
# both reported symptoms (over-eager prune + unbounded leak).
#
# The run id is portable (macOS/Linux, no `uuidgen`): a compact UTC timestamp, a
# PID component, and a random suffix, e.g.
#   sweep-20260722T231500Z-84213-a3f9c1
# It is a free-form string suitable for a checkpoint `task_id` and for embedding
# in a filename (charset restricted to [A-Za-z0-9-]).
#
# Registry entry (atomic write via .tmp + mv):
#   .loom/sweep-run/<RUN_ID>.json
#   {
#     "run_id": "<RUN_ID>",
#     "pid": <liveness PID>,
#     "timestamp": "<ISO 8601 UTC>"
#   }
#
# The "pid" is the LIVENESS handle for peer detection: `peers` treats an entry as
# a live peer only when the PID is still alive, and prunes the entry otherwise —
# the same pattern as the legacy `.loom/daemon-loop.pid` check. It must name the
# long-lived orchestrator/session process that spans the WHOLE sweep, and is
# overridable with `--pid`. The default is resolved by `resolve_liveness_pid`,
# NOT taken as a bare `$PPID` — see the "Liveness (#4691)" section in the source.
#
# Usage:
#   sweep-run-registry.sh new [--pid P]     # print a fresh RUN_ID, register it
#   sweep-run-registry.sh peers <RUN_ID>    # print live peers (one per line), prune dead
#   sweep-run-registry.sh cleanup <RUN_ID>  # remove own entry + baseline, prune dead peers
#   sweep-run-registry.sh list              # print all entries (run_id pid timestamp)
#
# `peers` output format (one live peer per line):
#   <run_id> <pid> <timestamp>
# Empty output means "no live peer sweeps" — the single-sweep (no-peer) case.
#
# Exit codes:
#   0 - success (including "no peers found")
#   1 - usage error

set -euo pipefail

# Print the leading comment header (from line 3 to the first non-comment line) as
# usage text. Derived, not a hard-coded line range, so editing the header can
# never silently truncate `--help`.
usage() {
    awk 'NR < 3 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"
    exit 1
}

# Resolve repo root (handles invocation from worktree subdirs, mirroring
# sweep-checkpoint.sh so both helpers agree on where .loom/ lives).
repo_root() {
    git rev-parse --show-toplevel 2>/dev/null || pwd
}

registry_dir() {
    echo "$(repo_root)/.loom/sweep-run"
}

# Where /loom:sweep keeps its per-run transients (checkpoints + the RUN_ID-keyed
# main-clean baseline). Same derivation as sweep-checkpoint.sh.
checkpoint_dir() {
    echo "$(repo_root)/.loom/sweep-checkpoint"
}

ensure_dir() {
    mkdir -p "$(registry_dir)"
}

# Remove every RUN_ID-keyed artifact of one run: the registry entry AND the
# main-clean baseline the sweep keyed by the same RUN_ID (#4450). Both are
# per-run transients with a one-invocation lifetime. Missing files are a
# silent no-op; the RUN_ID charset is restricted to [A-Za-z0-9-] so the path
# construction is injection-safe.
remove_run_artifacts() {
    local rid="${1:-}"
    [[ -n "$rid" ]] || return 0
    rm -f "$(registry_dir)/${rid}.json"
    rm -f "$(checkpoint_dir)/main-clean-baseline-${rid}.txt"
}

# ---------------------------------------------------------------------------
# Liveness (#4691)
# ---------------------------------------------------------------------------
#
# Is `$1` a one-shot `<shell> -c …` wrapper process?
#
# An agent harness (Claude Code's Bash tool, and any `bash -c`/`zsh -c` wrapper)
# spawns a FRESH shell per tool call and reaps it the moment that call returns.
# Such a process is never a valid liveness handle for a sweep that spans hundreds
# of tool calls. An INTERACTIVE or login shell (no `-c`) is long-lived and IS a
# valid handle, so the `-c` flag — not merely "is a shell" — is the discriminator.
is_oneshot_shell() {
    local pid="${1:-}" comm base args a0 a1
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    comm=$(ps -o comm= -p "$pid" 2>/dev/null) || return 1
    [[ -n "$comm" ]] || return 1
    base="${comm##*/}"
    base="${base#-}" # a login shell reports as "-zsh"
    case "$base" in
        sh | bash | zsh | dash | ksh | ksh93 | mksh) ;;
        *) return 1 ;;
    esac
    args=$(ps -o args= -p "$pid" 2>/dev/null) || return 1
    # argv[1] carries the flags; `-c`, `-lc`, `-ec` … all mean "run this string".
    read -r a0 a1 _ <<< "$args"
    [[ -n "$a0" ]] || return 1
    [[ "${a1:-}" == -*c* ]]
}

# Resolve the PID to record as this run's liveness handle: walk up from $PPID
# past every one-shot shell wrapper to the first ancestor that outlives a single
# tool call (in practice the `claude -p /loom:sweep …` orchestrator). Falls back
# to $PPID whenever `ps` is unavailable or the walk cannot proceed, which is
# exactly the pre-#4691 behavior — never worse.
resolve_liveness_pid() {
    local pid="${PPID:-$$}" parent depth=0
    while ((depth < 8)); do
        is_oneshot_shell "$pid" || break
        parent=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d '[:space:]')
        # Stop at an unreadable parent, or at pid 1 (init is not a sweep owner).
        if ! [[ "$parent" =~ ^[0-9]+$ ]] || ((parent <= 1)); then
            break
        fi
        pid="$parent"
        depth=$((depth + 1))
    done
    echo "$pid"
}

# Is `$1` a live process, biased to fail SAFE (#4691)?
#
# POSIX `kill(2)` has two distinct failure modes and a bare `kill -0` conflates
# them:
#   ESRCH — no such process        → genuinely dead, safe to prune.
#   EPERM — the process EXISTS but this caller may not signal it (different UID,
#           sandbox, namespace)    → NOT dead; pruning it destroys live state.
# `ps -p` answers "does this PID exist?" without needing signal permission, so it
# separates the two without parsing locale-dependent errno strings. A zombie
# (state `Z`) has exited and is only awaiting reaping, so it counts as dead.
pid_is_live() {
    local pid="${1:-}" state
    if ! [[ "$pid" =~ ^[0-9]+$ ]] || ((pid <= 0)); then
        return 1
    fi
    # Fast path: the signal would be deliverable ⇒ definitely alive.
    kill -0 "$pid" 2>/dev/null && return 0
    state=$(ps -o state= -p "$pid" 2>/dev/null | tr -d '[:space:]')
    [[ -n "$state" ]] || return 1        # ESRCH (or no usable `ps`): treat as dead.
    [[ "${state:0:1}" == "Z" ]] && return 1
    return 0 # EPERM and friends: the process exists — fail safe, treat as alive.
}

iso_now() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

# Extract a string field value from a registry JSON file (no jq dependency).
json_field() {
    local file="$1" field="$2"
    sed -n "s/.*\"${field}\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$file" | head -n1
}

# Extract a numeric field value from a registry JSON file (no jq dependency).
json_num() {
    local file="$1" field="$2"
    sed -n "s/.*\"${field}\"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p" "$file" | head -n1
}

# Generate a stable, portable, filename-safe run id.
gen_run_id() {
    local ts pidpart rand
    ts=$(date -u +"%Y%m%dT%H%M%SZ")
    pidpart="$$"
    # Two 16-bit RANDOM draws → 8 hex chars of entropy (bash builtin, portable).
    rand=$(printf '%04x%04x' "$((RANDOM))" "$((RANDOM))")
    echo "sweep-${ts}-${pidpart}-${rand}"
}

cmd_new() {
    local pid
    pid="$(resolve_liveness_pid)"
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --pid)
                pid="${2:-}"
                shift 2
                ;;
            *)
                echo "ERROR: unknown flag '$1'" >&2
                exit 1
                ;;
        esac
    done
    if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
        echo "ERROR: --pid must be a positive integer (got: '$pid')" >&2
        exit 1
    fi

    local run_id target tmp
    run_id=$(gen_run_id)
    ensure_dir
    target="$(registry_dir)/${run_id}.json"
    tmp="${target}.tmp.$$"

    cat > "$tmp" <<EOF
{
  "run_id": "$run_id",
  "pid": $pid,
  "timestamp": "$(iso_now)"
}
EOF
    mv "$tmp" "$target"

    # The RUN_ID is the load-bearing output: the caller captures it and threads it
    # (as a literal) through every subsequent --task-id / baseline path in the sweep.
    echo "$run_id"
}

# Prune any entry whose recorded PID is no longer alive. Optionally skip a
# specific run id (the caller's own, handled separately).
prune_dead() {
    local skip="${1:-}"
    local dir file rid pid
    dir="$(registry_dir)"
    [[ -d "$dir" ]] || return 0
    for file in "$dir"/*.json; do
        [[ -e "$file" ]] || continue
        rid=$(json_field "$file" run_id)
        [[ -n "$skip" && "$rid" == "$skip" ]] && continue
        pid=$(json_num "$file" pid)
        if ! pid_is_live "$pid"; then
            # Dead run: reap its baseline too, so a crashed sweep self-heals on
            # the next sweep's peer scan instead of waiting for the bulk path.
            remove_run_artifacts "$rid"
            # Backstop for a malformed entry with no readable run_id.
            rm -f "$file"
        fi
    done
}

cmd_peers() {
    local self="${1:-}"
    if [[ -z "$self" ]]; then
        echo "ERROR: peers requires a RUN_ID argument" >&2
        exit 1
    fi
    local dir file rid pid ts
    dir="$(registry_dir)"
    [[ -d "$dir" ]] || return 0
    for file in "$dir"/*.json; do
        [[ -e "$file" ]] || continue
        rid=$(json_field "$file" run_id)
        # Skip our own entry.
        [[ "$rid" == "$self" ]] && continue
        pid=$(json_num "$file" pid)
        if pid_is_live "$pid"; then
            ts=$(json_field "$file" timestamp)
            echo "$rid $pid $ts"
        else
            # Dead peer — prune so it never produces a false-positive warning
            # forever, and reap the baseline it left behind (#4450).
            remove_run_artifacts "$rid"
            rm -f "$file"
        fi
    done
}

cmd_cleanup() {
    local self="${1:-}"
    if [[ -z "$self" ]]; then
        echo "ERROR: cleanup requires a RUN_ID argument" >&2
        exit 1
    fi
    # Remove this run's registry entry AND its RUN_ID-keyed main-clean baseline
    # (#4450) — both are transients whose lifetime is this sweep invocation.
    remove_run_artifacts "$self"
    # Opportunistically prune any dead peers (and their baselines) too.
    prune_dead "$self"
}

cmd_list() {
    local dir file rid pid ts
    dir="$(registry_dir)"
    [[ -d "$dir" ]] || return 0
    for file in "$dir"/*.json; do
        [[ -e "$file" ]] || continue
        rid=$(json_field "$file" run_id)
        pid=$(json_num "$file" pid)
        ts=$(json_field "$file" timestamp)
        echo "$rid $pid $ts"
    done
}

main() {
    local cmd="${1:-}"
    shift || true
    case "$cmd" in
        new)     cmd_new "$@" ;;
        peers)   cmd_peers "$@" ;;
        cleanup) cmd_cleanup "$@" ;;
        list)    cmd_list "$@" ;;
        -h|--help|"") usage ;;
        *) echo "ERROR: unknown command '$cmd'" >&2; usage ;;
    esac
}

main "$@"
