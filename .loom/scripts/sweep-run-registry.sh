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
#     "timestamp": "<ISO 8601 UTC, set once at registration>",
#     "heartbeat": "<ISO 8601 UTC, refreshed by `heartbeat`>"
#   }
#
# The "pid" is the LIVENESS handle for peer detection: `peers` treats an entry as
# a live peer only when the PID is still alive, and prunes the entry otherwise —
# the same pattern as the legacy `.loom/daemon-loop.pid` check. It must name the
# long-lived orchestrator/session process that spans the WHOLE sweep, and is
# overridable with `--pid`. The default is resolved by `resolve_liveness_pid`,
# NOT taken as a bare `$PPID` — see the "Liveness (#4691)" section in the source.
#
# The "heartbeat" is a SEPARATE freshness signal from PID liveness (#5896). A
# `/clear` inside the long-lived `claude -p /loom:sweep …` orchestrator does not
# end that OS process, so a same-process `/clear` + re-invoke leaves a registry
# entry whose PID is (and stays) alive forever, indistinguishable by PID alone
# from a genuine live peer sharing that PID. `heartbeat` starts equal to
# `timestamp` at registration and must be refreshed periodically by the running
# sweep (`heartbeat <RUN_ID>`, e.g. at each wave boundary); `peers` uses PID
# liveness to prune the genuinely dead, and — for anything alive that shares the
# CALLER's own PID — heartbeat staleness to label a same-process zombie
# distinctly from a same-process entry that is still actually driving work.
#
# Usage:
#   sweep-run-registry.sh new [--pid P]     # print a fresh RUN_ID, register it
#   sweep-run-registry.sh heartbeat <RUN_ID> # refresh this run's heartbeat (own entry only)
#   sweep-run-registry.sh peers <RUN_ID>    # print live peers (one per line), prune dead
#   sweep-run-registry.sh cleanup <RUN_ID>  # remove own entry + baseline, prune dead peers
#   sweep-run-registry.sh list              # print all entries (run_id pid timestamp heartbeat)
#
# `peers` output format (one non-dead entry per line):
#   <run_id> <pid> <timestamp> <heartbeat> <status>
# where <status> is one of:
#   live             - a different PID than the caller's; an ordinary live peer.
#   live-same-pid    - the SAME PID as the caller, heartbeat still fresh
#                       (genuinely still driving work in this process).
#   stale-same-pid:Nm - the SAME PID as the caller, heartbeat stale for N minutes
#                       (>= SWEEP_RUN_HEARTBEAT_STALE_SECS, default 900) — almost
#                       certainly a pre-`/clear` zombie, not a live peer.
# Empty output means "no live peer sweeps" — the single-sweep (no-peer) case.
# The caller's OWN entry (matched by RUN_ID, not PID) is always excluded,
# regardless of status.
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

# ---------------------------------------------------------------------------
# Heartbeat staleness (#5896)
# ---------------------------------------------------------------------------
#
# How many seconds without a heartbeat refresh before a same-PID entry is
# labeled `stale-same-pid` in `peers` output. Overridable for tests/tuning;
# default 15 minutes aligns with the documented "refresh at each wave
# boundary" cadence in the sweep skill — a wave can legitimately take a few
# minutes, so the threshold must clear ordinary inter-wave gaps.
HEARTBEAT_STALE_SECS="${SWEEP_RUN_HEARTBEAT_STALE_SECS:-900}"

# Parse an ISO 8601 UTC timestamp (as written by iso_now) to epoch seconds.
# Tries GNU date first (Linux), then BSD/macOS date. Empty/unparseable input
# is reported via a non-zero exit, never a bogus epoch value.
iso_to_epoch() {
    local iso="${1:-}"
    [[ -n "$iso" ]] || return 1
    date -u -d "$iso" +%s 2>/dev/null && return 0
    date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$iso" +%s 2>/dev/null && return 0
    return 1
}

# Age, in seconds, of a heartbeat timestamp relative to now. Non-zero exit
# (and no stdout) if the timestamp cannot be parsed — the caller must treat
# that as "unknown", never as "stale" (fail-safe: an unparseable heartbeat
# must never manufacture a false stale-same-pid label).
heartbeat_age_secs() {
    local hb="${1:-}" hb_epoch now_epoch
    hb_epoch=$(iso_to_epoch "$hb") || return 1
    now_epoch=$(date -u +%s)
    echo $((now_epoch - hb_epoch))
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

    local run_id target tmp now
    run_id=$(gen_run_id)
    ensure_dir
    target="$(registry_dir)/${run_id}.json"
    tmp="${target}.tmp.$$"
    now="$(iso_now)"

    cat > "$tmp" <<EOF
{
  "run_id": "$run_id",
  "pid": $pid,
  "timestamp": "$now",
  "heartbeat": "$now"
}
EOF
    mv "$tmp" "$target"

    # The RUN_ID is the load-bearing output: the caller captures it and threads it
    # (as a literal) through every subsequent --task-id / baseline path in the sweep.
    echo "$run_id"
}

# Refresh this run's OWN heartbeat (#5896). Preserves `timestamp` (the
# original registration time) and `pid` — only `heartbeat` advances. This is
# what the running sweep calls periodically (documented cadence: each wave
# boundary) so a peer scan can tell "still driving this run" apart from a
# same-PID zombie left behind by a same-process `/clear` + re-invoke.
cmd_heartbeat() {
    local self="${1:-}"
    if [[ -z "$self" ]]; then
        echo "ERROR: heartbeat requires a RUN_ID argument" >&2
        exit 1
    fi
    local file tmp pid orig_ts
    file="$(registry_dir)/${self}.json"
    if [[ ! -f "$file" ]]; then
        echo "ERROR: no registry entry for RUN_ID '$self' (heartbeat requires a prior 'new')" >&2
        exit 1
    fi
    pid=$(json_num "$file" pid)
    orig_ts=$(json_field "$file" timestamp)
    tmp="${file}.tmp.$$"
    cat > "$tmp" <<EOF
{
  "run_id": "$self",
  "pid": $pid,
  "timestamp": "$orig_ts",
  "heartbeat": "$(iso_now)"
}
EOF
    mv "$tmp" "$file"
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
    local dir file rid pid ts hb self_pid self_file
    dir="$(registry_dir)"
    [[ -d "$dir" ]] || return 0

    # Resolve the CALLER's own liveness PID so a peer entry sharing that exact
    # PID (#5896 — the post-`/clear`-and-reinvoke case, same long-lived
    # orchestrator process) can be distinguished from a genuine live peer under
    # a different PID. Prefer the caller's own registered entry (authoritative:
    # it is exactly the PID `new` recorded for THIS run); fall back to
    # resolve_liveness_pid() — the same default `new` itself uses — if the
    # caller's own entry cannot be read (e.g. `peers` invoked before `new`).
    self_file="$dir/${self}.json"
    if [[ -f "$self_file" ]]; then
        self_pid=$(json_num "$self_file" pid)
    fi
    [[ -n "${self_pid:-}" ]] || self_pid="$(resolve_liveness_pid)"

    for file in "$dir"/*.json; do
        [[ -e "$file" ]] || continue
        rid=$(json_field "$file" run_id)
        # Skip our own entry.
        [[ "$rid" == "$self" ]] && continue
        pid=$(json_num "$file" pid)
        if ! pid_is_live "$pid"; then
            # Dead peer — prune so it never produces a false-positive warning
            # forever, and reap the baseline it left behind (#4450).
            remove_run_artifacts "$rid"
            rm -f "$file"
            continue
        fi
        ts=$(json_field "$file" timestamp)
        hb=$(json_field "$file" heartbeat)
        # Backward compat: an entry written by a pre-#5896 registry has no
        # "heartbeat" field — treat its registration time as the last known
        # activity rather than failing the age computation.
        [[ -n "$hb" ]] || hb="$ts"

        if [[ "$pid" == "$self_pid" ]]; then
            local age_secs age_min
            if age_secs=$(heartbeat_age_secs "$hb") && ((age_secs >= HEARTBEAT_STALE_SECS)); then
                age_min=$((age_secs / 60))
                echo "$rid $pid $ts $hb stale-same-pid:${age_min}m"
            else
                # Fresh heartbeat, or age unparseable (fail safe: never claim
                # stale on an unknown age) — a genuinely live same-process run.
                echo "$rid $pid $ts $hb live-same-pid"
            fi
        else
            echo "$rid $pid $ts $hb live"
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
    local dir file rid pid ts hb
    dir="$(registry_dir)"
    [[ -d "$dir" ]] || return 0
    for file in "$dir"/*.json; do
        [[ -e "$file" ]] || continue
        rid=$(json_field "$file" run_id)
        pid=$(json_num "$file" pid)
        ts=$(json_field "$file" timestamp)
        hb=$(json_field "$file" heartbeat)
        [[ -n "$hb" ]] || hb="$ts"
        echo "$rid $pid $ts $hb"
    done
}

main() {
    local cmd="${1:-}"
    shift || true
    case "$cmd" in
        new)       cmd_new "$@" ;;
        heartbeat) cmd_heartbeat "$@" ;;
        peers)     cmd_peers "$@" ;;
        cleanup)   cmd_cleanup "$@" ;;
        list)      cmd_list "$@" ;;
        -h|--help|"") usage ;;
        *) echo "ERROR: unknown command '$cmd'" >&2; usage ;;
    esac
}

main "$@"
