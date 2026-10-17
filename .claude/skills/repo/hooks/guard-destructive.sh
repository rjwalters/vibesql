#!/usr/bin/env bash
# guard-destructive.sh - PreToolUse hook to block destructive agent commands
#
# Part of Repo Skills (https://github.com/rjwalters/repo), and the CANONICAL
# generic destructive-command guard (rjwalters/repo#30): Loom and other tooling
# defer to this copy rather than shipping their own. It is installed by
# install.sh into .claude/skills/repo/hooks/ and wired into the consumer repo's
# .claude/settings.json PreToolUse -> Bash matcher.
#
# Provenance: the precision work in this file (segment parsing, quote-aware
# splitting, literal-text redaction, the read-only fast path, toggles, decision
# telemetry) was developed in rjwalters/loom and consolidated here. Bare issue
# refs like (#3553)/(#3771) refer to rjwalters/loom issues; refs written
# repo#NN refer to rjwalters/repo.
#
# =============================================================================
# STABLE INTERFACE (the contract downstream tools — e.g. Loom — rely on)
# =============================================================================
#
# Input:  JSON on stdin with .tool_input.command and .cwd (Claude Code
#         PreToolUse hook payload). An empty/absent command allows.
# Output: silence + exit 0  => allow;  otherwise a single JSON object:
#   { "hookSpecificOutput": { "hookEventName": "PreToolUse",
#       "permissionDecision": "deny|ask", "permissionDecisionReason": "..." } }
# Exit:   ALWAYS 0, even on deny/ask/internal error. The "hookEventName" field
#         is REQUIRED by Claude Code's schema — without it the decision is
#         silently discarded and the guard becomes inert.
# Errors: this script MUST never exit non-zero or emit invalid output. Any
#         internal error is caught by the ERR trap, logged to
#         <script-dir>/../logs/hook-errors.log, and resolves to allow (fail
#         open) to prevent infinite retry loops in Claude Code.
#
# Config: guards.* keys are read from BOTH config locations —
#           .claude/skills/repo/config.json   (Repo Skills' own; WINS)
#           .loom/config.json                 (legacy/Loom; fallback)
#         Env vars override config; the REPO_* name wins over the legacy
#         LOOM_* name. Both names are a stable part of this interface.
#
#   Toggle (config key)          REPO_* env                     legacy LOOM_* env              default
#   guards.readOnlyFastPath      REPO_GUARD_READONLY_FASTPATH   LOOM_GUARD_READONLY_FASTPATH   on
#   guards.readOnlyFastPathExtra (config-only extend list)      —                              []
#   guards.positionalMaskAllowlist (config-only extend list)    —                              []
#   guards.sqlDdl                REPO_GUARD_SQL                 LOOM_GUARD_SQL                 on
#   guards.cloudCli              REPO_GUARD_CLOUD               LOOM_GUARD_CLOUD               on
#   guards.reversibleGh          REPO_GUARD_REVERSIBLE_GH       LOOM_GUARD_REVERSIBLE_GH       off (opt-in)
#   guards.decisionLog           REPO_GUARD_DECISION_LOG        LOOM_GUARD_DECISION_LOG        off (opt-in)
#   (decision log path)          REPO_GUARD_DECISION_LOG_FILE   LOOM_GUARD_DECISION_LOG_FILE   <script-dir>/../logs/guard-decisions.log
#   guards.rmScope               REPO_RM_SCOPE                  LOOM_RM_SCOPE                  repo
#   guards.forceScope            REPO_FORCE_SCOPE               LOOM_FORCE_SCOPE               all
#   (default-branch seam)        REPO_DEFAULT_BRANCH            LOOM_DEFAULT_BRANCH            resolved from git
#   worktree.root (config key)   —                              LOOM_WORKTREE_ROOT             <repo>/.loom/worktrees
#
# positionalMaskAllowlist is a config-only array of command names (no single
# env var makes sense for a list, mirroring readOnlyFastPathExtra above): each
# entry masks that command's own quoted POSITIONAL arguments in the ASK-tier
# working copy ONLY (never the catastrophic scan), so a read-only tool's own
# search/dedup text is not misread as an ask-triggering phrase (#195). Absent
# or empty (the default) is a no-op. The command words the two DENY-tier
# consumers of that scan recognize as their subject — grep/egrep/fgrep/rg
# (SQL DDL) and cp/mv/tee/sed (#4178 write confinement) — can never be added,
# regardless of config — see mask_ask_positional_args()'s header comment and
# positional_mask_cmdre()'s consumer audit table.
#
# On/off toggles accept 0/false/no and 1/true/yes. rmScope accepts
# repo|off|permissive; forceScope accepts all|protected|off. Loom-compat
# surfaces (the .loom/config.json fallback, LOOM_* env names, and the
# .loom/worktrees rm allowlist) are permanent parts of this contract, not
# transitional shims — Loom installs no generic guard of its own.
# =============================================================================
#
# IMPORTANT: This hook only fires when Claude Code is invoked with:
#   --dangerously-skip-permissions  <- hooks FIRE
#
# It does NOT fire with:
#   --permission-mode bypassPermissions  <- hooks SKIPPED entirely
#
# If you have a shell alias like 'alias claude="claude --permission-mode bypassPermissions"',
# this safety hook will be silently disabled in interactive sessions.
# Use --dangerously-skip-permissions instead for automation that needs hooks.
#
# Decisions:
#   - Block (deny): Dangerous commands that should never run
#   - Ask: Commands that need human confirmation
#   - Allow: Everything else (exit 0, no output)

# Determine log directory relative to this script's location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd 2>/dev/null || echo ".")"
HOOK_ERROR_LOG="${SCRIPT_DIR}/../logs/hook-errors.log"

# Decision telemetry log (issue #3771) — a SEPARATE JSONL file from
# HOOK_ERROR_LOG. At runtime SCRIPT_DIR is the installed hook's own directory
# (.claude/skills/repo/hooks/), so this resolves to
# .claude/skills/repo/logs/guard-decisions.log in a real install.
# REPO_GUARD_DECISION_LOG_FILE (or the legacy LOOM_ name) overrides the path (a
# test seam; also lets an operator point the log elsewhere). Off by default —
# see decision_log_enabled() below.
DECISION_LOG="${REPO_GUARD_DECISION_LOG_FILE:-${LOOM_GUARD_DECISION_LOG_FILE:-${SCRIPT_DIR}/../logs/guard-decisions.log}}"

# ensure_log_dir <log-file> — create the log file's directory and make that
# directory ignore its own contents (repo#482).
#
# The logs directory is created by the HOOKS, never by install.sh: at runtime it
# resolves to .claude/skills/repo/logs/ inside the consumer repo, and nothing in
# the installed payload ignored it. That left "add a .gitignore rule for the
# guard's runtime logs" as an unstated per-consumer obligation — and a consumer
# who never learned of it carries an untracked hook-errors.log/guard-decisions.log
# in `git status` forever. In a repo whose installed-surface resync gates on a
# clean `git status --porcelain` (Loom's resync-installed.sh does), that one
# untracked log silently stalls every scheduled resync with no obvious cause.
#
# Writing a `*`-only .gitignore into the directory as we create it makes the
# directory ignore its own contents — the .gitignore file included — wherever
# the hooks create it, so no consumer .gitignore rule is needed at all. Doing it
# HERE rather than in install.sh is deliberate: this is the only code that ever
# creates the directory, so it covers a pre-existing install too (the next log
# write drops the file in), a bare hook copy made without running the installer,
# and whichever hook happens to create the directory first.
#
# An existing .gitignore is never overwritten — a consumer who wrote their own
# rules in that file keeps them.
#
# Best-effort like every other logging path in this file: a failed mkdir or
# write NEVER changes a decision and NEVER produces a non-zero exit.
ensure_log_dir() {  # <log-file-path>
    local dir
    dir="$(dirname "$1" 2>/dev/null)" || return 0
    [[ -n "$dir" ]] || return 0
    mkdir -p "$dir" 2>/dev/null || return 0
    [[ -e "$dir/.gitignore" ]] && return 0
    # Grouped so a FAILED redirection-open (unwritable dir) has its bash-level
    # error caught by the group's stderr redirect too — same reason
    # log_guard_decision's append below is grouped.
    { printf '%s\n' \
        "# Runtime output from the installed Repo Skills hooks (repo#482)." \
        "# Machine-local: these logs routinely carry absolute filesystem paths," \
        "# so they must never be committed. This file ignores the whole" \
        "# directory, itself included, so no consumer .gitignore rule is needed." \
        "*" >"$dir/.gitignore"; } 2>/dev/null || true
    return 0
}

# Log a diagnostic error message (best-effort, never fails the script)
log_hook_error() {
    local msg="$1"
    # Ensure log directory exists (and ignores itself — see ensure_log_dir)
    ensure_log_dir "$HOOK_ERROR_LOG"
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [guard-destructive] $msg" >> "$HOOK_ERROR_LOG" 2>/dev/null || true
}

# =============================================================================
# DECISION TELEMETRY (issue #3771) — one JSONL record per deny/ask decision.
#
# Append a machine-readable record to DECISION_LOG each time the guard denies or
# asks, so false-positive friction becomes measurable (which patterns fire, how
# often, before/after a precision fix). Deliberately does NOT log `allow`: the
# #3687 read-only fast path's zero-overhead silent-allow must stay silent, and
# allow-logging would swamp the log with the ~99% common case.
#
# STABLE SCHEMA (the contract #3772's reader/aggregation tooling stacks on — do
# NOT rename fields without considering that dependency), one JSON object per
# line:
#   {"ts":"<UTC>","decision":"deny"|"ask","pattern":"<tag>",
#    "tier":"catastrophic"|"ask","command":"<redacted>","context":"<optional>"}
#     ts       — UTC timestamp, same format as log_hook_error's date -u call.
#     decision — "deny" or "ask".
#     pattern  — a short, stable rule tag (NOT the full free-text reason). For
#                the pattern-array loops it is the matched pattern; the non-loop
#                sites pass a static tag (e.g. "sql-ddl", "rm-protected-path").
#     tier     — "catastrophic" for deny, "ask" for ask.
#     command  — the command string, REDACTED via strip_literal_text() so no raw
#                --body/-m/--title/--notes/--comment secret value is persisted.
#     context  — OPTIONAL free-form diagnostic string, ADDITIVE to the schema
#                (issue #312/rjwalters/loom#312): a call site may pass extra
#                state that a later false-positive review needs but the human-
#                readable permissionDecisionReason never persists anywhere (it
#                is only shown once, inline, in the denied session's own
#                transcript). The `worktree-write-confinement` /
#                `worktree-write-confinement-unresolved-var` tags use it to
#                record the resolved `_WT_MAIN_ROOT` / `_WT_MAIN_ROOT_LOGICAL`
#                roots the containment test actually compared against, so a
#                future audit of this log can tell "the guard resolved an
#                unexpectedly broad root" apart from "the target genuinely
#                sits inside the checkout" WITHOUT reproducing the session.
#                Omitted (absent key, not merely empty-string) when a call site
#                passes none, so every existing record/consumer is unaffected.
#
# Best-effort like log_hook_error: gated by the lazy decision_log_enabled()
# toggle, and a log-write failure (permission denied, disk full, missing dir)
# NEVER changes the deny/ask decision and NEVER causes a non-zero exit. Callers
# invoke it as `log_guard_decision ... || true` so it can never trip the ERR
# trap.
#
# One-liner to summarize fires by pattern (AC — full tooling is #3772):
#   jq -r '.pattern' .claude/skills/repo/logs/guard-decisions.log | sort | uniq -c | sort -rn
# =============================================================================
log_guard_decision() {
    # Args: <decision> <tier> <pattern-tag> [<context>]. The command is read
    # from the global $COMMAND and redacted here. Returns 0 unconditionally.
    decision_log_enabled || return 0
    local decision="$1" tier="$2" tag="${3:-$1}" context="${4:-}"
    local ts redacted line
    ts=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null) || ts=""
    # Redact quoted --body/-m/--title/--notes/--comment values (same redactor the
    # pattern-matching tiers use) so no raw secret text is persisted to a log that
    # aggregates across sessions. Fall back to raw only if redaction produced
    # nothing (awk unavailable) — impossible in practice since jq is required and
    # awk is used throughout.
    redacted=$(strip_literal_text "$COMMAND" 2>/dev/null) || redacted=""
    [[ -n "$redacted" ]] || redacted="$COMMAND"
    # Build the JSONL record with jq so all escaping is correct. If jq fails,
    # skip the write entirely rather than hand-roll a line that might mis-escape.
    # `context` is added to the object only when non-empty — the two jq filters
    # below differ only in whether the `context` key is constructed at all, so
    # the key stays ABSENT (not merely `""`) for every call site that does not
    # pass one, keeping the schema byte-identical for the ~99% of tags that
    # never set it.
    if [[ -n "$context" ]]; then
        line=$(jq -cn \
            --arg ts "$ts" \
            --arg decision "$decision" \
            --arg pattern "$tag" \
            --arg tier "$tier" \
            --arg command "$redacted" \
            --arg context "$context" \
            '{ts:$ts, decision:$decision, pattern:$pattern, tier:$tier, command:$command, context:$context}' \
            2>/dev/null) || return 0
    else
        line=$(jq -cn \
            --arg ts "$ts" \
            --arg decision "$decision" \
            --arg pattern "$tag" \
            --arg tier "$tier" \
            --arg command "$redacted" \
            '{ts:$ts, decision:$decision, pattern:$pattern, tier:$tier, command:$command}' \
            2>/dev/null) || return 0
    fi
    [[ -n "$line" ]] || return 0
    ensure_log_dir "$DECISION_LOG"
    # Group the append so a FAILED >> redirection (unwritable/nonexistent dir)
    # has its bash-level error caught by the group's stderr redirect too — a bare
    # `>> "$f" 2>/dev/null` does not suppress the redirection-open error itself.
    { printf '%s\n' "$line" >> "$DECISION_LOG"; } 2>/dev/null || true
    return 0
}

# Top-level error trap: on ANY unexpected error, output valid JSON "allow"
# and log the failure for debugging. This prevents Claude Code from showing
# "PreToolUse:Bash hook error" which causes infinite retry loops.
trap 'log_hook_error "Unexpected error on line ${LINENO}: ${BASH_COMMAND:-unknown} (exit=$?)"; exit 0' ERR

# Read stdin safely — if cat or jq fails, the ERR trap fires and we allow
INPUT=$(cat 2>/dev/null) || INPUT=""

# Verify jq is available before attempting to parse
if ! command -v jq &>/dev/null; then
    log_hook_error "jq not found in PATH — allowing command (cannot parse input)"
    exit 0
fi

COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null) || COMMAND=""
CWD=$(echo "$INPUT" | jq -r '.cwd // empty' 2>/dev/null) || CWD=""

# If no command to check, allow
if [[ -z "$COMMAND" ]]; then
    exit 0
fi

# =============================================================================
# READ-ONLY FAST PATH (issue #3687) — default ON.
#
# guard-destructive.sh is a PreToolUse/Bash hook, so it fires before EVERY Bash
# tool call. In Bash-dense sessions (remote ops, benchmark drivers) the vast
# majority of those calls are obviously read-only — `git status`, `ls`, `grep`,
# `aws … describe*`, `gh … list` — yet each one still runs the full deny/ask
# gauntlet (~37 grep/awk/sed forks + a git rev-parse, ~179ms measured). This
# block short-circuits that overwhelmingly-common case to a silent `allow` with
# a single bash-builtin structural test (zero forks) plus, only when that test
# passes, one lazy `jq` config read.
#
# SECURITY: a fast path is a guard bypass by construction, so admission is
# purely STRUCTURAL and conservative — never content-sensitive:
#   1. Reject fast-path eligibility if the raw command contains ANY of
#      ;  &  |  <  >  backtick  $(  or a newline. This kills chaining, piping,
#      redirection, and command substitution (so `git status && <force-push>`,
#      `git status; rm -rf /`, `git status $(rm -rf /)`, `git status > /etc/x`
#      all fall through to the full path unchanged).
#   2. Exact first-token (command-word) allowlist — never a wrapper. Because the
#      allowlist is keyed on the literal first token, wrapper forms (`bash -c`,
#      `sh -c`, `eval`, `xargs`, `env … git status`, `sudo git status`) are
#      excluded automatically: their first token isn't allowlisted.
#   3. Verb/subcommand exactness for multi-word tools, chosen to be provably
#      disjoint from every existing deny/ask pattern:
#        git status|log|diff|show  (bare — `git -C /p status` is NOT admitted)
#        ls  grep  rg
#        jq  wc  head  tail        (pure read-only text/JSON filters — none has
#          an in-place-mutation flag, so any args are admitted, #3772)
#        test  [  [[               (boolean file/string test builtins — no
#          mutation surface at all, #3772)
#        find                      (admitted for any args EXCEPT when a dangerous
#          action-primary is present: -delete, -exec, -execdir, -ok, -okdir,
#          -fls, -fprint, -fprint0, -fprintf. Any of those disqualifies eligibility
#          and falls through to the full path — structural, not content-scanned,
#          so a future `find -delete` deny rule is never silently bypassed, #3772)
#        gh <noun> view|list       (never delete/close/archive/…)
#        aws <service> describe*|get*|list*  and  aws s3 ls   (mirrors the
#          verb-anchoring already in CLOUD_ASK_PATTERNS: those verbs are never
#          mutating, so this only skips greps that were going to allow anyway)
#   cat and ssh are DELIBERATELY EXCLUDED from the built-in list:
#     - cat has a narrow existing ASK carve-out (cat …/.ssh/, cat …/.aws/
#       credentials); a blanket cat fast-path would silently skip it.
#     - ssh wraps an OPAQUE remote command string that the raw ALWAYS_BLOCK scan
#       still covers today; fast-pathing any `ssh …` would drop that coverage.
#
# False NEGATIVES (declining eligibility) are always safe — they just fall
# through to the correct, slower existing behavior. False POSITIVES are the only
# danger, so the eligibility test stays maximally conservative.
#
# CONFIG-ORDERING CHOICE: this block runs BEFORE REPO_ROOT is resolved (the git
# rev-parse subprocess below), on purpose — the structural test never needs the
# repo root. Only the toggle/extra-list config read needs a config file, and it
# is resolved LAZILY (only after structural admission already passed) by walking
# up from CWD to the nearest guard config (.claude/skills/repo/config.json or
# legacy .loom/config.json) WITHOUT forking git
# (fastpath_config_root). So a fast-pathed command pays: 1 bash-builtin test +
# (only if eligible) 1 stat-walk + 1 jq read — never the git rev-parse, never a
# deny/ask array, never a log write.
#
# Toggle: guards.readOnlyFastPath (default true) / LOOM_GUARD_READONLY_FASTPATH
# env (0/false/no disables, 1/true/yes forces on; env wins). Optional
# guards.readOnlyFastPathExtra is an EXTEND-ONLY array of literal first-word
# commands (each entry is a full-generality bypass for that command word).
# =============================================================================

# Locate the nearest guard config ROOT by walking up from CWD, fork-free (no git
# rev-parse): a directory holding EITHER Repo Skills' own config
# (.claude/skills/repo/config.json) OR the legacy .loom/config.json. Cached.
# Best-effort: empty when neither is found.
#
# Why the fast path does NOT call guard_cfg()/guard_cfg_array() below: those
# need REPO_ROOT (a `git rev-parse` fork) and this block can `exit 0` before
# REPO_ROOT is resolved. The readers here are the same two-tier contract
# (repo config wins, per-key, over legacy .loom) with the root found by a
# fork-free walk instead; they cost at most 2 file-scoped jq reads.
_FASTPATH_CFG_ROOT=""
_FASTPATH_CFG_ROOT_DONE=""
fastpath_config_root() {
    if [[ -z "$_FASTPATH_CFG_ROOT_DONE" ]]; then
        _FASTPATH_CFG_ROOT_DONE=1
        local d="$CWD"
        if [[ -n "$d" && "$d" == /* ]]; then
            while :; do
                if [[ -f "$d/.claude/skills/repo/config.json" || -f "$d/.loom/config.json" ]]; then
                    _FASTPATH_CFG_ROOT="$d"
                    break
                fi
                [[ "$d" == "/" ]] && break
                local parent="${d%/*}"
                [[ -z "$parent" ]] && parent="/"
                d="$parent"
            done
        fi
    fi
    printf '%s' "$_FASTPATH_CFG_ROOT"
}

# Read guards.<key> as a scalar (jq tostring) from the tiered fast-path config:
# Repo Skills' config first, falling back to the legacy .loom/config.json ONLY
# when the key is absent from (or unreadable in) the first tier -- a tier that
# defines the key wins outright. Malformed JSON reads as absent. Echoes the
# value, or nothing when the key is absent from both tiers.
_fastpath_tiered_get() {
    local key="$1" root cfg value
    root=$(fastpath_config_root)
    [[ -n "$root" ]] || return 0
    for cfg in "$root/.claude/skills/repo/config.json" "$root/.loom/config.json"; do
        [[ -f "$cfg" ]] || continue
        value=$(jq -r --arg k "$key" '((.guards? // {}) | if has($k) and .[$k] != null then (.[$k] | tostring) else empty end)' "$cfg" 2>/dev/null) || value=""
        if [[ -n "$value" ]]; then
            printf '%s' "$value"
            return 0
        fi
    done
    return 0
}

# Array-valued sibling: echoes the elements of guards.<key> one per line from
# whichever tier DEFINES the key first (Repo Skills, else legacy). A defined
# array is a whole-value override, never merged element-wise with the lower
# tier (matches guard_cfg_array()). A non-array / malformed value yields
# nothing. Nothing when the key is absent from both tiers.
_fastpath_tiered_get_array() {
    local key="$1" root cfg has
    root=$(fastpath_config_root)
    [[ -n "$root" ]] || return 0
    for cfg in "$root/.claude/skills/repo/config.json" "$root/.loom/config.json"; do
        [[ -f "$cfg" ]] || continue
        has=$(jq -r --arg k "$key" '((.guards? // {}) | has($k) and .[$k] != null)' "$cfg" 2>/dev/null) || has="false"
        if [[ "$has" == "true" ]]; then
            jq -r --arg k "$key" '(.guards[$k] | if type == "array" then .[] | strings else empty end)' "$cfg" 2>/dev/null || true
            return 0
        fi
    done
    return 0
}

# Resolve the fast-path toggle (config + env), cached. Default true. Only ever
# called after structural admission has already passed, so the jq read stays off
# the hot path for commands that don't structurally qualify.
_FASTPATH_ENABLED_CACHE=""
fastpath_enabled() {
    if [[ -z "$_FASTPATH_ENABLED_CACHE" ]]; then
        local enabled=true raw
        # Only an explicit `false` disables; an absent key on both tiers, or
        # malformed JSON, stays ON -- mirrors sql_guard_enabled().
        raw=$(_fastpath_tiered_get "readOnlyFastPath")
        [[ "$raw" == "false" ]] && enabled=false
        # Env override wins over config; REPO_* wins over the legacy LOOM_* name.
        case "${LOOM_GUARD_READONLY_FASTPATH:-}" in
            0|false|no)  enabled=false ;;
            1|true|yes)  enabled=true ;;
        esac
        case "${REPO_GUARD_READONLY_FASTPATH:-}" in
            0|false|no)  enabled=false ;;
            1|true|yes)  enabled=true ;;
        esac
        _FASTPATH_ENABLED_CACHE="$enabled"
    fi
    [[ "$_FASTPATH_ENABLED_CACHE" == "true" ]]
}

# Shared structural pre-check: reject any chaining/piping/redirection/
# substitution/newline. Pure bash builtins, zero forks.
fastpath_structural_ok() {
    case "$1" in
        *';'*|*'&'*|*'|'*|*'<'*|*'>'*|*'`'*|*'$('*) return 1 ;;
    esac
    [[ "$1" == *$'\n'* ]] && return 1
    return 0
}

# Built-in allowlist admission — bash-builtin regex/case only, zero forks.
fastpath_builtin_admits() {
    local cmd="$1"
    fastpath_structural_ok "$cmd" || return 1
    local -a t
    read -ra t <<< "$cmd"
    local n=${#t[@]}
    (( n >= 1 )) || return 1
    case "${t[0]}" in
        ls|grep|rg)
            return 0
            ;;
        jq|wc|head|tail)
            # Pure read-only text/JSON filters. None writes files or takes an
            # in-place-mutation flag (jq has no `-i`; wc/head/tail never mutate),
            # so any arguments are admitted with no sub-form check.
            return 0
            ;;
        test|'['|'[[')
            # Boolean file/string test builtins — no mutation surface at all.
            return 0
            ;;
        find)
            # find is read-only UNLESS a dangerous action-primary is present.
            # Structurally exclude the write/delete/exec primaries: if ANY token
            # exactly matches one, decline eligibility (fall through to the full
            # path). Pure bash-builtin string compares, zero forks.
            local i
            for (( i = 1; i < n; i++ )); do
                case "${t[i]}" in
                    -delete|-exec|-execdir|-ok|-okdir|-fls|-fprint|-fprint0|-fprintf)
                        return 1
                        ;;
                esac
            done
            return 0
            ;;
        git)
            (( n >= 2 )) || return 1
            case "${t[1]}" in
                status|log|diff|show) return 0 ;;
            esac
            return 1
            ;;
        gh)
            (( n >= 3 )) || return 1
            case "${t[2]}" in
                view|list) return 0 ;;
            esac
            return 1
            ;;
        aws)
            (( n >= 3 )) || return 1
            [[ "${t[1]}" == "s3" && "${t[2]}" == "ls" ]] && return 0
            case "${t[2]}" in
                describe*|get*|list*) return 0 ;;
            esac
            return 1
            ;;
    esac
    return 1
}

# -----------------------------------------------------------------------------
# Read-only search piped to a read-only sink (Loom #5263/#5673). A deliberately
# NARROW carve-out, not a general "pipes are OK" relaxation. Admits ONLY
#     <grep|egrep|fgrep|rg ...> | <read-only-sink ...>
# with exactly ONE real pipe and NO other shell metacharacter (; & < > ` $(
# newline) anywhere -- so wrappers, substitutions, redirections, and compounds
# keep taking the full deny/ask path. The sink allowlist is fixed:
#   head|tail|wc  -- pure read-only filters, any args (already fully admitted
#                    by fastpath_builtin_admits()).
#   cat           -- stdin only: any positional operand declines, so
#                    `grep x | cat ~/.ssh/id_rsa` still reaches the cat ASK.
# Intentional difference from Loom: less/more are NOT admitted (less -o /
# --log-file and the pager's shell escapes are write/exec surfaces).
# Downstream destructive commands cannot be skipped: any sink word outside the
# list (sh, tee, xargs, mysql, ...) declines and takes the full path.
# False negatives are safe; only false positives are dangerous.
# -----------------------------------------------------------------------------
_FASTPATH_PIPE_SINKS_ANYARG=" head tail wc "
_FASTPATH_PIPE_SINKS_STDIN=" cat "

# Quote/escape-aware pipe count, pure bash. A `|` inside quotes or after an
# unquoted backslash is data. Sets _FASTPATH_REAL_PIPE_COUNT (-1 on an
# unterminated quote -- never trust a partial scan) and _FASTPATH_REAL_PIPE_POS
# (offset of the first real pipe; meaningful only when the count is 1).
_fastpath_count_real_pipes() {
    local s="$1"
    local -i i=0 n=${#s} count=0 pos=-1
    local mode=0 c   # 0=unquoted 1=single-quoted 2=double-quoted
    while (( i < n )); do
        c="${s:i:1}"
        case "$mode" in
            0)
                case "$c" in
                    "'") mode=1 ;;
                    '"') mode=2 ;;
                    '\') i=$(( i + 1 )) ;;
                    '|') count=$(( count + 1 )); if (( pos == -1 )); then pos=$i; fi ;;
                esac
                ;;
            1)
                if [[ "$c" == "'" ]]; then mode=0; fi
                ;;
            2)
                case "$c" in
                    '"') mode=0 ;;
                    '\') i=$(( i + 1 )) ;;
                esac
                ;;
        esac
        i=$(( i + 1 ))
    done
    if (( mode != 0 )); then
        count=-1
    fi
    _FASTPATH_REAL_PIPE_COUNT=$count
    _FASTPATH_REAL_PIPE_POS=$pos
}

fastpath_grep_pipe_admits() {
    local cmd="$1"
    case "$cmd" in
        *';'*|*'&'*|*'<'*|*'>'*|*'`'*|*'$('*) return 1 ;;
    esac
    [[ "$cmd" == *$'\n'* ]] && return 1
    [[ "$cmd" == *'|'* ]] || return 1
    _fastpath_count_real_pipes "$cmd"
    (( _FASTPATH_REAL_PIPE_COUNT == 1 )) || return 1
    local left="${cmd:0:_FASTPATH_REAL_PIPE_POS}"
    local right="${cmd:_FASTPATH_REAL_PIPE_POS+1}"
    local -a lt rt
    read -ra lt <<< "$left"
    read -ra rt <<< "$right"
    (( ${#lt[@]} >= 1 && ${#rt[@]} >= 1 )) || return 1
    case "${lt[0]}" in
        grep|egrep|fgrep|rg) ;;
        *) return 1 ;;
    esac
    # rg --pre / --hostname-bin run an arbitrary program; decline (full path)
    # if the search side mentions either anywhere, even inside quotes.
    case "$left" in
        *--pre*|*--hostname-bin*) return 1 ;;
    esac
    local sink="${rt[0]}"
    if [[ "$_FASTPATH_PIPE_SINKS_ANYARG" == *" $sink "* ]]; then
        return 0
    fi
    if [[ "$_FASTPATH_PIPE_SINKS_STDIN" == *" $sink "* ]]; then
        local i
        for (( i = 1; i < ${#rt[@]}; i++ )); do
            case "${rt[i]}" in
                -*) ;;
                *) return 1 ;;
            esac
        done
        return 0
    fi
    return 1
}

# Words the extend-only escape hatch may NOT claim (Loom #4791): denial-floor
# command words and shell/exec wrappers. A configured entry naming one is
# IGNORED so the command takes the full deny/ask path -- a config file can never
# fast-path past the floor. Checked BEFORE the config read. Rejecting can only
# add work, never remove protection.
_fastpath_extra_reserved() {
    case "$1" in
        rm|git|gh|aws|docker|curl|wget|halt|reboot|poweroff|shutdown|init)
            return 0 ;;
        sudo|doas|env|eval|exec|xargs|nohup|timeout|ssh|bash|sh|zsh|ksh|dash|fish|python|python3|perl|ruby|node)
            return 0 ;;
    esac
    return 1
}

# Optional extend-only escape hatch: guards.readOnlyFastPathExtra is an array of
# literal first-word commands. Read lazily (only when the built-in list did not
# admit) and cached. Each entry is a full-generality bypass for that word.
_FASTPATH_EXTRA_CACHE=""
_FASTPATH_EXTRA_DONE=""
fastpath_extra_admits() {
    local cmd="$1"
    fastpath_structural_ok "$cmd" || return 1
    local -a t
    read -ra t <<< "$cmd"
    (( ${#t[@]} >= 1 )) || return 1
    local first="${t[0]}"
    _fastpath_extra_reserved "$first" && return 1
    if [[ -z "$_FASTPATH_EXTRA_DONE" ]]; then
        _FASTPATH_EXTRA_DONE=1
        _FASTPATH_EXTRA_CACHE=$(_fastpath_tiered_get_array "readOnlyFastPathExtra" 2>/dev/null) || _FASTPATH_EXTRA_CACHE=""
    fi
    [[ -n "$_FASTPATH_EXTRA_CACHE" ]] || return 1
    local w
    while IFS= read -r w; do
        [[ -n "$w" && "$first" == "$w" ]] && return 0
    done <<< "$_FASTPATH_EXTRA_CACHE"
    return 1
}

# Fast-path dispatch. The env fast-disable check is first so a fully-disabled
# feature stays entirely off the hot path (no structural test, no config read).
# REPO_* wins over the legacy LOOM_* name (fastpath_enabled applies the same
# precedence for the enable direction).
_fastpath_env="${REPO_GUARD_READONLY_FASTPATH:-${LOOM_GUARD_READONLY_FASTPATH:-}}"
if [[ "$_fastpath_env" != "0" && "$_fastpath_env" != "false" && "$_fastpath_env" != "no" ]]; then
    if fastpath_builtin_admits "$COMMAND"; then
        # Silent allow: no stdout/stderr, no log_hook_error, before REPO_ROOT.
        fastpath_enabled && exit 0
    elif fastpath_grep_pipe_admits "$COMMAND"; then
        # Read-only search piped to a read-only sink -- same silent allow.
        fastpath_enabled && exit 0
    elif fastpath_extra_admits "$COMMAND"; then
        fastpath_enabled && exit 0
    fi
fi

# Resolve repo root from cwd (handles worktree paths safely)
REPO_ROOT=""
if [[ -n "$CWD" ]] && [[ -d "$CWD" ]]; then
    REPO_ROOT=$(git -C "$CWD" rev-parse --show-toplevel 2>/dev/null || true)
elif [[ -n "$CWD" ]]; then
    # CWD doesn't exist (e.g., deleted worktree) — log but continue without repo root
    log_hook_error "cwd does not exist: $CWD — skipping repo root resolution"
fi

# =============================================================================
# Shared config reader for the guards.* toggles below.
#
# guard_cfg <key> — echo the raw value of .guards.<key> (via jq tostring, so
# booleans arrive as "true"/"false" and strings as their bare text), or "unset"
# when the key is absent, the file is missing/malformed, or there is no repo
# root. Repo Skills' own config (.claude/skills/repo/config.json) WINS over the
# legacy Loom location (.loom/config.json). Best-effort: any jq failure reads
# as unset and never trips the ERR trap; each caller applies its own default
# and polarity, so a malformed config always falls through to that caller's
# safe default.
# =============================================================================
guard_cfg() {
    local key="$1" cfg val
    for cfg in "$REPO_ROOT/.claude/skills/repo/config.json" "$REPO_ROOT/.loom/config.json"; do
        [[ -n "$REPO_ROOT" && -f "$cfg" ]] || continue
        val=$(jq -r --arg k "$key" '((.guards? // {}) | if has($k) then (.[$k] | tostring) else "unset" end)' "$cfg" 2>/dev/null) || val="unset"
        if [[ -n "$val" && "$val" != "unset" ]]; then
            printf '%s' "$val"
            return 0
        fi
    done
    printf 'unset'
}

# =============================================================================
# Array-valued sibling of guard_cfg() above, for a `guards.<key>` that holds a
# JSON array of strings rather than a scalar. Same dual-location resolution
# and per-file "key ABSENT falls through, key PRESENT (even as []) wins"
# contract as guard_cfg() — the first config file that actually DEFINES the
# key wins outright, matching guard_cfg()'s "unset" sentinel semantics rather
# than a naive "first non-empty array" scan. Each array element is echoed on
# its own line. Best-effort: any jq failure (malformed JSON, non-array value)
# yields no output and never trips the ERR trap.
# =============================================================================
guard_cfg_array() {
    local key="$1" cfg has
    for cfg in "$REPO_ROOT/.claude/skills/repo/config.json" "$REPO_ROOT/.loom/config.json"; do
        [[ -n "$REPO_ROOT" && -f "$cfg" ]] || continue
        has=$(jq -r --arg k "$key" '((.guards? // {}) | has($k))' "$cfg" 2>/dev/null) || has="false"
        if [[ "$has" == "true" ]]; then
            jq -r --arg k "$key" '(.guards[$k] // []) | .[]' "$cfg" 2>/dev/null
            return 0
        fi
    done
    return 0
}

# =============================================================================
# ASK-tier positional-argument masking allowlist (guards.positionalMaskAllowlist,
# #195) — resolved lazily (only invoked once mask_ask_positional_args() below
# is about to run) and cached, mirroring sql_guard_enabled()'s lazy-config-read
# discipline so the jq/array read never touches the hot path for the majority
# of commands that don't reach the ASK-tier working-copy build. Builds an ERE
# alternation of the configured command names, each ERE-metacharacter-escaped
# so a config-supplied name containing a literal `.` (e.g.
# "./.loom/scripts/check-duplicate.sh") can't accidentally widen the anchor
# regex built from it.
#
# MANDATORY EXCLUSION SET (_POSITIONAL_MASK_NEVER) — these command names are
# UNCONDITIONALLY dropped here, even if a repo configures them, because
# COMMAND_ASK_SCAN feeds two DENY-tier consumers as well as the ask-tier ones
# (audit table below). Masking a command that either deny-tier scan treats as
# its SUBJECT would silently downgrade a hard deny to an allow, which no
# operator config may ever do. See mask_ask_positional_args()'s header comment
# (below, near strip_datasink_literals()) for the per-consumer reasoning.
#
# COMMAND_ASK_SCAN consumer audit (#195 review) — every reader of this scan,
# and whether narrowing it is safe:
#
#   Consumer (search this file)          Tier   Narrowing safe?
#   -----------------------------------  -----  --------------------------------
#   ASK_PATTERNS                         ask    yes — the intended target (#195)
#   parse_force_ops (force-op:*)         ask    yes
#   stash-scope                          ask    yes
#   reversible-gh / git-read-tree        ask    yes
#   cloud-cli                            ask    yes
#   SQL_DDL_PATTERN (sql-ddl)            DENY   NO — scans raw quoted text for a
#                                               literal DDL phrase; masking a
#                                               grep/rg pattern argument blinds
#                                               it. => grep|egrep|fgrep|rg
#   extract_write_targets (#4178         DENY   NO — extracts a write idiom's own
#   worktree-write-confinement)                 target PATH from this scan;
#                                               masking that argument blinds the
#                                               confinement deny. => cp|mv|tee|sed
#   tmpfs_scratch_assignments            DENY   yes — no exclusion needed. It
#   (tmpfs-scratch-dir, shapes 1-3,             reads a leading `VAR=` run and a
#   #454/#461)                                  `--target-dir` flag value, neither
#                                               of which mask_ask_positional_args()
#                                               can reach: that function only masks
#                                               a QUOTED argument immediately after
#                                               an allowlisted command word (plus
#                                               its flags), never a `VAR=value`
#                                               token or a flag's own value.
#
# The deny-tier rows are why this set is hardcoded rather than advisory: with
# `positionalMaskAllowlist: ["cp"]` configured and no exclusion,
# `cp "/tmp/src.txt" "<main-checkout>/evil.sh"` issued from a builder worktree
# went from `deny` (worktree-write-confinement) to ALLOW, because
# extract_write_targets() could no longer see the masked destination path.
# cp/mv/tee/sed are exactly the command words extract_write_targets() recognizes
# as write idioms (its `toks[1] == "tee" / "sed" / "cp" / "mv"` scans; `>`/`>>`
# redirection has no command word and is never maskable by construction), so
# excluding them restores the pre-#195 deny in every configuration.
#
# Comparison is on the BASENAME of the configured entry, so a path-qualified
# spelling (`/bin/cp`, `./tee`) cannot smuggle an excluded name past the set.
#
# GENERAL OPERATOR RULE (the invariant this set enforces mechanically for the
# two known deny-tier subjects): only allowlist a command whose positional
# arguments are INERT TEXT it merely reads. A command that ACTS on its
# positional arguments — writes them as paths (cp/mv/tee/sed) or executes them
# as statements (`psql "DROP TABLE …"`, `sh -c "…"`) — must never be
# allowlisted; masking its arguments hides exactly the text a deny-tier scan
# exists to read. Extend _POSITIONAL_MASK_NEVER whenever a new deny-tier
# consumer of COMMAND_ASK_SCAN is added with a recognizable command word.
# =============================================================================
_POSITIONAL_MASK_NEVER='grep egrep fgrep rg cp mv tee sed'
_POSITIONAL_MASK_CMDRE_CACHE=""
_POSITIONAL_MASK_CMDRE_DONE=""
positional_mask_cmdre() {
    if [[ -z "$_POSITIONAL_MASK_CMDRE_DONE" ]]; then
        _POSITIONAL_MASK_CMDRE_DONE=1
        local raw cmd base never
        local -a escaped=()
        raw=$(guard_cfg_array positionalMaskAllowlist)
        if [[ -n "$raw" ]]; then
            while IFS= read -r cmd; do
                [[ -z "$cmd" ]] && continue
                # Basename comparison against the mandatory exclusion set
                # above: `/bin/cp` and `./tee` are dropped exactly like the
                # bare spellings.
                base="${cmd##*/}"
                local excluded=""
                for never in $_POSITIONAL_MASK_NEVER; do
                    [[ "$base" == "$never" ]] && { excluded=1; break; }
                done
                [[ -n "$excluded" ]] && continue
                escaped+=("$(printf '%s' "$cmd" | sed -E 's/[][(){}.*+?^$|\\]/\\&/g')")
            done <<< "$raw"
        fi
        if [[ ${#escaped[@]} -gt 0 ]]; then
            local joined
            joined=$(IFS='|'; printf '%s' "${escaped[*]}")
            _POSITIONAL_MASK_CMDRE_CACHE="$joined"
        fi
    fi
    printf '%s' "$_POSITIONAL_MASK_CMDRE_CACHE"
}

# =============================================================================
# Shared boolean-toggle resolver: config -> legacy env -> repo env -> cache.
#
# sql_guard_enabled(), cloud_guard_enabled(), reversible_gh_guard_enabled(), and
# decision_log_enabled() below each independently reimplemented this identical
# "resolve a boolean from repo config + env vars, REPO_*-over-legacy-LOOM_*,
# one-shot cache" shape (issue #326) — this helper implements the shared
# *mechanics* exactly once. Each toggle's own doc comment (immediately above
# its thin wrapper) still explains *why* its default polarity and resolution
# order are what they are; that reasoning is toggle-specific and belongs
# there, not here.
#
# Args:
#   $1  cache_var_name    — name of the caller's cache variable (e.g.
#                           _SQL_GUARD_CACHE), read/written by indirect
#                           expansion.
#   $2  config_key        — guard_cfg() key (e.g. sqlDdl).
#   $3  default           — "true" or "false": the resolved value when
#                           config, legacy env, and repo env are all
#                           absent/malformed.
#   $4  legacy_env_name   — name of the legacy LOOM_GUARD_* env var.
#   $5  repo_env_name     — name of the REPO_GUARD_* env var (wins over the
#                           legacy name).
#   $6  disable_pattern   — optional; extended-regex alternation of env
#                           values that disable (default: "0|false|no").
#   $7  enable_pattern    — optional; extended-regex alternation of env
#                           values that enable (default: "1|true|yes").
#
# Resolution order matches every caller exactly: guard_cfg() sets the
# baseline over `default`, then legacy env overrides config, then repo env
# overrides legacy env. Caches the resolved "true"/"false" string into the
# named cache variable so a command that matches multiple patterns for the
# same toggle pays for at most one guard_cfg() (jq) read. The config read
# stays best-effort: any parse failure falls through to `default` and never
# trips the ERR trap.
# =============================================================================
guard_toggle_enabled() {
    local cache_var_name="$1" config_key="$2" default="$3"
    local legacy_env_name="$4" repo_env_name="$5"
    local disable_pattern="${6:-0|false|no}" enable_pattern="${7:-1|true|yes}"
    local cache_val="${!cache_var_name}"
    if [[ -z "$cache_val" ]]; then
        local enabled="$default"
        # Only an explicit true/false from config moves the value; a missing
        # key or malformed config (guard_cfg() returns "unset") leaves it at
        # `default`.
        case "$(guard_cfg "$config_key")" in
            false) enabled=false ;;
            true)  enabled=true ;;
        esac
        # Env override wins over config; REPO_* wins over the legacy LOOM_* name.
        local legacy_val="${!legacy_env_name:-}"
        if [[ "$legacy_val" =~ ^($disable_pattern)$ ]]; then
            enabled=false
        elif [[ "$legacy_val" =~ ^($enable_pattern)$ ]]; then
            enabled=true
        fi
        local repo_val="${!repo_env_name:-}"
        if [[ "$repo_val" =~ ^($disable_pattern)$ ]]; then
            enabled=false
        elif [[ "$repo_val" =~ ^($enable_pattern)$ ]]; then
            enabled=true
        fi
        printf -v "$cache_var_name" '%s' "$enabled"
        cache_val="$enabled"
    fi
    [[ "$cache_val" == "true" ]]
}

# =============================================================================
# Shared mode-toggle resolver: config -> legacy env -> repo env -> cache.
#
# Mode-aware sibling of guard_toggle_enabled() above, for a toggle whose
# resolved value is a named mode string (e.g. "repo"/"off") rather than a
# plain boolean. Used by rm_scope_repo_enabled() below — see its own doc
# comment for *why* its default and resolution order are what they are; this
# helper only implements the shared *mechanics*.
#
# Args:
#   $1  cache_var_name     — name of the caller's cache variable.
#   $2  config_key         — guard_cfg() key.
#   $3  default_mode       — the "on" mode, resolved when config/env are all
#                            absent/malformed (e.g. "repo").
#   $4  off_value          — the "opt-out" mode value (e.g. "off").
#   $5  config_off_pattern — extended-regex alternation of guard_cfg() values
#                            that opt out to `off_value` (e.g. "off|permissive").
#   $6  legacy_env_name    — name of the legacy LOOM_* env var.
#   $7  repo_env_name      — name of the REPO_* env var (wins over legacy).
#   $8  env_on_pattern     — extended-regex alternation of env values that
#                            force `default_mode` (e.g. "repo").
#   $9  env_off_pattern    — extended-regex alternation of env values that
#                            force `off_value` (e.g. "off|0|no|permissive").
#
# Caches the resolved mode string; the predicate returns 0 exactly when the
# cached mode equals `default_mode` — matching each caller's own
# `[[ "$_CACHE" == "<on-mode>" ]]` check.
# =============================================================================
guard_toggle_mode() {
    local cache_var_name="$1" config_key="$2" default_mode="$3" off_value="$4"
    local config_off_pattern="$5" legacy_env_name="$6" repo_env_name="$7"
    local env_on_pattern="$8" env_off_pattern="$9"
    local cache_val="${!cache_var_name}"
    if [[ -z "$cache_val" ]]; then
        local mode="$default_mode"
        if [[ "$(guard_cfg "$config_key")" =~ ^($config_off_pattern)$ ]]; then
            mode="$off_value"
        fi
        # Env override wins over config; REPO_* wins over the legacy LOOM_* name.
        local legacy_val="${!legacy_env_name:-}"
        if [[ "$legacy_val" =~ ^($env_on_pattern)$ ]]; then
            mode="$default_mode"
        elif [[ "$legacy_val" =~ ^($env_off_pattern)$ ]]; then
            mode="$off_value"
        fi
        local repo_val="${!repo_env_name:-}"
        if [[ "$repo_val" =~ ^($env_on_pattern)$ ]]; then
            mode="$default_mode"
        elif [[ "$repo_val" =~ ^($env_off_pattern)$ ]]; then
            mode="$off_value"
        fi
        printf -v "$cache_var_name" '%s' "$mode"
        cache_val="$mode"
    fi
    [[ "$cache_val" == "$default_mode" ]]
}

# =============================================================================
# SQL DDL/DML guard toggle — default ON.
#
# The SQL DDL/DML blocks (DROP DATABASE/TABLE/SCHEMA, TRUNCATE TABLE, and
# DELETE FROM without WHERE) are a category error for repos that are themselves
# database engines, where those statements are the product's own dev/test
# vocabulary. Such repos opt out; everyone else keeps the guard on.
#
# Resolution order (highest precedence first):
#   1. REPO_GUARD_SQL env var, then legacy LOOM_GUARD_SQL
#      (0/false/no disables, 1/true/yes forces on)
#   2. guards.sqlDdl via guard_cfg() — repo config wins over legacy .loom
#      (default true when absent)
#   3. Default: true (guard on)
#
# The resolution runs LAZILY — sql_guard_enabled() is only invoked once a
# command has already matched a SQL DDL/DML pattern, so the jq config read never
# touches the hot path for the ~99% of commands that are not SQL. The result is
# cached so a command matching multiple SQL patterns pays for at most one read.
#
# The config read is best-effort: any parse failure falls through to guard-ON
# and never trips the ERR trap or produces a non-zero exit. Resolution mechanics
# shared via guard_toggle_enabled() above.
# =============================================================================
_SQL_GUARD_CACHE=""
sql_guard_enabled() {
    guard_toggle_enabled _SQL_GUARD_CACHE sqlDdl true LOOM_GUARD_SQL REPO_GUARD_SQL
}

# =============================================================================
# Cloud CLI guard toggle — default ON.
#
# The cloud/docker ASK patterns (mutating aws ec2/lambda/s3/... subcommands and
# docker rm/rmi/stop/kill/restart) prompt for confirmation on every match. For a
# repo whose *purpose* is managing cloud infrastructure (launch/stop/terminate
# dev VMs, build/tear-down containers), that friction is a category error — the
# mutating calls are the product's own dev/test vocabulary. Such repos opt out;
# everyone else keeps the guard on. The genuinely catastrophic aws/docker denies
# in ALWAYS_BLOCK_PATTERNS are NOT gated by this toggle and stay active.
#
# Resolution order (highest precedence first):
#   1. REPO_GUARD_CLOUD env var, then legacy LOOM_GUARD_CLOUD
#      (0/false/no disables, 1/true/yes forces on)
#   2. guards.cloudCli via guard_cfg() — repo config wins over legacy .loom
#      (default true when absent)
#   3. Default: true (guard on)
#
# Mirrors sql_guard_enabled() exactly: cached in _STASH_SCOPE_CACHE, invoked
# LAZILY only after the stash pattern has already matched. Resolution
# mechanics shared via guard_toggle_enabled() above.
# =============================================================================
_STASH_SCOPE_CACHE=""
stash_scope_guard_enabled() {
    guard_toggle_enabled _STASH_SCOPE_CACHE stashScope true LOOM_GUARD_STASH_SCOPE REPO_GUARD_STASH_SCOPE
}

# =============================================================================
# Mirrors sql_guard_enabled() exactly: cached in _CLOUD_GUARD_CACHE, invoked
# LAZILY only after a cloud pattern has already matched so the jq config read
# never touches the hot path for non-cloud commands. The config read is
# best-effort: any parse failure falls through to guard-ON. Resolution
# mechanics shared via guard_toggle_enabled() above.
# =============================================================================
_CLOUD_GUARD_CACHE=""
cloud_guard_enabled() {
    guard_toggle_enabled _CLOUD_GUARD_CACHE cloudCli true LOOM_GUARD_CLOUD REPO_GUARD_CLOUD
}

# =============================================================================
# Reversible-GitHub ask toggle — default OFF (opt-IN; inverse polarity, #3757).
#
# `gh pr close`, `gh issue close`, and `gh label delete` change shared state but
# are trivially reversible — `gh pr reopen`, `gh issue reopen`, and recreating a
# label (a repo with labels.yml restores in one `gh label sync`). A guard whose
# purpose is preventing irreversible loss should not add confirmation friction to
# these: an autonomous agent that closes its own issue/PR as part of a normal
# lifecycle would otherwise stall on a prompt (or, headless, block entirely). So
# they are NO LONGER in the ungated ASK_PATTERNS array; a repo that still wants
# the confirmation can opt IN here. The genuinely hard-to-reverse ops
# (`gh release delete` — published artifacts/tags; `git clean -fd` / `git
# checkout .` / `git restore .` — untracked/uncommitted loss) STAY in the ungated
# ask tier and are unaffected by this toggle.
#
# This is the INVERSE polarity of sql_guard_enabled()/cloud_guard_enabled():
# those default ON (guard active) and are opted OUT; this one defaults OFF (no
# ask) and is opted IN — because enabling it ADDS friction rather than removing
# it. So the default and the absent-key resolution are `false`, not `true`.
#
# Resolution order (highest precedence first):
#   1. REPO_GUARD_REVERSIBLE_GH env var, then legacy LOOM_GUARD_REVERSIBLE_GH
#      (1/true/yes enables the ask, 0/false/no forces it off)
#   2. guards.reversibleGh via guard_cfg() — repo config wins over legacy .loom
#      (default false when absent)
#   3. Default: false (no ask)
#
# Mirrors cloud_guard_enabled()'s lazy/cached shape: cached in
# _REVERSIBLE_GH_GUARD_CACHE, invoked LAZILY only after a reversible-gh pattern
# has already matched so the jq config read never touches the hot path for the
# common (non-matching) case. The config read is best-effort: any parse failure
# falls through to guard-OFF (the default), never blocking. Resolution
# mechanics shared via guard_toggle_enabled() above.
# =============================================================================
_REVERSIBLE_GH_GUARD_CACHE=""
reversible_gh_guard_enabled() {
    guard_toggle_enabled _REVERSIBLE_GH_GUARD_CACHE reversibleGh false \
        LOOM_GUARD_REVERSIBLE_GH REPO_GUARD_REVERSIBLE_GH
}

# =============================================================================
# Decision-telemetry toggle — default OFF (opt-IN; inverse polarity, #3771).
#
# The deny/ask decision log (log_guard_decision() near the top of this file) is
# OFF by default: it writes a new persistent, cross-session artifact of redacted
# commands, so — mirroring the other opt-in data-collection features in Loom
# (transcript archival #3726, the model-cost experiment #3725) — a zero-config
# install sees NO new file and NO behaviour change. An operator enables it to
# measure guard-hook friction.
#
# Same INVERSE polarity as reversible_gh_guard_enabled(): defaults false, the
# absent-key resolution is false, and only an explicit `true` (config) or a
# truthy env value enables it.
#
# Resolution order (highest precedence first):
#   1. REPO_GUARD_DECISION_LOG env var, then legacy LOOM_GUARD_DECISION_LOG
#      (1/true/yes/on enables; 0/false/no/off disables). Overrides config.
#   2. guards.decisionLog via guard_cfg() — repo config wins over legacy .loom
#      (default false when absent).
#   3. Default: false (no decision log written).
#
# Resolved LAZILY and cached in _DECISION_LOG_CACHE, invoked only from inside
# log_guard_decision() (i.e. only once a deny/ask is about to fire), exactly like
# the other toggles — so the config read NEVER touches the hot path for the ~99%
# of commands that neither deny nor ask, and in particular never runs on the
# #3687 read-only fast path (which exits before any deny/ask). The config read is
# best-effort: any parse failure falls through to guard-OFF (the default).
# Resolution mechanics shared via guard_toggle_enabled() above — this toggle is
# the only one of the four booleans that also accepts on/off env spellings, so
# it passes explicit enable/disable patterns rather than the helper's default.
# =============================================================================
_DECISION_LOG_CACHE=""
decision_log_enabled() {
    guard_toggle_enabled _DECISION_LOG_CACHE decisionLog false \
        LOOM_GUARD_DECISION_LOG REPO_GUARD_DECISION_LOG \
        '0|false|no|off' '1|true|yes|on'
}

# =============================================================================
# rm-scope repo mode toggle — default REPO (safe-by-default; opt out to off).
#
# As of issue #3628 (ADR Option B) this guard defaults to `repo` mode: it
# DENIES any rm target that is neither under the repo / worktree areas nor on a
# built-in ephemeral allowlist (system temp dirs + the Claude scratchpad), in
# addition to the catastrophic top-level deny. A zero-config install therefore
# gets outside-repo rm protection out of the box (e.g. `rm -rf
# /Users/someone/important` is DENIED).
#
# The legacy permissive behaviour — block only catastrophic rm targets (root,
# $HOME, bare top-level dirs) and ALLOW every deeper subpath including subpaths
# OUTSIDE the repo — is now an explicit opt-out: guards.rmScope:"off" (or the
# synonym "permissive") / LOOM_RM_SCOPE=off. Consumers who relied on the old
# permissive default must set one of those to restore it.
#
# The catastrophic top-level deny stays unconditional in BOTH modes, so bare
# /tmp and / are still blocked regardless of rmScope.
#
# Resolution order (highest precedence first):
#   1. REPO_RM_SCOPE env var, then legacy LOOM_RM_SCOPE (repo enables;
#      off/0/no/permissive disables). Overrides config. Absent → falls through
#      to config/default.
#   2. guards.rmScope via guard_cfg() — repo config wins over legacy .loom:
#      "off"/"permissive" => off; absent key / any other value / malformed
#      JSON => repo (the default).
#   3. Default: repo (safe-by-default, current behaviour after #3628)
#
# Mirrors sql_guard_enabled() / cloud_guard_enabled(): cached in
# _RM_SCOPE_CACHE, invoked LAZILY only after a candidate rm target survives the
# catastrophic check, so the jq config read never touches the hot path for
# non-rm commands. The config read is best-effort: any parse failure falls
# through to REPO (the safe default) and never trips the ERR trap. Resolution
# mechanics shared via guard_toggle_mode() above (this toggle is 3-valued —
# "repo"/"off" — rather than a plain boolean).
# =============================================================================
_RM_SCOPE_CACHE=""
rm_scope_repo_enabled() {
    guard_toggle_mode _RM_SCOPE_CACHE rmScope repo off 'off|permissive' \
        LOOM_RM_SCOPE REPO_RM_SCOPE repo 'off|0|no|permissive'
}

# Resolve the Loom worktree base dir for repo-scope checks. Mirrors the
# precedence of loom_worktree_root() in defaults/scripts/lib/worktree-root.sh
# (env -> config -> default), replicated inline so the hook stays
# self-contained and best-effort: any failure falls back to the default in-repo
# path and never fails the hook. Only called in repo mode, once per rm scan.
resolve_worktree_root() {
    local repo_root="$1"
    [[ -z "$repo_root" ]] && return 0
    # 1. Env override (highest priority); must be absolute. LOOM_WORKTREE_ROOT
    #    is kept as the only env name — it is a Loom concept and part of the
    #    Loom-compat contract.
    if [[ -n "${LOOM_WORKTREE_ROOT:-}" && "$LOOM_WORKTREE_ROOT" == /* ]]; then
        printf '%s/%s' "${LOOM_WORKTREE_ROOT%/}" "$(basename "$repo_root")"
        return 0
    fi
    # 2. Config key worktree.root (absolute only), read from both config
    #    locations — repo config wins over legacy .loom.
    local config_file
    for config_file in "$repo_root/.claude/skills/repo/config.json" "$repo_root/.loom/config.json"; do
        [[ -f "$config_file" ]] || continue
        local cfg_root
        cfg_root=$(jq -r '.worktree.root? // empty' "$config_file" 2>/dev/null) || cfg_root=""
        if [[ -n "$cfg_root" && "$cfg_root" == /* ]]; then
            printf '%s/%s' "${cfg_root%/}" "$(basename "$repo_root")"
            return 0
        fi
    done
    # 3. Default — in-repo worktrees dir.
    printf '%s/.loom/worktrees' "$repo_root"
}

# =============================================================================
# _force_op_cwd_outside_known_roots() — is the given force-op CWD
# unambiguously OUTSIDE every repo root this guard tracks (the main
# checkout's REPO_ROOT, its default in-repo worktrees dir, and any
# configured/overridden worktree root)?
#
# Used ONLY to narrow the force-op:detached ask (#320) for the case where a
# force op's branch identity is ambiguous (detached HEAD / unresolved) — a
# bare out-of-tree scratch clone (e.g. under /tmp, the standard workaround for
# a chronically stale local main: clone, point remote at origin, fetch,
# `reset --hard`, discard) can leave the working copy detached before the
# reset lands it on a named ref. A hard reset there cannot touch a protected
# branch of THIS repo regardless of what the scratch clone's HEAD resolves
# to, so asking buys no safety and stalls headless/autonomous runs with no
# human to answer.
#
# Deliberately conservative: any directory this function cannot cleanly
# resolve (empty, unreadable, or no known REPO_ROOT to compare against) is
# NOT "outside" — the caller keeps asking exactly as before. This is a
# precision fix, not a policy relaxation: a CWD inside the main checkout or a
# managed worktree, or one this guard cannot classify, must keep asking.
#
# REPO_ROOT SELF-MATCH — INVESTIGATED, NOT CHANGED (#350): REPO_ROOT (resolved
# once, near the top of this file) is `git -C "$CWD" rev-parse --show-toplevel`
# — derived from the SAME $CWD a force op's own cwd can equal directly (no
# `-C`/`cd` offset — e.g. a separate Bash call issued after an earlier
# `cd /tmp/scratch`, so this call's own $CWD already IS the scratch clone).
# When that happens, REPO_ROOT trivially resolves to the scratch clone's OWN
# root, so the plain `"$abs" in "$REPO_ROOT"|"$REPO_ROOT"/*` test below
# self-matches and this function returns "not outside" (still asks) rather
# than exempting — the #320/#330 exemption does not fire for THIS shape of
# the idiom (only for the #350-fixed `cd DIR && git …` single-command shape,
# where -C/cd threading gives `_fcwd` a value genuinely different from
# REPO_ROOT).
#
# This is a DELIBERATE gap, not an oversight: with $CWD as the only signal
# available to a single, stateless hook invocation, "the operator's real main
# checkout, given directly as cwd" and "an out-of-tree scratch clone, given
# directly as cwd" are PROVABLY INDISTINGUISHABLE by path comparison against a
# REPO_ROOT derived from that very same $CWD — both self-match identically,
# both can carry a `guards.forceScope:"protected"` config (a scratch clone of
# THIS repo inherits the tracked `.loom/config.json` verbatim), and both can
# resolve to a real or detached branch. A path-shape heuristic (e.g. "abs sits
# under /tmp") was prototyped and rejected: this file's own test fixtures
# (`make_sql_repo`, via `mktemp -d`) — including the #320/#330 controls that
# assert a self-matching cwd inside the "main checkout" still asks — ALSO live
# under /tmp, so any such heuristic exempts exactly the case those controls
# exist to pin. Soundly resolving this would need a $CWD-independent anchor
# for "the repo this guard installation protects" (e.g. a session-scoped
# project-root env var), which this file — a generic, portable guard installed
# across many unrelated repos — deliberately does not depend on; Loom's own
# dispatcher glue (`.loom/hooks/guard-destructive.sh`) already threads an
# analogous `LOOM_PROJECT_ROOT` for a DIFFERENT purpose (choosing which guard
# to exec) and could in principle export it further, but that is Loom-specific
# scope, not this file's. Fail-closed (keep asking) is preserved rather than
# guessing.
#
# #635 UPDATE: the anchor now exists as an OPTIONAL input. When
# CLAUDE_PROJECT_DIR (exported by Claude Code to hook processes) is set, its
# repository family is added to the known roots, so a scratch-clone cwd can no
# longer launder `-C <main>` / `--git-dir <main>/.git --work-tree <main>`.
# Unset keeps the previous behaviour; set-but-unresolvable fails closed. The
# cwd-equals-scratch self-match above is unchanged (still asks).
# =============================================================================
_force_op_cwd_outside_known_roots() {
    local dir="$1"
    [[ -n "$dir" ]] || return 1     # unresolved/empty — ambiguous, not "outside"
    [[ -d "$dir" ]] || return 1     # can't stat it — ambiguous, not "outside"
    [[ -n "$REPO_ROOT" ]] || return 1   # no known repo root to compare against
    local abs
    abs=$(cd "$dir" 2>/dev/null && pwd -P) || return 1

    case "$abs" in
        "$REPO_ROOT"|"$REPO_ROOT"/*) return 1 ;;
        # The default in-repo worktrees dir is always in scope, even when an
        # external worktree.root / LOOM_WORKTREE_ROOT is configured (mirrors
        # _rm_scope_in_scope()'s equivalent check).
        "$REPO_ROOT/.loom/worktrees"|"$REPO_ROOT/.loom/worktrees"/*) return 1 ;;
    esac

    local wt_root
    wt_root=$(resolve_worktree_root "$REPO_ROOT")
    if [[ -n "$wt_root" ]]; then
        case "$abs" in
            "$wt_root"|"$wt_root"/*) return 1 ;;
        esac
    fi

    # #602: REPO_ROOT is the ACTING checkout, which is a linked worktree when
    # the hook starts there. The repository family (main checkout + every
    # registered worktree + their configured roots) must also count as known,
    # or a reset aimed at the main checkout reads as an unrelated scratch
    # clone. Derived from git metadata only; any lookup failure or malformed
    # result is "not proven outside" (fail closed -> keep asking).
    # #635: REPO_ROOT derives from the hook's own cwd, so when the hook runs
    # inside an unrelated scratch clone the "family" is the scratch clone and
    # the main checkout reads as outside (`cd /tmp/s && git -C <main> reset
    # --hard`). CLAUDE_PROJECT_DIR is a cwd-independent anchor; when set, its
    # repository family is checked too. Set-but-unresolvable fails closed.
    local -a anchors=("$REPO_ROOT")
    if [[ -n "${CLAUDE_PROJECT_DIR:-}" ]]; then
        local anchor_top
        anchor_top=$(git -C "$CLAUDE_PROJECT_DIR" rev-parse --show-toplevel 2>/dev/null) || return 1
        [[ -n "$anchor_top" && -d "$anchor_top" ]] || return 1
        anchor_top=$(cd "$anchor_top" 2>/dev/null && pwd -P) || return 1
        anchors+=("$anchor_top")
        case "$abs" in
            "$anchor_top"|"$anchor_top"/*) return 1 ;;
        esac
    fi

    local abs_common
    abs_common=$(git -C "$abs" rev-parse --git-common-dir 2>/dev/null) || abs_common=""
    if [[ -n "$abs_common" ]]; then
        [[ "$abs_common" == /* ]] || abs_common="$abs/$abs_common"
        abs_common=$(cd "$abs_common" 2>/dev/null && pwd -P) || return 1
    fi

    local -a family_roots=()
    local anchor common main_root fam_wt wt_list line wt_path wt_count
    for anchor in "${anchors[@]}"; do
        common=$(git -C "$anchor" rev-parse --git-common-dir 2>/dev/null) || return 1
        [[ -n "$common" ]] || return 1
        [[ "$common" == /* ]] || common="$anchor/$common"
        common=$(cd "$common" 2>/dev/null && pwd -P) || return 1
        [[ "$(basename "$common")" == ".git" ]] || return 1   # bare/odd layout: ambiguous
        main_root=$(dirname "$common")
        [[ -n "$main_root" && "$main_root" != "/" && -d "$main_root" ]] || return 1

        # A cwd whose own git common dir is ours belongs to the family,
        # however it is spelled.
        [[ -n "$abs_common" && "$abs_common" == "$common" ]] && return 1

        family_roots+=("$main_root" "$main_root/.loom/worktrees")
        fam_wt=$(resolve_worktree_root "$main_root")
        [[ -n "$fam_wt" ]] && family_roots+=("$fam_wt")

        wt_count=0
        wt_list=$(git -C "$anchor" worktree list --porcelain 2>/dev/null) || return 1
        while IFS= read -r line; do
            case "$line" in
                "worktree "*)
                    wt_path="${line#worktree }"
                    [[ "$wt_path" == /* ]] || return 1   # malformed entry
                    family_roots+=("$wt_path")
                    # Also its physical spelling (symlinked prefixes), if it exists.
                    if [[ -d "$wt_path" ]]; then
                        family_roots+=("$(cd "$wt_path" 2>/dev/null && pwd -P)")
                    fi
                    wt_count=$((wt_count + 1))
                    ;;
            esac
        done <<< "$wt_list"
        [[ "$wt_count" -ge 1 ]] || return 1   # empty/unparseable listing: ambiguous
    done

    local r
    for r in "${family_roots[@]}"; do
        [[ -n "$r" && "$r" != "/" ]] || continue
        case "$abs" in
            "$r"|"$r"/*) return 1 ;;
        esac
    done

    return 0
}

# _force_op_dirs_outside_known_roots CWD GITDIR WORKTREE (repo#612) — the
# out-of-tree exemption for a force op that may name an explicit repository
# (--git-dir / GIT_DIR) and/or work tree (--work-tree / GIT_WORK_TREE). The
# ref that moves lives in the git dir, and the files a hard reset rewrites
# live in the work tree. Without a git dir, git DISCOVERS the repository from
# the process cwd; without a work tree, the cwd is the work tree. So the cwd
# is involved unless BOTH selectors are given, and EVERY location involved
# must be provably outside: a scratch work tree driven from the main
# checkout's repository (or vice versa) keeps asking. With neither selector
# this is exactly _force_op_cwd_outside_known_roots CWD.
_force_op_dirs_outside_known_roots() {
    local fcwd="$1" gitdir="$2" worktree="$3"
    if [[ -z "$gitdir" || -z "$worktree" ]]; then
        _force_op_cwd_outside_known_roots "$fcwd" || return 1
    fi
    if [[ -n "$worktree" ]]; then
        _force_op_cwd_outside_known_roots "$worktree" || return 1
    fi
    if [[ -n "$gitdir" ]]; then
        _force_op_cwd_outside_known_roots "$gitdir" || return 1
    fi
    return 0
}

# =============================================================================
# force-op branch-scope toggle — default ALL (preserve current behaviour).
#
# The three generic force-op ASK patterns (git push --force / -f /
# --force-with-lease and git reset --hard) prompt on EVERY match regardless of
# which branch is targeted. For an autonomous/background agent that cannot answer
# an interactive prompt, that stalls the agent on routine own-branch rebase /
# amend / reset work. The genuinely dangerous case is a force op against a
# PROTECTED branch (the repo default plus main/master), which stays a hard deny
# via ALWAYS_BLOCK_PATTERNS for the explicit main/master forms.
#
# guards.forceScope selects the behaviour:
#   "all"       (default) — ask on every force op, exactly as before (#3674).
#   "protected"           — ask only when the resolved target is a protected
#                           branch (repo default / main / master) or the branch
#                           identity is ambiguous (detached HEAD); allow force
#                           ops on the agent's own working branches.
#   "off"                 — never ask/deny on force ops. The unconditional
#                           main/master hard-denies in ALWAYS_BLOCK_PATTERNS
#                           STILL apply in every mode, including "off".
#
# Resolution order (highest precedence first):
#   1. REPO_FORCE_SCOPE env var, then legacy LOOM_FORCE_SCOPE
#      (all/protected/off). Overrides config.
#   2. guards.forceScope via guard_cfg() — repo config wins over legacy .loom:
#      "protected"/"off"; absent key / any other value / malformed JSON =>
#      "all" (the current-behaviour default).
#   3. Default: all (preserve current behaviour byte-for-byte)
#
# Mirrors sql_guard_enabled() / rm_scope_repo_enabled(): cached in
# _FORCE_SCOPE_CACHE, invoked LAZILY only after a command plausibly carries a
# force op, so the jq config read never touches the hot path for the ~99% of
# commands that are not force ops. The config read is best-effort: any parse
# failure falls through to "all" (the safe default) and never trips the ERR trap.
# =============================================================================
_FORCE_SCOPE_CACHE=""
force_scope_mode() {
    if [[ -z "$_FORCE_SCOPE_CACHE" ]]; then
        local mode=all
        # Only "protected"/"off" opt away from the default; a missing key, any
        # other value, or a malformed config (reads as unset) resolves to "all".
        case "$(guard_cfg forceScope)" in
            protected) mode=protected ;;
            off)       mode=off ;;
        esac
        # Env override wins over config; REPO_* wins over the legacy LOOM_* name.
        case "${LOOM_FORCE_SCOPE:-}" in
            all)         mode=all ;;
            protected)   mode=protected ;;
            off)         mode=off ;;
        esac
        case "${REPO_FORCE_SCOPE:-}" in
            all)         mode=all ;;
            protected)   mode=protected ;;
            off)         mode=off ;;
        esac
        _FORCE_SCOPE_CACHE="$mode"
    fi
    printf '%s' "$_FORCE_SCOPE_CACHE"
}

# Resolve the repository's default branch name for the protected-branch set.
# Inlined, offline-first detection mirroring loom_default_branch() in
# defaults/scripts/lib/default-branch.sh, replicated here so the hook stays
# self-contained (same rationale as resolve_worktree_root() mirroring
# loom_worktree_root() rather than sourcing it). Deliberately OMITS the network
# `git ls-remote` fallback — a PreToolUse hook must never touch the network — so
# resolution is env-var / local-ref only; the main/master literals in the
# protected set below cover the common case when local detection yields nothing.
# Best-effort: echoes the branch name or nothing on failure. Only invoked in
# "protected" mode after a force op has already matched.
resolve_default_branch() {
    local dir="$1"
    # 1. Env var override — highest priority (escape hatch + test seam).
    #    REPO_* wins over the legacy LOOM_* name.
    if [[ -n "${REPO_DEFAULT_BRANCH:-}" ]]; then
        printf '%s' "$REPO_DEFAULT_BRANCH"
        return 0
    fi
    if [[ -n "${LOOM_DEFAULT_BRANCH:-}" ]]; then
        printf '%s' "$LOOM_DEFAULT_BRANCH"
        return 0
    fi
    [[ -z "$dir" ]] && return 0
    # 2. Local symbolic ref for origin/HEAD — offline, no network.
    local sref
    sref=$(git -C "$dir" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null || true)
    if [[ -n "$sref" ]]; then
        printf '%s' "${sref#origin/}"
        return 0
    fi
    # 3. Local probe: prefer main, then master, whichever remote ref exists.
    local candidate
    for candidate in main master; do
        if git -C "$dir" show-ref --verify --quiet "refs/remotes/origin/$candidate" 2>/dev/null; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    # 4. No local answer — echo nothing (caller's main/master literals cover it).
    return 0
}

# =============================================================================
# BACKSLASH-ESCAPE HELPERS (shared by BOTH lexers)
#
# The character at position i is BACKSLASH-ESCAPED when it is preceded by an ODD
# number of backslashes (`\x` is an escape; `\\x` is a literal backslash followed
# by an unescaped `x`). Call sites depend on this parity:
#   - a NEWLINE: an escaped newline is a LINE CONTINUATION — the shell removes it
#     and the logical line continues, so pending heredoc BODIES must not start
#     there;
#   - the leading `<` of a `<<`: an escaped `\<` is a literal `<` inside a word,
#     NOT a redirection operator, so `\<<WORD` never opens a heredoc and must not
#     be probed as one (#108);
#   - a QUOTE character: `\"` is literal text, so it never OPENS a quoted span
#     and is never accepted as the authoritative CLOSE of an active one (#113).
#
# `trusted_close()` resolves that last case for the ACTIVE-span bookkeeping the
# two lexers share. Starting from the naive "next quote of the same kind" index
# it:
#   - SKIPS backslash-escaped quotes (`\"` is literal text, never a real close);
#   - returns 0 — meaning "do not record a close, walk the span the legacy way" —
#     when no unescaped candidate exists, or when the candidate directly follows
#     a backslash run (`\\"`). There the escaped BACKSLASH makes the pairing
#     genuinely ambiguous (the quote is real, but the shell re-parses quoting
#     inside `$( )`), and recording it would end the span at a position the
#     legacy walk pairs differently — the one direction that can lose a segment
#     boundary. Returning 0 reproduces the pre-#113 walk for that span exactly.
#   - for a DOUBLE-quoted span (`qc == dqc`), also SKIPS a same-kind quote that
#     sits at a DEEPER `$( )`/backtick substitution nesting depth than the
#     opening quote itself (#453). A `"` nested one substitution level below the
#     opener belongs to the INNER shell's own quoting, not to this span — it is
#     unescaped, so the pre-#453 walk accepted it as this span's close anyway
#     (a "phantom close"), which handed everything from there to the REAL close
#     — genuinely live code the outer `$( )` is still running — to the inert
#     verbatim-copy branch instead of to the active, separator-tracking one. See
#     subst_depth() below for how the per-byte depth is computed, and the #453
#     block in the ml_segment() copy of this guard for the worked repro. Scoped
#     to DOUBLE quotes only: a single-quoted span's close is *always* the very
#     next single-quote byte in real bash — single quotes cannot nest and admit
#     no expansion of any kind, so a `$(` appearing between them is inert LITERAL
#     text with no bearing on where the span ends, and depth-filtering it would
#     wrongly skip the correct (and only) close.
# Every use of THESE HELPERS is narrowing: treating something as
# escaped/ambiguous/depth-mismatched only falls back to (or stays in)
# separator-ACTIVE segmentation. That is a property of the helpers, not an
# unconditional property of the lexers — see the KNOWN LIMIT note on the
# inert-span branch (#130) for the one shape where correcting the active-span
# pairing lets a STRAY unmatched quote pair differently than it did before #113.
#
# Lives in its own awk source string, prepended to BOTH lexer sources, so
# qsplit() and ml_segment() share ONE definition and cannot drift (#113).
# subst_depth() (the `$( )`/backtick nesting-depth precompute trusted_close()
# needs for the DOUBLE-quote check above) lives here too, for the same reason —
# #453 needed it available to ml_segment(), which qsplit()-only placement
# (its pre-#453 home) did not provide.
# =============================================================================
_ESCAPE_AWK='
function bs_escaped(s, i,   bs, p) {
    bs = 0
    for (p = i - 1; p >= 1 && substr(s, p, 1) == "\\"; p--) bs++
    return (bs % 2)
}
# subst_depth(s, d) — fill d[i] with the number of `$( … )`/backtick command
# substitutions enclosing byte i of s (#436).
#
# The opening `$(` / opening backtick bytes carry the INNER depth and the
# closing `)` / closing backtick byte carries the OUTER depth, so a byte is
# "inside a substitution" exactly when d[i] > 0 — boundary bytes included on the
# side they belong to, which is what makes the inner-segment capture in
# subst_inner() terminate on the close.
#
# A plain `(` only nests when a substitution is already open: a genuine TOP-LEVEL
# subshell `( a ; b )` runs its own commands in the calling shell-s pipeline, so
# its separators must stay live. `$((` arithmetic therefore reads as `$(` plus a
# plain `(`, which nests and unnests symmetrically. A `)` never closes a backtick
# span. Escaped openers/closers (`\$(`, `` \` ``) are literal text, matching the
# escape convention the rest of this lexer uses (#113).
#
# QUOTE HANDLING — quote-BLIND at the TOP level, quote-AWARE inside an OPEN
# substitution (issue #539).
#
# At depth 0 an OPENER is recognised whatever quoting surrounds it, exactly like
# the `index(inner, "$(")` probe the active-span branch already uses: a
# `$( … )` inside single quotes is not expanded by the real shell, but treating
# it as a substitution here takes the conservative direction (separators inside
# stay capturable as inner segments). trusted_close() above compensates for this
# at its one call site that matters (the DOUBLE-quote depth check): a
# single-quoted span never uses this depth at all, so the top-level
# quote-blindness there is moot.
#
# Inside an OPEN substitution that same blindness was a WRITE-CONFINEMENT
# BYPASS (issue #539). The inner shell re-parses quoting from scratch, so a `)`
# between the inner shell-s OWN quotes is literal text and does not close the
# substitution — but a quote-blind walk counted it as a close, ending the
# substitution early and misaligning every later quote/paren pairing for EVERY
# consumer of this depth (qsplit(), subst_inner(), subst_heads(),
# trusted_close(), strip_datasink_literals()). From a worktree cwd,
#   echo "$(echo S)S > <main>/e.sh)"          (S = single quote)
#   echo "$(printf ")" )" && cp /tmp/a <main>/f; echo "z"
#   echo "$(printf ")" ; id > <main>/x)"
# and friends therefore ALLOWED a write into the main checkout that bash really
# performs — the #4178 escape reachable purely by quoting a paren, the same
# asymmetry repo#197/repo#439 closed for the other quoted spellings.
#
# qs[k] therefore tracks the inner shell-s open quote character at depth k:
#   - a quote character at depth > 0 with no span open OPENS one; the matching
#     character closes it;
#   - inside a SINGLE-quoted inner span nothing expands at all, so `)`, `(`,
#     `$(` and backticks are all literal;
#   - inside a DOUBLE-quoted inner span `$( )` and backticks STILL expand, so an
#     opener there still raises the depth — and the span it opens begins with a
#     fresh, unquoted parse of its own (qs[dep] = "" on every open);
#   - a BACKTICK span-s close is recognised regardless of qs[], because bash
#     delimits `…` by scanning for the next unescaped backtick without honouring
#     the quoting of the text between.
# Escape parity is bs_escaped() for both quote kinds, matching the convention
# qsplit()/ml_segment()/trusted_close() already use. That is exact for double
# quotes and over-broad for single ones (bash treats a backslash inside S…S as
# literal, so `\S` really does close the span) — over-broad in the fail-closed
# direction: the span stays open longer, the depth stays > 0 longer, and the
# consumers keep re-emitting inner segments rather than dropping them.
#
# SAFETY DIRECTION. Recognising an inner quoted `)` as literal only ever DELAYS
# the close, so a byte-s depth can rise but never fall relative to the old
# walk. Every consumer reads depth > 0 as "inside a substitution", whose
# contents are re-emitted as their own segments (subst_inner()/subst_heads())
# rather than masked — so a delayed close adds segment boundaries. Unbalanced
# input stays fail-closed by the same mechanism the #436 header describes: an
# inner quote with no partner leaves every later byte at depth > 0, where each
# separator still starts an inner segment.
function subst_depth(s, d,   n, i, c, dep, kind, qs, inq, SQ, DQ, BQ) {
    BQ = sprintf("%c", 96)   # backtick
    SQ = sprintf("%c", 39)   # single quote
    DQ = sprintf("%c", 34)   # double quote
    n = length(s)
    split("", d)
    split("", kind)
    split("", qs)            # qs[k] — inner-shell open quote char at depth k
    dep = 0
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        if (!bs_escaped(s, i)) {
            inq = (dep > 0) ? qs[dep] : ""
            # A backtick span ends at the next unescaped backtick whatever the
            # inner text quotes, so its close is resolved BEFORE the quote-state
            # branch below (and abandons any inner span still open at its level).
            if (c == BQ && dep > 0 && kind[dep] == "B") {
                qs[dep] = ""
                dep--
                d[i] = dep
                i++
                continue
            }
            if (inq != "") {
                if (c == inq) { qs[dep] = ""; d[i] = dep; i++; continue }
                if (inq == SQ) { d[i] = dep; i++; continue }
                # inq == DQ: only `$(` and a backtick still act.
                if (c == "$" && i < n && substr(s, i + 1, 1) == "(") {
                    dep++
                    kind[dep] = "P"
                    qs[dep] = ""
                    d[i] = dep
                    d[i + 1] = dep
                    i += 2
                    continue
                }
                if (c == BQ) {
                    dep++
                    kind[dep] = "B"
                    qs[dep] = ""
                    d[i] = dep
                    i++
                    continue
                }
                d[i] = dep
                i++
                continue
            }
            # Unquoted at this depth. A quote character OPENS an inner span —
            # only inside a substitution; at the top level this stays blind.
            if (dep > 0 && (c == SQ || c == DQ)) {
                qs[dep] = c
                d[i] = dep
                i++
                continue
            }
            if (c == "$" && i < n && substr(s, i + 1, 1) == "(") {
                dep++
                kind[dep] = "P"
                qs[dep] = ""
                d[i] = dep
                d[i + 1] = dep
                i += 2
                continue
            }
            if (c == BQ) {
                dep++
                kind[dep] = "B"
                qs[dep] = ""
                d[i] = dep
                i++
                continue
            }
            if (c == "(" && dep > 0) {
                dep++
                kind[dep] = "p"
                qs[dep] = ""
                d[i] = dep
                i++
                continue
            }
            if (c == ")" && dep > 0 && kind[dep] != "B") {
                dep--
                d[i] = dep
                i++
                continue
            }
        }
        d[i] = dep
        i++
    }
}
function trusted_close(s, n, ci, qc, dqc, d, dep0,   j) {
    while (ci > 0 && (bs_escaped(s, ci) || (qc == dqc && d[ci] != dep0))) {
        j = ci + 1
        ci = 0
        for (; j <= n; j++) {
            if (substr(s, j, 1) == qc && (qc != dqc || d[j] == dep0)) { ci = j; break }
        }
    }
    if (ci > 1 && substr(s, ci - 1, 1) == "\\") return 0
    return ci
}
'

# =============================================================================
# LIVE-vs-ESCAPED COMMAND SUBSTITUTION (escaped-backtick false positive)
#
# Several passes in this file ask "does this quoted span carry a command
# substitution?" before treating the span as inert (redacting its bytes, or
# keeping its separators literal). The historical test was a byte-presence
# check:
#
#     index(inner, "$(") == 0 && index(inner, "`") == 0
#
# which cannot tell a backslash-ESCAPED backtick or `\$(` from a live one. An
# escaped backtick inside a double-quoted string is LITERAL TEXT — it is the
# standard way to spell a markdown code span in a shell string — so it carries
# zero execution risk, yet it vetoed the inert treatment and produced a false
# DENY on ordinary text (e.g. `gh pr comment -b "see \`--force\` below"`).
#
# has_live_subst(str) returns 1 only when a backtick, or a `$` immediately
# followed by `(`, is preceded by an EVEN number of backslashes (0, 2, …) —
# i.e. is live at the shell's first parse — and 0 when every occurrence is
# escaped. Parity, not presence: `\`` is escaped, `\\\`` is a literal backslash
# followed by a LIVE backtick.
#
# SAFETY DIRECTION. This only ever NARROWS a false positive; it never widens a
# deny into an allow on live substitution:
#   - a span with any live `$(` or backtick is classified exactly as before;
#   - an escaped-only span expands to a plain string that merely CONTAINS those
#     characters — the same risk class as any other plain literal these passes
#     already redact. It can only execute if something RE-PARSES it, and every
#     pass gates re-parsing separately and independently of this scan (the
#     data-sink command-word anchoring, command_has_shell_segment()'s
#     pipe-to-interpreter check, and the positional-mask allowlist).
#
# Lives in its own awk source string, prepended to each consuming program, so
# the copies cannot drift — the same mechanism as _ESCAPE_AWK above. awk has no
# way to share a function across separately-invoked programs, so the string is
# concatenated at each call site rather than duplicated in source.
#
# NOT applied to dequote_inert_spans() — see the note at its own `index()` test
# for why that one is a deliberately conservative check, not this defect.
# =============================================================================
_HASLIVESUBST_AWK='
function has_live_subst(str,    i, c, bs) {
    bs = 0
    for (i = 1; i <= length(str); i++) {
        c = substr(str, i, 1)
        if (c == "\\") {
            bs++
            continue
        }
        if (bs % 2 == 0) {
            if (c == "`") return 1
            if (c == "$" && substr(str, i + 1, 1) == "(") return 1
        }
        bs = 0
    }
    return 0
}
'

# =============================================================================
# QUOTE-AWARE COMMAND SEGMENTATION (#3755)
#
# The three segment parsers below (parse_force_ops, lifecycle_or_cloud_reason,
# extract_rm_targets) split a command string on the shell separators ; | & && ||
# to find each simple command's command word. The historical split was a naive
#   gsub(/&&|\|\||[;|&]/, "\n")
# over the raw string, which has NO lexer — so a `|`-alternation INSIDE a quoted
# argument (e.g. `grep -E "lifecycle|halt|poweroff"`) was split as if it were a
# real pipe, manufacturing a phantom segment whose command word is the bare word
# `halt` and hard-denying a completely read-only command.
#
# `qsplit()` replaces that gsub: it walks the string tracking single-/double-quote
# state and emits a newline for a separator ONLY when it is OUTSIDE a quoted span.
# A quoted span is treated as inert (its separators are preserved as literal
# text) ONLY when it carries no command substitution — no `$(` and no backtick —
# mirroring strip_literal_text()'s #3679 safety floor: a smuggled
# `"$(a|halt)"` keeps its separators ACTIVE so the genuine protection is intact.
# Such an ACTIVE span still records where it really ENDS, so the character walk
# does not mistake its own closing quote for the opener of a new span (#113) —
# without that, a later unrelated quote paired with the re-opened one and every
# real separator between them (a genuine `; <destructive cmd>` after the span)
# was swallowed as bogus inert text.
# The token VALUES are preserved verbatim (unlike a redaction approach), so
# extract_rm_targets still sees the real `rm` targets. Best-effort like
# strip_literal_text(): where a BACKSLASH makes the quote pairing ambiguous both
# lexers resolve it in the one direction that keeps separators ACTIVE, so the
# result can only gain segment boundaries, never lose them (#113):
#   - a backslash-escaped quote (`\"`) never OPENS a span — it is literal text —
#     so it can no longer start a bogus inert run that swallows real separators;
#   - the close index an ACTIVE span records is resolved by trusted_close():
#     escaped candidates are skipped, and an ambiguous one (`\\"`) records
#     nothing at all so that span is walked exactly the legacy way;
#   - an unterminated quote advances ONE character with separators still active
#     (both lexers) instead of swallowing the remainder of the buffer.
# For every command the shell can actually PARSE, none of these widens a deny
# into an allow; the escaped-quote cases in the #113 test block pin the shapes
# that did. For input with an UNBALANCED quote count the guarantee is weaker —
# see the KNOWN LIMIT note on the inert-span branch (#130).
#
# SEPARATORS INSIDE A COMMAND SUBSTITUTION (#436)
# -----------------------------------------------
# "Keep separators ACTIVE inside a substitution-bearing span" used to mean
# "split the OUTER stream at every `;`/`&`/`|` byte in the span", including one
# that sits INSIDE the substitution's own `$( … )`/backtick boundaries. Such a
# byte is inert to the OUTER shell — it is a pipeline separator of the SUBSHELL,
# never a top-level one — so splitting there tore the enclosing token in half.
# The reported harm: a quoted redirect target
# `> "/tmp/out-$(echo $F|tr -d x).json"` reached extract_write_targets() as the
# fragment `"/tmp/out-$(echo $F`, which no longer looks absolute, so the main
# checkout was prepended and an ordinary /tmp scratch write hard-denied with the
# `worktree-write-confinement` (worktree-isolation-bypass) tag.
#
# qsplit() now models both facts at once instead of trading one for the other:
#   1. subst_depth() precomputes, for every byte, how many `$( … )`/backtick
#      substitutions enclose it (plain `( … )` nests only INSIDE one, so a
#      genuine top-level subshell keeps its separators live). A separator only
#      splits the outer stream when that depth is 0, so the enclosing token —
#      a redirect target, an rm target, a `cd` argument — stays intact.
#   2. subst_inner() re-emits the commands those inner separators really do
#      start as their OWN segments, appended after the outer stream. That is
#      exactly the set of post-separator segments the legacy inline split
#      produced (minus the text that trailed the substitution's close, which
#      belongs to the outer segment), so a smuggled `"$(cat f|sh -c …)"` still
#      exposes `sh` to command_has_shell_segment() and a `"$(x|tee <in-repo>)"`
#      still exposes the write target — the #3679/#3755 safety floor is kept,
#      not relaxed. Text BEFORE a substitution's first inner separator is not
#      re-emitted, matching the legacy behaviour exactly (it stayed part of the
#      enclosing segment there too), so this adds no segment the old lexer did
#      not already produce.
# Unbalanced input stays fail-closed by the same mechanism: an UNCLOSED `$(`
# leaves every later byte at depth > 0, but each separator after it still starts
# an inner segment, so a trailing `; <destructive command>` is still segmented
# and still denied.
# This is deliberately scoped to qsplit() (command_has_shell_segment(),
# resolve_stash_cwd(), extract_write_targets()) — ml_segment() below keeps the
# legacy inline split; its consumers are the three command-word parsers, where
# the split costs no token integrity this change needs to restore.
#
# Shared as a single awk source string so the three parsers cannot drift.
# =============================================================================
_QSPLIT_AWK='
# subst_depth() (the `$( )`/backtick nesting-depth precompute used below and by
# trusted_close()) now lives in the shared _ESCAPE_AWK source string above,
# prepended ahead of this one at every call site (#453) — it moved there so
# ml_segment() could use it too; see that block for the full doc comment.
#
# subst_inner(s, d) — the inner commands that separators INSIDE a substitution
# really do start, as "\n"-prefixed segments to append after the outer stream
# (#436). d[] comes from subst_depth().
#
# One segment per separator at depth > 0, running from just after that separator
# to whichever comes first: the next separator at or below its depth, or the
# close of the substitution that contains it. Text before a substitution-s FIRST
# inner separator is NOT emitted — it stayed part of the enclosing segment under
# the legacy inline split too, so this function only ever reproduces boundaries
# the old lexer already produced.
function subst_inner(s, d,   n, i, c, res, seg, cap, capd, act) {
    n = length(s)
    res = ""
    seg = ""
    cap = 0
    capd = 0
    split("", act)           # act[k] — a capture is open at depth k
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        # Leaving the depth the current capture belongs to ENDS it. Unwind to the
        # nearest still-open enclosing capture, if any, so an inner substitution
        # with its own separators does not silently swallow the rest of the outer
        # one.
        if (cap && d[i] < capd) {
            res = res (QS_MARK ? "\n" sprintf("%c", 5) "H" : "\n") seg
            seg = ""
            while (capd > d[i]) { act[capd] = 0; capd-- }
            cap = (capd > 0 && act[capd]) ? 1 : 0
            # This byte is the substitution-s own closing `)`/backtick — a
            # boundary, never the first byte of the enclosing capture resumed
            # above.
            continue
        }
        if (d[i] > 0 && (c == ";" || c == "&" || c == "|") && !bs_escaped(s, i)) {
            if (cap) { res = res (QS_MARK ? "\n" sprintf("%c", 5) "H" : "\n") seg }
            seg = ""
            cap = 1
            capd = d[i]
            act[capd] = 1
            # `&&` / `||` are one separator, not two.
            if ((c == "&" || c == "|") && i < n && substr(s, i + 1, 1) == c) i++
            continue
        }
        if (cap) seg = seg c
    }
    if (cap) res = res (QS_MARK ? "\n" sprintf("%c", 5) "H" : "\n") seg
    return res
}
# subst_heads(s) — the FIRST (or only) command of every `$( … )`/backtick
# substitution in s, as "\n"-prefixed segments to append after the outer stream
# (repo#439). The exact complement of subst_inner(), which deliberately emits
# only the commands a separator INSIDE a substitution starts and leaves the
# text BEFORE that first separator to the enclosing segment.
#
# THE GAP THIS CLOSES. A command substitution is EXECUTED by the shell
# regardless of the quoting around it, so the "quoted text is inert data"
# redaction that is correct for `echo "rm -rf /"` (repo#53) must never apply to
# a span carrying `$(` or a backtick — the invariant qsplit() already honours by
# keeping such a span-s separators ACTIVE, and the one the guard-equivalence
# harness header states outright. extract_write_targets() did not honour it for
# a substitution holding a SINGLE simple command: with no separator inside,
# subst_inner() emitted nothing, the whole span stayed one quoted token of the
# outer segment, and mask_gt()/mask_ws() then masked its `>` and its spaces as
# quoted data. From a worktree cwd, `echo "$(id > <main>/e.sh)"`,
# `echo "$(cp /tmp/s <main>/e.sh)"` and the backtick spelling therefore ALLOWED
# a write into the main checkout that the shell really performs — the #4178
# worktree-isolation escape, reachable purely by quoting (the same asymmetry
# repo#197 fixed for the catastrophic literals). Re-emitting the head as its own
# segment puts it in front of the write-idiom scan with its tokens intact.
#
# One segment per substitution, running from just after its opener (`$(` or an
# opening backtick) to whichever comes first: the first unescaped `;`/`&`/`|` at
# that substitution-s OWN depth, or the substitution-s close.
#
# Bytes are appended to EVERY still-open enclosing capture as well, so a nested
# substitution appears verbatim inside its parent-s head and no token is torn in
# half — the #436 token-integrity property this must not regress (a redirect
# target `"/tmp/out-$(echo $F|tr -d x).json"` stays whole in the outer stream,
# and the head emitted here, `echo $F`, carries no write idiom).
#
# A plain `(` subshell never starts a head: it only nests when a substitution is
# already open (subst_depth()-s rule), and `$((` arithmetic reads as `$(` plus a
# plain `(`, so `$((1>2))` emits `(1>2)` as one whitespace-bounded token, which
# the token-anchored `>` scan cannot misread as a redirection operator.
#
# Blind to a TOP-LEVEL quoted opener, exactly like subst_depth()/subst_inner()
# and the `index(inner, "$(")` probe in qsplit(): a `$( … )` inside SINGLE
# quotes is not expanded by the real shell, but treating it as a substitution
# here is the conservative direction this lexer already takes everywhere else,
# and consistency with subst_depth() matters more than recovering that one
# allow. INSIDE an open substitution the depth this walks is quote-AWARE
# (#539), so a `)` between the inner shell-s own quotes no longer ends a head
# early.
#
# `sep` (optional, default "\n") is the byte each head is PREFIXED with. The
# one caller passes a byte that cannot be a real segment boundary so it can
# tell one head from the next even when a head carries an embedded newline,
# and quote-mask every head in isolation (see extract_write_targets()).
function subst_heads(s, sep,   d, n, i, c, dep, k, res, cseg, con, top, BQ) {
    BQ = sprintf("%c", 96)   # backtick
    if (sep == "") sep = "\n"
    subst_depth(s, d)
    n = length(s)
    split("", cseg)          # cseg[k] — text captured for the head at depth k
    split("", con)           # con[k]  — that capture is still open
    res = ""
    top = 0                  # deepest capture slot currently in play
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        dep = d[i]
        # A byte SHALLOWER than an open capture is that substitution-s own
        # closing `)`/backtick: the head ends there.
        while (top > dep) {
            if (con[top]) { res = res sep cseg[top]; con[top] = 0; cseg[top] = "" }
            top--
        }
        # `$(` opener. Both bytes belong to the ENCLOSING heads (verbatim
        # nesting), never to the head they open.
        if (c == "$" && i < n && substr(s, i + 1, 1) == "(" && !bs_escaped(s, i)) {
            for (k = 1; k <= top; k++) if (con[k]) cseg[k] = cseg[k] "$("
            top = d[i]       # the inner depth subst_depth() recorded
            con[top] = 1
            cseg[top] = ""
            i += 2
            continue
        }
        # Backtick opener — an opener raises the depth, a closer lowers it, so
        # d[i] > d[i-1] distinguishes the two without re-deriving the pairing.
        if (c == BQ && !bs_escaped(s, i) && dep > (i > 1 ? d[i - 1] : 0)) {
            for (k = 1; k <= top; k++) if (con[k]) cseg[k] = cseg[k] c
            top = dep
            con[top] = 1
            cseg[top] = ""
            i++
            continue
        }
        # A separator at the capture-s OWN depth ends the head; whatever it
        # starts is subst_inner()-s segment, not this function-s.
        if (dep > 0 && con[dep] && (c == ";" || c == "&" || c == "|") && !bs_escaped(s, i)) {
            res = res sep cseg[dep]
            con[dep] = 0
            cseg[dep] = ""
            for (k = 1; k < dep; k++) if (con[k]) cseg[k] = cseg[k] c
            i++
            continue
        }
        for (k = 1; k <= top; k++) if (con[k]) cseg[k] = cseg[k] c
        i++
    }
    # Unclosed `$(`/backtick: emit what was captured rather than dropping it —
    # the same fail-closed direction subst_inner() takes for unbalanced input.
    while (top > 0) {
        if (con[top]) { res = res sep cseg[top]; con[top] = 0 }
        top--
    }
    return res
}
function qsplit(s,   out, n, i, c, j, qc, ci, tc, inner, SQ, DQ, acs, acn, sdep) {
    SQ = sprintf("%c", 39)   # single quote
    DQ = sprintf("%c", 34)   # double quote
    out = ""
    n = length(s)
    split("", acs)           # stack of pending active-span CLOSING quote indexes
    acn = 0
    subst_depth(s, sdep)     # byte -> enclosing `$( … )`/backtick depth (#436)
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        # A quote character at a position ALREADY KNOWN to be the closing quote of
        # an open active (command-substitution-bearing) span TERMINATES that span —
        # it is not the opener of a new one (#113). See the ml_segment() copy of
        # this guard for the full rationale; both lexers share the defect and
        # therefore share the fix so they cannot drift.
        while (acn > 0 && acs[acn] < i) acn--   # spans a jump already skipped past
        if (acn > 0 && i == acs[acn]) {
            out = out c
            acn--
            i++
            continue
        }
        # A BACKSLASH-ESCAPED quote (`\"`) is literal text, not a span opener
        # (#113). Opening a span there let an escaped quote AFTER an active span
        # pair with a much later quote and copy every real separator between
        # them as bogus inert text. Refusing to open is strictly NARROWING (the
        # separators stay ACTIVE), so it can only add segment boundaries.
        if ((c == DQ || c == SQ) && !bs_escaped(s, i)) {
            qc = c
            ci = 0
            # …and a backslash-escaped `"` is not a CLOSE either (#548). Inside
            # `"…"` bash reads `\"` as a literal quote and the span runs on, so
            # ending it there made the following REAL close open a bogus span:
            # for `echo "a \" b" && cp … <main>/f; echo "z"` the inert branch
            # below then copied ` && cp … <main>/f; echo ` verbatim as quoted
            # data, losing the separator and the write target. Skipping it
            # makes `ci` the span close bash itself pairs — exact, not merely
            # conservative, for every parseable input — so the inert branch
            # copies exactly what the shell treats as data and no separator
            # outside the span is ever swallowed. SINGLE quotes are
            # deliberately NOT skipped: between `S…S` a backslash is an
            # ordinary literal byte, so the next `S` really is the close and
            # skipping it would extend an inert span over live code. The
            # ACTIVE-span branch is unaffected either way — trusted_close()
            # below already resolved escaped candidates itself.
            for (j = i + 1; j <= n; j++) {
                if (substr(s, j, 1) == qc && !(qc == DQ && bs_escaped(s, j))) { ci = j; break }
            }
            if (ci == 0) {
                # Unterminated quote: fall back to separator-active processing so
                # a stray quote never suppresses a real split (never widen a deny).
                out = out c
                i++
                continue
            }
            inner = substr(s, i + 1, ci - i - 1)
            # LIVE substitution only: a backslash-ESCAPED backtick / `\$(` is
            # literal text, so the span really is inert and its separators
            # really are literal (has_live_subst(), parity not presence).
            if (!has_live_subst(inner)) {
                # Inert quoted span: copy verbatim, separators inside are literal.
                # KNOWN LIMIT (#130): this is the one branch that can LOSE segment
                # boundaries, because it consumes whatever the forward scan paired
                # with. When the opener is a STRAY unmatched quote, that partner
                # can be an unrelated later quote and the real separators between
                # them are swallowed. Two routes reach it: an opener sitting AFTER
                # an active span has closed, and an opener INSIDE an active span
                # whose partner lies past that span-s close. Both are confined to
                # odd-quote-count input the shell will not parse — see the KNOWN
                # LIMIT block in ml_segment().
                out = out substr(s, i, ci - i + 1)
                i = ci + 1
                continue
            }
            # Span carries command substitution: keep separators ACTIVE (copy the
            # opening quote and keep walking char-by-char so a `|` at the span-s
            # OWN level still splits; one nested inside the substitution-s
            # parens/backticks does not — see the #436 note in this block-s
            # header, and subst_inner() for where those inner commands resurface).
            # REMEMBER where the span really ENDS (#113) so the char-walk does not
            # re-read that quote as a NEW opener — which used to swallow
            # everything after the span. `trusted_close()` resolves the real close
            # (skipping backslash-escaped quotes, refusing an ambiguous one, and —
            # for a double-quoted span — refusing a same-kind quote nested at a
            # DEEPER substitution depth, #453); see the ml_segment() copy of this
            # guard for the full rationale — both lexers share the defect and
            # therefore share the fix so they cannot drift.
            out = out c
            tc = trusted_close(s, n, ci, qc, DQ, sdep, sdep[i])
            if (tc > 0) acs[++acn] = tc
            i++
            continue
        }
        # A separator INSIDE a `$( … )`/backtick substitution is inert to the
        # OUTER shell (#436): copy it literally so the enclosing token — e.g. a
        # quoted redirect target `"/tmp/out-$(echo $F|tr -d x).json"` — is not
        # torn in half. The command it really does start is re-emitted as its own
        # segment by subst_inner() below, so nothing stops being segmented.
        if ((c == ";" || c == "&" || c == "|") && sdep[i] > 0) {
            out = out c
            i++
            continue
        }
        # QS_MARK (repo#601): a caller that sets it gets the separator KIND
        # back, as a marker byte (ENQ) plus one kind letter at the start of
        # the segment the separator introduces: S ; A && O || P | B & R a
        # redirection ampersand (2>&1, &>f) that is not a separator at all.
        # Every other caller leaves QS_MARK unset and sees the old output.
        if (c == ";") { out = out "\n" (QS_MARK ? sprintf("%c", 5) "S" : ""); i++; continue }
        if (c == "&") {
            if (i < n && substr(s, i + 1, 1) == "&") { out = out "\n" (QS_MARK ? sprintf("%c", 5) "A" : ""); i += 2; continue }
            out = out "\n" (QS_MARK ? sprintf("%c", 5) ((substr(s, i - 1, 1) ~ /[<>]/ || substr(s, i + 1, 1) == ">") ? "R" : "B") : ""); i++; continue
        }
        if (c == "|") {
            if (i < n && substr(s, i + 1, 1) == "|") { out = out "\n" (QS_MARK ? sprintf("%c", 5) "O" : ""); i += 2; continue }
            out = out "\n" (QS_MARK ? sprintf("%c", 5) "P" : ""); i++; continue
        }
        out = out c
        i++
    }
    return out subst_inner(s, sdep)
}
'

# =============================================================================
# MULTI-LINE QUOTE-AWARE SEGMENTATION (#71)
#
# The three segment parsers (parse_force_ops, lifecycle_or_cloud_reason,
# extract_rm_targets) originally segmented per awk INPUT RECORD with
# `$0 = qsplit($0); split($0, segs, "\n")`. Because awk's default RS="\n" splits
# a multi-line command into separate records BEFORE the pattern block runs,
# qsplit()'s quote-tracking state — scoped to a single call — reset at every
# embedded newline. An interior line of an otherwise-inert multi-line quoted
# DATA literal (echo/printf/--body) was therefore lexed as its own top-level
# segment with no memory that it is still inside an open quote from a prior line,
# so a quoted `git push --force origin main` false-`ask`ed (parse_force_ops) and
# a quoted `halt` false-`deny`ed (lifecycle_or_cloud_reason). PR #69 fixed the
# identical defect in extract_rm_targets() with a whole-buffer slurp-then-segment
# lexer; this shared helper generalizes that lexer so all three parsers segment
# ONCE over the full command and cannot drift (the same rationale the _QSPLIT_AWK
# header gives for sharing qsplit()).
#
# `ml_segment(buf, segs)` fills the caller's `segs[]` out-array (AWK passes arrays
# by reference) with one entry per top-level segment and returns the count. It
# deliberately does NOT reuse qsplit()+split("\n"): qsplit() emits a `\n` for each
# REAL separator while ALSO leaving a literal newline that lived inside an inert
# quoted span untouched, so the two become indistinguishable to a downstream
# `split(s, segs, "\n")`. Segmentation is therefore done inline here, so an inert
# quoted span's embedded newlines never become segment boundaries.
#
# Segmentation contract (identical to qsplit()'s single-line behaviour, now
# correctly carried ACROSS embedded newlines):
#   - Separators `;` `&` (`&&`) `|` (`||`) OUTSIDE any quote split segments.
#   - A raw newline OUTSIDE any quote ALSO splits — so a GENUINE multi-line
#     command still yields a real later-line segment and still denies (safety
#     floor preserved; matches the old per-record behaviour where each input line
#     was its own record).
#   - An INERT quoted span is copied VERBATIM, so its embedded
#     newlines/separators stay literal and never manufacture a phantom segment
#     out of quoted documentation prose (the false positive). A span is inert
#     when it carries no `$(` and no backtick — and a TOP-LEVEL single-quoted
#     span is inert regardless of its content (#443), because bash never expands
#     or substitutes anything between `'...'`. "Top-level" is load-bearing
#     (#450): an apostrophe met while an ACTIVE span is still open is literal
#     text to the shell, not an opener, so its span may hold live code and is
#     NOT treated as inert.
#   - A DOUBLE-quoted span carrying command substitution (`$(` or a backtick)
#     keeps its separators ACTIVE (walked char-by-char, exactly like qsplit()),
#     so a smuggled payload is never hidden behind an opening quote. Its
#     already-computed CLOSING quote index is remembered (#113) so the walk that
#     reaches it recognises the span TERMINATOR instead of re-opening a phantom
#     span there — the mis-read that used to swallow the whole rest of the
#     command (and with it any `; <destructive cmd>` following the span).
#   - An unterminated quote copies the remainder verbatim (best-effort; never
#     widens a deny into an allow).
#
# HEREDOC AWARENESS (#84)
#
# The lexer above tracked quote state only — it had no concept of heredoc syntax
# (`<<WORD`, `<<-WORD`, `<<'WORD'`, `<<"WORD"`). A composite like
#   gh issue create --body "$(cat <<'EOF'
#   shutdown Iq is specified at ...
#   EOF
#   )"
# therefore fell through to the default "a raw newline splits" rule (the outer
# double-quoted span carries `$(`, so by the #3679 safety floor its separators
# stay ACTIVE), and every heredoc BODY line became its own phantom top-level
# segment. lifecycle_or_cloud_reason() then read `toks[1]` of that phantom
# segment and hard-denied on the word `shutdown`; the same phantom segments
# reach parse_force_ops() and extract_rm_targets(). Heredoc body lines are DATA,
# never command boundaries, so:
#   - A heredoc opener seen OUTSIDE any inert quoted span records its terminator
#     word (quotes/backslash stripped) and the `<<-` leading-TAB-strip flag. The
#     REST of the opener line is segmented normally, so `cat <<EOF | grep x`
#     still splits at the pipe.
#   - The newline that ends the opener line closes that segment (it IS a real
#     command boundary), and the body lines through the terminator are then
#     SKIPPED entirely — they are data, so they contribute no segment and no
#     tokens. Normal segmentation resumes on the line after the terminator, so a
#     real command following the terminator still gets its own segment and still
#     denies.
#   - Multiple heredocs on one line (`cmd <<A <<B`) consume body A in full before
#     body B, matching real shell semantics.
#   - An UNTERMINATED heredoc skips the remainder of the buffer — which is
#     exactly what a real shell does with it (the rest of the input IS the body
#     and never executes), and mirrors the unterminated-quote fallback above.
#     Anything BEFORE the opener is still segmented normally.
#   - SAFETY CARVE-OUT: a body attached to a BARE (unquoted) delimiter is still
#     parameter/command-expanded by the shell, so if such a body carries `$(` or
#     a backtick the whole pending-heredoc region reverts to the legacy
#     separator-active treatment — the same #3679 floor the quoted-span branch
#     applies. A quoted/escaped delimiter (`<<'EOF'`, `<<"EOF"`, `<<\EOF`)
#     suppresses expansion, so its body is unconditionally inert.
#   - `<<<` is a here-STRING, not a heredoc, and is deliberately not matched.
#   - A `<<` inside a shell COMMENT (`echo hi # <<EOF`) is not an operator at
#     all, so the opener probe is SUPPRESSED from a word-initial unquoted `#`
#     through the end of that physical line. Without this, the phantom opener's
#     terminator never appears and the unterminated-heredoc rule would skip the
#     rest of the buffer — hiding a real command on the NEXT line from
#     extract_rm_targets(), which parses raw $COMMAND rather than
#     $COMMAND_NO_COMMENT. Only the probe is suppressed (the characters still
#     flow through normal separator handling), because skipping to the newline
#     would in turn hide a trailing `; <cmd>` after a `#` that sits inside a
#     command-substitution-bearing quoted span.
#   - A newline preceded by an ODD number of backslashes is a LINE CONTINUATION,
#     not the end of the logical line, so the pending bodies do NOT start there
#     (`cat <<EOF \` + newline + `&& <cmd>` really does run `<cmd>`). Such a
#     newline falls through to the legacy separator handling, so the continued
#     line is segmented as the real commands it is; the bodies then start at the
#     first NON-continued newline, matching the shell.
#   - A `<<` whose leading `<` is itself preceded by an ODD number of backslashes
#     (`\<<WORD`) is NOT an operator — `\<` is a literal `<` inside a word — so it
#     is not probed at all (#108). Without this the phantom opener's terminator
#     never appeared and the unterminated-heredoc rule skipped the rest of the
#     buffer, hiding every later real command. The ESCAPED-DELIMITER form
#     `<<\WORD` is a different (and legitimate) thing and is unaffected.
# Safety floor unchanged: the raw ALWAYS_BLOCK_PATTERNS catastrophic scan reads
# the command string directly (never through ml_segment), so a `$(...)`-smuggled
# payload inside a heredoc body still denies, and a GENUINE multi-line command
# whose later real line is dangerous still yields a real segment and still denies.
# =============================================================================
_ML_QSPLIT_AWK='
# Probe for a heredoc redirection operator at position i (the caller guarantees
# substr(s, i, 2) is "<<"). On success fills out["delim"] (terminator word, with
# any surrounding quotes/backslash stripped), out["strip"] (1 for the `<<-` form,
# which strips leading TABs from the terminator line) and out["quoted"] (1 when
# the delimiter was quoted or backslash-escaped, i.e. the body is NOT expanded)
# and returns the index of the first character AFTER the operator. Returns 0 when
# this is NOT a heredoc opener: a `<<<` here-string, a `<<` with no delimiter
# word, or a delimiter whose opening quote is never closed.
function hd_opener(s, n, i, out,   j, c, q, w, SQ, DQ) {
    SQ = sprintf("%c", 39)   # single quote
    DQ = sprintf("%c", 34)   # double quote
    j = i + 2
    if (substr(s, j, 1) == "<") return 0        # `<<<` here-string, not a heredoc
    out["strip"] = 0
    out["quoted"] = 0
    if (substr(s, j, 1) == "-") { out["strip"] = 1; j++ }
    while (j <= n && (substr(s, j, 1) == " " || substr(s, j, 1) == "\t")) j++
    w = ""
    c = substr(s, j, 1)
    if (c == SQ || c == DQ) {
        q = c
        out["quoted"] = 1                       # no expansion inside the body
        j++
        while (j <= n && substr(s, j, 1) != q) { w = w substr(s, j, 1); j++ }
        if (j > n) return 0                     # unterminated delimiter quote
        j++                                     # consume the closing quote
    } else {
        if (c == "\\") { out["quoted"] = 1; j++ }   # `<<\EOF` (escaped, unexpanded)
        while (j <= n) {
            c = substr(s, j, 1)
            if (c ~ /[A-Za-z0-9_.:@%+=\/-]/) { w = w c; j++ } else break
        }
    }
    if (w == "") return 0
    # Reject shapes that are far more likely to be an arithmetic left-shift than
    # a heredoc, so `$((1 << 3))` / `(( x << 2 ))` are not misread as openers that
    # would swallow the rest of the buffer:
    #   - an all-digit BARE delimiter (`<< 3`) — real delimiters are words;
    #   - a delimiter not followed by a redirection/separator/whitespace boundary
    #     (`<< 3))` — a genuine opener is always followed by more redirections,
    #     a separator, or end of line.
    if (!out["quoted"] && w ~ /^[0-9]+$/) return 0
    c = substr(s, j, 1)
    if (j <= n && c != " " && c != "\t" && c != "\n" && c != ";" && c != "&" &&
        c != "|" && c != "<" && c != ">") return 0
    out["delim"] = w
    return j
}
# bs_escaped() (the backslash-parity helper every escape-sensitive branch below
# calls) lives in the shared _ESCAPE_AWK source string, which is prepended to
# BOTH this lexer and qsplit() so the two cannot drift (#113).
function ml_segment(buf, segs,   SQ, DQ, s, n, seg, segc, i, c, qc, ci, tc, j, inner,
                    hdc, hddelim, hdstrip, hdquoted, hdo, hdnext, h, k, unsafe,
                    arO, arC, eol, nexti, line, t, incmt, pc, acs, acn, sdep, segbeg) {
    SQ = sprintf("%c", 39)   # single quote
    DQ = sprintf("%c", 34)   # double quote
    split("", segs)          # clear the caller-supplied out-array
    # Side channel (repo#588 review): ml_segbeg[k] is the buffer index where
    # segment k starts and ml_segsep[k] the index of the separator byte that
    # ENDED it (n + 1 for the final segment). Globals, so existing callers are
    # unaffected; samecmd_binding_ctx_ok() reads them to prove WHERE a binding
    # sits (unconditional, top level) relative to the separators around it.
    split("", ml_segbeg)
    split("", ml_segsep)
    segbeg = 1
    split("", acs)           # stack of pending active-span CLOSING quote indexes
    acn = 0
    s = buf
    n = length(s)
    subst_depth(s, sdep)     # byte -> enclosing `$( … )`/backtick depth (#436, #453)
    seg = ""
    segc = 0
    hdc = 0                  # heredoc openers pending a body on the next line
    incmt = 0                # a shell COMMENT is open on the current line
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        # ACTIVE-SPAN CLOSE (#113). When the span below carries a command
        # substitution its separators stay ACTIVE, so the walk continues
        # character-by-character INTO the span — and eventually reaches the REAL
        # closing quote of that same span. Without this guard the top-of-loop quote
        # detector below re-read that closing quote as the OPENER of a brand-new
        # span and re-scanned forward for a partner: with no later quote the
        # unterminated-quote fallback swallowed the ENTIRE remainder of the buffer
        # into the current segment, and with a later quote everything up to it
        # (including real `;` `|` `&` separators) was copied as a bogus inert span.
        # Either way a genuinely destructive command AFTER a quoted `$(...)` span
        # never became its own segment, so parse_force_ops(),
        # lifecycle_or_cloud_reason() and extract_rm_targets() never saw its
        # command word and fell through to ALLOW.
        #
        # This was a fixable FALSE NEGATIVE, not an intentional safety floor: the
        # #3679/#3755 "keep separators active inside a substitution-bearing span"
        # rule exists so smuggled content INSIDE the span is not masked — it never
        # implied disabling matching for the text AFTER the span. Remembering the
        # already-computed close index (a stack, so NESTED active spans each pop
        # their own close) resumes correct segmentation right after the real close
        # while leaving in-span matching exactly as it was.
        #
        # The close index is only authoritative when a BACKSLASH did not make the
        # pairing ambiguous — see the escaped-quote rules in the shared header
        # above and the three escape-aware branches below. With those in place
        # this produces MORE segment boundaries than before for every command the
        # shell can PARSE, so it cannot turn an existing deny into an allow there.
        #
        # KNOWN LIMIT (#130) — it is NOT an unconditional guarantee. Correcting
        # the pairing also frees a STRAY unmatched quote sitting after the span to
        # pair with a LATER quote instead of with the close of this span. (No
        # apostrophes in this block: it lives inside a single-quoted awk source
        # string, where one would terminate the string.) When the text
        # between them carries no substitution, the inert branch below copies it
        # verbatim and swallows the real separators inside it — separators the
        # pre-#113 mis-pairing happened to leave ACTIVE, so a few shapes go
        # deny -> allow:
        #     echo "$(id)" " ; <destructive> ; echo "trailing"
        # The same branch is reached by a second route, where the stray opener
        # sits INSIDE an active span and its partner lies past that span-s close
        # (the #130 reproduction; written here with S for the single quote this
        # single-quoted awk string cannot contain):
        #     echo "$( S )" ; <destructive> S
        # Every member of the family needs an ODD quote count, so the shell
        # rejects the command outright and nothing executes; the swallowed text is
        # text the shell also treats as quoted. Both routes are pinned by the
        # KNOWN LIMIT cases in the #113 test block, each paired with an
        # assert_shell_rejects so the unparseability half is mechanical rather
        # than a claim in this comment. The underlying weakness is the inert
        # branch itself, tracked in #130 — not this close-index bookkeeping.
        while (acn > 0 && acs[acn] < i) acn--   # spans a jump already skipped past
        if (acn > 0 && i == acs[acn]) {
            seg = seg c
            acn--
            i++
            continue
        }
        # A BACKSLASH-ESCAPED quote (`\"`) is literal text and never OPENS a span
        # (#113): opening one there let an escaped quote sitting AFTER an active
        # span pair with a much later quote and copy every real separator between
        # them as bogus inert text (`echo "$(id)" \" ; <destructive>`). Refusing
        # to open keeps the separators ACTIVE, which is the narrowing direction.
        #
        # The forward close scan below used to ACCEPT an escaped quote as the
        # INERT-span boundary, on the theory that ending a literal span EARLIER
        # is also the active direction. It is not (#548): the span does not
        # just end early, the following REAL close is then read as a NEW opener
        # and the inert branch copies everything up to the next quote — a real
        # `&& <lifecycle command>` / `&& cp … <main>/f` included — verbatim as
        # quoted data. `echo "\"" && halt; echo "z"` allowed on exactly that
        # route. For DOUBLE quotes the scan therefore skips escaped candidates,
        # which makes `ci` the close bash itself pairs (exact for any parseable
        # input); SINGLE quotes keep the next-quote rule, which is likewise
        # exact for them since a backslash inside `S…S` is an ordinary byte.
        # The ACTIVE-span bookkeeping is unchanged — it resolves a real close
        # through trusted_close() (see below), which already skipped escapes.
        if ((c == DQ || c == SQ) && !bs_escaped(s, i)) {
            qc = c
            ci = 0
            for (j = i + 1; j <= n; j++) {
                if (substr(s, j, 1) == qc && !(qc == DQ && bs_escaped(s, j))) { ci = j; break }
            }
            if (ci == 0) {
                # Unterminated quote: advance ONE character with separators still
                # ACTIVE, exactly like qsplit() (#113). Copying the whole rest of
                # the buffer verbatim — the pre-#113 behaviour here — swallowed
                # every remaining separator, so a stray or escaped quote anywhere
                # ahead of a real `; <destructive>` hid it from all three parsers.
                # One-character advance can only ADD segment boundaries.
                seg = seg c
                i++
                continue
            }
            inner = substr(s, i + 1, ci - i - 1)
            # A TOP-LEVEL SINGLE-quoted span is inert, `$(`/backtick or not
            # (#443, scoped to the top level of the walk by #450).
            # (No apostrophes in this block: it lives inside a single-quoted awk
            # source string, where one would terminate the string. S below stands
            # for the single quote character.)
            #
            # Bash never expands or substitutes ANYTHING between S...S, so a
            # $(mktemp -d) written there is literal text, not live code — the
            # substitution probe below must therefore not apply to it. Marking an
            # SQ span ACTIVE because its text merely CONTAINS the characters `$(`
            # kept separators live inside quoted DATA, and a LITERAL `;` in that
            # data then leaked out as a phantom top-level command boundary: the
            # tail after it was re-segmented and classified as a real command, so
            #   ssh <host> S TMPDIR=$(mktemp -d); rm -rf "$TMPDIR" S
            # false-denied as a local rm with an unresolvable target, and
            #   echo S X=$(true); rm -rf <some-path-outside-the-repo> S
            # false-denied as a local out-of-repo rm. The SAME command WITHOUT a
            # `$( )` in the quoted string was already allowed, so this was a
            # quote-classification defect, not a missing remote-exec concept:
            # nothing about `ssh` is load-bearing here and the identical false
            # positive reproduced with a plain `echo`.
            #
            # The #3679/#3755 "keep separators ACTIVE inside a substitution-bearing
            # span" floor is UNCHANGED for DOUBLE-quoted and unquoted spans, where
            # `$( )` really does execute and a smuggled `; <destructive>` really
            # does run — that is the #113 protection and it must not be weakened.
            # There is no single-quoted equivalent to preserve: the shell cannot
            # execute anything inside S...S, so nothing is being masked here.
            # The catastrophic ALWAYS_BLOCK scan is unaffected either way — it
            # reads the raw command string and never goes through this lexer, so a
            # root-obliterating payload inside a single-quoted span still denies.
            #
            # The SQ branch is scoped to the TOP LEVEL of the walk (`acn == 0`)
            # — #450, a regression the unscoped form shipped with. `acn == 0` is
            # NECESSARY for an unescaped S to be a real OPENER whose span the
            # shell genuinely treats as inert (the only shape #443 was ever
            # about — every #443 payload is a top-level single-quoted argument)
            # — but it is not SUFFICIENT on its own (#453): `acn` is decremented
            # from the recorded active-span CLOSE index below, and that index
            # can itself be a PHANTOM close when a quote of the same kind sits
            # nested one `$( )`/backtick level deeper than the span-s real
            # opener (see the #453 block right below trusted_close()-s call at
            # the bottom of this branch). `acn == 0` still correctly rejects
            # every non-top-level S; it is the trusted_close() depth check that
            # keeps `acn` itself accurate. Do not read `acn == 0` in isolation
            # as "guaranteed top level" — it is "top level, GIVEN an accurate
            # acn" — the #453 fix is what makes that given hold.
            #
            # Reached while an ACTIVE span is still open, that S is a PHANTOM
            # quote: inside a double-quoted span bash reads it as ordinary
            # literal text, so the forward scan above pairs it with some
            # unrelated LATER S, and the stretch between them can hold genuinely
            # live, executing code. Copying that stretch verbatim made the lexer
            # skip a real $( ) that bash actually runs:
            #
            #   echo "donSt $(true; <recursive-force rm of an out-of-repo path>) wonSt"
            #
            # That is parseable (the apostrophes are balanced), the substitution
            # really executes, and the guard allowed it — a deny before #443
            # became an allow after it. The earlier claim that every such shape
            # needs an ODD quote count and so cannot parse was simply wrong; the
            # #450 test block pins the parseability mechanically next to the deny.
            # So: an S reached with `acn > 0` stays on the legacy active-walk path
            # below, where separators remain live and the smuggled payload is
            # still segmented and classified. Do NOT drop the `acn == 0` term as
            # a simplification — it is the entire scoping of the #443 branch.
            #
            # For a DOUBLE-quoted/unquoted span (the `acn > 0` fallthrough above
            # aside, this is the `qc != SQ` — or `acn > 0` — case), LIVE
            # substitution only (has_live_subst()): an escaped `\`code\`` inside
            # the span is literal text and keeps it inert, so the byte-presence
            # test this replaced (`index(inner, "$(") == 0 && index(inner, "`")
            # == 0`) is too strict — it vetoed spans whose only `$(`/backtick was
            # backslash-escaped, ordinary prose with zero execution risk.
            if ((qc == SQ && acn == 0) || !has_live_subst(inner)) {
                seg = seg substr(s, i, ci - i + 1)   # inert span: verbatim (newlines stay literal)
                # KNOWN LIMIT (#130): the only branch that can LOSE a boundary —
                # it consumes whatever the forward scan paired with, which for a
                # STRAY unmatched opener may be an unrelated later quote. That
                # opener may sit after a closed active span OR inside one with its
                # partner past the close. See the KNOWN LIMIT block at the
                # active-span-close guard above.
                # NOTE: `incmt` is deliberately NOT cleared here even when the
                # span carries a newline. A real comment ends at that newline, so
                # leaving the flag set can only suppress the opener probe for
                # LONGER than the shell would — and over-suppression is strictly
                # narrowing (it just restores the legacy pre-#84 treatment).
                # Clearing it here would re-enable the probe on text the shell may
                # still regard as commented, which is the widening direction.
                i = ci + 1
                continue
            }
            seg = seg c        # command substitution present: keep separators ACTIVE
            # ...but remember where the span really ENDS (#113). The naive "next
            # quote of the same kind" index is NOT usable here: a backslash-escaped
            # `\"` inside the span is literal text, so ending the span there would
            # leave the REAL close to open a bogus span (`echo "$(a \" b)" ;
            # <destructive>` — a shape the pre-#113 walk denied). It is ALSO not
            # usable when the span is double-quoted and that naive index actually
            # belongs to a NESTED `$( )`/backtick substitution one level deeper
            # than this span-s own opener (#453): a double-quoted string opened
            # INSIDE a command substitution is parsed by that inner subshell, not
            # by the shell that opened THIS span, so an unescaped `"` there is not
            # this span-s close — it just happens to be the same byte value. The
            # pre-#453 walk accepted it anyway (a phantom close), which handed
            # everything up to the REAL close — genuinely live code this span-s
            # own `$( )` is still executing — to the inert verbatim-copy branch
            # above instead of to this active, separator-tracking one (S stands
            # for the single quote this single-quoted awk source string cannot
            # contain):
            #
            #   echo "x $(echo "ySz $(true; <destructive>) wSv") q"
            #
            # trusted_close() now skips BOTH escaped candidates and (for a
            # double-quoted span) same-kind quotes at the wrong substitution
            # depth, continuing to scan forward for the real close; it returns 0
            # when no such candidate exists, in which case NO close is recorded
            # and this span is walked exactly the legacy way. Single-quoted spans
            # pass their own depth check trivially (see trusted_close()-s doc
            # comment in _ESCAPE_AWK) since a single quote-s close is always the
            # very next single-quote byte, nesting depth notwithstanding.
            tc = trusted_close(s, n, ci, qc, DQ, sdep, sdep[i])
            if (tc > 0) acs[++acn] = tc
            i++
            continue
        }
        # A `#` that STARTS a word opens a shell COMMENT that runs to the end of
        # the physical line, so any `<<WORD` after it is TEXT, not an operator.
        # A word starts at the beginning of the buffer or right after an unquoted
        # blank or shell METACHARACTER (` ` \t \n ; & | ( ) < >) — the full set
        # bash uses to delimit words, so `;#`, `&&#`, `|#` and `(cmd)#` are all
        # comment starts. A `#` in any OTHER position is part of a word
        # (`http://x#y`, `${#arr}`, `ab#cd`) and is correctly NOT a comment.
        #
        # Only the heredoc-opener PROBE is suppressed while the flag is set — the
        # characters still flow through the normal separator handling below,
        # because skipping to the newline here would hide a trailing `; <cmd>` in
        # a shape like `echo "$(id) # x" ; <cmd>` (a `#` inside a
        # command-substitution-bearing quoted span is walked by this loop, and
        # bash really does run that command). Probe suppression is strictly
        # NARROWING: it can only restore the legacy pre-#84 treatment, never
        # widen a deny into an allow — which is why the metacharacter set is
        # chosen as a SUPERSET of the shapes bash actually treats as comments.
        if (c == "#" && !incmt) {
            pc = (i == 1) ? "" : substr(s, i - 1, 1)
            if (i == 1 || pc == " " || pc == "\t" || pc == "\n" || pc == ";" ||
                pc == "&" || pc == "|" || pc == "(" || pc == ")" ||
                pc == "<" || pc == ">") incmt = 1
        }
        # A BACKSLASH-ESCAPED leading `<` (`\<<WORD`) is a literal `<` inside a
        # word, not a redirection operator, so the shell opens no heredoc there
        # (#108). Probing it anyway manufactured a phantom opener whose
        # terminator never appears, and the unterminated-heredoc rule then
        # skipped the REST OF THE BUFFER — hiding every later real command from
        # all three parsers. Note this checks only the FIRST `<`: when the SECOND
        # one is escaped (`<\<`) the `substr()` guard below already fails, and an
        # escaped DELIMITER (`<<\EOF`) is a legitimate opener that hd_opener()
        # handles by marking the body unexpanded.
        if (c == "<" && i < n && substr(s, i + 1, 1) == "<" && !incmt &&
            !bs_escaped(s, i)) {
            if (substr(s, i + 2, 1) == "<") {
                # `<<<` is a here-STRING: consume the whole operator so its third
                # `<` cannot be re-probed as the start of a `<< WORD` heredoc.
                seg = seg substr(s, i, 3)
                i += 3
                continue
            }
            # Inside an unclosed arithmetic expansion (`$((` / `((`) a `<<` is a
            # left-shift operator, never a heredoc — count the unbalanced opens
            # in the segment so far and skip the probe when we are inside one.
            t = seg; arO = gsub(/\(\(/, "", t)
            t = seg; arC = gsub(/\)\)/, "", t)
            hdnext = (arO > arC) ? 0 : hd_opener(s, n, i, hdo)
            if (hdnext > 0) {
                # Record the pending heredoc and copy the operator verbatim (this
                # also consumes a quoted delimiter, so its quotes never open a
                # phantom quoted span). The REST of this line segments normally.
                seg = seg substr(s, i, hdnext - i)
                hdc++
                hddelim[hdc] = hdo["delim"]
                hdstrip[hdc] = hdo["strip"]
                hdquoted[hdc] = hdo["quoted"]
                i = hdnext
                continue
            }
        }
        if (c == "\n" && hdc > 0 && !bs_escaped(s, i)) {
            # End of the opener line: the pending heredoc BODIES follow. Look
            # ahead across every pending body, in opener order, to find where the
            # last one ends — and whether any EXPANSION-CAPABLE body (bare
            # delimiter) carries a command substitution, which a real shell WOULD
            # execute. That case keeps the legacy separator-active treatment
            # (same #3679 floor the quoted-span branch above applies), so a
            # smuggled payload is never hidden behind a heredoc opener.
            k = i + 1
            unsafe = 0
            for (h = 1; h <= hdc; h++) {
                while (k <= n) {
                    eol = index(substr(s, k), "\n")
                    nexti = (eol == 0) ? n + 1 : k + eol
                    line = substr(s, k, nexti - k)
                    t = line
                    sub(/\n$/, "", t)
                    if (hdstrip[h]) sub(/^\t+/, "", t)
                    k = nexti
                    if (t == hddelim[h]) break        # terminator line, body done
                    if (!hdquoted[h] && (index(line, "$(") > 0 || index(line, "`") > 0)) unsafe = 1
                }
            }
            hdc = 0
            if (!unsafe) {
                # The opener line is a complete simple command; the body (through
                # its terminator, or through end-of-buffer when unterminated) is
                # inert DATA and contributes no segment at all.
                segs[++segc] = seg
                ml_segbeg[segc] = segbeg; ml_segsep[segc] = i; segbeg = k
                seg = ""
                incmt = 0
                i = k
                continue
            }
            # else: fall through to the ordinary separator handling below.
        }
        if (c == ";" || c == "&" || c == "|" || c == "\n") {
            if (c == "\n") incmt = 0   # a comment ends at the physical newline
            segs[++segc] = seg
            ml_segbeg[segc] = segbeg; ml_segsep[segc] = i; segbeg = i + 1
            seg = ""; i++; continue
        }
        seg = seg c
        i++
    }
    segs[++segc] = seg
    ml_segbeg[segc] = segbeg; ml_segsep[segc] = n + 1
    return segc
}
'

# Parse force-op segments out of a command, emitting one TAB-separated
# "<cpath>\t<target>" line per genuine git force-push / hard-reset. Portable awk
# only (mirrors extract_rm_targets / lifecycle_or_cloud_reason segment parsing):
#   - split on ; | & && || and newline, strip a leading sudo wrapper.
#   - repo#633: a chain of transparent command wrappers before the git word
#     (command, env, exec, nice, nohup, time, builtin, noglob, and sudo behind
#     one of them) is walked to the word it runs (fo_unwrap). env NAME=value,
#     -i/-u feed the GIT_DIR/GIT_WORK_TREE selectors, env -C moves the process
#     cwd like git -C, and a wrapper option the scan cannot decode fails
#     closed (FO_UNDEC cpath, or an unknown cwd / git dir).
#   - only a segment whose command word is `git` is considered. Since repo#612
#     the command word, the global options, the subcommand, the force flags
#     and the push refspecs are compared AFTER static quote removal
#     (cmdword_static), so `"git" re''set "--hard"` is recognized; a word that
#     needs runtime expansion in a deciding position fails closed (FO_UNDEC
#     cpath, or an @UNRESOLVED@ push target).
#   - the segment is split into real shell words (shell_words, repo#608), so a
#     quoted/escaped operand containing whitespace stays one word.
#   - pre-subcommand global options are walked with git's real arity: -C, -c,
#     --git-dir, --work-tree, --namespace, --super-prefix, --config-env and
#     --attr-source consume a separate value (the long ones also accept
#     --opt=value). Path operands are statically decoded (static_path_word,
#     repo#608) to a literal path, or the unknown-cwd sentinel when they hold
#     an expansion/glob/unbalanced quote. Repeated -C compose in git order
#     onto the process cwd, which is emitted as <cpath>. --git-dir/--work-tree
#     and a leading GIT_DIR=/GIT_WORK_TREE= assignment are resolved against
#     the final process cwd (flags override the environment) and emitted as
#     fields 4 and 5 (repo#612).
#   - a preceding `cd DIR &&`/`cd DIR;` segment earlier in the SAME compound
#     command threads DIR through as the effective cwd for later force-op
#     segments (#350) — see the cd-tracking block below for the full
#     rationale. An explicit `git -C <path>` on the force-op's OWN segment
#     still wins over a threaded `cd` (matches git's own -C-over-cwd
#     precedence).
#   - push: emitted only when a --force/-f/--force-with-lease flag is present.
#     ONE line is emitted per positional refspec (pos[2], pos[3], …) after the
#     remote — a multi-refspec push like `git push --force origin a b` emits a
#     line for `a` AND `b`, so a protected branch in any refspec position (not
#     just the first) reaches the caller's per-line check (#3674 follow-up).
#     <target> is the destination branch parsed from each refspec —
#       * `<src>:<dst>` form => <dst>
#       * a bare ref        => the ref with a leading `+` stripped
#       * `HEAD`, or no ref => the literal "@HEAD@" (resolve checked-out branch)
#   - reset --hard: always emitted with <target> = "@HEAD@".
# The caller resolves "@HEAD@" to the checked-out branch and applies the mode.
#
# Second positional arg is the hook's own $CWD, used to seed cd-tracking
# (`curcwd`, below) so a force-op segment with no preceding `cd` and no `-C`
# still emits an explicit <cpath> equal to the caller's own cwd — functionally
# identical to the pre-#350 empty-cpath fallback (`_fcwd="$CWD"` at the call
# site), just made explicit so a LATER `cd` in the same command can override it.
parse_force_ops() {
    printf '%s' "$1" | awk -v startcwd="$2" -v home="$HOME" "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_ML_QSPLIT_AWK$_CDEXPAND_AWK$_CDQUOTE_AWK$_CDWORD_AWK"'
    # repo#612 helpers. fo_is_force(w, sc) -- w (already decoded) is a force
    # flag of push (--force, -f, --force-with-lease[=...]) or reset (--hard),
    # selected by sc.
    function fo_is_force(w, sc) {
        if (sc == "push") return (w == "--force" || w == "-f" || w == "--force-with-lease" || w ~ /^--force-with-lease=/)
        return (w == "--hard")
    }
    # fo_any_force(A, from, to) -- some decodable word in A[from..to] is a
    # push or reset force flag.
    function fo_any_force(A, from, to,   j, w) {
        for (j = from; j <= to; j++) {
            w = cmdword_static(A[j])
            if (CMDW_UNKNOWN) continue
            if (fo_is_force(w, "push") || fo_is_force(w, "reset")) return 1
        }
        return 0
    }
    # fo_any_sub(A, from, to) -- some decodable word in A[from..to] is push
    # or reset.
    function fo_any_sub(A, from, to,   j, w) {
        for (j = from; j <= to; j++) {
            w = cmdword_static(A[j])
            if (CMDW_UNKNOWN) continue
            if (w == "push" || w == "reset") return 1
        }
        return 0
    }
    # fo_join(base, raw) -- resolve one raw path operand (-C, --git-dir,
    # --work-tree, GIT_DIR, GIT_WORK_TREE) against base the way git does,
    # decoding it with the repo#608 static_path_word(). An operand that
    # needs runtime expansion is UNKNOWN, and so is a relative operand from
    # an unknown or empty base. An empty operand (git -C "") leaves base
    # unchanged.
    function fo_join(base, raw,   v) {
        v = static_path_word(raw, home)
        if (v == UNK) return UNK
        if (v == "") return base
        if (v ~ /^\//) return v
        if (base == UNK || base == "") return UNK
        return base "/" v
    }
    # ---- repo#633: transparent command wrappers before the git word ----
    # `command git`, `env git`, `exec git`, `nice git`, `nohup git`,
    # `time git`, `builtin git`, `noglob git` and a `sudo` behind another
    # wrapper all run git itself, so fo_unwrap() walks them (and their
    # options) and returns the index of the word they run. It fails closed:
    # a wrapper option this scan cannot decode statically sets FW_UNDEC (the
    # caller then emits the FO_UNDEC sentinel when a push/reset word and a
    # force flag follow), and an operand that needs runtime expansion makes
    # the cwd or the git dir UNKNOWN. Nothing is ever evaluated.
    #
    # fo_lit(s) -- a DECODED word as a raw single-quoted literal for
    # fo_join() and cmdword_static(); a word holding a single quote becomes a
    # lone dollar, which never decodes (unknown).
    function fo_lit(s) {
        if (index(s, SQ1)) return "$"
        return SQ1 s SQ1
    }
    # fo_env_clear() -- the wrapper runs git with an empty environment
    # (env -i, env -, exec -c), so earlier GIT_DIR / GIT_WORK_TREE
    # assignments no longer apply and git discovers the repo from its cwd.
    function fo_env_clear() {
        envgd_raw = ""; envgd_set = 0; envwt_raw = ""; envwt_set = 0
    }
    # fo_env_uncertain() -- the wrapper may or may not pass the environment
    # on (sudo resets it by policy), so an earlier GIT_DIR / GIT_WORK_TREE
    # becomes unknown rather than trusted either way.
    function fo_env_uncertain() {
        if (envgd_set) envgd_raw = "$"
        if (envwt_set) envwt_raw = "$"
    }
    # fo_wassign(raw) -- 1 when raw is a NAME=value word after a wrapper
    # (env and sudo take them as arguments, the time keyword as a prefix
    # assignment). GIT_DIR / GIT_WORK_TREE feed the same env selectors a
    # leading assignment does. An appending += and a value starting with a
    # tilde (bash expands it in an assignment-shaped argument, zsh does not)
    # become a lone dollar, so the selector is unknown.
    function fo_wassign(raw,   d, nm, v, app) {
        if (raw ~ /^[A-Za-z_][A-Za-z0-9_]*\+?=/) {
            nm = raw; sub(/\+?=.*$/, "", nm)
            v = raw; sub(/^[A-Za-z_][A-Za-z0-9_]*\+?=/, "", v)
            app = (raw ~ /^[A-Za-z_][A-Za-z0-9_]*\+=/)
            if (substr(v, 1, 1) == "~") v = "$"
        } else {
            d = cmdword_static(raw)
            if (CMDW_UNKNOWN || d !~ /^[A-Za-z_][A-Za-z0-9_]*\+?=/) return 0
            nm = d; sub(/\+?=.*$/, "", nm)
            v = d; sub(/^[A-Za-z_][A-Za-z0-9_]*\+?=/, "", v)
            app = (d ~ /^[A-Za-z_][A-Za-z0-9_]*\+=/)
            v = fo_lit(v)
        }
        if (app) v = "$"
        if (nm == "GIT_DIR") { envgd_raw = v; envgd_set = 1 }
        else if (nm == "GIT_WORK_TREE") { envwt_raw = v; envwt_set = 1 }
        return 1
    }
    # fo_optfx(wr, c, v) -- the effect of short option c (value v, a raw
    # word) of wrapper wr. Only options that change the cwd or the
    # environment git sees have one: env -i / exec -c clear the
    # environment, env -u NAME unsets NAME, env -C DIR changes directory
    # exactly like git -C.
    function fo_optfx(wr, c, v,   nm) {
        if (wr == "env") {
            if (c == "i") fo_env_clear()
            else if (c == "u") {
                nm = cmdword_static(v)
                if (CMDW_UNKNOWN) fo_env_uncertain()
                else if (nm == "GIT_DIR") { envgd_raw = ""; envgd_set = 0 }
                else if (nm == "GIT_WORK_TREE") { envwt_raw = ""; envwt_set = 0 }
            } else if (c == "C") FW_CWD = fo_join(FW_CWD, v)
        } else if (wr == "exec") {
            if (c == "c") fo_env_clear()
        }
    }
    # fo_short(T, m, w, wr, noarg, witharg) -- decode the short-option
    # cluster T[w] of wrapper wr. A letter in noarg takes no value; a letter
    # in witharg takes the rest of the cluster or the next word. Any other
    # letter sets FW_UNDEC. Returns the index of the next word.
    function fo_short(T, m, w, wr, noarg, witharg,   o, j, c, n) {
        o = cmdword_static(T[w])
        n = length(o)
        for (j = 2; j <= n; j++) {
            c = substr(o, j, 1)
            if (index(noarg, c)) { fo_optfx(wr, c, ""); continue }
            if (index(witharg, c)) {
                if (j < n) { fo_optfx(wr, c, fo_lit(substr(o, j + 1))); return w + 1 }
                if (w + 1 > m) { FW_UNDEC = 1; return w + 1 }
                fo_optfx(wr, c, T[w + 1])
                return w + 2
            }
            FW_UNDEC = 1
            return w + 1
        }
        return w + 1
    }
    # fo_wopts(T, m, w, wr) -- walk the options of wrapper wr starting at
    # T[w]; returns the index of the first word after them. Sets FW_UNDEC on
    # an option it does not know, FW_NOTRUN when the wrapper only reports on
    # the command instead of running it (command -v / -V).
    function fo_wopts(T, m, w, wr,   o, v) {
        while (w <= m && !FW_UNDEC) {
            o = cmdword_static(T[w])
            # A NAME=value word ends the options even when its value needs
            # expansion; fo_wassign() then records it (unknown).
            if (CMDW_UNKNOWN && T[w] ~ /^[A-Za-z_][A-Za-z0-9_]*\+?=/) return w
            if (CMDW_UNKNOWN) { FW_UNDEC = 1; return w }
            if (o == "--") return w + 1
            if (wr == "env" && o == "-") { fo_env_clear(); w++; continue }
            if (o !~ /^-./) return w
            if (wr == "command") {
                if (o !~ /^-[pvV]+$/) { FW_UNDEC = 1; return w }
                if (o ~ /[vV]/) { FW_NOTRUN = 1; return w }
                w++; continue
            }
            if (wr == "env") {
                if (o == "--ignore-environment") { fo_env_clear(); w++; continue }
                if (o == "--null" || o == "--debug" || o == "--list-signal-handling" || \
                    o ~ /^--(block|default|ignore)-signal(=.*)?$/) { w++; continue }
                if (o == "--unset" || o == "--chdir") {
                    if (w + 1 > m) { FW_UNDEC = 1; return w }
                    fo_optfx("env", (o == "--unset") ? "u" : "C", T[w + 1])
                    w += 2; continue
                }
                if (o ~ /^--(unset|chdir)=/) {
                    v = o; sub(/^--[a-z]+=/, "", v)
                    fo_optfx("env", (o ~ /^--unset=/) ? "u" : "C", fo_lit(v))
                    w++; continue
                }
                if (o ~ /^--/) { FW_UNDEC = 1; return w }
                w = fo_short(T, m, w, "env", "i0v", "uC"); continue
            }
            if (wr == "exec") {
                if (o ~ /^--/) { FW_UNDEC = 1; return w }
                w = fo_short(T, m, w, "exec", "cl", "a"); continue
            }
            if (wr == "nice") {
                if (o ~ /^--?[0-9]+$/ || o ~ /^--adjustment=/) { w++; continue }
                if (o == "--adjustment") { w += 2; continue }
                if (o ~ /^--/) { FW_UNDEC = 1; return w }
                w = fo_short(T, m, w, "nice", "", "n"); continue
            }
            if (wr == "time") {
                if (o == "--append" || o == "--verbose" || o == "--portability" || o == "--quiet" || \
                    o ~ /^--(format|output)=/) { w++; continue }
                if (o == "--format" || o == "--output") { w += 2; continue }
                if (o ~ /^--/) { FW_UNDEC = 1; return w }
                w = fo_short(T, m, w, "time", "pavq", "fo"); continue
            }
            # nohup, builtin, noglob and sudo: no option is decoded.
            FW_UNDEC = 1
            return w
        }
        return w
    }
    # fo_undec_hit(T, m) -- fail-closed test for an undecodable wrapper: some
    # decodable word of T[1..m], read whitespace-split (env -S takes the
    # whole command as ONE word), is push or reset, and some piece is a
    # push or reset force flag.
    function fo_undec_hit(T, m,   j, w, np, P, q, hs, hf) {
        hs = 0; hf = 0
        for (j = 1; j <= m; j++) {
            w = cmdword_static(T[j])
            if (CMDW_UNKNOWN) continue
            np = split(w, P, /[ \t\n]+/)
            for (q = 1; q <= np; q++) {
                if (P[q] == "push" || P[q] == "reset") hs = 1
                if (fo_is_force(P[q], "push") || fo_is_force(P[q], "reset")) hf = 1
            }
        }
        return (hs && hf)
    }
    # fo_unwrap(T, m) -- index of the word the leading wrapper chain of
    # T[1..m] runs (1 when there is no wrapper). FW_CWD is the cwd that
    # word runs in (curcwd, moved by env -C).
    function fo_unwrap(T, m,   w, ww, wrapped) {
        FW_UNDEC = 0; FW_NOTRUN = 0; FW_CWD = curcwd
        w = 1; wrapped = 0
        while (w <= m) {
            if (wrapped && fo_wassign(T[w])) { w++; continue }
            ww = cmdword_static(T[w])
            if (CMDW_UNKNOWN) return w
            if (ww != "command" && ww != "env" && ww != "exec" && ww != "nice" && \
                ww != "nohup" && ww != "time" && ww != "builtin" && ww != "noglob" && \
                ww != "sudo") return w
            if (ww == "sudo") fo_env_uncertain()
            wrapped = 1
            w = fo_wopts(T, m, w + 1, ww)
            if (FW_UNDEC || FW_NOTRUN) return w
        }
        return w
    }
    BEGIN {
        SEP = sprintf("%c", 31)  # US (unit separator) — non-whitespace so bash
                                 # read does not trim an empty cpath.
        buf = ""
        curcwd = startcwd
        UNK = unknown_cwd()
        # cpath sentinel for a force op whose git word, global option or
        # subcommand cannot be decoded statically (repo#612). Must stay
        # byte-equal to the shell-side _LOOM_UNDECODABLE_FORCE_OP.
        FO_UNDEC = sprintf("%c", 14) "LOOM_UNDECODABLE_FORCE_OP"
        SQ1 = sprintf("%c", 39)
    }
    # Slurp the whole (possibly multi-line) command, then segment ONCE with the
    # shared quote-aware lexer (#71) so a multi-line quoted DATA literal whose
    # interior line is a force-push phrase is no longer mis-read as a real
    # segment (the pre-#71 per-record `qsplit()` reset quote state at each
    # embedded newline).
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        n = ml_segment(buf, segs)
        flow_ml_kinds(buf, n, SK)
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/^sudo[ \t]+/, "", seg)
            sub(/^[ \t]+/, "", seg)
            # repo#601: control-flow context shared with the other cwd
            # trackers (see flow_begin / flow_cd in _CDWORD_AWK).
            if (flow_begin(SK[i], seg)) curcwd = UNK
            kb = FL_EFF
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            flow_track(seg)
            # cd-TRACKING (#350): thread a `cd DIR &&`/`cd DIR;` prefix earlier
            # in the SAME compound command through to later force-op segments —
            # mirrors extract_write_targets()/resolve_stash_cwd()s identical
            # cd-tracking blocks byte-for-byte in spirit (their own header
            # comments cover expand_cd_arg()s #5315 tilde-expansion fix and
            # strip_cd_quoting()s #5363 quoted-absolute-path classification
            # fix). Without this, the idiomatic `cd /tmp/scratch && git reset
            # --hard origin/main` left `cpath` empty, so the caller fell back
            # to its OWN raw hook $CWD (typically the main checkout) instead of
            # the scratch directory the reset actually runs in — defeating the
            # #320/#330 out-of-tree exemption for exactly the idiom it targets.
            #
            # The command word goes through the shared static recognizer
            # (repo#600, _CDWORD_AWK): c\d / "c"d are the cd builtin too, and
            # a word this scan cannot resolve (c$()d, ${C:-c}d, a cd behind a
            # group or conditional) makes the cwd UNKNOWN, which the caller
            # turns into an ask instead of judging a stale cwd.
            #
            # repo#612: an "unknown" segment is NOT skipped. Its command word
            # may itself be git (`$G reset --hard`), so it falls through to
            # the classification below, which fails closed on it.
            cdk = cd_seg_class(seg)
            if (cdk == "unknown") { flow_cd(kb, ""); curcwd = UNK }
            if (cdk == "cd") {
                if (flow_cd(kb, flow_next_kind(SK, i, n))) { curcwd = UNK; continue }
                cdtok = CDW_ARG
                if (cdtok != "" && cdtok != "-") {
                    cdarg = expand_cd_arg(cdtok, home)
                    cdclass = strip_cd_quoting(cdarg)
                    cdbase = curcwd
                    if (cdclass ~ /^\//) {
                        curcwd = cdarg
                    } else if (curcwd != "" && curcwd != UNK) {
                        curcwd = curcwd "/" cdarg
                    }
                    # repo#625: a cd to a missing directory fails and stays put
                    if (cd_unreached(cdarg, cdbase, flow_next_kind(SK, i, n), home)) curcwd = UNK
                }
                continue
            }
            # repo#612: the command word, the global options, the subcommand
            # and the force flags are all DECODED with the shared static
            # recognizer (cmdword_static, _CDWORD_AWK) before they are
            # compared, so `"git" "reset" "--hard"`, `git re""set --ha\rd`
            # and friends are the same force op git runs. Nothing is ever
            # evaluated. A word this scan cannot decode (any `$`, a backtick,
            # an unquoted glob/brace, an unbalanced quote) in a position that
            # decides WHETHER this is a force op fails closed: when a force
            # flag is present the segment is emitted with the FO_UNDEC cpath
            # sentinel, which the caller always asks on.
            #
            # A leading run of NAME=value words is skipped (the same
            # skip_assign_words() bash-accurate word walk cd_seg_class uses),
            # and GIT_DIR / GIT_WORK_TREE assignments in it are recorded: they
            # select the repository and work tree exactly like --git-dir and
            # --work-tree (repo#202/#204 precedent in resolve_stash_cwd()).
            aoff = skip_assign_words(seg)
            if (aoff > length(seg)) continue
            envgd_raw = ""; envwt_raw = ""; envgd_set = 0; envwt_set = 0
            # An appending GIT_DIR+=x depends on the inherited value, so its
            # raw value is replaced by a lone dollar, which never decodes.
            if (aoff > 1) {
                na = shell_words(substr(seg, 1, aoff - 1), AW)
                for (a = 1; a <= na; a++) {
                    if (AW[a] ~ /^GIT_DIR\+?=/) { envgd_raw = AW[a]; sub(/^GIT_DIR\+?=/, "", envgd_raw); envgd_set = 1; if (AW[a] ~ /^GIT_DIR\+=/) envgd_raw = "$" }
                    else if (AW[a] ~ /^GIT_WORK_TREE\+?=/) { envwt_raw = AW[a]; sub(/^GIT_WORK_TREE\+?=/, "", envwt_raw); envwt_set = 1; if (AW[a] ~ /^GIT_WORK_TREE\+=/) envwt_raw = "$" }
                }
            }
            # repo#608: split into real shell WORDS (shell_words keeps each
            # word raw; nothing is expanded), so `-C "/a b"` stays one word.
            m = shell_words(substr(seg, aoff), T)
            # ml_segment() keeps separators INSIDE a command substitution
            # active (so smuggled commands there are still seen), which cuts
            # `git -C $(cd x && pwd) reset --hard` into `git -C $(cd x` and
            # `pwd) reset --hard`. When this segment ends inside an open
            # substitution or quote, re-read the ORIGINAL text through the
            # following segments until the word closes, so the subcommand is
            # found again (and the dynamic operand then makes the cwd unknown).
            jn = i
            while (SW_OPEN && jn < n) {
                jn++
                rseg = substr(buf, ml_segbeg[i], ml_segsep[jn] - ml_segbeg[i])
                sub(/^[ \t]+/, "", rseg)
                sub(/^sudo[ \t]+/, "", rseg)
                sub(/^[ \t]+/, "", rseg)
                m = shell_words(substr(rseg, skip_assign_words(rseg)), T)
            }
            if (m == 0) continue
            # repo#633: look through command / env / exec / nice / nohup /
            # time / builtin / noglob (and a sudo behind one of them) to the
            # word they run, so `env git reset --hard` is the same force op
            # as the bare form. env NAME=value and -u / -i feed the GIT_DIR /
            # GIT_WORK_TREE selectors above; env -C moves the process cwd.
            fs = fo_unwrap(T, m)
            if (FW_NOTRUN) continue
            if (FW_UNDEC) {
                if (fo_undec_hit(T, m)) print FO_UNDEC SEP "@HEAD@"
                continue
            }
            if (fs > m) continue
            if (fs > 1) {
                for (q = fs; q <= m; q++) T[q - fs + 1] = T[q]
                m = m - fs + 1
            }
            gw = cmdword_static(T[1])
            if (CMDW_UNKNOWN) {
                # An undecodable command word (`$G reset --hard`) may be git:
                # fail closed when a decoded push/reset word AND a force flag
                # follow it.
                if (fo_any_sub(T, 2, m) && fo_any_force(T, 2, m)) print FO_UNDEC SEP "@HEAD@"
                continue
            }
            if (gw != "git") continue
            # Walk the global options between `git` and the subcommand with
            # their real arity (git.c handle_options): -C, -c, --git-dir,
            # --work-tree, --namespace, --super-prefix, --config-env and
            # --attr-source take a SEPARATE value; the long ones also have a
            # joined --opt=value form. Before repo#612 only -C/-c consumed a
            # value, so `git --git-dir .git reset --hard` read `.git` as the
            # subcommand and was never classified.
            #
            # proccwd is the process cwd git runs in: each -C chdirs
            # immediately, composing relative to the previous one. A RELATIVE
            # --git-dir / --work-tree / GIT_DIR / GIT_WORK_TREE is interpreted
            # against the FINAL post--C cwd wherever it sits in the argument
            # order, so the raw values are recorded here and resolved after
            # the loop. A command-line flag overrides the environment, and a
            # later flag overrides an earlier one.
            proccwd = FW_CWD
            gd_raw = envgd_raw; gd_set = envgd_set
            wt_raw = envwt_raw; wt_set = envwt_set
            undec = 0
            k = 2
            while (k <= m) {
                rawt = T[k]
                # Joined long forms are matched on the RAW word when the
                # option name itself is literal, so the value keeps its own
                # quoting for fo_join() (`--git-dir=$HOME/x` stays resolvable).
                if (rawt ~ /^--(git-dir|work-tree)=/) {
                    v = rawt; sub(/^--[a-z-]+=/, "", v)
                    # bash and zsh do NOT tilde-expand after `=` in an
                    # ordinary argument, so git receives `<cwd>/~/…`: keep a
                    # leading tilde literal so fo_join() does not resolve
                    # $HOME (which allowed what main asks).
                    if (substr(v, 1, 1) == "~") v = "\\" v
                    if (rawt ~ /^--git-dir=/) { gd_raw = v; gd_set = 1 } else { wt_raw = v; wt_set = 1 }
                    k++; continue
                }
                if (rawt ~ /^--[A-Za-z][A-Za-z0-9-]*=/) { k++; continue }
                t = cmdword_static(rawt)
                if (CMDW_UNKNOWN) { undec = 1; break }
                if (t == "-C") {
                    if (k + 1 > m) break
                    proccwd = fo_join(proccwd, T[k + 1])
                    k += 2; continue
                }
                if (t == "--git-dir" || t == "--work-tree") {
                    if (k + 1 > m) break
                    if (t == "--git-dir") { gd_raw = T[k + 1]; gd_set = 1 } else { wt_raw = T[k + 1]; wt_set = 1 }
                    k += 2; continue
                }
                if (t == "-c" || t == "--namespace" || t == "--super-prefix" || \
                    t == "--config-env" || t == "--attr-source") { k += 2; continue }
                if (t ~ /^--(git-dir|work-tree)=/) {
                    # Quoted option name: the value was decoded with it, so
                    # hand fo_join() a single-quoted literal of the result.
                    v = t; sub(/^--[a-z-]+=/, "", v)
                    v = SQ1 v SQ1
                    if (t ~ /^--git-dir=/) { gd_raw = v; gd_set = 1 } else { wt_raw = v; wt_set = 1 }
                    k++; continue
                }
                if (t ~ /^-/) { k++; continue }
                break
            }
            if (k > m) continue
            if (undec) {
                if (fo_any_force(T, k + 1, m)) print FO_UNDEC SEP "@HEAD@"
                continue
            }
            subcmd = cmdword_static(T[k])
            if (CMDW_UNKNOWN) {
                if (fo_any_force(T, k + 1, m)) print FO_UNDEC SEP "@HEAD@"
                continue
            }
            if (subcmd != "push" && subcmd != "reset") continue
            # Resolve the repository / work-tree selectors against the final
            # process cwd. Anything that cannot be resolved statically makes
            # the whole op target UNKNOWN (the caller asks). cpath is the
            # process cwd (where git discovers the repo when no git dir is
            # given, and the work tree when none is given); gdir / wtree are
            # emitted as extra fields so the caller resolves the branch from
            # the repository git will really use.
            cpath = proccwd
            gdir = ""; wtree = ""
            if (gd_set) gdir = fo_join(proccwd, gd_raw)
            if (wt_set) wtree = fo_join(proccwd, wt_raw)
            if (cpath == UNK || gdir == UNK || wtree == UNK) { cpath = UNK; gdir = ""; wtree = "" }
            if (gd_set && gdir == "" && cpath != UNK) cpath = UNK
            if (wt_set && wtree == "" && cpath != UNK) cpath = UNK
            if (subcmd == "push") {
                force = 0
                np = 0
                # pos is a file-global awk array; clear it per segment
                # (portable — split with an empty string empties the array) so
                # refspecs from a prior segment cannot leak into this one now
                # that we read every positional slot, not just pos[2].
                split("", pos)
                split("", posunk)
                for (j = k+1; j <= m; j++) {
                    t = cmdword_static(T[j])
                    tu = CMDW_UNKNOWN
                    if (!tu && fo_is_force(t, "push")) { force = 1; continue }
                    if (!tu && t ~ /^-/) continue
                    np++
                    if (np >= 2 && tu && substr(T[j], 1, 1) == "+") force = 1
                    pos[np] = t
                    posunk[np] = tu
                }
                # repo#634: a `+`-prefixed refspec (`+main`, `+HEAD:main`) forces
                # the update by itself, so it is a force signal with no flag.
                for (p = 2; p <= np; p++) if (!posunk[p] && substr(pos[p], 1, 1) == "+") force = 1
                if (!force) continue
                # pos[1] is the remote; pos[2..np] are refspecs. Emit ONE line per
                # positional refspec so a protected branch in ANY refspec position
                # (not just the first) reaches the per-line check in the caller. A
                # bare push with no refspec (np < 2) resolves the checked-out branch.
                # Refspecs are DECODED (repo#612), so a quoted protected name is
                # still that name; one this scan cannot decode (`origin "$B"`)
                # becomes the @UNRESOLVED@ target, which the caller asks on.
                if (np < 2) {
                    print cpath SEP "@HEAD@" SEP "" SEP gdir SEP wtree
                } else {
                    for (p = 2; p <= np; p++) {
                        if (posunk[p]) { print cpath SEP "@UNRESOLVED@" SEP "" SEP gdir SEP wtree; continue }
                        rs = pos[p]
                        sub(/^\+/, "", rs)
                        ci = index(rs, ":")
                        if (ci > 0) rs = substr(rs, ci + 1)
                        # repo#634: a qualified destination names the same branch.
                        sub(/^refs\/heads\//, "", rs)
                        target = "@HEAD@"
                        if (rs != "HEAD" && rs != "") target = rs
                        print cpath SEP target SEP "" SEP gdir SEP wtree
                    }
                }
            } else if (subcmd == "reset") {
                hard = 0
                rt = ""
                # Capture the first positional (non-flag) token as the reset
                # TARGET literal (Loom #5772), e.g. "origin/main" -- emitted as
                # a THIRD field so the caller can recognize a known recovery
                # target when branch identity resolves to a detached HEAD. A
                # bare `git reset --hard` defaults to the literal "HEAD", so
                # the field is never empty on a reset line; a push line never
                # carries one, which is how the caller tells them apart. The
                # TARGET token stays raw (quotes intact): a quoted target
                # simply fails the exact-string tests in the caller and keeps
                # asking. Flags are decoded (repo#612).
                for (j = k+1; j <= m; j++) {
                    t = cmdword_static(T[j])
                    tu = CMDW_UNKNOWN
                    if (!tu && t == "--hard") { hard = 1; continue }
                    # `--` ends options; whatever follows is not a plain
                    # recovery target, so mark the line unrecognized.
                    if (!tu && t == "--") { if (rt == "") rt = "--"; break }
                    if (!tu && t ~ /^-/) continue
                    if (rt == "") rt = T[j]
                }
                if (hard) {
                    if (rt == "") rt = "HEAD"
                    print cpath SEP "@HEAD@" SEP rt SEP gdir SEP wtree
                }
            }
        }
    }'
}

# Redact the quoted VALUES of known text-carrying flags (--body, -m/--message,
# --title, --notes, --comment) so a dangerous-looking phrase quoted INSIDE such a
# value no longer trips the raw ALWAYS_BLOCK_PATTERNS substring scan (catastrophic
# tier) or the ASK_PATTERNS scan (ask tier, #3756). Used ONLY to build the
# literal-redacted working copies for those two loops (mirrors the
# COMMAND_NO_COMMENT precedent); every other scan keeps reading the raw command.
# This kills the #3679 false positive where `gh pr comment --body "…git push
# --force origin main…"` / `git commit -m "…"` hard-denied even though nothing
# executes, and (#3756) the analogous ask-tier false ask where an ask-phrase like
# `gh issue close` quoted inside a `--comment`/`--body` value prompted for
# confirmation despite no such command actually being run.
#
# Safety floor preserved two ways:
#   - `-c` is deliberately NOT a text-carrying flag, so `bash -c '<payload>'`
#     is never redacted and its payload stays caught by the raw scan.
#   - a quoted span is redacted ONLY when it carries no command-substitution or
#     backtick opener (`$(` — which also subsumes the arithmetic `$((` — or a
#     backtick). So a smuggling attempt like `git commit -m "$(git push --force
#     origin main)"` is left intact and still hard-denies.
# Each redacted span is replaced by a SAME-LENGTH placeholder so byte offsets of
# the surrounding command are unchanged. Best-effort like COMMAND_NO_COMMENT:
# it does not model backslash-escaped quotes, but since the result feeds only
# the narrowing (never widening) catastrophic scan, the worst case is a raw
# substring surviving — never a catastrophic block being skipped incorrectly.
# =============================================================================
# dequote_inert_spans (repo#197) — remove the quote CHARACTERS around inert
# quoted spans, leaving their contents in place.
#
# Turns `rm -rf "/"` into `rm -rf /` so the literal catastrophic patterns match
# what the shell would actually run. Callers MUST pass a copy that has already
# been through the sink-aware literal-text redaction, so that prose quoted in a
# --body/-m/--title/--notes/--comment value is already blanked and cannot be
# resurrected into an apparent command by dequoting.
#
# A span containing a command substitution is left completely intact, quotes and
# all, so smuggling still reaches the raw scan unchanged.
#
# Unterminated quotes are emitted verbatim: an unbalanced quote is ambiguous,
# and the raw copy is still scanned, so failing to dequote can only ever keep
# the existing verdict, never widen it.
#
# DELIBERATELY ESCAPE-BLIND, re-verified under #548 (which made the file's
# other quote-pairing scans escape-aware). This one is exempt because it only
# ever REMOVES quote characters — it never redacts, never masks and never
# segments — and its output is grep'"'"'ed for catastrophic patterns IN ADDITION
# to (never instead of) the un-dequoted copy. A mis-paired span can therefore
# only change WHICH extra text gets scanned, never hide text from the scan, so
# no deny can be lost here however the quotes pair. Making it escape-aware
# would change which spans get dequoted and so could only ADD denies — a
# widening with its own risk and no safety gain, exactly the reason the
# byte-presence `index()` test below is also kept as-is.
#
# NOTE: the awk program below is SINGLE-QUOTED. An apostrophe anywhere inside
# it, including in a comment, terminates the string and breaks the guard for
# every command in the repo. Keep comments here apostrophe-free.
# =============================================================================
dequote_inert_spans() {
    printf '%s' "$1" | awk '
    BEGIN {
        SQ = sprintf("%c", 39)
        DQ = sprintf("%c", 34)
        buf = ""
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        s = buf
        out = ""
        n = length(s)
        i = 1
        while (i <= n) {
            c = substr(s, i, 1)
            if (c == DQ || c == SQ) {
                # Find the matching close quote.
                endpos = 0
                for (j = i + 1; j <= n; j++) {
                    if (substr(s, j, 1) == c) { endpos = j; break }
                }
                if (endpos == 0) {
                    # Unterminated: emit the rest verbatim and stop.
                    out = out substr(s, i)
                    i = n + 1
                    continue
                }
                inner = substr(s, i + 1, endpos - i - 1)
                # DELIBERATELY the byte-presence test, NOT has_live_subst().
                # Every other span gate in this file decides whether to REDACT
                # (fewer denies); this one decides whether to DEQUOTE, and a
                # dequoted span is scanned IN ADDITION to the raw copy, so
                # dequoting more can only ADD denies. Accepting escaped-only
                # spans here would therefore widen the catastrophic tier, which
                # is a separate change with its own risk, not part of the
                # escaped-backtick false-positive fix. Leaving a span quoted is
                # the conservative direction and costs nothing: the raw copy
                # still sees it.
                if (index(inner, "$(") == 0 && index(inner, "`") == 0) {
                    out = out inner
                } else {
                    out = out substr(s, i, endpos - i + 1)
                }
                i = endpos + 1
                continue
            }
            out = out c
            i++
        }
        printf "%s", out
    }'
}

# =============================================================================
# HEREDOC-WRAPPED FLAG VALUES (#317)
#
# The `$(`-floor above is exactly right for a general command substitution, but
# it also declines to redact this repo's own pervasive idiom for a multi-line
# comment/commit-message body (this repo's own CLAUDE.md, "Committing changes
# with git" section, prescribes it):
#
#     gh pr comment 315 --body "$(cat <<'EOF'
#     …prose that may QUOTE a dangerous command as an example…
#     EOF
#     )"
#
# Every value built that way necessarily contains `$(`, so before this pass it
# was NEVER redacted — and a dangerous-command example merely quoted inside the
# body (e.g. a Judge documenting a rejected shell-injection payload, or a test
# fixture describing what an `rm -rf /` denial looks like) hard-denied the
# whole command on the catastrophic tier. Reproduced live against:
#   gh pr comment 315 --body "$(cat <<'EOF'
#   fixture asserts rm -rf / is denied
#   EOF
#   )"
#
# mask_flag_cat_heredocs() (below) closes the gap by masking ONLY the BODY of a
# heredoc in this one provably-inert shape, and only when ALL of these hold:
#   1. the opener is the complete tail of its line, immediately preceded by a
#      recognized text-carrying flag, its opening quote, and `$(cat`;
#   2. the heredoc delimiter is QUOTED (single- or double-quoted, `<<-`
#      allowed) — a quoted delimiter is what guarantees the outer shell
#      performs NO expansion on the body, so a `$(…)` sitting IN the body is
#      inert text rather than live code (an UNQUOTED delimiter is rejected
#      outright, so `--body "$(cat <<EOF ... EOF)"` still hard-denies, exactly
#      as before);
#   3. the block is CLOSED in this same buffer (never mask speculatively);
#   4. the very next line after the delimiter line is `)` + that same opening
#      quote — i.e. the substitution ends immediately, with nothing chained
#      after the heredoc inside it;
#   5. the body ITSELF carries no `$(` or backtick on any line. A single-quoted
#      heredoc delimiter genuinely prevents the outer shell from expanding a
#      `$(…)`/backtick that appears IN the body — `cat` only ever sees and
#      echoes it as literal text — so this condition is a deliberately
#      CONSERVATIVE belt-and-suspenders floor, not a correctness requirement:
#      it keeps this masking pass narrowly scoped to bodies that cannot even
#      be misread as carrying a substitution, rather than trusting every
#      caller of this function to reason about heredoc-quoting semantics.
# Condition 4 is what keeps `--body "$(cat <<'EOF' … EOF` <newline> `rm -rf /`
# <newline> `)"` denying: bash ends the heredoc at the delimiter line and then
# genuinely RUNS the following line inside the substitution, so nothing is
# masked there. Condition 1 is what keeps an INTERPRETER-FED heredoc denying —
# a body consumed by `bash <<DELIM`, `sh -s <<DELIM`, or `cat <<DELIM … | sh`
# is live code to the inner shell, and none of those match `<flag> <quote>$(cat`.
# Condition 5 is what keeps `--body "$(cat <<'EOF'` <newline> `$(rm -rf /)`
# <newline> `EOF` <newline> `)"` denying even though the nested `$(rm -rf /)`
# never actually executes (regression test in
# hooks/repo/tests/test-guard-destructive.sh).
#
# KNOWN LIMITATION (deliberate): this recognizes only the literal
# `cat`-consumed shape spelled out above. A semantically equivalent variant —
# `$(command cat <<DELIM …)`, a heredoc opened on a continuation line, or a
# body whose delimiter line is followed by `) "` with a space — is simply not
# recognized and keeps denying exactly as it does today. That is the safe
# direction (a false positive that already exists, never a new bypass), and
# the shape above is the one this repo's own role prompts prescribe.
#
# This is closely related to `.loom/hooks/guard-destructive-generic.sh`'s own
# mask_flag_cat_heredocs() (vendored from upstream Repo Skills rjwalters/repo
# #5216, which independently closed conditions 1-4 of this same gap) — this
# port additionally carries condition 5 (#317's AC #3 nested-smuggling floor);
# keep the two files' behavior in sync.
# =============================================================================
strip_literal_text() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK"'
    # Mask the body of a `<flag> "$(cat <<QUOTED_DELIM … DELIM\n)"` heredoc.
    # See the header comment above for the four conditions and why each is
    # load-bearing. Body bytes are replaced 1:1 with "X" so the buffer keeps
    # its byte offsets and line count; the opener line, the delimiter line and
    # everything outside the body are left untouched.
    function mask_flag_cat_heredocs(s,   lines, nl, i, j, line, pre, oq, delim, dq, closeat, trimmed, body, dashform, dirty) {
        if (index(s, "<<") == 0) return s
        nl = split(s, lines, "\n")
        for (i = 1; i <= nl; i++) {
            line = lines[i]
            # (2) opener must END the line and carry a QUOTED delimiter.
            if (match(line, /<<-?["'"'"'][A-Za-z0-9_]+["'"'"'][ \t]*$/) == 0) continue
            dashform = (substr(line, RSTART + 2, 1) == "-")
            delim = substr(line, RSTART, RLENGTH)
            sub(/^<<-?/, "", delim)
            sub(/[ \t]*$/, "", delim)
            dq = substr(delim, 1, 1)
            if (substr(delim, length(delim), 1) != dq) continue   # quotes must match
            delim = substr(delim, 2, length(delim) - 2)
            if (delim == "") continue
            # (1) …immediately preceded by <flag> <openquote>$(cat.
            pre = substr(line, 1, RSTART - 1)
            if (pre !~ /(^|[ \t])(--message|--body|--notes|--title|--comment|-m)[ \t]*=?[ \t]*["'"'"']\$\([ \t]*cat[ \t]+$/) continue
            oq = ""
            for (j = length(pre); j >= 1; j--) {
                if (substr(pre, j, 2) == "$(") { oq = substr(pre, j - 1, 1); break }
            }
            if (oq != DQ && oq != SQ) continue
            # (3) the block must be CLOSED inside this buffer.
            closeat = 0
            for (j = i + 1; j <= nl; j++) {
                trimmed = lines[j]
                if (dashform) sub(/^\t+/, "", trimmed)
                if (trimmed == delim) { closeat = j; break }
            }
            if (closeat == 0) continue
            # (4) the substitution must close IMMEDIATELY after the delimiter
            #     line — `)` + the same opening quote — so nothing chained
            #     after the heredoc inside `$( … )` is masked away.
            if (closeat == nl) continue
            if (substr(lines[closeat + 1], 1, 2) != ")" oq) continue
            # (5) the body must carry no `$(`/backtick on ANY line — a
            #     deliberately conservative floor, see the header comment.
            dirty = 0
            for (j = i + 1; j < closeat; j++) {
                if (index(lines[j], "$(") != 0 || index(lines[j], "`") != 0) { dirty = 1; break }
            }
            if (dirty) continue
            for (j = i + 1; j < closeat; j++) {
                body = lines[j]
                gsub(/./, "X", body)
                lines[j] = body
            }
            i = closeat
        }
        s = lines[1]
        for (i = 2; i <= nl; i++) s = s "\n" lines[i]
        return s
    }
    BEGIN {
        SQ = sprintf("%c", 39)   # single quote
        DQ = sprintf("%c", 34)   # double quote
        # boundary + text-carrying flag + optional (ws / = / ws) + quoted span.
        # The leading boundary class includes a newline so a `--body` that begins
        # a continuation line is still recognized; the quoted-span classes
        # ([^"]* / [^'"'"']*) already match a newline, so a MULTI-LINE quoted
        # value is captured as one span once the whole command is slurped below.
        #
        # `--search` (gh issue/pr list --search "<text>") and `--comment|--search`
        # both carry free text GitHub matches against, never executes.
        # Second alternative (Loom #5797): `jq --arg NAME "<value>"` /
        # `jq --argjson NAME "<value>"` — a bare identifier token (NAME) sits
        # between the flag and the quoted value, which the first alternative
        # does not anticipate, so it gets its own alternative. jq binds the
        # value as DATA; it is never executed. The same ESCAPED CLOSE
        # extension below applies to both alternatives (qpos is the first quote
        # byte, and NAME cannot contain one). The has_live_subst() floor below
        # is unchanged, so a double-quoted value carrying a live command
        # substitution stays visible and still denies.
        re = "(^|[ \t\n])(--message|--body|--notes|--title|--comment|--search|-m)[ \t]*=?[ \t]*(" \
             DQ "[^" DQ "]*" DQ "|" SQ "[^" SQ "]*" SQ ")" \
             "|(^|[ \t\n])(--arg|--argjson)[ \t]+[A-Za-z_][A-Za-z0-9_]*[ \t]+(" \
             DQ "[^" DQ "]*" DQ "|" SQ "[^" SQ "]*" SQ ")"
        buf = ""
    }
    # MULTI-LINE REDACTION (#3898): slurp the whole (possibly multi-line) command
    # into one buffer, preserving embedded newlines, then redact ONCE in END so a
    # quoted flag value that spans several lines is treated as a single inert
    # span. The old per-line ($0) processing split a multi-line `gh issue create
    # --body "…"` body at each newline, leaving a dangerous phrase quoted on an
    # interior line un-redacted — which then tripped the catastrophic scan on
    # documentation text that merely MENTIONS a dangerous command (the meta
    # false-positive that blocked filing #3898). Single-line input is
    # byte-for-byte identical to the previous behaviour.
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        # PRE-PASS (#317): blank the body of a `<flag> "$(cat <<QDELIMQ … )"`
        # heredoc before the quoted-span redaction below runs. It has to happen
        # here rather than inside the loop because `re`'"'"'s quoted-span classes
        # ([^"]* / [^'"'"']*) stop at the first quote character, and a heredoc
        # body is free to contain raw quotes (prose routinely does) — so the
        # span match alone cannot see such a value whole. Masking first also
        # means the `$(`-floor below needs no exception: by the time the loop
        # reads this span, the only text left inside it is `$(cat <<QDELIMQ`,
        # the delimiter, and `)`.
        s = mask_flag_cat_heredocs(buf)
        out = ""
        while (match(s, re)) {
            pre     = substr(s, 1, RSTART - 1)
            matched = substr(s, RSTART, RLENGTH)
            s       = substr(s, RSTART + RLENGTH)
            # Locate the opening quote inside the matched span.
            qpos = 0
            for (i = 1; i <= length(matched); i++) {
                c = substr(matched, i, 1)
                if (c == DQ || c == SQ) { qpos = i; break }
            }
            head  = substr(matched, 1, qpos)                              # up to & incl. opening quote
            qchar = substr(matched, qpos, 1)
            # ESCAPED CLOSE (#548). `re`'"'"'s quoted-span class is a plain
            # [^"]*, which has no way to express "not an ESCAPED quote" in
            # POSIX ERE, so the match ends at the first `"` byte even when
            # bash reads it as literal text. A value spelled
            # `--body "he said \"hi\" about id > <main>/f"` was therefore
            # redacted only as far as the `\"`, and the REST of the value —
            # with its own escaped quotes still in it — was handed downstream
            # as if it were unquoted shell text. Every later quote-tracking
            # pass then paired quotes differently from bash, which is how a
            # `>` sitting in prose became a live redirection operator and a
            # purely textual `gh pr comment` false-DENIED on write
            # confinement. Extend the span to the close bash itself pairs:
            # scan on for the first UNESCAPED same-kind quote, and if there is
            # none, leave the match exactly as the regex found it (never widen
            # the redaction on unbalanced input). Only DOUBLE quotes can carry
            # an escape — between `S…S` bash has no escape at all, so the
            # regex close is already exact there.
            if (qchar == DQ && bs_escaped(matched, length(matched))) {
                comb = matched s
                cl = 0
                for (i = length(matched) + 1; i <= length(comb); i++) {
                    if (substr(comb, i, 1) == qchar && !bs_escaped(comb, i)) { cl = i; break }
                }
                if (cl > 0) {
                    matched = substr(comb, 1, cl)
                    s = substr(comb, cl + 1)
                }
            }
            inner = substr(matched, qpos + 1, length(matched) - qpos - 1) # between the quotes
            # Redact ONLY provably inert text (no command substitution / backtick).
            # gsub(/./) leaves embedded newlines untouched (awk `.` never matches a
            # newline), so a multi-line span stays SAME-LENGTH and byte offsets of
            # the surrounding command are preserved.
            # LIVE substitution only (has_live_subst()): an escaped backtick
            # or `\$(` is literal text, so the value is still provably inert.
            if (!has_live_subst(inner)) {
                gsub(/./, "X", inner)
            }
            out = out pre head inner qchar
        }
        out = out s
        printf "%s", out
    }'
}

# Redact the quoted argument(s) of a non-executing "data sink" command word
# (echo, printf) so a dangerous-looking string that appears ONLY as quoted DATA
# handed to echo/printf no longer trips the raw ALWAYS_BLOCK_PATTERNS scan
# (catastrophic tier) or the ASK_PATTERNS scan (ask tier) (#53). echo/printf
# print their arguments verbatim; they never EXECUTE them, so a quoted argument
# is inert text — exactly like a --body/-m value — yet strip_literal_text()'s
# flag allowlist never covered it. This is the meta false-positive that blocked
# a guard self-test (`echo '{"…":"<dangerous cmd>"}' | guard-destructive.sh`)
# and blocked filing this very issue's heredoc body.
#
# Command-word anchored (mirrors fastpath_builtin_admits() and the segment
# parsers): the quoted args are redacted ONLY for a simple command whose FIRST
# token is exactly `echo` or `printf` (optionally behind a bare `sudo`/`env`
# wrapper). A wrapper that actually EXECUTES its argument — `bash -c '<payload>'`,
# `sh -c`, `eval`, `xargs` — is never a data sink and is never redacted here.
#
# Safety floor, identical to strip_literal_text()/qsplit():
#   - A quoted span is redacted ONLY when it carries no command substitution /
#     backtick opener (`$(` or a backtick), so a smuggled `echo "$(<payload>)"`
#     keeps its payload intact and still hard-denies.
#   - The `echo '<payload>' | sh` shape (data PIPED into a shell that WOULD
#     execute it) is handled by the command_has_shell_segment() gate at the call
#     site, which skips this redaction entirely whenever any pipeline segment's
#     command word is a shell — so the raw scan still sees and blocks the payload.
#
# Single-pass quote-aware lexer. It deliberately does NOT reuse qsplit(), whose
# `\n`-per-separator contract would conflate a real newline inside a multi-line
# quoted span with a shell separator; here a multi-line span is redacted as one
# inert unit (`.` never matches a newline, so each line stays SAME-LENGTH and the
# surrounding byte offsets are preserved). Best-effort like strip_literal_text():
# an unterminated quote copies the remainder verbatim (never redacts), and the
# result feeds only the NARROWING scans, so the worst case is a raw substring
# surviving (a false block) — never a catastrophic block being skipped.
#
# ---------------------------------------------------------------------------
# OPT-IN QUERY SINKS (repo#311) — $2 non-empty enables them
# ---------------------------------------------------------------------------
# `jq`, `grep`/`egrep`/`fgrep`/`rg`, `sed` and `awk` are data sinks in exactly
# the same sense echo/printf are: their pattern/program argument is text they
# MATCH AGAINST or PRINT, never text they execute. A `jq` query over this very
# repo's guard-decision log, or a `grep` for the literal text of a catastrophic
# pattern, was denied purely because the pattern text appeared as the query
# (repo#311). They are behind an opt-in second argument rather than always-on
# because COMMAND_ASK_SCAN feeds two DENY-tier consumers whose SUBJECT is a
# grep/sed command word (the SQL DDL scan and extract_write_targets()'s write
# confinement — see the consumer audit table at _POSITIONAL_MASK_NEVER); only
# the catastrophic working copy, whose sole consumer is the
# ALWAYS_BLOCK_PATTERNS loop, opts in.
#
# Unlike echo/printf — where every argument is data by definition — these
# commands have flags and sub-forms that DO act. A command word is admitted as
# a query sink only after its whole simple command (raw, pre-redaction, read
# with qseg() below) survives a per-command veto:
#
#   sed   — vetoed by `-i`/`--in-place` (edits the file), by a `w`/`W` write
#           command or s///w flag, and by an `e` execute command or s///e flag.
#           What remains is a query-only `sed -n '…p'` / `sed 's/…/…/'`.
#   awk   — vetoed by `system(…)`, by a pipe-to-command (`print | "cmd"`,
#           `|&` coprocess), and by the one-way `"cmd" | getline` exec form.
#           What remains is pure pattern/print program text.
#   rg    — vetoed by `--pre`/`--pre-glob`/`--hostname-bin`, which name an
#           external program ripgrep executes.
#   jq /
#   grep  — no execution surface at all (jq has no shell-out filter; grep has
#           no exec flag), so nothing to veto.
#
# The veto is whole-command, not per-argument: one `system(` anywhere in the
# simple command disqualifies the entire command from sink treatment, so there
# is no argument-position arithmetic to get wrong (a `sed -i` flag or an
# `awk -F ':'` separator can never be mistaken for program text, because a
# vetoed command is not a sink at all and a non-vetoed one has no argument that
# executes). The vetoes are deliberately over-broad — they may decline a
# genuinely inert command — because declining is the SAFE direction: it keeps
# today's behaviour (a false block), never widens a deny into an allow.
#
# The rest of the safety floor is shared verbatim with echo/printf above: spans
# carrying `$(`/backtick are never redacted, redirection targets are never
# redacted, the command_has_shell_segment() gate at the call site skips this
# whole function whenever any segment could feed a shell, and `bash -c`/`sh -c`/
# `eval`/`xargs` are not sinks and never will be.
#
# $1 = command string. $2 = non-empty to enable the query sinks (absent/empty
# keeps the historical echo/printf-only behaviour).
#
# CONTRACT (repo#434): when $2 enables the query sinks, $1 MUST be the RAW
# command, not a copy another masking pass has already redacted. The vetoes
# below are text checks — they can only refuse what they can still SEE — so a
# copy in which strip_literal_text() has already blanked a `--title "… -i …"`
# value hides the very `-i` the sed veto exists to catch, and admits as an inert
# query sink a command the veto was meant to refuse. strip_literal_text()'s
# quoted-value redaction is a global textual regex with no notion of which
# simple command a flag belongs to, so the hidden token need only share a simple
# command with the sink word. The call site therefore runs THIS function first
# and strip_literal_text() second; ordering them that way is safe in both
# directions because neither pass can create text the other keys on (see the
# PASS 2 comment at the call site for the full argument).
strip_datasink_literals() {
    printf '%s' "$1" | awk -v qsinks="${2:-}" "$_ESCAPE_AWK$_HASLIVESUBST_AWK"'
    # Raw text of the simple command starting at `start`, up to the first
    # UNQUOTED shell separator (or end of buffer). Quote-aware so an `awk`
    # program that contains `|` or `;` inside its quoted program text is read
    # as one unit — which is exactly what the vetoes below must see.
    #
    # Escape-aware on the same two rules as the main walk below (#548): an
    # escaped quote opens nothing, and a `\"` does not close a DOUBLE-quoted
    # span (a SINGLE-quoted one still ends at its next quote, since bash has
    # no escape between `S…S`). Blindness here could end a span at a `\"` and
    # make the NEXT separator a false command boundary, truncating the text the
    # vetoes read — so a `sed -i` whose `-i` fell past that boundary was never
    # vetoed and the command was admitted as an inert query sink.
    function qseg(s, start, n,    i, c, q, out) {
        out = ""; q = ""
        for (i = start; i <= n; i++) {
            c = substr(s, i, 1)
            if (q != "") {
                out = out c
                if (c == q && !(q == DQ && bs_escaped(s, i))) q = ""
                continue
            }
            if ((c == SQ || c == DQ) && !bs_escaped(s, i)) { q = c; out = out c; continue }
            if (c == ";" || c == "&" || c == "|" || c == "\n") break
            out = out c
        }
        return out
    }
    # Is `tok` a query-sink command word at all? (The veto is separate.)
    function is_query_sink(tok) {
        return (tok == "jq" || tok == "grep" || tok == "egrep" || \
                tok == "fgrep" || tok == "rg" || tok == "sed" || \
                tok == "awk" || tok == "gawk" || tok == "mawk")
    }
    # Per-command veto over the RAW simple command. Returns 1 to admit.
    #
    # "RAW" is a contract on the CALLER (repo#434): `seg` comes from qseg() over
    # $1, so these checks are only as honest as $1 is unredacted. See the
    # CONTRACT paragraph in the shell header comment above this function.
    function query_sink_ok(tok, seg) {
        if (tok == "sed") {
            # In-place edit: --in-place, or any short-flag cluster carrying i
            # (-i, -ni, -i.bak). A leading `--` can never start such a cluster.
            if (seg ~ /(^|[ \t])--in-place/) return 0
            if (seg ~ /(^|[ \t])-[A-Za-z]*i/) return 0
            # `w file` / `W file` / s///w flag — writes a file. `e cmd` /
            # s///e flag — EXECUTES the pattern space. The character BEFORE the
            # command letter is whatever delimiter the author chose (`s|a|b|w`
            # is as valid as `s/a/b/w`), so the guard is "not part of a word"
            # rather than a fixed delimiter set — deliberately over-broad, since
            # declining to treat a sed as a sink only preserves the verdict the
            # guard already produces.
            if (seg ~ /(^|[^A-Za-z0-9_])[wW][ \t]/) return 0
            if (seg ~ /(^|[^A-Za-z0-9_])e([ \t;}'"'"'"]|$)/) return 0
            return 1
        }
        if (tok == "awk" || tok == "gawk" || tok == "mawk") {
            if (seg ~ /system[ \t]*\(/) return 0      # system("cmd")
            if (seg ~ /\|[ \t]*"/) return 0           # print | "cmd"
            if (seg ~ /\|[ \t]*&/) return 0           # |& coprocess
            if (seg ~ /\|[ \t]*getline/) return 0     # "cmd" | getline (one-way exec)
            return 1
        }
        if (tok == "rg") {
            # ripgrep flags that name an external program it then executes.
            if (seg ~ /(^|[ \t])--pre([ \t=]|$)/) return 0
            if (seg ~ /(^|[ \t])--pre-glob([ \t=]|$)/) return 0
            if (seg ~ /(^|[ \t])--hostname-bin([ \t=]|$)/) return 0
            return 1
        }
        return 1   # jq / grep / egrep / fgrep: no execution surface
    }
    BEGIN {
        SQ = sprintf("%c", 39)   # single quote
        DQ = sprintf("%c", 34)   # double quote
        buf = ""
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        s = buf
        n = length(s)
        subst_depth(s, sdep)   # per-byte `$( )`/backtick depth (repo#439, #453)
        out = ""
        i = 1
        atcmd = 1     # at the start of a simple command (command-word position)
        sink = 0      # inside an echo/printf data-sink command
        redir = 0     # the previous token was a redirection operator (repo#197)
        while (i <= n) {
            c = substr(s, i, 1)
            # A shell separator resets to command-word position.
            if (c == ";" || c == "&" || c == "|" || c == "\n") {
                out = out c; i++; atcmd = 1; sink = 0; redir = 0; continue
            }
            # Leading whitespace is copied without leaving command-word position.
            # It also does NOT clear redir, so the space between the operator
            # and its target is transparent.
            if (c == " " || c == "\t") { out = out c; i++; continue }
            # A redirection operator. What follows is a FILENAME handed to the
            # redirection by the shell, never an argument to echo/printf, so it
            # must not be redacted as data. Without this, a quoted redirect
            # target after echo was blanked and Bash-tool write confinement went
            # blind to it -- deny for a bare target, allow for the identical
            # quoted one (repo#197). Only echo/printf were affected; cat, tee,
            # cp, mv and sed -i confine quoted targets correctly because they
            # are not data sinks.
            if (c == ">") { out = out c; i++; redir = 1; continue }
            # Command-word position: read the first token and classify it.
            if (atcmd) {
                atcmd = 0
                tok = ""
                j = i
                while (j <= n) {
                    cc = substr(s, j, 1)
                    if (cc == " " || cc == "\t" || cc == ";" || cc == "&" || cc == "|" || cc == "\n") break
                    tok = tok cc
                    j++
                }
                # A bare sudo/env wrapper: emit it and stay in command-word
                # position so the NEXT token is classified as the command word.
                if (tok == "sudo" || tok == "env") {
                    out = out tok; i = j; atcmd = 1; continue
                }
                if (tok == "echo" || tok == "printf") { sink = 1 }
                # repo#311 query sinks (opt-in): admitted only when the RAW
                # remainder of this simple command carries no acting sub-form.
                # A path-qualified spelling (/usr/bin/jq, ./rg) is classified on
                # its basename, exactly like command_has_shell_segment() does.
                else if (qsinks != "") {
                    base = tok
                    sub(/.*\//, "", base)
                    if (is_query_sink(base) && query_sink_ok(base, qseg(s, j, n))) { sink = 1 }
                }
                out = out tok; i = j; continue
            }
            # Mid-command: a quoted span is redacted only inside a data sink.
            #
            # A BACKSLASH-ESCAPED quote never OPENS a span (#548) — the same
            # rule qsplit()/ml_segment() have applied since #113. At an opener
            # position this lexer is, by construction, OUTSIDE any quoted span
            # (every span it recognises is consumed whole by the branch below),
            # so an escaped quote there is exactly what bash reads it as:
            # literal text. Opening a span on it was a write-confinement
            # BYPASS of the same family as the close scans below — from a
            # worktree cwd, `echo \" && cp /tmp/a <main>/f; echo "z"` paired
            # the escaped quote with the `"` in the trailing `echo "z"` and
            # redacted ` && cp /tmp/a <main>/f; echo ` as echo data, blanking
            # the separator and the write target before
            # extract_write_targets() ran. bash really performs that write.
            if ((c == DQ || c == SQ) && !bs_escaped(s, i)) {
                qc = c
                ci = 0
                # A DOUBLE-quoted span closes only on a `"` at the opener-s OWN
                # `$( )`/backtick depth (the #453 rule, applied here for repo#439).
                # A `"` one substitution level deeper belongs to the INNER
                # shell-s quoting, so accepting it is a phantom close: for
                # `echo "$(echo "a" > <main>/e.sh)"` the naive pairing split the
                # value into `"$(echo "` (live, kept) and `" > <main>/e.sh)"`
                # (no substitution, so REDACTED as echo data) — blanking the
                # substitution-s own `>` and target before extract_write_targets()
                # ever ran, so subst_heads() had nothing left to re-emit and the
                # write into the main checkout was allowed. Depth-matched, the
                # whole value is one span carrying a live `$(` and is never
                # redacted. Single quotes are exempt exactly as in
                # trusted_close(): they cannot nest, so their close is always
                # the next single-quote byte. A span with no depth-matched
                # close falls to the unterminated branch below, which never
                # redacts — the safe direction.
                #
                # A BACKSLASH-ESCAPED `"` is ALSO not a close (#548), for the
                # same reason it is not an opener above: inside `"…"` bash
                # reads `\"` as a literal quote character and the span runs on.
                # Accepting it ended the echo value early, the following REAL
                # `"` was read as a NEW opener, and everything up to the next
                # quote — including a real `&& cp … <main>/f` — was redacted as
                # echo data (`echo "\"" && cp /tmp/a <main>/f; echo "z"`, #548
                # row 1; bash really performs that write). SINGLE quotes are
                # exempt from the escape skip, and that exemption is exact
                # rather than conservative: between `S…S` a backslash is an
                # ORDINARY literal byte, so `S a\ S` really does end at that
                # quote and skipping it would extend the redaction PAST what
                # bash treats as data — the one direction that could hide a
                # live write (`echo Sa\S && cp /tmp/a <main>/f` must keep
                # denying). bs_escaped() is the same parity helper
                # trusted_close() uses, so `\\"` (escaped BACKSLASH, live
                # quote) still closes.
                for (j = i + 1; j <= n; j++) {
                    if (substr(s, j, 1) == qc && !(qc == DQ && bs_escaped(s, j)) && \
                        (qc != DQ || sdep[j] == sdep[i])) { ci = j; break }
                }
                # Redact only when the depth-matched close AGREES with the
                # naive next-quote close. This landed for the case subst_depth()
                # got wrong while it was quote-blind: a `)` inside the inner
                # shell-s own quotes (`"$(printf ")" )"`) ended the substitution
                # early, the depth-matched close landed on a LATER `"`, every
                # following span was misaligned, and a real unquoted
                # `> <main>/f` got redacted as echo data — allowed, where the
                # naive pairing denies. subst_depth() is quote-AWARE inside an
                # open substitution since #539, so that specific misalignment is
                # gone at the source; the agreement check is KEPT as the
                # belt-and-braces floor, because "the two pairings disagree" is
                # a general ambiguity signal (an escaped quote, an unbalanced
                # one) and falling to the unterminated branch below — copy
                # verbatim, never redact — is the safe direction in every case.
                #
                # This scan carries the SAME escaped-close skip as the
                # depth-matched one above (#548). It has to: the two are
                # compared for agreement, so leaving one escape-blind would
                # manufacture a permanent disagreement on every span holding a
                # `\"` and silently disable the redaction (a false-positive
                # source), while leaving BOTH escape-blind is the #548 bypass.
                cn = 0
                for (j = i + 1; j <= n; j++) {
                    if (substr(s, j, 1) == qc && !(qc == DQ && bs_escaped(s, j))) { cn = j; break }
                }
                if (cn != ci) ci = 0
                if (ci == 0) {
                    # Unterminated quote: copy the rest verbatim, never redact.
                    out = out substr(s, i); i = n + 1; continue
                }
                inner = substr(s, i + 1, ci - i - 1)
                # LIVE substitution only (has_live_subst()): an escaped
                # backtick in echo/printf data is literal text, not a command.
                if (sink && !redir && !has_live_subst(inner)) {
                    gsub(/./, "X", inner)   # . never matches \n: multi-line stays same-length
                }
                out = out qc inner qc; i = ci + 1; redir = 0; continue
            }
            out = out c; i++; redir = 0
        }
        printf "%s", out
    }'
}

# Mask quoted POSITIONAL arguments (no preceding flag name) to a repo-
# configurable allowlist of known non-executing commands/scripts (#195). Used
# to build the ASK-tier working copy (COMMAND_ASK_SCAN) ONLY — see the call
# site below strip_datasink_literals()'s invocation for that copy. This is
# strip_literal_text()'s counterpart for POSITIONAL text: strip_literal_text()
# only recognizes text following a NAMED flag (--body/-m/--title/--notes/
# --comment); it has no effect on a script whose free-text arguments are
# purely positional, e.g. `./scripts/check-duplicate.sh "TITLE"
# "DESCRIPTION"` where DESCRIPTION happens to quote an ask-phrase. Such a
# script never EXECUTES a positional argument — it only reads it as inert
# search/dedup text — so masking a quoted argument immediately following the
# configured command (optionally after short/long flags, e.g. `check-
# duplicate.sh --include-merged-prs "..."`) can never blind ASK_PATTERNS (or
# any other COMMAND_ASK_SCAN consumer) to a REAL invocation: a wrapper that
# WRAPS the phrase and then executes it — `sh -c "git stash pop"`, `bash -c
# '...'`, `eval "..."` — is never in the allowlist and stays fully visible.
#
# DELIBERATELY EXCLUDES grep/egrep/fgrep/rg AND cp/mv/tee/sed — enforced by
# the caller (positional_mask_cmdre()'s _POSITIONAL_MASK_NEVER set above drops
# them even when configured), not by this function, which simply masks
# whatever command-name alternation it is given. The reasons live with the
# caller (see its full COMMAND_ASK_SCAN consumer audit table); in short, this
# scan feeds TWO deny-tier consumers besides the ask-tier ones:
#
#   - the SQL DDL/DML check (SQL_DDL_PATTERN, below), which intentionally
#     scans a `grep '<pattern>' file` invocation's own quoted positional
#     pattern for a literal DDL phrase like "DROP TABLE" and DENIES, by
#     design — masking grep's own quoted argument here would blind that scan
#     to text it is specifically meant to catch. Adding grep/rg to the
#     allowlist was tried and directly regresses the "Fast path security" /
#     SQL-DDL test coverage in hooks/repo/tests/test-guard-destructive.sh.
#   - the #4178 Bash-tool WRITE CONFINEMENT block, which passes this exact
#     scan to extract_write_targets() and DENIES a write landing in the main
#     checkout from a builder worktree. cp/mv/tee/sed are the command words
#     that extractor recognizes as write idioms, and their target PATH is a
#     positional argument — masking it made `cp "/tmp/src.txt"
#     "<main-checkout>/evil.sh"` fall through from deny to ALLOW under
#     `positionalMaskAllowlist: ["cp"]` (#195 review finding).
#
# Extend the exclusion set only for another read-only positional-arg consumer
# with NO competing raw-text consumer elsewhere in this file (mirrors the
# vendored guard's own extend-only convention for this allowlist), and extend
# _POSITIONAL_MASK_NEVER whenever a NEW deny-tier consumer of
# COMMAND_ASK_SCAN with a recognizable command word is added.
#
# Only feeds COMMAND_ASK_SCAN, never the catastrophic scan (which keeps
# reading raw $COMMAND/$COMMAND_NO_LITERAL_TEXT). Within COMMAND_ASK_SCAN it
# narrows ask-tier matching only — the two deny-tier consumers above stay
# intact because their subject command words can never enter the allowlist.
#
# Masks EVERY quoted argument that directly, consecutively follows the
# command+flags (separated only by whitespace) — not just the first — so a
# multi-positional-arg script's whole argument list gets masked. Masking
# stops at the first token that is not a quoted string (a bare filename,
# `&&`, `|`, etc.), leaving anything after that boundary — including a real
# ask-triggering invocation chained onto the same line — fully visible.
#
# $1 = command string to mask. $2 = '|'-joined, ERE-escaped allowlist of
# command names (already filtered by positional_mask_cmdre()). The caller
# only invokes this when $2 is non-empty; an absent/empty allowlist is a
# no-op by construction (the anchor regex then never matches), matching the
# "absent config is a no-op" default (#195 AC).
mask_ask_positional_args() {
    # cmdre is threaded through ENVIRON, NOT -v: gawk's -v assignment runs the
    # value through the same C-style backslash-escape decoding as a string
    # constant (so a caller-supplied "\." — the ERE-escaped literal dot
    # positional_mask_cmdre() produces for a name like "check-duplicate.sh" —
    # would be silently decoded back to a bare "." before the regex engine
    # ever sees it, defeating the escaping and emitting a spurious "unknown
    # escape sequence" warning). ENVIRON values are passed through verbatim.
    printf '%s' "$1" | CMDRE_FOR_AWK="$2" awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK"'
    BEGIN {
        SQ = sprintf("%c", 39)
        DQ = sprintf("%c", 34)
        cmdre = ENVIRON["CMDRE_FOR_AWK"]
        # Zero or more short/long flags between the command name and the
        # first quoted positional argument (e.g.
        # `check-duplicate.sh --include-merged-prs --issue 195`).
        flagre = "([ \t]+-[A-Za-z0-9_-]+)*"
        anchor = "(^|[ \t\n;&|`(])(" cmdre ")" flagre "[ \t]+"
        buf = ""
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        s = buf
        out = ""
        while (match(s, anchor)) {
            pre     = substr(s, 1, RSTART - 1)
            matched = substr(s, RSTART, RLENGTH)
            rest    = substr(s, RSTART + RLENGTH)
            out = out pre matched
            # Mask every consecutive quoted positional argument immediately
            # following the anchor (whitespace-separated). Stops at the first
            # non-quote-starting token, so anything after the argument list
            # (a pipe, &&, an unrelated command) is left fully visible.
            while (1) {
                qc = substr(rest, 1, 1)
                if (qc != DQ && qc != SQ) break
                endpos = 0
                # A backslash-escaped `"` does not close a DOUBLE-quoted span
                # (#548) — bash reads it as a literal quote character. Ending
                # the masked argument there let the following REAL close be
                # read as a new opener, so this redaction could swallow a
                # chained `&& cp … <main>/f` as if it were positional data and
                # blank it out of extract_write_targets()-s view. SINGLE quotes
                # keep the next-quote rule: between `S…S` bash has no escape,
                # so skipping a quote there would mask live code instead.
                for (i = 2; i <= length(rest); i++) {
                    if (substr(rest, i, 1) == qc && !(qc == DQ && bs_escaped(rest, i))) { endpos = i; break }
                }
                if (endpos == 0) break
                inner = substr(rest, 2, endpos - 2)
                # LIVE substitution only (has_live_subst()): an escaped
                # backtick in a positional argument is literal text.
                if (!has_live_subst(inner)) {
                    gsub(/./, "X", inner)
                }
                out = out qc inner qc
                rest = substr(rest, endpos + 1)
                while (substr(rest, 1, 1) == " " || substr(rest, 1, 1) == "\t") {
                    out = out substr(rest, 1, 1)
                    rest = substr(rest, 2)
                }
            }
            s = rest
        }
        out = out s
        printf "%s", out
    }'
}

# Return 0 (success) if ANY quote-aware segment's command word is a shell binary
# (sh/bash/dash/zsh/ksh/csh/tcsh/fish/pwsh, with or without a leading path) OR a
# pipeline consumer that can itself spawn a shell over its input (`xargs`,
# `parallel`). GATES strip_datasink_literals(): when a shell could consume the
# command's data (e.g. `echo '<payload>' | sh`, or the same payload reached
# through `echo '<payload>' | xargs -I{} sh -c '{}'`), the data-sink redaction
# is skipped so the raw catastrophic scan still sees — and blocks — the
# payload. Conservative by construction: a shell-or-shell-spawner ANYWHERE in
# the command disables the (narrowing) redaction, so the worst case is a
# preserved false BLOCK, never a skipped one.
#
# `xargs`/`parallel` are matched UNCONDITIONALLY — regardless of what command
# they themselves invoke — because `xargs <anything>` handed attacker-shaped
# input on the pipe is the hazard, not just the `sh -c`/`bash -c` sub-form of
# it (repo#429). The precision cost is measured and accepted: an ordinary,
# non-shell `xargs`/`parallel` pipeline that carries no dangerous text (e.g.
# `grep '<text>' f | xargs -I{} echo {}`) goes back to being redaction-blind,
# i.e. it now depends on the raw scan not matching `<text>` rather than on the
# redaction — a false DENY only if `<text>` itself matches ALWAYS_BLOCK, never
# a missed catastrophic block.
#
# `guard-destructive.sh` (basename is not a bare shell/xargs/parallel word) is
# deliberately NOT matched, so the guard's own `echo '<json>' | guard-destructive.sh`
# self-test still redacts and no longer false-blocks (#53). Emits "yes"/"no".
command_has_shell_segment() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK"'
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        found = 0
        s = qsplit(buf)   # quote-aware segmentation (#3755); separators -> \n
        n = split(s, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            # Strip any run of leading VAR=val assignments, then a sudo/env wrapper,
            # so the REAL command word is classified. The required trailing [ \t]+
            # in the assignment pattern guarantees the loop makes progress. This
            # also composes with xargs/parallel below: `sudo xargs …` / `env
            # parallel …` are stripped down to the same bare command word.
            while (match(seg, /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/)) { seg = substr(seg, RLENGTH + 1) }
            sub(/^sudo[ \t]+/, "", seg)
            sub(/^env[ \t]+/, "", seg)
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            w = toks[1]
            sub(/.*\//, "", w)   # basename only
            if (w == "sh" || w == "bash" || w == "dash" || w == "zsh" || \
                w == "ksh" || w == "csh" || w == "tcsh" || w == "fish" || w == "pwsh" || \
                w == "xargs" || w == "parallel") { found = 1 }
        }
        print (found ? "yes" : "no")
    }'
}

# Toggle hint (loom#10434, ported in repo#585): one line naming the REPO_* env
# var (set in the agent's OWN environment — the hook is a separate process, so an
# inline `VAR=x cmd` prefix never reaches it and is never suggested) and the
# config key for a TOGGLEABLE category. Tags that no toggle can disable
# (catastrophic*, rm-protected-path, lifecycle-or-cloud-delete, git-read-tree,
# printenv/ssh/systemctl asks, ...) match no arm and get no hint. Env names and
# config path follow THIS guard's model (REPO_* wins over legacy LOOM_*;
# .claude/skills/repo/config.json), not Loom's LOOM_*/.loom/config.json. Callers
# skip the hint when the message already names a guards.* key, so existing
# inline hints are not duplicated.
toggle_hint_for_tag() {
    local env="" key=""
    case "$1" in
        sql-ddl|sql-delete-no-where)  env="REPO_GUARD_SQL=0";                key='"guards.sqlDdl": false' ;;
        cloud-cli:*)                  env="REPO_GUARD_CLOUD=0";              key='"guards.cloudCli": false' ;;
        reversible-gh:*)              env="REPO_GUARD_REVERSIBLE_GH=1";      key='"guards.reversibleGh": true' ;;
        cargo-clean-scope-outside-repo) env="REPO_GUARD_CARGO_CLEAN=0";      key='"guards.cargoCleanScope": false' ;;
        rm-scope-unresolved-var|rm-scope-outside-repo) env="REPO_RM_SCOPE=off"; key='"guards.rmScope": "off"' ;;
        force-op:*)                   env="REPO_FORCE_SCOPE=off";            key='"guards.forceScope": "off"' ;;
        stash-scope:*)                env="REPO_GUARD_STASH_SCOPE=0";        key='"guards.stashScope": false' ;;
        worktree-write-confinement*)  env="REPO_GUARD_WORKTREE_ISOLATION=0"; key='"guards.worktreeIsolation": false' ;;
        tmpfs-scratch-dir:*)          env="REPO_GUARD_TMPFS_SCRATCH=0";      key='"guards.tmpfsScratch": false' ;;
        *) return 0 ;;
    esac
    printf '\nToggle: set %s in the agent environment, or %s in .claude/skills/repo/config.json' "$env" "$key"
}

# Helper: output a deny decision and exit
#
# Optional second arg is a short, STABLE rule tag (issue #3771) recorded as the
# decision log's `pattern` field; it defaults to "deny" (a function-name-derived
# fallback) so this stays backward-compatible with call sites that don't pass
# one. Optional third arg is a free-form diagnostic `context` string (issue
# #312) forwarded verbatim to log_guard_decision()'s optional 4th arg — omitted
# by every call site that doesn't pass one, so this is additive-only. Telemetry
# is emitted BEFORE the JSON decision so a logging hiccup can never suppress
# the deny, and the `|| true` guarantees it never trips the ERR trap. Deny is
# always the "catastrophic" tier.
deny() {
    local reason="$1"
    local tag="${2:-deny}"
    local context="${3:-}"
    log_guard_decision "deny" "catastrophic" "$tag" "$context" || true
    [[ "$reason" == *"guards."* ]] || reason="${reason}$(toggle_hint_for_tag "$tag")"
    if jq -n --arg reason "$reason" '{
        hookSpecificOutput: {
            hookEventName: "PreToolUse",
            permissionDecision: "deny",
            permissionDecisionReason: $reason
        }
    }' 2>/dev/null; then
        exit 0
    fi
    # jq failed — emit raw JSON as fallback
    local escaped_reason
    escaped_reason=$(echo "$reason" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\n/\\n/g')
    echo "{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"deny\",\"permissionDecisionReason\":\"${escaped_reason}\"}}"
    exit 0
}

# Helper: output an ask decision and exit
#
# Same optional rule-tag convention as deny() (issue #3771); defaults to "ask".
# Same optional third `context` arg as deny() (issue #312), also additive-only.
# Ask is always the "ask" tier. Telemetry is best-effort and emitted before the
# JSON decision.
ask() {
    local reason="$1"
    local tag="${2:-ask}"
    local context="${3:-}"
    log_guard_decision "ask" "ask" "$tag" "$context" || true
    [[ "$reason" == *"guards."* ]] || reason="${reason}$(toggle_hint_for_tag "$tag")"
    if jq -n --arg reason "$reason" '{
        hookSpecificOutput: {
            hookEventName: "PreToolUse",
            permissionDecision: "ask",
            permissionDecisionReason: $reason
        }
    }' 2>/dev/null; then
        exit 0
    fi
    # jq failed — emit raw JSON as fallback
    local escaped_reason
    escaped_reason=$(echo "$reason" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\n/\\n/g')
    echo "{\"hookSpecificOutput\":{\"hookEventName\":\"PreToolUse\",\"permissionDecision\":\"ask\",\"permissionDecisionReason\":\"${escaped_reason}\"}}"
    exit 0
}

# =============================================================================
# ALWAYS BLOCK - Catastrophic commands that should never execute
# =============================================================================

ALWAYS_BLOCK_PATTERNS=(
    # GitHub destructive operations — command-position anchored (start-of-line
    # or a shell separator must precede the verb) so the phrase inside a flag
    # value no longer trips. NOTE: the catastrophic scan still runs over the
    # full raw command, including quoted/heredoc text, so a `gh repo delete`
    # that a shell would actually execute (leading, sudo-prefixed, or after a
    # separator) still denies (#3553).
    '(^|[;&|[:space:]])gh repo delete'
    '(^|[;&|[:space:]])gh repo archive'

    # Force push to main/master (various flag forms)
    'git push --force origin main'
    'git push --force origin master'
    'git push -f origin main'
    'git push -f origin master'
    'git push --force-with-lease origin main'
    'git push --force-with-lease origin master'
    # repo#634: qualified (refs/heads/<b>) and `+`-forced spellings of the same
    # push. Anchored on the end of the refspec so `+main:other` or
    # `refs/heads/main-x` do not match.
    'git push (--force|-f|--force-with-lease)(=[^[:space:]]*)? origin \+?([^[:space:]:]*:)?refs/heads/(main|master)([[:space:];&|]|$)'
    'git push ((--force|-f|--force-with-lease)(=[^[:space:]]*)? )?origin \+([^[:space:]:]*:)?(refs/heads/)?(main|master)([[:space:];&|]|$)'

    # Filesystem destruction — anchored to a *real* root/home target so that a
    # scoped path like `rm -rf /tmp/x` no longer trips the catastrophic rule,
    # while root / home obliteration still denies. The left side of `rm` is
    # deliberately NOT anchored, so a quoted payload such as `bash -c 'rm -rf /'`
    # (root followed by a closing quote) still matches (#3553). The trailing
    # class matches anything that is not a path-continuation character (so `/`,
    # `/ `, `/*`, `/;`, `/'` all count as "root itself" but `/tmp` does not).
    # NOTE (#72): these three patterns require `rm` to be immediately followed by
    # whitespace, so a command-word substitution like `$(which rm) -rf /` (where
    # `rm` is followed by `)`) does NOT match here. That shape is instead caught
    # by the extract_rm_targets() -> rm-protected-path path below, whose deny
    # covers root, $HOME, AND every top-level dir — a superset of these three.
    # For that superset claim to actually hold for the substitution shape, the
    # extract path must recognize the SAME home/root targets these literal
    # patterns do: extract_rm_targets() strips a leading `env` (and VAR=val
    # assignments) as well as `sudo`, and the protected-path loop expands a bare
    # `~`/`$HOME` target before the check — so `env $(which rm) -rf /`,
    # `$(which rm) -rf ~`, and `$(which rm) -rf $HOME` all deny just like their
    # literal counterparts. No parallel regex is needed for the substitution case.
    'rm[[:space:]]+-[a-zA-Z]*[rf][a-zA-Z]*[[:space:]]+/([^[:alnum:]._~/-]|$)'
    'rm[[:space:]]+-[a-zA-Z]*[rf][a-zA-Z]*[[:space:]]+~([^[:alnum:]._~/-]|$)'
    'rm[[:space:]]+-[a-zA-Z]*[rf][a-zA-Z]*[[:space:]]+\$HOME([^[:alnum:]._~/-]|$)'

    # Fork bombs
    ':\(\)\{ :\|:& \};:'

    # Pipe to shell (supply chain risk) — the piped-to COMMAND must itself be
    # a shell (repo#29). The old shapes ('curl .* \| .*sh', 'curl .* \| bash',
    # 'wget .* \| .*sh', 'wget .* -O- \| sh') matched "sh" anywhere after the
    # pipe, so piping a download to `tee /usr/share/...`, `shasum`, or any
    # path containing "sh" false-positived (and quoting such a pipeline in an
    # issue body blocked the bug report about it). The single fixed pattern
    # anchors on the command position immediately after a pipe: optional
    # sudo (with flags), an optional path prefix, then a shell word
    # (sh/bash/dash/zsh/ksh/csh/tcsh/fish/pwsh) followed by a non-word
    # character. `[^;&]*` spans pipes but not command separators, so a
    # multi-stage pipeline (`curl … | gunzip | sh`) still denies while a
    # neighbouring command after `&&`/`;` is never mis-joined. Known accepted
    # misses: a wrapper consuming the command position (`| sudo -u user sh`,
    # `| env sh`); `bash -c 'curl … | sh'` still denies (raw scan, `-c` is
    # never redacted).
    '(^|[;&|[:space:](])(curl|wget)[^;&]*\|[[:space:]]*(sudo[[:space:]]+(-[^[:space:]]+[[:space:]]+)*)?([^[:space:]|;&]*/)?(ba|da|z|k|c|tc|fi|pw)?sh([[:space:]]|$|[;&|)])'

    # Cloud infrastructure destruction. The aws forms below are specific
    # multi-token phrases, so they stay in this raw substring scan. The az/gcloud
    # CLIs, by contrast, need command-word anchoring — an unanchored `az.*delete`
    # matches "h·az·ard … delete" across unrelated prose tokens (#3584) — so they
    # are handled by the segment-parsed lifecycle/cloud check further below, NOT
    # here.
    # NOTE: `aws ec2 terminate` is deliberately NOT in this raw catastrophic
    # scan. For a repo whose job is standing up and tearing down dev VMs the
    # teardown path (`terminate-instances`) is a first-class workflow, so it is
    # downgraded to an ask via the toggle-gated CLOUD_ASK_PATTERNS below (and
    # fully bypassed when LOOM_GUARD_CLOUD=0 / guards.cloudCli:false). The other
    # aws forms here stay ungated — they remain a hard safety floor (#3593).
    'aws s3 rm.*--recursive'
    'aws s3 rb'
    'aws iam delete'
    'aws cloudformation delete-stack'

    # Docker mass destruction
    'docker system prune'

    # NOTE: system-lifecycle commands (halt/reboot/poweroff/shutdown/init 0/
    # init 6) are deliberately NOT in this raw substring scan. Even the
    # whitespace-inclusive boundary anchor they used to carry still fired inside
    # ordinary prose ("...the box will halt", "...after a reboot event"), and a
    # pure regex tweak can't separate `sudo halt` from `will halt` (both are
    # "<word> halt"). They are handled by the segment-parsed check below, which
    # denies only when a segment's *command word* is exactly the lifecycle word
    # (#3584).
)

# Build a redacted working copy ONLY for the catastrophic scan below. TWO
# passes feed it, and their ORDER is load-bearing (repo#434) — see the block
# comment on the second one for why the data-sink pass must run FIRST.
COMMAND_NO_LITERAL_TEXT="$COMMAND"
# PASS 1 — redact the quoted args of a data-sink command word (echo/printf), so
# a dangerous string handed to echo/printf as inert DATA no longer trips the raw
# scan (#53) — the meta false-positive that blocked guard self-tests and filing
# this issue's heredoc body. Gated on echo/printf being present (off the hot
# path otherwise) AND on NO shell segment existing: when data is piped into a
# shell that would execute it (`echo '<payload>' | sh`), the redaction is skipped
# so the raw scan still blocks the payload. `-c` wrappers (bash -c/sh -c) are
# never data sinks, and `$(`/backtick spans are never redacted, so smuggling
# still hard-denies.
#
# repo#311: the QUERY sinks (jq/grep/egrep/fgrep/rg/inert sed/awk) are enabled
# here — and ONLY here. Their pattern/program argument is text they match
# against or print, never text they execute, so a `jq` query over this repo's
# own guard-decision log, or a `grep` for the literal text of a catastrophic
# pattern, is inert data in exactly the sense an echo argument is. This copy's
# only consumer is the ALWAYS_BLOCK_PATTERNS loop below, which is why the
# opt-in is safe to make here: the ASK-tier copy deliberately does NOT enable
# them, because it also feeds two DENY-tier consumers whose subject IS a
# grep/sed command word (the SQL DDL scan and extract_write_targets()'s write
# confinement — see the consumer audit table at _POSITIONAL_MASK_NEVER above).
# Same gate, same floor: a shell segment anywhere, or `$(`/backtick inside the
# span, and nothing is redacted.
#
# ORDER (repo#434): this pass reads $COMMAND — the RAW command — and must keep
# doing so. Its query-sink vetoes (`sed -i`, `awk system(…)`, `rg --pre`, …) are
# text checks over the simple command they are classifying, so running
# strip_literal_text() first handed them a copy in which the veto token itself
# could already have been blanked: `sed -n --title "pass -i to edit" 's|<danger>|X|p'`
# had its `-i` masked to `X`s by the --title redaction, the `-i` veto therefore
# never fired, the sed was admitted as a query sink, and its program text — the
# real payload — was redacted out of the catastrophic scan. See
# strip_datasink_literals()'s "$1 MUST be the raw command" contract.
if [[ "$COMMAND" == *"echo"* || "$COMMAND" == *"printf"* || \
      "$COMMAND" == *"jq"* || "$COMMAND" == *"grep"* || "$COMMAND" == *"rg"* || \
      "$COMMAND" == *"sed"* || "$COMMAND" == *"awk"* ]] && \
   [[ "$(command_has_shell_segment "$COMMAND")" == "no" ]]; then
    COMMAND_NO_LITERAL_TEXT=$(strip_datasink_literals "$COMMAND" query)
fi
# PASS 2 — redact literal text, so a force-push-to-main phrase quoted inside a
# --body/-m/--title/--notes/--comment value no longer false-positives (#3679,
# --comment added #3756). The awk only runs when one of those flags is actually
# present, keeping it off the hot path (mirrors the COMMAND_NO_COMMENT
# `#`-present guard). `-c` is intentionally excluded so `bash -c '<payload>'`
# payloads still reach the raw scan; spans carrying `$(` / backtick are left
# intact so command-substitution smuggling still hard-denies.
#
# Running SECOND costs this pass nothing, because neither pass can hide input
# the other needs: both only ever rewrite the interior of a quoted span, both
# refuse any span carrying `$(`/backtick, and both replace a character with `X`
# — which can never synthesize a `-`, a quote, a `(` or a flag name. So pass 1
# can only ever shrink the set of spans pass 2 masks (never grow it), and the
# composed output is identical to the old order on every command where pass 1's
# vetoes were not being fooled. The gate still tests $COMMAND, so a flag name
# that pass 1 masked away (inside an echo argument) cannot skip this pass
# either — it just finds nothing left to redact there.
if [[ "$COMMAND" == *"--body"* || "$COMMAND" == *"--message"* || \
      "$COMMAND" == *"--title"* || "$COMMAND" == *"--notes"* || \
      "$COMMAND" == *"--comment"* || "$COMMAND" == *"-m"* || \
      "$COMMAND" == *"--search"* || "$COMMAND" == *"--arg"* ]]; then
    COMMAND_NO_LITERAL_TEXT=$(strip_literal_text "$COMMAND_NO_LITERAL_TEXT")
fi

# =============================================================================
# DEQUOTED CATASTROPHIC COPY (repo#197)
#
# The patterns above are literal command text, so quoting an argument used to
# defeat them outright: `rm -rf "/"` was ALLOWED while `rm -rf /` denied, and
# `git push --force origin "main"` fell through to a mere ask. Those are the
# same commands to the shell — the guard was enforcing a spelling, not a
# policy, and quoting a path is the ordinary thing to do.
#
# The fix is ORDER, not less redaction. This copy is derived from
# COMMAND_NO_LITERAL_TEXT, i.e. AFTER the sink-aware redaction above has already
# blanked the quoted values of --body/-m/--title/--notes/--comment and of
# echo/printf data sinks. So prose that merely quotes a dangerous command
# ("document rm -rf / hazard") is already inert before dequoting can see it,
# and stays inert. What dequoting exposes is only the quoting of an OPERATIVE
# argument, which is exactly what should be scanned.
#
# Spans containing $( or a backtick are left untouched, so command-substitution
# smuggling keeps hard-denying via the raw copy.
#
# Scanned IN ADDITION to the raw copy, never instead of it — dequoting changes
# byte offsets, so this copy is only ever fed to these pattern greps, never to
# target extraction.
# =============================================================================
COMMAND_DEQUOTED="$COMMAND_NO_LITERAL_TEXT"
if [[ "$COMMAND_NO_LITERAL_TEXT" == *'"'* || "$COMMAND_NO_LITERAL_TEXT" == *"'"* ]]; then
    COMMAND_DEQUOTED=$(dequote_inert_spans "$COMMAND_NO_LITERAL_TEXT")
fi

for pattern in "${ALWAYS_BLOCK_PATTERNS[@]}"; do
    if echo "$COMMAND_NO_LITERAL_TEXT" | grep -qiE "$pattern"; then
        deny "BLOCKED: Command matches dangerous pattern: $pattern" "catastrophic:$pattern"
    fi
    if [[ "$COMMAND_DEQUOTED" != "$COMMAND_NO_LITERAL_TEXT" ]] && \
       echo "$COMMAND_DEQUOTED" | grep -qiE "$pattern"; then
        deny "BLOCKED: Command matches dangerous pattern: $pattern (quoting an argument does not change what the shell runs)" "catastrophic-dequoted:$pattern"
    fi
done

# =============================================================================
# `gh pr/issue comment|edit --body @path` — literal-@ silent data loss
# (Loom #4523 comment, #4685 edit, #4601 shell-variable indirection; ported
# for repo#580).
#
# `gh ... --body @path` does NOT expand `@path` to the file's contents (unlike
# `gh api -F body=@path` or `--body-file path`): it posts the literal string
# `@path` as the comment / issue body. Never intentional, so this is an ungated
# hard deny, like the catastrophic tier above.
#
# DELIBERATELY scans the RAW $COMMAND, NOT COMMAND_NO_LITERAL_TEXT: the regex
# only inspects the character right after the --body/-b flag's opening quote,
# and strip_literal_text() would have blanked a quoted `"@/tmp/x"` to X's —
# silently disabling the quoted shape. The `@` must be followed by a
# path-shaped character (`/`, `.`, `~`); a bare `@reviewer ...` mention is
# prose and must not match (Loom #4577). `comment` and `edit` are separate
# regexes on purpose (Loom #4577/#4685 additive-not-widened precedent).
# =============================================================================
GH_COMMENT_BODY_AT_PATTERN="(^|[;&|[:space:]])gh[[:space:]]+(pr|issue)[[:space:]]+comment[^;&]*(-b|--body)[[:space:]]*=?[[:space:]]*[\"']?@[/.~]"
if echo "$COMMAND" | grep -qiE "$GH_COMMENT_BODY_AT_PATTERN"; then
    deny "BLOCKED: 'gh pr comment'/'gh issue comment --body @path' does NOT expand the file — it posts the literal string '@path' as the comment. Use --body \"\$(cat <<'EOF' ... EOF)\", -F/--body-file <path>, or 'gh api ... -F body=@<path>' instead." "gh-comment-body-literal-at"
fi
GH_EDIT_BODY_AT_PATTERN="(^|[;&|[:space:]])gh[[:space:]]+(pr|issue)[[:space:]]+edit[^;&]*(-b|--body)[[:space:]]*=?[[:space:]]*[\"']?@[/.~]"
if echo "$COMMAND" | grep -qiE "$GH_EDIT_BODY_AT_PATTERN"; then
    deny "BLOCKED: 'gh pr edit'/'gh issue edit --body @path' does NOT expand the file — it writes the literal string '@path' as the issue/PR body. Use --body \"\$(cat <<'EOF' ... EOF)\", -F/--body-file <path>, or 'gh api ... -F body=@<path>' instead." "gh-edit-body-literal-at"
fi

# Same loss through SHELL-VARIABLE INDIRECTION (Loom #4601): a variable
# assigned a path-shaped at-sign value, then passed as the body value.
# An unconditional deny on `--body "$VAR"` would be far too broad, so this
# CORRELATES: it denies only when the SAME command both assigns a path-shaped
# `@…` value to a variable and passes that variable as the --body/-b value.
# GH_AT_PATHISH requires real path shape (explicit `/`, `~/`, `./`, `../`
# prefix, or a text-file extension) so `@mention` / `@org/team` prose never
# matches. A variable assigned in an EARLIER Bash call is invisible to a single
# PreToolUse payload (known limit, by construction).
GH_AT_PATHISH="@((/|~/|\.\.?/)[^[:space:]\"';&|]*|[^[:space:]\"';&|]*\.(md|markdown|txt|text|log|json|ya?ml|diff|patch|out))"
# Both variable rules need a literal `@`; this bash-builtin prefilter keeps
# them off the hot path for nearly every command.
if [[ "$COMMAND" == *"@"* ]]; then
    for _gh_at_sub in comment edit; do
        if echo "$COMMAND" | grep -qiE "(^|[;&|[:space:]])gh[[:space:]]+(pr|issue)[[:space:]]+${_gh_at_sub}"; then
            _gh_at_path_vars=$(printf '%s\n' "$COMMAND" \
                | grep -oE "(^|[;&|(){}[:space:]])[A-Za-z_][A-Za-z0-9_]*=[\"']?$GH_AT_PATHISH" 2>/dev/null \
                | grep -oE "[A-Za-z_][A-Za-z0-9_]*=" 2>/dev/null \
                | tr -d '=' | sort -u)
            for _gh_at_var in $_gh_at_path_vars; do
                if echo "$COMMAND" | grep -qiE "(-b|--body)[[:space:]]*=?[[:space:]]*[\"']?[\$]\{?${_gh_at_var}(\}|[^A-Za-z0-9_]|\$)"; then
                    deny "BLOCKED: '\$${_gh_at_var}' is assigned a path-shaped '@<path>' value and passed as --body — 'gh ${_gh_at_sub}' does NOT expand '@path' from a variable either; it posts the literal string instead. Use --body-file <path>, 'gh api ... -F body=@<path>', or --body \"\$(cat <<'EOF' ... EOF)\"." "gh-${_gh_at_sub}-body-literal-at-var"
                fi
            done
        fi
    done
fi

# =============================================================================
# COMMENT-STRIPPED WORKING COPY - used ONLY for the ASK-word and SQL DDL/DML
# matches below, never for the catastrophic ALWAYS_BLOCK scan.
#
# Strips a `#…EOL` shell comment when the `#` is at start-of-line or preceded
# by whitespace (the common comment shape), so a pattern word that appears only
# in a trailing comment ("# drop database first", "# git push --force") no
# longer trips the ASK/DDL gates. This is best-effort: a `#` inside a quoted
# string that happens to be whitespace-preceded is also stripped, but since the
# stripped copy is used only for the *narrowing* ASK/DDL matches (never the
# catastrophic scan) the worst case is a missed ask on quoted data, never a
# missed catastrophic block. The sed only runs when a `#` is actually present,
# keeping it off the hot path (#3553).
# =============================================================================
if [[ "$COMMAND" == *"#"* ]]; then
    COMMAND_NO_COMMENT=$(printf '%s\n' "$COMMAND" | sed -E 's/(^|[[:space:]])#.*$//')
else
    COMMAND_NO_COMMENT="$COMMAND"
fi

# =============================================================================
# ASK-TIER WORKING COPY (#3756) — comment-stripped AND literal-text redacted.
#
# The ASK_PATTERNS loop below needs BOTH narrowings the catastrophic tier's two
# copies provide separately: COMMAND_NO_COMMENT's `#`-comment stripping AND
# strip_literal_text()'s quoted-flag-value redaction (the #3679 fix the ask tier
# never received). Building the ask copy from COMMAND_NO_COMMENT (not raw
# $COMMAND) preserves the comment-stripping the ask tier already relied on, then
# redacts --body/-m/--title/--notes/--comment values so an ask-phrase quoted
# inside such a value (e.g. `gh pr comment --body "…gh issue close…"`) no longer
# false-asks. The strip only runs when a text-carrying flag is present, keeping
# it off the hot path. Never feeds the catastrophic scan (that keeps reading the
# raw command), so this can only NARROW an ask, never miss a hard deny.
# =============================================================================
COMMAND_ASK_SCAN="$COMMAND_NO_COMMENT"
if [[ "$COMMAND_NO_COMMENT" == *"--body"* || "$COMMAND_NO_COMMENT" == *"--message"* || \
      "$COMMAND_NO_COMMENT" == *"--title"* || "$COMMAND_NO_COMMENT" == *"--notes"* || \
      "$COMMAND_NO_COMMENT" == *"--comment"* || "$COMMAND_NO_COMMENT" == *"-m"* || \
      "$COMMAND_NO_COMMENT" == *"--search"* || "$COMMAND_NO_COMMENT" == *"--arg"* ]]; then
    COMMAND_ASK_SCAN=$(strip_literal_text "$COMMAND_NO_COMMENT")
fi
# Mirror the catastrophic tier's data-sink redaction (#53): an ask-phrase quoted
# as inert echo/printf data (e.g. `echo 'run gh issue close 5 to clean up'`)
# should not false-ask. Same shell-segment gate keeps `echo '<phrase>' | sh`
# reaching the raw ask scan. Never feeds the catastrophic scan, so it can only
# NARROW an ask, never miss a hard deny.
if [[ "$COMMAND_NO_COMMENT" == *"echo"* || "$COMMAND_NO_COMMENT" == *"printf"* ]] && \
   [[ "$(command_has_shell_segment "$COMMAND_NO_COMMENT")" == "no" ]]; then
    COMMAND_ASK_SCAN=$(strip_datasink_literals "$COMMAND_ASK_SCAN")
fi
# Third narrowing: mask quoted POSITIONAL arguments of a repo-configured
# command allowlist (guards.positionalMaskAllowlist, #195) — the ASK-tier
# analog of the two named-flag/data-sink narrowings above, for tools whose
# free-text arguments are purely positional rather than behind --body/-m/
# echo (see mask_ask_positional_args()'s header comment, near
# strip_datasink_literals() above). Gated on a quote character being present
# at all (positional masking can only ever matter when there is a quoted
# argument to mask), which keeps the config read off the hot path for the
# many full-path commands that carry no quotes. positional_mask_cmdre() is
# itself cached and resolves to an empty string on the (default) absent/
# empty config, so this step is a true no-op on every repo that hasn't opted
# in.
#
# It never feeds the catastrophic scan. Note that NOT feeding the catastrophic
# scan is by itself NOT enough to guarantee "can only narrow an ask" (#195
# review): COMMAND_ASK_SCAN also feeds two DENY-tier consumers — the SQL DDL
# check below and the #4178 Bash-tool write-confinement block, which passes
# this very variable to extract_write_targets(). What actually preserves both
# denies is positional_mask_cmdre()'s mandatory _POSITIONAL_MASK_NEVER
# exclusion set (grep/egrep/fgrep/rg + cp/mv/tee/sed), which no operator
# config can override — see its consumer audit table.
if [[ "$COMMAND_NO_COMMENT" == *'"'* || "$COMMAND_NO_COMMENT" == *"'"* ]]; then
    _POSITIONAL_MASK_CMDRE="$(positional_mask_cmdre)"
    if [[ -n "$_POSITIONAL_MASK_CMDRE" ]]; then
        COMMAND_ASK_SCAN=$(mask_ask_positional_args "$COMMAND_ASK_SCAN" "$_POSITIONAL_MASK_CMDRE")
    fi
fi

# =============================================================================
# SYSTEM-LIFECYCLE + CLOUD-CLI DELETE (segment-parsed, command-word anchored)
#
# The system-lifecycle commands (halt/reboot/poweroff/shutdown/init 0/init 6)
# and the az/gcloud cloud-delete CLIs are far too common as ordinary prose,
# identifiers, and flag names to scan as unanchored substrings — and even a
# whitespace-inclusive boundary anchor still fired inside comments and commit
# messages ("...the box will halt", "...after a reboot event"). A pure regex
# tweak cannot separate `sudo halt` (a real command) from `will halt` (prose)
# because both are "<word> halt".
#
# So we segment-parse instead, mirroring extract_rm_targets(): split the command
# on ; | & && || and newline, strip a leading sudo/env wrapper from each segment,
# and deny only when a segment's *command word* (first token) is exactly a
# lifecycle word — or is `az`/`gcloud` with a `delete` subcommand token. This
# distinguishes `sudo halt` (command word = halt) from `will halt` (command word
# = echo/other) and from `--instance-initiated-shutdown-behavior` (not a command
# word at all). The scan runs against COMMAND_NO_COMMENT so a lifecycle/cloud
# word sitting in a trailing comment is already gone. The catastrophic
# ALWAYS_BLOCK scan above still reads the raw string for the symbolic patterns
# (rm -rf /, the fork bomb, curl|sh) that are not prose-prone (#3584).
# =============================================================================
lifecycle_or_cloud_reason() {
    # Emit a deny reason (one per line) for every segment whose command word is a
    # system-lifecycle command or an az/gcloud delete. Portable awk only.
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_ML_QSPLIT_AWK"'
    BEGIN { buf = "" }
    # Slurp the whole (possibly multi-line) command, then segment ONCE with the
    # shared quote-aware lexer (#71) so a multi-line quoted DATA literal whose
    # interior line is a lifecycle/cloud word (e.g. `halt`) is no longer mis-read
    # as a real segment (the pre-#71 per-record `qsplit()` reset quote state at
    # each embedded newline, hard-denying inert quoted prose).
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        n = ml_segment(buf, segs)
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/^sudo[ \t]+/, "", seg)
            # Strip a leading `env` wrapper, then loop-strip the env flags and
            # NAME=value assignments a shell resolves past before the command
            # word, so `env FOO=bar halt` resolves to command word `halt` (not
            # `FOO=bar`) and still denies. `env -i FOO=bar halt` and `env -u
            # NAME halt` likewise resolve to `halt`. A bare `env halt` (no
            # assignment) is unaffected — the loop matches nothing and leaves
            # `halt` as the command word. Portable awk only (no GNU/BSD-specific
            # escapes), consistent with extract_rm_targets(). (#3586)
            if (sub(/^env([ \t]+|$)/, "", seg)) {
                sub(/^[ \t]+/, "", seg)
                stripped = 1
                while (stripped) {
                    stripped = 0
                    if (sub(/^-u[ \t]+[^ \t]+([ \t]+|$)/, "", seg)) { stripped = 1; continue }
                    if (sub(/^-i([ \t]+|$)/, "", seg))              { stripped = 1; continue }
                    if (sub(/^--([ \t]+|$)/, "", seg))              { break }
                    if (sub(/^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*([ \t]+|$)/, "", seg)) { stripped = 1; continue }
                }
            }
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            cmd = toks[1]
            if (cmd == "halt" || cmd == "reboot" || cmd == "poweroff" || cmd == "shutdown") {
                print "system lifecycle command: " cmd
                continue
            }
            if (cmd == "init" && (toks[2] == "0" || toks[2] == "6")) {
                print "system lifecycle command: init " toks[2]
                continue
            }
            if (cmd == "az" || cmd == "gcloud") {
                for (j = 2; j <= m; j++) {
                    if (toks[j] == "delete") {
                        print "cloud resource deletion: " cmd " delete"
                        break
                    }
                }
            }
        }
    }'
}

# Lifecycle denies are unconditional. The az/gcloud delete denies are gated by
# the cloud-CLI toggle (Repo Skills refinement): for a repo whose job IS
# managing cloud infra, `az`/`gcloud … delete` is first-class teardown, so
# guards.cloudCli:false / REPO_GUARD_CLOUD=0 downgrades those denies to allow.
# Every emitted reason is inspected (not just the first) so a skipped cloud
# reason can never mask a lifecycle deny later in the same command.
while IFS= read -r _lifecycle_reason; do
    [[ -z "$_lifecycle_reason" ]] && continue
    if [[ "$_lifecycle_reason" == "cloud resource deletion:"* ]]; then
        cloud_guard_enabled && deny "BLOCKED: $_lifecycle_reason" "lifecycle-or-cloud-delete"
    else
        deny "BLOCKED: $_lifecycle_reason" "lifecycle-or-cloud-delete"
    fi
done < <(lifecycle_or_cloud_reason "$COMMAND_NO_COMMENT")

# =============================================================================
# DATABASE DESTRUCTION - Gated by the SQL DDL/DML guard toggle
#
# Kept separate from ALWAYS_BLOCK_PATTERNS so DB-engine repos can opt out
# (guards.sqlDdl:false / LOOM_GUARD_SQL=0). A single alternation grep matches
# all four DDL statements in one pass (cheaper than a per-pattern loop), and
# sql_guard_enabled() is consulted only after a match, so the config read stays
# off the hot path.
# =============================================================================
#
# Scanned against COMMAND_ASK_SCAN — the comment-stripped, literal-text-redacted
# working copy — NOT the raw COMMAND_NO_COMMENT (repo#188 parity fix). A DDL
# phrase quoted inside a `--body`/`-m`/`--title` value is prose *about* a
# destructive statement, not a destructive statement, and denying it blocks
# ordinary work: filing the issue that describes the hazard, or committing the
# migration note that mentions it. This guard's own repository trips it — a
# `grep` for the phrase, and this very comment, both used to deny. Loom's
# vendored copy has always scanned the redacted copy here; the raw scan was the
# single largest source of behavioral divergence between the two guards.
SQL_DDL_PATTERN='DROP DATABASE|DROP TABLE|DROP SCHEMA|TRUNCATE TABLE'
if echo "$COMMAND_ASK_SCAN" | grep -qiE "$SQL_DDL_PATTERN" && sql_guard_enabled; then
    matched=$(echo "$COMMAND_ASK_SCAN" | grep -oiE "$SQL_DDL_PATTERN" | head -1)
    deny "BLOCKED: Command matches dangerous pattern: ${matched:-SQL DDL statement}" "sql-ddl"
fi

# =============================================================================
# rm -rf SCOPE CHECK - Block rm with recursive/force flags on protected paths
#
# Only *actual local* `rm` command words are inspected. `extract_rm_targets`
# splits the command on ; | & && || and, for each simple-command segment whose
# command word is `rm` (optionally sudo-prefixed) — OR a command-word
# *substitution* `$(...)`/backtick in executable position (#72) — AND which
# carries a recursive/force flag, emits the non-flag argument tokens.
# Consequences (#3553):
#   - A token from an earlier command in the same line (e.g. the `host-ip.txt`
#     in `HOST=$(cat host-ip.txt); ssh $HOST rm -rf …`) is never mis-read as an
#     rm target — only tokens of a real `rm` segment are considered.
#   - An `rm` inside a remote payload (`ssh host 'rm -rf /home/ubuntu/foo'`) is
#     NOT treated as a local rm: the wrapper's command word is `ssh`/`scp`, not
#     `rm`, so no local target is emitted and the local scope check is skipped.
#     The ALWAYS_BLOCK catastrophic patterns above still scan the whole string,
#     so a remote or quoted `rm -rf /` still denies.
#   - Only root, the user's $HOME, and *top-level* directories (/tmp, /var, /etc,
#     /usr, /home, /opt, /bin, …) are blocked. A scoped subpath such as
#     `rm -rf /tmp/whatever` or `rm -rf /var/foo` is allowed — the guard stops
#     obliteration of a whole system/root directory, not cleanup of a subpath.
# =============================================================================

extract_rm_targets() {
    # Emit one rm-target token per line for every local `rm -r/-f` invocation.
    # Portable awk only (no GNU/BSD-specific escapes).
    #
    # MULTI-LINE QUOTE AWARENESS (#60): slurp the whole (possibly multi-line)
    # command into ONE buffer, then segment ONCE with a quote-aware walk — instead
    # of the old per-awk-record `$0 = qsplit($0)`, whose quote-tracking reset at
    # every input newline because awk's default RS split the command into separate
    # records. That per-record form false-blocked a multi-line quoted DATA literal
    # (echo/printf/--body) whose interior line merely BEGINS with `rm -rf /`: the
    # interior line was scanned as its own top-level segment with no memory that it
    # is still inside an open quote from a prior line, so its command word resolved
    # to a real `rm` and its lone `/` token hit the protected-root deny. Mirrors
    # the buffer-slurp the catastrophic/ask redactors (strip_datasink_literals /
    # strip_literal_text) and command_has_shell_segment() already use.
    #
    # Segmentation is delegated to the shared ml_segment() lexer (#71,
    # _ML_QSPLIT_AWK) rather than reusing qsplit()+split("\n"), because qsplit()
    # emits a `\n` for each real separator while ALSO leaving a literal newline
    # that lived inside an inert quoted span untouched — the two are then
    # indistinguishable to a downstream `split(s, segs, "\n")`, which is exactly
    # why the naive slurp-then-qsplit would still re-split the quoted `rm -rf /`
    # line into its own segment. ml_segment() walks the buffer once so an inert
    # quoted span's embedded newlines never become segment boundaries. PR #69
    # introduced this lexer inline here; #71 extracted it into the shared helper
    # so parse_force_ops()/lifecycle_or_cloud_reason() reuse the SAME algorithm
    # instead of duplicating it (see the _ML_QSPLIT_AWK header for the full
    # segmentation contract).
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_ML_QSPLIT_AWK"'
    BEGIN { buf = "" }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        segc = ml_segment(buf, segs)
        for (si = 1; si <= segc; si++) {
            seg = segs[si]
            sub(/^[ \t]+/, "", seg)
            # Strip a leading run of VAR=val assignments and sudo/env wrappers
            # (in any order/repetition) so the REAL command word — a literal `rm`
            # OR a command-word substitution — is what we classify. `env` is
            # stripped alongside `sudo` (#72): `env $(which rm) -rf /` and
            # `env rm -rf /` must be seen as an rm command word, not shielded
            # behind the wrapper. The required trailing [ \t]+ in each sub()
            # guarantees the while loop makes progress and terminates.
            while (sub(/^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*[ \t]+/, "", seg) || \
                   sub(/^sudo[ \t]+/, "", seg) || \
                   sub(/^env[ \t]+/, "", seg)) { }
            sub(/^[ \t]+/, "", seg)
            # Determine the argument tail and the token index to start scanning
            # from. Two command-word shapes emit targets:
            #   (a) a literal `rm` command word — scan from toks[2] (skip "rm").
            #   (b) a command-word *substitution* — `$(...)` or a backtick pair —
            #       in executable position (#72). The substitution result becomes
            #       the command word at run time, so `$(which rm) -rf /` never
            #       presents a literal `rm` token yet is exactly as dangerous. We
            #       deliberately do NOT try to resolve what the substitution names
            #       (`which rm`, `command -v rm`, an alias, a PATH-relative rm, … —
            #       unbounded and trivially bypassable); we key on the SHAPE
            #       (substitution in command-word position + a recursive/force
            #       flag + a protected-path target) and let the downstream
            #       protected-path check decide. A benign `$(which ls) -la /tmp`
            #       carries no recursive/force flag, so it emits no target and
            #       stays allowed — no blanket deny on command-word substitutions.
            tail = ""
            start = 0
            if (seg ~ /^rm([ \t]|$)/) {
                tail = seg
                start = 2
            } else if (substr(seg, 1, 2) == "$(") {
                # Balanced-paren skip past the substitution so an internal space
                # (e.g. `$(command -v rm)`) is NOT mis-split into a bogus
                # flag/target token. Start depth at 1 for the opening `(`.
                depth = 1
                p = 3
                L = length(seg)
                while (p <= L && depth > 0) {
                    ch = substr(seg, p, 1)
                    if (ch == "(") depth++
                    else if (ch == ")") depth--
                    p++
                }
                if (depth != 0) continue   # unterminated: emit nothing (conservative)
                tail = substr(seg, p)
                sub(/^[ \t]+/, "", tail)
                start = 1
            } else if (substr(seg, 1, 1) == "`") {
                # Backtick command word: skip to the closing backtick.
                p = 2
                L = length(seg)
                while (p <= L && substr(seg, p, 1) != "`") p++
                if (p > L) continue        # unterminated: emit nothing
                p++                         # step past the closing backtick
                tail = substr(seg, p)
                sub(/^[ \t]+/, "", tail)
                start = 1
            } else {
                continue
            }
            m = split(tail, toks, /[ \t]+/)
            has_rf = 0
            for (j = start; j <= m; j++)
                if (toks[j] ~ /^-/ && toks[j] ~ /[rRfF]/) has_rf = 1
            if (!has_rf) continue
            for (j = start; j <= m; j++) {
                if (toks[j] == "") continue
                if (toks[j] ~ /^-/) continue
                print toks[j]
            }
        }
    }'
}

normalize_abs_path() {
    # Lexically normalize an ABSOLUTE path without touching the filesystem:
    #   - collapse duplicate slashes    (//etc        -> /etc)
    #   - drop "." segments             (/usr/./      -> /usr)
    #   - resolve ".." segments         (/tmp/..      -> /,   /tmp/../etc -> /etc)
    #   - ".." at or above root stays at root (/a/../../../etc -> /etc)
    #   - strip trailing slash except bare root (/tmp/ -> /tmp)
    # Pure-bash and portable: `realpath -m` is GNU-only and silently no-ops on
    # macOS, so this MUST NOT rely on it. Without this normalization any
    # `..`/`//`/`.` traversal (e.g. `rm -rf /tmp/..` -> `/`) would slip past the
    # protected-path check below and wrongly ALLOW root/system-dir deletion.
    local path="$1"
    local seg
    local -a parts=() out=()
    local oldIFS="$IFS"
    IFS='/'
    read -r -a parts <<< "$path"
    IFS="$oldIFS"
    for seg in "${parts[@]}"; do
        case "$seg" in
            ''|'.')
                : ;;                                    # skip empties (// or leading /) and "."
            '..')
                if [[ ${#out[@]} -gt 0 ]]; then
                    out=("${out[@]:0:$(( ${#out[@]} - 1 ))}")   # pop last segment
                fi
                ;;                                       # ".." at/above root: stay at root
            *)
                out+=("$seg") ;;
        esac
    done
    if [[ ${#out[@]} -eq 0 ]]; then
        printf '/'
    else
        printf '/%s' "${out[@]}"
    fi
}

# =============================================================================
# _rm_scope_in_scope() — is an ABSOLUTE, already-normalized path inside the
# guards.rmScope=repo containment area (repo root, worktree areas, or the
# built-in ephemeral allowlist)?
#
# Factored out of the rm-scope target loop below (#239) so the SAME
# containment test can be applied both to a target's fully-resolved ABS_PATH
# AND to the statically-known prefix of a target whose path root is an
# unexpanded shell variable — one definition of "in scope" so the two call
# sites cannot drift apart. Only called from inside `rm_scope_repo_enabled`
# branches; REPO_ROOT/_WT_ROOT are the script-global values resolved earlier.
# =============================================================================
_rm_scope_in_scope() {
    local path="$1"
    [[ -n "$path" ]] || return 1

    if [[ -n "$REPO_ROOT" ]]; then
        if [[ "$path" == "$REPO_ROOT" || "$path" == "$REPO_ROOT"/* ]]; then
            return 0
        fi
        # The default in-repo worktrees dir is always in scope, even when an
        # external worktree.root / LOOM_WORKTREE_ROOT is set.
        if [[ "$path" == "$REPO_ROOT/.loom/worktrees" || "$path" == "$REPO_ROOT/.loom/worktrees"/* ]]; then
            return 0
        fi
        # Configured/overridden worktree root (external volumes).
        if [[ -z "${_WT_ROOT+x}" ]]; then
            _WT_ROOT=$(resolve_worktree_root "$REPO_ROOT")
        fi
        if [[ -n "$_WT_ROOT" ]] && { [[ "$path" == "$_WT_ROOT" || "$path" == "$_WT_ROOT"/* ]]; }; then
            return 0
        fi
    fi

    # Built-in ephemeral allowlist: system temp roots + the Claude scratchpad.
    # normalize_abs_path() is LEXICAL — it does NOT resolve symlinks — so on
    # macOS both the symlink form (/tmp, /var/tmp, /var/folders) AND its
    # /private target must be listed.
    case "$path" in
        /tmp/*|/private/tmp/*|\
        /var/tmp/*|/private/var/tmp/*|\
        /var/folders/*|/private/var/folders/*|\
        */claude-*/*/scratchpad/*)
            return 0 ;;
    esac

    return 1
}

# =============================================================================
# rm-scope SESSION-OWNED SCRATCH + SAME-COMMAND RESOLUTION (rjwalters/repo#581,
# part of #579; behavior group ported from Loom's vendored
# guard-destructive-generic.sh at 2072f82b, #8460 / #6520 / #6676 / #6805 /
# #7986 / #8221 / #9331).
#
# Under guards.rmScope=repo a target outside the repo/worktree/tmp areas is
# denied (`rm-scope-outside-repo`) and an unexpanded `$VAR` root is denied
# (`rm-scope-unresolved-var`). Narrow, PROOF-BASED allowances are added; none
# of them can reach the unconditional catastrophic floor (/, $HOME,
# /<one-segment>), which always runs first:
#
#   1. rm_scope_session_scratch_admits — `<scratch-root>/<this session id>` (and
#      paths under it) when the directory is a real non-symlink dir holding a
#      regular `.loom-session-scratch` marker whose `session=` line names this
#      session. The session id comes from the hook's own STDIN (`session_id`),
#      which the acting model cannot influence. Anything unprovable fails CLOSED.
#   2. rm_scope_mktemp_same_command_safe — a bare `$NAME` target whose ONLY
#      binding in the same command is `NAME=$(mktemp -d)` / `NAME=$(mktemp)`
#      (optionally followed by the exact `NAME=$(realpath "$NAME")` chain).
#   3. rm_scope_literal_same_command_resolve — `$NAME[/literal-suffix]` whose
#      only binding is `NAME=<absolute literal>`; the resolved path is then
#      judged exactly like a literal `rm -rf` target (never skips the floor).
#   4. _rm_scope_bare_var_name / _rm_scope_var_ref_split — parsers for 2/3.
#
# DIFFERENCES FROM LOOM (intentional):
#   - REPO_GUARD_SCRATCH_ROOT wins over LOOM_GUARD_SCRATCH_ROOT; config key
#     guards.scratchRoot is read via guard_cfg() (repo config wins over
#     legacy .loom). The default root stays $HOME/.cache/loom/session-scratch
#     so existing recipes keep working.
#   - The Loom heredoc-substitution-span rescan (#8217 provenance mark) is a
#     separate behavior group and is NOT ported; this guard's rm scan reads
#     $COMMAND as before.
#   - Scans use this file's whole-buffer ml_segment() lexer rather than
#     Loom's per-record qsplit(), so multi-line quoted text is not mis-split.
# =============================================================================

# Ownership marker file name. This guard only ever READS it.
SESSION_SCRATCH_MARKER=".loom-session-scratch"

_SCRATCH_ROOT_DONE=""
_SCRATCH_ROOT_CACHE=""
resolve_scratch_root() {
    if [[ -z "$_SCRATCH_ROOT_DONE" ]]; then
        _SCRATCH_ROOT_DONE=yes
        local root="" bad=0 cfgv=""
        # env (REPO_* beats legacy LOOM_*) -> config -> default.
        if [[ -n "${REPO_GUARD_SCRATCH_ROOT:-}" ]]; then
            root="$REPO_GUARD_SCRATCH_ROOT"
        elif [[ -n "${LOOM_GUARD_SCRATCH_ROOT:-}" ]]; then
            root="$LOOM_GUARD_SCRATCH_ROOT"
        else
            cfgv=$(guard_cfg scratchRoot) || cfgv="unset"
            [[ "$cfgv" == "unset" ]] || root="$cfgv"
        fi
        if [[ -z "$root" && -n "${HOME:-}" ]]; then
            root="$HOME/.cache/loom/session-scratch"
        fi
        # Absolute + lexically normalized; a relative value is dropped.
        if [[ "$root" == /* ]]; then
            root=$(normalize_abs_path "$root")
        else
            root=""
        fi
        # ROOT SANITY SCREEN: a misconfigured root makes the carve-out inert,
        # never broad (/, $HOME, /<one-segment>, the repo root or an ancestor).
        if [[ -n "$root" ]]; then
            if [[ "$root" == "/" ]]; then
                bad=1
            elif [[ -n "${HOME:-}" && "$root" == "$HOME" ]]; then
                bad=1
            elif [[ "$root" =~ ^/[^/]+$ ]]; then
                bad=1
            elif [[ -n "$REPO_ROOT" ]] && \
                 { [[ "$root" == "$REPO_ROOT" ]] || [[ "$REPO_ROOT" == "$root"/* ]]; }; then
                bad=1
            fi
            if [[ "$bad" == 1 ]]; then
                log_hook_error "scratch root rejected as unsafe; session-scratch carve-out inert: $root"
                root=""
            fi
        fi
        _SCRATCH_ROOT_CACHE="$root"
    fi
    [[ -n "$_SCRATCH_ROOT_CACHE" ]] || return 1
    printf '%s' "$_SCRATCH_ROOT_CACHE"
}

# The acting session's id, straight off this hook's stdin. Returns 1 (carve-out
# inert) when absent or not a plausible single path segment.
_SCRATCH_SID_DONE=""
_SCRATCH_SID_CACHE=""
current_session_id() {
    if [[ -z "$_SCRATCH_SID_DONE" ]]; then
        _SCRATCH_SID_DONE=yes
        local sid=""
        sid=$(echo "$INPUT" | jq -r '.session_id // empty' 2>/dev/null) || sid=""
        if ! [[ "$sid" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$ ]]; then
            sid=""
        fi
        _SCRATCH_SID_CACHE="$sid"
    fi
    [[ -n "$_SCRATCH_SID_CACHE" ]] || return 1
    printf '%s' "$_SCRATCH_SID_CACHE"
}

# Return 0 ONLY when $1 (a normalized absolute rm target) is this session's own
# scratch directory or something under it. $2 is the target's RAW absolute
# spelling (before normalize_abs_path). Every failure path returns 1.
rm_scope_session_scratch_admits() {
    local abs="$1" raw="${2-}"
    local root sid dir marker recorded pdir pabs
    # (0) No `..` in the raw spelling: normalize_abs_path pops `..` lexically
    # but the kernel resolves it physically (`<dir>/tunnel/../x`).
    [[ -n "$raw" ]] || return 1
    if [[ "/$raw/" == */../* ]]; then
        return 1
    fi
    root=$(resolve_scratch_root) || return 1
    sid=$(current_session_id) || return 1
    dir="$root/$sid"
    # (3) The session dir itself or a descendant; never the root or a sibling.
    if [[ "$abs" != "$dir" && "$abs" != "$dir"/* ]]; then
        return 1
    fi
    # (4) A real directory, not a symlink planted at <root>/<id>.
    if [[ ! -d "$dir" ]] || [[ -L "$dir" ]]; then
        return 1
    fi
    # (5) Ownership marker: regular non-symlink file naming THIS session.
    marker="$dir/$SESSION_SCRATCH_MARKER"
    if [[ ! -f "$marker" ]] || [[ -L "$marker" ]]; then
        return 1
    fi
    recorded=$(head -c 8192 "$marker" 2>/dev/null | sed -n 's/^session=//p' | head -1) || recorded=""
    if [[ -z "$recorded" || "$recorded" != "$sid" ]]; then
        return 1
    fi
    # (6) Symlink-traversal defense: re-check containment on physical paths,
    # using the session dir's own physical form as the base.
    pdir=$(physical_abs_path "$dir" 2>/dev/null) || pdir=""
    pabs=$(physical_abs_path "$abs" 2>/dev/null) || pabs=""
    if [[ -z "$pdir" || -z "$pabs" ]]; then
        return 1
    fi
    if [[ "$pabs" != "$pdir" && "$pabs" != "$pdir"/* ]]; then
        return 1
    fi
    return 0
}

# --- Self-referential canonicalization chain (#7986). -------------------------
# Masks the EXACT `$(realpath "$NAME")` text into one opaque separator-free
# token so the segment scan sees `NAME=<token>` as the (single permitted)
# second assignment. Refuses (fail closed) if the token bytes already occur in
# the command text. Only `realpath` is admitted -- never `$(cd "$NAME" && pwd
# -P)`, because `cd ""` succeeds on some shells and would print the cwd.
_MKTEMP_CANON_TOKEN=$'\001LOOM_MKTEMP_CANON\001'
_MKTEMP_CANON_MASKED=""
_mktemp_canon_mask() {
    local varname="$1" cmdtext="$2" needle_rp masked
    _MKTEMP_CANON_MASKED=""
    case "$cmdtext" in
        *"$_MKTEMP_CANON_TOKEN"*) return 1 ;;
    esac
    needle_rp='$(realpath "$'"$varname"'")'
    masked="$cmdtext"
    # The needle is pattern-METACHARACTER-FREE by construction (varname is
    # [A-Za-z_][A-Za-z0-9_]*, and the literal contains no *, ?, [ or \), so it
    # is spelled UNQUOTED in the pattern position on purpose: a metachar-free
    # pattern means the same thing to bash 3.2 (macOS stock) and to bash 5,
    # with no version-dependent question about how the quote characters
    # inside a QUOTED pattern are handled.
    masked="${masked//$needle_rp/$_MKTEMP_CANON_TOKEN}"
    _MKTEMP_CANON_MASKED="$masked"
    return 0
}

# --- Same-command REBINDING recognizer (#8221, widened by #9331). ------------
# A `NAME=` prefix test alone misses `NAME+=`, `NAME[i]=`, an empty `NAME=`,
# decl-keyword assignments, read/mapfile/readarray/getopts/unset/printf -v/
# for/select naming NAME, and eval/source (name-agnostic). Each of those
# poisons the single-binding proof (fail closed): `d=/tmp/a; d+=/../../etc;
# rm -rf "$d"` must never be allowed. Shared by the two resolvers below.
_MKTEMP_REBIND_AWK='
function _mktemp_strip_decl_kw(seg,   out) {
    out = seg
    if (out ~ /^(export|readonly|declare|typeset|local)[ \t]/) {
        sub(/^(export|readonly|declare|typeset|local)[ \t]+/, "", out)
        while (out ~ /^-/) {
            if (!sub(/^-[^ \t]*[ \t]*/, "", out)) break
        }
    }
    return out
}
function _mktemp_strip_cmd_prefix(seg,   out) {
    out = seg
    while (1) {
        if (sub(/^[A-Za-z_][A-Za-z0-9_]*(\[[^]]*\])?\+?=[^ \t]*[ \t]+/, "", out)) continue
        if (sub(/^(command|builtin|exec)[ \t]+/, "", out)) continue
        break
    }
    return out
}
function _mktemp_is_other_rebind(seg, varname,   n, i, toks, tok, bseg, cseg) {
    bseg = _mktemp_strip_decl_kw(seg)
    cseg = _mktemp_strip_cmd_prefix(bseg)
    if (bseg ~ ("^" varname "(=|\\+=|\\[)")) return 1
    if (cseg ~ ("^" varname "(=|\\+=|\\[)")) return 1
    if (cseg ~ /^(eval|source|\.)([ \t]|$)/) return 1
    if (cseg ~ /^printf([ \t]|$)/ && cseg ~ ("(^|[ \t])-v[ \t]*" varname "([ \t]|$)")) return 1
    if (cseg ~ ("^(for|select)[ \t]+" varname "[ \t]+in([ \t]|$)")) return 1
    if (cseg ~ /^(read|mapfile|readarray|getopts|unset)([ \t]|$)/) {
        n = split(cseg, toks, /[ \t]+/)
        for (i = 2; i <= n; i++) {
            # Whitespace splitting alone leaves the bound name GLUED to what
            # follows it, and both glues are ordinary spellings that hid the
            # rebinding completely: a redirection (`read d</tmp/list`,
            # `read -r d<<<"$x"` -- token `d</tmp/list`) and an array
            # subscript (`unset d[0]` -- token `d[0]`). Reduce each token to
            # the NAME it binds before comparing. Both subs only ever widen
            # what counts as a rebinding, so both fail closed.
            tok = toks[i]
            sub(/[<>].*$/, "", tok)
            sub(/\[[^]]*\]$/, "", tok)
            if (tok == varname) return 1
        }
    }
    return 0
}
'

# --- Same-command ORDER + CONTEXT proof (repo#588 review, P1). -----------------
# Counting bindings across the whole buffer is not enough: a binding proves the
# rm target only if it is GUARANTEED to have executed, in THIS shell, BEFORE the
# rm word expands. Both of these were admitted while the inherited value of `d`
# is what the rm actually receives:
#     rm -rf "$d"; d=/tmp/safe            (assignment AFTER the rm)
#     false && d=/tmp/safe; rm -rf "$d"   (assignment SKIPPED)
# These helpers require, on top of the single-binding count (fail closed on
# anything unprovable):
#   1. every reference to NAME sits in a segment AFTER the binding segment;
#   2. the binding is reached unconditionally: every separator between it and
#      the previous non-blank segment is `;` or an unescaped newline (never
#      `&&`, `||`, `|`, `&`), and no control-flow or grouping construct opens
#      before it (if/then/else/while/until/do/for/select/case/function/coproc,
#      `(`, `{`, `!`, `NAME()` function definitions);
#   3. it runs in the current shell: top level (not inside `$( )`/backticks),
#      and not the left side of a pipeline `|` or backgrounded with `&`;
#   4. no OTHER assignment-shaped occurrence of NAME exists anywhere (`NAME=`,
#      `NAME+=`, `NAME[` at a word boundary, `${NAME:=...}`), so a rebinding
#      buried in `then NAME=...`, `f() { NAME=...; }`, a case arm or a quoted
#      trap string still poisons the proof.
# Requires ml_segment() to have just filled ml_segbeg[] / ml_segsep[].
_SAMECMD_ORDER_AWK='
function samecmd_trim(t) {
    sub(/^[ \t\n]+/, "", t)
    sub(/[ \t\n]+$/, "", t)
    return t
}
function samecmd_assign_like(seg, varname) {
    if (seg ~ ("(^|[^A-Za-z0-9_$])" varname "(\\+?=|\\[)")) return 1
    if (seg ~ ("\\$\\{" varname ":?[=?]")) return 1
    return 0
}
function samecmd_refers(seg, varname) {
    if (seg ~ ("\\$" varname "([^A-Za-z0-9_]|$)")) return 1
    if (seg ~ ("\\$\\{[#!]?" varname "([^A-Za-z0-9_]|$)")) return 1
    return 0
}
function samecmd_opens_compound(t) {
    t = samecmd_trim(t)
    if (t ~ /^[({!]/) return 1
    if (t ~ /^(if|then|elif|else|fi|while|until|do|done|for|select|case|esac|function|coproc)([ \t;]|$)/) return 1
    if (t ~ /^[A-Za-z_][A-Za-z0-9_:.-]*[ \t]*\(\)/) return 1
    return 0
}
# 1 when segment k (a NAME=... binding) is unconditional, top level, current
# shell, and preceded by no control-flow construct. buf is the exact text
# ml_segment() just segmented into segs[1..n].
function samecmd_binding_ctx_ok(buf, segs, n, k,   blen, j, p, c, nc, pos, sdep, t) {
    if (k < 1 || k > n) return 0
    blen = length(buf)
    # (3a) top level: not inside a command substitution.
    subst_depth(buf, sdep)
    pos = ml_segbeg[k]
    while (pos <= blen && substr(buf, pos, 1) ~ /[ \t\n]/) pos++
    if (pos <= blen && (sdep[pos] + 0) > 0) return 0
    # (2a) nothing before it opens control flow or a grouping/subshell.
    for (j = 1; j < k; j++) {
        if (samecmd_opens_compound(segs[j])) return 0
    }
    # (2b) separators back to the previous non-blank, non-comment segment.
    for (j = k - 1; j >= 1; j--) {
        p = ml_segsep[j]
        c = substr(buf, p, 1)
        if (c == "&" || c == "|") return 0
        if (c == "\n" && bs_escaped(buf, p)) return 0
        if (c != ";" && c != "\n") return 0
        t = samecmd_trim(segs[j])
        # A blank segment (between `&&` and a newline) or a comment-only one
        # (`false && # note` + newline continues the list) carries the
        # previous separator forward; keep walking back.
        if (t != "" && t !~ /^#/) break
    }
    # (3b) the separator AFTER it must not pipe or background the binding.
    p = ml_segsep[k]
    if (p <= blen) {
        c = substr(buf, p, 1)
        nc = (p < blen) ? substr(buf, p + 1, 1) : ""
        if (c == "|" && nc != "|") return 0
        if (c == "&" && nc != "&") return 0
        if (c == ";" && (nc == ";" || nc == "&")) return 0
        if (c == "\n" && bs_escaped(buf, p)) return 0
    }
    return 1
}
# Index of the first segment that references NAME, 0 when none does.
function samecmd_first_ref(segs, n, varname,   i) {
    for (i = 1; i <= n; i++) {
        if (samecmd_refers(segs[i], varname)) return i
    }
    return 0
}
'

# _rm_scope_bare_var_name TOKEN -- print NAME when TOKEN is exactly `$NAME`/`${NAME}`
# (one optional quote layer), else return 1.
_rm_scope_bare_var_name() {
    local tok="$1" t c1 c2
    t="$tok"
    if [[ ${#t} -ge 2 ]]; then
        c1="${t:0:1}"
        c2="${t: -1}"
        if [[ ("$c1" == '"' && "$c2" == '"') || ("$c1" == "'" && "$c2" == "'") ]]; then
            t="${t:1:${#t}-2}"
        fi
    fi
    if [[ "$t" =~ ^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
        return 0
    fi
    if [[ "$t" =~ ^\$([A-Za-z_][A-Za-z0-9_]*)$ ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
        return 0
    fi
    return 1
}

# rm_scope_mktemp_same_command_safe TARGET CMDTEXT -- success only when TARGET is
# a BARE `$NAME`/`${NAME}` (one optional quote layer) and CMDTEXT (heredoc-body
# masked by the caller) binds NAME exactly once, to the plain `$(mktemp -d)` /
# `$(mktemp)` forms (optionally followed by the #7986 realpath chain). A custom
# template/--tmpdir never matches, so it falls through to the fail-closed deny.
rm_scope_mktemp_same_command_safe() {
    local target="$1" cmdtext="$2" varname
    varname=$(_rm_scope_bare_var_name "$target") || return 1
    [[ -n "$varname" ]] || return 1
    mktemp_same_command_bound "$varname" "$cmdtext"
}

# mktemp_same_command_bound VARNAME CMDTEXT -- the shared same-command mktemp
# PROOF behind both fast paths (rm scope above, #581; write confinement below,
# #582 / Loom #6949). Success only when CMDTEXT (heredoc-body masked by the
# caller) binds VARNAME exactly once, to the plain `$(mktemp -d)` / `$(mktemp)`
# forms (optionally followed by the exact #7986 realpath chain), with no other
# rebinding of any kind (#8221/#9331), and that binding is PROVEN to run, in
# this shell, before any reference to VARNAME (repo#588). One definition, so the
# rm and write consumers can never drift apart on the shape they admit. Every
# failure path returns 1; callers invoke it only in a condition context.
mktemp_same_command_bound() {
    local varname="$1" cmdtext="$2" tmplmode="${3:-}" tmpdir_ok=0 verdict
    [[ "$varname" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || return 1
    # repo#597: the write-confinement consumer passes "template" to also admit a
    # literal `/tmp/` or `$TMPDIR/` template prefix (and `-t`). `$TMPDIR/` is
    # admitted only when TMPDIR is set, absolute and not itself relative-ish;
    # the caller separately proves it lies outside the protected area.
    # "template-dir" (the `cd $NAME` consumer) is "template" plus a hard
    # requirement that the binding is a DIRECTORY (`-d`): without `-d` mktemp
    # creates a FILE, `cd` into it fails, and a following relative write would
    # land in the original cwd.
    if [[ "$tmplmode" == template* && -n "${TMPDIR:-}" && "$TMPDIR" == /* ]]; then
        tmpdir_ok=1
    fi
    # #7986: mask the self-referential canonicalization chain (if any) into an
    # opaque, separator-free token BEFORE the segment scan — see
    # _mktemp_canon_mask()'s doc comment above. A refusal (the token's bytes
    # already occur in the command text) fails closed.
    _mktemp_canon_mask "$varname" "$cmdtext" || return 1
    verdict=$(printf '%s' "$_MKTEMP_CANON_MASKED" | awk -v varname="$varname" -v canontok="$_MKTEMP_CANON_TOKEN" -v tmplmode="$tmplmode" -v tmpdir_ok="$tmpdir_ok" "$_ESCAPE_AWK""$_HASLIVESUBST_AWK""$_ML_QSPLIT_AWK""$_MKTEMP_REBIND_AWK""$_SAMECMD_ORDER_AWK"'
    # repo#597: a `$(mktemp [-d] [-t] <template>)` RHS whose template is a
    # literal `/tmp/<name>XXX` (or `$TMPDIR/<name>XXX` with TMPDIR proven by
    # the caller, or a bare `-t <name>XXX`). Any other option (--tmpdir=,
    # -p, -u, ...), quoting, expansion, directory or extra word refuses.
    function _mktemp_template_rhs_ok(rhs,   inner, n, toks, i, t, nd, nt, ntmpl, tmpl, base) {
        if (tmplmode != "template" && tmplmode != "template-dir") return 0
        if (rhs ~ /^".*"$/) rhs = substr(rhs, 2, length(rhs) - 2)
        if (substr(rhs, 1, 8) != "$(mktemp" || substr(rhs, length(rhs)) != ")") return 0
        inner = substr(rhs, 9, length(rhs) - 9)
        if (inner !~ /^ [A-Za-z0-9 ._\/$-]*$/) return 0
        n = split(inner, toks, " ")
        nd = 0; nt = 0; ntmpl = 0; tmpl = ""
        for (i = 1; i <= n; i++) {
            t = toks[i]
            if (t == "") continue
            if (t == "-d") { if (nd++) return 0 }
            else if (t == "-t") { if (nt++) return 0 }
            else if (substr(t, 1, 1) == "-") return 0
            else { if (ntmpl++) return 0; tmpl = t }
        }
        if (ntmpl != 1) return 0
        if (tmplmode == "template-dir" && nd != 1) return 0
        if (substr(tmpl, 1, 5) == "/tmp/") base = substr(tmpl, 6)
        else if (substr(tmpl, 1, 8) == "$TMPDIR/") {
            if (tmpdir_ok != 1) return 0
            base = substr(tmpl, 9)
        } else if (nt && index(tmpl, "/") == 0 && index(tmpl, "$") == 0) base = tmpl
        else return 0
        if (base !~ /^[A-Za-z0-9._-]*XXX+$/ ) return 0
        # `-t` takes a bare name; combined with a directory it is not a
        # plain template (GNU rejects it, BSD differs) -- refuse.
        if (nt && base != tmpl) return 0
        return 1
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        n = ml_segment(buf, segs)
        nsegs = n
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/[ \t]+$/, "", seg)
            # #8221: recognize export/readonly/declare/typeset/local NAME=...
            # as an assignment too (see _MKTEMP_REBIND_AWK'"'"'s header comment).
            bseg = _mktemp_strip_decl_kw(seg)
            prefix = varname "="
            plen = length(prefix)
            if (length(bseg) > plen && substr(bseg, 1, plen) == prefix) {
                rhs = substr(bseg, plen + 1)
                total++
                if (rhs == "$(mktemp -d)" || rhs == "\"$(mktemp -d)\"" || \
                    (tmplmode != "template-dir" && \
                     (rhs == "$(mktemp)" || rhs == "\"$(mktemp)\"")) || \
                    _mktemp_template_rhs_ok(rhs)) {
                    safe++
                    if (safeat == 0) { safeat = total; safeseg = i }
                } else if (rhs == canontok || rhs == "\"" canontok "\"") {
                    canon++
                    if (canonat == 0) canonat = total
                }
            } else if (_mktemp_is_other_rebind(seg, varname) || samecmd_assign_like(seg, varname)) {
                # #8221 + #9331: every rebinding of NAME that does not produce
                # a segment beginning `NAME=` -- read/mapfile/readarray/
                # getopts/unset/printf -v/for/select naming NAME, `NAME+=`,
                # `NAME[i]=`, an empty `NAME=`, and any eval/source. Poison
                # the count exactly like a second `NAME=` assignment, never
                # treated as safe/canon.
                total++
            }
        }
    }
    END {
        # The historic single-assignment proof (#6520), plus the ONE chained
        # form #7986 admits: EXACTLY two assignments, the mktemp-shaped one
        # FIRST and the self-referential canonicalization SECOND. A third
        # assignment, either shape repeated, or the reverse order all leave
        # this false and fail closed.
        ok = 0
        if (total == 1 && safe == 1) ok = 1
        else if (total == 2 && safe == 1 && canon == 1 && safeat == 1 && canonat == 2) ok = 1
        # repo#588: the mktemp binding must be PROVEN to run, in this shell,
        # before any use of NAME (see _SAMECMD_ORDER_AWK). The optional
        # realpath chain needs no context proof of its own: whether or not it
        # runs, NAME holds the mktemp path or its canonical spelling.
        if (ok) {
            fr = samecmd_first_ref(segs, nsegs, varname)
            if (fr != 0 && fr <= safeseg) ok = 0
            else if (!samecmd_binding_ctx_ok(buf, segs, nsegs, safeseg)) ok = 0
        }
        print (ok ? "SAFE" : "UNSAFE")
    }') || verdict=""
    [[ "$verdict" == "SAFE" ]]
}

# Lazily built, heredoc-body-masked scan text shared by BOTH same-command mktemp
# fast paths (rm scope and write confinement): a heredoc body is never a live
# top-level assignment of the current shell, so masking can only narrow (a
# decoy `NAME=$(mktemp -d)` inside an inert body cannot launder a live
# binding, Loom #6549). A masking failure leaves the text EMPTY, which no proof
# can pass (fail closed), instead of reaching the ERR trap's allow path.
_MKTEMP_SCAN_DONE=""
COMMAND_MKTEMP_SCAN=""
mktemp_scan_text_ensure() {
    [[ -n "$_MKTEMP_SCAN_DONE" ]] && return 0
    _MKTEMP_SCAN_DONE=1
    COMMAND_MKTEMP_SCAN="$COMMAND_NO_LITERAL_TEXT"
    if [[ "$COMMAND_MKTEMP_SCAN" == *"<<"* ]]; then
        COMMAND_MKTEMP_SCAN=$(printf '%s' "$COMMAND_MKTEMP_SCAN" | awk "$_MASKHEREDOC_AWK"'
        { buf = buf (NR > 1 ? "\n" : "") $0 }
        END { printf "%s", mask_heredoc_bodies(buf) }') || COMMAND_MKTEMP_SCAN=""
    fi
    return 0
}

# --- Write-confinement same-command mktemp outputs (#582, Loom #6949). -------
# _wt_write_mktemp_leading_var TOKEN -- succeed only when TOKEN (one optional
# surrounding quote layer) is exactly `$NAME` / `${NAME}`, optionally followed
# by a `/`-prefixed suffix. Publishes NAME in _WT_WRITE_VARNAME and the
# (possibly empty) suffix in _WT_WRITE_SUFFIX (out-parameters, so the bytes
# survive without a command-substitution round trip).
_WT_WRITE_VARNAME=""
_WT_WRITE_SUFFIX=""
_wt_write_mktemp_leading_var() {
    local tok="$1" t c1 c2
    _WT_WRITE_VARNAME=""
    _WT_WRITE_SUFFIX=""
    t="$tok"
    if [[ ${#t} -ge 2 ]]; then
        c1="${t:0:1}"
        c2="${t: -1}"
        if [[ ("$c1" == '"' && "$c2" == '"') || ("$c1" == "'" && "$c2" == "'") ]]; then
            # A single-quoted `$` is literal data, never an expansion: not a
            # variable reference at all, so it can prove nothing.
            [[ "$c1" == "'" ]] && return 1
            t="${t:1:${#t}-2}"
        fi
    fi
    if [[ "$t" =~ ^\$\{([A-Za-z_][A-Za-z0-9_]*)\}(/.*)?$ ]] || \
       [[ "$t" =~ ^\$([A-Za-z_][A-Za-z0-9_]*)(/.*)?$ ]]; then
        _WT_WRITE_VARNAME="${BASH_REMATCH[1]}"
        _WT_WRITE_SUFFIX="${BASH_REMATCH[2]}"
        return 0
    fi
    return 1
}

# wt_write_mktemp_same_command_safe TARGET CMDTEXT -- success only when TARGET
# is `$NAME[/suffix]` (see above) and NAME is proven by
# mktemp_same_command_bound(). On top of that shared proof, a write target
# needs conditions the bare-`$NAME` rm consumer never does:
#   1. The suffix is a plain literal path: no further expansion, quoting,
#      glob or backslash, and no `..` component (the mktemp directory's
#      depth is unknown, so `..` could walk back into a protected area).
#   2. TMPDIR is not rebound anywhere in the command (unless NAME *is*
#      TMPDIR, whose single binding the proof itself pins): `mktemp`
#      honours TMPDIR, so `export TMPDIR=<main>/x; t=$(mktemp -d)` would put
#      the "fresh" directory inside the main checkout. Fail closed.
# The caller additionally applies the protected-area checks that need the
# write-confinement block's roots (inherited TMPDIR, empty-expansion
# spelling) — see wt_write_mktemp_target_admitted() in that block.
wt_write_mktemp_same_command_safe() {
    local target="$1" cmdtext="$2" mode="${3:-template}" varname suffix scrubbed
    _wt_write_mktemp_leading_var "$target" || return 1
    varname="$_WT_WRITE_VARNAME"
    suffix="$_WT_WRITE_SUFFIX"
    [[ -n "$varname" ]] || return 1
    if [[ -n "$suffix" ]]; then
        case "$suffix" in
            *'$'*|*'`'*|*'"'*|*"'"*|*'\'*|*'*'*|*'?'*|*'['*) return 1 ;;
        esac
        case "$suffix/" in
            */../*|*/./*) return 1 ;;
        esac
    fi
    if [[ "$varname" != "TMPDIR" ]]; then
        # Only plain READS are scrubbed (`$TMPDIR`, `${TMPDIR}`); anything else
        # naming it -- an assignment, export/declare/read/unset, or a
        # `${TMPDIR:=...}` default-assignment -- survives and refuses.
        scrubbed="${cmdtext//\$\{TMPDIR\}/}"
        scrubbed="${scrubbed//\$TMPDIR/}"
        [[ "$scrubbed" == *TMPDIR* ]] && return 1
    fi
    mktemp_same_command_bound "$varname" "$cmdtext" "$mode"
}

# wt_write_mktemp_cd_chain_ok NAME CMDTEXT -- repo#597: the extra proof the
# `cd $NAME; <relative write>` form needs on top of the `$NAME` binding proof.
# A `$NAME/<x>` target never depends on the cwd, but a relative write after
# `cd $NAME` lands in `$NAME` only if that `cd` RAN, SUCCEEDED and was never
# undone. The binding proof cannot see any of that (`cd -`, a failed `cd` on a
# file or a missing `$NAME/sub`, `cd $NAME || true`), so admit ONLY this
# fail-closed shape (the caller separately requires `mktemp -d`):
#
#   [cd <literal> SEP]... NAME=<mktemp -d> [SEP NAME=<canon>] SEP cd $NAME SEP ...
#
#   - an optional prefix of plain `cd <literal-path>` steps (they cannot undo
#     a LATER cd), then the NAME binding (plain `NAME=`, never `export`/
#     `local`, whose status is not the mktemp's), then only further `NAME=`
#     steps, then the single `cd` of the whole remainder, whose argument is
#     exactly `$NAME` / `${NAME}` / `"$NAME"` / `"${NAME}"` (no suffix);
#   - SEP is `&&`, `;` or a newline. A fresh `mktemp -d` directory always
#     admits `cd`, so the only way that cd can fail is an EMPTY NAME (mktemp or
#     realpath failed). An all-`&&` chain short-circuits before the write.
#     Any `;`/newline chain ("loose") instead needs the empty case to land
#     somewhere harmless: only an UNQUOTED `cd $NAME` (empty -> `cd` -> $HOME,
#     whereas `cd ""` stays in the cwd), $HOME set, absolute and outside the
#     protected area (checked by the caller via _WT_CD_CHAIN_LOOSE), no
#     realpath chain, and an IFS/glob-safe TMPDIR so `$NAME` cannot split;
#   - from the binding on: no `||`, backtick or lone `&`; no pipe/group
#     around the steps before the cd; no `)`/`}` after it closing a group
#     opened earlier; and none of trap/eval/source/`.`/exec/builtin/alias/
#     function/enable (each can run a hidden cd before the write);
#   - anywhere in the command: no `cd -`, `pushd`, `popd`, `OLDPWD`, `CDPATH`,
#     and (repo#600) no command position that cmd_has_unproven_cwd_change()
#     flags -- a disguised cd spelling, an unresolvable command word, or a cd
#     behind a group/conditional prefix.
# Publishes _WT_CD_CHAIN_LOOSE=1 when a `;`/newline separator was involved.
_WT_CD_CHAIN_LOOSE=0
wt_write_mktemp_cd_chain_ok() {
    local varname="$1" cmd="$2" rest seg t w ncd=0 found=0 depth=0 bdepth=0
    local i c loose=0 quoted=0 nsteps=0 ws=$' \t' nl=$'\n'
    _WT_CD_CHAIN_LOOSE=0
    [[ "$varname" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || return 1
    [[ "$cmd" =~ (^|[^A-Za-z0-9_])(pushd|popd|OLDPWD|CDPATH)([^A-Za-z0-9_]|$) ]] && return 1
    [[ "$cmd" =~ (^|[^A-Za-z0-9_])cd[[:space:]]+- ]] && return 1
    # Optional prefix of literal `cd <path>` steps.
    rest="$cmd"
    while :; do
        rest="${rest#"${rest%%[![:space:]]*}"}"
        [[ "$rest" == "$varname="* ]] && break
        if [[ "$rest" =~ ^cd[$ws]+[A-Za-z0-9_./][A-Za-z0-9_./-]*[$ws]*(\&\&|\;|$nl) ]]; then
            rest="${rest:${#BASH_REMATCH[0]}}"
        else
            return 1
        fi
    done
    # From the binding on: banned separators / constructs.
    case "$rest" in
        *'||'*|*'`'*) return 1 ;;
    esac
    t="${rest//&&/}"; t="${t//>&/}"; t="${t//&>/}"; t="${t//<&/}"
    [[ "$t" == *'&'* ]] && return 1
    # A `.` (source) in command position.
    [[ "$rest" =~ (^|[\;\&\|\(\{$nl])[$ws]*\.[$ws] ]] && return 1
    t="${rest//[^A-Za-z0-9_]/ }"
    for w in $t; do
        case "$w" in
            cd) ncd=$((ncd + 1)) ;;
            trap|eval|source|exec|builtin|alias|function|enable) return 1 ;;
        esac
    done
    [[ "$ncd" -eq 1 ]] || return 1
    # repo#600: the count above only sees the literal word `cd` (punctuation
    # becomes spaces, so c''d / c\d / "c"d read as `c d`). Inspect every real
    # command position with the shared static recognizer instead: a disguised
    # cd, an unresolvable command word (c$()d, ${C:-c}d, $X), or a cd behind a
    # group/conditional prefix anywhere in the command refuses the proof.
    cmd_has_unproven_cwd_change "$cmd" && return 1
    # Walk the steps up to (and including the separator after) the cd.
    t="${rest//$nl/;}"
    while :; do
        local sep=""
        if [[ "$t" =~ ^([^\;\&]*)(\&\&|\;)(.*)$ ]]; then
            seg="${BASH_REMATCH[1]}"; sep="${BASH_REMATCH[2]}"; t="${BASH_REMATCH[3]}"
        else
            seg="$t"; t=""
        fi
        seg="${seg#"${seg%%[![:space:]]*}"}"
        seg="${seg%"${seg##*[![:space:]]}"}"
        case "$seg" in
            "cd \$$varname"|"cd \${$varname}")
                found=1 ;;
            "cd \"\$$varname\""|"cd \"\${$varname}\"")
                found=1; quoted=1 ;;
            "$varname="*)
                nsteps=$((nsteps + 1))
                case "$seg" in *'|'*|*'{'*|*'}'*) return 1 ;; esac
                ;;
            *) return 1 ;;
        esac
        [[ "$sep" == ";" ]] && loose=1
        [[ "$found" -eq 1 ]] && break
        [[ -n "$t" ]] || return 1
    done
    [[ "$found" -eq 1 ]] || return 1
    # Nothing after the cd may close a group opened before the binding.
    for ((i = 0; i < ${#t}; i++)); do
        c="${t:i:1}"
        case "$c" in
            '(') depth=$((depth + 1)) ;;
            ')') depth=$((depth - 1)); [[ "$depth" -lt 0 ]] && return 1 ;;
            '{') bdepth=$((bdepth + 1)) ;;
            '}') bdepth=$((bdepth - 1)); [[ "$bdepth" -lt 0 ]] && return 1 ;;
        esac
    done
    # A `;` anywhere after the cd step can still run the write after a
    # short-circuited (skipped) cd: `NAME=$(mktemp -d) && cd $NAME; > f`.
    [[ "$t" == *';'* ]] && loose=1
    if [[ "$loose" -eq 1 ]]; then
        [[ "$quoted" -eq 0 && "$nsteps" -eq 1 ]] || return 1
        if [[ -n "${TMPDIR:-}" && ! "$TMPDIR" =~ ^/[A-Za-z0-9._/-]*$ ]]; then
            return 1
        fi
        _WT_CD_CHAIN_LOOSE=1
    fi
    return 0
}

# _rm_scope_var_ref_split TOKEN -- split `$NAME<suffix>` into "NAME<TAB>suffix"; fail
# closed on any non-literal suffix or a variable that is not the path root.
# Built on mark_expandable_dollars() so quoting cannot dodge the tests.
_rm_scope_var_ref_split() {
    local tok="$1" marked body name rest
    mark_expandable_dollars "$tok"
    marked="$_MARKED_TOKEN"
    [[ "$marked" == $'\001'* ]] || return 1
    body="${marked:1}"
    if [[ "$body" =~ ^\{([A-Za-z_][A-Za-z0-9_]*)\}(.*)$ ]]; then
        name="${BASH_REMATCH[1]}"
        rest="${BASH_REMATCH[2]}"
    elif [[ "$body" =~ ^([A-Za-z_][A-Za-z0-9_]*)(.*)$ ]]; then
        name="${BASH_REMATCH[1]}"
        rest="${BASH_REMATCH[2]}"
    else
        return 1
    fi
    case "$rest" in
        *$'\001'*|*'`'*|*'$'*) return 1 ;;
    esac
    [[ -z "$rest" || "$rest" == /* ]] || return 1
    printf '%s\t%s' "$name" "$rest"
}

# rm_scope_literal_same_command_resolve TARGET CMDTEXT -- for `$NAME[/suffix]`
# whose ONLY same-command binding is a bare `NAME=<absolute literal>`, print the
# resolved path. The caller judges it like a literal `rm` target (floor +
# scope), so this never skips a check. Anything else returns 1 (fail closed).
rm_scope_literal_same_command_resolve() {
    local target="$1" cmdtext="$2" split varname suffix resolved
    split=$(_rm_scope_var_ref_split "$target") || return 1
    varname="${split%%$'\t'*}"
    suffix="${split#*$'\t'}"
    [[ -n "$varname" ]] || return 1
    resolved=$(printf '%s' "$cmdtext" | awk -v varname="$varname" "$_ESCAPE_AWK""$_HASLIVESUBST_AWK""$_ML_QSPLIT_AWK""$_MKTEMP_REBIND_AWK""$_SAMECMD_ORDER_AWK"'
    BEGIN {
        DQ = sprintf("%c", 34)
        SQ = sprintf("%c", 39)
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        n = ml_segment(buf, segs)
        nsegs = n
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/[ \t]+$/, "", seg)
            prefix = varname "="
            plen = length(prefix)
            if (length(seg) > plen && substr(seg, 1, plen) == prefix) {
                total++
                val = substr(seg, plen + 1)
                bindseg = i
            } else if (_mktemp_is_other_rebind(seg, varname) || samecmd_assign_like(seg, varname)) {
                # #9331: NAME+= / NAME[i]= / an EMPTY NAME= / a
                # declaration-keyword assignment / read|mapfile|readarray|
                # getopts|unset|printf -v|for|select naming NAME / any eval
                # or source. Each rebinds NAME (or can) without producing a
                # segment that BEGINS `NAME=`, so each was invisible to the
                # test above and the resolution below ran on a value the shell
                # would no longer be holding at the `rm` word. Poison it: this
                # resolver is a RELAXATION of the catastrophic floor, so
                # "cannot account for NAME" must reach the fail-closed deny,
                # never an allow. Counted separately from `total` because the
                # only resolvable form here stays a BARE `NAME=<literal>`
                # segment head -- recognizing a decl-keyword assignment as
                # RESOLVABLE would newly ALLOW `export d=/tmp/x; rm -rf "$d"`,
                # which is a relaxation this tightening must not smuggle in
                # (#9601 is where a widening like that belongs).
                rebind++
            }
        }
    }
    END {
        # repo#588: the binding must also be PROVEN to run, in this shell,
        # before any use of NAME (see _SAMECMD_ORDER_AWK).
        if (total == 1 && rebind == 0) {
            fr = samecmd_first_ref(segs, nsegs, varname)
            if (fr != 0 && fr <= bindseg) total = -1
            else if (!samecmd_binding_ctx_ok(buf, segs, nsegs, bindseg)) total = -1
        }
        if (total == 1 && rebind == 0) {
            vlen = length(val)
            quoted = 0
            if (vlen >= 2) {
                c1 = substr(val, 1, 1)
                c2 = substr(val, vlen, 1)
                if ((c1 == DQ && c2 == DQ) || (c1 == SQ && c2 == SQ)) {
                    val = substr(val, 2, vlen - 2)
                    quoted = 1
                }
            }
            # Any shell metacharacter or stray quote means the segment is not
            # a plain `NAME=<literal>` (a subshell close, a redirection, a
            # second word). Unquoted whitespace makes it a PREFIX assignment
            # (`NAME=/x cmd`) that never persists in the shell. Fail closed.
            if (val ~ /[;&|<>()\\\n]/ || index(val, DQ) || index(val, SQ)) val = ""
            if (!quoted && val ~ /[ \t]/) val = ""
            if (val ~ /^\// && val !~ /[$`]/) print val
        }
    }')
    [[ -n "$resolved" ]] || return 1
    printf '%s' "${resolved}${suffix}"
}

# physical_abs_path PATH -- resolve symlinks as far as the filesystem allows,
# appending not-yet-existing trailing segments lexically (symlink-resolving
# counterpart of the pure-lexical normalize_abs_path).
physical_abs_path() {
    # Resolve an ABSOLUTE path's symlinks as far as the filesystem allows,
    # keeping any not-yet-existing trailing segments lexically appended.
    #
    # This is the symlink-resolving counterpart to normalize_abs_path(), which
    # is deliberately pure-lexical (see its header) and therefore leaves a
    # symlinked ancestor intact. Whenever a lexically-built path is compared
    # against a path that came from git (`rev-parse --show-toplevel` /
    # `--git-common-dir`, both of which return the symlink-RESOLVED spelling),
    # the two can describe the same directory in different words and never
    # string-match. On macOS this is the default state of any $TMPDIR path:
    # /var is a symlink to /private/var, so `mktemp -d` yields
    # /var/folders/... while git reports /private/var/folders/... (#6684; the
    # same divergence the /tmp -> /private/tmp `pwd -P` note at the worktree
    # containment block below already handles for its own comparisons).
    #
    # Walking up to the deepest EXISTING ancestor before `cd`-ing matters:
    # the paths compared here (e.g. a target-dir cargo has not created yet)
    # routinely do not exist on disk, and `realpath -m`, which would handle
    # that, is GNU-only and silently no-ops on macOS.
    local path="$1" tail="" phys
    [[ "$path" == /* ]] || { printf '%s' "$path"; return 0; }
    path=$(normalize_abs_path "$path")
    while [[ ! -d "$path" && "$path" != "/" ]]; do
        tail="${path##*/}${tail:+/}$tail"
        path="${path%/*}"
        [[ -n "$path" ]] || path="/"
    done
    phys=$(cd "$path" 2>/dev/null && pwd -P) || phys=""
    [[ -n "$phys" ]] || phys="$path"
    if [[ -n "$tail" ]]; then
        if [[ "$phys" == "/" ]]; then
            printf '/%s' "$tail"
        else
            printf '%s/%s' "$phys" "$tail"
        fi
    else
        printf '%s' "$phys"
    fi
}

# =============================================================================
# Worktree-isolation guard toggle — default ON (rjwalters/repo#188, porting
# the BASH-TOOL WRITE CONFINEMENT category below from Loom's vendored
# guard-destructive-generic.sh, itself gated by worktree_isolation_guard_enabled()
# / guards.worktreeIsolation there). Kept as ONE switch so a repo/session that
# already opted out of Edit/Write-tool worktree confinement (a host tool layered
# on top of this guard, e.g. Loom's own guard-worktree-paths.sh) gets the
# identical decision from this guard's Bash-tool confinement below, and the
# documented escape hatch (a human/driver session that must edit the main
# checkout while worktrees exist) keeps working here too.
#
# Resolution order (highest precedence first), mirroring every other guard
# toggle in this file:
#   1. REPO_GUARD_WORKTREE_ISOLATION env var, then legacy
#      LOOM_GUARD_WORKTREE_ISOLATION (0/false/no disables, 1/true/yes forces on)
#   2. guards.worktreeIsolation via guard_cfg() — repo config wins over legacy
#      .loom (default true when absent)
#   3. Default: true (guard on)
#
# Mirrors sql_guard_enabled() / rm_scope_repo_enabled(): cached in
# _WORKTREE_ISOLATION_CACHE, invoked LAZILY — only once the cheap substring
# pre-check on the write-confinement block below has already matched — so the
# jq config read never touches the hot path for the vast majority of Bash calls
# that contain none of the recognized write idioms at all. The config read is
# best-effort: any parse failure falls through to guard-ON and never trips the
# ERR trap. Resolution mechanics shared via guard_toggle_enabled() above.
# =============================================================================
_WORKTREE_ISOLATION_CACHE=""
worktree_isolation_guard_enabled() {
    guard_toggle_enabled _WORKTREE_ISOLATION_CACHE worktreeIsolation true LOOM_GUARD_WORKTREE_ISOLATION REPO_GUARD_WORKTREE_ISOLATION
}

# True if $1 (an absolute, lexically-normalized path) sits inside ANY managed
# worktree — walks up looking for the `.loom-managed` sentinel worktree.sh
# writes at every worktree root. Inline copy of walk_up_for_sentinel() in
# guard-worktree-paths.sh: kept separate rather than sourced, same rationale
# as resolve_worktree_root() mirroring worktree-root.sh above — this hook is a
# distinct process with its own self-contained fail-open contract.
_in_any_managed_worktree() {
    local dir="$1"
    [[ -n "$dir" ]] || return 1
    if [[ ! -d "$dir" ]]; then
        dir="${dir%/*}"
        [[ -z "$dir" ]] && dir="/"
    fi
    local i=0
    while [[ $i -lt 64 ]]; do
        [[ -f "$dir/.loom-managed" ]] && return 0
        [[ "$dir" == "/" ]] && break
        dir="${dir%/*}"
        [[ -z "$dir" ]] && dir="/"
        i=$((i + 1))
    done
    return 1
}

# Reads the `# Branch: <name>` line worktree.sh records into a managed
# worktree's `.loom-managed` sentinel at creation time (Loom #7530; this repo's
# vendored .loom/scripts/worktree.sh writes the same line). Authoritative
# because it is recorded once at creation, so it survives the worktree later
# going detached -- exactly the ambiguous case it resolves. Walks up from $1
# like _in_any_managed_worktree() (same 64-hop cap) and echoes the recorded
# branch for the NEAREST sentinel, or nothing when that sentinel has no
# well-formed `# Branch:` line. A value that is not a plausible branch name
# (empty, whitespace, a leading `-`, `..`, or characters git forbids in a
# ref) is treated as absent. ALWAYS returns 0: this hook's ERR trap
# fail-opens, so the ordinary "nothing found" case must never be a non-zero
# exit from a helper used in an assignment.
_managed_worktree_branch() {
    local dir="$1" line="" br=""
    [[ -n "$dir" ]] || return 0
    if [[ ! -d "$dir" ]]; then
        dir="${dir%/*}"
        [[ -z "$dir" ]] && dir="/"
    fi
    local i=0
    while [[ $i -lt 64 ]]; do
        if [[ -f "$dir/.loom-managed" ]]; then
            line=$(grep -m1 '^# Branch: ' "$dir/.loom-managed" 2>/dev/null) || line=""
            br="${line#\# Branch: }"
            if [[ -n "$line" && -n "$br" ]] && \
               [[ "$br" =~ ^[A-Za-z0-9._/-]+$ ]] && \
               [[ "$br" != -* && "$br" != */ && "$br" != /* && "$br" != *..* && "$br" != *//* ]]; then
                printf '%s' "$br"
            fi
            return 0
        fi
        [[ "$dir" == "/" ]] && break
        dir="${dir%/*}"
        [[ -z "$dir" ]] && dir="/"
        i=$((i + 1))
    done
    return 0
}

# True if at least one managed worktree currently exists under $1
# (<base>/<name>/.loom-managed, depth 2 — matches worktree.sh's layout).
# Mirrors any_managed_worktree_exists() in guard-worktree-paths.sh.
_any_managed_worktree_exists() {
    local base="$1"
    [[ -n "$base" && -d "$base" ]] || return 1
    local hit
    hit=$(find "$base" -mindepth 2 -maxdepth 2 -name '.loom-managed' -print -quit 2>/dev/null) || hit=""
    [[ -n "$hit" ]]
}

# =============================================================================
# mark_expandable_dollars() — rewrite a raw write-target token into its
# "effective path shape" (#4921).
#
# extract_write_targets() is a TOKENIZER, not a shell evaluator: a token is
# emitted with its quote characters copied verbatim (qsplit's contract) and
# with every `$…` reference unexpanded. Before the write-confinement block can
# reason about WHERE a token lands, it needs to know which `$` characters the
# real shell would actually expand — a `$` inside a SINGLE-quoted span, or one
# preceded by a backslash, is literal data (a file really named `$X`), while a
# bare or DOUBLE-quoted `$` is an expansion the guard cannot resolve.
#
# Emits (in the global _MARKED_TOKEN) the token with:
#   - quote characters removed (so `"$A"/x`, `"$A/x"` and `$A/x` all normalize
#     to the same shape and quoting cannot be used to dodge the shape tests),
#   - backslash escapes applied (the backslash dropped, the escaped character
#     kept literal),
#   - every EXPANDABLE `$` replaced by SOH (0x01) — a character no real path
#     produced by this tokenizer contains — while a LITERAL `$` stays a `$`.
#
# A global rather than a subshell echo: this runs per write target and the
# callers are already in a `while read` loop.
#
# Implemented on top of the shared scanner below so the write-confinement
# block has exactly ONE definition of "what the shell would do to these
# quotes" — a second, hand-copied quote parser is precisely how the two
# consumers would drift apart, and a drift in this grammar IS a guard bypass.
# =============================================================================
_MARKED_TOKEN=""
mark_expandable_dollars() {
    _scan_token_quoting "$1" $'\001'
    _MARKED_TOKEN="$_SCANNED_TOKEN"
}

# =============================================================================
# strip_target_quoting() — shell-accurate quote removal + backslash
# unescaping for the write-confinement absolute/relative classification
# (#4926).
#
# extract_write_targets() emits tokens with their quote characters preserved
# VERBATIM (qsplit's contract, #3755) — extract_rm_targets() and
# parse_force_ops() depend on that raw form and MUST keep receiving it
# unchanged, so this is called ONLY from the write-confinement classification
# below, never from qsplit()/extract_write_targets() themselves. Without it a
# quoted absolute path (`'/main/evil'`, `"/main/evil"`) starts with a quote
# character rather than `/`, so the `[[ … == /* ]]` check misclassifies it as
# RELATIVE and cwd-prefixes it into a location the write will never have.
# From a LINKED-WORKTREE cwd — the canonical builder setup — that fabrication
# walks straight back into the acting worktree's own `.loom-managed` sentinel
# and is silently ALLOWED, defeating the #4178 confinement check by simply
# quoting the target (the same masked-allow shape as the unresolved-`$`
# bypass fixed by #4921/#4927, reached here through quoting instead).
#
# Emits (in the global _UNQUOTED_TARGET) the token with quote characters
# removed and backslash escapes applied, and every other character —
# INCLUDING `$` — copied through unchanged: any expandable-`$` shape was
# already judged by the dedicated unresolved-`$` block above, so a file
# genuinely named `$X` or `~` (single-quoted or backslash-escaped) unquotes
# to the literal `$X`/`~` and still resolves as a plain relative path,
# exactly as today (#4382 / #4921 contracts preserved).
#
# Returns 0 when every quote in the token is balanced (the caller may use
# _UNQUOTED_TARGET). Returns 1 on an unterminated quote — the caller MUST
# then fall back to the raw, quote-preserved token, so an unbalanced quote
# can only ever keep today's verdict, never widen a deny into an allow.
# =============================================================================
# =============================================================================
# resolve_stash_cwd — the effective cwd a `git stash pop/drop/clear` runs in.
#
# Transplanted from Loom's vendored copy (loom#5173) as part of the repo#188
# parity reconciliation. Mirrors parse_force_ops' cd-tracking: threads a
# `cd <dir> &&` prefix earlier in the SAME compound command through to the
# stash invocation, so `cd <worktree> && git stash pop` — hook session cwd
# still the main repo root, the common worktree shape — resolves scope
# against the cd TARGET rather than the hook's raw session cwd.
#
# Classification uses strip_cd_quoting() so a fully or partially quoted
# absolute argument is not misclassified as relative; curcwd is still built
# from the RAW cd argument (loom#5372), because the caller unquotes a COPY
# before touching the filesystem.
#
# `git -C <path>` threading (repo#194): a `-C`/`-c` run before `stash` is
# resolved the same way `parse_force_ops` already resolves it for force ops,
# so `git -C <main-checkout> stash pop` run from a worktree cwd is caught
# against the -C target rather than the worktree cwd. See the `toks[idx] ==
# "git"` block below.
#
# `--git-dir=`/`--work-tree=` and a leading `GIT_DIR=`/`GIT_WORK_TREE=`
# assignment run (repo#202): two further shapes reach the MAIN checkout's
# stash stack undetected. `git --git-dir=<main>/.git --work-tree=<main> stash
# pop` matches the pre-check but this parser only recognised -C/-c, so it fell
# back to the raw session cwd (the worktree) and the caller saw no reason to
# ask. `GIT_DIR=<main>/.git GIT_WORK_TREE=<main> git stash pop` does not even
# start with `cd`/`git` -- an assignment token precedes it -- so `toks[1] ==
# "git"` above never fired at all. Fixed by (1) skipping a leading run of
# `VAR=val` tokens before classifying a segment, capturing GIT_DIR/
# GIT_WORK_TREE along the way, and (2) recognising --git-dir/--work-tree (both
# `=`-joined and space-separated) in the same loop that already threads -C/-c.
#
# Output contract changed from one line to three (cwd / git-dir override /
# work-tree override) so the caller can resolve --git-dir scope via
# --git-common-dir rather than reusing the -C cd-and-rev-parse path verbatim --
# --git-dir takes a .git directory, not a worktree path, and cd-ing into it
# then asking git to "rev-parse --show-toplevel" is not the same operation git
# itself performs when --git-dir/--work-tree are passed explicitly. The only
# caller of this function is the stash pre-check block below; no other
# consumer or test calls it directly, so widening the contract here is safe.
#
# repo#204 review: -C and --git-dir/GIT_DIR COMPOSE, and git applies -C first
# no matter where it sits in the argument order -- a relative --git-dir /
# GIT_DIR / --work-tree / GIT_WORK_TREE value is interpreted against the
# post--C directory even when the flag precedes -C, and even when it arrives
# as an env prefix. The first cut resolved the env-prefix pair against curcwd
# BEFORE the -C loop ran, pinning it to the pre--C directory. Raw values are
# now recorded during the loop and resolved once, after it, against the final
# process cwd. (The matching caller-side gap -- probing git for the toplevel
# with no -C at all -- is fixed in the pre-check block below.)
# =============================================================================
resolve_stash_cwd() {
    printf '%s' "$1" | awk -v startcwd="$2" -v home="$HOME" "$_ESCAPE_AWK""$_HASLIVESUBST_AWK""$_QSPLIT_AWK""$_CDEXPAND_AWK""$_CDQUOTE_AWK""$_CDWORD_AWK""$_MASKWS_AWK"'
    BEGIN { curcwd = startcwd; found = 0; UNK = unknown_cwd(); QS_MARK = 1 }
    {
        $0 = qsplit($0)   # quote-aware segmentation
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) { segs[i] = flow_unmark(segs[i]); SK[i] = FL_KIND }
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/^sudo[ \t]+/, "", seg)
            sub(/^[ \t]+/, "", seg)
            # repo#601: control-flow context (see flow_begin in _CDWORD_AWK).
            if (flow_begin(SK[i], seg)) curcwd = UNK
            kb = FL_EFF
            if (seg == "") continue
            flow_track(seg)
            # Mask whitespace INSIDE quoted spans before tokenizing (repo#194
            # review). Splitting on raw whitespace shreds a quoted path that
            # contains a space, so a -C or cd argument like "/main dir" became
            # two tokens and resolution collapsed -- a silent allow for exactly
            # the shape this parser exists to catch. mask_ws/unmask_ws come
            # from _MASKWS_AWK; do NOT redefine them here, awk rejects a
            # duplicate function definition and the whole parser then fails
            # open.
            seg = mask_ws(seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            # Skip a leading run of `VAR=val` assignment tokens (repo#202) so
            # an env-prefixed invocation like `GIT_DIR=x GIT_WORK_TREE=y git
            # stash pop` still classifies past the assignments to "git" below,
            # instead of never matching toks[1] at all. Capture GIT_DIR/
            # GIT_WORK_TREE while skipping -- a later command-line
            # --git-dir/--work-tree flag on the same segment overrides these,
            # mirroring git own env-vs-flag precedence.
            envgitdir_raw = ""
            envworktree_raw = ""
            idx = 1
            while (idx <= m && toks[idx] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                eqpos = index(toks[idx], "=")
                vname = substr(toks[idx], 1, eqpos - 1)
                vval = substr(toks[idx], eqpos + 1)
                if (vname == "GIT_DIR") envgitdir_raw = vval
                else if (vname == "GIT_WORK_TREE") envworktree_raw = vval
                idx++
            }
            if (idx > m) continue   # nothing left but assignments
            # Shared static command-word recognizer (repo#600, _CDWORD_AWK):
            # a disguised spelling of cd is still cd, and an unresolvable
            # command word (or a cd behind a group/conditional) leaves the
            # cwd UNKNOWN -- the caller then asks rather than judging a stale
            # cwd. Relative cd / -C / --git-dir from UNKNOWN stay unknown
            # (they are prefixed with the sentinel, which the caller detects).
            cdk = cd_seg_class(seg)
            if (cdk == "unknown") { flow_cd(kb, ""); curcwd = UNK; continue }
            if (cdk == "cd") {
                if (flow_cd(kb, flow_next_kind(SK, i, n))) { curcwd = UNK; continue }
                cdtok = CDW_ARG
                if (cdtok != "" && cdtok != "-") {
                    cdarg = expand_cd_arg(cdtok, home)
                    cdclass = strip_cd_quoting(cdarg)
                    cdbase = curcwd
                    if (cdclass ~ /^\//) {
                        curcwd = cdarg
                    } else if (curcwd != "" && curcwd != UNK) {
                        curcwd = curcwd "/" cdarg
                    }
                    # repo#625: a cd to a missing directory fails and stays put
                    if (cd_unreached(cdarg, cdbase, flow_next_kind(SK, i, n), home)) curcwd = UNK
                }
                continue
            }
            # `[VAR=val ...] git [-C <path>] [-c k=v] [--git-dir(=)<path>]
            # [--work-tree(=)<path>] … stash pop|drop|clear`.
            #
            # The -C threading is repo#194: git resolves -C against the process
            # cwd and then operates there, so `git -C <main-checkout> stash pop`
            # issued from a linked worktree touches the MAIN checkout stash
            # stack while a cwd-only check sees only the worktree and allows it.
            # refs/stash is one stack shared across every linked worktree, so
            # that is a live path to destroying the WIP of another agent. This
            # mirrors the -C handling parse_force_ops already had; the asymmetry
            # was inherited from the vendored copy and documented there as a
            # known limitation rather than fixed.
            #
            # NOTE: this whole block sits inside a SINGLE-QUOTED awk program.
            # An apostrophe in a comment here terminates that string and breaks
            # the guard for every command in the repo (it happened while
            # writing this). Keep comments apostrophe-free.
            #
            # Multiple -C options compose in git (each resolved relative to the
            # previous), which is why this loops rather than reading only the
            # first. -c takes a key=value token and is skipped, not applied.
            if (toks[idx] == "git") {
                gi = idx + 1
                # proccwd is the PROCESS cwd git actually runs in: only -C
                # moves it, and -C chdirs immediately during option parsing.
                # A relative --git-dir/--work-tree (or GIT_DIR/GIT_WORK_TREE)
                # is therefore interpreted against the FINAL post--C cwd no
                # matter where it sits in the argument order (repo#204 review;
                # verified against git 2.43 for all three orders: env prefix,
                # flag-before--C, flag-after--C). So the raw values are only
                # RECORDED in this loop and resolved once the loop ends -- the
                # earlier version resolved the env-prefix pair against curcwd
                # before the -C loop ran, which pinned a relative GIT_DIR to
                # the pre--C directory.
                proccwd = curcwd
                gitdir_raw = envgitdir_raw
                worktree_raw = envworktree_raw
                have_gitdir = (envgitdir_raw != "")
                have_worktree = (envworktree_raw != "")
                while (gi <= m) {
                    if (toks[gi] == "-C" && gi + 1 <= m) {
                        gcarg = expand_cd_arg(unmask_ws(toks[gi + 1]), home)
                        gcclass = strip_cd_quoting(gcarg)
                        if (gcclass ~ /^\//) {
                            proccwd = gcarg
                        } else if (proccwd != "") {
                            proccwd = proccwd "/" gcarg
                        }
                        gi += 2
                        continue
                    }
                    if (toks[gi] == "-c" && gi + 1 <= m) { gi += 2; continue }
                    # A command-line flag overrides the env prefix, mirroring
                    # git own precedence; a later flag overrides an earlier one.
                    if (toks[gi] == "--git-dir" && gi + 1 <= m) {
                        gitdir_raw = toks[gi + 1]; have_gitdir = 1
                        gi += 2
                        continue
                    }
                    if (toks[gi] ~ /^--git-dir=/) {
                        gitdir_raw = substr(toks[gi], 11); have_gitdir = 1
                        gi += 1
                        continue
                    }
                    if (toks[gi] == "--work-tree" && gi + 1 <= m) {
                        worktree_raw = toks[gi + 1]; have_worktree = 1
                        gi += 2
                        continue
                    }
                    if (toks[gi] ~ /^--work-tree=/) {
                        worktree_raw = substr(toks[gi], 13); have_worktree = 1
                        gi += 1
                        continue
                    }
                    break
                }
                if (gi + 1 <= m && toks[gi] == "stash" && \
                    (toks[gi + 1] == "pop" || toks[gi + 1] == "drop" || toks[gi + 1] == "clear")) {
                    gitdirarg = ""
                    worktreearg = ""
                    if (have_gitdir) {
                        gdarg = expand_cd_arg(unmask_ws(gitdir_raw), home)
                        gdclass = strip_cd_quoting(gdarg)
                        gitdirarg = (gdclass ~ /^\//) ? gdarg : (proccwd != "" ? proccwd "/" gdarg : gdarg)
                    }
                    if (have_worktree) {
                        wtarg = expand_cd_arg(unmask_ws(worktree_raw), home)
                        wtclass = strip_cd_quoting(wtarg)
                        worktreearg = (wtclass ~ /^\//) ? wtarg : (proccwd != "" ? proccwd "/" wtarg : wtarg)
                    }
                    # An explicit work tree IS the directory the operation acts
                    # on, so it wins over the process cwd for the effective-cwd
                    # line; otherwise the post--C process cwd is what git infers
                    # the work tree from.
                    gitcwd = (worktreearg != "") ? worktreearg : proccwd
                    print gitcwd
                    print gitdirarg
                    print worktreearg
                    found = 1
                    exit
                }
            }
        }
    }
    END { if (!found) { print curcwd; print ""; print "" } }'
}

_UNQUOTED_TARGET=""
strip_target_quoting() {
    local rc=0
    _scan_token_quoting "$1" "" || rc=1
    _UNQUOTED_TARGET="$_SCANNED_TOKEN"
    return "$rc"
}

# =============================================================================
# _scan_token_quoting() — the single shell-accurate quote-removal /
# backslash-unescaping pass shared by the two helpers above.
#
#   $1  raw token (quote characters preserved verbatim, per qsplit's contract)
#   $2  text substituted for each EXPANDABLE `$`; empty keeps the `$` literal
#
# Sets _SCANNED_TOKEN. Returns 0 when every quote was closed, 1 when the token
# ended inside an unterminated quote (callers decide the fallback; no caller
# may treat an unterminated quote as license to widen an allow).
# =============================================================================
_SCANNED_TOKEN=""
_scan_token_quoting() {
    local tok="$1" dollar="$2"
    # Named _stq_out (not "out") to avoid colliding, in shellcheck's
    # cross-function SC2178/SC2179 heuristic, with the unrelated `local -a
    # out=()` ARRAY in normalize_abs_path() elsewhere in this file — two
    # different `local` variables in two different functions, but shellcheck
    # does not scope-isolate that particular check across sibling functions.
    local _stq_out="" c
    local n=${#tok}
    local i=0 in_s=0 in_d=0
    while [[ $i -lt $n ]]; do
        c="${tok:i:1}"
        if [[ $in_s -eq 1 ]]; then
            # Inside '…': nothing expands; only the closing quote is special.
            if [[ "$c" == "'" ]]; then in_s=0; else _stq_out+="$c"; fi
            i=$((i + 1))
            continue
        fi
        case "$c" in
            "'")
                if [[ $in_d -eq 1 ]]; then _stq_out+="$c"; else in_s=1; fi ;;
            '"')
                if [[ $in_d -eq 1 ]]; then in_d=0; else in_d=1; fi ;;
            '\')
                # Escapes the NEXT character (a trailing backslash is dropped).
                i=$((i + 1))
                [[ $i -lt $n ]] && _stq_out+="${tok:i:1}" ;;
            '$')
                if [[ -n "$dollar" ]]; then _stq_out+="$dollar"; else _stq_out+="$c"; fi ;;
            *)
                _stq_out+="$c" ;;
        esac
        i=$((i + 1))
    done
    _SCANNED_TOKEN="$_stq_out"
    [[ $in_s -eq 0 && $in_d -eq 0 ]]
}

# =============================================================================
# CD-ARGUMENT TILDE / $HOME EXPANSION (#5315)
#
# The three `cd`-tracking blocks below (extract_write_targets, parse_force_ops,
# resolve_stash_cwd) thread a `cd <dir> &&` prefix through the later segments of
# a compound command by joining <dir> onto a tracked `curcwd`. That join is a
# plain string concatenation with NO word expansion — so a `cd ~/GitHub/loom`
# prefix was joined VERBATIM, embedding a literal `~` mid-path
# (`.../loom/~/GitHub/loom/...`) and mis-resolving every later relative write /
# force-op / stash target of that command (the false positive reported in
# #5315). Only a leading `/` (already-absolute) was handled specially; a leading
# `~` or `$HOME` fell through to the plain repo-relative join.
#
# expand_cd_arg() performs the SAME narrow, unambiguous slice of shell word
# expansion the bash-side expand_leading_tilde() (#4382) already applies to
# write TARGETS, but for the cd ARGUMENT and inside awk (which the write-target
# helper runs too late to reach). `home` is the guard process's own $HOME,
# passed in via `-v home=...` exactly like expand_leading_tilde() reads the
# guard's process $HOME — a same-line `HOME=<x> cmd` prefix in the analyzed
# command text can never redefine it. Handled here:
#   ~            -> home
#   ~/rest       -> home "/rest"
#   $HOME        -> home
#   $HOME/rest   -> home "/rest"
# An expanded value starts with `/`, so the caller's existing `~ /^\//` branch
# then treats it as an ABSOLUTE curcwd (correct — `cd ~` replaces the cwd, it is
# not appended to it).
#
# Left DELIBERATELY UNEXPANDED (returned unchanged, so the caller joins it
# repo-relative — the fail-CLOSED direction this file always biases toward, and
# the same convention already used for `cd -` / a bare `cd`):
#   ~user, ~user/rest   awk cannot safely resolve another user's home (no
#                       getent/dscl without a shell-injection surface); leaving
#                       it repo-relative keeps a genuinely-out-of-tree write
#                       classified as in-tree (denied) rather than guessing it
#                       safe. See the #5315 DECISION note at the head of
#                       extract_write_targets() for the ~user/EPHEMERAL rationale.
# Because qsplit() copies a quoted span VERBATIM (including its quote chars) and
# leaves a literal backslash untouched, a token the real shell would NOT expand
# does not start with a bare `~`/`$HOME` here and falls through unchanged —
#   '~/x' / "~/x"  -> starts with a quote char (shell never tilde-expands it)
#   \~/x           -> starts with a backslash (shell never tilde-expands it)
#   foo~/x         -> tilde is not leading (not an expansion position)
# mirroring expand_leading_tilde()'s quoted-tilde treatment exactly. If `home`
# is empty (HOME unset) every case falls through unchanged, matching that
# helper's `[[ -n "$HOME" ]]` guard.
#
# Shared as a single awk source string (like _QSPLIT_AWK) so the three
# cd-tracking blocks cannot drift.
# =============================================================================
_CDEXPAND_AWK='
function expand_cd_arg(tok, home) {
    if (home == "") return tok
    if (tok == "~") return home
    if (tok == "$HOME") return home
    if (substr(tok, 1, 2) == "~/") return home substr(tok, 2)
    if (substr(tok, 1, 6) == "$HOME/") return home substr(tok, 6)
    return tok
}
'

# =============================================================================
# strip_cd_quoting() (#5363) — full quote-removal absolute/relative
# CLASSIFICATION helper for a tracked `cd` argument. Used by the three
# `cd`-tracking awk blocks in this file — extract_write_targets() (the
# write-confinement hard-deny path), parse_force_ops(), and
# resolve_stash_cwd() (the latter two feed the ask-gate for
# force-push/reset-hard branch identity and stash-scope cwd resolution,
# wired up in #5372) — NEVER on the RAW cdarg threaded into curcwd itself
# (see each call site's own comment for why).
#
# The #4933/#4941 fix (cdqc/cdlen leading-and-matching-trailing-quote strip)
# only recognizes a FULLY quoted argument ('/abs/path', "/abs/path"): it peels
# one leading quote character and, if the LAST character of the token is the
# SAME quote character, one trailing one. A PARTIALLY quoted absolute
# argument -- the quote closes mid-token, e.g. '<main>'/defaults -- still
# starts with a quote character, so it fails that narrow test and falls
# through unchanged, still starting with a quote rather than `/`, and is
# misclassified as RELATIVE -- the same masked-allow shape as #4933/#4926,
# reached through a partially-quoted `cd` argument instead of a fully-quoted
# or unquoted one (#5363).
#
# strip_cd_quoting() instead walks the ENTIRE token character-by-character,
# stripping every quote character (both single- and double-quoted spans, with
# ordinary shell nesting: a `"` is literal data inside a `'...'` span and vice
# versa) rather than only a leading/trailing pair -- so '<main>'/defaults
# correctly unquotes to <main>/defaults, which DOES start with `/`, and
# classifies as absolute. This mirrors (but, being pure awk, cannot literally
# share code with) the shell layer's _scan_token_quoting() used by
# strip_target_quoting() for the write-TARGET side (#4926) -- that scanner is
# unreachable from here because this decision is made entirely inside awk,
# before the shell layer ever sees a token. Backslash-escapes and `$` are
# deliberately left untouched (out of scope for a leading-`/` classification
# test, and the existing unresolved-`$` detector downstream,
# mark_expandable_dollars()/#4921, still needs the RAW curcwd this function
# never touches).
#
# Returns the token UNCHANGED whenever a quote is left open at end-of-token
# (in_s or in_d still true) -- an unbalanced/unterminated quote can therefore
# only ever KEEP today's classification, never flip a relative-looking token
# into an absolute one it never proved (same fallback contract as
# strip_target_quoting()/#4926 and the #4933 leading/trailing strip it
# replaces here).
# =============================================================================
_CDQUOTE_AWK='
function strip_cd_quoting(tok,   out, n, i, c, in_s, in_d, sq, dq) {
    sq = sprintf("%c", 39)
    dq = sprintf("%c", 34)
    out = ""
    n = length(tok)
    in_s = 0
    in_d = 0
    for (i = 1; i <= n; i++) {
        c = substr(tok, i, 1)
        if (in_s) {
            if (c == sq) { in_s = 0 } else { out = out c }
            continue
        }
        if (c == sq) {
            if (in_d) { out = out c } else { in_s = 1 }
            continue
        }
        if (c == dq) {
            if (in_d) { in_d = 0 } else { in_d = 1 }
            continue
        }
        out = out c
    }
    if (in_s || in_d) return tok
    return out
}
'

# =============================================================================
# STATIC COMMAND-WORD RECOGNITION FOR cwd TRACKING (repo#600)
#
# Every cwd consumer in this file (extract_write_targets, parse_force_ops,
# resolve_stash_cwd, and the repo#597 wt_write_mktemp_cd_chain_ok() proof)
# used to recognise a directory change only when the command word was the
# literal bytes `cd`. Bash runs the SAME builtin for c''d, c\d, "c"d, c$()d and
# ${C:-c}d, so those spellings changed the real cwd while every tracker kept
# the previous one -- and a later RELATIVE write was judged against a stale
# (often /tmp, i.e. "harmless") cwd while it really landed in the main
# checkout.
#
# This is the ONE shared recognizer all of them use. It is static on purpose:
# it never evaluates, expands or executes anything. Two results matter:
#
#   cmdword_static(tok)  -- the word bash would execute, derived by
#       shell-correct quote removal ONLY: single quotes, double quotes (with
#       the backslash escapes bash honours inside them) and unquoted backslash
#       escapes. Anything whose result depends on runtime state -- any `$`
#       (parameter expansion, $(..), $((..)), ANSI-C or locale quoting), a
#       backtick, an unquoted glob or brace character, or an unbalanced quote
#       -- sets CMDW_UNKNOWN=1. Deleting the "harmless-looking" pieces instead
#       (the normalisation first suggested in the issue) is NOT sound: an
#       unset `${C:-c}` and a bound `${C}` expand differently, and a pattern
#       can glob to a file literally named cd.
#
#   cd_word_class(toks, m, start) -- classifies the COMMAND POSITION of one
#       segment, starting at toks[start] (leading NAME=value words skipped):
#         "cd"       a straight-line cd; CDW_IDX is the index of the cd word,
#                    so the caller reads its operand from CDW_ARG (repo#604: cd
#                    options are skipped; an unprovable option/operand shape
#                    yields "unknown").
#         "unknown"  the cwd after this segment cannot be known statically:
#                    the command word itself is ambiguous (see above), or a
#                    cd sits behind a grouping / conditional / wrapper prefix
#                    (`(`, `{`, `!`, if/then/elif/else, while/until/do, time,
#                    builtin, command) whose scope or execution this
#                    control-flow-insensitive scan cannot follow, or the cd
#                    carries ANY redirection (repo#611 review: bash opens
#                    redirections BEFORE the builtin runs, and a failed open
#                    makes bash skip the cd entirely -- `</missing cd /tmp`
#                    leaves the cwd where it was -- so whether the cd ran
#                    depends on a file operation this scan cannot prove).
#                    The redirection targets of such a segment are still
#                    judged as writes against the PRE-cd cwd by the callers;
#                    only the post-segment cwd becomes unknown.
#         ""         not a directory change.
#
# Callers turn "unknown" into the unknown_cwd() sentinel. It never starts with
# `/`, so no consumer can mistake it for a real path, and it is STICKY: a
# relative cd from it stays unknown, only an absolute (or fresh-root `$NAME`)
# cd replaces it, and it can never inherit a previously safe temporary cwd.
# Each consumer fails closed on it (write-confinement denies a relative write,
# force-op and stash-scope ask).
#
# Words in ARGUMENT positions are never inspected here, so quoted data and
# comments that merely mention a disguised cd (`echo "c''d x"`) stay inert.
#
# Scope (deliberately NOT a general shell interpreter): repo#601 adds a small
# control-flow context on top of the recognizer (flow_* below). pushd, popd,
# eval, source, the dot builtin, case, and a function defined in the same
# command (and later called by name) make the cwd unknown, as does a cd inside
# a case arm / brace group / function body, behind an or-list, in a pipe
# stage or in a background job, and a cd that follows && once its list has
# ended (it may have been skipped). A cd and the operations
# gated by the same && chain stay tracked: those run only if it succeeded.
# Aliases, traps, and `exec` are not modelled.
#
# NOTE: this is a SINGLE-QUOTED awk program. Keep comments apostrophe-free.
# =============================================================================
_CDWORD_AWK='
function unknown_cwd() { return sprintf("%c", 14) "LOOM_UNKNOWN_CWD" }
function cmdword_static(tok,   n, i, c, nc, out, in_s, in_d, SQ, DQ, BQ) {
    SQ = sprintf("%c", 39)
    DQ = sprintf("%c", 34)
    BQ = sprintf("%c", 96)
    CMDW_UNKNOWN = 0
    if (tok == "[" || tok == "[[") return tok
    out = ""
    in_s = 0
    in_d = 0
    n = length(tok)
    for (i = 1; i <= n; i++) {
        c = substr(tok, i, 1)
        if (in_s) {
            if (c == SQ) in_s = 0
            else out = out c
            continue
        }
        if (in_d) {
            if (c == DQ) { in_d = 0; continue }
            if (c == "$" || c == BQ) { CMDW_UNKNOWN = 1; return "" }
            if (c == "\\") {
                if (i == n) { CMDW_UNKNOWN = 1; return "" }
                nc = substr(tok, i + 1, 1)
                if (nc == "\n") { i++; continue }
                if (nc == "$" || nc == BQ || nc == DQ || nc == "\\") { out = out nc; i++; continue }
                out = out c
                continue
            }
            out = out c
            continue
        }
        if (c == SQ) { in_s = 1; continue }
        if (c == DQ) { in_d = 1; continue }
        if (c == "\\") {
            if (i == n) { CMDW_UNKNOWN = 1; return "" }
            nc = substr(tok, i + 1, 1)
            i++
            if (nc == "\n") continue
            out = out nc
            continue
        }
        if (c == "$" || c == BQ || c == "*" || c == "?" || c == "[" || c == "{") {
            CMDW_UNKNOWN = 1
            return ""
        }
        out = out c
    }
    if (in_s || in_d) { CMDW_UNKNOWN = 1; return "" }
    return out
}
# redir_pos(tok) -- repo#604. 1-based position of the first UNQUOTED, unescaped
# < or > in tok, 0 when there is none. A redirection operator glued to the cd
# word (cd>f DIR) or to its operand (DIR>f) is syntax, not part of the name.
function redir_pos(tok,   n, i, c, in_s, in_d, SQ, DQ) {
    SQ = sprintf("%c", 39)
    DQ = sprintf("%c", 34)
    in_s = 0
    in_d = 0
    n = length(tok)
    for (i = 1; i <= n; i++) {
        c = substr(tok, i, 1)
        if (in_s) { if (c == SQ) in_s = 0; continue }
        if (in_d) {
            if (c == "\\") { i++; continue }
            if (c == DQ) in_d = 0
            continue
        }
        if (c == SQ) { in_s = 1; continue }
        if (c == DQ) { in_d = 1; continue }
        if (c == "\\") { i++; continue }
        if (c == "<" || c == ">") return i
    }
    return 0
}
# cd_redir_skip(toks, j, m) -- repo#604. When toks[j] is a redirection word
# (`>f`, `2>f`, `&>f`, `<f`, `{fd}>f`, `>&2`, `<<EOF`), return the index of the
# first word AFTER it, also skipping the separate target word of a bare
# operator (`>` `f`, `2>>` `f`). Return j unchanged when toks[j] is not a
# redirection and -1 when it is one whose extent this scan cannot prove (a
# process substitution target). Only an UNQUOTED leading operator counts, so
# quoted or escaped redirection-like data is never treated as syntax.
# repo#611 review: a caller that merely STEPS OVER a redirection word still
# cannot treat the cd as proven -- bash opens redirections before the builtin
# runs and a failed open skips the cd (`</missing cd /tmp` leaves the cwd
# alone). Every skip below therefore records CDW_REDIR = 1, which
# cd_word_class turns into the "unknown" class. This function only measures
# the EXTENT of a redirection word; it never vouches for the open succeeding.
function cd_redir_skip(toks, j, m,   w, r) {
    w = toks[j]
    if (w !~ /^([0-9]+|\{[A-Za-z_][A-Za-z0-9_]*\})?(&>|[<>])/) return j
    r = w
    sub(/^([0-9]+|\{[A-Za-z_][A-Za-z0-9_]*\})?/, "", r)
    sub(/^(&>>|&>|<<<|<<-|<<|<>|<&|>&|>>|>\||>|<)/, "", r)
    if (r ~ /^\(/) return -1
    if (r == "") {
        if (j + 1 > m) return -1
        return j + 2
    }
    return j + 1
}
# cd_skip_prefix(toks, j, m) -- skip any mix of NAME=value words and leading
# redirections (repo#604). Returns the index of the first other word, or -1.
# A skipped redirection sets CDW_REDIR: the cd behind it is unprovable
# (repo#611 review), even though its position is now known.
function cd_skip_prefix(toks, j, m,   nj) {
    while (j <= m) {
        if (toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*\+?=/) { j++; continue }
        nj = cd_redir_skip(toks, j, m)
        if (nj == -1) return -1
        if (nj != j) { CDW_LEAD = 1; CDW_REDIR = 1; j = nj; continue }
        break
    }
    return j
}
# cd_operand(toks, j, m) -- repo#604. j is the index of the cd word. Parse
# literal options (-L -P -e -@ and combinations, a terminating --) skipping
# redirection words, and set CDW_ARG to the operand word ("" when there is
# none). Returns 0 when the resulting directory cannot be proven (an unknown
# or non-literal option, an extra operand, an option without an operand, or a
# -P operand containing dot-dot whose physical meaning differs from the logical
# one, or a directory-stack operand +N / -N / ~+ / ~- (repo#626)), 1
# otherwise. After -- a leading dash is an operand.
function cd_operand(toks, j, m,   p, w, nj, opts, nopd, raw, physical, done) {
    CDW_ARG = ""
    opts = 0
    nopd = 0
    physical = 0
    done = 0
    p = j + 1
    while (p <= m) {
        w = toks[p]
        nj = cd_redir_skip(toks, p, m)
        if (nj == -1) {
            # A bare fd operator that ends the segment is the left half of a
            # `2>&1` / `>&2` that the segmenter cut at the ampersand: it has
            # no target to write. It is still a redirection whose success is
            # unprovable (fd 1 may be closed), so CDW_REDIR records it and
            # the cd stays unprovable (repo#611 review). Anything else
            # unprovable stays unknown.
            if (p == m && w ~ /^[0-9]*>>?$/) { CDW_REDIR = 1; p++; continue }
            return 0
        }
        if (nj != p) { CDW_REDIR = 1; p = nj; continue }
        if (w == "") { p++; continue }
        if (redir_pos(w) > 0) return 0
        if (!done) {
            if (w == "--") { done = 1; opts = 1; p++; continue }
            raw = w
            gsub(/[\\\047\042]/, "", raw)
            if (raw ~ /^-./) {
                # Only -L/-P are portable across bash 3.2, bash 5 and zsh. -e and -@
                # fail in some of them (cwd unchanged), so they are unprovable
                # (repo#611 review).
                if (w !~ /^-[LP]+$/) return 0
                if (w ~ /P/) physical = 1
                opts = 1
                p++
                continue
            }
        }
        nopd++
        if (nopd == 1) CDW_ARG = w
        p++
    }
    if (nopd > 1) return 0
    if (opts && nopd == 0) return 0
    if (physical && CDW_ARG ~ /\.\./) return 0
    if (CDW_ARG == "-") return 0
    # repo#626: in zsh (the default Bash-tool shell on macOS) `cd +N` and
    # `cd -N` select entry N of the directory stack, and zsh honors that even
    # when the word is quoted or follows `--`; `~+`, `~-`, `~+N` and `~-N`
    # are stack/OLDPWD tilde forms in both shells. The stack is invisible
    # here, so the resulting directory is unprovable -- never a relative
    # directory named `+1`. A literal directory of that name is `./+1`.
    raw = CDW_ARG
    gsub(/[\\\047\042]/, "", raw)
    if (raw ~ /^[+-][0-9]+$/) return 0
    if (raw ~ /^~[+-]/) return 0
    if (opts) CDW_LEAD = 1
    return 1
}
function cd_word_class(toks, m, start,   j, w, pre, cls) {
    CDW_IDX = 0
    CDW_ARG = ""
    CDW_LEAD = 0
    CDW_REDIR = 0
    pre = 0
    j = cd_skip_prefix(toks, start, m)
    if (j == -1) return "unknown"
    while (j <= m) {
        w = toks[j]
        while (substr(w, 1, 1) == "(") { w = substr(w, 2); pre = 1 }
        if (w == "") { j++; continue }
        if (w == "!" || w == "{" || w == "if" || w == "then" || w == "elif" || \
            w == "else" || w == "do" || w == "while" || w == "until" || \
            w == "time" || w == "builtin" || w == "command") {
            pre = 1
            j++
            if (w == "time" && j <= m && toks[j] == "-p") j++
            if (w == "command") {
                while (j <= m && toks[j] ~ /^-/) {
                    if (toks[j] ~ /[vV]/) return ""
                    j++
                }
            }
            j = cd_skip_prefix(toks, j, m)
            if (j == -1) return "unknown"
            continue
        }
        break
    }
    if (j > m) return ""
    if (w ~ /^[A-Za-z_][A-Za-z0-9_.:+@%-]*\(\)/ || (j < m && toks[j + 1] ~ /^\(\)/)) return "unknown"
    cls = cmdword_static(w)
    if (CMDW_UNKNOWN) return "unknown"
    if (cls == "pushd" || cls == "popd" || cls == "eval" || cls == "source" || \
        cls == "." || cls == "case" || cls == "function" || (cls in FN)) return "unknown"
    if (cls != "cd") {
        # repo#604: `cd>/dev/null DIR` is cd with a redirection glued on.
        if (redir_pos(w) > 1 && cmdword_static(substr(w, 1, redir_pos(w) - 1)) == "cd") return "unknown"
        return ""
    }
    if (pre) return "unknown"
    CDW_IDX = j
    if (!cd_operand(toks, j, m)) return "unknown"
    # repo#611 review: bash opens redirections BEFORE the builtin runs, and a
    # failed open makes bash skip the cd entirely (`</missing cd /tmp` leaves
    # the cwd where it was, while this tracker would have adopted /tmp). No
    # open is statically provable, so ANY redirection on the cd -- leading,
    # trailing, fd-dup, glued or separated -- keeps the post-segment cwd
    # unknown. The redirection targets themselves are still judged as writes
    # against the PRE-cd cwd by the callers.
    if (CDW_REDIR) return "unknown"
    return "cd"
}
# skip_assign_words(s) -- byte offset of the first word of segment s that is
# NOT a leading NAME=value assignment. A whitespace split cannot find it: in
# `X=$(basename $F) cmd` or `X=${A:-a b} cmd` the space inside the expansion
# does not end the assignment word, so naive tokens hand "$F)" or "b}" to the
# recognizer as if it were the command (a false unknown, or a missed cd). This
# walks quotes, $( ), ${ } and backticks the way bash delimits a word.
# Returns length(s) + 1 when nothing follows the assignments, including an
# unterminated construct (the rest of the segment is then not a command).
function skip_assign_words(s,   n, i, c, nx, lvl, q, qs, par, kind, SQ, DQ, BQ) {
    SQ = sprintf("%c", 39)
    DQ = sprintf("%c", 34)
    BQ = sprintf("%c", 96)
    n = length(s)
    i = 1
    while (1) {
        while (i <= n && (substr(s, i, 1) == " " || substr(s, i, 1) == "\t")) i++
        if (i > n) return n + 1
        if (substr(s, i) !~ /^[A-Za-z_][A-Za-z0-9_]*\+?=/) return i
        lvl = 0
        split("", qs)
        split("", par)
        split("", kind)
        qs[0] = ""
        while (i <= n) {
            c = substr(s, i, 1)
            nx = (i < n) ? substr(s, i + 1, 1) : ""
            q = qs[lvl]
            if (lvl == 0 && q == "" && (c == " " || c == "\t")) break
            if (q == SQ) { if (c == SQ) qs[lvl] = ""; i++; continue }
            if (c == "\\") { i += 2; continue }
            if (c == BQ && lvl > 0 && kind[lvl] == "B") { lvl--; i++; continue }
            if (q == DQ) {
                if (c == DQ) { qs[lvl] = ""; i++; continue }
            } else {
                if (c == SQ) { qs[lvl] = SQ; i++; continue }
                if (c == DQ) { qs[lvl] = DQ; i++; continue }
                if (lvl > 0 && kind[lvl] == "P") {
                    if (c == "(") { par[lvl]++; i++; continue }
                    if (c == ")") { if (par[lvl] > 0) par[lvl]--; else lvl--; i++; continue }
                }
                if (lvl > 0 && kind[lvl] == "C") {
                    if (c == "{") { par[lvl]++; i++; continue }
                    if (c == "}") { if (par[lvl] > 0) par[lvl]--; else lvl--; i++; continue }
                }
            }
            if (c == "$" && (nx == "(" || nx == "{")) {
                lvl++
                qs[lvl] = ""
                par[lvl] = 0
                kind[lvl] = (nx == "(") ? "P" : "C"
                i += 2
                continue
            }
            if (c == BQ) {
                lvl++
                qs[lvl] = ""
                par[lvl] = 0
                kind[lvl] = "B"
                i++
                continue
            }
            i++
        }
        if (i > n) return n + 1
    }
}
# cd_seg_class(mseg) -- classify one segment (leading whitespace and sudo
# already stripped; whitespace inside quotes masked by mask_ws() when the
# caller has it, raw otherwise). Same result as cd_word_class(); the words
# from the command word on are left in the global CT[] (unmasked), so for "cd"
# the operand is CDW_ARG.
function cd_seg_class(mseg,   off, cm, j) {
    split("", CT)
    off = skip_assign_words(mseg)
    if (off > length(mseg)) return ""
    cm = split(substr(mseg, off), CT, /[ \t]+/)
    if (cm < 1) return ""
    for (j = 1; j <= cm; j++) CT[j] = cmdw_unmask(CT[j])
    return cd_word_class(CT, cm, 1)
}
# ---- shell word splitting + static path decoding for git -C (repo#608) ----
# shell_words(s, W) -- split one segment into shell WORDS the way bash
# delimits them, keeping each word RAW (quotes and backslashes intact) in
# W[1..n]; returns n. A plain whitespace split shreds a quoted or
# backslash-escaped path that contains a space (git -C "/a b" reset --hard
# became -C "/a, then b" was read as the subcommand and the force op vanished
# from the scan). Unquoted blanks end a word; blanks inside single quotes,
# double quotes, a backslash escape, $( ), ${ } or backticks do not. An
# unterminated construct simply runs to the end of the segment. Nothing is
# expanded or evaluated.
function shell_words(s, W,   n, i, c, nx, w, cnt, inw, lvl, q, qs, par, kind, SQ, DQ, BQ) {
    SQ = sprintf("%c", 39)
    DQ = sprintf("%c", 34)
    BQ = sprintf("%c", 96)
    split("", W)
    split("", qs)
    split("", par)
    split("", kind)
    n = length(s)
    cnt = 0
    w = ""
    inw = 0
    lvl = 0
    qs[0] = ""
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        nx = (i < n) ? substr(s, i + 1, 1) : ""
        q = qs[lvl]
        if (lvl == 0 && q == "" && (c == " " || c == "\t")) {
            if (inw) { W[++cnt] = w; w = ""; inw = 0 }
            continue
        }
        inw = 1
        if (q == SQ) { if (c == SQ) qs[lvl] = ""; w = w c; continue }
        if (c == "\\") { w = w c nx; i++; continue }
        if (c == BQ && lvl > 0 && kind[lvl] == "B" && q == "") { lvl--; w = w c; continue }
        if (q == DQ) {
            if (c == DQ) { qs[lvl] = ""; w = w c; continue }
        } else {
            if (c == SQ) { qs[lvl] = SQ; w = w c; continue }
            if (c == DQ) { qs[lvl] = DQ; w = w c; continue }
            if (lvl > 0 && kind[lvl] == "P") {
                if (c == "(") { par[lvl]++; w = w c; continue }
                if (c == ")") { if (par[lvl] > 0) par[lvl]--; else lvl--; w = w c; continue }
            }
            if (lvl > 0 && kind[lvl] == "C") {
                if (c == "{") { par[lvl]++; w = w c; continue }
                if (c == "}") { if (par[lvl] > 0) par[lvl]--; else lvl--; w = w c; continue }
            }
        }
        if (c == "$" && (nx == "(" || nx == "{")) {
            lvl++
            qs[lvl] = ""
            par[lvl] = 0
            kind[lvl] = (nx == "(") ? "P" : "C"
            w = w c nx
            i++
            continue
        }
        if (c == BQ) {
            lvl++
            qs[lvl] = ""
            par[lvl] = 0
            kind[lvl] = "B"
            w = w c
            continue
        }
        w = w c
    }
    if (inw) W[++cnt] = w
    # SW_OPEN: the segment ended inside a quote or substitution, so the last
    # word continues past it (the caller may rejoin following segments).
    SW_OPEN = (lvl > 0 || qs[lvl] != "")
    return cnt
}
# static_path_word(tok, home) -- the literal directory a git -C operand names,
# or unknown_cwd() when that cannot be proven statically. Quote removal and
# backslash escapes follow cmdword_static() exactly (so any dollar expansion,
# backtick, unquoted glob or brace character, or unbalanced quote is unknown).
# A leading unquoted tilde or HOME reference is expanded from the guard process
# own HOME (same narrow slice as expand_cd_arg); any other tilde form (a named
# user home, or no HOME) is unknown. A result holding a newline or one of the
# record separator bytes this file emits is unknown too, so it can never split
# or forge an output record. The empty string means an empty operand, which git
# treats as "stay in the current directory".
function static_path_word(tok, home,   pre, rest, d) {
    pre = ""
    rest = tok
    if (substr(tok, 1, 1) == "~") {
        if (home == "" || (tok != "~" && substr(tok, 1, 2) != "~/")) return unknown_cwd()
        pre = home
        rest = substr(tok, 2)
    } else if (substr(tok, 1, 5) == "$HOME" && (length(tok) == 5 || substr(tok, 6, 1) == "/")) {
        if (home == "") return unknown_cwd()
        pre = home
        rest = substr(tok, 6)
    }
    d = (rest == "") ? "" : cmdword_static(rest)
    if (rest != "" && CMDW_UNKNOWN) return unknown_cwd()
    d = pre d
    if (index(d, "\n") || index(d, sprintf("%c", 31)) || index(d, sprintf("%c", 14))) return unknown_cwd()
    return d
}
# cd_unreached(cdarg, base, ka, home) -- repo#625: 1 when a statically known
# cd target is NOT proven to have been reached. A failed cd leaves the shell in
# its old directory, so after `cd /missing; cmd` (or newline / ||) the next
# command runs in the OLD cwd. A cd joined to the next command by && is safe
# (the next command never runs after a failed cd). Otherwise the target counts
# as reached only when it exists, checked on the literal path (a relative
# operand is joined onto base, which must itself be an absolute known cwd).
# An operand that is not statically decodable is left to the existing
# unknown/fresh-root handling (returns 0). The path goes to test as a single
# quoted argument, never through eval.
function cd_unreached(cdarg, base, ka, home,   d, q, sq) {
    if (ka == "A") return 0
    d = static_path_word(cdarg, home)
    if (d == "" || d == unknown_cwd()) return 0
    if (substr(d, 1, 1) != "/") {
        if (substr(base, 1, 1) != "/") return 0
        d = base "/" d
    }
    sq = sprintf("%c", 39)
    q = d
    gsub(sq, sq "\\" sq sq, q)
    return (system("test -d " sq q sq) != 0)
}
function cmdw_unmask(s) {
    gsub(sprintf("%c", 2), " ", s)
    gsub(sprintf("%c", 3), "\t", s)
    return s
}
# ---- control-flow context for the cwd trackers (repo#601) ----------------
# Segment KINDS (what separator introduced a segment): N start/newline,
# S semicolon, A and-and, O or-or, P pipe, B background ampersand, R a
# redirection ampersand (2>&1 or &>f, not a separator), H a command
# substitution head (runs in a subshell, never alters the cwd flow).
# qsplit() hands them over as an ENQ byte plus a letter when QS_MARK is set;
# ml_segment() hands over separator offsets, see flow_ml_kinds().
#
# flow_unmark(s) strips the marker and leaves the kind in FL_KIND.
function flow_unmark(s,   mk) {
    mk = sprintf("%c", 5)
    if (substr(s, 1, 1) == mk) { FL_KIND = substr(s, 2, 1); return substr(s, 3) }
    FL_KIND = "N"
    return s
}
# flow_ml_kinds(buf, n, K) -- K[k] = kind of the separator in front of
# segment k of the ml_segment() result just produced for buf. ml_segment()
# emits ONE segment per separator byte, so a two byte && or || leaves an
# empty segment between its bytes; both halves get the pair kind.
function flow_ml_kinds(buf, n, K,   k, p, ch, nx, pv) {
    K[1] = "N"
    for (k = 2; k <= n; k++) {
        p = ml_segsep[k - 1]
        ch = substr(buf, p, 1)
        nx = substr(buf, p + 1, 1)
        pv = substr(buf, p - 1, 1)
        if (ch == ";") K[k] = "S"
        else if (ch == "&") {
            if (nx == "&" || (pv == "&" && k > 2 && ml_segsep[k - 2] == p - 1)) K[k] = "A"
            else if (pv ~ /[<>]/ || nx == ">") K[k] = "R"
            else K[k] = "B"
        } else if (ch == "|") {
            if (nx == "|" || (pv == "|" && k > 2 && ml_segsep[k - 2] == p - 1)) K[k] = "O"
            else K[k] = "P"
        } else K[k] = "N"
    }
}
# flow_next_kind(K, i, n) -- kind of the separator after segment i, looking
# past redirection fragments. "" at the end of the command.
function flow_next_kind(K, i, n,   j) {
    for (j = i + 1; j <= n; j++) {
        if (K[j] == "R") continue
        if (K[j] == "H") return ""
        return K[j]
    }
    return ""
}
# flow_begin(kind, seg) -- call for every segment, in order, BEFORE its cwd
# is used (seg already trimmed). Returns 1 when the cwd is no longer known
# at this point: a new list begins after a conditional directory change
# (the change may have been skipped or may have failed), or an or-branch
# follows a list that changed directory (the branch runs only when that
# change failed). `|| exit` is the one recovery whose cwd is proven: the
# shell is gone before any later segment runs. FL_EFF is the effective kind.
function flow_begin(kind, seg,   r) {
    r = 0
    FL_EFF = kind
    if (kind == "H") return 0
    if (seg ~ /^[ \t]*$/) {
        if (kind == "A" || kind == "O" || kind == "P") FL_CARRY = kind
        return 0
    }
    if (kind == "N" && FL_CARRY != "") { kind = FL_CARRY; FL_EFF = kind }
    FL_CARRY = ""
    if (kind == "A" || kind == "P" || kind == "R") return 0
    if (kind == "O") {
        if (FL_LISTCD && seg !~ /^[ \t]*(sudo[ \t]+)?exit([ \t]|$)/) { FL_COND = 1; r = 1 }
        return r
    }
    if (FL_COND) r = 1
    FL_COND = 0
    FL_LISTCD = 0
    return r
}
# flow_cd(kb, ka) -- a cwd-changing command position was found (class cd or
# unknown). kb/ka are the kinds before/after it. Returns 1 when the change
# must NOT be applied as a known cwd: it sits inside a case arm / function
# body / brace group, behind an or-list, in a subshell pipe stage or in a
# background job. A change that follows && may have been skipped: it stays
# tracked for the && chain it gates (those operations run only if it ran) but
# marks the list conditional, so the cwd is unknown once the list ends. A
# leading cd followed by && is treated like a plain cd (its failure is the
# same accepted assumption as for `cd X; cmd`), except that an or-branch
# after it is ambiguous (see flow_begin).
function flow_cd(kb, ka) {
    if (kb == "H") return 0
    FL_LISTCD = 1
    if (FL_OPQ > 0) return 1
    if (kb == "O" || kb == "P" || ka == "P" || ka == "B") { FL_COND = 1; return 1 }
    if (kb == "A") FL_COND = 1
    return 0
}
# flow_track(seg) -- opaque-block depth and same-command function names.
# Inside a case statement, a brace group or a function body a directory
# change is conditional or deferred, so none is trusted until the block
# closes. A defined function name becomes an unknown command word.
function flow_track(seg,   w, nm) {
    w = seg
    sub(/^[ \t]+/, "", w)
    if (w ~ /^function[ \t]+[A-Za-z_]/) {
        nm = w
        sub(/^function[ \t]+/, "", nm)
        sub(/[^A-Za-z0-9_.:+@%-].*$/, "", nm)
        FN[nm] = 1
        FL_OPQ++
    } else if (w ~ /^[A-Za-z_][A-Za-z0-9_.:+@%-]*[ \t]*\(\)/) {
        nm = w
        sub(/[ \t]*\(\).*$/, "", nm)
        FN[nm] = 1
        FL_OPQ++
    } else if (w ~ /^(case|\{)([ \t]|$)/) FL_OPQ++
    else if (w ~ /^(esac|\})([ \t]|$)/) { if (FL_OPQ > 0) FL_OPQ-- }
}
'
# Shell-side spelling of unknown_cwd() above -- the two MUST stay byte-equal.
_LOOM_UNKNOWN_CWD=$'\016LOOM_UNKNOWN_CWD'
# Shell-side spelling of parse_force_ops' FO_UNDEC sentinel (repo#612) -- the
# two MUST stay byte-equal.
_LOOM_UNDECODABLE_FORCE_OP=$'\016LOOM_UNDECODABLE_FORCE_OP'

# cmd_has_unproven_cwd_change TEXT -- succeed when ANY segment of TEXT
# (including the first command of every $( )/backtick substitution) has a
# command position the shared recognizer classifies as "unknown", or a cd
# spelled any way other than the literal word `cd`. Used by the repo#597
# single-cd proof below, which may only ever admit literal, straight-line cd
# steps (repo#600). An awk failure counts as "found" (fail closed).
cmd_has_unproven_cwd_change() {
    local verdict
    verdict=$(printf '%s' "$1" | awk "$_ESCAPE_AWK""$_HASLIVESUBST_AWK""$_QSPLIT_AWK""$_CDWORD_AWK""$_MASKWS_AWK"'
    function scan_segments(text,   n, segs, i, seg, k) {
        n = split(text, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            sub(/^sudo[ \t]+/, "", seg)
            sub(/^[ \t]+/, "", seg)
            if (seg == "") continue
            k = cd_seg_class(seg)
            if (k == "unknown") return 1
            if (k == "cd" && (CT[CDW_IDX] != "cd" || CDW_LEAD)) return 1
        }
        return 0
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        HSEP = sprintf("%c", 30)
        bad = scan_segments(mask_ws(qsplit(buf)))
        nh = split(subst_heads(buf, HSEP), heads, HSEP)
        for (h = 2; h <= nh && !bad; h++) bad = scan_segments(mask_ws(heads[h]))
        print (bad ? "FOUND" : "NONE")
    }') || verdict="FOUND"
    [[ "$verdict" != "NONE" ]]
}

# =============================================================================
# QUOTE-AWARE REDIRECTION MASKING (#4245)
#
# extract_write_targets() (below) recognizes `>`/`>>` redirection by splitting
# a qsplit()-segmented command on whitespace and pattern-matching each token —
# but that whitespace split is NOT quote-aware, so a `>` that is DATA inside a
# quoted argument (e.g. `gh issue create --body "... env > config > default
# ..."`) can land in its own whitespace-bounded "token" and be misread as a
# real redirection operator, manufacturing a phantom write target and denying
# a command that writes nothing to the filesystem (#4245; same failure class
# as the #3755 qsplit()/#3679 strip_literal_text() quoting fixes above).
#
# mask_gt() walks the string tracking quote state (single-quoted,
# double-quoted, unquoted) and replaces every `>` found INSIDE a quoted span
# with SOH (0x01, a character that can never appear in a shell command and so
# can never itself be mis-split into a phantom target). It otherwise returns
# the input UNCHANGED byte-for-byte (same length, same whitespace positions),
# so a caller can split both the original and the masked string on whitespace
# and get IDENTICAL token boundaries — the masked tokens are used only to
# DECIDE whether a token is a real (unquoted) redirection operator; the
# ORIGINAL tokens are still used to extract the actual target text.
#
# ESCAPE PARITY (#548). This scan used to be deliberately escape-BLIND, on the
# argument that its input has already been through other escape-blind passes and
# that a stricter scan layered on top would only desynchronize their parity. It
# is now escape-AWARE, in exactly the two places where bash itself is, because
# blindness here was a confinement BYPASS rather than a mere simplification:
#   - a backslash-escaped quote does not OPEN a span (`echo \" && cp …
#     <main>/f; echo "z"` paired the escaped quote with the one in the trailing
#     `echo "z"` and masked the whole write out of the whitespace split);
#   - inside a DOUBLE-quoted span a `\"` does not CLOSE it (`echo "$(printf
#     \"x )" && cp … <main>/f; echo "z"` ended the span at the `\"`, re-opened
#     at the next `"`, and masked the separator and target the same way).
# Both really write into the main checkout under bash, and both ALLOWED.
# A SINGLE-quoted span keeps the next-quote rule: between `S…S` a backslash is
# an ordinary literal byte, so its next `S` genuinely is the close, and skipping
# it would mask live code. With those two rules the span this scan sees is the
# span bash sees for any parseable input, which is also what keeps it in parity
# with qsplit()/ml_segment()/strip_datasink_literals() — all of which now apply
# the same two rules — rather than merely in parity with their old blindness.
# Where an upstream redaction has already REMOVED a backslash (strip_literal_text()
# blanking a `--body "…\"…"` value to `X`s) there is no escape left to see, so
# the two passes still agree byte for byte. Same accepted risk direction as
# qsplit(): pathological unbalanced-quote input could in theory shift parity and
# mis-mask a genuine unquoted `>`, but that is the same best-effort risk this
# file already accepts for `;|&` segmentation -- never a NEW risk introduced
# here. An unterminated quote (no matching close before end-of-string) just runs
# to the end of the string in that quote state -- never crashes, never
# mis-indexes.
# =============================================================================
_MASKGT_AWK='
function mask_gt(s,   out, n, i, c, mode, SQ, DQ, MASK) {
    SQ = sprintf("%c", 39)    # single quote
    DQ = sprintf("%c", 34)    # double quote
    MASK = sprintf("%c", 1)   # SOH -- placeholder for a quoted ">" (never a real char)
    out = ""
    n = length(s)
    i = 1
    mode = 0   # 0 = unquoted, 1 = single-quoted, 2 = double-quoted
    while (i <= n) {
        c = substr(s, i, 1)
        if (mode == 0) {
            # An escaped quote is literal text, not an opener (#548).
            if (c == SQ && !bs_escaped(s, i)) { mode = 1; out = out c; i++; continue }
            if (c == DQ && !bs_escaped(s, i)) { mode = 2; out = out c; i++; continue }
            out = out c
            i++
            continue
        }
        if (mode == 1) {
            # Single-quoted: only the matching quote ends the span, escaped or
            # not -- a backslash between S...S is an ordinary literal byte.
            if (c == SQ) { mode = 0; out = out c; i++; continue }
            out = out (c == ">" ? MASK : c)
            i++
            continue
        }
        # mode == 2 (double-quoted): the matching quote ends the span unless it
        # is backslash-escaped, in which case bash reads it as literal text and
        # the span runs on (#548).
        if (c == DQ && !bs_escaped(s, i)) { mode = 0; out = out c; i++; continue }
        out = out (c == ">" ? MASK : c)
        i++
    }
    return out
}
'

# =============================================================================
# QUOTE-AWARE WHITESPACE MASKING (#4934)
#
# extract_write_targets() (below) recognizes each write-idiom argument by
# `split(seg, toks, /[ \t]+/)` — plain whitespace splitting, NOT quote-aware
# (documented as a known limitation at extract_write_targets()'s own header).
# A quoted target containing a literal space, e.g.
#   echo x > '/main/checkout/evil file.sh'
# is therefore split into TWO tokens (`'/main/checkout/evil` and `file.sh'`).
# Only the first fragment is ever used as the write target. That fragment
# starts with a quote character (not `/`), so it is misclassified as a
# RELATIVE path (strip_target_quoting() correctly reports the dangling quote
# as unbalanced and falls back to the raw fragment per #4926's "never widen a
# deny into an allow" contract -- but the fallback fragment itself still gets
# cwd-joined and can land INSIDE the acting worktree, turning what should be
# a main-checkout DENY into an ALLOW (#4934).
#
# mask_ws() is the same masking technique as mask_gt() above (#4245), applied
# to whitespace instead of `>`: it walks the string tracking quote state and
# replaces every space/tab found INSIDE a quoted span with a placeholder
# character that can never appear in real shell text (STX 0x02 for a masked
# space, ETX 0x03 for a masked tab), so `split(seg, toks, /[ \t]+/)` never
# splits inside a quoted span -- a quoted path containing spaces now yields
# exactly ONE token. unmask_ws() reverses the substitution so the token's
# TEXT is unchanged (real spaces/tabs restored) once splitting is done; only
# the whitespace bytes INSIDE quotes are ever touched, so mask_ws() (like
# mask_gt()) returns a byte-for-byte-length-identical string with identical
# non-whitespace content, which is what keeps the `>`-detection pass (mtoks[],
# masked via mask_gt() on top of mask_ws()'s output) in lockstep with the
# target-text pass (toks[], unmasked back to real whitespace): the two are
# always split into the SAME number of tokens at the SAME boundaries, because
# mask_gt() only ever changes `>` bytes, never whitespace-ness.
#
# Models backslash-escaped quotes exactly as mask_gt() above does (#548 — an
# escaped quote opens nothing, and a `\"` inside a DOUBLE-quoted span does not
# close it, while a SINGLE-quoted span still ends at its next quote); see
# mask_gt()'s ESCAPE PARITY note for why that blindness was a write-confinement
# bypass here rather than a safe simplification. It deliberately does NOT
# attempt look-ahead for a terminating quote: an unterminated quote just runs to
# the end of the string in that quote state; never crashes, never mis-indexes,
# and never widens a deny into an allow (the SAME fallback direction qsplit()'s
# own unterminated-quote handling already uses, #4926).
#
# This is scoped ONLY to extract_write_targets() -- qsplit() itself (and its
# verbatim-quote-preservation contract depended on by extract_rm_targets() /
# parse_force_ops()) is untouched.
# =============================================================================
_MASKWS_AWK='
function mask_ws(s,   out, n, i, c, mode, SQ, DQ, SPMASK, TABMASK) {
    SQ = sprintf("%c", 39)    # single quote
    DQ = sprintf("%c", 34)    # double quote
    SPMASK = sprintf("%c", 2)    # STX -- placeholder for a quoted space
    TABMASK = sprintf("%c", 3)   # ETX -- placeholder for a quoted tab
    out = ""
    n = length(s)
    i = 1
    mode = 0   # 0 = unquoted, 1 = single-quoted, 2 = double-quoted
    while (i <= n) {
        c = substr(s, i, 1)
        if (mode == 0) {
            # An escaped quote is literal text, not an opener (#548).
            if (c == SQ && !bs_escaped(s, i)) { mode = 1; out = out c; i++; continue }
            if (c == DQ && !bs_escaped(s, i)) { mode = 2; out = out c; i++; continue }
            out = out c
            i++
            continue
        }
        if (mode == 1) {
            # Single-quoted: only the matching quote ends the span, escaped or
            # not -- a backslash between S...S is an ordinary literal byte.
            if (c == SQ) { mode = 0; out = out c; i++; continue }
            if (c == " ") { out = out SPMASK; i++; continue }
            if (c == "\t") { out = out TABMASK; i++; continue }
            out = out c
            i++
            continue
        }
        # mode == 2 (double-quoted): the matching quote ends the span unless it
        # is backslash-escaped (#548).
        if (c == DQ && !bs_escaped(s, i)) { mode = 0; out = out c; i++; continue }
        if (c == " ") { out = out SPMASK; i++; continue }
        if (c == "\t") { out = out TABMASK; i++; continue }
        out = out c
        i++
    }
    return out
}
function unmask_ws(s) {
    gsub(sprintf("%c", 2), " ", s)
    gsub(sprintf("%c", 3), "\t", s)
    return s
}
'

# =============================================================================
# HEREDOC-BODY MASKING (#5000)
#
# extract_write_targets() (below) is fed the RAW, un-redacted value of a
# --body/-m/--title/--notes/--comment flag whenever strip_literal_text()'s own
# `$(`/backtick safety floor (#3679) declines to redact it -- the common
# real-world trigger being a heredoc-wrapped value, `--body "$(cat <<'EOF'
# ... EOF)"`, this repo's OWN recommended idiom (see CLAUDE.md/builder role)
# for any multi-line/special-character body text. A `>` (or `;`, `&`, `|`, or
# a write-idiom command word like `tee`) sitting on a heredoc BODY line is
# inert DATA *to the OUTER shell* -- the outer shell never shell-parses a
# heredoc body for redirection/separator syntax, regardless of whether its
# delimiter is quoted (see KNOWN LIMITATIONS below for two narrow cases where
# that is not the end of the story) -- but qsplit()/mask_gt()/mask_ws() are
# (like awk itself) driven one
# PHYSICAL LINE at a time, with no memory of the `"` opened several lines
# earlier once a later heredoc-body line is reached, so a write-idiom-looking
# byte on such a line was misread as real shell syntax, manufacturing a
# phantom write target and denying a command that writes nothing to the
# filesystem (#5000; same failure family as #4245/#3679, one line-boundary
# narrower).
#
# mask_heredoc_bodies() sidesteps that per-line memory gap entirely rather
# than teaching qsplit()/mask_gt()/mask_ws() cross-line state -- those three
# are SHARED with extract_rm_targets()/parse_force_ops()/
# lifecycle_or_cloud_reason(), out of THIS fix's scope (#5000's own Affected
# Files list names only strip_literal_text() and extract_write_targets()/
# mask_gt()). It walks the WHOLE (possibly multi-line) buffer ONCE, looking
# for a `<<`/`<<-` heredoc opener whose delimiter is a bare or single/double
# -quoted identifier (`EOF`, `'EOF'`, `"EOF"`, ... -- the near-universal
# real-world shape), then replaces every byte of the BODY -- every full line
# strictly between the opener line and the first following line that is
# exactly (barring `<<-`'s permitted leading tabs) the bare delimiter -- with
# a neutral placeholder byte (ETB, 0x17: never meaningful shell syntax, never
# matched by any pattern elsewhere in this file). Real newlines, the opener
# line, and the delimiter line itself are all left untouched, so line counts
# and the surrounding qsplit()/strip_literal_text() `$(`-floor logic are
# unaffected -- ONLY the inert body content disappears.
#
# MASK ONLY A *CLOSED* BLOCK (#5087 -- the fail-open regression this two-pass
# structure exists to prevent). The masking decision is made per candidate
# opener with the closing-delimiter line ALREADY LOCATED: for each candidate
# on a line, a forward scan looks for the terminating bare-delimiter line
# FIRST, and only a block that is genuinely closed inside this buffer has its
# body masked. A candidate whose delimiter line never appears masks NOTHING
# and the scan simply moves on to the next candidate -- because the original
# single-pass form (which flipped a sticky `inbody` flag the instant it saw
# `<<` and only ever cleared it on a delimiter line) silently masked
# EVERYTHING from a FALSE opener to the end of the command whenever no such
# line followed, swallowing any real `>`/`tee`/`cp`/`mv` target after it and
# defeating the write-confinement guard (#4178) outright. Two ordinary,
# heredoc-free command shapes hit that: a quoted string that merely CONTAINS
# `<<TOKEN` (`echo "test <<TOKEN"`), and an arithmetic bitshift
# (`x=$((1 << 3))`) -- both followed by a genuine out-of-worktree write, both
# ALLOWed pre-#5087 where `main` DENIed. Masking is a NARROWING operation, so
# it must never be applied speculatively: when in doubt, mask nothing and let
# the text flow through the pre-#5000 per-line scan.
#
# Opener detection is correspondingly tightened so the commonest false
# openers are rejected before the forward scan even runs: a `<<<` herestring
# is never a heredoc opener, and a BARE (unquoted) delimiter starting with a
# DIGIT is read as an arithmetic shift operand (`1 << 3`), not a delimiter --
# `<<'3'`/`<<"3"` stay recognized, since an explicitly quoted delimiter is
# unambiguous heredoc intent. Every `<<` occurrence on a line is considered
# in turn (not just the first), so one rejected candidate never hides a real
# terminated heredoc later on the same line.
#
# Deliberately narrow / best-effort, consistent with every other quote-
# tracking scan in this file: an exotic delimiter (not a bare identifier, or
# using shell metacharacters) is simply not recognized as a heredoc opener --
# fail-open for THIS masking pass only, never a NEW risk, since the text then
# flows through the pre-#5000 per-line scan exactly as it always has. Never
# denies by itself; only ever narrows what extract_write_targets() can find,
# matching that scanner's own documented fail-open contract (a missed target
# is the accepted safe direction there) -- a real write-idiom byte OUTSIDE
# any recognized heredoc body, even in the SAME multi-line command, is
# completely unaffected and still flows through unchanged.
#
# KNOWN LIMITATIONS (#5117 -- surfaced during Judge re-review of #5085, left
# in place deliberately rather than folded into that fix):
#
#   1. Interpreter-fed heredocs -- CLOSED for heredocs (#5351), broader
#      interpreter-mediated writes still open. "Inert to the outer shell"
#      (above) is NOT the same as "inert, full stop." When the heredoc body IS
#      the script handed to an interpreter -- `bash <<'EOF' ... EOF`,
#      `cat <<'EOF' ... EOF | bash`, `sh -s <<'EOF' ... EOF` -- a write-idiom
#      line inside that body is genuinely live code to the INNER interpreter,
#      even though the outer shell never parses it as redirection/separator
#      syntax. Plain mask_heredoc_bodies() masks it anyway, so a write that
#      `origin/main`'s single-pass scan correctly caught would be missed.
#      ORIGINAL DECISION (#5117): deferred -- extract_write_targets() kept
#      calling plain mask_heredoc_bodies() and masked interpreter-fed bodies,
#      an accepted ask-tier tradeoff (missed ASK, worst case), while #5198
#      introduced mask_heredoc_bodies_selective() for the CATASTROPHIC tier
#      only (masking an interpreter-fed body there flips a DENY to an ALLOW on
#      the #4523/#4601/#4685 data-loss shape -- never acceptable).
#      UPDATED DECISION (#5351): the deferral no longer stands. The catastrophic
#      tier proved the approach, so extract_write_targets() now ALSO calls
#      mask_heredoc_bodies_selective() (see its END block below) -- both tiers
#      share the same interpreter-aware masking. _selective() recognizes an
#      interpreter-fed opener and leaves that block's body VISIBLE to the scan
#      (so a write into the main checkout inside a `bash <<'EOF' ... EOF` body
#      now DENYs from a managed worktree), while still masking every
#      non-interpreter-fed heredoc in the same command -- so the #4914/#5000/
#      #5181 false-positive fixes (an inert `cat`-body / `--body "$(cat <<'EOF'
#      ... EOF)"` sink) stay intact.
#      STILL OPEN (its own follow-up, NOT closed here): the BROADER,
#      heredoc-independent class of interpreter-mediated writes -- `bash -c
#      '... > f'`, `printf ... | bash`, `dd of=f`, `install -m ... f` -- which
#      extract_write_targets(), a command-word-based scanner, still does not
#      cover regardless of heredocs. Closing that (spotting an inner interpreter
#      invocation and recursively re-scanning its script/stdin argument) is a
#      materially larger, separate piece of work than the heredoc masking pass
#      and is deliberately out of scope for #5351.
#
#   2. Crafted false opener whose delimiter later appears. Opener detection
#      (heredoc_delim_at()) runs on a single physical line, before qsplit()
#      -- it cannot know a `<<TOKEN` substring actually sits inside a quoted
#      string on that line (e.g. `echo "test <<EOF" > /etc/passwd`). If a
#      later line in the SAME buffer happens to equal the bare delimiter
#      (`EOF`) for unrelated reasons, PASS 1 finds it and PASS 2 masks every
#      line in between -- even though real bash treats the whole `<<EOF`
#      substring as quoted text (no heredoc at all) and executes the write
#      immediately. `origin/main` denies this; this masking pass ALLOWs it.
#      This is explicitly NOT fixed here: doing so would require teaching
#      heredoc_delim_at() the same quote state qsplit() tracks, and
#      qsplit()/mask_gt()/mask_ws() are SHARED with extract_rm_targets()/
#      parse_force_ops()/lifecycle_or_cloud_reason() -- exactly the
#      cross-function coupling #5000 deliberately avoided by giving
#      mask_heredoc_bodies() its own single, whole-buffer pre-pass instead of
#      threading state through the shared per-line scanners. A structural
#      fix belongs in its own issue, scoped against that coupling risk, not
#      folded in here.
# =============================================================================
_MASKHEREDOC_AWK='
# Return the heredoc delimiter opened by the `<<` at byte offset p in line,
# or "" when that `<<` is not a recognized heredoc opener.
function heredoc_delim_at(line, p,   start, qc, c, wordend, d, SQ, DQ) {
    SQ = sprintf("%c", 39)    # single quote
    DQ = sprintf("%c", 34)    # double quote
    start = p + 2
    # `<<<` is a herestring, never a heredoc opener.
    if (substr(line, start, 1) == "<") return ""
    if (substr(line, start, 1) == "-") start++
    while (substr(line, start, 1) == " " || substr(line, start, 1) == "\t") start++
    qc = ""
    c = substr(line, start, 1)
    if (c == SQ || c == DQ) { qc = c; start++ }
    wordend = start
    while (1) {
        c = substr(line, wordend, 1)
        if (c ~ /^[A-Za-z0-9_]$/) { wordend++; continue }
        break
    }
    if (wordend <= start) return ""
    d = substr(line, start, wordend - start)
    # A BARE delimiter starting with a digit is an arithmetic shift operand
    # (`$((1 << 3))`) far more often than a real heredoc delimiter. A quoted
    # one (`<<"3"`) is unambiguous heredoc intent, so it stays recognized.
    if (qc == "" && d ~ /^[0-9]/) return ""
    return d
}
function mask_heredoc_bodies(s,   out, lines, nl, i, j, line, trimmed, body, delim, closeat, p, off, MASKC) {
    MASKC = sprintf("%c", 23) # ETB -- placeholder for inert heredoc-body text
    nl = split(s, lines, "\n")
    if (nl == 0) return ""
    for (i = 1; i <= nl; i++) {
        line = lines[i]
        off = 1
        # Consider every `<<` on this line, left to right, until one is
        # confirmed to open a CLOSED heredoc block.
        while (1) {
            p = index(substr(line, off), "<<")
            if (p == 0) break
            p = off + p - 1        # absolute offset of `<<` within line
            off = p + 2            # where the next candidate search resumes
            delim = heredoc_delim_at(line, p)
            if (delim == "") continue
            # PASS 1 -- locate the terminating delimiter line. A `<<-` opener
            # permits (and strips) leading tabs on the delimiter line; only
            # leading TABS (never spaces) are ever stripped, per real heredoc
            # semantics. Stripping them unconditionally can only terminate the
            # block EARLIER, i.e. mask LESS -- the safe direction here.
            closeat = 0
            for (j = i + 1; j <= nl; j++) {
                trimmed = lines[j]
                sub(/^\t+/, "", trimmed)
                if (trimmed == delim) { closeat = j; break }
            }
            # Unterminated / false opener: mask NOTHING for this candidate and
            # keep looking. Everything after it stays visible to the caller,
            # exactly as in the pre-#5000 per-line scan (#5087).
            if (closeat == 0) continue
            # PASS 2 -- only now that the block is known to be closed, mask
            # the body lines strictly between opener and delimiter line.
            for (j = i + 1; j < closeat; j++) {
                body = lines[j]
                gsub(/./, MASKC, body)
                lines[j] = body
            }
            i = closeat            # resume scanning after the delimiter line
            break
        }
    }
    out = lines[1]
    for (i = 2; i <= nl; i++) out = out "\n" lines[i]
    return out
}
# True when a heredoc OPENER line looks like it feeds an interpreter --
# either the opener command itself (`bash <<EOF`, `sh -s <<EOF`,
# `python3 <<EOF`, `eval <<EOF`, `source <<EOF`, `. <<EOF`) or the opener is
# piped into one on the same line (`cat <<EOF | bash`). Deliberately a
# whole-line, best-effort check (matching the "narrow / best-effort" style
# of heredoc_delim_at() above): the interpreter must be the COMMAND WORD of
# some segment of the line, not an arbitrary substring -- e.g.
# `echo "installs bash" <<EOF` does NOT match, since "bash" there is a bare
# argument, not the command word.
#
# Recognizing the command word robustly (#5205, widened #5226): the
# interpreter word is matched against the path BASENAME of each segment
# command word, after normalizing away the shell decorations that do not
# change what actually executes --
#   * quoting / backslash-escaping of the command word itself
#     (`"bash" <<EOF`, `\bash <<EOF` -- the classic alias-dodge idiom),
#   * a bare `VAR=value` assignment prefix (`LC_ALL=C bash <<EOF`),
#   * a leading wrapper command with its own flags, assignments and
#     numeric/duration operands (env, command, exec, builtin, sudo, doas,
#     nohup, setsid, nice, ionice, stdbuf, timeout, time, xargs, unbuffer).
# So `/bin/bash <<EOF`, `env bash <<EOF`, `LC_ALL=C bash <<EOF`,
# `sudo bash <<EOF`, `cat <<EOF | sudo bash`, `timeout 60 bash <<EOF`,
# `"bash" <<EOF`, `\bash <<EOF` and `/usr/bin/python3 <<EOF` all resolve to
# the same real interpreter and are caught, closing the evasion class where
# those forms slipped past the older first-token-only / unwrapped checks and
# got their live bodies silently masked (i.e. ALLOWed).
#
# FAIL-CLOSED TAIL (#5226): an interpreter allowlist is an unbounded tail --
# there is always one more wrapper. The residual class no allowlist can ever
# enumerate is a command word the guard cannot resolve to a NAME at all:
# `$SHELL <<EOF`, `${INTERP} <<EOF`, `$(which bash) <<EOF`. Those are treated
# as interpreter-fed, so the body stays visible and the catastrophic-tier
# check still sees any live invocation inside it.
#
# Inverting the whole test (mask ONLY for a known-inert SINK allowlist, so
# every unknown opener fails closed) was considered and deliberately
# rejected: the canonical Loom issue-filing idiom is
# `create-issue.sh --title T --body "$(cat <<EOF ... EOF)"`, whose command
# word is an ordinary repo script -- as is every other repo wrapper around a
# forge call. Under a sink allowlist each of those hard-stalls on a
# catastrophic-tier deny the moment the prose it carries quotes the
# anti-pattern, which is precisely the #5181 false positive this masking
# exists to fix (that bug was found when an agent could not file the report
# about it). So the default for a resolvable-but-unknown command word stays
# "mask", and only unresolvable words fail closed.
function _interp_basename(tok,   base, SQ, DQ) {
    # Reduce a (possibly quoted, backslash-escaped, path-qualified) command
    # word to its basename: quotes and backslashes removed, then the text
    # after the last `/`. `/bin/bash` -> `bash`, `./bash` -> `bash`,
    # `/usr/bin/python3` -> `python3`, `"bash"` -> `bash`, `\bash` -> `bash`,
    # `"/bin/bash"` -> `bash`; a bare `bash` or `.` is unchanged. Stripping
    # quotes/backslashes ANYWHERE in the word (not just at its edges) also
    # collapses the `b"a"sh` / `b\ash` splitting idioms, which the shell
    # resolves to that same command.
    SQ = sprintf("%c", 39)    # single quote
    DQ = sprintf("%c", 34)    # double quote
    base = tok
    gsub(SQ, "", base)
    gsub(DQ, "", base)
    gsub(/\\/, "", base)
    sub(/^.*\//, "", base)
    return base
}
function interpreter_opener_kind(line,   n, segs, i, seg, m, toks, j, base) {
    # Split into command segments on ; & | (covers && and || too) so a piped
    # or chained interpreter is caught in ANY position, e.g. `cat <<EOF | bash`
    # and `cat <<EOF | sudo bash`.
    n = split(line, segs, /[;&|]+/)
    for (i = 1; i <= n; i++) {
        seg = segs[i]
        sub(/^[ \t]+/, "", seg)
        m = split(seg, toks, /[ \t]+/)
        if (m == 0) continue
        j = 1
        # (1) Strip a BARE `VAR=value` assignment prefix. This is ordinary
        # shell with no `env` in front (`LC_ALL=C bash <<EOF`) and is the
        # most common prefix in practice; before #5226 it fell straight
        # through, because assignments were only skipped AFTER an
        # env/command/exec/builtin token had already been seen.
        while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) j++
        # (2) Strip leading wrapper commands that do not change what runs,
        # each followed by its own -flags, `VAR=value` assignments and
        # numeric/duration operands (`timeout 60`, `timeout 1.5h`,
        # `nice -n 10`, `ionice -c 2`), then re-check for another wrapper.
        # The wrapper word goes through _interp_basename() too, so
        # `/usr/bin/sudo` and `\sudo` strip exactly like a bare `sudo`.
        while (j <= m) {
            base = _interp_basename(toks[j])
            if (base ~ /^(env|command|exec|builtin|sudo|doas|nohup|setsid|nice|ionice|stdbuf|timeout|time|xargs|unbuffer)$/) {
                j++
                while (j <= m && (toks[j] ~ /^-/ || toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/ || toks[j] ~ /^[0-9]+([.][0-9]+)?[smhd]?$/)) j++
                continue
            }
            break
        }
        if (j > m) continue
        base = _interp_basename(toks[j])
        # SHELL-family interpreters treat a heredoc body as genuine SHELL
        # syntax -- `>`/`>>`/tee/sed/cp/mv inside it really are the write
        # idioms extract_write_targets() looks for -- so this kind keeps the
        # body scanned exactly as before (the original #5351 behavior, no
        # change here).
        if (base ~ /^(bash|sh|zsh|dash|ksh|eval|source|\.)$/)
            return "shell"
        # STRUCTURED (non-shell) interpreters -- python/perl/ruby/node -- hand
        # the body to a language with its OWN grammar, in which a bare `>` is
        # routinely a comparison/generic operator, not a redirection (#331:
        # `while depth > 0 and i < len(src):` inside a Python heredoc was
        # misread by the extract_write_targets() shell-syntax `>` scan as a
        # redirect to a file literally named "0"). mask_heredoc_bodies_selective()
        # below applies a dedicated write-marker scan for this kind instead of
        # handing the raw body to the shell-syntax scanner.
        if (base ~ /^(python[0-9.]*|perl|ruby|node|nodejs)$/)
            return "structured"
        # (3) Fail CLOSED on a command word that resolves to no name at all --
        # a variable / command substitution, or an empty word. See the
        # FAIL-CLOSED TAIL note above: resolvable-but-unknown command words
        # (`cat`, `tee`, a repo script) keep masking, per #5181. Treated the
        # same as "shell" (body stays fully visible, unmasked) since this
        # guard cannot prove what the resolved interpreter actually is.
        if (base == "" || base ~ /[$`]/)
            return "unresolvable"
    }
    return ""
}
# Boolean wrapper over interpreter_opener_kind() -- kept so any FUTURE caller
# that only needs "is this opener interpreter-fed at all" (the pre-#331
# question) does not have to know about the kind classification. Currently
# has no runtime caller of its own (mask_heredoc_bodies_selective() below
# calls interpreter_opener_kind() directly, since it needs the kind, not just
# the boolean) -- kept as the reference boolean primitive, deliberately
# defined ON TOP of interpreter_opener_kind() (never a separate hand-copied
# regex) so the two can never drift apart -- see the #5226 "re-deriving X in a
# third place is exactly the drift ... is itself a bypass" rationale reused
# throughout this file.
function is_interpreter_opener(line) {
    return interpreter_opener_kind(line) != ""
}
# structured_body_has_write_marker() (#331) -- true when a heredoc body fed to
# a STRUCTURED (non-shell) interpreter -- python/perl/ruby/node, per
# interpreter_opener_kind() above -- contains a write-mode marker: an
# explicit write/append/create/exclusive-mode `open(...)`/`File.open(...)`
# call, a qualified stdlib/runtime call that writes, renames, or deletes a
# file (`os.remove(`, `shutil.rmtree(`, `Path(...).unlink(`, the Ruby
# `FileUtils.rm*` family, the Node `fs.writeFile*` family, ...), or a sign the
# payload spawns a NESTED shell at all (`subprocess.*`, `os.system(`,
# backticks, the Node `child_process`/`execSync(` family) -- since this guard cannot see into whatever
# command string reaches that nested shell, "it shells out" is itself treated
# as unresolvable and therefore a marker (same "unresolvable => fail closed"
# contract as the rest of this file, e.g. #4921).
#
# Deliberately NOT a marker: a bare `>`/`>>` character anywhere in the body.
# That is precisely the false-positive vector #331 reported -- Python/Perl/
# Ruby/JS all use `>` as an ordinary comparison/generic operator, and treating
# its mere presence as "this heredoc writes a file" is the exact bug this
# function exists to stop reproducing one level up. A REAL redirection reaching
# an actual shell from inside one of these languages is instead caught via the
# "spawns a nested shell" markers above.
#
# Deliberately broad ACROSS all four structured languages rather than keyed to
# which one actually opened THIS heredoc (interpreter_opener_kind() already
# collapsed that distinction to "structured") -- a marker false-HIT only ever
# costs one extra deny on a payload that turns out to be read-only, never a
# missed real write, matching the "narrow, never widen a deny into an allow"
# contract this file states throughout (e.g. the dequote_expandable() header).
#
# Deliberately excludes generic, unqualified method names that collide with
# extremely common non-filesystem operations (`.write(` -- stdout/socket/
# buffer writes are routine in read-only analysis/reporting scripts, exactly
# the #331 false-positive class this fix targets; `.replace(`/`rename(` without
# a qualifying prefix -- string methods, not filesystem calls, and the #331
# repro script itself calls `.replace(` on a string). The bare `unlink(`/
# `rename(`/`system(` markers ARE kept (word-boundary guarded below) because
# Perl has no dotted stdlib namespace -- `unlink $f;` / `system("cmd")` are
# its ordinary idiom for exactly these operations, and dropping them would
# silently stop catching real Perl writes/shell-outs.
function structured_body_has_write_marker(body,   SQ, DQ, BQ, qc, re) {
    SQ = sprintf("%c", 39)    # single quote
    DQ = sprintf("%c", 34)    # double quote
    BQ = sprintf("%c", 96)    # backtick

    if (body == "") return 0
    qc = "[" SQ DQ "]"

    # Explicit write/append/create/exclusive `open(...)` mode -- Python
    # `open(f, "w")` / `open(path, mode="wb")`, Ruby `File.open(p, "a")` --
    # a quote character immediately followed by w/a/x (case-insensitive)
    # ANYWHERE after a comma inside the SAME `open(...)` call. The comma is
    # load-bearing: it is what tells the mode argument apart from the first
    # (filename) argument, whose OWN value may innocently begin with any of
    # those letters (`open("write_report.txt")` is an ordinary DEFAULT-mode
    # -- i.e. read-only -- open whose filename happens to start with "w"; a
    # bare quote-then-letter test with no comma requirement misread that as
    # a write-mode marker). The default / explicit read mode with no comma at
    # all (`open(f)`, `open(f, "r")` -- the exact safehouse#112 shape) never
    # matches.
    re = "open\\([^)]*,[^)]*" qc "[wWaAxX]"
    if (match(body, re)) return 1

    # Perl classic two-arg `open(FH, ">file")` / `open FH, ">file"` --
    # `>`/`>>` as the FIRST character of the mode/target string argument.
    re = "open[ \t]*\\(?[^" DQ SQ "\n]*,[ \t]*" qc ">"
    if (match(body, re)) return 1

    # Qualified stdlib/runtime calls that write, rename, or delete a file --
    # module- or class-qualified, so a plain substring match is not expected
    # to collide with an unrelated identifier.
    if (index(body, "os.remove(")     > 0) return 1
    if (index(body, "os.unlink(")     > 0) return 1
    if (index(body, "os.rename(")     > 0) return 1
    if (index(body, "os.replace(")    > 0) return 1
    if (index(body, "os.write(")      > 0) return 1
    if (index(body, "shutil.rmtree(") > 0) return 1
    if (index(body, "shutil.move(")   > 0) return 1
    if (index(body, "shutil.copy")    > 0) return 1   # copy/copy2/copyfile/copytree
    if (index(body, ".write_text(")   > 0) return 1
    if (index(body, ".write_bytes(")  > 0) return 1
    if (index(body, ".writelines(")   > 0) return 1
    if (index(body, ".unlink(")       > 0) return 1
    if (index(body, ".rmdir(")        > 0) return 1
    if (index(body, "File.write(")    > 0) return 1
    if (index(body, "File.delete(")   > 0) return 1
    if (index(body, "FileUtils.rm")   > 0) return 1
    if (index(body, "FileUtils.mv")   > 0) return 1
    if (index(body, "FileUtils.cp")   > 0) return 1
    if (index(body, "IO.write(")      > 0) return 1
    if (index(body, "fs.writeFile")   > 0) return 1
    if (index(body, "fs.appendFile")  > 0) return 1
    if (index(body, "fs.unlink")      > 0) return 1
    if (index(body, "fs.rmSync")      > 0) return 1
    if (index(body, "fs.rmdirSync")   > 0) return 1
    if (index(body, "fs.rename")      > 0) return 1

    # Nested-shell spawn -- the marker list above cannot enumerate every write
    # a shelled-out command string might perform, so ANY sign the payload
    # spawns a shell at all is itself a marker (fail closed on the unresolved
    # command string), including a genuine `>`/`>>` reaching a REAL shell via
    # `os.system("cmd > file")` / `subprocess.run("cmd > file", shell=True)`.
    if (index(body, "os.system(")     > 0) return 1
    if (index(body, "os.popen(")      > 0) return 1
    if (index(body, "subprocess.")    > 0) return 1
    if (index(body, "child_process")  > 0) return 1
    if (index(body, "execSync(")      > 0) return 1
    if (index(body, BQ)               > 0) return 1

    # Bare, unqualified Perl idiom (no dotted stdlib namespace exists to
    # qualify these) -- word-boundary guarded so a substring collision inside
    # an unrelated identifier (`ecosystem(`, `resystem(`) is not mistaken for
    # the call itself.
    if (match(body, "(^|[^A-Za-z0-9_])unlink[ \t]*\\(")) return 1
    if (match(body, "(^|[^A-Za-z0-9_])rename[ \t]*\\(")) return 1
    if (match(body, "(^|[^A-Za-z0-9_])system[ \t]*\\(")) return 1

    return 0
}
# Replace lines[from..to) (to EXCLUSIVE, mirrors the closeat/j<closeat callers
# use throughout this file) with MASKC placeholders, one placeholder byte per
# original byte -- shared by both the plain (non-interpreter-fed) and the
# structured-with-no-write-marker branches below so both stay byte-for-byte
# identical to the masking mask_heredoc_bodies() itself performs.
function _mask_heredoc_body_lines(lines, from, to, MASKC,   j, body) {
    for (j = from; j < to; j++) {
        body = lines[j]
        gsub(/./, MASKC, body)
        lines[j] = body
    }
}
# Same closed-block detection as mask_heredoc_bodies(), but SKIPS masking
# (leaves the body visible) for any block whose opener is interpreter-fed
# per interpreter_opener_kind() -- see KNOWN LIMITATIONS #1 above. Used by BOTH
# tiers: the gh-api-rawfield-body-literal-at catastrophic check (#5198) and,
# as of #5351, the extract_write_targets() ask-tier write-confinement scan (the
# END-block call below) -- so a write into the main checkout inside an
# interpreter-fed heredoc body is no longer masked out of the confinement
# check. Plain mask_heredoc_bodies() above is retained as the reference
# primitive (identical minus the interpreter carve-out) but now has no
# runtime caller.
#
# #331 refinement: a "structured" (non-shell) interpreter-fed body -- python/
# perl/ruby/node, per interpreter_opener_kind() -- is no longer handed
# UNCONDITIONALLY visible to the extract_write_targets() shell-syntax scanner
# (bare `>`/`>>`, `tee`/`sed`/`cp`/`mv` command words). That scanner is sound
# for SHELL-family bodies (a `>` genuinely is a shell redirection there) but
# unsound for a structured language own grammar, where those same bytes
# routinely mean something else entirely (Python own `>` comparison operator
# -- the exact #331 false positive). Instead:
#   - no write-mode marker found (structured_body_has_write_marker() == 0) --
#     the body performs no recognized write/delete/shell-out operation, so it
#     is masked exactly like a plain non-interpreter-fed heredoc (safe: this
#     guard already proved there is nothing here to catch).
#   - a write-mode marker IS found -- this guard cannot parse the target path
#     out of arbitrary Python/Perl/Ruby/JS source, so rather than leave the
#     raw body to the shell-syntax scanner (unsound and unreliable for this
#     kind, per the above) the FIRST line of the body is replaced with a
#     single, unambiguous shell write-idiom (`> .`) that the EXISTING bare
#     `>`/`>>` scan below already recognizes -- deterministically producing
#     exactly one write target, resolved against the SAME tracked `cd` cwd
#     this heredoc line actually sits at (a plain text substitution at the
#     original line position, so the surrounding cd-tracking loop is
#     completely unaffected). The remaining body lines are masked so no OTHER
#     token in the payload can manufacture a second, spurious target. This
#     mirrors the existing "target unresolvable -> fail closed" contract this
#     file already applies elsewhere (#4921) rather than inventing a new one.
function mask_heredoc_bodies_selective(s,   out, lines, nl, i, j, line, trimmed, body, bodytext, delim, closeat, p, off, MASKC, kind, hasmarker, first) {
    MASKC = sprintf("%c", 23) # ETB -- placeholder for inert heredoc-body text
    nl = split(s, lines, "\n")
    if (nl == 0) return ""
    for (i = 1; i <= nl; i++) {
        line = lines[i]
        off = 1
        while (1) {
            p = index(substr(line, off), "<<")
            if (p == 0) break
            p = off + p - 1
            off = p + 2
            delim = heredoc_delim_at(line, p)
            if (delim == "") continue
            closeat = 0
            for (j = i + 1; j <= nl; j++) {
                trimmed = lines[j]
                sub(/^\t+/, "", trimmed)
                if (trimmed == delim) { closeat = j; break }
            }
            if (closeat == 0) continue
            kind = interpreter_opener_kind(line)
            if (kind == "") {
                # Not interpreter-fed at all -- unchanged (mask, inert body).
                _mask_heredoc_body_lines(lines, i + 1, closeat, MASKC)
            } else if (kind == "structured") {
                bodytext = ""
                for (j = i + 1; j < closeat; j++)
                    bodytext = (bodytext == "" ? lines[j] : bodytext "\n" lines[j])
                hasmarker = structured_body_has_write_marker(bodytext)
                if (hasmarker) {
                    first = 1
                    for (j = i + 1; j < closeat; j++) {
                        if (first) {
                            lines[j] = "> ."
                            first = 0
                        } else {
                            body = lines[j]
                            gsub(/./, MASKC, body)
                            lines[j] = body
                        }
                    }
                } else {
                    _mask_heredoc_body_lines(lines, i + 1, closeat, MASKC)
                }
            }
            # kind == "shell" or "unresolvable" -- leave the body fully
            # visible, byte-for-byte unchanged (original #5351 behavior).
            i = closeat            # resume scanning after the delimiter line
            break
        }
    }
    out = lines[1]
    for (i = 2; i <= nl; i++) out = out "\n" lines[i]
    return out
}
'

# =============================================================================
# extract_write_targets() — Bash-tool write-idiom target extraction (#4178).
#
# Emits one "<cwd>\t<target>" line (TAB-separated, US separator 0x1f — mirrors
# parse_force_ops' SEP convention) per recognized write idiom found in $1:
#   - `>` / `>>` redirection, bare or fd-prefixed (`2>file`), attached
#     (`>file`) or spaced (`> file`). A dup-to-fd form (`>&1`, `2>&1`) is
#     recognized and EXCLUDED — it never writes a file.
#   - `tee <file>...`            — every non-flag argument is a target.
#   - `sed -i ... <script> <file>...` — only when an -i/-i* flag is present;
#     the FIRST non-flag argument is assumed to be the sed script, the rest
#     are file targets. Exactly one non-flag argument is genuinely ambiguous
#     (could be an -f scriptfile with no positional file yet) and is SKIPPED
#     rather than guessed — allow on uncertainty, never deny on uncertainty.
#   - `cp` / `mv ... <dest>`     — the LAST non-flag argument (the common
#     `cp/mv src... dest` shape).
#
# In the three idiom scans above (NOT the `>`/`>>` scan, which has its own
# operator detection), a `<` stdin redirection is recognized and EXCLUDED
# (#5369): neither the operator token (`<`, `0<`, `</path`) nor the file a
# bare `<` reads FROM is a write target. Skipping it fixes both a false DENY
# (phantom `<repo>/<` targets on `tee f < in`) and, for `cp`/`mv` — whose
# destination is the LAST non-flag token — a false ALLOW where a trailing
# `< in` displaced the real destination. See the inline comment at the scan.
#
# $2 seeds the starting cwd. A `cd <path>` segment updates cwd for LATER
# segments of the SAME command (so `cd <worktree> && echo x > f` resolves the
# relative target against the worktree, not the hook's cwd) — global awk
# variable `curcwd`, threaded across the per-line pattern-action block exactly
# like parse_force_ops threads `cpath` via `git -C`.
#
# NOT a full shell parser: like parse_force_ops / extract_rm_targets, splitting
# a segment into tokens starts from plain whitespace splitting. Unlike those
# two, a QUOTED argument containing a literal space is NOT mis-split here: the
# split runs against mask_ws()'s output (#4934), which replaces whitespace
# INSIDE a quoted span with a non-whitespace placeholder before
# `split(seg, toks, /[ \t]+/)` runs, so a quoted path with an embedded space
# (e.g. `echo x > '/main/checkout/evil file.sh'`) yields exactly ONE token —
# unmask_ws() restores the real whitespace bytes in that token afterward. The
# `>`/`>>` redirection scan below is quote-aware in a second, independent way
# (#4245) via mask_gt() — a `>` inside a quoted argument (e.g. `gh issue
# create --body "... env > config > default ..."`) is never treated as a
# redirection operator, regardless of whether the caller's literal-text
# redaction (next paragraph) already removed it. The caller ALSO feeds this
# the ASK-tier working copy (COMMAND_ASK_SCAN — comment-stripped AND
# literal-text-redacted, i.e. --body/-m/--title/--notes/--comment values are
# replaced with same-length placeholder text) as a second, independent
# narrowing so a `>` that merely appears INSIDE such a quoted value (e.g.
# `git commit -m "a > b"`) cannot manufacture a phantom target even in the
# (non-`>`) tee/sed/cp/mv target-extraction paths below. Any remaining false
# positive resolves to, at worst, an extra deny on a target that isn't really
# a write (safe direction) or a missed target (also safe — the fail-open
# contract this file uses everywhere: ambiguity never widens a deny).
#
# The `cd <path>` tracking now tilde/$HOME-expands its argument via
# expand_cd_arg() (#5315, defined with _QSPLIT_AWK above) — see that function's
# header for the exact expansion rules and the quoted/escaped/`~user` fallbacks.
#
# --------------------------------------------------------------------------
# #5315 DECISION (recorded, NOT implemented in this pass) — two deliberate
# scope calls, documented here so a later reader does not mistake either for an
# oversight:
#
#   1. `~user` / `~user/rest` in a tracked `cd` argument is left UNRESOLVED
#      (joined repo-relative, i.e. classified in-tree / denied) rather than
#      resolved to another account's home. awk cannot look a user's home up via
#      getent/dscl without building a shell command string around an
#      attacker-influenced username token — a command-injection surface this
#      guard must not open. The write-TARGET side (expand_leading_tilde, #4382)
#      can afford that lookup because it runs in bash with the username passed
#      as a non-eval'd argv element; the cd-argument side runs inside awk and
#      cannot. Leaving it repo-relative is the fail-CLOSED direction (a
#      genuinely out-of-tree `cd ~alice && …` write stays denied, never
#      silently allowed), matching this file's `cd -` / bare-`cd` convention.
#      The overwhelmingly-common current-user forms (`~`, `~/rest`, `$HOME`,
#      `$HOME/rest` — the actual #5315 report) ARE expanded.
#
#   2. EPHEMERAL_PATTERNS-class runtime state (the daemon's own gitignored
#      files — `.loom/.daemon.pid` et al., authoritative list in
#      loom-daemon/src/init/post_init.rs) is NOT exempted from Bash write
#      confinement. As of this change no such exemption exists in either
#      guard-destructive-generic.sh or guard-worktree-paths.sh; adding one is
#      net-new policy, and CLAUDE.md documents an "ungated denial floor" that no
#      toggle may bypass, so a gitignore-aware carve-out risks widening that
#      floor into an allow if scoped even slightly too broadly. The concrete
#      #5315 report was a false POSITIVE caused entirely by defect (1) above
#      (the literal-`~` mis-join), which this change fixes directly — an
#      operator maintaining `.loom/.daemon.pid` runs from the primary checkout,
#      not a builder worktree, so once the path resolves correctly it is a
#      routine main-checkout write and the confinement question is orthogonal.
#      Deferred to a dedicated follow-up so the exemption's blast radius can be
#      designed against the denial floor deliberately rather than bolted on
#      alongside a canonicalization fix. See #5315 for the deferral rationale.
# --------------------------------------------------------------------------
#
# SAME-COMMAND VARIABLE RESOLUTION (#4881): a write-idiom target whose token
# is `$NAME`/`${NAME}[...]` is not itself a real path — the real shell
# substitutes it from that variable's value before the redirect/tee/sed/cp/mv
# ever runs. This tokenizer previously treated such a token as a literal
# relative path and cwd-prefixed it, manufacturing a phantom repo-relative
# target (e.g. `SCRATCH=/private/tmp/.../scratchpad` on one line, then `... >
# $SCRATCH/out.txt` on the next, was denied as a worktree-isolation bypass
# even though the real target resolves to /private/tmp, far outside the
# repo). `resolve_var()` below performs the ONE unambiguous, narrow piece of
# this: when the SAME command text contains a `NAME=value` assignment (no
# embedded whitespace in `value`, optionally single/double-quoted) earlier in
# the stream, later `$NAME`/`${NAME}` leading a write target is substituted
# with that value. Threaded via the awk global `varmap`, exactly like `curcwd`
# above. The assignment scan recognizes every ordinary shell assignment
# position, not just a segment that is exactly one bare `NAME=value`:
#   NAME=value                       (bare)
#   export/readonly/declare/typeset/local [-flags] NAME=value [NAME2=value2]
#   A=1 B=2                          (several assignments in one segment)
#   A=1 some-command args…           (env-var prefix — A recorded, then the
#                                     REST of the segment is still scanned as
#                                     a command for write idioms)
#
# CONFLICTING ASSIGNMENTS ARE UNRESOLVABLE (#4914 review): the scan is not
# control-flow aware — qsplit() flattens `||`/`&&`/`;` into plain segments — so
# a name assigned two DIFFERENT values in one command
# (`A=<repo>/defaults/hooks || A=/tmp/outside`) is poisoned to the unresolvable
# sentinel rather than resolved to whichever branch happens to come last in the
# token stream. See record_assign() below.
#
# FAIL-CLOSED ON UNRESOLVABLE (#4914 review): a `$NAME` with NO matching
# assignment, or a token starting with `$` that is not a bare variable
# reference at all (`$(...)` command substitution, `${VAR:-default}`, `$1`,
# an inherited/sourced env var, …), is UNRESOLVABLE. It is NEVER guessed —
# and it is NEVER skipped either. It falls back to the PRE-#4881 behavior:
# the raw token is treated as a literal (repo-relative) path, so a write that
# lands inside the main checkout still denies with the ordinary
# `worktree-write-confinement` tag. Skipping an unresolvable target would
# hand every un-parsed assignment shape a free #4178 worktree-isolation
# bypass (`export SNEAK=<repo>/defaults/hooks; echo x > $SNEAK/evil.sh`), so
# this fix only ever RELAXES the one literally-resolvable case it can prove
# lands outside the repo — it never flips the default for anything else. (The
# file's broader "ambiguity never widens a deny" contract is about not
# inventing NEW denies; preserving an EXISTING one is the conservative side.)
# =============================================================================
extract_write_targets() {
    # Reuses THIS file's own _ESCAPE_AWK/_QSPLIT_AWK (defined above, shared
    # with parse_force_ops()/extract_rm_targets()/command_has_shell_segment()
    # via the #113 escaped-quote fix) rather than re-vendoring the vendored
    # guard's separate, older qsplit() copy — the two must not both define a
    # `qsplit()` under the same awk source variable, and this file's version
    # is the more advanced of the two (rjwalters/repo#188).
    printf '%s' "$1" | awk -v startcwd="$2" -v home="$HOME" "$_ESCAPE_AWK""$_HASLIVESUBST_AWK""$_QSPLIT_AWK""$_CDEXPAND_AWK""$_CDQUOTE_AWK""$_CDWORD_AWK""$_MASKGT_AWK""$_MASKWS_AWK""$_MASKHEREDOC_AWK"'
    # Unresolvable cases all return tok UNCHANGED, which is exactly the
    # pre-#4881 treatment (literal, cwd-prefixed => still denied when it
    # lands in the main checkout). Fail-closed by construction: this function
    # can only ever REPLACE a token with a value it actually proved, never
    # make one disappear.
    #
    # QUOTED WRITE TARGETS (repo#293): qsplit() copies quote characters
    # VERBATIM, so the overwhelmingly common builder spelling of this exact
    # pattern -- `WORKTREE_ABS="<wt>"; cp x "$WORKTREE_ABS/rtl/y"` -- arrived
    # here as `"$WORKTREE_ABS/rtl/y"`, failed the `substr(tok,1,1) != "$"`
    # test on its opening double quote, and was emitted UNRESOLVED. It then
    # hit the #4921 unresolved-`$` classifier downstream and hard-denied with
    # the `worktree-write-confinement-unresolved-var` tag -- even though the
    # variable held a static, worktree-confined literal assigned in the SAME
    # command that the resolver was already fully capable of proving (the
    # unquoted spelling of the identical command has always resolved and
    # allowed). resolve_var() is now the quote-aware entry point and
    # resolve_var_core() is the unchanged resolution itself.
    function resolve_var(tok,   cand, res) {
        if (substr(tok, 1, 1) == "$") return resolve_var_core(tok)
        cand = dequote_expandable(tok)
        if (cand == "" || substr(cand, 1, 1) != "$") return tok
        res = resolve_var_core(cand)
        # Nothing proved => return the ORIGINAL, quote-preserved token, i.e.
        # byte-identical to the pre-repo#293 verdict for every shape this
        # cannot resolve.
        if (res == cand) return tok
        return res
    }
    # dequote_expandable() -- conservative ELIGIBILITY TEST, deliberately NOT a
    # quote parser (repo#293).
    #
    # The mark_expandable_dollars() header warns that a second, hand-copied copy
    # of "what the shell would do to these quotes" is exactly how the two
    # consumers drift apart, and that a drift in THAT grammar is a guard
    # bypass. This function does not re-implement that grammar. It answers one
    # much weaker, decidable question: "is this token so trivially quoted that
    # deleting every double quote is PROVABLY identical to what bash produces?"
    #
    # It refuses (returns "") the moment anything subtle is present:
    #   - a single quote  -> `$` inside it is literal data, never an expansion
    #   - a backslash     -> `\$` is a literal `$`, `\"` shifts the quoting
    #   - a backtick      -> legacy command substitution in a later component
    #   - unbalanced `"`  -> bash would not even accept the word
    # With NONE of those present, every `$` in the token is expanded by bash
    # whether it sits inside or outside the double-quoted spans, and the
    # quote characters contribute nothing to the resulting word -- so
    # `"$V/x"`, `"$V"/x` and `$V"/x"` all denote the same path, and deleting
    # the quotes is exact rather than approximate.
    #
    # Returns "" for "not eligible / nothing to strip", which callers must
    # treat as "keep the verdict this guard already produces". A resolved
    # value is NEVER trusted on
    # its own: it is substituted into the token and then judged by the SAME
    # confinement tests every literal target goes through, so proving a
    # variable holds `<main-checkout>/evil.sh` still DENIES (with the ordinary
    # `worktree-write-confinement` tag). This can only ever make an
    # unresolvable target resolvable -- it never relaxes a containment test.
    function dequote_expandable(tok,   n, i, c, out, dq) {
        if (index(tok, SQ) > 0) return ""
        if (index(tok, "\\") > 0) return ""
        if (index(tok, BQ) > 0) return ""
        n = length(tok)
        dq = 0
        out = ""
        for (i = 1; i <= n; i++) {
            c = substr(tok, i, 1)
            if (c == DQ) { dq++; continue }
            out = out c
        }
        if (dq == 0) return ""
        if (dq % 2 != 0) return ""
        return out
    }
    function resolve_var_core(tok,   vname, rest, vv) {
        if (substr(tok, 1, 1) != "$") return tok
        if (match(tok, /^\$\{[A-Za-z_][A-Za-z0-9_]*\}/)) {
            vname = substr(tok, RSTART + 2, RLENGTH - 3)
            rest = substr(tok, RSTART + RLENGTH)
        } else if (match(tok, /^\$[A-Za-z_][A-Za-z0-9_]*/)) {
            vname = substr(tok, RSTART + 1, RLENGTH - 1)
            rest = substr(tok, RSTART + RLENGTH)
        } else {
            # `$(...)`, `${VAR:-x}`, `$1`, … — not a bare variable reference.
            return tok
        }
        if (!(vname in varmap)) return tok
        vv = varmap[vname]
        # A value that itself still starts with an unresolved "$" (chained
        # assignment this single-pass resolver does not follow) stays
        # unresolved rather than being guessed.
        if (vv == "" || substr(vv, 1, 1) == "$") return tok
        return vv rest
    }
    # Record a single `NAME=value` word into varmap (value optionally wrapped
    # in matching single/double quotes, which qsplit() copies verbatim).
    #
    # CONFLICTING ASSIGNMENTS POISON THE VARIABLE (#4914 review): this scan is
    # NOT control-flow aware -- qsplit() flattens `||`/`&&`/`;` into plain
    # segments, so `A=<in-repo> || A=/tmp/outside` reaches here as two
    # assignments to the same name. A plain last-write-wins store would then
    # resolve `$A` to whichever branch happens to appear LAST in the token
    # stream, which real bash need never take (`||` short-circuits, so `$A` is
    # the in-repo value at runtime) -- silently ALLOWing a write into the main
    # checkout. So when a name is re-assigned a DIFFERENT value within the same
    # command, its entry is replaced with the AMBIG sentinel instead: a
    # `$`-leading value, which resolve_var() already refuses to substitute as
    # an unresolved chain. The token then falls back to the literal
    # (cwd-prefixed) treatment and denies -- the same fail-closed path every
    # other unresolvable shape takes. Poisoning is sticky (any later assignment
    # differs from the sentinel too) and deliberately blunt: it also covers
    # sequential `A=x; A=y` reassignment, where resolving is *possible* in
    # principle but the safe direction is to stop guessing. Re-assigning the
    # SAME value is not a conflict and still resolves normally -- quotes are
    # stripped above, before the comparison, so a bare and a quoted spelling of
    # one value compare equal.
    #
    # A NON-STATIC RHS POISONS THE VARIABLE TOO (repo#293 review): resolution is
    # only ever sound when the recorded value is a STATIC LITERAL — a value the
    # real shell would hand to the write idiom byte-for-byte, with nothing left
    # for it to expand. Before this check, "static" was enforced only by
    # a test inside resolve_var_core() on the FIRST character, which caught
    # `A=$B/x` and `A=$(pwd)/x` but nothing embedded further in. That left a
    # live fail-open bypass on this catastrophic-tier guard:
    #
    #     V="<worktree>/`echo evil`/x"; cp /tmp/y "$V/pwned.sh"
    #
    # was ALLOWED — the backtick command substitution sits mid-value, so the
    # leading-byte test never saw it, the value was stored as if it were a
    # proven literal, and the downstream #4921 unresolved-`$` backstop found no
    # `$` in the resolved token to trip on (a bare backtick pair carries none).
    # The `$(...)` spelling happened to be caught only incidentally, because its
    # literal `$` survived substitution into the final token. Relying on that
    # accident is not a safety property.
    #
    # So the eligibility bar is now enforced where the value is CAPTURED, not
    # where it is consumed, and over the WHOLE string rather than its first
    # byte: any RHS containing a backtick or a `$` ANYWHERE is poisoned to
    # AMBIG and never becomes a resolvable literal. This is checked against the
    # RAW word (before the outer quote pair is stripped), so no spelling of the
    # quoting can hide an expansion character from it. Deliberately blunt, and
    # deliberately on the conservative side of the "ambiguity never widens a
    # deny" contract this file states elsewhere: poisoning only ever routes the
    # token back to the pre-#4881 literal treatment, which fails CLOSED under
    # `worktree-write-confinement-unresolved-var`. A value containing a `$` the
    # shell would NOT expand (single-quoted, backslash-escaped) is refused here
    # as well — that costs a resolution this guard was never entitled to make,
    # and re-deriving "which `$` would bash expand" in a third place is exactly
    # the drift the mark_expandable_dollars() header warns is itself a bypass.
    # repo#604: the redirections of a cd segment (`>F cd X`, `cd X >F`,
    # `2>F cd X`) open their targets in the PRE-cd cwd, so they are write
    # targets like any other. The cd branch used to `continue` past
    # the redirection scan, which let `>MAIN/evil.sh cd /tmp` through. Same
    # mtoks-based operator test as the general scan; called before curcwd moves.
    function cd_redir_targets(   j, mt, op) {
        for (j = 1; j <= m; j++) {
            mt = mtoks[j]
            if (mt == "") continue
            if (mt ~ /^[0-9]*>>?$/) {
                if (j + 1 <= m && toks[j+1] != "" && mtoks[j+1] !~ /^&/) {
                    print curcwd SEP resolve_var(toks[j+1])
                }
                continue
            }
            if (mt ~ /^[0-9]*>>?[^ \t&]/) {
                op = toks[j]
                sub(/^[0-9]*>>?/, "", op)
                if (op != "") print curcwd SEP resolve_var(op)
            }
        }
    }
    function record_assign(word,   eqpos, vname, vval, vlen, c1, c2) {
        eqpos = index(word, "=")
        if (eqpos < 2) return
        vname = substr(word, 1, eqpos - 1)
        vval = substr(word, eqpos + 1)
        # Non-static RHS -> poison, never store. Checked on the raw value.
        if (index(vval, BQ) > 0 || index(vval, "$") > 0) {
            varmap[vname] = AMBIG
            return
        }
        vlen = length(vval)
        if (vlen >= 2) {
            c1 = substr(vval, 1, 1)
            c2 = substr(vval, vlen, 1)
            if ((c1 == DQ && c2 == DQ) || (c1 == SQ && c2 == SQ)) {
                vval = substr(vval, 2, vlen - 2)
            }
        }
        if ((vname in varmap) && varmap[vname] != vval) {
            varmap[vname] = AMBIG
            return
        }
        varmap[vname] = vval
    }
    BEGIN {
        SEP = sprintf("%c", 31)
        # subst_heads() head delimiter (repo#439): RS, a byte no shell command
        # carries as a separator, so one head is told from the next even when
        # a head holds an embedded newline.
        HSEP = sprintf("%c", 30)
        DQ = sprintf("%c", 34)
        SQ = sprintf("%c", 39)
        # Backtick — legacy command substitution. dequote_expandable()
        # (repo#293) refuses any token containing one rather than proving a
        # prefix around it.
        BQ = sprintf("%c", 96)
        # Poison value for a name assigned two different values in one command
        # (see record_assign). The leading "$" is load-bearing: it routes into
        # the existing unresolved-chain refusal inside resolve_var().
        AMBIG = "$__LOOM_AMBIGUOUS_ASSIGNMENT__"
        curcwd = startcwd
        UNK = unknown_cwd()
        QS_MARK = 1   # repo#601: qsplit() reports separator kinds
    }
    # Slurp the whole (possibly multi-line) command into ONE buffer,
    # preserving embedded newlines (mirrors the #3898 multi-line
    # accumulation strip_literal_text() already uses), then do ALL
    # processing ONCE in END rather than once per PHYSICAL LINE -- the
    # default per-record awk behaviour, which is what silently reset
    # qsplit()/mask_gt()/mask_ws() to "unquoted" at every embedded newline
    # before the #5000 fix below.
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        # Heredoc-body masking (#5000) runs BEFORE qsplit(): once a heredoc
        # body write-idiom-looking bytes (`>`, `;`, `tee`, ...) are replaced
        # with inert placeholders, nothing dangerous-looking is left to
        # misread on those lines. A real write-idiom byte OUTSIDE any
        # recognized heredoc body, even later in the SAME multi-line command,
        # is untouched and still flows through the unchanged pipeline below.
        #
        # INTERPRETER-AWARE (#5351): use the _selective() variant, not plain
        # mask_heredoc_bodies(). A write-idiom line inside a body handed to an
        # interpreter (`bash <<'EOF' ... EOF`, `sh -s <<EOF`, `cat <<EOF |
        # bash`, ...) is genuinely LIVE code to that inner interpreter, so
        # masking it would blank a real out-of-worktree write into an ALLOW --
        # exactly what KNOWN LIMITATIONS #1 recorded as an interpreter-fed gap
        # in this ask-tier scan. _selective() leaves an interpreter-fed body
        # VISIBLE (so the write reaches the confinement check) while still
        # masking every INERT sink body (`cat <<'EOF' ... EOF`,
        # `--body "$(cat <<'EOF' ... EOF)"`), preserving the #4914/#5000/#5181
        # false-positive fixes. This gives the confinement tier the SAME
        # interpreter-awareness the catastrophic tier already has (#5198/#5205).
        buf = mask_heredoc_bodies_selective(buf)
        # Quote-aware segmentation (#3755), plus the FIRST command of every
        # `$( … )`/backtick substitution as its own appended segment (repo#439).
        #
        # qsplit() alone surfaces only what a separator INSIDE a substitution
        # starts (subst_inner(), #436) — so a substitution holding a SINGLE
        # simple command stayed one quoted token of the outer segment, and
        # mask_gt()/mask_ws() below then masked its `>` and its spaces as
        # quoted data. The shell EXECUTES a substitution whatever quoting wraps
        # it, so `echo "$(id > <main>/e.sh)"` really did write into the main
        # checkout from a worktree cwd and this scan saw nothing (repo#439).
        # subst_heads() re-emits that head with its tokens intact; the outer
        # stream is untouched, so the #436 token-integrity fix still holds.
        #
        # Scoped HERE rather than inside qsplit() on purpose: qsplit()-s other
        # consumers (command_has_shell_segment(), resolve_stash_cwd()) answer
        # different questions, and handing them new segments would widen denies
        # this issue did not measure.
        #
        # The heads are kept in their OWN buffer (hbuf) until masking is done;
        # see the note at wbuf/gbuf below for why.
        qbuf = qsplit(buf)
        hbuf = subst_heads(buf, HSEP)

        # Whole-BUFFER quote-aware masking (#5157), not per-segment.
        #
        # mask_ws()/mask_gt() themselves track quote state one character at a
        # time and never special-case "\n" -- an embedded newline inside an
        # OPEN quoted span (e.g. a plain multi-line double-quoted string,
        # `msg="line one\necho pwned > /main/checkout/f.sh\nline three"`, no
        # heredoc involved at all) is simply copied through like any other
        # byte while quote mode stays "on". qsplit() above already preserves
        # such an embedded newline as part of the ONE atomic quoted span it
        # copies verbatim (it finds the matching closing quote by index, not
        # by line), so by construction every "\n" surviving in its output
        # already sits OUTSIDE any then-open quote from the perspective of
        # qsplit() itself -- but mask_ws() and mask_gt() do their own independent
        # char-by-char quote tracking, and calling them per-SEGMENT (after
        # `split($0, segs, "\n")`) resets that tracking to "unquoted" at every
        # such newline, discarding the "still inside this quote" context a
        # PRIOR segment established. A `>` sitting on the continuation line of
        # an otherwise-inert multi-line double-quoted string is then
        # misread as a live redirection operator, manufacturing a phantom
        # write target for text that never reaches the shell as anything but
        # quoted data (#5157). This is the direct multi-line analog of the
        # single-line #4245 fix and the heredoc-body #5000 fix above --
        # masking the WHOLE buffer once, before any "\n"-splitting happens,
        # keeps quote state correctly threaded across every embedded newline,
        # heredoc or not.
        #
        # The subst_heads() segments are the ONE exception, and each is masked
        # IN ISOLATION rather than threaded (repo#439). A head is a copy of
        # text that ALSO appears inside the outer stream, so the outer stream-s
        # quote state at its end says nothing about the head. Threading it
        # through was an escape. For
        #     echo "x $(echo "y<APOS>z" > <main>/e.sh) q"
        # (<APOS> is an apostrophe, spelled out because this awk program is
        # itself single-quoted) the outer segment reads, byte by byte, as `"x $(echo "` (closed) then
        # an apostrophe that OPENS a single-quoted run the outer text never
        # closes — so the mode carried into the appended head was "inside
        # quotes", and the head-s own `>` was masked as data. The same applies
        # head to head. Masking each head from an unquoted start is exactly
        # the view the shell has of it (the substitution-s body is parsed by
        # its own shell, from scratch). A head is never threaded INTO the outer
        # stream either, because the outer stream is masked before any head.
        wbuf = mask_ws(qbuf)
        gbuf = mask_gt(wbuf)
        $0 = qbuf
        nh = split(hbuf, heads, HSEP)
        for (k = 2; k <= nh; k++) {
            hw = mask_ws(heads[k])
            HM = sprintf("%c", 5) "H"   # substitution head: own subshell
            $0 = $0 "\n" HM heads[k]
            wbuf = wbuf "\n" HM hw
            gbuf = gbuf "\n" HM mask_gt(hw)
        }
        n = split($0, segs, "\n")
        nw = split(wbuf, wsegs, "\n")
        ng = split(gbuf, gsegs, "\n")
        # repo#601: peel the separator-kind markers off all three parallel
        # views (same length by construction, so they stay aligned).
        for (i = 1; i <= n; i++) {
            segs[i] = flow_unmark(segs[i]); SK[i] = FL_KIND
            wsegs[i] = flow_unmark(wsegs[i])
            gsegs[i] = flow_unmark(gsegs[i])
        }
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            origlen = length(seg)
            sub(/^[ \t]+/, "", seg)
            sub(/^sudo[ \t]+/, "", seg)
            sub(/^[ \t]+/, "", seg)
            # repo#601: control-flow context (see flow_begin in _CDWORD_AWK).
            if (flow_begin(SK[i], seg)) curcwd = UNK
            kb = FL_EFF
            if (seg == "") continue
            flow_track(seg)

            # repo#600: classify the command position BEFORE the assignment
            # stripping below, on the quote-masked segment, with the shared
            # recognizer (see COMMAND-WORD RECOGNITION further down). The
            # naive `NAME=[^ \t]*` strip splits `X=$(basename $F) cmd` inside
            # the substitution; skip_assign_words() does not.
            cdk = cd_seg_class(substr(wsegs[i], origlen - length(seg) + 1))
            pend_unknown = (cdk == "unknown")
            cdtok = (cdk == "cd") ? CDW_ARG : ""

            # `NAME=value` assignments in any ordinary shell assignment
            # position (#4881; keyword/multi-assignment shapes added by the
            # #4914 review). Recorded into varmap for LATER write targets in
            # this same command. A leading declaration keyword
            # (export/readonly/declare/typeset/local) and its flags are
            # stripped first, then EVERY leading `NAME=value` word is
            # consumed. Whatever remains is the real command for the segment,
            # still scanned for write idioms below (the `A=1 cmd …` env-var
            # prefix shape), so recognizing an assignment never causes a
            # command in the same segment to be skipped.
            if (seg ~ /^(export|readonly|declare|typeset|local)[ \t]/) {
                sub(/^(export|readonly|declare|typeset|local)[ \t]+/, "", seg)
                while (seg ~ /^-/) {
                    if (!sub(/^-[^ \t]*[ \t]*/, "", seg)) break
                }
            }
            segprefix = 0
            while (match(seg, /^[A-Za-z_][A-Za-z0-9_]*=[^ \t]*([ \t]+|$)/)) {
                assignword = substr(seg, 1, RLENGTH)
                seg = substr(seg, RLENGTH + 1)
                sub(/[ \t]+$/, "", assignword)
                record_assign(assignword)
                segprefix = 1
            }
            # A segment that was NOTHING but assignments writes nothing.
            # Anything left over keeps flowing into the command scan below
            # (a redirection is honoured even on a declaration statement —
            # `export FOO > f` really does truncate `f`), so consuming an
            # assignment can never make a real write idiom disappear.
            if (seg == "") continue

            # wsegs[i]/gsegs[i] are byte-for-byte identical in LENGTH to the
            # unstripped segs[i] (masking only ever substitutes one byte for
            # one byte, never adds/removes any) -- `stripped` is the number of
            # leading bytes the three sub() calls above just removed from the
            # (unmasked) seg, so re-applying that same byte count via substr()
            # keeps wseg/mseg positionally aligned with the stripped seg
            # regardless of whether those leading bytes were literal
            # whitespace or (on a mid-quote continuation segment) already
            # masked to a placeholder byte.
            stripped = origlen - length(seg)

            # Quote-aware whitespace masking (#4934, threaded whole-buffer
            # per #5157 above): wseg is byte-for-byte identical to seg except
            # a space/tab INSIDE a quoted span is replaced with a
            # non-whitespace placeholder, so splitting on /[ \t]+/ never
            # breaks a quoted argument (e.g. a quoted path containing a
            # literal space) into more than one token. toks[] is then
            # unmasked back to the real whitespace bytes so the target TEXT
            # downstream is unchanged.
            wseg = substr(wsegs[i], stripped + 1)
            m = split(wseg, toks, /[ \t]+/)
            if (m < 1) continue
            for (j = 1; j <= m; j++) toks[j] = unmask_ws(toks[j])

            # Quote-aware parallel tokenization (#4245, threaded whole-buffer
            # per #5157 above): mseg is byte-for-byte identical to wseg except
            # a `>` inside a quoted span is replaced with an SOH placeholder,
            # so whitespace splitting yields the SAME token boundaries
            # (mm == m always) but mtoks[] can be tested for a REAL (unquoted)
            # redirection operator without ever matching a `>` that was only
            # quoted data. The actual target text is still read from the
            # ORIGINAL toks[] (unmasked) once a real operator is confirmed.
            mseg = substr(gsegs[i], stripped + 1)
            mm = split(mseg, mtoks, /[ \t]+/)

            # COMMAND-WORD RECOGNITION (repo#600): the shared static
            # recognizer (_CDWORD_AWK) replaces the old literal
            # `toks[1] == "cd"` test. A statically provable spelling of cd
            # (c\d, "c"d, c<APOS><APOS>d) is tracked exactly like the literal word,
            # reading its argument from the token after it. A command word
            # whose value this scan cannot prove (c$()d, ${C:-c}d, $X, a glob)
            # or a cd behind a group/conditional prefix sets curcwd to the
            # UNKNOWN sentinel AFTER this segment (its own redirections are
            # opened before the command runs, in the old cwd), and the shell
            # layer denies any later RELATIVE write -- an unknown cwd never
            # inherits the previous, possibly harmless /tmp or `$NAME` one.
            # (cdk / pend_unknown / cdtok were computed above, before the
            # assignment strip.)
            if (cdk == "cd") cd_redir_targets()
            if (cdk != "") {
                if (flow_cd(kb, flow_next_kind(SK, i, n)) && cdk == "cd") { curcwd = UNK; continue }
            }
            if (cdk == "cd") {
                if (cdtok != "" && cdtok != "-") {
                    cdarg = expand_cd_arg(cdtok, home)   # #5315
                    # SAME-COMMAND LITERAL RESOLUTION OF THE cd ARGUMENT
                    # (#582, Loom #7294): `TMP=/tmp/x; cd "$TMP/repo"; echo
                    # hi > README.md` left curcwd carrying the unexpanded
                    # `$TMP`, so the later RELATIVE write failed closed even
                    # though the identical `$TMP/...` shape already resolves
                    # as a DIRECT write target (#4881/repo#293). resolve_var()
                    # is the same quote-aware, static-literal-only resolver
                    # (single quotes, backslashes, backticks, chained or
                    # conflicting values all refuse), and the resolved cwd is
                    # then judged by the SAME containment rule as a literal
                    # `cd /that/path`, so it can never grant more than spelling
                    # that path outright. Two extra refusals, both stricter
                    # than Loom:
                    #   - the resolved value must be ABSOLUTE. record_assign()
                    #     stores `V=~/x` verbatim while bash tilde-expands it,
                    #     so a non-absolute value is not what bash would cd to;
                    #   - no resolution when THIS segment carries its own
                    #     `NAME=value` prefix: bash expands `$V` in `V=/x cd
                    #     "$V"` BEFORE the prefix applies, so the recorded
                    #     value is not the one cd receives.
                    # Anything refused keeps the pre-existing path below
                    # (fresh-root classification => fail closed).
                    if (!segprefix) {
                        cdres = resolve_var(cdtok)
                        if (cdres != cdtok && substr(cdres, 1, 1) == "/") cdarg = cdres
                    }
                    # Quote-aware absolute/relative CLASSIFICATION only
                    # (#4933, widened to a PARTIALLY quoted argument by
                    # #5363 -- see the strip_cd_quoting() header comment
                    # above). qsplit() preserves quote characters VERBATIM in
                    # toks[] (its contract -- extract_rm_targets()/
                    # parse_force_ops() depend on that raw form elsewhere in
                    # this file), so a quoted ABSOLUTE `cd` argument can start
                    # with a quote character rather than `/`, fail the plain
                    # ^/ test below, and fall into the RELATIVE join branch --
                    # fabricating curcwd as "<worktree>/<quoted-abs-path>", a
                    # location the write never has. From a linked-worktree
                    # cwd that fabrication walks straight back into the
                    # acting worktree own .loom-managed sentinel and the
                    # write is silently ALLOWED, i.e. the #4178 confinement
                    # check is defeated by quoting the cd argument -- fully
                    # (#4933) or only PARTIALLY (#5363, e.g. a quoted
                    # <main> segment followed directly by /sub, no space).
                    #
                    # The fully quote-stripped value (cdclass) is used ONLY
                    # to CLASSIFY. curcwd is still built from the RAW,
                    # quote-preserved cdarg because curcwd is emitted
                    # verbatim as the shell layer `_wcwd`, and the
                    # unresolved-`$` detector there (mark_expandable_dollars,
                    # #4921/#4927) needs those quote characters to tell a
                    # LITERAL `$` inside a single-quoted span (a directory
                    # genuinely named $FOO, explicitly a "deliberately NOT
                    # denied" case in the write-confinement block below) from
                    # an EXPANDABLE one (bare or double-quoted, which the
                    # guard cannot resolve and so fails closed on). Stripping
                    # the quotes here would make every `$` in the last cd
                    # segment look expandable and would deny writes that are
                    # allowed today. The shell layer re-strips quoting for
                    # its own cwd join, mirroring the write-target side raw
                    # `_wtarget` vs. stripped `_wclassify` split
                    # (strip_target_quoting(), #4926).
                    #
                    # An unbalanced/unterminated quote leaves cdclass == cdarg
                    # (strip_cd_quoting() own fallback contract), so
                    # ambiguity can only ever keep the existing verdict, never
                    # widen a deny into an allow (same fallback contract as
                    # #4926).
                    cdclass = strip_cd_quoting(cdarg)
                    # FRESH-ROOT classification (#582, Loom #7294): a bare
                    # `$NAME`/`${NAME}` argument (optionally `/suffix`)
                    # supplies its OWN root at runtime -- `cd "$X"` never
                    # lands under the prior cwd -- so it starts a new curcwd
                    # exactly like an absolute path, instead of being joined
                    # onto the prior cwd as if relative. Joining it fabricated
                    # a REAL known prefix: from a cwd outside the repo
                    # (`/tmp`), `cd "$X"; echo x > f` was judged as
                    # `/tmp/$X/f` and ALLOWED even though $X may hold the main
                    # checkout. As a fresh root, the shell layer below sees an
                    # unknown first path component and fails closed under the
                    # isolation gate. Only ever turns an allow into a deny.
                    # Restricted to an EXPANDABLE `$` (unquoted or
                    # double-quoted, no single quote / backslash anywhere): a
                    # single-quoted `cd '"'"'$X'"'"'` names a directory literally called
                    # `$X` under the prior cwd and must keep the relative join.
                    cdfresh = 0
                    if (index(cdarg, SQ) == 0 && index(cdarg, "\\") == 0 && \
                        (cdclass ~ /^\$\{[A-Za-z_][A-Za-z0-9_]*\}(\/.*)?$/ || \
                         cdclass ~ /^\$[A-Za-z_][A-Za-z0-9_]*(\/.*)?$/)) cdfresh = 1
                    cdbase = curcwd
                    if (cdclass ~ /^\// || cdfresh) {
                        curcwd = cdarg
                    } else if (curcwd != "" && curcwd != UNK) {
                        curcwd = curcwd "/" cdarg
                    }
                    # repo#625: a cd to a missing directory fails and stays put
                    if (cd_unreached(cdarg, cdbase, flow_next_kind(SK, i, n), home)) curcwd = UNK
                }
                continue
            }

            # STDIN-REDIRECTION EXCLUSION (#5369) -- `<` is a redirection
            # operator, never a write-target operand, so neither it nor the
            # file it reads FROM may be scanned as a write target by the
            # tee / sed -i / cp-mv loops below. Two symptoms motivated this,
            # one in each direction:
            #
            #   * false DENY (tee/sed -i): the bare `<` token and its operand
            #     were both scanned, resolving against curcwd into phantom
            #     `<repo>/<` and `<repo>/in` targets -- so a wholly
            #     out-of-tree `tee /tmp/f.md < /tmp/in` was denied as a
            #     confinement bypass.
            #   * false ALLOW (cp/mv) -- the serious one: that branch takes
            #     the LAST non-flag token as the destination, so a trailing
            #     `< /tmp/in` displaced the REAL destination and a
            #     `cp /tmp/a <main-checkout>/p.sh < /tmp/in` was waved
            #     through -- a #4178 worktree-confinement escape.
            #
            # Token-boundary test, exactly like the `>`/`>>` operator loop
            # below (never a mid-token character scan):
            #   `<` / `0<`  (bare, optionally fd-prefixed) consumes the NEXT
            #               non-empty token, which is the file read FROM.
            #   `</tmp/in`  (attached, optionally fd-prefixed) consumes only
            #               itself.
            #
            # QUOTE AWARENESS COMES FREE, no mask_gt()-style parallel
            # tokenization needed: qsplit() preserves quote characters
            # VERBATIM in toks[] and mask_ws() guarantees a quoted span never
            # spans two tokens, so a quoted/escaped literal filename that
            # merely BEGINS with `<` (single-quoted, double-quoted, or
            # backslash-escaped) starts its token with the quote/backslash
            # byte and can never match the anchored patterns here -- it stays
            # a scanned write target, opening no new escape vector, which is
            # the fail-closed direction this file requires.
            # (mask_gt() exists because a `>` can appear
            # MID-token inside a quoted span; these patterns only ever look at
            # the first bytes of a token, so that case cannot arise.)
            #
            # Deliberately NOT matched: `<<`, `<<-`, `<<<`. Those are heredoc
            # /herestring operators handled separately by the pre-tokenization
            # heredoc machinery above (mask_heredoc_bodies_selective) and by
            # #5232/#5233; the `[^<]` guard below keeps this fix strictly
            # disjoint from that one.
            delete stdin_redir
            for (j = 1; j <= m; j++) {
                if (toks[j] == "") continue
                if (toks[j] ~ /^[0-9]*<$/) {
                    stdin_redir[j] = 1
                    for (k = j + 1; k <= m; k++) {
                        if (toks[k] == "") continue
                        stdin_redir[k] = 1
                        break
                    }
                } else if (toks[j] ~ /^[0-9]*<[^<]/) {
                    stdin_redir[j] = 1
                }
            }

            # STDOUT-REDIRECTION EXCLUSION (#340) -- exactly the same
            # rationale and shape as the STDIN-REDIRECTION EXCLUSION directly
            # above, mirrored for `>`/`>>` instead of `<`. A trailing
            # redirect on a `tee`/`sed -i`/`cp`/`mv` segment (the
            # `curl ... | sudo tee /usr/share/keyrings/foo.gpg >/dev/null`
            # apt-keyring idiom repo#29 fixed to allow) is an OPERATOR, not a
            # tee/sed/cp/mv operand -- but the loops below previously had no
            # exclusion for it, so the bare `>`/`>>` token (or its consumed
            # next token, for the bare-operator form) was scanned as a
            # literal tee/sed/cp/mv argument and cwd-joined into a phantom
            # target (`<repo>/>/dev/null`), triggering a false
            # worktree-confinement DENY even though `/dev/null` (or any
            # other absolute, out-of-repo redirect target) is not a write
            # into the repo at all. The REAL `>`/`>>` scan below (the
            # existing "`>`/`>>`  redirection" block) still runs over every
            # token unfiltered and correctly resolves the redirect target on
            # its own -- this exclusion only stops the tee/sed/cp/mv loops
            # from ALSO misreading the same bytes as one of their own idiom
            # operands.
            #
            # Token-boundary test, matching the REAL `>`/`>>` scan below
            # exactly (mtoks[], not toks[], so a `>` that is only quoted DATA
            # can never match as an operator here either):
            #   `>` / `>>` / `2>` (bare, optionally fd-prefixed) consumes the
            #               NEXT non-empty, non-`&...` token (dup-to-fd `>&1`
            #               targets no file and is left unmarked).
            #   `>file` / `2>>file` (attached, optionally fd-prefixed)
            #               consumes only itself.
            delete stdout_redir
            for (j = 1; j <= m; j++) {
                mt = mtoks[j]
                if (mt == "") continue
                if (mt ~ /^[0-9]*>>?$/) {
                    stdout_redir[j] = 1
                    if (j + 1 <= m && toks[j+1] != "" && mtoks[j+1] !~ /^&/) {
                        stdout_redir[j+1] = 1
                    }
                } else if (mt ~ /^[0-9]*>>?[^ \t&]/) {
                    stdout_redir[j] = 1
                }
            }

            if (toks[1] == "tee") {
                for (j = 2; j <= m; j++) {
                    if (j in stdin_redir || j in stdout_redir) continue
                    if (toks[j] == "" || toks[j] ~ /^-/) continue
                    print curcwd SEP resolve_var(toks[j])
                }
            } else if (toks[1] == "sed") {
                has_i = 0
                nf = 0
                delete nfargs
                for (j = 2; j <= m; j++) {
                    if (j in stdin_redir || j in stdout_redir) continue
                    if (toks[j] ~ /^-i/) has_i = 1
                    if (toks[j] ~ /^-/) continue
                    if (toks[j] == "") continue
                    nf++
                    nfargs[nf] = toks[j]
                }
                if (has_i && nf >= 2) {
                    for (j = 2; j <= nf; j++) print curcwd SEP resolve_var(nfargs[j])
                }
            } else if (toks[1] == "cp" || toks[1] == "mv") {
                nf = 0
                delete nfargs
                for (j = 2; j <= m; j++) {
                    if (j in stdin_redir || j in stdout_redir) continue
                    if (toks[j] ~ /^-/) continue
                    if (toks[j] == "") continue
                    nf++
                    nfargs[nf] = toks[j]
                }
                if (nf >= 2) print curcwd SEP resolve_var(nfargs[nf])
            }

            # >/>>  redirection — token-boundary detection only (never a
            # mid-token char scan), so scanning stays anchored to whitespace
            # boundaries rather than manufacturing a target out of a `>`
            # sitting inside an already-multi-char token. The MATCH test reads
            # mtoks[] (quote-masked, #4245) so a `>` that is only quoted DATA
            # can never match as an operator; the actual target text is still
            # read from the ORIGINAL toks[] (unmasked) once a real operator is
            # confirmed.
            for (j = 1; j <= m; j++) {
                mt = mtoks[j]
                if (mt == "") continue
                if (mt ~ /^[0-9]*>>?$/) {
                    # Bare operator token. Dup-to-fd (`> &1`) is recognized by
                    # the NEXT token starting with `&` and excluded.
                    if (j + 1 <= m && toks[j+1] != "" && mtoks[j+1] !~ /^&/) {
                        print curcwd SEP resolve_var(toks[j+1])
                    }
                    continue
                }
                if (mt ~ /^[0-9]*>>?[^ \t&]/) {
                    # Attached form (`>file`, `2>file`, `>>file`).
                    op = toks[j]
                    sub(/^[0-9]*>>?/, "", op)
                    if (op != "") print curcwd SEP resolve_var(op)
                }
            }
            # repo#600: an unresolvable command word (or a cd behind a group /
            # conditional prefix) leaves every LATER segment with an unknown
            # cwd. Applied after this segment-s own writes were emitted.
            if (pend_unknown) curcwd = UNK
        }
    }'
}

# =============================================================================
# expand_leading_tilde() — shell-accurate tilde expansion for write targets
# (#4382, same fix family as the quote-aware `>` scanning of #4245/#4289).
#
# extract_write_targets() (below) is a plain-whitespace/quote-aware TOKENIZER,
# not a shell evaluator — it never performs word expansions (tilde, variable,
# glob, ...). A raw token like `~/.local/bin/x` was therefore resolved as a
# REPO-RELATIVE path (cwd-prefixed) even though the real shell would expand it
# to "$HOME/.local/bin/x" before `cp`/`mv`/`tee`/`sed -i`/redirection ever see
# it — producing a false-positive worktree-confinement deny on a write that
# actually lands far outside the repo (#4382).
#
# This performs ONLY the narrow, unambiguous piece of shell word-expansion
# tilde-expansion applies to: an UNQUOTED, UNESCAPED tilde as the FIRST
# character of the token, i.e. exactly the shell-eligible positions:
#   ~/rest        -> "$HOME/rest"
#   ~             -> "$HOME"
#   ~user/rest    -> "<user's home>/rest"   (only if that user resolves)
#   ~user         -> "<user's home>"
#
# Because qsplit() (the shared tokenizer, #3755) copies a quoted span
# VERBATIM including its quote characters, and leaves a literal backslash
# untouched, a token whose raw text does not start with a bare `~` was NOT
# eligible for shell tilde-expansion and MUST stay untouched here:
#   '~/x'   -> starts with a quote char, shell never expands it (stays literal)
#   \~/x    -> starts with a literal backslash, shell never expands it either
#   foo~/x  -> tilde is not the leading character -- not an expansion position
# Any of these three cases falls through unchanged (echoed back as-is), which
# preserves the existing (correct) repo-relative/deny behavior for them.
#
# `~user` lookup uses getent (Linux) / dscl (macOS) with the username passed
# as a plain CLI argument (never eval'd/interpolated into a shell string) so a
# hostile username token cannot inject a command. If the user cannot be
# resolved on this host, the token is returned UNCHANGED (falls back to the
# existing repo-relative treatment) -- consistent with this file's fail-open
# contract: uncertainty here biases toward the (safe) deny path, never toward
# a silent new allow.
# =============================================================================
expand_leading_tilde() {
    local tok="$1"
    # shellcheck disable=SC2088 # intentional: these `~`-prefixed case
    # patterns are literal PATTERN matches against an unexpanded leading
    # tilde in $tok (the whole point of this function), not an attempt at
    # shell tilde expansion.
    case "$tok" in
        '~')
            [[ -n "$HOME" ]] && { printf '%s' "$HOME"; return; }
            printf '%s' "$tok"
            return
            ;;
        '~/'*)
            if [[ -n "$HOME" ]]; then
                printf '%s' "$HOME/${tok#\~/}"
            else
                printf '%s' "$tok"
            fi
            return
            ;;
        '~'*)
            local rest="${tok#\~}"
            local user="${rest%%/*}"
            local remainder=""
            if [[ "$rest" == */* ]]; then
                remainder="/${rest#*/}"
            fi
            if [[ -n "$user" ]]; then
                local home=""
                if command -v getent >/dev/null 2>&1; then
                    home=$(getent passwd "$user" 2>/dev/null | cut -d: -f6)
                elif command -v dscl >/dev/null 2>&1; then
                    home=$(dscl . -read "/Users/$user" NFSHomeDirectory 2>/dev/null | awk '{print $2}')
                fi
                if [[ -n "$home" ]]; then
                    printf '%s' "${home}${remainder}"
                    return
                fi
            fi
            # Unresolvable ~user (unknown user / no lookup tool available):
            # leave untouched -- falls back to repo-relative resolution.
            printf '%s' "$tok"
            return
            ;;
        *)
            printf '%s' "$tok"
            return
            ;;
    esac
}

# Cheap pre-check keeps awk off the hot path for the ~99% of commands that have
# no recursive/force rm at all. The first alternative matches a literal `rm`
# command word; the second admits a command-word *substitution* (#72) — a
# closing `)` or backtick immediately followed by a recursive/force flag, as in
# `$(which rm) -rf /` or `` `which rm` -rf / `` — so extract_rm_targets() is
# invoked for that shape too. This gate is only an optimization: a false match
# here is harmless because extract_rm_targets() emits a target only for a real
# rm-flavored (substitution/rm command word + recursive-force flag) segment.
if echo "$COMMAND" | grep -qE 'rm[[:space:]]+-[a-zA-Z]*[rf]|[)`][[:space:]]+-[a-zA-Z]*[rf]'; then
    RM_TARGETS=$(extract_rm_targets "$COMMAND" | head -20)

    for target in $RM_TARGETS; do
        # Skip empty targets
        [[ -z "$target" ]] && continue

        # Skip known-safe patterns (allowlist)
        case "$target" in
            node_modules|./node_modules|*/node_modules)
                continue ;;
            target|./target|*/target)
                continue ;;
            dist|./dist|*/dist)
                continue ;;
            build|./build|*/build)
                continue ;;
            .loom/worktrees/*|*/.loom/worktrees/*)
                continue ;;
            .next|./.next|*/.next)
                continue ;;
            __pycache__|./__pycache__|*/__pycache__)
                continue ;;
            .pytest_cache|./.pytest_cache|*/.pytest_cache)
                continue ;;
            *.pyc)
                continue ;;
        esac

        # Expand a leading `~` or `$HOME` token so home-directory targets are
        # recognized the same way the literal-rm ALWAYS_BLOCK `$HOME`/`~`
        # patterns handle them (#72). extract_rm_targets() emits the raw token,
        # so a substitution-path target of `~` or `$HOME` (as in
        # `$(which rm) -rf ~`) would otherwise be treated as a CWD-relative path
        # and never flagged, an asymmetry vs. literal `rm -rf ~`/`rm -rf $HOME`.
        # Only bare-home and home-subpath forms are expanded — this mirrors the
        # literal floor exactly: bare `$HOME`/`~` deny (whole-home wipe) while a
        # home *subpath* expands to a deeper path and stays allowed.
        if [[ -n "$HOME" ]]; then
            # The `~` in the case globs below is a LITERAL match against the
            # emitted target token, not a path we want the shell to expand —
            # SC2088 (tilde-does-not-expand-in-quotes) is exactly the intended
            # behaviour here, so it is suppressed.
            # shellcheck disable=SC2088
            case "$target" in
                '~')          target="$HOME" ;;
                '~/'*)        target="$HOME/${target#\~/}" ;;
                '$HOME')      target="$HOME" ;;
                '$HOME/'*)    target="$HOME/${target#\$HOME/}" ;;
                '${HOME}')    target="$HOME" ;;
                '${HOME}/'*)  target="$HOME/${target#\$\{HOME\}/}" ;;
            esac
        fi

        # -------------------------------------------------------------
        # UNRESOLVED SHELL VARIABLE IN AN rm TARGET, under guards.rmScope=repo
        # (#239). `target` still carries any `$VAR` reference the `~`/`$HOME`
        # case above didn't expand — extract_rm_targets() is a tokenizer, not
        # a shell evaluator. The CWD-relative fallback a few lines below
        # silently reinterprets an unresolved token as "<CWD>/$target" — the
        # ONE interpretation guaranteed to land inside the repo when cwd is
        # inside it, regardless of what the variable actually expands to at
        # runtime. That is the #239 regression: `rm -rf "$p"` at repo cwd,
        # `$p` really pointing six directories outside the repo, silently
        # ALLOWED because it happened to be unresolvable.
        #
        # Reuses mark_expandable_dollars() (the write-confinement helper
        # above, #4921/#4927) so "which `$` would the real shell expand" has
        # exactly one definition in this file. The POLICY is deliberately
        # narrower than write-confinement's — see skills/repo/SKILL.md's
        # `rmScope` row for the documented rationale — the "middle option":
        #   (1) the variable IS the path root — nothing literal precedes the
        #       first unexpanded `$` (`$p`, `$(mktemp -d)`, `/$X/evil`). This
        #       guard cannot tell whether the runtime value is absolute or
        #       relative, so it is ALWAYS denied when rmScope=repo — exactly
        #       write confinement's own root-unresolved case.
        #   (2) the variable is in a LATER directory component, with a real
        #       literal path root before it (`build/$sub/out`,
        #       `./cache/$name/tmp`). The root here IS known (literal, or
        #       cwd when relative), so the KNOWN prefix — everything before
        #       the first unexpanded `$`, trimmed to its directory — is
        #       scope-tested on its own via _rm_scope_in_scope(): in scope ->
        #       ALLOW (the rm stays inside the area rmScope=repo already
        #       permits), not in scope / unusable -> DENY. This is the
        #       OPPOSITE polarity from write confinement's equivalent case
        #       (which denies an in-scope known prefix, because there the
        #       risk is an escaping WRITE); here the risk is a DELETE landing
        #       OUTSIDE the repo, so an in-scope known prefix is exactly the
        #       evidence that risk did not materialize.
        #   - a `$` only in the FINAL path component (`rm -rf out/$stamp`)
        #       matches neither case: the directory is fully literal, so it
        #       falls through UNCHANGED to the ordinary resolution below —
        #       identical to today's behaviour, and to write confinement's
        #       own "deliberately not denied" carve-out for the same shape.
        #
        # Gated on rm_scope_repo_enabled() so guards.rmScope=off/permissive
        # stays byte-for-byte unchanged — no new denials appear when the
        # feature is off (the existing CWD-relative fallback still applies).
        # -------------------------------------------------------------
        _rm_literal_resolved=""
        if [[ "$target" == *'$'* ]] && rm_scope_repo_enabled; then
            mark_expandable_dollars "$target"
            _rmarked="$_MARKED_TOKEN"
            if [[ "$_rmarked" == *$'\001'* ]]; then
                if [[ "$_rmarked" == $'\001'* || "$_rmarked" == /$'\001'* ]]; then
                    # Case (1): path root unresolved. Two narrow SAME-COMMAND
                    # proofs may still vet the target (#581, ported from Loom
                    # #6520/#6676/#6805); see rm_scope_mktemp_same_command_safe()
                    # and rm_scope_literal_same_command_resolve(). Both scan a
                    # heredoc-body-MASKED copy of the command: a heredoc body is
                    # never a live top-level assignment of the current shell, so
                    # masking can only narrow (turn a decoy-inflated SAFE into
                    # UNSAFE), never widen.
                    mktemp_scan_text_ensure
                    # (a) Proven `NAME=$(mktemp -d)` target: fully vetted.
                    if rm_scope_mktemp_same_command_safe "$target" "$COMMAND_MKTEMP_SCAN"; then
                        continue
                    fi
                    # (b) Proven literal value: NOT skipped past the checks --
                    # the resolved path replaces the token and is judged below
                    # exactly like a literal `rm -rf /that/path`.
                    _rm_literal_resolved=$(rm_scope_literal_same_command_resolve "$target" "$COMMAND_MKTEMP_SCAN") || true
                    if [[ -z "$_rm_literal_resolved" ]]; then
                        deny "BLOCKED: rm target '${target}' is an unexpanded shell variable from the path root down, so this guard cannot tell where it resolves at runtime under guards.rmScope=repo — it may point far outside the repo (the #239 regression: an unresolvable target at a repo cwd was silently treated as repo-relative). Unresolvable rm targets fail closed. Spell out the literal path, or unroll the loop so each rm target is a concrete string." "rm-scope-unresolved-var"
                    fi
                fi
                _rdirpart=""
                case "$_rmarked" in
                    */*) _rdirpart="${_rmarked%/*}" ;;
                esac
                # A literal-resolved root (case 1b) is judged below as a whole
                # path, so the known-prefix test (case 2) does not apply.
                if [[ -z "$_rm_literal_resolved" && "$_rdirpart" == *$'\001'* ]]; then
                    # Case (2): unresolved variable in a directory component,
                    # with a known literal root. Build the effective path the
                    # same way the resolution below does (cwd-joined when
                    # relative), then test only the KNOWN prefix.
                    _reff=""
                    if [[ "$_rmarked" == /* ]]; then
                        _reff="$_rmarked"
                    elif [[ -n "$CWD" ]]; then
                        _reff="$CWD/$_rmarked"
                    fi
                    _rknown="${_reff%%$'\001'*}"
                    _rknown="${_rknown%/*}"
                    # Normalize BEFORE judging: a `..` traversal in the known
                    # prefix otherwise hands the test a prefix that is not
                    # where the resolved path actually starts.
                    [[ "$_rknown" == /* ]] && _rknown=$(normalize_abs_path "$_rknown")
                    if [[ "$_rknown" != /* || "$_rknown" == "/" ]] || ! _rm_scope_in_scope "$_rknown"; then
                        _rknown_desc="no usable known prefix"
                        [[ "$_rknown" == /* && "$_rknown" != "/" ]] && _rknown_desc="known prefix '${_rknown}'"
                        deny "BLOCKED: rm target '${target}' has an unexpanded shell variable in a directory component, and its ${_rknown_desc} is not verifiably inside repo scope under guards.rmScope=repo — this guard cannot tell where it resolves at runtime. Unresolvable rm targets fail closed. Spell out the literal path, or unroll the loop so each rm target is a concrete string." "rm-scope-unresolved-var"
                    fi
                    # Known prefix is in scope: this target is vetted, move on.
                    continue
                fi
            fi
        fi

        # Resolve path to absolute (raw — normalization happens next).
        #
        # Shell-accurate quote removal, for the CLASSIFICATION only (Loom
        # #4926/#6814, ported with #581): extract_rm_targets() emits tokens
        # with their quote characters preserved verbatim, so a quoted absolute
        # target (`'/opt/evil'`, `"/opt/evil"`) would start with a quote
        # rather than `/`, be misread as RELATIVE, be cwd-prefixed into
        # `<repo>/'/opt/evil'` and pass the in-repo prefix check -- admitting
        # an out-of-repo target merely by quoting it. Unquote a COPY; the
        # allowlist above and the deny messages keep the raw token. An
        # unterminated quote falls back to the raw token (today's verdict).
        _rmclassify="$target"
        strip_target_quoting "$target" && _rmclassify="$_UNQUOTED_TARGET"
        ABS_PATH=""
        if [[ -n "$_rm_literal_resolved" ]]; then
            ABS_PATH="$_rm_literal_resolved"
        elif [[ "$_rmclassify" = /* ]]; then
            ABS_PATH="$_rmclassify"
        elif [[ -n "$CWD" ]]; then
            ABS_PATH="$CWD/$_rmclassify"
        fi
        # Pre-normalization spelling, for the session-scratch carve-out's
        # `..`-segment refusal (see rm_scope_session_scratch_admits()).
        _rm_abs_raw="$ABS_PATH"

        # Lexically normalize the absolute target BEFORE the protected-path
        # check. This collapses //, resolves . and .., and strips trailing
        # slashes, so traversal/normalization tricks cannot smuggle a
        # root/system-dir deletion past the check below:
        #   /tmp/..  -> /        //etc     -> /etc
        #   /usr/./  -> /usr      /a/../../../etc -> /etc
        # Done in pure shell because `realpath -m` is GNU-only (no-ops on macOS).
        if [[ "$ABS_PATH" = /* ]]; then
            ABS_PATH=$(normalize_abs_path "$ABS_PATH")
        fi

        # Block catastrophic targets only: root, the user's home directory, and
        # any top-level directory (^/<one-segment>$ — covers /tmp, /home, /usr,
        # /var, /etc, /opt, /bin, /lib, …). Deeper paths are allowed.
        if [[ -n "$ABS_PATH" ]]; then
            if [[ "$ABS_PATH" == "/" ]] || \
               [[ -n "$HOME" && "$ABS_PATH" == "$HOME" ]] || \
               [[ "$ABS_PATH" =~ ^/[^/]+$ ]]; then
                deny "BLOCKED: rm on protected system path: $ABS_PATH" "rm-protected-path"
            fi

            # Opt-in repo-scoped strict mode (guards.rmScope:"repo" /
            # LOOM_RM_SCOPE=repo). The catastrophic top-level deny above stays
            # unconditional; here we additionally DENY any target that is
            # neither under the repo / worktree areas nor on the built-in
            # ephemeral allowlist. Default OFF preserves the permissive
            # behaviour byte-for-byte (rm_scope_repo_enabled() returns false).
            if rm_scope_repo_enabled; then
                # Repo/worktree areas + the built-in ephemeral allowlist,
                # via the SAME containment test the unresolved-variable
                # handling above uses (#239) — one definition of "in scope".
                # THIS SESSION'S OWN proven scratch directory (#581, Loom #8460)
                # is admitted LAST: the cheaper lexical tests above ran first,
                # and this one stats the filesystem and reads the marker.
                if ! _rm_scope_in_scope "$ABS_PATH" && ! rm_scope_session_scratch_admits "$ABS_PATH" "$_rm_abs_raw"; then
                    _rm_scratch_hint=""
                    _rm_scratch_root=$(resolve_scratch_root) || _rm_scratch_root=""
                    if [[ -n "$_rm_scratch_root" ]] && \
                       { [[ "$ABS_PATH" == "$_rm_scratch_root" ]] || [[ "$ABS_PATH" == "$_rm_scratch_root"/* ]]; }; then
                        _rm_scratch_hint=" This path is under the session-scratch root ($_rm_scratch_root), but this session cannot prove it owns it. The only removable path there is <root>/<this session's id> -- or something under it -- and that directory must be a real (non-symlink) directory holding a regular file named $SESSION_SCRATCH_MARKER whose 'session=' line names that same id. The root itself is never removable, and another session's directory never is."
                    fi
                    deny "BLOCKED: rm target outside repo scope (guards.rmScope=repo; set guards.rmScope:\"off\" in .claude/skills/repo/config.json to opt out): ${ABS_PATH}${_rm_scratch_hint}" "rm-scope-outside-repo"
                fi
            fi
        fi
    done
fi

# =============================================================================
# BASH-TOOL WRITE CONFINEMENT — worktree isolation for `>`/`>>`/tee/sed -i/
# cp/mv (issue #4178)
#
# guard-worktree-paths.sh confines Edit/Write tool calls to a builder's issue
# worktree, but the Bash tool has no equivalent confinement — a session denied
# on Edit/Write could fall back to a Bash write and land the same edit in the
# main checkout (the #4178 incident: sweep #4063 escaped this way and edited
# live guard hooks in the main checkout while its own worktree stayed clean).
#
# Gated by the SAME toggle as guard-worktree-paths.sh
# (guards.worktreeIsolation / LOOM_GUARD_WORKTREE_ISOLATION,
# worktree_isolation_guard_enabled() above) and only denies when a managed
# worktree actually exists somewhere for this repo — exactly the
# path_derived_allow() logic in guard-worktree-paths.sh, reimplemented here
# because this is a separate Bash-matcher hook with its own fail-open
# contract. A cheap substring pre-check keeps the segmenter off the hot path
# for the vast majority of Bash calls that contain none of the recognized
# write idioms at all.
#
# `<<` is ALSO in this pre-check (#331): a structured-interpreter (python/
# perl/ruby/node) heredoc body containing a write-mode marker but none of the
# literal shell bytes above -- `open(f, "w")`, `os.remove(...)`,
# `shutil.rmtree(...)` -- must still reach extract_write_targets() for its
# structured_body_has_write_marker() carve-out (see mask_heredoc_bodies_selective()
# above) to have a chance to convert that marker into the synthetic `> .`
# write idiom it deliberately injects. Without `<<` here, such a command never
# even reaches the segmenter and the write-mode marker is never seen -- a
# silent hole in the #331 safety floor ("a write-mode payload must still
# deny"), not merely a missed optimization. Heredocs are rare enough in
# practice that this stays a cheap, narrow widening of the pre-check, not a
# reintroduction of the hot-path cost this comment describes avoiding.
#
# NARROW ALLOWANCES (#582, reconciled from Loom's vendored copy). Each is an
# allow condition with its own proof, checked only for a target that already
# resolved inside the main checkout; none is a general worktree opt-out, and
# none applies when the configured isolation toggle is off (nothing is denied
# then anyway):
#   - registered worktree (Loom #7415): inside a git-registered, non-main
#     worktree that really exists (a directory carrying its own `.git` file),
#     e.g. `<main>/.claude/worktrees/x` made by a plain `git worktree add`.
#   - env-selected worktree (Loom #7415): under the hook process's OWN
#     inherited LOOM_WORKTREE_PATH pin. A pin at (or above) the main checkout
#     root is ignored, so one env var cannot switch confinement off; an inline
#     `LOOM_WORKTREE_PATH=... <cmd>` prefix never reaches this process.
#   - read-only-role scratch (Loom #6021): LOOM_ROLE names a role with no
#     Write/Edit tool AND the target is under `<main>/dist/`.
#   - same-command mktemp output (Loom #6949/#7986/#8221): `$NAME[/suffix]`
#     whose only binding is a proven `NAME=$(mktemp -d)` / `$(mktemp)`.
# DIFFERENCES FROM LOOM (intentional, all stricter): an allowance is judged on
# the target's PHYSICAL spelling when it differs from the lexical one; a raw
# target or cwd containing a `..` component never earns one (the kernel resolves `..`
# physically, normalize_abs_path() lexically); a registered root must exist as
# a real linked worktree and must not be the main root or an ancestor of it;
# the mktemp proof keeps repo#588's ordering/context rule, refuses a suffix
# with further expansion or `..`, refuses when TMPDIR is rebound (or the
# inherited TMPDIR is protected) and when the suffix's empty-expansion spelling
# (`mktemp` failed, NAME empty) lands in the protected area; a `cd "$NAME"`
# cwd is never admitted through the mktemp proof (a failed or no-op `cd`
# leaves the write in the prior cwd).
# =============================================================================

# Roles whose agent definitions grant no Write/Edit tool (Loom #6021). Builder
# and Doctor are deliberately absent; an unset or unknown LOOM_ROLE fails
# closed. LOOM_ROLE is set by Loom's own dispatcher -- it is a Loom concept with
# no REPO_* counterpart, the same treatment as LOOM_WORKTREE_ROOT above.
_WT_READONLY_ROLES=" architect auditor champion curator guide hermit judge "

# True if the CURRENT LOOM_ROLE identifies a role with no Write/Edit tool.
# Case-insensitive; empty/unset never matches.
_wt_readonly_role_active() {
    [[ -n "${LOOM_ROLE:-}" ]] || return 1
    local _role_lc
    _role_lc=$(printf '%s' "$LOOM_ROLE" | tr '[:upper:]' '[:lower:]') || return 1
    [[ "$_role_lc" =~ ^[a-z]+$ ]] || return 1
    [[ "$_WT_READONLY_ROLES" == *" ${_role_lc} "* ]]
}

# True if $1 (an absolute, normalized path) sits inside the well-known `dist/`
# scratch directory at the main-checkout root (either root spelling). A
# path-component boundary: `<main>/dist-other` never matches.
_wt_dist_scratch_path() {
    local _p="$1"
    [[ -n "$_p" ]] || return 1
    if [[ -n "${_WT_MAIN_ROOT:-}" ]]; then
        case "$_p" in
            "$_WT_MAIN_ROOT/dist"|"$_WT_MAIN_ROOT/dist"/*) return 0 ;;
        esac
    fi
    if [[ -n "${_WT_MAIN_ROOT_LOGICAL:-}" ]]; then
        case "$_p" in
            "$_WT_MAIN_ROOT_LOGICAL/dist"|"$_WT_MAIN_ROOT_LOGICAL/dist"/*) return 0 ;;
        esac
    fi
    return 1
}

if worktree_isolation_guard_enabled && \
   { [[ "$COMMAND_ASK_SCAN" == *">"* ]] || [[ "$COMMAND_ASK_SCAN" == *"tee"* ]] || \
     [[ "$COMMAND_ASK_SCAN" == *"sed"* ]] || [[ "$COMMAND_ASK_SCAN" == *"cp "* ]] || \
     [[ "$COMMAND_ASK_SCAN" == *"mv "* ]] || [[ "$COMMAND_ASK_SCAN" == *"<<"* ]]; }; then
    _WT_WRITE_BASE=""
    _WT_WRITE_BASE_DONE=""

    # Derive the TRUE main-checkout root — NOT REPO_ROOT. REPO_ROOT is resolved
    # via `git rev-parse --show-toplevel`, which returns the *worktree* root when
    # CWD is a linked worktree (the canonical builder setup: `cd
    # .loom/worktrees/issue-N`). Keying the "resolves inside the main checkout"
    # test below on REPO_ROOT would therefore miss an absolute-path (or
    # `cd $MAIN && …`) Bash write into the main checkout issued from a builder's
    # own worktree — the exact "denied on Edit/Write → retry via Bash" escape
    # this block exists to close (#4178). Mirror the sibling guard
    # guard-worktree-paths.sh: `--git-common-dir/..` is always the main checkout,
    # from a worktree or not. `pwd -P` resolves symlinks so it matches the
    # git-resolved forms consistently (and sidesteps the macOS
    # /tmp -> /private/tmp mismatch vs. normalize_abs_path's lexical-only form).
    # Fail open to REPO_ROOT if the git resolution is unavailable.
    _WT_MAIN_ROOT=""
    _WT_MAIN_ROOT_LOGICAL=""
    if [[ -n "$CWD" && -d "$CWD" ]]; then
        _wt_common=$(cd "$CWD" 2>/dev/null && git rev-parse --git-common-dir 2>/dev/null) || _wt_common=""
        if [[ -n "$_wt_common" ]]; then
            _WT_MAIN_ROOT=$(cd "$CWD" 2>/dev/null && cd "$_wt_common/.." 2>/dev/null && pwd -P) || _WT_MAIN_ROOT=""
            # ...and the LOGICAL spelling of the same root (symlinks intact).
            # `pwd -P` alone was NOT sufficient (#4495): the write targets this
            # block compares against are produced by normalize_abs_path(), which
            # is lexical-only and therefore keeps a symlinked ancestor intact. A
            # repo reached through a symlinked path (a `/tmp` checkout on macOS,
            # a symlinked home, a bind-mounted workspace) produced targets that
            # never string-matched the physical root, so EVERY Bash write into
            # the main checkout was silently allowed there — the exact #4178
            # escape this block exists to close. Both spellings are checked.
            _WT_MAIN_ROOT_LOGICAL=$(cd "$CWD" 2>/dev/null && cd "$_wt_common/.." 2>/dev/null && pwd) || _WT_MAIN_ROOT_LOGICAL=""
        fi
    fi
    [[ -n "$_WT_MAIN_ROOT" ]] || _WT_MAIN_ROOT="$REPO_ROOT"
    [[ -n "$_WT_MAIN_ROOT_LOGICAL" ]] || _WT_MAIN_ROOT_LOGICAL="$_WT_MAIN_ROOT"

    # Diagnostic `context` string (issue #312) for every deny in this block:
    # the resolved main-checkout root, in BOTH spellings this guard's
    # containment tests actually compare against, plus REPO_ROOT (the
    # `git rev-parse --show-toplevel` value FROM CWD — the WORKTREE's own
    # toplevel when CWD is a linked worktree, per the header comment above) so
    # a future false-positive review can tell "the guard resolved an
    # unexpectedly broad root" apart from "the target genuinely sits inside
    # the checkout" without reproducing the session (#312's own report: a
    # denied write target that looked, on a static read, like it should have
    # been outside `_WT_MAIN_ROOT` — this makes the actually-resolved root
    # part of the persisted record instead of only the ephemeral, per-session
    # permissionDecisionReason text). An optional trailing arg adds the
    # specific resolved write-target path (`_wabs`/`_wknown`) the containment
    # test judged, when the call site has one.
    _wt_confinement_context() {
        local _target="${1:-}" _target_physical="${2:-}"
        local _ctx="wtMainRoot=${_WT_MAIN_ROOT} wtMainRootLogical=${_WT_MAIN_ROOT_LOGICAL} repoRoot=${REPO_ROOT} cwd=${CWD}"
        [[ -n "$_target" ]] && _ctx="${_ctx} target=${_target}"
        [[ -n "$_target_physical" ]] && _ctx="${_ctx} targetPhysical=${_target_physical}"
        printf '%s' "$_ctx"
    }

    # "Worktree isolation is actually in play for this repo/session" — a
    # managed worktree exists somewhere under the worktree base derived from
    # the SAME main-checkout root the containment tests use. Resolved lazily
    # and cached, so a command with no confinement-relevant target never pays
    # for the find(1). Defined here (inside the block) because it reads the
    # block-local _WT_MAIN_ROOT / _WT_WRITE_BASE* state.
    _wt_isolation_in_play() {
        if [[ -z "$_WT_WRITE_BASE_DONE" ]]; then
            _WT_WRITE_BASE=$(resolve_worktree_root "$_WT_MAIN_ROOT")
            _WT_WRITE_BASE_DONE=1
        fi
        _any_managed_worktree_exists "$_WT_WRITE_BASE"
    }

    # True if $1 (an absolute, normalized path) sits anywhere in the area this
    # guard protects: inside a managed worktree, inside the main checkout
    # (either spelling), or under the configured worktree base (which may live
    # on an external volume, outside the main checkout entirely).
    # Physical (symlink-resolved) spelling of an absolute path that may not
    # exist yet — the write TARGET usually doesn't. normalize_abs_path() is
    # lexical-only, so it keeps a symlinked ancestor intact; walk up to the
    # longest ancestor that does exist, resolve THAT with `pwd -P`, and
    # re-append the remainder. Prints nothing when the path is relative or no
    # ancestor resolves, so callers can treat empty as "no second spelling".
    _wt_physical_form() {
        local _p="$1" _dir _tail="" _resolved
        [[ "$_p" == /* ]] || return 0
        _dir="$_p"
        while [[ -n "$_dir" && "$_dir" != "/" && ! -d "$_dir" ]]; do
            _tail="/${_dir##*/}$_tail"
            _dir="${_dir%/*}"
            [[ -z "$_dir" ]] && _dir="/"
        done
        [[ -d "$_dir" ]] || return 0
        _resolved=$(cd "$_dir" 2>/dev/null && pwd -P) || return 0
        [[ -n "$_resolved" ]] || return 0
        printf '%s%s' "$_resolved" "$_tail"
    }

    # The string comparisons, run against ONE spelling of the target.
    _wt_in_protected_area_spelling() {
        local _p="$1"
        [[ -n "$_p" ]] || return 1
        _in_any_managed_worktree "$_p" && return 0
        if [[ -n "$_WT_MAIN_ROOT" ]]; then
            case "$_p" in
                "$_WT_MAIN_ROOT"|"$_WT_MAIN_ROOT"/*) return 0 ;;
                "$_WT_MAIN_ROOT_LOGICAL"|"$_WT_MAIN_ROOT_LOGICAL"/*) return 0 ;;
            esac
        fi
        if [[ -z "$_WT_WRITE_BASE_DONE" ]]; then
            _WT_WRITE_BASE=$(resolve_worktree_root "$_WT_MAIN_ROOT")
            _WT_WRITE_BASE_DONE=1
        fi
        if [[ -n "$_WT_WRITE_BASE" ]]; then
            case "$_p" in
                "$_WT_WRITE_BASE"|"$_WT_WRITE_BASE"/*) return 0 ;;
            esac
        fi
        return 1
    }

    # True if $1 (an absolute, normalized path) sits anywhere in the protected
    # area, tested against BOTH the target's own spelling and its physical
    # spelling.
    #
    # The second test closes the remaining half of the #4495 class. That fix
    # captured a logical spelling of the ROOTS, which covers a symlink at or
    # below the repo root — but not one in an ANCESTOR of it. Both roots here
    # are derived through git, which reports physical paths, so a target
    # written through a symlinked ancestor (`/var/... -> /private/var/...` on
    # macOS, where every `mktemp` path is exactly that; a symlinked home; a
    # bind-mounted workspace) matched neither root spelling and EVERY Bash
    # write into the main checkout was silently allowed. Loom's vendored copy
    # still has this gap — verified by probing both guards with the two
    # spellings of the same fixture, which is also why this repo's own
    # write-confinement tests were red before this change.
    #
    # Resolved LAZILY, only after the direct comparisons have all failed, so a
    # command whose target is already physical never pays for the subshell.
    _wt_in_protected_area() {
        local _p="$1" _pp
        [[ -n "$_p" ]] || return 1
        _wt_in_protected_area_spelling "$_p" && return 0
        _pp=$(_wt_physical_form "$_p")
        [[ -n "$_pp" && "$_pp" != "$_p" ]] || return 1
        _wt_in_protected_area_spelling "$_pp"
    }

    # The worktree location to point a denied write at (Loom #7415): the
    # ACTUALLY configured worktree root (LOOM_WORKTREE_ROOT env > worktree.root
    # config, repo config over legacy .loom > in-repo default, i.e.
    # resolve_worktree_root()) instead of a hardcoded `.loom/worktrees`, which
    # is wrong for any repo that relocates its worktree root.
    _wt_worktree_hint() {
        if [[ -z "$_WT_WRITE_BASE_DONE" ]]; then
            _WT_WRITE_BASE=$(resolve_worktree_root "$_WT_MAIN_ROOT") || _WT_WRITE_BASE=""
            _WT_WRITE_BASE_DONE=1
        fi
        if [[ -n "$_WT_WRITE_BASE" ]]; then
            printf '%s/issue-<N>' "$_WT_WRITE_BASE"
        else
            printf '.loom/worktrees/issue-<N>'
        fi
    }

    # Shared tail of every deny in this block (Loom #6110): the reliable
    # opt-out is the config toggle; an inline env prefix never reaches this
    # hook, which runs as a separate process with its own environment.
    _WT_OPTOUT_HINT="Not a Builder and need to write here directly? Set guards.worktreeIsolation:false in .loom/config.json (or in .claude/skills/repo/config.json, which takes precedence) for the session -- an inline 'REPO_GUARD_WORKTREE_ISOLATION=0 <command>' or 'LOOM_GUARD_WORKTREE_ISOLATION=0 <command>' prefix does NOT work (this hook runs as a separate process)."
    _WT_RESOLVE_HINT="Need this variable resolved instead? Declare it literally in the SAME command, before the write: VAR=/literal/path; <write> -- the same-command resolver substitutes it before this check runs, and the resolved path is still judged by this same containment rule, so a declaration can never grant more than writing that literal path outright."

    # True if $1 is a single raw path spelling with no `..` component. The
    # kernel resolves `..` PHYSICALLY (through any symlink before it) while
    # normalize_abs_path() pops it LEXICALLY, so a target spelled with `..`
    # can land somewhere other than its normalized form claims. The narrow
    # allowances below never trust such a spelling.
    _wt_no_dotdot() {
        [[ "/$1/" != */../* ]]
    }

    # ---------------------------------------------------------------------
    # Env-selected worktree -- LOOM_WORKTREE_PATH (Loom #7415). ALLOW-ONLY:
    # the hook's OWN inherited pin (set by a launcher that owns one process
    # per worktree) admits a write under it, matching guard-worktree-paths.sh.
    # Ignored when absent, when it does not resolve to a directory, and when
    # it resolves to the main checkout root OR ANY ANCESTOR of it (stricter
    # than Loom, which drops only the exact root): a pin there would switch
    # the whole confinement off.
    # ---------------------------------------------------------------------
    _WT_ENV_WT=""
    _WT_ENV_WT_LEX=""
    _WT_ENV_WT_DONE=""
    _wt_env_pin_resolve() {
        local _phys _lex _r
        [[ -n "$_WT_ENV_WT_DONE" ]] && return 0
        _WT_ENV_WT_DONE=1
        [[ -n "${LOOM_WORKTREE_PATH:-}" && "$LOOM_WORKTREE_PATH" == /* ]] || return 0
        _phys=$(cd "$LOOM_WORKTREE_PATH" 2>/dev/null && pwd -P 2>/dev/null) || _phys=""
        _phys="${_phys%/}"
        [[ -n "$_phys" ]] || return 0
        _lex=$(normalize_abs_path "$LOOM_WORKTREE_PATH") || _lex=""
        _lex="${_lex%/}"
        for _r in "$_WT_MAIN_ROOT" "$_WT_MAIN_ROOT_LOGICAL"; do
            [[ -n "$_r" ]] || continue
            # The pin equals, or contains, the main checkout: not a pin.
            if [[ "$_r" == "$_phys" || "$_r" == "$_phys"/* ]]; then
                return 0
            fi
            if [[ -n "$_lex" ]] && [[ "$_r" == "$_lex" || "$_r" == "$_lex"/* ]]; then
                return 0
            fi
        done
        _WT_ENV_WT="$_phys"
        [[ -n "$_lex" && "$_lex" != "$_phys" ]] && _WT_ENV_WT_LEX="$_lex"
        return 0
    }
    _wt_under_env_worktree() {
        local _p="$1"
        [[ -n "$_p" ]] || return 1
        _wt_env_pin_resolve
        [[ -n "$_WT_ENV_WT" ]] || return 1
        case "$_p" in
            "$_WT_ENV_WT"|"$_WT_ENV_WT"/*) return 0 ;;
        esac
        if [[ -n "$_WT_ENV_WT_LEX" ]]; then
            case "$_p" in
                "$_WT_ENV_WT_LEX"|"$_WT_ENV_WT_LEX"/*) return 0 ;;
            esac
        fi
        return 1
    }

    # ---------------------------------------------------------------------
    # Registered-but-unmanaged git worktrees (Loom #7415). A worktree made by a
    # plain `git worktree add` nested under the main checkout carries no
    # `.loom-managed` sentinel, so the main-root prefix test denied it even
    # though git treats it as a separate working tree. Consult
    # `git worktree list --porcelain` and accept any registered entry OTHER
    # than the main one, in both root spellings.
    #
    # TRUST BOUNDARY (#4245): this widens recognition from "worktrees Loom
    # created" to "worktrees git knows about" -- the sentinel was never an
    # authentication boundary either. What is NOT widened: the main checkout's
    # own working tree. Stricter than Loom: an entry must currently exist as a
    # real linked worktree (a directory holding its own `.git` file -- a stale
    # or prunable entry would otherwise let a write CREATE files in the main
    # checkout), and an entry equal to, or an ancestor of, either main-root
    # spelling is skipped. Resolved lazily and cached; builtins only per entry.
    # ---------------------------------------------------------------------
    _WT_REG_ROOTS=""
    _WT_REG_ROOTS_DONE=""
    # Populates _WT_REG_ROOTS (newline-separated) once per hook run. Called
    # directly -- never through `$(...)` -- so the cache survives into the
    # caller's shell. Always returns 0.
    _wt_registered_worktree_roots() {
        local _line _wtp _alt _r _skip
        if [[ -z "$_WT_REG_ROOTS_DONE" ]]; then
            _WT_REG_ROOTS_DONE=1
            if [[ -n "$_WT_MAIN_ROOT" && -d "$_WT_MAIN_ROOT" ]]; then
                while IFS= read -r _line; do
                    [[ "$_line" == "worktree "* ]] || continue
                    _wtp="${_line#worktree }"
                    [[ "$_wtp" == /* ]] || continue
                    _wtp="${_wtp%/}"
                    [[ -n "$_wtp" ]] || continue
                    _skip=""
                    for _r in "$_WT_MAIN_ROOT" "$_WT_MAIN_ROOT_LOGICAL"; do
                        [[ -n "$_r" ]] || continue
                        if [[ "$_r" == "$_wtp" || "$_r" == "$_wtp"/* ]]; then
                            _skip=1
                        fi
                    done
                    [[ -z "$_skip" ]] || continue
                    [[ -d "$_wtp" && -f "$_wtp/.git" ]] || continue
                    _WT_REG_ROOTS+="${_wtp}"$'\n'
                    _alt=""
                    if [[ -n "$_WT_MAIN_ROOT_LOGICAL" && "$_WT_MAIN_ROOT_LOGICAL" != "$_WT_MAIN_ROOT" ]]; then
                        case "$_wtp" in
                            "$_WT_MAIN_ROOT"/*) _alt="${_WT_MAIN_ROOT_LOGICAL}/${_wtp#"$_WT_MAIN_ROOT"/}" ;;
                            "$_WT_MAIN_ROOT_LOGICAL"/*) _alt="${_WT_MAIN_ROOT}/${_wtp#"$_WT_MAIN_ROOT_LOGICAL"/}" ;;
                        esac
                    fi
                    [[ -n "$_alt" ]] && _WT_REG_ROOTS+="${_alt}"$'\n'
                done < <(git -C "$_WT_MAIN_ROOT" worktree list --porcelain 2>/dev/null || true)
            fi
        fi
        return 0
    }

    # True if $1 (absolute, normalized) sits inside a registered worktree that
    # is not the main checkout.
    _wt_in_registered_worktree() {
        local _p="$1" _root
        [[ -n "$_p" ]] || return 1
        _wt_registered_worktree_roots
        [[ -n "$_WT_REG_ROOTS" ]] || return 1
        while IFS= read -r _root; do
            [[ -n "$_root" ]] || continue
            if [[ "$_p" == "$_root" || "$_p" == "$_root"/* ]]; then
                return 0
            fi
        done <<< "$_WT_REG_ROOTS"
        return 1
    }

    # Same-command mktemp output admission for a root-unknown write target.
    # wt_write_mktemp_same_command_safe() supplies the shared proof; this adds
    # the two checks that need this block's protected-area roots:
    #   - the INHERITED TMPDIR (what `mktemp` will use) must be absolute and
    #     outside the protected area;
    #   - the EMPTY-EXPANSION spelling of the target (`mktemp` or the realpath
    #     chain failed, NAME is empty, the write goes to `/<suffix>`) must be
    #     outside the protected area too.
    _wt_mktemp_target_admitted() {
        local _t="$1" _mode="${2:-template}" _td _empty
        mktemp_scan_text_ensure
        wt_write_mktemp_same_command_safe "$_t" "$COMMAND_MKTEMP_SCAN" "$_mode" || return 1
        _empty="${_WT_WRITE_SUFFIX:-/}"
        _empty=$(normalize_abs_path "$_empty") || return 1
        _wt_in_protected_area "$_empty" && return 1
        if [[ -n "${TMPDIR:-}" ]]; then
            [[ "$TMPDIR" == /* ]] || return 1
            _td=$(normalize_abs_path "$TMPDIR") || return 1
            _wt_in_protected_area "$_td" && return 1
        fi
        return 0
    }

    # repo#597: a `;`-chained `NAME=$(mktemp -d); cd $NAME; <write>` falls to
    # `cd` -> $HOME when NAME is empty (mktemp failed), so $HOME must be set,
    # absolute and outside the protected area. An all-`&&` chain short-
    # circuits instead and needs nothing here.
    _wt_mktemp_cd_home_ok() {
        local _h
        [[ "$_WT_CD_CHAIN_LOOSE" -eq 1 ]] || return 0
        [[ -n "${HOME:-}" && "$HOME" == /* ]] || return 1
        _h=$(normalize_abs_path "$HOME") || return 1
        _wt_in_protected_area "$_h" && return 1
        return 0
    }

    WRITE_TARGETS=$(extract_write_targets "$COMMAND_ASK_SCAN" "$CWD" | head -20)
    while IFS=$'\037' read -r _wcwd _wtarget; do
        [[ -z "$_wtarget" ]] && continue

        # Same-command $VAR/${VAR} resolution (#4881) happens inside
        # extract_write_targets(): a target whose leading `$NAME`/`${NAME}`
        # matched an assignment earlier in the SAME command arrives here
        # already substituted. A target it could NOT resolve (no matching
        # assignment, or a $-prefixed token that is not a bare variable
        # reference at all — `$(...)`, `${VAR:-x}`, an inherited env var)
        # arrives UNCHANGED and is deliberately still treated as a literal
        # repo-relative path here, exactly as it was before #4881 — an
        # unresolvable target must stay fail-closed, or every assignment
        # shape the scan cannot parse becomes a free #4178 bypass (#4914
        # review).
        #
        # Shell-accurate tilde expansion (#4382): an unquoted/unescaped
        # leading `~/` or `~user/` in the raw token is what the real shell
        # would expand BEFORE cp/mv/tee/sed -i/redirection ever see it, so
        # expand it here before the relative-path resolution below runs.
        # Quoted ('~/x') / escaped (\~/x) tildes are left untouched (see
        # expand_leading_tilde()'s doc comment) — no change to their
        # existing repo-relative treatment.
        _wtarget=$(expand_leading_tilde "$_wtarget")

        # -------------------------------------------------------------
        # Unknown cwd (repo#600). extract_write_targets() emits the
        # _LOOM_UNKNOWN_CWD sentinel as the cwd once a segment ran a command
        # word it cannot resolve statically (c$()d, ${C:-c}d, $X, a glob) or
        # a cd behind a group/conditional prefix: bash may have changed
        # directory there, so a RELATIVE write after it lands somewhere this
        # guard cannot name -- possibly the main checkout, even when the
        # previous tracked cwd was /tmp or a proven mktemp directory. Fail
        # closed for a relative target; an absolute one, a `$NAME`-rooted one
        # (judged by the root-unknown rules below), and the no-isolation case
        # keep their existing treatment.
        # -------------------------------------------------------------
        if [[ "$_wcwd" == "$_LOOM_UNKNOWN_CWD" ]]; then
            mark_expandable_dollars "$_wtarget"
            if [[ "$_MARKED_TOKEN" != $'\001'* ]]; then
                _wclassify="$_wtarget"
                strip_target_quoting "$_wtarget" && _wclassify="$_UNQUOTED_TARGET"
                if [[ "$_wclassify" != /* ]]; then
                    if _wt_isolation_in_play; then
                        deny "BLOCKED: Bash-tool write target '${_wtarget}' is relative, but an earlier command word in this same command could change the working directory in a way this guard cannot resolve statically (a quoted/escaped/expanded spelling of cd such as c''d, c\\d, \"c\"d, c\$()d or \${C:-c}d, a command word built from a variable or glob, or a cd inside a group/conditional), so it cannot tell where the write lands — it may resolve inside the main repository checkout ('${_WT_MAIN_ROOT}'), and a Loom-managed worktree exists in this repository. Unknown working directories fail closed (repo#600). Spell cd literally, or write to an explicit absolute path — inside your issue worktree ($(_wt_worktree_hint)) for repo files, or a spelled-out /tmp path for scratch. ${_WT_OPTOUT_HINT} (#4178)" "worktree-write-confinement-unknown-cwd" "$(_wt_confinement_context "$_wtarget")"
                    fi
                    continue
                fi
            fi
        fi

        # -------------------------------------------------------------
        # Unresolved `$…` write targets must fail CLOSED, in every cwd (#4921)
        #
        # extract_write_targets() never expands variables; a target it cannot
        # resolve is emitted as the RAW token (`$A/evil.sh`). The resolution
        # below then treats that literal as a relative path and cwd-prefixes
        # it — which fabricates a location the write will not actually have.
        # From a MAIN-CHECKOUT cwd that fabrication happened to land inside
        # the main checkout, so the containment test denied and the token was
        # (accidentally) fail-closed. From a LINKED-WORKTREE cwd — the
        # canonical builder setup, `cd .loom/worktrees/issue-N` — the very
        # same fabrication instead walks straight back up into the acting
        # worktree's own `.loom-managed` sentinel, so check (a) below ALLOWED
        # it before the main-root containment test ever ran, no matter what
        # the variable would expand to at runtime (#4921). That silently
        # defeated the fail-closed backstop for every unresolvable `$` shape
        # (`$(...)`, `${VAR:-x}`, an inherited env var, a chained or
        # conflicting same-command assignment) in the ONE operating mode the
        # #4178 guard exists to protect.
        #
        # So: decide on the token's SHAPE before trusting either test. A
        # target is denial-worthy when the unexpanded `$` makes its LOCATION
        # (not merely its filename) unknowable:
        #
        #   (1) the token IS a variable from the root down — it either starts
        #       with an expandable `$` (`> $DEST`, `tee "${OUT}"`,
        #       `> $(mktemp)`) or starts with `/$` (`> /$X`, `> /$X/evil`).
        #       The path root itself is unknown, so the variable may hold (or
        #       complete) an absolute path into the main checkout and the cwd
        #       prefix is pure invention. Denied regardless of where cwd is.
        #   (2) an expandable `$` appears in a DIRECTORY component of the
        #       resolved path (`> $A/evil`, `> ./$A/evil`, `cd $A && > f`)
        #       AND the known prefix — everything before the first `$`, i.e.
        #       the only part that is a real path — is inside the area this
        #       guard protects, or there is no usable known prefix at all
        #       (it is relative, or it normalizes to `/` as in
        #       `> /tmp/../$A/evil`). An unknown directory component under the
        #       repo can resolve into the main checkout (directly, or via
        #       `..`), and neither the sentinel walk-up nor the containment
        #       test can see it.
        #
        # Deliberately NOT denied (no new false positives — these keep their
        # existing treatment):
        #   - a `$` only in the FINAL component (`> out-$STAMP.log`,
        #     `sed -i s/a/b/ src/$f`): the directory is fully known and really
        #     is cwd-relative, so the sentinel check (a) and the main-root
        #     containment test below are meaningful again.
        #   - a known prefix OUTSIDE the protected area (`> /tmp/$D/f.log`):
        #     the write lands where this guard protects nothing.
        #   - a LITERAL `$` the shell would never expand — inside a
        #     single-quoted span or backslash-escaped (`> '$A/evil'`) — which
        #     really is a relative path to a file named `$A` (mirrors the
        #     quoted-tilde treatment in expand_leading_tilde, #4382).
        #
        # Fail-open contract is preserved: like every other deny in this
        # block, it only fires when a managed worktree actually exists for
        # this repo (_wt_isolation_in_play).
        # -------------------------------------------------------------
        if [[ "$_wtarget" == *'$'* || "$_wcwd" == *'$'* ]]; then
            mark_expandable_dollars "$_wtarget"
            _wmarked="$_MARKED_TOKEN"
            # The cwd itself can carry the unexpanded `$` instead of the
            # target (`cd $A && echo x > f.sh` — extract_write_targets threads
            # the unresolved `cd` argument into curcwd), so mark it too and
            # judge the JOINED path. A cwd that is absolute and `$`-free
            # marks to itself, leaving every existing case byte-identical.
            _wmarkedcwd=""
            if [[ -n "$_wcwd" ]]; then
                mark_expandable_dollars "$_wcwd"
                _wmarkedcwd="$_MARKED_TOKEN"
            fi
            if [[ "$_wmarked" == *$'\001'* || ( "$_wmarked" != /* && "$_wmarkedcwd" == *$'\001'* ) ]]; then
                # (1) Root unknown — the token is a variable from the root
                # down (`$DEST`, `$(mktemp)`) or is root + a variable
                # (`/$X`, `/$X/evil`, whose runtime value picks the top-level
                # directory — the main checkout's own included).
                if [[ "$_wmarked" == $'\001'* || "$_wmarked" == /$'\001'* ]]; then
                    if _wt_isolation_in_play; then
                        # Same-command mktemp output (#582, Loom #6949/#7986/
                        # #8221): `$NAME[/suffix]` whose only binding is a
                        # proven `NAME=$(mktemp -d)` / `$(mktemp)` lands in a
                        # fresh scratch path outside every worktree -- see
                        # wt_write_mktemp_same_command_safe() and
                        # _wt_mktemp_target_admitted() for the exact proof.
                        # Anything it cannot prove falls through to the deny.
                        if _wt_mktemp_target_admitted "$_wtarget"; then
                            continue
                        fi
                        deny "BLOCKED: Bash-tool write target '${_wtarget}' is an unexpanded shell variable from the path root down, so this guard cannot tell where the write lands — it may resolve to an absolute path inside the main repository checkout ('${_WT_MAIN_ROOT}'), and a Loom-managed worktree exists in this repository. Unresolvable write targets fail closed (#4921). ${_WT_RESOLVE_HINT} Otherwise, write to an explicit literal path — inside your issue worktree ($(_wt_worktree_hint)) for repo files, or a spelled-out /tmp path for scratch. ${_WT_OPTOUT_HINT} (#4178)" "worktree-write-confinement-unresolved-var" "$(_wt_confinement_context "$_wtarget")"
                    fi
                    continue
                fi
                # (2) Unknown DIRECTORY component. Build the effective path
                # the same way the resolution below does (cwd-joined when
                # relative), then test only the KNOWN prefix — everything
                # before the first unexpanded `$`, trimmed to its directory.
                _weff=""
                if [[ "$_wmarked" == /* ]]; then
                    _weff="$_wmarked"
                elif [[ -n "$_wmarkedcwd" ]]; then
                    _weff="$_wmarkedcwd/$_wmarked"
                fi
                _wdirpart=""
                [[ "$_weff" == */* ]] && _wdirpart="${_weff%/*}"
                if [[ "$_wdirpart" == *$'\001'* ]]; then
                    # repo#597: a RELATIVE target whose unknown directory is the
                    # tracked `cd $NAME` cwd, with NAME a proven same-command
                    # `mktemp -d` output. The tracked cwd must be EXACTLY
                    # `$NAME` (no suffix: `cd $NAME/sub` can fail), the binding
                    # must be a directory (template-dir: `-d` required, or the
                    # cd fails on a file), and wt_write_mktemp_cd_chain_ok()
                    # must prove the cd ran, succeeded and was not undone.
                    # Then rebuild the `$NAME/<target>` spelling and apply the
                    # same proof + protected-area checks as (1); the suffix
                    # rules refuse `..`, expansions, quotes and globs, so a
                    # relative escape out of the scratch dir still denies.
                    if [[ "$_wmarked" != /* && "$_wmarkedcwd" == *$'\001'* ]] \
                       && _wt_isolation_in_play; then
                        _wjoin="$_wcwd"
                        if [[ "$_wjoin" == \"*\" && ${#_wjoin} -ge 2 ]]; then
                            _wjoin="${_wjoin:1:${#_wjoin}-2}"
                        fi
                        if [[ "$_wjoin" =~ ^\$([A-Za-z_][A-Za-z0-9_]*)$ ]] || \
                           [[ "$_wjoin" =~ ^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$ ]]; then
                            if wt_write_mktemp_cd_chain_ok "${BASH_REMATCH[1]}" "$COMMAND" \
                               && _wt_mktemp_target_admitted "${_wjoin}/${_wtarget}" template-dir \
                               && _wt_mktemp_cd_home_ok; then
                                continue
                            fi
                        fi
                    fi
                    _wknown="${_weff%%$'\001'*}"
                    _wknown="${_wknown%/*}"
                    # Normalize BEFORE judging: a `..` traversal in the known
                    # prefix (`> /tmp/../$A/evil`) otherwise hands the test a
                    # prefix that is not where the write actually starts.
                    [[ "$_wknown" == /* ]] && _wknown=$(normalize_abs_path "$_wknown")
                    if [[ "$_wknown" != /* || "$_wknown" == "/" ]]; then
                        # No usable known prefix — either it is relative (no
                        # cwd to join against) or it collapses to `/`, i.e.
                        # the first real path component IS the variable
                        # (`> /$A/evil`, `> /tmp/../$A/evil`), whose runtime
                        # value picks a top-level directory, the main
                        # checkout's own included. Same verdict as (1).
                        if _wt_isolation_in_play; then
                            deny "BLOCKED: Bash-tool write target '${_wtarget}' has an unexpanded shell variable as its first real path component, so this guard cannot tell where the write lands — it may resolve inside the main repository checkout ('${_WT_MAIN_ROOT}'), and a Loom-managed worktree exists in this repository. Unresolvable write targets fail closed (#4921). ${_WT_RESOLVE_HINT} Otherwise, write to an explicit literal path — inside your issue worktree ($(_wt_worktree_hint)) for repo files, or a spelled-out /tmp path for scratch. ${_WT_OPTOUT_HINT} (#4178)" "worktree-write-confinement-unresolved-var" "$(_wt_confinement_context "$_wtarget")"
                        fi
                    elif _wt_in_protected_area "$_wknown"; then
                        if _wt_isolation_in_play; then
                            deny "BLOCKED: Bash-tool write target '${_wtarget}' contains an unexpanded shell variable in a directory component, and its known prefix ('${_wknown}') is inside this repository's worktree/checkout area — this guard cannot tell whether the expanded path stays in your worktree or lands in the main repository checkout ('${_WT_MAIN_ROOT}'). Unresolvable write targets fail closed (#4921). ${_WT_RESOLVE_HINT} Otherwise, write to an explicit literal path — inside your issue worktree ($(_wt_worktree_hint)) for repo files, or a spelled-out /tmp path for scratch. ${_WT_OPTOUT_HINT} (#4178)" "worktree-write-confinement-unresolved-var" "$(_wt_confinement_context "$_wknown")"
                        fi
                    fi
                    continue
                fi
            fi
        fi

        # Shell-accurate quote removal, for the classification only (#4926):
        # `'/main/evil'` / `"/main/evil"` reach here with their quote
        # characters intact (qsplit's contract), so they start with a quote
        # rather than `/` and the test below would call an ABSOLUTE path
        # relative and cwd-prefix it into a location the write never has.
        # Unquote a COPY: extract_rm_targets()/parse_force_ops() keep their
        # verbatim tokens, and the deny message below still quotes the raw
        # `$_wtarget` the operator actually typed. An unterminated quote keeps
        # the raw token (today's verdict) rather than risk widening a deny.
        _wclassify="$_wtarget"
        strip_target_quoting "$_wtarget" && _wclassify="$_UNQUOTED_TARGET"

        # Same split for the CWD half of the pair (#4933). A tracked
        # `cd <dir>` argument reaches here with its quote characters intact
        # too — extract_write_targets() deliberately builds curcwd from the
        # RAW, quote-preserved token so the unresolved-`$` block ABOVE can
        # still tell a literal single-quoted `$` from an expandable one
        # (stripping the quotes in awk instead turned every `$` in the last
        # `cd` segment into an "unresolvable" deny). By the time we get here
        # that judgement is already made, so unquote a COPY for the join —
        # otherwise a quoted absolute `cd` argument would be joined with its
        # quote characters embedded and normalize to a path the write never
        # has. Only touched when a quote character is actually present, so a
        # quote-free cwd (every ordinary case) stays byte-identical; an
        # unterminated quote falls back to the raw value, i.e. today's
        # verdict, never widening a deny into an allow.
        _wcwdclassify="$_wcwd"
        if [[ "$_wcwd" == *"'"* || "$_wcwd" == *'"'* ]]; then
            strip_target_quoting "$_wcwd" && _wcwdclassify="$_UNQUOTED_TARGET"
        fi

        # Resolve to absolute; a relative target with no resolvable cwd is
        # ambiguous — skip it (allow on uncertainty, never deny on it).
        _wabs=""
        if [[ "$_wclassify" == /* ]]; then
            _wabs="$_wclassify"
        elif [[ -n "$_wcwdclassify" ]]; then
            _wabs="$_wcwdclassify/$_wclassify"
        else
            continue
        fi
        # Raw (pre-normalization) spelling, kept for the narrow allowances'
        # `..` refusal below (_wt_no_dotdot).
        _wraw="$_wabs"
        _wabs=$(normalize_abs_path "$_wabs")

        # Second spelling of the same target: physical, symlink-resolved.
        # normalize_abs_path() is lexical-only, so it keeps a symlinked
        # ancestor intact, while BOTH roots below come from git and are
        # therefore physical. Without this, a target written through a
        # symlinked ancestor matches neither root and every Bash write into
        # the main checkout is silently allowed — the #4495 class, of which
        # the earlier logical-root fix caught only the half where the symlink
        # sits at or below the repo root. On macOS every `mktemp` path is a
        # `/var -> /private/var` symlink, which is why this repo's own
        # write-confinement tests were red. Loom's vendored copy still has
        # this gap. Resolved once per target and only when it differs.
        _wabsp=$(_wt_physical_form "$_wabs")
        [[ "$_wabsp" == "$_wabs" ]] && _wabsp=""

        # (a) Already inside some managed worktree -> allow. This is exactly
        # where a builder is supposed to write. Checked against both spellings
        # so the allow stays as wide as the deny below.
        _in_any_managed_worktree "$_wabs" && continue
        [[ -n "$_wabsp" ]] && _in_any_managed_worktree "$_wabsp" && continue

        # Not under any worktree. If it's also not under the main checkout,
        # there is nothing this guard protects (e.g. /tmp scratch) -> allow.
        [[ -z "$_WT_MAIN_ROOT" ]] && continue
        # Both spellings are tested with a QUOTED `case`, never a `for` over an
        # unquoted expansion: `for x in ${var:+"$var"}` word-splits (and globs)
        # its result despite the inner quotes, so a target containing a space
        # or a glob character would be compared as fragments. The pre-existing
        # single-spelling code was a quoted `case` for that reason, and adding
        # the second spelling must not regress it.
        #
        # Defensive, not a fix for an observed escape: an UNQUOTED spaced
        # redirect target genuinely splits in the shell too (`> /a/b c` really
        # does redirect to `/a/b`), so the guard is right to see the first
        # word there. The exposure would be a quoted target whose expansion
        # reaches this comparison intact.
        _wt_root_hit=""
        case "$_wabs" in
            "$_WT_MAIN_ROOT"|"$_WT_MAIN_ROOT"/*) _wt_root_hit=1 ;;
            "$_WT_MAIN_ROOT_LOGICAL"|"$_WT_MAIN_ROOT_LOGICAL"/*) _wt_root_hit=1 ;;
        esac
        if [[ -z "$_wt_root_hit" && -n "$_wabsp" ]]; then
            case "$_wabsp" in
                "$_WT_MAIN_ROOT"|"$_WT_MAIN_ROOT"/*) _wt_root_hit=1 ;;
                "$_WT_MAIN_ROOT_LOGICAL"|"$_WT_MAIN_ROOT_LOGICAL"/*) _wt_root_hit=1 ;;
            esac
        fi
        [[ -n "$_wt_root_hit" ]] || continue

        # NARROW ALLOWANCES (#582; see the block header). Each is judged on
        # the PHYSICAL spelling of the target whenever it differs from the
        # lexical one -- that is where the kernel actually writes, so a
        # symlinked ancestor can neither earn nor dodge an allowance -- and a
        # raw spelling containing `..` never qualifies (the kernel resolves
        # `..` physically, normalize_abs_path() lexically). None of them
        # consults anything the acting command controls.
        _wjudge="$_wabs"
        [[ -n "$_wabsp" ]] && _wjudge="$_wabsp"
        if _wt_no_dotdot "$_wraw"; then
            # (b) The worktree this hook process is pinned to via its OWN
            # inherited LOOM_WORKTREE_PATH (Loom #7415).
            if _wt_under_env_worktree "$_wjudge"; then
                continue
            fi
            # (c) A read-only-by-role session staging a scratch artifact under
            # `<main>/dist/` (Loom #6021) -- never any other main-checkout path,
            # never Builder/Doctor, never an unset/unknown role.
            if _wt_dist_scratch_path "$_wjudge" && _wt_readonly_role_active; then
                continue
            fi
            # (d) Inside a git-registered, non-main worktree nested under the
            # main checkout, sentinel or not (Loom #7415).
            if _wt_in_registered_worktree "$_wjudge"; then
                continue
            fi
        fi

        # Target resolves inside the main checkout and outside every
        # worktree. Deny only if worktree isolation is actually in play for
        # this repo/session (a managed worktree exists somewhere); otherwise
        # fail open — a repo/session that has never created a worktree is
        # unaffected, mirroring guard-worktree-paths.sh exactly. The worktree
        # base is resolved off the same main-checkout root so the "a managed
        # worktree exists" gate stays consistent with the containment test.
        if _wt_isolation_in_play; then
            deny "BLOCKED: Bash-tool write to '${_wabs}' resolves to the main repository checkout ('${_WT_MAIN_ROOT}'), but a Loom-managed worktree exists elsewhere in this repository (this check cannot verify it belongs to the acting session — see #4245). This is a worktree-isolation bypass via Bash redirection/tee/sed -i/cp/mv — do NOT retry the write through Bash. cd into your issue worktree ($(_wt_worktree_hint)) and write there instead. ${_WT_OPTOUT_HINT} (#4178)" "worktree-write-confinement" "$(_wt_confinement_context "$_wabs" "$_wabsp")"
        fi
    done <<< "$WRITE_TARGETS"
fi

# =============================================================================
# DELETE without WHERE - Database safety
# =============================================================================

# Gated by the SQL DDL/DML guard toggle. DB-engine repos opt out via
# guards.sqlDdl:false or LOOM_GUARD_SQL=0. sql_guard_enabled() is consulted only
# after the DELETE-FROM-without-WHERE match, keeping the config read off the hot
# path for non-SQL commands.
if echo "$COMMAND_NO_COMMENT" | grep -qiE 'DELETE[[:space:]]+FROM[[:space:]]+' && \
   ! echo "$COMMAND_NO_COMMENT" | grep -qiE 'WHERE[[:space:]]+'; then
    sql_guard_enabled && deny "BLOCKED: DELETE FROM without WHERE clause" "sql-delete-no-where"
fi

# =============================================================================
# FORCE-OP BRANCH SCOPE - branch-aware git push --force / git reset --hard
#
# Gated by guards.forceScope / LOOM_FORCE_SCOPE (see force_scope_mode() above).
#   - "all"       (default): every force op asks — byte-for-byte the pre-#3674
#                            behaviour, so existing tests still see an ask.
#   - "protected"          : ask only when the resolved target is a protected
#                            branch (repo default / main / master) or the branch
#                            identity is ambiguous (detached HEAD / unresolved);
#                            own working branches pass straight through.
#   - "off"                : never ask/deny here.
#
# The explicit main/master force-push hard-denies in ALWAYS_BLOCK_PATTERNS above
# already fired for those forms and are NOT reachable here in ANY mode — this
# block only ever downgrades to ask/allow, never weakens a hard deny.
#
# A cheap pre-check keeps the config read + segment parser off the hot path for
# the ~99% of commands with no force flag at all.
# =============================================================================

# _force_detached_reset_recovery_ok CWD RESET_TARGET -- succeed only for the
# "resync my own Loom-managed worktree" recovery shape (#582, ported from Loom
# #5772 / #7530): a `git reset --hard` line (RESET_TARGET non-empty -- a push
# line never carries one) whose CWD is a linked git worktree, NOT the main
# checkout, with a `.loom-managed` sentinel at that worktree's own toplevel,
# and whose target is exactly one of:
#   HEAD | origin/main | origin/master | origin/<repo default branch>
#   | origin/<the branch recorded in the sentinel's `# Branch:` line>
# A detached worktree reset to any of those names no protected branch and no
# other agent's work (it moves only this worktree's detached HEAD). A sibling
# branch, an absent/malformed sentinel, an unmanaged location, the main
# checkout, a quoted/unrecognized target and every push all fail and keep the
# existing ask. Stricter than Loom, which accepts a sentinel on ANY ancestor of
# the cwd: here the sentinel must sit at the cwd's own git toplevel and that
# toplevel must not be the main checkout. Every failure returns 1.
_force_detached_reset_recovery_ok() {
    local fcwd="$1" rtarget="$2" top common main def own
    [[ -n "$rtarget" && -n "$fcwd" && "$fcwd" == /* && -d "$fcwd" ]] || return 1
    top=$(git -C "$fcwd" rev-parse --show-toplevel 2>/dev/null) || return 1
    [[ -n "$top" && -d "$top" ]] || return 1
    top=$(cd "$top" 2>/dev/null && pwd -P) || return 1
    common=$(git -C "$fcwd" rev-parse --git-common-dir 2>/dev/null) || return 1
    [[ -n "$common" ]] || return 1
    main=$(cd "$fcwd" 2>/dev/null && cd "$common/.." 2>/dev/null && pwd -P) || return 1
    [[ -n "$main" && "$top" != "$main" ]] || return 1
    [[ -f "$top/.loom-managed" && ! -L "$top/.loom-managed" ]] || return 1
    case "$rtarget" in
        HEAD|origin/main|origin/master) return 0 ;;
    esac
    def=$(resolve_default_branch "$fcwd") || def=""
    if [[ -n "$def" && "$rtarget" == "origin/$def" ]]; then
        return 0
    fi
    own=$(_managed_worktree_branch "$top") || own=""
    [[ -n "$own" && "$rtarget" == "origin/$own" ]]
}

# Pre-check and parse both read COMMAND_ASK_SCAN, not the raw
# COMMAND_NO_COMMENT (repo#188 parity fix) — a `--force` mentioned inside a
# quoted `--body`/`-m` value is prose, and asking on it stalls ordinary issue
# and commit authoring. Matches Loom's vendored copy, which has always scanned
# the redacted copy here.
# repo#612: the pre-check reads a copy with every quote and backslash removed,
# so a quoted or partially quoted spelling (`git reset "--hard"`, `"-f"`,
# `--ha''rd`, `"git"`) still reaches the parser, which decodes each word
# statically. `push`/`reset` also admit the parse without a literal `git`, so
# an undecodable command word (`$G reset --hard`) fails closed there.
_FORCE_PRESCAN="${COMMAND_ASK_SCAN//[\"\'\\]/}"
if [[ "$_FORCE_PRESCAN" == *git* || "$_FORCE_PRESCAN" == *push* || "$_FORCE_PRESCAN" == *reset* ]] && \
   echo "$_FORCE_PRESCAN" | grep -qE '(--force|--force-with-lease|(^|[[:space:]])-f([[:space:]]|$)|--hard|push[^;&|]*[[:space:]]\+[^[:space:]])'; then
    _FORCE_MODE=$(force_scope_mode)
    if [[ "$_FORCE_MODE" != "off" ]]; then
        _FORCE_OPS=$(parse_force_ops "$COMMAND_ASK_SCAN" "$CWD")
        if [[ -n "$_FORCE_OPS" ]]; then
            if [[ "$_FORCE_MODE" == "all" ]]; then
                # Preserve pre-#3674 behaviour byte-for-byte: any force op asks.
                ask "Command requires confirmation: $COMMAND" "force-op:all"
            fi
            # "protected" mode: ask only for protected-branch or ambiguous
            # targets; allow own working branches. resolve_default_branch() plus
            # the main/master literals form the protected set.
            #
            # Fields 4/5 (repo#612) are the resolved --git-dir/GIT_DIR and
            # --work-tree/GIT_WORK_TREE selectors, empty when absent.
            while IFS=$'\037' read -r _fcpath _ftarget _fresettarget _fgitdir _fworktree; do
                [[ -z "$_ftarget" ]] && _ftarget="@HEAD@"
                _fcwd="$_fcpath"
                [[ -z "$_fcwd" ]] && _fcwd="$CWD"
                # repo#612: the git word, a global option or the subcommand
                # could not be decoded statically next to a force flag, so
                # whether this is a force op at all is unknown -- ask.
                if [[ "$_fcwd" == "$_LOOM_UNDECODABLE_FORCE_OP" ]]; then
                    ask "Command requires confirmation: $COMMAND (possible force operation whose git command, global option, or subcommand word cannot be decoded statically — e.g. it depends on a variable or substitution — so this guard cannot tell what it does)" "force-op:undecodable"
                fi
                # repo#600: a command word parse_force_ops() could not resolve
                # statically (a disguised/expanded cd spelling, a cd inside a
                # group or conditional, or -- repo#608 -- a git -C operand
                # with an expansion, glob or unbalanced quote) left the cwd
                # UNKNOWN, so neither the
                # branch identity nor the out-of-tree exemption can be judged
                # -- ask, never silently allow. repo#612: the same applies to
                # a -C / --git-dir / --work-tree / GIT_DIR / GIT_WORK_TREE
                # operand that needs runtime expansion.
                if [[ "$_fcwd" == "$_LOOM_UNKNOWN_CWD" ]]; then
                    ask "Command requires confirmation: $COMMAND (force operation after a command word that may change the working directory, or with a git -C/--git-dir/--work-tree/GIT_DIR/GIT_WORK_TREE operand, that this guard cannot resolve statically — e.g. a quoted/escaped/expanded spelling of cd, or an operand built from an expansion or glob — so the branch it acts on is unknown)" "force-op:unknown-cwd"
                fi
                # repo#612: a refspec that cannot be decoded statically
                # (`git push --force origin "$B"`) may name a protected branch.
                # Same out-of-tree exemption as a protected target (#330).
                if [[ "$_ftarget" == "@UNRESOLVED@" ]]; then
                    if ! _force_op_dirs_outside_known_roots "$_fcwd" "$_fgitdir" "$_fworktree"; then
                        ask "Command requires confirmation: $COMMAND (force push to a refspec this guard cannot resolve statically, so it may name a protected branch)" "force-op:unresolved-target"
                    fi
                    continue
                fi
                if [[ "$_ftarget" == "@HEAD@" ]]; then
                    _fbranch=""
                    if [[ -n "$_fgitdir" ]]; then
                        # An explicit git dir decides which HEAD moves; it
                        # is already absolute (resolved by the parser).
                        _fbranch=$(git --git-dir="$_fgitdir" symbolic-ref --short HEAD 2>/dev/null || true)
                    elif [[ -n "$_fcwd" ]]; then
                        _fbranch=$(git -C "$_fcwd" symbolic-ref --short HEAD 2>/dev/null || true)
                    fi
                    # repo#632: the files a hard reset rewrites live in the
                    # WORK TREE, not in the git dir / cwd the branch above was
                    # read from. A --work-tree/GIT_WORK_TREE aimed at a
                    # checkout of this repo family (the protected main
                    # checkout, or another worktree) is judged by THAT
                    # checkout's branch -- same verdict as running from there.
                    # A work tree provably outside the family (scratch clone,
                    # #320/#330) is left to the existing logic.
                    if [[ -n "$_fworktree" ]] && \
                       ! _force_op_cwd_outside_known_roots "$_fworktree"; then
                        _wtbranch=$(git -C "$_fworktree" symbolic-ref --short HEAD 2>/dev/null || true)
                        if [[ -z "$_wtbranch" ]]; then
                            ask "Command requires confirmation: $COMMAND (force operation rewrites a work tree in this repository whose branch cannot be resolved)" "force-op:detached"
                        fi
                        _wtdefault=$(resolve_default_branch "$_fworktree")
                        if [[ "$_wtbranch" == "main" || "$_wtbranch" == "master" ]] || \
                           { [[ -n "$_wtdefault" && "$_wtbranch" == "$_wtdefault" ]]; }; then
                            ask "Command requires confirmation: $COMMAND (force operation rewrites the work tree of protected branch '$_wtbranch')" "force-op:protected"
                        fi
                    fi
                    # The managed-worktree recovery carve-out is only proven
                    # for the plain cwd shape; an explicit git dir or work
                    # tree keeps asking.
                    if [[ -z "$_fbranch" && -z "$_fgitdir" && -z "$_fworktree" ]] && \
                       _force_detached_reset_recovery_ok "$_fcwd" "$_fresettarget"; then
                        # Managed-worktree recovery reset (#582, Loom #5772 /
                        # #7530) -- see _force_detached_reset_recovery_ok().
                        # Only a detached RESET line can reach here; it names
                        # no branch, so nothing below has a target to judge.
                        continue
                    fi
                    if [[ -z "$_fbranch" ]]; then
                        # Detached HEAD / unresolved identity is ambiguous — ask,
                        # never silently allow (fail toward asking) — UNLESS the
                        # force op's CWD is unambiguously outside every repo
                        # root this guard tracks (main checkout + managed
                        # worktrees), e.g. a bare /tmp scratch clone (#320). A
                        # hard reset there cannot touch a protected branch of
                        # THIS repo, so asking buys no safety and stalls
                        # headless runs with no human to answer. Any CWD
                        # inside the repo/a worktree, or one this guard cannot
                        # classify, still asks exactly as before.
                        if ! _force_op_dirs_outside_known_roots "$_fcwd" "$_fgitdir" "$_fworktree"; then
                            ask "Command requires confirmation: $COMMAND (force operation on a detached or unresolved branch)" "force-op:detached"
                        fi
                    fi
                    _ftarget="$_fbranch"
                fi
                _fdefault=$(resolve_default_branch "${_fgitdir:-$_fcwd}")
                if [[ "$_ftarget" == "main" || "$_ftarget" == "master" ]] || \
                   { [[ -n "$_fdefault" && "$_ftarget" == "$_fdefault" ]]; }; then
                    # Protected-branch target — ask, never silently allow
                    # (fail toward asking) — UNLESS the force op's CWD is
                    # unambiguously outside every repo root this guard
                    # tracks (main checkout + managed worktrees), e.g. a
                    # bare /tmp scratch clone (#330, mirroring #320's
                    # force-op:detached exemption above). A hard reset
                    # there cannot touch a protected branch of THIS repo,
                    # so asking buys no safety and stalls headless runs
                    # with no human to answer. Any CWD inside the repo/a
                    # worktree, or one this guard cannot classify, still
                    # asks exactly as before.
                    if ! _force_op_dirs_outside_known_roots "$_fcwd" "$_fgitdir" "$_fworktree"; then
                        ask "Command requires confirmation: $COMMAND (force operation targets protected branch '$_ftarget')" "force-op:protected"
                    fi
                fi
            done <<< "$_FORCE_OPS"
            # No protected/ambiguous target matched — fall through to allow.
        fi
    fi
fi

# =============================================================================
# TMPFS BUILD/SCRATCH DIR — deny an assignment that parks build output in RAM
# (repo#454, split out of rjwalters/loom#8512 via rjwalters/loom#8570)
#
# THE INCIDENT: a sweep parked a Cargo target dir in `/dev/shm`. `/dev/shm` is
# a tmpfs — a RAM-backed filesystem — so the 6.2 GB of build output written
# there was 6.2 GB of the host's memory, held for 2.5 days after the build
# exited (nothing deletes a scratch dir nobody remembers creating), and it
# drove a 15.7 GiB worker into a kernel OOM-kill storm. Loom has since added a
# RECLAIM pass (`loom_daemon::tmpfs_reclaim`) that frees an orphan six hours
# after its last write. This block is the PREVENTION half: six hours of a small
# host's RAM is still six hours, and no reclaim can help at all while a build is
# actively writing into RAM it should never have been pointed at.
#
# WHAT IS CLASSIFIED — an EXPLICIT build/scratch-dir assignment carried by the
# command itself, in any of the four shapes the incident family can take:
#   1. a same-command (or `env`/`export`/`sudo`-prefixed) `CARGO_TARGET_DIR=…`
#   2. the same for `TMPDIR=…` (the generic scratch root — covers every build
#      tool, not just cargo, which is why this block is NOT cargo-specific)
#   3. `--target-dir <path>` / `--target-dir=<path>` anywhere in the segment
#   4. a `target-dir = <path>` line being WRITTEN into a cargo config file
#      (`.cargo/config.toml`) — the persistent form of the same mistake
#
# …plus, since repo#462, the AMBIENT effective target dir on a bare `cargo`
# invocation that assigns nothing at all: an exported `CARGO_TARGET_DIR`
# inherited from the agent's environment, or a PRE-EXISTING `[build]
# target-dir` in `.cargo/config.toml` (repo-local, walked-up, or
# `$CARGO_HOME`). See the repo#462 header further down for that resolution
# chain and the command-word gate that keeps it off the hot path.
#
# CLASSIFY BY MOUNT TYPE, NEVER BY PATH PREFIX. A hardcoded `/dev/shm` check
# would be both over- and under-inclusive: a systemd host very commonly mounts
# `/tmp` as tmpfs (so `TMPDIR=/tmp/build` is the same hazard under a completely
# ordinary-looking path), while a disk-backed bind mount at `/dev/shm/…` is not
# a hazard at all. So the resolved path is matched against the LONGEST-PREFIX
# entry of the kernel's own mount table and gated on its fs type being `tmpfs`
# or `ramfs`.
#
# UNMEASURABLE MUST NOT DENY. There is no `/proc/mounts` on macOS (and none in
# a sandbox that hides it). "Unknown" means NO OPINION — the block exits
# silently, exactly the contract Loom's own tmpfs_reclaim pass uses. This is
# also why the whole block is a no-op on every developer Mac: it can only ever
# fire where it can actually read the mount table.
#
# THE "ALREADY IN RAM" EXEMPTION. If the acting cwd is itself on the SAME RAM
# mount as the resolved target dir, the assignment is not redirecting anything
# into RAM that wasn't already there (a whole checkout under a tmpfs `/tmp` is a
# different, pre-existing situation), and the deny message's advice — "use the
# on-disk default `<repo>/target` instead" — would be nonsense, because that
# default is on the same tmpfs. Staying silent there keeps the deny honest: it
# fires only when there IS a sanctioned on-disk alternative to name.
#
# Tier is DENY, not ask: the failure is host-wide, silent, and outlives the
# command by days, while refusing costs nothing (no work is lost — the build
# simply has to name an on-disk path), and an ask is unanswerable in the
# headless sweeps where this actually happens.
#
# Gated by tmpfs_scratch_guard_enabled() (guards.tmpfsScratch /
# REPO_GUARD_TMPFS_SCRATCH / LOOM_GUARD_TMPFS_SCRATCH), consulted only AFTER
# the cheap substring pre-check has matched, so the jq config read never
# touches the hot path.
# =============================================================================
_TMPFS_SCRATCH_CACHE=""
tmpfs_scratch_guard_enabled() {
    guard_toggle_enabled _TMPFS_SCRATCH_CACHE tmpfsScratch true LOOM_GUARD_TMPFS_SCRATCH REPO_GUARD_TMPFS_SCRATCH
}

# The mount table to classify against. `/proc/mounts` is the kernel's live view
# on Linux; the env override exists so this block's tests can drive a FIXTURE
# table (there is no portable way to create a real tmpfs mount in a test, and a
# test that only ran on a tmpfs-having Linux host would be a test that never
# ran in CI). REPO_* wins over the legacy LOOM_* name, same precedence contract
# as every other toggle in this file.
tmpfs_mount_table_path() {
    printf '%s' "${REPO_GUARD_MOUNTS_FILE:-${LOOM_GUARD_MOUNTS_FILE:-/proc/mounts}}"
}

# =============================================================================
# _mount_entry_for_path() — longest-prefix mount lookup.
#
# Args: $1 = an ABSOLUTE, already-normalize_abs_path'd path; $2 = mount table.
# Prints "<fstype><TAB><mountpoint>" for the mount that actually backs that
# path, or nothing at all when the table is unreadable/has no covering entry.
# "Nothing" is the unmeasurable signal the caller treats as no-opinion.
#
# Longest-prefix is the whole point: `/dev/shm` and `/` both "match" a path
# under `/dev/shm`, and only the longer one describes the filesystem the bytes
# land on. Ties go to the LAST entry, because a later mount at the same
# mountpoint is an overmount that shadows the earlier one.
#
# Mountpoints in /proc/mounts are octal-escaped (a space is `\040`), so they are
# decoded before comparison. The decode is written out by hand rather than using
# awk's strtonum(), which is a gawk extension absent from the one-true-awk that
# ships as /usr/bin/awk on macOS and the BSDs.
# =============================================================================
_mount_entry_for_path() {
    local path="$1" table="$2"
    [[ -n "$path" && -n "$table" && -r "$table" ]] || return 1
    awk -v path="$path" '
        function decode(s,   out, i, n, c, code) {
            if (index(s, "\\") == 0) return s
            out = ""; n = length(s); i = 1
            while (i <= n) {
                c = substr(s, i, 1)
                if (c == "\\" && i + 3 <= n && substr(s, i + 1, 3) ~ /^[0-7][0-7][0-7]$/) {
                    code = (substr(s, i + 1, 1) + 0) * 64 + (substr(s, i + 2, 1) + 0) * 8 + (substr(s, i + 3, 1) + 0)
                    out = out sprintf("%c", code)
                    i += 4
                } else {
                    out = out c
                    i++
                }
            }
            return out
        }
        BEGIN { bestlen = -1; bestfs = ""; bestmp = "" }
        NF >= 3 {
            mp = decode($2); fs = $3
            ok = 0
            if (mp == "/") ok = (substr(path, 1, 1) == "/")
            else if (path == mp) ok = 1
            else if (substr(path, 1, length(mp) + 1) == mp "/") ok = 1
            if (ok && length(mp) >= bestlen) { bestlen = length(mp); bestfs = fs; bestmp = mp }
        }
        END { if (bestlen >= 0) printf "%s\t%s\n", bestfs, bestmp }
    ' "$table" 2>/dev/null
}

# =============================================================================
# tmpfs_scratch_assignments() — extract explicit build/scratch-dir assignments.
#
# Prints one `<name><TAB><value>` line per assignment found, where <name> is the
# human-facing spelling used in the deny message (`CARGO_TARGET_DIR`, `TMPDIR`,
# `--target-dir`). Values keep their raw spelling (relative paths are resolved
# by the caller, which is the only place that knows the cwd).
#
# Segmentation reuses this file's own qsplit() so a `foo && CARGO_TARGET_DIR=…
# cargo build` is seen as two segments and the assignment is still read from the
# segment it actually belongs to. Leading `VAR=value` runs are walked in the
# same order a shell reads them, and `sudo` / `env` / `export` prefixes are
# stepped over (each may carry its own flags before the assignments resume) —
# `export CARGO_TARGET_DIR=/dev/shm/x && cargo build` is the same hazard as the
# same-command form and must not escape by wearing a different hat.
#
# _HASLIVESUBST_AWK is load-bearing: since #433 (d4d7df0) qsplit() calls
# has_live_subst(), so every program that prepends _QSPLIT_AWK must prepend it
# too. Without it awk dies with "calling undefined function has_live_subst",
# this helper prints nothing, and every shape 1-3 deny silently turns into an
# allow — the failure mode the merge of main into this branch first produced.
# =============================================================================
tmpfs_scratch_assignments() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK"'
    function unq(v) {
        sub(/^["\047]/, "", v); sub(/["\047]$/, "", v)
        return v
    }
    function emitassign(t,   nm, vl) {
        nm = substr(t, 1, index(t, "=") - 1)
        if (nm != "CARGO_TARGET_DIR" && nm != "TMPDIR") return
        vl = unq(substr(t, index(t, "=") + 1))
        if (vl == "") return
        print nm "\t" vl
    }
    {
        $0 = qsplit($0)
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            j = 1
            while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) { emitassign(toks[j]); j++ }
            # `sudo`, `env` and `export` may each be followed by their own
            # flags and then by a further run of assignments; loop so a
            # `sudo -E env TMPDIR=… cargo build` is fully unwrapped.
            while (j <= m && (toks[j] == "sudo" || toks[j] == "env" || toks[j] == "export")) {
                j++
                while (j <= m && toks[j] ~ /^-/) j++
                while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) { emitassign(toks[j]); j++ }
            }
            # `--target-dir` is a cargo flag and is meaningless outside a
            # cargo invocation; anchor on the command word so this scan does
            # not fire on prose or write targets that merely mention the flag
            # text (git commit -am, sed -i, heredoc bodies, #461 review).
            if (!(toks[j] == "cargo" || toks[j] == "cross")) continue
            for (k = j; k <= m; k++) {
                if (toks[k] ~ /^--target-dir=/) {
                    v = unq(substr(toks[k], index(toks[k], "=") + 1))
                    if (v != "") print "--target-dir\t" v
                } else if (toks[k] == "--target-dir" && k < m) {
                    v = unq(toks[k + 1])
                    if (v != "") print "--target-dir\t" v
                }
            }
        }
    }'
}

# =============================================================================
# AMBIENT effective target dir (repo#462) — the PERSISTENT form of the same
# hazard, where the command itself carries no assignment at all.
#
# Everything above keys on an EXPLICIT assignment carried by the command
# (`CARGO_TARGET_DIR=…`, `TMPDIR=…`, `--target-dir`, a `target-dir = …` write
# into a cargo config). That covers the loom#8512 incident shape, but it goes
# silent on the two shapes where the RAM-backed target dir was configured
# EARLIER and every subsequent build inherits it invisibly:
#
#   1. `CARGO_TARGET_DIR` already exported in the agent's own environment (a
#      parent shell, a profile, a daemon env) — the hook is a child process,
#      so it simply reads its own inherited value;
#   2. a PRE-EXISTING `[build] target-dir` in `.cargo/config.toml` — repo-local,
#      any walked-up ancestor, or `$CARGO_HOME/config.toml`.
#
# In both, the command is a bare `cargo build`, so the first build after the
# setup is unguarded and so is every one after it.
#
# The three helpers below are ported from Loom's vendored
# `guard-destructive-generic.sh` (`_cargo_toml_target_dir_value()`,
# `_cargo_config_walk_up_target_dir()`, `_cargo_home_config_target_dir()`,
# used there by `cargo_clean_effective_target_dir()` for the cargo-clean-scope
# guard), per the ownership direction in `.loom/docs/guard-hooks.md`: this repo
# is the canonical upstream and Loom re-vendors from here, so the resolution
# chain needs to exist HERE rather than being referenced across the boundary.
#
# ONE DELIBERATE DEVIATION FROM THE VENDORED CHAIN: no `cargo config get
# build.target-dir` probe. The vendored site runs it because it fires only on
# a bare `cargo clean` (rare), whereas this site sees EVERY cargo invocation —
# spawning a cargo process on each one is a real, per-command latency cost on
# the hot path. It also buys almost nothing: `cargo config get` still requires
# `-Z unstable-options` on stable cargo, so it fails and falls through to this
# same manual walk-up in the overwhelmingly common case. The walk-up plus the
# `$CARGO_HOME` fallback below reproduces cargo's documented precedence
# (closest ancestor `.cargo/config.toml` wins, then the user-global one), which
# is exactly the resolution set #462's acceptance criteria name.
# =============================================================================

# Minimal TOML reader for a single `[build]` -> `target-dir` key. Not a general
# TOML parser: it only tracks top-level `[table]` headers so a `target-dir =
# "..."` line is attributed to the literal `[build]` table (not `[build.foo]`
# or an unrelated table), which is the one key this resolution needs.
_cargo_toml_target_dir_value() {
    local f="$1"
    [[ -f "$f" ]] || return 1
    awk '
        function strip(v) {
            gsub(/^[ \t]+/, "", v); gsub(/[ \t]+$/, "", v)
            gsub(/^"/, "", v); gsub(/"$/, "", v)
            gsub(/^\047/, "", v); gsub(/\047$/, "", v)
            return v
        }
        BEGIN { in_build = 0 }
        /^[ \t]*\[/ {
            line = $0
            gsub(/^[ \t]+/, "", line)
            in_build = (line ~ /^\[build\][ \t]*(#.*)?$/) ? 1 : 0
            next
        }
        in_build && /^[ \t]*target-dir[ \t]*=/ {
            val = $0
            sub(/^[^=]*=/, "", val)
            sub(/#.*$/, "", val)
            print strip(val)
            exit
        }
    ' "$f" 2>/dev/null
}

# Walks up from $1 to the filesystem root looking for `.cargo/config.toml` /
# `.cargo/config`, mirroring cargo's own directory-ancestor search order — the
# CLOSEST ancestor that sets `build.target-dir` wins. A relative value is
# resolved against the directory the config file itself was found in (cargo's
# documented behavior: a relative target-dir is relative to the config file's
# own location, not the invocation cwd). Prints "<path>\t<config file>".
_cargo_config_walk_up_target_dir() {
    local dir="$1" f val
    while [[ -n "$dir" ]]; do
        for f in "$dir/.cargo/config.toml" "$dir/.cargo/config"; do
            if [[ -f "$f" ]]; then
                val=$(_cargo_toml_target_dir_value "$f")
                if [[ -n "$val" ]]; then
                    [[ "$val" != /* ]] && val="$dir/$val"
                    printf '%s\t%s' "$val" "$f"
                    return 0
                fi
            fi
        done
        [[ "$dir" == "/" ]] && break
        dir=$(dirname "$dir")
    done
    return 1
}

# Lowest-precedence fallback: the user-global $CARGO_HOME/config.toml (default
# ~/.cargo/config.toml). Prints "<path>\t<config file>".
_cargo_home_config_target_dir() {
    local home="${CARGO_HOME:-$HOME/.cargo}" f val
    [[ -n "$home" ]] || return 1
    for f in "$home/config.toml" "$home/config"; do
        if [[ -f "$f" ]]; then
            val=$(_cargo_toml_target_dir_value "$f")
            if [[ -n "$val" ]]; then
                [[ "$val" != /* ]] && val="$home/$val"
                printf '%s\t%s' "$val" "$f"
                return 0
            fi
        fi
    done
    return 1
}

# =============================================================================
# tmpfs_ambient_target_dir() — resolve the effective cargo target dir for a
# command that carries NO explicit assignment of its own.
#
# Arg $1 = the acting cwd. Prints a single `<name><TAB><value><TAB><origin>`
# record in exactly the shape the classification loop below consumes, or
# nothing when no ambient setting exists (cargo's on-disk `<repo>/target`
# default is never a hazard and is deliberately not emitted — "no ambient
# setting" and "the default" are the same no-opinion answer here).
#
# Precedence follows cargo's own: the exported CARGO_TARGET_DIR wins over any
# config file, and the closest ancestor config wins over $CARGO_HOME's.
# =============================================================================
tmpfs_ambient_target_dir() {
    local base="$1" hit=""
    if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
        printf '%s\t%s\t%s\n' "CARGO_TARGET_DIR" "$CARGO_TARGET_DIR" \
            "exported in the agent's own environment, not by this command"
        return 0
    fi
    [[ -n "$base" ]] || return 1
    hit="$(_cargo_config_walk_up_target_dir "$base")" || hit=""
    [[ -n "$hit" ]] || { hit="$(_cargo_home_config_target_dir)" || hit=""; }
    [[ -n "$hit" ]] || return 1
    printf '%s\t%s\t%s\n' "build.target-dir" "${hit%%$'\t'*}" \
        "already set in ${hit#*$'\t'}, not by this command"
}

# =============================================================================
# tmpfs_cargo_command_present() — the ambient path's cheap gate.
#
# The ambient shapes have NO substring in the command to key on (the command is
# a bare `cargo build`), so the gate is a COMMAND-WORD anchor instead: prints
# "1" only when some segment's actual command word — after stepping over
# leading `VAR=value` assignments and `sudo`/`env`/`export` prefixes exactly as
# tmpfs_scratch_assignments() does — is `cargo` or `cross`.
#
# This is what keeps the config walk-up and the guards.tmpfsScratch jq read off
# the hot path: `git commit -m "note about cargo"` and `echo cargo build` reach
# this awk (they contain the substring) but stop here, and a command with no
# `cargo` substring at all never even reaches the awk.
# =============================================================================
tmpfs_cargo_command_present() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK"'
    {
        $0 = qsplit($0)
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            j = 1
            while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) j++
            while (j <= m && (toks[j] == "sudo" || toks[j] == "env" || toks[j] == "export")) {
                j++
                while (j <= m && toks[j] ~ /^-/) j++
                while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) j++
            }
            if (j <= m && (toks[j] == "cargo" || toks[j] == "cross")) { found = 1; exit }
        }
    }
    END { if (found) print "1" }'
}

# Cheap substring pre-check: nothing below runs — not the mount-table read, not
# the config read — unless the command literally carries one of the assignment
# spellings, or (for the ambient path) the bare word `cargo`/`cross`. Note
# `CARGO_TARGET_DIR` does not contain the lowercase `target-dir`, so both
# spellings are needed here. The pre-check reads the comment-stripped copy (the
# superset) rather than COMMAND_ASK_SCAN, because shape 4 below deliberately
# scans that superset — gating the whole block on the masked copy would skip
# shape 4 entirely. Both arms are pure-bash `[[ == * *]]` globs: a command that
# carries neither spelling nor the word `cargo` forks nothing at all.
_TMPFS_EXPLICIT_HINT=0
if [[ "$COMMAND_NO_COMMENT" == *"CARGO_TARGET_DIR="* || "$COMMAND_NO_COMMENT" == *"TMPDIR="* || \
      "$COMMAND_NO_COMMENT" == *"target-dir"* ]]; then
    _TMPFS_EXPLICIT_HINT=1
fi
# `cross` is here for the same reason it is in shape 3's anchor: it is a cargo
# wrapper and reads cargo's own config/env, so the ambient value applies to it
# identically. It does mean a command containing an unrelated `cross`/`across`
# pays one awk in tmpfs_cargo_command_present() — but no jq and no config read,
# because that anchor rejects it on the command word.
_TMPFS_CARGO_HINT=0
if [[ "$COMMAND_NO_COMMENT" == *cargo* || "$COMMAND_NO_COMMENT" == *cross* ]]; then
    _TMPFS_CARGO_HINT=1
fi

if [[ "$_TMPFS_EXPLICIT_HINT" == 1 || "$_TMPFS_CARGO_HINT" == 1 ]]; then
    _TMPFS_MOUNTS="$(tmpfs_mount_table_path)"
    # Unmeasurable => no opinion. This is the macOS/no-/proc/mounts exit, and it
    # is checked FIRST so a host that cannot classify never even reads config.
    _TMPFS_CARGO_CMD=0
    if [[ -r "$_TMPFS_MOUNTS" && "$_TMPFS_CARGO_HINT" == 1 ]]; then
        # Command-word anchor for the ambient path. Runs at most one awk, and
        # only on a command that already contains the `cargo` substring.
        [[ "$(tmpfs_cargo_command_present "$COMMAND_ASK_SCAN")" == "1" ]] && _TMPFS_CARGO_CMD=1
    fi
    if [[ -r "$_TMPFS_MOUNTS" ]] && \
       [[ "$_TMPFS_EXPLICIT_HINT" == 1 || "$_TMPFS_CARGO_CMD" == 1 ]] && \
       tmpfs_scratch_guard_enabled; then
        # Explicit shapes 1-3. Skipped entirely when only the ambient gate let
        # us in (a bare `cargo build` carries none of their spellings, so this
        # awk could only ever return empty).
        _TMPFS_ASSIGNMENTS=""
        if [[ "$_TMPFS_EXPLICIT_HINT" == 1 ]]; then
            _TMPFS_ASSIGNMENTS="$(tmpfs_scratch_assignments "$COMMAND_ASK_SCAN")" || _TMPFS_ASSIGNMENTS=""
        fi

        _TMPFS_BASE="${CWD:-$REPO_ROOT}"
        [[ -n "$_TMPFS_BASE" ]] || _TMPFS_BASE="$PWD"

        # Shape 4: a `target-dir = <path>` line being written INTO a cargo
        # config file. The three substrings this used to gate on (TOML key,
        # cargo config filename, write idiom — each checked independently
        # ANYWHERE in the command) are uncorrelated and false-deny on ordinary
        # prose ABOUT this hazard, because nothing ties the write idiom to the
        # config file or either to the TOML key (#461 review):
        #   gh pr comment 461 --body "target-dir = /dev/shm/x lands in
        #     .cargo/config.toml; use > /dev/null to hide"
        #   echo "in .cargo/config.toml, target-dir = /dev/shm/x is a hazard" > notes.md
        # Neither writes into a cargo config — the first writes nothing at all
        # (its only `>` is inside the quoted --body value), the second writes
        # notes.md — yet both satisfied all three substrings.
        #
        # So gate on extract_write_targets()'s RESOLVED destination instead of
        # hoping the substrings imply each other: it already tokenizes every
        # `>`/`>>`/tee/sed -i/cp/mv target in the command, and its `>` scan is
        # independently quote-aware (mask_gt(), #4245) — a `>` inside a quoted
        # argument is never treated as a redirection operator, which is why
        # the --body example above naturally yields no write target at all.
        # This reads COMMAND_NO_COMMENT, NOT COMMAND_ASK_SCAN, on purpose: the
        # canonical spelling of this write is `echo 'target-dir = "…"' >>
        # .cargo/config.toml`, and strip_datasink_literals() redacts exactly
        # that quoted echo argument, which would hide the TOML value extracted
        # below (COMMAND_ASK_SCAN's own literal-text redaction plays no part
        # in extract_write_targets()'s quote-awareness, which is independent
        # of it).
        if [[ "$COMMAND_NO_COMMENT" == *"target-dir"* ]] && \
           printf '%s' "$COMMAND_NO_COMMENT" | grep -qE '(\.cargo/config(\.toml)?|config\.toml)'; then
            _TMPFS_CARGO_CONFIG_WRITE=""
            _TMPFS_WRITE_TARGETS="$(extract_write_targets "$COMMAND_NO_COMMENT" "$_TMPFS_BASE" | head -20)" || _TMPFS_WRITE_TARGETS=""
            while IFS=$'\037' read -r _wcwd _wtarget; do
                [[ -n "$_wtarget" ]] || continue
                # repo#600: an unknown cwd still ends in the target-s own
                # path, which is all the cargo-config suffix test reads.
                [[ "$_wcwd" == "$_LOOM_UNKNOWN_CWD" ]] && _wcwd="/__loom_unknown_cwd__"
                _wabs="$_wtarget"
                [[ "$_wabs" != /* ]] && _wabs="${_wcwd:-$_TMPFS_BASE}/$_wabs"
                [[ "$_wabs" == /* ]] || continue
                _wabs="$(normalize_abs_path "$_wabs")"
                case "$_wabs" in
                    */.cargo/config.toml|*/.cargo/config) _TMPFS_CARGO_CONFIG_WRITE=1; break ;;
                esac
            done <<< "$_TMPFS_WRITE_TARGETS"

            if [[ -n "$_TMPFS_CARGO_CONFIG_WRITE" ]]; then
                # The quote class is a RUN (`*`, and it includes a literal
                # backslash) rather than a single optional quote, because the
                # `echo "target-dir = \"…\"" >> …` spelling reaches this scan with
                # its inner quotes still backslash-escaped — matching only one
                # unescaped quote character would capture the backslash as the
                # whole path and silently classify nothing.
                _TMPFS_TOML_VALUES="$(printf '%s' "$COMMAND_NO_COMMENT" \
                    | grep -oE '(^|[^-[:alnum:]])target-dir[[:space:]]*=[[:space:]]*[\"'"'"']*[^\"'"'"'[:space:],]+' \
                    | sed -E 's/.*target-dir[[:space:]]*=[[:space:]]*[\"'"'"']*//')" || _TMPFS_TOML_VALUES=""
                while IFS= read -r _tmpfs_tv; do
                    [[ -n "$_tmpfs_tv" ]] || continue
                    _TMPFS_ASSIGNMENTS+=$'\n'"build.target-dir"$'\t'"$_tmpfs_tv"
                done <<< "$_TMPFS_TOML_VALUES"
            fi
        fi

        # Ambient shapes (repo#462): an exported CARGO_TARGET_DIR or a
        # pre-existing `[build] target-dir` config, on a command that assigns
        # nothing itself. Only consulted when a real cargo command word is
        # present AND the command names no cargo target dir of its own —
        # anything explicit SHADOWS the ambient value in cargo's own precedence
        # (`--target-dir` > CARGO_TARGET_DIR > config), so classifying the
        # ambient one too would deny on a path the build will never write to.
        # TMPDIR is not an override of the cargo target dir and so does not
        # shadow it (a `TMPDIR=… cargo build` still inherits the ambient one).
        if [[ "$_TMPFS_CARGO_CMD" == 1 ]]; then
            _TMPFS_EXPLICIT_TARGET=""
            while IFS=$'\t' read -r _tmpfs_en _tmpfs_ev; do
                [[ -n "$_tmpfs_ev" ]] || continue
                case "$_tmpfs_en" in
                    CARGO_TARGET_DIR|--target-dir|build.target-dir) _TMPFS_EXPLICIT_TARGET=1; break ;;
                esac
            done <<< "$_TMPFS_ASSIGNMENTS"
            if [[ -z "$_TMPFS_EXPLICIT_TARGET" ]]; then
                _TMPFS_AMBIENT="$(tmpfs_ambient_target_dir "$_TMPFS_BASE")" || _TMPFS_AMBIENT=""
                [[ -n "$_TMPFS_AMBIENT" ]] && _TMPFS_ASSIGNMENTS+=$'\n'"$_TMPFS_AMBIENT"
            fi
        fi

        # The acting cwd's own mount, for the "already in RAM" exemption above.
        _TMPFS_CWD_MP=""
        if [[ "$_TMPFS_BASE" == /* ]]; then
            _tmpfs_cwd_entry="$(_mount_entry_for_path "$(normalize_abs_path "$_TMPFS_BASE")" "$_TMPFS_MOUNTS")" || _tmpfs_cwd_entry=""
            [[ -n "$_tmpfs_cwd_entry" ]] && _TMPFS_CWD_MP="${_tmpfs_cwd_entry#*$'\t'}"
        fi

        # The third field is the AMBIENT origin (repo#462) — empty for every
        # explicit shape above, which keeps their two-field records readable
        # here unchanged.
        while IFS=$'\t' read -r _tmpfs_name _tmpfs_value _tmpfs_origin; do
            [[ -n "$_tmpfs_name" && -n "$_tmpfs_value" ]] || continue
            # An unexpanded shell variable / substitution is unknowable to a
            # static scan — same no-opinion rule as an unreadable mount table.
            case "$_tmpfs_value" in
                *'$'*|*'`'*) continue ;;
            esac
            _tmpfs_abs="$_tmpfs_value"
            [[ "$_tmpfs_abs" != /* ]] && _tmpfs_abs="$_TMPFS_BASE/$_tmpfs_abs"
            [[ "$_tmpfs_abs" == /* ]] || continue
            _tmpfs_abs="$(normalize_abs_path "$_tmpfs_abs")"
            _tmpfs_entry="$(_mount_entry_for_path "$_tmpfs_abs" "$_TMPFS_MOUNTS")" || _tmpfs_entry=""
            [[ -n "$_tmpfs_entry" ]] || continue
            _tmpfs_fs="${_tmpfs_entry%%$'\t'*}"
            _tmpfs_mp="${_tmpfs_entry#*$'\t'}"
            case "$_tmpfs_fs" in
                tmpfs|ramfs) ;;
                *) continue ;;
            esac
            # Already-in-RAM exemption: nothing is being redirected into RAM,
            # and there would be no on-disk alternative to recommend.
            [[ -n "$_TMPFS_CWD_MP" && "$_TMPFS_CWD_MP" == "$_tmpfs_mp" ]] && continue
            # The suggested fix differs by assignment: CARGO_TARGET_DIR / --target-dir
            # are cargo-specific, so the repo's on-disk `target/` is the natural
            # default; TMPDIR is a generic scratch root with no such default, so
            # naming `<repo>/target` there would be inapplicable advice (#461 review).
            _tmpfs_suggest="${REPO_ROOT:-$_TMPFS_BASE}/target"
            if [[ -n "$_tmpfs_origin" ]]; then
                # AMBIENT (repo#462): there is no assignment in this command to
                # drop, so "drop the assignment" would be unfollowable advice —
                # name the thing that actually has to change instead.
                if [[ "$_tmpfs_name" == "CARGO_TARGET_DIR" ]]; then
                    _tmpfs_advice="unset CARGO_TARGET_DIR (or re-export it at a disk-backed path) so the build lands on disk — cargo's own default is $_tmpfs_suggest"
                else
                    _tmpfs_advice="edit that file's [build] target-dir to a disk-backed path, or delete the key to fall back to cargo's default $_tmpfs_suggest"
                fi
                _tmpfs_where=" ($_tmpfs_origin)"
            elif [[ "$_tmpfs_name" == "TMPDIR" ]]; then
                _tmpfs_advice="point it at a disk-backed scratch path instead (e.g. ${REPO_ROOT:-$_TMPFS_BASE}/.tmp)"
                _tmpfs_where=""
            else
                _tmpfs_advice="drop the assignment to build into the default $_tmpfs_suggest, or point it at another disk-backed path (e.g. an on-disk [build] target-dir in .cargo/config.toml)"
                _tmpfs_where=""
            fi
            deny "BLOCKED: $_tmpfs_name=$_tmpfs_value$_tmpfs_where resolves to $_tmpfs_abs, which is on a RAM-backed $_tmpfs_fs mount ($_tmpfs_mp). Build/scratch output written there consumes the host's memory for as long as it exists, and nothing deletes it when the build ends (rjwalters/loom#8512: a 6.2 GB target dir left in /dev/shm pinned RAM for 2.5 days and drove a worker into an OOM-kill storm). Use an on-disk location instead: $_tmpfs_advice. Set guards.tmpfsScratch:false in .claude/skills/repo/config.json if this host deliberately builds in RAM." "tmpfs-scratch-dir:$_tmpfs_name"
        done <<< "$_TMPFS_ASSIGNMENTS"
    fi
fi

# =============================================================================
# REQUIRE CONFIRMATION - Potentially dangerous but sometimes legitimate
# =============================================================================

ASK_PATTERNS=(
    # NOTE: the force-op patterns (git push --force / -f / --force-with-lease and
    # git reset --hard) are NOT in this ungated array. They are handled by the
    # branch-aware FORCE-OP BRANCH SCOPE block above, gated by
    # force_scope_mode() (guards.forceScope / LOOM_FORCE_SCOPE, #3674), so an
    # autonomous agent can force-push / hard-reset its own working branch without
    # a stall while protected-branch force ops still ask. git clean / checkout .
    # / restore . stay here — they are not force ops and have no branch scope.
    #
    # COMMAND-POSITION ANCHORING (#3756): every entry is prefixed with
    # `(^|[;&|[:space:]])`, mirroring ALWAYS_BLOCK_PATTERNS's `gh repo delete`
    # anchor (#3553), so the phrase only fires at start-of-command or after a
    # shell separator — an ask-phrase that merely appears inside another
    # command's quoted argument (e.g. `jq -n '{cmd:"gh issue close 123"}'`, the
    # phrase preceded by `"`) no longer false-asks. Entries whose command is a
    # multi-word phrase (`kubectl rollout restart`, `git checkout \.`) are
    # anchored at the FIRST token only — the phrase's leading command word — per
    # the `gh repo delete` precedent. (Like the catastrophic tier, this anchor
    # cannot distinguish a real separator from a whitespace INSIDE a quoted
    # string, so a mid-quote prose mention such as `echo "… gh pr close …"` still
    # matches on its leading space — an accepted limitation shared with the
    # ALWAYS_BLOCK tier; command-word segment classification is #3757's scope.)
    '(^|[;&|[:space:]])git clean -fd'
    '(^|[;&|[:space:]])git checkout \.'
    '(^|[;&|[:space:]])git restore \.'

    # GitHub operations that are genuinely hard to reverse. `gh release delete`
    # removes published artifacts/tags — it STAYS an ungated ask. The reversible
    # GitHub state changes (`gh pr close`, `gh issue close`, `gh label delete`)
    # were REMOVED from this array (#3757): they are trivially undone (gh pr
    # reopen / gh issue reopen / recreate the label) and are only asked for when
    # a repo opts IN via guards.reversibleGh (REVERSIBLE_GH_ASK_PATTERNS below).
    '(^|[;&|[:space:]])gh release delete'

    # NOTE: cloud CLI (aws) + docker ASK patterns are NOT in this ungated array.
    # They live in CLOUD_ASK_PATTERNS below, gated by cloud_guard_enabled() so
    # cloud-dev repos can opt down (LOOM_GUARD_CLOUD=0 / guards.cloudCli:false).

    # NOTE: `systemctl restart|stop|disable` is NOT a plain substring entry here.
    # It is handled by the segment-parsed, command-word-anchored
    # systemctl_ask_reason() check after this loop (loom#5214, repo#585).

    # Kubernetes operations
    '(^|[;&|[:space:]])kubectl delete'
    '(^|[;&|[:space:]])kubectl rollout restart'
    '(^|[;&|[:space:]])kubectl drain'

    # SkyPilot infrastructure
    '(^|[;&|[:space:]])sky down'
    '(^|[;&|[:space:]])sky stop'

    # Credential exposure. `printenv ... SECRET|TOKEN|KEY` and `cat .../.ssh/<f>`
    # are NOT plain substring entries any more: they are the segment-parsed
    # printenv_ask_reason() / ssh_cat_ask_reason() checks after this loop
    # (loom#6245 / loom#5824, repo#585). `.aws/credentials` has no safe
    # sibling file to allowlist, so it stays a substring entry.
    '(^|[;&|[:space:]])cat.*/\.aws/credentials'
)

for pattern in "${ASK_PATTERNS[@]}"; do
    if echo "$COMMAND_ASK_SCAN" | grep -qE "$pattern"; then
        ask "Command requires confirmation: $COMMAND" "ask:$pattern"
    fi
done

# =============================================================================
# COMMAND-WORD RESOLUTION through launcher wrappers (repo#585 review fix)
#
# Shared by systemctl_ask_reason(), ssh_cat_ask_reason() and
# printenv_ask_reason() below. Those parsers originally stripped only a bare
# leading `sudo ` and a narrow `env` form before treating the next token as
# the command word, so `sudo -u root systemctl restart x` or `sudo --
# printenv TOKEN` resolved the command word to `-u` / `--` and silently
# ALLOWED — a regression versus the substring patterns they replaced, which
# found the invocation after any wrapper options.
#
# cw_resolve(toks, m) walks toks[1..m] (one qsplit() segment, whitespace
# split) past leading NAME=value assignments, shell grammar openers, and the
# launcher wrappers sudo / doas / env / command / exec / nohup / nice / time /
# timeout / stdbuf / ionice / setsid / xargs — each with its OWN option
# grammar (flag clusters such as `-nu root`, attached `-uroot`, `--user=root`,
# `--user root`, and the `--` terminator) — and returns:
#     >0  index of the resolved command word
#      0  no command word in the segment
#     -1  AMBIGUOUS: a wrapper option this grammar does not model (`sudo -h`,
#         `env -S "..."`, `ionice -p`, an unknown flag, ...), an unmodelled
#         launcher (eval, find -exec, watch, flock, chroot, nsenter, busybox,
#         ssh, sh/bash -c, ... — cw_ambiguous_launcher), or a token before
#         the command word carrying a quote/backslash/`$` (a quoted value
#         with a space, e.g. `sudo -p "x y"`, mis-splits — cw_tok_unsafe)
# Callers FAIL CLOSED on -1 by checking EVERY token position of the segment
# as a candidate command word, so an unmodelled wrapper form can only ever
# add asks, never hide one.
# =============================================================================
_CMDWORD_AWK='
function cw_unq(t) { gsub(/[\047\042]/, "", t); return t }
function cw_base(w,   k) {
    k = index(w, "/")
    while (k > 0) { w = substr(w, k + 1); k = index(w, "/") }
    return w
}
# Skip the options of a wrapper starting at toks[j]. sflag/sarg: short flag
# letters without / with a required argument. lflag/larg: space-delimited
# (" --a --b ") long options without / with a required argument. Returns the
# index of the first non-option token, or -1 on an unmodelled option.
function cw_skip_opts(toks, m, j, sflag, sarg, lflag, larg,   t, k, c, n, name, eq) {
    while (j <= m) {
        t = cw_unq(toks[j])
        if (t == "--") return j + 1
        if (t == "-") { j++; continue }
        if (substr(t, 1, 1) != "-") return j
        if (substr(t, 1, 2) == "--") {
            eq = index(t, "=")
            name = (eq > 0) ? substr(t, 1, eq - 1) : t
            if (eq > 0 && (index(larg, " " name " ") || index(lflag, " " name " "))) { j++; continue }
            if (index(lflag, " " name " ")) { j++; continue }
            if (index(larg, " " name " ")) { j += 2; continue }
            return -1
        }
        n = length(t)
        for (k = 2; k <= n; k++) {
            c = substr(t, k, 1)
            if (index(sflag, c)) continue
            if (index(sarg, c)) {
                # Argument is the rest of this token, or the next token.
                if (k == n) j++
                break
            }
            return -1
        }
        j++
    }
    return j
}
# Launchers whose own argument grammar this resolver does NOT model — they
# run a command found somewhere later in their arguments (eval, find -exec,
# watch, flock, chroot, nsenter, busybox, ssh <host> <cmd>, sh/bash -c, ...).
# Reaching one of these makes the segment AMBIGUOUS (-1), so callers check
# every token: the replaced substring checks asked on all of these shapes,
# and failing closed keeps that (repo#585 review).
function cw_ambiguous_launcher(b) {
    return (b ~ /^(eval|find|watch|flock|chroot|nsenter|busybox|ssh|mosh|sh|bash|zsh|dash|ksh|mksh|fish|csh|tcsh|su|runuser|pkexec|script|strace|ltrace|unshare|taskset|chrt|numactl|prlimit|systemd-run|setpriv|sg|fakeroot|firejail|parallel|unbuffer|caffeinate|gtimeout|gnice|builtin|coproc|docker|podman|kubectl|lxc|vagrant|gdb|valgrind|perf|catchsegv|arch|torsocks|proxychains|proxychains4|dbus-run-session|xvfb-run|nix-shell|direnv|tmux|screen|expect)$/)
}
# The resolver splits on whitespace, so a quoted/escaped value with a space
# (`sudo -p "x y" ...`, `env -u "A B" ...`, `FOO="a b" ...`, `-p x\ y`) is
# broken across tokens and the walk lands on a fragment. Any token BEFORE the
# resolved command word that carries a quote, backslash or `$` therefore makes
# the segment ambiguous (-1) instead of trusting the split.
function cw_tok_unsafe(t) { return (t ~ /[\047\042\\$]/) }
function cw_resolve(toks, m,   j, k) {
    j = cw_resolve_raw(toks, m)
    if (j <= 0) return j
    for (k = 1; k < j; k++)
        if (cw_tok_unsafe(toks[k])) return -1
    return j
}
function cw_resolve_raw(toks, m,   j, t, b, nj) {
    j = 1
    while (j <= m) {
        t = toks[j]
        if (t == "") { j++; continue }
        if (t ~ /^[A-Za-z_][A-Za-z0-9_]*=/) { j++; continue }
        b = cw_base(cw_unq(t))
        if (b == "!" || b == "{" || b == "(" || b == "if" || b == "then" ||
            b == "else" || b == "elif" || b == "while" || b == "until" || b == "do") {
            j++; continue
        }
        if (cw_ambiguous_launcher(b)) return -1
        if (b == "sudo") {
            nj = cw_skip_opts(toks, m, j + 1, "ABbEeHiKklNnPSsVv", "aCcDgpRrTtUu",
                " --askpass --background --bell --edit --set-home --login --remove-timestamp --reset-timestamp --list --no-update --non-interactive --preserve-groups --stdin --shell --version --validate --preserve-env --help ",
                " --auth-type --close-from --login-class --chdir --group --host --prompt --chroot --role --type --command-timeout --other-user --user ")
        } else if (b == "doas") {
            nj = cw_skip_opts(toks, m, j + 1, "nsL", "uC", "", "")
        } else if (b == "env") {
            nj = cw_skip_opts(toks, m, j + 1, "0iv", "uCP",
                " --ignore-environment --null --debug --default-signal --ignore-signal --block-signal --list-signal-handling ",
                " --unset --chdir ")
        } else if (b == "command") {
            nj = cw_skip_opts(toks, m, j + 1, "pvV", "", "", "")
        } else if (b == "exec") {
            nj = cw_skip_opts(toks, m, j + 1, "cl", "a", "", "")
        } else if (b == "nohup") {
            nj = cw_skip_opts(toks, m, j + 1, "", "", "", "")
        } else if (b == "nice") {
            nj = cw_skip_opts(toks, m, j + 1, "0123456789", "n", "", " --adjustment ")
        } else if (b == "time") {
            nj = cw_skip_opts(toks, m, j + 1, "apqvV", "fo",
                " --append --portability --quiet --verbose ", " --format --output ")
        } else if (b == "timeout") {
            nj = cw_skip_opts(toks, m, j + 1, "v", "ks",
                " --preserve-status --foreground --verbose ", " --kill-after --signal ")
            if (nj > 0 && nj <= m) nj++    # the DURATION operand
        } else if (b == "stdbuf") {
            nj = cw_skip_opts(toks, m, j + 1, "", "eio", "", " --input --output --error ")
        } else if (b == "ionice") {
            nj = cw_skip_opts(toks, m, j + 1, "t", "cn", " --ignore ", " --class --classdata ")
        } else if (b == "setsid") {
            nj = cw_skip_opts(toks, m, j + 1, "cfw", "", " --ctty --fork --wait ", "")
        } else if (b == "xargs") {
            nj = cw_skip_opts(toks, m, j + 1, "0prtx", "adEILnPs",
                " --null --interactive --no-run-if-empty --verbose --exit ",
                " --arg-file --delimiter --max-lines --max-args --max-procs --max-chars --process-slot-var ")
        } else {
            return j
        }
        if (nj < 0) return -1
        j = nj
    }
    return 0
}
'

# Ported from rjwalters/loom guard-destructive-generic.sh at 2072f82b
# (rjwalters/repo#585, part of #579): the three checks below replace the plain
# substring ASK_PATTERNS entries that false-asked on quoted search text.
# =============================================================================
# SERVICE-MANAGEMENT ASK — systemctl restart/stop/disable, segment-parsed,
# command-word anchored (#5214)
#
# These three verbs used to live in ASK_PATTERNS above as plain substring
# patterns anchored only by '(^|[;&|[:space:]])' (#3756) — a boundary that
# cannot distinguish a real shell separator from a whitespace character sitting
# INSIDE a quoted string literal. So a phrase like `systemctl restart` merely
# being quoted as SEARCH TEXT (a grep pattern, a jq filter, prose) still matched
# on its leading space, even though no such command was ever invoked:
#   grep -n "idle\|systemctl restart\|systemd\|relaunch\|--idle-shutdown" f.sh
#   jq -c 'select(.pattern | contains("systemctl"))' guard-decisions.log
#
# Mirrors lifecycle_or_cloud_reason()'s fix for the analogous halt/reboot/
# az-delete false positive: segment-parse the command with qsplit() (quote-aware,
# #3755) instead of scanning raw substrings, resolve the command word through
# sudo/env/... wrappers and their options (cw_resolve, _CMDWORD_AWK; fails
# closed on unmodelled forms), and ask ONLY when a segment's actual command word is `systemctl`
# AND its very next token is restart/stop/disable. A quoted `|` inside
# `grep`/`jq` arguments (no `$(`/backtick) is inert to qsplit(), so both example
# commands above stay a single `grep`/`jq` segment — command word never
# resolves to `systemctl` — and no longer false-ask. A genuine invocation
# (bare, after `;`/`&&`/`|`, or with a later quoted argument such as
# `systemctl restart "my service"`) still asks, since toks[1]/toks[2] are
# unaffected by trailing quoted content.
#
# Scoped narrowly to this one "Service management" ASK_PATTERNS block per
# #5214 — #5157/#5158 describe the same false-positive CLASS for other
# patterns but were judged too broad a fix to land autonomously; this is not
# an attempt at a general-purpose fix for the whole ASK_PATTERNS family.
# =============================================================================
systemctl_ask_reason() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK$_CMDWORD_AWK"'
    # Is toks[c] a `systemctl` whose verb is restart/stop/disable? Leading
    # systemctl options are skipped (value-taking ones consume their operand)
    # so `systemctl --user restart x` / `systemctl -H h stop x` still match.
    function sc_check(toks, m, c,   j, t) {
        if (cw_base(cw_unq(toks[c])) != "systemctl") return ""
        j = c + 1
        while (j <= m) {
            t = cw_unq(toks[j])
            if (t == "--") { j++; break }
            if (t ~ /^-/) {
                if (t ~ /^(-[HMtpnosP]|--(host|machine|type|property|lines|output|signal|kill-whom|root|state|job-mode|what|kill-value|image|preset-mode|message|timestamp|check-inhibitors|drop-in|when))$/) {
                    # Fail closed if the consumed operand is itself a
                    # mutating verb (an option misjudged as value-taking).
                    if (j + 1 <= m) {
                        t = cw_unq(toks[j + 1])
                        if (t == "restart" || t == "stop" || t == "disable") return "systemctl " t
                    }
                    j += 2
                }
                else j++
                continue
            }
            break
        }
        if (j > m) return ""
        t = cw_unq(toks[j])
        if (t == "restart" || t == "stop" || t == "disable") return "systemctl " t
        return ""
    }
    {
        $0 = qsplit($0)   # quote-aware segmentation (#3755)
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m < 2) continue
            # Resolve the command word through sudo/env/... wrappers and
            # their options (cw_resolve, see _CMDWORD_AWK). On an ambiguous
            # wrapper form, FAIL CLOSED: every token is a candidate.
            c = cw_resolve(toks, m)
            if (c > 0) {
                r = sc_check(toks, m, c)
                if (r != "") print r
            } else if (c < 0) {
                for (c = 1; c <= m; c++) {
                    r = sc_check(toks, m, c)
                    if (r != "") { print r; break }
                }
            }
        }
    }'
}
_SYSTEMCTL_ASK=$(systemctl_ask_reason "$COMMAND_NO_COMMENT" | head -1)
if [[ -n "$_SYSTEMCTL_ASK" ]]; then
    ask "Command requires confirmation: $COMMAND" "ask:$_SYSTEMCTL_ASK"
fi

# =============================================================================
# SSH-DIRECTORY READ ASK — cat under .ssh/, basename-allowlisted (#5824)
#
# The plain-substring ASK_PATTERNS entry this replaced —
# '(^|[;&|[:space:]])cat.*/\.ssh/' — matched the whole `.ssh/` directory, so
# reading a routine, non-secret file (`config`, `known_hosts`,
# `known_hosts.old`, `authorized_keys` — at most host aliases / key
# fingerprints, never key material) asked identically to reading an actual
# private key. `grep -E` substring matching cannot capture the matched
# operand to inspect its basename, so — mirroring systemctl_ask_reason()
# above — this segment-parses the command with qsplit() (quote-aware,
# #3755), resolves the command word through sudo/env/... wrappers and their
# options (cw_resolve, _CMDWORD_AWK; fails closed on unmodelled forms), and
# only inspects segments whose command word is `cat`.
#
# ALLOWLIST, NOT DENYLIST (deliberate, per the issue's acceptance criteria):
# a `cat` operand under `.ssh/` still asks unless its basename is one of the
# four known-safe filenames below. Any unrecognized/unlisted filename —
# including a bare `.ssh/` with no filename at all — falls through to the
# safer default (ask), so a new key-naming convention or an unforeseen file
# is never silently allowed. Private key material (`id_rsa`, `id_ed25519`,
# anything else) always misses the allowlist and keeps asking.
# =============================================================================
ssh_cat_ask_reason() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK$_CMDWORD_AWK"'
    # Is toks[c] a `cat` reading a non-allowlisted file under .ssh/? Returns
    # the ask reason, or "".
    function ssh_check(toks, m, c,   j, tok, rest, base) {
        if (cw_base(cw_unq(toks[c])) != "cat") return ""
        for (j = c + 1; j <= m; j++) {
            tok = toks[j]
            if (tok !~ /\/\.ssh\//) continue
            # Operand after the LAST /.ssh/ in this token (greedy .*
            # backtracks to the rightmost occurrence).
            if (!match(tok, /.*\/\.ssh\//)) continue
            rest = substr(tok, RLENGTH + 1)
            # basename: strip any further path components after /.ssh/
            if (match(rest, /.*\//)) {
                base = substr(rest, RLENGTH + 1)
            } else {
                base = rest
            }
            # Strip stray quote characters a quoted operand (copied
            # verbatim by qsplit) may leave attached to the basename.
            gsub(/[\047\042]/, "", base)
            if (base != "config" && base != "known_hosts" && base != "known_hosts.old" && base != "authorized_keys") {
                return "cat .ssh/" base
            }
        }
        return ""
    }
    {
        $0 = qsplit($0)   # quote-aware segmentation (#3755)
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m < 2) continue
            # Resolve the command word through sudo/env/... wrappers and
            # their options (cw_resolve, see _CMDWORD_AWK). On an ambiguous
            # wrapper form, FAIL CLOSED: every token is a candidate.
            c = cw_resolve(toks, m)
            if (c > 0) {
                r = ssh_check(toks, m, c)
                if (r != "") { print r; exit }
            } else if (c < 0) {
                for (c = 1; c <= m; c++) {
                    r = ssh_check(toks, m, c)
                    if (r != "") { print r; exit }
                }
            }
        }
    }'
}
_SSH_CAT_ASK=$(ssh_cat_ask_reason "$COMMAND_ASK_SCAN" | head -1)
if [[ -n "$_SSH_CAT_ASK" ]]; then
    ask "Command requires confirmation: $COMMAND" "ask:$_SSH_CAT_ASK"
fi

# =============================================================================
# PRINTENV CREDENTIAL-NAME ASK — segment-parsed, name-allowlisted (#6245)
#
# The plain-substring ASK_PATTERNS entries this replaced — three separate
# '(^|[;&|[:space:]])printenv.*SECRET' / '...TOKEN' / '...KEY' patterns —
# matched ANY printenv invocation whose command text contained one of those
# three substrings anywhere after "printenv", with no way to distinguish a
# genuinely secret-bearing read (`printenv GITHUB_TOKEN`) from a non-secret
# pointer/identity variable that merely has one of those words in its name
# (`printenv LOOM_TOKEN_NAME` — an account-label string identifying which
# OAuth token slot is active, not a credential value; see
# docs/token-pool.md — spawn-claude.sh already logs it in plaintext).
#
# DENYLIST substring check, ALLOWLIST override (deliberate): mirroring
# systemctl_ask_reason()/ssh_cat_ask_reason() above, this segment-parses the
# command with qsplit() (quote-aware, #3755), resolves the command word
# through sudo/env/... wrappers and their options (cw_resolve, _CMDWORD_AWK;
# fails closed on unmodelled forms), and only inspects segments whose command
# word is `printenv`. Each remaining operand (the variable name being
# read) still asks if its name contains SECRET/TOKEN/KEY as a substring —
# the same narrowing the old patterns used — UNLESS the operand is an
# EXACT match for a documented non-secret var (LOOM_TOKEN_NAME,
# LOOM_TOKEN_MODE). The allowlist match is exact-string, not substring, so
# a lookalike name that merely CONTAINS an allowlisted name (e.g.
# LOOM_TOKEN_NAME_BACKUP) still asks — guards against a suffix/prefix-match
# bypass. Any unrecognized/unlisted credential-shaped name falls through to
# the safer default (ask), so a new var-naming convention is never silently
# allowed.
# =============================================================================
printenv_ask_reason() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK$_CMDWORD_AWK"'
    # Is toks[c] a `printenv` reading a credential-shaped, non-allowlisted
    # name? Returns the ask reason, or "".
    function pe_check(toks, m, c,   j, var) {
        if (cw_base(cw_unq(toks[c])) != "printenv") return ""
        for (j = c + 1; j <= m; j++) {
            var = toks[j]
            gsub(/[\047\042]/, "", var)
            if (var ~ /^-/) continue
            if (var !~ /SECRET|TOKEN|KEY/) continue
            if (var == "LOOM_TOKEN_NAME" || var == "LOOM_TOKEN_MODE") continue
            return "printenv " var
        }
        return ""
    }
    {
        $0 = qsplit($0)   # quote-aware segmentation (#3755)
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m < 2) continue
            # Resolve the command word through sudo/env/... wrappers and
            # their options (cw_resolve, see _CMDWORD_AWK). On an ambiguous
            # wrapper form, FAIL CLOSED: every token is a candidate.
            c = cw_resolve(toks, m)
            if (c > 0) {
                r = pe_check(toks, m, c)
                if (r != "") { print r; exit }
            } else if (c < 0) {
                for (c = 1; c <= m; c++) {
                    r = pe_check(toks, m, c)
                    if (r != "") { print r; exit }
                }
            }
        }
    }'
}
_PRINTENV_ASK=$(printenv_ask_reason "$COMMAND_ASK_SCAN" | head -1)
if [[ -n "$_PRINTENV_ASK" ]]; then
    ask "Command requires confirmation: $COMMAND" "ask:$_PRINTENV_ASK"
fi


# =============================================================================
# CARGO CLEAN SCOPE DENY — an unscoped `cargo clean` that would clear a
# config-derived build.target-dir SHARED outside this repo (rjwalters/repo#583,
# ported from rjwalters/loom guard-destructive-generic.sh at 2072f82b, #6684 /
# #7795). Gated by cargo_clean_guard_enabled() (guards.cargoCleanScope;
# REPO_GUARD_CARGO_CLEAN wins over legacy LOOM_GUARD_CARGO_CLEAN, then config,
# then default ON).
#
# `cargo clean -p <pkg>` and any repo-local/default target dir are untouched.
# An EXPLICIT target dir — command-local or inherited CARGO_TARGET_DIR, or
# `--target-dir PATH` / `--target-dir=PATH` — is a deliberate scoping decision
# and is never refused here (it remains subject to the independent tmpfs-scratch
# guard above, which this block neither replaces nor bypasses). Only the
# CONFIG-derived resolution (`cargo config get`, then a manual ancestor
# `.cargo/config{,.toml}` walk-up, then $CARGO_HOME) is compared to the repo.
#
# Differences from the Loom source: (1) the `--target-dir` CLI forms are
# honored (Loom's resolver ignores them); (2) every unscoped clean segment is
# examined, not just the first, so a safe earlier clean cannot hide a later
# shared one; (3) the toggle uses the canonical guard_toggle_enabled()
# resolver (REPO_* > LOOM_* > repo config > default) instead of Loom's
# LOOM_-only read; (4) the walk-up / Cargo-home helpers already emit
# "<path>\t<config file>" here (for the tmpfs guard), so
# cargo_clean_effective_target_dir() splits that record rather than taking a
# bare path; (5) Cargo global options and `+toolchain` selectors before
# `clean` are skipped (`cargo --quiet clean`, `cargo +stable clean`), and a
# `--config build.target-dir=PATH` override counts as an explicit target;
# (6) containment is decided on the physically resolved target and repo root,
# so a repo-local symlink pointing at a shared external dir is still refused.
# =============================================================================
_CARGO_CLEAN_GUARD_CACHE=""
cargo_clean_guard_enabled() {
    guard_toggle_enabled _CARGO_CLEAN_GUARD_CACHE cargoCleanScope true \
        LOOM_GUARD_CARGO_CLEAN REPO_GUARD_CARGO_CLEAN
}

# Prints one line per UNSCOPED `cargo clean` segment: "x<same-command
# CARGO_TARGET_DIR>\tx<--target-dir value>" (the "x" sentinels keep empty
# fields from collapsing under tab IFS splitting). Prints nothing when there
# is no candidate.
cargo_clean_scope_match() {
    printf '%s' "$1" | awk "$_ESCAPE_AWK$_HASLIVESUBST_AWK$_QSPLIT_AWK"'
    function unq(v) { gsub(/[\047\042]/, "", v); return v }
    {
        orig = $0
        # qsplit() re-emits only commands after a separator INSIDE a
        # substitution; subst_heads() adds the first command of each
        # `$( ... )`/backtick span, which the shell also executes.
        $0 = qsplit(orig) "\n" subst_heads(orig, "\n")
        n = split($0, segs, "\n")
        for (i = 1; i <= n; i++) {
            seg = segs[i]
            sub(/^[ \t]+/, "", seg)
            m = split(seg, toks, /[ \t]+/)
            if (m == 0) continue
            j = 1
            envval = ""
            while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                if (toks[j] ~ /^CARGO_TARGET_DIR=/)
                    envval = unq(substr(toks[j], index(toks[j], "=") + 1))
                j++
            }
            if (j <= m && toks[j] == "sudo") j++
            if (j <= m && toks[j] == "env") {
                j++
                while (j <= m && toks[j] ~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                    if (toks[j] ~ /^CARGO_TARGET_DIR=/)
                        envval = unq(substr(toks[j], index(toks[j], "=") + 1))
                    j++
                }
            }
            if (j > m || toks[j] != "cargo") continue
            j++
            # Cargo accepts a rustup toolchain selector and GLOBAL options
            # before the subcommand (`cargo +stable clean`, `cargo --quiet
            # clean`, `cargo --color never -v clean`); skip them so they cannot
            # hide the subcommand. Value-taking globals consume their argument
            # unless given in `--opt=value` / attached `-Zflag` form. A
            # `--config build.target-dir=PATH` override is an explicit,
            # command-local target choice, so it is treated like --target-dir.
            cli = ""
            if (j <= m && toks[j] ~ /^\+/) j++
            while (j <= m && toks[j] ~ /^-/ && toks[j] != "--") {
                opt = toks[j]
                val = ""
                if (opt == "--config" || opt == "--color" || opt == "--explain" || opt == "-Z" || opt == "-C") {
                    if (j < m) val = toks[j + 1]
                    j += 2
                } else {
                    if (opt ~ /^--config=/) val = substr(opt, index(opt, "=") + 1)
                    j++
                }
                if ((opt == "--config" || opt ~ /^--config=/) && unq(val) ~ /^build\.target-dir=/) {
                    val = unq(val)
                    cli = substr(val, index(val, "=") + 1)
                }
            }
            if (j > m || toks[j] != "clean") continue
            j++
            scoped = 0
            for (k = j; k <= m; k++) {
                if (toks[k] == "-p" || toks[k] == "--package" || toks[k] ~ /^--package=/) scoped = 1
                else if (toks[k] == "--target-dir" && k < m) cli = unq(toks[k + 1])
                else if (toks[k] ~ /^--target-dir=/) cli = unq(substr(toks[k], index(toks[k], "=") + 1))
            }
            if (scoped) continue
            printf "x%s\tx%s\n", envval, cli
        }
    }'
}

# Resolves the effective target dir for one candidate clean. Prints three
# lines: SOURCE (env|cli|config|default), the absolute path, and the config
# file the path came from (empty unless SOURCE=config). Precedence: --target-dir
# CLI flag, command-local CARGO_TARGET_DIR, inherited CARGO_TARGET_DIR, cargo's
# own config query, ancestor config, Cargo-home config, default <repo>/target.
cargo_clean_effective_target_dir() {
    local repo_root="$1" cwd="$2" same_cmd_env="$3" cli_dir="${4:-}"
    local base_cwd="${cwd:-$repo_root}"
    local resolved="" source="" cfgfile="" hit=""
    if [[ -n "$cli_dir" ]]; then
        resolved="$cli_dir"; source="cli"
    elif [[ -n "$same_cmd_env" ]]; then
        resolved="$same_cmd_env"; source="env"
    elif [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
        resolved="$CARGO_TARGET_DIR"; source="env"
    else
        source="config"
        if command -v cargo >/dev/null 2>&1; then
            local cg
            cg=$(cd "$base_cwd" 2>/dev/null && cargo config get build.target-dir 2>/dev/null) || cg=""
            if [[ -n "$cg" ]]; then
                resolved=$(printf '%s' "$cg" | sed -e 's/^build\.target-dir[[:space:]]*=[[:space:]]*//' -e 's/^"//' -e 's/"$//')
                cfgfile="cargo config get build.target-dir"
            fi
        fi
        if [[ -z "$resolved" ]]; then
            # Adapter: both helpers print "<path>\t<config file>".
            hit=$(_cargo_config_walk_up_target_dir "$base_cwd") || hit=""
            [[ -n "$hit" ]] || { hit=$(_cargo_home_config_target_dir) || hit=""; }
            if [[ -n "$hit" ]]; then
                resolved="${hit%%$'\t'*}"
                cfgfile="${hit#*$'\t'}"
            fi
        fi
        if [[ -z "$resolved" ]]; then
            resolved="${repo_root}/target"
            source="default"
        fi
    fi
    if [[ -n "$resolved" && "$resolved" != /* ]]; then
        resolved="$base_cwd/$resolved"
    fi
    [[ "$resolved" = /* ]] && resolved=$(normalize_abs_path "$resolved")
    printf '%s\n%s\n%s\n' "$source" "$resolved" "$cfgfile"
}

# Prefilter allows any run of option-like words (`+stable`, `--quiet`,
# `--color never`) between `cargo` and `clean`; the awk parser above makes the
# exact decision.
if [[ "$COMMAND_ASK_SCAN" == *cargo* && "$COMMAND_ASK_SCAN" == *clean* ]] && \
   printf '%s' "$COMMAND_ASK_SCAN" | grep -qE '(^|[;&|(`[:space:]])cargo([[:space:]]+[^[:space:];&|]+)*[[:space:]]+clean'; then
    _CARGO_CLEAN_MATCH=$(cargo_clean_scope_match "$COMMAND_ASK_SCAN")
    if [[ -n "$_CARGO_CLEAN_MATCH" ]] && [[ -n "$REPO_ROOT" ]] && cargo_clean_guard_enabled; then
        while IFS=$'\t' read -r _cc_env _cc_cli; do
            [[ -n "$_cc_env$_cc_cli" ]] || continue
            _cc_env="${_cc_env#x}"; _cc_cli="${_cc_cli#x}"
            _CARGO_TD_INFO=$(cargo_clean_effective_target_dir "$REPO_ROOT" "$CWD" "$_cc_env" "$_cc_cli")
            _CARGO_TD_SOURCE=$(printf '%s\n' "$_CARGO_TD_INFO" | sed -n '1p')
            _CARGO_TD_PATH=$(printf '%s\n' "$_CARGO_TD_INFO" | sed -n '2p')
            _CARGO_TD_FILE=$(printf '%s\n' "$_CARGO_TD_INFO" | sed -n '3p')
            [[ "$_CARGO_TD_SOURCE" == "config" && -n "$_CARGO_TD_PATH" ]] || continue
            # Containment is decided on the PHYSICAL (symlink-resolved) target
            # and root only. A lexical pre-check would wrongly allow a
            # repo-local symlink (`target-dir = "target-link"` -> external
            # shared dir), while the physical comparison still allows the
            # reverse case: an external spelling that resolves into the repo.
            _CARGO_TD_PATH_PHYS=$(physical_abs_path "$_CARGO_TD_PATH")
            _CARGO_REPO_ROOT_PHYS=$(physical_abs_path "$REPO_ROOT")
            [[ "$_CARGO_TD_PATH_PHYS" != "$_CARGO_REPO_ROOT_PHYS" && \
               "$_CARGO_TD_PATH_PHYS" != "$_CARGO_REPO_ROOT_PHYS"/* ]] || continue
            deny "Blocked: $COMMAND (cargo's target-dir is shared at '$_CARGO_TD_PATH'${_CARGO_TD_FILE:+ (from $_CARGO_TD_FILE)} — OUTSIDE this repo — so this clears the build output of every project on this host, including whatever sweep is compiling right now. Nothing has been deleted: just rerun with a scoped form. Package-scoped: 'cargo clean -p <pkg>'. Repo-scoped: 'cargo clean --target-dir <repo>/target' or prefix with CARGO_TARGET_DIR=<repo>/target. To opt out repo-wide set guards.cargoCleanScope:false in the repo config, or export REPO_GUARD_CARGO_CLEAN=0 in the agent's OWN environment before the session — an inline 'REPO_GUARD_CARGO_CLEAN=0 cargo clean' prefix does not reach this hook, which runs as a separate process)" "cargo-clean-scope-outside-repo"  # scan-reads: COMMAND_ASK_SCAN
        done <<< "$_CARGO_CLEAN_MATCH"
    fi
fi
# =============================================================================
# REVERSIBLE-GITHUB ASK patterns — gated by the reversible-gh guard toggle (#3757)
#
# Kept OUT of the ungated ASK_PATTERNS array (mirroring the CLOUD_ASK_PATTERNS
# split) because these GitHub state changes are trivially reversible and should
# NOT prompt by default — an autonomous agent closing its own issue/PR as part of
# a normal lifecycle would otherwise stall. reversible_gh_guard_enabled() defaults
# OFF and is consulted only AFTER a pattern matches, so the config read stays off
# the hot path for non-matching commands (mirrors the SQL DDL / cloud blocks).
#
# These entries are anchored (#3756) and scanned against COMMAND_ASK_SCAN — the
# comment-stripped, literal-text-redacted ask working copy — exactly as they were
# while living in ASK_PATTERNS, so #3756's redaction still applies when the toggle
# is opted IN (an ask-phrase quoted inside a --body/--comment value does not
# false-ask). `gh release delete` deliberately stays in the ungated ASK_PATTERNS
# above (hard to reverse) and is NOT gated here.
# =============================================================================
REVERSIBLE_GH_ASK_PATTERNS=(
    '(^|[;&|[:space:]])gh pr close'
    '(^|[;&|[:space:]])gh issue close'
    '(^|[;&|[:space:]])gh label delete'
)

for pattern in "${REVERSIBLE_GH_ASK_PATTERNS[@]}"; do
    if echo "$COMMAND_ASK_SCAN" | grep -qE "$pattern" && reversible_gh_guard_enabled; then
        ask "Command requires confirmation: $COMMAND (set guards.reversibleGh:true in .claude/skills/repo/config.json to keep this ask; it is off by default because the op is trivially reversible)" "reversible-gh:$pattern"
    fi
done

# =============================================================================
# INDEX-MUTATION ANALYZER (#7923) — the executable-context / per-segment
# isolation engine behind the `git-read-tree` deny site below.
#
# WHY A STRUCTURED PASS AND NOT A REGEX
#
# The deny site used to ask two independent substring questions of
# COMMAND_NO_COMMENT:
#
#   1. does `(^|[;&|(`]|[[:space:]])git[[:space:]]+read-tree` appear anywhere?
#   2. does `GIT_INDEX_FILE=` appear anywhere?
#
# Neither question is about EXECUTION, so both were wrong in both directions:
#
#   * FALSE DENY — COMMAND_NO_COMMENT strips only `#` comments, so a quoted
#     `--body` value or a `cat > file <<QUOTED` heredoc body that merely
#     MENTIONS the phrase matched (1) and was hard-denied. That blocked
#     filing/commenting on any issue about this guard, including #7923 itself.
#   * FALSE ALLOW — the boundary class has no quote characters, so
#     `bash -c <SQ>git read-tree HEAD<SQ>`, `sh -c "…"` and `eval <SQ>…<SQ>`
#     -- which DO mutate the real index -- never matched (1) at all. And (2)
#     accepted a `GIT_INDEX_FILE=` occurrence from ANYWHERE in the string, so
#     `echo <SQ>GIT_INDEX_FILE=<SQ> ; git read-tree HEAD` and
#     `GIT_INDEX_FILE=/tmp/i git status; git read-tree HEAD` (assignment scoped
#     to a DIFFERENT simple command) both authorized an unrelated invocation.
#
# No lossier scan COPY fixes this: #7923 measured the obvious
# COMMAND_ASK_SCAN swap and it flipped `printf <SQ>%s\n<SQ> <SQ>git read-tree
# HEAD<SQ> > /tmp/notes.txt` to a deny (a quoted POSITIONAL of a non-executing
# command is inert, and no copy in the chain masks those generally) while
# leaving both isolation-scoping holes open. The questions this site actually
# needs answered are structural, so it gets a structural pass.
#
# WHAT IT ANSWERS
#
# For each SIMPLE COMMAND in the command text: is its command word `git` with
# subcommand `read-tree`, and is a `GIT_INDEX_FILE=` assignment in force FOR
# THAT SIMPLE COMMAND? Quoted text is inert DATA unless the segment that
# carries it actually re-executes it — the wrapper vocabulary is exactly the
# one mask_ask_positional_args() already names as the reason it excludes
# wrappers from its allowlist (`sh|bash|zsh|dash -c`, `eval`, `source`/`.`),
# plus the `$(…)` / backtick substitutions qsplit()/has_live_subst() already
# treat as live and a pipeline whose sink is a stdin-reading shell.
#
# MONOTONE ON THE DENY SET (the security floor, argued not asserted)
#
# For any text that is actually EXECUTED, this pass is a strict superset of
# the old regex:
#   * The old regex required a separator/whitespace boundary then the literal
#     bytes `git` + whitespace + `read-tree`. That is exactly an ADJACENT
#     `git` / `read-tree` word pair inside one segment, which im_segments()
#     re-checks verbatim as its "lenient net" AFTER the precise command-word
#     walk — so nothing the old matcher caught in executable text is lost,
#     even when a prefix this pass does not model (`timeout 5 …`, `sudo -u x
#     …`) sits in front of it.
#   * The precise walk ADDS shapes the old regex structurally could not see:
#     `git -c core.quotepath=false read-tree` (git accepts `-c`/`-C`/
#     `--git-dir` config overrides BEFORE the subcommand, so the two words are
#     not adjacent), a quoted subcommand, and every interpreter-wrapped body.
#   * Isolation only ever gets STRICTER: a bare `GIT_INDEX_FILE=` substring no
#     longer authorizes anything; the assignment must be an assignment PREFIX
#     of the same simple command, an `env`/`sudo`-carried assignment for it, or
#     a genuinely persistent earlier `export GIT_INDEX_FILE=` / standalone
#     assignment segment (the shapes where the real shell would in fact export
#     it to the later segment).
# The ONLY direction it narrows is the intended one: a `git read-tree` phrase
# that no shell would ever execute — quoted data handed to a non-executing
# command, or a LITERAL heredoc body fed to something that is not an
# interpreter. "Literal" is load-bearing there: only a QUOTED delimiter
# (`<<'EOF'` / `<<"EOF"`) makes a body literal. A bare `<<EOF` body is
# expanded BY THE SHELL before the sink reads a byte of it, so its `$( … )` /
# backtick spans are scanned as executable text (im_hd_expand()) even when the
# owning command is a known inert sink.
#
# Fail-closed by construction: an unterminated quote, an unbalanced `$(`, an
# unclosed heredoc and a recursion beyond the depth bound all leave the text
# VISIBLE/treated as executable rather than inert, and index_mutation_unisolated()
# falls back to the legacy regex pair if awk itself fails.
#
# MEASURED, not asserted. A differential sweep of a 148-shape corpus against
# the merge-base hook (`origin/main` at 44e9ab48), each version built into its
# OWN isolated tree and fed PreToolUse JSON exactly as
# tests/hooks/lib/guard-destructive-harness.sh make_input() builds it (throwaway
# git cwd, no .loom/config.json, every LOOM_* unset):
#
#     36 shapes  allow -> deny    wrapper escapes, both isolation-scoping
#                                 holes, git -c/-C/--git-dir/--work-tree
#     27 shapes  deny  -> allow
#     85 shapes  unchanged
#
# Of the 27 deny -> allow, 22 are PROVABLY inert: each was probed by replacing
# the index subcommand with a marker program and RUNNING the shape — the marker
# never fired, so no shell ever executed that text. They are quoted
# `--body`/`--comment` values and heredoc bodies owned by a known inert sink
# (`cat > file`, `tee`, `git commit -F -`, `grep`, `jq`, the
# `--body "$(cat <<QUOTED … )"` filing idiom), either with a QUOTED delimiter or
# with no substitution in the body at all. The other 5 are the write-then-execute
# shapes recorded as limitation 4 below: their marker DOES fire, and they are
# accepted with reasons, not claimed inert.
#
# KNOWN LIMITATIONS (unchanged from the old matcher — recorded, not introduced)
#
#   1. A phrase QUOTED inside a non-shell interpreter body — `python - <<EOF` /
#      `os.system("git read-tree HEAD")` — is not seen. The old regex missed it
#      for the same reason (no quote character in its boundary class), and this
#      pass does not parse Python/Perl/Ruby syntax. Bodies of such commands keep
#      the old regex verbatim (im_legacy()) rather than the structural pass, so
#      the coverage is identical to before, not narrower.
#   2. A payload ASSEMBLED at runtime — `bash -c "$(printf 'git read-tree')"`,
#      an `eval` of a variable — is not resolvable without executing it. Also
#      unchanged: the old matcher missed every one of these too.
#   3. `git` reached through an alias/variable command word (`$G read-tree`) is
#      not resolved, matching every other command-word-anchored check in this
#      file (see the printenv/systemctl/ssh-cat segment parsers).
#
# ACCEPTED LIMITATIONS INTRODUCED BY THIS PASS (measured, with reasons)
#
#   4. WRITE-THEN-EXECUTE inside ONE command string —
#      `cat > /tmp/x.sh <<QUOTED … EOF` followed by `bash /tmp/x.sh` (also the
#      `tee`, `source` and `cat <<QUOTED > file && bash file` variants). The
#      heredoc body genuinely IS literal to `cat`, so this pass treats it as
#      file content; the same string then executes the file it just wrote. The
#      old regex denied these only by accident — it matched the raw bytes
#      wherever they sat — and the shape was never actually covered: no guard
#      can follow a file across `bash /tmp/x.sh`, and splitting the write and
#      the run into two tool calls escaped the old matcher exactly as it escapes
#      this one. 5 shapes in the sweep; accepted as out of remit, recorded here
#      rather than omitted from the deny -> allow table.
#   5. PROCESS SUBSTITUTION as an interpreter payload —
#      `source <(echo 'git read-tree HEAD')`, `. <(…)`, `bash <(…)`.
#      ALLOW on both sides (no regression, so not a deny -> allow move), but it
#      is the same class of escape this pass set out to close: the phrase is a
#      quoted argument of a non-executing producer whose output the interpreter
#      then runs through a /dev/fd path. Recorded so this inventory stays
#      honest rather than silently short. (A process substitution whose own
#      text is unquoted — `diff <(git read-tree HEAD) f` — is still denied by
#      the lenient net, which sees the adjacent word pair.)
#
# Hot path: gated behind a `read-tree` substring test at the call site, so the
# awk fork only happens for a command that mentions the phrase at all.
# =============================================================================
_INDEXMUT_AWK="$_HASLIVESUBST_AWK"'
# Basename of a command word (so /usr/bin/git is still git).
function im_base(w,   p, k) {
    p = w
    k = index(p, "/")
    while (k > 0) { p = substr(p, k + 1); k = index(p, "/") }
    return p
}

# Interpreters whose ARGUMENTS are shell source text. Deliberately the same
# vocabulary mask_ask_positional_args() names in its own header as the set it
# refuses to allowlist ("a command that WRAPS the phrase and then executes it").
function im_is_interp(b) {
    return (b == "sh" || b == "bash" || b == "zsh" || b == "dash" ||
            b == "ksh" || b == "mksh" || b == "ash" || b == "busybox" ||
            b == "eval" || b == "source" || b == ".")
}

# Shells that execute their STANDARD INPUT when given no -c payload -- the
# sink shape of `echo <SQ>…<SQ> | sh`.
function im_is_stdin_shell(b) {
    return (b == "sh" || b == "bash" || b == "zsh" || b == "dash" ||
            b == "ksh" || b == "mksh" || b == "ash")
}

# Commands that can own a heredoc without EXECUTING its body: the body is file
# content, message text or search input. DELIBERATELY NARROW, the same
# convention mask_ask_positional_args() states for its own allowlist -- this is
# the ONLY list that lets a heredoc body stop being scanned, so anything not on
# it (an interpreter, a language runtime, an unknown command) keeps the
# pre-#7923 treatment via im_legacy() below. Adding an entry here is a claim
# that the command cannot run its stdin as code.
function im_is_inert_sink(b) {
    return (b == "cat" || b == "tee" || b == "grep" || b == "egrep" ||
            b == "fgrep" || b == "rg" || b == "head" || b == "tail" ||
            b == "wc" || b == "sort" || b == "uniq" || b == "diff" ||
            b == "cmp" || b == "jq" || b == "yq" || b == "gh" || b == "git" ||
            b == "base64" || b == "tr" || b == "column" || b == "cut" ||
            b == "md5sum" || b == "shasum" || b == "sha1sum" || b == "sha256sum")
}

# The PRE-#7923 matcher, verbatim, over a raw string. Applied to a heredoc body
# whose owning command is neither a shell (re-scanned structurally) nor a known
# inert sink (ignored) -- a `python - <<EOF` / `perl <<EOF` body can call out to
# the shell in its own syntax, which this pass does not parse, so those bodies
# keep exactly the treatment they had before this change rather than silently
# becoming allow. Monotonicity over cleverness.
function im_legacy(s) {
    if (index(s, "read-tree") == 0) return
    if (s ~ /GIT_INDEX_FILE=/) return
    if (s ~ /(^|[;&|(`]|[ \t\n])git[ \t]+read-tree/) IMHIT = 1
}

# Words that may PRECEDE the real command word without being it: shell
# grammar keywords/openers and env-preserving launcher prefixes.
function im_is_prefix(b) {
    return (b == "{" || b == "!" || b == "if" || b == "then" || b == "else" ||
            b == "elif" || b == "while" || b == "until" || b == "do" ||
            b == "time" || b == "command" || b == "builtin" || b == "exec" ||
            b == "nohup" || b == "sudo" || b == "doas" || b == "env" ||
            b == "stdbuf" || b == "nice" || b == "ionice" || b == "setsid" ||
            b == "xargs")
}

# git GLOBAL options that consume the NEXT word as their value, so the
# subcommand walk does not mistake the value for the subcommand. This is what
# makes `git -c core.quotepath=false read-tree` visible.
function im_git_opt_takes_value(u) {
    return (u == "-c" || u == "-C" || u == "--git-dir" || u == "--work-tree" ||
            u == "--namespace" || u == "--exec-path" || u == "--super-prefix" ||
            u == "--config-env")
}

# NAME of a shell assignment token, or "" when the token is not one. Tested on
# the RAW token on purpose: `"GIT_INDEX_FILE=/tmp/i"` (quote BEFORE the name)
# is a command word to the shell, not an assignment, and must not isolate.
function im_assign_name(rawtok) {
    if (match(rawtok, /^[A-Za-z_][A-Za-z0-9_]*=/)) return substr(rawtok, 1, RLENGTH - 1)
    return ""
}

# An assignment/export only reaches a LATER segment across a separator that
# keeps the same shell: `;`, `&&` and a newline. Never across a pipe.
function im_persist_sep(sep) {
    return (sep == ";" || sep == "&&" || sep == "\n" || sep == "")
}

function im_hd_index(u) {
    if (match(u, /^IMHD[0-9]+IM$/)) return substr(u, 5, length(u) - 6) + 0
    return 0
}

# Remove ONE quoting layer, shell-accurately: quotes at the outer level are
# removed, quotes nested inside the other kind are preserved, so recursing into
# `bash -c <SQ>echo "hi"<SQ>` still sees the inner double quotes.
function im_unquote(tok,   out, i, n, c, q) {
    out = ""; n = length(tok); i = 1; q = ""
    while (i <= n) {
        c = substr(tok, i, 1)
        if (q == SQ) {
            if (c == SQ) q = ""; else out = out c
            i++; continue
        }
        if (q == DQ) {
            if (c == "\\") { i++; if (i <= n) out = out substr(tok, i, 1); i++; continue }
            if (c == DQ) { q = ""; i++; continue }
            out = out c; i++; continue
        }
        if (c == SQ) { q = SQ; i++; continue }
        if (c == DQ) { q = DQ; i++; continue }
        if (c == "\\") { i++; if (i <= n) out = out substr(tok, i, 1); i++; continue }
        out = out c; i++
    }
    return out
}

# Replace each heredoc BODY with a marker word on the opener line, remembering
# the body in the GLOBAL IMHD[] (global, and keyed by a global counter, so a
# marker still resolves when the opener line is later re-scanned one recursion
# level down -- e.g. the `cat` inside `--body "$(cat <<QUOTED … )"`). What
# happens to a body is decided by its OWNING command in im_segments(), not
# here: a `bash <<QUOTED` body executes, a `cat > file <<QUOTED` body is file
# content, and anything else keeps the pre-#7923 regex treatment.
# An UNCLOSED heredoc is left untouched (body stays visible => fail-closed).
function im_mask_heredocs(s,   lines, nl, i, j, line, delim, closeat, trimmed,
                          dashform, k, out, skip, rs, rl, body, any, hdquoted) {
    if (index(s, "<<") == 0) return s
    nl = split(s, lines, "\n")
    for (i = 1; i <= nl; i++) skip[i] = 0
    any = 0
    for (i = 1; i <= nl; i++) {
        if (skip[i]) continue
        line = lines[i]
        if (!match(line, HDRE)) continue
        rs = RSTART; rl = RLENGTH
        delim = substr(line, rs, rl)
        dashform = (substr(delim, 3, 1) == "-")
        sub(/^<<-?[ \t]*/, "", delim)
        # QUOTEDNESS of the delimiter is security-relevant, so it is RECORDED
        # here, not merely stripped: a quoted delimiter makes the body literal,
        # but a bare `<<EOF` does NOT -- the shell performs parameter expansion
        # and command substitution on such a body BEFORE the owning command
        # reads a single byte of it. See im_hd_expand().
        # (No apostrophes in this awk program: it is a single-quoted string.)
        hdquoted = (substr(delim, 1, 1) == SQ || substr(delim, 1, 1) == DQ)
        if (hdquoted) {
            delim = substr(delim, 2, length(delim) - 2)
        }
        if (delim == "") continue
        closeat = 0
        for (j = i + 1; j <= nl; j++) {
            trimmed = lines[j]
            if (dashform) sub(/^\t+/, "", trimmed)
            sub(/[ \t]+$/, "", trimmed)
            if (trimmed == delim) { closeat = j; break }
        }
        if (closeat == 0) continue
        IMHDN++
        k = IMHDN
        any = 1
        body = ""
        for (j = i + 1; j < closeat; j++) {
            body = body (body == "" ? "" : "\n") lines[j]
            skip[j] = 1
        }
        skip[closeat] = 1
        IMHD[k] = body
        IMHDQ[k] = hdquoted
        lines[i] = substr(line, 1, rs - 1) " IMHD" k "IM " substr(line, rs + rl)
    }
    if (!any) return s
    out = ""
    for (i = 1; i <= nl; i++) {
        if (skip[i]) continue
        out = out (out == "" ? "" : "\n") lines[i]
    }
    return out
}

# Recurse into every LIVE command substitution -- `$( … )` and backticks,
# including inside double quotes -- and replace the span with an inert
# placeholder so the outer lex is not confused by its separators. A span inside
# SINGLE quotes is skipped: bash performs no expansion there at all, the same
# floor strip_literal_text() records for its own single-quote carve-out.
# Unbalanced/unterminated spans are left verbatim (fail-closed).
function im_extract_subst(s, depth,   out, n, i, c, q, dep, j, inner) {
    out = ""; n = length(s); i = 1; q = ""
    while (i <= n) {
        c = substr(s, i, 1)
        if (q == SQ) { out = out c; if (c == SQ) q = ""; i++; continue }
        if (q == DQ) {
            if (c == "\\") { out = out c; i++; if (i <= n) { out = out substr(s, i, 1); i++ }; continue }
            if (c == DQ) { out = out c; q = ""; i++; continue }
        } else {
            if (c == SQ) { q = SQ; out = out c; i++; continue }
            if (c == DQ) { q = DQ; out = out c; i++; continue }
            if (c == "\\") { out = out c; i++; if (i <= n) { out = out substr(s, i, 1); i++ }; continue }
        }
        if (c == "$" && substr(s, i + 1, 1) == "(") {
            dep = 1; j = i + 2
            while (j <= n) {
                if (substr(s, j, 1) == "(") dep++
                else if (substr(s, j, 1) == ")") { dep--; if (dep == 0) break }
                j++
            }
            if (dep != 0) { out = out c; i++; continue }
            inner = substr(s, i + 2, j - i - 2)
            im_scan(inner, depth + 1)
            out = out "IMSUB"
            i = j + 1
            continue
        }
        if (c == "`") {
            j = i + 1
            while (j <= n && substr(s, j, 1) != "`") j++
            if (j > n) { out = out c; i++; continue }
            inner = substr(s, i + 1, j - i - 1)
            im_scan(inner, depth + 1)
            out = out "IMSUB"
            i = j + 1
            continue
        }
        out = out c; i++
    }
    return out
}

# LIVE expansions inside an UNQUOTED-delimiter heredoc body.
#
# `cat > f <<QUOTED` is genuinely literal, but a bare `cat > f <<EOF` is NOT:
# the SHELL performs parameter expansion and command substitution on the body
# and hands the RESULT to the sink, so a `$( ... )` / backtick span in such a
# body executes against the real index even though cat/tee/gh/jq/grep never run
# a byte of it as code. Only the SPANS are live -- the surrounding text really
# is data -- so this scans the spans and nothing else, which is why a plain
# `cat > f <<EOF` body naming the index command stays allow.
#
# DELIBERATELY QUOTE-BLIND, unlike im_extract_subst(): quote characters carry
# no quoting meaning inside a heredoc body, so a span wrapped in single quotes
# there is expanded exactly like a bare one. A BACKSLASH is the one suppressor
# the shell honours, so it is honoured here too. An unbalanced/unterminated
# span is not resolvable -- fail closed by handing the whole body to the
# pre-#7923 regex rather than ignoring it.
function im_hd_expand(s, depth,   n, i, c, j, dep) {
    n = length(s); i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        if (c == "\\") { i += 2; continue }
        if (c == "$" && substr(s, i + 1, 1) == "(") {
            dep = 1; j = i + 2
            while (j <= n) {
                if (substr(s, j, 1) == "(") dep++
                else if (substr(s, j, 1) == ")") { dep--; if (dep == 0) break }
                j++
            }
            if (dep != 0) { im_legacy(s); return }
            im_scan(substr(s, i + 2, j - i - 2), depth + 1)
            i = j + 1
            continue
        }
        if (c == "`") {
            j = i + 1
            while (j <= n && substr(s, j, 1) != "`") j++
            if (j > n) { im_legacy(s); return }
            im_scan(substr(s, i + 1, j - i - 1), depth + 1)
            i = j + 1
            continue
        }
        i++
    }
}

# Quote-aware lexer. tok[]/typ[] hold words ("w", quote characters PRESERVED so
# im_assign_name() can tell an assignment from a quoted look-alike) and
# separators ("s": ; && || | & newline ( ) ).
function im_lex(s, tok, typ,   n, i, c, cur, q, cnt) {
    n = length(s); i = 1; cur = ""; q = ""; cnt = 0
    while (i <= n) {
        c = substr(s, i, 1)
        if (q != "") {
            cur = cur c
            if (q == DQ && c == "\\") { i++; if (i <= n) cur = cur substr(s, i, 1); i++; continue }
            if (c == q) q = ""
            i++; continue
        }
        if (c == SQ || c == DQ) { q = c; cur = cur c; i++; continue }
        if (c == "\\") {
            # Backslash-newline is a LINE CONTINUATION: both bytes vanish and
            # the word continues, so `git \<newline>  read-tree` is the same
            # adjacent word pair as `git read-tree`.
            if (substr(s, i + 1, 1) == "\n") { i += 2; continue }
            cur = cur c; i++; if (i <= n) { cur = cur substr(s, i, 1); i++ }
            continue
        }
        if (c == " " || c == "\t") {
            if (cur != "") { cnt++; tok[cnt] = cur; typ[cnt] = "w"; cur = "" }
            i++; continue
        }
        if (c == "\n" || c == ";" || c == "&" || c == "|" || c == "(" || c == ")") {
            if (cur != "") { cnt++; tok[cnt] = cur; typ[cnt] = "w"; cur = "" }
            cnt++
            if (c == "&" && substr(s, i + 1, 1) == "&") { tok[cnt] = "&&"; i += 2 }
            else if (c == "|" && substr(s, i + 1, 1) == "|") { tok[cnt] = "||"; i += 2 }
            else { tok[cnt] = c; i++ }
            typ[cnt] = "s"
            continue
        }
        cur = cur c; i++
    }
    if (cur != "") { cnt++; tok[cnt] = cur; typ[cnt] = "w" }
    return cnt
}

# Index of the command word in tok[lo..hi] (assignments and launcher prefixes
# skipped), or 0 when the segment has none.
function im_cmdidx(tok, typ, lo, hi,   i, b) {
    i = lo
    while (i <= hi) {
        if (typ[i] != "w") { i++; continue }
        if (im_assign_name(tok[i]) != "") { i++; continue }
        b = im_base(im_unquote(tok[i]))
        if (b == "" || im_is_prefix(b)) { i++; continue }
        return i
    }
    return 0
}

# Does tok[lo..hi] name a shell that will EXECUTE ITS STDIN (no -c payload)?
function im_seg_is_stdin_shell(tok, typ, lo, hi,   ci, j) {
    ci = im_cmdidx(tok, typ, lo, hi)
    if (ci == 0) return 0
    if (!im_is_stdin_shell(im_base(im_unquote(tok[ci])))) return 0
    for (j = ci + 1; j <= hi; j++) {
        if (typ[j] == "w" && im_unquote(tok[j]) == "-c") return 0
    }
    return 1
}

function im_segments(tok, typ, n, depth,
                     ns, sstart, send, ssep, feeds, k, i, hi, j, b, u,
                     seg_iso, env_iso, assigned, payload, hdk) {
    ns = 1; sstart[1] = 1
    for (i = 1; i <= n; i++) {
        if (typ[i] != "s") continue
        send[ns] = i - 1
        ssep[ns] = tok[i]
        ns++
        sstart[ns] = i + 1
    }
    send[ns] = n
    ssep[ns] = ""

    # A segment whose pipeline SINK is a stdin-reading shell has its own data
    # words executed, so they are source text, not inert arguments.
    for (k = 1; k <= ns; k++) feeds[k] = 0
    for (k = ns - 1; k >= 1; k--) {
        if (ssep[k] != "|") continue
        if (feeds[k + 1] || im_seg_is_stdin_shell(tok, typ, sstart[k + 1], send[k + 1])) feeds[k] = 1
    }

    env_iso = 0
    for (k = 1; k <= ns; k++) {
        i = sstart[k]; hi = send[k]
        if (i > hi) continue
        seg_iso = env_iso
        assigned = 0
        # Assignment PREFIX of this simple command.
        while (i <= hi && typ[i] == "w" && im_assign_name(tok[i]) != "") {
            if (im_assign_name(tok[i]) == "GIT_INDEX_FILE") { seg_iso = 1; assigned = 1 }
            i++
        }
        if (i > hi) {
            # Assignments only: these DO persist into the following segments.
            if (assigned && im_persist_sep(ssep[k])) env_iso = 1
            continue
        }
        # Launcher prefixes (env/sudo/…) may carry further assignments.
        while (i <= hi && typ[i] == "w") {
            if (im_assign_name(tok[i]) != "") {
                if (im_assign_name(tok[i]) == "GIT_INDEX_FILE") seg_iso = 1
                i++; continue
            }
            b = im_base(im_unquote(tok[i]))
            if (b == "" || im_is_prefix(b)) { i++; continue }
            break
        }
        if (i > hi) continue
        b = im_base(im_unquote(tok[i]))

        # Heredoc bodies OWNED by this segment, routed by what the owning
        # command does with its stdin. Done before the branches below so every
        # command word reaches it, including `git` and `export`.
        for (j = sstart[k]; j <= hi; j++) {
            if (typ[j] != "w") continue
            hdk = im_hd_index(im_unquote(tok[j]))
            if (hdk == 0) continue
            if (im_is_interp(b) || feeds[k]) im_scan(IMHD[hdk], depth + 1)
            else if (!im_is_inert_sink(b)) im_legacy(IMHD[hdk])
            else if (!IMHDQ[hdk]) im_hd_expand(IMHD[hdk], depth)
        }

        if (b == "export") {
            for (j = i + 1; j <= hi; j++) {
                if (typ[j] != "w") continue
                if (im_assign_name(tok[j]) == "GIT_INDEX_FILE" && im_persist_sep(ssep[k])) env_iso = 1
            }
            continue
        }

        if (im_is_interp(b)) {
            # Every argument of an interpreter is shell SOURCE: scan each one
            # on its own AND the joined non-option run, so both
            # `bash -c <SQ>git read-tree HEAD<SQ>` and `eval git read-tree HEAD`
            # are seen. Its heredoc body was already scanned above.
            payload = ""
            for (j = i + 1; j <= hi; j++) {
                if (typ[j] != "w") continue
                u = im_unquote(tok[j])
                if (im_hd_index(u) > 0) continue
                if (substr(u, 1, 1) == "-") continue
                payload = payload (payload == "" ? "" : " ") u
                im_scan(u, depth + 1)
            }
            if (payload != "") im_scan(payload, depth + 1)
            continue
        }

        if (b == "git") {
            j = i + 1
            while (j <= hi && typ[j] == "w") {
                u = im_unquote(tok[j])
                if (length(u) > 1 && substr(u, 1, 1) == "-") {
                    if (im_git_opt_takes_value(u)) j += 2; else j++
                    continue
                }
                break
            }
            if (j <= hi && typ[j] == "w" && im_unquote(tok[j]) == "read-tree") {
                if (!seg_iso) IMHIT = 1
                continue
            }
        }

        # LENIENT NET -- the old regex, re-expressed per segment: an ADJACENT
        # `git` / `read-tree` word pair in executable text. Keeps every shape
        # the previous matcher denied denied, including behind a prefix this
        # pass does not model (`timeout 5 git read-tree`).
        for (j = i; j < hi; j++) {
            if (typ[j] != "w" || typ[j + 1] != "w") continue
            if (im_base(im_unquote(tok[j])) == "git" && im_unquote(tok[j + 1]) == "read-tree") {
                if (!seg_iso) IMHIT = 1
            }
        }

        if (feeds[k]) {
            for (j = i + 1; j <= hi; j++) {
                if (typ[j] != "w") continue
                u = im_unquote(tok[j])
                if (im_hd_index(u) > 0) continue
                im_scan(u, depth + 1)
            }
        }
    }
}

function im_scan(s, depth,   tok, typ, n) {
    if (index(s, "read-tree") == 0 && index(s, "IMHD") == 0) return
    if (depth > 5) { IMHIT = 1; return }
    s = im_mask_heredocs(s)
    s = im_extract_subst(s, depth)
    n = im_lex(s, tok, typ)
    im_segments(tok, typ, n, depth)
}

BEGIN {
    SQ = sprintf("%c", 39)
    DQ = sprintf("%c", 34)
    HDRE = "<<-?[ \t]*(" SQ "[A-Za-z_][A-Za-z0-9_]*" SQ "|" DQ "[A-Za-z_][A-Za-z0-9_]*" DQ "|[A-Za-z_][A-Za-z0-9_]*)"
    IMHIT = 0
    IMHDN = 0
    buf = ""
}
{ buf = buf (NR > 1 ? "\n" : "") $0 }
END {
    im_scan(buf, 0)
    printf "%d", IMHIT
}
'


# Returns 0 (true) when $1 contains a `git read-tree` that would mutate the
# REAL staging index without an isolating GIT_INDEX_FILE assignment in force
# for that invocation. Falls back to the pre-#7923 regex pair if awk itself
# fails, so a broken analyzer can only ever be as permissive as the old check
# (never more) — the ERR trap`s fail-open must not reach this deny floor.
index_mutation_unisolated() {
    local verdict
    verdict=$(printf '%s' "$1" | awk "$_INDEXMUT_AWK" 2>/dev/null) || verdict="awk-failed"
    if [[ "$verdict" == "1" ]]; then
        return 0
    elif [[ "$verdict" == "0" ]]; then
        return 1
    fi
    log_hook_error "index_mutation_unisolated: analyzer failed, falling back to legacy matcher"
    if echo "$1" | grep -qE '(^|[;&|(`]|[[:space:]])git[[:space:]]+read-tree' &&
        ! echo "$1" | grep -qE 'GIT_INDEX_FILE='; then
        return 0
    fi
    return 1
}

# =============================================================================
# git read-tree WITHOUT an isolating GIT_INDEX_FILE assignment
#
# A bare `git read-tree` (no tree-ish, no isolated index) is equivalent to
# `git read-tree --empty`: it clobbers the repository's REAL staging index,
# turning every tracked file into a phantom staged deletion. The working tree
# and HEAD are left untouched and NO reflog entry is written, so the corruption
# is silent and near-invisible (issue #3637 — a judge ran one against the main
# checkout during a merge simulation and emptied the live index).
#
# This is an ASK (not a deny) because it is generic git hygiene, not a Loom
# workflow rule, and an isolated form is legitimate. It is kept narrow: the
# safe, index-free path is `git merge-tree --write-tree <base> <branch>` for a
# merge preview, or `GIT_INDEX_FILE=$(mktemp) git read-tree <tree>` when a
# temporary index really is needed. Any command that carries a `GIT_INDEX_FILE=`
# assignment is treated as isolated and passes through untouched.
#
# `git commit-tree` is intentionally NOT guarded here — it writes a commit
# object from an existing tree and does not mutate the index.
#
# EXECUTABLE vs. INERT (loom#7923, ported in repo#585): the two substring tests
# this site used to make were context-blind. index_mutation_unisolated() above
# asks the structural question instead: is a `git read-tree` simple command
# actually going to run (command word, `git -c/-C/--git-dir` options, quoted
# subcommand, `bash -c`/`eval`/`source` payloads, `$(..)`/backticks, unquoted
# heredocs), and is an isolating GIT_INDEX_FILE= assignment in force FOR THAT
# invocation (assignment prefix, env/sudo-carried, or a persistent earlier
# export/standalone assignment). A quoted `--body` value or literal heredoc
# body that merely MENTIONS the phrase no longer asks. TIER: stays ASK here
# (Loom escalated this to deny in loom#7795; the canonical guard keeps its
# documented ask contract — see the PR body for the rationale).
# =============================================================================
if [[ "$COMMAND_NO_COMMENT" == *"read-tree"* ]] && index_mutation_unisolated "$COMMAND_NO_COMMENT"; then
    ask "Command requires confirmation: $COMMAND (this 'git read-tree' targets the REAL staging index — a bare form empties it outright with no reflog trace; use 'git merge-tree --write-tree <base> <branch>' for a merge preview, or isolate with GIT_INDEX_FILE=\$(mktemp) on THAT invocation)" "git-read-tree"
fi

# True if $1 (the ask-scan form of a command) contains at least one stash
# CREATE invocation: bare `git stash`, `git stash push …`, `git stash save …`,
# or an option-prefixed create (`git stash -u`, `git stash --include-untracked`,
# `git stash -m wip`).
#
# Deliberately NOT treated as a create (#5754):
#   - `pop` / `drop` / `clear` — the RECOVERY half, handled by its own ask
#     below. Never escalate those: once WIP is on `refs/stash`, `pop` is the
#     only way to get it back (worktree.sh's stash-pop reads a per-issue ref,
#     not `refs/stash`), so blocking them strands work with no recovery path.
#   - `apply` / `list` / `show` / `branch` — do not remove entries from the
#     shared stack.
#   - `create` / `store` — plumbing. `git stash create` is exactly what
#     worktree.sh's own `stash-push` runs, so matching it would deny the
#     sanctioned replacement path itself.
#   - `-h` / `--help` — not an operation at all.
#
# ERE has no lookahead, and one command can chain several `git stash`
# invocations of different kinds (`git stash && <check>; git stash pop` is the
# exact shape this fires on), so the subcommand token is extracted per
# occurrence and classified in shell rather than encoded in a single pattern.
# The trailing `([[:space:]]|[;&|)]|$)` on the match is what makes `stash` a
# whole token — without it `git stashx` would match the `git stash` prefix and
# be misread as a bare create.
#
# BACKTICK BOUNDARY (#5783): the leading class, the subcommand token's
# excluded-character class, and the trailing class all now also admit a
# backtick — `` `git stash push` `` used to be invisible to the leading
# anchor entirely, and even after that half is fixed, an unfixed subcommand
# class would swallow the closing backtick into the token itself (`push\``)
# and fail the `push` case match below. All three sites need the same
# widening together for a backtick-wrapped create to classify correctly.
stash_create_invoked() {
    local scan="$1" occurrence subcmd
    local -a parts
    while IFS= read -r occurrence; do
        [[ -n "$occurrence" ]] || continue
        IFS=$' \t' read -r -a parts <<< "$occurrence"
        subcmd="${parts[2]:-}"
        # The match may swallow a trailing separator (`git stash push;`), so
        # keep only the token up to the first shell delimiter.
        subcmd="${subcmd%%[;&|)\`]*}"
        case "$subcmd" in
            -h|--help)        ;;
            ""|push|save|-*)  return 0 ;;
            *)                ;;
        esac
    done < <(printf '%s\n' "$scan" \
        | grep -oE '(^|[;&|(`]|[[:space:]])git[[:space:]]+stash([[:space:]]+[^[:space:];&|)`]+)?([[:space:]]|[;&|)`]|$)' \
        | sed -E 's/^.*(git[[:space:]]+stash)/\1/')
    return 1
}

# Mask quoted POSITIONAL arguments to grep/egrep/fgrep/rg/awk (loom#7363) —
# used ONLY to build COMMAND_STASH_SCAN for the stash detectors below, never
# fed back into COMMAND_ASK_SCAN (whose SQL-DDL consumer must keep seeing
# grep's own quoted argument). Escape-aware for double quotes.
mask_stash_scan_positional_args() {
    printf '%s' "$1" | awk "$_HASLIVESUBST_AWK"'
    BEGIN {
        SQ = sprintf("%c", 39)
        DQ = sprintf("%c", 34)
        BS = sprintf("%c", 92)
        # Command-name allowlist: read-only search commands whose quoted
        # pattern/program argument is inert search text, never a live shell
        # invocation. Safe to include grep/rg AND awk here — see the function
        # header comment for why this copy has no SQL-DDL (or other raw-text)
        # consumer to protect, unlike COMMAND_ASK_SCAN.
        cmdre = "(grep|egrep|fgrep|rg|awk)"
        flagre = "([ \t]+-[A-Za-z0-9_-]+)*"
        anchor = "(^|[ \t\n;&|`(])" cmdre flagre "[ \t]+"
        buf = ""
    }
    { buf = buf (NR > 1 ? "\n" : "") $0 }
    END {
        s = buf
        out = ""
        while (match(s, anchor)) {
            pre     = substr(s, 1, RSTART - 1)
            matched = substr(s, RSTART, RLENGTH)
            rest    = substr(s, RSTART + RLENGTH)
            out = out pre matched
            # Mask every consecutive quoted positional argument immediately
            # following the anchor (whitespace-separated), same boundary
            # convention as mask_catastrophic_positional_args() above.
            while (1) {
                qc = substr(rest, 1, 1)
                if (qc != DQ && qc != SQ) break
                endpos = 0
                if (qc == DQ) {
                    # Escape-aware: a backslash swallows the NEXT character as
                    # one atomic unit, so an escaped `\"` can never be misread
                    # as the closing quote.
                    i = 2
                    rlen = length(rest)
                    while (i <= rlen) {
                        c = substr(rest, i, 1)
                        if (c == BS) { i += 2; continue }
                        if (c == DQ) { endpos = i; break }
                        i++
                    }
                } else {
                    # Single-quoted: bash gives backslash no special meaning
                    # inside real single quotes, so a plain same-character
                    # scan is correct here.
                    for (i = 2; i <= length(rest); i++) {
                        if (substr(rest, i, 1) == qc) { endpos = i; break }
                    }
                }
                if (endpos == 0) break
                inner = substr(rest, 2, endpos - 2)
                if (!has_live_subst(inner)) {
                    gsub(/./, "X", inner)
                }
                out = out qc inner qc
                rest = substr(rest, endpos + 1)
                while (substr(rest, 1, 1) == " " || substr(rest, 1, 1) == "\t") {
                    out = out substr(rest, 1, 1)
                    rest = substr(rest, 2)
                }
            }
            s = rest
        }
        out = out s
        printf "%s", out
    }'
}

# =============================================================================
# GIT STASH SCOPE ASK — gated by the stash-scope guard toggle
#
# Transplanted from Loom's vendored copy (loom#5173/#4821/#5217) as part of the
# repo#188 parity reconciliation. Before this, the canonical guard had ZERO
# coverage of `git stash` — an agent could destroy operator-preserved WIP with
# no confirmation, while the vendored guard asked. That was the single largest
# capability gap between the two.
#
# Two distinct hazards, both real:
#
# 1. MAIN CHECKOUT. The main checkout's stash stack is operator-owned, not
#    scratch space for an integration check. `pop`/`drop`/`clear` there can
#    destroy state a human deliberately preserved.
#
# 2. WORKTREE-TO-WORKTREE COLLISION. `refs/stash` is a SINGLE stack shared by
#    every linked worktree of a repo, not per-worktree. Two agents working in
#    different worktrees can pop or drop each other's WIP, and a main-checkout-
#    only check asks for neither side. A single active worktree has nobody to
#    collide with, so it stays ungated.
#
#    GENERALIZED from Loom's version, which counted `.loom-managed` marker
#    files under `<main>/.loom/worktrees/`. This counts what git itself
#    reports, so the hazard is caught for any tool's worktrees — or none at
#    all — rather than only Loom's. Same reasoning as the tool-agnostic
#    worktree-root detection elsewhere in this file.
#
# A same-chain heuristic ("push and pop appear in the same command, so allow")
# was considered and rejected upstream (loom#5217): push and pop are separate
# guard-approved calls with arbitrary time between them, so another worktree's
# concurrent push can land on the shared stack in that window and the paired
# pop then restores the WRONG entry. A same-chain check cannot see that.
#
# Gated by stash_scope_guard_enabled() (guards.stashScope /
# REPO_GUARD_STASH_SCOPE, legacy LOOM_GUARD_STASH_SCOPE, default on), invoked
# LAZILY only after the pattern matched, mirroring every other cold-path toggle.
# =============================================================================
# The optional `(-C <path>|-c <k=v>)*` run between `git` and `stash` is
# repo#194: without it this pre-check never matches `git -C <path> stash pop`,
# so the parser below never runs and the -C form escapes the ask entirely —
# the pre-check, not the parser, is the actual gate.
#
# The flag value must tolerate whitespace inside quotes, and MIXED forms like
# `-c user.name="John Doe"` where one token is part bare and part quoted. A
# first cut used `[^[:space:]]+`, which silently reintroduced the very bypass
# this closes: any quoted value containing a space failed the positional match,
# so the whole pre-check missed and the ask was skipped — a silent allow, not
# even an ask. Rather than enumerate token shapes, match the flag run
# non-greedily up to `stash`, and let the parser below (which is genuinely
# quote-aware via qsplit/mask_ws) decide scope. This gate only needs to be
# permissive enough not to miss; being over-inclusive here costs a parser call,
# never a wrong verdict.
#
# repo#202: a leading `GIT_DIR=<path> GIT_WORK_TREE=<path>` assignment run
# before `git` is a DIFFERENT parse shape — the command does not start with
# `git` at all — so it needed its own alternative in this same pre-check
# rather than a tweak to the existing one. `([A-Za-z_][A-Za-z0-9_]*=[^;&|[:space:]]*[[:space:]]+)*`
# tolerates zero or more such assignments (any name, not just GIT_DIR/
# GIT_WORK_TREE — resolve_stash_cwd only ACTS on the two it recognises, so
# being permissive here again only costs a parser call, never a wrong verdict).
# COMMAND_STASH_SCAN (loom#7363): a branched copy of COMMAND_ASK_SCAN with the
# quoted positional pattern/program of grep/egrep/fgrep/rg/awk masked, read ONLY
# by the stash detectors below. A read-only `grep -n "...git stash pop..." f`
# searching for a literal test-case name is not a stash invocation.
COMMAND_STASH_SCAN="$COMMAND_ASK_SCAN"
if [[ "$COMMAND" == *"grep"* || "$COMMAND" == *"awk"* || "$COMMAND" == *"rg "* ]]; then
    COMMAND_STASH_SCAN=$(mask_stash_scan_positional_args "$COMMAND_STASH_SCAN")
fi
# BACKTICK BOUNDARY (loom#5783): a backtick is admitted as a leading and
# trailing boundary alongside `(`/`)`, so `` `git stash pop` `` and
# `$(git stash pop)` are visible to the scan.
_stash_is_recover=false
_stash_is_create=false
if echo "$COMMAND_STASH_SCAN" | grep -qE '(^|[;&|(`]|[[:space:]])([A-Za-z_][A-Za-z0-9_]*=[^;&|[:space:]]*[[:space:]]+)*git[[:space:]]+([^;&|]*[[:space:]]+)?stash[[:space:]]+(pop|drop|clear)([[:space:]]|[;&|)`]|$)'; then
    _stash_is_recover=true
fi
# CREATE detection (stash_create_invoked, loom#5754): `git stash`, `push`,
# `save`, or an option-prefixed create. Used ONLY by the create-side redirect
# deny inside the linked-worktree branch below; creates elsewhere stay allowed.
if [[ "$COMMAND_STASH_SCAN" == *"stash"* ]] && stash_create_invoked "$COMMAND_STASH_SCAN"; then
    _stash_is_create=true
fi
if [[ "$_stash_is_recover" == true || "$_stash_is_create" == true ]] \
   && stash_scope_guard_enabled; then
    _stash_effective_cwd="$CWD"
    _stash_effective_gitdir=""
    _stash_effective_worktree=""
    if [[ -n "$CWD" ]]; then
        _stash_resolved=$(resolve_stash_cwd "$COMMAND_NO_COMMENT" "$CWD")
        _stash_effective_cwd=$(printf '%s\n' "$_stash_resolved" | sed -n '1p')
        _stash_effective_gitdir=$(printf '%s\n' "$_stash_resolved" | sed -n '2p')
        _stash_effective_worktree=$(printf '%s\n' "$_stash_resolved" | sed -n '3p')
        [[ -z "$_stash_effective_cwd" ]] && _stash_effective_cwd="$CWD"
    fi
    # repo#600: resolve_stash_cwd() reports the _LOOM_UNKNOWN_CWD sentinel
    # (alone, or as the prefix of a relative -C / --git-dir / --work-tree
    # joined onto it) once a command word it cannot resolve statically -- a
    # disguised/expanded spelling of cd, a cd inside a group or conditional --
    # may have moved the cwd. Scope is then unknowable: a recovery asks; a
    # create stays allowed, exactly like a create anywhere the redirect cannot
    # name a lossless per-issue replacement.
    if [[ "$_stash_effective_cwd" == "$_LOOM_UNKNOWN_CWD"* || \
          "$_stash_effective_gitdir" == "$_LOOM_UNKNOWN_CWD"* || \
          "$_stash_effective_worktree" == "$_LOOM_UNKNOWN_CWD"* ]]; then
        if [[ "$_stash_is_recover" == true ]]; then
            ask "Command requires confirmation: $COMMAND (git stash pop/drop/clear after a command word that may change the working directory in a way this guard cannot resolve statically — e.g. a quoted/escaped/expanded spelling of cd — so the checkout whose stash stack it touches is unknown; set guards.stashScope:false / REPO_GUARD_STASH_SCOPE=0 to disable this ask)" "stash-scope:unknown-cwd"
        fi
        _stash_effective_cwd=""
        _stash_effective_gitdir=""
        _stash_effective_worktree=""
    fi
    # Shell-accurate quote removal for cwd/gitdir/worktree RESOLUTION only —
    # resolve_stash_cwd() threads these from the RAW argument (quotes intact),
    # so unquote a COPY of each before resolving against the filesystem. An
    # unterminated quote falls back to the raw value (today's verdict —
    # ambiguous/ask), never widening to allow.
    if [[ "$_stash_effective_cwd" == *"'"* || "$_stash_effective_cwd" == *'"'* ]]; then
        strip_target_quoting "$_stash_effective_cwd" && _stash_effective_cwd="$_UNQUOTED_TARGET"
    fi
    if [[ "$_stash_effective_gitdir" == *"'"* || "$_stash_effective_gitdir" == *'"'* ]]; then
        strip_target_quoting "$_stash_effective_gitdir" && _stash_effective_gitdir="$_UNQUOTED_TARGET"
    fi
    if [[ "$_stash_effective_worktree" == *"'"* || "$_stash_effective_worktree" == *'"'* ]]; then
        strip_target_quoting "$_stash_effective_worktree" && _stash_effective_worktree="$_UNQUOTED_TARGET"
    fi

    _stash_toplevel=""
    _stash_common_parent=""
    # Every `git --git-dir=…` probe below must run from the SAME directory the
    # real command runs from (repo#204 review): git resolves --show-toplevel by
    # cwd-based worktree inference whenever --work-tree/GIT_WORK_TREE is absent,
    # so a bare `git --git-dir=… rev-parse --show-toplevel` answers for the
    # GUARD process cwd, not for the command being judged. That silently
    # allowed `GIT_DIR=<main>/.git git -C <main> stash pop` issued from a linked
    # worktree — the toplevel came back as the guard cwd, never matched the
    # main checkout common-dir parent, and fell through the collision branch.
    # Threading the resolved cwd through -C makes the probe ask git the same
    # question the command asks. A cwd that is not a directory makes `git -C`
    # fail, leaving toplevel/common empty -> the cd-unresolved ask, which is
    # the intended fail-safe (never a widened allow).
    _stash_gitdir_cd=()
    [[ -n "$_stash_effective_cwd" ]] && _stash_gitdir_cd=(-C "$_stash_effective_cwd")
    if [[ -n "$_stash_effective_gitdir" ]]; then
        # --git-dir / GIT_DIR override (repo#202). --git-dir names a .git
        # DIRECTORY, not a worktree path, so this resolves scope by querying
        # git directly through the override (--git-common-dir) instead of
        # cd-ing into it and running the -C-style rev-parse below — cd-ing
        # into a .git directory and asking for --show-toplevel is not the same
        # operation git performs when --git-dir/--work-tree are passed
        # explicitly. An explicit --work-tree/GIT_WORK_TREE (present in both
        # of #202's reproduction shapes) is exactly what git itself would use
        # for the toplevel side, so prefer it over asking git to guess one.
        if [[ -n "$_stash_effective_worktree" && -d "$_stash_effective_worktree" ]]; then
            _stash_toplevel=$(cd "$_stash_effective_worktree" 2>/dev/null && pwd -P) || _stash_toplevel=""
        elif [[ -e "$_stash_effective_gitdir" ]]; then
            _stash_toplevel=$(git "${_stash_gitdir_cd[@]}" --git-dir="$_stash_effective_gitdir" rev-parse --show-toplevel 2>/dev/null) || _stash_toplevel=""
            [[ -n "$_stash_toplevel" && -d "$_stash_toplevel" ]] && \
                _stash_toplevel=$(cd "$_stash_toplevel" 2>/dev/null && pwd -P) || _stash_toplevel=""
        fi

        if [[ -e "$_stash_effective_gitdir" ]]; then
            _stash_common=$(git "${_stash_gitdir_cd[@]}" --git-dir="$_stash_effective_gitdir" rev-parse --git-common-dir 2>/dev/null) || _stash_common=""
            if [[ -n "$_stash_common" ]]; then
                case "$_stash_common" in
                    /*) : ;;
                    *) _stash_common="$_stash_effective_gitdir/$_stash_common" ;;
                esac
                [[ -d "$_stash_common" ]] && \
                    _stash_common_parent=$(cd "$_stash_common/.." 2>/dev/null && pwd -P) || _stash_common_parent=""
            fi
        fi
    elif [[ -n "$_stash_effective_cwd" && -d "$_stash_effective_cwd" ]]; then
        _stash_toplevel=$(cd "$_stash_effective_cwd" 2>/dev/null && git rev-parse --show-toplevel 2>/dev/null) || _stash_toplevel=""
        [[ -n "$_stash_toplevel" && -d "$_stash_toplevel" ]] && \
            _stash_toplevel=$(cd "$_stash_toplevel" 2>/dev/null && pwd -P) || _stash_toplevel=""

        _stash_common=$(cd "$_stash_effective_cwd" 2>/dev/null && git rev-parse --git-common-dir 2>/dev/null) || _stash_common=""
        if [[ -n "$_stash_common" ]]; then
            _stash_common_parent=$(cd "$_stash_effective_cwd" 2>/dev/null && cd "$_stash_common/.." 2>/dev/null && pwd -P) || _stash_common_parent=""
        fi
    fi

    if [[ -n "$_stash_toplevel" && -n "$_stash_common_parent" && "$_stash_toplevel" == "$_stash_common_parent" ]]; then
        # MAIN CHECKOUT. Only the RECOVERY half is gated; a stash create here
        # stays allowed (the create-side deny below is linked-worktree only).
        if [[ "$_stash_is_recover" == true ]]; then
        ask "Command requires confirmation: $COMMAND (git stash pop/drop/clear in the MAIN checkout can destroy operator-preserved state — the main checkout's stash stack is operator-owned, not scratch space for an integration check. Run test-merges in an isolated worktree instead; set guards.stashScope:false / REPO_GUARD_STASH_SCOPE=0 to disable this ask)" "stash-scope:main-checkout"
        fi
    elif [[ -n "$_stash_toplevel" && -n "$_stash_common_parent" ]]; then
        # cwd is a linked worktree, not the main checkout. Count the repo's
        # linked worktrees as git reports them — a collision needs at least one
        # other active worktree to race with.
        if [[ -n "$_stash_effective_gitdir" ]]; then
            _stash_worktree_count=$(git "${_stash_gitdir_cd[@]}" --git-dir="$_stash_effective_gitdir" worktree list --porcelain 2>/dev/null | grep -c '^worktree ') || _stash_worktree_count=0
        else
            _stash_worktree_count=$(cd "$_stash_effective_cwd" 2>/dev/null && \
                git worktree list --porcelain 2>/dev/null | grep -c '^worktree ') || _stash_worktree_count=0
        fi
        [[ "$_stash_worktree_count" =~ ^[0-9]+$ ]] || _stash_worktree_count=0

        # >=3 entries = the main checkout plus two or more linked worktrees, so
        # some OTHER worktree exists besides this one to collide with.
        if [[ "$_stash_worktree_count" -ge 3 ]]; then
            # CREATE-SIDE REDIRECT (loom#5754), evaluated BEFORE the recovery
            # ask. Denies a raw stash CREATE only where a named, lossless
            # replacement provably exists: cwd is a linked worktree carrying
            # the `.loom-managed` sentinel, its directory name is `issue-<N>`,
            # and `<main>/.loom/scripts/worktree.sh` exists. Anywhere else (no
            # Loom tooling, solo worktree, main checkout) a create stays
            # ALLOWED: this never blocks a caller with no alternative.
            if [[ "$_stash_is_create" == true && -f "$_stash_toplevel/.loom-managed" \
                  && -f "$_stash_common_parent/.loom/scripts/worktree.sh" ]]; then
                _stash_wt_base="${_stash_toplevel##*/}"
                if [[ "$_stash_wt_base" =~ ^issue-([0-9]+)$ ]]; then
                    _stash_issue_num="${BASH_REMATCH[1]}"
                    deny "Blocked: $COMMAND (raw 'git stash' puts WIP on refs/stash — a SINGLE stack SHARED across every linked worktree of this repo, not per-worktree — where any of the $((_stash_worktree_count - 1)) currently-active linked worktrees can pop or drop it, and where the recovery step ('git stash pop') is itself gated. Nothing has been run: your working tree is untouched, so just rerun with the per-issue equivalent, which never touches refs/stash. Shelve WIP as a patch: './.loom/scripts/worktree.sh snapshot $_stash_issue_num'. Clean baseline vs. diff: './.loom/scripts/worktree.sh stash-push $_stash_issue_num' ... './.loom/scripts/worktree.sh stash-pop $_stash_issue_num'. To opt out set guards.stashScope:false in .claude/skills/repo/config.json, or export REPO_GUARD_STASH_SCOPE=0 in the agent's OWN environment before the session — an inline prefix does not reach this hook, which runs as a separate process)" "stash-scope:create-redirect"
                fi
            fi
        fi
        if [[ "$_stash_is_recover" == true && "$_stash_worktree_count" -ge 3 ]]; then
            ask "Command requires confirmation: $COMMAND (git stash pop/drop/clear from a linked worktree can destroy ANOTHER agent's WIP — refs/stash is a single stack SHARED across every linked worktree of this repo, not per-worktree, and $((_stash_worktree_count - 1)) linked worktrees are currently active. Use a per-worktree WIP ref instead of the shared stash stack; set guards.stashScope:false / REPO_GUARD_STASH_SCOPE=0 to disable this ask)" "stash-scope:worktree-collision"
        fi
    elif [[ "$_stash_is_recover" == true && ( "$_stash_effective_cwd" != "$CWD" || -n "$_stash_effective_gitdir" ) ]]; then
        # A `cd <dir>` prefix, or a --git-dir/GIT_DIR override, resolved to a
        # target that does not exist or is not inside any git checkout —
        # ambiguous. Fail toward asking rather than guessing (mirrors
        # parse_force_ops' detached-HEAD fail-safe).
        ask "Command requires confirmation: $COMMAND (the cd/--git-dir target for this stash operation could not be resolved to a git checkout, so scope cannot be determined — refusing to silently allow an ambiguous stash pop/drop/clear; set guards.stashScope:false / REPO_GUARD_STASH_SCOPE=0 to disable this ask)" "stash-scope:cd-unresolved"
    fi
fi

# =============================================================================
# CLOUD CLI ASK patterns — gated by the cloud CLI guard toggle
#
# Kept separate from ASK_PATTERNS so cloud-dev repos can opt out
# (guards.cloudCli:false / LOOM_GUARD_CLOUD=0). cloud_guard_enabled() is
# consulted only AFTER a cloud pattern matches, so the config read stays off the
# hot path for non-cloud commands (mirrors the SQL DDL block above).
#
# The aws entries are VERB-ANCHORED (case-sensitive ERE against the
# comment-stripped command): only mutating subcommands match, never read-only
# describe*/get*/list*/ls. So `aws ec2 describe-instances`, `aws s3 ls`, and
# `aws lambda list-functions` no longer prompt, while `run-instances`,
# `create-*`, `terminate-instances`, `stop-instances`, `lambda invoke`,
# `lambda publish*`, `sns publish`, etc. still ask.
#
# The docker entries already name only mutating verbs (rm/rmi/stop/kill/restart)
# and never match read-only `docker ps`/`docker logs`, so they are unchanged —
# they only move under this toggle.
# =============================================================================
CLOUD_ASK_PATTERNS=(
    # aws mutating subcommands (verb-anchored). The service list covers the
    # common infra-mutating namespaces; the verb list is the mutating vocabulary
    # (never describe*/get*/list*/ls). terminate lands here — an ask, not a deny.
    # invoke/publish are mutating (lambda invoke runs arbitrary code with side
    # effects; lambda publish-version / publish-layer-version and sns publish
    # mutate state) — there is no read-only `aws <svc> invoke|publish`, so they
    # cannot introduce describe/get/list false-positives. copy (ec2
    # copy-image/copy-snapshot) and assign (ec2 assign-*-addresses) are likewise
    # mutating-only. All were caught by the pre-#3593 bare `aws ec2|lambda`
    # prefixes and must stay asks (#3595).
    'aws (ec2|lambda|s3api|rds|iam|autoscaling|cloudformation|eks|ecs|elb|elbv2|route53|dynamodb|sns|sqs) (run|create|delete|terminate|stop|start|modify|update|put|reboot|authorize|revoke|attach|detach|associate|disassociate|register|deregister|enable|disable|add|remove|set|import|restore|reset|cancel|scale|invoke|publish|copy|assign)'
    # aws s3 (high-level) mutating verbs. `ls` is intentionally excluded. `mb`
    # (make-bucket) is mutating and was caught by the old bare `aws s3` prefix.
    'aws s3 (rm|rb|cp|mv|sync|mb)'

    # Docker operations (already mutating-verb only; does not match docker ps/logs)
    'docker rm'
    'docker rmi'
    'docker stop'
    'docker kill'
    'docker restart'
)

# Scanned against COMMAND_ASK_SCAN, not the raw COMMAND_NO_COMMENT (repo#188
# parity fix) — same reasoning as the SQL DDL and force-op blocks above, and
# the same copy the ungated ASK_PATTERNS loop already used. A cloud verb quoted
# inside an issue body is documentation, not an invocation.
for pattern in "${CLOUD_ASK_PATTERNS[@]}"; do
    if echo "$COMMAND_ASK_SCAN" | grep -qE "$pattern" && cloud_guard_enabled; then
        ask "Command requires confirmation: $COMMAND (set guards.cloudCli:false in .claude/skills/repo/config.json if this repo manages cloud infra as a first-class workflow)" "cloud-cli:$pattern"
    fi
done

# =============================================================================
# NOTE: This file is the CANONICAL generic repository-hygiene guard
# (rjwalters/repo#30). Loom-workflow-specific guards (the 'gh pr merge' →
# merge-pr.sh redirect, the 'pip install -e' worktree block) live in Loom's
# guard-loom-workflow.sh, registered as a separate PreToolUse/Bash hook that
# fires independently of this one — orchestration concerns stay with the
# orchestrator, generic protection lives here.
# =============================================================================

# =============================================================================
# ALLOW - Everything else passes through
# =============================================================================

exit 0
