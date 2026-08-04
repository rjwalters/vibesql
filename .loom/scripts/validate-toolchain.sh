#!/bin/bash
# validate-toolchain.sh - Validate the Loom command toolchain is available
#
# Validates that the commands Tier 2 dispatch (/loom:sweep / loom-daemon) needs
# to drive worker roles are present before it starts. Provides tiered validation
# with critical vs optional commands.
#
# Every command it checks is now a native `loom-daemon` subcommand: the Python
# `loom-tools` package this script was originally written against was retired in
# epic #4081 Phase 4 (#4557), so a missing command means "build/provision the
# daemon binary", never "pip install something".
#
# Exit codes:
#   0 - All critical commands available (optional warnings may exist)
#   1 - Critical commands missing (dispatch cannot start)
#   2 - Invalid arguments
#
# Usage:
#   validate-toolchain.sh           # Validate all commands
#   validate-toolchain.sh --quick   # Only validate critical commands
#   validate-toolchain.sh --json    # JSON output for automation
#   validate-toolchain.sh --help    # Show help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/locate-daemon-bin.sh
source "$SCRIPT_DIR/lib/locate-daemon-bin.sh"

# Critical commands - /loom:sweep / loom-daemon dispatch cannot function without these
# Issue #4272 (epic #4081 Phase 3 family 2): `loom-cleanup` and
# `loom-recover-orphans` are now native `loom-daemon` subcommands
# (`cleanup logs` / `recover-orphans`) — see `daemon_subcommand_available`
# below. `loom-recover-orphans` replaces the historical `loom-orphan-recovery`
# entry, which never existed as a console script name (pyproject always
# shipped `loom-recover-orphans`; the old entry only ever validated via a
# bare Python-module-import fallback).
CRITICAL_COMMANDS=(
    "loom-cleanup"
    "loom-recover-orphans"
)

# Optional commands - degraded functionality without these.
# Currently empty: `loom-agent-wait` / `loom-agent-spawn` were the last two
# entries and were removed in epic #4081 Phase 3 family 4 (#4415) when
# agent_wait.py / agent_spawn.py were ported to native `loom-daemon
# agent-wait` / `agent-spawn`. The array is kept (rather than deleted) so the
# tiering stays in place for future optional console scripts; every loop over
# it is length-guarded because bash 3.2 + `set -u` errors on "${EMPTY[@]}".
OPTIONAL_COMMANDS=()

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Output format
JSON_OUTPUT=false
QUICK_MODE=false

show_help() {
    cat << 'EOF'
validate-toolchain.sh - Validate the Loom command toolchain

USAGE:
    validate-toolchain.sh [OPTIONS]

OPTIONS:
    --quick     Only validate critical commands (faster)
    --json      Output results as JSON
    --help      Show this help message

CRITICAL COMMANDS (required):
    loom-cleanup          - Log archival and lock-dir cleanup
    loom-recover-orphans  - Recover orphaned tasks after a sweep crash

OPTIONAL COMMANDS (degraded without):
    (none) - loom-agent-wait / loom-agent-spawn are now native
             `loom-daemon agent-wait` / `agent-spawn` subcommands (#4415)

INSTALLATION:
    These are native `loom-daemon` subcommands (`cleanup logs` /
    `recover-orphans`). If they are reported missing, the loom-daemon binary is
    absent or predates them — build and provision a fresh one:

    # From the repository root:
    ./.loom/scripts/cli/loom-daemon-update.sh

    # Or by hand:
    cargo build --release -p loom-daemon
    ./scripts/install/provision-daemon.sh

    # Verify:
    loom-daemon --version
    loom-daemon cleanup logs --help

    (There is no pip install. The Python `loom-tools` package these commands
    once came from was retired in epic #4081 Phase 4, #4557 — see
    docs/adr/0013-loom-tools-python-retirement.md.)

EXIT CODES:
    0 - All critical commands available
    1 - Critical commands missing
    2 - Invalid arguments

EXAMPLES:
    # Full validation
    validate-toolchain.sh

    # Quick check (critical only)
    validate-toolchain.sh --quick

    # JSON output for automation
    validate-toolchain.sh --json
EOF
}

# Native `loom-daemon` capability check (issue #4272): `loom-cleanup` and
# `loom-recover-orphans` are ported to `loom-daemon cleanup logs` /
# `loom-daemon recover-orphans`. Resolve the daemon binary once and probe its
# `--help` for the relevant subcommand — cheap, side-effect-free, and detects
# a stale pre-#4272 binary the same way `probe-tokens.sh` does for `tokens`.
_VALIDATE_TOOLCHAIN_REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
_VALIDATE_TOOLCHAIN_DAEMON_BIN="$(loom_locate_daemon_bin "$_VALIDATE_TOOLCHAIN_REPO_ROOT")"

daemon_subcommand_available() {
    local cmd="$1"
    [[ -n "$_VALIDATE_TOOLCHAIN_DAEMON_BIN" ]] || return 1
    case "$cmd" in
        loom-cleanup) "$_VALIDATE_TOOLCHAIN_DAEMON_BIN" cleanup logs --help >/dev/null 2>&1 ;;
        loom-recover-orphans) "$_VALIDATE_TOOLCHAIN_DAEMON_BIN" recover-orphans --help >/dev/null 2>&1 ;;
        *) return 1 ;;
    esac
}

# Check if a command exists
command_exists() {
    local cmd="$1"

    # First try: native loom-daemon subcommand (issue #4272).
    if daemon_subcommand_available "$cmd"; then
        return 0
    fi

    # Second try: check if command is in PATH (installed console script or
    # PATH shim next to loom-daemon).
    if command -v "$cmd" >/dev/null 2>&1; then
        return 0
    fi

    # There is NO third tier. A Python-module-import fallback (`python3 -c
    # "import <module>"`) used to sit here, but its command→module map had
    # already emptied out — `loom-cleanup`/`loom-recover-orphans` went native in
    # #4272, `loom-agent-wait`/`loom-agent-spawn` in #4415 — making it
    # unreachable dead code. Epic #4081 Phase 4 (#4557) deleted the Python
    # package outright, so it can never be reachable again; it was removed
    # rather than left as a misleading "extension point".
    return 1
}

# Main validation
main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --quick)
                QUICK_MODE=true
                shift
                ;;
            --json)
                JSON_OUTPUT=true
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1" >&2
                echo "Run 'validate-toolchain.sh --help' for usage" >&2
                exit 2
                ;;
        esac
    done

    local start_time
    start_time=$(date +%s%N 2>/dev/null || date +%s)

    local critical_missing=()
    local critical_found=()
    local optional_missing=()
    local optional_found=()

    # Validate critical commands
    for cmd in "${CRITICAL_COMMANDS[@]}"; do
        if command_exists "$cmd"; then
            critical_found+=("$cmd")
        else
            critical_missing+=("$cmd")
        fi
    done

    # Validate optional commands (unless quick mode).
    # The length guard is required: OPTIONAL_COMMANDS is currently empty
    # (#4415) and bash 3.2 under `set -u` treats "${EMPTY[@]}" as unbound.
    if [[ "$QUICK_MODE" != "true" && ${#OPTIONAL_COMMANDS[@]} -gt 0 ]]; then
        for cmd in "${OPTIONAL_COMMANDS[@]}"; do
            if command_exists "$cmd"; then
                optional_found+=("$cmd")
            else
                optional_missing+=("$cmd")
            fi
        done
    fi

    local end_time
    end_time=$(date +%s%N 2>/dev/null || date +%s)

    # Calculate duration (handle both nanosecond and second precision)
    local duration_ms
    if [[ "$start_time" =~ ^[0-9]{10,}$ ]]; then
        # Nanosecond precision available
        duration_ms=$(( (end_time - start_time) / 1000000 ))
    else
        # Only second precision
        duration_ms=$(( (end_time - start_time) * 1000 ))
    fi

    # Determine overall status
    local status="ok"
    local exit_code=0
    if [[ ${#critical_missing[@]} -gt 0 ]]; then
        status="critical"
        exit_code=1
    elif [[ ${#optional_missing[@]} -gt 0 ]]; then
        status="degraded"
    fi

    # Output results - handle empty arrays carefully
    local cf_str="" cm_str="" of_str="" om_str=""
    [[ ${#critical_found[@]} -gt 0 ]] && cf_str="${critical_found[*]}"
    [[ ${#critical_missing[@]} -gt 0 ]] && cm_str="${critical_missing[*]}"
    [[ ${#optional_found[@]} -gt 0 ]] && of_str="${optional_found[*]}"
    [[ ${#optional_missing[@]} -gt 0 ]] && om_str="${optional_missing[*]}"

    if [[ "$JSON_OUTPUT" == "true" ]]; then
        local cf_json="[]" cm_json="[]" of_json="[]" om_json="[]"
        [[ ${#critical_found[@]} -gt 0 ]] && cf_json="$(printf '%s\n' "${critical_found[@]}" | jq -R . | jq -s .)"
        [[ ${#critical_missing[@]} -gt 0 ]] && cm_json="$(printf '%s\n' "${critical_missing[@]}" | jq -R . | jq -s .)"
        [[ ${#optional_found[@]} -gt 0 ]] && of_json="$(printf '%s\n' "${optional_found[@]}" | jq -R . | jq -s .)"
        [[ ${#optional_missing[@]} -gt 0 ]] && om_json="$(printf '%s\n' "${optional_missing[@]}" | jq -R . | jq -s .)"
        output_json "$status" "$duration_ms" "$cf_json" "$cm_json" "$of_json" "$om_json"
    else
        output_text "$status" "$duration_ms" "$cf_str" "$cm_str" "$of_str" "$om_str"
    fi

    exit "$exit_code"
}

output_json() {
    local j_status="$1"
    local j_duration_ms="$2"
    local cf_json="$3"
    local cm_json="$4"
    local of_json="$5"
    local om_json="$6"

    # Handle empty arrays
    [[ -z "$cf_json" || "$cf_json" == "[]" ]] && cf_json="[]"
    [[ -z "$cm_json" || "$cm_json" == "[]" ]] && cm_json="[]"
    [[ -z "$of_json" || "$of_json" == "[]" ]] && of_json="[]"
    [[ -z "$om_json" || "$om_json" == "[]" ]] && om_json="[]"

    cat << EOF
{
  "status": "$j_status",
  "duration_ms": $j_duration_ms,
  "critical": {
    "found": $cf_json,
    "missing": $cm_json
  },
  "optional": {
    "found": $of_json,
    "missing": $om_json
  }
}
EOF
}

output_text() {
    local t_status="$1"
    local t_duration_ms="$2"
    local cf_str="$3"
    local cm_str="$4"
    local of_str="$5"
    local om_str="$6"

    echo "Loom Toolchain Validation"
    echo "========================="
    echo ""

    # Critical commands
    echo "Critical commands:"
    if [[ -n "$cf_str" ]]; then
        for cmd in $cf_str; do
            echo -e "  ${GREEN}✓${NC} $cmd"
        done
    fi
    if [[ -n "$cm_str" ]]; then
        for cmd in $cm_str; do
            echo -e "  ${RED}✗${NC} $cmd (MISSING)"
        done
    fi
    echo ""

    # Optional commands (if checked)
    if [[ "$QUICK_MODE" != "true" ]]; then
        echo "Optional commands:"
        if [[ -n "$of_str" ]]; then
            for cmd in $of_str; do
                echo -e "  ${GREEN}✓${NC} $cmd"
            done
        fi
        if [[ -n "$om_str" ]]; then
            for cmd in $om_str; do
                echo -e "  ${YELLOW}○${NC} $cmd (optional, degraded functionality)"
            done
        fi
        echo ""
    fi

    # Summary
    echo "---"
    echo "Validation completed in ${t_duration_ms}ms"

    case "$t_status" in
        ok)
            echo -e "${GREEN}Status: OK${NC} - All commands available"
            ;;
        degraded)
            echo -e "${YELLOW}Status: DEGRADED${NC} - Optional commands missing"
            echo ""
            echo "Dispatch will continue with degraded functionality."
            echo "Some features (stuck detection, health monitoring) may not work."
            ;;
        critical)
            echo -e "${RED}Status: CRITICAL${NC} - Essential commands missing"
            echo ""
            echo "Dispatch cannot start without these commands."
            echo ""
            echo "These are native 'loom-daemon' subcommands — build/provision"
            echo "a fresh binary (there is no pip install; #4557):"
            echo "  ./.loom/scripts/cli/loom-daemon-update.sh"
            echo ""
            echo "Or by hand:"
            echo "  cargo build --release -p loom-daemon"
            echo "  ./scripts/install/provision-daemon.sh"
            ;;
    esac
}

main "$@"
