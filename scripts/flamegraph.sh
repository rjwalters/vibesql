#!/usr/bin/env bash
#
# flamegraph.sh - Generate CPU profiles for VibeSQL benchmarks using samply
#
# Usage:
#   ./scripts/flamegraph.sh tpch [QUERY]    # Profile TPC-H (optionally single query like Q6)
#   ./scripts/flamegraph.sh tpcc            # Profile TPC-C transactions
#   ./scripts/flamegraph.sh sysbench        # Profile Sysbench OLTP
#   ./scripts/flamegraph.sh select          # Profile point SELECT operations
#   ./scripts/flamegraph.sh custom <cmd>    # Profile custom command
#
# Requirements:
#   - samply: cargo install samply (no sudo required)
#
# Environment:
#   PROFILE_FREQ       Sampling frequency in Hz (default: 1000)
#   QUERY_TIMEOUT_SECS Timeout per query (default: 30)
#   SAVE_ONLY          Set to 1 to save profile to file, 0 to open browser
#                      (Auto-detected: file output in non-TTY, browser in TTY)
#   DRY_RUN            Set to 1 to show commands without executing
#
# Examples:
#   ./scripts/flamegraph.sh tpch            # Profile all TPC-H queries
#   ./scripts/flamegraph.sh tpch Q6         # Profile only Q6
#   SAVE_ONLY=1 ./scripts/flamegraph.sh tpch Q6  # Save profile.json without UI
#   DRY_RUN=1 ./scripts/flamegraph.sh tpch  # Show what would be run
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
FREQ="${PROFILE_FREQ:-1000}"
TIMEOUT="${QUERY_TIMEOUT_SECS:-30}"
DRY_RUN="${DRY_RUN:-0}"

# Auto-detect SAVE_ONLY: default to file output when not in a TTY (agent mode)
# Use SAVE_ONLY=0 to force browser output, SAVE_ONLY=1 to force file output
if [[ -z "${SAVE_ONLY:-}" ]]; then
    if [[ -t 1 ]]; then
        SAVE_ONLY="0"  # Interactive terminal: open browser
    else
        SAVE_ONLY="1"  # Non-interactive (agent): save to file
    fi
else
    SAVE_ONLY="${SAVE_ONLY}"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
cmd() { echo -e "${CYAN}[CMD]${NC} $1"; }

check_dependencies() {
    # Skip checks in dry-run mode
    if [[ "$DRY_RUN" == "1" ]]; then
        info "Dry-run mode: skipping dependency checks"
        return
    fi

    if ! command -v samply &> /dev/null; then
        error "samply not found. Install with: cargo install samply"
    fi
    info "Using samply profiler (no sudo required)"
}

build_benchmark() {
    local bench_name="$1"
    local features="${2:-}"
    local package="${3:-vibesql-executor}"

    info "Building $bench_name benchmark..."

    local build_cmd="cargo build --profile profiling --package $package --bench $bench_name"
    if [[ -n "$features" ]]; then
        build_cmd="$build_cmd --features $features"
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$build_cmd"
    else
        eval "$build_cmd"
    fi
}

find_benchmark_binary() {
    local bench_name="$1"

    # In dry-run mode, return a placeholder
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "<${bench_name}-binary>"
        return
    fi

    local search_dir="$PROJECT_ROOT/target/profiling/deps"
    local binary=$(find "$search_dir" -maxdepth 1 -name "${bench_name}-*" -type f -perm +111 ! -name "*.d" ! -name "*.o" 2>/dev/null | head -1)

    if [[ -z "$binary" ]]; then
        error "Could not find benchmark binary for $bench_name in $search_dir/"
    fi

    info "Found binary: $(basename "$binary")"
    echo "$binary"
}

run_profiler() {
    local binary="$1"
    local output_name="$2"
    local env_vars="$3"

    local samply_args="--rate $FREQ"
    if [[ "$SAVE_ONLY" == "1" ]]; then
        samply_args="$samply_args --save-only -o ${output_name}.json.gz"
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$env_vars samply record $samply_args -- $binary"
    else
        info "Starting samply profiler..."
        if [[ "$SAVE_ONLY" != "1" ]]; then
            info "Profile will open in Firefox Profiler (profiler.firefox.com)"
        fi
        eval "$env_vars samply record $samply_args -- $binary"
    fi

    if [[ "$DRY_RUN" != "1" && "$SAVE_ONLY" == "1" ]]; then
        success "Profile saved to: ${output_name}.json.gz"
        info "Load later with: samply load ${output_name}.json.gz"
    fi
}

profile_tpch() {
    local query="${1:-}"
    local output_name="profile-tpch"

    if [[ -n "$query" ]]; then
        local query_lower=$(echo "$query" | tr '[:upper:]' '[:lower:]')
        output_name="profile-tpch-${query_lower}"
        info "Profiling TPC-H query: $query"
    else
        info "Profiling all TPC-H queries"
    fi

    build_benchmark "tpch_profiling" "benchmark-comparison"
    local binary=$(find_benchmark_binary "tpch_profiling")

    local env_vars="SCALE_FACTOR=0.01 PROFILING_ITERATIONS=3 QUERY_TIMEOUT_SECS=$TIMEOUT"
    if [[ -n "$query" ]]; then
        env_vars="QUERY_FILTER=$query $env_vars"
    fi

    run_profiler "$binary" "$output_name" "$env_vars"
}

profile_tpcc() {
    local duration="${1:-30}"
    local warmup="${2:-5}"
    local scale="${3:-1}"

    info "Profiling TPC-C transactions (duration=${duration}s, warmup=${warmup}s, scale=${scale})"

    build_benchmark "tpcc_benchmark" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "tpcc_benchmark")

    local env_vars="TPCC_DURATION_SECS=$duration TPCC_WARMUP_SECS=$warmup TPCC_SCALE_FACTOR=$scale"

    run_profiler "$binary" "profile-tpcc" "$env_vars"
}

profile_sysbench() {
    local table_size="${1:-10000}"
    local duration="${2:-10}"
    local warmup="${3:-2}"

    info "Profiling Sysbench OLTP (table_size=$table_size, duration=${duration}s, warmup=${warmup}s)"

    build_benchmark "sysbench_benchmark" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "sysbench_benchmark")

    local env_vars="SYSBENCH_TABLE_SIZE=$table_size SYSBENCH_DURATION_SECS=$duration SYSBENCH_WARMUP_SECS=$warmup"

    run_profiler "$binary" "profile-sysbench" "$env_vars"
}

profile_select() {
    info "Profiling point SELECT operations"

    build_benchmark "iterator_execution" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "iterator_execution")

    run_profiler "$binary" "profile-select" ""
}

profile_custom() {
    shift  # Remove 'custom' argument
    local cmd_to_run="$@"

    if [[ -z "$cmd_to_run" ]]; then
        error "Usage: $0 custom <command>"
    fi

    info "Profiling custom command: $cmd_to_run"

    local samply_args="--rate $FREQ"
    if [[ "$SAVE_ONLY" == "1" ]]; then
        samply_args="$samply_args --save-only -o profile-custom.json.gz"
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "samply record $samply_args -- $cmd_to_run"
    else
        samply record $samply_args -- $cmd_to_run
    fi

    if [[ "$DRY_RUN" != "1" && "$SAVE_ONLY" == "1" ]]; then
        success "Profile saved to: profile-custom.json.gz"
    fi
}

usage() {
    cat <<EOF
Usage: $0 <benchmark> [options]

Benchmarks:
  tpch [QUERY]           Profile TPC-H queries (optionally single query like Q6)
  tpcc [DURATION] [WARMUP] [SCALE]
                         Profile TPC-C transactions
                         Defaults: 30s duration, 5s warmup, scale=1
  sysbench [SIZE] [DURATION] [WARMUP]
                         Profile Sysbench OLTP
                         Defaults: 10000 rows, 10s duration, 2s warmup
  select                 Profile point SELECT operations
  custom <cmd>           Profile custom command

Options:
  -h, --help, help       Show this help message

Environment variables:
  PROFILE_FREQ           Sampling frequency in Hz (default: 1000)
  QUERY_TIMEOUT_SECS     Timeout per query in seconds (default: 30)
  SAVE_ONLY              Set to 1 for file output, 0 for browser UI
                         (Auto-detects: file in non-TTY, browser in TTY)
  DRY_RUN                Set to 1 to show commands without executing

Requirements:
  samply                 Install: cargo install samply
                         No sudo required on macOS or Linux

Examples:
  $0 tpch                       # Profile all TPC-H queries
  $0 tpch Q6                    # Profile only Q6
  $0 tpcc 60 10 2               # TPC-C: 60s duration, 10s warmup, scale=2
  $0 sysbench 50000 20 5        # Sysbench: 50k rows, 20s duration, 5s warmup
  SAVE_ONLY=1 $0 tpch Q6        # Save profile.json without opening UI
  DRY_RUN=1 $0 tpch Q6          # Show commands without running

Output:
  Interactive (TTY): Opens Firefox Profiler in your browser (profiler.firefox.com)
  Non-interactive:   Saves to .json.gz file for later viewing with 'samply load'
  Override with SAVE_ONLY=0 (browser) or SAVE_ONLY=1 (file)
EOF
    exit 1
}

# Main
cd "$PROJECT_ROOT"

if [[ $# -lt 1 ]]; then
    usage
fi

# Handle help first, before dependency check
case "$1" in
    -h|--help|help)
        usage
        ;;
esac

check_dependencies

case "$1" in
    tpch)
        profile_tpch "$2"
        ;;
    tpcc)
        profile_tpcc "${2:-30}" "${3:-5}" "${4:-1}"
        ;;
    sysbench)
        profile_sysbench "${2:-10000}" "${3:-10}" "${4:-2}"
        ;;
    select)
        profile_select
        ;;
    custom)
        profile_custom "$@"
        ;;
    *)
        error "Unknown benchmark: $1. Use '$0 --help' for usage."
        ;;
esac
