#!/usr/bin/env bash
#
# flamegraph.sh - Generate CPU flamegraphs for VibeSQL benchmarks
#
# Usage:
#   ./scripts/flamegraph.sh tpch [QUERY]    # Profile TPC-H (optionally single query like Q6)
#   ./scripts/flamegraph.sh tpcc            # Profile TPC-C transactions
#   ./scripts/flamegraph.sh sysbench        # Profile Sysbench OLTP
#   ./scripts/flamegraph.sh select          # Profile point SELECT operations
#   ./scripts/flamegraph.sh custom <cmd>    # Profile custom command
#
# Requirements:
#   - cargo-flamegraph: cargo install flamegraph
#   - macOS: dtrace (built-in, requires sudo or disabled SIP)
#   - Linux: perf (install via package manager)
#
# Environment:
#   FLAMEGRAPH_OUTPUT  Output file (default: flamegraph-<benchmark>.svg)
#   FLAMEGRAPH_FREQ    Sampling frequency (default: 997 Hz)
#   QUERY_TIMEOUT_SECS Timeout per query (default: 30)
#   DRY_RUN            Set to 1 to show commands without executing
#
# Examples:
#   ./scripts/flamegraph.sh tpch            # Profile all TPC-H queries
#   ./scripts/flamegraph.sh tpch Q6         # Profile only Q6
#   FLAMEGRAPH_FREQ=99 ./scripts/flamegraph.sh tpch Q6  # Lower freq sampling
#   DRY_RUN=1 ./scripts/flamegraph.sh tpch  # Show what would be run
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
OUTPUT="${FLAMEGRAPH_OUTPUT:-flamegraph.svg}"
FREQ="${FLAMEGRAPH_FREQ:-997}"
TIMEOUT="${QUERY_TIMEOUT_SECS:-30}"
DRY_RUN="${DRY_RUN:-0}"

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

run_cmd() {
    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$*"
    else
        "$@"
    fi
}

check_dependencies() {
    if ! command -v cargo-flamegraph &> /dev/null && ! cargo flamegraph --version &> /dev/null; then
        error "cargo-flamegraph not found. Install with: cargo install flamegraph"
    fi

    # Skip sudo check in dry-run mode
    if [[ "$DRY_RUN" == "1" ]]; then
        info "Dry-run mode: skipping sudo/profiler checks"
        return
    fi

    # Check for profiling tools
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if ! command -v dtrace &> /dev/null; then
            error "dtrace not found (should be built into macOS)"
        fi

        # On macOS, flamegraph uses dtrace which requires root
        info "macOS detected - flamegraph will use dtrace (requires sudo)"

        # Check if we already have cached sudo credentials
        if ! sudo -n true 2>/dev/null; then
            warn "dtrace requires root. You will be prompted for your password."
            echo ""
            # Prompt for sudo password now so it's cached for the actual profiling
            if ! sudo -v; then
                error "Failed to obtain sudo credentials. Cannot run dtrace-based profiling."
            fi
            success "Sudo credentials cached"
        else
            info "Sudo credentials already available"
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if ! command -v perf &> /dev/null; then
            error "perf not found. Install with: sudo apt install linux-tools-common linux-tools-generic"
        fi

        # Check perf_event_paranoid setting
        local paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
        if [[ "$paranoid" != "unknown" && "$paranoid" -gt 1 ]]; then
            warn "perf_event_paranoid is $paranoid (restrictive)"
            warn "For best results, run: sudo sysctl kernel.perf_event_paranoid=1"
        fi
    fi
}

build_benchmark() {
    local bench_name="$1"
    local features="${2:-}"
    local package="${3:-vibesql-executor}"

    info "Building $bench_name benchmark..."

    local build_cmd="cargo build --profile bench --package $package --bench $bench_name"
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

    # First try the deps directory (where criterion benchmarks go)
    local binary=$(find "$PROJECT_ROOT/target/release/deps" -maxdepth 1 -name "${bench_name}-*" -type f -perm +111 ! -name "*.d" ! -name "*.o" 2>/dev/null | head -1)

    if [[ -z "$binary" ]]; then
        error "Could not find benchmark binary for $bench_name in target/release/deps/"
    fi

    info "Found binary: $(basename "$binary")"
    echo "$binary"
}

profile_tpch() {
    local query="${1:-}"
    local output_name="flamegraph-tpch"

    if [[ -n "$query" ]]; then
        # Convert to lowercase using tr (compatible with bash 3.x on macOS)
        local query_lower=$(echo "$query" | tr '[:upper:]' '[:lower:]')
        output_name="flamegraph-tpch-${query_lower}"
        info "Profiling TPC-H query: $query"
    else
        info "Profiling all TPC-H queries"
    fi

    build_benchmark "tpch_profiling" "benchmark-comparison"
    local binary=$(find_benchmark_binary "tpch_profiling")

    info "Running flamegraph (this may take a while)..."

    local env_vars="SCALE_FACTOR=0.01 PROFILING_ITERATIONS=3 QUERY_TIMEOUT_SECS=$TIMEOUT"
    if [[ -n "$query" ]]; then
        env_vars="QUERY_FILTER=$query $env_vars"
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$env_vars cargo flamegraph --root --freq $FREQ --output ${output_name}.svg -- $binary"
    else
        eval "$env_vars cargo flamegraph --root --freq $FREQ --output ${output_name}.svg -- $binary"
    fi

    if [[ "$DRY_RUN" != "1" ]]; then
        success "Flamegraph saved to: ${output_name}.svg"
        info "Open with: open ${output_name}.svg  (or view in browser)"
    fi
}

profile_tpcc() {
    local duration="${1:-30}"
    local warmup="${2:-5}"
    local scale="${3:-1}"

    info "Profiling TPC-C transactions (duration=${duration}s, warmup=${warmup}s, scale=${scale})"

    build_benchmark "tpcc_benchmark" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "tpcc_benchmark")

    info "Running flamegraph for TPC-C..."

    local env_vars="TPCC_DURATION_SECS=$duration TPCC_WARMUP_SECS=$warmup TPCC_SCALE_FACTOR=$scale"

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$env_vars cargo flamegraph --root --freq $FREQ --output flamegraph-tpcc.svg -- $binary"
    else
        eval "$env_vars cargo flamegraph --root --freq $FREQ --output flamegraph-tpcc.svg -- $binary"
    fi

    if [[ "$DRY_RUN" != "1" ]]; then
        success "Flamegraph saved to: flamegraph-tpcc.svg"
    fi
}

profile_sysbench() {
    local table_size="${1:-10000}"
    local duration="${2:-10}"
    local warmup="${3:-2}"

    info "Profiling Sysbench OLTP (table_size=$table_size, duration=${duration}s, warmup=${warmup}s)"

    # sysbench_benchmark is the standalone benchmark runner (not criterion-based)
    # It doesn't require benchmark-comparison feature
    build_benchmark "sysbench_benchmark" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "sysbench_benchmark")

    info "Running flamegraph for Sysbench..."

    local env_vars="SYSBENCH_TABLE_SIZE=$table_size SYSBENCH_DURATION_SECS=$duration SYSBENCH_WARMUP_SECS=$warmup"

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "$env_vars cargo flamegraph --root --freq $FREQ --output flamegraph-sysbench.svg -- $binary"
    else
        eval "$env_vars cargo flamegraph --root --freq $FREQ --output flamegraph-sysbench.svg -- $binary"
    fi

    if [[ "$DRY_RUN" != "1" ]]; then
        success "Flamegraph saved to: flamegraph-sysbench.svg"
    fi
}

profile_select() {
    info "Profiling point SELECT operations"

    build_benchmark "iterator_execution" "" "vibesql-executor"
    local binary=$(find_benchmark_binary "iterator_execution")

    info "Running flamegraph for SELECT operations..."

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "cargo flamegraph --root --freq $FREQ --output flamegraph-select.svg -- $binary"
    else
        cargo flamegraph \
            --root \
            --freq "$FREQ" \
            --output "flamegraph-select.svg" \
            -- "$binary"
    fi

    if [[ "$DRY_RUN" != "1" ]]; then
        success "Flamegraph saved to: flamegraph-select.svg"
    fi
}

profile_custom() {
    shift  # Remove 'custom' argument
    local cmd_to_run="$@"

    if [[ -z "$cmd_to_run" ]]; then
        error "Usage: $0 custom <command>"
    fi

    info "Profiling custom command: $cmd_to_run"

    if [[ "$DRY_RUN" == "1" ]]; then
        cmd "cargo flamegraph --root --freq $FREQ --output flamegraph-custom.svg -- $cmd_to_run"
    else
        cargo flamegraph \
            --root \
            --freq "$FREQ" \
            --output "flamegraph-custom.svg" \
            -- $cmd_to_run
    fi

    if [[ "$DRY_RUN" != "1" ]]; then
        success "Flamegraph saved to: flamegraph-custom.svg"
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
  FLAMEGRAPH_OUTPUT      Output file (default: flamegraph-<benchmark>.svg)
  FLAMEGRAPH_FREQ        Sampling frequency in Hz (default: 997)
  QUERY_TIMEOUT_SECS     Timeout per query in seconds (default: 30)
  DRY_RUN                Set to 1 to show commands without executing

Requirements:
  - cargo-flamegraph: cargo install flamegraph
  - macOS: dtrace (built-in, requires sudo)
  - Linux: perf (linux-tools-common package)

Examples:
  $0 tpch                    # Profile all 22 TPC-H queries
  $0 tpch Q6                 # Profile only Q6
  $0 tpcc 60 10 2            # TPC-C: 60s duration, 10s warmup, scale=2
  $0 sysbench 50000 20 5     # Sysbench: 50k rows, 20s duration, 5s warmup
  $0 custom ./my-benchmark   # Profile custom binary
  DRY_RUN=1 $0 tpch Q6       # Show commands without running

Output:
  Flamegraphs are saved as SVG files in the current directory.
  Open with: open flamegraph-tpch-q6.svg (macOS)
             xdg-open flamegraph-tpch-q6.svg (Linux)
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
