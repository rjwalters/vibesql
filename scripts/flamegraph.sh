#!/bin/bash
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
#   - macOS: dtrace (built-in, may need sudo)
#   - Linux: perf (install via package manager)
#
# Environment:
#   FLAMEGRAPH_OUTPUT  Output file (default: flamegraph.svg)
#   FLAMEGRAPH_FREQ    Sampling frequency (default: 997 Hz)
#   QUERY_TIMEOUT_SECS Timeout per query (default: 30)
#
# Examples:
#   ./scripts/flamegraph.sh tpch            # Profile all TPC-H queries
#   ./scripts/flamegraph.sh tpch Q6         # Profile only Q6
#   FLAMEGRAPH_FREQ=99 ./scripts/flamegraph.sh tpch Q6  # Lower freq sampling
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
OUTPUT="${FLAMEGRAPH_OUTPUT:-flamegraph.svg}"
FREQ="${FLAMEGRAPH_FREQ:-997}"
TIMEOUT="${QUERY_TIMEOUT_SECS:-30}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }

check_dependencies() {
    if ! command -v cargo-flamegraph &> /dev/null && ! command -v flamegraph &> /dev/null; then
        error "cargo-flamegraph not found. Install with: cargo install flamegraph"
    fi

    # Check for profiling tools
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if ! command -v dtrace &> /dev/null; then
            error "dtrace not found (should be built into macOS)"
        fi
        # Check if we can use dtrace (may need to disable SIP or use sudo)
        if ! sudo -n dtrace -l 2>/dev/null | head -1 &>/dev/null; then
            warn "dtrace may require sudo. You might be prompted for password."
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if ! command -v perf &> /dev/null; then
            error "perf not found. Install with: sudo apt install linux-tools-common linux-tools-generic"
        fi
    fi
}

build_benchmark() {
    local bench_name="$1"
    local features="${2:-benchmark-comparison}"

    info "Building $bench_name benchmark with features: $features"
    cargo build --release --package vibesql-executor --bench "$bench_name" --features "$features"
}

find_benchmark_binary() {
    local bench_name="$1"
    local binary=$(find "$PROJECT_ROOT/target/release/deps" -maxdepth 1 -name "${bench_name}-*" -type f ! -name "*.d" | head -1)

    if [[ -z "$binary" ]]; then
        error "Could not find benchmark binary for $bench_name"
    fi
    echo "$binary"
}

profile_tpch() {
    local query="${1:-}"
    local output_name="flamegraph-tpch"

    if [[ -n "$query" ]]; then
        output_name="flamegraph-tpch-${query,,}"
        info "Profiling TPC-H query: $query"
    else
        info "Profiling all TPC-H queries"
    fi

    build_benchmark "tpch_profiling"
    local binary=$(find_benchmark_binary "tpch_profiling")

    info "Running flamegraph (this may take a while)..."
    local args=""
    if [[ -n "$query" ]]; then
        args="$query"
    fi

    QUERY_TIMEOUT_SECS="$TIMEOUT" cargo flamegraph \
        --root \
        --freq "$FREQ" \
        --output "${output_name}.svg" \
        -- "$binary" $args

    success "Flamegraph saved to: ${output_name}.svg"
}

profile_tpcc() {
    info "Profiling TPC-C transactions"

    build_benchmark "tpcc_benchmark"
    local binary=$(find_benchmark_binary "tpcc_benchmark")

    info "Running flamegraph for TPC-C..."
    TPCC_DURATION_SECS=30 TPCC_WARMUP_SECS=5 TPCC_SCALE_FACTOR=1 \
    cargo flamegraph \
        --root \
        --freq "$FREQ" \
        --output "flamegraph-tpcc.svg" \
        -- "$binary"

    success "Flamegraph saved to: flamegraph-tpcc.svg"
}

profile_sysbench() {
    info "Profiling Sysbench OLTP"

    build_benchmark "sysbench_oltp"
    local binary=$(find_benchmark_binary "sysbench_oltp")

    info "Running flamegraph for Sysbench..."
    cargo flamegraph \
        --root \
        --freq "$FREQ" \
        --output "flamegraph-sysbench.svg" \
        -- "$binary"

    success "Flamegraph saved to: flamegraph-sysbench.svg"
}

profile_select() {
    info "Profiling point SELECT operations"

    build_benchmark "iterator_execution" ""
    local binary=$(find_benchmark_binary "iterator_execution")

    info "Running flamegraph for SELECT operations..."
    cargo flamegraph \
        --root \
        --freq "$FREQ" \
        --output "flamegraph-select.svg" \
        -- "$binary"

    success "Flamegraph saved to: flamegraph-select.svg"
}

profile_custom() {
    shift  # Remove 'custom' argument
    local cmd="$@"

    if [[ -z "$cmd" ]]; then
        error "Usage: $0 custom <command>"
    fi

    info "Profiling custom command: $cmd"

    cargo flamegraph \
        --root \
        --freq "$FREQ" \
        --output "flamegraph-custom.svg" \
        -- $cmd

    success "Flamegraph saved to: flamegraph-custom.svg"
}

usage() {
    echo "Usage: $0 <benchmark> [options]"
    echo ""
    echo "Benchmarks:"
    echo "  tpch [QUERY]    Profile TPC-H queries (optionally single query like Q6)"
    echo "  tpcc            Profile TPC-C transactions"
    echo "  sysbench        Profile Sysbench OLTP"
    echo "  select          Profile point SELECT operations"
    echo "  custom <cmd>    Profile custom command"
    echo ""
    echo "Environment variables:"
    echo "  FLAMEGRAPH_OUTPUT   Output file (default: flamegraph-<benchmark>.svg)"
    echo "  FLAMEGRAPH_FREQ     Sampling frequency in Hz (default: 997)"
    echo "  QUERY_TIMEOUT_SECS  Timeout per query in seconds (default: 30)"
    echo ""
    echo "Examples:"
    echo "  $0 tpch           # Profile all 22 TPC-H queries"
    echo "  $0 tpch Q6        # Profile only Q6"
    echo "  $0 tpcc           # Profile TPC-C workload"
    echo "  $0 custom ./my-benchmark  # Profile custom binary"
    exit 1
}

# Main
cd "$PROJECT_ROOT"

if [[ $# -lt 1 ]]; then
    usage
fi

check_dependencies

case "$1" in
    tpch)
        profile_tpch "$2"
        ;;
    tpcc)
        profile_tpcc
        ;;
    sysbench)
        profile_sysbench
        ;;
    select)
        profile_select
        ;;
    custom)
        profile_custom "$@"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        error "Unknown benchmark: $1"
        ;;
esac
