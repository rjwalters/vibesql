#!/bin/bash
#
# profile-query.sh - Profile a specific SQL query with detailed timing breakdown
#
# Usage:
#   ./scripts/profile-query.sh <query_file.sql>
#   ./scripts/profile-query.sh --tpch Q6
#   ./scripts/profile-query.sh --inline "SELECT * FROM lineitem LIMIT 10"
#
# This script:
#   1. Runs the query multiple times for warm-up
#   2. Collects timing breakdown (parse, plan, optimize, execute)
#   3. Shows operation-level breakdown where available
#
# Environment:
#   PROFILE_ITERATIONS  Number of iterations (default: 5)
#   PROFILE_WARMUP      Warmup iterations (default: 2)
#   QUERY_TIMEOUT_SECS  Timeout per query (default: 30)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
ITERATIONS="${PROFILE_ITERATIONS:-5}"
WARMUP="${PROFILE_WARMUP:-2}"
TIMEOUT="${QUERY_TIMEOUT_SECS:-30}"

# Colors
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }

profile_tpch_query() {
    local query="$1"

    info "Building TPC-H profiling benchmark..."
    cargo build --release --package vibesql-executor --bench tpch_profiling --features benchmark-comparison 2>/dev/null

    local binary=$(find "$PROJECT_ROOT/target/release/deps" -maxdepth 1 -name "tpch_profiling-*" -type f ! -name "*.d" ! -name "*.o" | head -1)

    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  Profiling TPC-H Query: $query"
    echo "  Iterations: $WARMUP warmup + $ITERATIONS measured"
    echo "  Timeout: ${TIMEOUT}s per execution"
    echo "═══════════════════════════════════════════════════════════════════════"
    echo ""

    # Run warmup iterations
    info "Running $WARMUP warmup iterations..."
    for i in $(seq 1 $WARMUP); do
        QUERY_TIMEOUT_SECS="$TIMEOUT" "$binary" "$query" 2>&1 | grep -E "^\s+(Parse|Executor|Execute|TOTAL):" || true
        echo "  Warmup $i complete"
    done

    echo ""
    info "Running $ITERATIONS measured iterations..."
    echo ""

    # Run measured iterations and collect timing
    for i in $(seq 1 $ITERATIONS); do
        echo "--- Iteration $i ---"
        QUERY_TIMEOUT_SECS="$TIMEOUT" "$binary" "$query" 2>&1
        echo ""
    done

    success "Profiling complete for $query"
}

usage() {
    echo "Usage: $0 <options>"
    echo ""
    echo "Options:"
    echo "  --tpch <QUERY>     Profile TPC-H query (Q1-Q22)"
    echo "  --help             Show this help"
    echo ""
    echo "Environment variables:"
    echo "  PROFILE_ITERATIONS  Number of measured iterations (default: 5)"
    echo "  PROFILE_WARMUP      Warmup iterations (default: 2)"
    echo "  QUERY_TIMEOUT_SECS  Timeout per query (default: 30)"
    echo ""
    echo "Examples:"
    echo "  $0 --tpch Q6"
    echo "  PROFILE_ITERATIONS=10 $0 --tpch Q1"
    exit 1
}

# Main
cd "$PROJECT_ROOT"

if [[ $# -lt 1 ]]; then
    usage
fi

case "$1" in
    --tpch)
        if [[ -z "$2" ]]; then
            echo "Error: Missing query name (Q1-Q22)"
            exit 1
        fi
        profile_tpch_query "$2"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Error: Unknown option: $1"
        usage
        ;;
esac
