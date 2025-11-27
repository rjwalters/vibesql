#!/bin/bash
# TPC-H Benchmark Runner
#
# Quick and easy way to run TPC-H benchmarks during performance sprints
#
# Usage:
#   ./scripts/bench-tpch.sh              # Run with 30s timeout (default)
#   ./scripts/bench-tpch.sh 60           # Run with custom timeout
#   ./scripts/bench-tpch.sh 30 summary   # Show summary only

set -e

# Configuration
TIMEOUT_SECS=${1:-30}
MODE=${2:-full}
OUTPUT_FILE="/tmp/tpch_results.txt"
BENCH_NAME="tpch_profiling"

# Find timeout command (GNU coreutils timeout or gtimeout on macOS)
if command -v timeout &> /dev/null; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout &> /dev/null; then
    TIMEOUT_CMD="gtimeout"
else
    echo "Error: 'timeout' command not found."
    echo "On macOS, install with: brew install coreutils"
    exit 1
fi

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== TPC-H Benchmark Runner ===${NC}"
echo "Timeout: ${TIMEOUT_SECS}s per query"
echo "Output: ${OUTPUT_FILE}"
echo ""

# Build benchmark
echo -e "${YELLOW}Building benchmark...${NC}"
cargo build --release -p vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison 2>&1 | grep -E "(Compiling|Finished)" || true
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Run benchmark
echo -e "${YELLOW}Running benchmark...${NC}"
QUERY_TIMEOUT_SECS=${TIMEOUT_SECS} ${TIMEOUT_CMD} 300 ./target/release/deps/${BENCH_NAME}-* > ${OUTPUT_FILE} 2>&1
echo -e "${GREEN}✓ Benchmark complete${NC}"
echo ""

# Show results
if [ "$MODE" = "summary" ]; then
    # Summary mode - just show query totals
    echo -e "${BLUE}=== Results Summary ===${NC}"

    # Parse results line by line
    while IFS= read -r line; do
        # Check if this is a query header
        if echo "$line" | grep -q "^=== Q[0-9]"; then
            current_query=$(echo "$line" | sed 's/^=== \(Q[0-9]*\) ===$/\1/')
        # Check if this is a TOTAL line for current query
        elif [ -n "$current_query" ] && echo "$line" | grep -q "TOTAL:"; then
            if echo "$line" | grep -q "TIMEOUT"; then
                echo -e "  $current_query: ${YELLOW}TIMEOUT (>${TIMEOUT_SECS}s)${NC}"
            else
                # Extract time - handle various formats (ms, s, µs)
                time=$(echo "$line" | sed 's/.*TOTAL:[[:space:]]*//' | awk '{print $1}')
                echo -e "  $current_query: ${GREEN}$time${NC}"
            fi
            current_query=""
        # Check for ERROR on Execute line
        elif [ -n "$current_query" ] && echo "$line" | grep -q "Execute:.*ERROR"; then
            error=$(echo "$line" | sed 's/.*ERROR: //' | cut -c1-50)
            echo -e "  $current_query: ${RED}ERROR${NC} - $error"
            current_query=""
        # Check for parse ERROR on its own line
        elif [ -n "$current_query" ] && echo "$line" | grep -q "^ERROR:"; then
            error=$(echo "$line" | sed 's/ERROR: //' | cut -c1-50)
            echo -e "  $current_query: ${RED}PARSE ERROR${NC} - $error"
            current_query=""
        fi
    done < ${OUTPUT_FILE}
else
    # Full mode - show complete output
    cat ${OUTPUT_FILE}
fi

echo ""
echo -e "${BLUE}=== Quick Stats ===${NC}"
total=$(grep -c "^=== Q" ${OUTPUT_FILE} || echo "0")
passed=$(grep -E "Execute:.*rows\)" ${OUTPUT_FILE} | wc -l | tr -d ' ')
timeout=$(grep -c "TIMEOUT" ${OUTPUT_FILE} || echo "0")
errors=$(grep -E "Execute:.*ERROR" ${OUTPUT_FILE} | wc -l | tr -d ' ')

echo -e "Total queries: $total"
echo -e "${GREEN}Passed: $passed${NC}"
echo -e "${YELLOW}Timeout: $timeout${NC}"
echo -e "${RED}Errors: $errors${NC}"

echo ""
echo "Full results: ${OUTPUT_FILE}"
