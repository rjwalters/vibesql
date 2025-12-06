#!/bin/bash
# TPC-DS Isolated Benchmark Runner
#
# Runs TPC-DS benchmarks with each database engine in a separate process
# to prevent memory pressure from running all engines simultaneously.
#
# Usage:
#   ./scripts/bench-tpcds-isolated.sh [output_file]
#
# Examples:
#   ./scripts/bench-tpcds-isolated.sh /tmp/tpcds_results.txt
#   ./scripts/bench-tpcds-isolated.sh  # Uses default output
#
# Environment Variables:
#   MYSQL_URL - MySQL connection URL for optional MySQL comparison
#               Example: MYSQL_URL="mysql://user:pass@localhost:3306/tpcds"
#               If not set, MySQL phase is skipped

set -e

# Configuration
OUTPUT_FILE=${1:-/tmp/tpcds_results.txt}
BENCH_NAME="tpcds_benchmark"

# Criterion settings from environment (for --quick mode)
CRITERION_ARGS="--noplot"
if [ -n "$CRITERION_SAMPLE_SIZE" ]; then
    CRITERION_ARGS="$CRITERION_ARGS --sample-size $CRITERION_SAMPLE_SIZE"
    echo "Using reduced sample size: $CRITERION_SAMPLE_SIZE"
fi
if [ -n "$CRITERION_MEASUREMENT_TIME" ]; then
    CRITERION_ARGS="$CRITERION_ARGS --measurement-time $CRITERION_MEASUREMENT_TIME"
    echo "Using reduced measurement time: ${CRITERION_MEASUREMENT_TIME}s"
fi

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Strip macOS quarantine attribute from built binaries
# macOS Gatekeeper can flag freshly-compiled binaries as 'malware'
strip_quarantine() {
    if [[ "$(uname)" == "Darwin" ]] && [[ -d "$REPO_ROOT/target" ]]; then
        find "$REPO_ROOT/target" -type f -perm +111 -exec xattr -d com.apple.quarantine {} \; 2>/dev/null || true
    fi
}

echo -e "${BLUE}=== TPC-DS Isolated Benchmark Runner ===${NC}"
echo "Output: ${OUTPUT_FILE}"
echo ""
echo "This script runs each database engine in a separate process"
echo "to avoid memory pressure from concurrent engine loading."
echo ""

# Initialize output file
cat > "$OUTPUT_FILE" << EOF
=== TPC-DS Isolated Benchmark Results ===
Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")

EOF

# Track results
ENGINES_PASSED=0
ENGINES_FAILED=0

# Phase 1: Run VibeSQL-only benchmark (sanity + queries)
echo -e "${YELLOW}Phase 1: Running VibeSQL-only benchmark...${NC}"
echo ""
echo "--- Phase 1: VibeSQL-only ---" >> "$OUTPUT_FILE"

if cargo bench --package vibesql-executor --bench ${BENCH_NAME} -- $CRITERION_ARGS 2>&1 | tee -a "$OUTPUT_FILE"; then
    strip_quarantine
    echo -e "${GREEN}✓ VibeSQL benchmark completed${NC}"
    ((ENGINES_PASSED++))
else
    echo -e "${RED}✗ VibeSQL benchmark failed${NC}"
    ((ENGINES_FAILED++))
fi
echo "" >> "$OUTPUT_FILE"
echo ""

# Phase 2: Run SQLite comparison benchmark (in separate process)
echo -e "${YELLOW}Phase 2: Running SQLite comparison benchmark...${NC}"
echo ""
echo "--- Phase 2: SQLite comparison ---" >> "$OUTPUT_FILE"

if TPCDS_ENGINE=sqlite cargo bench --package vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison -- $CRITERION_ARGS 2>&1 | tee -a "$OUTPUT_FILE"; then
    strip_quarantine
    echo -e "${GREEN}✓ SQLite comparison completed${NC}"
    ((ENGINES_PASSED++))
else
    echo -e "${YELLOW}⚠ SQLite comparison failed (continuing)${NC}"
    ((ENGINES_FAILED++))
fi
echo "" >> "$OUTPUT_FILE"
echo ""

# Phase 3: Run DuckDB comparison benchmark (in separate process)
echo -e "${YELLOW}Phase 3: Running DuckDB comparison benchmark...${NC}"
echo ""
echo "--- Phase 3: DuckDB comparison ---" >> "$OUTPUT_FILE"

if TPCDS_ENGINE=duckdb cargo bench --package vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison -- $CRITERION_ARGS 2>&1 | tee -a "$OUTPUT_FILE"; then
    strip_quarantine
    echo -e "${GREEN}✓ DuckDB comparison completed${NC}"
    ((ENGINES_PASSED++))
else
    echo -e "${YELLOW}⚠ DuckDB comparison failed (continuing)${NC}"
    ((ENGINES_FAILED++))
fi
echo "" >> "$OUTPUT_FILE"
echo ""

# Phase 4: Run MySQL comparison benchmark (optional, requires MYSQL_URL)
if [ -n "$MYSQL_URL" ]; then
    echo -e "${YELLOW}Phase 4: Running MySQL comparison benchmark...${NC}"
    echo ""
    echo "--- Phase 4: MySQL comparison ---" >> "$OUTPUT_FILE"

    if TPCDS_ENGINE=mysql cargo bench --package vibesql-executor --bench ${BENCH_NAME} --features benchmark-comparison -- $CRITERION_ARGS 2>&1 | tee -a "$OUTPUT_FILE"; then
        strip_quarantine
        echo -e "${GREEN}✓ MySQL comparison completed${NC}"
        ((ENGINES_PASSED++))
    else
        echo -e "${YELLOW}⚠ MySQL comparison failed (continuing)${NC}"
        ((ENGINES_FAILED++))
    fi
    echo "" >> "$OUTPUT_FILE"
    echo ""
else
    echo -e "${YELLOW}Phase 4: Skipping MySQL (MYSQL_URL not set)${NC}"
    echo ""
    echo "--- Phase 4: MySQL comparison (skipped - MYSQL_URL not set) ---" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
fi

# Summary
echo -e "${BLUE}=== Summary ===${NC}"
echo -e "Engines passed: ${GREEN}$ENGINES_PASSED${NC}"
echo -e "Engines failed: ${RED}$ENGINES_FAILED${NC}"
echo ""
echo "Full results: $OUTPUT_FILE"

# Add summary to output file
cat >> "$OUTPUT_FILE" << EOF

=== Summary ===
Engines passed: $ENGINES_PASSED
Engines failed: $ENGINES_FAILED
EOF

# Exit with success if at least VibeSQL completed
[ $ENGINES_PASSED -ge 1 ]
