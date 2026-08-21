#!/usr/bin/env bash
# TPC-H Benchmark Runner
#
# Consolidated script for running TPC-H benchmarks in various modes.
# Supports isolated runs, comparisons, and different output formats.
#
# Usage:
#   ./scripts/bench-tpch.sh [--mode MODE] [--timeout SECS] [--output FILE]
#
# Modes:
#   standard      Run VibeSQL profiling benchmark (default, detailed timing)
#   engines       Run all 4 engines (VibeSQL, SQLite, DuckDB, MySQL) via Criterion
#   isolated      Run each query in separate subprocess
#   compare       Full Criterion comparison with all engines (same as engines)
#   quick-compare Single-run comparison (much faster)
#   analyze       Analyze previous results
#   web-demo      Generate web demo format (JSON)
#
# Examples:
#   ./scripts/bench-tpch.sh                              # Standard run, 30s timeout
#   ./scripts/bench-tpch.sh --timeout 60                 # Standard run, 60s timeout
#   ./scripts/bench-tpch.sh --mode engines               # All 4 engines comparison
#   ./scripts/bench-tpch.sh --mode isolated --timeout 30 # Isolated run
#   ./scripts/bench-tpch.sh --mode compare               # Full comparison
#   ./scripts/bench-tpch.sh --mode quick-compare         # Quick comparison
#   ./scripts/bench-tpch.sh --mode web-demo --output /tmp/results.json

set -e

# Defaults
MODE="standard"
TIMEOUT_SECS=30
OUTPUT_FILE="/tmp/tpch_results.txt"
BENCH_NAME="tpch_profiling"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SECS="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --help|-h)
            sed -n '2,/^$/p' "$0" | sed 's/^# //'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Strip macOS quarantine attribute from built binaries
# macOS Gatekeeper can flag freshly-compiled binaries as 'malware'
strip_quarantine() {
    if [[ "$(uname)" == "Darwin" ]] && [[ -d "$REPO_ROOT/target" ]]; then
        find "$REPO_ROOT/target" -type f -perm +111 -exec xattr -d com.apple.quarantine {} \; 2>/dev/null || true
    fi
}

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

# ============================================================================
# Mode: standard - Run all queries together
# ============================================================================
run_standard_mode() {
    echo -e "${BLUE}=== TPC-H Benchmark (Standard Mode) ===${NC}"
    echo "Timeout: ${TIMEOUT_SECS}s per query"
    echo "Output: ${OUTPUT_FILE}"
    echo ""

    # Build benchmark
    echo -e "${YELLOW}Building benchmark...${NC}"
    cargo build --release -p vibesql-executor --bench ${BENCH_NAME} --features sqlite,in-memory-indexes 2>&1 | grep -E "(Compiling|Finished)" || true
    strip_quarantine
    echo -e "${GREEN}✓ Build complete${NC}"
    echo ""

    # Run benchmark
    echo -e "${YELLOW}Running benchmark...${NC}"
    # Find the benchmark binary (exclude .d and .o files, get most recently modified)
    BENCH_BIN=$(find ./target/release/deps -maxdepth 1 -name "${BENCH_NAME}-*" -type f ! -name "*.d" ! -name "*.o" -exec ls -t {} + | head -1)
    if [ -z "$BENCH_BIN" ]; then
        echo -e "${RED}Error: Benchmark binary not found${NC}"
        exit 1
    fi
    QUERY_TIMEOUT_SECS=${TIMEOUT_SECS} ${TIMEOUT_CMD} 300 ${BENCH_BIN} > ${OUTPUT_FILE} 2>&1 || true
    echo -e "${GREEN}✓ Benchmark complete${NC}"
    echo ""

    # Show summary
    show_summary
}

# ============================================================================
# Mode: isolated - Run each query in separate subprocess
# ============================================================================
run_isolated_mode() {
    echo -e "${BLUE}=== TPC-H Isolated Benchmark ===${NC}"
    echo "Timeout: ${TIMEOUT_SECS}s per query"
    echo "Output: ${OUTPUT_FILE}"
    echo ""

    # Build benchmark binary
    echo -e "${YELLOW}Building benchmark...${NC}"
    cargo build --release -p vibesql-executor --bench ${BENCH_NAME} --features sqlite,in-memory-indexes 2>&1 | grep -E "(Compiling|Finished)" || true
    strip_quarantine
    BENCH_BIN=$(find target/release/deps -name "${BENCH_NAME}-*" -type f -perm +111 | head -1)

    if [ -z "$BENCH_BIN" ]; then
        echo -e "${RED}Error: Benchmark binary not found${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Build complete: $BENCH_BIN${NC}"
    echo ""

    # Initialize output file
    cat > "$OUTPUT_FILE" << EOF
=== TPC-H Isolated Benchmark Results ===
Timeout: ${TIMEOUT_SECS}s per query
Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")
Binary: $BENCH_BIN

EOF

    # Query list
    QUERIES=(Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22)

    # Counters
    PASSED=0
    TIMEOUT=0
    CRASHED=0
    TOTAL=${#QUERIES[@]}

    echo -e "${YELLOW}Running ${TOTAL} queries in isolation...${NC}"
    echo ""

    # Run each query in isolation
    for query in "${QUERIES[@]}"; do
        echo -ne "  ${query}: "

        # Run in subprocess with timeout
        QUERY_OUTPUT=$(mktemp)

        if ${TIMEOUT_CMD} ${TIMEOUT_SECS} env QUERY_TIMEOUT_SECS=${TIMEOUT_SECS} "$BENCH_BIN" "$query" 2>&1 | \
           tee "$QUERY_OUTPUT" | \
           grep -q "TOTAL:"; then
            # Extract timing
            TIMING=$(grep "TOTAL:" "$QUERY_OUTPUT" | head -1 | awk '{print $2}')
            if echo "$TIMING" | grep -q "TIMEOUT"; then
                echo -e "${YELLOW}TIMEOUT${NC}"
                TIMEOUT=$((TIMEOUT+1))
            else
                echo -e "${GREEN}${TIMING}${NC}"
                PASSED=$((PASSED+1))
            fi
        else
            # Check exit status
            EXIT_CODE=$?
            if [ $EXIT_CODE -eq 124 ]; then
                echo -e "${YELLOW}TIMEOUT (wallclock)${NC}"
                TIMEOUT=$((TIMEOUT+1))
            else
                echo -e "${RED}CRASHED (exit $EXIT_CODE)${NC}"
                CRASHED=$((CRASHED+1))
            fi
        fi

        # Append to results
        cat "$QUERY_OUTPUT" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"

        rm -f "$QUERY_OUTPUT"
    done

    # Summary
    echo ""
    echo -e "${BLUE}=== Summary ===${NC}"
    echo -e "Total queries: $TOTAL"
    echo -e "${GREEN}Passed: $PASSED${NC}"
    echo -e "${YELLOW}Timeout: $TIMEOUT${NC}"
    echo -e "${RED}Crashed: $CRASHED${NC}"
    echo ""
    echo "Full results: $OUTPUT_FILE"

    # Add summary to output file
    cat >> "$OUTPUT_FILE" << EOF

=== Summary ===
Total queries: $TOTAL
Passed: $PASSED
Timeout: $TIMEOUT
Crashed: $CRASHED
EOF
}

# ============================================================================
# Mode: compare - Full comparison with SQLite and DuckDB
# ============================================================================
run_compare_mode() {
    echo -e "${BLUE}=== TPC-H Performance Comparison ===${NC}"
    echo ""

    # Find the benchmark binary
    BENCH_BIN=$(find target/release/deps -name 'tpch_benchmark-*' -type f -executable | head -1)

    if [ -z "$BENCH_BIN" ]; then
        echo -e "${YELLOW}Building benchmark...${NC}"
        cargo bench --package vibesql-executor --bench tpch_benchmark --features sqlite,in-memory-indexes --no-run
        strip_quarantine
        BENCH_BIN=$(find target/release/deps -name 'tpch_benchmark-*' -type f -executable | head -1)
    fi

    if [ -z "$BENCH_BIN" ]; then
        echo -e "${RED}Error: Benchmark binary not found${NC}"
        exit 1
    fi

    echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")" | tee "$OUTPUT_FILE"
    echo "Scale Factor: 0.01" | tee -a "$OUTPUT_FILE"
    echo "Timeout: ${TIMEOUT_SECS}s per query" | tee -a "$OUTPUT_FILE"
    echo "" | tee -a "$OUTPUT_FILE"
    echo "Using benchmark binary: $BENCH_BIN" | tee -a "$OUTPUT_FILE"
    echo "" | tee -a "$OUTPUT_FILE"

    # Run benchmarks for each query with minimal samples
    echo -e "${YELLOW}Running benchmarks (this may take 10-15 minutes)...${NC}" | tee -a "$OUTPUT_FILE"
    echo "" | tee -a "$OUTPUT_FILE"

    # Use criterion with minimal configuration for quick comparison
    CARGO_TARGET_DIR=target cargo bench \
        --package vibesql-executor \
        --bench tpch_benchmark \
        --features sqlite,in-memory-indexes \
        -- --quick \
        2>&1 | tee -a "$OUTPUT_FILE"

    echo "" | tee -a "$OUTPUT_FILE"
    echo -e "${GREEN}✓ Comparison complete${NC}" | tee -a "$OUTPUT_FILE"
    echo "Results saved to: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
}

# ============================================================================
# Mode: quick-compare - Single-run comparison (much faster)
# ============================================================================
run_quick_compare_mode() {
    echo -e "${BLUE}=== Quick TPC-H Comparison (Single Run) ===${NC}"
    echo ""

    # Build if needed
    if [ ! -f target/release/deps/tpch_benchmark-* ]; then
        echo -e "${YELLOW}Building benchmark...${NC}"
        cargo bench --package vibesql-executor --bench tpch_benchmark --features sqlite,in-memory-indexes --no-run
        strip_quarantine
    fi

    BENCH=$(find target/release/deps -name 'tpch_benchmark-*' -type f -executable | head -1)

    if [ -z "$BENCH" ]; then
        echo -e "${RED}Error: Benchmark binary not found${NC}"
        exit 1
    fi

    echo "Date: $(date)" | tee "$OUTPUT_FILE"
    echo "SF: 0.01" | tee -a "$OUTPUT_FILE"
    echo "Benchmark binary: $BENCH" | tee -a "$OUTPUT_FILE"
    echo "" | tee -a "$OUTPUT_FILE"

    # Use criterion's --test mode for single runs
    $BENCH --test 2>&1 | tee -a "$OUTPUT_FILE"

    echo "" | tee -a "$OUTPUT_FILE"
    echo -e "${GREEN}✓ Quick comparison complete: $OUTPUT_FILE${NC}"
}

# ============================================================================
# Mode: analyze - Analyze previous results
# ============================================================================
run_analyze_mode() {
    if [ ! -f "$OUTPUT_FILE" ]; then
        echo -e "${RED}Error: Log file not found: $OUTPUT_FILE${NC}"
        exit 1
    fi

    echo -e "${BLUE}=== TPC-H Benchmark Results Analysis ===${NC}"
    echo ""

    # Extract and analyze results
    grep -E "tpch_q[0-9]+/(vibesql|sqlite|duckdb)/SF" "$OUTPUT_FILE" | \
        grep "time:" | \
        awk '{
            # Extract query number, database, and time
            split($1, a, "/");
            query = a[1];
            db = a[2];

            # Extract time value and unit
            time_val = $4;
            time_unit = $5;

            # Convert to milliseconds for consistent comparison
            if (time_unit == "ms]") {
                time_ms = time_val;
            } else if (time_unit == "µs]") {
                time_ms = time_val / 1000;
            } else if (time_unit == "s]") {
                time_ms = time_val * 1000;
            } else {
                time_ms = time_val;
            }

            # Store results
            results[query, db] = time_ms;
            queries[query] = 1;
            dbs[db] = 1;
        }
        END {
            # Print header
            printf "%-10s %15s %15s %15s %12s %12s\n", "Query", "VibeSQL", "SQLite", "DuckDB", "vs SQLite", "vs DuckDB";
            printf "%-10s %15s %15s %15s %12s %12s\n", "------", "-------", "------", "------", "---------", "---------";

            total_vibesql = 0;
            total_sqlite = 0;
            total_duckdb = 0;
            count = 0;

            # Sort and print results
            n = asorti(queries, sorted_queries);
            for (i = 1; i <= n; i++) {
                q = sorted_queries[i];
                v = results[q, "vibesql"];
                s = results[q, "sqlite"];
                d = results[q, "duckdb"];

                if (v > 0) {
                    vibesql_str = sprintf("%.2f ms", v);
                    total_vibesql += v;
                    count++;
                } else {
                    vibesql_str = "N/A";
                }

                if (s > 0) {
                    sqlite_str = sprintf("%.2f ms", s);
                    total_sqlite += s;
                } else {
                    sqlite_str = "N/A";
                }

                if (d > 0) {
                    duckdb_str = sprintf("%.2f ms", d);
                    total_duckdb += d;
                } else {
                    duckdb_str = "N/A";
                }

                # Calculate ratios
                if (v > 0 && s > 0) {
                    ratio_sqlite = v / s;
                    ratio_str = sprintf("%.2fx", ratio_sqlite);
                } else {
                    ratio_str = "N/A";
                }

                if (v > 0 && d > 0) {
                    ratio_duckdb = v / d;
                    ratio_duckdb_str = sprintf("%.2fx", ratio_duckdb);
                } else {
                    ratio_duckdb_str = "N/A";
                }

                printf "%-10s %15s %15s %15s %12s %12s\n", q, vibesql_str, sqlite_str, duckdb_str, ratio_str, ratio_duckdb_str;
            }

            # Print totals
            printf "\n%-10s %15s %15s %15s\n", "TOTAL", sprintf("%.2f ms", total_vibesql), sprintf("%.2f ms", total_sqlite), sprintf("%.2f ms", total_duckdb);

            if (total_sqlite > 0) {
                avg_ratio_sqlite = total_vibesql / total_sqlite;
                printf "%-10s %15s %15s %15s %12s\n", "AVG RATIO", "", "", "", sprintf("%.2fx", avg_ratio_sqlite);
            }

            if (total_duckdb > 0) {
                avg_ratio_duckdb = total_vibesql / total_duckdb;
                printf "%-10s %15s %15s %15s %12s %12s\n", "", "", "", "", "", sprintf("%.2fx", avg_ratio_duckdb);
            }
        }' | column -t

    echo ""
    echo -e "${BLUE}=== Performance Assessment ===${NC}"
    echo ""
    echo "According to docs/performance/BENCHMARK_STRATEGY.md:"
    echo "  < 1.5x slower than SQLite  = Competitive ✅"
    echo "  1.5x - 2.5x slower         = Acceptable ⚠️"
    echo "  2.5x - 5.0x slower         = Needs improvement ⚠️"
    echo "  > 5.0x slower              = Performance issue ❌"
    echo ""
}

# ============================================================================
# Mode: web-demo - Generate web demo format (JSON)
# ============================================================================
run_web_demo_mode() {
    echo -e "${BLUE}=== Running TPC-H Benchmarks (Web Demo Format) ===${NC}"
    echo ""

    # Use the Python script for web demo format conversion
    python3 "$SCRIPT_DIR/run_tpch_benchmarks.py" --output "$OUTPUT_FILE"
}

# ============================================================================
# Helper: Show summary for standard mode
# ============================================================================
show_summary() {
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
}

# ============================================================================
# Main
# ============================================================================
cd "$REPO_ROOT"

case "$MODE" in
    standard)
        run_standard_mode
        ;;
    engines|compare)
        # Both 'engines' and 'compare' run all 4 engines via Criterion
        run_compare_mode
        ;;
    isolated)
        run_isolated_mode
        ;;
    quick-compare)
        run_quick_compare_mode
        ;;
    analyze)
        run_analyze_mode
        ;;
    web-demo)
        run_web_demo_mode
        ;;
    *)
        echo -e "${RED}Error: Unknown mode '$MODE'${NC}"
        echo "Valid modes: standard, engines, isolated, compare, quick-compare, analyze, web-demo"
        exit 1
        ;;
esac
