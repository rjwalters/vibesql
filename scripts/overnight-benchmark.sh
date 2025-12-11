#!/bin/bash
# overnight-benchmark.sh - Run full benchmark suite multiple times overnight
#
# Usage: ./scripts/overnight-benchmark.sh [num_runs] [scale_factor]
#   num_runs:     Number of benchmark runs (default: 3)
#   scale_factor: Database scale factor (default: 0.1, which is 10x the normal default)
#
# Output is logged to /tmp/vibesql-overnight-benchmark.log
#
# This script runs larger databases than the default 'make benchmark-all' to get
# more meaningful performance data for overnight runs.

set -e

NUM_RUNS="${1:-3}"
SCALE_FACTOR="${2:-0.1}"
LOG_FILE="/tmp/vibesql-overnight-benchmark.log"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cd "$REPO_DIR"

echo "════════════════════════════════════════════════════════════════" | tee "$LOG_FILE"
echo "  VibeSQL Overnight Benchmark Runner" | tee -a "$LOG_FILE"
echo "  Starting $NUM_RUNS runs with scale factor $SCALE_FACTOR" | tee -a "$LOG_FILE"
echo "  Start time: $(date)" | tee -a "$LOG_FILE"
echo "  Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Run a single benchmark suite (embedded + server benchmarks)
run_benchmark_suite() {
    local run_num=$1

    echo "  [Embedded] Running TPC-H, TPC-C, TPC-DS, Sysbench (VibeSQL, SQLite, DuckDB)..." | tee -a "$LOG_FILE"
    ./scripts/bench --test=all --engine=vibesql,sqlite,duckdb --scale="$SCALE_FACTOR" 2>&1 | tee -a "$LOG_FILE"

    echo "" | tee -a "$LOG_FILE"
    echo "  [Server] Running TPC-H server (VibeSQL-server, MySQL)..." | tee -a "$LOG_FILE"
    ./scripts/bench --test=tpch-server --engine=vibesql-server,mysql --scale="$SCALE_FACTOR" 2>&1 | tee -a "$LOG_FILE"

    echo "" | tee -a "$LOG_FILE"
    echo "  [Server] Running TPC-C server (VibeSQL-server, MySQL)..." | tee -a "$LOG_FILE"
    ./scripts/bench --test=tpcc-server --engine=vibesql-server,mysql --scale="$SCALE_FACTOR" 2>&1 | tee -a "$LOG_FILE"

    echo "" | tee -a "$LOG_FILE"
    echo "  [Server] Running Sysbench server (VibeSQL-server, MySQL)..." | tee -a "$LOG_FILE"
    ./scripts/bench --test=sysbench-server --engine=vibesql-server,mysql --scale="$SCALE_FACTOR" 2>&1 | tee -a "$LOG_FILE"
}

for i in $(seq 1 "$NUM_RUNS"); do
    echo "────────────────────────────────────────────────────────────────" | tee -a "$LOG_FILE"
    echo "  Run $i of $NUM_RUNS - Started: $(date)" | tee -a "$LOG_FILE"
    echo "  Scale factor: $SCALE_FACTOR" | tee -a "$LOG_FILE"
    echo "────────────────────────────────────────────────────────────────" | tee -a "$LOG_FILE"

    if run_benchmark_suite "$i"; then
        echo "" | tee -a "$LOG_FILE"
        echo "  ✓ Run $i completed successfully at $(date)" | tee -a "$LOG_FILE"
    else
        echo "" | tee -a "$LOG_FILE"
        echo "  ✗ Run $i failed at $(date)" | tee -a "$LOG_FILE"
        echo "  Continuing to next run..." | tee -a "$LOG_FILE"
    fi
    echo "" | tee -a "$LOG_FILE"
done

echo "════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
echo "  All $NUM_RUNS benchmark runs completed" | tee -a "$LOG_FILE"
echo "  End time: $(date)" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
