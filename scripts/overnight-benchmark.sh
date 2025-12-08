#!/bin/bash
# overnight-benchmark.sh - Run make benchmark-all multiple times overnight
#
# Usage: ./scripts/overnight-benchmark.sh [num_runs]
#   num_runs: Number of benchmark runs (default: 3)
#
# Output is logged to /tmp/vibesql-overnight-benchmark.log

set -e

NUM_RUNS="${1:-3}"
LOG_FILE="/tmp/vibesql-overnight-benchmark.log"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cd "$REPO_DIR"

echo "════════════════════════════════════════════════════════════════" | tee "$LOG_FILE"
echo "  VibeSQL Overnight Benchmark Runner" | tee -a "$LOG_FILE"
echo "  Starting $NUM_RUNS runs of 'make benchmark-all'" | tee -a "$LOG_FILE"
echo "  Start time: $(date)" | tee -a "$LOG_FILE"
echo "  Log file: $LOG_FILE" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

for i in $(seq 1 "$NUM_RUNS"); do
    echo "────────────────────────────────────────────────────────────────" | tee -a "$LOG_FILE"
    echo "  Run $i of $NUM_RUNS - Started: $(date)" | tee -a "$LOG_FILE"
    echo "────────────────────────────────────────────────────────────────" | tee -a "$LOG_FILE"

    if make benchmark-all 2>&1 | tee -a "$LOG_FILE"; then
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
