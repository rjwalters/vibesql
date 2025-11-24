#!/bin/bash
# Quick TPC-H comparison - single run per query per system
# Much faster than full criterion benchmarking

set -e

OUTPUT="${1:-/tmp/tpch_quick_comparison.txt}"

echo "=== Quick TPC-H Comparison (Single Run) ===" | tee "$OUTPUT"
echo "Date: $(date)" | tee -a "$OUTPUT"
echo "SF: 0.01" | tee -a "$OUTPUT"
echo "" | tee -a "$OUTPUT"

# Build if needed
if [ ! -f target/release/deps/tpch_benchmark-* ]; then
    echo "Building benchmark..." | tee -a "$OUTPUT"
    cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison --no-run
fi

BENCH=$(find target/release/deps -name 'tpch_benchmark-*' -type f -executable | head -1)

echo "Benchmark binary: $BENCH" | tee -a "$OUTPUT"
echo "" | tee -a "$OUTPUT"

# Use criterion's --test mode for single runs
# This is much faster than full benchmarking
$BENCH --test 2>&1 | tee -a "$OUTPUT"

echo "" | tee -a "$OUTPUT"
echo "Quick comparison complete: $OUTPUT"
