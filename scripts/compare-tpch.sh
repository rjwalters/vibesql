#!/bin/bash
# Compare TPC-H performance across VibeSQL, SQLite, and DuckDB
# Usage: ./scripts/compare-tpch.sh [output_file]

set -e

OUTPUT_FILE="${1:-/tmp/tpch_comparison.txt}"
TIMEOUT=30

echo "=== TPC-H Performance Comparison ===" | tee "$OUTPUT_FILE"
echo "Date: $(date -u +"%Y-%m-%d %H:%M:%S UTC")" | tee -a "$OUTPUT_FILE"
echo "Scale Factor: 0.01" | tee -a "$OUTPUT_FILE"
echo "Timeout: ${TIMEOUT}s per query" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Find the benchmark binary
BENCH_BIN=$(find target/release/deps -name 'tpch_benchmark-*' -type f -executable | head -1)

if [ -z "$BENCH_BIN" ]; then
    echo "Error: Benchmark binary not found. Run: cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison --no-run"
    exit 1
fi

echo "Using benchmark binary: $BENCH_BIN" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Run benchmarks for each query with minimal samples
echo "Running benchmarks (this may take 10-15 minutes)..." | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Use criterion with minimal configuration for quick comparison
CARGO_TARGET_DIR=target cargo bench \
    --package vibesql-executor \
    --bench tpch_benchmark \
    --features benchmark-comparison \
    -- --quick \
    2>&1 | tee -a "$OUTPUT_FILE"

echo "" | tee -a "$OUTPUT_FILE"
echo "=== Comparison complete ===" | tee -a "$OUTPUT_FILE"
echo "Results saved to: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
