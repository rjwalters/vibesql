#!/bin/bash
# Test script for validating join optimizations (Phase 4 of #2494)
#
# This script runs the problematic queries (Q3, Q7, Q10, Q12) with verbose
# join reordering output to validate that optimizations are working.

set -e

echo "=== Testing Join Optimizations (Issue #2494) ==="
echo ""
echo "Building profiling benchmark..."
cargo build --release --package vibesql-executor --bench tpch_profiling --features benchmark-comparison 2>&1 | tail -5

PROFILER=$(ls -t target/release/deps/tpch_profiling-* | head -1)
echo "Using profiler: $PROFILER"
echo ""

# Test Q3: 3-way join (165x gap)
echo "=== Q3: 3-way JOIN (customer -> orders -> lineitem) ==="
echo "Expected: Should start with filtered customer table (~30K rows)"
echo ""
JOIN_REORDER_VERBOSE=1 $PROFILER Q3 2>&1 | grep -E "\[JOIN_REORDER\]|\[JOIN_COST\]|Execute:" | head -30
echo ""

# Test Q7: 6-way join (151x gap)
echo "=== Q7: 6-way JOIN (should start with nation tables) ==="
echo "Expected: Should start with filtered nation tables (~2 rows each)"
echo ""
JOIN_REORDER_VERBOSE=1 $PROFILER Q7 2>&1 | grep -E "\[JOIN_REORDER\]|\[JOIN_COST\]|Execute:" | head -40
echo ""

# Test Q10: 3-way join (95x gap)
echo "=== Q10: 3-way JOIN ==="
JOIN_REORDER_VERBOSE=1 $PROFILER Q10 2>&1 | grep -E "\[JOIN_REORDER\]|\[JOIN_COST\]|Execute:" | head -30
echo ""

# Test Q12: 2-way join (94x gap)
echo "=== Q12: 2-way JOIN ==="
JOIN_REORDER_VERBOSE=1 $PROFILER Q12 2>&1 | grep -E "\[JOIN_REORDER\]|\[JOIN_COST\]|Execute:" | head -30
echo ""

echo "=== Summary ==="
echo "Phase 2 optimizations implemented:"
echo "  ✓ Smart selectivity heuristics (equality: 10%, range: 25%)"
echo "  ✓ Improved hash join cost model (2x build cost)"
echo "  ✓ Adaptive time budget (500ms-2000ms based on table count)"
echo ""
echo "Phase 3 diagnostics added:"
echo "  ✓ Detailed cost logging"
echo "  ✓ Table cardinality logging with selectivity"
echo "  ✓ Final join order logging"
echo ""
echo "Next: Compare execution times to baseline (338ms, 450ms, 310ms, 237ms)"
echo "Target: 5-10x improvement per query"
