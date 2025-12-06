# TPC-H Q6 Performance Investigation - Issue #2493

**Date**: 2025-11-24
**Issue**: #2493
**Goal**: Investigate Q6's unexpectedly poor performance (74-646ms vs 0.54ms DuckDB - 137x gap)
**Status**: Initial Investigation Complete

## Problem Statement

Q6 is one of the **simplest** TPC-H queries but has a **137x performance gap**:
- **VibeSQL**: 74-646ms (varies by caching)
- **DuckDB**: 0.54ms
- **SQLite**: ~3-7ms

This is concerning because Q6 is just: scan + filter + aggregate (single SUM).

## Initial Measurements

```bash
# Cold run (with DB loading):
Execute:    645.62ms (1 rows)
TOTAL:      646.89ms

# Warm run (DB already loaded):
Execute:     89.94ms (1 rows)
TOTAL:       91.08ms
```

**Performance is highly variable (7x difference), suggesting DB loading overhead.**

## Query Analysis

```sql
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
```

**Query Characteristics** - PERFECT for columnar execution:
- ✅ Single table scan (lineitem, ~60K rows at SF 0.01)
- ✅ Simple AND predicates on numeric/date columns
- ✅ Aggregation with arithmetic (SUM(a * b))
- ✅ No JOINs, no GROUP BY
- ✅ Selective projection (1 column output)

## Architecture Review

### Columnar Execution Path

VibeSQL has a SIMD-accelerated columnar execution path that **should** be selected for Q6.

**Execution Model Selection** (`optimizer/adaptive/mod.rs:104`):
```rust
pub fn choose_execution_model(query: &SelectStmt) -> ExecutionModel {
    // Check for query hints first (manual override)
    if let Some(hint) = extract_query_hint(query) {
        return hint;
    }

    // Apply heuristics to detect analytical patterns
    if has_analytical_pattern(query) {
        ExecutionModel::Columnar
    } else {
        ExecutionModel::RowOriented
    }
}
```

**Analytical Pattern Detection** (`optimizer/adaptive/patterns.rs:46`):
```rust
pub(super) fn has_analytical_pattern(query: &SelectStmt) -> bool {
    // Requirements:
    // - No GROUP BY (Phase 5 limitation)
    // - Single table only
    // - No window functions
    // - No DISTINCT
    // - Has aggregation
    // - Either has arithmetic OR selective projection

    has_aggregation && (has_arithmetic || selective_projection)
}
```

**Q6 meets ALL criteria** ✅:
- ✅ No GROUP BY
- ✅ Single table (lineitem)
- ✅ No window functions
- ✅ No DISTINCT
- ✅ Has aggregation (SUM)
- ✅ Has arithmetic (multiplication)
- ✅ Selective projection (1 column)

### Columnar Execution Features

**SIMD Support** (`Cargo.toml:58`):
```toml
default = ["parallel", "spatial", "simd"]
simd = ["wide"]
```

**SIMD is enabled by default**, using the `wide` crate for vectorized operations.

**Columnar Path** (`select/columnar/mod.rs`):
- Zero-copy column references
- SIMD-accelerated filtering
- Vectorized aggregates
- Minimal allocations

## Historical Context

Based on existing documentation:

**q6-analysis.md** (2025-11-20):
- After Phase 1+2 optimizations: **35.2ms** (~586ns/row)
- 6.5x improvement from 230ms baseline
- Still 11x slower than SQLite, 195x slower than DuckDB

**Q6_BENCHMARK_RESULTS.md** (2025-11-23):
- Measured performance: **~396ms average**
- 1.5x improvement over ~600ms baseline
- **Critical finding**: "Performance doesn't reflect expected 6-10x speedup"
- **Recommendation**: "Verify columnar execution is actually being used"

## Key Questions

### 1. Is Columnar Execution Actually Being Used?

**Evidence FOR**:
- Q6 meets ALL criteria for columnar selection
- Adaptive execution should select ExecutionModel::Columnar
- Code path exists and is well-tested

**Evidence AGAINST**:
- Performance doesn't match expected columnar speedup (6-10x)
- 396ms is much slower than expected for SIMD-accelerated execution
- High variance (254-516ms) suggests row-by-row processing overhead

**Investigation Needed**:
```rust
// Add to execute_with_ctes() before columnar check:
log::info!("=== Q6 EXECUTION PATH DEBUG ===");
log::info!("Checking columnar eligibility...");

if let Some(result) = self.try_columnar_execution(stmt, cte_results)? {
    log::info!("✓ USING COLUMNAR EXECUTION PATH");
    return Ok(result);
} else {
    log::info!("✗ COLUMNAR NOT USED - Falling back to row-oriented");
}
```

### 2. If Columnar IS Used - Where Is The Time Spent?

Potential bottlenecks in columnar path:
- Data loading from storage (B-tree pages → columnar arrays)
- Predicate evaluation (even with SIMD)
- Arithmetic expression evaluation (l_extendedprice * l_discount)
- Aggregate accumulation
- Memory allocation/copying

**Need profiling** with `profile-q6` feature (currently building).

### 3. If Columnar is NOT Used - Why Not?

Potential issues:
- Execution model selection failing
- try_columnar_execution() returning None
- Predicate extraction failing (extract_column_predicates)
- Aggregate extraction failing (extract_aggregates)

## Next Steps

### Immediate Actions

1. **✅ DONE**: Build benchmark with `profile-q6` feature
   ```bash
   cargo build --release -p vibesql-executor --bench tpch_profiling \
     --features "benchmark-comparison,profile-q6"
   ```

2. **TODO**: Run Q6 with debug logging to confirm execution path
   ```bash
   RUST_LOG=vibesql_executor=debug \
   ./target/release/deps/tpch_profiling-* Q6 2>&1 | grep -E "(COLUMNAR|DEBUG)"
   ```

3. **TODO**: Run Q6 with profiling enabled
   ```bash
   # The profile-q6 feature adds timing measurements
   ./target/release/deps/tpch_profiling-* Q6
   ```

4. **TODO**: If columnar is used, profile hot spots
   ```bash
   # Option A: Use cargo-flamegraph (if available on macOS)
   cargo flamegraph --bench tpch_profiling -- Q6

   # Option B: Use macOS Instruments
   instruments -t "Time Profiler" -D profile.trace \
     ./target/release/deps/tpch_profiling-* Q6

   # Option C: Use sample
   sample ./target/release/deps/tpch_profiling-* 5 -f profile.txt
   ```

5. **TODO**: Add detailed instrumentation to columnar path
   - Time each phase: scan, filter, aggregate
   - Count SIMD operations vs scalar fallback
   - Measure memory allocations
   - Track row processing rate

### Investigation Scenarios

**Scenario A: Columnar is NOT being used**
- Debug execution model selection
- Check why try_columnar_execution returns None
- Verify predicate/aggregate extraction
- Fix selection logic or query compatibility

**Scenario B: Columnar IS used but slow**
- Profile columnar execution phases
- Identify bottleneck (scan, filter, or aggregate)
- Optimize SIMD utilization
- Reduce memory allocations
- Improve cache locality

## Hypotheses

Based on analysis, ranked by likelihood:

1. **Columnar execution is NOT being used** (70% confidence)
   - Performance profile matches row-oriented execution
   - High variance suggests row-by-row overhead
   - Would explain 400ms vs expected 35ms

2. **Columnar IS used but SIMD isn't effective** (20% confidence)
   - SIMD code paths exist but may have issues
   - Fallback to scalar operations
   - Compiler optimizations not working

3. **DB loading dominates timing** (10% confidence)
   - Warm runs show 90ms (7x faster)
   - But still far from 0.54ms DuckDB target
   - Not the primary issue

## Related Issues & Documentation

- **Current Issue**: #2493 (Investigate Q6 poor performance)
- **Historical**: #2430 (Benchmark Q6 with columnar execution)
- **Historical**: #2440 (Verify Q6 uses columnar execution path)
- **Historical**: #2439 (Benchmark execution hangs - CLOSED)
- **Parent Epic**: #2220 (DuckDB-level performance targets)

**Key Documents**:
- `docs/profiling/q6-analysis.md` - Detailed optimization roadmap
- `docs/archive/Q6_BENCHMARK_RESULTS.md` - Latest benchmark data
- `crates/vibesql-executor/src/optimizer/adaptive/` - Execution model selection
- `crates/vibesql-executor/src/select/columnar/` - Columnar execution engine

## Build Status

**Current**: Building with `profile-q6` feature to enable detailed timing.
**Next**: Once build completes, run profiling and confirm execution path.

---

**Generated by**: Builder agent (Loom workflow)
**Worktree**: `.loom/worktrees/issue-2493`
**Branch**: `feature/issue-2493`
