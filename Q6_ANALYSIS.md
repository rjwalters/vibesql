# Q6 Performance Analysis - Issue #2493

## Problem Statement

Q6 is currently 137x slower than DuckDB (74ms vs 0.54ms) despite being the simplest TPC-H query (scan + filter + aggregate, no JOINs or GROUP BY).

## Profiling Results

### Current Performance (SF 0.01, 60,000 rows)

```
Total execution:      74.49ms
- Phase 1 (Scan):     0.04µs (negligible)
- Phase 2 (Filter):   45.76ms (61% of total) ← BOTTLENECK
- Phase 3 (Aggregate): 1.11ms (1.5% of total)
```

## Root Cause

The bottleneck is in **Phase 2 - Filter** (45.76ms out of 74.49ms total).

### Current Implementation

The filter phase uses `create_filter_bitmap()` in `crates/vibesql-executor/src/select/columnar/filter/mod.rs:70-112`, which:

1. Iterates through all 60,000 rows
2. For each row, evaluates predicates by calling `get_value(row_idx, col_idx)`
3. `get_value` performs row-oriented access: `rows.get(row_idx).and_then(|row| row.get(col_idx))`
4. This is **NOT** columnar - it's accessing rows one at a time!

```rust
// Current approach (row-oriented, SLOW)
for row_idx in 0..row_count {
    for predicate in predicates.iter() {
        let column_idx = /* extract from predicate */;
        if let Some(value) = get_value(row_idx, column_idx) {
            let result = evaluate_predicate(predicate, value);
            if !result {
                bitmap[row_idx] = false;
                break;
            }
        }
    }
}
```

### Why This Is Slow

1. **Poor cache locality**: Jumping between rows instead of processing a column at a time
2. **No SIMD vectorization**: Processing one value at a time
3. **Enum matching overhead**: Every value access requires SqlValue enum matching
4. **Function call overhead**: `get_value` closure invoked 60,000+ times

## Solution: True Columnar SIMD Filtering

The codebase already has the infrastructure for true columnar filtering:

1. **ColumnarBatch** (`crates/vibesql-executor/src/select/columnar/batch.rs`):
   - Stores data in column-oriented format
   - Type-specialized arrays (Vec<i64>, Vec<f64>, etc.)
   - Separate NULL bitmaps

2. **SIMD Filter** (`crates/vibesql-executor/src/select/columnar/simd_filter.rs`):
   - `simd_filter_batch()`: SIMD-accelerated filtering
   - Uses SIMD comparison operations from `crates/vibesql-executor/src/simd/comparison.rs`
   - Process 4-8 values per CPU instruction

### Implementation Plan

**Step 1**: Convert rows to ColumnarBatch before filtering

```rust
// In execute_columnar_aggregate() at line ~135
let batch = ColumnarBatch::from_rows(rows)?;
```

**Step 2**: Use SIMD-accelerated filter

```rust
#[cfg(feature = "simd")]
let filtered_batch = simd_filter_batch(&batch, predicates)?;

#[cfg(not(feature = "simd"))]
let filter_bitmap = create_filter_bitmap_tree(/* ... */)?;
```

**Step 3**: Extract filtered rows for aggregation

```rust
let filtered_rows = filtered_batch.to_rows()?;
```

### Expected Performance Impact

DuckDB achieves 0.54ms on Q6. With true columnar SIMD filtering:

- **Current**: 74ms total (45ms filter + 1ms agg + 28ms overhead)
- **Target**: <10ms total (2-3ms filter + 1ms agg + 6-7ms overhead)
- **Improvement**: ~7-10x speedup

Key benefits:
- SIMD operations process 4-8 values per instruction (4-8x speedup)
- Better cache locality from columnar access (2-3x speedup)
- Reduced enum matching overhead (1.5-2x speedup)
- Combined: 12-48x theoretical speedup on filter phase

### Trade-offs

**Pros**:
- Massive performance improvement for large scans
- Already have the infrastructure (ColumnarBatch, simd_filter)
- Aligns with "columnar execution" architecture

**Cons**:
- Batch conversion cost (~5-10ms for 60K rows based on similar operations)
- More memory for columnar format (but short-lived)
- Complexity in maintaining two code paths

**When it pays off**:
- Batch conversion: ~5-10ms one-time cost
- Current filter: ~45ms (row-oriented)
- SIMD filter: ~2-3ms (columnar)
- Net benefit: ~35-40ms savings for 60K rows

For smaller row counts (<1000 rows), row-oriented might be faster. Consider adaptive threshold.

## Implementation Results

### Optimization Applied

Modified `execute_columnar_aggregate()` to use true columnar SIMD filtering:

1. Convert rows to `ColumnarBatch` (type-specialized column arrays)
2. Apply SIMD-accelerated filter using `simd_filter_batch()`
3. Convert filtered batch back to rows for aggregation

### Performance Comparison

**Before optimization** (row-oriented filter):
```
Total execution:      74.88ms
- Scan:               0.04µs
- Filter (row-based): 45.76ms  ← bottleneck
- Aggregate:          1.11ms
```

**After optimization** (SIMD columnar filter):
```
Total execution:      55.70ms (26% faster)
- Convert to batch:   23.75ms  ← new cost
- SIMD filter:        14.62ms  ← 3x faster than before
- Aggregate:          0.28ms   ← 4x faster than before
```

### Analysis

**Gains**:
- Filter phase: 45.76ms → 14.62ms (3.1x speedup)
- Aggregate phase: 1.11ms → 0.28ms (4x speedup)
- Total filter+agg improvement: 46.87ms → 14.90ms (3.1x speedup)

**Costs**:
- Batch conversion: 23.75ms (new overhead)

**Net result**:
- Overall: 74.88ms → 55.70ms (1.34x speedup, 26% improvement)
- Still 103x slower than DuckDB (0.54ms target)

### Why Not Faster?

The batch conversion (rows → columnar → rows) adds 23.75ms overhead, which eats into the filtering gains. To achieve parity with DuckDB, we would need:

1. **Columnar storage from the start**: Avoid rows→batch conversion by storing data columnar natively
2. **Batch-native aggregation**: Compute aggregates directly on ColumnarBatch without converting back to rows
3. **Zero-copy predicates**: Apply WHERE clause during scan, not after

These optimizations would eliminate ~30ms of conversion overhead, potentially achieving:
- Scan + filter + aggregate: 14.62ms + 0.28ms = ~15ms total
- With further SIMD tuning: potentially <10ms

### Next Steps for Further Optimization

1. ⬜ Implement columnar storage layer (avoid initial conversion)
2. ⬜ Add batch-based aggregation functions
3. ⬜ Push filters down to scan layer (predicate pushdown)
4. ⬜ Profile batch conversion to optimize type inference
5. ⬜ Add adaptive threshold (skip columnar for small row counts)

## Action Items

1. ✅ Profile Q6 and identify bottleneck
2. ✅ Modify `execute_columnar_aggregate` to use ColumnarBatch + SIMD filter
3. ⬜ Add adaptive threshold (skip columnar for <1000 rows)
4. ✅ Benchmark and verify improvement
5. ✅ Update tests (all columnar tests pass)
6. ✅ Document the optimization

## Files to Modify

1. `crates/vibesql-executor/src/select/columnar/mod.rs:109-168` - execute_columnar_aggregate()
2. Consider adding threshold logic in `crates/vibesql-executor/src/optimizer/adaptive.rs`

## References

- Issue: #2493
- Related: #2490 (TPC-H performance tracking)
- Similar optimization: #2530 (Q6 speedup from eliminating row materialization)
