# Q6 Filter Optimization - Investigation Findings

## Problem Statement

Filter phase consumes 21.44ms (47.5% of Q6 total time = 45.14ms) using scalar row-by-row evaluation.

## Attempted Solution 1: Full ColumnarBatch Conversion

### Approach
Convert all rows to `ColumnarBatch` using `ColumnarBatch::from_rows()`, then apply SIMD filtering via `simd_create_filter_mask()`.

### Results
**FAILED** - Performance regressed significantly:
- **Before**: 45.14ms total (21.44ms filter)
- **After**: 546.04ms total (464.83ms filter)
- **Regression**: 12x slower!

### Breakdown
```
Convert to batch:  348.29ms  (75% of filter time)
SIMD filter:        97.57ms  (21% of filter time)
Other overhead:     18.97ms  (4% of filter time)
Total filter:      464.83ms
```

### Root Cause
`ColumnarBatch::from_rows()` fully materializes ALL columns for ALL 60,000 rows into typed arrays. This is extremely expensive:
- Original scalar filter: 357ns/row × 60,000 rows = 21.44ms
- Batch materialization: 5,805ns/row × 60,000 rows = 348.29ms
- **Materialization is 16x more expensive than scalar filtering!**

The SIMD filtering itself IS faster (97ms vs 21ms would be slower, but includes conversion overhead), but the conversion cost completely dominates.

## Key Insight

**Converting 60,000 rows to columnar format costs 348ms. This is 16x more than the 21ms we're trying to save!**

The problem: `ColumnarBatch::from_rows()` is designed for scenarios where you'll perform many operations on the batch (multiple filters, aggregates, joins). For a single filter operation, the overhead is not worth it.

## Better Approach: Targeted Column Extraction

Instead of converting the entire batch, we should:

1. **Extract ONLY the columns referenced in predicates** into typed arrays
2. **Run SIMD comparisons** on those arrays only
3. **Return the filter bitmap** directly

This would give us:
- Extraction cost: ~4 columns × 60,000 rows × (cost per value)
- SIMD filter cost: ~5ms (based on 97ms / number of predicates)
- Total: Much less than 21ms

### Implementation Plan

Create a new function `create_simd_filter_bitmap()` that:
```rust
pub fn create_simd_filter_bitmap(
    row_count: usize,
    predicates: &[ColumnPredicate],
    get_value: impl Fn(usize, usize) -> Option<&SqlValue>,
) -> Result<Vec<bool>, ExecutorError> {
    // For each unique column referenced in predicates:
    //   1. Determine column type from first non-null value
    //   2. Extract column values into typed Vec (i64/f64/etc)
    //   3. Track null positions

    // For each predicate:
    //   1. Look up the extracted typed array for this column
    //   2. Run SIMD comparison (simd_lt_i64, etc.)
    //   3. Apply null mask
    //   4. AND with result bitmap

    // Return final bitmap
}
```

### Expected Performance

Assuming extraction cost is similar to SqlValue access:
- Column extraction: 4 predicates × 60,000 rows × 100ns = 24ms
- SIMD filtering: 4 predicates × 1ms = 4ms  (conservative)
- Total: ~28ms

This is still worse than the original 21ms! The problem is that extracting values from SqlValue enums is inherently expensive.

## Alternative: Table-Scan Integration

The REAL solution is to integrate SIMD filtering at the TABLE SCAN level:
- Scan produces typed column arrays directly (no SqlValue materialization)
- Filter operates on those arrays with SIMD
- Only passing rows get converted to SqlValue/Row format

This requires deeper integration with the storage layer and is out of scope for this PR.

## Conclusion

**Q6 filter optimization is blocked** by the cost of SqlValue materialization. We have two paths forward:

1. **Short-term**: Accept the 21ms filter cost as acceptable for now
2. **Long-term**: Redesign table scan to produce columnar output natively

The filter itself is not the bottleneck - **SqlValue materialization** is the bottleneck. This affects not just filtering, but all query operations.

## Recommendation

**Do not optimize Q6 filter in isolation.** Instead, focus on:

1. **Storage-level columnar output**: Make table scans produce typed arrays directly
2. **End-to-end columnar pipeline**: Filter → Aggregate → Output without Row materialization
3. **Benchmark impact**: Measure improvement across all TPC-H queries, not just Q6

This is a much larger effort but will provide 10x+ improvements across ALL queries, not just Q6.
