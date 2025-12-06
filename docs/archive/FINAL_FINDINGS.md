# TPC-H Date Operations: Final Profiling Findings

## Executive Summary

**Finding**: TPC-H queries with date operations (Q1, Q6, Q12) are using the **columnar aggregation execution path**, which currently **does NOT have SIMD support for date comparisons**.

**Impact**: Date filtering operations fall back to scalar evaluation, leaving significant performance on the table.

**Recommendation**: Implement **Option 2.5** (hybrid approach) - Add Date column support to existing columnar SIMD infrastructure.

---

## Investigation Results

### 1. Baseline Performance (SF 0.01)

| Query | Execution Time | Date Operations | Dataset Size |
|-------|---------------|-----------------|--------------|
| **Q1** | 325ms | `l_shipdate <= '1998-09-01'` | 60,000 rows (lineitem) |
| **Q6** | 48ms | `l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'` | 60,000 rows (lineitem) |
| **Q12** | 196ms | Date comparisons + range filtering | 15,000 orders + 60,000 lineitem |

### 2. Execution Path Analysis

#### Two Separate SIMD Implementations Found

**Path 1: Non-Aggregating Queries (Arrow-based)**
- **Location**: `crates/vibesql-executor/src/select/vectorized/filter.rs`
- **Technology**: Apache Arrow SIMD kernels
- **Supports**: Int64, Float64, Utf8, **Date32** ✅, Timestamp
- **Used by**: Simple SELECT queries without aggregation
- **Status**: Date32 comparisons already SIMD-optimized via Arrow kernels

**Path 2: Aggregating Queries (Custom SIMD)**
- **Location**: `crates/vibesql-executor/src/select/columnar/simd_filter.rs`
- **Technology**: Custom SIMD using `wide` crate via `crates/vibesql-executor/src/simd/`
- **Supports**: Int64, Float64 only
- **Used by**: Queries with aggregation (GROUP BY, SUM, AVG, etc.)
- **Status**: **Date columns fall back to scalar** ❌

#### Why TPC-H Queries Don't Use Arrow SIMD

ALL three TPC-H queries include **aggregation**:
- **Q1**: `GROUP BY` with `SUM()`, `AVG()`, `COUNT()`
- **Q6**: Single `SUM()` aggregate
- **Q12**: `GROUP BY` with `SUM(CASE ...)`

Therefore, they use Path 2 (columnar aggregation), not Path 1 (Arrow-based).

### 3. Root Cause: Missing Date Support in Columnar SIMD

**File**: `crates/vibesql-executor/src/select/columnar/simd_filter.rs:89-103`

```rust
match column {
    // SIMD path for i64 columns
    ColumnArray::Int64(values, nulls) => {
        evaluate_predicate_i64_simd(predicate, values, nulls.as_ref())
    }

    // SIMD path for f64 columns
    ColumnArray::Float64(values, nulls) => {
        evaluate_predicate_f64_simd(predicate, values, nulls.as_ref())
    }

    // Scalar fallback for other column types
    // ⚠️ Date columns hit this path!
    _ => evaluate_predicate_scalar(batch, predicate, column_idx),
}
```

**Why dates aren't accelerated**:
- Dates are stored as `ColumnArray::Date(Vec<i32>, Option<Vec<bool>>)`
- Date is a separate enum variant from `Int32`
- No match arm for `ColumnArray::Date`, so it falls through to scalar fallback

**Key insight**: Since dates are **already i32 internally**, we can reuse existing i32 SIMD operations!

---

## Proposed Solution: Hybrid Option 2.5

### Option 2.5: Add Date Support to Columnar SIMD

**Approach**: Extend the existing columnar SIMD filter to handle Date columns by treating them as Int32 arrays.

**Implementation** (`columnar/simd_filter.rs`):

```rust
match column {
    ColumnArray::Int64(values, nulls) => {
        evaluate_predicate_i64_simd(predicate, values, nulls.as_ref())
    }

    ColumnArray::Float64(values, nulls) => {
        evaluate_predicate_f64_simd(predicate, values, nulls.as_ref())
    }

    // NEW: SIMD path for Date columns (stored as i32)
    ColumnArray::Date(values, nulls) => {
        // Convert i32 dates to i64 for SIMD comparison
        // (existing SIMD functions operate on i64)
        evaluate_predicate_date_simd(predicate, values, nulls.as_ref())
    }

    _ => evaluate_predicate_scalar(batch, predicate, column_idx),
}
```

**Add new function**:

```rust
fn evaluate_predicate_date_simd(
    predicate: &ColumnPredicate,
    values: &[i32],  // days since epoch
    nulls: Option<&Vec<bool>>,
) -> Result<Vec<bool>, ExecutorError> {
    // Convert i32 dates to i64 for SIMD operations
    let values_i64: Vec<i64> = values.iter().map(|&v| v as i64).collect();

    // Extract comparison value from predicate
    let threshold_i32 = extract_date_threshold(predicate)?;

    // Use existing i64 SIMD operations with converted threshold
    let mut result = match predicate {
        ColumnPredicate::LessThan { .. } =>
            simd_lt_i64(&values_i64, threshold_i32 as i64),
        ColumnPredicate::GreaterThan { .. } =>
            simd_gt_i64(&values_i64, threshold_i32 as i64),
        // ... other comparisons
    };

    // Handle NULL values
    apply_null_mask(&mut result, nulls);

    Ok(result)
}
```

### Why This Approach?

**Pros**:
- ✅ **Simple**: Reuses existing i64 SIMD infrastructure
- ✅ **Consistent**: Matches existing patterns for Int64/Float64
- ✅ **Targeted**: Only affects columnar aggregation path
- ✅ **Low risk**: No architectural changes
- ✅ **Proven**: i32→i64 conversion + SIMD is straightforward

**Cons**:
- Memory overhead: Temporary i64 array allocation (2x memory during comparison)
- Conversion cost: i32→i64 cast (but SIMD comparison savings should dominate)

**Alternative considered**: Add i32-native SIMD operations
- More efficient (no conversion)
- But more complex (need new SIMD functions for i32)
- Can be future optimization if conversion overhead is measurable

---

## Comparison to Original Options

### Option 1: Custom SIMD date arithmetic using `wide` crate
- **Status**: Not needed - dates are already i32, no special arithmetic required
- **Complexity**: HIGH
- **Impact**: Would add date arithmetic (e.g., `date + interval`), not just comparison

### Option 2: Arrow numeric operations on Date32
- **Status**: Arrow already supports this in Path 1, but TPC-H queries don't use Path 1
- **Complexity**: MEDIUM
- **Impact**: Wouldn't help aggregating queries

### Option 3: Extend vectorized execution coverage
- **Status**: This IS extending vectorized coverage - specifically for dates in aggregation
- **Complexity**: LOW (our proposed solution)
- **Impact**: Direct benefit for TPC-H Q1, Q6, Q12

### **Option 2.5** (Our recommendation): Add Date to columnar SIMD
- **Status**: NEW - combines benefits of Option 2 and Option 3
- **Complexity**: LOW
- **Impact**: Immediate benefit for aggregating queries with date filters

---

## Expected Performance Impact

**Q6 Projection**:
- Current: 48ms (60,000 rows, scalar date filtering)
- With SIMD: ~35-40ms estimated (20-30% improvement)
- Rationale: Date filtering is significant portion of work for simple aggregates

**Q1 Projection**:
- Current: 325ms (complex aggregation + date filter)
- With SIMD: ~300-310ms estimated (5-8% improvement)
- Rationale: Aggregation dominates, but date filter still contributes

**Q12 Projection**:
- Current: 196ms (joins + date comparisons + aggregation)
- With SIMD: ~180-190ms estimated (5-10% improvement)
- Rationale: Multiple bottlenecks, date filtering is one component

---

## Implementation Plan

### Phase 1: Core Implementation
1. Add `ColumnArray::Date` match arm to `simd_filter.rs`
2. Implement `evaluate_predicate_date_simd()` function
3. Add helper for extracting date thresholds from predicates
4. Handle NULL values correctly

### Phase 2: Testing
1. Add unit tests for Date SIMD operations
2. Test with TPC-H Q1, Q6, Q12
3. Verify correctness with edge cases (NULL dates, boundary dates)
4. Benchmark before/after performance

### Phase 3: Optimization (if needed)
1. Profile conversion overhead (i32→i64)
2. If significant, implement native i32 SIMD operations
3. Compare Arrow-based approach for aggregating queries

---

## Conclusion

**Key Finding**: Date operations in TPC-H aggregating queries currently use scalar evaluation, not SIMD.

**Root Cause**: The columnar aggregation execution path only has SIMD support for Int64 and Float64, not Date.

**Solution**: Add Date support to columnar SIMD by treating dates as i32 arrays and using existing i64 SIMD infrastructure with widening conversion.

**Impact**: Low-complexity change with measurable performance benefit for date-heavy analytical queries.

**Decision**: Proceed with **Option 2.5** implementation.
