# TPC-H Date Operations Profiling Analysis

## Issue Context

This analysis addresses issue #2522 - deciding the best approach for SIMD date arithmetic optimization (Phase 2 of #2506).

Phase 1 (PR #2520) implemented SIMD date extraction using Arrow temporal kernels. This phase investigates whether date **arithmetic** and **comparison** operations need similar optimization.

## Baseline Performance

Profiling TPC-H queries (SF 0.01) with date operations:

| Query | Execution Time | Date Operations | Dataset Size |
|-------|---------------|-----------------|--------------|
| **Q1** | 325ms | `l_shipdate <= '1998-09-01'` | 60,000 rows (lineitem) |
| **Q6** | 48ms | `l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'` | 60,000 rows (lineitem) |
| **Q12** | 196ms | Date comparisons between columns + range filtering | 15,000 orders + 60,000 lineitem |

**Key observation**: Q6 (pure date range filtering) is already quite fast at 48ms for 60,000 rows.

## Code Analysis Findings

### 1. SIMD Date Comparisons Already Exist

**Location**: `crates/vibesql-executor/src/select/vectorized/filter.rs:235-265`

```rust
fn compare_date32(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator)
    -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<Date32Array>()?;
    let val = date_to_days_since_epoch(date);
    let scalar_array = Date32Array::from(vec![val; array.len()]);

    // Uses Arrow SIMD kernels: eq, neq, lt, lt_eq, gt, gt_eq
    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)?,
        BinaryOperator::LessThan => lt(array, &scalar_array)?,
        // ... other comparisons
    };
    Ok(result)
}
```

**Finding**: Date comparisons **already use Arrow's SIMD comparison kernels**. These are the same high-performance kernels used for int64 and float64 comparisons.

### 2. Vectorized Execution Path Requirements

**Location**: `crates/vibesql-executor/src/select/executor/nonagg/simd.rs:23-32`

The vectorized/SIMD path is used when:
- Row count >= `VECTORIZE_THRESHOLD` (100 rows)
- WHERE clause is simple (supported by Arrow)
- All column types are supported by Arrow (Int64, Float64, Utf8, Date32, etc.)

**Dataset sizes** for TPC-H SF 0.01:
- lineitem: 60,000 rows ✅ (well above threshold)
- orders: 15,000 rows ✅ (well above threshold)

### 3. Critical Question: Are Queries Using SIMD Path?

The code has SIMD date comparisons, but we need to verify:
1. Are Q1, Q6, Q12 actually using the vectorized path?
2. Or are they falling back to row-based execution?
3. If falling back, why?

**Instrumentation added**: Added `SIMD_DEBUG` environment variable to trace execution path decisions.

## Execution Path Analysis

### Where is try_simd_filter() Called?

The SIMD filter optimization is attempted in **non-aggregating** SELECT queries only:
- Location: `crates/vibesql-executor/src/select/executor/nonagg/simd.rs`
- Called from: Non-aggregating query execution path

### What About Q1, Q6, Q12?

**Critical realization**: ALL three TPC-H queries with date operations include **aggregation**:

- **Q1**: `GROUP BY l_returnflag, l_linestatus` with `SUM()`, `AVG()`, `COUNT()` aggregates
- **Q6**: `SUM(l_extendedprice * l_discount)` - single aggregate
- **Q12**: `GROUP BY l_shipmode` with `SUM(CASE ...)` aggregates

**Conclusion**: These queries likely use a **different execution path** that handles aggregation + filtering together, not the `try_simd_filter()` path for non-aggregating queries.

### Aggregating Query Execution

Need to investigate:
- How does the columnar aggregation path handle WHERE clause filtering?
- Does it use vectorized operations or row-by-row evaluation?
- Location: `crates/vibesql-executor/src/select/columnar/`

## Next Steps

1. **Investigate columnar aggregation filtering** to understand how Q1/Q6/Q12 actually execute
2. **Check if columnar aggregation uses Arrow operations** for date comparisons
3. **Run instrumented profiling** to confirm execution path (may need more instrumentation in aggregation code)
4. **Measure impact** of any optimizations proposed

## Hypotheses

### Hypothesis 1: Queries Already Use SIMD (Best Case)
If Q6 is using SIMD and runs in 48ms, then:
- Date comparisons are already optimized ✅
- Further optimization would have minimal impact
- **Recommendation**: No date arithmetic SIMD needed

### Hypothesis 2: Queries Don't Use SIMD (Optimization Opportunity)
If queries fall back to row-based execution:
- Identify barriers preventing SIMD usage
- **Recommendation**: Fix barriers (Option 3 from #2522)
- This would benefit ALL operations, not just dates

### Hypothesis 3: Mixed Usage
Different parts of the query use different paths:
- Need detailed breakdown of execution phases
- **Recommendation**: Targeted optimization of slowest phase

## Architectural Insights

From issue #2522, the proposed options were:
1. **Option 1**: Custom SIMD using `wide` crate (high complexity)
2. **Option 2**: Arrow numeric operations on Date32 primitive arrays (medium complexity)
3. **Option 3**: Extend vectorized execution coverage (requires profiling first)

**Current finding supports Option 3 investigation**:
- Date comparisons already have SIMD via Arrow kernels
- The question is execution path coverage, not date operation primitives
- Profiling is needed before implementing new optimizations
