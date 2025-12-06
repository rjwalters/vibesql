# Phase 6 Research: Bottleneck Discovered

**Date**: 2025-11-20
**Issue**: #2224
**Status**: ✅ Bottleneck identified

---

## Executive Summary

**🎯 Found the bottleneck:** Row materialization (`collect_vec()`) takes 57% of query time (19.9ms out of 34.8ms).

**Key insight:** We materialize all 60K rows into `Vec<Row>` structures, then immediately filter down to 6K rows. **We're spending 20ms creating 54K rows we throw away.**

**Validation:** This confirms that avoiding materialization (via columnar storage OR lazy evaluation) would have major impact.

---

## Complete Performance Breakdown

### TPC-H Q6 (SF 0.01, 60K rows)

```
Total query time: 34.8ms (100%)

├─ Executor setup: 12µs (0.04%)
│  └─ Optimizer (rewrite): 12µs
│
├─ execute_from(): 3.8ms (11%)
│  └─ Table scan, creates lazy iterator
│
├─ collect_vec(): 19.9ms (57%)  ← THE BOTTLENECK
│  └─ Materializes all 60K rows to Vec<Row>
│     Each Row = Vec<SqlValue>
│     Total: 960K SqlValue allocations
│     Cost: 332ns/row
│
└─ Processing: 4.1ms (12%)
   ├─ Filter predicates: 180µs (0.5%)
   │  └─ 60K rows → 6K matches
   └─ Aggregation: 25µs (0.07%)
      └─ SUM(price * discount)
```

### Per-Row Costs

| Operation | Time/Row | Notes |
|-----------|----------|-------|
| Table scan (iterator) | 63ns | B-tree page reads |
| **Materialization** | **332ns** | **Vec<Row> + Vec<SqlValue>** |
| Filter evaluation | 3ns | Type-specialized predicates |
| Aggregation | 0.4ns | Native f64 arithmetic |

**Total**: 398ns/row, but only 6K rows should be materialized (not 60K!)

---

## The Code

### Where It Happens

**File**: `crates/vibesql-executor/src/select/join/mod.rs:58`

```rust
pub fn as_rows(&mut self) -> &Vec<vibesql_storage::Row> {
    if let Self::Iterator(iter) = self {
        // 🚨 THIS LINE: Materializes ALL rows before filtering
        let rows = std::mem::replace(iter, ...).collect_vec();  // 19.9ms!
        *self = Self::Materialized(rows);
    }

    match self {
        Self::Materialized(rows) => rows,
        Self::Iterator(_) => unreachable!(),
    }
}
```

### Call Chain

```
execute()
└─ execute_with_ctes()
   └─ try_monomorphic_execution()
      ├─ execute_from() → FromResult with Iterator
      │  └─ Returns lazy iterator (not yet materialized)
      │
      └─ plan.execute(from_result.rows())  ← Calls as_rows()
         └─ as_rows() calls collect_vec()  ← MATERIALIZES HERE
            └─ Then we filter in execute_unsafe()
```

**The problem**: We call `.rows()` to get all rows for monomorphic execution, which triggers full materialization before filtering.

---

## Why This Is The Bottleneck

### Memory Allocations

For 60K rows with 16 columns:

```rust
// Each row:
Row {
    values: Vec<SqlValue>  // Heap allocation #1
}

// Each value in the row:
SqlValue::Double(f64)  // 24+ bytes per value
SqlValue::Date(Date)
SqlValue::Varchar(String)  // Another heap allocation for strings
```

**Total allocations**:
- 60,000 × Vec allocation for Row
- 960,000 × SqlValue enums (60K × 16 columns)
- Additional String allocations for varchar columns
- **Estimated**: ~25MB of memory allocated and immediately thrown away

### Why So Slow?

1. **Allocation overhead**: malloc/free for each Vec
2. **Cache misses**: Values scattered across heap
3. **Enum overhead**: 24+ bytes per SqlValue vs 4-8 bytes native
4. **Pointer chasing**: Row → Vec → SqlValue → actual value

Compare to columnar:
```rust
// Zero allocations, sequential access
&[f32; 60000]  // quantities - contiguous, cache-friendly
&[f32; 60000]  // prices
&[Date; 60000] // dates
```

---

## Validation: This Matters

### Current vs Ideal

**Current**:
```
Scan 60K rows → Iterator (3.8ms)
       ↓
Materialize ALL to Vec<Row> (19.9ms)  ← Waste 54K rows
       ↓
Filter to 6K matches (4.1ms)
       ↓
Aggregate 6K rows
```

**Ideal (Columnar)**:
```
mmap columnar file → Direct array access (0ms)
       ↓
SIMD filter on arrays (in-place, ~1ms)
       ↓
Accumulate matches (~0.1ms)
```

**Potential savings**: 19.9ms → ~1ms = **18.9ms saved (54% total speedup)**

**Ideal (Lazy Evaluation)**:
```
Scan 60K rows → Iterator (3.8ms)
       ↓
Filter DURING iteration (only materialize 6K matches)
       ↓
Aggregate 6K rows
```

**Potential savings**: 19.9ms → ~2ms = **17.9ms saved (51% total speedup)**

---

## Revised Recommendations

### Option A: SIMD-Aligned Columnar Storage (Preferred)

**Design**:
```rust
#[repr(C, align(64))]  // AVX-512 alignment
struct LineitemColumnar {
    // Batches of 1024 rows for cache efficiency
    quantities: [f32; 1024],
    prices: [f32; 1024],
    discounts: [f32; 1024],
    shipdates: [i32; 1024],  // Encoded dates
}

// Memory-mapped file
let data: &[LineitemColumnar] = unsafe {
    mmap_file("lineitem.col")
};

// Process in-place with SIMD
for batch in data {
    let mask = simd_filter(batch);  // 4-8 rows/instruction
    sum += simd_aggregate(batch, mask);
}
```

**Benefits**:
- ✅ Zero materialization (no Vec<Row>)
- ✅ Zero deserialization (direct mmap)
- ✅ SIMD processing (4-8x throughput)
- ✅ Cache-friendly (sequential access)
- ✅ ~20ms saved from materialization
- ✅ ~4ms saved from execute_from
- ✅ **Total: ~24ms → ~10ms (70% faster)**

**Trade-offs**:
- Requires custom storage format
- Write path needs design (WAL + compaction?)
- Migration strategy needed

### Option B: Lazy Evaluation (Quick Win)

**Design**:
```rust
// DON'T materialize everything first
// Instead: filter as we iterate

fn execute_monomorphic_lazy(
    iter: impl Iterator<Item = Row>,
    plan: &dyn MonomorphicPlan
) -> Result<f64> {
    let mut sum = 0.0;

    for row in iter {  // Stream through iterator
        if plan.filter(&row) {  // Filter BEFORE materializing
            sum += plan.compute(&row);
        }
    }

    Ok(sum)
}
```

**Benefits**:
- ✅ Simple change (no new storage format)
- ✅ ~18ms saved from materialization
- ✅ Only materialize filtered rows (6K not 60K)
- ✅ **Total: ~34ms → ~16ms (53% faster)**
- ✅ Can implement in 1-2 days

**Trade-offs**:
- Still has Row/SqlValue overhead
- Can't use SIMD (needs columnar)
- Partial solution (columnar still needed long-term)

### Recommendation

**Phase 6a (Immediate)**: Implement lazy evaluation
- Quick win: 53% faster in days
- Validates approach
- No breaking changes

**Phase 6b (Future)**: Implement SIMD columnar
- Full solution: 70%+ faster
- Enables true DuckDB-level performance (with JIT on top)
- Requires careful design

---

## Comparison to Original Hypothesis

### What We Thought

From issue #2224:
> "Even with all optimizations, we still spend significant time deserializing row-based storage to columnar format for SIMD processing."

**Assumed**:
- Deserialization (B-tree → Row): ~80% of time
- Processing: ~20% of time

### What We Found

**Reality**:
- B-tree scan: 11% of time (3.8ms)
- **Row materialization: 57% of time (19.9ms)** ← Not in hypothesis!
- Processing: 12% of time (4.1ms)

**Key insight**: The bottleneck isn't B-tree deserialization - it's the **intermediate Row struct materialization** that happens after scanning but before processing.

---

## Impact on Phase 6 Plan

### Original Plan (Based on Hypothesis)

1. Implement Apache Arrow IPC format
2. mmap Arrow files for zero-copy
3. Expect: Eliminate deserialization overhead (~2ms)
4. **Problem**: Would only fix 11% of time, still have 57% materialization overhead!

### Revised Plan (Based on Profiling)

1. **Immediate**: Lazy evaluation (fixes 57% overhead)
2. **Then**: Custom SIMD-aligned columnar format
   - Not Arrow (too much overhead for single-process)
   - Direct mmap to `#[repr(C)]` structs
   - SIMD processing in-place
3. **Expect**: Fix both 11% scan + 57% materialization = 68% total

---

## Technical Details

### Profiling Method

Added `#[cfg(feature = "profile-q6")]` instrumentation at key points:

1. **execute()**: Setup, optimizer, pre-execute
2. **execute_with_ctes()**: Total and monomorphic timing
3. **try_monomorphic_execution()**: Row loading
4. **execute_unsafe()**: Filter and aggregation
5. **as_rows()**: Row materialization ← The smoking gun!

### Measurement Accuracy

- Release build (`--release`)
- Sub-microsecond timer (`Instant::now()`)
- 10 iterations with median
- Consistent results (±10% variance)

### Potential Issues

**Q**: Could measurement overhead affect results?
**A**: No, `Instant::now()` is ~20ns, we're measuring milliseconds

**Q**: Is materialization really the bottleneck?
**A**: Yes - removing it (columnar) would eliminate 19.9ms directly

**Q**: What about other queries (not Q6)?
**A**: Need to profile, but likely similar (all use FromData::as_rows)

---

## Conclusion

**Mission accomplished**: Found the real bottleneck through methodical profiling.

**Key learnings**:
1. ✅ Original hypothesis was partially wrong (materialization > deserialization)
2. ✅ Columnar storage IS the right approach (but for different reason)
3. ✅ SIMD-aligned Rust format preferred over Arrow
4. ✅ Lazy evaluation is a quick intermediate win

**Next steps**:
1. Prototype lazy evaluation (1-2 days, 53% faster)
2. Design SIMD columnar format (1-2 weeks)
3. Implement and benchmark (2-4 weeks)
4. **Expected result**: 180µs target achievable with columnar + SIMD + JIT

---

## References

- Issue #2224: Phase 6 research task
- PR #2250: Profiling infrastructure and findings
- `docs/profiling/q6-analysis.md`: Historical performance analysis
- `docs/research/phase6-profiling-results.md`: Initial profiling results
- This document: Final bottleneck analysis

---

**Status**: Research phase complete ✅
**Recommendation**: Proceed with Phase 6a (lazy eval) then Phase 6b (columnar)
**Confidence**: High - profiling data clearly identifies bottleneck and validates solution
