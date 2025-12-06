# Memory Allocation Analysis - TPC-H Performance Optimization

## Executive Summary

Based on comprehensive code analysis of the VibeSQL executor, we've identified multiple allocation hotspots contributing to the 100-3000x performance gap with DuckDB. This document outlines the findings and optimization strategy.

## Key Findings

### 1. Hot Path Allocations

#### A. Expression Evaluation (Per Row)
**Location**: `crates/vibesql-executor/src/evaluator/expressions/eval.rs`

**Problem**: Every expression evaluation may allocate:
- New `SqlValue` instances for intermediate results
- HashMap entries for CSE (Common Subexpression Elimination) cache
- RefCell borrows and potential reallocations

**Frequency**: O(rows × expressions) - millions of allocations for Q6

**Code Evidence**:
```rust
// Line 24-31: CSE cache allocates on every unique expression
LruCache<u64, SqlValue>

// eval() returns Result<SqlValue, ...> - ownership transfer
// Intermediate calculations create temporary SqlValue instances
```

#### B. Filter Bitmap Creation
**Location**: `crates/vibesql-executor/src/select/columnar/filter/mod.rs`

**Problem**: Filter evaluation allocates new `Vec<bool>` for every batch:
```rust
// Line 36: Allocates full bitmap for all rows
vec![false; row_count]
```

**Frequency**: O(batches) - reallocates for each query phase

#### C. Row Materialization
**Location**: `crates/vibesql-executor/src/select/columnar/batch.rs`

**Problem**: Converting columnar → row format allocates extensively:
```rust
// Line 180-183: to_rows() allocates per row
let mut rows = Vec::with_capacity(self.row_count);
for row_idx in 0..self.row_count {
    let mut values = Vec::with_capacity(self.columns.len());
    ...
}
```

**Impact**: Double allocation (outer Vec + inner Vec per row)

#### D. Aggregate Accumulators
**Location**: `crates/vibesql-executor/src/select/grouping/aggregates.rs`

**Problem**: DISTINCT aggregates allocate HashSet per group:
```rust
// Lines 9-14: HashSet for each DISTINCT aggregate
Count { count: i64, distinct: bool, seen: Option<HashSet<SqlValue>> }
```

**Frequency**: O(groups × distinct_aggregates)

#### E. Hash Join Build Phase
**Location**: `crates/vibesql-executor/src/select/join/hash_join/build.rs`

**Problem**: Parallel build creates per-thread HashMaps then merges:
```rust
// Multiple HashMap allocations + merge overhead
// Each thread: HashMap::new()
// Then: sequential merge of partial tables
```

**Frequency**: O(threads × distinct_keys)

### 2. Memory Layout Issues

#### A. SqlValue Enum Overhead
**Location**: `vibesql-types` crate

**Problem**: `SqlValue` enum has large memory footprint:
- Size: likely 24-32 bytes (enum tag + largest variant)
- Cache inefficient: accessing array of SqlValue causes cache misses
- Type matching overhead on every access

**Evidence**: Columnar batch converts TO/FROM SqlValue repeatedly

#### B. Row-Oriented Intermediate Results
**Location**: Throughout executor

**Problem**: Despite having columnar batches, operations convert back to rows:
- `to_rows()` called frequently
- Joins materialize row-by-row
- Aggregation works on `Vec<Row>`

**Impact**: Cache thrashing, pointer chasing, poor SIMD utilization

#### C. Non-Aligned Allocations
**Problem**: No explicit cache-line alignment (64 bytes on modern CPUs)
- Vec allocations not guaranteed aligned
- Column arrays may span cache lines
- False sharing possible in parallel ops

### 3. Buffer Reuse Opportunities

#### Current State: Zero Buffer Reuse
Every operation allocates fresh:
- Filter bitmaps reallocated per batch
- Expression results allocated per evaluation
- Join output allocates from scratch
- Group-by hash tables recreated

**Missed Opportunities**:
1. Query-lifetime arena allocator for temp buffers
2. Pooled bitmaps for filters (fixed sizes: 256, 1024, 4096, etc.)
3. Reusable result buffers for expressions
4. Pre-sized hash tables based on cardinality hints

## Quantitative Analysis

### Allocation Estimates for Q6 (SF 0.01 ~ 60K rows)

```sql
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
```

**Estimated Allocations**:

1. **Scan Phase**:
   - Read ~60,000 rows
   - If columnar: 9 column arrays × Vec allocation
   - If row-oriented: 60,000 Row allocations

2. **Filter Phase**:
   - Bitmap: 60,000 bools = ~60 KB (1 allocation)
   - Predicate evaluation: 4 predicates × 60K rows = 240K SqlValue temps
   - **Total: ~240,000+ allocations**

3. **Projection Phase**:
   - Expression: `l_extendedprice * l_discount`
   - 60K multiplication results (SqlValue::Float)
   - CSE cache entries (if enabled)
   - **Total: ~60,000 allocations**

4. **Aggregation Phase**:
   - 1 accumulator (SUM)
   - 60K accumulate() calls
   - If DISTINCT: 60K HashSet insertions
   - **Total: 1 + 0-60K allocations**

**Grand Total**: **~300,000 - 360,000 allocations** for a simple query!

**DuckDB Comparison**: High-performance databases aim for <100 allocations per query through:
- Buffer pooling
- Arena allocators
- Vectorized operations on pre-allocated buffers

### Memory Bandwidth Impact

With 300K allocations:
- Malloc overhead: 16-32 bytes per allocation = 4.8-9.6 MB
- Fragmentation: ~10-20% overhead = 1-2 MB
- Cache pollution: Constant malloc/free thrashes L1/L2/L3
- **Total wasted bandwidth**: ~10-15 MB for Q6 (vs. ~2 MB actual data)

## Optimization Strategy

### Phase 1: Buffer Pre-allocation (Week 1-2)

**Goal**: Reduce allocations per query by 80%+

1. **Query-Lifetime Arena Allocator**
   - Create `QueryArena` struct with bump allocator (bumpalo crate)
   - Pass `&Arena` through executor context
   - Allocate all temp buffers from arena
   - Reset arena between top-level queries

2. **Pooled Filter Bitmaps**
   - Create `BitmapPool` with sizes: [256, 1024, 4096, 16384, 65536]
   - Checkout bitmap at start of filter op
   - Return to pool when done (don't deallocate)
   - Separate pools per thread to avoid contention

3. **Pre-sized Hash Tables**
   - Estimate join cardinality from stats
   - Pre-allocate `HashMap::with_capacity(estimate)`
   - Reduces rehashing from O(log n) resizes to O(1)

4. **Result Buffer Reuse**
   - Pass mutable `&mut Vec<SqlValue>` for expression results
   - Clear and reuse instead of allocate
   - Store in executor context, not per-call

**Expected Impact**: 5-10x fewer allocations

### Phase 2: Columnar Processing (Week 2-3)

**Goal**: Eliminate row materialization overhead

1. **Keep Data Columnar**
   - Never call `to_rows()` in hot paths
   - Implement columnar filter (already exists, use it!)
   - Columnar projection without row materialization
   - Columnar aggregate evaluation

2. **SIMD-Friendly Memory Layout**
   - Ensure column arrays are Vec<T> of primitives
   - Add `#[repr(align(64))]` for cache alignment
   - Use `Vec::with_capacity()` with padded sizes (multiples of 64)
   - Consider SmallVec for tiny columns (<= 4 elements inline)

3. **Zero-Copy Slicing**
   - Use `&[T]` slices instead of `Vec<T>` where possible
   - Avoid cloning column arrays
   - Reference counting (Rc/Arc) only when necessary

**Expected Impact**: 3-5x improvement from cache locality + SIMD

### Phase 3: Specialized Fast Paths (Week 3-4)

**Goal**: Ultra-fast execution for common patterns

1. **Simple Scan + Filter + Agg**
   - Detect Q6-style queries
   - Single-pass streaming evaluation
   - No intermediate materialization
   - Direct accumulation from columns

2. **COUNT(*) Fast Path**
   - Already exists (line 42-54 in aggregate_function.rs)
   - Extend to COUNT(column) with NULL bitmap
   - O(1) vs. O(n) row iteration

3. **Predicate Pushdown**
   - Evaluate filters during scan
   - Skip row creation for filtered-out rows
   - Bitmap-based selection

**Expected Impact**: 2-3x for simple queries

### Phase 4: Advanced Techniques (Week 4+)

1. **Adaptive Buffer Sizing**
   - Track allocation patterns per query
   - Learn optimal buffer sizes
   - Cache in query plan metadata

2. **NUMA Awareness** (if applicable)
   - Allocate on same NUMA node as thread
   - Thread affinity for consistent placement

3. **Lazy Materialization**
   - Late materialization (don't build rows until needed)
   - Projection pushdown (only select needed columns)
   - Expression delay (evaluate only surviving rows)

## Success Metrics

### Quantitative Targets

| Metric | Current | Target (Phase 1-2) | Target (Phase 3-4) |
|--------|---------|-------------------|-------------------|
| **Allocations/1K rows** | ~5,000 | <500 | <50 |
| **Peak Memory (Q6)** | ~15 MB | ~5 MB | ~3 MB |
| **Q6 Runtime** | ~200ms (est) | ~40ms | ~10-20ms |
| **Q7 Runtime** | TBD | 50% reduction | 70% reduction |
| **Cache Miss Rate** | TBD | 50% reduction | 70% reduction |

### Validation Checklist

- [ ] Heaptrack / dhat shows <50 allocations/1K rows
- [ ] Perf shows improved cache hit rate (>95% L1, >90% L2)
- [ ] No memory leaks (valgrind clean)
- [ ] All TPC-H queries pass correctness tests
- [ ] 2-3x improvement on Q1, Q6, Q12 (simple aggregates)
- [ ] 1.5-2x improvement on Q3, Q5, Q8 (joins + aggregates)

## Implementation Priority

### High Priority (Week 1)
1. Query arena allocator
2. Buffer pooling for filters
3. Pre-sized hash tables

### Medium Priority (Week 2)
1. Columnar filter (already exists, just use it)
2. Eliminate to_rows() calls
3. Result buffer reuse

### Low Priority (Week 3+)
1. SIMD alignment
2. Specialized fast paths
3. Advanced techniques

## Related Issues

- #2490 - TPC-H performance tracking
- #2493 - Q6 profiling (prerequisite)
- #2496 - SIMD optimization (depends on memory layout)
- #2298 - DuckDB comparison (reference implementation)

## References

### Papers
- "MonetDB/X100: Hyper-Pipelining Query Execution" (CIDR 2005)
  - Vectorized execution with batches
  - Minimizes interpretation overhead
- "Efficiently Compiling Efficient Query Plans for Modern Hardware" (VLDB 2011)
  - Code generation vs. interpretation
  - Memory hierarchy optimization

### Crates
- `bumpalo` - Arena allocator (fast bump-pointer allocation)
- `typed-arena` - Typed arena (drop support)
- `smallvec` - Inline small vectors (avoid heap for <= 4 elements)

## Next Steps

1. **Baseline Measurement** (Today)
   - Run Q6 with dhat profiling
   - Capture allocation counts and hotspots
   - Benchmark current performance

2. **Implement Arena** (Day 1-2)
   - Add bumpalo dependency
   - Create QueryArena wrapper
   - Thread through executor context
   - Convert first hot path (filter bitmaps)

3. **Measure & Iterate** (Day 3-5)
   - Re-profile with arena
   - Validate allocation reduction
   - Benchmark performance gain
   - Expand to next hot path

---

**Last Updated**: 2025-11-24
**Owner**: Builder Agent (Issue #2497)
