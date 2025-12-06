# Join Optimization Analysis - Phase 1

**Issue**: #2494
**Date**: 2025-11-24
**Scope**: Multi-way JOIN performance optimization (Q1, Q3, Q7, Q10, Q12)

## Executive Summary

Multiple TPC-H queries with multi-way JOINs show 90-165x performance gaps vs DuckDB:

| Query | VibeSQL | DuckDB | Gap | JOINs |
|-------|---------|---------|-----|-------|
| Q3 | 337.85ms | 2.05ms | **165x** | 3-way JOIN |
| Q7 | 449.64ms | 2.98ms | **151x** | 6-way JOIN |
| Q10 | 309.72ms | 3.25ms | **95x** | 3-way JOIN |
| Q12 | 237.44ms | 2.51ms | **94x** | 2-way JOIN |

## Current Architecture

### Join Order Optimization

VibeSQL uses a sophisticated time-bounded search approach:

1. **Join Reordering Analyzer** (`select/join/reorder.rs`)
   - Analyzes predicates to identify equijoin edges
   - Extracts local predicates (filters on single tables)
   - Estimates local selectivity (0.1 per equality predicate)
   - Builds join graph connecting tables

2. **Search-Based Optimizer** (`select/join/search/`)
   - **Algorithm**: Parallel BFS with branch-and-bound pruning
   - **Time Budget**: 1000ms default (configurable via `JOIN_REORDER_TIME_BUDGET_MS`)
   - **Search Space**: Connected subgraph enumeration (avoids CROSS JOINs)
   - **Pruning**: States with cost > best × 1.5 threshold

3. **Cost Model** (`select/join/search/cost.rs`)
   - **Cardinality**: Uses table statistics with WHERE clause selectivity applied
   - **Join Selectivity**: `1 / max(NDV_left, NDV_right)` (NDV = number of distinct values)
   - **Join Cost**: Hash join assumed with `O(left + right)` cost
   - **Output Size**: `left_card × right_card × selectivity` for INNER joins

## Root Cause Analysis

### 1. Cardinality Estimation Issues

**Problem**: The cost model may significantly overestimate intermediate result sizes.

**Evidence**:
- Line cost.rs:209-213: Uses `left_cardinality × right_cardinality × selectivity`
- This doesn't account for cascading filters in multi-way joins
- Example: Q3 has 3 filters (c_mktsegment, o_orderdate, l_shipdate) that compound

**Impact**:
```
Q3: customer (150K) → orders (1.5M) → lineitem (6M)
Without proper selectivity:
  - customer filtered to ~30K (c_mktsegment = 'BUILDING': 20% selectivity)
  - orders filtered to ~750K (o_orderdate < '1995-03-15': 50% selectivity)
  - lineitem filtered to ~3M (l_shipdate > '1995-03-15': 50% selectivity)

Current estimate: 30K × 750K × 0.001 (join sel) = 22.5M intermediate rows (!!)
Actual should be: Much lower due to cascading filters
```

**Root cause**: Independent selectivity assumption breaks down for correlated filters.

### 2. Join Selectivity Model

**Problem**: `1 / max(NDV)` assumes uniform distribution.

**Evidence**:
- Line cost.rs:117-119: Join selectivity = `1 / max(NDV_left, NDV_right)`
- Uniform distribution assumption rarely holds in real data
- Foreign key relationships (customer → orders) have specific patterns

**Impact**:
```
Q3: c_custkey = o_custkey
  - customer.c_custkey: 150K distinct values (primary key)
  - orders.o_custkey: 100K distinct values (foreign key, ~15 orders per customer)

Current: selectivity = 1 / max(150K, 100K) = 1/150K = 0.0000067
Expected: ~15 orders per customer, so selectivity should be ~15/1.5M = 0.00001

Error: 1.5x underestimate (not terrible, but compounds across joins)
```

### 3. Search Time Budget

**Problem**: 1000ms budget may be insufficient for complex queries.

**Evidence**:
- Line config.rs:29: Default time budget is 1000ms
- Q7 has 6 tables (6! = 720 possible orderings without pruning)
- With pruning, still exploring ~thousands of states

**Impact**:
- May fall back to greedy heuristic before finding optimal order
- Greedy can make poor early choices that cascade

### 4. Hash Join Cost Model

**Problem**: `O(left + right)` doesn't account for memory pressure.

**Evidence**:
- Line cost.rs:221-227: Hash join cost = `left_cardinality + right_cardinality`
- Doesn't consider hash table build cost or memory allocation overhead
- Large left sides cause memory pressure and cache misses

**Impact**:
```
If optimizer chooses large left side:
  - Build hash table from 1M rows: ~expensive (memory allocation, hashing)
  - Probe with 100K rows: cheap
  - Total: dominated by build cost

If optimizer chooses small left side:
  - Build hash table from 100K rows: cheap
  - Probe with 1M rows: ~expensive but streaming
  - Total: better memory locality
```

## Query-Specific Analysis

### Q3: 3-Way JOIN (165x gap)

```sql
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'          -- Filter: customer to 20%
  AND c_custkey = o_custkey              -- Join: customer → orders
  AND l_orderkey = o_orderkey            -- Join: orders → lineitem
  AND o_orderdate < '1995-03-15'         -- Filter: orders to 50%
  AND l_shipdate > '1995-03-15'          -- Filter: lineitem to 50%
```

**Optimal Order** (likely):
1. Start with customer (filtered by c_mktsegment: 150K → 30K rows)
2. Join orders (30K × ~15 orders/customer × 0.5 date filter = ~225K rows)
3. Join lineitem (225K × ~4 items/order × 0.5 date filter = ~450K rows)

**Suspected Issues**:
- May be starting with lineitem (6M rows) instead of customer (30K filtered)
- Cardinality estimates not accounting for filter cascading
- Hash table built from wrong side (large instead of small)

### Q7: 6-Way JOIN (151x gap)

```sql
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE s_suppkey = l_suppkey
  AND o_orderkey = l_orderkey
  AND c_custkey = o_custkey
  AND s_nationkey = n1.n_nationkey
  AND c_nationkey = n2.n_nationkey
  AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
       OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
  AND l_shipdate >= '1995-01-01'
  AND l_shipdate <= '1996-12-31'
```

**Optimal Order** (likely):
1. Start with nation n1 (filtered to 2 rows: FRANCE or GERMANY)
2. Join nation n2 (filtered to 2 rows: FRANCE or GERMANY)
3. Early elimination cascades through remaining tables

**Suspected Issues**:
- OR condition complicates selectivity estimation
- 6! = 720 possible orderings, search may time out
- May not identify nation tables as most selective starting point

## Optimization Strategy

### Phase 2: Improve Cardinality Estimation (Priority: HIGH)

**Goal**: More accurate intermediate result size estimates

**Changes**:
1. **Cascading Filter Awareness**
   - Track filter application across join chain
   - Use conditional selectivity: `P(A|B)` not just `P(A) × P(B)`
   - Example: `o_orderdate < date` has different selectivity when joined to filtered customers

2. **Better Default Selectivity**
   - Current: 30% per predicate (pessimistic)
   - Proposed: Use predicate-specific heuristics
     - Equality: 10%
     - Range: 25% (depends on range width)
     - IN clause: 5% × num_values

3. **FK Relationship Detection**
   - Detect PK-FK relationships from statistics
   - Use actual cardinality ratios (e.g., ~15 orders per customer)
   - Improves join selectivity estimates

### Phase 3: Improve Join Ordering (Priority: HIGH)

**Goal**: Better search and ordering decisions

**Changes**:
1. **Increase Time Budget for Complex Queries**
   - Adaptive budget: base 1000ms + 200ms per table over 4
   - Q7 (6 tables): 1000 + (6-4)×200 = 1400ms budget
   - Q21 (worst case): may need 2-3 seconds

2. **Improved Cost Model**
   - Account for hash table build cost: `build_cost × log(left_card)`
   - Penalize large intermediate results more heavily
   - Consider memory pressure in cost function

3. **Better Heuristics**
   - Prioritize highly filtered tables (current approach is good)
   - Break ties using join selectivity
   - Prefer FK → PK direction (fewer rows on left)

### Phase 4: Join Execution Optimizations (Priority: MEDIUM)

**Goal**: Faster join execution regardless of ordering

**Changes**:
1. **Hash Table Optimizations**
   - Bloom filters for early rejection
   - Smaller hash table from right side
   - Reduced memory allocation

2. **Vectorized Execution**
   - Process batches of rows through join
   - Better cache locality

3. **Adaptive Join Strategy**
   - Use nested loop for very small tables (< 1000 rows)
   - Use hash join for medium tables
   - Consider sort-merge for large sorted inputs

## Success Metrics

**Target**: 5-10x improvement per query

- Q3: 338ms → 50-70ms (5-7x improvement)
- Q7: 450ms → 50-90ms (5-9x improvement)
- Q10: 310ms → 50-60ms (5-6x improvement)
- Q12: 237ms → 40-50ms (5-6x improvement)

**Validation**:
- Run TPC-H benchmark suite
- Check for regressions on other queries
- Verify correctness with result comparisons

## Implementation Plan

### Week 1: Cardinality Estimation
- [ ] Implement cascading filter tracking
- [ ] Add FK relationship detection
- [ ] Improve default selectivity heuristics
- [ ] Add logging to compare estimated vs actual cardinalities

### Week 2: Join Ordering
- [ ] Implement adaptive time budget
- [ ] Improve cost model (hash table build cost)
- [ ] Add FK-aware join ordering
- [ ] Profile and validate improvements on Q3, Q7

### Week 3: Join Execution
- [ ] Add Bloom filters for hash joins
- [ ] Optimize hash table construction
- [ ] Implement vectorized probe phase
- [ ] Benchmark and validate improvements

### Week 4: Testing and Validation
- [ ] Full TPC-H benchmark suite
- [ ] Regression testing
- [ ] Performance profiling
- [ ] Documentation

## Next Steps

1. Build and run profiling benchmark with `JOIN_REORDER_VERBOSE=1`
2. Capture actual join order decisions for Q3, Q7
3. Compare estimated vs actual cardinalities
4. Identify specific join order mistakes
5. Begin implementation of cardinality improvements

## References

- Issue #2494: Multi-way JOIN performance optimization
- Issue #2490: TPC-H performance tracking
- Issue #2491: Q21 worst-case join performance
- Codebase files:
  - `crates/vibesql-executor/src/select/join/reorder.rs`
  - `crates/vibesql-executor/src/select/join/search/`
  - `crates/vibesql-executor/src/select/join/search/cost.rs`
  - `crates/vibesql-executor/src/select/join/search/bfs.rs`
  - `crates/vibesql-executor/src/select/join/search/config.rs`
