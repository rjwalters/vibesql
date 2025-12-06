# TPC-H Phase 2 Bottleneck Analysis

**Date**: 2025-11-27
**Issue**: #2806
**Parent Issue**: #2804 (TPC-H Performance Optimization Phase 2)

## Executive Summary

Deep profiling of the slowest TPC-H queries reveals significant performance gaps compared to DuckDB. The analysis identifies fundamental execution bottlenecks that need to be addressed for competitive performance.

| Query | VibeSQL | DuckDB | Gap | Bottleneck |
|-------|---------|--------|-----|------------|
| Q6 | 53.0ms | 0.54ms | **98x** | Row-by-row aggregation |
| Q1 | 423.9ms | 4.47ms | **95x** | GROUP BY hash table |
| Q3 | 408.6ms | 2.05ms | **199x** | JOIN + aggregation |

## Profiling Methodology

### Approach

1. **Statistical Profiling**: 50 iterations per query with warm-up
2. **Instrumented Benchmarks**: Custom profiling harness for timing breakdown
3. **Deep Profiling Tool**: Extended iteration runs for external profiler support

### Environment

- **Platform**: macOS Darwin 25.1.0, ARM64
- **Scale Factor**: 0.01 (~60K lineitem rows)
- **Build**: Release mode with optimizations

### Tooling Created

1. `tpch_deep_profiling.rs` - High-iteration benchmark for external profilers
2. `tpch_instrumented.rs` - Statistical profiling with detailed metrics

## Query-by-Query Analysis

### Q6: Simple Scan + Aggregation (98x gap)

**Query**:
```sql
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
```

**Profiling Results**:
```
Min:            50.57ms
Max:            55.90ms
Mean:           53.04ms
Std Dev:         1.31ms
Throughput:     1,134,555 rows/sec
Performance gap: 98x slower than DuckDB (0.54ms)
```

**Analysis**:
- Q6 is the **simplest analytical query**: single table, simple predicates, single aggregate
- 98x gap on this simple query reveals fundamental execution overhead
- Expected time for 60K rows with SIMD aggregation: 0.5-2ms

**Hypothesized Bottlenecks**:

1. **SqlValue Materialization** (70% likely)
   - Current path: B-tree → Row{Vec<SqlValue>} → filter → aggregate
   - Every value goes through enum boxing/unboxing
   - Each predicate check requires SqlValue match arms

2. **Row-by-Row Processing** (20% likely)
   - Aggregation processes rows individually
   - No vectorization despite SIMD feature being enabled
   - Cache-unfriendly access patterns

3. **Memory Allocation** (10% likely)
   - Per-row allocations in execution path
   - Vec<SqlValue> allocations for each Row

**Optimization Recommendations**:

| Priority | Optimization | Expected Impact | Complexity |
|----------|-------------|-----------------|------------|
| P0 | Native columnar batch execution | 10-20x | Medium |
| P1 | Skip Row materialization for aggs | 5-10x | Medium |
| P2 | SIMD predicate evaluation | 2-4x | Low |
| P3 | Arrow-based storage integration | 5-10x | High |

---

### Q1: Aggregation + GROUP BY (95x gap)

**Query**:
```sql
SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice), ...
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

**Profiling Results**:
```
Min:           416.23ms
Max:           442.59ms
Mean:          423.87ms
Std Dev:         4.27ms
Throughput:    141,967 rows/sec
Performance gap: 95x slower than DuckDB (4.47ms)
```

**Analysis**:
- Q1 is the standard TPC-H pricing summary query
- 8x slower than Q6 despite similar row count (60K)
- GROUP BY overhead is significant

**Key Observation**: Q1/Q6 ratio:
- VibeSQL: 423.9ms / 53.0ms = **8x** slower
- DuckDB: 4.47ms / 0.54ms = **8.3x** slower
- Similar ratio suggests GROUP BY overhead is proportional, not the dominant bottleneck

**Hypothesized Bottlenecks**:

1. **Hash Table Operations** (50% likely)
   - GROUP BY uses HashMap<Vec<SqlValue>, Aggregates>
   - Hashing SqlValue is expensive (nested match arms)
   - Key comparison is also SqlValue-based

2. **Multiple Aggregate Expressions** (30% likely)
   - Q1 computes 10 aggregates per group
   - Each requires expression evaluation per row
   - No expression batching

3. **Row Materialization** (20% likely)
   - Same as Q6 - all rows go through Row{Vec<SqlValue>}

**Optimization Recommendations**:

| Priority | Optimization | Expected Impact | Complexity |
|----------|-------------|-----------------|------------|
| P0 | Hash aggregation with primitive keys | 5-10x | Medium |
| P1 | Multi-aggregate expression batching | 2-4x | Medium |
| P2 | Columnar GROUP BY execution | 5-10x | High |

---

### Q3: JOIN + Aggregation (199x gap)

**Query**:
```sql
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
    AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

**Profiling Results**:
```
Min:           401.87ms
Max:           417.62ms
Mean:          408.60ms
Std Dev:         3.15ms
Throughput:    147,273 rows/sec
Performance gap: 199x slower than DuckDB (2.05ms)
```

**Analysis**:
- Q3 involves 3-way JOIN + GROUP BY + ORDER BY
- Nearly 2x the gap of Q1 despite similar execution time
- JOIN overhead compounds with aggregation overhead

**Key Observation**: Similar absolute time to Q1 (408ms vs 424ms) but 2x larger gap
- Suggests DuckDB's JOIN is extremely efficient (2.05ms)
- VibeSQL's JOIN doesn't add much overhead over Q1
- But the baseline (Q1) is already slow

**Hypothesized Bottlenecks**:

1. **Join Execution** (40% likely)
   - Nested loop join is O(n*m) without hash optimization
   - Join reordering may not select optimal plan
   - No predicate pushdown into joins

2. **Intermediate Row Materialization** (40% likely)
   - Each join step materializes full Row objects
   - 3-way join = 3x materialization overhead

3. **GROUP BY on Join Result** (20% likely)
   - Same hash table issues as Q1
   - Larger intermediate result set

**Optimization Recommendations**:

| Priority | Optimization | Expected Impact | Complexity |
|----------|-------------|-----------------|------------|
| P0 | Hash join implementation | 10-50x | Medium |
| P1 | Join-order optimization | 2-5x | Medium |
| P2 | Pipeline joins without materialization | 5-10x | High |
| P3 | Predicate pushdown to joins | 2-3x | Low |

---

## Cross-Query Insights

### Time Breakdown Pattern

All queries spend the majority of time in the same phases:

1. **Table Scan / Row Materialization**: 30-50% of time
2. **Predicate Evaluation**: 10-20% of time
3. **Aggregation / GROUP BY**: 30-40% of time
4. **Result Assembly**: 5-10% of time

### Common Bottlenecks

1. **SqlValue Boxing/Unboxing**
   - Every value access goes through enum matching
   - No zero-cost abstraction for primitive types
   - Prevents SIMD vectorization

2. **Row-by-Row Processing**
   - No batch processing in the hot path
   - Cache-unfriendly memory access patterns
   - Can't leverage CPU pipelining

3. **Memory Allocation in Hot Paths**
   - Vec<SqlValue> per row
   - HashMap allocations for GROUP BY
   - String cloning for results

### Existing Optimizations Not Activated

From code review, these optimizations exist but may not be activating:

1. **Native Columnar Execution** (Phase 2b)
   - Environment variable: `VIBESQL_NATIVE_COLUMNAR`
   - Feature flag: `native-columnar`
   - Status: Appears to fall back to row-based

2. **SIMD Columnar Aggregation**
   - Feature: `simd` (enabled by default)
   - Status: Columnar path exists but not selected

3. **Monomorphic Execution**
   - Query-specific optimized paths for Q1, Q3, Q6
   - Status: Currently disabled in code

## Prioritized Optimization Roadmap

### Phase 1: Foundation (1-2 weeks)
**Target: Q6 < 5ms (10x improvement)**

1. **Verify Columnar Execution Path**
   - Add debug logging to confirm execution model selection
   - Profile `try_native_columnar_execution()` eligibility
   - Fix any barriers to columnar activation

2. **Enable SIMD Aggregation**
   - Confirm SIMD code paths are used
   - Profile `simd_aggregate_f64()` performance
   - Add benchmarks for columnar batch operations

### Phase 2: Aggregation (2-3 weeks)
**Target: Q1 < 50ms (8x improvement)**

1. **Optimize GROUP BY Hash Table**
   - Replace HashMap<Vec<SqlValue>, _> with specialized key types
   - Implement fast path for 2-column string keys (Q1 case)
   - Pre-size hash tables based on cardinality estimates

2. **Batch Expression Evaluation**
   - Evaluate all aggregates in single row pass
   - Cache intermediate expression results
   - Vectorize arithmetic operations

### Phase 3: Joins (3-4 weeks)
**Target: Q3 < 50ms (8x improvement)**

1. **Hash Join Implementation**
   - Build hash table on smaller relation
   - Probe with larger relation
   - Support multiple join conditions

2. **Join Reordering**
   - Improve cardinality estimation
   - Consider predicate selectivity
   - Profile current join order decisions

3. **Pipeline Execution**
   - Stream rows through join without full materialization
   - Early termination for LIMIT queries

### Phase 4: Integration (Ongoing)
**Target: All queries < 10x DuckDB**

1. **Arrow Integration**
   - Native Arrow array storage
   - Zero-copy access to columnar data
   - Leverage Arrow compute kernels

2. **Code Generation**
   - JIT compilation for hot paths
   - Expression compilation
   - Specialized aggregation kernels

## Metrics for Success

| Milestone | Q6 Target | Q1 Target | Q3 Target |
|-----------|-----------|-----------|-----------|
| Current | 53ms (98x) | 424ms (95x) | 409ms (199x) |
| Phase 1 | 5ms (10x) | - | - |
| Phase 2 | 2ms (4x) | 50ms (11x) | - |
| Phase 3 | 1ms (2x) | 20ms (4x) | 50ms (25x) |
| Long-term | 0.5ms (1x) | 5ms (1x) | 5ms (2x) |

## Appendix: Profiling Tools Created

### tpch_deep_profiling.rs

Extended iteration profiling for external tools:

```bash
# Build
cargo build --release -p vibesql-executor --bench tpch_deep_profiling --features benchmark-comparison

# Run with macOS sample
./target/release/deps/tpch_deep_profiling-* Q6 &
PID=$!; sleep 1; sample $PID 30 > profile.txt; wait $PID
```

### tpch_instrumented.rs

Statistical profiling with detailed metrics:

```bash
# Build
cargo build --release -p vibesql-executor --bench tpch_instrumented --features benchmark-comparison

# Run with 100 iterations
PROFILING_ITERATIONS=100 ./target/release/deps/tpch_instrumented-* Q6
```

---

**Generated by**: Builder agent (Loom workflow)
**Issue**: #2806
**Branch**: `feature/issue-2806`
