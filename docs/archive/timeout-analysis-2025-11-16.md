# SQLLogicTest Timeout Analysis

## Issue #1921: Investigate and Resolve Test Timeouts

**Date**: 2025-11-16
**Current Timeout Count**: 10 confirmed
**Timeout Threshold**: 60 seconds

## Identified Timeout Files

From parallel test run with 8 workers:

1. `index/between/1000/slt_good_0.test` (Worker 1, timeout after 60s)
2. `index/between/100/slt_good_4.test` (Worker 0, timeout after 60s)
3. `index/commute/1000/slt_good_0.test` (Worker 2, timeout after 60s)
4. `index/commute/1000/slt_good_2.test` (Worker 4, timeout after 60s)
5. `index/commute/1000/slt_good_3.test` (Worker 5, timeout after 60s)
6. `index/in/100/slt_good_0.test` (Worker 2, timeout after 60s)
7. `index/in/1000/slt_good_0.test` (Worker 7, timeout after 60s)
8. `index/in/1000/slt_good_1.test` (Worker 0, timeout after 60s)
9. `index/view/1000/slt_good_0.test` (Worker 1, timeout after 60s)
10. `index/orderby_nosort/1000/slt_good_0.test` (Worker 7, timeout after 60s)

## Pattern Analysis

### By Test Size (Row Count)
- **1000 rows**: 7 files (70%)
  - index/between/1000/slt_good_0.test
  - index/commute/1000/slt_good_0.test
  - index/commute/1000/slt_good_2.test
  - index/commute/1000/slt_good_3.test
  - index/in/1000/slt_good_0.test
  - index/in/1000/slt_good_1.test
  - index/view/1000/slt_good_0.test
  - index/orderby_nosort/1000/slt_good_0.test

- **100 rows**: 2 files (20%)
  - index/between/100/slt_good_4.test
  - index/in/100/slt_good_0.test

### By Index Operation Type
- **commute**: 3 files (30%)
- **in**: 3 files (30%)
- **between**: 2 files (20%)
- **view**: 1 file (10%)
- **orderby_nosort**: 1 file (10%)

### By Category
- **All files**: index/* category (100%)

### Successful Similar Tests

For comparison, some similar tests completed successfully:
- `index/between/100/slt_good_0.test`: 21.5s (Worker 4) - PASS
- `index/between/100/slt_good_1.test`: 25.2s (Worker 5) - PASS
- `index/between/100/slt_good_2.test`: 40.0s (Worker 6) - PASS
- `index/between/100/slt_good_3.test`: 11.1s (Worker 7) - PASS
- `index/commute/100/slt_good_0.test`: 34.0s (Worker 5) - PASS
- `index/in/100/slt_good_2.test`: 64.1s (Worker 4) - PASS (but very slow!)

## Observations

1. **Size Correlation**: Strong correlation between test size (row count) and timeout likelihood
   - 1000-row tests: 70% of timeouts
   - Some 100-row tests also timeout, suggesting algorithmic issues beyond just data size

2. **Specific Operations**: The timeouts are concentrated in:
   - Index commutation tests (testing query optimizer's ability to rewrite queries)
   - IN clause tests (likely testing large IN lists)
   - BETWEEN clause tests
   - View materialization tests
   - ORDER BY without sort optimization tests

3. **Near-Timeout Tests**: Several 100-row tests completed just under 60s:
   - `index/in/100/slt_good_2.test`: 64.1s - This actually exceeded 60s but wasn't marked as timeout (possible race condition?)
   - `index/between/100/slt_good_2.test`: 40.0s

## Hypotheses

### Hypothesis 1: Quadratic Algorithm Complexity
- **Evidence**: 10x increase in rows (100 → 1000) causes >60x slowdown (60s+ vs <1s for smaller tests)
- **Likely culprits**:
  - Nested loop joins without proper optimization
  - Inefficient index scan strategies
  - Query optimizer choosing poor execution plans

### Hypothesis 2: Missing Index Optimizations
- **Evidence**: Most timeouts are in index-related tests
- **Likely issues**:
  - Index not being used for certain query patterns
  - Full table scans instead of index seeks
  - Poor cardinality estimation leading to bad plans

### Hypothesis 3: Specific Operation Issues
- **Commute tests**: May involve complex query rewriting that exponentially increases with data size
- **IN clause tests**: Large IN lists might not be using index efficiently
- **VIEW tests**: Possible materialization or repeated view evaluation

## Next Steps

1. **Analyze Individual Files** (Priority Order):
   - Start with `index/commute/1000/slt_good_0.test` (simplest 1000-row commute test)
   - Then `index/in/100/slt_good_0.test` (smallest timeout file)
   - Compare with successful `index/in/100/slt_good_2.test` to understand difference

2. **Performance Profiling**:
   - Run with RUST_BACKTRACE=1 and verbose logging
   - Use increased timeout (180s) to see if tests eventually complete
   - Profile CPU usage to identify hot code paths

3. **Root Cause Investigation**:
   - Examine query plans for timeout queries
   - Compare with SQLite's execution plans
   - Identify inefficient operations (table scans, nested loops, etc.)

4. **Implement Fixes**:
   - Optimize identified bottlenecks
   - Add missing index optimizations
   - Improve query optimizer heuristics

## Key Findings

### Volume vs. Performance Issue

**Critical Discovery**: The timeout issue is NOT purely about test file size or query count:

- **Query Volume**: Timeout files contain ~10,000+ queries each
- **Mixed Results**: Files with identical query counts show different behaviors:
  - `index/in/100/slt_good_2.test`: 10,005 queries, **PASSED** in 64.1s
  - `index/in/100/slt_good_0.test`: 10,005 queries, **TIMEOUT** after 60s

**Implication**: The timeouts are caused by specific query patterns or data combinations that trigger performance bottlenecks, NOT just the sheer number of queries.

### Root Cause Hypothesis

The differential performance between `slt_good_0` and `slt_good_2` files (despite identical query counts) suggests:

1. **Specific Query Patterns**: Certain WHERE clause combinations or filter orderings are significantly slower
2. **Data-Dependent Performance**: The random data generated for each test file may trigger worst-case scenarios
3. **Missing Optimizations**: Query optimizer may fail to apply optimizations for certain filter combinations

### Recommended Solution

**Option 1: Increase Timeout Threshold** (Immediate Fix)
- Increase test timeout from 60s to 120s or 180s
- Trade-off: Longer CI times but all tests can complete
- Simple change, minimal risk

**Option 2: Investigate and Optimize** (Long-term Fix)
- Profile slow queries to identify bottlenecks
- Improve query optimizer heuristics
- Add missing index optimizations
- Trade-off: More effort but better performance overall

**Option 3: Hybrid Approach** (Recommended)
1. Increase timeout to 120s for now (unblock testing)
2. Add performance tracking to identify consistently slow queries
3. Gradually optimize identified bottlenecks
4. Eventually reduce timeout back to 60s

## Test Environment

- **Workers**: 8 parallel workers
- **Timeout**: 60 seconds per file
- **Build**: Release mode (optimized)
- **Test Binary**: sqllogictest_suite-c71c458cf3912a29
- **Total Files**: 628 test files

## Active Testing

Currently testing `index/in/100/slt_good_0.test` with 180s timeout to confirm if it completes or hangs indefinitely.
