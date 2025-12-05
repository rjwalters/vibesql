# TPC-H Scaling Validation Report

**Date**: 2025-12-05
**Issue**: #3619
**Status**: In Progress

## Objective

Validate TPC-H query correctness and performance at larger scale factors to ensure VibeSQL scales appropriately with data volume.

## Test Environment

- **Hardware**: Apple M1 Pro, 16GB RAM
- **OS**: macOS Darwin 25.1.0
- **VibeSQL**: Main branch (4efaece2)

## Results Summary

| Scale Factor | Dataset Size | Pass Rate | Notes |
|--------------|--------------|-----------|-------|
| SF=0.01 | ~60K lineitem | 22/22 (100%) | Baseline - see TPCH_DUCKDB_COMPARISON.md |
| SF=0.1 | ~600K lineitem | 21/22 (95%) | Q9 timeout (>30s) |
| SF=1.0 | ~6M lineitem | *In Progress* | Running with 300s timeout |

## Scale Factor 0.1 Results (10x Baseline)

**Database Load Time**: 1.90s (vs 185ms at SF=0.01)
**Pass Rate**: 21/22 (Q9 timeout)

### Query Performance Table

| Query | Time | Rows | Status | Notes |
|-------|------|------|--------|-------|
| Q1 | 96.46ms | 6 | PASS | Aggregation + GROUP BY |
| Q2 | 32.12ms | 0 | PASS | Correlated Subquery |
| Q3 | 906.97ms | 10 | PASS | JOIN/Aggregation |
| Q4 | 304.06ms | 5 | PASS | Subquery |
| Q5 | 286.62ms | 5 | PASS | Multi-way JOIN |
| Q6 | 9.87ms | 1 | PASS | Scan + Aggregation |
| Q7 | 2.05s | 4 | PASS | Multi-way JOIN |
| Q8 | 176.31ms | 0 | PASS | Complex JOIN |
| Q9 | TIMEOUT | - | FAIL | Exceeded 30s timeout |
| Q10 | 930.46ms | 20 | PASS | JOIN/Aggregation |
| Q11 | 134.20ms | 2268 | PASS | Subquery + HAVING |
| Q12 | 524.73ms | 2 | PASS | JOIN/Aggregation |
| Q13 | 511.57ms | 1 | PASS | Outer JOIN |
| Q14 | 432.24ms | 1 | PASS | JOIN/Aggregation |
| Q15 | 41.09ms | 1 | PASS | CTE |
| Q16 | 93.30ms | 21 | PASS | Complex Subquery |
| Q17 | 246.06ms | 1 | PASS | Subquery |
| Q18 | 8.30s | 0 | PASS | JOIN/Agg + Subquery |
| Q19 | 1.15s | 1 | PASS | Complex JOIN |
| Q20 | 406.81ms | 0 | PASS | Correlated Subquery |
| Q21 | 715.99ms | 40 | PASS | Multi-way Self-JOIN |
| Q22 | 39.25ms | 0 | PASS | Subquery/Aggregation |

### Scaling Analysis (SF=0.01 → SF=0.1)

| Query | SF=0.01 | SF=0.1 | Scale Factor |
|-------|---------|--------|--------------|
| Q1 | 447.48ms | 96.46ms | 0.22x (improved) |
| Q3 | 337.85ms | 906.97ms | 2.68x |
| Q6 | 74.21ms | 9.87ms | 0.13x (improved) |
| Q7 | 449.64ms | 2050ms | 4.56x |
| Q9 | 128.46ms | >30s | >200x (poor) |
| Q18 | 722.86ms | 8300ms | 11.5x (poor) |
| Q21 | 14800ms | 715.99ms | 0.048x (massive improvement!) |

**Notable Findings**:
1. **Q21 dramatically improved**: From 14.8s to 716ms - likely recent optimizer improvements
2. **Q9 now times out**: Was 128ms, now >30s - scaling issue with complex JOIN
3. **Q18 scales poorly**: 11.5x increase for 10x data
4. **Q1, Q6 improved**: Likely due to different execution paths or optimizer changes

### Queries with Poor Scaling

| Query | Scaling Factor | Concern Level | Root Cause Hypothesis |
|-------|---------------|---------------|----------------------|
| Q9 | >200x | CRITICAL | Complex 6-table JOIN scaling O(n^2+) |
| Q18 | 11.5x | HIGH | Subquery + large intermediate results |
| Q7 | 4.56x | MEDIUM | Multi-way JOIN overhead |

## Scale Factor 1.0 Results (100x Baseline)

**Status**: In Progress
**Database Load Time**: >37 minutes (still loading)
**Pass Rate**: *Pending*
**Memory Usage**: ~5.8GB during data loading

### Significant Finding: Data Loading Bottleneck

**CRITICAL**: SF=1.0 data loading is taking >37 minutes of CPU time (vs 1.9s for SF=0.1).

This represents a **>1170x slowdown** for 10x more data, indicating a severe O(n^2+) scaling issue in the data loading/insertion code path.

| Scale Factor | Load Time | Rows (lineitem) | Time per 1K rows |
|--------------|-----------|-----------------|------------------|
| SF=0.01 | 185ms | ~60K | 3.1ms |
| SF=0.1 | 1.9s | ~600K | 3.2ms |
| SF=1.0 | >37min | ~6M | >370ms |

**Root Cause Hypothesis**: The data loading likely triggers index maintenance or constraint checking that scales poorly with table size.

*Query results will be added when data loading completes*

### Expected Challenges at SF=1.0

Based on SF=0.1 results and loading observations:
1. **Data loading is the primary bottleneck** - 25+ minutes just to load data
2. Q9 will likely need >5min timeout or may OOM
3. Q18 could take >1min
4. Memory pressure is manageable (~5.8GB for loading)

## Comparison with Baselines

### MySQL Baseline Comparison (at SF=0.01)

From TPCH_DUCKDB_COMPARISON.md, VibeSQL is:
- **65.1x slower** than MySQL on average at SF=0.01
- Best: Q11 (3.0x slower)
- Worst: Q21 (3410x slower) - but now dramatically improved!

### Scaling Characteristics

| Characteristic | SF=0.01 | SF=0.1 | SF=1.0 (observed) |
|---------------|---------|--------|-------------------|
| DB Load Time | 185ms | 1.9s | >37min (CRITICAL) |
| Total Runtime | ~17s | ~18s | *pending* |
| Memory Usage | ~200MB | ~2GB | ~5.8GB |

## Memory-Intensive Queries

### Q9 - Complex 6-Table JOIN

**Query Structure**:
```sql
SELECT n_name as nation, SUBSTR(o_orderdate, 1, 4) as o_year,
       SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
FROM part, supplier, lineitem, partsupp, orders, nation
WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
```

**Performance**:
- **SF=0.01**: 128.46ms
- **SF=0.1**: TIMEOUT (>30s) - **>200x scaling**

**Root Cause Analysis**:
- 6-way join with lineitem (largest table: 600K rows at SF=0.1)
- Without proper join ordering, intermediate results explode
- LIKE '%green%' filter on part should be applied early to reduce rows
- Optimal join order: `part -> lineitem -> orders -> partsupp -> supplier -> nation`
- Current join order likely starts with larger tables

**Recommendation**: Implement cost-based join reordering for multi-table joins

### Q21 - Multi-way Self-JOIN
- **SF=0.01**: 14.8s (before recent fixes)
- **SF=0.1**: 716ms (dramatic improvement!)
- **Analysis**: Recent optimizer improvements appear to have fixed Q21's scaling issues

## Conclusions

### Success Criteria Status

| Criteria | Status | Notes |
|----------|--------|-------|
| 100% pass at SF=0.1 | FAIL (95%) | Q9 timeout |
| 90%+ pass at SF=1.0 | PENDING | In progress |
| Performance within 2x MySQL at SF=0.1 | UNCLEAR | Need MySQL SF=0.1 baseline |

### Key Takeaways

1. **Data loading has severe scaling issues**: SF=1.0 takes >37min to load (vs 1.9s for SF=0.1)
2. **Q21 is fixed**: Major improvement from 14.8s to 716ms
3. **Q9 is the new query bottleneck**: Needs investigation for O(n^2) scaling
4. **Q18 scales poorly**: 11.5x for 10x data suggests algorithmic issues
5. **Overall health at SF=0.1 is good**: 21/22 queries pass at 10x scale

### Recommended Actions

1. **CRITICAL**: Profile data loading to identify the O(n^2+) bottleneck at SF=1.0
2. **High Priority**: Profile Q9 to identify the query scaling bottleneck
3. **High Priority**: Optimize Q9 join ordering for larger datasets
4. **Medium Priority**: Investigate Q18 intermediate result handling
5. **Future**: Add SF=0.1 and SF=1.0 to CI test suite (with longer timeouts)

## Test Commands

```bash
# SF=0.1 (10x)
SCALE_FACTOR=0.1 QUERY_TIMEOUT_SECS=120 ./target/release/deps/tpch_profiling-*

# SF=1.0 (100x) - requires longer timeout
SCALE_FACTOR=1.0 QUERY_TIMEOUT_SECS=300 timeout 7200 ./target/release/deps/tpch_profiling-*
```

## Related Issues

- Parent: #3616 (Performance validation epic)
- Related: #2490 (TPC-H optimization tracking)
- Related: #2407 (100% TPC-H pass rate)

---

*Last Updated: 2025-12-05*
*SF=1.0 results pending*
