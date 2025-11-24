# TPC-H Performance Comparison: VibeSQL vs DuckDB

**Date**: 2025-11-24
**Scale Factor**: 0.01
**Issue**: #2490

## Executive Summary

Complete benchmark comparison of all 22 TPC-H queries between VibeSQL and DuckDB at SF 0.01.

**Overall Result**: VibeSQL is **321.4x slower** on average than DuckDB

**Best Case**: Q2 (3.9x slower) - Correlated subquery
**Worst Case**: Q21 (3468x slower) - Multi-way self-joins with NOT EXISTS

## Complete Comparison Table

| Query | VibeSQL | DuckDB | Gap | Category | Priority |
|-------|---------|---------|-----|----------|----------|
| Q1 | 447.48ms | 4.47ms | 100.2x | Aggregation + GROUP BY | HIGH |
| Q2 | 12.70ms | 3.27ms | 3.9x ⭐ | Correlated Subquery | LOW |
| Q3 | 337.85ms | 2.05ms | 165.1x | JOIN/Aggregation | HIGH |
| Q4 | 63.85ms | 2.31ms | 27.6x | Subquery | MEDIUM |
| Q5 | 119.21ms | 2.89ms | 41.2x | Multi-way JOIN | MEDIUM |
| Q6 | 74.21ms | 0.54ms | 137.1x | Scan + Aggregation | HIGH |
| Q7 | 449.64ms | 2.98ms | 150.7x | Multi-way JOIN | HIGH |
| Q8 | 103.65ms | 3.36ms | 30.8x | Complex JOIN | MEDIUM |
| Q9 | 128.46ms | 3.63ms | 35.4x | Complex JOIN | MEDIUM |
| Q10 | 309.72ms | 3.25ms | 95.2x | JOIN/Aggregation | HIGH |
| Q11 | 11.87ms | 1.72ms | 6.9x ⭐ | Subquery + HAVING | LOW |
| Q12 | 237.44ms | 2.51ms | 94.4x | JOIN/Aggregation | HIGH |
| Q13 | 88.21ms | 2.88ms | 30.6x | Outer JOIN | MEDIUM |
| Q14 | 22.05ms | 1.03ms | 21.4x | JOIN/Aggregation | MEDIUM |
| Q15 | 75.97ms | 1.21ms | 62.6x | CTE | MEDIUM |
| Q16 | 20.91ms | 2.61ms | 8.0x ⭐ | Complex Subquery | LOW |
| Q17 | 56.22ms | 0.99ms | 56.9x | Subquery | MEDIUM |
| Q18 | 722.86ms | 3.08ms | 235.0x | JOIN/Agg + Subquery | CRITICAL |
| Q19 | 53.32ms | 3.41ms | 15.6x | Complex JOIN | MEDIUM |
| Q20 | 143.27ms | 2.53ms | 56.7x | Correlated Subquery | MEDIUM |
| Q21 | 14800.0ms | 4.27ms | 3468.1x 🔴 | Multi-way Self-JOIN | CRITICAL |
| Q22 | 12.08ms | 1.90ms | 6.4x ⭐ | Subquery/Aggregation | LOW |
| **AVG** | **831.41ms** | **2.59ms** | **321.4x** | | |

## Performance Categories

### Excellent (Under 10x gap) ⭐
- Q2: 3.9x - Correlated subquery
- Q11: 6.9x - Subquery with HAVING
- Q22: 6.4x - Subquery/Aggregation
- Q16: 8.0x - Complex subquery

**Analysis**: Subquery optimization is relatively strong

### Good (10-30x gap)
- Q19: 15.6x - Complex JOIN
- Q14: 21.4x - JOIN/Aggregation
- Q4: 27.6x - Subquery

**Analysis**: Room for improvement but acceptable

### Moderate (30-100x gap)
- Q8, Q13: ~31x - JOINs
- Q9: 35.4x - Complex JOIN
- Q5: 41.2x - Multi-way JOIN
- Q15, Q17, Q20: 56-63x - CTEs and subqueries
- Q10, Q12: 94-95x - JOIN/Aggregation

**Analysis**: Significant optimization opportunities

### Poor (100-200x gap)
- Q1: 100.2x - Simple GROUP BY aggregation
- Q6: 137.1x - Single table scan + aggregate
- Q7: 150.7x - Multi-way JOIN
- Q3: 165.1x - JOIN/Aggregation

**Analysis**: Core execution primitives need work

### Critical (>200x gap) 🔴
- Q18: 235.0x - JOIN/Aggregation with subquery
- Q21: 3468.1x - Multi-way self-joins with NOT EXISTS

**Analysis**: Urgent attention required

## Key Findings

### 1. Aggregation Performance is Poor
Simple aggregation queries (Q1, Q6) are 100-137x slower despite being straightforward:
- Q6 is just a table scan with filter + SUM - should be <10x
- Q1 is GROUP BY aggregation - should be <20x

**Root cause**: Likely row-at-a-time execution instead of vectorized aggregation

### 2. Subquery Handling is Relatively Good
Best-performing queries (Q2, Q11, Q16, Q22) all involve subqueries:
- Suggests subquery optimization is working
- Correlated subquery handling is decent

### 3. Multi-way JOINs are Problematic
Queries with complex join graphs (Q1, Q3, Q7, Q21) are all >100x:
- Join order selection may be suboptimal
- Join execution is slow
- Q21 with self-joins is catastrophic (3468x)

### 4. Q21 is an Outlier
At 14.8 seconds and 3468x slower, Q21 dominates the average:
- Multi-way self-joins on LINEITEM, ORDERS, SUPPLIER
- Uses NOT EXISTS for semi-join patterns
- Fixing Q21 alone would reduce average gap from 321x to ~85x

## Optimization Priorities

### P0: Fix Q21 (14.8s → <100ms)
**Impact**: Massive - reduces average from 321x to ~85x
**Approach**:
- Profile to identify bottleneck
- Join order optimization
- Semi-join/anti-join detection for NOT EXISTS
- Consider hash joins vs nested loops

### P1: Optimize Simple Aggregations (Q1, Q6)
**Impact**: High - these should be easy wins
**Approach**:
- Vectorized/SIMD aggregation functions
- Columnar execution for scans
- Better expression evaluation
**Target**: Q6 <10ms (10x improvement), Q1 <50ms (9x improvement)

### P2: Fix Q18 (723ms → <50ms)
**Impact**: Medium - another slow outlier
**Approach**:
- Subquery optimization
- Join order improvement
- Aggregation pipeline optimization

### P3: Improve Multi-way JOINs (Q3, Q7, Q10, Q12)
**Impact**: Medium-High - affects many queries
**Approach**:
- Better join order selection
- Hash join optimization
- Cardinality estimation improvements
**Target**: All <100ms

### P4: General Optimizations
**Impact**: Broad - helps all queries
**Approach**:
- Filter pushdown
- Memory layout optimization
- Expression evaluation
- Constant folding

## Realistic Goals

### 6-Month Target
- **Q21 fixed**: <100ms (currently 14.8s)
- **Simple queries optimized**: Q1, Q6, Q14 all <20ms
- **Average**: 30-50x slower (currently 321x) - **10x improvement**
- **All queries**: <100ms except complex multi-way joins

### 12-Month Target
- **Average**: 10-20x slower - **3x improvement from 6-month**
- **Vectorized execution**: Throughout query pipeline
- **Parallel execution**: Basic intra-query parallelism
- **All queries**: <50ms

### 18-24 Month Target
- **Average**: 5-10x slower - **2x improvement from 12-month**
- **SIMD optimization**: Mature implementation
- **Parallel execution**: Full intra-query parallelism
- **JIT exploration**: For hot paths

## Benchmark Methodology

**DuckDB Version**: 1.4.2
**DuckDB Data**: Built-in TPC-H extension (`dbgen(sf=0.01)`)
**VibeSQL Version**: Commit c0b65b74
**Runs**: 3 per query, averaged
**Environment**: macOS Darwin 25.1.0, ARM64

**DuckDB Query Execution**:
```python
conn.execute("PRAGMA tpch(1)")  # Q1
conn.execute("PRAGMA tpch(6)")  # Q6
# etc.
```

**VibeSQL Query Execution**:
```bash
./scripts/bench-tpch-isolated.sh 30 /tmp/results.txt
```

## Raw Data

Full results available at:
- `/tmp/tpch_full_comparison.txt`
- Issue: https://github.com/rjwalters/vibesql/issues/2490

## Next Steps

1. Profile Q21 to identify specific bottleneck
2. Profile Q1 and Q6 for aggregation performance
3. Implement vectorized aggregation pipeline
4. Improve join order optimizer
5. Add semi-join/anti-join detection

---

*Last updated: 2025-11-24*
*Related: #2407 (100% TPC-H pass rate), #2490 (performance optimization tracking)*
