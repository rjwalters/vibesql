# TPC-H Performance Comparison: VibeSQL vs DuckDB vs MySQL

**Date**: 2025-11-29
**Scale Factor**: 0.01
**Issue**: #2490

## Executive Summary

Complete benchmark comparison of all 22 TPC-H queries between VibeSQL, DuckDB, and MySQL at SF 0.01.

**Overall Result**:
- VibeSQL is **321.4x slower** than DuckDB on average
- VibeSQL is **65.1x slower** than MySQL on average
- MySQL is **4.9x slower** than DuckDB on average

**Best Case (VibeSQL vs MySQL)**: Q11 (3.0x slower) - Subquery + HAVING
**Worst Case (VibeSQL vs MySQL)**: Q21 (3410x slower) - Multi-way self-joins with NOT EXISTS

## Complete Comparison Table

| Query | VibeSQL | DuckDB | MySQL | vs DuckDB | vs MySQL | Category |
|-------|---------|--------|-------|-----------|----------|----------|
| Q1 | 447.48ms | 4.47ms | 71.60ms | 100.2x | 6.3x | Aggregation + GROUP BY |
| Q2 | 12.70ms | 3.27ms | 7.02ms | 3.9x ⭐ | 1.8x ⭐ | Correlated Subquery |
| Q3 | 337.85ms | 2.05ms | 8.55ms | 165.1x | 39.5x | JOIN/Aggregation |
| Q4 | 63.85ms | 2.31ms | 1.77ms | 27.6x | 36.1x | Subquery |
| Q5 | 119.21ms | 2.89ms | 4.91ms | 41.2x | 24.3x | Multi-way JOIN |
| Q6 | 74.21ms | 0.54ms | 7.67ms | 137.1x | 9.7x | Scan + Aggregation |
| Q7 | 449.64ms | 2.98ms | 4.04ms | 150.7x | 111.3x | Multi-way JOIN |
| Q8 | 103.65ms | 3.36ms | 10.28ms | 30.8x | 10.1x | Complex JOIN |
| Q9 | 128.46ms | 3.63ms | 84.07ms | 35.4x | 1.5x ⭐ | Complex JOIN |
| Q10 | 309.72ms | 3.25ms | 13.33ms | 95.2x | 23.2x | JOIN/Aggregation |
| Q11 | 11.87ms | 1.72ms | 3.95ms | 6.9x ⭐ | 3.0x ⭐ | Subquery + HAVING |
| Q12 | 237.44ms | 2.51ms | 13.13ms | 94.4x | 18.1x | JOIN/Aggregation |
| Q13 | 88.21ms | 2.88ms | 13.92ms | 30.6x | 6.3x | Outer JOIN |
| Q14 | 22.05ms | 1.03ms | 1.50ms | 21.4x | 14.7x | JOIN/Aggregation |
| Q15 | 75.97ms | 1.21ms | 2.74ms | 62.6x | 27.7x | CTE |
| Q16 | 20.91ms | 2.61ms | 3.52ms | 8.0x ⭐ | 5.9x | Complex Subquery |
| Q17 | 56.22ms | 0.99ms | 1.17ms | 56.9x | 48.1x | Subquery |
| Q18 | 722.86ms | 3.08ms | 17.93ms | 235.0x | 40.3x | JOIN/Agg + Subquery |
| Q19 | 53.32ms | 3.41ms | 1.07ms | 15.6x | 49.8x | Complex JOIN |
| Q20 | 143.27ms | 2.53ms | 3.05ms | 56.7x | 47.0x | Correlated Subquery |
| Q21 | 14800.0ms | 4.27ms | 4.34ms | 3468.1x 🔴 | 3410.1x 🔴 | Multi-way Self-JOIN |
| Q22 | 12.08ms | 1.90ms | 1.08ms | 6.4x ⭐ | 11.2x | Subquery/Aggregation |
| **AVG** | **831.41ms** | **2.59ms** | **12.76ms** | **321.4x** | **65.1x** | |

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
**MySQL Version**: 8.4.7 (Docker)
**VibeSQL Version**: Commit c0b65b74
**Runs**: 3 per query, averaged
**Environment**: macOS Darwin 25.1.0, ARM64

**DuckDB Query Execution**:
```python
conn.execute("PRAGMA tpch(1)")  # Q1
conn.execute("PRAGMA tpch(6)")  # Q6
# etc.
```

**MySQL Query Execution**:
```bash
./scripts/mysql-benchmark/run-benchmark.sh --scale-factor 0.01 --iterations 3
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
