# Query Performance Profiling at Scale

This document captures profiling results and identifies optimization opportunities for query performance at larger scale factors.

## Key Finding: Data Loading Bottleneck

**Critical Issue**: Data loading and index creation are prohibitively slow at SF >= 0.1.

| Scale Factor | TPC-H Load Time | TPC-DS Load Time |
|-------------|-----------------|------------------|
| SF=0.01     | ~200ms          | ~5s              |
| SF=0.05     | >10 minutes     | N/A              |
| SF=0.1      | >15 minutes (timeout) | >10 minutes (stuck on indexes) |

The data loading bottleneck prevents meaningful query profiling at larger scale factors. This is the first optimization priority.

## TPC-H Query Profiling (SF=0.01)

### Top 10 Slowest Queries

| Rank | Query | Time (ms) | Primary Bottleneck |
|------|-------|-----------|-------------------|
| 1 | Q19 | 372.0 | Column reorder (24ms), 60K result rows |
| 2 | Q7  | 360.7 | Multi-table joins (customer 29ms, orders 18ms) |
| 3 | Q9  | 179.5 | partsupp join (137ms) - 74% of join time |
| 4 | Q18 | 153.4 | Orders table scan (113ms) for subquery |
| 5 | Q10 | 98.6  | lineitem scan (17ms) + 4-table join |
| 6 | Q12 | 96.2  | lineitem scan (21ms), 127M cartesian |
| 7 | Q3  | 94.0  | lineitem scan (22ms), 3-table join |
| 8 | Q13 | 54.3  | Left outer join aggregation |
| 9 | Q21 | 52.6  | 4-table join with NOT EXISTS |
| 10 | Q16 | 52.3 | Anti-join pattern |

### Detailed Breakdown: Slowest Queries

#### Q19 (372ms) - Large Cartesian Product
```
Cartesian products: 120,000,000 (lineitem × part)
Result rows: 60,000
Column reorder: 24ms (40% of post-join time)
```
**Bottleneck**: Missing join predicate pushdown results in massive intermediate result

#### Q9 (179ms) - partsupp Join Dominates
```
Join timing breakdown:
  - lineitem: 6.7ms
  - partsupp: 136.7ms (76% of join time!)
  - supplier: 0.8ms
  - nation: 0.8ms
  - orders: 2.8ms
```
**Bottleneck**: partsupp join iterates 10.5M cartesian pairs to produce 1320 rows

#### Q7 (360ms) - Multi-way Join Overhead
```
Join timing:
  - supplier: 0.03ms
  - lineitem: 6.2ms
  - orders: 11.5ms
  - customer: 29.0ms (largest)
  - n2: 15.5ms
Column reorder: 14.2ms
```
**Bottleneck**: Late filtering causes large intermediate results through join pipeline

## Identified Optimization Opportunities

### 1. Data Loading Performance (Critical)
**Impact**: Blocking profiling at SF >= 0.1

Current observations:
- TPC-H SF=0.1 timeout after 15 minutes during data generation
- TPC-DS stuck on index creation for 1.17M row inventory table

**Recommendations**:
- Profile data generator separately
- Consider bulk loading optimizations
- Evaluate index creation strategy for large tables

### 2. partsupp Join Optimization
**Impact**: 137ms of 179ms total in Q9 (76%)

The `part.p_partkey = lineitem.l_partkey AND part.p_partkey = partsupp.ps_partkey AND lineitem.l_suppkey = partsupp.ps_suppkey` 4-way join is expensive.

**Recommendations**:
- Evaluate hash join build side selection
- Consider composite key index on partsupp(ps_partkey, ps_suppkey)
- Investigate semi-join transformation

### 3. Table Scan with Subquery Filters (Q18)
**Impact**: 113ms for orders scan producing 0 rows

The orders table is fully scanned for subquery correlation that eliminates all rows.

**Recommendations**:
- Push subquery filter earlier in execution
- Evaluate EXISTS transformation
- Consider materialized subquery results

### 4. Column Reorder Overhead
**Impact**: 24ms in Q19, 14ms in Q7

Large result sets require expensive row reconstruction.

**Recommendations**:
- Defer column projection to final output
- Consider columnar intermediate results
- Evaluate lazy materialization

### 5. Cartesian Product Explosion
**Impact**: Q19 (120M), Q12 (127M)

Even with efficient filtering, large cartesian products consume memory and CPU.

**Recommendations**:
- Improve join order estimation
- Consider bloom filter pre-filtering for semi-joins
- Evaluate hash join memory limits

## Success Metrics

From the issue requirements:
- [x] Top 10 slowest queries documented
- [x] At least 2 optimization opportunities identified (5 identified)
- [ ] 20%+ improvement on worst-performing query (requires implementation)

## Next Steps

1. **Priority 1**: Fix data loading bottleneck to enable SF >= 0.1 profiling
2. **Priority 2**: Optimize partsupp join (Q9) - target 50% reduction
3. **Priority 3**: Improve subquery filter pushdown (Q18)
4. **Priority 4**: Evaluate column reorder deferral

## Profiling Commands

```bash
# TPC-H profiling with join timing
JOIN_PROFILE=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-*

# Single query profiling
JOIN_PROFILE=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-* Q9

# TPC-DS runner
SCALE_FACTOR=0.01 SKIP_SLOW=1 ./target/release/deps/tpcds_runner-*
```
