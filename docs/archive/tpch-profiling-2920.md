# TPC-H Bottleneck Analysis (Issue #2920)

## Summary

Profiled the slowest TPC-H queries to identify root causes before optimization work.
Testing at SF 0.01 (60,175 rows in lineitem table).

## Results

| Query | VibeSQL | DuckDB | Gap | Execution Strategy | Key Bottleneck |
|-------|---------|--------|-----|-------------------|----------------|
| Q6 | 67ms | 0.54ms | **125x** | StandardColumnar | Row-to-columnar conversion |
| Q1 | 636ms | 4.47ms | **142x** | RowOriented | GROUP BY + expression eval |
| Q3 | 424ms | 2.05ms | **207x** | RowOriented | JOINs + GROUP BY overhead |
| Q7 | Error | 3.0ms | N/A | - | SUBSTR(date) unsupported |

## Detailed Analysis

### Q6: Simple Scan + Aggregate (125x slower)

**Query Pattern:**
```sql
SELECT SUM(l_extendedprice * l_discount) as revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
```

**Execution Path:**
1. Table scan returns `Vec<Row>` (row-oriented data)
2. StandardColumnar pipeline selected (analytical pattern)
3. `ColumnarBatch::from_rows()` - O(n*m) conversion
4. SIMD-accelerated filter applied
5. `to_rows()` conversion back to rows
6. `ColumnarBatch::from_rows()` AGAIN for aggregation
7. SIMD aggregation computed

**Bottlenecks Identified:**
- **Double row-to-columnar conversion**: Data converted Row→Columnar twice
- **No native columnar storage**: Base storage is row-oriented
- **Potential double filtering**: Predicate pushdown in scan + pipeline filter

**Throughput:** ~890K rows/sec

---

### Q1: GROUP BY + Multiple Aggregates (142x slower)

**Query Pattern:**
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice), ...
FROM lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

**Execution Path:**
1. Table scan returns `Vec<Row>`
2. **RowOriented** strategy forced (GROUP BY not supported in columnar)
3. Row-by-row expression evaluation
4. Hash-based GROUP BY aggregation
5. Row-by-row aggregate accumulation

**Bottlenecks Identified:**
- **RowOriented fallback**: GROUP BY prevents columnar execution
- **Per-row expression evaluation**: ~8 aggregate expressions per row
- **Hash table overhead**: Building/probing hash map per group

**Throughput:** ~94K rows/sec (9x slower than Q6)

---

### Q3: JOINs + GROUP BY + ORDER BY (207x slower - worst)

**Query Pattern:**
```sql
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)), o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

**Execution Path:**
1. Three-table join (customer × orders × lineitem)
2. Predicate pushdown on each table
3. Join algorithm execution (nested loop or hash)
4. GROUP BY aggregation
5. ORDER BY sorting
6. LIMIT selection

**Bottlenecks Identified:**
- **Join algorithm overhead**: Multi-way join cost
- **Intermediate result materialization**: Full join result before GROUP BY
- **RowOriented execution**: No vectorized join processing

**Throughput:** ~142K rows/sec

---

### Q7: Multi-way JOIN (Error)

**Issue:** Query fails with:
```
UnsupportedFeature("SUBSTRING requires string argument, got Date(...)")
```

**Root Cause:** Q7 uses `SUBSTR(l_shipdate, 1, 4)` to extract year from date.
The SUBSTR function doesn't support Date type conversion.

**Fix Required:** Either:
- Add EXTRACT(YEAR FROM date) support
- Add automatic Date→String coercion for SUBSTR

---

## Top 3 Optimization Opportunities

### 1. Native Columnar Storage (Impact: High)

**Problem:** All data is stored row-oriented. Each query pays O(n*m) conversion cost.

**Evidence:** Q6 converts 60K rows × 16 columns twice per query execution.

**Solution Options:**
- Store tables in columnar format (ColumnarTable)
- Use `ColumnarBatch::from_storage_columnar()` for zero-copy access
- Lazy column materialization (only load accessed columns)

**Expected Impact:** 10-50x improvement for scan-heavy queries

### 2. Columnar GROUP BY Support (Impact: High)

**Problem:** GROUP BY forces RowOriented execution, losing SIMD benefits.

**Evidence:** Q1 at 636ms uses RowOriented vs Q6 at 67ms using StandardColumnar.

**Solution Options:**
- Implement columnar hash aggregation
- Use vectorized group key extraction
- SIMD-accelerated aggregate accumulation

**Expected Impact:** 5-10x improvement for GROUP BY queries

### 3. Vectorized Hash Join (Impact: Medium-High)

**Problem:** Multi-table joins use row-oriented processing.

**Evidence:** Q3 (3-table join) is 207x slower than DuckDB.

**Solution Options:**
- Implement columnar hash join
- Build/probe hash tables on columnar data
- Batch join result materialization

**Expected Impact:** 3-5x improvement for JOIN queries

---

## Recommended Priority

1. **Native Columnar Storage** - Highest ROI, benefits all queries
2. **Columnar GROUP BY** - Unlocks SIMD for aggregation queries
3. **Vectorized Hash Join** - Critical for TPC-H Q3, Q5, Q7-Q10

## Metrics for Success

Target after optimizations:
- Q6: <10ms (currently 67ms) → 7x improvement needed
- Q1: <50ms (currently 636ms) → 13x improvement needed
- Q3: <30ms (currently 424ms) → 14x improvement needed

---

*Generated by Issue #2920 profiling task*
*Date: 2025-11-27*
