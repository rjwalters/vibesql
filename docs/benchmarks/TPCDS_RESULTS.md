# TPC-DS Benchmark Results

This document captures the TPC-DS benchmark execution results for VibeSQL.

## Summary

| Metric | Value |
|--------|-------|
| Scale Factor | 0.001 |
| Total Queries | 99 |
| Successful | 88 (88.9%) |
| Errors | 6 |
| Skipped (slow) | 5 |
| Total Execution Time | 133.57s |
| Average Time (successful) | 1.52s |
| Data Load Time | 92ms |

## Progress Tracking

| Date | Passing | Rate | Notes |
|------|---------|------|-------|
| 2024-11-27 | 73 | 73.7% | Initial baseline |
| 2024-12-02 | 88 | 88.9% | +15 queries, parser improvements |

## Execution Environment

- Date: 2024-12-02
- Platform: macOS (Darwin)
- Rust: stable

## Coverage Report

| Category | Count | Queries |
|----------|-------|---------|
| Passing | 88 | Q1-Q3, Q6-Q10, Q12-Q16, Q18-Q23, Q25-Q28, Q30-Q35, Q37-Q55, Q57-Q68, Q72-Q99 (excluding failures) |
| Failing (execute) | 6 | Q5, Q36, Q56, Q70, Q71, Q86 |
| Skipped (slow) | 5 | Q4, Q11, Q17, Q24, Q29 |

## Query Results

### Successful Queries (88)

| Query | Time (ms) | Rows | Description |
|-------|-----------|------|-------------|
| Q1 | 7.49 | 0 | Customer store returns analysis |
| Q2 | 159.72 | 100 | Web-catalog inventory comparison |
| Q3 | 17.39 | 62 | Sales by brand report |
| Q6 | 677.08 | 0 | Customer item category analysis |
| Q7 | 26.37 | 100 | Customer demographics sales |
| Q8 | 2339.94 | 0 | Store sales by ZIP code |
| Q9 | 63.61 | 1 | CASE expression buckets |
| Q10 | 1.19 | 0 | Customer county filter |
| Q12 | 9.36 | 10 | Window function analysis |
| Q13 | 8.66 | 1 | Customer analysis |
| Q14 | 2000.29 | 100 | Cross-channel intersect |
| Q15 | 16.69 | 12 | Catalog sales by ZIP |
| Q16 | 8.22 | 1 | Catalog returns |
| Q18 | 12.53 | 1 | Catalog demographics |
| Q19 | 32.38 | 3 | Brand/manufacturer report |
| Q20 | 11.23 | 60 | Catalog items |
| Q21 | 16.41 | 60 | Inventory analysis |
| Q22 | 12.91 | 1 | Inventory CUBE |
| Q23 | 108.09 | 100 | Frequent customer |
| Q25 | 143.36 | 100 | Store returns analysis |
| Q26 | 25.33 | 64 | Promotion analysis |
| Q27 | 25.64 | 30 | Store profit analysis |
| Q28 | 6.00 | 1 | Quantity buckets |
| Q30 | 5.68 | 0 | Web returns |
| Q31 | 86.67 | 100 | Web-store comparison |
| Q32 | 8.00 | 1 | Catalog sales |
| Q33 | 72.70 | 1 | Manufacturer totals |
| Q34 | 21.38 | 0 | Customer demographics |
| Q35 | 7.82 | 0 | Customer demographics |
| Q37 | 8.89 | 89 | Inventory catalog analysis |
| Q38 | 1365.72 | 1 | Cross-channel count |
| Q39 | 29.86 | 0 | Inventory variance |
| Q40 | 22.73 | 5 | Catalog returns |
| Q41 | 4.09 | 100 | Item manufacturing |
| Q42 | 11.77 | 0 | Item summary |
| Q43 | 55.67 | 12 | Store weekly sales |
| Q44 | 110.14 | 100 | Item ranking |
| Q45 | 17.98 | 27 | Web sales customer |
| Q46 | 38.57 | 100 | Customer household store |
| Q47 | 87.22 | 100 | Store monthly sales |
| Q48 | 65.17 | 1 | Store demographics |
| Q49 | 69.04 | 0 | Channel returns |
| Q50 | 8.30 | 60 | Store returns by reason |
| Q51 | 8.89 | 0 | Web store totals |
| Q52 | 14.14 | 36 | Brand sales |
| Q53 | 23.65 | 0 | Item monthly analysis |
| Q54 | 70.97 | 1 | Cross-channel customer |
| Q55 | 21.26 | 2 | Brand monthly sales |
| Q57 | 106.27 | 0 | Catalog monthly |
| Q58 | 29.24 | 0 | Catalog web comparison |
| Q59 | 328.82 | 0 | Weekly store sales |
| Q60 | 6.21 | 35 | Cross-channel totals |
| Q61 | 23.74 | 11 | Store promotion |
| Q62 | 17.26 | 5 | Web sales shipping |
| Q63 | 22.94 | 0 | Item analysis |
| Q64 | 9.37 | 1 | Cross-channel profit |
| Q65 | 22.21 | 0 | Store revenue |
| Q66 | 9.97 | 0 | Web-store warehouse |
| Q67 | 23.59 | 1 | Store ROLLUP |
| Q68 | 27.26 | 100 | Customer demographics |
| Q69 | 123030.08 | 100 | Customer demographics (slow) |
| Q72 | 21.43 | 100 | Catalog promotion |
| Q73 | 98.79 | 100 | Customer ticket analysis |
| Q74 | 458.51 | 0 | Customer year totals |
| Q75 | 32.50 | 0 | Catalog-store comparison |
| Q76 | 7.20 | 60 | Channel profit |
| Q77 | 25.90 | 28 | Store-web-catalog profit |
| Q78 | 40.62 | 0 | Web store comparison |
| Q79 | 876.82 | 100 | Customer ticket |
| Q80 | 17.69 | 14 | Store returns profit |
| Q81 | 10.50 | 0 | Customer web returns |
| Q82 | 0.22 | 11 | Inventory analysis |
| Q83 | 6.48 | 90 | Item returns |
| Q84 | 2.75 | 0 | Web returns customer |
| Q85 | 4.71 | 0 | Web returns analysis |
| Q87 | 14.32 | 1 | Cross-channel customers |
| Q88 | 55.43 | 1 | Store hourly sales |
| Q89 | 85.00 | 0 | Store sales summary |
| Q90 | 11.99 | 1 | Web morning/afternoon |
| Q91 | 4.84 | 0 | Call center analysis |
| Q92 | 5.74 | 1 | Web sales discount |
| Q93 | 6.50 | 0 | Store returns reason |
| Q94 | 5.03 | 1 | Web sales location |
| Q95 | 63.64 | 1 | Web sales shipping |
| Q96 | 18.06 | 2 | Store time analysis |
| Q97 | 10.24 | 1 | Multi-channel purchases |
| Q98 | 16.25 | 45 | Item yearly sales |
| Q99 | 41.23 | 30 | Call center shipping |

### Skipped Queries (5)

These queries are known to be extremely slow due to complex join patterns and were skipped:

| Query | Reason |
|-------|--------|
| Q4 | Complex CTE with 6-way self-join |
| Q11 | Customer web vs store sales growth (complex CTE) |
| Q17 | Store sales-returns-catalog analysis |
| Q24 | Complex multi-table join |
| Q29 | Large date dimension join |

### Failed Queries (6)

#### Memory Limit Exceeded (1)

| Query | Error |
|-------|-------|
| Q5 | MemoryLimitExceeded (28.9GB used, 10GB limit) |

This query executes a complex multi-channel analysis that exceeds memory limits.

#### GROUPING Function Not Implemented (3)

| Query | Error |
|-------|-------|
| Q36 | Unknown function: GROUPING |
| Q70 | Unknown function: GROUPING |
| Q86 | Unknown function: GROUPING |

The `GROUPING()` function used with ROLLUP/CUBE is not yet implemented.

#### Column Resolution Issues (2)

| Query | Error |
|-------|-------|
| Q56 | I_ITEM_ID not found in STORE_SALES context |
| Q71 | I_BRAND_ID not found in result context |

These indicate column reference ambiguity in complex subquery correlations.

## Performance Analysis

### Fast Queries (<10ms) - 20 queries

Q10, Q28, Q30, Q35, Q41, Q60, Q76, Q82, Q83, Q84, Q85, Q91, Q92, Q93, Q94, Q1, Q12, Q13, Q16, Q66

### Moderate Queries (10-100ms) - 42 queries

Q2, Q3, Q7, Q9, Q15, Q18, Q19, Q20, Q21, Q22, Q25, Q26, Q27, Q31, Q32, Q33, Q34, Q37, Q39, Q40, Q42, Q43, Q45, Q46, Q47, Q48, Q49, Q50, Q51, Q52, Q53, Q54, Q55, Q61, Q62, Q63, Q64, Q65, Q67, Q68, Q72, Q77, Q80, Q81, Q88, Q89, Q90, Q95, Q96, Q97, Q98, Q99

### Slow Queries (100ms-10s) - 23 queries

Q2 (160ms), Q6 (677ms), Q14 (2000ms), Q23 (108ms), Q38 (1366ms), Q43 (56ms), Q44 (110ms), Q47 (87ms), Q57 (106ms), Q59 (329ms), Q73 (99ms), Q74 (459ms), Q79 (877ms), Q8 (2340ms)

### Very Slow Queries (>10s) - 1 query

| Query | Time (s) | Issue |
|-------|----------|-------|
| Q69 | 123.0 | Customer demographics, inefficient join |

## Priority Fixes

Based on the analysis, here are the highest-impact improvements:

### 1. GROUPING Function (High Impact)
- **Unblocks**: Q36, Q70, Q86 (3 queries)
- **Effort**: Medium
- **Notes**: Part of SQL:1999 ROLLUP/CUBE support

### 2. Column Resolution in Complex Subqueries (Medium Impact)
- **Unblocks**: Q56, Q71 (2 queries)
- **Effort**: Medium-High
- **Notes**: Requires improved scoping for correlated subqueries

### 3. Query Optimization for Q69 (Performance)
- **Impact**: Reduce 123s to <10s
- **Effort**: High
- **Notes**: Inefficient customer demographics join pattern

### 4. Memory Optimization for Q5 (Resource)
- **Impact**: Allow Q5 to complete within memory limits
- **Effort**: High
- **Notes**: May require streaming or spilling to disk

## Running the Benchmark

```bash
# Default run (small scale)
SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner

# Skip known slow queries for faster iteration
SKIP_SLOW=1 SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner

# With jemalloc for better memory management
SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner --features jemalloc

# Full verbose output
SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner 2>&1 | tee tpcds_results.log
```

## CSV Export

The benchmark runner outputs CSV data at the end of each run for easy import into spreadsheets or tracking systems.

```csv
Query,Time_ms,Rows,Status
Q1,7.49,0,OK
Q2,159.72,100,OK
...
```
