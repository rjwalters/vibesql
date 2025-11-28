# TPC-DS Benchmark Results

This document captures the TPC-DS benchmark execution results for VibeSQL.

## Summary

| Metric | Value |
|--------|-------|
| Scale Factor | 0.001 |
| Total Queries | 99 |
| Successful | 73 (73.7%) |
| Errors | 21 |
| Skipped (slow) | 5 |
| Total Execution Time | 158.69s |
| Average Time (successful) | 2.17s |
| Data Load Time | 102ms |

## Execution Environment

- Date: 2024-11-27
- Platform: macOS (Darwin)
- Rust: stable

## Query Results by Category

### Successful Queries (73)

The following queries executed successfully:

| Query | Time (ms) | Rows | Notes |
|-------|-----------|------|-------|
| Q1 | 10.46 | 0 | Customer Store Returns Analysis |
| Q3 | 10.64 | 62 | Sales by brand report |
| Q6 | 982.55 | 0 | Customer item category analysis |
| Q7 | 24.30 | 100 | Customer demographics sales |
| Q8 | 1892.81 | 0 | Store sales by ZIP code |
| Q9 | 45.95 | 1 | CASE expression buckets |
| Q10 | 12.69 | 0 | Customer county filter |
| Q15 | 10.08 | 12 | Catalog sales by ZIP |
| Q18 | 22.00 | 1 | Catalog demographics |
| Q19 | 9.79 | 3 | Brand/manufacturer report |
| Q42 | 4.40 | 0 | Item summary |
| Q52 | 6.93 | 36 | Brand sales |
| Q55 | 5.30 | 2 | Brand monthly sales |
| Q68 | 14.86 | 100 | Customer demographics |
| Q73 | 100.34 | 100 | Customer ticket analysis |
| Q89 | 72.22 | 0 | Store sales summary |
| Q96 | 14.74 | 2 | Store time analysis |
| Q25 | 141.95 | 100 | Store returns analysis |
| Q26 | 11.20 | 64 | Promotion analysis |
| Q27 | 9.96 | 30 | Store profit analysis |
| Q35 | 8.10 | 0 | Customer demographics |
| Q50 | 7.59 | 60 | Store returns by reason |
| Q81 | 11.47 | 0 | Customer web returns |
| Q82 | 0.87 | 11 | Inventory analysis |
| Q83 | 6.20 | 90 | Item returns |
| Q13 | 5.80 | 1 | Customer analysis |
| Q16 | 4.62 | 1 | Catalog returns |
| Q32 | 5.29 | 1 | Catalog sales |
| Q37 | 10.33 | 89 | Inventory catalog analysis |
| Q60 | 5.35 | 35 | Cross-channel totals |
| Q62 | 12.73 | 5 | Web sales shipping |
| Q76 | 5.46 | 60 | Channel profit |
| Q84 | 2.03 | 0 | Web returns customer |
| Q92 | 4.34 | 1 | Web sales discount |
| Q21 | 9.36 | 60 | Inventory analysis |
| Q22 | 5.23 | 1 | Inventory CUBE |
| Q23 | 112.00 | 100 | Frequent customer |
| Q28 | 6.61 | 1 | Quantity buckets |
| Q30 | 5.86 | 0 | Web returns |
| Q34 | 7.11 | 0 | Customer demographics |
| Q38 | 1523.98 | 1 | Cross-channel count |
| Q39 | 25.91 | 0 | Inventory variance |
| Q40 | 19.71 | 5 | Catalog returns |
| Q41 | 39.25 | 100 | Item manufacturing |
| Q43 | 35.98 | 12 | Store weekly sales |
| Q45 | 8.79 | 27 | Web sales customer |
| Q46 | 13.81 | 100 | Customer householdstore |
| Q47 | 77.59 | 100 | Store monthly sales |
| Q48 | 28.35 | 1 | Store demographics |
| Q49 | 66.63 | 0 | Channel returns |
| Q53 | 10.25 | 0 | Item monthly analysis |
| Q54 | 26.40 | 1 | Cross-channel customer |
| Q58 | 29.84 | 0 | Catalog web comparison |
| Q59 | 423.24 | 0 | Weekly store sales |
| Q61 | 16.66 | 11 | Store promotion |
| Q63 | 12.96 | 0 | Item analysis |
| Q64 | 14.07 | 1 | Cross-channel profit |
| Q65 | 9.35 | 0 | Store revenue |
| Q67 | 10.98 | 1 | Store ROLLUP |
| Q69 | 151421.15 | 100 | Customer demographics (slow) |
| Q72 | 36.54 | 100 | Catalog promotion |
| Q78 | 46.85 | 0 | Web store comparison |
| Q79 | 903.58 | 100 | Customer ticket |
| Q85 | 6.25 | 0 | Web returns analysis |
| Q87 | 16.80 | 1 | Cross-channel customers |
| Q88 | 61.80 | 1 | Store hourly sales |
| Q90 | 19.42 | 1 | Web morning/afternoon |
| Q91 | 6.96 | 0 | Call center analysis |
| Q93 | 7.51 | 0 | Store returns reason |
| Q94 | 5.10 | 1 | Web sales location |
| Q95 | 80.31 | 1 | Web sales shipping |
| Q97 | 12.77 | 1 | Multi-channel purchases |
| Q99 | 27.60 | 30 | Call center shipping |

### Skipped Queries (5)

These queries are known to be extremely slow and were skipped:

| Query | Reason |
|-------|--------|
| Q4 | Complex CTE with 6-way self-join |
| Q11 | Customer web vs store sales growth |
| Q17 | Store sales-returns-catalog analysis |
| Q24 | Complex multi-table join |
| Q29 | Large date dimension join |

### Failed Queries (21)

#### Parse Errors (5)

| Query | Error |
|-------|-------|
| Q2 | Expected keyword Select, found LParen |
| Q31 | Expected identifier after AS |
| Q66 | Expected identifier after AS |
| Q74 | Expected identifier after AS |
| Q75 | Expected identifier after AS |

These queries use SQL syntax patterns not yet supported by the parser.

#### Table Not Found (6)

| Query | Missing Table | Reason |
|-------|---------------|--------|
| Q5 | SSR | CTE alias used as table name |
| Q14 | CROSS_ITEMS | CTE alias used as table name |
| Q33 | SS | Table alias resolution |
| Q51 | WEB_V1 | CTE alias used as table name |
| Q77 | SS | Table alias resolution |
| Q80 | SSR | CTE alias used as table name |

#### Unsupported Window Functions (5)

| Query | Error |
|-------|-------|
| Q12 | Window functions not supported in aggregate context |
| Q20 | Window functions not supported in aggregate context |
| Q44 | Window functions not supported in aggregate context |
| Q57 | Window functions not supported in aggregate context |
| Q98 | Window functions not supported in aggregate context |

These queries use `SUM(...) OVER (PARTITION BY ...)` nested within aggregate expressions.

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

These indicate column reference issues in complex queries.

## Performance Observations

### Fast Queries (<10ms)
Q42, Q52, Q55, Q19, Q82, Q13, Q16, Q84, Q92, Q22, Q32, Q60, Q76, Q83, Q35, Q81, Q93, Q91, Q94, Q85, Q86, Q30, Q28, Q34, Q51, Q21, Q50

### Moderate Queries (10-100ms)
Q1, Q3, Q15, Q18, Q26, Q27, Q37, Q62, Q45, Q46, Q67, Q63, Q65, Q90, Q87, Q97, Q99, Q7, Q10, Q9, Q68, Q40, Q47, Q58, Q72, Q78, Q61, Q54, Q43, Q53, Q88, Q95, Q49, Q48, Q89

### Slow Queries (100ms-10s)
Q73, Q23, Q59, Q8, Q6, Q38, Q79, Q71, Q14

### Very Slow Queries (>10s)
Q5 (60s), Q69 (151s)

## Recommendations

1. **Parser Improvements**: Add support for parenthesized UNION ALL and AS without identifier
2. **CTE Resolution**: Improve CTE alias handling for complex multi-CTE queries
3. **Window Functions**: Support window functions in aggregate expressions
4. **GROUPING Function**: Implement SQL:1999 GROUPING() for ROLLUP/CUBE queries
5. **Query Optimization**: Optimize Q5 and Q69 which take >10s at small scale

## Running the Benchmark

```bash
# Default scale factor (0.01)
cargo bench --bench tpcds_runner

# Smaller scale factor
SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner

# Skip known slow queries
SKIP_SLOW=1 SCALE_FACTOR=0.001 cargo bench --bench tpcds_runner
```
