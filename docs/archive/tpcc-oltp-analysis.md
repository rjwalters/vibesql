# TPC-C OLTP Performance Analysis

**Date**: 2025-11-30
**Issue**: #3078

## Executive Summary

Investigation into VibeSQL's 37x performance gap vs SQLite on TPC-C OLTP workloads reveals that the primary bottleneck is **composite index underutilization** - index scans only use the first column of composite indexes, causing point lookups to degenerate into partial table scans.

## Benchmark Results

### Overall Performance (Scale Factor = 1)

| Database | TPS | vs SQLite |
|----------|-----|-----------|
| SQLite | 2,523 | 1x |
| DuckDB | 363 | 7x slower |
| VibeSQL | 67.89 | 37x slower |

### Transaction Breakdown

| Transaction | VibeSQL (us) | SQLite (us) | Ratio |
|-------------|-------------|-------------|-------|
| Payment | 13,594 | 7.2 | **1,888x** |
| New-Order | 13,986 | 44.8 | **312x** |
| Order-Status | 14,690 | 161 | 91x |
| Delivery | 2,659 | 26 | 102x |
| Stock-Level | 65,760 | 9,157 | 7x |

### Query-Level Analysis

Payment transaction (3 queries, 13.6ms total):
- Average query: ~4,500 us
- Parse time: 0.1% (negligible)
- Execute time: 99.9% (bottleneck)

New-Order transaction (~27 queries, 14ms total):
- Average query: ~613 us
- Parse time: 0.3% (negligible)
- Execute time: 99.7% (bottleneck)

## Root Cause Analysis

### Bottleneck #1: Composite Index Underutilization (CRITICAL)

**Issue**: Index scans only extract predicates for the FIRST column of composite indexes.

**Example**: For query `WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 42`:
- Index: `idx_customer_pk (c_w_id, c_d_id, c_id)`
- Current: Only uses `c_w_id = 1`, returns 3,000 rows, filters in memory
- Expected: Uses full key `(1, 1, 42)`, returns 1 row directly

**Location**: `crates/vibesql-executor/src/select/scan/index_scan/execution.rs:46-54`

**Impact**: 100-3000x overhead for customer table lookups

**Fix**: #3084 - Use full composite key for index lookups

### Bottleneck #2: Expression Evaluation Overhead (HIGH)

**Issue**: Per-query expression evaluation has significant overhead for simple predicates.

**Overhead per query**:
- CombinedExpressionEvaluator creation
- CSE cache clearing per row
- Full AST traversal for `col = value`

**Location**: `crates/vibesql-executor/src/select/scan/index_scan/execution.rs:220-221`

**Impact**: 10-50x overhead for simple predicate evaluation

**Fix**: #3085 - Specialized fast path for simple predicates

### Bottleneck #3: Per-Query Executor Setup (MEDIUM)

**Issue**: Each query creates new SelectExecutor with allocations.

**Allocations per query**:
- QueryArena (bump allocator)
- HashMap for aggregate_cache
- Instant::now() system call

**Location**: `crates/vibesql-executor/src/select/executor/builder.rs:48-63`

**Impact**: 2-5x overhead for executor creation

**Fix**: #3086 - Executor pooling / lazy initialization

## Recommendations

### Priority Order

1. **#3084 Composite Index** - CRITICAL
   - Expected improvement: 100-3000x for affected queries
   - Low implementation risk
   - Addresses Payment transaction slowdown (1,888x → <10x)

2. **#3085 Expression Evaluation** - HIGH
   - Expected improvement: 10-50x for simple queries
   - Medium implementation complexity
   - Benefits all OLTP transactions

3. **#3086 Executor Setup** - MEDIUM
   - Expected improvement: 2-5x for query overhead
   - Low implementation risk
   - Cumulative benefit for high-frequency workloads

### Expected Outcome

After implementing all fixes:
- Payment: ~10-50x slower than SQLite (vs 1,888x now)
- New-Order: ~10-30x slower than SQLite (vs 312x now)
- Overall TPC-C: ~10-50x slower than SQLite (vs 37x now)

Further optimizations (prepared statements, predicate compilation) could close the remaining gap.

## Follow-up Issues

- #3084: perf(index): Use full composite key for index lookups
- #3085: perf(eval): Reduce per-query expression evaluation overhead
- #3086: perf(executor): Reduce per-query SelectExecutor setup overhead
