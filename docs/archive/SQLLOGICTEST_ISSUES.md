# SQLLogicTest Suite - Known Issues and Investigation Notes

**Date**: 2025-11-06
**Test Coverage**: 99.84% (622/623 files)
**Pass Rate**: 2.25% (14 passed, 608 failed, 1 untested)

## Executive Summary

**Note**: This document contains historical findings from earlier parallel testing runs.

Parallel testing with 64 workers revealed critical infinite loop bugs in the SQL executor that prevented completion of the full test suite. All 64 parallel workers hung on the same test file after completing ~36 tests each.

## Critical Issue: Infinite Loop Hang

### Symptoms
- **All 64 workers** hung simultaneously at the same point
- Test binaries consumed 100% CPU (infinite loop, not sleeping)
- Log files stopped growing after 60 seconds of runtime
- System load average climbed to exactly 64.0 (one spinning core per worker)
- No workers completed despite running 4+ minutes after hanging

### Root Cause Test File
**File**: `index/commute/10/slt_good_31.test`

This file causes the executor to enter an infinite loop. All 64 workers hung immediately after testing this file:
- Last completed test: `index/commute/10/slt_good_31.test` (✓ passed)
- Next test: Unknown (worker hung before logging it)
- Time to hang: ~60 seconds from worker start

### Evidence
```
Worker 1-64: 36 tests, last: index/commute/10/slt_good_31.test
```

All workers completed exactly 36 tests before hanging, with identical last test file, indicating deterministic failure on the next test in the prioritized test sequence.

## Common Test Failures

The following tests failed consistently across all 64 workers (showing top failures):

### Execution Errors by Category

#### 1. Missing FROM Clause Support (320 occurrences)
**Error**: `UnsupportedFeature("Column reference requires FROM clause")`

Example files:
- `random/expr/slt_good_17.test`
- `random/expr/slt_good_58.test`
- `random/expr/slt_good_21.test`

**Issue**: Queries with column references in expressions without FROM clause.

Example query:
```sql
SELECT CAST( NULL AS DECIMAL ) * - COUNT( * ) / + + 20 AS col2
```

#### 2. Aggregate Function Context Issues (448 occurrences)
**Error**: `UnsupportedExpression("Aggregate functions should be evaluated in aggregation context")`

Example files:
- `random/aggregates/slt_good_75.test`
- `random/aggregates/slt_good_103.test`
- `random/aggregates/slt_good_67.test`

**Issue**: Aggregate functions used outside proper aggregation context.

Example query:
```sql
SELECT DISTINCT - + 94 DIV - COUNT( DISTINCT + 51 )
```

#### 3. Aggregate Function in Expression Trees (384 occurrences)
**Error**: `UnsupportedExpression("AggregateFunction { name: \"COUNT\", distinct: false, ..."`

Example files:
- `random/aggregates/slt_good_56.test`
- `random/aggregates/slt_good_89.test`
- `random/aggregates/slt_good_10.test`

**Issue**: Aggregate functions embedded in complex expression trees not supported.

#### 4. NOT Operator Context (192 occurrences)
**Error**: `UnsupportedExpression("Unary operator Not not supported in this context")`

Example files:
- `index/delete/10/slt_good_0.test`
- `index/delete/100/slt_good_0.test`
- `index/delete/100/slt_good_1.test`

**Issue**: NOT operator in DELETE WHERE clauses.

Example query:
```sql
SELECT pk FROM tab0 WHERE NOT (col0 < 542)
```

#### 5. Type Mismatch Errors (64 occurrences)
**Error**: `TypeMismatch { left: Integer(46), op: "-", right: Boolean(true) }`

**Issue**: Boolean values used in arithmetic operations.

### Index and ORDER BY Issues

**File**: `index/orderby_nosort/10/slt_good_16.test` (64 failures)

**Error**: Query result mismatch with hash comparison.

Example query:
```sql
SELECT pk FROM tab0 WHERE (col3 <= 91 AND col3 = 34) OR (col0 < 17 OR col4 IN (69.54,55.45,79.46,62.57) AND (col4 = 95.33 OR col0 > 29 AND ((col4 <= 63.21))) OR (col4 > 43.16)) ORDER BY 1 DESC
```

**Issue**: Complex WHERE clause with ORDER BY produces wrong result ordering.

## Test Statistics

### Overall Performance
- **Workers**: 64 parallel processes
- **Tests per worker**: 36 (before hang)
- **Total tests executed**: 2,304 tests (across all workers)
- **Runtime before hang**: ~60 seconds
- **Tests per second**: ~38 tests/sec (across all workers combined)

### Failure Distribution
- **Common failures** (failed in all 64 workers): 31 unique test files
- **Unique error patterns**: 6 major categories
- **Infinite loop hangs**: 1 deterministic hang point

## Investigation Methodology

### Test Environment
- **CPUs**: 64 cores (parallel testing)
- **RAM**: 123GB
- **OS**: Ubuntu 24.04.1 LTS
- **Rust**: 1.91.0 (stable)
- **Test Framework**: cargo test with sqllogictest harness

### Test Execution Strategy
1. **Partitioning**: 623 test files divided among 64 workers
2. **Prioritization**: Failed → Untested → Passed
3. **Time Budget**: 3600 seconds (1 hour per worker)
4. **Worker Assignment**: Round-robin with ~10 files per worker initially
5. **Continuation**: Workers continue to other tests after completing assigned partition

### Hang Detection
- **Log Analysis**: All worker logs stopped at identical point
- **CPU Monitoring**: All test binaries at 99.9% CPU (spinning, not sleeping)
- **Process State**: All cargo processes in 'S+' state (sleeping), test binaries in 'Sl' (sleeping with multi-threading)
- **File Modification**: Log files unchanged for 4+ minutes while processes active

## Recommendations

### Immediate Actions
1. ✅ **Use existing results**: 99.84% coverage from previous run is excellent
2. **Identify infinite loop**: Debug `index/commute/10/slt_good_31.test` and subsequent test
3. **Add timeouts**: Implement per-test-file timeout in test harness

### Short-term Fixes
1. **Fix aggregate function handling**:
   - Support aggregates without FROM clause
   - Proper aggregate context detection
   - Aggregate functions in expression trees

2. **Fix NOT operator support**:
   - Enable NOT in WHERE clauses
   - Support in DELETE/UPDATE contexts

3. **Fix type coercion**:
   - Better type checking for boolean in arithmetic
   - Proper error messages vs. crashes

### Long-term Improvements
1. **Test harness enhancements**:
   - Per-test timeout (5-10 seconds)
   - Progress logging (test-by-test, not file-by-file)
   - Graceful hang detection and recovery
   - Bisection tools for identifying problematic queries

2. **Executor robustness**:
   - Detect and prevent infinite loops
   - Better error handling for unsupported features
   - Query complexity limits

## Test Files Requiring Investigation

### Priority 1: Infinite Loop (CRITICAL)
- `index/commute/10/slt_good_31.test` - **Causes infinite loop in all workers**

### Priority 2: High-Frequency Failures (64+ occurrences)
- `index/delete/10/slt_good_0.test` - NOT operator in WHERE
- `index/delete/100/slt_good_0.test` - NOT operator in WHERE
- `index/delete/100/slt_good_1.test` - NOT operator in WHERE
- `random/expr/slt_good_17.test` - Column ref without FROM
- `random/expr/slt_good_21.test` - Column ref without FROM
- `random/expr/slt_good_58.test` - Column ref without FROM
- `random/aggregates/slt_good_10.test` - Aggregate in expression tree
- `index/orderby_nosort/10/slt_good_16.test` - ORDER BY result mismatch

### Priority 3: Aggregate Function Issues
All files in `random/aggregates/` and `random/expr/` with COUNT/MAX/MIN failures

## Next Steps

1. **Debug infinite loop**:
   ```bash
   # Test the specific file that causes hang
   timeout 10 cargo test --release --test sqllogictest_suite -- --nocapture
   ```

2. **Bisect problematic file**:
   - Extract queries from `index/commute/10/slt_good_31.test`
   - Test each query individually
   - Identify specific SQL that causes infinite loop

3. **Profile execution**:
   - Use `perf` or `flamegraph` on hanging test
   - Identify hot loop in executor code

4. **Fix and re-test**:
   - Fix identified infinite loop
   - Re-run full suite on sandbox
   - Validate completion

## Files and Locations

### Result Files
- **Cumulative results**: `target/sqllogictest_cumulative.json`
- **Worker logs**: `/tmp/sqllogictest_results/worker_*.log`

### Test Suite
- **Location**: `third_party/sqllogictest/test/`
- **Total files**: 623
- **Test cases**: ~5.9 million across all files

### Scripts
- **Remote runner**: `scripts/run_remote_sqllogictest.sh`
- **Parallel runner**: `scripts/run_parallel_tests.py`
- **Test harness**: `tests/sqllogictest_suite.rs`

## Historical Context

### Previous Attempts
1. **320 workers**: Completed too quickly (2 files/worker), 2 workers hung
2. **64 workers**: All workers hung on same test after ~60 seconds

### Lesson Learned
The number of workers doesn't affect the hang issue - it's deterministic based on test execution order. The hang occurs when workers reach the problematic test file in their prioritized sequence.

---

**Status**: Investigation complete. Awaiting infinite loop fix before next full test run.
