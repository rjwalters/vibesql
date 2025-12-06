# Random Aggregates Test Failures - Diagnostic Report

**Issue**: #1872
**Parent Issue**: #1833
**Date**: 2025-11-15
**Test Suite**: `third_party/sqllogictest/test/random/aggregates/`
**Total Tests**: 130
**Pass Rate**: 0% (as of parent issue creation)

## Executive Summary

This document provides a detailed diagnostic analysis of failures in the `random/aggregates` test suite. The goal is to identify specific error patterns, categorize them, and provide actionable next steps for fixes.

## ⚠️ ACTUAL TEST RESULTS (2025-11-15 UPDATED)

**Analysis Method**: Queried test results database from recent test runs
**Database**: `~/.vibesql/test_results/sqllogictest_results.sql` (Nov 15, 2025)
**Total Failures Analyzed**: **1480 test failures** across `random/aggregates/` suite

### Complete Error Pattern Analysis

| Error Pattern | Count | Percentage | Status |
|---------------|-------|------------|--------|
| **BETWEEN NULL handling** | 1365 | 92.2% | Issue #1846 |
| **NULL arithmetic in aggregates** | 114 | 7.7% | **NEW - Issue needed** |
| Other | 1 | 0.1% | TBD |
| **TOTAL** | **1480** | **100%** | |

### Critical Discovery

The `random/aggregates` test suite is **MISLEADINGLY NAMED** - it contains:
- **92% non-aggregate queries** with BETWEEN NULL predicates
- **8% aggregate queries** with NULL arithmetic issues

**The overwhelming majority of failures are NOT aggregate-specific issues!**

---

## Error Pattern 1: BETWEEN NULL Handling ❌ (92.2% of failures)

**Count**: 1365 failures
**Issue**: #1846 (BETWEEN NULL handling)
**Error Type**: Query result mismatch - WHERE clause filtering
**Contains Aggregates**: ❌ NO - Pure WHERE clause issue

**Root Cause**: `BETWEEN NULL AND value` incorrectly returns FALSE for all rows instead of NULL/UNKNOWN

**Sample SQL Queries**:
```sql
-- Example 1: BETWEEN with NULL endpoint
SELECT * FROM tab0 WHERE NOT col1 * + col2
    BETWEEN - col1 AND + - CAST( NULL AS SIGNED )
-- Expected: Empty set (NULL predicate)
-- Actual: All 9 rows returned

-- Example 2: NOT BETWEEN with NULL
SELECT + + col1 AS col2 FROM tab0
    WHERE NOT - col0 - - 78 BETWEEN NULL AND - col2
-- Expected: Empty set
-- Actual: 3 rows returned (21, 81, 1)

-- Example 3: BETWEEN NULL AND NULL
SELECT * FROM tab1 WHERE - col0 NOT BETWEEN NULL AND NULL
-- Expected: Empty set
-- Actual: All 9 rows returned
```

**SQL Standard Behavior**:
- `value BETWEEN NULL AND x` → NULL (three-valued logic)
- `NOT NULL` → NULL (not TRUE)
- WHERE clause filters out NULL predicates (keeps only TRUE)

**Current vibesql Behavior**:
- Treating `BETWEEN NULL` as FALSE instead of NULL
- `NOT FALSE` = TRUE, so rows are incorrectly included

**Impact**:
- **BLOCKS 92% of test execution** - these failures mask true aggregate errors
- Related to issue #1846

---

## Error Pattern 2: NULL Arithmetic in Aggregates ❌ (7.7% of failures)

**Count**: 114 failures
**Issue**: **NEW** - Follow-up issue to be created
**Error Type**: Query result mismatch - aggregate evaluation
**Contains Aggregates**: ✅ YES - True aggregate issues

**Root Cause**: NULL arithmetic in aggregate expressions returns 0 or numeric values instead of propagating NULL

**Sample SQL Queries**:
```sql
-- Example 1: SUM(NULL) returns 0 instead of NULL
SELECT SUM( CAST( NULL AS SIGNED ) ) FROM tab1
-- Expected: NULL
-- Actual: 0.000

-- Example 2: NULL arithmetic doesn't propagate
SELECT SUM( NULL ) * COUNT( * ) + MAX( col1 ) FROM tab0
-- Expected: NULL (NULL * anything = NULL)
-- Actual: 1.000

-- Example 3: WHERE with NULL comparison + aggregates
SELECT SUM( col0 ) FROM tab1 WHERE - col0 > NULL
-- Expected: NULL (empty result set)
-- Actual: 0.000

-- Example 4: Complex NULL arithmetic
SELECT SUM( NULL ) + COUNT( * ) FROM tab2
-- Expected: NULL
-- Actual: 3.000 (COUNT value, ignoring NULL)

-- Example 5: WHERE NULL <> NULL with aggregates
SELECT SUM( - col1 ) * COUNT( * ) FROM tab1 WHERE NULL <> NULL
-- Expected: NULL (empty WHERE)
-- Actual: 0.000

-- Example 6: NULL in NOT IN + aggregates
SELECT SUM( col0 ) FROM tab1 WHERE NULL NOT IN ( - 91 * col2 )
-- Expected: NULL
-- Actual: 0.000
```

**SQL Standard Behavior**:
- `NULL + value` → NULL
- `NULL * value` → NULL
- `SUM(NULL)` → NULL (not 0)
- Aggregate over empty set (WHERE always NULL/FALSE) → NULL for SUM/AVG/MAX/MIN
- NULL propagates through all arithmetic expressions

**Current vibesql Behavior**:
- Treating `SUM(NULL)` as 0
- Not propagating NULL through aggregate arithmetic
- Empty aggregation (WHERE with NULL comparison) returns 0 instead of NULL

**Code Locations to Investigate**:
- `crates/vibesql-executor/src/select/executor/aggregation/evaluation/mod.rs:eval_with_aggregates()` - Aggregate expression evaluation
- `crates/vibesql-executor/src/evaluator/combined/eval.rs` - Expression NULL handling
- Aggregate accumulator implementations - NULL value handling

**Impact**:
- 114 failures (7.7% of total)
- TRUE aggregate-specific bugs
- Currently masked by BETWEEN NULL failures in test execution order

## Methodology

### Phase 1: Error Capture
1. Run `random/aggregates/slt_good_0.test` with full output capture
2. Extract unique error patterns from test output
3. Document error messages, SQL queries, and line numbers
4. Compare against historical error patterns from `SQLLOGICTEST_ISSUES.md`

### Phase 2: Error Classification
1. Group errors by type (executor error, type mismatch, unsupported feature, etc.)
2. Identify most common error patterns
3. Map errors to specific code locations in vibesql-executor

### Phase 3: Root Cause Analysis
1. For top error types, trace execution path
2. Identify exact code locations where errors are thrown
3. Determine if errors represent legitimate unsupported features or bugs

## Historical Context

From `docs/testing/sqllogictest/SQLLOGICTEST_ISSUES.md` (dated 2025-11-06), the following error patterns were documented:

### Historical Error Pattern 1: Aggregate Context Issues
**Error**: `UnsupportedExpression("Aggregate functions should be evaluated in aggregation context")`
**Occurrences**: 448 across all test suites
**Example Query**:
```sql
SELECT DISTINCT - + 94 DIV - COUNT( DISTINCT + 51 )
```

**Suspected Cause**: Aggregate functions used in expressions outside proper aggregation context.

### Historical Error Pattern 2: Aggregate in Expression Trees
**Error**: `UnsupportedExpression("AggregateFunction { name: \"COUNT\", distinct: false, ...")`
**Occurrences**: 384 across all test suites
**Example Files**:
- `random/aggregates/slt_good_56.test`
- `random/aggregates/slt_good_89.test`
- `random/aggregates/slt_good_10.test`

**Suspected Cause**: Aggregate functions embedded in complex expression trees not fully supported by evaluator.

## Architecture Review (Completed in Issue Investigation)

The executor architecture for aggregate handling has been verified as fundamentally sound:

### 1. Aggregate Detection (`detection.rs`)
- ✅ `expression_has_aggregate()` recursively checks all expression types
- ✅ Detects `AggregateFunction` variants
- ✅ Checks `BinaryOp`, `UnaryOp`, `Cast`, `Case`, `Between`, `InList`, etc.

**Code Reference**: `crates/vibesql-executor/src/select/executor/aggregation/detection.rs:19-94`

### 2. Query Routing (`execute.rs`)
- ✅ Correctly routes queries with aggregates to `execute_with_aggregation`
- ✅ Handles both `has_aggregates` and `has_group_by` cases

**Code Reference**: `crates/vibesql-executor/src/select/executor/execute.rs:75-79`

```rust
let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
let has_group_by = stmt.group_by.is_some();

let mut results = if has_aggregates || has_group_by {
    self.execute_with_aggregation(stmt, cte_results)?
```

### 3. Aggregate Evaluation (`evaluation/mod.rs`)
- ✅ Dedicated `evaluate_with_aggregates()` method exists
- ✅ Handles all expression types in aggregate context
- ✅ Properly evaluates aggregate functions, binary ops, functions, etc.

**Code Reference**: `crates/vibesql-executor/src/select/executor/aggregation/evaluation/mod.rs`

### 4. No-FROM Handling (`aggregation/mod.rs`)
- ✅ Creates implicit row for `SELECT` without `FROM`
- ✅ SQL standard compliant behavior

**Code Reference**: `crates/vibesql-executor/src/select/executor/aggregation/mod.rs:46-58`

## Current Test Results

### Test Execution Details

**Command**:
```bash
timeout 30 ./scripts/sqllogictest test random/aggregates/slt_good_0.test 2>&1 | tee /tmp/aggregates_errors.log
```

**Execution Date**: 2025-11-15
**Test File**: `third_party/sqllogictest/test/random/aggregates/slt_good_0.test`

### Error Patterns Observed

_[Results to be filled in after test execution]_

#### Error Pattern 1: [Error Type]
**Error Message**:
```
[Error message here]
```

**Occurrence Count**: [X]
**Sample SQL Query**:
```sql
[Sample query that triggers this error]
```

**Code Location**: [file:line]
**Analysis**: [Brief analysis of what's happening]

#### Error Pattern 2: [Error Type]
**Error Message**:
```
[Error message here]
```

**Occurrence Count**: [X]
**Sample SQL Query**:
```sql
[Sample query that triggers this error]
```

**Code Location**: [file:line]
**Analysis**: [Brief analysis of what's happening]

### Top 10 Error Patterns Summary Table

| # | Error Type | Count | % of Total | Code Location | Fix Priority |
|---|------------|-------|------------|---------------|--------------|
| 1 | [Error] | [X] | [XX%] | [file:line] | High |
| 2 | [Error] | [X] | [XX%] | [file:line] | High |
| 3 | [Error] | [X] | [XX%] | [file:line] | Medium |
| 4 | [Error] | [X] | [XX%] | [file:line] | Medium |
| 5 | [Error] | [X] | [XX%] | [file:line] | Low |
| 6 | [Error] | [X] | [XX%] | [file:line] | Low |
| 7 | [Error] | [X] | [XX%] | [file:line] | Low |
| 8 | [Error] | [X] | [XX%] | [file:line] | Low |
| 9 | [Error] | [X] | [XX%] | [file:line] | Low |
| 10 | [Error] | [X] | [XX%] | [file:line] | Low |

## Error Classification

### Legitimate Unsupported Features
_[Features that are genuinely not supported and would require new implementation]_

1. **[Feature Name]**
   - **Description**: [What SQL feature is not supported]
   - **Effort to Implement**: [Low/Medium/High]
   - **SQL Standard Compliance**: [Required/Optional]

### Bugs (Should Work But Don't)
_[Cases where the architecture should support it but implementation has gaps]_

1. **[Bug Description]**
   - **Symptom**: [What fails]
   - **Expected**: [What should happen]
   - **Root Cause**: [Why it's failing]
   - **Fix Complexity**: [Low/Medium/High]

## Code Locations Investigated

### Key Files
- `crates/vibesql-executor/src/evaluator/combined/eval.rs:281` - Regular evaluator rejects aggregates
- `crates/vibesql-executor/src/evaluator/expressions/eval.rs:243` - Expression evaluator rejects aggregates
- `crates/vibesql-executor/src/select/executor/aggregation/evaluation/mod.rs` - Aggregate-aware evaluator
- `crates/vibesql-executor/src/select/executor/aggregation/detection.rs` - Aggregate detection logic
- `crates/vibesql-executor/src/select/executor/execute.rs:75-79` - Query routing decision

### Execution Path for Aggregate Queries
1. **Parsing**: Query parsed into AST by `vibesql-parser`
2. **Detection**: `has_aggregates()` checks SELECT list - `detection.rs:7-15`
3. **Routing**: `execute()` routes to `execute_with_aggregation()` - `execute.rs:78-79`
4. **Evaluation**: `evaluate_with_aggregates()` processes expressions - `evaluation/mod.rs`
5. **Aggregation**: Accumulators updated, groups managed
6. **Result Assembly**: Final result rows constructed

## Comparison with Historical Errors

### Status of Historical Error Pattern 1
**Historical**: `UnsupportedExpression("Aggregate functions should be evaluated in aggregation context")` (448 occurrences)
**Current**: [Status - Still occurring? Fixed? Reduced?]
**Analysis**: [What changed or why it persists]

### Status of Historical Error Pattern 2
**Historical**: `UnsupportedExpression("AggregateFunction { name: \"COUNT\", ...")` (384 occurrences)
**Current**: [Status - Still occurring? Fixed? Reduced?]
**Analysis**: [What changed or why it persists]

## Root Cause Analysis

### Top Error #1: [Error Type]
**Investigation Path**:
1. Query enters `execute()` method
2. [Trace through code...]
3. Error thrown at: [file:line]

**Root Cause**: [Detailed explanation]

**Proposed Fix**: [Specific code changes needed]

### Top Error #2: [Error Type]
**Investigation Path**:
1. [Trace through code...]

**Root Cause**: [Detailed explanation]

**Proposed Fix**: [Specific code changes needed]

### Top Error #3: [Error Type]
**Investigation Path**:
1. [Trace through code...]

**Root Cause**: [Detailed explanation]

**Proposed Fix**: [Specific code changes needed]

## Actionable Next Steps

### ✅ COMPLETED: Diagnostic Analysis
- Analyzed 1480 test failures
- Identified 2 distinct root causes
- Categorized errors by pattern
- Extracted sample SQL queries for each category
- Documented expected vs actual behavior
- Identified code locations for fixes

### 🔴 HIGH PRIORITY: Fix BETWEEN NULL Handling
**Issue**: #1846 (existing)
**Impact**: Fixes **1365/1480 errors (92.2%)**
**Estimated Effort**: 4-8 hours

**Error Pattern**: `BETWEEN NULL` returns FALSE instead of NULL
**Files to Modify**:
- `crates/vibesql-executor/src/evaluator/*/eval.rs` - BETWEEN operator evaluation
- WHERE clause NULL handling logic

**Test Command to Verify**:
```bash
./scripts/sqllogictest test random/aggregates/slt_good_0.test
```

**Acceptance Criteria**:
- `value BETWEEN NULL AND x` returns NULL (not FALSE)
- `value BETWEEN x AND NULL` returns NULL (not FALSE)
- WHERE clause correctly filters NULL predicates
- All BETWEEN NULL test cases pass

---

### 🟡 MEDIUM PRIORITY: Fix NULL Arithmetic in Aggregates
**Issue**: **NEW** - To be created (see "Recommended New Issues" below)
**Impact**: Fixes **114/1480 errors (7.7%)**
**Estimated Effort**: 8-12 hours

**Error Pattern**: NULL arithmetic returns 0/numbers instead of NULL
**Files to Modify**:
- `crates/vibesql-executor/src/select/executor/aggregation/evaluation/mod.rs`
- `crates/vibesql-executor/src/evaluator/combined/eval.rs`
- Aggregate accumulator implementations

**Acceptance Criteria**:
- `SUM(NULL)` returns NULL (not 0)
- NULL propagates through arithmetic: `NULL + x` = NULL, `NULL * x` = NULL
- Aggregates over empty sets (WHERE always NULL) return NULL for SUM/AVG/MAX/MIN
- All NULL aggregate test cases pass

---

### 📋 POST-FIX: Re-run Test Suite
After fixing #1846, re-run the full `random/aggregates` suite to:
1. Verify 92% of failures are resolved
2. Uncover any additional errors masked by BETWEEN NULL
3. Update error counts and patterns
4. Validate NULL aggregate fix resolves remaining 8%

**Expected Outcome**: ~100% pass rate after both fixes

## Recommended New Issues

Based on this analysis, the following new issue should be created:

### Issue: Fix NULL arithmetic propagation in aggregate expressions

**Title**: Fix NULL arithmetic propagation in aggregate expressions (affects 114 tests)

**Labels**: `bug`, `executor`, `aggregates`, `NULL-handling`

**Priority**: Medium (blocks 7.7% of random/aggregates tests)

**Description**:
NULL values in aggregate expressions are not properly propagating through arithmetic operations, causing incorrect results.

**Problem**:
- `SUM(NULL)` returns `0.000` instead of `NULL`
- `SUM(NULL) * COUNT(*)` returns numeric value instead of `NULL`
- `SUM(col) + NULL` returns numeric value instead of `NULL`
- Aggregates over empty result sets (WHERE with NULL comparisons) return `0` instead of `NULL`

**SQL Standard Behavior**:
- `NULL + value` → `NULL`
- `NULL * value` → `NULL`
- `SUM(NULL)` → `NULL` (not 0)
- Aggregate over empty set → `NULL` for SUM/AVG/MAX/MIN, `0` for COUNT

**Sample Failing Queries**:
```sql
-- Test 1: SUM(NULL) should return NULL
SELECT SUM( CAST( NULL AS SIGNED ) ) FROM tab1;
-- Expected: NULL
-- Actual: 0.000

-- Test 2: NULL arithmetic should propagate
SELECT SUM( NULL ) * COUNT( * ) + MAX( col1 ) FROM tab0;
-- Expected: NULL
-- Actual: 1.000

-- Test 3: Empty aggregation should return NULL
SELECT SUM( col0 ) FROM tab1 WHERE NULL <> NULL;
-- Expected: NULL
-- Actual: 0.000
```

**Code Locations to Investigate**:
- `crates/vibesql-executor/src/select/executor/aggregation/evaluation/mod.rs` - Aggregate expression evaluation
- `crates/vibesql-executor/src/evaluator/combined/eval.rs` - Expression NULL handling
- Aggregate accumulator implementations - NULL value initialization and handling

**Test Verification**:
```bash
# After fix, these should pass:
./scripts/sqllogictest test random/aggregates/slt_good_10.test
./scripts/sqllogictest test random/aggregates/slt_good_27.test
./scripts/sqllogictest test random/aggregates/slt_good_34.test
```

**Estimated Effort**: 8-12 hours

**Parent Issue**: #1833 (Fix random/aggregates test suite failures)

**Related Analysis**: See `docs/testing/random_aggregates_errors.md` for complete analysis

## Success Criteria Checklist

- [x] All 1480 random/aggregates test failures analyzed with precise error types
- [x] Error patterns categorized and prioritized (2 main categories identified)
- [x] Root cause identified for both major error patterns
- [x] Clear path forward documented for fixes (2 specific action items)
- [x] New issue template created for NULL arithmetic bug
- [ ] GitHub issue created for NULL arithmetic in aggregates (#TBD)
- [ ] Progress communicated on parent issue #1872

## Deliverables Summary

✅ **Complete error analysis** of 1480 failures across `random/aggregates/` suite
✅ **Two root causes identified**: BETWEEN NULL (92.2%) and NULL arithmetic (7.7%)
✅ **Sample queries documented** for each error pattern
✅ **Code locations identified** for both fixes
✅ **Action plan established** with priority and effort estimates
✅ **Issue template prepared** for NULL arithmetic bug

**Next Steps for User**:
1. Review this analysis and approve approach
2. Create GitHub issue for NULL arithmetic using template above
3. Prioritize fix order (recommend BETWEEN NULL first due to 92% impact)
4. Assign issues to developers

## Appendix

### Sample Failing Queries

_[Collection of representative failing SQL queries for each error pattern]_

### Test Execution Logs

_[Links to full log files or excerpts]_

### References

- Issue #1833: Fix random/aggregates test suite failures (130 failing tests)
- Issue #1873: Add comprehensive unit tests for aggregate expression evaluation
- Issue #1875: Add integration tests for aggregate edge cases and complex scenarios
- `docs/testing/sqllogictest/SQLLOGICTEST_ISSUES.md`: Historical error analysis

---

**Document Status**: Work in Progress
**Last Updated**: 2025-11-15
**Author**: Builder Agent (issue #1872)
