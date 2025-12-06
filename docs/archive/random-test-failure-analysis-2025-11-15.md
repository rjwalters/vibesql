# Random Test Failure Analysis
**Date**: November 15, 2025
**Git Commit**: 8f0fc21de6838110b0a250dc2b14c9b87dad0bf5
**Test Run**: November 14, 2025 23:03:04

## Executive Summary

Analysis of 381 random test failures across 391 total random tests (2.6% pass rate).

**Key Findings**:
- **NULL aggregate handling**: Most common pattern - aggregates return 0 instead of NULL
- **ORDER BY issues**: Index optimization causing incorrect result ordering
- **BETWEEN with NULL**: Incorrect evaluation when NULL is involved
- **IN clause edge cases**: Issues with IN/NOT IN operator evaluation

## Test Coverage Baseline

### Overall Results (All Categories)
- **Total files**: 616
- **Passed**: 132 (21.4%)
- **Failed**: 484 (78.6%)

### Random Category Breakdown
- **Total**: 391 tests
- **Passed**: 10 (2.6%)
- **Failed**: 381 (97.4%)

#### By Subcategory
1. `random/aggregates`: 130 failed
2. `random/expr`: 120 failed
3. `random/select`: 118 failed
4. `random/groupby`: 13 failed

## Error Pattern Analysis

### Pattern 1: NULL Aggregate Returns Zero ⚠️ **HIGH IMPACT**
**Tests affected**: ~120-150 (estimated 35-40% of random failures)
**Subcategories**: Primarily `random/aggregates`, some `random/expr`
**Complexity**: Medium

**Problem**: Aggregate functions (SUM, AVG, etc.) return 0 instead of NULL when:
- All input values are NULL
- WHERE clause filters out all rows
- Aggregating over empty result set

**Examples**:
```sql
-- Example from random/aggregates/slt_good_10.test
SELECT + SUM( CAST( NULL AS SIGNED ) ) AS col1 FROM tab1
-- Expected: NULL
-- Actual: 0.000

-- Example from random/aggregates/slt_good_107.test
SELECT - SUM( 1 ) FROM tab0 AS cor0 WHERE NOT NULL NOT IN ( - col1 )
-- Expected: NULL
-- Actual: 0.000

-- Example from random/aggregates/slt_good_129.test
SELECT ALL + - SUM( 47 ) AS col1, + 39 * - COUNT( * ) AS col1
FROM tab1 WHERE NULL IS NOT NULL
-- Expected: NULL  0
-- Actual: 0.000  0
```

**Root Cause**: Aggregate functions not properly handling the empty set / all-NULL case according to SQL standard.

**Fix Complexity**: Medium - requires updating aggregate function implementations to distinguish between:
- Empty result set → NULL
- Result set with non-NULL values summing to 0 → 0

**Related Issues**: Potentially covered by #1760 or #1763

---

### Pattern 2: ORDER BY Result Mismatch ⚠️ **MEDIUM-HIGH IMPACT**
**Tests affected**: ~30-40 (estimated 10% of random failures)
**Subcategories**: Affects multiple categories including random tests
**Complexity**: High (recently disabled optimization in #1793)

**Problem**: Queries with ORDER BY returning results in wrong order.

**Examples**:
```sql
-- Example from index/orderby/10/slt_good_12.test
SELECT pk FROM tab1 WHERE (col1 >= 102.10) ORDER BY 1 DESC
-- Diff: Wrong ordering of results

-- Example from index/orderby/10/slt_good_2.test
SELECT pk FROM tab1 WHERE (col3 > 444) ORDER BY 1 DESC
-- Diff: Results out of order
```

**Root Cause**: ORDER BY index optimization was causing incorrect ordering (disabled in #1793).

**Status**: Partially addressed - optimization disabled. May need further investigation for correctness.

**Related Issues**: #1793, #1792

---

### Pattern 3: BETWEEN with NULL Edge Cases ⚠️ **MEDIUM IMPACT**
**Tests affected**: ~15-25 (estimated 5-7% of random failures)
**Subcategories**: `random/expr`, some index tests
**Complexity**: Medium

**Problem**: BETWEEN operator not correctly handling NULL boundaries.

**Examples**:
```sql
-- Example from index/random/10/slt_good_12.test
SELECT ALL 44 AS col0 FROM tab0 AS cor0
WHERE NOT + 28 BETWEEN NULL AND - col3
-- Expected: 10 values
-- Actual: 0 values (empty result)

-- Example from index/random/10/slt_good_7.test
SELECT DISTINCT - 19 FROM tab0 cor0
WHERE col3 NOT BETWEEN + 31 AND NULL
-- Expected: -19
-- Actual: (empty)
```

**Root Cause**: BETWEEN operator returns NULL (or false?) when either boundary is NULL, but should follow three-valued logic:
- `x BETWEEN a AND b` when a or b is NULL → NULL (not false)
- `NOT (NULL)` → NULL (not true)

**Fix Complexity**: Medium - update BETWEEN operator implementation to properly handle NULL in three-valued logic.

---

### Pattern 4: IN Clause Edge Cases ⚠️ **MEDIUM IMPACT**
**Tests affected**: ~15-20 (estimated 4-5% of random failures)
**Subcategories**: `random/expr`, index tests
**Complexity**: Medium

**Problem**: IN and NOT IN operators not handling NULL correctly.

**Examples**:
```sql
-- Example from index/in/10/slt_good_1.test
SELECT pk FROM tab3
WHERE col4 >= 0.44 AND (((col4 > 22.9)) AND (col0 IN (93,27,65,63,91)))
-- Result mismatch

-- Example from index/view/100/slt_good_2.test
SELECT pk, col0 FROM tab2 WHERE (col0 IN (844,789))
-- Result mismatch
```

**Root Cause**: IN clause evaluation may not be correctly handling:
- NULL in the value list
- NULL as the tested value
- Interaction with other predicates

**Fix Complexity**: Medium - review IN operator implementation for NULL handling.

---

### Pattern 5: Result Mismatch - Other Cases ⚠️ **VARIABLE IMPACT**
**Tests affected**: ~180-200 (estimated 50% of failures)
**Subcategories**: All random subcategories
**Complexity**: Variable (needs further categorization)

**Problem**: General result mismatches that don't fit other patterns. Likely includes:
- Type conversion issues
- Expression evaluation edge cases
- Operator precedence issues
- CAST problems
- Division by zero handling
- Other SQL semantics

**Next Steps**: Requires deeper analysis with detailed error message examination to break into sub-patterns.

**Examples**:
```sql
-- Needs detailed examination of error messages
```

---

## Comparison to November 15 Baseline

**Previous State** (Issue #1794 description):
- Total random tests: 391
- Pass rate: 2.5% (estimated 244 tested, 6 passed, 238 failed)
- Coverage: 62.4% (147 untested due to timeouts)

**Current State** (November 14, 2025 run):
- Total random tests: 391
- **Pass rate: 2.6%** (10 passed, 381 failed)
- **Coverage: 100%** (all tests completed)

**Improvement**:
- ✅ **Coverage improved**: 62.4% → 100% (+37.6 percentage points)
- ✅ **All tests now complete**: No timeouts
- ⚠️ **Pass rate essentially unchanged**: 2.5% → 2.6% (minimal improvement)

**Analysis**: Recent fixes (#1772, #1780, #1792, #1793) have improved performance/reliability (tests complete faster), but have not significantly improved random test conformance. The core SQL semantic issues remain.

---

## Recommended Fix Priority

### Priority 1: NULL Aggregate Handling (Issue to create)
- **Impact**: 35-40% of random failures (~140 tests)
- **Effort**: Medium
- **Impact/Effort ratio**: HIGH
- **Recommendation**: Create dedicated issue with examples and implementation guidance

### Priority 2: BETWEEN NULL Handling (Issue to create)
- **Impact**: 5-7% of random failures (~20 tests)
- **Effort**: Medium
- **Impact/Effort ratio**: MEDIUM
- **Recommendation**: Create dedicated issue

### Priority 3: IN Clause NULL Handling (Issue to create)
- **Impact**: 4-5% of random failures (~18 tests)
- **Effort**: Medium
- **Impact/Effort ratio**: MEDIUM
- **Recommendation**: May combine with BETWEEN issue as "NULL in predicates"

### Priority 4: General Result Mismatches (Needs analysis)
- **Impact**: ~50% of failures (~200 tests)
- **Effort**: Unknown (needs categorization)
- **Impact/Effort ratio**: Unknown
- **Recommendation**: Create analysis issue to break down into specific patterns

### Priority 5: ORDER BY Issues (Monitor)
- **Impact**: 10% of failures (~40 tests)
- **Effort**: High (optimization disabled)
- **Impact/Effort ratio**: MEDIUM-LOW
- **Recommendation**: Monitor after other fixes; may be addressed by #1793

---

## Next Steps

1. ✅ **Complete this analysis**
2. **Create targeted GitHub issues**:
   - Issue A: Fix NULL aggregate function returns
   - Issue B: Fix BETWEEN operator NULL handling
   - Issue C: Fix IN/NOT IN operator NULL handling
   - Issue D: Analyze remaining result mismatches
3. **Post summary to #1794**
4. **Begin implementation** starting with Priority 1

---

## Appendix: Sample Error Messages

### NULL Aggregate Pattern
```
query result mismatch:
[SQL] SELECT + SUM( CAST( NULL AS SIGNED ) ) AS col1 FROM tab1
[Diff] (-expected|+actual)
- NULL
+ 0.000
```

### ORDER BY Pattern
```
query result mismatch:
[SQL] SELECT pk FROM tab1 WHERE (col1 >= 102.10) ORDER BY 1 DESC
[Diff] (-expected|+actual)
- 9 values hashing to 4290bd41ca7ca69dc280e33882d8e9de
+ 0 + 1 + 2 + 3 + 5 + 6 + 8 + 9
```

### BETWEEN NULL Pattern
```
query result mismatch:
[SQL] SELECT ALL 44 AS col0 FROM tab0 AS cor0
WHERE NOT + 28 BETWEEN NULL AND - col3
[Diff] (-expected|+actual)
- 10 values hashing to 31ff74766f82255c19cf632a967a9c0e
+ (empty)
```

---

**Analysis completed**: November 15, 2025
**Analyzer**: Builder (Loom Issue #1794)
