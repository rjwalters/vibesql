# SQLLogicTest Random Test Suite Analysis

**Issue:** #1805
**Analysis Date:** 2025-11-15
**Baseline Test Run:** 2025-11-15 (commit: 1b5d5854d9fb4d8600b7990f908ea85f90ca1612)

## Executive Summary

The SQLLogicTest random test suite currently has a **3.3% pass rate** (13/391 files passing). This analysis establishes a baseline, identifies key characteristics of the failures, and recommends a systematic approach for incremental improvement.

## Current Baseline

### Overall Random Test Results

| Subcategory | Passing | Failing | Total | Pass Rate |
|-------------|---------|---------|-------|-----------|
| aggregates  | 0       | 130     | 130   | 0%        |
| expr        | 7       | 113     | 120   | 5.8%      |
| groupby     | 1       | 13      | 14    | 7.1%      |
| select      | 5       | 122     | 127   | 3.9%      |
| **Total**   | **13**  | **378** | **391** | **3.3%**  |

### Passing Tests

The following tests are currently passing:

**expr (7 passing):**
- `random/expr/slt_good_1.test`
- `random/expr/slt_good_23.test`
- `random/expr/slt_good_26.test`
- `random/expr/slt_good_32.test`
- `random/expr/slt_good_33.test`
- `random/expr/slt_good_43.test`
- `random/expr/slt_good_98.test`

**groupby (1 passing):**
- `random/groupby/slt_good_13.test`

**select (5 passing):**
- `random/select/slt_good_3.test`
- `random/select/slt_good_5.test`
- `random/select/slt_good_7.test`
- `random/select/slt_good_125.test`
- `random/select/slt_good_126.test`

**aggregates (0 passing):**
- All 130 files failing

## Test Characteristics

### Test File Scale

Random test files are extremely comprehensive:
- **Typical file size**: 78,000-90,000 lines
- **Queries per file**: 13,000-15,000 queries
- **Test approach**: Randomly generated SQL stress tests

Example:
- `slt_good_1.test` (passing): 78,043 lines, ~13,883 queries
- `slt_good_0.test` (failing): 90,403 lines, ~15,426 queries

### Test Content

The random tests exercise:

1. **Complex Expressions**
   - Nested arithmetic and logical operations
   - Type coercion scenarios
   - NULL propagation in expressions
   - Operator precedence edge cases

2. **Aggregate Functions**
   - COUNT, SUM, AVG, MIN, MAX with various combinations
   - DISTINCT with aggregates
   - Aggregates with NULL values
   - Nested aggregates
   - GROUP BY with complex expressions
   - HAVING clauses

3. **SELECT Operations**
   - Multi-table joins (CROSS JOIN, LEFT JOIN, etc.)
   - Subqueries (correlated and uncorrelated)
   - UNION/INTERSECT/EXCEPT operations
   - Complex WHERE clauses
   - Result ordering and sorting

4. **Special Features**
   - `hash-threshold` directive for hash-based result verification
   - `onlyif mysql` / `skipif mysql` conditional directives
   - Result hash verification (e.g., "9 values hashing to 75c998aa...")

## Related Work Context

### Recent Fixes That Should Help

The following NULL handling fixes were recently merged and should improve pass rates:
- #1801 - Fix NULL aggregate returns (SUM/AVG for empty sets)
- #1803 - Fix BETWEEN operator NULL handling
- #1802 - Fix IN operator NULL handling in index optimization
- #1769 - Fix NULL handling in BETWEEN predicates
- #1735 - Add NULL propagation to arithmetic operators

**Note:** These fixes were included in the baseline test run, so current results already reflect their benefits.

### Related Tracking Issues

- #1810 - Full SQLLogicTest suite results (22.4% overall pass rate)
- #1729 - SQLLogicTest 100% Pass Rate Roadmap
- #1807 - Index optimization failures (45 files)
- #1806 - Index ORDER BY failures (52 files)
- #1808 - DDL and other test failures (4 files)

## Key Findings

### 1. Aggregate Tests Have 0% Pass Rate

**Impact:** High (130 files failing)
**Severity:** Critical

All 130 aggregate test files are failing, suggesting systematic issues with:
- Aggregate function edge cases
- NULL handling in aggregates (despite recent fixes)
- DISTINCT with aggregates
- Window functions
- Aggregate type handling

### 2. Test Scale Makes Debugging Difficult

**Impact:** Process
**Severity:** High

With 13,000-15,000 queries per file, identifying specific failure points is challenging:
- Cannot easily isolate which query in a file causes failure
- Need better tooling to extract first failure from test runs
- Manual debugging is impractical

### 3. Hash-Based Result Verification

**Impact:** Medium
**Severity:** Medium

Many tests use hash-based result verification:
- Tests specify `hash-threshold 8`
- Results are verified by hash (e.g., "9 values hashing to...")
- May require specific hash implementation in test runner
- Result ordering differences could cause hash mismatches

### 4. MySQL-Specific Conditionals

**Impact:** Low
**Severity:** Low

Tests include MySQL-specific sections:
- `onlyif mysql` / `skipif mysql` directives
- Should be handled by test runner, but may affect coverage

## Recommended Approach

Given the scale and complexity, we recommend an **incremental, pattern-based approach**:

### Phase 1: Enhanced Diagnostics (Immediate)

1. **Improve test runner output** to show:
   - First failing query in each file
   - Specific error message for failure
   - Line number of failure

2. **Create sampling script** to:
   - Test first N queries from each file
   - Binary search for first failure
   - Extract minimal failing examples

### Phase 2: Pattern Identification (Week 1)

1. **Sample 10 files from each subcategory**
2. **Extract first failure from each**
3. **Group by error pattern:**
   - Parse errors
   - Type errors
   - NULL handling errors
   - Aggregate errors
   - Join/subquery errors

4. **Create focused issues** for top 5-10 patterns

### Phase 3: Systematic Fixes (Ongoing)

1. **Fix patterns in priority order** (by impact)
2. **Re-run full suite after each fix**
3. **Track progress** toward 50% pass rate milestone
4. **Document wins** and share learnings

### Phase 4: Continuous Improvement

1. **Automate pattern detection**
2. **Add regression tests** for fixed patterns
3. **Monitor pass rate** in CI
4. **Celebrate milestones** (25%, 50%, 75%, 100%)

## Success Criteria

This tracking issue should be closed when we achieve:

1. ✅ **Baseline established** - DONE (3.3% pass rate documented)
2. ⏳ **Top 5-10 failure patterns identified**
3. ⏳ **Focused issues created** for each pattern
4. ⏳ **At least 3 major patterns fixed**
5. ⏳ **Pass rate ≥ 50%** (197+ files passing)

## Next Steps

### Immediate Actions

1. **Run targeted test sampling:**
   ```bash
   # Test first 100 queries from each failing file
   for file in $(ls third_party/sqllogictest/test/random/aggregates/*.test | head -5); do
     head -1000 "$file" > "/tmp/sample_$(basename $file)"
     ./scripts/sqllogictest test "/tmp/sample_$(basename $file)"
   done
   ```

2. **Enhance test runner** to output first failure details

3. **Create sampling/bisection tool** for finding minimal failures

### Follow-up Issues

Based on patterns found, create focused issues like:
- "Fix CASE expression NULL handling in aggregates"
- "Fix hash-based result verification"
- "Fix DISTINCT with COUNT aggregate"
- "Fix correlated subqueries in random tests"
- etc.

## Appendix: Test Infrastructure

### Test Execution

```bash
# Test single file
./scripts/sqllogictest test random/aggregates/slt_good_0.test

# Run all random tests (parallel, uses all available CPU cores by default)
./scripts/sqllogictest run --parallel --time 900

# Query results
./scripts/sqllogictest query --query "
  SELECT category, subcategory, status, COUNT(*) as count
  FROM test_files
  WHERE category = 'random'
  GROUP BY category, subcategory, status
"
```

### Test Database

Results stored in: `~/.vibesql/test_results/sqllogictest_results.sql`

Schema:
- `test_files`: FILE_PATH, CATEGORY, SUBCATEGORY, STATUS, LAST_TESTED, LAST_PASSED
- `test_results`: RESULT_ID, RUN_ID, FILE_PATH, STATUS, TESTED_AT, DURATION_MS

### Relevant Code

Implementation areas affected (in `crates/vibesql-executor/src/`):
- `tests/aggregate_*.rs` - Aggregate function tests
- `select/grouping.rs` - GROUP BY implementation
- `select/executor/execute.rs` - SELECT execution
- `select/executor/index_optimization/` - Index optimization

---

**Document Version:** 1.0
**Last Updated:** 2025-11-15
**Maintained by:** Issue #1805 tracking
