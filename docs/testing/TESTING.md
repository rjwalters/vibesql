# SQLLogicTest Testing Strategy

This document describes our approach to achieving 100% SQLLogicTest compliance.

## Current Status

As of 2025-11-08:
- **Coverage**: 98.4% (613/623 files tested)
- **Pass Rate**: 13.5% (83/613 files passing)
- **Failing**: 530 files
- **Untested**: 10 files

## Testing Infrastructure

### Running the Full Test Suite

We use parallel testing for comprehensive coverage:

```bash
# Run full suite with parallel workers (uses all available CPU cores by default)
./scripts/sqllogictest run --parallel --time 300

# Check status of most recent test run
./scripts/sqllogictest status

# View detailed results
./scripts/sqllogictest query --preset failed-files
```

### Test Result Files

- `target/sqllogictest_cumulative.json` - Aggregated results from all workers
- `target/sqllogictest_results_analysis.json` - Detailed failure analysis with error categories
- `/tmp/sqllogictest_results/worker_*.log` - Individual worker logs
- `/tmp/sqllogictest_results/worker_*_analysis.json` - Per-worker analysis

## Current Test Results Breakdown

### Passing Categories (83 files)

**Strong Areas:**
- `index/commute/*` - 35+ files - Commutative property tests
- `index/between/*` - 12 files - BETWEEN clause tests
- `index/in/*` - 11 files - IN clause tests
- `evidence/*` - 6 files - Basic language feature tests
- `index/random/1000/*` - 2 files - Random index tests
- `random/select/*` - 2 files - Random SELECT tests

### Failing Categories (530 files)

**Major Failure Types:**

1. **Result Mismatch (325 failures)** - 61% of failures
   - Decimal formatting: `7.000` vs `7`, `95.000` vs `95`
   - Multi-row vs single-row results
   - Hash mismatches in complex queries
   - Row ordering issues

2. **ColumnNotFound (39 failures)**
   - Case sensitivity issues (COL0 vs col0)
   - Table alias resolution problems
   - Cross-join column resolution

3. **TypeMismatch (33 failures)**
   - `NOT NULL` operations
   - Type coercion in expressions
   - Integer vs Numeric arithmetic

4. **Parse Errors (27 failures)**
   - Complex expression parsing
   - Nested operations
   - Operator precedence

5. **UnsupportedExpression (18 failures)**
   - Missing aggregate functions: NULLIF, COALESCE
   - Unary plus in aggregates: `SUM(+col2)`
   - Nested function calls

## Problem: Slow Progress

### Why Progress is Slow

1. **Many-to-One Relationship**: Different test failures often stem from the same root cause
2. **Cross-Cutting Issues**: Formatting problems affect hundreds of tests
3. **Compound Failures**: Tests fail for multiple reasons, fixing one doesn't help
4. **Random Test Complexity**: Random tests combine many features, any single failure fails the whole test

### Example: The Decimal Formatting Problem

The issue where we return `7` instead of `7.000` affects ~50+ tests across multiple categories:
- `random/select/slt_good_21.test`
- `random/select/slt_good_49.test`
- `random/select/slt_good_84.test`
- `index/orderby_nosort/*/slt_good_*.test`
- And many more...

Fixing this ONE issue could improve our pass rate by 8-10%.

## Proposed: More Efficient Process

### 1. Root Cause Analysis

Instead of creating issues for individual test failures, analyze failure categories:

```bash
# Analyze test failures with clustering and pattern analysis
./scripts/sqllogictest analyze

# Or run via the test runner
./scripts/sqllogictest run --parallel --analyze
```

**Output includes:**
- Error type frequency
- Common error patterns
- Impact analysis (how many tests affected)
- Clustered failures by similarity

### 2. Prioritization Matrix

Focus on fixes with highest impact:

| Priority | Category | Estimated Tests Affected | Effort | Impact/Effort |
|----------|----------|-------------------------|--------|---------------|
| P0 | Decimal formatting | 50+ | Low | Very High |
| P0 | Multi-row result formatting | 100+ | Medium | High |
| P1 | NULLIF/COALESCE functions | 18+ | Medium | Medium |
| P1 | Case-insensitive column resolution | 39+ | Medium | Medium |
| P2 | NOT NULL type coercion | 33+ | High | Low |
| P3 | Complex parse errors | 27+ | High | Low |

### 3. Batch Testing Strategy

Instead of full suite runs after each fix:

```bash
# Test specific category after fix
cargo test --test sqllogictest_suite -- --test-threads=1 random/select

# Or test specific file
cargo test --test sqllogictest_suite -- --exact random/select/slt_good_21.test
```

### 4. Regression Testing

After fixing high-impact issues, verify improvements:

```bash
# Quick smoke test (10 minutes, samples across categories)
SQLLOGICTEST_TIME_BUDGET=600 cargo test --test sqllogictest_suite

# Compare pass rates before/after
python3 scripts/compare_test_runs.py \
  target/sqllogictest_cumulative_before.json \
  target/sqllogictest_cumulative_after.json
```

## Recommended Workflow

### Phase 1: Analysis (Week 1-2)

1. ✅ Run comprehensive test suite (DONE - 98.4% coverage)
2. ✅ Aggregate and categorize failures (DONE - 530 failures categorized)
3. **TODO**: Create detailed root cause analysis script
4. **TODO**: Build prioritization matrix based on impact
5. **TODO**: Identify top 5 highest-impact fixes

### Phase 2: High-Impact Fixes (Week 3-4)

Focus on the top 5 issues that affect the most tests:

1. **Fix #1: Decimal Formatting** (~50+ tests)
   - Issue: Return `7.000` instead of `7` for decimal context
   - Location: `crates/executor/src/evaluator/operators/arithmetic.rs`
   - Expected improvement: +8-10% pass rate

2. **Fix #2: Multi-row Result Formatting** (~100+ tests)
   - Issue: Results returned as single row instead of multiple rows
   - Location: `crates/executor/src/result_formatter.rs`
   - Expected improvement: +15-20% pass rate

3. **Fix #3: Add NULLIF/COALESCE** (~18+ tests)
   - Issue: Missing SQL functions
   - Location: `crates/executor/src/evaluator/functions/`
   - Expected improvement: +3% pass rate

4. **Fix #4: Case-insensitive Column Resolution** (~39+ tests)
   - Issue: COL0 vs col0 not matching
   - Location: `crates/executor/src/resolver/column_resolver.rs`
   - Expected improvement: +6% pass rate

5. **Fix #5: Unary Plus in Aggregates** (~18+ tests)
   - Issue: `SUM(+col2)` not supported
   - Location: `crates/executor/src/evaluator/aggregates/`
   - Expected improvement: +3% pass rate

**Expected Total Improvement: +35-42% pass rate → Target: 48-55%**

### Phase 3: Medium-Impact Fixes (Week 5-6)

Address issues affecting 10-30 tests each:
- NOT NULL type coercion
- Complex operator precedence
- Subquery support
- Additional aggregate functions

### Phase 4: Long Tail (Ongoing)

Address individual test failures and edge cases.

## Success Metrics

Track progress with each major fix:

```bash
# Before fix
Pass Rate: 13.5% (83/613)

# After Fix #1 (Decimal Formatting)
Target: 21-24% (130-145/613)

# After Fix #2 (Multi-row Formatting)
Target: 36-44% (220-270/613)

# After all 5 high-impact fixes
Target: 48-55% (295-337/613)

# After medium-impact fixes
Target: 65-75% (400-460/613)

# After long tail cleanup
Target: 95-100% (580-613/613)
```

## Tools to Build

### 1. Test Run Comparator

```bash
# Compare two test runs
python3 scripts/compare_test_runs.py \
  before.json after.json \
  --show-improvements \
  --show-regressions
```

### 3. Impact Estimator

```bash
# Estimate impact of fixing an error category
python3 scripts/estimate_fix_impact.py \
  target/sqllogictest_results_analysis.json \
  --error-pattern "Decimal formatting" \
  --similar-errors
```

## Documentation Maintenance

Update this document after each major milestone:
- [ ] After Phase 1 Analysis - Update root cause findings
- [ ] After each high-impact fix - Update actual vs expected improvements
- [ ] After Phase 2 completion - Reassess remaining issues
- [ ] Monthly - Update current status and metrics

## References

- SQLLogicTest Suite: `tests/sqllogictest/`
- Test Runner: `tests/sqllogictest_suite.rs`
- CLI Tool: `scripts/sqllogictest`
- Database: `scripts/schema/test_results.sql`
- Test Files: `tests/sqllogictest/test_files/`

## Questions to Answer

1. **Are we fixing the right things?**
   - Use impact analysis to validate priorities
   - Focus on issues affecting 20+ tests first

2. **How do we measure progress?**
   - Track pass rate after each major fix
   - Compare against expected improvements
   - Document actual impact vs estimates

3. **When do we rerun the full suite?**
   - After completing a batch of related fixes
   - Weekly full suite run for regression detection
   - Before releases/milestones

4. **How do we avoid regressions?**
   - Maintain passing test list
   - Run smoke tests before full suite
   - CI integration for pull requests

## Next Steps

1. **Immediate**: Create failure pattern analyzer script
2. **This week**: Build prioritization matrix with impact estimates
3. **This week**: Create detailed plan for top 5 fixes
4. **Next sprint**: Implement Fix #1 (Decimal Formatting) and measure impact
5. **Ongoing**: Update this document with actual results vs estimates
