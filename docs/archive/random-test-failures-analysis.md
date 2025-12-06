# Random/* Test Failures - Comprehensive Analysis

**Date**: 2025-11-11
**Issue**: #1261
**Analyst**: Builder Agent
**Data Source**: Test results from `/tmp/sqllogictest_results/worker_*_analysis.json`

## Executive Summary

Analyzed **467 random/* test failures** from recent test runs. The analysis reveals that **65.7% of all failures are caused by a single issue: decimal formatting inconsistencies**.

### Key Findings

1. **Decimal Formatting (65.7%)**: Expected integers formatted as decimals (or vice versa)
2. **Hash Mismatches (10.3%)**: Result hash comparison failures
3. **Multi-Column Formatting (8.6%)**: Columns on single line vs multiple rows
4. **Execution Errors (7.9%)**: Type mismatches and cast errors
5. **Logic Errors (3.9%)**: Wrong or missing query results
6. **Parse Errors (3.6%)**: Unsupported SQL syntax (@@variables)
7. **Unsupported Features (0.9%)**: Missing SQL features

## Detailed Breakdown

### Overall Statistics

```
Total random/* tests analyzed: 440
Passed: 2 (0.5%)
Failed: 438 (99.5%)

Test failures analyzed: 467 individual failures
```

### Error Category Distribution

| Category | Count | % of Total | % of Result Mismatches |
|----------|-------|------------|------------------------|
| **Decimal Formatting** | 307 | 65.7% | 74.3% |
| **Hash Mismatch** | 48 | 10.3% | 11.6% |
| **Multi-Column Format** | 40 | 8.6% | 9.7% |
| **Execution Error** | 37 | 7.9% | N/A |
| **Logic Error** | 18 | 3.9% | 4.4% |
| **Parse Error** | 17 | 3.6% | N/A |
| **Unsupported Feature** | 4 | 0.9% | N/A |

Result Mismatches account for **413/467 (88.4%)** of all failures.

## Pattern Deep-Dive

### 1. Decimal Formatting Issues (307 failures, 65.7%)

**Problem**: Inconsistent decimal formatting between expected and actual results.

**Pattern A: Adding .000 to integers**
```sql
-- Example 1
[SQL] SELECT - 61 * + ( + 62 )
Expected: -3782
Actual:   -3782.000

-- Example 2
[SQL] SELECT ALL 16 * 70
Expected: 1120
Actual:   1120.000
```

**Pattern B: Removing .000 from expected decimals**
```sql
-- Example 3
[SQL] SELECT ALL - - 59 * col0 col1 FROM tab1 AS cor0
Expected: 177.000, 3776.000, 4720.000
Actual:   177, 3776, 4720
```

**Root Cause**:
- SQLLogicTest expects specific numeric formatting based on MySQL dialect
- Our implementation formats numbers differently
- Likely related to result serialization in MySQL compatibility mode

**Impact**: This single issue causes **2/3 of all random/* test failures**

**Recommendation**:
- **Issue #1265**: "Fix numeric formatting in MySQL dialect mode"
- Priority: **CRITICAL** - Largest single failure cause
- Estimated fix difficulty: **Medium** - Requires result formatter changes
- Estimated impact: ~**300-350 tests** could pass after fix (from 2 → ~300-350)

### 2. Hash Mismatch (48 failures, 10.3%)

**Problem**: Result set hashing doesn't match expected hash.

**Example**:
```sql
[SQL] SELECT DISTINCT - col1 FROM tab0 WHERE NOT NULL IS NULL
Expected: (empty result)
Actual:   100 values hashing to 9421b20999603213858f1c22428f1a05
```

**Root Cause**:
- Tests use hash comparison for large result sets
- Hash algorithm or result ordering may differ
- Could be related to decimal formatting affecting hash calculation

**Recommendation**:
- Investigate after decimal formatting fix (may resolve automatically)
- If persists, create issue for result set hashing investigation

### 3. Multi-Column Formatting (40 failures, 8.6%)

**Problem**: Multiple columns output on single line instead of separate rows.

**Example**:
```sql
[SQL] SELECT ALL + col1 AS col0, col2 AS col2 FROM tab1
Expected:
  14
  96
  47
  68
  5
  59
Actual:
  14 96
  47 68
  5 59
```

**Root Cause**:
- SQLLogicTest expects each value on separate line (row-oriented)
- Our output format combines columns on single line (column-oriented)
- Likely result formatter configuration issue

**Recommendation**:
- **Issue #1266**: "Fix multi-column result formatting for SQLLogicTest"
- Priority: **HIGH** - Affects ~40 tests
- Estimated fix difficulty: **Low-Medium** - Result formatter adjustment
- Should be addressed after decimal formatting fix (#1265)

### 4. Execution Errors (37 failures, 7.9%)

**Problem**: Runtime execution errors during query evaluation.

**Pattern A: Type Mismatches (most common)**
```sql
-- Boolean/Numeric operation type errors
[SQL] SELECT ALL * FROM tab0 WHERE NOT ( - 91 ) * + 31 NOT IN ( - 44 * 94 )
Error: TypeMismatch { left: Boolean(false), op: "*", right: Numeric(31.0) }
```

**Pattern B: Cast Errors**
```sql
[SQL] SELECT - 11 * CAST( + 17 AS SIGNED ) AS col1, + 16
Error: CastError { from_type: "Numeric(17.0)", to_type: "INTEGER" }
```

**Root Cause**:
- Type system doesn't handle all implicit conversions
- CAST to SIGNED with Numeric type not supported
- Boolean arithmetic evaluation issues

**Recommendation**:
- **Issue #1267**: "Fix type coercion and CAST errors in random/* tests"
- Priority: **MEDIUM** - Affects ~37 tests
- Estimated fix difficulty: **Medium-High** - Type system changes
- Examples: Boolean arithmetic, Numeric → INTEGER casts

### 5. Logic Errors (18 failures, 3.9%)

**Problem**: Query returns wrong results or missing rows.

**Example**:
```sql
[SQL] SELECT DISTINCT 22 AS col0 FROM tab0 AS cor0 WHERE NOT + 93 * 1 = - col2
Expected: 22
Actual:   (empty)
```

**Root Cause**:
- Various query evaluation logic bugs
- WHERE clause evaluation issues
- DISTINCT handling problems

**Recommendation**:
- Needs individual investigation per failure
- Likely multiple underlying bugs
- Priority: **LOW** - Small impact, diverse causes

### 6. Parse Errors (17 failures, 3.6%)

**Problem**: SQL syntax not supported by parser.

**Pattern**: All 17 failures are the **same error**:
```sql
[SQL] SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))
Error: Lexer error at position 37: Unexpected character: '@'
```

**Root Cause**:
- MySQL session variable syntax `@@variable` not supported
- Parser doesn't recognize `@@` prefix

**Recommendation**:
- **Issue #1268**: "Support MySQL session variables (@@syntax)"
- Priority: **LOW-MEDIUM** - Affects 17 tests (all same issue)
- Estimated fix difficulty: **Low** - Parser lexer update
- Single fix resolves all 17 failures

### 7. Unsupported Features (4 failures, 0.9%)

**Problem**: SQL features not yet implemented.

**Recommendation**:
- Individual assessment needed
- Priority: **LOW** - Minimal impact

## Impact Assessment

### If We Fix Decimal Formatting Only

**Conservative Estimate**:
- Direct fixes: ~280-300 tests (decimal formatting)
- Indirect fixes: ~30-40 tests (hash mismatches may resolve)
- **New pass rate: 65-70%** (up from 0.5%)

**Optimistic Estimate**:
- Direct + indirect: ~350 tests
- **New pass rate: ~80%** (up from 0.5%)

### If We Fix Top 3 Issues

1. Decimal formatting: ~307 tests
2. Hash mismatches: ~48 tests (may auto-fix with #1)
3. Multi-column formatting: ~40 tests

**Estimated new pass rate: 85-90%**

## Recommended Action Plan

### Phase 1: Quick Win (Highest ROI)
**Target**: 65-70% pass rate

1. **Issue #1265**: Fix decimal formatting in MySQL dialect mode
   - **Priority**: CRITICAL
   - **Difficulty**: Medium
   - **Impact**: ~300-350 tests
   - **Assignee**: Builder
   - **Labels**: `loom:issue`

### Phase 2: Formatting Fixes
**Target**: 85-90% pass rate

2. **Issue #1266**: Fix multi-column result formatting
   - **Priority**: HIGH
   - **Difficulty**: Low-Medium
   - **Impact**: ~40 tests
   - **Assignee**: Builder
   - **Labels**: `loom:issue`

3. **Re-evaluate hash mismatches** after Phase 1
   - May auto-resolve with decimal formatting fix (#1265)

### Phase 3: Type System Improvements
**Target**: 90-95% pass rate

4. **Issue #1267**: Fix type coercion and CAST errors
   - **Priority**: MEDIUM
   - **Difficulty**: Medium-High
   - **Impact**: ~37 tests
   - **Assignee**: Builder
   - **Labels**: `loom:issue`

### Phase 4: Parser Improvements
**Target**: 93-96% pass rate

5. **Issue #1268**: Support MySQL session variables (@@syntax)
   - **Priority**: LOW-MEDIUM
   - **Difficulty**: Low
   - **Impact**: 17 tests
   - **Assignee**: Builder
   - **Labels**: `loom:issue`

### Phase 5: Individual Bug Fixes
**Target**: 95%+ pass rate

6. Address logic errors individually
7. Implement remaining unsupported features

## Success Metrics

| Milestone | Pass Rate | Tests Passing | Completion |
|-----------|-----------|---------------|------------|
| Current | 0.5% | 2/440 | Baseline |
| Phase 1 Complete | 65-70% | ~300/440 | Q1 2025 |
| Phase 2 Complete | 85-90% | ~380/440 | Q2 2025 |
| Phase 3 Complete | 90-95% | ~400/440 | Q3 2025 |
| Phase 4 Complete | 93-96% | ~415/440 | Q4 2025 |
| Ultimate Goal | 95%+ | 418+/440 | 2026 |

## Appendix: Data Collection Method

Data collected from parallel test run results in `/tmp/sqllogictest_results/`:
- 8 worker analysis files
- Test run date: ~2025-11-11
- Analysis scripts used:
  - Custom Python analysis scripts
  - Pattern matching and categorization
  - Statistical aggregation across workers

## Related Issues

- #1200 - Overall SQLLogicTest conformance tracking
- #1230 - SQL dialect modes for MySQL compatibility
- #1261 - This analysis issue
- #1265 - Fix numeric formatting (MySQL dialect) - **65.7% impact**
- #1266 - Fix multi-column result formatting - **8.6% impact**
- #1267 - Fix type coercion and CAST errors - **7.9% impact**
- #1268 - Support MySQL session variables - **3.6% impact**

---

**Analysis completed**: 2025-11-11
**Next step**: Create targeted sub-issues and begin Phase 1 implementation
