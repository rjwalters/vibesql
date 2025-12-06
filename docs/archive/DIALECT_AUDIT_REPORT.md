# Dialect-Specific Test Audit Report

**Issue**: #2675
**Parent Epic**: #2656 (Runtime SQL Dialect Switching) - Phase 3: Test Migration
**Date**: 2025-11-26

## Executive Summary

This audit reviewed the test suite to identify tests with dialect-specific behavior that may need `skipif`/`onlyif` or `dialect` directives. The analysis focused on two test locations:

1. **Custom tests** (`tests/` directory): 28 test files
2. **Third-party SQLLogicTest corpus** (`third_party/sqllogictest/test/`): 624 test files

## Key Findings

### 1. Custom Tests (`tests/` Directory)

**Current State**: No dialect directives exist in any custom test files.

**Tests with Division Operations** (potential dialect-sensitive behavior):

| File | Line | Pattern | Expected Result | Dialect Sensitivity |
|------|------|---------|-----------------|---------------------|
| `tests/sqllogictest-files/functions/math.slt` | 20 | `SELECT 20 / 4` | 5 | SQLite-style integer division |
| `tests/debug/component_test.test` | 21 | `SELECT - 9 / - col2` | 0 | SQLite-style integer division |
| `tests/debug/component_test.test` | 49 | `SELECT - col1 + - 9 / - col2` | -1, -21, -81 | SQLite-style integer division |
| `tests/debug/arithmetic_debug.test` | 22 | `SELECT - 9 / - col2` | 0 | SQLite-style integer division |
| `tests/debug/arithmetic_debug.test` | 36 | `SELECT - col1 + - 9 / - col2` | -1, -21, -81 | SQLite-style integer division |
| `tests/debug/simple_debug.test` | 8 | `SELECT - col1 + - 9 / - col2` | -1 | SQLite-style integer division |
| `tests/debug/arithmetic_bug.test` | 14 | `SELECT - col1 + - 9 / - col2` | -1 | SQLite-style integer division |
| `tests/issue-1929/aggregate_failing_pattern.test` | 35 | `COUNT(*) / MIN(-CAST(48 AS REAL))` | -0.188 | Mixed (INT/REAL division) |

**Analysis**:
- The debug tests and `math.slt` expect **SQLite-style integer division** (e.g., `20 / 4 = 5`)
- In **MySQL mode**, integer division with `/` returns a DECIMAL/REAL result
- The `issue-1929` test already documents this behavior difference

### 2. Third-Party SQLLogicTest Corpus

**Current State**: The third-party tests already contain extensive `skipif`/`onlyif` directives.

**Directive Counts** (sample from evidence/ directory):

| File | MySQL-related | SQLite-related |
|------|--------------|----------------|
| `evidence/in1.test` | 8 skipif/onlyif mysql | 82 onlyif sqlite |
| `evidence/in2.test` | 8 skipif mysql | - |
| `evidence/slt_lang_createtrigger.test` | 22 skipif mysql | - |
| `evidence/slt_lang_aggfunc.test` | - | 1 skipif sqlite |
| `evidence/slt_lang_createview.test` | - | 8 skipif sqlite |

The `index/random/` tests have thousands of MySQL-specific skipif directives per file (e.g., `slt_good_0.test` has 4,280).

### 3. Function/Syntax Audit

**Standard SQL:1999 Functions** (work in both dialects):
- `UPPER()`, `LOWER()` - String case conversion
- `SUBSTRING(... FROM ... FOR ...)` - String extraction
- `CHAR_LENGTH()` - String length
- `||` - String concatenation
- `TRIM()` - Whitespace removal
- `ABS()`, `FLOOR()`, `CEILING()`, `MOD()` - Math functions
- Standard aggregate functions (COUNT, SUM, MIN, MAX, AVG)

**No MySQL-Specific Syntax Found in Custom Tests**:
- No `DIV` operator usage
- No backtick (`) identifier quoting
- No MySQL-specific functions (IFNULL, GROUP_CONCAT, etc.)

## Recommendations

### Immediate Actions (No Code Changes Needed)

1. **Auto-dialect switching is already enabled** in the test harness (`crates/vibesql-sqllogictest/src/harness.rs:43`), which handles the third-party tests appropriately.

2. **Custom tests are working correctly** because VibeSQL's current default mode handles SQLite-style integer division.

### Future Considerations

If dialect behavior needs to be made explicit in custom tests, add directives like:

```sql
# For SQLite-specific integer division behavior
onlyif sqlite
query I
SELECT 20 / 4
----
5
```

Or using the new `dialect` directive:

```sql
dialect sqlite
query I
SELECT 20 / 4
----
5
```

### Files That May Need Annotation (if dialect-explicit tests are desired)

| Priority | File | Reason |
|----------|------|--------|
| Low | `tests/sqllogictest-files/functions/math.slt` | Integer division test |
| Low | `tests/debug/component_test.test` | Integer division in expressions |
| Low | `tests/debug/arithmetic_debug.test` | Integer division in expressions |
| Low | `tests/debug/simple_debug.test` | Integer division in expressions |
| Low | `tests/debug/arithmetic_bug.test` | Integer division in expressions |
| None | `tests/issue-1929/aggregate_failing_pattern.test` | Already documents dialect behavior |

## Conclusion

The audit found that:

1. **No immediate action is required** - The test harness's auto-dialect switching handles dialect differences in the third-party corpus.

2. **Custom tests are minimal and dialect-neutral** - Most custom tests use standard SQL:1999 syntax that works in both MySQL and SQLite modes.

3. **Integer division is the primary dialect difference** - A small number of debug/test files assume SQLite-style integer division, but these work correctly with the current configuration.

4. **Third-party tests are well-annotated** - The SQLLogicTest corpus already contains comprehensive `skipif`/`onlyif` directives.

The current test infrastructure properly handles dialect switching, and no test files require immediate annotation changes.
