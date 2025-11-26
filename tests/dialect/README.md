# Dialect-Specific Test Suite

This directory contains sqllogictest files that specifically test dialect differences between MySQL and SQLite modes in vibesql.

## Purpose

vibesql supports runtime SQL dialect switching via the `SET SQL_MODE` statement. These tests validate that:

1. Each dialect behaves correctly according to its specification
2. Switching between dialects at runtime works properly
3. Key behavioral differences are correctly implemented

## Using the Dialect Directive

Tests use the `dialect` directive to switch modes:

```sql
dialect mysql
# Tests run in MySQL mode

dialect sqlite
# Tests run in SQLite mode
```

This directive executes `SET SQL_MODE = '<dialect>'` internally.

## Test Files

- **division.test** - Integer division behavior (MySQL returns decimal, SQLite truncates)
- **string_handling.test** - String concatenation and functions
- **boolean_and_comparison.test** - Boolean literals and comparison operators
- **aggregates.test** - Aggregate function behavior and NULL handling

## Key Dialect Differences

### Division
- MySQL: `5 / 2 = 2.5` (returns REAL/DECIMAL)
- SQLite: `5 / 2 = 2` (returns INTEGER, truncates)

### String Concatenation
- MySQL: `CONCAT('a', 'b')` function
- SQLite: `'a' || 'b'` operator

### NULL in String Operations
- Both: NULL propagates in concatenation
- MySQL CONCAT_WS: Skips NULL values

### Boolean Values
- Both use 1/0 for TRUE/FALSE internally
- MySQL has explicit TRUE/FALSE keywords

## Running Tests

These tests are run via the sqllogictest harness:

```bash
./scripts/sqllogictest run tests/dialect/
```

Or run individual test files:

```bash
./scripts/sqllogictest run tests/dialect/division.test
```

## Adding New Tests

When adding dialect-specific tests:

1. Start with a clear comment explaining what dialect difference is being tested
2. Use `dialect mysql` and `dialect sqlite` directives to switch modes
3. Document expected behavior in comments
4. Include tests that verify switching back and forth works correctly

## Related Issues

- #2656 - Runtime SQL Dialect Switching (Epic)
- #2666 - Dialect directive implementation
- #2671 - SET SQL_MODE statement implementation
