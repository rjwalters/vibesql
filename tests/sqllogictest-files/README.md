# SQLLogicTest Files

This directory contains `.slt` (SQLLogicTest) test files for comprehensive SQL correctness testing.

## Directory Structure

```
sqllogictest-files/
├── select/          # SELECT queries and WHERE clauses
│   ├── basic.slt    # Basic SELECT, INSERT, DROP
│   ├── where.slt    # WHERE predicates (>, <, BETWEEN, IN)
│   └── joins.slt    # INNER/LEFT JOIN operations
├── dml/             # Data Manipulation Language
│   └── insert.slt   # INSERT operations (single, multi-row)
├── functions/       # Built-in functions
│   ├── string.slt   # String functions (UPPER, LOWER, SUBSTRING, etc.)
│   └── math.slt     # Math functions (ABS, FLOOR, CEILING, MOD)
└── identifiers/     # Identifier handling
    └── case_sensitivity.slt  # SQL:1999 case sensitivity tests
```

## Running Tests

```bash
# Run all SQLLogicTest files
cargo test --test sqllogictest_suite

# Run with detailed output
cargo test --test sqllogictest_suite -- --nocapture
```

## Test File Format

SQLLogicTest files use a simple format:

```sql
# Comment

statement ok
CREATE TABLE test (a INTEGER)

statement error
INSERT INTO nonexistent VALUES (1)

query I
SELECT 42
----
42

query IT rowsort
SELECT * FROM test
----
1 hello
2 world
```

### Query Types
- `I` = Integer
- `T` = Text
- `R` = Real (floating point)

### Modifiers
- `rowsort` = Sort rows before comparison (order doesn't matter)
- `valuesort` = Sort all values before comparison

## Current Status

**7 test files** covering:
- ✅ Basic SELECT, INSERT, DROP operations
- ✅ WHERE clause predicates (comparison, BETWEEN, IN)
- ✅ JOIN operations (INNER, LEFT)
- ✅ Aggregate functions with GROUP BY
- ✅ String functions (UPPER, LOWER, SUBSTRING, TRIM, CHAR_LENGTH)
- ✅ Math functions (ABS, FLOOR, CEILING, MOD)
- ✅ SQL:1999 case sensitivity (unquoted identifiers are case-insensitive)

**Pass Rate**: 100% (7/7 tests passing)

## Adding New Tests

1. Create a new `.slt` file in the appropriate subdirectory
2. Follow the SQLLogicTest format
3. Run `cargo test --test sqllogictest_suite` to verify
4. Tests are automatically discovered and run

Example:
```sql
# tests/sqllogictest-files/select/subqueries.slt

statement ok
CREATE TABLE t1 (x INTEGER)

statement ok
INSERT INTO t1 VALUES (1), (2), (3)

query I
SELECT x FROM t1 WHERE x IN (SELECT x FROM t1 WHERE x > 1)
----
2
3

statement ok
DROP TABLE t1
```

## Known Issues

- **SQRT/POWER**: Return Numeric type which has conversion issues (commented out)
- **SUM with NULL**: Returns 0 instead of NULL when no rows match (documented in test)

## Future Expansion

Potential areas to add tests:
- Subqueries (scalar, table, correlated)
- CTEs (WITH clause)
- Set operations (UNION, INTERSECT, EXCEPT)
- Window functions
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Constraints (PRIMARY KEY, FOREIGN KEY, CHECK)
- More functions (date/time, type conversions)

## References

- **SQLLogicTest Documentation**: https://sqlite.org/sqllogictest/doc/trunk/about.wiki
- **Test Runner**: `tests/sqllogictest_suite.rs`
- **Rust Library**: https://github.com/risinglightdb/sqllogictest-rs
