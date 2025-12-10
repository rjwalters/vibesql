# PostgreSQL Regression Tests

This directory contains PostgreSQL-style regression tests for VibeSQL, adapted from PostgreSQL's `src/test/regress/sql/` directory.

## Overview

The PostgreSQL regression test suite provides comprehensive SQL conformance testing by porting tests from PostgreSQL's regression test framework and adapting them for VibeSQL's SQLite-compatible syntax.

## Structure

```
tests/pgsql/
├── mod.rs          # Module declarations
├── runner.rs       # Test runner and parser
├── stats.rs        # Statistics tracking
├── sql/            # SQL test files
│   └── triggers.sql
└── README.md       # This file
```

## Running Tests

```bash
# Run all PostgreSQL regression tests
cargo test -p vibesql --test pgsql_regress -- --nocapture

# Run specific test functions
cargo test -p vibesql --test pgsql_regress test_pgsql_triggers -- --nocapture
```

## Test File Format

Test files use a simple directive-based format:

```sql
-- TEST: Description of what this test does
-- EXPECT_OK (optional - indicates success expected)
-- EXPECT_ERROR: pattern (optional - indicates error containing "pattern" expected)
-- EXPECT: value1|value2|value3 (optional - expected result row)
-- EXPECT_COUNT: N (optional - expected row count)
-- SKIP: reason (optional - skip this test)
CREATE TABLE example (id INTEGER PRIMARY KEY);

-- TEST: Insert a row
INSERT INTO example VALUES (1);
```

### Directives

| Directive | Description |
|-----------|-------------|
| `-- TEST:` | Starts a new test case with description |
| `-- EXPECT_OK` | Expect the statement to succeed |
| `-- EXPECT_ERROR: pattern` | Expect an error containing "pattern" |
| `-- EXPECT: val1\|val2` | Expect a specific result row |
| `-- EXPECT_COUNT: N` | Expect N rows in the result |
| `-- SKIP: reason` | Skip this test (not counted as failure) |

## Adding New Tests

1. Create a new `.sql` file in `tests/pgsql/sql/` named after the PostgreSQL test category (e.g., `views.sql`, `constraints.sql`)

2. Start with a section header comment:
   ```sql
   -- ============================================================================
   -- PostgreSQL-inspired [Category] Regression Tests for VibeSQL
   -- ============================================================================
   ```

3. Document any known limitations at the top of the file

4. Organize tests into logical sections using `SECTION` comments

5. Use appropriate directives to specify expected outcomes

## Current Test Coverage

| Category | Tests | Passed | Skipped | Pass Rate |
|----------|-------|--------|---------|-----------|
| Triggers | 63 | 59 | 4 | 100% |

## Known Limitations

### Trigger Body Parsing

VibeSQL's trigger parser currently stores trigger bodies in a debug token format (`{:?}`) rather than preserving the original SQL text. This causes triggers with non-empty bodies to fail execution when re-parsed from SQL.

**Workarounds:**
- Use empty `BEGIN END` blocks for syntax testing
- Create triggers programmatically for execution testing
- Tests requiring trigger execution are marked with `SKIP`

### INSTEAD OF Triggers

`INSTEAD OF` triggers on views require views to be fully registered in the catalog, which has limitations in the current VibeSQL version. These tests are skipped.

## CI Integration

The PostgreSQL regression tests run as part of the CI pipeline:
- Dedicated `pgsql-regress` job in `.github/workflows/ci.yml`
- Results appear in GitHub Actions step summary
- Also included in website conformance dashboard

## Results Export

Test results are exported to `~/.vibesql/test_results/pgsql_regress_results.json` and included in the website conformance report via `scripts/export_website_data.py`.

## Contributing

When porting tests from PostgreSQL:

1. Check PostgreSQL's license compatibility (PostgreSQL License - BSD-style)
2. Adapt syntax for SQLite/VibeSQL compatibility
3. Document any limitations or deviations
4. Use `SKIP` for features not yet supported
5. Test locally before committing
