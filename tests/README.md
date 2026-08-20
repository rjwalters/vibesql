# VibeSQL Test Organization

This directory contains integration tests for VibeSQL, organized by feature area for easy navigation and discoverability.

## Directory Structure

Tests are organized into feature-based subdirectories that mirror the crate structure:

### Core Execution Tests

- **`executor/`** - Query execution and operations
  - Basic CRUD operations (`basic_crud.rs`)
  - Complex queries and joins (`complex_queries.rs`, `northwind_joins.rs`)
  - Set operations (`set_operations.rs`)
  - Aggregates and window functions (`aggregate_patterns.rs`, `window_functions.rs`)
  - CTEs and subqueries (`cte.rs`, `subquery_without_from.rs`)
  - SELECT variations (`select_without_from.rs`, `count_star.rs`)
  - Predicates and WHERE clauses (`predicates.rs`, `complex_where_clauses.rs`)

### Function Tests

- **`functions/`** - Built-in SQL functions
  - String functions (`string_functions.rs`)
  - Spatial/geometric functions (`spatial_functions.rs`)
  - NULL handling (`coalesce.rs`, `nullif_basic.rs`, `is_null_expressions.rs`)
  - Boolean logic (`boolean_short_circuit.rs`, `null_boolean_logic.rs`)
  - Numeric formatting (`numeric_formatting.rs`)
  - Assertions (`assertions.rs`)

### DDL Tests

- **`ddl/`** - Data Definition Language
  - Table operations (`alter_table.rs`)
  - View creation (`create_view.rs`)
  - Schema objects (`create_schema_embedded.rs`, `schema_ddl.rs`)
  - Advanced objects (`create_domain.rs`, `create_type.rs`, `advanced_objects.rs`)

### Storage Tests

- **`storage/`** - Storage engine and indexes
  - Index operations (`update_hash_indexes.rs`)
  - Performance tests (`update_pk_performance.rs`, `insert_constraint_performance.rs`)
  - Transactions (`transaction_features.rs`)
  - Referential integrity (`referential_integrity.rs`, `cascade_operations.rs`)

### Security Tests

- **`security/`** - Access control and privileges
  - GRANT operations (`grant_tests.rs`)
  - REVOKE operations (`revoke_privileges.rs`, `revoke_nonexistent.rs`)

### Compliance Tests

- **`z_compliance/`** - SQL standard conformance
  - SQLLogicTest suite (`sqllogictest_suite.rs`, `sqllogictest_basic.rs`)
  - SQL:1999 conformance (`sqltest_conformance.rs`)
  - Benchmark tests (`sqllogictest_benchmark.rs`)
  - PostgreSQL regression harness (`pgsql_regress.rs`)

### Conformance Suites and Fixtures

- **`sqllogictest/`** - SQLLogicTest runner infrastructure (execution, scheduling, reporting, db adapters)
- **`sqllogictest-files/`** - `.slt`/`.test` files organized by feature (`dml/`, `functions/`, `identifiers/`, `order/`, `select/`)
- **`pgsql/`** - PostgreSQL-style regression tests adapted from PostgreSQL's `src/test/regress/sql/`
- **`sql1999/`** - SQL:1999 conformance tests from the sqltest suite
- **`dialect/`** - Dialect-difference tests (MySQL vs SQLite modes)
- **`vibesql/`** - VibeSQL-specific `.test` files
- **`benchmarks/`** - Benchmark-oriented `.slt` files

### Client Compatibility Tests

- **`jdbc/`** - JDBC driver compatibility tests (Java, PostgreSQL wire protocol)
- **`odbc/`** - ODBC driver compatibility tests

### Data Type Tests

- **`e2e_data_types/`** - End-to-end data type coverage (numeric, character, etc.)

### Error Handling Tests

- **`test_error_handling/`** - Error types and messages (catalog, executor, storage)

### Integration Tests

- **`integration/`** - End-to-end scenarios
  - Web demo tests (`web_demo_*.rs`)
  - Benchmark examples (`benchmark_example.rs`)
  - Resource limits (`resource_limits.rs`)
  - Metrics and monitoring (`metrics.rs`)

### Regression Tests

- **`regression/`** - Issue-specific test cases
  - Individual issue reproductions (`issue_*.rs`)
  - Ensures fixed bugs stay fixed

### Parser Tests

- **`parser/`** - SQL parsing and syntax
  - Error handling (`error_handling.rs`)
  - Comments and directives (`directive_comments.rs`)
  - Identifiers (`delimited_identifiers.rs`)

### Support and Miscellaneous

- **`common/`** - Shared fixtures and helpers used across test files
- **`select5_samples/`** - Minimal table extracts for select5 debugging; regenerate `select5_minimal.test` with `select5_samples/extract_minimal_tables.sh` (one-shot fixture generator, run from the repo root)

## Running Tests

### Run All Tests
```bash
cargo test
```

### Run Tests for Specific Feature
```bash
# Run all executor tests
cargo test --test "*" -- executor

# Run specific test file
cargo test --test basic_crud

# Run tests matching pattern
cargo test count_star
```

### Run Integration Tests Only
```bash
cargo test --tests
```

### Run with Output
```bash
cargo test -- --nocapture
```

## Adding New Tests

When adding new tests, place them in the appropriate feature directory:

1. **Identify the feature area** - executor, functions, storage, etc.
2. **Create/update test file** in that directory
3. **Use descriptive names** - `{feature}_{aspect}.rs`
4. **Keep tests focused** - one feature area per file

Example:
```bash
# Adding a new aggregate function test
touch tests/functions/aggregate_functions.rs

# Adding a new JOIN algorithm test
touch tests/executor/merge_join.rs
```

## Test Organization Benefits

**For AI Agents**:
- Quick feature discovery through directory structure
- Pattern-based navigation (`tests/{feature}/`)
- Clear test coverage visibility

**For Developers**:
- Easy to find tests for specific features
- Logical grouping reduces cognitive load
- Scales well as test count grows

## Legacy Structure

Prior to this organization, all 73 integration tests were in a flat `tests/` directory with inconsistent naming (`test_*`, `e2e_*`, `*_tests`). The new structure provides better discoverability and maintainability.
