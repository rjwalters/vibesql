# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added

#### Core SQL Engine
- **Complete SQL:1999 Core compliance** - All 169 mandatory Core features implemented
- **100% sqltest conformance** - 739/739 tests passing
- **In-memory storage engine** with full CRUD operations
- **Type system** supporting all SQL:1999 data types (INTEGER, VARCHAR, NUMERIC, DATE, TIMESTAMP, BOOLEAN, etc.)
- **NULL handling** with proper three-valued logic

#### Query Features
- **SELECT queries** with full expression support
- **Complex JOINs** - INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **Subqueries** - scalar, correlated, and in predicates
- **Common Table Expressions (CTEs)** with recursive support
- **Window functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, aggregates with OVER
- **Aggregate functions** - COUNT, SUM, AVG, MIN, MAX, with DISTINCT support
- **GROUP BY and HAVING** clauses
- **ORDER BY with multiple columns** and NULL ordering (NULLS FIRST/LAST)
- **LIMIT and OFFSET** for pagination
- **DISTINCT** queries
- **Set operations** - UNION, INTERSECT, EXCEPT (with ALL variants)

#### DML Operations
- **INSERT** - single row, multi-row, and INSERT...SELECT
- **UPDATE** - with WHERE clauses and subqueries
- **DELETE** - with WHERE clauses
- **TRUNCATE TABLE** optimization

#### DDL & Schema Management
- **CREATE TABLE** with comprehensive column options
- **ALTER TABLE** - ADD COLUMN, DROP COLUMN, RENAME COLUMN, etc.
- **DROP TABLE** with CASCADE/RESTRICT
- **CREATE/DROP INDEX**
- **CREATE/DROP VIEW**
- **CREATE/DROP SCHEMA**
- **Constraint support** - PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL
- **Default values** and auto-increment

#### Security & Privileges
- **GRANT/REVOKE** privilege system
- **User and role management**
- **Table and column-level permissions**
- **WITH GRANT OPTION** support

#### Advanced Features
- **Transaction support** - BEGIN, COMMIT, ROLLBACK
- **Foreign key constraints** with referential integrity
- **Check constraints** with complex expressions
- **Sequences** for auto-incrementing values
- **Domains** for custom type definitions
- **Assertions** for database-wide constraints
- **Triggers** (basic support)

#### Functions & Operators
- **Arithmetic operators** - +, -, *, /, %
- **Comparison operators** - =, <>, <, >, <=, >=
- **Logical operators** - AND, OR, NOT
- **String functions** - CONCAT, SUBSTRING, LENGTH, UPPER, LOWER, TRIM, POSITION
- **Numeric functions** - ABS, CEIL, FLOOR, ROUND, POWER, SQRT, trigonometric functions
- **Date/time functions** - CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, EXTRACT, date arithmetic
- **Conversion functions** - CAST, COALESCE, NULLIF
- **Conditional expressions** - CASE...WHEN...THEN...ELSE...END
- **Pattern matching** - LIKE, BETWEEN, IN
- **NULL handling** - IS NULL, IS NOT NULL, COALESCE, NULLIF

#### Query Optimization
- **Columnar execution engine** - SIMD-accelerated analytical queries
- **Columnar cache** - Lazy conversion with automatic invalidation on mutations
- **Join reordering** - Cost-based optimization for multi-way joins
- **Predicate pushdown** - Filters pushed to table scans
- **Subquery-to-join transformation** - Converts correlated subqueries to efficient joins
- **Hash joins** for equi-join conditions
- **Index-based query optimization**
- **Query plan caching** for repeated queries

#### Bindings & Interfaces
- **Rust library** - Full programmatic API
- **Command-line interface (vibesql-cli)** - Interactive SQL shell with:
  - PostgreSQL-compatible meta-commands (\d, \dt, \l, etc.)
  - Multiple output formats (table, CSV, JSON, markdown)
  - Query history and auto-completion
  - Import/export functionality
- **WebAssembly bindings** - Run in browser
- **Python bindings** - PyO3-based interface

#### Testing & Quality
- **2,000+ test cases** with 86% code coverage
- **SQLLogicTest integration** - Progressive conformance testing
- **Comprehensive unit tests** for all components
- **Integration tests** for end-to-end scenarios

#### Documentation
- **API documentation** for all public interfaces
- **Examples** demonstrating common use cases
- **Live browser demo** at https://rjwalters.github.io/vibesql/

### Architecture

- **10 modular crates**:
  - `vibesql-types` - Type system
  - `vibesql-ast` - Abstract Syntax Tree
  - `vibesql-parser` - SQL parser
  - `vibesql-storage` - Storage engine
  - `vibesql-catalog` - Schema management
  - `vibesql-executor` - Query execution
  - `vibesql` - Main library (re-exports)
  - `vibesql-cli` - Command-line interface
  - `vibesql-wasm-bindings` - WebAssembly
  - `vibesql-python-bindings` - Python interface

### Known Limitations

- **In-memory storage** - Primary storage is in-memory with SQL dump persistence
- **Single-threaded** - No concurrent transactions
- **No network protocol** - Library/CLI only (no server mode)

### Links

- **Repository**: https://github.com/rjwalters/vibesql
- **Documentation**: https://docs.rs/vibesql
- **Live Demo**: https://rjwalters.github.io/vibesql/
- **Crates.io**: https://crates.io/crates/vibesql
