# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added

#### Core SQL Engine
- **Complete SQL:1999 Core compliance** - All 169 mandatory Core features implemented
- **100% sqltest conformance** - 739/739 tests passing
- **100% SQLLogicTest conformance** - 623 files (~5.9M tests)
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
- **CREATE/DROP INDEX** including B-tree, R-tree, and FULLTEXT indexes
- **CREATE/DROP VIEW** with OR REPLACE and column lists
- **CREATE/DROP SCHEMA**
- **Constraint support** - PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL
- **Default values** and auto-increment

#### Security & Privileges
- **GRANT/REVOKE** privilege system
- **User and role management**
- **Table and column-level permissions**
- **WITH GRANT OPTION** support

#### Advanced SQL Features
- **Transaction support** - BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- **Foreign key constraints** with referential integrity
- **Check constraints** with complex expressions
- **Sequences** for auto-incrementing values
- **Domains** for custom type definitions
- **Stored procedures & functions** with IN/OUT/INOUT parameters
- **Triggers** (BEFORE/AFTER)
- **Full-text search** with MATCH AGAINST and FULLTEXT indexes
- **Spatial/geometric types** with ST_* functions and R-tree indexes

#### Server & APIs
- **PostgreSQL wire protocol** - Compatible with psql, JDBC, ODBC clients
- **HTTP REST API** - Full CRUD operations with JSON responses
- **GraphQL API** - Schema introspection and queries
- **Real-time subscriptions** - Convex-like reactivity with delta updates
- **Server-Sent Events (SSE)** for HTTP streaming
- **WebSocket support** for persistent connections
- **Authentication and session management**

#### Extended Features
- **Vector types** - VECTOR(n) for AI/ML embeddings
- **Vector similarity search** - Cosine, Euclidean, dot product distance functions
- **Vector indexes** - HNSW and IVFFlat for approximate nearest neighbor
- **File/blob storage** - STORAGE_URL, STORAGE_SIZE functions
- **Scheduled functions** - SCHEDULE AFTER/AT, CREATE CRON

#### Query Optimization
- **Columnar execution engine** - SIMD-accelerated analytical queries
- **Columnar cache** - Lazy conversion with automatic invalidation on mutations
- **Cost-based join reordering** for multi-way joins
- **Predicate pushdown** - Filters pushed to table scans
- **Subquery-to-join transformation** - Converts correlated subqueries to efficient joins
- **Hash joins** for equi-join conditions
- **Index-based query optimization**
- **Query plan caching** for repeated queries
- **Parallel execution** - Hardware-aware parallelism for scans, joins, aggregation

#### Functions & Operators
- **200+ built-in functions**
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

#### Bindings & Interfaces
- **Rust library** - Full programmatic API
- **Command-line interface (vibesql-cli)** - Interactive SQL shell with:
  - PostgreSQL-compatible meta-commands (\d, \dt, \l, etc.)
  - Multiple output formats (table, CSV, JSON, markdown, HTML)
  - Query history and auto-completion
  - Import/export functionality (\copy command)
  - Persistence (\save command for SQL dumps)
- **WebAssembly bindings** - Run in browser with live demo
- **Python bindings** - DB-API 2.0 compatible interface via PyO3
- **TypeScript SDK** - React hooks (useSubscription, useQuery) and Drizzle ORM adapter

#### Testing & Benchmarks
- **4,800+ unit tests** with comprehensive coverage
- **SQLLogicTest integration** - 623 files (~5.9M individual tests)
- **TPC-H benchmark** - All 22 queries passing
- **TPC-C benchmark** - All OLTP transactions passing
- **TPC-DS benchmark** - 97/99 queries passing
- **Sysbench** - OLTP workload testing

### Architecture

- **11 modular crates**:
  - `vibesql-types` - SQL:1999 type system
  - `vibesql-ast` - Abstract Syntax Tree definitions
  - `vibesql-parser` - Hand-written SQL parser
  - `vibesql-storage` - Storage engine with B-tree, R-tree, FULLTEXT indexes
  - `vibesql-catalog` - Schema and metadata management
  - `vibesql-executor` - Query execution with columnar engine
  - `vibesql-server` - Network server (PostgreSQL protocol, HTTP, GraphQL)
  - `vibesql-cli` - Command-line interface
  - `vibesql-wasm-bindings` - WebAssembly bindings
  - `vibesql-python-bindings` - Python interface
  - `vibesql-sqllogictest` - Conformance testing infrastructure

### Documentation
- **API documentation** for all public interfaces
- **CLI Guide** with meta-commands and output formats
- **Python Bindings Guide** with DB-API 2.0 reference
- **HTTP/GraphQL API documentation**
- **Live browser demo** at https://rjwalters.github.io/vibesql/

### Links

- **Repository**: https://github.com/rjwalters/vibesql
- **Documentation**: https://docs.rs/vibesql
- **Live Demo**: https://rjwalters.github.io/vibesql/
- **Crates.io**: https://crates.io/crates/vibesql
