# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2025-12-08

This release focuses on **Phase 9 performance optimization** and **cross-connection subscription enhancements**. With over 500 merged PRs since v0.1.2, highlights include O(1) row deletion, skip-scan optimization, Bloom filter joins, selective column updates for subscriptions, and comprehensive observability metrics.

### Performance

#### Phase 9b Core Optimizations

- **Deletion bitmap** - O(1) row deletion via bitmap-tracked slots (#3789)
- **SimpleFastPath column caching** - Cache column names for prepared statements (#3788)
- **Streaming range scan** - Iterator-based range scans avoiding full materialization (#3793)
- **Single-row PK DELETE fast path** - Optimized deletion for single-row primary key lookups (#3801)
- **Skip unaffected indexes** - Skip updating user-defined indexes when UPDATE doesn't touch indexed columns (#3800)
- **Covering index scan** - Return results directly from indexes without table lookup (#3804)

#### Query Execution

- **Skip-scan optimization** - Non-prefix index usage for range queries (#4081, #4088, #4089)
- **Bloom filter joins** - Build-side Bloom filters for hash join optimization (#4079)
- **ONEPASS UPDATE** - Single-pass optimization for single-row UPDATE operations (#4080)
- **Streaming range SELECT** - Pre-allocation for streaming range queries (#4073)
- **Lazy FromResult materialization** - Deferred row cloning in query results (#4068)
- **DISTINCT containment check** - Avoid cloning when checking DISTINCT containment (#4070)
- **Streaming aggregation** - Fast path for SUM range queries (#3818)
- **Early projection** - Project columns early in PK range scans (#3814)
- **Range scan row cloning** - Reduce cloning overhead in range scan fast path (#3848)
- **Column-to-column predicates** - Support column comparisons in columnar filtering (#4048)
- **Columnar deduplication** - DISTINCT queries in columnar join path (#3787)
- **LIMIT/OFFSET in columnar** - Support LIMIT/OFFSET in columnar join path (#3782)
- **Column resolution caching** - 34% faster Sysbench point lookups (#3593)

#### Optimizer Improvements

- **Semi-join filter pushdown** - Early semi-join filter pushdown for aggregate IN subqueries (#3685)
- **Composite key selectivity** - Fix composite key selectivity estimation (#3680)
- **IN subquery conversion** - Convert IN subqueries with GROUP BY/HAVING to semi-joins (#3666)
- **Case-insensitive column lookup** - Optimize column_index_cache for case-insensitive lookups (#3726)

#### Storage Engine

- **SmallVec for Row.values** - Reduce heap allocations for rows with ≤8 columns (#3954)
- **String interning** - Intern low-cardinality string columns (#3973)
- **Arc\<str\> for strings** - Use Arc\<str\> for Varchar/Character values (#3904)
- **Batch index updates** - Batch optimize B+tree and spatial index operations (#3877, #3882, #3888, #3896, #3916)
- **DELETE hot path** - Direct delete_by_pk_fast avoiding double-cloning (#3860)
- **Lazy row ID adjustment** - O(1) single-row deletes (#3725)
- **Index point lookup** - Reduce allocation overhead in index point lookups (#3701)
- **Skip trigger overhead** - Skip trigger overhead when no triggers defined (#3704)
- **LIMIT-aware range scan** - Early termination for LIMIT queries (#3699)
- **Batch DELETE** - Batch index updates for DELETE operations (#3693)
- **Incremental index adjustment** - Incremental adjustment for DELETE operations (#3416)

#### Index Optimizations

- **O(n²) index creation fix** - Fix O(n²) index creation scalability issue (#3679)
- **Hash join row combination** - Optimize row combination in hash joins (#3677)
- **Plan caching** - SimpleFastPath plan caching for prepared statements (#3676)

#### Data Loading

- **TPC data loading** - Optimize TPC data loading for large scale factors (#3640)
- **Fix O(n²) batch insert** - Fix O(n²) data loading in batch insert (#3671)

### Added

#### Cross-Connection Subscriptions

- **Real-time notifications** - Notify subscribers across connections when data changes (#3825)
- **Delta updates** - Send only changed rows for cross-connection notifications (#3834)
- **Async select optimization** - Use async select for efficient notification dispatch (#3832)
- **Selective column updates** - Send only changed columns in subscription updates (#3843, #3854)
- **Partial event type** - New SSE event type for selective column updates (#3952)
- **PartialRowUpdate messages** - Wire protocol support for selective updates (#3930)
- **Per-subscription config** - Configure selective update thresholds per subscription (#3996)
- **PK-based delta computation** - Use primary key columns for efficient delta detection (#3895)
- **Subscription filtering** - Add filtering expressions to subscriptions (#3846)
- **Protocol extensions** - Add Ack, Pause, Resume subscription protocol messages (#3837)
- **PK detection** - Detect primary key columns for selective subscription updates (#3861)

#### Observability & Metrics

- **Subscription metrics** - Active subscriptions gauge metric (#4015)
- **Bytes saved counter** - Track bytes saved by selective updates (#4017)
- **Eligibility breakdown** - Metrics for selective update eligibility reasons (#4009)
- **Partial update efficiency** - Metrics for partial vs full update ratio (#3951, #3893)
- **Selective-eligible gauge** - Track subscriptions eligible for selective updates (#3922)
- **HTTP efficiency endpoint** - `/stats/subscriptions/efficiency` endpoint (#3986)
- **Observability documentation** - Comprehensive metrics documentation (#4003)

#### Server Configuration

- **Environment variable overrides** - Configure server via environment variables (#4020)
- **API key env vars** - Set API keys via `VIBESQL_API_KEY` (#4027)
- **HTTP auth env vars** - Configure HTTP auth via environment (#4024)
- **Selective update thresholds** - Make column update thresholds configurable (#3969)

#### Developer Experience

- **VIBESQL_DEBUG flag** - Umbrella flag for enabling all debug output (#4056)
- **JSON debug output** - Structured JSON format for debug output (#4057)
- **Profiling decision tree** - Documentation for choosing profiling tools (#4058)
- **Skip-scan in EXPLAIN** - Show skip-scan plans in EXPLAIN output (#4087)
- **CPU profiling guide** - Guide for using samply profiler
- **DELETE hot path profiling** - Profiling instrumentation for DELETE operations (#3873)
- **Range scan profiling** - Add range scan profiling instrumentation (#3830)

#### SQL Compatibility

- **CREATE TABLE IF NOT EXISTS** - Standard SQL syntax support (#3820)
- **PostgreSQL type conversion** - Expanded type conversion coverage (#3897)
- **Window functions in CASE** - Support window functions in CASE and IS NULL expressions (#3813)

#### Internationalization

- **19 language translations** - Complete translations for web demo:
  - Arabic (ar) (#3760)
  - Dutch (nl) (#3759)
  - Hindi (hi) (#3757)
  - Indonesian (id) (#3769)
  - Italian (it) (#3756)
  - Polish (pl) (#3758)
  - Russian (ru) (#3755)
  - Swedish (sv) (#3772)
  - Thai (th) (#3771)
  - Turkish (tr) (#3762)
  - Ukrainian (uk) (#3768)
  - Vietnamese (vi) (#3770)
- **Locale switching** - Runtime locale switching for web demo (#3819)
- **Conformance page i18n** - Locale support for conformance page

#### TypeScript Client

- **Subscription events** - Emit subscription events from Connection class (#3881)
- **PartialData parser** - Parse SubscriptionPartialData (0xF7) messages (#3870, #3886)
- **Type narrowing fix** - Resolve TypeScript type narrowing in Connection.query() (#3878)

### Changed

- **DML cost estimation** - Use statistics-based cost estimates for DML optimization (#3968, #3974)
- **Row size WAL cost** - Consider row size in WAL write cost estimation (#3976)
- **Statistics fallback** - Prefer actual avg_row_bytes over schema estimates (#4018)
- **Switched to samply** - Replace flamegraph with samply (no sudo required)
- **Separate benchmark features** - Separate benchmark comparison features to avoid DuckDB overhead (#3594)

### Fixed

#### Benchmark Integrity

- **Remove SQL bypasses** - Remove fast-path SQL bypasses from TPC-C benchmark (#4046)
- **Sysbench SQL** - Replace direct API bypass with SQL in sysbench update_index (#4047)
- **DELETE timing** - Fix sysbench DELETE benchmark to measure actual DELETE time (#4037)
- **TPC-DS deduplication** - Deduplicate TPC-DS benchmark results by averaging iterations (#3817)

#### Query Correctness

- **Columnar cache invalidation** - Add columnar cache invalidation for ALTER TABLE, INSERT, TRUNCATE (#3931, #3941, #3946, #3989)
- **Missing columnar invalidation** - Fix missing columnar cache invalidation in REPLACE path (#3890)
- **Ambiguous column resolution** - Resolve ambiguous columns to leftmost table in LEFT JOINs (#3783)
- **Constant folding** - Add constant folding before predicate extraction in columnar join (#3775)
- **Semi-join equijoin extraction** - Extract equijoin predicates for comma-separated tables in semi-join (#3744)
- **Multi-column LEFT JOIN** - Add multi-column LEFT OUTER JOIN support (#3723)
- **Multi-column hash join** - Add multi-column hash join for WHERE clause conditions (#3670)
- **Table name normalization** - Normalize table names to lowercase for case-insensitive lookups (#3661)
- **Predicate pushdown validation** - Validate predicate pushdown in covering index scan (#3811)
- **Window functions in aggregates** - Apply window functions to aggregate queries for AVG(SUM(...)) patterns (#3706)

#### Storage & Indexes

- **Deleted row filtering** - SELECT queries filter deleted rows from deletion bitmap (#3791)
- **Index rebuild after compaction** - Rebuild user-defined indexes after table compaction (#3808)
- **Index rebuild after load** - Rebuild indexes after loading data from binary format (#3607)

#### Server

- **Connection closure** - Fix connection closure after ~150-190 queries (#3669)
- **Cross-connection reliability** - Improve cross-connection subscription notification reliability (#3867)
- **Axum compatibility** - Update axum route parameter syntax for v0.7 compatibility (#3853)
- **HTTP SSE port overflow** - Fix HTTP SSE tests port overflow on high TCP ports (#3836)

#### TPC Benchmark Alignment

- **TPC-DS Q69** - Implement official TPC-DS Q69 query per specification (#3729)
- **TPC-DS Q69 cartesian** - Remove cartesian product bug from Q69 query (#3717)
- **DuckDB TPC-H compat** - Fix DuckDB compatibility for TPC-H Q7-Q9 (#3686)
- **TPC-DS data generation** - Align TPC-DS data generation with DuckDB for consistent validation (#3652)
- **Warehouse loader RNG** - Align warehouse loader RNG parameters with DuckDB (#3796)
- **TPC-DS RNG patterns** - Align RNG patterns in VibeSQL loaders with DuckDB (#3792)

#### CLI

- **Table output** - Show actual column names and values in table output (#3812)

### Documentation

- **TPC-H Q4 analysis** - Root cause analysis for Q4 performance gap (#4043)
- **Anti-gaming warnings** - Add benchmark integrity warnings (#4044, #4045, #4049)
- **Selective update docs** - Document configuration options (#4007)
- **Metrics documentation** - Document all observability metrics (#4003, #4022, #4025)
- **Cloudflare CDN setup** - Add Cloudflare CDN setup guide for Brotli compression

---

## [0.1.2] - 2024-12-04

### Added

#### Vector Search
- **Distance operators** - `<->` (L2/Euclidean), `<#>` (inner product), `<=>` (cosine)
- **Distance functions** - `l2_distance()`, `inner_product()`, `cosine_distance()`, `cosine_similarity()`
- **IVFFlat index** - Inverted file index for approximate nearest neighbor search
- **HNSW index** - Hierarchical Navigable Small World graph for high-performance ANN

#### HTTP Server & API
- **REST API** - Auto-generated CRUD endpoints for all tables
- **GraphQL endpoint** - Full query support with schema introspection
- **Relationship resolution** - Nested queries following foreign key relationships
- **Authentication** - API key and token-based HTTP authentication
- **SSE subscriptions** - Real-time data streaming with Server-Sent Events
- **Delta updates** - Efficient change notifications for subscriptions
- **Backpressure handling** - Configurable channel buffers and rate limiting
- **Subscription limits** - Per-connection and global quotas
- **Retry with exponential backoff** - Automatic recovery from transient errors
- **Pagination** - `limit` and `offset` query parameters

#### Blob Storage
- **SQL integration** - `vibesql_storage` system table for blob metadata
- **HTTP endpoints** - Upload and download blobs via REST API
- **OpenDAL integration** - Cloud storage backends (S3, GCS, Azure, local filesystem)
- **TypeScript SDK** - Storage methods in `@vibesql/client`

#### SDK & Tooling
- **Drizzle ORM adapter** - `@vibesql/drizzle` package using sqlite-proxy driver
- **TypeScript codegen** - `vibesql-cli codegen` command for type-safe database access

#### Scheduled Functions
- **Cron scheduling** - Execute SQL statements on a schedule
- **Job management** - Create, list, pause, resume scheduled jobs

### Changed

- Unified benchmark CLI (`scripts/bench`) with consistent interface
- Consolidated benchmark result processing into `process_results.py`
- Improved SQLite dialect compatibility for dogfooding scenarios

### Fixed

- Clippy warning for approximate PI constant in tests
- WASM UUID generation with `js` feature for RNG support

---

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
