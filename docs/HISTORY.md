# VibeSQL Development History

A timeline of major milestones in the development of VibeSQL.

## Timeline

### October 25 - November 1, 2025: SQL:1999 Core Compliance

- Achieved 100% SQL:1999 Core compliance (739/739 sqltest tests)
- Implemented all 169 mandatory Core features
- Complete type system, query engine, DDL, transactions, and security model
- Strategic pivot to direct API testing saved months of ODBC/JDBC development

### November 1-12, 2025: Extended SQL:1999 Features

- Views with OR REPLACE and column lists
- Stored procedures & functions with parameter modes
- Spatial/geometric functions with R-tree indexes
- Full-text search with FULLTEXT indexes
- Triggers and information schema views

### November 12-18, 2025: SQLLogicTest Conformance

- Achieved 100% pass rate on SQLite's test corpus (623 files, ~5.9M tests)
- Started at 13.5% (83/613 files), reached 100% through systematic bug fixes
- Key fixes: BETWEEN NULL handling, aggregate NULL handling, CSE cache isolation

### November 2025: Query Performance Infrastructure

- Expression evaluation caching with LRU cache
- Short-circuit predicate evaluation
- Index scan optimization for BETWEEN ranges
- Memory allocation pooling with thread-local buffers
- Query result caching with automatic invalidation

### November 2025: Parallelism

- Automatic hardware-aware parallel execution (8+ core systems)
- Parallel table scans, hash join build, aggregation, and sorting
- 4-8x speedup on analytical queries

### November 18-19, 2025: Web Demo Optimization

- Fixed OPFS deadlock causing 30-second timeouts
- Reduced WASM bundle from 2.8 MB to 1.68 MB (40% reduction)
- Monaco Editor lazy loading for faster initial load

### November 2025: Columnar Execution Engine

- SIMD-accelerated analytical query execution
- Lazy columnar cache with automatic invalidation on mutations
- Cost-based join reordering for multi-way joins
- Subquery-to-join transformation for correlated subqueries

### December 2025: TPC-DS Full Coverage & Server Enhancements

- Achieved 100% TPC-DS coverage (102/102 queries)
- Fixed GROUPING() function for ROLLUP/CUBE queries
- Resolved complex CTE and column resolution issues
- PostgreSQL wire protocol server with environment variable configuration
- Real-time subscription efficiency metrics and selective updates
- Observability improvements with comprehensive metrics documentation
- TPC-C server benchmark implementation

### December 2025: Developer Experience

- CPU profiling decision tree documentation (samply integration)
- Debug instrumentation consolidation with VIBESQL_DEBUG umbrella flag
- Structured JSON debug output for performance analysis
- Cloudflare CDN deployment documentation for web demo

### January - June 2026: Replication, MVCC, and SQLite Compatibility (v0.2.0)

- **Raft replication track** — new `vibesql-consensus` crate built on `openraft`, single-group whole-DB replication (ADR-0004)
  - Durable Raft log + vote persistence, network snapshot transfer + purge safety
  - MVCC state machine applies committed transactions from the Raft log
  - Linearizable leader reads + stale-leader fencing
  - Bounded-staleness follower reads + read-your-writes tokens
  - TCP transport + multi-node test cluster (`make test-cluster`)
  - HTTP REST, GraphQL, CRUD, blob storage, and prepared statements all routable through consensus
- **MVCC end-to-end** — `xmin`/`xmax` row stamps, visibility filter threaded through all read sites
  - `VACUUM` / `VACUUM INTO` syntax mapped to on-demand old-version GC
  - SIMD/columnar fast paths preserved under `mvcc_enabled`
- **SQLite compatibility push** — full SQLITE_MAX_TRIGGER_DEPTH (1000) trigger semantics, ALTER TABLE RENAME COLUMN with trigger-body rewrite, schema-aware index manager, FK deferral, window-function correctness
- **EXPLAIN QUERY PLAN** moved close to sqlite3 parity (view/subquery expansion, window-sort annotations, ordering-index scan lines)
- Released as **v0.2.0** on 2026-06-15 (369 commits since v0.1.4)
