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

### June 15, 2026: TPC-C Dashboard Contention Caveat

- v0.2.0 release-window TPC-C dashboard captured all three engines (VibeSQL, SQLite, DuckDB) at ~1/3 of their v0.1.4 throughput, the signature of host-level contention rather than a code regression (see [docs/performance/tpcc_regression.md](performance/tpcc_regression.md))
- Same-host re-measurement on `feature/issue-5643` (head `9e1ac205`, descended from the v0.2.0 release commit `ec54522d1`) recovered VibeSQL TPC-C to **9,276 TPS** (vs the 5,307 TPS dashboard number, vs the 10,758 TPS v0.1.4 README baseline); SQLite recovered to 2,813 TPS and DuckDB to 450 TPS
- Re-measurement was itself taken under sweep-concurrency contention; a truly idle re-run is still warranted before refreshing the website dashboard or filing a bisect

### June 2026: WAL-default durability + SQLite TCL frontier correction

- **WAL became the default CLI durability path** — file-backed databases now recover automatically from an unclean shutdown via checkpoint + WAL replay (DDL and committed DML), opt out with `[database] wal = false`. Shipped across #5698 Phases 1-2 (#5706, #5760), with LSN resume across CLI restarts so committed DML survives reopen (#5770)
- **The SQLite TCL Priority-1 frontier collapsed** after analysis confirmed the join suite (joinB/C/D/E) passes 100% (1,974 tests, 0 failures), retiring the earlier "join/index/where optimizer-plan parity" framing
  - `indexA.test` was a TEXT-affinity comparison bug (numeric literals vs TEXT columns), not a missing partial-index feature — fixed in #5769
  - `wherelimit.test` was a WAL durability regression (committed DML lost on reopen), not a DML LIMIT/OFFSET gap — fixed in #5770; the last 2 failures trace to views not yet serialized in the checkpoint format (#5771, open)
- **`tcltest` now forwards `--timeout`** (native per-file default raised to 1200s) so slow files are no longer silently dropped, making conformance numbers trustworthy (#5768)
