# VibeSQL Roadmap

## Current Status

VibeSQL has achieved **100% SQL:1999 compliance** and **100% SQLLogicTest conformance**.

| Metric | Status |
|--------|--------|
| SQL:1999 Core (sqltest) | 739/739 (100%) |
| SQLLogicTest | 622/622 files (100%) |
| Unit tests | 7,000+ |
| TPC-H | 22/22 queries (100%) |
| TPC-C | All transactions |
| TPC-DS | 102/102 queries (100%) |
| SQLite TCL suite (canonical) | Core SQL passing; join suite (joinB/C/D/E) at 100%; the two largest Priority-1 clusters (indexA, wherelimit) now resolved, tail is small — run `make test-tcl-status` for the live number |

The curated suites above are at 100%. The canonical SQLite TCL suite (1,174 files) is the
ongoing conformance frontier — see [Current Focus](#current-focus) and [Known Gaps](#known-gaps).
Trustworthy TCL numbers require forwarding a generous per-file timeout (e.g.
`./scripts/tcltest run --priority 1 --timeout 1200`; the native default is 1200s after #5768),
otherwise slow files time out and are silently dropped. Read the canonical pass-rate / per-file
queries against the `tcl_test_results` table (see `CLAUDE.md`) or run `make test-tcl-status`.

See [HISTORY.md](HISTORY.md) for development timeline.

## Recently Completed

### Replication & Consensus (v0.2.0)
- Single-group Raft replication via `vibesql-consensus` crate (built on `openraft`) — see [ADR-0004](decisions/0004-consensus-library.md)
- Durable Raft log + vote persistence, network snapshot transfer + purge safety
- MVCC state machine applies committed transactions from the Raft log
- Linearizable leader reads + stale-leader fencing
- Bounded-staleness follower reads + read-your-writes tokens
- TCP transport + multi-node test cluster (`make test-cluster`)
- HTTP REST, GraphQL, CRUD, blob storage, prepared statements all routable through consensus
- SSE subscriptions fed from applied consensus entries

### Storage & Persistence (v0.2.0)
- Versioned `VBSQL` binary snapshot format (currently format v9, evolved across MVCC and other additions) with optional zstd compression
- CLI loads a database on open and auto-saves on exit (`auto_save = true`); JSON and SQL-dump load/save also supported
- Write-ahead log engine — writer/reader/checkpoint/scheduler/truncate plus crash recovery (checkpoint + WAL replay with corruption tolerance), exposed via `enable_persistence()` / `sync_persistence()` / `emit_wal_*`. **WAL is on by default** for file-backed CLI databases — an unclean shutdown recovers automatically via checkpoint + WAL replay (DDL and committed DML); opt out with `[database] wal = false`. Shipped across #5698 Phases 1-2 (#5706, #5760), with LSN resume across CLI restarts in #5770.
- OPFS backend for browser/WASM (Origin Private File System)
- Server-mode durability runs through the Raft log + MVCC state machine (see Replication & Consensus)

### MVCC (v0.2.0)
- Snapshot isolation via `xmin`/`xmax` row stamps (behind `mvcc_enabled` feature)
- Visibility filter threaded through all read sites (sequential scan, index scan, PK lookup, UNIQUE)
- `VACUUM` / `VACUUM INTO` syntax mapped to on-demand old-version GC
- SIMD/columnar fast paths preserved under MVCC

### Server & Real-Time Features
- PostgreSQL wire protocol (compatible with psql, JDBC, ODBC)
- HTTP REST API with full CRUD operations
- GraphQL API with schema introspection
- Real-time subscriptions with delta updates
- Server-Sent Events (SSE) for HTTP streaming

### Columnar Execution Engine
- SIMD-accelerated analytical queries
- Lazy columnar cache with automatic invalidation
- Cost-based join reordering for multi-way joins
- Subquery-to-join transformation

### Extended SQL Features
- Vector types (VECTOR(n)) for AI/ML embeddings
- Vector similarity search (cosine, euclidean, dot product)
- Vector indexes (HNSW, IVFFlat)
- File/blob storage with STORAGE_URL/STORAGE_SIZE functions
- Scheduled functions (SCHEDULE AFTER/AT, CREATE CRON)

### Parallelism
- Hardware-aware parallel execution
- Parallel table scans, hash join build, aggregation, sorting
- 4-8x speedup on analytical queries

## Current Focus

1. **SQLite TCL conformance** - Closing the canonical-suite tail. The join suite (joinB/C/D/E) passes 100%, and the two clusters that were previously the bulk of Priority-1 failures are now fixed: `indexA.test` (a TEXT-affinity comparison bug — `CREATE INDEX ... WHERE` partial indexes already worked, #5769) and `wherelimit.test` (a WAL durability regression where committed DML was lost on reopen, not a DML LIMIT/OFFSET gap, #5770). The remaining Priority-1 frontier is therefore small — e.g. view serialization in binary checkpoints (#5771). Trustworthy measurement requires forwarding a generous `--timeout` (native default 1200s after #5768) and reading the `tcl_test_results` detail table; run `make test-tcl-status` for the live number.
2. **Performance optimization** - TPC-C OLTP throughput improvements
3. **Bug fixes** - Address issues as discovered
4. **Documentation** - Keep guides current, improve API docs
5. **Code quality** - Technical debt, test coverage

## Future Ideas

These are potential enhancements, not committed work. They would only be pursued if profiling demonstrates specific bottlenecks.

### Query Compilation (JIT)

Compile SQL to native code for hot queries using LLVM or Cranelift. Could provide 5-10x speedup but requires significant architectural changes.

### Range-Sharded Replication

Multi-Raft-group replication where each key range owns its own consensus group (CockroachDB/TiKV model). VibeSQL v0.2.0 ships single-group whole-database replication (rqlite/dqlite model) via the `vibesql-consensus` crate; range sharding and distributed multi-shard transactions are deferred until single-group scale shows specific bottlenecks. See [ADR-0004](decisions/0004-consensus-library.md) for the topology decision.

### Materialized Views

Pre-computed aggregations with incremental refresh. Useful for repeated complex queries.

## Known Gaps

The curated suites (SQLLogicTest, SQL:1999, TPC-H/DS/C) are at 100%. Open areas:

- **SQLite TCL conformance** - canonical suite not yet fully green, but the frontier is now small: the join suite (joinB/C/D/E) passes 100% and the previously dominant indexA / wherelimit clusters are resolved (#5769, #5770). Run `make test-tcl-status` for the live number (forward a generous `--timeout`; native default is 1200s after #5768 so slow files are no longer silently dropped)
- **View serialization in checkpoints (#5771)** - views are not serialized in the binary checkpoint/`.vbsql` format, so they are lost across a WAL restart; this is the residual cause of the last 2 `wherelimit.test` failures
- TPC-C OLTP throughput optimization
- Query performance for complex analytical queries
- Memory efficiency for large-scale workloads

## Contributing

The project uses [Loom orchestration](https://github.com/rjwalters/loom):

- Check issues labeled `loom:issue` for ready work
- PRs labeled `loom:review-requested` need review
- See `good-first-issue` for beginner-friendly tasks
