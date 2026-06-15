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

1. **Performance optimization** - TPC-C OLTP throughput improvements
2. **Bug fixes** - Address issues as discovered
3. **Documentation** - Keep guides current, improve API docs
4. **Code quality** - Technical debt, test coverage

## Future Ideas

These are potential enhancements, not committed work. They would only be pursued if profiling demonstrates specific bottlenecks.

### Query Compilation (JIT)

Compile SQL to native code for hot queries using LLVM or Cranelift. Could provide 5-10x speedup but requires significant architectural changes.

### Persistent Storage

Durable on-disk storage with WAL and recovery. Current in-memory storage works well for many use cases.

### Range-Sharded Replication

Multi-Raft-group replication where each key range owns its own consensus group (CockroachDB/TiKV model). VibeSQL v0.2.0 ships single-group whole-database replication (rqlite/dqlite model) via the `vibesql-consensus` crate; range sharding and distributed multi-shard transactions are deferred until single-group scale shows specific bottlenecks. See [ADR-0004](decisions/0004-consensus-library.md) for the topology decision.

### Materialized Views

Pre-computed aggregations with incremental refresh. Useful for repeated complex queries.

## Known Gaps

All major test suites are at 100% coverage. Current focus areas:

- TPC-C OLTP throughput optimization
- Query performance for complex analytical queries
- Memory efficiency for large-scale workloads

## Contributing

The project uses [Loom orchestration](https://github.com/loomhq/loom):

- Check issues labeled `loom:issue` for ready work
- PRs labeled `loom:review-requested` need review
- See `good-first-issue` for beginner-friendly tasks
