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

### Distributed Execution

Multi-node query execution with partitioning and replication. Would require complete architectural redesign.

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
