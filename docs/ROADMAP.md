# VibeSQL Roadmap

## Current Status

VibeSQL has achieved **100% SQL:1999 compliance** and **100% SQLLogicTest conformance**.

| Metric | Status |
|--------|--------|
| SQL:1999 Core (sqltest) | 739/739 (100%) |
| SQLLogicTest | 623/623 files (100%) |
| Unit tests | 2,991+ |
| Code coverage | 86% |

See [HISTORY.md](HISTORY.md) for development timeline.

## Current Focus

The project is in **maintenance mode**:

1. **Bug fixes** - Address issues as discovered
2. **Documentation** - Keep guides current, improve API docs
3. **Code quality** - Technical debt, test coverage
4. **Community** - Issue triage, contributions

## Future Ideas

These are potential enhancements, not committed work. They would only be pursued if profiling demonstrates specific bottlenecks.

### Query Compilation (JIT)

Compile SQL to native code for hot queries using LLVM or Cranelift. Could provide 5-10x speedup but requires significant architectural changes.

### Columnar Storage Engine

Alternative storage format for scan-heavy analytical queries. Column-oriented layout with SIMD execution and compression. Major undertaking - current row storage works well for general workloads.

### Distributed Execution

Multi-node query execution with partitioning and replication. Would require complete architectural redesign.

### Materialized Views

Pre-computed aggregations with incremental refresh. Useful for repeated complex queries.

## Deferred Work

### Predicate Pushdown (Phase 2-3)

Infrastructure is complete. Scanner and join-level integration deferred - not blocking functionality.

### Parallelism (Phase 2+)

Phase 1 delivers 4-8x speedup. Concurrent query execution and morsel-driven execution deferred - no immediate need.

## Contributing

The project uses [Loom orchestration](https://github.com/loomhq/loom):

- Check issues labeled `loom:issue` for ready work
- PRs labeled `loom:review-requested` need review
- See `good-first-issue` for beginner-friendly tasks
