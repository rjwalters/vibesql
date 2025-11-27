# Sysbench OLTP Native Rust Benchmarks

## Overview

This benchmark suite measures **OLTP (Online Transaction Processing) latency** using industry-standard sysbench-compatible workloads. It provides apples-to-apples comparison across:

- **VibeSQL** (native Rust API - no PyO3 overhead)
- **SQLite** (via rusqlite - native C bindings)
- **DuckDB** (via duckdb-rs - native bindings)

All databases run **in-memory** with **no Python/FFI overhead**, ensuring pure SQL engine comparison.

## Why Sysbench Benchmarks?

### Complementing TPC-H

TPC-H measures **analytical (OLAP) workloads** with complex joins, aggregations, and subqueries. Sysbench measures **transactional (OLTP) workloads** with:

- Point queries (single row by primary key)
- Single-row inserts
- Updates to existing rows
- Mixed read/write workloads

| Benchmark | Workload Type | Focus |
|-----------|---------------|-------|
| TPC-H     | OLAP (Analytical) | Complex queries, joins, aggregations |
| Sysbench  | OLTP (Transactional) | Point lookups, inserts, updates |

### Industry Standard

Sysbench is the de facto standard for MySQL-compatible database benchmarking. Using sysbench-compatible tests allows direct comparison with:

- MySQL/MariaDB
- Dolt
- TiDB
- PlanetScale
- Other MySQL-compatible databases

## Quick Start

### Run All Sysbench Benchmarks

```bash
# Run all benchmarks (VibeSQL, SQLite, DuckDB)
cargo bench --bench sysbench_oltp --features benchmark-comparison

# Run VibeSQL only
cargo bench --bench sysbench_oltp

# Generate HTML reports
cargo bench --bench sysbench_oltp --features benchmark-comparison -- --save-baseline main
```

### Run Specific Tests

```bash
# Run only point select benchmarks
cargo bench --bench sysbench_oltp --features benchmark-comparison -- point_select

# Run only insert benchmarks
cargo bench --bench sysbench_oltp --features benchmark-comparison -- insert

# Run only read/write mixed workload
cargo bench --bench sysbench_oltp --features benchmark-comparison -- read_write

# Run only VibeSQL benchmarks
cargo bench --bench sysbench_oltp --features benchmark-comparison -- vibesql

# Run only SQLite benchmarks
cargo bench --bench sysbench_oltp --features benchmark-comparison -- sqlite
```

## Test Categories

### 1. Point Select (`oltp_point_select`)

**What it measures**: Single-row lookup by primary key

**Query**: `SELECT c FROM sbtest1 WHERE id = ?`

**Why it matters**: This is the most common OLTP operation. It tests:
- Primary key index lookup performance
- Row retrieval speed
- Parser/planner overhead for simple queries

### 2. Insert (`oltp_insert`)

**What it measures**: Single-row insert performance

**Query**: `INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)`

**Why it matters**: Tests write path performance including:
- Index maintenance (primary key + secondary index on `k`)
- Storage engine efficiency
- Transaction overhead

### 3. Read/Write Mixed (`oltp_read_write`)

**What it measures**: Mixed OLTP workload simulating typical application usage

**Workload per iteration**:
- 10 point select queries
- 1 update (non-indexed column)

**Why it matters**: Real applications rarely do pure reads or pure writes. This test measures:
- Read-after-write consistency
- Cache efficiency under mixed workloads
- Overall OLTP throughput

## Schema

The benchmark uses the standard sysbench OLTP schema:

```sql
CREATE TABLE sbtest1 (
    id INTEGER PRIMARY KEY,
    k INTEGER NOT NULL DEFAULT 0,
    c CHAR(120) NOT NULL DEFAULT '',
    pad CHAR(60) NOT NULL DEFAULT ''
);
CREATE INDEX k_1 ON sbtest1(k);
```

| Column | Type | Purpose |
|--------|------|---------|
| `id`   | INTEGER | Primary key (sequential) |
| `k`    | INTEGER | Secondary index column (random values) |
| `c`    | CHAR(120) | String data column (format: `###-###-...-###`) |
| `pad`  | CHAR(60) | Padding column (format: `###-###-...-###`) |

## Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| Table Size | 10,000 rows | Matches sysbench default |
| Measurement Time | 10 seconds | Per benchmark |
| Seed | 42 | Deterministic for reproducibility |

## Understanding Results

### Criterion Output

```
sysbench_point_select/vibesql/10000  time:   [45.123 us 45.456 us 45.789 us]
sysbench_point_select/sqlite/10000   time:   [12.234 us 12.456 us 12.678 us]
sysbench_point_select/duckdb/10000   time:   [25.123 us 25.234 us 25.345 us]
```

**Interpretation**:
- VibeSQL: 45.5 us per point select
- SQLite: 12.5 us per point select (3.6x faster)
- DuckDB: 25.2 us per point select (1.8x faster than VibeSQL)

### HTML Reports

Criterion generates detailed HTML reports:

```bash
open target/criterion/sysbench_point_select/report/index.html
```

Shows:
- Performance over time
- Violin plots of distribution
- Comparison to baseline
- Statistical confidence intervals

## Performance Targets

Based on typical OLTP performance expectations:

| Metric | Target | Acceptable | Needs Improvement |
|--------|--------|------------|-------------------|
| Point Select | < 100 us | < 500 us | > 500 us |
| Insert | < 200 us | < 1 ms | > 1 ms |
| Read/Write | < 2 ms | < 10 ms | > 10 ms |

**Note**: These are rough guidelines. Actual targets depend on use case requirements.

## Comparison to Dolt

Dolt publishes their [sysbench latency benchmarks](https://docs.dolthub.com/sql-reference/benchmarks/latency) comparing against MySQL. Key metrics they report:

| Test | Dolt | MySQL | Multiple |
|------|------|-------|----------|
| oltp_point_select | ~0.35 ms | ~0.18 ms | 1.9x |
| oltp_insert | ~2.0 ms | ~0.5 ms | 4.0x |
| oltp_read_write | ~15 ms | ~5 ms | 3.0x |

**Note**: Dolt is versioned/Git-compatible, so some overhead is expected.

## Adding New Tests

To add a new sysbench test:

1. **Add helper functions** in `sysbench_oltp.rs`:

```rust
fn vibesql_my_test(db: &VibeDB, ...) -> Result {
    // Implementation
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_my_test(conn: &SqliteConn, ...) -> Result {
    // Implementation
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_my_test(conn: &DuckDBConn, ...) -> Result {
    // Implementation
}
```

2. **Create benchmark functions**:

```rust
fn benchmark_my_test_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_my_test");
    group.measurement_time(Duration::from_secs(10));
    // ...
}
```

3. **Add to criterion_group!**:

```rust
criterion_group!(
    benches,
    // existing benchmarks...
    benchmark_my_test_vibesql,
    benchmark_my_test_sqlite,
    benchmark_my_test_duckdb
);
```

## Potential Future Tests

- `oltp_update_index` - Updates to indexed columns
- `oltp_delete_insert` - Delete + insert cycles
- `covering_index_scan` - Index-only queries
- `table_scan` - Full table scan performance
- `index_join` - Join on indexed columns

## References

- [sysbench GitHub](https://github.com/akopytov/sysbench)
- [sysbench OLTP Lua scripts](https://github.com/akopytov/sysbench/blob/master/src/lua/oltp_common.lua)
- [Dolt Latency Benchmarks](https://docs.dolthub.com/sql-reference/benchmarks/latency)
- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)

## Troubleshooting

### Compilation Errors

**Error**: `cannot find type rusqlite in scope`
```bash
cargo update && cargo clean && cargo check --benches --features benchmark-comparison
```

**Error**: `duckdb not found`
```bash
# DuckDB requires bundled feature - already configured in Cargo.toml
```

### Runtime Issues

**Results vary wildly between runs**:
```rust
// Increase measurement time in benchmark code
group.measurement_time(Duration::from_secs(30));
```

**Benchmarks take too long**:
```bash
# Run specific test only
cargo bench --bench sysbench_oltp -- point_select/vibesql
```
