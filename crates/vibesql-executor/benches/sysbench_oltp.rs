//! Sysbench OLTP Benchmark Suite - Native Rust Implementation
//!
//! This benchmark measures OLTP (Online Transaction Processing) latency performance
//! using industry-standard sysbench-compatible workloads. It compares:
//! - VibeSQL (native Rust API)
//! - SQLite (via rusqlite) - requires 'benchmark-comparison' feature
//! - DuckDB (via duckdb-rs) - requires 'benchmark-comparison' feature
//!
//! All measurements are done in-memory with no Python/FFI overhead.
//!
//! ## Test Categories
//!
//! **Read Tests:**
//! - `oltp_point_select` - Single row lookup by primary key
//!
//! **Write Tests:**
//! - `oltp_insert` - Single row inserts
//!
//! **Mixed Tests:**
//! - `oltp_read_write` - Mixed read/write workload (10 reads, 4 writes per transaction)
//!
//! ## Usage
//!
//! ```bash
//! # Run all sysbench benchmarks
//! cargo bench --bench sysbench_oltp --features benchmark-comparison
//!
//! # Run only point select benchmarks
//! cargo bench --bench sysbench_oltp --features benchmark-comparison -- point_select
//!
//! # Run only insert benchmarks
//! cargo bench --bench sysbench_oltp --features benchmark-comparison -- insert
//!
//! # Run only VibeSQL benchmarks
//! cargo bench --bench sysbench_oltp --features benchmark-comparison -- vibesql
//! ```
//!
//! ## Table Size
//!
//! Default: 10,000 rows (matches sysbench default)
//!
//! ## References
//!
//! - [Dolt Latency Benchmarks](https://docs.dolthub.com/sql-reference/benchmarks/latency)
//! - [sysbench GitHub](https://github.com/akopytov/sysbench)

mod sysbench;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::hint::black_box;
use std::time::Duration;
use sysbench::schema::load_vibesql;
use sysbench::SysbenchData;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database as VibeDB, Row};
use vibesql_types::SqlValue;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;
#[cfg(feature = "benchmark-comparison")]
use sysbench::schema::{load_duckdb, load_sqlite};

/// Default table size for sysbench tests
const TABLE_SIZE: usize = 10_000;

// =============================================================================
// Helper Functions - VibeSQL
// =============================================================================

/// Execute a point select query on VibeSQL
fn vibesql_point_select(db: &VibeDB, id: i64) -> usize {
    let sql = format!("SELECT c FROM sbtest1 WHERE id = {}", id);
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

/// Execute an insert on VibeSQL using direct API (avoids SQL parsing overhead)
fn vibesql_insert(db: &mut VibeDB, id: i64, k: i64, c: &str, pad: &str) {
    let row = Row::new(vec![
        SqlValue::Integer(id),
        SqlValue::Integer(k),
        SqlValue::Varchar(c.to_string()),
        SqlValue::Varchar(pad.to_string()),
    ]);
    db.insert_row("SBTEST1", row).unwrap();
}

/// Execute an update query on VibeSQL (update non-indexed column)
fn vibesql_update_non_index(db: &mut VibeDB, id: i64, c: &str) {
    let sql = format!("UPDATE sbtest1 SET c = '{}' WHERE id = {}", c, id);
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Update(update) = stmt {
        vibesql_executor::UpdateExecutor::execute(&update, db).unwrap();
    }
}

// =============================================================================
// Helper Functions - SQLite
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn sqlite_point_select(conn: &SqliteConn, id: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1")
        .unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_insert(conn: &SqliteConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn
        .prepare_cached("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)")
        .unwrap();
    stmt.execute(rusqlite::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_update_non_index(conn: &SqliteConn, id: i64, c: &str) {
    let mut stmt = conn
        .prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2")
        .unwrap();
    stmt.execute(rusqlite::params![c, id]).unwrap();
}

// =============================================================================
// Helper Functions - DuckDB
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn duckdb_point_select(conn: &DuckDBConn, id: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1")
        .unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_insert(conn: &DuckDBConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn
        .prepare_cached("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)")
        .unwrap();
    stmt.execute(duckdb::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_update_non_index(conn: &DuckDBConn, id: i64, c: &str) {
    let mut stmt = conn
        .prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2")
        .unwrap();
    stmt.execute(duckdb::params![c, id]).unwrap();
}

// =============================================================================
// Point Select Benchmarks
// =============================================================================

/// Benchmark oltp_point_select on VibeSQL
///
/// This test measures single-row lookup by primary key, which is the most
/// common OLTP operation. It tests index lookup performance.
fn benchmark_point_select_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_point_select");
    group.measurement_time(Duration::from_secs(10));

    let db = load_vibesql(TABLE_SIZE);
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            black_box(vibesql_point_select(&db, id))
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_point_select_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_point_select");
    group.measurement_time(Duration::from_secs(10));

    let conn = load_sqlite(TABLE_SIZE);
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            black_box(sqlite_point_select(&conn, id))
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_point_select_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_point_select");
    group.measurement_time(Duration::from_secs(10));

    let conn = load_duckdb(TABLE_SIZE);
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            black_box(duckdb_point_select(&conn, id))
        })
    });

    group.finish();
}

// =============================================================================
// Insert Benchmarks
// =============================================================================

/// Benchmark oltp_insert on VibeSQL
///
/// This test measures single-row insert performance. Each iteration inserts
/// a new row with a unique ID.
fn benchmark_insert_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_insert");
    group.measurement_time(Duration::from_secs(10));

    // We need a fresh database for each benchmark run to avoid duplicate key errors
    // So we use iter_custom to set up a new database for each measurement batch
    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let mut db = load_vibesql(TABLE_SIZE);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let k = data_gen.random_k();
                let c = generate_c_string();
                let pad = generate_pad_string();
                vibesql_insert(&mut db, next_id, k, &c, &pad);
                next_id += 1;
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_insert_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_insert");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_sqlite(TABLE_SIZE);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let k = data_gen.random_k();
                let c = generate_c_string();
                let pad = generate_pad_string();
                sqlite_insert(&conn, next_id, k, &c, &pad);
                next_id += 1;
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_insert_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_insert");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_duckdb(TABLE_SIZE);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let k = data_gen.random_k();
                let c = generate_c_string();
                let pad = generate_pad_string();
                duckdb_insert(&conn, next_id, k, &c, &pad);
                next_id += 1;
            }
            start.elapsed()
        })
    });

    group.finish();
}

// =============================================================================
// Read-Write Mixed Workload Benchmarks
// =============================================================================

/// Benchmark oltp_read_write on VibeSQL
///
/// This test simulates a mixed OLTP workload with:
/// - 10 point select queries
/// - 1 update (non-indexed column)
///
/// This ratio is based on typical OLTP workloads where reads dominate.
fn benchmark_read_write_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_read_write");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let mut db = load_vibesql(TABLE_SIZE);
            let mut rng = ChaCha8Rng::seed_from_u64(42);

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // 10 point selects
                for _ in 0..10 {
                    let id = rng.random_range(1..=TABLE_SIZE as i64);
                    black_box(vibesql_point_select(&db, id));
                }

                // 1 update (non-indexed column)
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                let c = generate_c_string();
                vibesql_update_non_index(&mut db, id, &c);
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_read_write_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_read_write");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_sqlite(TABLE_SIZE);
            let mut rng = ChaCha8Rng::seed_from_u64(42);

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // 10 point selects
                for _ in 0..10 {
                    let id = rng.random_range(1..=TABLE_SIZE as i64);
                    black_box(sqlite_point_select(&conn, id));
                }

                // 1 update (non-indexed column)
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                let c = generate_c_string();
                sqlite_update_non_index(&conn, id, &c);
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_read_write_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_read_write");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_duckdb(TABLE_SIZE);
            let mut rng = ChaCha8Rng::seed_from_u64(42);

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // 10 point selects
                for _ in 0..10 {
                    let id = rng.random_range(1..=TABLE_SIZE as i64);
                    black_box(duckdb_point_select(&conn, id));
                }

                // 1 update (non-indexed column)
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                let c = generate_c_string();
                duckdb_update_non_index(&conn, id, &c);
            }
            start.elapsed()
        })
    });

    group.finish();
}

// =============================================================================
// Helper Functions for Data Generation
// =============================================================================

/// Generate a 120-char 'c' column value
fn generate_c_string() -> String {
    let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
    let mut s = String::with_capacity(120);
    for i in 0..11 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 10 {
            s.push('-');
        }
    }
    s
}

/// Generate a 60-char 'pad' column value
fn generate_pad_string() -> String {
    let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
    let mut s = String::with_capacity(60);
    for i in 0..5 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 4 {
            s.push('-');
        }
    }
    while s.len() < 60 {
        s.push(' ');
    }
    s
}

// =============================================================================
// Criterion Benchmark Groups
// =============================================================================

#[cfg(not(feature = "benchmark-comparison"))]
criterion_group!(
    benches,
    benchmark_point_select_vibesql,
    benchmark_insert_vibesql,
    benchmark_read_write_vibesql
);

#[cfg(feature = "benchmark-comparison")]
criterion_group!(
    benches,
    benchmark_point_select_vibesql,
    benchmark_point_select_sqlite,
    benchmark_point_select_duckdb,
    benchmark_insert_vibesql,
    benchmark_insert_sqlite,
    benchmark_insert_duckdb,
    benchmark_read_write_vibesql,
    benchmark_read_write_sqlite,
    benchmark_read_write_duckdb
);

criterion_main!(benches);
