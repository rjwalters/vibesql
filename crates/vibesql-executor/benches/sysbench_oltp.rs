//! Sysbench OLTP Benchmark Suite - Native Rust Implementation
//!
//! This benchmark suite implements standard sysbench OLTP read-only workloads:
//! - `oltp_read_only`: Full read-only transaction (10 point selects + 4 range queries)
//! - `select_random_points`: Multiple random point selects (index lookup throughput)
//! - `select_random_ranges`: Range queries with BETWEEN (range scan performance)
//!
//! All measurements are done in-memory with no Python/FFI overhead.
//!
//! Usage:
//!   cargo bench --bench sysbench_oltp
//!   cargo bench --bench sysbench_oltp --features benchmark-comparison
//!   cargo bench --bench sysbench_oltp -- oltp_read_only  # Run specific benchmark

mod sysbench;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use std::time::Duration;
use sysbench::data::SysbenchData;
use sysbench::schema::*;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;

// =============================================================================
// Constants
// =============================================================================

/// Default table size for benchmarks (matches sysbench default)
const TABLE_SIZE: usize = 10000;

/// Range size for range queries (sysbench default is 100)
const RANGE_SIZE: usize = 100;

/// Number of point selects in oltp_read_only transaction
const POINT_SELECTS_PER_TXN: usize = 10;

/// Number of random IDs for select_random_points benchmark
const RANDOM_POINTS_COUNT: usize = 10;

// =============================================================================
// Benchmark Helper Functions - VibeSQL
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

/// Execute a simple range query on VibeSQL
fn vibesql_simple_range(db: &VibeDB, start: i64, end: i64) -> usize {
    let sql = format!("SELECT c FROM sbtest1 WHERE id BETWEEN {} AND {}", start, end);
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

/// Execute a sum range query on VibeSQL
fn vibesql_sum_range(db: &VibeDB, start: i64, end: i64) -> usize {
    let sql = format!(
        "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {} AND {}",
        start, end
    );
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

/// Execute an order range query on VibeSQL
fn vibesql_order_range(db: &VibeDB, start: i64, end: i64) -> usize {
    let sql = format!(
        "SELECT c FROM sbtest1 WHERE id BETWEEN {} AND {} ORDER BY c",
        start, end
    );
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

/// Execute a distinct range query on VibeSQL
fn vibesql_distinct_range(db: &VibeDB, start: i64, end: i64) -> usize {
    let sql = format!(
        "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {} AND {} ORDER BY c",
        start, end
    );
    let stmt = Parser::parse_sql(&sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

// =============================================================================
// Benchmark Helper Functions - SQLite
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn sqlite_point_select(conn: &SqliteConn, id: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id = ?")
        .unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_simple_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_sum_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_order_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_distinct_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

// =============================================================================
// Benchmark Helper Functions - DuckDB
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn duckdb_point_select(conn: &DuckDBConn, id: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id = ?")
        .unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_simple_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_sum_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_order_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_distinct_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
        .unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

// =============================================================================
// oltp_read_only Benchmark
// =============================================================================

/// Standard sysbench read-only transaction:
/// - 10 point selects
/// - 1 simple range query
/// - 1 sum range query
/// - 1 order range query
/// - 1 distinct range query
fn benchmark_oltp_read_only_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("oltp_read_only");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let mut total = 0;

            // 10 point selects
            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += vibesql_point_select(&db, id);
            }

            // 1 simple range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_simple_range(&db, start, end);

            // 1 sum range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_sum_range(&db, start, end);

            // 1 order range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_order_range(&db, start, end);

            // 1 distinct range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_distinct_range(&db, start, end);

            black_box(total);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_oltp_read_only_sqlite(c: &mut Criterion) {
    let conn = load_sqlite(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("oltp_read_only");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter(|| {
            let mut total = 0;

            // 10 point selects
            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += sqlite_point_select(&conn, id);
            }

            // 1 simple range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += sqlite_simple_range(&conn, start, end);

            // 1 sum range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += sqlite_sum_range(&conn, start, end);

            // 1 order range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += sqlite_order_range(&conn, start, end);

            // 1 distinct range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += sqlite_distinct_range(&conn, start, end);

            black_box(total);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_oltp_read_only_duckdb(c: &mut Criterion) {
    let conn = load_duckdb(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("oltp_read_only");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter(|| {
            let mut total = 0;

            // 10 point selects
            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += duckdb_point_select(&conn, id);
            }

            // 1 simple range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += duckdb_simple_range(&conn, start, end);

            // 1 sum range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += duckdb_sum_range(&conn, start, end);

            // 1 order range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += duckdb_order_range(&conn, start, end);

            // 1 distinct range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += duckdb_distinct_range(&conn, start, end);

            black_box(total);
        });
    });

    group.finish();
}

// =============================================================================
// select_random_points Benchmark
// =============================================================================

/// Multiple random point selects - tests index lookup throughput.
fn benchmark_select_random_points_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_points");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += vibesql_point_select(&db, id);
            }
            black_box(total);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_select_random_points_sqlite(c: &mut Criterion) {
    let conn = load_sqlite(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_points");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter(|| {
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += sqlite_point_select(&conn, id);
            }
            black_box(total);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_select_random_points_duckdb(c: &mut Criterion) {
    let conn = load_duckdb(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_points");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter(|| {
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += duckdb_point_select(&conn, id);
            }
            black_box(total);
        });
    });

    group.finish();
}

// =============================================================================
// select_random_ranges Benchmark
// =============================================================================

/// Range queries with BETWEEN clause - tests range scan performance.
fn benchmark_select_random_ranges_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_ranges");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let (start, end) = data.random_range(RANGE_SIZE);
            black_box(vibesql_simple_range(&db, start, end));
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_select_random_ranges_sqlite(c: &mut Criterion) {
    let conn = load_sqlite(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_ranges");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter(|| {
            let (start, end) = data.random_range(RANGE_SIZE);
            black_box(sqlite_simple_range(&conn, start, end));
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_select_random_ranges_duckdb(c: &mut Criterion) {
    let conn = load_duckdb(TABLE_SIZE);
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_ranges");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter(|| {
            let (start, end) = data.random_range(RANGE_SIZE);
            black_box(duckdb_simple_range(&conn, start, end));
        });
    });

    group.finish();
}

// =============================================================================
// Criterion Benchmark Groups
// =============================================================================

#[cfg(not(feature = "benchmark-comparison"))]
criterion_group!(
    benches,
    benchmark_oltp_read_only_vibesql,
    benchmark_select_random_points_vibesql,
    benchmark_select_random_ranges_vibesql,
);

#[cfg(feature = "benchmark-comparison")]
criterion_group!(
    benches,
    benchmark_oltp_read_only_vibesql,
    benchmark_oltp_read_only_sqlite,
    benchmark_oltp_read_only_duckdb,
    benchmark_select_random_points_vibesql,
    benchmark_select_random_points_sqlite,
    benchmark_select_random_points_duckdb,
    benchmark_select_random_ranges_vibesql,
    benchmark_select_random_ranges_sqlite,
    benchmark_select_random_ranges_duckdb,
);

criterion_main!(benches);
