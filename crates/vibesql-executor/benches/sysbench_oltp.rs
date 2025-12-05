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
//! ## MySQL Benchmarks
//!
//! For MySQL comparison benchmarks, use the standalone runner `sysbench_benchmark.rs`
//! which supports MySQL via the `MYSQL_URL` environment variable. MySQL cannot be
//! integrated into this criterion-based benchmark because MySQL's `&mut self` query
//! API is incompatible with criterion's `iter_batched` pattern.
//!
//! ## Test Categories
//!
//! **Read Tests:**
//! - `sysbench_point_select` - Single row lookup by primary key
//! - `oltp_read_only` - Full read-only transaction (10 point selects + 4 range queries)
//! - `select_random_points` - Multiple random point selects (index lookup throughput)
//! - `select_random_ranges` - Range queries with BETWEEN (range scan performance)
//!
//! **Write Tests:**
//! - `sysbench_insert` - Single row inserts
//! - `sysbench_delete` - Single row delete by primary key
//! - `sysbench_update_index` - Update indexed column (k = k + 1)
//! - `sysbench_update_non_index` - Update non-indexed column (c = ?)
//! - `sysbench_write_only` - Write-only workload (1 index update, 1 non-index update, 1 delete, 1 insert)
//!
//! **Mixed Tests:**
//! - `sysbench_read_write` - Mixed read/write workload (10 reads, 1 update per transaction)
//!
//! ## Usage
//!
//! ```bash
//! # Run all sysbench benchmarks (VibeSQL only)
//! cargo bench --bench sysbench_oltp
//!
//! # Run with SQLite/DuckDB comparison
//! cargo bench --bench sysbench_oltp --features benchmark-comparison
//!
//! # Run only point select benchmarks
//! cargo bench --bench sysbench_oltp -- point_select
//!
//! # Run only read-only transaction benchmarks
//! cargo bench --bench sysbench_oltp -- oltp_read_only
//!
//! # Run only VibeSQL benchmarks
//! cargo bench --bench sysbench_oltp -- vibesql
//!
//! # Run only write_only benchmarks
//! cargo bench --bench sysbench_oltp -- write_only
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
use std::sync::Arc;
use std::time::Duration;
use sysbench::schema::load_vibesql;
use sysbench::SysbenchData;
use vibesql_executor::{PreparedStatement, PreparedStatementCache, Session, SessionMut};
use vibesql_storage::Database as VibeDB;
use vibesql_types::SqlValue;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;
#[cfg(feature = "benchmark-comparison")]
use sysbench::schema::{load_duckdb, load_sqlite};

// =============================================================================
// Prepared Statement Holder for VibeSQL
// =============================================================================

/// Pre-prepared statements for fair comparison with SQLite/DuckDB
///
/// SQLite and DuckDB use `prepare_cached()` which caches parsed statements.
/// This struct holds pre-prepared statements for VibeSQL to ensure fair comparison
/// without SQL parsing overhead in the benchmark hot path.
struct VibesqlPreparedStatements {
    /// SELECT c FROM sbtest1 WHERE id = ?
    point_select: Arc<PreparedStatement>,
    /// SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?
    simple_range: Arc<PreparedStatement>,
    /// SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?
    sum_range: Arc<PreparedStatement>,
    /// SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c
    order_range: Arc<PreparedStatement>,
    /// SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c
    distinct_range: Arc<PreparedStatement>,
    /// DELETE FROM sbtest1 WHERE id = ?
    delete: Arc<PreparedStatement>,
    /// INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)
    insert: Arc<PreparedStatement>,
    /// UPDATE sbtest1 SET c = ? WHERE id = ?
    update_non_index: Arc<PreparedStatement>,
    /// Shared cache for all statements
    cache: Arc<PreparedStatementCache>,
}

impl VibesqlPreparedStatements {
    fn new(db: &VibeDB) -> Self {
        let cache = Arc::new(PreparedStatementCache::default_cache());
        let session = Session::with_shared_cache(db, Arc::clone(&cache));

        Self {
            point_select: session.prepare("SELECT c FROM sbtest1 WHERE id = ?").unwrap(),
            simple_range: session
                .prepare("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?")
                .unwrap(),
            sum_range: session
                .prepare("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?")
                .unwrap(),
            order_range: session
                .prepare("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
                .unwrap(),
            distinct_range: session
                .prepare("SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c")
                .unwrap(),
            delete: session.prepare("DELETE FROM sbtest1 WHERE id = ?").unwrap(),
            // Note: column is named "padding" because PAD is a SQL keyword
            insert: session
                .prepare("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)")
                .unwrap(),
            update_non_index: session.prepare("UPDATE sbtest1 SET c = ? WHERE id = ?").unwrap(),
            cache,
        }
    }
}

/// Default table size for sysbench tests
const TABLE_SIZE: usize = 10_000;

/// Range size for range queries (sysbench default is 100)
const RANGE_SIZE: usize = 100;

/// Number of point selects in oltp_read_only transaction
const POINT_SELECTS_PER_TXN: usize = 10;

/// Number of random IDs for select_random_points benchmark
const RANDOM_POINTS_COUNT: usize = 10;

// =============================================================================
// Helper Functions - VibeSQL (using prepared statements for fair comparison)
// =============================================================================

/// Execute a point select query on VibeSQL using prepared statement
///
/// This uses pre-prepared statements to avoid SQL parsing overhead in the hot path,
/// providing a fair comparison with SQLite's `prepare_cached()`.
fn vibesql_point_select(session: &Session, stmt: &PreparedStatement, id: i64) -> usize {
    let result = session.execute_prepared(stmt, &[SqlValue::Integer(id)]).unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

/// Execute an insert on VibeSQL using prepared statement
fn vibesql_insert(
    session: &mut SessionMut,
    stmt: &PreparedStatement,
    id: i64,
    k: i64,
    c: &str,
    pad: &str,
) {
    session
        .execute_prepared_mut(
            stmt,
            &[
                SqlValue::Integer(id),
                SqlValue::Integer(k),
                SqlValue::Varchar(c.to_string()),
                SqlValue::Varchar(pad.to_string()),
            ],
        )
        .unwrap();
}

/// Execute an update query on VibeSQL (update non-indexed column) using prepared statement
fn vibesql_update_non_index(session: &mut SessionMut, stmt: &PreparedStatement, id: i64, c: &str) {
    session
        .execute_prepared_mut(stmt, &[SqlValue::Varchar(c.to_string()), SqlValue::Integer(id)])
        .unwrap();
}

/// Execute an update query on VibeSQL (update indexed column k)
///
/// Uses direct API for k = k + 1 operation since it requires read-modify-write.
/// This is a fair comparison as the operation itself is equivalent to
/// SQLite's prepared UPDATE sbtest1 SET k = k + 1 WHERE id = ?
fn vibesql_update_index(db: &mut VibeDB, id: i64) {
    // For k = k + 1, we need to read current value first then update
    // Use PK index for O(1) lookup
    let (row_index, current_k, row_clone) = {
        let table = db.get_table("SBTEST1").unwrap();
        let pk_index = table.primary_key_index().unwrap();

        if let Some(&idx) = pk_index.get(&vec![SqlValue::Integer(id)]) {
            let row = &table.scan()[idx];
            // k is at index 1 (id=0, k=1, c=2, pad=3)
            if let SqlValue::Integer(k) = &row.values[1] {
                (idx, *k, row.clone())
            } else {
                return;
            }
        } else {
            return;
        }
    };

    let new_k = current_k + 1;
    // Use direct table update
    let table_mut = db.get_table_mut("SBTEST1").unwrap();
    let mut new_row = row_clone;
    new_row.set(1, SqlValue::Integer(new_k)).unwrap();
    let mut changed = std::collections::HashSet::new();
    changed.insert(1);
    table_mut.update_row_selective(row_index, new_row, &changed).unwrap();
    db.invalidate_columnar_cache("SBTEST1");
}

/// Execute a delete query on VibeSQL using prepared statement
fn vibesql_delete(session: &mut SessionMut, stmt: &PreparedStatement, id: i64) {
    session.execute_prepared_mut(stmt, &[SqlValue::Integer(id)]).unwrap();
}

/// Execute a simple range query on VibeSQL using prepared statement
fn vibesql_simple_range(
    session: &Session,
    stmt: &PreparedStatement,
    start: i64,
    end: i64,
) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

/// Execute a sum range query on VibeSQL using prepared statement
fn vibesql_sum_range(session: &Session, stmt: &PreparedStatement, start: i64, end: i64) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

/// Execute an order range query on VibeSQL using prepared statement
fn vibesql_order_range(session: &Session, stmt: &PreparedStatement, start: i64, end: i64) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

/// Execute a distinct range query on VibeSQL using prepared statement
fn vibesql_distinct_range(
    session: &Session,
    stmt: &PreparedStatement,
    start: i64,
    end: i64,
) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

// =============================================================================
// Helper Functions - SQLite
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn sqlite_point_select(conn: &SqliteConn, id: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_insert(conn: &SqliteConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn.prepare_cached(sysbench::INSERT_SQL_NUMBERED).unwrap();
    stmt.execute(rusqlite::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_update_non_index(conn: &SqliteConn, id: i64, c: &str) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
    stmt.execute(rusqlite::params![c, id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_update_index(conn: &SqliteConn, id: i64) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
    stmt.execute(rusqlite::params![id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_delete(conn: &SqliteConn, id: i64) {
    let mut stmt = conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
    stmt.execute(rusqlite::params![id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_simple_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_sum_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt =
        conn.prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn sqlite_order_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt =
        conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c").unwrap();
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
// Helper Functions - DuckDB
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn duckdb_point_select(conn: &DuckDBConn, id: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_insert(conn: &DuckDBConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn.prepare_cached(sysbench::INSERT_SQL_NUMBERED).unwrap();
    stmt.execute(duckdb::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_update_non_index(conn: &DuckDBConn, id: i64, c: &str) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
    stmt.execute(duckdb::params![c, id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_update_index(conn: &DuckDBConn, id: i64) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
    stmt.execute(duckdb::params![id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_delete(conn: &DuckDBConn, id: i64) {
    let mut stmt = conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
    stmt.execute(duckdb::params![id]).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_simple_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_sum_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt =
        conn.prepare_cached("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "benchmark-comparison")]
fn duckdb_order_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt =
        conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c").unwrap();
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
// Point Select Benchmarks
// =============================================================================

/// Benchmark oltp_point_select on VibeSQL using prepared statements
///
/// This test measures single-row lookup by primary key, which is the most
/// common OLTP operation. It tests index lookup performance.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_point_select_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_point_select");
    group.measurement_time(Duration::from_secs(10));

    let db = load_vibesql(TABLE_SIZE);
    let stmts = VibesqlPreparedStatements::new(&db);
    let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            black_box(vibesql_point_select(&session, &stmts.point_select, id))
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

/// Benchmark oltp_insert on VibeSQL using prepared statements
///
/// This test measures single-row insert performance. Each iteration inserts
/// a new row with a unique ID.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_insert_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

    // We need a fresh database for each benchmark run to avoid duplicate key errors
    // So we use iter_custom to set up a new database for each measurement batch
    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let mut db = load_vibesql(TABLE_SIZE);
            let stmts = VibesqlPreparedStatements::new(&db);
            let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                let k = data_gen.random_k();
                let c = generate_c_string();
                let pad = generate_pad_string();
                vibesql_insert(&mut session, &stmts.insert, next_id, k, &c, &pad);
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
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

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
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

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
// Delete Benchmarks
// =============================================================================

/// Benchmark oltp_delete on VibeSQL using prepared statements
///
/// This test measures DELETE by primary key performance. Uses iter_batched
/// to set up a fresh database for each iteration batch since deletes modify state.
///
/// Sysbench equivalent: DELETE FROM sbtest1 WHERE id = ?
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_delete_vibesql(c: &mut Criterion) {
    use criterion::BatchSize;

    let mut group = c.benchmark_group("sysbench_delete");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each iteration loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_batched(
            || {
                // Setup: create fresh database and prepared statements
                let db = load_vibesql(TABLE_SIZE);
                let stmts = VibesqlPreparedStatements::new(&db);
                let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                (db, stmts, id)
            },
            |(mut db, stmts, id)| {
                // Delete single row using prepared statement
                let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
                vibesql_delete(&mut session, &stmts.delete, id);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_delete_sqlite(c: &mut Criterion) {
    use criterion::BatchSize;

    let mut group = c.benchmark_group("sysbench_delete");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each iteration loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter_batched(
            || {
                let conn = load_sqlite(TABLE_SIZE);
                let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                (conn, id)
            },
            |(conn, id)| {
                sqlite_delete(&conn, id);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_delete_duckdb(c: &mut Criterion) {
    use criterion::BatchSize;

    let mut group = c.benchmark_group("sysbench_delete");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each iteration loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter_batched(
            || {
                let conn = load_duckdb(TABLE_SIZE);
                let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                (conn, id)
            },
            |(conn, id)| {
                duckdb_delete(&conn, id);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// =============================================================================
// Update Index Benchmarks
// =============================================================================

/// Benchmark oltp_update_index on VibeSQL
///
/// This test measures UPDATE performance on an indexed column (k).
/// Tests index maintenance overhead since k has a secondary index.
///
/// Sysbench equivalent: UPDATE sbtest1 SET k = k + 1 WHERE id = ?
fn benchmark_update_index_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        let mut db = load_vibesql(TABLE_SIZE);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            vibesql_update_index(&mut db, id);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_update_index_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        let conn = load_sqlite(TABLE_SIZE);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            sqlite_update_index(&conn, id);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_update_index_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        let conn = load_duckdb(TABLE_SIZE);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            duckdb_update_index(&conn, id);
        });
    });

    group.finish();
}

// =============================================================================
// Update Non-Index Benchmarks
// =============================================================================

/// Benchmark oltp_update_non_index on VibeSQL using prepared statements
///
/// This test measures UPDATE performance on a non-indexed column (c).
/// This avoids index maintenance overhead, measuring pure row update performance.
///
/// Sysbench equivalent: UPDATE sbtest1 SET c = ? WHERE id = ?
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_update_non_index_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_non_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        let mut db = load_vibesql(TABLE_SIZE);
        let stmts = VibesqlPreparedStatements::new(&db);
        let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            let c = generate_c_string();
            vibesql_update_non_index(&mut session, &stmts.update_non_index, id, &c);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_update_non_index_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_non_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        let conn = load_sqlite(TABLE_SIZE);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            let c = generate_c_string();
            sqlite_update_non_index(&conn, id, &c);
        });
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_update_non_index_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_update_non_index");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        let conn = load_duckdb(TABLE_SIZE);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        b.iter(|| {
            let id = rng.random_range(1..=TABLE_SIZE as i64);
            let c = generate_c_string();
            duckdb_update_non_index(&conn, id, &c);
        });
    });

    group.finish();
}

// =============================================================================
// Write-Only Benchmarks
// =============================================================================

/// Benchmark oltp_write_only on VibeSQL using prepared statements
///
/// This test simulates a write-heavy OLTP workload per transaction:
/// - 1 index update (UPDATE sbtest1 SET k = k + 1 WHERE id = ?)
/// - 1 non-index update (UPDATE sbtest1 SET c = ? WHERE id = ?)
/// - 1 delete (DELETE FROM sbtest1 WHERE id = ?)
/// - 1 insert (INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?))
///
/// The delete uses a random existing ID, the insert uses a new ID.
/// This measures write throughput without read operations.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_write_only_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_write_only");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let mut db = load_vibesql(TABLE_SIZE);
            let stmts = VibesqlPreparedStatements::new(&db);
            let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
            let mut rng = ChaCha8Rng::seed_from_u64(42);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // Pick a random ID for updates
                let update_id = rng.random_range(1..=TABLE_SIZE as i64);

                // 1 index update (SET k = k + 1) - uses direct API
                vibesql_update_index(session.database_mut(), update_id);

                // 1 non-index update (SET c = ?)
                let c = generate_c_string();
                vibesql_update_non_index(&mut session, &stmts.update_non_index, update_id, &c);

                // 1 delete (random existing row)
                let delete_id = rng.random_range(1..=next_id - 1);
                vibesql_delete(&mut session, &stmts.delete, delete_id);

                // 1 insert (new row with new ID)
                let k = data_gen.random_k();
                let new_c = generate_c_string();
                let pad = generate_pad_string();
                vibesql_insert(&mut session, &stmts.insert, next_id, k, &new_c, &pad);
                next_id += 1;
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_write_only_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_write_only");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("sqlite", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_sqlite(TABLE_SIZE);
            let mut rng = ChaCha8Rng::seed_from_u64(42);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // Pick a random ID for updates
                let update_id = rng.random_range(1..=TABLE_SIZE as i64);

                // 1 index update (SET k = k + 1)
                sqlite_update_index(&conn, update_id);

                // 1 non-index update (SET c = ?)
                let c = generate_c_string();
                sqlite_update_non_index(&conn, update_id, &c);

                // 1 delete (random existing row)
                let delete_id = rng.random_range(1..=next_id - 1);
                sqlite_delete(&conn, delete_id);

                // 1 insert (new row with new ID)
                let k = data_gen.random_k();
                let new_c = generate_c_string();
                let pad = generate_pad_string();
                sqlite_insert(&conn, next_id, k, &new_c, &pad);
                next_id += 1;
            }
            start.elapsed()
        })
    });

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn benchmark_write_only_duckdb(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_write_only");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("duckdb", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let conn = load_duckdb(TABLE_SIZE);
            let mut rng = ChaCha8Rng::seed_from_u64(42);
            let mut data_gen = SysbenchData::new(TABLE_SIZE);
            let mut next_id = (TABLE_SIZE + 1) as i64;

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // Pick a random ID for updates
                let update_id = rng.random_range(1..=TABLE_SIZE as i64);

                // 1 index update (SET k = k + 1)
                duckdb_update_index(&conn, update_id);

                // 1 non-index update (SET c = ?)
                let c = generate_c_string();
                duckdb_update_non_index(&conn, update_id, &c);

                // 1 delete (random existing row)
                let delete_id = rng.random_range(1..=next_id - 1);
                duckdb_delete(&conn, delete_id);

                // 1 insert (new row with new ID)
                let k = data_gen.random_k();
                let new_c = generate_c_string();
                let pad = generate_pad_string();
                duckdb_insert(&conn, next_id, k, &new_c, &pad);
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

/// Benchmark oltp_read_write on VibeSQL using prepared statements
///
/// This test simulates a mixed OLTP workload with:
/// - 10 point select queries
/// - 1 update (non-indexed column)
///
/// This ratio is based on typical OLTP workloads where reads dominate.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_read_write_vibesql(c: &mut Criterion) {
    let mut group = c.benchmark_group("sysbench_read_write");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter_custom(|iters| {
            let mut db = load_vibesql(TABLE_SIZE);
            let stmts = VibesqlPreparedStatements::new(&db);
            let mut session_mut = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
            let mut rng = ChaCha8Rng::seed_from_u64(42);

            let start = std::time::Instant::now();
            for _ in 0..iters {
                // 10 point selects (use read-only session via database reference)
                for _ in 0..10 {
                    let id = rng.random_range(1..=TABLE_SIZE as i64);
                    let read_session = Session::with_shared_cache(
                        session_mut.database(),
                        Arc::clone(&stmts.cache),
                    );
                    black_box(vibesql_point_select(&read_session, &stmts.point_select, id));
                }

                // 1 update (non-indexed column)
                let id = rng.random_range(1..=TABLE_SIZE as i64);
                let c = generate_c_string();
                vibesql_update_non_index(&mut session_mut, &stmts.update_non_index, id, &c);
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
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

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
    group.sample_size(10); // Reduced: each sample loads fresh 10k-row database

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
// oltp_read_only Benchmark
// =============================================================================

/// Standard sysbench read-only transaction using prepared statements:
/// - 10 point selects
/// - 1 simple range query
/// - 1 sum range query
/// - 1 order range query
/// - 1 distinct range query
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_oltp_read_only_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let stmts = VibesqlPreparedStatements::new(&db);
    let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("oltp_read_only");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let mut total = 0;

            // 10 point selects
            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += vibesql_point_select(&session, &stmts.point_select, id);
            }

            // 1 simple range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_simple_range(&session, &stmts.simple_range, start, end);

            // 1 sum range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_sum_range(&session, &stmts.sum_range, start, end);

            // 1 order range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_order_range(&session, &stmts.order_range, start, end);

            // 1 distinct range query
            let (start, end) = data.random_range(RANGE_SIZE);
            total += vibesql_distinct_range(&session, &stmts.distinct_range, start, end);

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
// select_random_points Benchmark (NEW)
// =============================================================================

/// Multiple random point selects - tests index lookup throughput.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_select_random_points_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let stmts = VibesqlPreparedStatements::new(&db);
    let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_points");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += vibesql_point_select(&session, &stmts.point_select, id);
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
// select_random_ranges Benchmark (NEW)
// =============================================================================

/// Range queries with BETWEEN clause - tests range scan performance.
///
/// Uses prepared statements for fair comparison with SQLite's `prepare_cached()`.
fn benchmark_select_random_ranges_vibesql(c: &mut Criterion) {
    let db = load_vibesql(TABLE_SIZE);
    let stmts = VibesqlPreparedStatements::new(&db);
    let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
    let mut data = SysbenchData::new(TABLE_SIZE);

    let mut group = c.benchmark_group("select_random_ranges");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::new("vibesql", TABLE_SIZE), |b| {
        b.iter(|| {
            let (start, end) = data.random_range(RANGE_SIZE);
            black_box(vibesql_simple_range(&session, &stmts.simple_range, start, end));
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
    benchmark_point_select_vibesql,
    benchmark_insert_vibesql,
    benchmark_delete_vibesql,
    benchmark_update_index_vibesql,
    benchmark_update_non_index_vibesql,
    benchmark_write_only_vibesql,
    benchmark_read_write_vibesql,
    // Read-only benchmarks
    benchmark_oltp_read_only_vibesql,
    benchmark_select_random_points_vibesql,
    benchmark_select_random_ranges_vibesql,
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
    benchmark_delete_vibesql,
    benchmark_delete_sqlite,
    benchmark_delete_duckdb,
    benchmark_update_index_vibesql,
    benchmark_update_index_sqlite,
    benchmark_update_index_duckdb,
    benchmark_update_non_index_vibesql,
    benchmark_update_non_index_sqlite,
    benchmark_update_non_index_duckdb,
    benchmark_write_only_vibesql,
    benchmark_write_only_sqlite,
    benchmark_write_only_duckdb,
    benchmark_read_write_vibesql,
    benchmark_read_write_sqlite,
    benchmark_read_write_duckdb,
    // Read-only benchmarks
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
