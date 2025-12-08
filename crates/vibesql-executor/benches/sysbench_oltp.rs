//! Sysbench OLTP Benchmark Suite - Native Rust Implementation
//!
//! WARNING: BENCHMARK INTEGRITY REQUIREMENT
//! =========================================
//! All database operations in this benchmark MUST go through the SQL execution path.
//! Direct API calls (e.g., Table::get_by_pk, Table::insert_row) that bypass SQL parsing
//! and planning would produce misleading benchmark results since they skip the query
//! processing overhead that real users experience.
//!
//! Acceptable patterns:
//! - session.execute_prepared() / session.execute_prepared_mut() - SQL execution
//! - session.execute() / session.execute_mut() - SQL execution
//!
//! NOT acceptable in benchmark hot path:
//! - db.get_table().get_by_pk() - bypasses SQL
//! - table.insert_row() - bypasses SQL (OK for setup only)
//! - table.scan_all() - bypasses SQL
//!
//! This benchmark measures OLTP (Online Transaction Processing) latency performance
//! using industry-standard sysbench-compatible workloads. It compares:
//! - VibeSQL (native Rust API)
//! - SQLite (via rusqlite) - requires 'sqlite-comparison' feature
//! - DuckDB (via duckdb-rs) - requires 'duckdb-comparison' feature
//!
//! All measurements are done in-memory with no Python/FFI overhead.
//!
//! ## MySQL Benchmarks
//!
//! For MySQL comparison benchmarks, use the standalone runner `sysbench_benchmark.rs`
//! which supports MySQL via the `MYSQL_URL` environment variable.
//!
//! ## Test Categories
//!
//! **Read Tests:**
//! - `point_select` - Single row lookup by primary key
//! - `oltp_read_only` - Full read-only transaction (10 point selects + 4 range queries)
//! - `select_random_points` - Multiple random point selects (index lookup throughput)
//! - `select_random_ranges` - Range queries with BETWEEN (range scan performance)
//!
//! **Write Tests:**
//! - `insert` - Single row inserts
//! - `delete` - Single row delete by primary key
//! - `update_index` - Update indexed column (k = k + 1)
//! - `update_non_index` - Update non-indexed column (c = ?)
//! - `write_only` - Write-only workload (1 index update, 1 non-index update, 1 delete, 1 insert)
//!
//! **Mixed Tests:**
//! - `read_write` - Mixed read/write workload (10 reads, 1 update per transaction)
//!
//! ## Usage
//!
//! ```bash
//! # Run all sysbench benchmarks (VibeSQL only)
//! cargo bench --bench sysbench_oltp
//!
//! # With comparison engines
//! cargo bench --bench sysbench_oltp --features sqlite-comparison
//! cargo bench --bench sysbench_oltp --features duckdb-comparison
//! cargo bench --bench sysbench_oltp --features benchmark-comparison
//!
//! # Environment variables for configuration
//! SYSBENCH_TABLE_SIZE=10000  # Number of rows (default: 10000)
//! WARMUP_ITERATIONS=3        # Warmup iterations (default: 3)
//! BENCHMARK_ITERATIONS=10    # Measurement iterations (default: 10)
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

mod harness;
mod sysbench;

use harness::{print_group_header, print_summary_table, BenchResult, Harness};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use sysbench::schema::load_vibesql;
use sysbench::SysbenchData;
use vibesql_executor::{PreparedStatement, PreparedStatementCache, Session, SessionMut};
use vibesql_storage::Database as VibeDB;
use vibesql_types::SqlValue;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;
#[cfg(feature = "duckdb-comparison")]
use sysbench::schema::load_duckdb;
#[cfg(feature = "sqlite-comparison")]
use sysbench::schema::load_sqlite;

// =============================================================================
// Configuration
// =============================================================================

/// Default table size for sysbench tests
fn table_size() -> usize {
    env::var("SYSBENCH_TABLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

/// Range size for range queries (sysbench default is 100)
const RANGE_SIZE: usize = 100;

/// Number of point selects in oltp_read_only transaction
const POINT_SELECTS_PER_TXN: usize = 10;

/// Number of random IDs for select_random_points benchmark
const RANDOM_POINTS_COUNT: usize = 10;

// =============================================================================
// Prepared Statement Holder for VibeSQL
// =============================================================================

/// Pre-prepared statements for fair comparison with SQLite/DuckDB
struct VibesqlPreparedStatements {
    point_select: Arc<PreparedStatement>,
    simple_range: Arc<PreparedStatement>,
    sum_range: Arc<PreparedStatement>,
    order_range: Arc<PreparedStatement>,
    distinct_range: Arc<PreparedStatement>,
    delete: Arc<PreparedStatement>,
    insert: Arc<PreparedStatement>,
    update_index: Arc<PreparedStatement>,
    update_non_index: Arc<PreparedStatement>,
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
            insert: session
                .prepare("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)")
                .unwrap(),
            update_index: session.prepare("UPDATE sbtest1 SET k = k + 1 WHERE id = ?").unwrap(),
            update_non_index: session.prepare("UPDATE sbtest1 SET c = ? WHERE id = ?").unwrap(),
            cache,
        }
    }
}

// =============================================================================
// Helper Functions - VibeSQL
// =============================================================================

fn vibesql_point_select(session: &Session, stmt: &PreparedStatement, id: i64) -> usize {
    let result = session.execute_prepared(stmt, &[SqlValue::Integer(id)]).unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

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
                SqlValue::Varchar(arcstr::ArcStr::from(c)),
                SqlValue::Varchar(arcstr::ArcStr::from(pad)),
            ],
        )
        .unwrap();
}

fn vibesql_update_non_index(session: &mut SessionMut, stmt: &PreparedStatement, id: i64, c: &str) {
    session
        .execute_prepared_mut(stmt, &[SqlValue::Varchar(arcstr::ArcStr::from(c)), SqlValue::Integer(id)])
        .unwrap();
}

fn vibesql_update_index(session: &mut SessionMut, stmt: &PreparedStatement, id: i64) {
    session.execute_prepared_mut(stmt, &[SqlValue::Integer(id)]).unwrap();
}

fn vibesql_delete(session: &mut SessionMut, stmt: &PreparedStatement, id: i64) {
    session.execute_prepared_mut(stmt, &[SqlValue::Integer(id)]).unwrap();
}

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

fn vibesql_sum_range(session: &Session, stmt: &PreparedStatement, start: i64, end: i64) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

fn vibesql_order_range(session: &Session, stmt: &PreparedStatement, start: i64, end: i64) -> usize {
    let result = session
        .execute_prepared(stmt, &[SqlValue::Integer(start), SqlValue::Integer(end)])
        .unwrap();
    result.rows().map(|r| r.len()).unwrap_or(0)
}

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

#[cfg(feature = "sqlite-comparison")]
fn sqlite_point_select(conn: &SqliteConn, id: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "sqlite-comparison")]
fn sqlite_insert(conn: &SqliteConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn.prepare_cached(sysbench::INSERT_SQL_NUMBERED).unwrap();
    stmt.execute(rusqlite::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn sqlite_update_non_index(conn: &SqliteConn, id: i64, c: &str) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
    stmt.execute(rusqlite::params![c, id]).unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn sqlite_update_index(conn: &SqliteConn, id: i64) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
    stmt.execute(rusqlite::params![id]).unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn sqlite_delete(conn: &SqliteConn, id: i64) {
    let mut stmt = conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
    stmt.execute(rusqlite::params![id]).unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn sqlite_simple_range(conn: &SqliteConn, start: i64, end: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "sqlite-comparison")]
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

#[cfg(feature = "sqlite-comparison")]
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

#[cfg(feature = "sqlite-comparison")]
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

#[cfg(feature = "duckdb-comparison")]
fn duckdb_point_select(conn: &DuckDBConn, id: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1").unwrap();
    let mut rows = stmt.query([id]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "duckdb-comparison")]
fn duckdb_insert(conn: &DuckDBConn, id: i64, k: i64, c: &str, pad: &str) {
    let mut stmt = conn.prepare_cached(sysbench::INSERT_SQL_NUMBERED).unwrap();
    stmt.execute(duckdb::params![id, k, c, pad]).unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn duckdb_update_non_index(conn: &DuckDBConn, id: i64, c: &str) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2").unwrap();
    stmt.execute(duckdb::params![c, id]).unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn duckdb_update_index(conn: &DuckDBConn, id: i64) {
    let mut stmt = conn.prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1").unwrap();
    stmt.execute(duckdb::params![id]).unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn duckdb_delete(conn: &DuckDBConn, id: i64) {
    let mut stmt = conn.prepare_cached("DELETE FROM sbtest1 WHERE id = ?1").unwrap();
    stmt.execute(duckdb::params![id]).unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn duckdb_simple_range(conn: &DuckDBConn, start: i64, end: i64) -> usize {
    let mut stmt = conn.prepare_cached("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?").unwrap();
    let mut rows = stmt.query([start, end]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

#[cfg(feature = "duckdb-comparison")]
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

#[cfg(feature = "duckdb-comparison")]
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

#[cfg(feature = "duckdb-comparison")]
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
// Data Generation Helpers
// =============================================================================

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
// Benchmark Implementations
// =============================================================================

fn run_point_select_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("vibesql", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            black_box(vibesql_point_select(&session, &stmts.point_select, id));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("sqlite", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            black_box(sqlite_point_select(&conn, id));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("duckdb", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            black_box(duckdb_point_select(&conn, id));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

fn run_insert_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL - uses batched pattern because we need fresh DB state
    {
        let stats = harness.run_batched(
            "vibesql",
            || {
                let db = load_vibesql(tbl_size);
                let stmts = VibesqlPreparedStatements::new(&db);
                (db, stmts, SysbenchData::new(tbl_size), (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (mut db, stmts, mut data_gen, mut next_id) = state;
                let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    vibesql_insert(&mut session, &stmts.insert, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let stats = harness.run_batched(
            "sqlite",
            || {
                let conn = load_sqlite(tbl_size);
                (conn, SysbenchData::new(tbl_size), (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    sqlite_insert(&conn, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let stats = harness.run_batched(
            "duckdb",
            || {
                let conn = load_duckdb(tbl_size);
                (conn, SysbenchData::new(tbl_size), (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    duckdb_insert(&conn, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    results
}

fn run_delete_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let stats = harness.run_batched(
            "vibesql",
            || {
                let db = load_vibesql(tbl_size);
                let stmts = VibesqlPreparedStatements::new(&db);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (db, stmts, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (mut db, stmts, mut rng, mut data_gen, mut next_id) = state;
                let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let delete_id = rng.random_range(1..=next_id - 1);
                    vibesql_delete(&mut session, &stmts.delete, delete_id);

                    // Re-insert to maintain table size
                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    vibesql_insert(&mut session, &stmts.insert, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let stats = harness.run_batched(
            "sqlite",
            || {
                let conn = load_sqlite(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (conn, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut rng, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let delete_id = rng.random_range(1..=next_id - 1);
                    sqlite_delete(&conn, delete_id);

                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    sqlite_insert(&conn, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let stats = harness.run_batched(
            "duckdb",
            || {
                let conn = load_duckdb(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (conn, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut rng, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let delete_id = rng.random_range(1..=next_id - 1);
                    duckdb_delete(&conn, delete_id);

                    let k = data_gen.random_k();
                    let c = generate_c_string();
                    let pad = generate_pad_string();
                    duckdb_insert(&conn, next_id, k, &c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    results
}

fn run_update_index_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let mut db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("vibesql", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            vibesql_update_index(&mut session, &stmts.update_index, id);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("sqlite", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            sqlite_update_index(&conn, id);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("duckdb", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let start = Instant::now();
            duckdb_update_index(&conn, id);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

fn run_update_non_index_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let mut db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("vibesql", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let c = generate_c_string();
            let start = Instant::now();
            vibesql_update_non_index(&mut session, &stmts.update_non_index, id, &c);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("sqlite", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let c = generate_c_string();
            let start = Instant::now();
            sqlite_update_non_index(&conn, id, &c);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let stats = harness.run("duckdb", || {
            let id = rng.random_range(1..=tbl_size as i64);
            let c = generate_c_string();
            let start = Instant::now();
            duckdb_update_non_index(&conn, id, &c);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

fn run_write_only_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let stats = harness.run_batched(
            "vibesql",
            || {
                let db = load_vibesql(tbl_size);
                let stmts = VibesqlPreparedStatements::new(&db);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (db, stmts, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (mut db, stmts, mut rng, mut data_gen, mut next_id) = state;
                let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let update_id = rng.random_range(1..=tbl_size as i64);

                    // 1 index update
                    vibesql_update_index(&mut session, &stmts.update_index, update_id);

                    // 1 non-index update
                    let c = generate_c_string();
                    vibesql_update_non_index(&mut session, &stmts.update_non_index, update_id, &c);

                    // 1 delete
                    let delete_id = rng.random_range(1..=next_id - 1);
                    vibesql_delete(&mut session, &stmts.delete, delete_id);

                    // 1 insert
                    let k = data_gen.random_k();
                    let new_c = generate_c_string();
                    let pad = generate_pad_string();
                    vibesql_insert(&mut session, &stmts.insert, next_id, k, &new_c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let stats = harness.run_batched(
            "sqlite",
            || {
                let conn = load_sqlite(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (conn, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut rng, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let update_id = rng.random_range(1..=tbl_size as i64);

                    sqlite_update_index(&conn, update_id);

                    let c = generate_c_string();
                    sqlite_update_non_index(&conn, update_id, &c);

                    let delete_id = rng.random_range(1..=next_id - 1);
                    sqlite_delete(&conn, delete_id);

                    let k = data_gen.random_k();
                    let new_c = generate_c_string();
                    let pad = generate_pad_string();
                    sqlite_insert(&conn, next_id, k, &new_c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let stats = harness.run_batched(
            "duckdb",
            || {
                let conn = load_duckdb(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                let data_gen = SysbenchData::new(tbl_size);
                (conn, rng, data_gen, (tbl_size + 1) as i64)
            },
            |state, batch_iters| {
                let (conn, mut rng, mut data_gen, mut next_id) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    let update_id = rng.random_range(1..=tbl_size as i64);

                    duckdb_update_index(&conn, update_id);

                    let c = generate_c_string();
                    duckdb_update_non_index(&conn, update_id, &c);

                    let delete_id = rng.random_range(1..=next_id - 1);
                    duckdb_delete(&conn, delete_id);

                    let k = data_gen.random_k();
                    let new_c = generate_c_string();
                    let pad = generate_pad_string();
                    duckdb_insert(&conn, next_id, k, &new_c, &pad);
                    next_id += 1;
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    results
}

fn run_read_write_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let stats = harness.run_batched(
            "vibesql",
            || {
                let db = load_vibesql(tbl_size);
                let stmts = VibesqlPreparedStatements::new(&db);
                let rng = ChaCha8Rng::seed_from_u64(42);
                (db, stmts, rng)
            },
            |state, batch_iters| {
                let (mut db, stmts, mut rng) = state;
                let mut session_mut = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));

                let start = Instant::now();
                for _ in 0..batch_iters {
                    // 10 point selects
                    for _ in 0..10 {
                        let id = rng.random_range(1..=tbl_size as i64);
                        let read_session = Session::with_shared_cache(
                            session_mut.database(),
                            Arc::clone(&stmts.cache),
                        );
                        black_box(vibesql_point_select(&read_session, &stmts.point_select, id));
                    }

                    // 1 update
                    let id = rng.random_range(1..=tbl_size as i64);
                    let c = generate_c_string();
                    vibesql_update_non_index(&mut session_mut, &stmts.update_non_index, id, &c);
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let stats = harness.run_batched(
            "sqlite",
            || {
                let conn = load_sqlite(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                (conn, rng)
            },
            |state, batch_iters| {
                let (conn, mut rng) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    for _ in 0..10 {
                        let id = rng.random_range(1..=tbl_size as i64);
                        black_box(sqlite_point_select(&conn, id));
                    }

                    let id = rng.random_range(1..=tbl_size as i64);
                    let c = generate_c_string();
                    sqlite_update_non_index(&conn, id, &c);
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let stats = harness.run_batched(
            "duckdb",
            || {
                let conn = load_duckdb(tbl_size);
                let rng = ChaCha8Rng::seed_from_u64(42);
                (conn, rng)
            },
            |state, batch_iters| {
                let (conn, mut rng) = state;

                let start = Instant::now();
                for _ in 0..batch_iters {
                    for _ in 0..10 {
                        let id = rng.random_range(1..=tbl_size as i64);
                        black_box(duckdb_point_select(&conn, id));
                    }

                    let id = rng.random_range(1..=tbl_size as i64);
                    let c = generate_c_string();
                    duckdb_update_non_index(&conn, id, &c);
                }
                start.elapsed()
            },
        );
        results.push(stats);
    }

    results
}

fn run_oltp_read_only_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("vibesql", || {
            let start = Instant::now();
            let mut total = 0;

            // 10 point selects
            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += vibesql_point_select(&session, &stmts.point_select, id);
            }

            // 1 simple range
            let (s, e) = data.random_range(RANGE_SIZE);
            total += vibesql_simple_range(&session, &stmts.simple_range, s, e);

            // 1 sum range
            let (s, e) = data.random_range(RANGE_SIZE);
            total += vibesql_sum_range(&session, &stmts.sum_range, s, e);

            // 1 order range
            let (s, e) = data.random_range(RANGE_SIZE);
            total += vibesql_order_range(&session, &stmts.order_range, s, e);

            // 1 distinct range
            let (s, e) = data.random_range(RANGE_SIZE);
            total += vibesql_distinct_range(&session, &stmts.distinct_range, s, e);

            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("sqlite", || {
            let start = Instant::now();
            let mut total = 0;

            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += sqlite_point_select(&conn, id);
            }

            let (s, e) = data.random_range(RANGE_SIZE);
            total += sqlite_simple_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += sqlite_sum_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += sqlite_order_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += sqlite_distinct_range(&conn, s, e);

            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("duckdb", || {
            let start = Instant::now();
            let mut total = 0;

            let ids = data.random_ids(POINT_SELECTS_PER_TXN);
            for id in ids {
                total += duckdb_point_select(&conn, id);
            }

            let (s, e) = data.random_range(RANGE_SIZE);
            total += duckdb_simple_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += duckdb_sum_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += duckdb_order_range(&conn, s, e);

            let (s, e) = data.random_range(RANGE_SIZE);
            total += duckdb_distinct_range(&conn, s, e);

            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

fn run_select_random_points_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("vibesql", || {
            let start = Instant::now();
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += vibesql_point_select(&session, &stmts.point_select, id);
            }
            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("sqlite", || {
            let start = Instant::now();
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += sqlite_point_select(&conn, id);
            }
            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("duckdb", || {
            let start = Instant::now();
            let ids = data.random_ids(RANDOM_POINTS_COUNT);
            let mut total = 0;
            for id in ids {
                total += duckdb_point_select(&conn, id);
            }
            black_box(total);
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

fn run_select_random_ranges_benchmarks(harness: &Harness, tbl_size: usize) -> Vec<harness::BenchStats> {
    let mut results = Vec::new();

    // VibeSQL
    {
        let db = load_vibesql(tbl_size);
        let stmts = VibesqlPreparedStatements::new(&db);
        let session = Session::with_shared_cache(&db, Arc::clone(&stmts.cache));
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("vibesql", || {
            let start = Instant::now();
            let (s, e) = data.random_range(RANGE_SIZE);
            black_box(vibesql_simple_range(&session, &stmts.simple_range, s, e));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // SQLite
    #[cfg(feature = "sqlite-comparison")]
    {
        let conn = load_sqlite(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("sqlite", || {
            let start = Instant::now();
            let (s, e) = data.random_range(RANGE_SIZE);
            black_box(sqlite_simple_range(&conn, s, e));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    // DuckDB
    #[cfg(feature = "duckdb-comparison")]
    {
        let conn = load_duckdb(tbl_size);
        let mut data = SysbenchData::new(tbl_size);

        let stats = harness.run("duckdb", || {
            let start = Instant::now();
            let (s, e) = data.random_range(RANGE_SIZE);
            black_box(duckdb_simple_range(&conn, s, e));
            BenchResult::Ok(start.elapsed())
        });
        results.push(stats);
    }

    results
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() {
    let tbl_size = table_size();
    let harness = Harness::new();

    eprintln!("=== Sysbench OLTP Benchmark Suite ===");
    eprintln!("Table size: {} rows", tbl_size);
    eprintln!("Warmup iterations: {}", harness::DEFAULT_WARMUP_ITERATIONS);
    eprintln!("Benchmark iterations: {}", harness::DEFAULT_BENCHMARK_ITERATIONS);
    eprintln!();

    // Point Select
    print_group_header("point_select");
    let results = run_point_select_benchmarks(&harness, tbl_size);
    print_summary_table("point_select", &results);

    // Insert
    print_group_header("insert");
    let results = run_insert_benchmarks(&harness, tbl_size);
    print_summary_table("insert", &results);

    // Delete
    print_group_header("delete");
    let results = run_delete_benchmarks(&harness, tbl_size);
    print_summary_table("delete", &results);

    // Update Index
    print_group_header("update_index");
    let results = run_update_index_benchmarks(&harness, tbl_size);
    print_summary_table("update_index", &results);

    // Update Non-Index
    print_group_header("update_non_index");
    let results = run_update_non_index_benchmarks(&harness, tbl_size);
    print_summary_table("update_non_index", &results);

    // Write-Only
    print_group_header("write_only");
    let results = run_write_only_benchmarks(&harness, tbl_size);
    print_summary_table("write_only", &results);

    // Read-Write
    print_group_header("read_write");
    let results = run_read_write_benchmarks(&harness, tbl_size);
    print_summary_table("read_write", &results);

    // OLTP Read-Only
    print_group_header("oltp_read_only");
    let results = run_oltp_read_only_benchmarks(&harness, tbl_size);
    print_summary_table("oltp_read_only", &results);

    // Select Random Points
    print_group_header("select_random_points");
    let results = run_select_random_points_benchmarks(&harness, tbl_size);
    print_summary_table("select_random_points", &results);

    // Select Random Ranges
    print_group_header("select_random_ranges");
    let results = run_select_random_ranges_benchmarks(&harness, tbl_size);
    print_summary_table("select_random_ranges", &results);

    eprintln!("\n=== Benchmark Complete ===");
}
