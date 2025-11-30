//! TPC-DS Benchmark Suite - Native Rust Implementation
//!
//! This benchmark tests TPC-DS queries across three phases:
//! - Phase 1: Core tables (date_dim, time_dim, item, customer, store, store_sales)
//! - Phase 2: Extended tables (promotion, warehouse, ship_mode, reason, store_returns)
//! - Phase 3: Full e-commerce (catalog_sales/returns, web_sales/returns)
//!
//! Parse errors are handled gracefully - queries that fail to parse are skipped
//! with a warning, allowing the benchmark suite to continue.
//!
//! Usage:
//!   cargo bench --bench tpcds_benchmark
//!   cargo bench --bench tpcds_benchmark --features benchmark-comparison

mod tpcds;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use std::sync::{Mutex, OnceLock};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;

// =============================================================================
// Query Result Tracking
// =============================================================================

/// Track results for each query during benchmarking
#[derive(Clone, Debug)]
#[allow(dead_code)] // Fields are stored for potential debugging/future use
enum QueryResult {
    Passed { name: String, row_count: usize },
    Skipped { name: String, reason: String },
}

/// Global tracker for query results
static QUERY_RESULTS: OnceLock<Mutex<Vec<QueryResult>>> = OnceLock::new();

fn get_query_results() -> &'static Mutex<Vec<QueryResult>> {
    QUERY_RESULTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn record_query_result(result: QueryResult) {
    if let Ok(mut results) = get_query_results().lock() {
        results.push(result);
    }
}

/// Print summary of all query results at the end of benchmarking
fn print_query_summary() {
    if let Ok(results) = get_query_results().lock() {
        if results.is_empty() {
            return;
        }

        let passed: Vec<_> = results
            .iter()
            .filter(|r| matches!(r, QueryResult::Passed { .. }))
            .collect();
        let skipped: Vec<_> = results
            .iter()
            .filter(|r| matches!(r, QueryResult::Skipped { .. }))
            .collect();

        eprintln!("\n{}", "=".repeat(60));
        eprintln!("TPC-DS BENCHMARK SUMMARY");
        eprintln!("{}", "=".repeat(60));
        eprintln!("Passed:  {} queries", passed.len());
        eprintln!("Skipped: {} queries", skipped.len());
        eprintln!("{}", "-".repeat(60));

        if !skipped.is_empty() {
            eprintln!("\nSkipped queries:");
            for result in &skipped {
                if let QueryResult::Skipped { name, reason } = result {
                    eprintln!("  - {}: {}", name, reason);
                }
            }
        }

        eprintln!("{}", "=".repeat(60));
    }
}

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;

use std::time::Duration;
use tpcds::queries::{TPCDS_QUERIES, TPCDS_SANITY_QUERIES};
use tpcds::schema::*;

// =============================================================================
// Database Caching
// =============================================================================
// Cache databases to avoid reloading for each benchmark group.
// TPC-DS data loading is expensive (~10+ minutes), so we load once and reuse.
//
// Memory Management:
// When running with benchmark-comparison feature, you can set TPCDS_ENGINE
// environment variable to only load specific engines:
//   - TPCDS_ENGINE=sqlite  - Load VibeSQL + SQLite only
//   - TPCDS_ENGINE=duckdb  - Load VibeSQL + DuckDB only
//   - TPCDS_ENGINE=all (or unset) - Load all engines (may cause memory pressure)
//
// The isolated benchmark script (scripts/bench-tpcds-isolated.sh) runs each
// engine in a separate process to avoid memory exhaustion from loading all
// engines simultaneously.

/// Default scale factor for TPC-DS benchmarks
/// Using 0.001 for faster loading (~1 minute vs ~10+ minutes at 0.01)
const SCALE_FACTOR: f64 = 0.001;

/// Check which comparison engine to use (for memory-conscious sequential execution)
#[cfg(feature = "benchmark-comparison")]
fn get_comparison_engine() -> Option<String> {
    std::env::var("TPCDS_ENGINE").ok()
}

/// Check if SQLite comparison should be enabled
#[cfg(feature = "benchmark-comparison")]
fn sqlite_enabled() -> bool {
    match get_comparison_engine() {
        None => true,                                    // Default: all engines
        Some(ref e) if e == "all" => true,               // Explicit all
        Some(ref e) if e == "sqlite" => true,            // SQLite only
        _ => false,
    }
}

/// Check if DuckDB comparison should be enabled
#[cfg(feature = "benchmark-comparison")]
fn duckdb_enabled() -> bool {
    match get_comparison_engine() {
        None => true,                                    // Default: all engines
        Some(ref e) if e == "all" => true,               // Explicit all
        Some(ref e) if e == "duckdb" => true,            // DuckDB only
        _ => false,
    }
}

/// Cached VibeSQL database (loaded once, reused across all benchmarks)
static VIBESQL_DB: OnceLock<VibeDB> = OnceLock::new();

/// Get or initialize the cached VibeSQL database
fn get_vibesql_db() -> &'static VibeDB {
    VIBESQL_DB.get_or_init(|| {
        eprintln!("Loading TPC-DS VibeSQL database (scale factor {})...", SCALE_FACTOR);
        let start = std::time::Instant::now();
        let db = load_vibesql(SCALE_FACTOR);
        eprintln!("VibeSQL database loaded in {:?}", start.elapsed());
        db
    })
}

#[cfg(feature = "benchmark-comparison")]
static SQLITE_CONN: OnceLock<Mutex<SqliteConn>> = OnceLock::new();

#[cfg(feature = "benchmark-comparison")]
fn get_sqlite_conn() -> Option<&'static Mutex<SqliteConn>> {
    if !sqlite_enabled() {
        return None;
    }
    Some(SQLITE_CONN.get_or_init(|| {
        eprintln!("Loading TPC-DS SQLite database (scale factor {})...", SCALE_FACTOR);
        let start = std::time::Instant::now();
        let conn = load_sqlite(SCALE_FACTOR);
        eprintln!("SQLite database loaded in {:?}", start.elapsed());
        Mutex::new(conn)
    }))
}

#[cfg(feature = "benchmark-comparison")]
static DUCKDB_CONN: OnceLock<Mutex<DuckDBConn>> = OnceLock::new();

#[cfg(feature = "benchmark-comparison")]
fn get_duckdb_conn() -> Option<&'static Mutex<DuckDBConn>> {
    if !duckdb_enabled() {
        return None;
    }
    Some(DUCKDB_CONN.get_or_init(|| {
        eprintln!("Loading TPC-DS DuckDB database (scale factor {})...", SCALE_FACTOR);
        let start = std::time::Instant::now();
        let conn = load_duckdb(SCALE_FACTOR);
        eprintln!("DuckDB database loaded in {:?}", start.elapsed());
        Mutex::new(conn)
    }))
}

// =============================================================================
// Benchmark Helper Functions
// =============================================================================

/// Error type for query execution failures
#[derive(Debug)]
enum QueryError {
    ParseError(String),
    ExecutionError(String),
    NotASelect,
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            QueryError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            QueryError::NotASelect => write!(f, "Not a SELECT statement"),
        }
    }
}

/// Try to parse and execute a query, returning an error if it fails
fn try_vibesql_query(db: &VibeDB, sql: &str) -> Result<usize, QueryError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| QueryError::ParseError(e.to_string()))?;

    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor
            .execute(&select)
            .map_err(|e| QueryError::ExecutionError(e.to_string()))?;
        Ok(result.len())
    } else {
        Err(QueryError::NotASelect)
    }
}

/// Helper function to benchmark a query on VibeSQL (panics on error - only use after validation)
fn benchmark_vibesql_query(db: &VibeDB, sql: &str) -> usize {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).unwrap();
        result.len()
    } else {
        0
    }
}

/// Helper function to benchmark a query on SQLite
#[cfg(feature = "benchmark-comparison")]
fn benchmark_sqlite_query(conn: &SqliteConn, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Helper function to benchmark a query on DuckDB
#[cfg(feature = "benchmark-comparison")]
fn benchmark_duckdb_query(conn: &DuckDBConn, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut count = 0;
    while rows.next().unwrap().is_some() {
        count += 1;
    }
    count
}

// =============================================================================
// Sanity Check Benchmarks
// =============================================================================

fn bench_sanity_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpcds_sanity");
    group.measurement_time(Duration::from_secs(5));

    // Use cached database
    let db = get_vibesql_db();

    for (name, sql) in TPCDS_SANITY_QUERIES {
        // Validate query before benchmarking
        match try_vibesql_query(db, sql) {
            Ok(row_count) => {
                record_query_result(QueryResult::Passed {
                    name: format!("sanity_{}", name),
                    row_count,
                });
                group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
                    b.iter(|| {
                        let count = benchmark_vibesql_query(db, sql);
                        black_box(count);
                    });
                });
            }
            Err(e) => {
                eprintln!("[SKIP] sanity_{}: {}", name, e);
                record_query_result(QueryResult::Skipped {
                    name: format!("sanity_{}", name),
                    reason: e.to_string(),
                });
            }
        }
    }

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn bench_sanity_queries_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpcds_sanity_comparison");
    group.measurement_time(Duration::from_secs(5));

    // Use cached databases (only load if enabled via TPCDS_ENGINE env var)
    let vibesql_db = get_vibesql_db();
    let sqlite_conn = get_sqlite_conn();  // Returns None if SQLite disabled
    let duckdb_conn = get_duckdb_conn();  // Returns None if DuckDB disabled

    // Log which engines are enabled
    if let Some(engine) = get_comparison_engine() {
        eprintln!("Running comparison with TPCDS_ENGINE={}", engine);
    }

    for (name, sql) in TPCDS_SANITY_QUERIES {
        // Only benchmark if VibeSQL can parse and execute the query
        if let Err(e) = try_vibesql_query(vibesql_db, sql) {
            eprintln!("[SKIP] comparison_{}: {}", name, e);
            continue;
        }

        group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
            b.iter(|| {
                let count = benchmark_vibesql_query(vibesql_db, sql);
                black_box(count);
            });
        });

        // SQLite benchmark (only if enabled)
        if let Some(sqlite) = sqlite_conn {
            group.bench_function(BenchmarkId::new("sqlite", *name), |b| {
                b.iter(|| {
                    let conn = sqlite.lock().unwrap();
                    let count = benchmark_sqlite_query(&conn, sql);
                    black_box(count);
                });
            });
        }

        // DuckDB benchmark (only if enabled)
        if let Some(duckdb) = duckdb_conn {
            group.bench_function(BenchmarkId::new("duckdb", *name), |b| {
                b.iter(|| {
                    let conn = duckdb.lock().unwrap();
                    let count = benchmark_duckdb_query(&conn, sql);
                    black_box(count);
                });
            });
        }
    }

    group.finish();
}

// =============================================================================
// TPC-DS Query Benchmarks
// =============================================================================

fn bench_tpcds_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpcds_queries");
    group.measurement_time(Duration::from_secs(10));

    // Use cached database
    let db = get_vibesql_db();

    for (name, sql) in TPCDS_QUERIES {
        // Validate query before benchmarking
        match try_vibesql_query(db, sql) {
            Ok(row_count) => {
                record_query_result(QueryResult::Passed {
                    name: name.to_string(),
                    row_count,
                });
                group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
                    b.iter(|| {
                        let count = benchmark_vibesql_query(db, sql);
                        black_box(count);
                    });
                });
            }
            Err(e) => {
                eprintln!("[SKIP] {}: {}", name, e);
                record_query_result(QueryResult::Skipped {
                    name: name.to_string(),
                    reason: e.to_string(),
                });
            }
        }
    }

    group.finish();

    // Print summary at the end
    print_query_summary();
}

// =============================================================================
// Criterion Configuration
// =============================================================================

#[cfg(not(feature = "benchmark-comparison"))]
criterion_group!(
    benches,
    bench_sanity_queries,
    bench_tpcds_queries,
);

#[cfg(feature = "benchmark-comparison")]
criterion_group!(
    benches,
    bench_sanity_queries,
    bench_sanity_queries_comparison,
    bench_tpcds_queries,
);

criterion_main!(benches);
