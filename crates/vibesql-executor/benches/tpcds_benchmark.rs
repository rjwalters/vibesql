//! TPC-DS Benchmark Suite - Native Rust Implementation
//!
//! This benchmark tests TPC-DS queries across three phases:
//! - Phase 1: Core tables (date_dim, time_dim, item, customer, store, store_sales)
//! - Phase 2: Extended tables (promotion, warehouse, ship_mode, reason, store_returns)
//! - Phase 3: Full e-commerce (catalog_sales/returns, web_sales/returns)
//!
//! Usage:
//!   cargo bench --bench tpcds_benchmark
//!   cargo bench --bench tpcds_benchmark --features benchmark-comparison

mod tpcds;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;

use std::time::Duration;
use tpcds::queries::{TPCDS_QUERIES, TPCDS_SANITY_QUERIES};
use tpcds::schema::*;

// =============================================================================
// Benchmark Helper Functions
// =============================================================================

/// Helper function to benchmark a query on VibeSQL
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

    let db = load_vibesql(0.01);

    for (name, sql) in TPCDS_SANITY_QUERIES {
        group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
            b.iter(|| {
                let count = benchmark_vibesql_query(&db, sql);
                black_box(count);
            });
        });
    }

    group.finish();
}

#[cfg(feature = "benchmark-comparison")]
fn bench_sanity_queries_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpcds_sanity_comparison");
    group.measurement_time(Duration::from_secs(5));

    let vibesql_db = load_vibesql(0.01);
    let sqlite_conn = load_sqlite(0.01);
    let duckdb_conn = load_duckdb(0.01);

    for (name, sql) in TPCDS_SANITY_QUERIES {
        group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
            b.iter(|| {
                let count = benchmark_vibesql_query(&vibesql_db, sql);
                black_box(count);
            });
        });

        group.bench_function(BenchmarkId::new("sqlite", *name), |b| {
            b.iter(|| {
                let count = benchmark_sqlite_query(&sqlite_conn, sql);
                black_box(count);
            });
        });

        group.bench_function(BenchmarkId::new("duckdb", *name), |b| {
            b.iter(|| {
                let count = benchmark_duckdb_query(&duckdb_conn, sql);
                black_box(count);
            });
        });
    }

    group.finish();
}

// =============================================================================
// TPC-DS Query Benchmarks
// =============================================================================

fn bench_tpcds_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpcds_queries");
    group.measurement_time(Duration::from_secs(10));

    let db = load_vibesql(0.01);

    for (name, sql) in TPCDS_QUERIES {
        group.bench_function(BenchmarkId::new("vibesql", *name), |b| {
            b.iter(|| {
                let count = benchmark_vibesql_query(&db, sql);
                black_box(count);
            });
        });
    }

    group.finish();
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
