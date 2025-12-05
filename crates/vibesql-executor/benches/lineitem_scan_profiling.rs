//! Lineitem Scan Profiling Benchmark
//!
//! This benchmark isolates lineitem table scan performance to understand:
//! - Pure scan overhead (no predicates, no aggregation)
//! - Date predicate filtering cost
//! - Comparison with DuckDB equivalent operations
//!
//! Usage:
//!   cargo bench --bench lineitem_scan_profiling --features benchmark-comparison
//!
//! Part of issue #2962: Profile lineitem table scan performance

mod tpch;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

use tpch::schema::*;

// =============================================================================
// Query Constants
// =============================================================================

/// Pure scan - no predicates, no aggregation
const LINEITEM_FULL_SCAN: &str = "SELECT * FROM lineitem";

/// Simple COUNT(*) - minimal processing
const LINEITEM_COUNT: &str = "SELECT COUNT(*) FROM lineitem";

/// Date predicate only (same as Q1)
const LINEITEM_DATE_FILTER: &str = "SELECT * FROM lineitem WHERE l_shipdate <= '1998-09-01'";

/// Date predicate with COUNT
const LINEITEM_DATE_COUNT: &str = "SELECT COUNT(*) FROM lineitem WHERE l_shipdate <= '1998-09-01'";

/// Single column projection
const LINEITEM_SINGLE_COLUMN: &str = "SELECT l_orderkey FROM lineitem";

/// Two columns projection
const LINEITEM_TWO_COLUMNS: &str = "SELECT l_orderkey, l_quantity FROM lineitem";

/// LIMIT scan (measure startup cost)
const LINEITEM_LIMIT_100: &str = "SELECT * FROM lineitem LIMIT 100";

/// LIMIT scan with date filter
const LINEITEM_DATE_LIMIT_100: &str =
    "SELECT * FROM lineitem WHERE l_shipdate <= '1998-09-01' LIMIT 100";

// =============================================================================
// VibeSQL Benchmark Functions
// =============================================================================

fn benchmark_vibesql(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(5));

    let db = load_vibesql(0.01);

    // Get row count for throughput calculation
    let row_count = db.get_table("lineitem").map(|t| t.row_count()).unwrap_or(0);

    group.throughput(Throughput::Elements(row_count as u64));

    group.bench_function("vibesql", |b| {
        b.iter(|| {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::Select(select) = stmt {
                let executor = SelectExecutor::new(&db);
                let result = executor.execute(&select).unwrap();
                black_box(result.len())
            } else {
                0
            }
        });
    });

    group.finish();
}

// =============================================================================
// DuckDB Benchmark Functions
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn benchmark_duckdb(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(5));

    let conn = load_duckdb(0.01);

    // Get row count for throughput calculation
    let row_count: i64 = conn
        .prepare("SELECT COUNT(*) FROM lineitem")
        .unwrap()
        .query_row([], |row| row.get(0))
        .unwrap();

    group.throughput(Throughput::Elements(row_count as u64));

    group.bench_function("duckdb", |b| {
        b.iter(|| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while rows.next().unwrap().is_some() {
                count += 1;
            }
            black_box(count)
        });
    });

    group.finish();
}

// =============================================================================
// Benchmark Functions
// =============================================================================

fn bench_full_scan_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_full_scan", LINEITEM_FULL_SCAN);
}

fn bench_count_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_count", LINEITEM_COUNT);
}

fn bench_date_filter_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_date_filter", LINEITEM_DATE_FILTER);
}

fn bench_date_count_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_date_count", LINEITEM_DATE_COUNT);
}

fn bench_single_column_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_single_column", LINEITEM_SINGLE_COLUMN);
}

fn bench_two_columns_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_two_columns", LINEITEM_TWO_COLUMNS);
}

fn bench_limit_100_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_limit_100", LINEITEM_LIMIT_100);
}

fn bench_date_limit_100_vibesql(c: &mut Criterion) {
    benchmark_vibesql(c, "lineitem_date_limit_100", LINEITEM_DATE_LIMIT_100);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_full_scan_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_full_scan", LINEITEM_FULL_SCAN);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_count_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_count", LINEITEM_COUNT);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_date_filter_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_date_filter", LINEITEM_DATE_FILTER);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_date_count_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_date_count", LINEITEM_DATE_COUNT);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_single_column_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_single_column", LINEITEM_SINGLE_COLUMN);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_two_columns_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_two_columns", LINEITEM_TWO_COLUMNS);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_limit_100_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_limit_100", LINEITEM_LIMIT_100);
}

#[cfg(feature = "benchmark-comparison")]
fn bench_date_limit_100_duckdb(c: &mut Criterion) {
    benchmark_duckdb(c, "lineitem_date_limit_100", LINEITEM_DATE_LIMIT_100);
}

// =============================================================================
// Criterion Benchmark Groups
// =============================================================================

#[cfg(not(feature = "benchmark-comparison"))]
criterion_group!(
    benches,
    bench_full_scan_vibesql,
    bench_count_vibesql,
    bench_date_filter_vibesql,
    bench_date_count_vibesql,
    bench_single_column_vibesql,
    bench_two_columns_vibesql,
    bench_limit_100_vibesql,
    bench_date_limit_100_vibesql
);

#[cfg(feature = "benchmark-comparison")]
criterion_group!(
    benches,
    bench_full_scan_vibesql,
    bench_full_scan_duckdb,
    bench_count_vibesql,
    bench_count_duckdb,
    bench_date_filter_vibesql,
    bench_date_filter_duckdb,
    bench_date_count_vibesql,
    bench_date_count_duckdb,
    bench_single_column_vibesql,
    bench_single_column_duckdb,
    bench_two_columns_vibesql,
    bench_two_columns_duckdb,
    bench_limit_100_vibesql,
    bench_limit_100_duckdb,
    bench_date_limit_100_vibesql,
    bench_date_limit_100_duckdb
);

criterion_main!(benches);
