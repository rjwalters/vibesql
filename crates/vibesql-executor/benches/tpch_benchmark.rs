//! TPC-H Benchmark Suite - Native Rust Implementation
//!
//! This benchmark compares pure SQL engine performance across:
//! - VibeSQL (native Rust API)
//! - SQLite (via rusqlite) - requires 'benchmark-comparison' feature
//! - DuckDB (via duckdb-rs) - requires 'benchmark-comparison' feature
//!
//! All measurements are done in-memory with no Python/FFI overhead.
//!
//! Usage:
//!   cargo bench --bench tpch_benchmark --features benchmark-comparison
//!   cargo bench --bench tpch_benchmark --features benchmark-comparison -- --baseline=main
//!   cargo bench --bench tpch_benchmark --features benchmark-comparison -- q1  # Run only Q1
//!
//! Scale factors:
//!   SF 0.01 (10MB) - Fast iteration
//!   SF 0.1 (100MB) - Realistic testing
//!   SF 1.0 (1GB) - Full benchmark

// Allow single-element loops for benchmark configuration flexibility
#![allow(clippy::single_element_loop)]

mod tpch;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;

use std::time::Duration;
use tpch::queries::*;
use tpch::schema::*;

// =============================================================================
// Benchmark Helper Functions
// =============================================================================

/// Helper function to benchmark a query on VibeSQL
fn benchmark_vibesql_query(c: &mut Criterion, name: &str, sql: &str) {
    let db = load_vibesql(0.01);

    c.bench_function(name, |b| {
        b.iter(|| {
            let stmt = Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::Select(select) = stmt {
                let executor = SelectExecutor::new(&db);
                let result = executor.execute(&select).unwrap();
                let count = result.len();
                black_box(count);
            }
        });
    });
}

/// Helper function to benchmark a query on VibeSQL with benchmark groups (for Q1)
fn benchmark_vibesql_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let db = load_vibesql(sf);

        group.bench_with_input(BenchmarkId::new("vibesql", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let stmt = Parser::parse_sql(sql).unwrap();
                if let vibesql_ast::Statement::Select(select) = stmt {
                    let executor = SelectExecutor::new(&db);
                    let result = executor.execute(&select).unwrap();
                    black_box(result.len());
                }
            });
        });
    }

    group.finish();
}

/// Helper function to benchmark a query on SQLite
#[cfg(feature = "benchmark-comparison")]
fn benchmark_sqlite_query(c: &mut Criterion, name: &str, sql: &str) {
    let conn = load_sqlite(0.01);

    c.bench_function(name, |b| {
        b.iter(|| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while rows.next().unwrap().is_some() {
                count += 1;
            }
            black_box(count);
        });
    });
}

/// Helper function to benchmark a query on SQLite with benchmark groups (for Q1)
#[cfg(feature = "benchmark-comparison")]
fn benchmark_sqlite_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let conn = load_sqlite(sf);

        group.bench_with_input(BenchmarkId::new("sqlite", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let mut stmt = conn.prepare(sql).unwrap();
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, f64>(2)?,
                        ))
                    })
                    .unwrap();

                let mut count = 0;
                for _ in rows {
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

/// Helper function to benchmark a query on DuckDB
#[cfg(feature = "duckdb-comparison")]
fn benchmark_duckdb_query(c: &mut Criterion, name: &str, sql: &str) {
    let conn = load_duckdb(0.01);

    c.bench_function(name, |b| {
        b.iter(|| {
            let mut stmt = conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut count = 0;
            while rows.next().unwrap().is_some() {
                count += 1;
            }
            black_box(count);
        });
    });
}

/// Helper function to benchmark a query on MySQL
/// Only runs if MYSQL_URL environment variable is set
#[cfg(feature = "mysql-comparison")]
fn benchmark_mysql_query(c: &mut Criterion, name: &str, sql: &str) {
    // MySQL benchmarks are optional - skip if no connection available
    let Some(mut conn) = load_mysql(0.01) else {
        eprintln!("Skipping MySQL benchmark {} - MYSQL_URL not set", name);
        return;
    };

    c.bench_function(name, |b| {
        b.iter(|| {
            let result: Vec<mysql::Row> = conn.query(sql).unwrap();
            black_box(result.len());
        });
    });
}

/// Helper function to benchmark a query on DuckDB with benchmark groups (for Q1)
#[cfg(feature = "duckdb-comparison")]
fn benchmark_duckdb_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        let conn = load_duckdb(sf);

        group.bench_with_input(BenchmarkId::new("duckdb", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let mut stmt = conn.prepare(sql).unwrap();
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, f64>(2)?,
                        ))
                    })
                    .unwrap();

                let mut count = 0;
                for _ in rows {
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

/// Helper function to benchmark a query on MySQL with benchmark groups (for Q1)
#[cfg(feature = "mysql-comparison")]
fn benchmark_mysql_query_grouped(c: &mut Criterion, group_name: &str, sql: &str) {
    let Some(mut conn) = load_mysql(0.01) else {
        eprintln!("Skipping MySQL grouped benchmark {} - MYSQL_URL not set", group_name);
        return;
    };

    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(10));

    for &sf in &[0.01] {
        group.bench_with_input(BenchmarkId::new("mysql", format!("SF{}", sf)), &sf, |b, _| {
            b.iter(|| {
                let result: Vec<mysql::Row> = conn.query(sql).unwrap();
                black_box(result.len());
            });
        });
    }

    group.finish();
}

// =============================================================================
// Macro to Generate Benchmark Functions
// =============================================================================

/// Macro to generate four benchmark functions (vibesql, sqlite, duckdb, mysql) for a given query
macro_rules! tpch_benchmark {
    ($query_num:expr, $sql:expr) => {
        paste::paste! {
            fn [<benchmark_q $query_num _vibesql>](c: &mut Criterion) {
                benchmark_vibesql_query(c, concat!("tpch_q", stringify!($query_num), "_vibesql"), $sql);
            }

            #[cfg(feature = "sqlite-comparison")]
            fn [<benchmark_q $query_num _sqlite>](c: &mut Criterion) {
                benchmark_sqlite_query(c, concat!("tpch_q", stringify!($query_num), "_sqlite"), $sql);
            }

            #[cfg(feature = "duckdb-comparison")]
            fn [<benchmark_q $query_num _duckdb>](c: &mut Criterion) {
                benchmark_duckdb_query(c, concat!("tpch_q", stringify!($query_num), "_duckdb"), $sql);
            }

            #[cfg(feature = "mysql-comparison")]
            fn [<benchmark_q $query_num _mysql>](c: &mut Criterion) {
                benchmark_mysql_query(c, concat!("tpch_q", stringify!($query_num), "_mysql"), $sql);
            }
        }
    };
}

/// Special macro for Q1 which uses grouped benchmarks
macro_rules! tpch_benchmark_grouped {
    ($query_num:expr, $sql:expr) => {
        paste::paste! {
            fn [<benchmark_q $query_num _vibesql>](c: &mut Criterion) {
                benchmark_vibesql_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }

            #[cfg(feature = "sqlite-comparison")]
            fn [<benchmark_q $query_num _sqlite>](c: &mut Criterion) {
                benchmark_sqlite_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }

            #[cfg(feature = "duckdb-comparison")]
            fn [<benchmark_q $query_num _duckdb>](c: &mut Criterion) {
                benchmark_duckdb_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }

            #[cfg(feature = "mysql-comparison")]
            fn [<benchmark_q $query_num _mysql>](c: &mut Criterion) {
                benchmark_mysql_query_grouped(c, concat!("tpch_q", stringify!($query_num)), $sql);
            }
        }
    };
}

// =============================================================================
// Generate All Benchmark Functions
// =============================================================================

// Q1 uses grouped benchmarks
tpch_benchmark_grouped!(1, TPCH_Q1);

// Q2-Q22 use simple benchmarks
tpch_benchmark!(2, TPCH_Q2);
tpch_benchmark!(3, TPCH_Q3);
tpch_benchmark!(4, TPCH_Q4);
tpch_benchmark!(5, TPCH_Q5);
tpch_benchmark!(6, TPCH_Q6);
tpch_benchmark!(7, TPCH_Q7);
tpch_benchmark!(8, TPCH_Q8);
tpch_benchmark!(9, TPCH_Q9);
tpch_benchmark!(10, TPCH_Q10);
tpch_benchmark!(11, TPCH_Q11);
tpch_benchmark!(12, TPCH_Q12);
tpch_benchmark!(13, TPCH_Q13);
tpch_benchmark!(14, TPCH_Q14);
tpch_benchmark!(15, TPCH_Q15);
tpch_benchmark!(16, TPCH_Q16);
tpch_benchmark!(17, TPCH_Q17);
tpch_benchmark!(18, TPCH_Q18);
tpch_benchmark!(19, TPCH_Q19);
tpch_benchmark!(20, TPCH_Q20);
tpch_benchmark!(21, TPCH_Q21);
tpch_benchmark!(22, TPCH_Q22);

// =============================================================================
// Criterion Benchmark Group
// =============================================================================

// VibeSQL-only benchmarks (no comparison features enabled)
#[cfg(not(any(
    feature = "sqlite-comparison",
    feature = "duckdb-comparison",
    feature = "mysql-comparison"
)))]
criterion_group!(
    benches,
    benchmark_q1_vibesql,
    benchmark_q2_vibesql,
    benchmark_q3_vibesql,
    benchmark_q4_vibesql,
    benchmark_q5_vibesql,
    benchmark_q6_vibesql,
    benchmark_q7_vibesql,
    benchmark_q8_vibesql,
    benchmark_q9_vibesql,
    benchmark_q10_vibesql,
    benchmark_q11_vibesql,
    benchmark_q12_vibesql,
    benchmark_q13_vibesql,
    benchmark_q14_vibesql,
    benchmark_q15_vibesql,
    benchmark_q16_vibesql,
    benchmark_q17_vibesql,
    benchmark_q18_vibesql,
    benchmark_q19_vibesql,
    benchmark_q20_vibesql,
    benchmark_q21_vibesql,
    benchmark_q22_vibesql
);

// VibeSQL + SQLite benchmarks (sqlite-comparison enabled, but not duckdb or mysql)
#[cfg(all(
    feature = "sqlite-comparison",
    not(feature = "duckdb-comparison"),
    not(feature = "mysql-comparison")
))]
criterion_group!(
    benches,
    benchmark_q1_vibesql,
    benchmark_q1_sqlite,
    benchmark_q2_vibesql,
    benchmark_q2_sqlite,
    benchmark_q3_vibesql,
    benchmark_q3_sqlite,
    benchmark_q4_vibesql,
    benchmark_q4_sqlite,
    benchmark_q5_vibesql,
    benchmark_q5_sqlite,
    benchmark_q6_vibesql,
    benchmark_q6_sqlite,
    benchmark_q7_vibesql,
    benchmark_q7_sqlite,
    benchmark_q8_vibesql,
    benchmark_q8_sqlite,
    benchmark_q9_vibesql,
    benchmark_q9_sqlite,
    benchmark_q10_vibesql,
    benchmark_q10_sqlite,
    benchmark_q11_vibesql,
    benchmark_q11_sqlite,
    benchmark_q12_vibesql,
    benchmark_q12_sqlite,
    benchmark_q13_vibesql,
    benchmark_q13_sqlite,
    benchmark_q14_vibesql,
    benchmark_q14_sqlite,
    benchmark_q15_vibesql,
    benchmark_q15_sqlite,
    benchmark_q16_vibesql,
    benchmark_q16_sqlite,
    benchmark_q17_vibesql,
    benchmark_q17_sqlite,
    benchmark_q18_vibesql,
    benchmark_q18_sqlite,
    benchmark_q19_vibesql,
    benchmark_q19_sqlite,
    benchmark_q20_vibesql,
    benchmark_q20_sqlite,
    benchmark_q21_vibesql,
    benchmark_q21_sqlite,
    benchmark_q22_vibesql,
    benchmark_q22_sqlite
);

// VibeSQL + SQLite + DuckDB benchmarks (duckdb-comparison enabled, but not mysql)
#[cfg(all(
    feature = "sqlite-comparison",
    feature = "duckdb-comparison",
    not(feature = "mysql-comparison")
))]
criterion_group!(
    benches,
    benchmark_q1_vibesql,
    benchmark_q1_sqlite,
    benchmark_q1_duckdb,
    benchmark_q2_vibesql,
    benchmark_q2_sqlite,
    benchmark_q2_duckdb,
    benchmark_q3_vibesql,
    benchmark_q3_sqlite,
    benchmark_q3_duckdb,
    benchmark_q4_vibesql,
    benchmark_q4_sqlite,
    benchmark_q4_duckdb,
    benchmark_q5_vibesql,
    benchmark_q5_sqlite,
    benchmark_q5_duckdb,
    benchmark_q6_vibesql,
    benchmark_q6_sqlite,
    benchmark_q6_duckdb,
    benchmark_q7_vibesql,
    benchmark_q7_sqlite,
    benchmark_q7_duckdb,
    benchmark_q8_vibesql,
    benchmark_q8_sqlite,
    benchmark_q8_duckdb,
    benchmark_q9_vibesql,
    benchmark_q9_sqlite,
    benchmark_q9_duckdb,
    benchmark_q10_vibesql,
    benchmark_q10_sqlite,
    benchmark_q10_duckdb,
    benchmark_q11_vibesql,
    benchmark_q11_sqlite,
    benchmark_q11_duckdb,
    benchmark_q12_vibesql,
    benchmark_q12_sqlite,
    benchmark_q12_duckdb,
    benchmark_q13_vibesql,
    benchmark_q13_sqlite,
    benchmark_q13_duckdb,
    benchmark_q14_vibesql,
    benchmark_q14_sqlite,
    benchmark_q14_duckdb,
    benchmark_q15_vibesql,
    benchmark_q15_sqlite,
    benchmark_q15_duckdb,
    benchmark_q16_vibesql,
    benchmark_q16_sqlite,
    benchmark_q16_duckdb,
    benchmark_q17_vibesql,
    benchmark_q17_sqlite,
    benchmark_q17_duckdb,
    benchmark_q18_vibesql,
    benchmark_q18_sqlite,
    benchmark_q18_duckdb,
    benchmark_q19_vibesql,
    benchmark_q19_sqlite,
    benchmark_q19_duckdb,
    benchmark_q20_vibesql,
    benchmark_q20_sqlite,
    benchmark_q20_duckdb,
    benchmark_q21_vibesql,
    benchmark_q21_sqlite,
    benchmark_q21_duckdb,
    benchmark_q22_vibesql,
    benchmark_q22_sqlite,
    benchmark_q22_duckdb
);

// Full comparison: VibeSQL + SQLite + DuckDB + MySQL (all comparison features enabled)
#[cfg(all(
    feature = "sqlite-comparison",
    feature = "duckdb-comparison",
    feature = "mysql-comparison"
))]
criterion_group!(
    benches,
    benchmark_q1_vibesql,
    benchmark_q1_sqlite,
    benchmark_q1_duckdb,
    benchmark_q1_mysql,
    benchmark_q2_vibesql,
    benchmark_q2_sqlite,
    benchmark_q2_duckdb,
    benchmark_q2_mysql,
    benchmark_q3_vibesql,
    benchmark_q3_sqlite,
    benchmark_q3_duckdb,
    benchmark_q3_mysql,
    benchmark_q4_vibesql,
    benchmark_q4_sqlite,
    benchmark_q4_duckdb,
    benchmark_q4_mysql,
    benchmark_q5_vibesql,
    benchmark_q5_sqlite,
    benchmark_q5_duckdb,
    benchmark_q5_mysql,
    benchmark_q6_vibesql,
    benchmark_q6_sqlite,
    benchmark_q6_duckdb,
    benchmark_q6_mysql,
    benchmark_q7_vibesql,
    benchmark_q7_sqlite,
    benchmark_q7_duckdb,
    benchmark_q7_mysql,
    benchmark_q8_vibesql,
    benchmark_q8_sqlite,
    benchmark_q8_duckdb,
    benchmark_q8_mysql,
    benchmark_q9_vibesql,
    benchmark_q9_sqlite,
    benchmark_q9_duckdb,
    benchmark_q9_mysql,
    benchmark_q10_vibesql,
    benchmark_q10_sqlite,
    benchmark_q10_duckdb,
    benchmark_q10_mysql,
    benchmark_q11_vibesql,
    benchmark_q11_sqlite,
    benchmark_q11_duckdb,
    benchmark_q11_mysql,
    benchmark_q12_vibesql,
    benchmark_q12_sqlite,
    benchmark_q12_duckdb,
    benchmark_q12_mysql,
    benchmark_q13_vibesql,
    benchmark_q13_sqlite,
    benchmark_q13_duckdb,
    benchmark_q13_mysql,
    benchmark_q14_vibesql,
    benchmark_q14_sqlite,
    benchmark_q14_duckdb,
    benchmark_q14_mysql,
    benchmark_q15_vibesql,
    benchmark_q15_sqlite,
    benchmark_q15_duckdb,
    benchmark_q15_mysql,
    benchmark_q16_vibesql,
    benchmark_q16_sqlite,
    benchmark_q16_duckdb,
    benchmark_q16_mysql,
    benchmark_q17_vibesql,
    benchmark_q17_sqlite,
    benchmark_q17_duckdb,
    benchmark_q17_mysql,
    benchmark_q18_vibesql,
    benchmark_q18_sqlite,
    benchmark_q18_duckdb,
    benchmark_q18_mysql,
    benchmark_q19_vibesql,
    benchmark_q19_sqlite,
    benchmark_q19_duckdb,
    benchmark_q19_mysql,
    benchmark_q20_vibesql,
    benchmark_q20_sqlite,
    benchmark_q20_duckdb,
    benchmark_q20_mysql,
    benchmark_q21_vibesql,
    benchmark_q21_sqlite,
    benchmark_q21_duckdb,
    benchmark_q21_mysql,
    benchmark_q22_vibesql,
    benchmark_q22_sqlite,
    benchmark_q22_duckdb,
    benchmark_q22_mysql
);

criterion_main!(benches);
