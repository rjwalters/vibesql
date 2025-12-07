//! Benchmark: Columnar vs Row Storage for TPC-H Queries
//!
//! This benchmark compares the performance of:
//! 1. Row storage with columnar cache (on-demand conversion)
//! 2. Native columnar storage (direct column storage)
//!
//! The benchmark focuses on analytical queries (TPC-H Q6 and Q1) where
//! columnar storage is expected to provide significant benefits.
//!
//! ## Expected Results
//!
//! - Native columnar tables should show ~10-20% improvement over row+cache for scans
//!   (eliminates row-to-columnar conversion overhead)
//! - INSERT should show significant slowdown for native columnar tables
//!   (O(n) rebuild cost for columnar tables)
//!
//! ## Usage
//!
//! ```bash
//! # Run all columnar storage benchmarks
//! cargo bench --bench columnar_storage_benchmark
//!
//! # Run only Q6 benchmarks
//! cargo bench --bench columnar_storage_benchmark -- q6
//!
//! # Run only Q1 benchmarks
//! cargo bench --bench columnar_storage_benchmark -- q1
//!
//! # Run only INSERT benchmarks
//! cargo bench --bench columnar_storage_benchmark -- insert
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;
use vibesql_catalog::{ColumnSchema, StorageFormat, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, Date, SqlValue};

// =============================================================================
// TPC-H Query Strings
// =============================================================================

/// TPC-H Q6: Forecasting Revenue Change
/// - Single table scan (lineitem)
/// - Simple predicates (date range, BETWEEN, comparison)
/// - Aggregate function (SUM) with no GROUP BY
const TPCH_Q6: &str = r#"
SELECT
    SUM(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
FROM LINEITEM
WHERE
    L_SHIPDATE >= '1994-01-01'
    AND L_SHIPDATE < '1995-01-01'
    AND L_DISCOUNT BETWEEN 0.05 AND 0.07
    AND L_QUANTITY < 24
"#;

/// TPC-H Q1: Pricing Summary Report
/// - Single table scan (lineitem)
/// - Simple date predicate
/// - Multiple aggregates with GROUP BY
const TPCH_Q1: &str = r#"
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_QUANTITY) as sum_qty,
    SUM(L_EXTENDEDPRICE) as sum_base_price,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as sum_disc_price,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as sum_charge,
    AVG(L_QUANTITY) as avg_qty,
    AVG(L_EXTENDEDPRICE) as avg_price,
    AVG(L_DISCOUNT) as avg_disc,
    COUNT(*) as count_order
FROM LINEITEM
WHERE L_SHIPDATE <= '1998-09-01'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS
"#;

// =============================================================================
// Data Generation Utilities
// =============================================================================

/// Generate TPC-H lineitem rows
fn generate_lineitem_data(row_count: usize) -> Vec<Row> {
    let mut rows = Vec::with_capacity(row_count);

    for i in 0..row_count {
        let quantity = ((i * 11) % 50 + 1) as f64;
        let extendedprice = quantity * ((i * 97) as f64 % 100000.0 + 900.0);
        let discount = ((i * 7) % 10) as f64 / 100.0;
        let tax = ((i * 3) % 8) as f64 / 100.0;

        // Generate dates within 1992-1998 range
        let year = 1992 + (i % 7);
        let month = (i % 12) + 1;
        let day = (i % 28) + 1;
        let ship_date = Date::new(year as i32, month as u8, day as u8).unwrap();
        let commit_date = Date::new(year as i32, month as u8, day as u8).unwrap();
        let receipt_date = Date::new(year as i32, month as u8, day as u8).unwrap();

        let return_flag = ["N", "R", "A"][i % 3];
        let line_status = ["O", "F"][i % 2];

        let row = Row::new(vec![
            SqlValue::Integer((i / 7 + 1) as i64),             // l_orderkey
            SqlValue::Integer(((i * 13) % 200000 + 1) as i64), // l_partkey
            SqlValue::Integer(((i * 17) % 100 + 1) as i64),    // l_suppkey
            SqlValue::Integer(((i % 7) + 1) as i64),           // l_linenumber
            SqlValue::Numeric(quantity),                       // l_quantity
            SqlValue::Numeric(extendedprice),                  // l_extendedprice
            SqlValue::Numeric(discount),                       // l_discount
            SqlValue::Numeric(tax),                            // l_tax
            SqlValue::Varchar(arcstr::ArcStr::from(return_flag)),        // l_returnflag
            SqlValue::Varchar(arcstr::ArcStr::from(line_status)),        // l_linestatus
            SqlValue::Date(ship_date),                         // l_shipdate
            SqlValue::Date(commit_date),                       // l_commitdate
            SqlValue::Date(receipt_date),                      // l_receiptdate
            SqlValue::Varchar(arcstr::ArcStr::from("DELIVER IN PERSON")), // l_shipinstruct
            SqlValue::Varchar(arcstr::ArcStr::from("TRUCK")),            // l_shipmode
            SqlValue::Varchar(arcstr::ArcStr::from("test comment")),     // l_comment
        ]);

        rows.push(row);
    }

    rows
}

/// Create lineitem table schema
fn lineitem_schema() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema::new("l_orderkey".to_string(), DataType::Integer, false),
        ColumnSchema::new("l_partkey".to_string(), DataType::Integer, false),
        ColumnSchema::new("l_suppkey".to_string(), DataType::Integer, false),
        ColumnSchema::new("l_linenumber".to_string(), DataType::Integer, false),
        ColumnSchema::new(
            "l_quantity".to_string(),
            DataType::Decimal { precision: 15, scale: 2 },
            false,
        ),
        ColumnSchema::new(
            "l_extendedprice".to_string(),
            DataType::Decimal { precision: 15, scale: 2 },
            false,
        ),
        ColumnSchema::new(
            "l_discount".to_string(),
            DataType::Decimal { precision: 15, scale: 2 },
            false,
        ),
        ColumnSchema::new(
            "l_tax".to_string(),
            DataType::Decimal { precision: 15, scale: 2 },
            false,
        ),
        ColumnSchema::new(
            "l_returnflag".to_string(),
            DataType::Varchar { max_length: Some(1) },
            false,
        ),
        ColumnSchema::new(
            "l_linestatus".to_string(),
            DataType::Varchar { max_length: Some(1) },
            false,
        ),
        ColumnSchema::new("l_shipdate".to_string(), DataType::Date, false),
        ColumnSchema::new("l_commitdate".to_string(), DataType::Date, false),
        ColumnSchema::new("l_receiptdate".to_string(), DataType::Date, false),
        ColumnSchema::new(
            "l_shipinstruct".to_string(),
            DataType::Varchar { max_length: Some(25) },
            false,
        ),
        ColumnSchema::new(
            "l_shipmode".to_string(),
            DataType::Varchar { max_length: Some(10) },
            false,
        ),
        ColumnSchema::new(
            "l_comment".to_string(),
            DataType::Varchar { max_length: Some(44) },
            true,
        ),
    ]
}

// =============================================================================
// Database Setup Helpers
// =============================================================================

/// Create database with row storage (default)
fn create_row_database(row_count: usize) -> Database {
    let mut db = Database::new();

    let schema = TableSchema::with_storage_format(
        "LINEITEM".to_string(),
        lineitem_schema(),
        StorageFormat::Row,
    );
    db.create_table(schema).unwrap();

    let rows = generate_lineitem_data(row_count);
    for row in rows {
        db.insert_row("LINEITEM", row).unwrap();
    }

    // Analyze for statistics
    if let Some(table) = db.get_table_mut("LINEITEM") {
        table.analyze();
    }

    db
}

/// Create database with native columnar storage
fn create_columnar_database(row_count: usize) -> Database {
    let mut db = Database::new();

    let schema = TableSchema::with_storage_format(
        "LINEITEM".to_string(),
        lineitem_schema(),
        StorageFormat::Columnar,
    );
    db.create_table(schema).unwrap();

    let rows = generate_lineitem_data(row_count);
    for row in rows {
        db.insert_row("LINEITEM", row).unwrap();
    }

    // Analyze for statistics
    if let Some(table) = db.get_table_mut("LINEITEM") {
        table.analyze();
    }

    db
}

/// Execute a SQL query and return row count
fn execute_query(db: &Database, sql: &str) -> usize {
    let stmt = Parser::parse_sql(sql).expect("Query should parse");
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute(&select).expect("Query should execute");
        result.len()
    } else {
        panic!("Expected SELECT statement");
    }
}

// =============================================================================
// TPC-H Q6 Benchmark: Scan + Filter + Aggregate
// =============================================================================

fn bench_tpch_q6(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpch_q6_storage");
    group.measurement_time(Duration::from_secs(10));

    // Test with different data sizes
    for &row_count in &[10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(row_count as u64));

        // Create databases once (outside of benchmark loop)
        let row_db = create_row_database(row_count);
        let columnar_db = create_columnar_database(row_count);

        // Benchmark row storage (with columnar cache)
        group.bench_with_input(BenchmarkId::new("row_storage", row_count), &row_count, |b, _| {
            b.iter(|| {
                let count = execute_query(&row_db, TPCH_Q6);
                black_box(count);
            });
        });

        // Benchmark native columnar storage
        group.bench_with_input(
            BenchmarkId::new("columnar_storage", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let count = execute_query(&columnar_db, TPCH_Q6);
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// TPC-H Q1 Benchmark: Aggregation with GROUP BY
// =============================================================================

fn bench_tpch_q1(c: &mut Criterion) {
    let mut group = c.benchmark_group("tpch_q1_storage");
    group.measurement_time(Duration::from_secs(10));

    // Test with different data sizes
    for &row_count in &[10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(row_count as u64));

        // Create databases once
        let row_db = create_row_database(row_count);
        let columnar_db = create_columnar_database(row_count);

        // Benchmark row storage
        group.bench_with_input(BenchmarkId::new("row_storage", row_count), &row_count, |b, _| {
            b.iter(|| {
                let count = execute_query(&row_db, TPCH_Q1);
                black_box(count);
            });
        });

        // Benchmark native columnar storage
        group.bench_with_input(
            BenchmarkId::new("columnar_storage", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let count = execute_query(&columnar_db, TPCH_Q1);
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// INSERT Performance Benchmark
// =============================================================================

fn bench_insert_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_performance");
    group.measurement_time(Duration::from_secs(5));

    // Test INSERT with different batch sizes
    for &batch_size in &[100, 1_000, 5_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        let rows = generate_lineitem_data(batch_size);

        // Benchmark INSERT into row storage
        group.bench_with_input(BenchmarkId::new("row_storage", batch_size), &batch_size, |b, _| {
            b.iter(|| {
                let mut db = Database::new();
                let schema = TableSchema::with_storage_format(
                    "LINEITEM".to_string(),
                    lineitem_schema(),
                    StorageFormat::Row,
                );
                db.create_table(schema).unwrap();

                for row in &rows {
                    db.insert_row("LINEITEM", row.clone()).unwrap();
                }
                black_box(db);
            });
        });

        // Benchmark INSERT into native columnar storage
        group.bench_with_input(
            BenchmarkId::new("columnar_storage", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let mut db = Database::new();
                    let schema = TableSchema::with_storage_format(
                        "LINEITEM".to_string(),
                        lineitem_schema(),
                        StorageFormat::Columnar,
                    );
                    db.create_table(schema).unwrap();

                    for row in &rows {
                        db.insert_row("LINEITEM", row.clone()).unwrap();
                    }
                    black_box(db);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Bulk INSERT Performance Benchmark (measures overhead for existing data)
// =============================================================================

fn bench_bulk_insert_into_existing(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert_existing");
    group.measurement_time(Duration::from_secs(5));

    // Pre-populate tables with existing data, then insert more
    let initial_row_count = 50_000;
    let insert_batch_size = 1_000;

    let insert_rows = generate_lineitem_data(insert_batch_size);

    group.throughput(Throughput::Elements(insert_batch_size as u64));

    // Benchmark INSERT into row storage with existing data
    group.bench_function(
        BenchmarkId::new("row_storage", format!("{}+{}", initial_row_count, insert_batch_size)),
        |b| {
            b.iter_batched(
                || create_row_database(initial_row_count),
                |mut db| {
                    for row in &insert_rows {
                        db.insert_row("LINEITEM", row.clone()).unwrap();
                    }
                    black_box(db);
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    // Benchmark INSERT into native columnar storage with existing data
    // This should show the O(n) rebuild cost
    group.bench_function(
        BenchmarkId::new(
            "columnar_storage",
            format!("{}+{}", initial_row_count, insert_batch_size),
        ),
        |b| {
            b.iter_batched(
                || create_columnar_database(initial_row_count),
                |mut db| {
                    for row in &insert_rows {
                        db.insert_row("LINEITEM", row.clone()).unwrap();
                    }
                    black_box(db);
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

// =============================================================================
// Columnar Cache Warmup Benchmark
// =============================================================================

fn bench_columnar_cache_warmup(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnar_cache_warmup");
    group.measurement_time(Duration::from_secs(5));

    for &row_count in &[10_000, 50_000] {
        group.throughput(Throughput::Elements(row_count as u64));

        // First query (cold cache) vs subsequent queries (warm cache)
        // This demonstrates the conversion overhead for row storage

        group.bench_with_input(
            BenchmarkId::new("row_storage_cold", row_count),
            &row_count,
            |b, _| {
                b.iter_batched(
                    || create_row_database(row_count),
                    |db| {
                        // First query - cold columnar cache
                        let count = execute_query(&db, TPCH_Q6);
                        black_box(count);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Warm cache - create DB, run query once, then benchmark
        let row_db = {
            let db = create_row_database(row_count);
            // Warm up the cache
            let _ = execute_query(&db, TPCH_Q6);
            db
        };

        group.bench_with_input(
            BenchmarkId::new("row_storage_warm", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let count = execute_query(&row_db, TPCH_Q6);
                    black_box(count);
                });
            },
        );

        // Native columnar - no cache needed
        let columnar_db = create_columnar_database(row_count);

        group.bench_with_input(
            BenchmarkId::new("columnar_storage", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let count = execute_query(&columnar_db, TPCH_Q6);
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Memory Efficiency Benchmark (row count for reference)
// =============================================================================

fn bench_full_table_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_table_scan");
    group.measurement_time(Duration::from_secs(5));

    // Simple COUNT(*) to measure pure scan performance
    const COUNT_QUERY: &str = "SELECT COUNT(*) FROM LINEITEM";

    for &row_count in &[10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(row_count as u64));

        let row_db = create_row_database(row_count);
        let columnar_db = create_columnar_database(row_count);

        group.bench_with_input(BenchmarkId::new("row_storage", row_count), &row_count, |b, _| {
            b.iter(|| {
                let count = execute_query(&row_db, COUNT_QUERY);
                black_box(count);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("columnar_storage", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let count = execute_query(&columnar_db, COUNT_QUERY);
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Criterion Group and Main
// =============================================================================

criterion_group!(
    benches,
    bench_tpch_q6,
    bench_tpch_q1,
    bench_insert_performance,
    bench_bulk_insert_into_existing,
    bench_columnar_cache_warmup,
    bench_full_table_scan,
);

criterion_main!(benches);
