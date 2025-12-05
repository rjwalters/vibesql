//! Benchmarks for parallel hash join build phase
//!
//! These benchmarks measure the performance improvements from parallelizing
//! the hash table build phase in hash joins (PR #1580, Phase 1.3 of parallelism roadmap).
//!
//! Expected performance characteristics (from PARALLELISM_ROADMAP.md):
//! - 4-6x speedup on large equi-joins with 4+ cores
//! - Threshold: parallelization beneficial for 50k+ rows
//! - Linear scaling up to 4 cores, diminishing returns beyond 8 cores

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse a SELECT statement from SQL
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Setup: Create two tables for join benchmarks
fn setup_join_tables(db: &mut Database, left_rows: usize, right_rows: usize) {
    // Create left table (customers)
    let left_schema = vibesql_catalog::TableSchema::new(
        "customers".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "name".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(left_schema).unwrap();

    // Create right table (orders)
    let right_schema = vibesql_catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "customer_id".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "amount".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(right_schema).unwrap();

    // Insert into left table (customers)
    for i in 0..left_rows {
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(format!("customer_{}", i)),
        ]);
        db.insert_row("customers", row).unwrap();
    }

    // Insert into right table (orders) - each customer has multiple orders
    for i in 0..right_rows {
        let customer_id = (i % left_rows) as i64; // Distribute orders across customers
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(customer_id),
            SqlValue::Integer((i % 1000) as i64),
        ]);
        db.insert_row("orders", row).unwrap();
    }
}

/// Benchmark: Hash join scaling with different data sizes
///
/// This measures the speedup from parallel hash table building
/// as the dataset size increases.
fn bench_hash_join_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_scaling");

    // Test different dataset sizes to see where parallelization kicks in
    for size in [1_000, 10_000, 50_000, 100_000] {
        let mut db = Database::new();
        setup_join_tables(&mut db, size / 10, size); // 10:1 orders to customers ratio

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("equi_join", size), &size, |b, _| {
            b.iter(|| {
                let stmt = parse_select(
                    "SELECT c.name, o.amount
                         FROM customers c
                         JOIN orders o ON c.id = o.customer_id;",
                );
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Hash join with different join ratios
///
/// This tests how the parallel build performs with different
/// cardinalities (1:1, 1:many, many:many).
fn bench_hash_join_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_cardinality");

    let base_size = 50_000;

    // 1:1 join (each customer has exactly 1 order)
    {
        let mut db = Database::new();
        setup_join_tables(&mut db, base_size, base_size);

        group.bench_function("one_to_one", |b| {
            b.iter(|| {
                let stmt = parse_select(
                    "SELECT c.name, o.amount
                     FROM customers c
                     JOIN orders o ON c.id = o.customer_id;",
                );
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    // 1:10 join (each customer has 10 orders)
    {
        let mut db = Database::new();
        setup_join_tables(&mut db, base_size / 10, base_size);

        group.bench_function("one_to_many", |b| {
            b.iter(|| {
                let stmt = parse_select(
                    "SELECT c.name, o.amount
                     FROM customers c
                     JOIN orders o ON c.id = o.customer_id;",
                );
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Hash join build phase in isolation
///
/// This benchmark focuses specifically on the hash table build phase
/// by using a minimal probe phase (small right table).
fn bench_hash_build_phase(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_build_phase");

    for build_size in [10_000, 50_000, 100_000] {
        let mut db = Database::new();
        // Large left table (will be used for building hash table)
        // Small right table (minimal probe cost)
        setup_join_tables(&mut db, build_size, 100);

        group.throughput(Throughput::Elements(build_size as u64));

        group.bench_with_input(BenchmarkId::new("build_phase", build_size), &build_size, |b, _| {
            b.iter(|| {
                let stmt = parse_select(
                    "SELECT c.name, o.amount
                         FROM customers c
                         JOIN orders o ON c.id = o.customer_id;",
                );
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

/// Benchmark: Compare sequential threshold behavior
///
/// This tests the automatic fallback to sequential execution
/// for small datasets.
fn bench_threshold_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("threshold_behavior");

    // Test sizes around the parallelization threshold
    for size in [1_000, 5_000, 10_000, 20_000, 50_000] {
        let mut db = Database::new();
        setup_join_tables(&mut db, size / 10, size);

        group.bench_with_input(BenchmarkId::new("auto_threshold", size), &size, |b, _| {
            b.iter(|| {
                let stmt = parse_select(
                    "SELECT c.name, o.amount
                         FROM customers c
                         JOIN orders o ON c.id = o.customer_id;",
                );
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_hash_join_scaling,
    bench_hash_join_cardinality,
    bench_hash_build_phase,
    bench_threshold_behavior
);
criterion_main!(benches);
