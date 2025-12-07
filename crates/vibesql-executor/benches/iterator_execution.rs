//! Benchmarks for iterator-based vs materialized execution
//!
//! These benchmarks demonstrate the performance benefits of lazy iterator execution
//! for queries that can avoid full materialization (no ORDER BY, DISTINCT, or window functions).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Parse a SELECT statement from SQL
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Setup: Create a table with N rows
fn setup_large_table(db: &mut Database, row_count: usize) {
    // Create table (use uppercase since SQL parser normalizes identifiers to uppercase)
    let schema = vibesql_catalog::TableSchema::new(
        "LARGE_TABLE".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "VALUE".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "AMOUNT".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows
    for i in 0..row_count {
        let row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(i as i64),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("row_{}", i))),
            vibesql_types::SqlValue::Integer((i % 100) as i64),
        ]);
        db.insert_row("LARGE_TABLE", row).unwrap();
    }
}

/// Benchmark: SELECT with LIMIT (iterator path) vs without LIMIT (materialized path)
fn bench_limit_early_termination(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit_early_termination");

    for row_count in [1000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_large_table(&mut db, row_count);

        group.throughput(Throughput::Elements(10)); // We only fetch 10 rows

        // Iterator path: SELECT with LIMIT (should only process ~10 rows)
        group.bench_with_input(
            BenchmarkId::new("iterator_with_limit", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select("SELECT * FROM large_table LIMIT 10;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );

        // Materialized path: SELECT without LIMIT (processes all rows)
        group.bench_with_input(
            BenchmarkId::new("materialized_full_scan", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select("SELECT * FROM large_table;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: SELECT with WHERE and LIMIT (lazy filtering + early termination)
fn bench_where_with_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_with_limit");

    for row_count in [1000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_large_table(&mut db, row_count);

        group.throughput(Throughput::Elements(10));

        // Iterator path: Lazy filtering with early termination
        group.bench_with_input(
            BenchmarkId::new("iterator_where_limit", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt =
                        parse_select("SELECT * FROM large_table WHERE amount > 50 LIMIT 10;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );

        // Materialized path: Full filtering without LIMIT
        group.bench_with_input(
            BenchmarkId::new("materialized_where_no_limit", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select("SELECT * FROM large_table WHERE amount > 50;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: OFFSET + LIMIT (lazy skip vs materialized drain)
fn bench_offset_with_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_with_limit");

    for row_count in [1000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_large_table(&mut db, row_count);

        group.throughput(Throughput::Elements(10));

        // Iterator path: Lazy skip + take
        group.bench_with_input(
            BenchmarkId::new("iterator_offset_limit", row_count),
            &row_count,
            |b, &count| {
                b.iter(|| {
                    let stmt = parse_select(&format!(
                        "SELECT * FROM large_table OFFSET {} LIMIT 10;",
                        count / 2
                    ));
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );

        // For comparison: Full scan to show the overhead we're avoiding
        group.bench_with_input(
            BenchmarkId::new("materialized_full_scan", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select("SELECT * FROM large_table;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Complex projection with LIMIT (tests that projection cost is amortized)
fn bench_projection_with_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection_with_limit");

    for row_count in [1000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_large_table(&mut db, row_count);

        group.throughput(Throughput::Elements(10));

        // Iterator path: Complex expressions with LIMIT
        group.bench_with_input(
            BenchmarkId::new("iterator_projection_limit", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select(
                        "SELECT id * 2, amount + 100, value FROM large_table LIMIT 10;",
                    );
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );

        // Materialized path: Same projection without LIMIT
        group.bench_with_input(
            BenchmarkId::new("materialized_projection_no_limit", row_count),
            &row_count,
            |b, _| {
                b.iter(|| {
                    let stmt = parse_select("SELECT id * 2, amount + 100, value FROM large_table;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Queries that must use materialized path (ORDER BY, DISTINCT)
fn bench_materialized_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialized_queries");

    for row_count in [1000, 10_000] {
        let mut db = Database::new();
        setup_large_table(&mut db, row_count);

        group.throughput(Throughput::Elements(row_count as u64));

        // ORDER BY requires materialization (cannot use iterator path)
        group.bench_with_input(BenchmarkId::new("order_by", row_count), &row_count, |b, _| {
            b.iter(|| {
                let stmt = parse_select("SELECT * FROM large_table ORDER BY amount;");
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });

        // DISTINCT requires materialization (cannot use iterator path)
        group.bench_with_input(BenchmarkId::new("distinct", row_count), &row_count, |b, _| {
            b.iter(|| {
                let stmt = parse_select("SELECT DISTINCT amount FROM large_table;");
                let executor = SelectExecutor::new(&db);
                black_box(executor.execute(&stmt).unwrap())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_limit_early_termination,
    bench_where_with_limit,
    bench_offset_with_limit,
    bench_projection_with_limit,
    bench_materialized_queries,
);
criterion_main!(benches);
