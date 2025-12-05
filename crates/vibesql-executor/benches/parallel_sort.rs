//! Benchmarks for parallel ORDER BY sorting performance
//!
//! These benchmarks validate the 2-3x speedup claims for parallel sorting
//! implemented in PR #1594, as part of Phase 1.5 of the PARALLELISM_ROADMAP.md

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

/// Setup: Create a test table with various data types for sorting benchmarks
fn setup_sort_table(db: &mut Database, row_count: usize, include_nulls: bool) {
    // Create table (uppercase for SQL parser normalization)
    let schema = vibesql_catalog::TableSchema::new(
        "SORT_TEST".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "CATEGORY".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "VALUE".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "NAME".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with varied data
    for i in 0..row_count {
        let category_val = if include_nulls && i % 10 == 0 {
            vibesql_types::SqlValue::Null
        } else {
            vibesql_types::SqlValue::Varchar(format!("cat_{}", i % 20))
        };

        let value_val = if include_nulls && i % 13 == 0 {
            vibesql_types::SqlValue::Null
        } else {
            vibesql_types::SqlValue::Integer((row_count - i) as i64) // Reverse sorted
        };

        let row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(i as i64),
            category_val,
            value_val,
            vibesql_types::SqlValue::Varchar(format!("name_{:06}", i)),
        ]);
        db.insert_row("SORT_TEST", row).unwrap();
    }
}

/// Benchmark: Simple integer sort (best case for parallelization)
fn bench_simple_integer_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_integer_sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        group.throughput(Throughput::Elements(row_count as u64));

        // Benchmark with different core counts
        for cores in [1, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    // Create a new thread pool for each benchmark
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt = parse_select("SELECT * FROM sort_test ORDER BY id;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: Multi-column sort (more complex comparisons)
fn bench_multi_column_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_column_sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        group.throughput(Throughput::Elements(row_count as u64));

        for cores in [1, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt =
                                parse_select("SELECT * FROM sort_test ORDER BY category, id DESC;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: String sort (expensive comparisons)
fn bench_string_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        group.throughput(Throughput::Elements(row_count as u64));

        for cores in [1, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt = parse_select("SELECT * FROM sort_test ORDER BY name;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: Sort with NULLs (worst case for comparisons)
fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, true); // ~10% NULL values

        group.throughput(Throughput::Elements(row_count as u64));

        for cores in [1, 2, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt = parse_select("SELECT * FROM sort_test ORDER BY value;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: Threshold boundary testing
/// Tests performance at the threshold boundary to validate threshold values
fn bench_threshold_boundary(c: &mut Criterion) {
    let mut group = c.benchmark_group("threshold_boundary");

    // Test around the 8-core threshold of 5,000 rows (from parallel.rs:116-121)
    for row_count in [4_500, 5_000, 5_500] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        group.throughput(Throughput::Elements(row_count as u64));

        // Test with 8 cores (aggressive threshold: 5,000)
        let cores = 8;
        group.bench_with_input(BenchmarkId::new("8_cores", row_count), &row_count, |b, &_| {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

            pool.install(|| {
                b.iter(|| {
                    let stmt = parse_select("SELECT * FROM sort_test ORDER BY id;");
                    let executor = SelectExecutor::new(&db);
                    black_box(executor.execute(&stmt).unwrap())
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Already sorted data (best case for sort algorithm)
fn bench_presorted_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("presorted_data");

    for row_count in [10_000, 100_000] {
        let mut db = Database::new();

        // Create table with already sorted data
        let schema = vibesql_catalog::TableSchema::new(
            "PRESORTED_TEST".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            }],
        );
        db.create_table(schema).unwrap();

        for i in 0..row_count {
            let row = vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(i as i64), // Already sorted
            ]);
            db.insert_row("PRESORTED_TEST", row).unwrap();
        }

        group.throughput(Throughput::Elements(row_count as u64));

        for cores in [1, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt = parse_select("SELECT * FROM presorted_test ORDER BY id;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark: Reverse sorted data (worst case for some sort algorithms)
fn bench_reverse_sorted_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("reverse_sorted_data");

    for row_count in [10_000, 100_000] {
        let mut db = Database::new();

        let schema = vibesql_catalog::TableSchema::new(
            "REVERSE_TEST".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            }],
        );
        db.create_table(schema).unwrap();

        for i in 0..row_count {
            let row = vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer((row_count - i) as i64), // Reverse sorted
            ]);
            db.insert_row("REVERSE_TEST", row).unwrap();
        }

        group.throughput(Throughput::Elements(row_count as u64));

        for cores in [1, 4, 8] {
            group.bench_with_input(
                BenchmarkId::new(format!("{}_cores", cores), row_count),
                &(row_count, cores),
                |b, &(_, cores)| {
                    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

                    pool.install(|| {
                        b.iter(|| {
                            let stmt = parse_select("SELECT * FROM reverse_test ORDER BY id;");
                            let executor = SelectExecutor::new(&db);
                            black_box(executor.execute(&stmt).unwrap())
                        });
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_integer_sort,
    bench_multi_column_sort,
    bench_string_sort,
    bench_sort_with_nulls,
    bench_threshold_boundary,
    bench_presorted_data,
    bench_reverse_sorted_data
);
criterion_main!(benches);
