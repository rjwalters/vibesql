//! Benchmarks for parallel ORDER BY sorting performance
//!
//! These benchmarks validate the 2-3x speedup claims for parallel sorting
//! implemented in PR #1594, as part of Phase 1.5 of the PARALLELISM_ROADMAP.md
//!
//! Migrated from Criterion to custom harness for deterministic timing.
//!
//! Usage:
//!   cargo bench --bench parallel_sort
//!
//! Environment variables:
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)

mod harness;

use harness::{print_group_header, BenchResult, Harness};
use std::hint::black_box;
use std::time::Instant;
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
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("cat_{}", i % 20)))
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
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("name_{:06}", i))),
        ]);
        db.insert_row("SORT_TEST", row).unwrap();
    }
}

/// Setup: Create a table with already sorted data
fn setup_presorted_table(db: &mut Database, row_count: usize, table_name: &str) {
    let schema = vibesql_catalog::TableSchema::new(
        table_name.to_string(),
        vec![vibesql_catalog::ColumnSchema {
            name: "ID".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            default_value: None,
        }],
    );
    db.create_table(schema).unwrap();

    for i in 0..row_count {
        let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(i as i64)]);
        db.insert_row(table_name, row).unwrap();
    }
}

/// Setup: Create a table with reverse sorted data
fn setup_reverse_sorted_table(db: &mut Database, row_count: usize, table_name: &str) {
    let schema = vibesql_catalog::TableSchema::new(
        table_name.to_string(),
        vec![vibesql_catalog::ColumnSchema {
            name: "ID".to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: false,
            default_value: None,
        }],
    );
    db.create_table(schema).unwrap();

    for i in 0..row_count {
        let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(
            (row_count - i) as i64,
        )]);
        db.insert_row(table_name, row).unwrap();
    }
}

/// Run a benchmark with a specific thread pool configuration
fn run_benchmark_with_cores(
    harness: &Harness,
    name: &str,
    db: &Database,
    sql: &str,
    cores: usize,
) -> harness::BenchStats {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(cores).build().unwrap();

    pool.install(|| {
        harness.run(name, || {
            let start = Instant::now();
            let stmt = parse_select(sql);
            let executor = SelectExecutor::new(db);
            let result = executor.execute(&stmt);
            match result {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        })
    })
}

/// Benchmark: Simple integer sort (best case for parallelization)
fn bench_simple_integer_sort(harness: &Harness) {
    print_group_header("Simple Integer Sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 2, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats =
                run_benchmark_with_cores(harness, &name, &db, "SELECT * FROM sort_test ORDER BY id;", cores);
            stats.print_compact();
        }
    }
}

/// Benchmark: Multi-column sort (more complex comparisons)
fn bench_multi_column_sort(harness: &Harness) {
    print_group_header("Multi-Column Sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 2, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats = run_benchmark_with_cores(
                harness,
                &name,
                &db,
                "SELECT * FROM sort_test ORDER BY category, id DESC;",
                cores,
            );
            stats.print_compact();
        }
    }
}

/// Benchmark: String sort (expensive comparisons)
fn bench_string_sort(harness: &Harness) {
    print_group_header("String Sort");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 2, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats = run_benchmark_with_cores(
                harness,
                &name,
                &db,
                "SELECT * FROM sort_test ORDER BY name;",
                cores,
            );
            stats.print_compact();
        }
    }
}

/// Benchmark: Sort with NULLs (worst case for comparisons)
fn bench_sort_with_nulls(harness: &Harness) {
    print_group_header("Sort with NULLs");

    for row_count in [1_000, 10_000, 100_000] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, true); // ~10% NULL values

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 2, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats = run_benchmark_with_cores(
                harness,
                &name,
                &db,
                "SELECT * FROM sort_test ORDER BY value;",
                cores,
            );
            stats.print_compact();
        }
    }
}

/// Benchmark: Threshold boundary testing
/// Tests performance at the threshold boundary to validate threshold values
fn bench_threshold_boundary(harness: &Harness) {
    print_group_header("Threshold Boundary (8 cores)");

    // Test around the 8-core threshold of 5,000 rows (from parallel.rs:116-121)
    for row_count in [4_500, 5_000, 5_500] {
        let mut db = Database::new();
        setup_sort_table(&mut db, row_count, false);

        let cores = 8;
        let name = format!("8_cores/{}_rows", row_count);
        let stats =
            run_benchmark_with_cores(harness, &name, &db, "SELECT * FROM sort_test ORDER BY id;", cores);
        stats.print();
    }
}

/// Benchmark: Already sorted data (best case for sort algorithm)
fn bench_presorted_data(harness: &Harness) {
    print_group_header("Pre-sorted Data");

    for row_count in [10_000, 100_000] {
        let mut db = Database::new();
        setup_presorted_table(&mut db, row_count, "PRESORTED_TEST");

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats = run_benchmark_with_cores(
                harness,
                &name,
                &db,
                "SELECT * FROM presorted_test ORDER BY id;",
                cores,
            );
            stats.print_compact();
        }
    }
}

/// Benchmark: Reverse sorted data (worst case for some sort algorithms)
fn bench_reverse_sorted_data(harness: &Harness) {
    print_group_header("Reverse-sorted Data");

    for row_count in [10_000, 100_000] {
        let mut db = Database::new();
        setup_reverse_sorted_table(&mut db, row_count, "REVERSE_TEST");

        eprintln!("\n  Row count: {}", row_count);

        for cores in [1, 4, 8] {
            let name = format!("{}_cores/{}_rows", cores, row_count);
            let stats = run_benchmark_with_cores(
                harness,
                &name,
                &db,
                "SELECT * FROM reverse_test ORDER BY id;",
                cores,
            );
            stats.print_compact();
        }
    }
}

fn main() {
    eprintln!("\n=== Parallel Sort Benchmarks ===\n");

    let harness = Harness::new();

    bench_simple_integer_sort(&harness);
    bench_multi_column_sort(&harness);
    bench_string_sort(&harness);
    bench_sort_with_nulls(&harness);
    bench_threshold_boundary(&harness);
    bench_presorted_data(&harness);
    bench_reverse_sorted_data(&harness);

    eprintln!("\n=== Benchmark Complete ===\n");
}
