//! Benchmarks for parallel hash join build phase
//!
//! These benchmarks measure the performance improvements from parallelizing
//! the hash table build phase in hash joins (PR #1580, Phase 1.3 of parallelism roadmap).
//!
//! Expected performance characteristics (from PARALLELISM_ROADMAP.md):
//! - 4-6x speedup on large equi-joins with 4+ cores
//! - Threshold: parallelization beneficial for 50k+ rows
//! - Linear scaling up to 4 cores, diminishing returns beyond 8 cores
//!
//! Migrated from Criterion to custom harness for deterministic timing.
//!
//! Usage:
//!   cargo bench --bench hash_join_parallel
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
    // Create left table (customers) - uppercase for SQL parser normalization
    let left_schema = vibesql_catalog::TableSchema::new(
        "CUSTOMERS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "NAME".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(left_schema).unwrap();

    // Create right table (orders) - uppercase for SQL parser normalization
    let right_schema = vibesql_catalog::TableSchema::new(
        "ORDERS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema {
                name: "ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "CUSTOMER_ID".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: false,
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
    db.create_table(right_schema).unwrap();

    // Insert into left table (customers)
    for i in 0..left_rows {
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(format!("customer_{}", i))),
        ]);
        db.insert_row("CUSTOMERS", row).unwrap();
    }

    // Insert into right table (orders) - each customer has multiple orders
    for i in 0..right_rows {
        let customer_id = (i % left_rows) as i64; // Distribute orders across customers
        let row = vibesql_storage::Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(customer_id),
            SqlValue::Integer((i % 1000) as i64),
        ]);
        db.insert_row("ORDERS", row).unwrap();
    }
}

const JOIN_SQL: &str = "SELECT c.name, o.amount
     FROM customers c
     JOIN orders o ON c.id = o.customer_id;";

/// Benchmark: Hash join scaling with different data sizes
///
/// This measures the speedup from parallel hash table building
/// as the dataset size increases.
fn bench_hash_join_scaling(harness: &Harness) {
    print_group_header("Hash Join Scaling");

    // Test different dataset sizes to see where parallelization kicks in
    for size in [1_000, 10_000, 50_000, 100_000] {
        let mut db = Database::new();
        setup_join_tables(&mut db, size / 10, size); // 10:1 orders to customers ratio

        let name = format!("equi_join/{}_rows", size);
        let stats = harness.run(&name, || {
            let start = Instant::now();
            let stmt = parse_select(JOIN_SQL);
            let executor = SelectExecutor::new(&db);
            match executor.execute(&stmt) {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        });
        stats.print();
    }
}

/// Benchmark: Hash join with different join ratios
///
/// This tests how the parallel build performs with different
/// cardinalities (1:1, 1:many, many:many).
fn bench_hash_join_cardinality(harness: &Harness) {
    print_group_header("Hash Join Cardinality");

    let base_size = 50_000;

    // 1:1 join (each customer has exactly 1 order)
    {
        let mut db = Database::new();
        setup_join_tables(&mut db, base_size, base_size);

        let stats = harness.run("one_to_one", || {
            let start = Instant::now();
            let stmt = parse_select(JOIN_SQL);
            let executor = SelectExecutor::new(&db);
            match executor.execute(&stmt) {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        });
        stats.print();
    }

    // 1:10 join (each customer has 10 orders)
    {
        let mut db = Database::new();
        setup_join_tables(&mut db, base_size / 10, base_size);

        let stats = harness.run("one_to_many", || {
            let start = Instant::now();
            let stmt = parse_select(JOIN_SQL);
            let executor = SelectExecutor::new(&db);
            match executor.execute(&stmt) {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        });
        stats.print();
    }
}

/// Benchmark: Hash join build phase in isolation
///
/// This benchmark focuses specifically on the hash table build phase
/// by using a minimal probe phase (small right table).
fn bench_hash_build_phase(harness: &Harness) {
    print_group_header("Hash Build Phase");

    for build_size in [10_000, 50_000, 100_000] {
        let mut db = Database::new();
        // Large left table (will be used for building hash table)
        // Small right table (minimal probe cost)
        setup_join_tables(&mut db, build_size, 100);

        let name = format!("build_phase/{}_rows", build_size);
        let stats = harness.run(&name, || {
            let start = Instant::now();
            let stmt = parse_select(JOIN_SQL);
            let executor = SelectExecutor::new(&db);
            match executor.execute(&stmt) {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        });
        stats.print();
    }
}

/// Benchmark: Compare sequential threshold behavior
///
/// This tests the automatic fallback to sequential execution
/// for small datasets.
fn bench_threshold_behavior(harness: &Harness) {
    print_group_header("Threshold Behavior");

    // Test sizes around the parallelization threshold
    for size in [1_000, 5_000, 10_000, 20_000, 50_000] {
        let mut db = Database::new();
        setup_join_tables(&mut db, size / 10, size);

        let name = format!("auto_threshold/{}_rows", size);
        let stats = harness.run(&name, || {
            let start = Instant::now();
            let stmt = parse_select(JOIN_SQL);
            let executor = SelectExecutor::new(&db);
            match executor.execute(&stmt) {
                Ok(rows) => {
                    black_box(rows);
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        });
        stats.print();
    }
}

fn main() {
    eprintln!("\n=== Hash Join Parallel Benchmarks ===\n");

    let harness = Harness::new();

    bench_hash_join_scaling(&harness);
    bench_hash_join_cardinality(&harness);
    bench_hash_build_phase(&harness);
    bench_threshold_behavior(&harness);

    eprintln!("\n=== Benchmark Complete ===\n");
}
