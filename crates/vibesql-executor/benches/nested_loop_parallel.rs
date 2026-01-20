//! Benchmarks for parallel nested loop join execution
//!
//! These benchmarks measure the performance improvements from morsel-driven
//! parallelism in nested loop joins (PR #4280).
//!
//! Nested loop joins are used when:
//! - No equi-join condition exists (e.g., range predicates like a.x < b.y)
//! - Hash join is not applicable
//! - Cross joins are requested
//!
//! Expected performance characteristics:
//! - 2-4x speedup on large joins with 4+ cores
//! - Better utilization on non-equi join predicates
//! - Linear scaling up to available cores for CPU-bound joins
//!
//! Usage:
//!   cargo bench --bench nested_loop_parallel
//!
//! Environment variables:
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)
//!   MAX_THREADS - Maximum thread count to test (default: 8)
//!   BENCHMARK_FILTER - Run specific benchmark (data_size, thread_scaling, predicates)

mod harness;

use std::{env, hint::black_box, time::Instant};

use harness::{print_group_header, BenchConfig, BenchResult, BenchStats, Harness};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Parse a SELECT statement from SQL
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement"),
    }
}

/// Run a query and return execution time
fn run_query(db: &Database, sql: &str) -> BenchResult {
    let stmt = parse_select(sql);
    let executor = SelectExecutor::new(db);
    let start = Instant::now();
    match executor.execute(&stmt) {
        Ok(rows) => {
            black_box(rows);
            BenchResult::Ok(start.elapsed())
        }
        Err(e) => BenchResult::Error(e.to_string()),
    }
}

/// Get max threads to test from environment
fn get_max_threads() -> usize {
    env::var("MAX_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(8)
}

/// Get benchmark filter from environment
fn get_benchmark_filter() -> Option<String> {
    env::var("BENCHMARK_FILTER").ok().map(|s| s.to_lowercase())
}

// =============================================================================
// Test Data Setup
// =============================================================================

/// Create tables for nested loop join benchmarks
///
/// Creates two tables:
/// - PRODUCTS: id, price, category
/// - PROMOTIONS: id, min_price, max_price, discount
///
/// The join predicate `price BETWEEN min_price AND max_price` forces
/// a nested loop join (cannot use hash join for range predicates).
fn setup_range_join_tables(db: &mut Database, products: usize, promotions: usize) {
    // Create PRODUCTS table
    let products_schema = TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "PRICE".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "CATEGORY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ],
    );
    db.create_table(products_schema).unwrap();

    // Create PROMOTIONS table
    let promotions_schema = TableSchema::new(
        "PROMOTIONS".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "MIN_PRICE".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "MAX_PRICE".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "DISCOUNT".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ],
    );
    db.create_table(promotions_schema).unwrap();

    // Insert products with prices 0-999
    for i in 0..products {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer((i % 1000) as i64), // price 0-999
            SqlValue::Integer((i % 10) as i64),   // category 0-9
        ]);
        db.insert_row("PRODUCTS", row).unwrap();
    }

    // Insert promotions with overlapping price ranges
    for i in 0..promotions {
        let min_price = (i * 100 % 800) as i64; // Range starts: 0, 100, 200, ...
        let max_price = min_price + 200; // 200-wide bands
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(min_price),
            SqlValue::Integer(max_price),
            SqlValue::Integer((5 + i % 20) as i64), // discount 5-24%
        ]);
        db.insert_row("PROMOTIONS", row).unwrap();
    }
}

/// Create tables for cross join benchmarks
fn setup_cross_join_tables(db: &mut Database, left_rows: usize, right_rows: usize) {
    // Create LEFT_TABLE
    let left_schema = TableSchema::new(
        "LEFT_TABLE".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "VALUE".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ],
    );
    db.create_table(left_schema).unwrap();

    // Create RIGHT_TABLE
    let right_schema = TableSchema::new(
        "RIGHT_TABLE".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "FACTOR".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ],
    );
    db.create_table(right_schema).unwrap();

    // Insert data
    for i in 0..left_rows {
        let row =
            Row::new(vec![SqlValue::Integer(i as i64), SqlValue::Integer((i * 7 + 13) as i64)]);
        db.insert_row("LEFT_TABLE", row).unwrap();
    }

    for i in 0..right_rows {
        let row =
            Row::new(vec![SqlValue::Integer(i as i64), SqlValue::Integer((i * 3 + 5) as i64)]);
        db.insert_row("RIGHT_TABLE", row).unwrap();
    }
}

// =============================================================================
// Data Size Scaling Benchmark
// =============================================================================

/// Range join query: price BETWEEN min_price AND max_price
const RANGE_JOIN_SQL: &str = "SELECT p.id, p.price, pr.discount
     FROM products p
     JOIN promotions pr ON p.price BETWEEN pr.min_price AND pr.max_price";

/// Benchmark: Nested loop join scaling with different data sizes
///
/// This measures performance as the outer relation size increases.
fn bench_data_size_scaling(harness: &Harness) {
    print_group_header("Nested Loop Join - Data Size Scaling");

    // Test different outer relation sizes with fixed inner relation
    // Inner (promotions) is small to focus on outer scanning overhead
    let inner_size = 50;

    for outer_size in [1_000, 5_000, 10_000, 50_000, 100_000] {
        let mut db = Database::new();
        setup_range_join_tables(&mut db, outer_size, inner_size);

        let name = format!("range_join/{}x{}", outer_size, inner_size);
        let stats = harness.run(&name, || run_query(&db, RANGE_JOIN_SQL));
        stats.print();
    }
}

// =============================================================================
// Thread Scaling Benchmark
// =============================================================================

/// Benchmark thread scaling for nested loop joins
///
/// Measures:
/// - Execution time at each thread count
/// - Speedup relative to single-threaded (T1 / Tn)
/// - Efficiency (speedup / thread_count)
fn bench_thread_scaling(harness: &Harness) {
    print_group_header("Nested Loop Join - Thread Scaling");

    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> =
        [1, 2, 4, 8, 16].iter().copied().filter(|&t| t <= max_threads).collect();

    eprintln!("Testing thread counts: {:?}", thread_counts);
    eprintln!("(Set MAX_THREADS env var to adjust, default: 8)\n");

    // Use a moderately large dataset to see parallel benefits
    let outer_size = 50_000;
    let inner_size = 100;

    let mut db = Database::new();
    setup_range_join_tables(&mut db, outer_size, inner_size);

    eprintln!("Dataset: {} products x {} promotions\n", outer_size, inner_size);

    let mut baseline_time = None;
    let mut results: Vec<(usize, BenchStats)> = Vec::new();

    for &thread_count in &thread_counts {
        // Configure rayon thread pool for this run
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .expect("Failed to create thread pool");

        let stats = pool.install(|| {
            let name = format!("range_join_{}t", thread_count);
            harness.run(&name, || run_query(&db, RANGE_JOIN_SQL))
        });

        if thread_count == 1 {
            baseline_time = Some(stats.mean);
        }

        results.push((thread_count, stats));
    }

    // Print results table
    eprintln!("\n{:<8} {:>12} {:>10} {:>12}", "Threads", "Mean Time", "Speedup", "Efficiency");
    eprintln!("{:-<8} {:->12} {:->10} {:->12}", "", "", "", "");

    if let Some(t1) = baseline_time {
        for (thread_count, stats) in &results {
            let speedup = t1.as_secs_f64() / stats.mean.as_secs_f64();
            let efficiency = speedup / *thread_count as f64 * 100.0;

            eprintln!(
                "{:<8} {:>12.2?} {:>10.2}x {:>11.1}%",
                thread_count, stats.mean, speedup, efficiency
            );
        }
    }
}

// =============================================================================
// Predicate Complexity Benchmark
// =============================================================================

/// Benchmark different join predicate types
///
/// Tests various non-equi predicates that force nested loop joins:
/// - Range predicate (BETWEEN)
/// - Inequality (< or >)
/// - Complex compound predicates
fn bench_predicate_types(harness: &Harness) {
    print_group_header("Nested Loop Join - Predicate Types");

    let outer_size = 10_000;
    let inner_size = 100;

    let mut db = Database::new();
    setup_range_join_tables(&mut db, outer_size, inner_size);

    // Range predicate (BETWEEN)
    let range_sql = "SELECT COUNT(*) FROM products p
        JOIN promotions pr ON p.price BETWEEN pr.min_price AND pr.max_price";
    let stats = harness.run("range_between", || run_query(&db, range_sql));
    stats.print();

    // Less-than predicate
    let lt_sql = "SELECT COUNT(*) FROM products p
        JOIN promotions pr ON p.price < pr.max_price";
    let stats = harness.run("less_than", || run_query(&db, lt_sql));
    stats.print();

    // Greater-than predicate
    let gt_sql = "SELECT COUNT(*) FROM products p
        JOIN promotions pr ON p.price > pr.min_price";
    let stats = harness.run("greater_than", || run_query(&db, gt_sql));
    stats.print();

    // Compound predicate (AND)
    let compound_sql = "SELECT COUNT(*) FROM products p
        JOIN promotions pr ON p.price >= pr.min_price AND p.price <= pr.max_price AND p.category < 5";
    let stats = harness.run("compound_and", || run_query(&db, compound_sql));
    stats.print();
}

// =============================================================================
// Cross Join Benchmark
// =============================================================================

/// Benchmark cross join (Cartesian product) performance
///
/// Cross joins are a special case of nested loop joins with no predicate.
/// They produce M x N rows and are compute-intensive.
fn bench_cross_join(harness: &Harness) {
    print_group_header("Cross Join Benchmark");

    // Small tables to avoid excessive output (1000 x 100 = 100K rows)
    for (left, right) in [(100, 100), (500, 100), (1000, 100), (1000, 500)] {
        let mut db = Database::new();
        setup_cross_join_tables(&mut db, left, right);

        let sql = "SELECT COUNT(*) FROM left_table CROSS JOIN right_table";
        let name = format!("cross_join/{}x{}", left, right);
        let stats = harness.run(&name, || run_query(&db, sql));
        stats.print();
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n=== Nested Loop Join Parallel Benchmarks ===\n");

    let warmup_iterations =
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(3);
    let benchmark_iterations =
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);
    let config = BenchConfig::new(warmup_iterations, benchmark_iterations, 60);
    let harness = Harness::with_config(config);

    let filter = get_benchmark_filter();

    eprintln!("Configuration:");
    eprintln!("  Max Threads: {}", get_max_threads());
    eprintln!("  Warmup: {} iterations", warmup_iterations);
    eprintln!("  Benchmark: {} iterations", benchmark_iterations);
    eprintln!("  Filter: {:?}", filter);
    eprintln!();

    // Run benchmarks based on filter
    match filter.as_deref() {
        Some("data_size") | Some("size") | Some("scaling") => {
            bench_data_size_scaling(&harness);
        }
        Some("thread_scaling") | Some("thread") | Some("threads") => {
            bench_thread_scaling(&harness);
        }
        Some("predicates") | Some("predicate") => {
            bench_predicate_types(&harness);
        }
        Some("cross_join") | Some("cross") => {
            bench_cross_join(&harness);
        }
        None => {
            // Run all benchmarks
            bench_data_size_scaling(&harness);
            bench_thread_scaling(&harness);
            bench_predicate_types(&harness);
            bench_cross_join(&harness);
        }
        Some(unknown) => {
            eprintln!("Unknown benchmark filter: {}", unknown);
            eprintln!("Valid filters: data_size, thread_scaling, predicates, cross_join");
            std::process::exit(1);
        }
    }

    eprintln!("\n=== Benchmark Complete ===\n");
}
