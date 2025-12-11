//! Benchmarks for parallel filter (WHERE clause) operation
//!
//! These benchmarks isolate the filter operation to measure the effect of
//! morsel size on parallel filtering with different selectivities.
//!
//! ## Benchmark Types
//!
//! ### Direct API Benchmark (Row-Oriented)
//! - `BENCHMARK_FILTER=morsel_filter` - Tests `morsel_parallel_filter` directly
//! - Bypasses SQL parsing and columnar execution
//! - Shows true morsel work-stealing performance (~1.8-3.5x speedup)
//!
//! ### SQL-Based Benchmark (Columnar by Default)
//! - `BENCHMARK_FILTER=sql_filter` - Tests via SQL queries
//! - Routes through columnar SIMD execution engine (~100x faster than row-based)
//! - Different parallelism characteristics than morsel-driven execution
//!
//! **Note**: SQL filter benchmarks measure columnar SIMD performance, not morsel
//! work-stealing. Use `morsel_filter` to test the direct row-oriented API.
//!
//! ## Usage
//!
//! ```bash
//! # Build the benchmark
//! cargo bench --package vibesql-executor --bench filter_parallel --no-run
//!
//! # Run all benchmarks
//! ./target/release/deps/filter_parallel-*
//!
//! # Run only direct API benchmark (tests morsel parallelism)
//! BENCHMARK_FILTER=morsel_filter ./target/release/deps/filter_parallel-*
//!
//! # Run only SQL-based benchmark (tests columnar execution)
//! BENCHMARK_FILTER=sql_filter ./target/release/deps/filter_parallel-*
//!
//! # Test specific morsel sizes
//! MORSEL_SIZES=2048,8192,50000 ./target/release/deps/filter_parallel-*
//!
//! # Vary thread counts
//! MAX_THREADS=8 ./target/release/deps/filter_parallel-*
//! ```
//!
//! ## Environment Variables
//!
//! - `BENCHMARK_FILTER` - Run specific benchmark (morsel_filter, sql_filter, or omit for all)
//! - `MORSEL_SIZES` - Comma-separated list of morsel sizes to test
//! - `ROW_COUNTS` - Comma-separated list of row counts to test
//! - `MAX_THREADS` - Maximum thread count to test (default: 16)
//! - `WARMUP_ITERATIONS` - Number of warmup runs (default: 2)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs (default: 5)
//! - `MORSEL_DEBUG` - Enable detailed morsel execution logging

mod harness;

use std::{
    env,
    hint::black_box,
    time::Instant,
};

use harness::{print_group_header, BenchConfig, BenchResult, Harness};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::select::morsel::{morsel_parallel_filter, MorselConfig};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Type alias for predicate functions used in filter benchmarks
type PredicateFn = Box<dyn Fn(&Row) -> bool + Send + Sync>;

// =============================================================================
// Configuration
// =============================================================================

const DEFAULT_MORSEL_SIZES: &[usize] = &[1024, 2048, 4096, 8192, 16384, 32768, 50000];
const DEFAULT_ROW_COUNTS: &[usize] = &[100_000, 500_000, 1_000_000];
const DEFAULT_THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

fn get_morsel_sizes() -> Vec<usize> {
    env::var("MORSEL_SIZES")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .unwrap_or_else(|| DEFAULT_MORSEL_SIZES.to_vec())
}

fn get_row_counts() -> Vec<usize> {
    env::var("ROW_COUNTS")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .unwrap_or_else(|| DEFAULT_ROW_COUNTS.to_vec())
}

fn get_thread_counts() -> Vec<usize> {
    let max_threads: usize =
        env::var("MAX_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(16);
    DEFAULT_THREAD_COUNTS.iter().copied().filter(|&t| t <= max_threads).collect()
}

fn get_benchmark_filter() -> Option<String> {
    env::var("BENCHMARK_FILTER").ok()
}

// =============================================================================
// Data Generator
// =============================================================================

/// Create a database for filter benchmarks
///
/// Table: FILTER_DATA (id INTEGER, category INTEGER, value BIGINT, flag INTEGER)
///
/// Characteristics:
/// - category: 0-99 (uniform distribution for varied selectivity)
/// - value: random-ish values for range queries
/// - flag: 0 or 1 (50% selectivity)
fn create_filter_database(row_count: usize) -> Database {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "FILTER_DATA".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "CATEGORY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "VALUE".to_string(),
                data_type: DataType::Bigint,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "FLAG".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    for i in 0..row_count {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer((i % 100) as i64),                  // 100 categories
            SqlValue::Bigint(((i * 17 + 42) % 1_000_000) as i64), // pseudo-random
            SqlValue::Integer((i % 2) as i64),                    // 50% selectivity
        ]);
        db.insert_row("FILTER_DATA", row).unwrap();
    }

    db
}

// =============================================================================
// Query Execution Helper
// =============================================================================

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

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

// =============================================================================
// Helper Functions
// =============================================================================

fn print_morsel_size_header(morsel_sizes: &[usize]) {
    eprint!("      ");
    for &size in morsel_sizes {
        if size >= 1000 {
            eprint!(" {:>7}K", size / 1000);
        } else {
            eprint!(" {:>8}", size);
        }
    }
    eprintln!();
    eprint!("      ");
    for _ in morsel_sizes {
        eprint!(" --------");
    }
    eprintln!();
}

// =============================================================================
// Benchmark
// =============================================================================

/// Benchmark filter operation with different selectivities
fn bench_filter_operation(harness: &Harness) {
    print_group_header("Filter Operation (WHERE clause)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Queries with different selectivities
    let queries = [
        ("filter_50pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE FLAG = 1"), // 50%
        ("filter_10pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY < 10"), // 10%
        ("filter_1pct", "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY = 0"), // 1%
        (
            "filter_compound",
            "SELECT COUNT(*) FROM FILTER_DATA WHERE CATEGORY < 50 AND FLAG = 1",
        ), // ~25%
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);
        let db = create_filter_database(row_count);

        for (query_name, query_sql) in &queries {
            eprintln!("\n  Query: {}", query_name);

            // Test morsel size sensitivity
            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    env::set_var("MORSEL_SIZE", morsel_size.to_string());

                    let stats = pool.install(|| {
                        let name = format!("{}_{}t_{}m", query_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query_sql))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

/// Benchmark morsel_parallel_filter directly (bypasses SQL/columnar execution)
///
/// This tests the raw row-oriented morsel work-stealing performance without
/// going through the SQL parser or columnar execution engine.
fn bench_morsel_filter_direct(harness: &Harness) {
    print_group_header("Morsel Filter (Direct API - Row-Oriented)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        // Generate rows directly (no database)
        let rows: Vec<Row> = (0..row_count)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Integer((i % 100) as i64),                  // category
                    SqlValue::Bigint(((i * 17 + 42) % 1_000_000) as i64), // value
                    SqlValue::Integer((i % 2) as i64),                    // flag
                ])
            })
            .collect();

        // Different predicates with varying selectivities
        let predicates: Vec<(&str, PredicateFn)> = vec![
            (
                "50pct_flag",
                Box::new(|row: &Row| matches!(row.values[3], SqlValue::Integer(1))),
            ),
            (
                "10pct_category",
                Box::new(|row: &Row| matches!(row.values[1], SqlValue::Integer(c) if c < 10)),
            ),
            (
                "1pct_category",
                Box::new(|row: &Row| matches!(row.values[1], SqlValue::Integer(0))),
            ),
            (
                "25pct_compound",
                Box::new(|row: &Row| {
                    matches!(row.values[1], SqlValue::Integer(c) if c < 50)
                        && matches!(row.values[3], SqlValue::Integer(1))
                }),
            ),
        ];

        for (pred_name, predicate) in &predicates {
            eprintln!("\n  Predicate: {}", pred_name);
            print_morsel_size_header(&morsel_sizes);

            for &threads in &thread_counts {
                eprint!("  {:>2}T ", threads);

                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .expect("Failed to create thread pool");

                for &morsel_size in &morsel_sizes {
                    let config = MorselConfig {
                        morsel_size,
                        ..MorselConfig::default()
                    };

                    let stats = pool.install(|| {
                        let name = format!("morsel_{}_{}t_{}m", pred_name, threads, morsel_size);
                        harness.run(&name, || {
                            let start = Instant::now();
                            let result = morsel_parallel_filter(&rows, &config, |row| predicate(row));
                            black_box(result);
                            BenchResult::Ok(start.elapsed())
                        })
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n========================================");
    eprintln!("  Filter Parallel Benchmark");
    eprintln!("========================================\n");

    // Configuration
    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();
    let benchmark_filter = get_benchmark_filter();

    eprintln!("Configuration:");
    eprintln!("  Row Counts: {:?}", row_counts);
    eprintln!("  Morsel Sizes: {:?}", morsel_sizes);
    eprintln!("  Thread Counts: {:?}", thread_counts);
    eprintln!("  BENCHMARK_FILTER: {:?}", benchmark_filter);
    eprintln!("  MORSEL_DEBUG: {}", env::var("MORSEL_DEBUG").unwrap_or_default());
    eprintln!();

    // Create harness
    let config = BenchConfig::new(
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(2),
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(5),
        120, // 2 minute timeout
    );
    let harness = Harness::with_config(config);

    // Run selected benchmarks based on BENCHMARK_FILTER
    match benchmark_filter.as_deref() {
        Some("morsel_filter") => {
            bench_morsel_filter_direct(&harness);
        }
        Some("sql_filter") => {
            bench_filter_operation(&harness);
        }
        _ => {
            // Run all benchmarks
            bench_morsel_filter_direct(&harness);
            bench_filter_operation(&harness);
        }
    }

    eprintln!("\n========================================");
    eprintln!("  Benchmark Complete");
    eprintln!("========================================\n");
}
