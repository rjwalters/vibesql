//! Benchmarks for parallel table scan (materialization) operation
//!
//! These benchmarks isolate the scan operation to measure the effect of
//! morsel size on parallel table materialization.
//!
//! ## Usage
//!
//! ```bash
//! # Build the benchmark
//! cargo bench --package vibesql-executor --bench scan_parallel --no-run
//!
//! # Run the benchmark
//! ./target/release/deps/scan_parallel-*
//!
//! # Test specific morsel sizes
//! MORSEL_SIZES=2048,8192,50000 ./target/release/deps/scan_parallel-*
//!
//! # Vary thread counts
//! MAX_THREADS=8 ./target/release/deps/scan_parallel-*
//! ```
//!
//! ## Environment Variables
//!
//! - `MORSEL_SIZES` - Comma-separated list of morsel sizes to test
//! - `ROW_COUNTS` - Comma-separated list of row counts to test
//! - `MAX_THREADS` - Maximum thread count to test (default: 16)
//! - `WARMUP_ITERATIONS` - Number of warmup runs (default: 2)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs (default: 5)
//! - `MORSEL_DEBUG` - Enable detailed morsel execution logging

mod harness;

use std::{env, hint::black_box, time::Instant};

use harness::{print_group_header, BenchConfig, BenchResult, Harness};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

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

// =============================================================================
// Data Generator
// =============================================================================

/// Create a database for scan benchmarks
///
/// Table: SCAN_DATA (id INTEGER, category INTEGER, value BIGINT, flag INTEGER)
///
/// Reuses the same schema as filter benchmarks for consistency.
fn create_scan_database(row_count: usize) -> Database {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "SCAN_DATA".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
            collation: None,
            },
            ColumnSchema {
                name: "CATEGORY".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
            collation: None,
            },
            ColumnSchema {
                name: "VALUE".to_string(),
                data_type: DataType::Bigint,
                nullable: false,
                default_value: None,
                generated_expr: None,
            collation: None,
            },
            ColumnSchema {
                name: "FLAG".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
            collation: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    for i in 0..row_count {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer((i % 100) as i64),
            SqlValue::Bigint(((i * 17 + 42) % 1_000_000) as i64),
            SqlValue::Integer((i % 2) as i64),
        ]);
        db.insert_row("SCAN_DATA", row).unwrap();
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

/// Benchmark scan operation (table materialization)
fn bench_scan_operation(harness: &Harness) {
    print_group_header("Scan Operation (Table Materialization)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    for &row_count in &row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        let db = create_scan_database(row_count);

        // Simple scan with projection
        let query = "SELECT ID, CATEGORY, VALUE FROM SCAN_DATA";

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
                    let name = format!("scan_{}t_{}m", threads, morsel_size);
                    harness.run(&name, || run_query(&db, query))
                });

                eprint!(" {:>8.2?}", stats.mean);
            }
            eprintln!();
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n========================================");
    eprintln!("  Scan Parallel Benchmark");
    eprintln!("========================================\n");

    // Configuration
    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    eprintln!("Configuration:");
    eprintln!("  Row Counts: {:?}", row_counts);
    eprintln!("  Morsel Sizes: {:?}", morsel_sizes);
    eprintln!("  Thread Counts: {:?}", thread_counts);
    eprintln!("  MORSEL_DEBUG: {}", env::var("MORSEL_DEBUG").unwrap_or_default());
    eprintln!();

    // Create harness
    let config = BenchConfig::new(
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(2),
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(5),
        120, // 2 minute timeout
    );
    let harness = Harness::with_config(config);

    bench_scan_operation(&harness);

    eprintln!("\n========================================");
    eprintln!("  Benchmark Complete");
    eprintln!("========================================\n");
}
