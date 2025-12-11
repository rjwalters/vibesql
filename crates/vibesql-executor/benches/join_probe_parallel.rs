//! Benchmarks for parallel hash join probe phase
//!
//! These benchmarks isolate the hash join probe operation to measure the effect of
//! morsel size on parallel join probing. Note that hash join build phase benchmarks
//! are in `hash_join_parallel.rs`.
//!
//! ## Usage
//!
//! ```bash
//! # Build the benchmark
//! cargo bench --package vibesql-executor --bench join_probe_parallel --no-run
//!
//! # Run the benchmark
//! ./target/release/deps/join_probe_parallel-*
//!
//! # Test specific morsel sizes
//! MORSEL_SIZES=2048,8192,50000 ./target/release/deps/join_probe_parallel-*
//!
//! # Vary thread counts
//! MAX_THREADS=8 ./target/release/deps/join_probe_parallel-*
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

use std::{
    env,
    hint::black_box,
    time::Instant,
};

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

/// Create a database for hash join benchmarks
///
/// Tables:
/// - BUILD_TABLE (id INTEGER, value BIGINT) - smaller table for hash build
/// - PROBE_TABLE (id INTEGER, build_id INTEGER, data BIGINT) - larger table for probe
///
/// Characteristics:
/// - Configurable build:probe ratio
/// - Each build row matches multiple probe rows (fan-out)
fn create_join_database(probe_count: usize, build_size: usize) -> Database {
    let mut db = Database::new();

    // Build table (smaller)
    let build_schema = TableSchema::new(
        "BUILD_TABLE".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
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
        ],
    );
    db.create_table(build_schema).unwrap();

    for i in 0..build_size {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Bigint((i * 100) as i64),
        ]);
        db.insert_row("BUILD_TABLE", row).unwrap();
    }

    // Probe table (larger)
    let probe_schema = TableSchema::new(
        "PROBE_TABLE".to_string(),
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "BUILD_ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "DATA".to_string(),
                data_type: DataType::Bigint,
                nullable: false,
                default_value: None,
            },
        ],
    );
    db.create_table(probe_schema).unwrap();

    for i in 0..probe_count {
        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer((i % build_size) as i64), // foreign key to build table
            SqlValue::Bigint((i * 11) as i64),
        ]);
        db.insert_row("PROBE_TABLE", row).unwrap();
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
// Benchmarks
// =============================================================================

/// Benchmark hash join build and probe phases
fn bench_join_operation(harness: &Harness) {
    print_group_header("Hash Join Operation (Build + Probe)");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different build:probe ratios
    let ratios = [
        ("ratio_1_10", 10),  // 1:10 ratio (build is 10% of probe)
        ("ratio_1_100", 100), // 1:100 ratio (build is 1% of probe)
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} probe rows ---", row_count);

        for (ratio_name, ratio) in &ratios {
            let build_size = row_count / ratio;
            eprintln!(
                "\n  Ratio: {} ({} build rows, {} probe rows)",
                ratio_name, build_size, row_count
            );

            let db = create_join_database(row_count, build_size);

            let query = "SELECT COUNT(*) FROM PROBE_TABLE p JOIN BUILD_TABLE b ON p.BUILD_ID = b.ID";

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
                        let name = format!("{}_{}t_{}m", ratio_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

/// Benchmark join with post-join filter (tests filter on join output)
///
/// This tests work-stealing effectiveness when filtering join results.
fn bench_join_filter_operation(harness: &Harness) {
    print_group_header("Join with Filter Operation");

    let row_counts = get_row_counts();
    let morsel_sizes = get_morsel_sizes();
    let thread_counts = get_thread_counts();

    // Different filter selectivities on join result
    let selectivities = [
        ("filter_low", 1000),     // p.ID < 1000 (1% at 100K)
        ("filter_mid", 50000),    // p.ID < 50000 (50%)
        ("filter_high", 90000),   // p.ID < 90000 (90%)
    ];

    for &row_count in &row_counts {
        eprintln!("\n--- {} probe rows ---", row_count);

        // Create join database with 1:10 ratio
        let build_size = row_count / 10;
        let db = create_join_database(row_count, build_size);

        for (select_name, threshold) in &selectivities {
            let pct = (*threshold as f64 / row_count as f64 * 100.0) as i32;
            eprintln!("\n  Filter: {} (~{}% of rows)", select_name, pct);

            // Simple filter on ID column
            let query = format!(
                "SELECT COUNT(*) FROM PROBE_TABLE p JOIN BUILD_TABLE b ON p.BUILD_ID = b.ID WHERE p.ID < {}",
                threshold
            );

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
                        let name = format!("{}_{}t_{}m", select_name, threads, morsel_size);
                        harness.run(&name, || run_query(&db, &query))
                    });

                    eprint!(" {:>8.2?}", stats.mean);
                }
                eprintln!();
            }
        }
    }

    env::remove_var("MORSEL_SIZE");
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n========================================");
    eprintln!("  Join Probe Parallel Benchmark");
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

    bench_join_operation(&harness);
    bench_join_filter_operation(&harness);

    eprintln!("\n========================================");
    eprintln!("  Benchmark Complete");
    eprintln!("========================================\n");
}
