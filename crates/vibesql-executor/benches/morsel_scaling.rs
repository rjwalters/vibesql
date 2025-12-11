//! Morsel-Driven Execution Scaling Benchmarks
//!
//! This benchmark measures the scaling characteristics of morsel-driven work-stealing
//! parallelism on multi-core systems, as implemented in PR #4160.
//!
//! ## Benchmarks
//!
//! 1. **Thread Scaling**: Run TPC-H Q1, Q6 at varying thread counts (1, 2, 4, 8, 16) and measure
//!    speedup and efficiency relative to single-threaded execution.
//!
//! 2. **Load Balancing**: Test work-stealing on skewed data distributions where some partitions
//!    have much more work than others.
//!
//! 3. **Morsel Size Sensitivity**: Test different morsel sizes to find optimal configuration for
//!    different query types.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run
//! cargo bench --package vibesql-executor --bench morsel_scaling --no-run
//! ./target/release/deps/morsel_scaling-*
//!
//! # Run specific benchmark
//! BENCHMARK_FILTER=thread_scaling ./target/release/deps/morsel_scaling-*
//! BENCHMARK_FILTER=load_balancing ./target/release/deps/morsel_scaling-*
//! BENCHMARK_FILTER=morsel_size ./target/release/deps/morsel_scaling-*
//! ```
//!
//! ## Environment Variables
//!
//! - `BENCHMARK_FILTER` - Run only specific benchmark (thread_scaling, load_balancing, morsel_size)
//! - `MORSEL_DEBUG=1` - Enable detailed morsel execution logging (shows work stealing)
//! - `MORSEL_SIZE=<rows>` - Override default morsel size (default: 50,000)
//! - `WARMUP_ITERATIONS` - Number of warmup runs (default: 2)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs (default: 5)
//! - `SCALE_FACTOR` - TPC-H scale factor for data generation (default: 0.01)
//! - `MAX_THREADS` - Maximum thread count to test (default: 16)

mod harness;
mod tpch;

use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use harness::{print_group_header, BenchConfig, BenchResult, BenchStats, Harness};
use tpch::{
    queries::{TPCH_Q1, TPCH_Q6},
    schema::load_vibesql,
};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;
use vibesql_types::SqlValue;

/// Parse a SELECT statement from SQL
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement"),
    }
}

/// Run a query on VibeSQL and return the execution time
fn run_query(db: &VibeDB, sql: &str) -> BenchResult {
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

/// Get scale factor from environment
fn get_scale_factor() -> f64 {
    env::var("SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01)
}

/// Get max threads to test from environment
fn get_max_threads() -> usize {
    env::var("MAX_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(16)
}

/// Get benchmark filter from environment
fn get_benchmark_filter() -> Option<String> {
    env::var("BENCHMARK_FILTER").ok().map(|s| s.to_lowercase())
}

// =============================================================================
// Thread Scaling Benchmark
// =============================================================================

/// Benchmark thread scaling for TPC-H Q1 and Q6
///
/// Measures:
/// - Execution time at each thread count
/// - Speedup relative to single-threaded (T1 / Tn)
/// - Efficiency (speedup / thread_count)
fn bench_thread_scaling(db: &VibeDB, harness: &Harness) {
    print_group_header("Thread Scaling Benchmark");

    let max_threads = get_max_threads();
    let thread_counts: Vec<usize> =
        [1, 2, 4, 8, 16, 32].iter().copied().filter(|&t| t <= max_threads).collect();

    eprintln!("Testing thread counts: {:?}", thread_counts);
    eprintln!("(Set MAX_THREADS env var to adjust, default: 16)\n");

    // Test queries
    let queries = [("Q1", TPCH_Q1), ("Q6", TPCH_Q6)];

    for (query_name, query_sql) in queries {
        eprintln!("\n--- {} Thread Scaling ---", query_name);

        let mut baseline_time: Option<Duration> = None;
        let mut results: Vec<(usize, BenchStats)> = Vec::new();

        for &thread_count in &thread_counts {
            // Configure rayon thread pool for this run
            // Note: We use a scoped pool to isolate thread count changes
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(thread_count)
                .build()
                .expect("Failed to create thread pool");

            let stats = pool.install(|| {
                let name = format!("{}_{}t", query_name, thread_count);
                harness.run(&name, || run_query(db, query_sql))
            });

            // Record baseline for speedup calculation
            if thread_count == 1 {
                baseline_time = Some(stats.mean);
            }

            results.push((thread_count, stats));
        }

        // Print results with speedup and efficiency
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
}

// =============================================================================
// Load Balancing Benchmark
// =============================================================================

/// Create a database with skewed data distribution
///
/// Creates a simple table where data is heavily skewed - 90% of rows
/// match a predicate that should trigger work-stealing to balance load.
fn create_skewed_database(row_count: usize, skew_ratio: f64) -> VibeDB {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::DataType;

    let mut db = VibeDB::new();

    // Create a simple table
    let schema = TableSchema::new(
        "SKEWED_DATA".to_string(),
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
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "PADDING".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    // Insert skewed data
    // skew_ratio of 0.9 means 90% of rows have category=0, 10% have category=1-9
    let skewed_count = (row_count as f64 * skew_ratio) as usize;
    let padding = "x".repeat(50); // Add some padding to make rows larger

    for i in 0..row_count {
        let category = if i < skewed_count {
            0 // Most rows in category 0
        } else {
            ((i - skewed_count) % 9) + 1 // Remaining distributed across 1-9
        };

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(category as i64),
            SqlValue::Integer((i * 17 + 42) as i64), // Some computed value
            SqlValue::Varchar(arcstr::ArcStr::from(padding.as_str())),
        ]);
        db.insert_row("SKEWED_DATA", row).unwrap();
    }

    db
}

/// Benchmark load balancing on skewed data
///
/// Tests whether work-stealing effectively balances load when data
/// distribution is non-uniform.
fn bench_load_balancing(harness: &Harness) {
    print_group_header("Load Balancing Benchmark (Skewed Data)");

    let row_counts = [100_000, 500_000];
    let skew_ratios = [0.5, 0.9, 0.99]; // 50%, 90%, 99% skew

    // Query that filters on the skewed category column
    // This creates uneven work distribution across morsels
    let filter_query = "SELECT COUNT(*), SUM(VALUE) FROM SKEWED_DATA WHERE CATEGORY = 0";
    let agg_query = "SELECT CATEGORY, COUNT(*), AVG(VALUE) FROM SKEWED_DATA GROUP BY CATEGORY";

    for row_count in row_counts {
        eprintln!("\n--- {} rows ---", row_count);

        for skew_ratio in skew_ratios {
            eprintln!("\nSkew ratio: {:.0}% in category 0", skew_ratio * 100.0);

            let db = create_skewed_database(row_count, skew_ratio);

            // Test filter query (benefits from work-stealing on selective predicates)
            let name = format!("filter_{}k_{:.0}pct", row_count / 1000, skew_ratio * 100.0);
            let stats = harness.run(&name, || run_query(&db, filter_query));
            stats.print_compact();

            // Test aggregation query (benefits from work-stealing on grouping)
            let name = format!("agg_{}k_{:.0}pct", row_count / 1000, skew_ratio * 100.0);
            let stats = harness.run(&name, || run_query(&db, agg_query));
            stats.print_compact();
        }
    }

    eprintln!("\nNote: Enable MORSEL_DEBUG=1 to see work-stealing activity");
}

// =============================================================================
// Morsel Size Sensitivity Benchmark
// =============================================================================

/// Benchmark different morsel sizes on various query types
fn bench_morsel_size(db: &VibeDB, harness: &Harness) {
    print_group_header("Morsel Size Sensitivity Benchmark");

    let morsel_sizes = [10_000, 25_000, 50_000, 100_000, 200_000];

    // Different query types to test
    let queries = [
        ("Q6_filter", TPCH_Q6), // Filter-heavy
        ("Q1_agg", TPCH_Q1),    // Aggregation-heavy
    ];

    eprintln!("Testing morsel sizes: {:?}\n", morsel_sizes);

    // Results table
    eprintln!(
        "{:<15} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "Query", "10K", "25K", "50K", "100K", "200K"
    );
    eprintln!("{:-<15} {:->12} {:->12} {:->12} {:->12} {:->12}", "", "", "", "", "", "");

    for (query_name, query_sql) in queries {
        let mut times: Vec<Duration> = Vec::new();

        for morsel_size in morsel_sizes {
            // Set morsel size via environment variable
            env::set_var("MORSEL_SIZE", morsel_size.to_string());

            // Need to run in a fresh context to pick up the new morsel size
            // The morsel config is lazily initialized, so we run multiple iterations
            let name = format!("{}_{}", query_name, morsel_size);
            let stats = harness.run(&name, || run_query(db, query_sql));
            times.push(stats.mean);
        }

        // Print row with all morsel size results
        eprint!("{:<15}", query_name);
        for time in &times {
            eprint!(" {:>12.2?}", time);
        }
        eprintln!();
    }

    // Reset to default
    env::remove_var("MORSEL_SIZE");

    eprintln!("\nNote: Use MORSEL_SIZE=<rows> to set a specific morsel size globally");
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    eprintln!("\n========================================");
    eprintln!("  Morsel-Driven Execution Scaling");
    eprintln!("========================================\n");

    // Configuration
    let scale_factor = get_scale_factor();
    let filter = get_benchmark_filter();

    eprintln!("Configuration:");
    eprintln!("  Scale Factor: {}", scale_factor);
    eprintln!("  Max Threads: {}", get_max_threads());
    eprintln!("  Benchmark Filter: {:?}", filter);
    eprintln!("  MORSEL_DEBUG: {}", env::var("MORSEL_DEBUG").unwrap_or_default());
    eprintln!();

    // Create harness with fewer iterations for faster runs
    let config = BenchConfig::new(
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(2),
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(5),
        60, // 60 second timeout
    );
    let harness = Harness::with_config(config);

    // Load TPC-H database for scaling and morsel size benchmarks
    let db = if filter.as_deref() != Some("load_balancing") {
        eprintln!("Loading TPC-H database...");
        Some(load_vibesql(scale_factor))
    } else {
        None
    };

    // Run benchmarks based on filter
    match filter.as_deref() {
        Some("thread_scaling") | Some("thread") | Some("scaling") => {
            if let Some(ref db) = db {
                bench_thread_scaling(db, &harness);
            }
        }
        Some("load_balancing") | Some("load") | Some("balancing") | Some("skew") => {
            bench_load_balancing(&harness);
        }
        Some("morsel_size") | Some("morsel") | Some("size") => {
            if let Some(ref db) = db {
                bench_morsel_size(db, &harness);
            }
        }
        None => {
            // Run all benchmarks
            if let Some(ref db) = db {
                bench_thread_scaling(db, &harness);
                bench_morsel_size(db, &harness);
            }
            bench_load_balancing(&harness);
        }
        Some(unknown) => {
            eprintln!("Unknown benchmark filter: {}", unknown);
            eprintln!("Valid filters: thread_scaling, load_balancing, morsel_size");
            std::process::exit(1);
        }
    }

    eprintln!("\n========================================");
    eprintln!("  Benchmark Complete");
    eprintln!("========================================\n");
}
