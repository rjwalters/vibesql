//! Deep profiling for TPC-H queries
//!
//! This benchmark runs queries multiple times in a loop to enable external profiling tools.
//!
//! Build:
//!   cargo build --release -p vibesql-executor --bench tpch_deep_profiling --features benchmark-comparison
//!
//! Run with sample (macOS):
//!   ./target/release/deps/tpch_deep_profiling-* Q6 &
//!   PID=$!; sleep 0.5; sample $PID 30 > profile.txt; wait
//!
//! Or use perf (Linux):
//!   perf record -g ./target/release/deps/tpch_deep_profiling-* Q6
//!   perf report

mod tpch;

use std::env;
use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

const DEFAULT_ITERATIONS: u32 = 50;

fn run_query_iterations(
    db: &vibesql_storage::Database,
    name: &str,
    sql: &str,
    iterations: u32,
    timeout: Duration,
) -> Vec<Duration> {
    eprintln!("\n=== {} - Deep Profiling ===", name);
    eprintln!("Running {} iterations...", iterations);

    // Pre-parse the query
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => {
            eprintln!("ERROR: Not a SELECT");
            return vec![];
        }
        Err(e) => {
            eprintln!("ERROR: Parse error: {}", e);
            return vec![];
        }
    };

    let mut times = Vec::with_capacity(iterations as usize);

    // Warm up run
    {
        let executor = SelectExecutor::new(db).with_timeout(timeout.as_secs());
        let _ = executor.execute(&stmt);
    }

    // Timed runs
    for i in 0..iterations {
        let executor = SelectExecutor::new(db).with_timeout(timeout.as_secs());

        let start = Instant::now();
        let result = executor.execute(&stmt);
        let elapsed = start.elapsed();

        if let Ok(rows) = &result {
            if i == 0 {
                eprintln!("  First run: {:>10.2?} ({} rows)", elapsed, rows.len());
            }
        } else if let Err(e) = &result {
            eprintln!("  Run {} ERROR: {}", i + 1, e);
            break;
        }

        times.push(elapsed);
    }

    // Report statistics
    if !times.is_empty() {
        let sum: Duration = times.iter().sum();
        let avg = sum / times.len() as u32;
        let min = times.iter().min().unwrap();
        let max = times.iter().max().unwrap();

        // Calculate median
        let mut sorted_times = times.clone();
        sorted_times.sort();
        let median = sorted_times[sorted_times.len() / 2];

        eprintln!("\n  Statistics ({} iterations):", times.len());
        eprintln!("    Min:    {:>10.2?}", min);
        eprintln!("    Max:    {:>10.2?}", max);
        eprintln!("    Avg:    {:>10.2?}", avg);
        eprintln!("    Median: {:>10.2?}", median);
        eprintln!("    Total:  {:>10.2?}", sum);
    }

    times
}

fn main() {
    eprintln!("=== TPC-H Deep Profiling ===");
    eprintln!("Use this with external profiling tools (sample, perf, Instruments)");

    // Get iterations from env (default 50)
    let iterations: u32 = env::var("PROFILING_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS);

    // Get timeout from env (default 30s)
    let timeout_secs: u64 =
        env::var("QUERY_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let timeout = Duration::from_secs(timeout_secs);

    eprintln!("Iterations: {} (set PROFILING_ITERATIONS to change)", iterations);
    eprintln!("Timeout: {}s per query (set QUERY_TIMEOUT_SECS to change)", timeout_secs);

    // All target queries for profiling
    let all_queries: Vec<(&str, &str)> = vec![
        ("Q1", TPCH_Q1),
        ("Q3", TPCH_Q3),
        ("Q6", TPCH_Q6),
        // Q7 has EXTRACT issue - skip for now
        // ("Q7", TPCH_Q7),
    ];

    // Check for single-query mode
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [QUERY]", args[0]);
        eprintln!("\nArguments:");
        eprintln!("  QUERY    Query to profile (Q1, Q3, Q6). If not specified, runs all.");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  PROFILING_ITERATIONS  Number of iterations (default: {})", DEFAULT_ITERATIONS);
        eprintln!("  QUERY_TIMEOUT_SECS    Timeout per query in seconds (default: 30)");
        eprintln!("\nExamples:");
        eprintln!("  {} Q6                              # Profile Q6 50 times", args[0]);
        eprintln!("  PROFILING_ITERATIONS=100 {} Q6    # Profile Q6 100 times", args[0]);
        eprintln!("\nExternal profiling (macOS):");
        eprintln!("  {} Q6 &                            # Start in background", args[0]);
        eprintln!("  PID=$!; sleep 1; sample $PID 30 > profile.txt; wait");
        std::process::exit(0);
    }

    let queries_to_run: Vec<_> = if args.len() > 1 {
        let target_query = &args[1];
        eprintln!("Single-query mode: {}", target_query);
        all_queries.into_iter().filter(|(name, _)| *name == target_query).collect()
    } else {
        eprintln!("Running all profiling queries");
        all_queries
    };

    if queries_to_run.is_empty() {
        eprintln!("Error: Query '{}' not found. Valid queries: Q1, Q3, Q6", args[1]);
        std::process::exit(1);
    }

    // Load database
    eprintln!("\nLoading TPC-H database (SF 0.01)...");
    let load_start = Instant::now();
    let db = load_vibesql(0.01);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Run selected queries
    for (name, sql) in &queries_to_run {
        run_query_iterations(&db, name, sql, iterations, timeout);
    }

    eprintln!("\n=== Profiling Complete ===");
}
