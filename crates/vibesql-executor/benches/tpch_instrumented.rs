//! Instrumented profiling for TPC-H queries
//!
//! This benchmark adds internal timing instrumentation to understand
//! where execution time is spent in each query phase.
//!
//! Build:
//!   cargo build --release -p vibesql-executor --bench tpch_instrumented --features benchmark-comparison
//!
//! Run:
//!   ./target/release/deps/tpch_instrumented-* Q6

mod tpch;

use std::env;
use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Detailed timing breakdown for query execution
#[derive(Default, Debug)]
#[allow(dead_code)]
struct TimingBreakdown {
    parse_time: Duration,
    executor_creation: Duration,
    total_execution: Duration,
    // Note: Internal phases can only be measured with code instrumentation
    // This serves as the outer measurement framework
}

fn profile_query(
    db: &vibesql_storage::Database,
    name: &str,
    sql: &str,
    iterations: u32,
) -> TimingBreakdown {
    eprintln!("\n{} {} Profiling {}", "=".repeat(20), name, "=".repeat(20));

    // Parse phase
    let parse_start = Instant::now();
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => {
            eprintln!("ERROR: Not a SELECT");
            return TimingBreakdown::default();
        }
        Err(e) => {
            eprintln!("ERROR: Parse error: {}", e);
            return TimingBreakdown::default();
        }
    };
    let parse_time = parse_start.elapsed();

    eprintln!("\n1. QUERY INFO:");
    eprintln!(
        "   SQL: {}",
        sql.trim()
            .lines()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(70)
            .collect::<String>()
    );
    eprintln!("   Parse time: {:?}", parse_time);

    // Warm-up run
    eprintln!("\n2. WARM-UP RUN:");
    let executor = SelectExecutor::new(db).with_timeout(30);
    let warmup_start = Instant::now();
    match executor.execute(&stmt) {
        Ok(rows) => {
            eprintln!("   Result: {} rows in {:?}", rows.len(), warmup_start.elapsed());
        }
        Err(e) => {
            eprintln!("   ERROR: {}", e);
            return TimingBreakdown::default();
        }
    }

    // Collect execution times for statistical analysis
    let mut exec_times = Vec::with_capacity(iterations as usize);

    eprintln!("\n3. PROFILING {} ITERATIONS:", iterations);
    for i in 0..iterations {
        let executor = SelectExecutor::new(db).with_timeout(30);
        let start = Instant::now();
        let _ = executor.execute(&stmt);
        let elapsed = start.elapsed();
        exec_times.push(elapsed);

        // Progress dots
        if i % 10 == 9 {
            eprint!(".");
        }
    }
    eprintln!();

    // Calculate statistics
    let total: Duration = exec_times.iter().sum();
    let avg = total / iterations;

    let mut sorted = exec_times.clone();
    sorted.sort();
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let p50 = sorted[sorted.len() / 2];
    let p90 = sorted[(sorted.len() * 90) / 100];
    let p99 = sorted[(sorted.len() * 99) / 100];

    // Calculate standard deviation
    let avg_nanos = avg.as_nanos() as f64;
    let variance: f64 = exec_times
        .iter()
        .map(|t| {
            let diff = t.as_nanos() as f64 - avg_nanos;
            diff * diff
        })
        .sum::<f64>()
        / iterations as f64;
    let std_dev = Duration::from_nanos(variance.sqrt() as u64);

    eprintln!("\n4. EXECUTION STATISTICS:");
    eprintln!("   Iterations:  {}", iterations);
    eprintln!("   Min:         {:>10.2?}", min);
    eprintln!("   Max:         {:>10.2?}", max);
    eprintln!("   Mean:        {:>10.2?}", avg);
    eprintln!("   Std Dev:     {:>10.2?}", std_dev);
    eprintln!("   P50:         {:>10.2?}", p50);
    eprintln!("   P90:         {:>10.2?}", p90);
    eprintln!("   P99:         {:>10.2?}", p99);

    // Performance estimates
    eprintln!("\n5. PERFORMANCE ANALYSIS:");
    let rows_per_query = match name {
        "Q6" => 60175, // lineitem rows at SF 0.01
        "Q1" => 60175,
        "Q3" => 60175,
        _ => 60000,
    };
    let rows_per_sec = (rows_per_query as f64) / avg.as_secs_f64();
    eprintln!("   Rows processed: ~{} per query", rows_per_query);
    eprintln!("   Throughput: {:.0} rows/sec", rows_per_sec);

    // DuckDB comparison (from known benchmarks)
    let duckdb_times: &[(&str, f64)] = &[("Q1", 4.47), ("Q3", 2.05), ("Q6", 0.54)];
    if let Some((_, duckdb_ms)) = duckdb_times.iter().find(|(q, _)| *q == name) {
        let gap = avg.as_secs_f64() * 1000.0 / duckdb_ms;
        eprintln!("   DuckDB reference: {:.2}ms", duckdb_ms);
        eprintln!("   Performance gap: {:.1}x slower", gap);
    }

    TimingBreakdown { parse_time, executor_creation: Duration::ZERO, total_execution: avg }
}

fn main() {
    eprintln!("=== TPC-H Instrumented Profiling ===");

    let iterations: u32 =
        env::var("PROFILING_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(100);

    eprintln!("Iterations: {} (set PROFILING_ITERATIONS to change)", iterations);

    let queries: Vec<(&str, &str)> = vec![("Q1", TPCH_Q1), ("Q3", TPCH_Q3), ("Q6", TPCH_Q6)];

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        eprintln!("\nUsage: {} [QUERY]", args[0]);
        eprintln!("  QUERY: Q1, Q3, Q6 (default: all)");
        eprintln!("\nEnvironment:");
        eprintln!("  PROFILING_ITERATIONS=N (default: 100)");
        std::process::exit(0);
    }

    let queries_to_run: Vec<_> = if args.len() > 1 {
        let target = &args[1];
        queries.into_iter().filter(|(n, _)| *n == target).collect()
    } else {
        queries
    };

    if queries_to_run.is_empty() {
        eprintln!("Query not found. Valid: Q1, Q3, Q6");
        std::process::exit(1);
    }

    // Load database
    eprintln!("\nLoading TPC-H database (SF 0.01)...");
    let load_start = Instant::now();
    let db = load_vibesql(0.01);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Profile each query
    let mut results = Vec::new();
    for (name, sql) in &queries_to_run {
        let timing = profile_query(&db, name, sql, iterations);
        results.push((name, timing));
    }

    // Summary
    eprintln!("\n{} SUMMARY {}", "=".repeat(30), "=".repeat(30));
    for (name, timing) in &results {
        eprintln!("  {}: {:?} avg execution", name, timing.total_execution);
    }

    eprintln!("\n=== Profiling Complete ===");
}
