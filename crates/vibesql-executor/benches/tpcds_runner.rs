//! TPC-DS Benchmark Runner
//!
//! Run all TPC-DS queries once and report timing for each query.
//! Unlike the criterion benchmark, this runs each query only once to capture
//! a complete snapshot of query performance across the entire suite.
//!
//! This runner includes memory management features to prevent OOM during
//! long benchmark runs:
//! - Batched query execution with cleanup between batches
//! - Memory usage tracking and reporting
//! - Configurable memory warning thresholds
//! - Optional jemalloc allocator for better memory release
//!
//! Usage:
//!   cargo run --release --bench tpcds_runner
//!   SCALE_FACTOR=0.001 cargo run --release --bench tpcds_runner  # Smaller dataset
//!   SKIP_SLOW=1 cargo run --release --bench tpcds_runner  # Skip known slow queries
//!   BATCH_SIZE=5 cargo run --release --bench tpcds_runner  # Run 5 queries per batch
//!   MEMORY_WARN_MB=4000 cargo run --release --bench tpcds_runner  # Warn at 4GB RSS
//!
//! For better memory release, use jemalloc:
//!   cargo run --release --bench tpcds_runner --features jemalloc

// Set up jemalloc as the global allocator when feature is enabled
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod tpcds;

use std::collections::HashSet;
use std::time::{Duration, Instant};
use tpcds::memory::{get_jemalloc_stats, get_memory_usage, hint_memory_release, is_jemalloc_enabled, MemoryTracker};
use tpcds::queries::TPCDS_QUERIES;
use tpcds::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Queries known to be extremely slow due to complex joins/subqueries
/// These can be skipped with SKIP_SLOW=1 environment variable
const SLOW_QUERIES: &[&str] = &[
    "Q4",  // Complex CTE with 6-way self-join
    "Q11", // Customer web vs store sales growth (complex)
    "Q17", // Store sales-returns-catalog analysis
    "Q24", // Complex multi-table join
    "Q29", // Large date dimension join
];

/// Default batch size for query execution
const DEFAULT_BATCH_SIZE: usize = 10;

/// Default memory warning threshold in MB
const DEFAULT_MEMORY_WARN_MB: f64 = 6000.0;

fn main() {
    println!("=== TPC-DS Benchmark Runner ===\n");

    // Get configuration from environment
    let scale_factor: f64 = std::env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);

    let batch_size: usize = std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);

    let memory_warn_mb: f64 = std::env::var("MEMORY_WARN_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MEMORY_WARN_MB);

    let skip_slow = std::env::var("SKIP_SLOW").is_ok();
    let slow_queries: HashSet<&str> = if skip_slow {
        SLOW_QUERIES.iter().copied().collect()
    } else {
        HashSet::new()
    };

    // Print configuration
    println!("Configuration:");
    println!("  Scale factor:    {}", scale_factor);
    println!("  Batch size:      {} queries", batch_size);
    println!("  Memory warning:  {:.0} MB", memory_warn_mb);
    println!("  Allocator:       {}", if is_jemalloc_enabled() { "jemalloc" } else { "system" });
    if skip_slow {
        println!("  Skipping:        {} known slow queries", SLOW_QUERIES.len());
    }

    // Initialize memory tracker
    let mut memory_tracker = MemoryTracker::new(memory_warn_mb);

    // Report initial memory
    if let Some(stats) = get_memory_usage() {
        println!("\nInitial memory: {}", stats);
    }

    // Load data
    println!("\nLoading TPC-DS data...");
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    let load_time = load_start.elapsed();
    println!("Data loaded in {:?}", load_time);

    // Report post-load memory
    if let Some(stats) = memory_tracker.record() {
        println!("Post-load memory: {}", stats);
    }

    println!("\nRunning {} TPC-DS queries in batches of {}...\n", TPCDS_QUERIES.len(), batch_size);
    println!(
        "{:<8} {:>12} {:>10} {:>12} {:>10}",
        "Query", "Time (ms)", "Rows", "RSS (MB)", "Status"
    );
    println!("{}", "-".repeat(60));

    let mut results: Vec<(String, Option<Duration>, usize, String)> = Vec::new();
    let mut total_time = Duration::ZERO;
    let mut success_count = 0;
    let mut error_count = 0;
    let mut skipped_count = 0;
    let mut queries_in_batch = 0;

    for (idx, (name, sql)) in TPCDS_QUERIES.iter().enumerate() {
        // Check if query should be skipped
        if slow_queries.contains(name) {
            skipped_count += 1;
            results.push((name.to_string(), None, 0, "SKIPPED (slow)".to_string()));
            println!("{:<8} {:>12} {:>10} {:>12} SKIPPED", name, "-", "-", "-");
            continue;
        }

        let start = Instant::now();

        // Parse the query
        let stmt = match Parser::parse_sql(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err_msg = format!("Parse error: {:?}", e);
                println!("{:<8} {:>12} {:>10} {:>12} PARSE_ERR", name, "-", "-", "-");
                error_count += 1;
                results.push((name.to_string(), None, 0, err_msg));
                continue;
            }
        };

        // Execute the query
        if let vibesql_ast::Statement::Select(select) = stmt {
            let executor = SelectExecutor::new(&db);

            match executor.execute(&select) {
                Ok(rows) => {
                    let elapsed = start.elapsed();
                    success_count += 1;
                    total_time += elapsed;
                    let row_count = rows.len();

                    // Explicitly drop rows to allow memory reclamation
                    drop(rows);

                    // Hint memory release after each query to reduce memory pressure
                    // This is especially important for CTE-heavy queries like Q2
                    hint_memory_release();

                    // Record memory after query
                    let rss_mb = memory_tracker.record()
                        .map(|s| format!("{:.1}", s.rss_mb()))
                        .unwrap_or_else(|| "-".to_string());

                    results.push((name.to_string(), Some(elapsed), row_count, "OK".to_string()));
                    println!(
                        "{:<8} {:>12.2} {:>10} {:>12} OK",
                        name,
                        elapsed.as_secs_f64() * 1000.0,
                        row_count,
                        rss_mb
                    );
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    error_count += 1;
                    let err_msg = format!("{:?}", e);
                    let short_err = if err_msg.len() > 30 {
                        format!("{}...", &err_msg[..30])
                    } else {
                        err_msg.clone()
                    };

                    // Hint memory release even after errors
                    hint_memory_release();

                    let rss_mb = memory_tracker.record()
                        .map(|s| format!("{:.1}", s.rss_mb()))
                        .unwrap_or_else(|| "-".to_string());

                    results.push((
                        name.to_string(),
                        Some(elapsed),
                        0,
                        format!("Error: {}", err_msg),
                    ));
                    println!(
                        "{:<8} {:>12.2} {:>10} {:>12} ERROR: {}",
                        name,
                        elapsed.as_secs_f64() * 1000.0,
                        "-",
                        rss_mb,
                        short_err
                    );
                }
            }
        } else {
            error_count += 1;
            results.push((name.to_string(), None, 0, "Not a SELECT".to_string()));
            println!("{:<8} {:>12} {:>10} {:>12} NOT_SELECT", name, "-", "-", "-");
        }

        queries_in_batch += 1;

        // End of batch: hint memory release
        if queries_in_batch >= batch_size {
            queries_in_batch = 0;

            // Hint to allocator to release memory
            hint_memory_release();

            // Small pause to allow OS to reclaim memory
            std::thread::sleep(std::time::Duration::from_millis(10));

            // Report batch completion
            if let Some(stats) = memory_tracker.record() {
                if let Some(je_stats) = get_jemalloc_stats() {
                    eprintln!(
                        "  [Batch {} complete, RSS: {:.1} MB, jemalloc resident: {:.1} MB]",
                        (idx / batch_size) + 1,
                        stats.rss_mb(),
                        je_stats.resident as f64 / (1024.0 * 1024.0)
                    );
                } else {
                    eprintln!(
                        "  [Batch {} complete, RSS: {:.1} MB]",
                        (idx / batch_size) + 1,
                        stats.rss_mb()
                    );
                }
            }
        }
    }

    // Final memory release hint
    hint_memory_release();

    println!("\n{}", "=".repeat(60));
    println!("=== Summary ===");
    println!("Total queries:   {}", TPCDS_QUERIES.len());
    println!(
        "Successful:      {} ({:.1}%)",
        success_count,
        100.0 * success_count as f64 / TPCDS_QUERIES.len() as f64
    );
    println!("Errors:          {}", error_count);
    println!("Skipped:         {}", skipped_count);
    println!("Total exec time: {:?}", total_time);
    if success_count > 0 {
        println!("Average time:    {:?}", total_time / success_count as u32);
    }

    // Print memory summary
    memory_tracker.print_summary();

    // Print jemalloc-specific stats if available
    if let Some(je_stats) = get_jemalloc_stats() {
        eprintln!("\n--- jemalloc Stats ---");
        eprintln!("{}", je_stats);
    }

    // Output CSV for documentation
    println!("\n=== CSV Output ===");
    println!("Query,Time_ms,Rows,Status");
    for (name, time, rows, status) in &results {
        let time_str = match time {
            Some(t) => format!("{:.2}", t.as_secs_f64() * 1000.0),
            None => "-".to_string(),
        };
        // Escape status for CSV
        let csv_status = if status.contains(',') || status.contains('"') {
            format!("\"{}\"", status.replace('"', "\"\""))
        } else {
            status.clone()
        };
        println!("{},{},{},{}", name, time_str, rows, csv_status);
    }
}
