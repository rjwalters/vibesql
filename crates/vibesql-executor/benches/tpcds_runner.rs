// ============================================================================
// ⚠️  BENCHMARK INTEGRITY WARNING ⚠️
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. Benchmarks MUST execute actual SQL to produce
// meaningful results. "Optimizing" benchmarks this way is cheating.
// ============================================================================

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
//! - Optional parallel execution within batches (PARALLEL_BATCHES=1)
//!
//! Parallel Execution Mode:
//! When PARALLEL_BATCHES=1 is set, queries within each batch are executed in
//! parallel using rayon. This can significantly reduce wall-clock time (2-4x speedup
//! typical) but may affect per-query memory tracking accuracy. Parallel mode is
//! automatically disabled when VALIDATE=1 is set (to ensure deterministic comparison).
//!
//! Validation Mode:
//! When VALIDATE=1 is set (requires --features benchmark-comparison), the runner
//! compares VibeSQL results against DuckDB as ground truth. This validates that
//! queries return the correct number of rows.
//!
//! Usage:
//!   cargo run --release --bench tpcds_runner
//!   SCALE_FACTOR=0.001 cargo run --release --bench tpcds_runner  # Smaller dataset
//!   SKIP_SLOW=1 cargo run --release --bench tpcds_runner  # Skip known slow queries
//!   BATCH_SIZE=5 cargo run --release --bench tpcds_runner  # Run 5 queries per batch
//!   MEMORY_WARN_MB=4000 cargo run --release --bench tpcds_runner  # Warn at 4GB RSS
//!   VALIDATE=1 cargo run --release --bench tpcds_runner --features benchmark-comparison  # Validation mode
//!   PARALLEL_BATCHES=1 cargo run --release --bench tpcds_runner  # Enable parallel execution
//!   PARALLEL_WORKERS=4 cargo run --release --bench tpcds_runner  # Override worker count
//!
//! For better memory release, use jemalloc:
//!   cargo run --release --bench tpcds_runner --features jemalloc

// Set up jemalloc as the global allocator when feature is enabled
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod memory_monitor;
mod tpcds;

use memory_monitor::compute_parallelism;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tpcds::memory::{
    get_jemalloc_stats, get_memory_usage, hint_memory_release, is_jemalloc_enabled, MemoryTracker,
};
use tpcds::queries::TPCDS_QUERIES;
use tpcds::schema::load_vibesql;
use vibesql_executor::{clear_in_subquery_cache, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::QueryBufferPool;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "duckdb-comparison")]
use tpcds::schema::load_duckdb;

/// Queries known to be extremely slow or memory-intensive
/// These can be skipped with SKIP_SLOW=1 environment variable
const SLOW_QUERIES: &[&str] = &[
    // Q4, Q11 fixed by PR #3393 (case-insensitive predicate plan lookups for CTE aliases)
    // Q69 was fixed by PR #3338 (EXISTS→semi-join transformation)
    // Q17, Q24, Q29 were fixed by PR #3347 (hash join for derived tables)
];

/// Default batch size for query execution
const DEFAULT_BATCH_SIZE: usize = 10;

/// Default memory warning threshold in MB
const DEFAULT_MEMORY_WARN_MB: f64 = 6000.0;

/// Result of executing a single query (for parallel collection)
#[derive(Debug, Clone)]
struct QueryResult {
    name: String,
    elapsed: Option<Duration>,
    row_count: usize,
    status: String,
    error: Option<String>,
}

impl QueryResult {
    fn success(name: &str, elapsed: Duration, row_count: usize) -> Self {
        Self {
            name: name.to_string(),
            elapsed: Some(elapsed),
            row_count,
            status: "OK".to_string(),
            error: None,
        }
    }

    fn error(name: &str, elapsed: Duration, error: String) -> Self {
        Self {
            name: name.to_string(),
            elapsed: Some(elapsed),
            row_count: 0,
            status: "ERROR".to_string(),
            error: Some(error),
        }
    }

    fn parse_error(name: &str, error: String) -> Self {
        Self {
            name: name.to_string(),
            elapsed: None,
            row_count: 0,
            status: "PARSE_ERR".to_string(),
            error: Some(error),
        }
    }

    fn skipped(name: &str, reason: &str) -> Self {
        Self {
            name: name.to_string(),
            elapsed: None,
            row_count: 0,
            status: format!("SKIPPED ({})", reason),
            error: None,
        }
    }

    fn not_select(name: &str) -> Self {
        Self {
            name: name.to_string(),
            elapsed: None,
            row_count: 0,
            status: "NOT_SELECT".to_string(),
            error: None,
        }
    }
}

/// Execute a single query and return the result
fn execute_query(
    db: &vibesql_storage::Database,
    name: &str,
    sql: &str,
) -> QueryResult {
    let start = Instant::now();

    // Parse the query
    let stmt = match Parser::parse_sql(sql) {
        Ok(stmt) => stmt,
        Err(e) => {
            return QueryResult::parse_error(name, format!("{:?}", e));
        }
    };

    // Execute the query
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);

        match executor.execute(&select) {
            Ok(rows) => {
                let elapsed = start.elapsed();
                let row_count = rows.len();
                drop(rows); // Explicitly drop for memory reclamation
                QueryResult::success(name, elapsed, row_count)
            }
            Err(e) => {
                let elapsed = start.elapsed();
                QueryResult::error(name, elapsed, format!("{:?}", e))
            }
        }
    } else {
        QueryResult::not_select(name)
    }
}

/// Get expected row counts from DuckDB for validation
#[cfg(feature = "duckdb-comparison")]
fn get_expected_row_counts(
    duckdb: &DuckDBConn,
    queries: &[(&str, &str)],
) -> HashMap<String, usize> {
    let mut expected = HashMap::new();

    for (name, sql) in queries {
        match duckdb.prepare(sql) {
            Ok(mut stmt) => match stmt.query([]) {
                Ok(mut rows) => {
                    let mut count = 0;
                    while rows.next().map(|r| r.is_some()).unwrap_or(false) {
                        count += 1;
                    }
                    expected.insert(name.to_string(), count);
                }
                Err(e) => {
                    eprintln!("DuckDB query error for {}: {:?}", name, e);
                }
            },
            Err(e) => {
                eprintln!("DuckDB prepare error for {}: {:?}", name, e);
            }
        }
    }

    expected
}

fn main() {
    println!("=== TPC-DS Benchmark Runner ===\n");

    // Get configuration from environment
    let scale_factor: f64 =
        std::env::var("SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    let batch_size: usize =
        std::env::var("BATCH_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_BATCH_SIZE);

    let memory_warn_mb: f64 = std::env::var("MEMORY_WARN_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MEMORY_WARN_MB);

    let skip_slow = std::env::var("SKIP_SLOW").is_ok();
    let slow_queries: HashSet<&str> =
        if skip_slow { SLOW_QUERIES.iter().copied().collect() } else { HashSet::new() };

    // Query filter: run only specific queries (comma-separated, e.g., "Q17,Q24,Q29")
    let query_filter: HashSet<String> = std::env::var("QUERY_FILTER")
        .ok()
        .map(|s| s.split(',').map(|q| q.trim().to_uppercase()).collect())
        .unwrap_or_default();

    // Check for validation mode
    let validate_mode = std::env::var("VALIDATE").is_ok();

    // Check for parallel execution mode
    // Automatically disabled in validation mode for deterministic output
    let parallel_batches = std::env::var("PARALLEL_BATCHES").is_ok() && !validate_mode;
    let parallelism = if parallel_batches { compute_parallelism() } else { 1 };

    #[cfg(not(feature = "duckdb-comparison"))]
    if validate_mode {
        eprintln!("ERROR: VALIDATE=1 requires --features duckdb-comparison");
        eprintln!("Usage: VALIDATE=1 cargo run --release --bench tpcds_runner --features duckdb-comparison");
        std::process::exit(1);
    }

    // Print configuration
    println!("Configuration:");
    println!("  Scale factor:    {}", scale_factor);
    println!("  Batch size:      {} queries", batch_size);
    println!("  Memory warning:  {:.0} MB", memory_warn_mb);
    println!("  Allocator:       {}", if is_jemalloc_enabled() { "jemalloc" } else { "system" });
    if parallel_batches {
        println!("  Parallel mode:   {} workers", parallelism);
    } else if std::env::var("PARALLEL_BATCHES").is_ok() && validate_mode {
        println!("  Parallel mode:   disabled (VALIDATE=1 requires sequential execution)");
    } else {
        println!("  Parallel mode:   disabled (set PARALLEL_BATCHES=1 to enable)");
    }
    if skip_slow {
        println!("  Skipping:        {} known slow queries", SLOW_QUERIES.len());
    }
    if !query_filter.is_empty() {
        println!("  Query filter:    {:?}", query_filter);
    }
    if validate_mode {
        println!("  Mode:            VALIDATION (comparing with DuckDB)");
    }

    // Initialize memory tracker
    let mut memory_tracker = MemoryTracker::new(memory_warn_mb);

    // Report initial memory
    if let Some(stats) = get_memory_usage() {
        println!("\nInitial memory: {}", stats);
    }

    // Configure rayon thread pool if parallel mode is enabled
    if parallel_batches {
        rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism)
            .build_global()
            .unwrap_or_else(|e| eprintln!("Warning: Failed to configure rayon thread pool: {}", e));
    }

    // Load data
    println!("\nLoading TPC-DS data...");
    let load_start = Instant::now();
    let db = Arc::new(load_vibesql(scale_factor));
    let load_time = load_start.elapsed();
    println!("VibeSQL data loaded in {:?}", load_time);

    // Load DuckDB and get expected row counts in validation mode
    #[cfg(feature = "duckdb-comparison")]
    let expected_rows: Arc<HashMap<String, usize>> = Arc::new(if validate_mode {
        println!("Loading DuckDB for validation...");
        let duckdb_start = Instant::now();
        let duckdb = load_duckdb(scale_factor);
        println!("DuckDB loaded in {:?}", duckdb_start.elapsed());

        println!("Computing expected row counts from DuckDB...");
        let expect_start = Instant::now();
        let expected = get_expected_row_counts(&duckdb, TPCDS_QUERIES);
        println!(
            "Expected row counts computed in {:?} ({} queries)",
            expect_start.elapsed(),
            expected.len()
        );
        expected
    } else {
        HashMap::new()
    });

    #[cfg(not(feature = "duckdb-comparison"))]
    let expected_rows: Arc<HashMap<String, usize>> = Arc::new(HashMap::new());

    // Report post-load memory
    if let Some(stats) = memory_tracker.record() {
        println!("Post-load memory: {}", stats);
    }

    // Filter queries based on configuration
    let queries_to_run: Vec<(&str, &str)> = TPCDS_QUERIES
        .iter()
        .filter(|(name, _)| {
            if !query_filter.is_empty() && !query_filter.contains(&name.to_uppercase()) {
                return false;
            }
            true
        })
        .map(|(n, s)| (*n, *s))
        .collect();

    println!(
        "\nRunning {} TPC-DS queries in batches of {}{}...\n",
        queries_to_run.len(),
        batch_size,
        if parallel_batches { format!(" ({} workers)", parallelism) } else { String::new() }
    );
    if validate_mode {
        println!(
            "{:<8} {:>12} {:>10} {:>10} {:>12} {:>10}",
            "Query", "Time (ms)", "Rows", "Expected", "RSS (MB)", "Status"
        );
        println!("{}", "-".repeat(75));
    } else {
        println!(
            "{:<8} {:>12} {:>10} {:>12} {:>10}",
            "Query", "Time (ms)", "Rows", "RSS (MB)", "Status"
        );
        println!("{}", "-".repeat(60));
    }

    let mut all_results: Vec<QueryResult> = Vec::new();
    let mut total_time = Duration::ZERO;
    let wall_clock_start = Instant::now();

    // Process queries in batches
    for (batch_idx, batch) in queries_to_run.chunks(batch_size).enumerate() {
        let batch_start = Instant::now();

        // Separate skipped queries from queries to execute
        let mut skipped_in_batch: Vec<QueryResult> = Vec::new();
        let mut to_execute: Vec<(&str, &str)> = Vec::new();

        for (name, sql) in batch {
            if slow_queries.contains(name) {
                skipped_in_batch.push(QueryResult::skipped(name, "slow"));
            } else {
                to_execute.push((name, sql));
            }
        }

        // Execute queries (parallel or sequential based on configuration)
        let batch_results: Vec<QueryResult> = if parallel_batches && to_execute.len() > 1 {
            // Parallel execution within batch
            to_execute
                .par_iter()
                .map(|(name, sql)| execute_query(&db, name, sql))
                .collect()
        } else {
            // Sequential execution
            to_execute
                .iter()
                .map(|(name, sql)| execute_query(&db, name, sql))
                .collect()
        };

        // Combine skipped and executed results
        let mut combined_results = skipped_in_batch;
        combined_results.extend(batch_results);

        // Print results for this batch (maintaining query order for readability)
        for result in &combined_results {
            let rss_mb = memory_tracker
                .record()
                .map(|s| format!("{:.1}", s.rss_mb()))
                .unwrap_or_else(|| "-".to_string());

            if validate_mode {
                let expected = expected_rows.get(&result.name);
                let (status_str, expected_str) = match expected {
                    Some(&exp) if result.row_count == exp => ("PASS".to_string(), exp.to_string()),
                    Some(&exp) => (format!("FAIL (exp {})", exp), exp.to_string()),
                    None if result.status == "OK" => ("NO_EXP".to_string(), "-".to_string()),
                    _ => (result.status.clone(), "-".to_string()),
                };
                let time_str = result
                    .elapsed
                    .map(|e| format!("{:.2}", e.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "-".to_string());
                let rows_str = if result.row_count > 0 || result.status == "OK" {
                    result.row_count.to_string()
                } else {
                    "-".to_string()
                };
                println!(
                    "{:<8} {:>12} {:>10} {:>10} {:>12} {}",
                    result.name, time_str, rows_str, expected_str, rss_mb, status_str
                );
            } else {
                let time_str = result
                    .elapsed
                    .map(|e| format!("{:.2}", e.as_secs_f64() * 1000.0))
                    .unwrap_or_else(|| "-".to_string());
                let rows_str = if result.row_count > 0 || result.status == "OK" {
                    result.row_count.to_string()
                } else {
                    "-".to_string()
                };
                let status_str = if let Some(ref err) = result.error {
                    let short_err =
                        if err.len() > 30 { format!("{}...", &err[..30]) } else { err.clone() };
                    format!("{}: {}", result.status, short_err)
                } else {
                    result.status.clone()
                };
                println!(
                    "{:<8} {:>12} {:>10} {:>12} {}",
                    result.name, time_str, rows_str, rss_mb, status_str
                );
            }
        }

        // Accumulate total execution time
        for result in &combined_results {
            if let Some(elapsed) = result.elapsed {
                total_time += elapsed;
            }
        }

        all_results.extend(combined_results);

        // End of batch: clear caches and hint memory release
        clear_in_subquery_cache();
        QueryBufferPool::clear_thread_local_pools();
        hint_memory_release();

        // Small pause to allow OS to reclaim memory
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Report batch completion
        let batch_elapsed = batch_start.elapsed();
        if let Some(stats) = memory_tracker.record() {
            if let Some(je_stats) = get_jemalloc_stats() {
                eprintln!(
                    "  [Batch {} complete in {:?}, RSS: {:.1} MB, jemalloc resident: {:.1} MB]",
                    batch_idx + 1,
                    batch_elapsed,
                    stats.rss_mb(),
                    je_stats.resident as f64 / (1024.0 * 1024.0)
                );
            } else {
                eprintln!(
                    "  [Batch {} complete in {:?}, RSS: {:.1} MB]",
                    batch_idx + 1,
                    batch_elapsed,
                    stats.rss_mb()
                );
            }
        }
    }

    let wall_clock_time = wall_clock_start.elapsed();

    // Count results
    let success_count = all_results.iter().filter(|r| r.status == "OK").count();
    let error_count = all_results
        .iter()
        .filter(|r| r.status == "ERROR" || r.status == "PARSE_ERR" || r.status == "NOT_SELECT")
        .count();
    let skipped_count = all_results.iter().filter(|r| r.status.starts_with("SKIPPED")).count();
    let pass_count = if validate_mode {
        all_results
            .iter()
            .filter(|r| {
                if let Some(&exp) = expected_rows.get(&r.name) {
                    r.row_count == exp && r.status == "OK"
                } else {
                    false
                }
            })
            .count()
    } else {
        0
    };
    let fail_count = if validate_mode {
        all_results
            .iter()
            .filter(|r| {
                if let Some(&exp) = expected_rows.get(&r.name) {
                    r.row_count != exp && r.status == "OK"
                } else {
                    false
                }
            })
            .count()
    } else {
        0
    };

    // Final cleanup: clear all caches and hint memory release
    clear_in_subquery_cache();
    QueryBufferPool::clear_thread_local_pools();
    hint_memory_release();

    println!("\n{}", "=".repeat(60));
    println!("=== Summary ===");
    println!("Total queries:   {}", queries_to_run.len());
    println!(
        "Successful:      {} ({:.1}%)",
        success_count,
        100.0 * success_count as f64 / queries_to_run.len().max(1) as f64
    );
    println!("Errors:          {}", error_count);
    println!("Skipped:         {}", skipped_count);
    println!("Total exec time: {:?} (sum of individual query times)", total_time);
    println!("Wall-clock time: {:?}", wall_clock_time);
    if parallel_batches && success_count > 0 {
        let speedup = total_time.as_secs_f64() / wall_clock_time.as_secs_f64();
        println!("Parallel speedup: {:.2}x ({} workers)", speedup, parallelism);
    }
    if success_count > 0 {
        println!("Average time:    {:?}", total_time / success_count as u32);
    }

    // Print validation summary
    if validate_mode {
        println!("\n=== Validation Results ===");
        println!(
            "Passed:          {} ({:.1}%)",
            pass_count,
            100.0 * pass_count as f64 / success_count.max(1) as f64
        );
        println!("Failed:          {}", fail_count);
        if fail_count > 0 {
            println!("\nVALIDATION FAILED: {} queries returned incorrect row counts", fail_count);
        } else if pass_count > 0 {
            println!("\nVALIDATION PASSED: All {} queries returned correct row counts", pass_count);
        }
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
    for result in &all_results {
        let time_str = match result.elapsed {
            Some(t) => format!("{:.2}", t.as_secs_f64() * 1000.0),
            None => "-".to_string(),
        };
        // Escape status for CSV
        let status = if let Some(ref err) = result.error {
            format!("{}: {}", result.status, err)
        } else {
            result.status.clone()
        };
        let csv_status = if status.contains(',') || status.contains('"') {
            format!("\"{}\"", status.replace('"', "\"\""))
        } else {
            status
        };
        println!("{},{},{},{}", result.name, time_str, result.row_count, csv_status);
    }
}
