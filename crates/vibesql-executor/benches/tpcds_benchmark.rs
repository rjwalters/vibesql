// ============================================================================
// TPC-DS Benchmark Suite - Custom Timing Harness
// ============================================================================
//! TPC-DS Benchmark Suite
//!
//! This benchmark compares TPC-DS query performance across multiple database engines.
//! Uses a custom timing harness instead of Criterion for deterministic, fast execution.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL + SQLite)
//! cargo bench --package vibesql-executor --bench tpcds_benchmark --no-run
//! ./target/release/deps/tpcds_benchmark-*
//!
//! # With DuckDB comparison
//! cargo bench --package vibesql-executor --bench tpcds_benchmark --features duckdb-comparison --no-run
//! ./target/release/deps/tpcds_benchmark-*
//!
//! # With query filter
//! QUERY_FILTER=Q1,Q6,Q9 ./target/release/deps/tpcds_benchmark-*
//! ```
//!
//! ## Environment Variables
//!
//! - `WARMUP_ITERATIONS` - Number of warmup runs per query (default: 3)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs per query (default: 10)
//! - `BENCHMARK_TIMEOUT_SECS` - Timeout per query (default: 30)
//! - `SCALE_FACTOR` - TPC-DS scale factor (default: 0.001)
//! - `QUERY_FILTER` - Comma-separated list of queries to run (e.g., "Q1,Q6,Q9")
//! - `VIBESQL_MEMORY_THRESHOLD` - Memory pressure threshold (default: 80%)
//! - `MYSQL_URL` - MySQL connection string (optional)
//! - `ENGINE_FILTER` - Engines to run (default: vibesql,sqlite,duckdb; MySQL excluded)
//!   Use ENGINE_FILTER=all to include MySQL

mod harness;
mod memory_monitor;
mod tpcds;

use harness::{BenchConfig, BenchResult, BenchStats, EngineFilter, Harness};
use memory_monitor::{format_bytes, MemoryMonitor, MemoryPressure};
use std::env;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tpcds::memory::hint_memory_release;
use tpcds::queries::{TPCDS_QUERIES, TPCDS_SANITY_QUERIES};
use tpcds::schema::*;
use vibesql_executor::{clear_in_subquery_cache, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::PooledConn;
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

/// Default scale factor for TPC-DS benchmarks
const DEFAULT_SCALE_FACTOR: f64 = 0.001;

/// Queries that use SQL features SQLite doesn't support (ROLLUP, CUBE, STDDEV_SAMP, parenthesized UNION)
#[cfg(feature = "sqlite-comparison")]
fn sqlite_should_skip(query_name: &str) -> bool {
    matches!(
        query_name,
        // Parenthesized UNION ALL (SQLite doesn't support parentheses around SELECT in UNION)
        "Q2" |
        // ROLLUP/CUBE queries
        "Q5" | "Q14" | "Q18" | "Q22" | "Q36" | "Q67" | "Q70" | "Q77" | "Q80" | "Q86" |
        // STDDEV_SAMP queries
        "Q17"
    )
}

/// Global memory monitor
static MEMORY_MONITOR: std::sync::OnceLock<Mutex<MemoryMonitor>> = std::sync::OnceLock::new();

fn get_memory_monitor() -> &'static Mutex<MemoryMonitor> {
    MEMORY_MONITOR.get_or_init(|| {
        let monitor = MemoryMonitor::new();
        eprintln!(
            "Memory monitor initialized (threshold: {:.0}%)",
            monitor.threshold_percent()
        );
        Mutex::new(monitor)
    })
}

/// Check memory pressure before running a query
fn check_memory_before_query(query_name: &str) -> bool {
    if let Ok(mut monitor) = get_memory_monitor().lock() {
        match monitor.check_pressure() {
            MemoryPressure::High { stats, threshold_percent } => {
                eprintln!(
                    "[MEMORY] Skipping {} - pressure ({:.1}% > {:.0}% threshold, {} used of {})",
                    query_name,
                    stats.usage_percent,
                    threshold_percent,
                    format_bytes(stats.used_bytes),
                    format_bytes(stats.total_bytes)
                );
                false
            }
            MemoryPressure::Ok(_) => true,
        }
    } else {
        true
    }
}

/// Parse query filter from environment or CLI args
fn get_query_filter() -> Option<Vec<String>> {
    let args: Vec<String> = env::args().collect();
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();

    if !user_args.is_empty() && user_args[0] != "help" && user_args[0] != "--help" {
        return Some(
            user_args[0]
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
        );
    }

    env::var("QUERY_FILTER").ok().map(|s| {
        s.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect()
    })
}

/// Get the engine filter for this benchmark
fn get_engine_filter() -> EngineFilter {
    EngineFilter::from_env_embedded()
}

/// Run a query on VibeSQL
fn run_vibesql_query(db: &VibeDB, sql: &str, timeout: Duration) -> BenchResult {
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return BenchResult::Error("Not a SELECT statement".to_string()),
        Err(e) => return BenchResult::Error(format!("Parse error: {}", e)),
    };

    let executor = SelectExecutor::new(db).with_timeout(timeout.as_secs());
    let start = Instant::now();
    match executor.execute(&stmt) {
        Ok(_rows) => BenchResult::Ok(start.elapsed()),
        Err(e) => {
            if e.to_string().contains("timeout") {
                BenchResult::Timeout
            } else {
                BenchResult::Error(format!("{}", e))
            }
        }
    }
}

/// Run a query on SQLite
#[cfg(feature = "sqlite-comparison")]
fn run_sqlite_query(conn: &SqliteConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.query([]) {
            Ok(mut rows) => {
                while let Ok(Some(_)) = rows.next() {}
                BenchResult::Ok(start.elapsed())
            }
            Err(e) => BenchResult::Error(format!("{}", e)),
        },
        Err(e) => BenchResult::Error(format!("{}", e)),
    }
}

/// Run a query on DuckDB
#[cfg(feature = "duckdb-comparison")]
fn run_duckdb_query(conn: &DuckDBConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.query([]) {
            Ok(mut rows) => {
                while let Ok(Some(_)) = rows.next() {}
                BenchResult::Ok(start.elapsed())
            }
            Err(e) => BenchResult::Error(format!("{}", e)),
        },
        Err(e) => BenchResult::Error(format!("{}", e)),
    }
}

/// Run a query on MySQL
#[cfg(feature = "mysql-comparison")]
fn run_mysql_query(conn: &mut PooledConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.query_iter(sql) {
        Ok(result) => {
            for row_result in result {
                if let Err(e) = row_result {
                    return BenchResult::Error(format!("{}", e));
                }
            }
            BenchResult::Ok(start.elapsed())
        }
        Err(e) => BenchResult::Error(format!("{}", e)),
    }
}

/// Get queries to run based on filter
fn get_queries_to_run(
    all_queries: &[(&'static str, &'static str)],
    filter: &Option<Vec<String>>,
) -> Vec<(&'static str, &'static str)> {
    match filter {
        Some(queries) => all_queries
            .iter()
            .filter(|(name, _)| queries.iter().any(|q| q == *name))
            .copied()
            .collect(),
        None => all_queries.to_vec(),
    }
}

fn print_help(program: &str) {
    eprintln!("TPC-DS Benchmark Runner");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  {} [QUERIES]           Run specific queries (comma-separated)", program);
    eprintln!("  {}                     Run all queries", program);
    eprintln!("  {} --help              Show this help", program);
    eprintln!();
    eprintln!("Environment Variables:");
    eprintln!("  WARMUP_ITERATIONS        Warmup runs per query (default: 3)");
    eprintln!("  BENCHMARK_ITERATIONS     Timed runs per query (default: 10)");
    eprintln!("  BENCHMARK_TIMEOUT_SECS   Timeout per query (default: 30)");
    eprintln!("  SCALE_FACTOR             TPC-DS scale factor (default: 0.001)");
    eprintln!("  QUERY_FILTER             Queries to run (e.g., Q1,Q6,Q9)");
    eprintln!("  VIBESQL_MEMORY_THRESHOLD Memory threshold percentage (default: 80)");
    eprintln!("  TPCDS_ENGINE             Engine selection: sqlite,duckdb,mysql,all");
    eprintln!("  MYSQL_URL                MySQL connection string");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} Q1,Q6               # Run Q1 and Q6", program);
    eprintln!("  QUERY_FILTER=Q1,Q6 {}  # Same, via env var", program);
    eprintln!("  SCALE_FACTOR=0.01 {}   # Use larger dataset", program);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Handle help
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        print_help(&args[0]);
        return;
    }

    eprintln!("=== TPC-DS Benchmark ===");

    // Get configuration
    let scale_factor: f64 = env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SCALE_FACTOR);

    let config = BenchConfig::default();
    let harness = Harness::with_config(config.clone());
    let engine_filter = get_engine_filter();

    eprintln!("Configuration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Warmup iterations: {}", config.warmup_iterations);
    eprintln!("  Benchmark iterations: {}", config.benchmark_iterations);
    eprintln!("  Timeout: {}s", config.timeout.as_secs());
    eprintln!("  Engines: {}", engine_filter.enabled_list());

    // Initialize memory monitor
    let _ = get_memory_monitor();

    // Combine sanity queries and main queries
    let all_queries: Vec<(&str, &str)> =
        TPCDS_SANITY_QUERIES.iter().chain(TPCDS_QUERIES.iter()).copied().collect();

    // Get queries to run based on filter
    let query_filter = get_query_filter();
    let queries = get_queries_to_run(&all_queries, &query_filter);

    if queries.is_empty() {
        eprintln!("No queries to run. Check QUERY_FILTER or command line arguments.");
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    }

    eprintln!("  Queries: {} total", queries.len());
    if query_filter.is_some() {
        eprintln!("  Filtered: {:?}", queries.iter().map(|(n, _)| *n).collect::<Vec<_>>());
    }

    // Collect results for all engines
    let mut all_results: Vec<(&str, Vec<BenchStats>)> = Vec::new();

    // ========================================
    // VibeSQL Benchmark
    // ========================================
    if engine_filter.vibesql {
        eprintln!("\nLoading VibeSQL database (SF {})...", scale_factor);
        let load_start = Instant::now();
        let vibesql_db = load_vibesql(scale_factor);
        eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

        let mut vibesql_results = Vec::new();
        let mut passed = 0;
        let mut skipped = 0;

        eprintln!("\n--- VibeSQL ---");

        for (name, sql) in &queries {
            // Check memory pressure
            if !check_memory_before_query(name) {
                skipped += 1;
                continue;
            }

            let timeout = harness.timeout();
            let stats = harness.run(name, || run_vibesql_query(&vibesql_db, sql, timeout));

            if stats.iterations > 0 {
                stats.print_compact();
                vibesql_results.push(stats);
                passed += 1;
            } else {
                eprintln!("  {} SKIPPED (0 successful iterations)", name);
                skipped += 1;
            }

            // Clear caches periodically to prevent OOM
            clear_in_subquery_cache();
        }

        eprintln!("\nVibeSQL: {} passed, {} skipped", passed, skipped);
        all_results.push(("VibeSQL", vibesql_results));

        // Release memory before loading next engine
        hint_memory_release();
    } else {
        eprintln!("\nSkipping VibeSQL (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // SQLite Benchmark (if feature enabled)
    // ========================================
    #[cfg(feature = "sqlite-comparison")]
    if engine_filter.sqlite {
        eprintln!("\nLoading SQLite database...");
        let load_start = Instant::now();
        let sqlite_conn = load_sqlite(scale_factor);
        eprintln!("SQLite loaded in {:?}", load_start.elapsed());

        let mut sqlite_results = Vec::new();
        let mut sqlite_skipped = 0;
        eprintln!("\n--- SQLite ---");

        for (name, sql) in &queries {
            // Skip queries that use SQL features SQLite doesn't support
            if sqlite_should_skip(name) {
                eprintln!("  {:20} SKIPPED (unsupported SQL feature)", name);
                sqlite_skipped += 1;
                continue;
            }

            if !check_memory_before_query(name) {
                continue;
            }

            let stats = harness.run(name, || run_sqlite_query(&sqlite_conn, sql));
            if stats.iterations > 0 {
                stats.print_compact();
                sqlite_results.push(stats);
            }
        }

        if sqlite_skipped > 0 {
            eprintln!(
                "\nSQLite: {} queries skipped (ROLLUP/CUBE/STDDEV_SAMP not supported)",
                sqlite_skipped
            );
        }
        all_results.push(("SQLite", sqlite_results));
        hint_memory_release();
    } else {
        #[cfg(feature = "sqlite-comparison")]
        eprintln!("\nSkipping SQLite (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // DuckDB Benchmark (if feature enabled)
    // ========================================
    #[cfg(feature = "duckdb-comparison")]
    if engine_filter.duckdb {
        eprintln!("\nLoading DuckDB database...");
        let load_start = Instant::now();
        let duckdb_conn = load_duckdb(scale_factor);
        eprintln!("DuckDB loaded in {:?}", load_start.elapsed());

        let mut duckdb_results = Vec::new();
        eprintln!("\n--- DuckDB ---");

        for (name, sql) in &queries {
            if !check_memory_before_query(name) {
                continue;
            }

            let stats = harness.run(name, || run_duckdb_query(&duckdb_conn, sql));
            if stats.iterations > 0 {
                stats.print_compact();
                duckdb_results.push(stats);
            }
        }
        all_results.push(("DuckDB", duckdb_results));
        hint_memory_release();
    } else {
        #[cfg(feature = "duckdb-comparison")]
        eprintln!("\nSkipping DuckDB (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // MySQL Benchmark (if feature enabled)
    // Note: MySQL is excluded by default (use ENGINE_FILTER=all or ENGINE_FILTER=...,mysql)
    // ========================================
    #[cfg(feature = "mysql-comparison")]
    if engine_filter.mysql {
        if let Some(mut conn) = load_mysql(scale_factor) {
            let mut mysql_results = Vec::new();
            eprintln!("\n--- MySQL ---");

            for (name, sql) in &queries {
                if !check_memory_before_query(name) {
                    continue;
                }

                let stats = harness.run(name, || run_mysql_query(&mut conn, sql));
                if stats.iterations > 0 {
                    stats.print_compact();
                    mysql_results.push(stats);
                }
            }
            all_results.push(("MySQL", mysql_results));
        } else {
            eprintln!("\nSkipping MySQL (MYSQL_URL not set)");
        }
    } else {
        #[cfg(feature = "mysql-comparison")]
        eprintln!("\nSkipping MySQL (filtered out by ENGINE_FILTER)");
    }

    // Print comparison summary
    harness::print_comparison_table(&all_results);

    // Print memory statistics
    if let Ok(mut monitor) = get_memory_monitor().lock() {
        let stats = monitor.current_stats();
        eprintln!("\nMemory Statistics:");
        eprintln!(
            "  Final usage: {} / {} ({:.1}%)",
            format_bytes(stats.used_bytes),
            format_bytes(stats.total_bytes),
            stats.usage_percent
        );
        eprintln!("  High-water mark: {}", format_bytes(monitor.high_water_mark_bytes()));
    }

    eprintln!("\n=== Done ===");
}
