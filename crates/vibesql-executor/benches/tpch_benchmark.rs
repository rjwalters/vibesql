//! TPC-H Benchmark Runner
//!
//! This benchmark compares TPC-H query performance across multiple database engines.
//! Uses a custom timing harness instead of Criterion for deterministic, fast execution.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL + SQLite)
//! cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison --no-run
//! ./target/release/deps/tpch_benchmark-*
//!
//! # With DuckDB comparison
//! cargo bench --package vibesql-executor --bench tpch_benchmark --features benchmark-comparison,duckdb-comparison --no-run
//! ./target/release/deps/tpch_benchmark-*
//!
//! # With MySQL comparison
//! MYSQL_URL=mysql://user:pass@localhost:3306/tpch ./target/release/deps/tpch_benchmark-*
//!
//! # Run only specific engines (runtime filtering)
//! ENGINE_FILTER=vibesql ./target/release/deps/tpch_benchmark-*
//! ENGINE_FILTER=vibesql,sqlite ./target/release/deps/tpch_benchmark-*
//! ```
//!
//! ## Environment Variables
//!
//! - `ENGINE_FILTER` - Comma-separated list of engines to run (e.g., "vibesql,sqlite")
//!   Valid values: vibesql, sqlite, duckdb, mysql (default: vibesql,sqlite,duckdb)
//!   Note: MySQL is excluded by default (client-server database). Use ENGINE_FILTER=all to include.
//! - `WARMUP_ITERATIONS` - Number of warmup runs per query (default: 3)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs per query (default: 10)
//! - `BENCHMARK_TIMEOUT_SECS` - Timeout per query (default: 30)
//! - `SCALE_FACTOR` - TPC-H scale factor (default: 0.01)
//! - `QUERY_FILTER` - Comma-separated list of queries to run (e.g., "Q1,Q6,Q9")
//! - `MYSQL_URL` - MySQL connection string (optional)
//!
//! ## Query Selection
//!
//! ```bash
//! ./target/release/deps/tpch_benchmark-* Q1,Q6      # Run only Q1 and Q6
//! QUERY_FILTER=Q1,Q6,Q9 ./target/release/deps/tpch_benchmark-*
//! ./target/release/deps/tpch_benchmark-*            # Run all queries
//! ```
//!
//! ## Engine Selection (Runtime Filtering)
//!
//! The ENGINE_FILTER environment variable allows running only specific engines
//! without recompiling. This eliminates redundant data loading and speeds up
//! targeted benchmarks.
//!
//! ```bash
//! ENGINE_FILTER=vibesql ./target/release/deps/tpch_benchmark-*    # VibeSQL only
//! ENGINE_FILTER=sqlite ./target/release/deps/tpch_benchmark-*     # SQLite only
//! ENGINE_FILTER=vibesql,duckdb ./target/release/deps/tpch_benchmark-*  # Multiple engines
//! ```

mod harness;
mod tpch;

use harness::{BenchConfig, BenchResult, BenchStats, EngineFilter, Harness};
use std::env;
use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::PooledConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;
#[cfg(feature = "duckdb-comparison")]
use tpch::schema::load_duckdb;
#[cfg(feature = "mysql-comparison")]
use tpch::schema::load_mysql;
#[cfg(feature = "benchmark-comparison")]
use tpch::schema::load_sqlite;

/// All available TPC-H queries (standard SQL with EXTRACT)
const ALL_QUERIES: &[(&str, &str)] = &[
    ("Q1", TPCH_Q1),
    ("Q2", TPCH_Q2),
    ("Q3", TPCH_Q3),
    ("Q4", TPCH_Q4),
    ("Q5", TPCH_Q5),
    ("Q6", TPCH_Q6),
    ("Q7", TPCH_Q7),
    ("Q8", TPCH_Q8),
    ("Q9", TPCH_Q9),
    ("Q10", TPCH_Q10),
    ("Q11", TPCH_Q11),
    ("Q12", TPCH_Q12),
    ("Q13", TPCH_Q13),
    ("Q14", TPCH_Q14),
    ("Q15", TPCH_Q15),
    ("Q16", TPCH_Q16),
    ("Q17", TPCH_Q17),
    ("Q18", TPCH_Q18),
    ("Q19", TPCH_Q19),
    ("Q20", TPCH_Q20),
    ("Q21", TPCH_Q21),
    ("Q22", TPCH_Q22),
];

/// SQLite-specific TPC-H queries (uses strftime instead of EXTRACT)
#[cfg(feature = "benchmark-comparison")]
const ALL_QUERIES_SQLITE: &[(&str, &str)] = &[
    ("Q1", TPCH_Q1),
    ("Q2", TPCH_Q2),
    ("Q3", TPCH_Q3),
    ("Q4", TPCH_Q4),
    ("Q5", TPCH_Q5),
    ("Q6", TPCH_Q6),
    ("Q7", TPCH_Q7_SQLITE),
    ("Q8", TPCH_Q8_SQLITE),
    ("Q9", TPCH_Q9_SQLITE),
    ("Q10", TPCH_Q10),
    ("Q11", TPCH_Q11),
    ("Q12", TPCH_Q12),
    ("Q13", TPCH_Q13),
    ("Q14", TPCH_Q14),
    ("Q15", TPCH_Q15),
    ("Q16", TPCH_Q16),
    ("Q17", TPCH_Q17),
    ("Q18", TPCH_Q18),
    ("Q19", TPCH_Q19),
    ("Q20", TPCH_Q20),
    ("Q21", TPCH_Q21),
    ("Q22", TPCH_Q22),
];

/// Parse query filter from environment or CLI args
fn get_query_filter() -> Option<Vec<String>> {
    // Check CLI args first
    let args: Vec<String> = env::args().collect();
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();

    if !user_args.is_empty() && user_args[0] != "help" && user_args[0] != "--help" {
        // Parse comma-separated query list from CLI
        return Some(
            user_args[0]
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
        );
    }

    // Fall back to environment variable
    env::var("QUERY_FILTER").ok().map(|s| {
        s.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect()
    })
}

/// Get queries to run based on filter
fn get_queries_to_run(filter: &Option<Vec<String>>) -> Vec<(&'static str, &'static str)> {
    match filter {
        Some(queries) => ALL_QUERIES
            .iter()
            .filter(|(name, _)| queries.iter().any(|q| q == *name))
            .copied()
            .collect(),
        None => ALL_QUERIES.to_vec(),
    }
}

/// Get SQLite-specific queries to run based on filter
#[cfg(feature = "benchmark-comparison")]
fn get_sqlite_queries_to_run(filter: &Option<Vec<String>>) -> Vec<(&'static str, &'static str)> {
    match filter {
        Some(queries) => ALL_QUERIES_SQLITE
            .iter()
            .filter(|(name, _)| queries.iter().any(|q| q == *name))
            .copied()
            .collect(),
        None => ALL_QUERIES_SQLITE.to_vec(),
    }
}

/// Run a query on VibeSQL and return the execution time
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

/// Run a query on SQLite and return the execution time
#[cfg(feature = "benchmark-comparison")]
fn run_sqlite_query(conn: &SqliteConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.query([]) {
            Ok(mut rows) => {
                // Consume all rows to ensure complete execution
                while let Ok(Some(_)) = rows.next() {}
                BenchResult::Ok(start.elapsed())
            }
            Err(e) => BenchResult::Error(format!("{}", e)),
        },
        Err(e) => BenchResult::Error(format!("{}", e)),
    }
}

/// Run a query on DuckDB and return the execution time
#[cfg(feature = "duckdb-comparison")]
fn run_duckdb_query(conn: &DuckDBConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.prepare(sql) {
        Ok(mut stmt) => match stmt.query([]) {
            Ok(mut rows) => {
                // Consume all rows to ensure complete execution
                while let Ok(Some(_)) = rows.next() {}
                BenchResult::Ok(start.elapsed())
            }
            Err(e) => BenchResult::Error(format!("{}", e)),
        },
        Err(e) => BenchResult::Error(format!("{}", e)),
    }
}

/// Run a query on MySQL and return the execution time
#[cfg(feature = "mysql-comparison")]
fn run_mysql_query(conn: &mut PooledConn, sql: &str) -> BenchResult {
    let start = Instant::now();
    match conn.query_iter(sql) {
        Ok(result) => {
            // Consume all rows to ensure complete execution
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

fn print_help(program: &str) {
    eprintln!("TPC-H Benchmark Runner");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  {} [QUERIES]           Run specific queries (comma-separated)", program);
    eprintln!("  {}                     Run all queries", program);
    eprintln!("  {} --help              Show this help", program);
    eprintln!();
    eprintln!("Environment Variables:");
    eprintln!("  ENGINE_FILTER          Engines to run (e.g., vibesql,sqlite)");
    eprintln!("                         Valid: vibesql, sqlite, duckdb, mysql, all");
    eprintln!("  WARMUP_ITERATIONS      Warmup runs per query (default: 3)");
    eprintln!("  BENCHMARK_ITERATIONS   Timed runs per query (default: 10)");
    eprintln!("  BENCHMARK_TIMEOUT_SECS Timeout per query (default: 30)");
    eprintln!("  SCALE_FACTOR           TPC-H scale factor (default: 0.01)");
    eprintln!("  QUERY_FILTER           Queries to run (e.g., Q1,Q6,Q9)");
    eprintln!("  MYSQL_URL              MySQL connection string");
    eprintln!();
    eprintln!("Available Queries:");
    for (name, _) in ALL_QUERIES {
        eprintln!("  {}", name);
    }
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  {} Q1,Q6                    # Run Q1 and Q6", program);
    eprintln!("  QUERY_FILTER=Q1,Q6 {}       # Same, via env var", program);
    eprintln!("  SCALE_FACTOR=0.1 {}         # Use larger dataset", program);
    eprintln!("  ENGINE_FILTER=vibesql {}    # VibeSQL only (skip other engines)", program);
    eprintln!("  ENGINE_FILTER=sqlite {}     # SQLite only", program);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Handle help
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        print_help(&args[0]);
        return;
    }

    eprintln!("=== TPC-H Benchmark ===");

    // Get configuration
    let scale_factor: f64 =
        env::var("SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    let config = BenchConfig::default();
    let harness = Harness::with_config(config.clone());

    // Parse engine filter for runtime selection
    // Embedded benchmarks default to embedded databases only (no MySQL)
    let engine_filter = EngineFilter::from_env_embedded();

    eprintln!("Configuration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Warmup iterations: {}", config.warmup_iterations);
    eprintln!("  Benchmark iterations: {}", config.benchmark_iterations);
    eprintln!("  Timeout: {}s", config.timeout.as_secs());
    eprintln!("  Engines: {}", engine_filter.enabled_list());

    // Get queries to run
    let query_filter = get_query_filter();
    let queries = get_queries_to_run(&query_filter);

    if queries.is_empty() {
        eprintln!("No queries to run. Check QUERY_FILTER or command line arguments.");
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    }

    eprintln!("  Queries: {:?}", queries.iter().map(|(n, _)| *n).collect::<Vec<_>>());

    // Collect results for all engines
    let mut all_results: Vec<(&str, Vec<BenchStats>)> = Vec::new();

    // ========================================
    // VibeSQL Benchmark (if enabled in filter)
    // ========================================
    if engine_filter.vibesql {
        eprintln!("\nLoading VibeSQL database (SF {})...", scale_factor);
        let load_start = Instant::now();
        let vibesql_db = load_vibesql(scale_factor);
        eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

        let mut vibesql_results = Vec::new();
        eprintln!("\n--- VibeSQL ---");

        for (name, sql) in &queries {
            let timeout = harness.timeout();
            let stats = harness.run(name, || run_vibesql_query(&vibesql_db, sql, timeout));
            stats.print_compact();
            vibesql_results.push(stats);
        }
        all_results.push(("VibeSQL", vibesql_results));
    } else {
        eprintln!("\nSkipping VibeSQL (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // SQLite Benchmark (if feature enabled and in filter)
    // ========================================
    #[cfg(feature = "benchmark-comparison")]
    if engine_filter.sqlite {
        eprintln!("\nLoading SQLite database...");
        let load_start = Instant::now();
        let sqlite_conn = load_sqlite(scale_factor);
        eprintln!("SQLite loaded in {:?}", load_start.elapsed());

        // Use SQLite-specific queries (strftime instead of EXTRACT)
        let sqlite_queries = get_sqlite_queries_to_run(&query_filter);
        let mut sqlite_results = Vec::new();
        eprintln!("\n--- SQLite ---");

        for (name, sql) in &sqlite_queries {
            let stats = harness.run(name, || run_sqlite_query(&sqlite_conn, sql));
            stats.print_compact();
            sqlite_results.push(stats);
        }
        all_results.push(("SQLite", sqlite_results));
    } else {
        eprintln!("\nSkipping SQLite (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // DuckDB Benchmark (if feature enabled and in filter)
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
            let stats = harness.run(name, || run_duckdb_query(&duckdb_conn, sql));
            stats.print_compact();
            duckdb_results.push(stats);
        }
        all_results.push(("DuckDB", duckdb_results));
    } else {
        eprintln!("\nSkipping DuckDB (filtered out by ENGINE_FILTER)");
    }

    // ========================================
    // MySQL Benchmark (if feature enabled, in filter, and MYSQL_URL set)
    // ========================================
    #[cfg(feature = "mysql-comparison")]
    if engine_filter.mysql {
        if let Some(mut conn) = load_mysql(scale_factor) {
            let mut mysql_results = Vec::new();
            eprintln!("\n--- MySQL ---");

            for (name, sql) in &queries {
                let stats = harness.run(name, || run_mysql_query(&mut conn, sql));
                stats.print_compact();
                mysql_results.push(stats);
            }
            all_results.push(("MySQL", mysql_results));
        } else {
            eprintln!("\nSkipping MySQL (MYSQL_URL not set)");
        }
    } else {
        eprintln!("\nSkipping MySQL (filtered out by ENGINE_FILTER)");
    }

    // Print comparison summary
    harness::print_comparison_table(&all_results);

    eprintln!("\n=== Done ===");
}
