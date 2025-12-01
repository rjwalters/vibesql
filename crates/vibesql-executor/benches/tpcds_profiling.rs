//! TPC-DS Query Profiling
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpcds_profiling --features benchmark-comparison --no-run && ./target/release/deps/tpcds_profiling-*
//!
//! Run single query:
//!   ./target/release/deps/tpcds_profiling-* Q2
//!
//! Run all queries:
//!   ./target/release/deps/tpcds_profiling-*

mod tpcds;

use std::env;
use std::time::{Duration, Instant};
use tpcds::queries::TPCDS_QUERIES;
use tpcds::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

fn run_query_detailed(db: &vibesql_storage::Database, name: &str, sql: &str, timeout: Duration) {
    eprintln!("\n=== {} ===", name);
    eprintln!("SQL: {}", sql.trim().lines().take(3).collect::<Vec<_>>().join(" ").chars().take(80).collect::<String>());

    // Parse
    let parse_start = Instant::now();
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => { eprintln!("ERROR: Not a SELECT"); return; }
        Err(e) => { eprintln!("ERROR: Parse error: {}", e); return; }
    };
    let parse_time = parse_start.elapsed();
    eprintln!("  Parse:    {:>10.2?}", parse_time);

    // Create executor with timeout
    let exec_create_start = Instant::now();
    let executor = SelectExecutor::new(db).with_timeout(timeout.as_secs());
    let exec_create_time = exec_create_start.elapsed();
    eprintln!("  Executor: {:>10.2?} (timeout: {:?})", exec_create_time, timeout);

    // Execute query directly (executor has built-in timeout)
    let execute_start = Instant::now();
    let result = executor.execute(&stmt);
    let execute_time = execute_start.elapsed();

    match result {
        Ok(rows) => {
            eprintln!("  Execute:  {:>10.2?} ({} rows)", execute_time, rows.len());
            let total = parse_time + exec_create_time + execute_time;
            eprintln!("  TOTAL:    {:>10.2?}", total);
        }
        Err(e) => {
            eprintln!("  Execute:  {:>10.2?} ERROR: {}", execute_time, e);
            if execute_time >= timeout {
                eprintln!("  TOTAL:    TIMEOUT (>{}s)", timeout.as_secs());
            }
        }
    }
}

fn main() {
    eprintln!("=== TPC-DS Query Profiling ===");

    // Get timeout from env (default 30s)
    let timeout_secs: u64 = env::var("QUERY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let timeout = Duration::from_secs(timeout_secs);
    eprintln!("Per-query timeout: {}s (set QUERY_TIMEOUT_SECS to change)", timeout_secs);

    // Get scale factor from env (default 0.01)
    let scale_factor: f64 = env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);
    eprintln!("Scale factor: {} (set SCALE_FACTOR to change)", scale_factor);

    // All TPC-DS queries
    let all_queries: Vec<(&str, &str)> = TPCDS_QUERIES.to_vec();

    // Check for single-query mode
    let args: Vec<String> = env::args().collect();

    // Handle help flag
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [QUERY]", args[0]);
        eprintln!("\nArguments:");
        eprintln!("  QUERY    Optional query to run (Q1-Q99). If not specified, runs all queries.");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  QUERY_TIMEOUT_SECS        Timeout per query in seconds (default: 30)");
        eprintln!("  SCALE_FACTOR              TPC-DS scale factor (default: 0.01)");
        eprintln!("\nExamples:");
        eprintln!("  {}                          # Run all queries", args[0]);
        eprintln!("  {} Q2                       # Run only Q2", args[0]);
        eprintln!("  SCALE_FACTOR=0.01 {} Q2     # Run Q2 at scale 0.01", args[0]);
        std::process::exit(0);
    }

    let queries_to_run = if args.len() > 1 {
        // Run only specified query
        let target_query = &args[1];
        eprintln!("Single-query mode: {}", target_query);
        all_queries.into_iter()
            .filter(|(name, _)| *name == target_query)
            .collect()
    } else {
        // Run all queries
        eprintln!("Running all {} queries", all_queries.len());
        all_queries
    };

    if queries_to_run.is_empty() {
        eprintln!("Error: Query '{}' not found.", args[1]);
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    }

    // Load database
    eprintln!("\nLoading TPC-DS database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Run selected queries
    for (name, sql) in &queries_to_run {
        run_query_detailed(&db, name, sql, timeout);
    }

    eprintln!("\n=== Profiling Complete ===");
}
