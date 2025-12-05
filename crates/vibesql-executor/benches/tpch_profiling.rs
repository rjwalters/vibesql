//! Comprehensive profiling for all TPC-H queries
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpch_profiling --features benchmark-comparison --no-run && ./target/release/deps/tpch_profiling-*
//!
//! Environment Variables:
//!   QUERY_TIMEOUT_SECS    Timeout per query in seconds (default: 30)
//!   QUERY_FILTER          Comma-separated list of queries to run (e.g., "Q1,Q6,Q9")
//!
//! Run single query (CLI arg):
//!   ./target/release/deps/tpch_profiling-* Q1
//!   ./target/release/deps/tpch_profiling-* Q2
//!
//! Run filtered queries (env var):
//!   QUERY_FILTER=Q1,Q6 ./target/release/deps/tpch_profiling-*
//!
//! Run all queries (default):
//!   ./target/release/deps/tpch_profiling-*

mod tpch;

use std::collections::HashSet;
use std::env;
use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

fn run_query_detailed(db: &vibesql_storage::Database, name: &str, sql: &str, timeout: Duration) {
    eprintln!("\n=== {} ===", name);
    eprintln!(
        "SQL: {}",
        sql.trim()
            .lines()
            .take(3)
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(80)
            .collect::<String>()
    );

    // Parse
    let parse_start = Instant::now();
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => {
            eprintln!("ERROR: Not a SELECT");
            return;
        }
        Err(e) => {
            eprintln!("ERROR: Parse error: {}", e);
            return;
        }
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
    eprintln!("=== TPC-H Query Profiling ===");

    // Get timeout from env (default 30s)
    let timeout_secs: u64 =
        env::var("QUERY_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let timeout = Duration::from_secs(timeout_secs);
    eprintln!("Per-query timeout: {}s (set QUERY_TIMEOUT_SECS to change)", timeout_secs);

    // All 22 TPC-H queries
    let all_queries: Vec<(&str, &str)> = vec![
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

    // Check for single-query mode
    let args: Vec<String> = env::args().collect();

    // Handle help flag
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [QUERY]", args[0]);
        eprintln!("\nArguments:");
        eprintln!("  QUERY    Optional query to run (Q1-Q22). If not specified, runs all queries.");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  SCALE_FACTOR              Scale factor for data size (default: 0.01)");
        eprintln!("  QUERY_TIMEOUT_SECS        Timeout per query in seconds (default: 30)");
        eprintln!("  QUERY_FILTER              Comma-separated list of queries (e.g., Q1,Q6,Q9)");
        eprintln!("  JOIN_REORDER_VERBOSE      Enable verbose join reordering logs");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run all 22 queries", args[0]);
        eprintln!("  {} Q9                        # Run only Q9", args[0]);
        eprintln!("  QUERY_FILTER=Q1,Q6 {}        # Run Q1 and Q6", args[0]);
        eprintln!("  QUERY_TIMEOUT_SECS=60 {} Q9  # Run Q9 with 60s timeout", args[0]);
        std::process::exit(0);
    }

    // Query filter from env var (comma-separated, e.g., "Q1,Q6,Q9")
    let query_filter: HashSet<String> = env::var("QUERY_FILTER")
        .ok()
        .map(|s| s.split(',').map(|q| q.trim().to_uppercase()).collect())
        .unwrap_or_default();

    let queries_to_run = if args.len() > 1 {
        // CLI arg takes precedence: run only specified query
        let target_query = &args[1];
        eprintln!("Single-query mode: {}", target_query);
        all_queries.into_iter().filter(|(name, _)| *name == target_query).collect()
    } else if !query_filter.is_empty() {
        // Env var filter: run multiple specified queries
        eprintln!("Query filter mode: {:?}", query_filter);
        all_queries
            .into_iter()
            .filter(|(name, _)| query_filter.contains(&name.to_uppercase()))
            .collect()
    } else {
        // Run all queries
        eprintln!("Running all 22 queries");
        all_queries
    };

    if queries_to_run.is_empty() {
        if args.len() > 1 {
            eprintln!("Error: Query '{}' not found. Valid queries: Q1-Q22", args[1]);
        } else {
            eprintln!(
                "Error: No matching queries for filter {:?}. Valid queries: Q1-Q22",
                query_filter
            );
        }
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    }

    // Get scale factor from env (default 0.01)
    let scale_factor: f64 =
        env::var("SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    // Load database
    eprintln!("\nLoading TPC-H database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Run selected queries
    for (name, sql) in &queries_to_run {
        run_query_detailed(&db, name, sql, timeout);
    }

    if queries_to_run.len() == 1 {
        eprintln!("\n=== Done - Single Query ===");
    } else {
        eprintln!("\n=== Done - All 22 TPC-H Queries ===");
    }
}
