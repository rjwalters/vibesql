// ============================================================================
// ⚠️  BENCHMARK INTEGRITY WARNING ⚠️
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. Benchmarks MUST execute actual SQL to produce
// meaningful results. "Optimizing" benchmarks this way is cheating.
// ============================================================================

//! Comprehensive profiling for all TPC-H queries
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpch_profiling --features benchmark-comparison --no-run && ./target/release/deps/tpch_profiling-*
//!
//! Environment Variables:
//!   QUERY_TIMEOUT_SECS    Timeout per query in seconds (default: 30)
//!   QUERY_FILTER          Comma-separated list of queries to run (e.g., "Q1,Q6,Q9")
//!   PARALLEL_QUERIES      Control parallelism: 0=disable, 1-16=force N workers (default: auto)
//!   PARALLEL_DEBUG        Set to 1 to show parallelism decision details
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
//!
//! Parallel execution (auto-enabled when running multiple queries):
//!   Parallelism is automatically determined based on available memory:
//!   - >70% free: 8 workers
//!   - 50-70% free: 4 workers
//!   - 30-50% free: 2 workers
//!   - <30% free: sequential
//!
//!   PARALLEL_QUERIES=0 ./target/release/deps/tpch_profiling-*  # Force sequential
//!   PARALLEL_QUERIES=4 ./target/release/deps/tpch_profiling-*  # Force 4 workers

mod memory_monitor;
mod tpch;

use std::collections::HashSet;
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tpch::queries::*;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

use memory_monitor::compute_parallelism;

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
        eprintln!("  PARALLEL_QUERIES          Control parallelism: 0=disable, 1-16=force N (default: auto)");
        eprintln!("  PARALLEL_DEBUG            Set to 1 to show parallelism decision details");
        eprintln!("  JOIN_REORDER_VERBOSE      Enable verbose join reordering logs");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run all 22 queries (parallel)", args[0]);
        eprintln!("  {} Q9                        # Run only Q9 (sequential)", args[0]);
        eprintln!("  QUERY_FILTER=Q1,Q6 {}        # Run Q1 and Q6 (parallel)", args[0]);
        eprintln!("  PARALLEL_QUERIES=0 {}        # Run all queries sequentially", args[0]);
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

    // Determine parallelism level based on available memory
    let parallelism = compute_parallelism();
    let parallel_enabled = queries_to_run.len() > 1 && parallelism > 1;

    eprintln!(
        "Parallelism: {} workers (set PARALLEL_QUERIES=0 to disable, PARALLEL_DEBUG=1 for details)",
        if parallel_enabled { parallelism } else { 1 }
    );

    // Load database
    eprintln!("\nLoading TPC-H database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Run queries (parallel or sequential based on configuration)
    let overall_start = Instant::now();

    if parallel_enabled {
        // Run queries in parallel using rayon thread pool
        use rayon::prelude::*;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism)
            .build()
            .expect("Failed to create thread pool");

        // Collect output in order using a mutex
        let output_buffer: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));

        pool.install(|| {
            queries_to_run.par_iter().enumerate().for_each(|(idx, (name, sql))| {
                // Capture output for this query
                let mut output = String::new();
                output.push_str(&format!("\n=== {} ===\n", name));
                output.push_str(&format!(
                    "SQL: {}\n",
                    sql.trim()
                        .lines()
                        .take(3)
                        .collect::<Vec<_>>()
                        .join(" ")
                        .chars()
                        .take(80)
                        .collect::<String>()
                ));

                // Parse
                let parse_start = Instant::now();
                let stmt = match Parser::parse_sql(sql) {
                    Ok(vibesql_ast::Statement::Select(s)) => s,
                    Ok(_) => {
                        output.push_str("ERROR: Not a SELECT\n");
                        output_buffer.lock().unwrap().push((idx, output));
                        return;
                    }
                    Err(e) => {
                        output.push_str(&format!("ERROR: Parse error: {}\n", e));
                        output_buffer.lock().unwrap().push((idx, output));
                        return;
                    }
                };
                let parse_time = parse_start.elapsed();
                output.push_str(&format!("  Parse:    {:>10.2?}\n", parse_time));

                // Create executor with timeout
                let exec_create_start = Instant::now();
                let executor = SelectExecutor::new(&db).with_timeout(timeout.as_secs());
                let exec_create_time = exec_create_start.elapsed();
                output.push_str(&format!(
                    "  Executor: {:>10.2?} (timeout: {:?})\n",
                    exec_create_time, timeout
                ));

                // Execute query
                let execute_start = Instant::now();
                let result = executor.execute(&stmt);
                let execute_time = execute_start.elapsed();

                match result {
                    Ok(rows) => {
                        output.push_str(&format!(
                            "  Execute:  {:>10.2?} ({} rows)\n",
                            execute_time,
                            rows.len()
                        ));
                        let total = parse_time + exec_create_time + execute_time;
                        output.push_str(&format!("  TOTAL:    {:>10.2?}\n", total));
                    }
                    Err(e) => {
                        output.push_str(&format!("  Execute:  {:>10.2?} ERROR: {}\n", execute_time, e));
                        if execute_time >= timeout {
                            output.push_str(&format!("  TOTAL:    TIMEOUT (>{}s)\n", timeout.as_secs()));
                        }
                    }
                }

                output_buffer.lock().unwrap().push((idx, output));
            });
        });

        // Print output in order
        let mut outputs = output_buffer.lock().unwrap();
        outputs.sort_by_key(|(idx, _)| *idx);
        for (_, output) in outputs.iter() {
            eprint!("{}", output);
        }
    } else {
        // Run queries sequentially (single query or parallelism disabled)
        for (name, sql) in &queries_to_run {
            run_query_detailed(&db, name, sql, timeout);
        }
    }

    let overall_elapsed = overall_start.elapsed();

    if queries_to_run.len() == 1 {
        eprintln!("\n=== Done - Single Query ===");
    } else {
        eprintln!("\n=== Done - {} TPC-H Queries in {:?} ===", queries_to_run.len(), overall_elapsed);
        if parallel_enabled {
            eprintln!("(Executed with {} parallel workers)", parallelism);
        }
    }
}
