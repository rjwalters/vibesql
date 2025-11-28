//! TPC-DS Benchmark Runner
//!
//! Run all TPC-DS queries once and report timing for each query.
//! Unlike the criterion benchmark, this runs each query only once to capture
//! a complete snapshot of query performance across the entire suite.
//!
//! Usage:
//!   cargo run --release --bench tpcds_runner
//!   SCALE_FACTOR=0.001 cargo run --release --bench tpcds_runner  # Smaller dataset
//!   SKIP_SLOW=1 cargo run --release --bench tpcds_runner  # Skip known slow queries

mod tpcds;

use std::collections::HashSet;
use std::time::{Duration, Instant};
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

fn main() {
    println!("=== TPC-DS Benchmark Runner ===\n");

    // Get scale factor from environment or use default
    let scale_factor: f64 = std::env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);

    // Check if we should skip slow queries
    let skip_slow = std::env::var("SKIP_SLOW").is_ok();
    let slow_queries: HashSet<&str> = if skip_slow {
        SLOW_QUERIES.iter().copied().collect()
    } else {
        HashSet::new()
    };

    println!("Scale factor: {}", scale_factor);
    if skip_slow {
        println!("Skipping {} known slow queries", SLOW_QUERIES.len());
    }

    // Load data
    println!("Loading TPC-DS data...");
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    let load_time = load_start.elapsed();
    println!("Data loaded in {:?}\n", load_time);

    println!("Running {} TPC-DS queries...\n", TPCDS_QUERIES.len());
    println!(
        "{:<8} {:>12} {:>10} {:>10}",
        "Query", "Time (ms)", "Rows", "Status"
    );
    println!("{}", "-".repeat(45));

    let mut results: Vec<(String, Option<Duration>, usize, String)> = Vec::new();
    let mut total_time = Duration::ZERO;
    let mut success_count = 0;
    let mut error_count = 0;
    let mut skipped_count = 0;

    for (name, sql) in TPCDS_QUERIES {
        // Check if query should be skipped
        if slow_queries.contains(name) {
            skipped_count += 1;
            results.push((name.to_string(), None, 0, "SKIPPED (slow)".to_string()));
            println!("{:<8} {:>12} {:>10} {}", name, "-", "-", "SKIPPED");
            continue;
        }

        let start = Instant::now();

        // Parse the query
        let stmt = match Parser::parse_sql(sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err_msg = format!("Parse error: {:?}", e);
                println!("{:<8} {:>12} {:>10} {}", name, "-", "-", "PARSE_ERR");
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
                    results.push((name.to_string(), Some(elapsed), row_count, "OK".to_string()));
                    println!(
                        "{:<8} {:>12.2} {:>10} {}",
                        name,
                        elapsed.as_secs_f64() * 1000.0,
                        row_count,
                        "OK"
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
                    results.push((
                        name.to_string(),
                        Some(elapsed),
                        0,
                        format!("Error: {}", err_msg),
                    ));
                    println!(
                        "{:<8} {:>12.2} {:>10} {}",
                        name,
                        elapsed.as_secs_f64() * 1000.0,
                        "-",
                        format!("ERROR: {}", short_err)
                    );
                }
            }
        } else {
            error_count += 1;
            results.push((name.to_string(), None, 0, "Not a SELECT".to_string()));
            println!("{:<8} {:>12} {:>10} {}", name, "-", "-", "NOT_SELECT");
        }
    }

    println!("\n{}", "=".repeat(45));
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
