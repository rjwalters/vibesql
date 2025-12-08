//! Lineitem Scan Profiling Benchmark
//!
//! This benchmark isolates lineitem table scan performance to understand:
//! - Pure scan overhead (no predicates, no aggregation)
//! - Date predicate filtering cost
//! - Comparison with DuckDB equivalent operations
//!
//! Migrated from Criterion to custom harness for deterministic timing.
//!
//! Usage:
//!   cargo bench --bench lineitem_scan_profiling
//!   cargo bench --bench lineitem_scan_profiling --features duckdb-comparison
//!
//! Environment variables:
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)
//!   SCALE_FACTOR - TPC-H scale factor (default: 0.01)
//!
//! Part of issue #2962: Profile lineitem table scan performance

mod harness;
mod tpch;

use harness::{print_group_header, print_summary_table, BenchResult, Harness};
use std::hint::black_box;
use std::time::Instant;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

use tpch::schema::*;

// =============================================================================
// Query Constants
// =============================================================================

/// Pure scan - no predicates, no aggregation
const LINEITEM_FULL_SCAN: &str = "SELECT * FROM lineitem";

/// Simple COUNT(*) - minimal processing
const LINEITEM_COUNT: &str = "SELECT COUNT(*) FROM lineitem";

/// Date predicate only (same as Q1)
const LINEITEM_DATE_FILTER: &str = "SELECT * FROM lineitem WHERE l_shipdate <= '1998-09-01'";

/// Date predicate with COUNT
const LINEITEM_DATE_COUNT: &str = "SELECT COUNT(*) FROM lineitem WHERE l_shipdate <= '1998-09-01'";

/// Single column projection
const LINEITEM_SINGLE_COLUMN: &str = "SELECT l_orderkey FROM lineitem";

/// Two columns projection
const LINEITEM_TWO_COLUMNS: &str = "SELECT l_orderkey, l_quantity FROM lineitem";

/// LIMIT scan (measure startup cost)
const LINEITEM_LIMIT_100: &str = "SELECT * FROM lineitem LIMIT 100";

/// LIMIT scan with date filter
const LINEITEM_DATE_LIMIT_100: &str =
    "SELECT * FROM lineitem WHERE l_shipdate <= '1998-09-01' LIMIT 100";

// =============================================================================
// VibeSQL Benchmark Functions
// =============================================================================

fn run_vibesql_benchmark(
    harness: &Harness,
    name: &str,
    db: &vibesql_storage::Database,
    sql: &str,
) -> harness::BenchStats {
    harness.run(name, || {
        let start = Instant::now();
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Select(select) = stmt {
            let executor = SelectExecutor::new(db);
            match executor.execute(&select) {
                Ok(result) => {
                    black_box(result.len());
                    BenchResult::Ok(start.elapsed())
                }
                Err(e) => BenchResult::Error(e.to_string()),
            }
        } else {
            BenchResult::Error("Not a SELECT statement".to_string())
        }
    })
}

// =============================================================================
// DuckDB Benchmark Functions
// =============================================================================

#[cfg(feature = "duckdb-comparison")]
fn run_duckdb_benchmark(
    harness: &Harness,
    name: &str,
    conn: &duckdb::Connection,
    sql: &str,
) -> harness::BenchStats {
    harness.run(name, || {
        let start = Instant::now();
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        black_box(count);
        BenchResult::Ok(start.elapsed())
    })
}

// =============================================================================
// Benchmark Runner
// =============================================================================

fn main() {
    eprintln!("\n=== Lineitem Scan Profiling Benchmarks ===\n");

    let scale_factor: f64 = std::env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);

    eprintln!("Scale factor: {}", scale_factor);

    let harness = Harness::new();

    // Load VibeSQL database
    eprintln!("\nLoading VibeSQL TPC-H data...");
    let db = load_vibesql(scale_factor);
    let row_count = db.get_table("lineitem").map(|t| t.row_count()).unwrap_or(0);
    eprintln!("Loaded {} lineitem rows\n", row_count);

    // Define benchmarks
    let benchmarks = [
        ("full_scan", LINEITEM_FULL_SCAN),
        ("count", LINEITEM_COUNT),
        ("date_filter", LINEITEM_DATE_FILTER),
        ("date_count", LINEITEM_DATE_COUNT),
        ("single_column", LINEITEM_SINGLE_COLUMN),
        ("two_columns", LINEITEM_TWO_COLUMNS),
        ("limit_100", LINEITEM_LIMIT_100),
        ("date_limit_100", LINEITEM_DATE_LIMIT_100),
    ];

    // Run VibeSQL benchmarks
    print_group_header("VibeSQL Lineitem Scans");
    let mut vibesql_results = Vec::new();
    for (name, sql) in &benchmarks {
        let stats = run_vibesql_benchmark(&harness, name, &db, sql);
        stats.print();
        vibesql_results.push(stats);
    }

    // Run DuckDB benchmarks if feature is enabled
    #[cfg(feature = "duckdb-comparison")]
    {
        eprintln!("\nLoading DuckDB TPC-H data...");
        let conn = load_duckdb(scale_factor);

        print_group_header("DuckDB Lineitem Scans");
        let mut duckdb_results = Vec::new();
        for (name, sql) in &benchmarks {
            let stats = run_duckdb_benchmark(&harness, name, &conn, sql);
            stats.print();
            duckdb_results.push(stats);
        }

        // Print comparison summary
        harness::print_comparison_table(&[("VibeSQL", vibesql_results), ("DuckDB", duckdb_results)]);
    }

    #[cfg(not(feature = "duckdb-comparison"))]
    {
        print_summary_table("VibeSQL", &vibesql_results);
    }

    eprintln!("\n=== Benchmark Complete ===\n");
}
