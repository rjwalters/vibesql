//! Concurrent Query Throughput Benchmark
//!
//! This benchmark measures and validates concurrent read query throughput using
//! a shared database with RwLock. It demonstrates that concurrent reads actually
//! execute in parallel.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run
//! cargo bench --package vibesql-executor --bench concurrent_throughput --no-run
//! ./target/release/deps/concurrent_throughput-*
//!
//! # Run with specific concurrency level
//! CONCURRENCY=8 ./target/release/deps/concurrent_throughput-*
//!
//! # Run with custom TPC-H scale factor
//! SCALE_FACTOR=0.1 ./target/release/deps/concurrent_throughput-*
//! ```
//!
//! ## Environment Variables
//!
//! - `SCALE_FACTOR` - TPC-H scale factor (default: 0.1)
//! - `CONCURRENCY` - Number of concurrent query tasks (default: CPU count)
//! - `WARMUP_ITERATIONS` - Number of warmup runs (default: 2)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs (default: 5)
//! - `QUERY_COUNT` - Number of queries per benchmark iteration (default: 100)

#![allow(dead_code)]

mod tpch;

use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use tpch::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Thread-safe wrapper around Database for concurrent access.
/// This is a local type alias since this benchmark needs sync RwLock, not async.
#[derive(Clone)]
struct SharedDatabase {
    inner: Arc<RwLock<Database>>,
}

impl SharedDatabase {
    fn new(db: Database) -> Self {
        Self { inner: Arc::new(RwLock::new(db)) }
    }

    fn read(&self) -> parking_lot::RwLockReadGuard<'_, Database> {
        self.inner.read()
    }

    #[allow(dead_code)]
    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Database> {
        self.inner.write()
    }
}

/// Test queries for the benchmark - varying complexity levels
const QUERIES: &[(&str, &str)] = &[
    // Simple point lookup (fast)
    (
        "point_lookup",
        "SELECT * FROM LINEITEM WHERE L_ORDERKEY = 1 AND L_LINENUMBER = 1",
    ),
    // Simple aggregation
    (
        "count_agg",
        "SELECT COUNT(*), SUM(L_QUANTITY) FROM LINEITEM WHERE L_SHIPDATE > DATE '1995-01-01'",
    ),
    // Join query
    (
        "join_query",
        "SELECT O.O_ORDERKEY, C.C_NAME
         FROM ORDERS O
         JOIN CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY
         LIMIT 100",
    ),
    // Filter aggregation (TPC-H Q6 style)
    (
        "filter_agg",
        "SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
         FROM LINEITEM
         WHERE L_SHIPDATE >= DATE '1994-01-01'
           AND L_SHIPDATE < DATE '1995-01-01'
           AND L_DISCOUNT >= 0.05
           AND L_DISCOUNT <= 0.07
           AND L_QUANTITY < 24",
    ),
];

/// Percentile calculation helper
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Extended statistics with percentiles
#[derive(Debug, Clone)]
struct ExtendedStats {
    pub name: String,
    pub total_queries: usize,
    pub qps: f64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub scaling_factor: Option<f64>,
}

impl ExtendedStats {
    fn from_latencies(name: &str, total_queries: usize, elapsed: Duration, latencies: Vec<Duration>) -> Self {
        let mut sorted = latencies.clone();
        sorted.sort();

        let qps = if elapsed.as_secs_f64() > 0.0 {
            total_queries as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let min = sorted.first().copied().unwrap_or(Duration::ZERO);
        let max = sorted.last().copied().unwrap_or(Duration::ZERO);
        let mean = if !sorted.is_empty() {
            sorted.iter().sum::<Duration>() / sorted.len() as u32
        } else {
            Duration::ZERO
        };

        Self {
            name: name.to_string(),
            total_queries,
            qps,
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
            min,
            max,
            mean,
            scaling_factor: None,
        }
    }

    fn print(&self) {
        eprintln!("  {}", self.name);
        eprintln!("    Queries:        {:>10}", self.total_queries);
        eprintln!("    QPS:            {:>10.2}", self.qps);
        eprintln!("    P50 latency:    {:>10.2?}", self.p50);
        eprintln!("    P95 latency:    {:>10.2?}", self.p95);
        eprintln!("    P99 latency:    {:>10.2?}", self.p99);
        eprintln!("    Min latency:    {:>10.2?}", self.min);
        eprintln!("    Max latency:    {:>10.2?}", self.max);
        eprintln!("    Mean latency:   {:>10.2?}", self.mean);
        if let Some(sf) = self.scaling_factor {
            eprintln!("    Scaling factor: {:>10.2}x", sf);
        }
    }
}

/// Run a single query and return execution time
fn execute_query(db: &Database, sql: &str) -> Result<Duration, String> {
    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Err("Not a SELECT statement".to_string()),
        Err(e) => return Err(format!("Parse error: {}", e)),
    };

    let executor = SelectExecutor::new(db);
    let start = Instant::now();
    executor.execute(&stmt).map_err(|e| format!("{}", e))?;
    Ok(start.elapsed())
}

/// Run queries sequentially
fn run_sequential(db: &Database, queries: &[(&str, &str)], query_count: usize) -> ExtendedStats {
    let mut latencies = Vec::with_capacity(query_count);
    let start = Instant::now();

    for i in 0..query_count {
        let (_, sql) = queries[i % queries.len()];
        if let Ok(latency) = execute_query(db, sql) {
            latencies.push(latency);
        }
    }

    let elapsed = start.elapsed();
    ExtendedStats::from_latencies("sequential", query_count, elapsed, latencies)
}

/// Run queries concurrently using tokio tasks
fn run_concurrent(
    shared_db: &SharedDatabase,
    queries: &[(&str, &str)],
    query_count: usize,
    concurrency: usize,
) -> ExtendedStats {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let shared_db = Arc::new(shared_db.clone());
    let queries: Arc<Vec<(String, String)>> = Arc::new(
        queries
            .iter()
            .map(|(n, s)| (n.to_string(), s.to_string()))
            .collect(),
    );

    runtime.block_on(async {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(query_count);

        for i in 0..query_count {
            let db = shared_db.clone();
            let qs = queries.clone();
            let query_idx = i % qs.len();

            handles.push(tokio::spawn(async move {
                let sql = &qs[query_idx].1;
                let guard = db.read();
                execute_query(&guard, sql)
            }));
        }

        let mut latencies = Vec::with_capacity(query_count);
        for handle in handles {
            if let Ok(Ok(latency)) = handle.await {
                latencies.push(latency);
            }
        }

        let elapsed = start.elapsed();
        ExtendedStats::from_latencies(
            &format!("concurrent({})", concurrency),
            query_count,
            elapsed,
            latencies,
        )
    })
}

/// Run mixed read/write workload
fn run_mixed_workload(
    shared_db: &SharedDatabase,
    queries: &[(&str, &str)],
    query_count: usize,
    concurrency: usize,
    write_ratio: f64,
) -> ExtendedStats {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(concurrency)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let shared_db = Arc::new(shared_db.clone());
    let queries: Arc<Vec<(String, String)>> = Arc::new(
        queries
            .iter()
            .map(|(n, s)| (n.to_string(), s.to_string()))
            .collect(),
    );

    runtime.block_on(async {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(query_count);

        for i in 0..query_count {
            let db = shared_db.clone();
            let qs = queries.clone();
            let query_idx = i % qs.len();
            let is_write = (i as f64 / query_count as f64) < write_ratio;

            handles.push(tokio::spawn(async move {
                if is_write {
                    // Simulate a write operation (acquire write lock, do some work)
                    // We use spawn_blocking to handle the non-Send guard
                    let db_clone = db.clone();
                    tokio::task::spawn_blocking(move || {
                        let start = Instant::now();
                        let _guard = db_clone.write();
                        // Simulate some write work (spin briefly)
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        start.elapsed()
                    }).await.map_err(|e| format!("{}", e))
                } else {
                    let sql = &qs[query_idx].1;
                    let guard = db.read();
                    execute_query(&guard, sql)
                }
            }));
        }

        let mut latencies = Vec::with_capacity(query_count);
        for handle in handles {
            if let Ok(Ok(latency)) = handle.await {
                latencies.push(latency);
            }
        }

        let elapsed = start.elapsed();
        ExtendedStats::from_latencies(
            &format!("mixed({}% write)", (write_ratio * 100.0) as u32),
            query_count,
            elapsed,
            latencies,
        )
    })
}

fn print_help(program: &str) {
    eprintln!("Concurrent Query Throughput Benchmark");
    eprintln!();
    eprintln!("Usage:");
    eprintln!("  {}                     Run all benchmarks", program);
    eprintln!("  {} --help              Show this help", program);
    eprintln!();
    eprintln!("Environment Variables:");
    eprintln!("  SCALE_FACTOR           TPC-H scale factor (default: 0.1)");
    eprintln!("  CONCURRENCY            Number of concurrent tasks (default: CPU count)");
    eprintln!("  WARMUP_ITERATIONS      Warmup runs (default: 2)");
    eprintln!("  BENCHMARK_ITERATIONS   Timed runs (default: 5)");
    eprintln!("  QUERY_COUNT            Queries per iteration (default: 100)");
    eprintln!();
    eprintln!("Test Queries:");
    for (name, _) in QUERIES {
        eprintln!("  - {}", name);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Handle help
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        print_help(&args[0]);
        return;
    }

    eprintln!("=== Concurrent Query Throughput Benchmark ===");

    // Configuration from environment
    let scale_factor: f64 = env::var("SCALE_FACTOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.1);

    let concurrency: usize = env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(num_cpus::get);

    let warmup_iterations: usize = env::var("WARMUP_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let benchmark_iterations: usize = env::var("BENCHMARK_ITERATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let query_count: usize = env::var("QUERY_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    eprintln!("Configuration:");
    eprintln!("  Scale factor:          {}", scale_factor);
    eprintln!("  Concurrency:           {}", concurrency);
    eprintln!("  Warmup iterations:     {}", warmup_iterations);
    eprintln!("  Benchmark iterations:  {}", benchmark_iterations);
    eprintln!("  Queries per iteration: {}", query_count);

    // Load TPC-H database
    eprintln!("\nLoading TPC-H database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let db = load_vibesql(scale_factor);
    eprintln!("Database loaded in {:?}", load_start.elapsed());

    // Wrap in SharedDatabase for concurrent access
    let shared_db = SharedDatabase::new(db);

    // ========================================
    // Benchmark 1: Sequential vs Concurrent
    // ========================================
    eprintln!("\n=== Sequential vs Concurrent Comparison ===");

    // Warmup
    eprintln!("\nWarmup ({} iterations)...", warmup_iterations);
    for _ in 0..warmup_iterations {
        let _ = run_sequential(&shared_db.read(), QUERIES, query_count / 10);
        let _ = run_concurrent(&shared_db, QUERIES, query_count / 10, concurrency);
    }

    // Run benchmarks
    eprintln!("\nRunning benchmarks ({} iterations)...", benchmark_iterations);

    let mut sequential_results = Vec::new();
    let mut concurrent_results = Vec::new();

    for i in 0..benchmark_iterations {
        eprintln!("\n--- Iteration {} ---", i + 1);

        // Sequential
        let seq_stats = run_sequential(&shared_db.read(), QUERIES, query_count);
        eprintln!("Sequential:");
        eprintln!("  QPS: {:.2}, P50: {:?}, P99: {:?}", seq_stats.qps, seq_stats.p50, seq_stats.p99);
        sequential_results.push(seq_stats);

        // Concurrent
        let conc_stats = run_concurrent(&shared_db, QUERIES, query_count, concurrency);
        eprintln!("Concurrent({}):", concurrency);
        eprintln!("  QPS: {:.2}, P50: {:?}, P99: {:?}", conc_stats.qps, conc_stats.p50, conc_stats.p99);
        concurrent_results.push(conc_stats);
    }

    // Calculate averages
    let avg_seq_qps: f64 = sequential_results.iter().map(|s| s.qps).sum::<f64>() / benchmark_iterations as f64;
    let avg_conc_qps: f64 = concurrent_results.iter().map(|s| s.qps).sum::<f64>() / benchmark_iterations as f64;
    let scaling_factor = avg_conc_qps / avg_seq_qps;

    eprintln!("\n=== Summary ===");
    eprintln!("Average Sequential QPS:  {:.2}", avg_seq_qps);
    eprintln!("Average Concurrent QPS:  {:.2}", avg_conc_qps);
    eprintln!("Scaling Factor:          {:.2}x", scaling_factor);

    // ========================================
    // Benchmark 2: Concurrency Scaling
    // ========================================
    eprintln!("\n=== Concurrency Scaling ===");

    let concurrency_levels = [1, 2, 4, 8, 16].into_iter()
        .filter(|&c| c <= concurrency * 2)
        .collect::<Vec<_>>();

    for &c in &concurrency_levels {
        let stats = run_concurrent(&shared_db, QUERIES, query_count, c);
        let sf = stats.qps / avg_seq_qps;
        eprintln!(
            "  Concurrency {:>2}: QPS {:>10.2}, P50 {:>10.2?}, P99 {:>10.2?}, Scaling {:.2}x",
            c, stats.qps, stats.p50, stats.p99, sf
        );
    }

    // ========================================
    // Benchmark 3: Mixed Read/Write Workload
    // ========================================
    eprintln!("\n=== Mixed Read/Write Workload ===");

    for write_pct in [0, 5, 10, 25] {
        let write_ratio = write_pct as f64 / 100.0;
        let stats = run_mixed_workload(&shared_db, QUERIES, query_count, concurrency, write_ratio);
        eprintln!(
            "  {:>2}% writes: QPS {:>10.2}, P50 {:>10.2?}, P99 {:>10.2?}",
            write_pct, stats.qps, stats.p50, stats.p99
        );
    }

    // ========================================
    // Per-Query Analysis
    // ========================================
    eprintln!("\n=== Per-Query Performance (Sequential) ===");

    for (name, sql) in QUERIES {
        let mut latencies = Vec::new();
        let iterations = 20;

        for _ in 0..iterations {
            if let Ok(latency) = execute_query(&shared_db.read(), sql) {
                latencies.push(latency);
            }
        }

        latencies.sort();
        let p50 = percentile(&latencies, 0.50);
        let p99 = percentile(&latencies, 0.99);
        let mean: Duration = latencies.iter().sum::<Duration>() / iterations as u32;

        eprintln!(
            "  {:<15} mean {:>10.2?}, P50 {:>10.2?}, P99 {:>10.2?}",
            name, mean, p50, p99
        );
    }

    // Success criteria check
    eprintln!("\n=== Success Criteria ===");
    let pass_scaling = scaling_factor >= 2.0;

    // P99 latency check: concurrent P99 should not be more than 5x sequential P99
    // (some increase is expected due to task scheduling overhead)
    let avg_seq_p99: Duration = sequential_results.iter().map(|s| s.p99).sum::<Duration>()
        / benchmark_iterations as u32;
    let avg_conc_p99: Duration = concurrent_results.iter().map(|s| s.p99).sum::<Duration>()
        / benchmark_iterations as u32;
    let p99_ratio = avg_conc_p99.as_secs_f64() / avg_seq_p99.as_secs_f64();
    let pass_p99 = p99_ratio < 5.0;

    eprintln!(
        "  Concurrent scaling >= 2x: {} ({:.2}x)",
        if pass_scaling { "PASS" } else { "FAIL" },
        scaling_factor
    );
    eprintln!(
        "  P99 latency ratio < 5x:   {} ({:.2}x, seq {:?} vs conc {:?})",
        if pass_p99 { "PASS" } else { "FAIL" },
        p99_ratio,
        avg_seq_p99,
        avg_conc_p99
    );

    eprintln!("\n=== Done ===");
}
