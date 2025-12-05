//! TPC-C Benchmark Profiling
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpcc_benchmark --features benchmark-comparison --no-run && ./target/release/deps/tpcc_benchmark-*
//!
//! Set environment variables:
//!   TPCC_SCALE_FACTOR  - Number of warehouses (default: 1)
//!   TPCC_DURATION_SECS - Benchmark duration in seconds (default: 60)
//!   TPCC_WARMUP_SECS   - Warmup duration in seconds (default: 10)
//!
//! Run specific transaction type:
//!   ./target/release/deps/tpcc_benchmark-* new-order
//!   ./target/release/deps/tpcc_benchmark-* payment
//!   ./target/release/deps/tpcc_benchmark-* order-status
//!   ./target/release/deps/tpcc_benchmark-* delivery
//!   ./target/release/deps/tpcc_benchmark-* stock-level
//!
//! Run mixed workload (default):
//!   ./target/release/deps/tpcc_benchmark-*

mod tpcc;

use std::env;
use std::time::{Duration, Instant};
use tpcc::schema::load_vibesql;
use tpcc::transactions::*;

/// Transaction type enum
#[derive(Debug, Clone, Copy, PartialEq)]
enum TransactionType {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
    Mixed, // Standard TPC-C mix
}

impl TransactionType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "new-order" | "neworder" | "no" => Some(Self::NewOrder),
            "payment" | "pay" | "p" => Some(Self::Payment),
            "order-status" | "orderstatus" | "os" => Some(Self::OrderStatus),
            "delivery" | "del" | "d" => Some(Self::Delivery),
            "stock-level" | "stocklevel" | "sl" => Some(Self::StockLevel),
            "mixed" | "all" | "mix" => Some(Self::Mixed),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::NewOrder => "New-Order",
            Self::Payment => "Payment",
            Self::OrderStatus => "Order-Status",
            Self::Delivery => "Delivery",
            Self::StockLevel => "Stock-Level",
            Self::Mixed => "Mixed",
        }
    }
}

fn print_results(results: &TPCCBenchmarkResults, transaction_type: TransactionType) {
    eprintln!("\n=== TPC-C Benchmark Results ===");
    eprintln!("Transaction type: {}", transaction_type.name());
    eprintln!("Total transactions: {}", results.total_transactions);
    if results.total_transactions > 0 {
        eprintln!(
            "Successful: {} ({:.1}%)",
            results.successful_transactions,
            results.successful_transactions as f64 / results.total_transactions as f64 * 100.0
        );
    }
    eprintln!("Failed: {}", results.failed_transactions);
    eprintln!("Duration: {} ms", results.total_duration_ms);
    eprintln!("Throughput: {:.2} TPS", results.transactions_per_second);

    eprintln!("\n--- Transaction Breakdown ---");
    if results.new_order_count > 0 {
        eprintln!(
            "New-Order:     {:>6} txns, avg {:>10.2} us",
            results.new_order_count, results.new_order_avg_us
        );
    }
    if results.payment_count > 0 {
        eprintln!(
            "Payment:       {:>6} txns, avg {:>10.2} us",
            results.payment_count, results.payment_avg_us
        );
    }
    if results.order_status_count > 0 {
        eprintln!(
            "Order-Status:  {:>6} txns, avg {:>10.2} us",
            results.order_status_count, results.order_status_avg_us
        );
    }
    if results.delivery_count > 0 {
        eprintln!(
            "Delivery:      {:>6} txns, avg {:>10.2} us",
            results.delivery_count, results.delivery_avg_us
        );
    }
    if results.stock_level_count > 0 {
        eprintln!(
            "Stock-Level:   {:>6} txns, avg {:>10.2} us",
            results.stock_level_count, results.stock_level_avg_us
        );
    }
}

/// Run a TPC-C benchmark with any executor that implements `TPCCExecutor`.
///
/// This generic function replaces the previous three separate functions
/// (`run_benchmark`, `run_sqlite_benchmark`, `run_duckdb_benchmark`)
/// that contained nearly identical code.
fn run_benchmark<E: TPCCExecutor>(
    executor: &E,
    transaction_type: TransactionType,
    num_warehouses: i32,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> TPCCBenchmarkResults {
    let mut workload = TPCCWorkload::new(42, num_warehouses);

    let mut results = TPCCBenchmarkResults::new();
    let mut new_order_times: Vec<u64> = Vec::new();
    let mut payment_times: Vec<u64> = Vec::new();
    let mut order_status_times: Vec<u64> = Vec::new();
    let mut delivery_times: Vec<u64> = Vec::new();
    let mut stock_level_times: Vec<u64> = Vec::new();

    // Warmup phase
    if print_phases {
        eprintln!("Warmup phase ({:?})...", warmup);
    }
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let txn_type = match transaction_type {
            TransactionType::Mixed => workload.next_transaction_type(),
            TransactionType::NewOrder => 0,
            TransactionType::Payment => 1,
            TransactionType::OrderStatus => 2,
            TransactionType::Delivery => 3,
            TransactionType::StockLevel => 4,
        };

        match txn_type {
            0 => {
                let _ = executor.new_order(&workload.generate_new_order());
            }
            1 => {
                let _ = executor.payment(&workload.generate_payment());
            }
            2 => {
                let _ = executor.order_status(&workload.generate_order_status());
            }
            3 => {
                let _ = executor.delivery(&workload.generate_delivery());
            }
            4 => {
                let _ = executor.stock_level(&workload.generate_stock_level());
            }
            _ => unreachable!(),
        }
    }

    // Measurement phase
    if print_phases {
        eprintln!("Measurement phase ({:?})...", duration);
    }
    let benchmark_start = Instant::now();
    while benchmark_start.elapsed() < duration {
        let txn_type = match transaction_type {
            TransactionType::Mixed => workload.next_transaction_type(),
            TransactionType::NewOrder => 0,
            TransactionType::Payment => 1,
            TransactionType::OrderStatus => 2,
            TransactionType::Delivery => 3,
            TransactionType::StockLevel => 4,
        };

        let result = match txn_type {
            0 => {
                let r = executor.new_order(&workload.generate_new_order());
                new_order_times.push(r.duration_us);
                r
            }
            1 => {
                let r = executor.payment(&workload.generate_payment());
                payment_times.push(r.duration_us);
                r
            }
            2 => {
                let r = executor.order_status(&workload.generate_order_status());
                order_status_times.push(r.duration_us);
                r
            }
            3 => {
                let r = executor.delivery(&workload.generate_delivery());
                delivery_times.push(r.duration_us);
                r
            }
            4 => {
                let r = executor.stock_level(&workload.generate_stock_level());
                stock_level_times.push(r.duration_us);
                r
            }
            _ => unreachable!(),
        };

        results.total_transactions += 1;
        if result.success {
            results.successful_transactions += 1;
        } else {
            results.failed_transactions += 1;
        }
    }

    results.total_duration_ms = benchmark_start.elapsed().as_millis() as u64;
    if results.total_duration_ms > 0 {
        results.transactions_per_second =
            results.total_transactions as f64 / (results.total_duration_ms as f64 / 1000.0);
    }

    // Calculate averages
    if !new_order_times.is_empty() {
        results.new_order_count = new_order_times.len() as u64;
        results.new_order_avg_us =
            new_order_times.iter().sum::<u64>() as f64 / new_order_times.len() as f64;
    }
    if !payment_times.is_empty() {
        results.payment_count = payment_times.len() as u64;
        results.payment_avg_us =
            payment_times.iter().sum::<u64>() as f64 / payment_times.len() as f64;
    }
    if !order_status_times.is_empty() {
        results.order_status_count = order_status_times.len() as u64;
        results.order_status_avg_us =
            order_status_times.iter().sum::<u64>() as f64 / order_status_times.len() as f64;
    }
    if !delivery_times.is_empty() {
        results.delivery_count = delivery_times.len() as u64;
        results.delivery_avg_us =
            delivery_times.iter().sum::<u64>() as f64 / delivery_times.len() as f64;
    }
    if !stock_level_times.is_empty() {
        results.stock_level_count = stock_level_times.len() as u64;
        results.stock_level_avg_us =
            stock_level_times.iter().sum::<u64>() as f64 / stock_level_times.len() as f64;
    }

    results
}

/// Run MySQL benchmark separately since it requires &mut self for queries
#[cfg(feature = "mysql-comparison")]
fn run_mysql_benchmark(
    conn: &mut mysql::PooledConn,
    transaction_type: TransactionType,
    num_warehouses: i32,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> TPCCBenchmarkResults {
    let mut workload = TPCCWorkload::new(42, num_warehouses);
    let executor = MysqlTransactionExecutor::new(conn);

    let mut results = TPCCBenchmarkResults::new();
    let mut new_order_times: Vec<u64> = Vec::new();
    let mut payment_times: Vec<u64> = Vec::new();
    let mut order_status_times: Vec<u64> = Vec::new();
    let mut delivery_times: Vec<u64> = Vec::new();
    let mut stock_level_times: Vec<u64> = Vec::new();

    // Warmup phase
    if print_phases {
        eprintln!("Warmup phase ({:?})...", warmup);
    }
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let txn_type = match transaction_type {
            TransactionType::Mixed => workload.next_transaction_type(),
            TransactionType::NewOrder => 0,
            TransactionType::Payment => 1,
            TransactionType::OrderStatus => 2,
            TransactionType::Delivery => 3,
            TransactionType::StockLevel => 4,
        };

        match txn_type {
            0 => {
                let _ = executor.new_order(&workload.generate_new_order());
            }
            1 => {
                let _ = executor.payment(&workload.generate_payment());
            }
            2 => {
                let _ = executor.order_status(&workload.generate_order_status());
            }
            3 => {
                let _ = executor.delivery(&workload.generate_delivery());
            }
            4 => {
                let _ = executor.stock_level(&workload.generate_stock_level());
            }
            _ => unreachable!(),
        }
    }

    // Measurement phase
    if print_phases {
        eprintln!("Measurement phase ({:?})...", duration);
    }
    let benchmark_start = Instant::now();
    while benchmark_start.elapsed() < duration {
        let txn_type = match transaction_type {
            TransactionType::Mixed => workload.next_transaction_type(),
            TransactionType::NewOrder => 0,
            TransactionType::Payment => 1,
            TransactionType::OrderStatus => 2,
            TransactionType::Delivery => 3,
            TransactionType::StockLevel => 4,
        };

        let result = match txn_type {
            0 => {
                let r = executor.new_order(&workload.generate_new_order());
                new_order_times.push(r.duration_us);
                r
            }
            1 => {
                let r = executor.payment(&workload.generate_payment());
                payment_times.push(r.duration_us);
                r
            }
            2 => {
                let r = executor.order_status(&workload.generate_order_status());
                order_status_times.push(r.duration_us);
                r
            }
            3 => {
                let r = executor.delivery(&workload.generate_delivery());
                delivery_times.push(r.duration_us);
                r
            }
            4 => {
                let r = executor.stock_level(&workload.generate_stock_level());
                stock_level_times.push(r.duration_us);
                r
            }
            _ => unreachable!(),
        };

        results.total_transactions += 1;
        if result.success {
            results.successful_transactions += 1;
        } else {
            results.failed_transactions += 1;
        }
    }

    results.total_duration_ms = benchmark_start.elapsed().as_millis() as u64;
    if results.total_duration_ms > 0 {
        results.transactions_per_second =
            results.total_transactions as f64 / (results.total_duration_ms as f64 / 1000.0);
    }

    // Calculate averages
    if !new_order_times.is_empty() {
        results.new_order_count = new_order_times.len() as u64;
        results.new_order_avg_us =
            new_order_times.iter().sum::<u64>() as f64 / new_order_times.len() as f64;
    }
    if !payment_times.is_empty() {
        results.payment_count = payment_times.len() as u64;
        results.payment_avg_us =
            payment_times.iter().sum::<u64>() as f64 / payment_times.len() as f64;
    }
    if !order_status_times.is_empty() {
        results.order_status_count = order_status_times.len() as u64;
        results.order_status_avg_us =
            order_status_times.iter().sum::<u64>() as f64 / order_status_times.len() as f64;
    }
    if !delivery_times.is_empty() {
        results.delivery_count = delivery_times.len() as u64;
        results.delivery_avg_us =
            delivery_times.iter().sum::<u64>() as f64 / delivery_times.len() as f64;
    }
    if !stock_level_times.is_empty() {
        results.stock_level_count = stock_level_times.len() as u64;
        results.stock_level_avg_us =
            stock_level_times.iter().sum::<u64>() as f64 / stock_level_times.len() as f64;
    }

    results
}

fn main() {
    eprintln!("=== TPC-C Benchmark Profiling ===");

    // Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [TRANSACTION_TYPE]", args[0]);
        eprintln!("\nTransaction Types:");
        eprintln!("  new-order      Run only New-Order transactions");
        eprintln!("  payment        Run only Payment transactions");
        eprintln!("  order-status   Run only Order-Status transactions");
        eprintln!("  delivery       Run only Delivery transactions");
        eprintln!("  stock-level    Run only Stock-Level transactions");
        eprintln!("  mixed          Run standard TPC-C mix (default)");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  TPCC_SCALE_FACTOR    Number of warehouses (default: 1)");
        eprintln!("  TPCC_DURATION_SECS   Benchmark duration in seconds (default: 60)");
        eprintln!("  TPCC_WARMUP_SECS     Warmup duration in seconds (default: 10)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run mixed workload", args[0]);
        eprintln!("  {} new-order                 # Run only New-Order", args[0]);
        eprintln!("  TPCC_SCALE_FACTOR=2 {}       # Run with 2 warehouses", args[0]);
        std::process::exit(0);
    }

    // Get configuration from environment
    // Scale factor can be fractional (e.g., 0.01 for micro mode)
    let scale_factor: f64 =
        env::var("TPCC_SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);

    let duration_secs: u64 =
        env::var("TPCC_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(60);

    let warmup_secs: u64 =
        env::var("TPCC_WARMUP_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(warmup_secs);

    // Parse transaction type
    let transaction_type = if args.len() > 1 {
        match TransactionType::from_str(&args[1]) {
            Some(t) => t,
            None => {
                eprintln!(
                    "Error: Unknown transaction type '{}'. Run with --help for usage.",
                    args[1]
                );
                std::process::exit(1);
            }
        }
    } else {
        TransactionType::Mixed
    };

    // Convert to integer warehouses (minimum 1)
    let num_warehouses = scale_factor.max(1.0) as i32;
    let is_micro_mode = scale_factor < 1.0;

    eprintln!("Configuration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Warehouses: {}", num_warehouses);
    if is_micro_mode {
        eprintln!("  Mode: MICRO (reduced row counts)");
    }
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Transaction type: {}", transaction_type.name());

    // Load VibeSQL database
    eprintln!("\nLoading VibeSQL TPC-C database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let vibesql_db = load_vibesql(scale_factor);
    eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

    // Run VibeSQL benchmark using SQL execution (fair comparison with other databases)
    eprintln!("\n--- VibeSQL Benchmark ---");
    tpcc::transactions::reset_profile_counters();
    let vibesql_executor = VibesqlTransactionExecutor::new(&vibesql_db);
    let vibesql_results =
        run_benchmark(&vibesql_executor, transaction_type, num_warehouses, duration, warmup, true);
    print_results(&vibesql_results, transaction_type);
    tpcc::transactions::print_profile_summary();

    // Comparison benchmarks (if feature enabled)
    #[cfg(feature = "benchmark-comparison")]
    {
        use tpcc::schema::load_sqlite;
        #[cfg(feature = "duckdb-comparison")]
        use tpcc::schema::load_duckdb;
        #[cfg(feature = "mysql-comparison")]
        use tpcc::schema::load_mysql;

        // SQLite benchmark
        eprintln!("\n\n--- SQLite Benchmark ---");
        eprintln!("Loading SQLite database...");
        let sqlite_load_start = Instant::now();
        let sqlite_conn = load_sqlite(scale_factor);
        eprintln!("SQLite loaded in {:?}", sqlite_load_start.elapsed());

        let sqlite_executor = SqliteTransactionExecutor::new(&sqlite_conn);
        let sqlite_results = run_benchmark(
            &sqlite_executor,
            transaction_type,
            num_warehouses,
            duration,
            warmup,
            true,
        );
        print_results(&sqlite_results, transaction_type);

        // DuckDB benchmark (requires duckdb-comparison feature)
        #[cfg(feature = "duckdb-comparison")]
        let duckdb_results = {
            eprintln!("\n\n--- DuckDB Benchmark ---");
            eprintln!("Loading DuckDB database...");
            let duckdb_load_start = Instant::now();
            let duckdb_conn = load_duckdb(scale_factor);
            eprintln!("DuckDB loaded in {:?}", duckdb_load_start.elapsed());

            let duckdb_executor = DuckdbTransactionExecutor::new(&duckdb_conn);
            let duckdb_results = run_benchmark(
                &duckdb_executor,
                transaction_type,
                num_warehouses,
                duration,
                warmup,
                true,
            );
            print_results(&duckdb_results, transaction_type);
            Some(duckdb_results)
        };
        #[cfg(not(feature = "duckdb-comparison"))]
        let duckdb_results: Option<TPCCBenchmarkResults> = None;

        // MySQL benchmark (requires mysql-comparison feature and MYSQL_URL env var)
        #[cfg(feature = "mysql-comparison")]
        let mysql_results = if let Some(mut mysql_conn) = load_mysql(scale_factor) {
            eprintln!("\n\n--- MySQL Benchmark ---");
            eprintln!("MySQL connected and loaded");

            // MySQL requires &mut self, so we run it manually instead of using run_benchmark
            let mysql_results = run_mysql_benchmark(
                &mut mysql_conn,
                transaction_type,
                num_warehouses,
                duration,
                warmup,
                true,
            );
            print_results(&mysql_results, transaction_type);
            Some(mysql_results)
        } else {
            eprintln!("\n\n--- MySQL Benchmark ---");
            eprintln!("Skipping MySQL (set MYSQL_URL env var to enable)");
            None
        };
        #[cfg(not(feature = "mysql-comparison"))]
        let mysql_results: Option<TPCCBenchmarkResults> = None;

        // Summary comparison
        eprintln!("\n\n=== Comparison Summary ===");
        eprintln!("Transaction type: {}", transaction_type.name());
        eprintln!("{:<12} {:>12} {:>12}", "Database", "TPS", "Avg (us)");
        eprintln!("{:-<12} {:->12} {:->12}", "", "", "");

        fn compute_avg(results: &TPCCBenchmarkResults) -> f64 {
            if results.total_transactions > 0 {
                let total_time = results.new_order_avg_us * results.new_order_count as f64
                    + results.payment_avg_us * results.payment_count as f64
                    + results.order_status_avg_us * results.order_status_count as f64
                    + results.delivery_avg_us * results.delivery_count as f64
                    + results.stock_level_avg_us * results.stock_level_count as f64;
                total_time / results.total_transactions as f64
            } else {
                0.0
            }
        }

        eprintln!(
            "{:<12} {:>12.2} {:>12.2}",
            "VibeSQL",
            vibesql_results.transactions_per_second,
            compute_avg(&vibesql_results)
        );
        eprintln!(
            "{:<12} {:>12.2} {:>12.2}",
            "SQLite",
            sqlite_results.transactions_per_second,
            compute_avg(&sqlite_results)
        );
        if let Some(ref duckdb_res) = duckdb_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2}",
                "DuckDB",
                duckdb_res.transactions_per_second,
                compute_avg(duckdb_res)
            );
        }
        if let Some(ref mysql_res) = mysql_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2}",
                "MySQL",
                mysql_res.transactions_per_second,
                compute_avg(mysql_res)
            );
        }
    }

    #[cfg(not(feature = "benchmark-comparison"))]
    {
        // Without comparison feature, just show VibeSQL summary
        fn compute_avg(results: &TPCCBenchmarkResults) -> f64 {
            if results.total_transactions > 0 {
                let total_time = results.new_order_avg_us * results.new_order_count as f64
                    + results.payment_avg_us * results.payment_count as f64
                    + results.order_status_avg_us * results.order_status_count as f64
                    + results.delivery_avg_us * results.delivery_count as f64
                    + results.stock_level_avg_us * results.stock_level_count as f64;
                total_time / results.total_transactions as f64
            } else {
                0.0
            }
        }
        eprintln!("\n=== Summary ===");
        eprintln!("Transaction type: {}", transaction_type.name());
        eprintln!("{:<12} {:>12} {:>12}", "Database", "TPS", "Avg (us)");
        eprintln!("{:-<12} {:->12} {:->12}", "", "", "");
        eprintln!(
            "{:<12} {:>12.2} {:>12.2}",
            "VibeSQL",
            vibesql_results.transactions_per_second,
            compute_avg(&vibesql_results)
        );
    }

    eprintln!("\n=== Done ===");
}
