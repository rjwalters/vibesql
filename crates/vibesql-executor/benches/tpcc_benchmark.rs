//! TPC-C Benchmark Profiling
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpcc_benchmark --features benchmark-comparison --no-run && ./target/release/deps/tpcc_benchmark-*
//!
//! Set environment variables:
//!   TPCC_SCALE_FACTOR  - Number of warehouses (default: 1)
//!   TPCC_DURATION_SECS - Benchmark duration in seconds (default: 60)
//!   TPCC_WARMUP_SECS   - Warmup duration in seconds (default: 10)
//!   TPCC_CLIENTS       - Number of parallel clients (default: 1)
//!                        Set to 0 or "auto" for memory-aware auto-scaling
//!   ENGINE_FILTER      - Comma-separated list of engines to run (default: vibesql,sqlite,duckdb)
//!                        Note: MySQL excluded by default (use server benchmarks for MySQL)
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
//!
//! Multi-client parallel execution:
//!   TPCC_CLIENTS=4 ./target/release/deps/tpcc_benchmark-*
//!   TPCC_CLIENTS=auto ./target/release/deps/tpcc_benchmark-*
//!
//! Engine selection (embedded databases only by default):
//!   ENGINE_FILTER=vibesql ./target/release/deps/tpcc_benchmark-*
//!   ENGINE_FILTER=vibesql,sqlite ./target/release/deps/tpcc_benchmark-*
//!   ENGINE_FILTER=vibesql,sqlite,duckdb,mysql ./target/release/deps/tpcc_benchmark-*  # Include MySQL

mod harness;
mod tpcc;

use harness::EngineFilter;
use rayon::prelude::*;
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

fn print_parallel_results(
    client_results: &[TPCCBenchmarkResults],
    aggregate: &TPCCBenchmarkResults,
    transaction_type: TransactionType,
) {
    eprintln!("\n=== TPC-C Parallel Benchmark Results ===");
    eprintln!("Transaction type: {}", transaction_type.name());
    eprintln!("Number of clients: {}", client_results.len());

    // Print per-client summary
    eprintln!("\n--- Per-Client Summary ---");
    eprintln!(
        "{:>8} {:>12} {:>12} {:>10}",
        "Client", "Txns", "TPS", "Success%"
    );
    eprintln!("{:-<8} {:-<12} {:-<12} {:-<10}", "", "", "", "");
    for (i, result) in client_results.iter().enumerate() {
        let success_pct = if result.total_transactions > 0 {
            result.successful_transactions as f64 / result.total_transactions as f64 * 100.0
        } else {
            0.0
        };
        eprintln!(
            "{:>8} {:>12} {:>12.2} {:>9.1}%",
            i + 1,
            result.total_transactions,
            result.transactions_per_second,
            success_pct
        );
    }

    // Print aggregate results
    eprintln!("\n--- Aggregate Results ---");
    eprintln!("Total transactions: {}", aggregate.total_transactions);
    if aggregate.total_transactions > 0 {
        eprintln!(
            "Successful: {} ({:.1}%)",
            aggregate.successful_transactions,
            aggregate.successful_transactions as f64 / aggregate.total_transactions as f64 * 100.0
        );
    }
    eprintln!("Failed: {}", aggregate.failed_transactions);
    eprintln!("Duration: {} ms", aggregate.total_duration_ms);
    eprintln!(
        "Aggregate throughput: {:.2} TPS (tpmC: {:.2})",
        aggregate.transactions_per_second,
        aggregate.transactions_per_second * 60.0
    );

    eprintln!("\n--- Transaction Breakdown (Aggregate) ---");
    if aggregate.new_order_count > 0 {
        eprintln!(
            "New-Order:     {:>6} txns, avg {:>10.2} us",
            aggregate.new_order_count, aggregate.new_order_avg_us
        );
    }
    if aggregate.payment_count > 0 {
        eprintln!(
            "Payment:       {:>6} txns, avg {:>10.2} us",
            aggregate.payment_count, aggregate.payment_avg_us
        );
    }
    if aggregate.order_status_count > 0 {
        eprintln!(
            "Order-Status:  {:>6} txns, avg {:>10.2} us",
            aggregate.order_status_count, aggregate.order_status_avg_us
        );
    }
    if aggregate.delivery_count > 0 {
        eprintln!(
            "Delivery:      {:>6} txns, avg {:>10.2} us",
            aggregate.delivery_count, aggregate.delivery_avg_us
        );
    }
    if aggregate.stock_level_count > 0 {
        eprintln!(
            "Stock-Level:   {:>6} txns, avg {:>10.2} us",
            aggregate.stock_level_count, aggregate.stock_level_avg_us
        );
    }
}

/// Compute memory-aware parallelism for TPC-C clients.
/// Returns a reasonable number of parallel clients based on available CPU cores
/// and memory constraints.
fn compute_parallelism(num_warehouses: i32) -> usize {
    // Get available CPU cores
    let cpu_cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);

    // For TPC-C, each client should ideally operate on its own warehouse(s)
    // to minimize contention. We limit clients to the number of warehouses.
    let max_by_warehouses = num_warehouses.max(1) as usize;

    // Also consider memory - each client maintains its own workload state
    // which is relatively small (~1KB per client), so memory isn't a major constraint.
    // However, we cap at 16 clients to avoid overwhelming the system.
    let max_clients = 16;

    // Return the minimum of all constraints
    cpu_cores.min(max_by_warehouses).min(max_clients).max(1)
}

/// Aggregate results from multiple client runs into a single result.
fn aggregate_results(client_results: &[TPCCBenchmarkResults]) -> TPCCBenchmarkResults {
    if client_results.is_empty() {
        return TPCCBenchmarkResults::new();
    }

    let mut aggregate = TPCCBenchmarkResults::new();

    // Sum up transaction counts
    for result in client_results {
        aggregate.total_transactions += result.total_transactions;
        aggregate.successful_transactions += result.successful_transactions;
        aggregate.failed_transactions += result.failed_transactions;
        aggregate.new_order_count += result.new_order_count;
        aggregate.payment_count += result.payment_count;
        aggregate.order_status_count += result.order_status_count;
        aggregate.delivery_count += result.delivery_count;
        aggregate.stock_level_count += result.stock_level_count;
    }

    // Duration is the max across all clients (they run in parallel)
    aggregate.total_duration_ms = client_results
        .iter()
        .map(|r| r.total_duration_ms)
        .max()
        .unwrap_or(0);

    // Calculate aggregate throughput
    if aggregate.total_duration_ms > 0 {
        aggregate.transactions_per_second =
            aggregate.total_transactions as f64 / (aggregate.total_duration_ms as f64 / 1000.0);
    }

    // Calculate weighted average latencies
    let total_new_order_time: f64 = client_results
        .iter()
        .map(|r| r.new_order_avg_us * r.new_order_count as f64)
        .sum();
    if aggregate.new_order_count > 0 {
        aggregate.new_order_avg_us = total_new_order_time / aggregate.new_order_count as f64;
    }

    let total_payment_time: f64 = client_results
        .iter()
        .map(|r| r.payment_avg_us * r.payment_count as f64)
        .sum();
    if aggregate.payment_count > 0 {
        aggregate.payment_avg_us = total_payment_time / aggregate.payment_count as f64;
    }

    let total_order_status_time: f64 = client_results
        .iter()
        .map(|r| r.order_status_avg_us * r.order_status_count as f64)
        .sum();
    if aggregate.order_status_count > 0 {
        aggregate.order_status_avg_us =
            total_order_status_time / aggregate.order_status_count as f64;
    }

    let total_delivery_time: f64 = client_results
        .iter()
        .map(|r| r.delivery_avg_us * r.delivery_count as f64)
        .sum();
    if aggregate.delivery_count > 0 {
        aggregate.delivery_avg_us = total_delivery_time / aggregate.delivery_count as f64;
    }

    let total_stock_level_time: f64 = client_results
        .iter()
        .map(|r| r.stock_level_avg_us * r.stock_level_count as f64)
        .sum();
    if aggregate.stock_level_count > 0 {
        aggregate.stock_level_avg_us = total_stock_level_time / aggregate.stock_level_count as f64;
    }

    aggregate
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

/// Run a single client benchmark for parallel execution.
/// This version takes a client_id to assign different warehouse ranges per client.
fn run_client_benchmark<E: TPCCExecutor + Sync>(
    executor: &E,
    transaction_type: TransactionType,
    num_warehouses: i32,
    num_clients: usize,
    client_id: usize,
    duration: Duration,
    warmup: Duration,
) -> TPCCBenchmarkResults {
    // Each client gets a unique seed based on client_id for reproducibility
    let seed = 42 + client_id as u64 * 1000;

    // For warehouse partitioning: each client operates primarily on its assigned warehouses
    // This minimizes contention while still allowing cross-warehouse transactions per TPC-C spec
    let warehouses_per_client = (num_warehouses as usize / num_clients).max(1);
    let start_warehouse = (client_id * warehouses_per_client) as i32 + 1;
    let _end_warehouse = ((client_id + 1) * warehouses_per_client).min(num_warehouses as usize) as i32;

    // Create workload with client-specific seed
    // The workload will generate transactions primarily for the client's warehouse range
    let mut workload = TPCCWorkload::new(seed, num_warehouses);

    // Override warehouse selection to prefer this client's assigned warehouses
    // For simplicity, we'll use the full warehouse range but with different random seeds
    // This ensures statistical variation while keeping reproducibility
    let _ = (start_warehouse, _end_warehouse); // Future: could customize workload generation

    let mut results = TPCCBenchmarkResults::new();
    let mut new_order_times: Vec<u64> = Vec::new();
    let mut payment_times: Vec<u64> = Vec::new();
    let mut order_status_times: Vec<u64> = Vec::new();
    let mut delivery_times: Vec<u64> = Vec::new();
    let mut stock_level_times: Vec<u64> = Vec::new();

    // Warmup phase (each client does its own warmup)
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

/// Run TPC-C benchmark with multiple parallel clients.
/// Each client operates independently, and results are aggregated at the end.
fn run_parallel_benchmark<E: TPCCExecutor + Sync>(
    executor: &E,
    transaction_type: TransactionType,
    num_warehouses: i32,
    num_clients: usize,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> (Vec<TPCCBenchmarkResults>, TPCCBenchmarkResults) {
    if print_phases {
        eprintln!(
            "Running {} clients in parallel (warmup: {:?}, duration: {:?})...",
            num_clients, warmup, duration
        );
    }

    // Configure rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_clients)
        .build_global()
        .ok(); // Ignore error if already initialized

    // Run all clients in parallel
    let client_results: Vec<TPCCBenchmarkResults> = (0..num_clients)
        .into_par_iter()
        .map(|client_id| {
            run_client_benchmark(
                executor,
                transaction_type,
                num_warehouses,
                num_clients,
                client_id,
                duration,
                warmup,
            )
        })
        .collect();

    // Aggregate results
    let aggregate = aggregate_results(&client_results);

    (client_results, aggregate)
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

/// Run SQLite benchmark with multiple parallel clients.
/// Each client gets its own in-memory database with loaded data.
#[cfg(feature = "benchmark-comparison")]
fn run_sqlite_parallel_benchmark(
    scale_factor: f64,
    transaction_type: TransactionType,
    num_warehouses: i32,
    num_clients: usize,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> (Vec<TPCCBenchmarkResults>, TPCCBenchmarkResults) {
    use tpcc::schema::load_sqlite;

    if print_phases {
        eprintln!(
            "Running {} SQLite clients in parallel (warmup: {:?}, duration: {:?})...",
            num_clients, warmup, duration
        );
        eprintln!("Note: Each client loads its own in-memory database");
    }

    // Run all clients in parallel, each with its own connection and data
    let client_results: Vec<TPCCBenchmarkResults> = (0..num_clients)
        .into_par_iter()
        .map(|client_id| {
            // Each client gets its own connection with loaded data
            let conn = load_sqlite(scale_factor);
            let executor = SqliteTransactionExecutor::new(&conn);

            // Use client-specific seed for reproducibility
            let seed = 42 + client_id as u64 * 1000;
            let mut workload = TPCCWorkload::new(seed, num_warehouses);

            let mut results = TPCCBenchmarkResults::new();
            let mut new_order_times: Vec<u64> = Vec::new();
            let mut payment_times: Vec<u64> = Vec::new();
            let mut order_status_times: Vec<u64> = Vec::new();
            let mut delivery_times: Vec<u64> = Vec::new();
            let mut stock_level_times: Vec<u64> = Vec::new();

            // Warmup phase
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
        })
        .collect();

    // Aggregate results
    let aggregate = aggregate_results(&client_results);

    (client_results, aggregate)
}

/// Run DuckDB benchmark with multiple parallel clients.
/// Each client gets its own in-memory database with loaded data.
#[cfg(feature = "duckdb-comparison")]
fn run_duckdb_parallel_benchmark(
    scale_factor: f64,
    transaction_type: TransactionType,
    num_warehouses: i32,
    num_clients: usize,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> (Vec<TPCCBenchmarkResults>, TPCCBenchmarkResults) {
    use tpcc::schema::load_duckdb;

    if print_phases {
        eprintln!(
            "Running {} DuckDB clients in parallel (warmup: {:?}, duration: {:?})...",
            num_clients, warmup, duration
        );
        eprintln!("Note: Each client loads its own in-memory database");
    }

    // Run all clients in parallel, each with its own connection and data
    let client_results: Vec<TPCCBenchmarkResults> = (0..num_clients)
        .into_par_iter()
        .map(|client_id| {
            // Each client gets its own connection with loaded data
            let conn = load_duckdb(scale_factor);
            let executor = DuckdbTransactionExecutor::new(&conn);

            // Use client-specific seed for reproducibility
            let seed = 42 + client_id as u64 * 1000;
            let mut workload = TPCCWorkload::new(seed, num_warehouses);

            let mut results = TPCCBenchmarkResults::new();
            let mut new_order_times: Vec<u64> = Vec::new();
            let mut payment_times: Vec<u64> = Vec::new();
            let mut order_status_times: Vec<u64> = Vec::new();
            let mut delivery_times: Vec<u64> = Vec::new();
            let mut stock_level_times: Vec<u64> = Vec::new();

            // Warmup phase
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
        })
        .collect();

    // Aggregate results
    let aggregate = aggregate_results(&client_results);

    (client_results, aggregate)
}

/// Run MySQL benchmark with multiple parallel clients.
/// Each client gets its own connection from the pool.
#[cfg(feature = "mysql-comparison")]
fn run_mysql_parallel_benchmark(
    pool: &mysql::Pool,
    transaction_type: TransactionType,
    num_warehouses: i32,
    num_clients: usize,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> (Vec<TPCCBenchmarkResults>, TPCCBenchmarkResults) {
    if print_phases {
        eprintln!(
            "Running {} MySQL clients in parallel (warmup: {:?}, duration: {:?})...",
            num_clients, warmup, duration
        );
    }

    // Run all clients in parallel, each with its own connection
    let client_results: Vec<TPCCBenchmarkResults> = (0..num_clients)
        .into_par_iter()
        .map(|client_id| {
            // Each client gets its own connection from the pool
            let mut conn = pool.get_conn().expect("Failed to get MySQL connection from pool");

            // Use client-specific seed for reproducibility
            let seed = 42 + client_id as u64 * 1000;
            let mut workload = TPCCWorkload::new(seed, num_warehouses);
            let executor = MysqlTransactionExecutor::new(&mut conn);

            let mut results = TPCCBenchmarkResults::new();
            let mut new_order_times: Vec<u64> = Vec::new();
            let mut payment_times: Vec<u64> = Vec::new();
            let mut order_status_times: Vec<u64> = Vec::new();
            let mut delivery_times: Vec<u64> = Vec::new();
            let mut stock_level_times: Vec<u64> = Vec::new();

            // Warmup phase
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
        })
        .collect();

    // Aggregate results
    let aggregate = aggregate_results(&client_results);

    (client_results, aggregate)
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
        eprintln!("  TPCC_CLIENTS         Number of parallel clients (default: 1)");
        eprintln!("                       Set to 0 or \"auto\" for memory-aware auto-scaling");
        eprintln!("  ENGINE_FILTER        Engines to run (default: vibesql,sqlite,duckdb)");
        eprintln!("                       Note: MySQL excluded by default (client-server)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run mixed workload", args[0]);
        eprintln!("  {} new-order                 # Run only New-Order", args[0]);
        eprintln!("  TPCC_SCALE_FACTOR=2 {}       # Run with 2 warehouses", args[0]);
        eprintln!("  TPCC_CLIENTS=4 {}            # Run with 4 parallel clients", args[0]);
        eprintln!("  TPCC_CLIENTS=auto {}         # Auto-scale clients based on resources", args[0]);
        eprintln!("  ENGINE_FILTER=vibesql {}     # VibeSQL only", args[0]);
        eprintln!("  ENGINE_FILTER=all {}         # Include MySQL (all engines)", args[0]);
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

    // Parse number of clients (0 or "auto" means auto-scale)
    let num_clients_env = env::var("TPCC_CLIENTS").ok();
    let num_clients_raw: Option<usize> = num_clients_env.as_ref().and_then(|s| {
        if s.eq_ignore_ascii_case("auto") {
            Some(0) // 0 means auto-scale
        } else {
            s.parse().ok()
        }
    });

    // Parse transaction type (skip benchmark harness arguments like --bench)
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();
    let transaction_type = if !user_args.is_empty() {
        match TransactionType::from_str(user_args[0]) {
            Some(t) => t,
            None => {
                eprintln!(
                    "Error: Unknown transaction type '{}'. Run with --help for usage.",
                    user_args[0]
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

    // Determine number of clients (after we know num_warehouses for auto-scaling)
    let num_clients = match num_clients_raw {
        Some(0) => compute_parallelism(num_warehouses), // Auto-scale
        Some(n) => n.max(1),                            // User-specified
        None => 1,                                      // Default: single client
    };

    // Parse engine filter (defaults to embedded databases only - no MySQL)
    let engine_filter = EngineFilter::from_env_embedded();

    eprintln!("Configuration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Warehouses: {}", num_warehouses);
    if is_micro_mode {
        eprintln!("  Mode: MICRO (reduced row counts)");
    }
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Transaction type: {}", transaction_type.name());
    eprintln!("  Engines: {}", engine_filter.enabled_list());
    if num_clients > 1 {
        eprintln!("  Clients: {} (parallel mode)", num_clients);
    } else {
        eprintln!("  Clients: 1 (single-threaded)");
    }

    // Track results for comparison summary
    let mut vibesql_results: Option<TPCCBenchmarkResults> = None;

    // Load and run VibeSQL benchmark
    if engine_filter.vibesql {
        eprintln!("\nLoading VibeSQL TPC-C database (SF {})...", scale_factor);
        let load_start = Instant::now();
        let vibesql_db = load_vibesql(scale_factor);
        eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

        // Run VibeSQL benchmark using SQL execution (fair comparison with other databases)
        eprintln!("\n--- VibeSQL Benchmark ---");
        tpcc::transactions::reset_profile_counters();
        let vibesql_executor = VibesqlTransactionExecutor::new(&vibesql_db);

        vibesql_results = Some(if num_clients > 1 {
            // Parallel multi-client execution
            let (client_results, aggregate) = run_parallel_benchmark(
                &vibesql_executor,
                transaction_type,
                num_warehouses,
                num_clients,
                duration,
                warmup,
                true,
            );
            print_parallel_results(&client_results, &aggregate, transaction_type);
            aggregate
        } else {
            // Single-client execution (original behavior)
            let results =
                run_benchmark(&vibesql_executor, transaction_type, num_warehouses, duration, warmup, true);
            print_results(&results, transaction_type);
            results
        });

        tpcc::transactions::print_profile_summary();
    } else {
        eprintln!("\nSkipping VibeSQL (filtered out by ENGINE_FILTER)");
    }

    // Comparison benchmarks (if feature enabled)
    #[cfg(feature = "benchmark-comparison")]
    let sqlite_results: Option<TPCCBenchmarkResults> = if engine_filter.sqlite {
        use tpcc::schema::load_sqlite;

        // SQLite benchmark
        eprintln!("\n\n--- SQLite Benchmark ---");
        Some(if num_clients > 1 {
            // Parallel multi-client execution (each client gets its own DB)
            let (client_results, aggregate) = run_sqlite_parallel_benchmark(
                scale_factor,
                transaction_type,
                num_warehouses,
                num_clients,
                duration,
                warmup,
                true,
            );
            print_parallel_results(&client_results, &aggregate, transaction_type);
            aggregate
        } else {
            // Single-client execution
            eprintln!("Loading SQLite database...");
            let sqlite_load_start = Instant::now();
            let sqlite_conn = load_sqlite(scale_factor);
            eprintln!("SQLite loaded in {:?}", sqlite_load_start.elapsed());

            let sqlite_executor = SqliteTransactionExecutor::new(&sqlite_conn);
            let results = run_benchmark(
                &sqlite_executor,
                transaction_type,
                num_warehouses,
                duration,
                warmup,
                true,
            );
            print_results(&results, transaction_type);
            results
        })
    } else {
        eprintln!("\n\nSkipping SQLite (filtered out by ENGINE_FILTER)");
        None
    };
    #[cfg(not(feature = "benchmark-comparison"))]
    let sqlite_results: Option<TPCCBenchmarkResults> = None;

    // DuckDB benchmark (requires duckdb-comparison feature)
    #[cfg(feature = "duckdb-comparison")]
    let duckdb_results: Option<TPCCBenchmarkResults> = if engine_filter.duckdb {
        use tpcc::schema::load_duckdb;

        eprintln!("\n\n--- DuckDB Benchmark ---");
        Some(if num_clients > 1 {
            // Parallel multi-client execution (each client gets its own DB)
            let (client_results, aggregate) = run_duckdb_parallel_benchmark(
                scale_factor,
                transaction_type,
                num_warehouses,
                num_clients,
                duration,
                warmup,
                true,
            );
            print_parallel_results(&client_results, &aggregate, transaction_type);
            aggregate
        } else {
            // Single-client execution
            eprintln!("Loading DuckDB database...");
            let duckdb_load_start = Instant::now();
            let duckdb_conn = load_duckdb(scale_factor);
            eprintln!("DuckDB loaded in {:?}", duckdb_load_start.elapsed());

            let duckdb_executor = DuckdbTransactionExecutor::new(&duckdb_conn);
            let results = run_benchmark(
                &duckdb_executor,
                transaction_type,
                num_warehouses,
                duration,
                warmup,
                true,
            );
            print_results(&results, transaction_type);
            results
        })
    } else {
        eprintln!("\n\nSkipping DuckDB (filtered out by ENGINE_FILTER)");
        None
    };
    #[cfg(not(feature = "duckdb-comparison"))]
    let duckdb_results: Option<TPCCBenchmarkResults> = None;

    // MySQL benchmark (requires mysql-comparison feature and MYSQL_URL env var)
    // Note: MySQL is excluded by default (use ENGINE_FILTER=all or ENGINE_FILTER=...,mysql to include)
    #[cfg(feature = "mysql-comparison")]
    let mysql_results: Option<TPCCBenchmarkResults> = if engine_filter.mysql {
        use tpcc::schema::load_mysql;

        if let Some(mut mysql_conn) = load_mysql(scale_factor) {
            eprintln!("\n\n--- MySQL Benchmark ---");
            eprintln!("MySQL connected and loaded");

            Some(if num_clients > 1 {
                // Parallel multi-client execution with connection pool
                use tpcc::schema::get_mysql_pool;
                if let Some(pool) = get_mysql_pool() {
                    let (client_results, aggregate) = run_mysql_parallel_benchmark(
                        &pool,
                        transaction_type,
                        num_warehouses,
                        num_clients,
                        duration,
                        warmup,
                        true,
                    );
                    print_parallel_results(&client_results, &aggregate, transaction_type);
                    aggregate
                } else {
                    eprintln!("Warning: Failed to create MySQL connection pool for parallel execution");
                    eprintln!("Falling back to single-client execution");
                    let results = run_mysql_benchmark(
                        &mut mysql_conn,
                        transaction_type,
                        num_warehouses,
                        duration,
                        warmup,
                        true,
                    );
                    print_results(&results, transaction_type);
                    results
                }
            } else {
                // Single-client execution
                let results = run_mysql_benchmark(
                    &mut mysql_conn,
                    transaction_type,
                    num_warehouses,
                    duration,
                    warmup,
                    true,
                );
                print_results(&results, transaction_type);
                results
            })
        } else {
            eprintln!("\n\n--- MySQL Benchmark ---");
            eprintln!("Skipping MySQL (set MYSQL_URL env var to enable)");
            None
        }
    } else {
        eprintln!("\n\nSkipping MySQL (filtered out by ENGINE_FILTER)");
        None
    };
    #[cfg(not(feature = "mysql-comparison"))]
    let mysql_results: Option<TPCCBenchmarkResults> = None;

    // Summary comparison
    {
        eprintln!("\n\n=== Comparison Summary ===");
        eprintln!("Transaction type: {}", transaction_type.name());
        eprintln!("{:<12} {:>12} {:>12} {:>10}", "Database", "TPS", "Avg (us)", "Clients");
        eprintln!("{:-<12} {:->12} {:->12} {:->10}", "", "", "", "");

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

        if let Some(ref vibesql_res) = vibesql_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2} {:>10}",
                "VibeSQL",
                vibesql_res.transactions_per_second,
                compute_avg(vibesql_res),
                num_clients
            );
        }
        if let Some(ref sqlite_res) = sqlite_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2} {:>10}",
                "SQLite",
                sqlite_res.transactions_per_second,
                compute_avg(sqlite_res),
                num_clients
            );
        }
        if let Some(ref duckdb_res) = duckdb_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2} {:>10}",
                "DuckDB",
                duckdb_res.transactions_per_second,
                compute_avg(duckdb_res),
                num_clients
            );
        }
        if let Some(ref mysql_res) = mysql_results {
            eprintln!(
                "{:<12} {:>12.2} {:>12.2} {:>10}",
                "MySQL",
                mysql_res.transactions_per_second,
                compute_avg(mysql_res),
                num_clients
            );
        }
    }

    eprintln!("\n=== Done ===");
}
