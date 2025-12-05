//! TPC-E Benchmark Profiling
//!
//! **First open-source Rust implementation of TPC-E**
//!
//! Run with:
//!   cargo bench --package vibesql-executor --bench tpce_benchmark --no-run && ./target/release/deps/tpce_benchmark-*
//!
//! Set environment variables:
//!   TPCE_SCALE_FACTOR  - Customer scale (1 = 1000 customers, default: 0.1)
//!   TPCE_DURATION_SECS - Benchmark duration in seconds (default: 60)
//!   TPCE_WARMUP_SECS   - Warmup duration in seconds (default: 10)
//!
//! Run specific transaction type:
//!   ./target/release/deps/tpce_benchmark-* broker-volume
//!   ./target/release/deps/tpce_benchmark-* customer-position
//!   ./target/release/deps/tpce_benchmark-* market-watch
//!   ./target/release/deps/tpce_benchmark-* security-detail
//!   ./target/release/deps/tpce_benchmark-* trade-lookup
//!   ./target/release/deps/tpce_benchmark-* trade-order
//!   ./target/release/deps/tpce_benchmark-* trade-status
//!   ./target/release/deps/tpce_benchmark-* trade-update
//!   ./target/release/deps/tpce_benchmark-* market-feed
//!   ./target/release/deps/tpce_benchmark-* trade-result
//!   ./target/release/deps/tpce_benchmark-* data-maintenance
//!   ./target/release/deps/tpce_benchmark-* trade-cleanup
//!
//! Run mixed workload (default):
//!   ./target/release/deps/tpce_benchmark-*

mod tpce;

use std::env;
use std::time::{Duration, Instant};
use tpce::data::TPCEData;
use tpce::schema::load_vibesql;
use tpce::transactions::*;

/// Transaction type enum
#[derive(Debug, Clone, Copy, PartialEq)]
enum TransactionType {
    BrokerVolume,
    CustomerPosition,
    MarketWatch,
    SecurityDetail,
    TradeLookup,
    TradeOrder,
    TradeStatus,
    TradeUpdate,
    MarketFeed,
    TradeResult,
    DataMaintenance,
    TradeCleanup,
    Mixed,
}

impl TransactionType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "broker-volume" | "brokervolume" | "bv" => Some(Self::BrokerVolume),
            "customer-position" | "customerposition" | "cp" => Some(Self::CustomerPosition),
            "market-watch" | "marketwatch" | "mw" => Some(Self::MarketWatch),
            "security-detail" | "securitydetail" | "sd" => Some(Self::SecurityDetail),
            "trade-lookup" | "tradelookup" | "tl" => Some(Self::TradeLookup),
            "trade-order" | "tradeorder" | "to" => Some(Self::TradeOrder),
            "trade-status" | "tradestatus" | "ts" => Some(Self::TradeStatus),
            "trade-update" | "tradeupdate" | "tu" => Some(Self::TradeUpdate),
            "market-feed" | "marketfeed" | "mf" => Some(Self::MarketFeed),
            "trade-result" | "traderesult" | "tr" => Some(Self::TradeResult),
            "data-maintenance" | "datamaintenance" | "dm" => Some(Self::DataMaintenance),
            "trade-cleanup" | "tradecleanup" | "tc" => Some(Self::TradeCleanup),
            "mixed" | "all" | "mix" => Some(Self::Mixed),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::BrokerVolume => "Broker-Volume",
            Self::CustomerPosition => "Customer-Position",
            Self::MarketWatch => "Market-Watch",
            Self::SecurityDetail => "Security-Detail",
            Self::TradeLookup => "Trade-Lookup",
            Self::TradeOrder => "Trade-Order",
            Self::TradeStatus => "Trade-Status",
            Self::TradeUpdate => "Trade-Update",
            Self::MarketFeed => "Market-Feed",
            Self::TradeResult => "Trade-Result",
            Self::DataMaintenance => "Data-Maintenance",
            Self::TradeCleanup => "Trade-Cleanup",
            Self::Mixed => "Mixed",
        }
    }
}

fn print_results(results: &TPCEBenchmarkResults, transaction_type: TransactionType) {
    eprintln!("\n=== TPC-E Benchmark Results ===");
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
    eprintln!("Throughput: {:.2} tpsE", results.transactions_per_second);

    eprintln!("\n--- Transaction Breakdown ---");
    if results.broker_volume_count > 0 {
        eprintln!(
            "Broker-Volume:      {:>6} txns, avg {:>10.2} us",
            results.broker_volume_count, results.broker_volume_avg_us
        );
    }
    if results.customer_position_count > 0 {
        eprintln!(
            "Customer-Position:  {:>6} txns, avg {:>10.2} us",
            results.customer_position_count, results.customer_position_avg_us
        );
    }
    if results.market_watch_count > 0 {
        eprintln!(
            "Market-Watch:       {:>6} txns, avg {:>10.2} us",
            results.market_watch_count, results.market_watch_avg_us
        );
    }
    if results.security_detail_count > 0 {
        eprintln!(
            "Security-Detail:    {:>6} txns, avg {:>10.2} us",
            results.security_detail_count, results.security_detail_avg_us
        );
    }
    if results.trade_lookup_count > 0 {
        eprintln!(
            "Trade-Lookup:       {:>6} txns, avg {:>10.2} us",
            results.trade_lookup_count, results.trade_lookup_avg_us
        );
    }
    if results.trade_order_count > 0 {
        eprintln!(
            "Trade-Order:        {:>6} txns, avg {:>10.2} us",
            results.trade_order_count, results.trade_order_avg_us
        );
    }
    if results.trade_status_count > 0 {
        eprintln!(
            "Trade-Status:       {:>6} txns, avg {:>10.2} us",
            results.trade_status_count, results.trade_status_avg_us
        );
    }
    if results.trade_update_count > 0 {
        eprintln!(
            "Trade-Update:       {:>6} txns, avg {:>10.2} us",
            results.trade_update_count, results.trade_update_avg_us
        );
    }
    if results.market_feed_count > 0 {
        eprintln!(
            "Market-Feed:        {:>6} txns, avg {:>10.2} us",
            results.market_feed_count, results.market_feed_avg_us
        );
    }
    if results.trade_result_count > 0 {
        eprintln!(
            "Trade-Result:       {:>6} txns, avg {:>10.2} us",
            results.trade_result_count, results.trade_result_avg_us
        );
    }
    if results.data_maintenance_count > 0 {
        eprintln!(
            "Data-Maintenance:   {:>6} txns, avg {:>10.2} us",
            results.data_maintenance_count, results.data_maintenance_avg_us
        );
    }
    if results.trade_cleanup_count > 0 {
        eprintln!(
            "Trade-Cleanup:      {:>6} txns, avg {:>10.2} us",
            results.trade_cleanup_count, results.trade_cleanup_avg_us
        );
    }
}

/// Run a TPC-E benchmark with any executor that implements `TPCEExecutor`.
fn run_benchmark<E: TPCEExecutor>(
    executor: &E,
    workload: &mut TPCEWorkload,
    transaction_type: TransactionType,
    duration: Duration,
    warmup: Duration,
    print_phases: bool,
) -> TPCEBenchmarkResults {
    let mut results = TPCEBenchmarkResults::new();
    let mut times: [Vec<u64>; 12] = Default::default();

    // Warmup phase
    if print_phases {
        eprintln!("Warmup phase ({:?})...", warmup);
    }
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let txn_type = match transaction_type {
            TransactionType::Mixed => workload.next_transaction_type(),
            TransactionType::BrokerVolume => 0,
            TransactionType::CustomerPosition => 1,
            TransactionType::MarketWatch => 2,
            TransactionType::SecurityDetail => 3,
            TransactionType::TradeLookup => 4,
            TransactionType::TradeOrder => 5,
            TransactionType::TradeStatus => 6,
            TransactionType::TradeUpdate => 7,
            TransactionType::MarketFeed => 8,
            TransactionType::TradeResult => 9,
            TransactionType::DataMaintenance => 10,
            TransactionType::TradeCleanup => 11,
        };

        match txn_type {
            0 => {
                let _ = executor.broker_volume(&workload.generate_broker_volume());
            }
            1 => {
                let _ = executor.customer_position(&workload.generate_customer_position());
            }
            2 => {
                let _ = executor.market_watch(&workload.generate_market_watch());
            }
            3 => {
                let _ = executor.security_detail(&workload.generate_security_detail());
            }
            4 => {
                let _ = executor.trade_lookup(&workload.generate_trade_lookup());
            }
            5 => {
                let _ = executor.trade_order(&workload.generate_trade_order());
            }
            6 => {
                let _ = executor.trade_status(&workload.generate_trade_status());
            }
            7 => {
                let _ = executor.trade_update(&workload.generate_trade_update());
            }
            8 => {
                let _ = executor.market_feed(&workload.generate_market_feed());
            }
            9 => {
                let _ = executor.trade_result(&workload.generate_trade_result());
            }
            10 => {
                let _ = executor.data_maintenance(&workload.generate_data_maintenance());
            }
            11 => {
                let _ = executor.trade_cleanup(&workload.generate_trade_cleanup());
            }
            _ => {}
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
            TransactionType::BrokerVolume => 0,
            TransactionType::CustomerPosition => 1,
            TransactionType::MarketWatch => 2,
            TransactionType::SecurityDetail => 3,
            TransactionType::TradeLookup => 4,
            TransactionType::TradeOrder => 5,
            TransactionType::TradeStatus => 6,
            TransactionType::TradeUpdate => 7,
            TransactionType::MarketFeed => 8,
            TransactionType::TradeResult => 9,
            TransactionType::DataMaintenance => 10,
            TransactionType::TradeCleanup => 11,
        };

        let result = match txn_type {
            0 => {
                let r = executor.broker_volume(&workload.generate_broker_volume());
                times[0].push(r.duration_us);
                r
            }
            1 => {
                let r = executor.customer_position(&workload.generate_customer_position());
                times[1].push(r.duration_us);
                r
            }
            2 => {
                let r = executor.market_watch(&workload.generate_market_watch());
                times[2].push(r.duration_us);
                r
            }
            3 => {
                let r = executor.security_detail(&workload.generate_security_detail());
                times[3].push(r.duration_us);
                r
            }
            4 => {
                let r = executor.trade_lookup(&workload.generate_trade_lookup());
                times[4].push(r.duration_us);
                r
            }
            5 => {
                let r = executor.trade_order(&workload.generate_trade_order());
                times[5].push(r.duration_us);
                r
            }
            6 => {
                let r = executor.trade_status(&workload.generate_trade_status());
                times[6].push(r.duration_us);
                r
            }
            7 => {
                let r = executor.trade_update(&workload.generate_trade_update());
                times[7].push(r.duration_us);
                r
            }
            8 => {
                let r = executor.market_feed(&workload.generate_market_feed());
                times[8].push(r.duration_us);
                r
            }
            9 => {
                let r = executor.trade_result(&workload.generate_trade_result());
                times[9].push(r.duration_us);
                r
            }
            10 => {
                let r = executor.data_maintenance(&workload.generate_data_maintenance());
                times[10].push(r.duration_us);
                r
            }
            _ => {
                let r = executor.trade_cleanup(&workload.generate_trade_cleanup());
                times[11].push(r.duration_us);
                r
            }
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
    fn avg(times: &[u64]) -> (u64, f64) {
        if times.is_empty() {
            (0, 0.0)
        } else {
            let count = times.len() as u64;
            let sum: u64 = times.iter().sum();
            (count, sum as f64 / count as f64)
        }
    }

    let (c, a) = avg(&times[0]);
    results.broker_volume_count = c;
    results.broker_volume_avg_us = a;

    let (c, a) = avg(&times[1]);
    results.customer_position_count = c;
    results.customer_position_avg_us = a;

    let (c, a) = avg(&times[2]);
    results.market_watch_count = c;
    results.market_watch_avg_us = a;

    let (c, a) = avg(&times[3]);
    results.security_detail_count = c;
    results.security_detail_avg_us = a;

    let (c, a) = avg(&times[4]);
    results.trade_lookup_count = c;
    results.trade_lookup_avg_us = a;

    let (c, a) = avg(&times[5]);
    results.trade_order_count = c;
    results.trade_order_avg_us = a;

    let (c, a) = avg(&times[6]);
    results.trade_status_count = c;
    results.trade_status_avg_us = a;

    let (c, a) = avg(&times[7]);
    results.trade_update_count = c;
    results.trade_update_avg_us = a;

    let (c, a) = avg(&times[8]);
    results.market_feed_count = c;
    results.market_feed_avg_us = a;

    let (c, a) = avg(&times[9]);
    results.trade_result_count = c;
    results.trade_result_avg_us = a;

    let (c, a) = avg(&times[10]);
    results.data_maintenance_count = c;
    results.data_maintenance_avg_us = a;

    let (c, a) = avg(&times[11]);
    results.trade_cleanup_count = c;
    results.trade_cleanup_avg_us = a;

    results
}

fn main() {
    eprintln!("=== TPC-E Benchmark Profiling ===");
    eprintln!("*** First Open-Source Rust Implementation ***");

    // Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [TRANSACTION_TYPE]", args[0]);
        eprintln!("\nTransaction Types:");
        eprintln!("  broker-volume      Run only Broker-Volume transactions");
        eprintln!("  customer-position  Run only Customer-Position transactions");
        eprintln!("  market-watch       Run only Market-Watch transactions");
        eprintln!("  security-detail    Run only Security-Detail transactions");
        eprintln!("  trade-lookup       Run only Trade-Lookup transactions");
        eprintln!("  trade-order        Run only Trade-Order transactions");
        eprintln!("  trade-status       Run only Trade-Status transactions");
        eprintln!("  trade-update       Run only Trade-Update transactions");
        eprintln!("  market-feed        Run only Market-Feed transactions");
        eprintln!("  trade-result       Run only Trade-Result transactions");
        eprintln!("  data-maintenance   Run only Data-Maintenance transactions");
        eprintln!("  trade-cleanup      Run only Trade-Cleanup transactions");
        eprintln!("  mixed              Run standard TPC-E mix (default)");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  TPCE_SCALE_FACTOR    Customer scale (1 = 1000 customers, default: 0.1)");
        eprintln!("  TPCE_DURATION_SECS   Benchmark duration in seconds (default: 60)");
        eprintln!("  TPCE_WARMUP_SECS     Warmup duration in seconds (default: 10)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run mixed workload", args[0]);
        eprintln!("  {} trade-order               # Run only Trade-Order", args[0]);
        eprintln!("  TPCE_SCALE_FACTOR=1 {}       # Run with 1000 customers", args[0]);
        std::process::exit(0);
    }

    // Get configuration from environment
    let scale_factor: f64 =
        env::var("TPCE_SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.1);

    let duration_secs: u64 =
        env::var("TPCE_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(60);

    let warmup_secs: u64 =
        env::var("TPCE_WARMUP_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(warmup_secs);

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

    // Calculate derived values
    let num_customers = ((TPCEData::CUSTOMERS_PER_SF as f64 * scale_factor) as i32).max(10);
    let num_accounts = num_customers * TPCEData::ACCOUNTS_PER_CUSTOMER;
    let num_securities = if scale_factor < 1.0 {
        ((TPCEData::NUM_SECURITIES as f64 * scale_factor) as i32).max(100)
    } else {
        TPCEData::NUM_SECURITIES
    };

    eprintln!("Configuration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Customers: {}", num_customers);
    eprintln!("  Accounts: {}", num_accounts);
    eprintln!("  Securities: {}", num_securities);
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Transaction type: {}", transaction_type.name());

    // Load VibeSQL database
    eprintln!("\nLoading VibeSQL TPC-E database (SF {})...", scale_factor);
    let load_start = Instant::now();
    let vibesql_db = load_vibesql(scale_factor);
    eprintln!("VibeSQL loaded in {:?}", load_start.elapsed());

    // Create workload generator
    let mut workload = TPCEWorkload::new(42, num_customers, num_accounts, num_securities);

    // Run VibeSQL benchmark
    eprintln!("\n--- VibeSQL Benchmark ---");
    tpce::transactions::reset_profile_counters();

    let vibesql_executor = VibesqlTransactionExecutor::new(&vibesql_db);
    let vibesql_results =
        run_benchmark(&vibesql_executor, &mut workload, transaction_type, duration, warmup, true);
    print_results(&vibesql_results, transaction_type);

    tpce::transactions::print_profile_summary();

    // Summary
    eprintln!("\n=== Summary ===");
    eprintln!("Transaction type: {}", transaction_type.name());
    eprintln!("{:<12} {:>12} {:>12}", "Database", "tpsE", "Avg (us)");
    eprintln!("{:-<12} {:->12} {:->12}", "", "", "");

    let avg_time = if vibesql_results.total_transactions > 0 {
        vibesql_results.total_duration_ms as f64 * 1000.0
            / vibesql_results.total_transactions as f64
    } else {
        0.0
    };
    eprintln!(
        "{:<12} {:>12.2} {:>12.2}",
        "VibeSQL", vibesql_results.transactions_per_second, avg_time
    );

    eprintln!("\n=== Done ===");
}
