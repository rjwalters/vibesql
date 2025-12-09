//! TPC-H Client-Server Benchmark
//!
//! This benchmark compares TPC-H query performance between vibesql-server (PostgreSQL wire
//! protocol) and MySQL, providing a fair apples-to-apples comparison for analytical workloads.
//!
//! ## Why This Benchmark?
//!
//! The existing `tpch_benchmark.rs` compares embedded (in-process) databases. This benchmark
//! uses vibesql-server with PostgreSQL wire protocol, providing an equivalent client-server
//! comparison against MySQL for OLAP workloads.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL server only)
//! cargo bench --package vibesql-executor --bench tpch_server --no-run
//! ./target/release/deps/tpch_server-*
//!
//! # With MySQL comparison
//! MYSQL_URL=mysql://user:pass@localhost:3306/tpch \
//! ./target/release/deps/tpch_server-*
//!
//! # Run specific queries
//! ./target/release/deps/tpch_server-* Q1,Q6,Q14
//! ```
//!
//! ## Environment Variables
//!
//! - `SCALE_FACTOR` - TPC-H scale factor (default: 0.01)
//! - `WARMUP_ITERATIONS` - Number of warmup runs per query (default: 1)
//! - `BENCHMARK_ITERATIONS` - Number of timed runs per query (default: 3)
//! - `QUERY_FILTER` - Comma-separated list of queries to run (e.g., "Q1,Q6,Q9")
//! - `MYSQL_URL` - MySQL connection string (optional)
//! - `VIBESQL_PORT` - Port for vibesql-server (default: 15433)

mod tpch;

use std::env;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tpch::queries::*;
use tpch::schema::load_vibesql;

// PostgreSQL wire protocol client
use tokio_postgres::NoTls;

// MySQL client (optional)
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::Pool;
#[cfg(feature = "mysql-comparison")]
use tpch::schema::load_mysql;

/// Default port for vibesql-server (different from sysbench_server to avoid conflicts)
const DEFAULT_VIBESQL_PORT: u16 = 15433;

/// All available TPC-H queries for server benchmark
const ALL_QUERIES: &[(&str, &str)] = &[
    ("Q1", TPCH_Q1),
    ("Q3", TPCH_Q3),
    ("Q4", TPCH_Q4),
    ("Q5", TPCH_Q5),
    ("Q6", TPCH_Q6),
    ("Q7", TPCH_Q7),
    ("Q9", TPCH_Q9),
    ("Q10", TPCH_Q10),
    ("Q11", TPCH_Q11),
    ("Q12", TPCH_Q12),
    ("Q14", TPCH_Q14),
    ("Q19", TPCH_Q19),
];

/// Query result for a single query
#[derive(Debug, Clone)]
struct QueryResult {
    name: String,
    mean_ms: f64,
    min_ms: f64,
    max_ms: f64,
    stddev_ms: f64,
    iterations: usize,
    error: Option<String>,
}

// =============================================================================
// Server Startup Helper
// =============================================================================

/// Start vibesql-server with pre-loaded TPC-H database
async fn start_vibesql_server_with_db(
    port: u16,
    db: vibesql_storage::Database,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    use vibesql_server::config::{AuthConfig, Config, LoggingConfig, ServerConfig};
    use vibesql_server::connection::ConnectionHandler;
    use vibesql_server::observability::ObservabilityProvider;
    use vibesql_server::registry::DatabaseRegistry;
    use vibesql_server::subscription::SubscriptionManager;

    // Create minimal configuration for benchmark
    let config = Config {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            max_connections: 100,
            ssl_enabled: false,
            ssl_cert: None,
            ssl_key: None,
        },
        auth: AuthConfig { method: "trust".to_string(), password_file: None },
        logging: LoggingConfig { level: "error".to_string(), file: None },
        http: Default::default(),
        observability: Default::default(),
        subscriptions: Default::default(),
    };

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let listener = TcpListener::bind(&addr).await?;
    let bound_addr = listener.local_addr()?;

    let config = Arc::new(config);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let subscription_manager = Arc::new(SubscriptionManager::new());

    // Create database registry with pre-loaded TPC-H database
    let database_registry = DatabaseRegistry::new();
    database_registry.register_database("tpch", db).await;

    let observability = Arc::new(
        ObservabilityProvider::init(&Default::default())
            .expect("Disabled observability config should not fail"),
    );
    let (mutation_broadcast_tx, _mutation_broadcast_rx) =
        tokio::sync::broadcast::channel::<vibesql_server::TableMutationNotification>(1024);

    // Spawn accept loop
    tokio::spawn(async move {
        let mut shutdown = shutdown_signal;

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let config = Arc::clone(&config);
                            let active_connections = Arc::clone(&active_connections);
                            let subscription_manager = Arc::clone(&subscription_manager);
                            let database_registry = database_registry.clone();
                            let observability = Arc::clone(&observability);
                            let mutation_broadcast_tx = mutation_broadcast_tx.clone();

                            tokio::spawn(async move {
                                let mut handler = ConnectionHandler::new(
                                    stream,
                                    peer_addr,
                                    config,
                                    observability,
                                    None, // No password store (trust auth)
                                    active_connections,
                                    database_registry,
                                    subscription_manager,
                                    mutation_broadcast_tx,
                                );
                                let _ = handler.handle().await;
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    Ok(bound_addr)
}

// =============================================================================
// PostgreSQL Client Executor (for vibesql-server)
// =============================================================================

struct PostgresExecutor {
    client: tokio_postgres::Client,
}

impl PostgresExecutor {
    async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (client, connection) = tokio_postgres::connect(addr, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    async fn query(&self, sql: &str) -> Result<usize, tokio_postgres::Error> {
        let msgs = self.client.simple_query(sql).await?;
        Ok(msgs
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count())
    }
}

// =============================================================================
// MySQL Executor
// =============================================================================

#[cfg(feature = "mysql-comparison")]
struct MysqlExecutor {
    pool: Pool,
}

#[cfg(feature = "mysql-comparison")]
impl MysqlExecutor {
    fn connect(url: &str) -> Option<Self> {
        let pool = Pool::new(url).ok()?;
        Some(Self { pool })
    }

    fn run_query(&self, sql: &str) -> Result<usize, mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        let result: Vec<mysql::Row> = conn.query(sql)?;
        Ok(result.len())
    }
}

// =============================================================================
// Query Filter Parsing
// =============================================================================

fn get_query_filter() -> Option<Vec<String>> {
    let args: Vec<String> = env::args().collect();
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();

    if !user_args.is_empty() && user_args[0] != "help" && user_args[0] != "--help" {
        return Some(
            user_args[0]
                .split(',')
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty())
                .collect(),
        );
    }

    env::var("QUERY_FILTER").ok().map(|s| {
        s.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect()
    })
}

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

// =============================================================================
// Result Formatting
// =============================================================================

fn print_results(results: &[QueryResult], db_name: &str) {
    eprintln!("\n{}", db_name);
    eprintln!(
        "{:<8} {:>12} {:>12} {:>12} {:>12}",
        "Query", "Mean (ms)", "Min (ms)", "Max (ms)", "Iterations"
    );
    eprintln!("{:-<8} {:->12} {:->12} {:->12} {:->12}", "", "", "", "", "");

    for result in results {
        if let Some(err) = &result.error {
            eprintln!("{:<8} ERROR: {}", result.name, err);
        } else {
            eprintln!(
                "{:<8} {:>12.2} {:>12.2} {:>12.2} {:>12}",
                result.name, result.mean_ms, result.min_ms, result.max_ms, result.iterations
            );
        }
    }
}

fn print_comparison(vibesql_results: &[QueryResult], mysql_results: &[QueryResult]) {
    eprintln!("\n=== Comparison Summary ===");
    eprintln!(
        "{:<8} {:>15} {:>15} {:>12}",
        "Query", "VibeSQL (ms)", "MySQL (ms)", "Ratio"
    );
    eprintln!("{:-<8} {:->15} {:->15} {:->12}", "", "", "", "");

    for vibesql in vibesql_results {
        if vibesql.error.is_some() {
            continue;
        }

        if let Some(mysql) = mysql_results.iter().find(|r| r.name == vibesql.name) {
            if mysql.error.is_some() {
                eprintln!(
                    "{:<8} {:>15.2} {:>15} {:>12}",
                    vibesql.name, vibesql.mean_ms, "ERROR", "-"
                );
            } else {
                let ratio = mysql.mean_ms / vibesql.mean_ms;
                eprintln!(
                    "{:<8} {:>15.2} {:>15.2} {:>11.2}x",
                    vibesql.name, vibesql.mean_ms, mysql.mean_ms, ratio
                );
            }
        }
    }
}

fn output_json(vibesql_results: &[QueryResult], mysql_results: &[QueryResult], scale_factor: f64) {
    // Output JSON for scripts/bench to parse
    println!("{{");
    println!("  \"benchmarks\": [");

    let mut first = true;
    for result in vibesql_results {
        if result.error.is_some() {
            continue;
        }
        if !first {
            println!(",");
        }
        first = false;

        let mean_s = result.mean_ms / 1000.0;
        let min_s = result.min_ms / 1000.0;
        let max_s = result.max_ms / 1000.0;
        let stddev_s = result.stddev_ms / 1000.0;

        print!(
            "    {{\"name\": \"tpch_server_{}_vibesql_server\", \"stats\": {{\"mean\": {}, \"stddev\": {}, \"min\": {}, \"max\": {}, \"rounds\": {}}}}}",
            result.name.to_lowercase(),
            mean_s,
            stddev_s,
            min_s,
            max_s,
            result.iterations
        );
    }

    for result in mysql_results {
        if result.error.is_some() {
            continue;
        }
        println!(",");

        let mean_s = result.mean_ms / 1000.0;
        let min_s = result.min_ms / 1000.0;
        let max_s = result.max_ms / 1000.0;
        let stddev_s = result.stddev_ms / 1000.0;

        print!(
            "    {{\"name\": \"tpch_server_{}_mysql\", \"stats\": {{\"mean\": {}, \"stddev\": {}, \"min\": {}, \"max\": {}, \"rounds\": {}}}}}",
            result.name.to_lowercase(),
            mean_s,
            stddev_s,
            min_s,
            max_s,
            result.iterations
        );
    }

    println!();
    println!("  ],");
    println!("  \"metadata\": {{");
    println!("    \"suite\": \"tpch-server\",");
    println!("    \"scale_factor\": {},", scale_factor);
    println!("    \"timestamp\": \"{}\"", chrono::Utc::now().to_rfc3339());
    println!("  }}");
    println!("}}");
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() {
    eprintln!("=== TPC-H Client-Server Benchmark ===");
    eprintln!("(Comparing vibesql-server via PostgreSQL protocol vs MySQL)");

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [QUERY_FILTER]", args[0]);
        eprintln!("\nQuery Filter:");
        eprintln!("  Q1,Q6,Q14    Run only specified queries");
        eprintln!("  (empty)      Run all queries");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  SCALE_FACTOR          TPC-H scale factor (default: 0.01)");
        eprintln!("  WARMUP_ITERATIONS     Warmup runs per query (default: 1)");
        eprintln!("  BENCHMARK_ITERATIONS  Timed runs per query (default: 3)");
        eprintln!("  MYSQL_URL             MySQL connection string (optional)");
        eprintln!("  VIBESQL_PORT          Port for vibesql-server (default: 15433)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run all queries", args[0]);
        eprintln!("  {} Q1,Q6,Q14                 # Run specific queries", args[0]);
        eprintln!("  SCALE_FACTOR=0.1 {}          # Run with SF 0.1", args[0]);
        eprintln!("  MYSQL_URL=mysql://user:pass@localhost/tpch {}", args[0]);
        std::process::exit(0);
    }

    // Get configuration
    let scale_factor: f64 =
        env::var("SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    let warmup_iterations: usize =
        env::var("WARMUP_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let benchmark_iterations: usize =
        env::var("BENCHMARK_ITERATIONS").ok().and_then(|s| s.parse().ok()).unwrap_or(3);

    let vibesql_port: u16 =
        env::var("VIBESQL_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_VIBESQL_PORT);

    let query_filter = get_query_filter();
    let queries = get_queries_to_run(&query_filter);

    eprintln!("\nConfiguration:");
    eprintln!("  Scale factor: {}", scale_factor);
    eprintln!("  Warmup iterations: {}", warmup_iterations);
    eprintln!("  Benchmark iterations: {}", benchmark_iterations);
    eprintln!("  Queries: {:?}", queries.iter().map(|(n, _)| *n).collect::<Vec<_>>());
    eprintln!("  VibeSQL port: {}", vibesql_port);

    let rt = Runtime::new().expect("Failed to create tokio runtime");

    // Load TPC-H data using existing schema module (reuse embedded loading)
    eprintln!("\nLoading TPC-H data into VibeSQL (SF={})...", scale_factor);
    let db = load_vibesql(scale_factor);
    eprintln!("Data loaded successfully");

    // ========================================
    // VibeSQL Server Benchmark
    // ========================================

    eprintln!("\nStarting vibesql-server on port {}...", vibesql_port);

    let vibesql_results: Vec<QueryResult> = rt.block_on(async {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server_addr = match start_vibesql_server_with_db(vibesql_port, db, shutdown_rx).await {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Failed to start vibesql-server: {}", e);
                return Vec::new();
            }
        };

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let conn_str = format!("host=127.0.0.1 port={} user=postgres dbname=tpch", server_addr.port());
        eprintln!("Connecting to vibesql-server at {}...", conn_str);

        let executor = match PostgresExecutor::connect(&conn_str).await {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Failed to connect to vibesql-server: {}", e);
                let _ = shutdown_tx.send(());
                return Vec::new();
            }
        };

        eprintln!("Connected successfully");

        // Run benchmarks
        let mut results = Vec::new();

        for (query_name, query_sql) in &queries {
            eprintln!("\nRunning {}...", query_name);

            // Warmup
            for _ in 0..warmup_iterations {
                let _ = executor.query(query_sql).await;
            }

            // Benchmark
            let mut times = Vec::new();
            let mut error = None;

            for i in 0..benchmark_iterations {
                let start = Instant::now();
                match executor.query(query_sql).await {
                    Ok(rows) => {
                        let elapsed = start.elapsed();
                        times.push(elapsed.as_secs_f64() * 1000.0);
                        eprintln!("  Run {}: {:.2}ms ({} rows)", i + 1, times.last().unwrap(), rows);
                    }
                    Err(e) => {
                        error = Some(e.to_string());
                        eprintln!("  Run {}: ERROR - {}", i + 1, e);
                        break;
                    }
                }
            }

            if error.is_none() && !times.is_empty() {
                let mean = times.iter().sum::<f64>() / times.len() as f64;
                let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                let variance =
                    times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times.len() as f64;
                let stddev = variance.sqrt();

                results.push(QueryResult {
                    name: query_name.to_string(),
                    mean_ms: mean,
                    min_ms: min,
                    max_ms: max,
                    stddev_ms: stddev,
                    iterations: times.len(),
                    error: None,
                });
            } else {
                results.push(QueryResult {
                    name: query_name.to_string(),
                    mean_ms: 0.0,
                    min_ms: 0.0,
                    max_ms: 0.0,
                    stddev_ms: 0.0,
                    iterations: 0,
                    error,
                });
            }
        }

        let _ = shutdown_tx.send(());
        results
    });

    if !vibesql_results.is_empty() {
        print_results(&vibesql_results, "VibeSQL Server");
    }

    // ========================================
    // MySQL Comparison (if feature enabled)
    // ========================================

    #[allow(unused_mut)]
    let mut mysql_results: Vec<QueryResult> = Vec::new();

    #[cfg(feature = "mysql-comparison")]
    {
        if let Ok(mysql_url) = env::var("MYSQL_URL") {
            eprintln!("\n\nLoading TPC-H data into MySQL...");

            // Load MySQL database using existing schema module
            if let Some(_mysql_conn) = load_mysql(scale_factor) {
                eprintln!("MySQL data loaded successfully");

                if let Some(executor) = MysqlExecutor::connect(&mysql_url) {
                    // MySQL uses slightly different SQL syntax for some queries
                    // We'll use the same queries but some may need adjustment
                    for (query_name, query_sql) in &queries {
                        eprintln!("\nRunning {} on MySQL...", query_name);

                        // Warmup
                        for _ in 0..warmup_iterations {
                            let _ = executor.run_query(query_sql);
                        }

                        // Benchmark
                        let mut times = Vec::new();
                        let mut error = None;

                        for i in 0..benchmark_iterations {
                            let start = Instant::now();
                            match executor.run_query(query_sql) {
                                Ok(rows) => {
                                    let elapsed = start.elapsed();
                                    times.push(elapsed.as_secs_f64() * 1000.0);
                                    eprintln!(
                                        "  Run {}: {:.2}ms ({} rows)",
                                        i + 1,
                                        times.last().unwrap(),
                                        rows
                                    );
                                }
                                Err(e) => {
                                    error = Some(e.to_string());
                                    eprintln!("  Run {}: ERROR - {}", i + 1, e);
                                    break;
                                }
                            }
                        }

                        if error.is_none() && !times.is_empty() {
                            let mean = times.iter().sum::<f64>() / times.len() as f64;
                            let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
                            let max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                            let variance = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>()
                                / times.len() as f64;
                            let stddev = variance.sqrt();

                            mysql_results.push(QueryResult {
                                name: query_name.to_string(),
                                mean_ms: mean,
                                min_ms: min,
                                max_ms: max,
                                stddev_ms: stddev,
                                iterations: times.len(),
                                error: None,
                            });
                        } else {
                            mysql_results.push(QueryResult {
                                name: query_name.to_string(),
                                mean_ms: 0.0,
                                min_ms: 0.0,
                                max_ms: 0.0,
                                stddev_ms: 0.0,
                                iterations: 0,
                                error,
                            });
                        }
                    }

                    print_results(&mysql_results, "MySQL");
                } else {
                    eprintln!("Failed to connect to MySQL at {}", mysql_url);
                }
            } else {
                eprintln!("Failed to load MySQL database");
            }
        }
    }

    // Print comparison if both ran
    if !vibesql_results.is_empty() && !mysql_results.is_empty() {
        print_comparison(&vibesql_results, &mysql_results);
    }

    // Output JSON for parsing
    output_json(&vibesql_results, &mysql_results, scale_factor);
}
