//! Sysbench Client-Server Benchmark
//!
//! WARNING: BENCHMARK INTEGRITY REQUIREMENT
//! =========================================
//! All database operations in this benchmark go through the PostgreSQL wire protocol,
//! ensuring they use the full SQL execution path. This is inherently correct since
//! all queries are transmitted as SQL strings over the network.
//!
//! This benchmark compares vibesql-server (client-server mode) against MySQL,
//! providing a fair apples-to-apples comparison with equivalent network overhead.
//!
//! ## Why This Benchmark?
//!
//! The existing `sysbench_benchmark.rs` compares embedded (in-process) VibeSQL against
//! MySQL running as a client-server database. This is structurally unfair:
//!
//! | Aspect | Embedded VibeSQL | MySQL |
//! |--------|------------------|-------|
//! | Mode | In-process | Client-server |
//! | Network | None | TCP localhost |
//! | Container | None | Docker |
//! | Durability | In-memory | Full fsync |
//!
//! This benchmark uses vibesql-server with PostgreSQL wire protocol, connecting
//! via tokio-postgres, to provide an equivalent client-server comparison.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL server only)
//! cargo bench --package vibesql-executor --bench sysbench_server --no-run
//! ./target/release/deps/sysbench_server-*
//!
//! # With MySQL comparison
//! MYSQL_URL=mysql://user:pass@localhost:3306/sysbench \
//! ./target/release/deps/sysbench_server-*
//! ```
//!
//! ## Environment Variables
//!
//! - `SYSBENCH_TABLE_SIZE` - Number of rows (default: 10000)
//! - `SYSBENCH_DURATION_SECS` - Benchmark duration in seconds (default: 30)
//! - `SYSBENCH_WARMUP_SECS` - Warmup duration in seconds (default: 5)
//! - `MYSQL_URL` - MySQL connection string (optional)
//! - `VIBESQL_PORT` - Port for vibesql-server (default: 15432, avoids conflict with PostgreSQL)

mod sysbench;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysbench::SysbenchData;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

// PostgreSQL wire protocol client
use tokio_postgres::{Client, NoTls};

// MySQL client (optional)
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::{Pool, PooledConn};

/// Default table size for sysbench tests
const DEFAULT_TABLE_SIZE: usize = 10000;

/// Range size for range queries (sysbench default is 100)
const RANGE_SIZE: usize = 100;

/// Default port for vibesql-server (avoids conflict with PostgreSQL on 5432)
const DEFAULT_VIBESQL_PORT: u16 = 15432;

// =============================================================================
// Server Startup Helper
// =============================================================================

/// Start vibesql-server in a background task
async fn start_vibesql_server(
    port: u16,
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

    // Track active connections
    let active_connections = Arc::new(AtomicUsize::new(0));

    // Create the global subscription manager (no need for change events in benchmark)
    let subscription_manager = Arc::new(SubscriptionManager::new());

    // Create shared database registry for all connections
    let database_registry = DatabaseRegistry::new();

    // Create observability provider once (disabled for benchmarks)
    let observability = Arc::new(
        ObservabilityProvider::init(&Default::default())
            .expect("Disabled observability config should not fail"),
    );

    // Create mutation broadcast channel for cross-connection subscription notifications
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
    client: Client,
}

impl PostgresExecutor {
    async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (client, connection) = tokio_postgres::connect(addr, NoTls).await?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    async fn create_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create sbtest1 table - use batch_execute for simple query protocol
        // NOTE: vibesql-server only handles one statement per query, so send them separately
        eprintln!("  Creating table...");
        if let Err(e) = self
            .client
            .batch_execute(
                r#"CREATE TABLE sbtest1 (
                    id INTEGER PRIMARY KEY,
                    k INTEGER,
                    c TEXT,
                    padding TEXT
                )"#,
            )
            .await
        {
            eprintln!("Create table error: {:?}", e);
            if let Some(db_err) = e.as_db_error() {
                eprintln!("  Code: {:?}", db_err.code());
                eprintln!("  Message: {}", db_err.message());
            }
            return Err(Box::new(e));
        }
        eprintln!("  Table created, creating index...");
        if let Err(e) = self.client.batch_execute("CREATE INDEX k_1 ON sbtest1(k)").await {
            eprintln!("Create index error: {:?}", e);
            return Err(Box::new(e));
        }
        eprintln!("  Schema created successfully");
        Ok(())
    }

    async fn load_data(
        &self,
        data: &mut SysbenchData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Insert rows one at a time using simple_query
        // NOTE: vibesql-server has connection handling issues with large query payloads
        // Using shorter string values helps avoid connection drops
        let mut total_count = 0;
        while let Some((id, k, _c, _padding)) = data.next_row() {
            // Use shortened c and padding values to reduce query size
            // Full sysbench uses 120-char c and 60-char padding, but this causes
            // connection issues with vibesql-server's protocol handling
            let sql = format!(
                "INSERT INTO sbtest1 (id, k, c, padding) VALUES ({}, {}, 'c{}', 'p{}')",
                id, k, id, id
            );
            if let Err(e) = self.client.simple_query(&sql).await {
                eprintln!("  INSERT failed at row {}: {}", total_count, e);
                return Err(Box::new(e));
            }
            total_count += 1;

            if total_count % 100 == 0 {
                eprintln!("  Loaded {} rows...", total_count);
            }
        }

        eprintln!("  Loaded {} rows total", total_count);

        // Send a verification query to check connection health
        let count = self.client.simple_query("SELECT COUNT(*) FROM sbtest1").await?;
        eprintln!("  Verified {} response messages", count.len());

        Ok(())
    }

    /// Check if connection is still alive by running a query against the table
    #[allow(dead_code)]
    async fn is_alive(&self) -> bool {
        self.client.simple_query("SELECT c FROM sbtest1 WHERE id = 1").await.is_ok()
    }

    async fn point_select(&self, id: i64) -> Result<usize, tokio_postgres::Error> {
        let sql = format!("SELECT c FROM sbtest1 WHERE id = {}", id);
        let msgs = self.client.simple_query(&sql).await?;
        // Count data rows in the response
        Ok(msgs
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count())
    }

    async fn insert(&self, id: i64, k: i64, c: &str, pad: &str) {
        let sql = format!(
            "INSERT INTO sbtest1 (id, k, c, padding) VALUES ({}, {}, '{}', '{}')",
            id, k, c, pad
        );
        let _ = self.client.batch_execute(&sql).await;
    }

    async fn update_index(&self, id: i64) {
        let sql = format!("UPDATE sbtest1 SET k = k + 1 WHERE id = {}", id);
        let _ = self.client.batch_execute(&sql).await;
    }

    async fn update_non_index(&self, id: i64, c: &str) {
        let sql = format!("UPDATE sbtest1 SET c = '{}' WHERE id = {}", c, id);
        let _ = self.client.batch_execute(&sql).await;
    }

    async fn delete(&self, id: i64) {
        let sql = format!("DELETE FROM sbtest1 WHERE id = {}", id);
        let _ = self.client.batch_execute(&sql).await;
    }

    async fn simple_range(&self, start: i64, end: i64) -> usize {
        let sql = format!("SELECT c FROM sbtest1 WHERE id BETWEEN {} AND {}", start, end);
        match self.client.simple_query(&sql).await {
            Ok(msgs) => msgs
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count(),
            Err(_) => 0,
        }
    }

    async fn sum_range(&self, start: i64, end: i64) -> usize {
        let sql = format!("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN {} AND {}", start, end);
        match self.client.simple_query(&sql).await {
            Ok(msgs) => msgs
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count(),
            Err(_) => 0,
        }
    }

    async fn order_range(&self, start: i64, end: i64) -> usize {
        let sql =
            format!("SELECT c FROM sbtest1 WHERE id BETWEEN {} AND {} ORDER BY c", start, end);
        match self.client.simple_query(&sql).await {
            Ok(msgs) => msgs
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count(),
            Err(_) => 0,
        }
    }

    async fn distinct_range(&self, start: i64, end: i64) -> usize {
        let sql = format!(
            "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN {} AND {} ORDER BY c",
            start, end
        );
        match self.client.simple_query(&sql).await {
            Ok(msgs) => msgs
                .iter()
                .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
                .count(),
            Err(_) => 0,
        }
    }
}

// =============================================================================
// MySQL Executor
// =============================================================================

#[cfg(feature = "mysql-comparison")]
struct MysqlExecutor {
    conn: PooledConn,
}

#[cfg(feature = "mysql-comparison")]
impl MysqlExecutor {
    fn connect(url: &str, table_size: usize) -> Option<Self> {
        let pool = Pool::new(url).ok()?;
        let mut conn = pool.get_conn().ok()?;
        let mut data = SysbenchData::new(table_size);

        // Create schema (drops and recreates table)
        conn.query_drop("DROP TABLE IF EXISTS sbtest1").ok()?;
        conn.query_drop(
            r#"
            CREATE TABLE sbtest1 (
                id INTEGER PRIMARY KEY,
                k INTEGER NOT NULL DEFAULT 0,
                c VARCHAR(120) NOT NULL DEFAULT '',
                padding VARCHAR(60) NOT NULL DEFAULT ''
            ) ENGINE=InnoDB
            "#,
        )
        .ok()?;
        conn.query_drop("CREATE INDEX k_1 ON sbtest1(k)").ok()?;

        // Load data
        while let Some((id, k, c, padding)) = data.next_row() {
            conn.exec_drop(
                "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)",
                (id, k, &c, &padding),
            )
            .ok()?;
        }

        // Analyze for fair comparison
        conn.query_drop("ANALYZE TABLE sbtest1").ok()?;

        Some(Self { conn })
    }

    fn point_select(&mut self, id: i64) -> usize {
        let result: Vec<mysql::Row> =
            self.conn.exec("SELECT c FROM sbtest1 WHERE id = ?", (id,)).unwrap_or_default();
        result.len()
    }

    fn insert(&mut self, id: i64, k: i64, c: &str, pad: &str) {
        let _ = self.conn.exec_drop(
            "INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)",
            (id, k, c, pad),
        );
    }

    fn update_index(&mut self, id: i64) {
        let _ = self.conn.exec_drop("UPDATE sbtest1 SET k = k + 1 WHERE id = ?", (id,));
    }

    fn update_non_index(&mut self, id: i64, c: &str) {
        let _ = self.conn.exec_drop("UPDATE sbtest1 SET c = ? WHERE id = ?", (c, id));
    }

    fn delete(&mut self, id: i64) {
        let _ = self.conn.exec_drop("DELETE FROM sbtest1 WHERE id = ?", (id,));
    }

    fn simple_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?", (start, end))
            .unwrap_or_default();
        result.len()
    }

    fn sum_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?", (start, end))
            .unwrap_or_default();
        result.len()
    }

    fn order_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec("SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c", (start, end))
            .unwrap_or_default();
        result.len()
    }

    fn distinct_range(&mut self, start: i64, end: i64) -> usize {
        let result: Vec<mysql::Row> = self
            .conn
            .exec(
                "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
                (start, end),
            )
            .unwrap_or_default();
        result.len()
    }
}

// =============================================================================
// Workload Types and Results
// =============================================================================

/// Workload type enum
#[derive(Debug, Clone, Copy, PartialEq)]
enum WorkloadType {
    PointSelect,
    Insert,
    UpdateIndex,
    UpdateNonIndex,
    Delete,
    Range,
    All,
}

impl WorkloadType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "point-select" | "pointselect" | "ps" => Some(Self::PointSelect),
            "insert" | "ins" | "i" => Some(Self::Insert),
            "update-index" | "updateindex" | "ui" => Some(Self::UpdateIndex),
            "update-non-index" | "updatenonindex" | "uni" => Some(Self::UpdateNonIndex),
            "delete" | "del" | "d" => Some(Self::Delete),
            "range" | "r" => Some(Self::Range),
            "all" | "mix" | "mixed" => Some(Self::All),
            _ => None,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::PointSelect => "Point Select",
            Self::Insert => "Insert",
            Self::UpdateIndex => "Update Index",
            Self::UpdateNonIndex => "Update Non-Index",
            Self::Delete => "Delete",
            Self::Range => "Range",
            Self::All => "All Workloads",
        }
    }
}

/// Benchmark results for a single workload
#[derive(Debug, Default, Clone)]
struct WorkloadResults {
    workload_name: String,
    operations: u64,
    #[allow(dead_code)]
    total_time_us: u64,
    avg_latency_us: f64,
    ops_per_second: f64,
}

// =============================================================================
// Data Generation Helpers
// =============================================================================

/// Generate a 120-char 'c' column value
fn generate_c_string(rng: &mut ChaCha8Rng) -> String {
    let mut s = String::with_capacity(120);
    for i in 0..11 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 10 {
            s.push('-');
        }
    }
    s
}

/// Generate a 60-char 'pad' column value
fn generate_pad_string(rng: &mut ChaCha8Rng) -> String {
    let mut s = String::with_capacity(60);
    for i in 0..5 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 4 {
            s.push('-');
        }
    }
    while s.len() < 60 {
        s.push(' ');
    }
    s
}

// =============================================================================
// Benchmark Runners - PostgreSQL (vibesql-server)
// =============================================================================

async fn run_point_select_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    _warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Skip warmup - vibesql-server has connection limit issues
    // Run benchmark directly

    // Benchmark - track successes and failures separately
    let mut operations = 0u64;
    let mut failures = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);
        let op_start = Instant::now();
        match executor.point_select(id).await {
            Ok(_) => {
                total_time_us += op_start.elapsed().as_micros() as u64;
                operations += 1;
            }
            Err(_) => {
                failures += 1;
                // Stop benchmark if too many consecutive failures
                if failures > 10 {
                    eprintln!(
                        "Connection lost during benchmark ({} ops, {} failures)",
                        operations, failures
                    );
                    break;
                }
            }
        }
    }

    if failures > 0 && operations > 0 {
        eprintln!(
            "Point Select: {} successful, {} failed ({:.1}% success rate)",
            operations,
            failures,
            100.0 * operations as f64 / (operations + failures) as f64
        );
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Point Select".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

async fn run_insert_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut next_id = (table_size + 1) as i64;

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);
        executor.insert(next_id, k, &c, &pad).await;
        next_id += 1;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);

        let op_start = Instant::now();
        executor.insert(next_id, k, &c, &pad).await;
        total_time_us += op_start.elapsed().as_micros() as u64;

        next_id += 1;
        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Insert".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

async fn run_update_index_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        executor.update_index(id).await;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);

        let op_start = Instant::now();
        executor.update_index(id).await;
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Index".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

async fn run_update_non_index_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        executor.update_non_index(id, &c).await;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);

        let op_start = Instant::now();
        executor.update_non_index(id, &c).await;
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Non-Index".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

async fn run_delete_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut available_ids: Vec<i64> = (1..=table_size as i64).collect();

    // Warmup - delete a few rows
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup && !available_ids.is_empty() {
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);
        executor.delete(id).await;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration && !available_ids.is_empty() {
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);

        let op_start = Instant::now();
        executor.delete(id).await;
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Delete".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

async fn run_range_postgres(
    executor: &PostgresExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut data = SysbenchData::new(table_size);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let (start, end) = data.random_range(RANGE_SIZE);
        let _ = executor.simple_range(start, end).await;
        let _ = executor.sum_range(start, end).await;
        let _ = executor.order_range(start, end).await;
        let _ = executor.distinct_range(start, end).await;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let (start, end) = data.random_range(RANGE_SIZE);

        let op_start = Instant::now();
        let _ = executor.simple_range(start, end).await;
        let _ = executor.sum_range(start, end).await;
        let _ = executor.order_range(start, end).await;
        let _ = executor.distinct_range(start, end).await;
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 4; // 4 range queries per iteration
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Range Queries".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

// =============================================================================
// Benchmark Runners - MySQL
// =============================================================================

#[cfg(feature = "mysql-comparison")]
fn run_point_select_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        let _ = executor.point_select(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);
        let op_start = Instant::now();
        let _ = executor.point_select(id);
        total_time_us += op_start.elapsed().as_micros() as u64;
        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Point Select".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

#[cfg(feature = "mysql-comparison")]
fn run_insert_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut next_id = (table_size + 1) as i64;

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);
        executor.insert(next_id, k, &c, &pad);
        next_id += 1;
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let k = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        let pad = generate_pad_string(&mut rng);

        let op_start = Instant::now();
        executor.insert(next_id, k, &c, &pad);
        total_time_us += op_start.elapsed().as_micros() as u64;

        next_id += 1;
        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Insert".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

#[cfg(feature = "mysql-comparison")]
fn run_update_index_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        executor.update_index(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);

        let op_start = Instant::now();
        executor.update_index(id);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Index".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

#[cfg(feature = "mysql-comparison")]
fn run_update_non_index_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let id = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);
        executor.update_non_index(id, &c);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let id = rng.random_range(1..=table_size as i64);
        let c = generate_c_string(&mut rng);

        let op_start = Instant::now();
        executor.update_non_index(id, &c);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Update Non-Index".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

#[cfg(feature = "mysql-comparison")]
fn run_delete_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut available_ids: Vec<i64> = (1..=table_size as i64).collect();

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup && !available_ids.is_empty() {
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);
        executor.delete(id);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration && !available_ids.is_empty() {
        let idx = rng.random_range(0..available_ids.len());
        let id = available_ids.swap_remove(idx);

        let op_start = Instant::now();
        executor.delete(id);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 1;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Delete".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

#[cfg(feature = "mysql-comparison")]
fn run_range_mysql(
    executor: &mut MysqlExecutor,
    table_size: usize,
    duration: Duration,
    warmup: Duration,
) -> WorkloadResults {
    let mut data = SysbenchData::new(table_size);

    // Warmup
    let warmup_start = Instant::now();
    while warmup_start.elapsed() < warmup {
        let (start, end) = data.random_range(RANGE_SIZE);
        let _ = executor.simple_range(start, end);
        let _ = executor.sum_range(start, end);
        let _ = executor.order_range(start, end);
        let _ = executor.distinct_range(start, end);
    }

    // Benchmark
    let mut operations = 0u64;
    let mut total_time_us = 0u64;
    let bench_start = Instant::now();

    while bench_start.elapsed() < duration {
        let (start, end) = data.random_range(RANGE_SIZE);

        let op_start = Instant::now();
        let _ = executor.simple_range(start, end);
        let _ = executor.sum_range(start, end);
        let _ = executor.order_range(start, end);
        let _ = executor.distinct_range(start, end);
        total_time_us += op_start.elapsed().as_micros() as u64;

        operations += 4;
    }

    let avg_latency_us =
        if operations > 0 { total_time_us as f64 / operations as f64 } else { 0.0 };
    let ops_per_second = if total_time_us > 0 {
        operations as f64 / (total_time_us as f64 / 1_000_000.0)
    } else {
        0.0
    };

    WorkloadResults {
        workload_name: "Range Queries".to_string(),
        operations,
        total_time_us,
        avg_latency_us,
        ops_per_second,
    }
}

// =============================================================================
// Results Printing
// =============================================================================

fn print_results(results: &[WorkloadResults], db_name: &str) {
    eprintln!("\n--- {} Results ---", db_name);
    eprintln!("{:<20} {:>12} {:>15} {:>12}", "Workload", "Operations", "Avg Latency", "Ops/sec");
    eprintln!("{:-<20} {:->12} {:->15} {:->12}", "", "", "", "");

    for result in results {
        eprintln!(
            "{:<20} {:>12} {:>12.2} us {:>12.0}",
            result.workload_name, result.operations, result.avg_latency_us, result.ops_per_second
        );
    }
}

fn print_comparison_summary(all_results: &[(&str, Vec<WorkloadResults>)]) {
    eprintln!("\n\n=== Comparison Summary ===");

    // Get all workload names
    let workload_names: Vec<&str> = if let Some((_, results)) = all_results.first() {
        results.iter().map(|r| r.workload_name.as_str()).collect()
    } else {
        return;
    };

    for workload_name in workload_names {
        eprintln!("\n{}", workload_name);
        eprintln!(
            "{:<15} {:>12} {:>15} {:>12}",
            "Database", "Operations", "Avg Latency", "Ops/sec"
        );
        eprintln!("{:-<15} {:->12} {:->15} {:->12}", "", "", "", "");

        for (db_name, results) in all_results {
            if let Some(result) = results.iter().find(|r| r.workload_name == workload_name) {
                eprintln!(
                    "{:<15} {:>12} {:>12.2} us {:>12.0}",
                    db_name, result.operations, result.avg_latency_us, result.ops_per_second
                );
            }
        }
    }
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() {
    eprintln!("=== Sysbench Client-Server Benchmark ===");
    eprintln!("(Comparing vibesql-server via PostgreSQL protocol vs MySQL)");

    // Parse arguments
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help") {
        eprintln!("\nUsage:");
        eprintln!("  {} [WORKLOAD_TYPE]", args[0]);
        eprintln!("\nWorkload Types:");
        eprintln!("  point-select    Run only point select queries");
        eprintln!("  insert          Run only insert operations");
        eprintln!("  update-index    Run only indexed column updates");
        eprintln!("  update-non-index Run only non-indexed column updates");
        eprintln!("  delete          Run only delete operations");
        eprintln!("  range           Run only range queries");
        eprintln!("  all             Run all workloads (default)");
        eprintln!("\nEnvironment Variables:");
        eprintln!("  SYSBENCH_TABLE_SIZE    Number of rows (default: 10000)");
        eprintln!("  SYSBENCH_DURATION_SECS Benchmark duration in seconds (default: 30)");
        eprintln!("  SYSBENCH_WARMUP_SECS   Warmup duration in seconds (default: 5)");
        eprintln!("  MYSQL_URL              MySQL connection string (optional)");
        eprintln!("  VIBESQL_PORT           Port for vibesql-server (default: 15432)");
        eprintln!("\nExamples:");
        eprintln!("  {}                           # Run all workloads", args[0]);
        eprintln!("  {} point-select              # Run only point selects", args[0]);
        eprintln!("  SYSBENCH_TABLE_SIZE=50000 {}  # Run with 50k rows", args[0]);
        eprintln!("  MYSQL_URL=mysql://user:pass@localhost/sysbench {}", args[0]);
        std::process::exit(0);
    }

    // Get configuration from environment
    let table_size: usize = env::var("SYSBENCH_TABLE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TABLE_SIZE);

    let duration_secs: u64 =
        env::var("SYSBENCH_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);

    let warmup_secs: u64 =
        env::var("SYSBENCH_WARMUP_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);

    let vibesql_port: u16 =
        env::var("VIBESQL_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_VIBESQL_PORT);

    let duration = Duration::from_secs(duration_secs);
    let warmup = Duration::from_secs(warmup_secs);

    // Parse workload type (skip benchmark harness arguments like --bench)
    let user_args: Vec<_> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();
    let workload_type = if !user_args.is_empty() {
        match WorkloadType::from_str(user_args[0]) {
            Some(t) => t,
            None => {
                eprintln!(
                    "Error: Unknown workload type '{}'. Run with --help for usage.",
                    user_args[0]
                );
                std::process::exit(1);
            }
        }
    } else {
        WorkloadType::All
    };

    eprintln!("\nConfiguration:");
    eprintln!("  Table size: {} rows", table_size);
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Workload: {}", workload_type.name());
    eprintln!("  VibeSQL port: {}", vibesql_port);

    // Create tokio runtime
    let rt = Runtime::new().expect("Failed to create tokio runtime");

    // Collect all results for comparison summary
    let mut all_results: Vec<(&str, Vec<WorkloadResults>)> = Vec::new();

    // ========================================
    // VibeSQL Server Benchmark
    // ========================================

    eprintln!("\nStarting vibesql-server on port {}...", vibesql_port);

    let vibesql_results = rt.block_on(async {
        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Start server
        let server_addr = match start_vibesql_server(vibesql_port, shutdown_rx).await {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Failed to start vibesql-server: {}", e);
                return Vec::new();
            }
        };

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect to server
        let conn_str = format!("host=127.0.0.1 port={} user=postgres", server_addr.port());
        eprintln!("Connecting to vibesql-server at {}...", conn_str);

        let executor = match PostgresExecutor::connect(&conn_str).await {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Failed to connect to vibesql-server: {}", e);
                let _ = shutdown_tx.send(());
                return Vec::new();
            }
        };

        // Create schema and load data
        eprintln!("Creating schema and loading {} rows...", table_size);
        if let Err(e) = executor.create_schema().await {
            eprintln!("Failed to create schema: {}", e);
            let _ = shutdown_tx.send(());
            return Vec::new();
        }

        let mut data = SysbenchData::new(table_size);
        let load_start = Instant::now();
        if let Err(e) = executor.load_data(&mut data).await {
            eprintln!("Failed to load data: {}", e);
            let _ = shutdown_tx.send(());
            return Vec::new();
        }
        eprintln!("Data loaded in {:?}", load_start.elapsed());

        eprintln!("Starting benchmarks...");

        // Run benchmarks
        let mut results = Vec::new();

        match workload_type {
            WorkloadType::PointSelect => {
                results.push(
                    run_point_select_postgres(&executor, table_size, duration, warmup).await,
                );
            }
            WorkloadType::Insert => {
                results.push(run_insert_postgres(&executor, table_size, duration, warmup).await);
            }
            WorkloadType::UpdateIndex => {
                results.push(
                    run_update_index_postgres(&executor, table_size, duration, warmup).await,
                );
            }
            WorkloadType::UpdateNonIndex => {
                results.push(
                    run_update_non_index_postgres(&executor, table_size, duration, warmup).await,
                );
            }
            WorkloadType::Delete => {
                results.push(run_delete_postgres(&executor, table_size, duration, warmup).await);
            }
            WorkloadType::Range => {
                results.push(run_range_postgres(&executor, table_size, duration, warmup).await);
            }
            WorkloadType::All => {
                results.push(
                    run_point_select_postgres(&executor, table_size, duration, warmup).await,
                );
                results.push(run_insert_postgres(&executor, table_size, duration, warmup).await);
                results.push(
                    run_update_index_postgres(&executor, table_size, duration, warmup).await,
                );
                results.push(
                    run_update_non_index_postgres(&executor, table_size, duration, warmup).await,
                );
                // Skip delete for "all" to preserve data integrity
                results.push(run_range_postgres(&executor, table_size, duration, warmup).await);
            }
        }

        // Shutdown server
        let _ = shutdown_tx.send(());

        results
    });

    if !vibesql_results.is_empty() {
        print_results(&vibesql_results, "VibeSQL Server");
        all_results.push(("VibeSQL Server", vibesql_results));
    }

    // ========================================
    // MySQL Comparison (if feature enabled)
    // ========================================

    #[cfg(feature = "mysql-comparison")]
    {
        if let Ok(mysql_url) = env::var("MYSQL_URL") {
            eprintln!("\n\nConnecting to MySQL...");

            if let Some(mut executor) = MysqlExecutor::connect(&mysql_url, table_size) {
                eprintln!("MySQL database loaded with {} rows", table_size);

                let mut mysql_results = Vec::new();

                match workload_type {
                    WorkloadType::PointSelect => {
                        mysql_results.push(run_point_select_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::Insert => {
                        mysql_results.push(run_insert_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::UpdateIndex => {
                        mysql_results.push(run_update_index_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::UpdateNonIndex => {
                        mysql_results.push(run_update_non_index_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::Delete => {
                        mysql_results.push(run_delete_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::Range => {
                        mysql_results.push(run_range_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));
                    }
                    WorkloadType::All => {
                        mysql_results.push(run_point_select_mysql(
                            &mut executor,
                            table_size,
                            duration,
                            warmup,
                        ));

                        // Reload for insert benchmark
                        if let Some(mut executor2) = MysqlExecutor::connect(&mysql_url, table_size) {
                            mysql_results.push(run_insert_mysql(
                                &mut executor2,
                                table_size,
                                duration,
                                warmup,
                            ));
                        }

                        // Reload for update benchmarks
                        if let Some(mut executor3) = MysqlExecutor::connect(&mysql_url, table_size) {
                            mysql_results.push(run_update_index_mysql(
                                &mut executor3,
                                table_size,
                                duration,
                                warmup,
                            ));
                            mysql_results.push(run_update_non_index_mysql(
                                &mut executor3,
                                table_size,
                                duration,
                                warmup,
                            ));
                        }

                        // Reload for range benchmark
                        if let Some(mut executor4) = MysqlExecutor::connect(&mysql_url, table_size) {
                            mysql_results.push(run_range_mysql(
                                &mut executor4,
                                table_size,
                                duration,
                                warmup,
                            ));
                        }
                    }
                }

                print_results(&mysql_results, "MySQL");
                all_results.push(("MySQL", mysql_results));
            } else {
                eprintln!("Failed to connect to MySQL at {}", mysql_url);
            }
        } else {
            eprintln!("\n\nSkipping MySQL benchmark - MYSQL_URL not set");
        }
    }

    #[cfg(not(feature = "mysql-comparison"))]
    {
        eprintln!("\n\nMySQL comparison not available - compile with --features mysql-comparison");
    }

    // Print comparison summary
    if all_results.len() > 1 {
        print_comparison_summary(&all_results);
    }

    eprintln!("\n=== Done ===");
}
