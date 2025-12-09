//! TPC-C Client-Server Benchmark
//!
//! This benchmark compares TPC-C transaction performance between vibesql-server (PostgreSQL wire
//! protocol) and MySQL, providing a fair apples-to-apples comparison for OLTP workloads.
//!
//! ## Why This Benchmark?
//!
//! The existing `tpcc_benchmark.rs` compares embedded (in-process) databases. This benchmark
//! uses vibesql-server with PostgreSQL wire protocol, providing an equivalent client-server
//! comparison against MySQL for OLTP workloads.
//!
//! ## Usage
//!
//! ```bash
//! # Build and run (VibeSQL server only)
//! cargo bench --package vibesql-executor --bench tpcc_server --no-run
//! ./target/release/deps/tpcc_server-*
//!
//! # With MySQL comparison
//! MYSQL_URL=mysql://user:pass@localhost:3306/tpcc \
//! ./target/release/deps/tpcc_server-*
//!
//! # Run specific transaction type
//! ./target/release/deps/tpcc_server-* new-order
//! ./target/release/deps/tpcc_server-* payment
//! ```
//!
//! ## Environment Variables
//!
//! - `TPCC_SCALE_FACTOR` - Number of warehouses (default: 1)
//! - `TPCC_DURATION_SECS` - Benchmark duration in seconds (default: 60)
//! - `TPCC_WARMUP_SECS` - Warmup duration in seconds (default: 10)
//! - `MYSQL_URL` - MySQL connection string (optional)
//! - `VIBESQL_PORT` - Port for vibesql-server (default: 15434)

mod tpcc;

use std::env;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tpcc::data::TPCCRng;
use tpcc::schema::load_vibesql;
use tpcc::transactions::*;

// PostgreSQL wire protocol client
use tokio_postgres::NoTls;

// MySQL client (optional)
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::Pool;
#[cfg(feature = "mysql-comparison")]
use tpcc::schema::load_mysql;

/// Default port for vibesql-server (different from other server benchmarks to avoid conflicts)
/// - 15432: sysbench_server
/// - 15433: tpch_server
/// - 15434: tpcc_server
const DEFAULT_VIBESQL_PORT: u16 = 15434;

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

/// Benchmark results for TPC-C transactions
#[derive(Debug, Clone, Default)]
struct TPCCServerResults {
    total_transactions: u64,
    successful_transactions: u64,
    failed_transactions: u64,
    total_duration_ms: u64,
    transactions_per_second: f64,

    // Per-transaction type breakdown (averages computed for reporting)
    new_order_count: u64,
    new_order_avg_us: f64,

    payment_count: u64,
    payment_avg_us: f64,

    order_status_count: u64,
    order_status_avg_us: f64,

    delivery_count: u64,
    delivery_avg_us: f64,

    stock_level_count: u64,
    stock_level_avg_us: f64,
}

// =============================================================================
// Server Startup Helper
// =============================================================================

/// Start vibesql-server with pre-loaded TPC-C database
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

    // Create database registry with pre-loaded TPC-C database
    let database_registry = DatabaseRegistry::new();
    database_registry.register_database("tpcc", db).await;

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

struct PostgresTPCCExecutor {
    client: tokio_postgres::Client,
    num_warehouses: i32,
}

impl PostgresTPCCExecutor {
    async fn connect(
        addr: &str,
        num_warehouses: i32,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (client, connection) = tokio_postgres::connect(addr, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self { client, num_warehouses })
    }

    async fn query(&self, sql: &str) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
        self.client.query(sql, &[]).await
    }

    /// Execute New-Order transaction
    async fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let w_query = format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id);
        if let Err(e) = self.query(&w_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        let d_query = format!(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer info
        let c_query = format!(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
            input.w_id, input.d_id, input.c_id
        );
        if let Err(e) = self.query(&c_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Process each order line - query item and stock info
        for item in &input.items {
            // Get item info
            let i_query =
                format!("SELECT i_price, i_name, i_data FROM item WHERE i_id = {}", item.ol_i_id);
            if let Err(e) = self.query(&i_query).await {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Item query failed: {}", e)),
                };
            }

            // Get stock info
            let s_query = format!(
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = {} AND s_w_id = {}",
                item.ol_i_id, item.ol_supply_w_id
            );
            if let Err(e) = self.query(&s_query).await {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock query failed: {}", e)),
                };
            }
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    /// Execute Payment transaction
    async fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse info
        let w_query = format!(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}",
            input.w_id
        );
        if let Err(e) = self.query(&w_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        let d_query = format!(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer (by ID or last name)
        let c_query = if let Some(c_id) = input.c_id {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.c_w_id, input.c_d_id, c_id
            )
        } else {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()
            )
        };
        if let Err(e) = self.query(&c_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    /// Execute Order-Status transaction
    async fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();

        // Get customer (by ID or last name)
        let c_query = if let Some(c_id) = input.c_id {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.w_id, input.d_id, c_id
            )
        } else {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.w_id, input.d_id, input.c_last.as_ref().unwrap()
            )
        };
        if let Err(e) = self.query(&c_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Get last order for customer
        let c_id = input.c_id.unwrap_or(1);
        let o_query = format!(
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
            input.w_id, input.d_id, c_id
        );
        if let Err(e) = self.query(&o_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Order query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    /// Execute Delivery transaction
    async fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district
        for d_id in 1..=10 {
            // Get oldest undelivered order
            let no_query = format!(
                "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                input.w_id, d_id
            );
            if let Err(e) = self.query(&no_query).await {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("New order query failed: {}", e)),
                };
            }
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    /// Execute Stock-Level transaction
    async fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();

        // Get next order ID for district
        let d_query = format!(
            "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Count distinct items with low stock in recent orders
        // This is the key Stock-Level query
        let sl_query = format!(
            "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock \
             WHERE ol_w_id = {} AND ol_d_id = {} \
             AND ol_o_id >= (SELECT d_next_o_id - 20 FROM district WHERE d_w_id = {} AND d_id = {}) \
             AND s_w_id = {} AND s_i_id = ol_i_id AND s_quantity < {}",
            input.w_id, input.d_id, input.w_id, input.d_id, input.w_id, input.threshold
        );
        if let Err(e) = self.query(&sl_query).await {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Stock level query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    /// Run a single transaction based on type
    async fn run_transaction(&self, tx_type: TransactionType, rng: &mut TPCCRng) -> TransactionResult {
        match tx_type {
            TransactionType::NewOrder => {
                let input = generate_new_order_input(rng, self.num_warehouses);
                self.new_order(&input).await
            }
            TransactionType::Payment => {
                let input = generate_payment_input(rng, self.num_warehouses);
                self.payment(&input).await
            }
            TransactionType::OrderStatus => {
                let input = generate_order_status_input(rng, self.num_warehouses);
                self.order_status(&input).await
            }
            TransactionType::Delivery => {
                let input = generate_delivery_input(rng, self.num_warehouses);
                self.delivery(&input).await
            }
            TransactionType::StockLevel => {
                let input = generate_stock_level_input(rng, self.num_warehouses);
                self.stock_level(&input).await
            }
            TransactionType::Mixed => {
                // Standard TPC-C mix: 45% New-Order, 43% Payment, 4% Order-Status, 4% Delivery, 4% Stock-Level
                let r = rng.random_int(1, 100);
                if r <= 45 {
                    let input = generate_new_order_input(rng, self.num_warehouses);
                    self.new_order(&input).await
                } else if r <= 88 {
                    let input = generate_payment_input(rng, self.num_warehouses);
                    self.payment(&input).await
                } else if r <= 92 {
                    let input = generate_order_status_input(rng, self.num_warehouses);
                    self.order_status(&input).await
                } else if r <= 96 {
                    let input = generate_delivery_input(rng, self.num_warehouses);
                    self.delivery(&input).await
                } else {
                    let input = generate_stock_level_input(rng, self.num_warehouses);
                    self.stock_level(&input).await
                }
            }
        }
    }
}

// =============================================================================
// MySQL Executor
// =============================================================================

#[cfg(feature = "mysql-comparison")]
struct MysqlTPCCExecutor {
    pool: Pool,
    num_warehouses: i32,
}

#[cfg(feature = "mysql-comparison")]
impl MysqlTPCCExecutor {
    fn connect(url: &str, num_warehouses: i32) -> Option<Self> {
        let pool = Pool::new(url).ok()?;
        Some(Self { pool, num_warehouses })
    }

    fn query(&self, sql: &str) -> Result<Vec<mysql::Row>, mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        conn.query(sql)
    }

    fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let w_query = format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id);
        if let Err(e) = self.query(&w_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        let d_query = format!(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer info
        let c_query = format!(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
            input.w_id, input.d_id, input.c_id
        );
        if let Err(e) = self.query(&c_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Process each order line
        for item in &input.items {
            let i_query =
                format!("SELECT i_price, i_name, i_data FROM item WHERE i_id = {}", item.ol_i_id);
            if let Err(e) = self.query(&i_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Item query failed: {}", e)),
                };
            }

            let s_query = format!(
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = {} AND s_w_id = {}",
                item.ol_i_id, item.ol_supply_w_id
            );
            if let Err(e) = self.query(&s_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock query failed: {}", e)),
                };
            }
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        let w_query = format!(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}",
            input.w_id
        );
        if let Err(e) = self.query(&w_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        let d_query = format!(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        let c_query = if let Some(c_id) = input.c_id {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.c_w_id, input.c_d_id, c_id
            )
        } else {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()
            )
        };
        if let Err(e) = self.query(&c_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();

        let c_query = if let Some(c_id) = input.c_id {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.w_id, input.d_id, c_id
            )
        } else {
            format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.w_id, input.d_id, input.c_last.as_ref().unwrap()
            )
        };
        if let Err(e) = self.query(&c_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        let c_id = input.c_id.unwrap_or(1);
        let o_query = format!(
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
            input.w_id, input.d_id, c_id
        );
        if let Err(e) = self.query(&o_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Order query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        for d_id in 1..=10 {
            let no_query = format!(
                "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                input.w_id, d_id
            );
            if let Err(e) = self.query(&no_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("New order query failed: {}", e)),
                };
            }
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();

        let d_query = format!(
            "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        if let Err(e) = self.query(&d_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        let sl_query = format!(
            "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock \
             WHERE ol_w_id = {} AND ol_d_id = {} \
             AND ol_o_id >= (SELECT d_next_o_id - 20 FROM district WHERE d_w_id = {} AND d_id = {}) \
             AND s_w_id = {} AND s_i_id = ol_i_id AND s_quantity < {}",
            input.w_id, input.d_id, input.w_id, input.d_id, input.w_id, input.threshold
        );
        if let Err(e) = self.query(&sl_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Stock level query failed: {}", e)),
            };
        }

        TransactionResult { success: true, duration_us: start.elapsed().as_micros() as u64, error: None }
    }

    fn run_transaction(&self, tx_type: TransactionType, rng: &mut TPCCRng) -> TransactionResult {
        match tx_type {
            TransactionType::NewOrder => {
                let input = generate_new_order_input(rng, self.num_warehouses);
                self.new_order(&input)
            }
            TransactionType::Payment => {
                let input = generate_payment_input(rng, self.num_warehouses);
                self.payment(&input)
            }
            TransactionType::OrderStatus => {
                let input = generate_order_status_input(rng, self.num_warehouses);
                self.order_status(&input)
            }
            TransactionType::Delivery => {
                let input = generate_delivery_input(rng, self.num_warehouses);
                self.delivery(&input)
            }
            TransactionType::StockLevel => {
                let input = generate_stock_level_input(rng, self.num_warehouses);
                self.stock_level(&input)
            }
            TransactionType::Mixed => {
                let r = rng.random_int(1, 100);
                if r <= 45 {
                    let input = generate_new_order_input(rng, self.num_warehouses);
                    self.new_order(&input)
                } else if r <= 88 {
                    let input = generate_payment_input(rng, self.num_warehouses);
                    self.payment(&input)
                } else if r <= 92 {
                    let input = generate_order_status_input(rng, self.num_warehouses);
                    self.order_status(&input)
                } else if r <= 96 {
                    let input = generate_delivery_input(rng, self.num_warehouses);
                    self.delivery(&input)
                } else {
                    let input = generate_stock_level_input(rng, self.num_warehouses);
                    self.stock_level(&input)
                }
            }
        }
    }
}

// =============================================================================
// Result Formatting
// =============================================================================

fn print_results(results: &TPCCServerResults, db_name: &str, transaction_type: TransactionType) {
    eprintln!("\n=== {} TPC-C Results ===", db_name);
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

fn print_comparison(vibesql_results: &TPCCServerResults, mysql_results: &TPCCServerResults) {
    eprintln!("\n=== Comparison Summary ===");
    eprintln!(
        "{:<15} {:>15} {:>15} {:>12}",
        "Metric", "VibeSQL", "MySQL", "Ratio"
    );
    eprintln!("{:-<15} {:->15} {:->15} {:->12}", "", "", "", "");

    let ratio = if mysql_results.transactions_per_second > 0.0 {
        vibesql_results.transactions_per_second / mysql_results.transactions_per_second
    } else {
        0.0
    };
    eprintln!(
        "{:<15} {:>15.2} {:>15.2} {:>11.2}x",
        "TPS", vibesql_results.transactions_per_second, mysql_results.transactions_per_second, ratio
    );
}

fn output_json(
    vibesql_results: &TPCCServerResults,
    mysql_results: &TPCCServerResults,
    scale_factor: i32,
    duration_secs: u64,
) {
    println!("{{");
    println!("  \"benchmarks\": [");

    // VibeSQL results
    print!(
        "    {{\"name\": \"tpcc_server_mixed_vibesql_server\", \"stats\": {{\"tps\": {}, \"total_txns\": {}, \"successful_txns\": {}, \"duration_ms\": {}}}}}",
        vibesql_results.transactions_per_second,
        vibesql_results.total_transactions,
        vibesql_results.successful_transactions,
        vibesql_results.total_duration_ms
    );

    // MySQL results (if available)
    if mysql_results.total_transactions > 0 {
        println!(",");
        print!(
            "    {{\"name\": \"tpcc_server_mixed_mysql\", \"stats\": {{\"tps\": {}, \"total_txns\": {}, \"successful_txns\": {}, \"duration_ms\": {}}}}}",
            mysql_results.transactions_per_second,
            mysql_results.total_transactions,
            mysql_results.successful_transactions,
            mysql_results.total_duration_ms
        );
    }

    println!();
    println!("  ],");
    println!("  \"metadata\": {{");
    println!("    \"suite\": \"tpcc-server\",");
    println!("    \"scale_factor\": {},", scale_factor);
    println!("    \"duration_secs\": {},", duration_secs);
    println!("    \"timestamp\": \"{}\"", chrono::Utc::now().to_rfc3339());
    println!("  }}");
    println!("}}");
}

// =============================================================================
// Main Entry Point
// =============================================================================

fn main() {
    eprintln!("=== TPC-C Client-Server Benchmark ===");
    eprintln!("(Comparing vibesql-server via PostgreSQL protocol vs MySQL)");

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
        eprintln!("  TPCC_SCALE_FACTOR   Number of warehouses (default: 1)");
        eprintln!("  TPCC_DURATION_SECS  Benchmark duration in seconds (default: 60)");
        eprintln!("  TPCC_WARMUP_SECS    Warmup duration in seconds (default: 10)");
        eprintln!("  MYSQL_URL           MySQL connection string (optional)");
        eprintln!("  VIBESQL_PORT        Port for vibesql-server (default: 15434)");
        eprintln!("\nExamples:");
        eprintln!("  {}                               # Run mixed workload", args[0]);
        eprintln!("  {} new-order                     # Run only New-Order", args[0]);
        eprintln!("  TPCC_SCALE_FACTOR=2 {}           # Run with 2 warehouses", args[0]);
        eprintln!("  MYSQL_URL=mysql://user:pass@localhost/tpcc {}", args[0]);
        std::process::exit(0);
    }

    // Parse transaction type from args
    let transaction_type = if args.len() > 1 {
        TransactionType::from_str(&args[1]).unwrap_or(TransactionType::Mixed)
    } else {
        TransactionType::Mixed
    };

    // Get configuration
    let scale_factor: i32 =
        env::var("TPCC_SCALE_FACTOR").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let duration_secs: u64 =
        env::var("TPCC_DURATION_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(60);

    let warmup_secs: u64 =
        env::var("TPCC_WARMUP_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);

    let vibesql_port: u16 =
        env::var("VIBESQL_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_VIBESQL_PORT);

    eprintln!("\nConfiguration:");
    eprintln!("  Scale factor (warehouses): {}", scale_factor);
    eprintln!("  Duration: {} seconds", duration_secs);
    eprintln!("  Warmup: {} seconds", warmup_secs);
    eprintln!("  Transaction type: {}", transaction_type.name());
    eprintln!("  VibeSQL port: {}", vibesql_port);

    let rt = Runtime::new().expect("Failed to create tokio runtime");

    // Load TPC-C data using existing schema module
    eprintln!("\nLoading TPC-C data into VibeSQL ({} warehouses)...", scale_factor);
    let db = load_vibesql(scale_factor as f64);
    eprintln!("Data loaded successfully");

    // ========================================
    // VibeSQL Server Benchmark
    // ========================================

    eprintln!("\nStarting vibesql-server on port {}...", vibesql_port);

    let vibesql_results: TPCCServerResults = rt.block_on(async {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server_addr = match start_vibesql_server_with_db(vibesql_port, db, shutdown_rx).await {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Failed to start vibesql-server: {}", e);
                return TPCCServerResults::default();
            }
        };

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let conn_str = format!("host=127.0.0.1 port={} user=postgres dbname=tpcc", server_addr.port());
        eprintln!("Connecting to vibesql-server at {}...", conn_str);

        let executor = match PostgresTPCCExecutor::connect(&conn_str, scale_factor).await {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Failed to connect to vibesql-server: {}", e);
                let _ = shutdown_tx.send(());
                return TPCCServerResults::default();
            }
        };

        eprintln!("Connected successfully");

        // Warmup phase
        if warmup_secs > 0 {
            eprintln!("\nWarmup phase ({} seconds)...", warmup_secs);
            let warmup_end = Instant::now() + Duration::from_secs(warmup_secs);
            let mut rng = TPCCRng::new(42);
            while Instant::now() < warmup_end {
                let _ = executor.run_transaction(transaction_type, &mut rng).await;
            }
        }

        // Benchmark phase
        eprintln!("\nBenchmark phase ({} seconds)...", duration_secs);
        let mut results = TPCCServerResults::default();
        let mut rng = TPCCRng::new(12345);
        let benchmark_start = Instant::now();
        let benchmark_end = benchmark_start + Duration::from_secs(duration_secs);

        while Instant::now() < benchmark_end {
            let tx_result = executor.run_transaction(transaction_type, &mut rng).await;
            results.total_transactions += 1;

            if tx_result.success {
                results.successful_transactions += 1;
            } else {
                results.failed_transactions += 1;
            }

            // Track per-transaction type stats (for mixed workload)
            // Note: We don't track which type was run in mixed mode for simplicity
        }

        results.total_duration_ms = benchmark_start.elapsed().as_millis() as u64;
        results.transactions_per_second = results.successful_transactions as f64
            / (results.total_duration_ms as f64 / 1000.0);

        let _ = shutdown_tx.send(());
        results
    });

    print_results(&vibesql_results, "VibeSQL Server", transaction_type);

    // ========================================
    // MySQL Comparison (if feature enabled)
    // ========================================

    #[allow(unused_mut)]
    let mut mysql_results = TPCCServerResults::default();

    #[cfg(feature = "mysql-comparison")]
    {
        if let Ok(mysql_url) = env::var("MYSQL_URL") {
            eprintln!("\n\nLoading TPC-C data into MySQL...");

            if let Some(_mysql_conn) = load_mysql(scale_factor as f64) {
                eprintln!("MySQL data loaded successfully");

                if let Some(executor) = MysqlTPCCExecutor::connect(&mysql_url, scale_factor) {
                    // Warmup phase
                    if warmup_secs > 0 {
                        eprintln!("\nWarmup phase ({} seconds)...", warmup_secs);
                        let warmup_end = Instant::now() + Duration::from_secs(warmup_secs);
                        let mut rng = TPCCRng::new(42);
                        while Instant::now() < warmup_end {
                            let _ = executor.run_transaction(transaction_type, &mut rng);
                        }
                    }

                    // Benchmark phase
                    eprintln!("\nBenchmark phase ({} seconds)...", duration_secs);
                    let mut rng = TPCCRng::new(12345);
                    let benchmark_start = Instant::now();
                    let benchmark_end = benchmark_start + Duration::from_secs(duration_secs);

                    while Instant::now() < benchmark_end {
                        let tx_result = executor.run_transaction(transaction_type, &mut rng);
                        mysql_results.total_transactions += 1;

                        if tx_result.success {
                            mysql_results.successful_transactions += 1;
                        } else {
                            mysql_results.failed_transactions += 1;
                        }
                    }

                    mysql_results.total_duration_ms = benchmark_start.elapsed().as_millis() as u64;
                    mysql_results.transactions_per_second = mysql_results.successful_transactions as f64
                        / (mysql_results.total_duration_ms as f64 / 1000.0);

                    print_results(&mysql_results, "MySQL", transaction_type);
                } else {
                    eprintln!("Failed to connect to MySQL at {}", mysql_url);
                }
            } else {
                eprintln!("Failed to load MySQL database");
            }
        }
    }

    // Print comparison if both ran
    if vibesql_results.total_transactions > 0 && mysql_results.total_transactions > 0 {
        print_comparison(&vibesql_results, &mysql_results);
    }

    // Output JSON for parsing
    output_json(&vibesql_results, &mysql_results, scale_factor, duration_secs);
}
