// ============================================================================
// ⚠️  BENCHMARK INTEGRITY WARNING ⚠️
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. This includes:
//
// - Direct index/table access instead of SQL queries
// - Caching or memoization of query results
// - Conditional logic that detects benchmarks and takes shortcuts
// - Any code path that doesn't execute real SQL
//
// Benchmarks MUST execute actual SQL to produce meaningful results.
// "Optimizing" benchmarks this way is cheating and has happened before.
// If you're tempted to add a fast-path, DON'T. Fix the actual performance
// issue in the query engine instead.
// ============================================================================

//! TPC-C Transaction Implementations
//!
//! This module implements the 5 TPC-C transactions:
//! 1. New-Order (45%): Create new order with multiple line items
//! 2. Payment (43%): Process customer payment
//! 3. Order-Status (4%): Query customer's last order
//! 4. Delivery (4%): Batch process pending orders
//! 5. Stock-Level (4%): Check low stock items

use std::time::Instant;

/// Trait for TPC-C transaction executors.
///
/// This trait abstracts over different database backends (VibeSQL, SQLite, DuckDB)
/// allowing a single generic benchmark runner to work with any executor type.
///
/// Note: The `Sync` bound is NOT part of this trait because some backends (SQLite, DuckDB)
/// use connections that aren't thread-safe. For parallel execution, these backends use
/// dedicated parallel functions that create per-client connections. Only backends that
/// support shared executor references (like VibeSQL) use the generic `run_parallel_benchmark`
/// function which requires `E: TPCCExecutor + Sync`.
pub trait TPCCExecutor {
    fn new_order(&self, input: &NewOrderInput) -> TransactionResult;
    fn payment(&self, input: &PaymentInput) -> TransactionResult;
    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult;
    fn delivery(&self, input: &DeliveryInput) -> TransactionResult;
    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult;
}

use super::data::TPCCRng;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Transaction input for New-Order
#[derive(Debug, Clone)]
pub struct NewOrderInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_id: i32,
    pub ol_cnt: i32,
    pub items: Vec<NewOrderItemInput>,
}

#[derive(Debug, Clone)]
pub struct NewOrderItemInput {
    pub ol_i_id: i32,
    pub ol_supply_w_id: i32,
    pub ol_quantity: i32,
}

/// Transaction input for Payment
#[derive(Debug, Clone)]
pub struct PaymentInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_w_id: i32,
    pub c_d_id: i32,
    pub c_id: Option<i32>,
    pub c_last: Option<String>,
    pub h_amount: f64,
}

/// Transaction input for Order-Status
#[derive(Debug, Clone)]
pub struct OrderStatusInput {
    pub w_id: i32,
    pub d_id: i32,
    pub c_id: Option<i32>,
    pub c_last: Option<String>,
}

/// Transaction input for Delivery
#[derive(Debug, Clone)]
pub struct DeliveryInput {
    pub w_id: i32,
    pub o_carrier_id: i32,
}

/// Transaction input for Stock-Level
#[derive(Debug, Clone)]
pub struct StockLevelInput {
    pub w_id: i32,
    pub d_id: i32,
    pub threshold: i32,
}

/// Transaction result with timing information
#[derive(Debug, Clone)]
pub struct TransactionResult {
    pub success: bool,
    pub duration_us: u64,
    pub error: Option<String>,
}

/// Generate random New-Order transaction input
pub fn generate_new_order_input(rng: &mut TPCCRng, num_warehouses: i32) -> NewOrderInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;
    let c_id = rng.nurand(1023, 1, 3000) as i32;
    let ol_cnt = rng.random_int(5, 15) as i32;

    let mut items = Vec::with_capacity(ol_cnt as usize);
    for _ in 0..ol_cnt {
        // 1% of items are from remote warehouse
        let ol_supply_w_id = if num_warehouses > 1 && rng.random_int(1, 100) == 1 {
            let mut remote = rng.random_int(1, num_warehouses as i64) as i32;
            while remote == w_id && num_warehouses > 1 {
                remote = rng.random_int(1, num_warehouses as i64) as i32;
            }
            remote
        } else {
            w_id
        };

        items.push(NewOrderItemInput {
            ol_i_id: rng.nurand(8191, 1, 100000) as i32,
            ol_supply_w_id,
            ol_quantity: rng.random_int(1, 10) as i32,
        });
    }

    NewOrderInput { w_id, d_id, c_id, ol_cnt, items }
}

/// Generate random Payment transaction input
pub fn generate_payment_input(rng: &mut TPCCRng, num_warehouses: i32) -> PaymentInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;

    // 85% local, 15% remote
    let (c_w_id, c_d_id) = if num_warehouses > 1 && rng.random_int(1, 100) <= 15 {
        let mut remote_w = rng.random_int(1, num_warehouses as i64) as i32;
        while remote_w == w_id && num_warehouses > 1 {
            remote_w = rng.random_int(1, num_warehouses as i64) as i32;
        }
        (remote_w, rng.random_int(1, 10) as i32)
    } else {
        (w_id, d_id)
    };

    // 60% by customer ID, 40% by last name
    let (c_id, c_last) = if rng.random_int(1, 100) <= 60 {
        (Some(rng.nurand(1023, 1, 3000) as i32), None)
    } else {
        (None, Some(TPCCRng::last_name(rng.nurand(255, 0, 999))))
    };

    PaymentInput {
        w_id,
        d_id,
        c_w_id,
        c_d_id,
        c_id,
        c_last,
        h_amount: rng.random_int(100, 500000) as f64 / 100.0,
    }
}

/// Generate random Order-Status transaction input
pub fn generate_order_status_input(rng: &mut TPCCRng, num_warehouses: i32) -> OrderStatusInput {
    let w_id = rng.random_int(1, num_warehouses as i64) as i32;
    let d_id = rng.random_int(1, 10) as i32;

    // 60% by customer ID, 40% by last name
    let (c_id, c_last) = if rng.random_int(1, 100) <= 60 {
        (Some(rng.nurand(1023, 1, 3000) as i32), None)
    } else {
        (None, Some(TPCCRng::last_name(rng.nurand(255, 0, 999))))
    };

    OrderStatusInput { w_id, d_id, c_id, c_last }
}

/// Generate random Delivery transaction input
pub fn generate_delivery_input(rng: &mut TPCCRng, num_warehouses: i32) -> DeliveryInput {
    DeliveryInput {
        w_id: rng.random_int(1, num_warehouses as i64) as i32,
        o_carrier_id: rng.random_int(1, 10) as i32,
    }
}

/// Generate random Stock-Level transaction input
pub fn generate_stock_level_input(rng: &mut TPCCRng, num_warehouses: i32) -> StockLevelInput {
    StockLevelInput {
        w_id: rng.random_int(1, num_warehouses as i64) as i32,
        d_id: rng.random_int(1, 10) as i32,
        threshold: rng.random_int(10, 20) as i32,
    }
}

// Thread-local profiling accumulators for query breakdown
thread_local! {
    static PARSE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static EXECUTE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static QUERY_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Helper function to execute a SQL query
fn execute_query(db: &vibesql_storage::Database, sql: &str) -> Result<(), String> {
    let parse_start = Instant::now();

    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Ok(()), // Non-select statements are OK
        Err(e) => return Err(format!("Parse error: {}", e)),
    };

    let parse_time = parse_start.elapsed().as_micros() as u64;
    PARSE_TIME_US.with(|t| t.set(t.get() + parse_time));

    let execute_start = Instant::now();

    let executor = SelectExecutor::new(db);
    let result = match executor.execute(&stmt) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Execute error: {}", e)),
    };

    let execute_time = execute_start.elapsed().as_micros() as u64;
    EXECUTE_TIME_US.with(|t| t.set(t.get() + execute_time));
    QUERY_COUNT.with(|c| c.set(c.get() + 1));

    result
}

/// Helper function to execute a SQL query and return the first integer value
fn execute_query_for_int(db: &vibesql_storage::Database, sql: &str) -> Result<i64, String> {
    use vibesql_types::SqlValue;

    let parse_start = Instant::now();

    let stmt = match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Err("Expected SELECT statement".to_string()),
        Err(e) => return Err(format!("Parse error: {}", e)),
    };

    let parse_time = parse_start.elapsed().as_micros() as u64;
    PARSE_TIME_US.with(|t| t.set(t.get() + parse_time));

    let execute_start = Instant::now();

    let executor = SelectExecutor::new(db);
    let rows = match executor.execute(&stmt) {
        Ok(r) => r,
        Err(e) => return Err(format!("Execute error: {}", e)),
    };

    let execute_time = execute_start.elapsed().as_micros() as u64;
    EXECUTE_TIME_US.with(|t| t.set(t.get() + execute_time));
    QUERY_COUNT.with(|c| c.set(c.get() + 1));

    // Extract first value from first row
    if let Some(row) = rows.first() {
        if let Some(value) = row.values.first() {
            match value {
                SqlValue::Integer(i) => return Ok(*i),
                SqlValue::Bigint(i) => return Ok(*i),
                _ => return Err("Expected integer value".to_string()),
            }
        }
    }
    Err("No result returned".to_string())
}

/// Print profiling summary (call at end of benchmark)
pub fn print_profile_summary() {
    PARSE_TIME_US.with(|parse| {
        EXECUTE_TIME_US.with(|execute| {
            QUERY_COUNT.with(|count| {
                let p = parse.get();
                let e = execute.get();
                let c = count.get();
                if c > 0 {
                    eprintln!("\n--- Query Profiling ---");
                    eprintln!("Total queries: {}", c);
                    eprintln!("Parse time:   {} us total, {:.2} us avg", p, p as f64 / c as f64);
                    eprintln!("Execute time: {} us total, {:.2} us avg", e, e as f64 / c as f64);
                    eprintln!("Parse %:      {:.1}%", p as f64 / (p + e) as f64 * 100.0);
                }
            });
        });
    });
}

/// Reset profiling counters
pub fn reset_profile_counters() {
    PARSE_TIME_US.with(|t| t.set(0));
    EXECUTE_TIME_US.with(|t| t.set(0));
    QUERY_COUNT.with(|c| c.set(0));
}

/// TPC-C transaction executor for VibeSQL
pub struct VibesqlTransactionExecutor<'a> {
    pub db: &'a vibesql_storage::Database,
}

impl<'a> VibesqlTransactionExecutor<'a> {
    pub fn new(db: &'a vibesql_storage::Database) -> Self {
        Self { db }
    }

    /// Execute New-Order transaction (read-only simulation)
    ///
    /// Executes SQL queries to simulate the New-Order transaction:
    /// 1. SELECT warehouse tax rate
    /// 2. SELECT district tax rate and next order ID
    /// 3. SELECT customer discount, last name, and credit status
    /// 4. For each order line item:
    ///    - SELECT item price, name, and data
    ///    - SELECT stock quantity, YTD, and order count
    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let w_query = format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id);
        if let Err(e) = execute_query(self.db, &w_query) {
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
        if let Err(e) = execute_query(self.db, &d_query) {
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
        if let Err(e) = execute_query(self.db, &c_query) {
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
            if let Err(e) = execute_query(self.db, &i_query) {
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
            if let Err(e) = execute_query(self.db, &s_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock query failed: {}", e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Payment transaction (read-only simulation)
    ///
    /// Executes SQL queries to simulate the Payment transaction:
    /// 1. SELECT warehouse address and name
    /// 2. SELECT district address and name
    /// 3. SELECT customer info (by ID or last name)
    pub fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse info
        let w_query = format!(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}",
            input.w_id
        );
        if let Err(e) = execute_query(self.db, &w_query) {
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
        if let Err(e) = execute_query(self.db, &d_query) {
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
        if let Err(e) = execute_query(self.db, &c_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Order-Status transaction
    pub fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
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
        if let Err(e) = execute_query(self.db, &c_query) {
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
        if let Err(e) = execute_query(self.db, &o_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Order query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Delivery transaction (read-only simulation)
    ///
    /// Executes SQL queries to simulate the Delivery transaction:
    /// For each of 10 districts, SELECT the oldest new order (minimum no_o_id)
    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district - query for oldest new order
        for d_id in 1..=10 {
            let query = format!(
                "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                input.w_id, d_id
            );
            if let Err(e) = execute_query(self.db, &query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("New order query failed for district {}: {}", d_id, e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Stock-Level transaction
    ///
    /// Per TPC-C spec 2.8, the Stock-Level transaction checks the last 20 orders
    /// for items with stock below the threshold.
    ///
    /// Executes SQL queries:
    /// 1. SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?
    /// 2. SELECT COUNT(DISTINCT ol_i_id) FROM order_line WHERE ... AND ol_i_id IN (SELECT ...)
    pub fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();

        // Get district next order ID
        let d_query = format!(
            "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );
        let d_next_o_id = match execute_query_for_int(self.db, &d_query) {
            Ok(id) => id,
            Err(e) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("District query failed: {}", e)),
                };
            }
        };

        // Count low stock items for the last 20 orders (per TPC-C spec 2.8)
        // Use subquery approach matching SQLite/DuckDB/MySQL implementations
        let ol_o_id_min = d_next_o_id - 20;
        let stock_query = format!(
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
             WHERE ol_w_id = {} AND ol_d_id = {} \
             AND ol_o_id >= {} AND ol_o_id < {} \
             AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = {} AND s_quantity < {})",
            input.w_id, input.d_id, ol_o_id_min, d_next_o_id, input.w_id, input.threshold
        );
        if let Err(e) = execute_query(self.db, &stock_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Stock level query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

impl<'a> TPCCExecutor for VibesqlTransactionExecutor<'a> {
    fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        self.new_order(input)
    }

    fn payment(&self, input: &PaymentInput) -> TransactionResult {
        self.payment(input)
    }

    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        self.order_status(input)
    }

    fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        self.delivery(input)
    }

    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        self.stock_level(input)
    }
}

/// TPC-C transaction executor for SQLite
#[cfg(feature = "sqlite-comparison")]
pub struct SqliteTransactionExecutor<'a> {
    pub conn: &'a rusqlite::Connection,
}

#[cfg(feature = "sqlite-comparison")]
impl<'a> SqliteTransactionExecutor<'a> {
    pub fn new(conn: &'a rusqlite::Connection) -> Self {
        Self { conn }
    }

    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let _ = self
            .conn
            .execute(&format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id), []);

        // Get district info
        let _ = self.conn.execute(
            &format!(
                "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
        );

        // Get customer info
        let _ = self.conn.execute(
            &format!(
                "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.w_id, input.d_id, input.c_id
            ),
            [],
        );

        // Process each order line - query item and stock info
        for item in &input.items {
            // Get item info
            let _ = self.conn.execute(
                &format!("SELECT i_price, i_name, i_data FROM item WHERE i_id = {}", item.ol_i_id),
                [],
            );

            // Get stock info
            let _ = self.conn.execute(
                &format!(
                    "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = {} AND s_w_id = {}",
                    item.ol_i_id, item.ol_supply_w_id
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse info
        let _ = self.conn.execute(
            &format!(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}",
                input.w_id
            ),
            [],
        );

        // Get district info
        let _ = self.conn.execute(
            &format!(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
        );

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    input.c_w_id, input.c_d_id, c_id
                ),
                [],
            );
        } else {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                    input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();

        // Get customer (by ID or last name)
        let c_id = if let Some(c_id) = input.c_id {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    input.w_id, input.d_id, c_id
                ),
                [],
            );
            c_id
        } else {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                    input.w_id, input.d_id, input.c_last.as_ref().unwrap()
                ),
                [],
            );
            1 // Default c_id for order lookup
        };

        // Get last order for customer
        let _ = self.conn.execute(
            &format!(
                "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
                input.w_id, input.d_id, c_id
            ),
            [],
        );

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district - query for new orders
        for d_id in 1..=10 {
            let _ = self.conn.execute(
                &format!(
                    "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                    input.w_id, d_id
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();

        // Get district next order ID
        let d_next_o_id: i32 = self
            .conn
            .query_row(
                &format!(
                    "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                    input.w_id, input.d_id
                ),
                [],
                |row| row.get(0),
            )
            .unwrap_or(3001); // Default to 3001 if query fails

        // Count low stock items for the last 20 orders (per TPC-C spec 2.8)
        // Use subquery approach for better optimization
        let ol_o_id_min = d_next_o_id - 20;
        let _ = self.conn.execute(
            &format!(
                "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
                 WHERE ol_w_id = {} AND ol_d_id = {} \
                 AND ol_o_id >= {} AND ol_o_id < {} \
                 AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = {} AND s_quantity < {})",
                input.w_id, input.d_id, ol_o_id_min, d_next_o_id, input.w_id, input.threshold
            ),
            [],
        );

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

#[cfg(feature = "sqlite-comparison")]
impl<'a> TPCCExecutor for SqliteTransactionExecutor<'a> {
    fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        self.new_order(input)
    }

    fn payment(&self, input: &PaymentInput) -> TransactionResult {
        self.payment(input)
    }

    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        self.order_status(input)
    }

    fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        self.delivery(input)
    }

    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        self.stock_level(input)
    }
}

/// TPC-C transaction executor for DuckDB
#[cfg(feature = "duckdb-comparison")]
pub struct DuckdbTransactionExecutor<'a> {
    pub conn: &'a duckdb::Connection,
}

#[cfg(feature = "duckdb-comparison")]
impl<'a> DuckdbTransactionExecutor<'a> {
    pub fn new(conn: &'a duckdb::Connection) -> Self {
        Self { conn }
    }

    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let _ = self
            .conn
            .execute(&format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id), []);

        // Get district info
        let _ = self.conn.execute(
            &format!(
                "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
        );

        // Get customer info
        let _ = self.conn.execute(
            &format!(
                "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                input.w_id, input.d_id, input.c_id
            ),
            [],
        );

        // Process each order line - query item and stock info
        for item in &input.items {
            // Get item info
            let _ = self.conn.execute(
                &format!("SELECT i_price, i_name, i_data FROM item WHERE i_id = {}", item.ol_i_id),
                [],
            );

            // Get stock info
            let _ = self.conn.execute(
                &format!(
                    "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = {} AND s_w_id = {}",
                    item.ol_i_id, item.ol_supply_w_id
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse info
        let _ = self.conn.execute(
            &format!(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}",
                input.w_id
            ),
            [],
        );

        // Get district info
        let _ = self.conn.execute(
            &format!(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
        );

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    input.c_w_id, input.c_d_id, c_id
                ),
                [],
            );
        } else {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                    input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();

        // Get customer (by ID or last name)
        let c_id = if let Some(c_id) = input.c_id {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    input.w_id, input.d_id, c_id
                ),
                [],
            );
            c_id
        } else {
            let _ = self.conn.execute(
                &format!(
                    "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                    input.w_id, input.d_id, input.c_last.as_ref().unwrap()
                ),
                [],
            );
            1 // Default c_id for order lookup
        };

        // Get last order for customer
        let _ = self.conn.execute(
            &format!(
                "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1",
                input.w_id, input.d_id, c_id
            ),
            [],
        );

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district - query for new orders
        for d_id in 1..=10 {
            let _ = self.conn.execute(
                &format!(
                    "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                    input.w_id, d_id
                ),
                [],
            );
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();

        // Get district next order ID
        let d_next_o_id: i32 = self
            .conn
            .query_row(
                &format!(
                    "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                    input.w_id, input.d_id
                ),
                [],
                |row| row.get(0),
            )
            .unwrap_or(3001); // Default to 3001 if query fails

        // Count low stock items for the last 20 orders (per TPC-C spec 2.8)
        // Use subquery approach for better optimization
        let ol_o_id_min = d_next_o_id - 20;
        let _ = self.conn.execute(
            &format!(
                "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
                 WHERE ol_w_id = {} AND ol_d_id = {} \
                 AND ol_o_id >= {} AND ol_o_id < {} \
                 AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = {} AND s_quantity < {})",
                input.w_id, input.d_id, ol_o_id_min, d_next_o_id, input.w_id, input.threshold
            ),
            [],
        );

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

#[cfg(feature = "duckdb-comparison")]
impl<'a> TPCCExecutor for DuckdbTransactionExecutor<'a> {
    fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        self.new_order(input)
    }

    fn payment(&self, input: &PaymentInput) -> TransactionResult {
        self.payment(input)
    }

    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        self.order_status(input)
    }

    fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        self.delivery(input)
    }

    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        self.stock_level(input)
    }
}

/// TPC-C transaction executor for MySQL
///
/// Uses `RefCell` for interior mutability to allow the `TPCCExecutor` trait
/// (which requires `&self`) to call MySQL methods that need `&mut self`.
#[cfg(feature = "mysql-comparison")]
pub struct MysqlTransactionExecutor<'a> {
    pub conn: std::cell::RefCell<&'a mut mysql::PooledConn>,
}

#[cfg(feature = "mysql-comparison")]
impl<'a> MysqlTransactionExecutor<'a> {
    pub fn new(conn: &'a mut mysql::PooledConn) -> Self {
        Self { conn: std::cell::RefCell::new(conn) }
    }

    pub fn new_order_impl(&self, input: &NewOrderInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Get warehouse tax rate
        if let Err(e) =
            conn.exec_drop("SELECT w_tax FROM warehouse WHERE w_id = ?", (input.w_id,))
        {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        if let Err(e) = conn.exec_drop(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer info
        if let Err(e) = conn.exec_drop(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            (input.w_id, input.d_id, input.c_id),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Process each order line - query item and stock info
        for item in &input.items {
            // Get item info
            if let Err(e) = conn.exec_drop(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                (item.ol_i_id,),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Item query failed: {}", e)),
                };
            }

            // Get stock info
            if let Err(e) = conn.exec_drop(
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                (item.ol_i_id, item.ol_supply_w_id),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock query failed: {}", e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn payment_impl(&self, input: &PaymentInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Get warehouse info
        if let Err(e) = conn.exec_drop(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
            (input.w_id,),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        if let Err(e) = conn.exec_drop(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            if let Err(e) = conn.exec_drop(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                (input.c_w_id, input.c_d_id, c_id),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Customer query failed: {}", e)),
                };
            }
        } else if let Err(e) = conn.exec_drop(
            "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
            (input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn order_status_impl(&self, input: &OrderStatusInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Get customer (by ID or last name)
        let c_id = if let Some(c_id) = input.c_id {
            if let Err(e) = conn.exec_drop(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                (input.w_id, input.d_id, c_id),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Customer query failed: {}", e)),
                };
            }
            c_id
        } else {
            if let Err(e) = conn.exec_drop(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                (input.w_id, input.d_id, input.c_last.as_ref().unwrap()),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Customer query failed: {}", e)),
                };
            }
            1 // Default c_id for order lookup
        };

        // Get last order for customer
        if let Err(e) = conn.exec_drop(
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            (input.w_id, input.d_id, c_id),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Order query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn delivery_impl(&self, input: &DeliveryInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Process each district - query for new orders
        for d_id in 1..=10 {
            if let Err(e) = conn.exec_drop(
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
                (input.w_id, d_id),
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("New order query failed: {}", e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn stock_level_impl(&self, input: &StockLevelInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Get district next order ID
        let d_next_o_id: i32 = match conn.exec_first(
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ) {
            Ok(Some((id,))) => id,
            Ok(None) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("District not found".to_string()),
                };
            }
            Err(e) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("District query failed: {}", e)),
                };
            }
        };

        // Count low stock items for the last 20 orders (per TPC-C spec 2.8)
        // Use subquery approach for better optimization
        let ol_o_id_min = d_next_o_id - 20;
        if let Err(e) = conn.exec_drop(
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
             WHERE ol_w_id = ? AND ol_d_id = ? \
             AND ol_o_id >= ? AND ol_o_id < ? \
             AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)",
            (input.w_id, input.d_id, ol_o_id_min, d_next_o_id, input.w_id, input.threshold),
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Stock level query failed: {}", e)),
            };
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

#[cfg(feature = "mysql-comparison")]
impl<'a> TPCCExecutor for MysqlTransactionExecutor<'a> {
    fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        self.new_order_impl(input)
    }

    fn payment(&self, input: &PaymentInput) -> TransactionResult {
        self.payment_impl(input)
    }

    fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        self.order_status_impl(input)
    }

    fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        self.delivery_impl(input)
    }

    fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        self.stock_level_impl(input)
    }
}

/// TPC-C workload generator following standard transaction mix
pub struct TPCCWorkload {
    pub rng: TPCCRng,
    pub num_warehouses: i32,
}

impl TPCCWorkload {
    pub fn new(seed: u64, num_warehouses: i32) -> Self {
        Self { rng: TPCCRng::new(seed), num_warehouses }
    }

    /// Generate next transaction according to TPC-C mix
    /// Returns: transaction_type (0=NewOrder, 1=Payment, 2=OrderStatus, 3=Delivery, 4=StockLevel)
    pub fn next_transaction_type(&mut self) -> i32 {
        let roll = self.rng.random_int(1, 100);
        if roll <= 45 {
            0 // New-Order (45%)
        } else if roll <= 88 {
            1 // Payment (43%)
        } else if roll <= 92 {
            2 // Order-Status (4%)
        } else if roll <= 96 {
            3 // Delivery (4%)
        } else {
            4 // Stock-Level (4%)
        }
    }

    pub fn generate_new_order(&mut self) -> NewOrderInput {
        generate_new_order_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_payment(&mut self) -> PaymentInput {
        generate_payment_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_order_status(&mut self) -> OrderStatusInput {
        generate_order_status_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_delivery(&mut self) -> DeliveryInput {
        generate_delivery_input(&mut self.rng, self.num_warehouses)
    }

    pub fn generate_stock_level(&mut self) -> StockLevelInput {
        generate_stock_level_input(&mut self.rng, self.num_warehouses)
    }
}

/// Benchmark results summary
#[derive(Debug, Clone, Default)]
pub struct TPCCBenchmarkResults {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub total_duration_ms: u64,
    pub transactions_per_second: f64,
    pub new_order_count: u64,
    pub new_order_avg_us: f64,
    pub payment_count: u64,
    pub payment_avg_us: f64,
    pub order_status_count: u64,
    pub order_status_avg_us: f64,
    pub delivery_count: u64,
    pub delivery_avg_us: f64,
    pub stock_level_count: u64,
    pub stock_level_avg_us: f64,
}

impl TPCCBenchmarkResults {
    pub fn new() -> Self {
        Self::default()
    }
}
