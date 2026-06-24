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

use std::{sync::Arc, time::Instant};

use vibesql_executor::{
    DeleteExecutor, InsertExecutor, PreparedStatementCache, SelectExecutor, UpdateExecutor,
};
use vibesql_types::SqlValue;

/// Fixed timestamp used for write-faithful row payloads (reproducible, matches loader).
const WRITE_FAITHFUL_TS: &str = "2024-01-01 00:00:00";
/// Fixed 24-char district-info string used for synthesized order_line rows.
const WRITE_FAITHFUL_DIST_INFO: &str = "ABCDEFGHIJKLMNOPQRSTUVWX";

// Import transaction types from shared crate
pub use vibesql_bench_common::tpcc::{
    generate_delivery_input, generate_new_order_input, generate_order_status_input,
    generate_payment_input, generate_stock_level_input, DeliveryInput, NewOrderInput,
    NewOrderItemInput, OrderStatusInput, PaymentInput, StockLevelInput, TPCCBenchmarkResults,
    TPCCRng, TPCCWorkload, TransactionResult,
};

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

// Thread-local profiling accumulators for query breakdown
thread_local! {
    static PARSE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static EXECUTE_TIME_US: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static QUERY_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Helper function to execute a templated SQL query through the prepared-statement cache.
///
/// `template` is a stable `?`-placeholder SQL string (e.g. `"SELECT w_tax FROM warehouse
/// WHERE w_id = ?"`). The cache is keyed on that template, so repeated executions with
/// different parameter values hit the cache and skip re-parsing entirely. The `parse_start`
/// timer now measures cache lookup + AST-level parameter binding, which collapses toward zero
/// once the template is cached.
///
/// Execution still goes through the real engine path (`SelectExecutor::execute`), so this only
/// changes the parse/bind layer — not SELECT semantics.
fn execute_query(
    cache: &PreparedStatementCache,
    db: &vibesql_storage::Database,
    template: &str,
    params: &[SqlValue],
) -> Result<(), String> {
    let parse_start = Instant::now();

    let prepared = cache.get_or_prepare(template).map_err(|e| format!("Prepare error: {}", e))?;
    let stmt = match prepared.bind(params) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Ok(()), // Non-select statements are OK
        Err(e) => return Err(format!("Bind error: {}", e)),
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

/// Helper function to execute a templated SQL query and return the first integer value.
///
/// See [`execute_query`] for how templating routes through the prepared-statement cache.
fn execute_query_for_int(
    cache: &PreparedStatementCache,
    db: &vibesql_storage::Database,
    template: &str,
    params: &[SqlValue],
) -> Result<i64, String> {
    let parse_start = Instant::now();

    let prepared = cache.get_or_prepare(template).map_err(|e| format!("Prepare error: {}", e))?;
    let stmt = match prepared.bind(params) {
        Ok(vibesql_ast::Statement::Select(s)) => s,
        Ok(_) => return Err("Expected SELECT statement".to_string()),
        Err(e) => return Err(format!("Bind error: {}", e)),
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

/// Helper function to execute a templated DML statement (INSERT/UPDATE/DELETE).
///
/// This is the write-path sibling of [`execute_query`]. It binds the same stable
/// `?`-placeholder template through the prepared-statement cache, but instead of routing the
/// bound AST to `SelectExecutor` it dispatches on the DML variant to the real
/// `InsertExecutor` / `UpdateExecutor` / `DeleteExecutor`. Because it goes through those
/// executors against a `&mut Database`, it exercises the genuine write path: index
/// maintenance, delete compaction (`should_compact()`), and WAL flush.
///
/// `execute_query`'s `match` only accepts `Statement::Select` (silently dropping DML), so the
/// write-faithful variant must use this helper rather than reusing `execute_query`.
///
/// Returns the number of rows affected.
fn execute_dml(
    cache: &PreparedStatementCache,
    db: &mut vibesql_storage::Database,
    template: &str,
    params: &[SqlValue],
) -> Result<usize, String> {
    let parse_start = Instant::now();

    let prepared = cache.get_or_prepare(template).map_err(|e| format!("Prepare error: {}", e))?;
    let stmt = prepared.bind(params).map_err(|e| format!("Bind error: {}", e))?;

    let parse_time = parse_start.elapsed().as_micros() as u64;
    PARSE_TIME_US.with(|t| t.set(t.get() + parse_time));

    let execute_start = Instant::now();

    let result = match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert).map_err(|e| format!("Insert error: {}", e))
        }
        vibesql_ast::Statement::Update(update) => {
            UpdateExecutor::execute(&update, db).map_err(|e| format!("Update error: {}", e))
        }
        vibesql_ast::Statement::Delete(delete) => {
            DeleteExecutor::execute(&delete, db).map_err(|e| format!("Delete error: {}", e))
        }
        _ => Err("execute_dml expected INSERT/UPDATE/DELETE statement".to_string()),
    };

    let execute_time = execute_start.elapsed().as_micros() as u64;
    EXECUTE_TIME_US.with(|t| t.set(t.get() + execute_time));
    QUERY_COUNT.with(|c| c.set(c.get() + 1));

    result
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
///
/// Holds a single prepared-statement cache that is reused across every transaction this
/// executor runs. TPC-C queries are issued as stable `?`-placeholder templates, so the cache
/// key is the template (a handful of distinct entries) rather than a freshly interpolated
/// literal — which is what makes the cache hit instead of missing on every call.
///
/// The cache (`Arc<PreparedStatementCache>`) is thread-safe, so a single executor can be shared
/// across parallel benchmark clients (the `TPCCExecutor + Sync` path).
pub struct VibesqlTransactionExecutor<'a> {
    pub db: &'a vibesql_storage::Database,
    cache: Arc<PreparedStatementCache>,
}

impl<'a> VibesqlTransactionExecutor<'a> {
    pub fn new(db: &'a vibesql_storage::Database) -> Self {
        // A small cache is sufficient: TPC-C uses only a handful of distinct query templates.
        Self { db, cache: Arc::new(PreparedStatementCache::new(64)) }
    }

    /// Expose cache statistics (hits/misses/hit_rate) for benchmark reporting.
    pub fn cache_stats(&self) -> vibesql_executor::PreparedStatementCacheStats {
        self.cache.stats()
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
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT w_tax FROM warehouse WHERE w_id = ?",
            &[SqlValue::Integer(input.w_id as i64)],
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer info
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(input.c_id as i64),
            ],
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
            if let Err(e) = execute_query(
                &self.cache,
                self.db,
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                &[SqlValue::Integer(item.ol_i_id as i64)],
            ) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Item query failed: {}", e)),
                };
            }

            // Get stock info
            if let Err(e) = execute_query(
                &self.cache,
                self.db,
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                &[
                    SqlValue::Integer(item.ol_i_id as i64),
                    SqlValue::Integer(item.ol_supply_w_id as i64),
                ],
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

    /// Execute Payment transaction (read-only simulation)
    ///
    /// Executes SQL queries to simulate the Payment transaction:
    /// 1. SELECT warehouse address and name
    /// 2. SELECT district address and name
    /// 3. SELECT customer info (by ID or last name)
    pub fn payment(&self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse info
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
            &[SqlValue::Integer(input.w_id as i64)],
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        // Get district info
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        ) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        // Get customer (by ID or last name)
        let customer_result = if let Some(c_id) = input.c_id {
            execute_query(
                &self.cache,
                self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlValue::Integer(input.c_w_id as i64),
                    SqlValue::Integer(input.c_d_id as i64),
                    SqlValue::Integer(c_id as i64),
                ],
            )
        } else {
            execute_query(
                &self.cache,
                self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                &[
                    SqlValue::Integer(input.c_w_id as i64),
                    SqlValue::Integer(input.c_d_id as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(input.c_last.as_ref().unwrap().as_str())),
                ],
            )
        };
        if let Err(e) = customer_result {
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
        let customer_result = if let Some(c_id) = input.c_id {
            execute_query(
                &self.cache,
                self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(input.d_id as i64),
                    SqlValue::Integer(c_id as i64),
                ],
            )
        } else {
            execute_query(
                &self.cache,
                self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                &[
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(input.d_id as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(input.c_last.as_ref().unwrap().as_str())),
                ],
            )
        };
        if let Err(e) = customer_result {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Get last order for customer
        let c_id = input.c_id.unwrap_or(1);
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(c_id as i64),
            ],
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

    /// Execute Delivery transaction (read-only simulation)
    ///
    /// Executes SQL queries to simulate the Delivery transaction:
    /// For each of 10 districts, SELECT the oldest new order (minimum no_o_id)
    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district - query for oldest new order
        for d_id in 1..=10 {
            if let Err(e) = execute_query(
                &self.cache,
                self.db,
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
                &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(d_id as i64)],
            ) {
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
        let d_next_o_id = match execute_query_for_int(
            &self.cache,
            self.db,
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        ) {
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
        if let Err(e) = execute_query(
            &self.cache,
            self.db,
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
             WHERE ol_w_id = ? AND ol_d_id = ? \
             AND ol_o_id >= ? AND ol_o_id < ? \
             AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(ol_o_id_min),
                SqlValue::Integer(d_next_o_id),
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.threshold as i64),
            ],
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

// ============================================================================
// Write-faithful TPC-C transactions (TPCC_WRITE_FAITHFUL=1)
// ============================================================================
//
// The read-only executors above model the five transactions as SELECTs only, so write-path
// costs (delete compaction, index maintenance, WAL flush) never surface. The write-faithful
// path issues the real INSERT/UPDATE/DELETE statements for New-Order, Payment, and Delivery
// (Order-Status and Stock-Level are read-only in real TPC-C and stay read-only here).
//
// CONCURRENCY RESOLUTION (serial-only, v1): the VibeSQL DML executors require
// `db: &mut Database`, but the read-only `VibesqlTransactionExecutor` holds a shared
// `&Database` and is `Sync` across parallel clients — a shared `&Database` cannot drive
// `&mut`-requiring DML. Rather than introduce per-client owned databases (more code) or
// unsafe aliasing, the write-faithful path runs SERIAL-ONLY: the bench forces
// `num_clients = 1` and the executor owns a `&mut Database`. Comparative per-transaction
// latency does not require parallelism. See `TPCCWriteExecutor` + `run_write_benchmark` in
// `tpcc_benchmark.rs`.

/// Trait for write-faithful TPC-C transaction executors.
///
/// Unlike [`TPCCExecutor`] (which takes `&self` and is shared across parallel clients), this
/// trait takes `&mut self` so the VibeSQL implementation can drive `&mut Database` DML. It is
/// driven by the serial `run_write_benchmark` runner.
pub trait TPCCWriteExecutor {
    fn new_order(&mut self, input: &NewOrderInput) -> TransactionResult;
    fn payment(&mut self, input: &PaymentInput) -> TransactionResult;
    fn order_status(&mut self, input: &OrderStatusInput) -> TransactionResult;
    fn delivery(&mut self, input: &DeliveryInput) -> TransactionResult;
    fn stock_level(&mut self, input: &StockLevelInput) -> TransactionResult;
}

/// Write-faithful TPC-C transaction executor for VibeSQL.
///
/// Owns a mutable borrow of the database so it can run real DML through
/// `InsertExecutor`/`UpdateExecutor`/`DeleteExecutor` via [`execute_dml`]. Read-side lookups
/// reuse [`execute_query`]/[`execute_query_for_int`] through a shared reborrow of the same
/// database. Cache is a plain (non-`Arc`) instance since this path is single-threaded.
pub struct VibesqlWriteExecutor<'a> {
    pub db: &'a mut vibesql_storage::Database,
    cache: PreparedStatementCache,
}

impl<'a> VibesqlWriteExecutor<'a> {
    pub fn new(db: &'a mut vibesql_storage::Database) -> Self {
        Self { db, cache: PreparedStatementCache::new(64) }
    }

    /// Write-faithful New-Order: reads warehouse/district/customer/item/stock, then writes
    /// the order header, the new_order marker row, and one order_line per item, updating
    /// stock and advancing the district's next-order-id (which also keeps the orders /
    /// order_line / new_order primary keys unique across repeated transactions).
    fn new_order_impl(&mut self, input: &NewOrderInput) -> Result<(), String> {
        // Read warehouse tax (read-only lookup).
        execute_query(
            &self.cache,
            &*self.db,
            "SELECT w_tax FROM warehouse WHERE w_id = ?",
            &[SqlValue::Integer(input.w_id as i64)],
        )?;

        // Read the district's next order id; this becomes the new order's o_id.
        let o_id = execute_query_for_int(
            &self.cache,
            &*self.db,
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        )?;

        // Read customer info (read-only lookup).
        execute_query(
            &self.cache,
            &*self.db,
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(input.c_id as i64),
            ],
        )?;

        // Advance the district's next order id (write).
        execute_dml(
            &self.cache,
            self.db,
            "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        )?;

        // Insert the order header (o_carrier_id is NULL until delivered).
        execute_dml(
            &self.cache,
            self.db,
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                SqlValue::Integer(o_id),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.c_id as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(WRITE_FAITHFUL_TS)),
                SqlValue::Null,
                SqlValue::Integer(input.ol_cnt as i64),
                SqlValue::Integer(1),
            ],
        )?;

        // Insert the new_order marker row.
        execute_dml(
            &self.cache,
            self.db,
            "INSERT INTO new_order VALUES (?, ?, ?)",
            &[
                SqlValue::Integer(o_id),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(input.w_id as i64),
            ],
        )?;

        // Per item: read item + stock, update stock, insert the order_line row.
        for (idx, item) in input.items.iter().enumerate() {
            execute_query(
                &self.cache,
                &*self.db,
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                &[SqlValue::Integer(item.ol_i_id as i64)],
            )?;

            execute_query(
                &self.cache,
                &*self.db,
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                &[
                    SqlValue::Integer(item.ol_i_id as i64),
                    SqlValue::Integer(item.ol_supply_w_id as i64),
                ],
            )?;

            execute_dml(
                &self.cache,
                self.db,
                "UPDATE stock SET s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, \
                 s_quantity = s_quantity - ? WHERE s_i_id = ? AND s_w_id = ?",
                &[
                    SqlValue::Integer(item.ol_quantity as i64),
                    SqlValue::Integer(item.ol_quantity as i64),
                    SqlValue::Integer(item.ol_i_id as i64),
                    SqlValue::Integer(item.ol_supply_w_id as i64),
                ],
            )?;

            execute_dml(
                &self.cache,
                self.db,
                "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                &[
                    SqlValue::Integer(o_id),
                    SqlValue::Integer(input.d_id as i64),
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer((idx + 1) as i64),
                    SqlValue::Integer(item.ol_i_id as i64),
                    SqlValue::Integer(item.ol_supply_w_id as i64),
                    SqlValue::Null,
                    SqlValue::Integer(item.ol_quantity as i64),
                    SqlValue::Numeric(item.ol_quantity as f64 * 10.0),
                    SqlValue::Varchar(arcstr::ArcStr::from(WRITE_FAITHFUL_DIST_INFO)),
                ],
            )?;
        }

        Ok(())
    }

    /// Write-faithful Payment: updates warehouse / district / customer balances and inserts a
    /// history row.
    fn payment_impl(&mut self, input: &PaymentInput) -> Result<(), String> {
        // Update warehouse YTD.
        execute_dml(
            &self.cache,
            self.db,
            "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
            &[SqlValue::Numeric(input.h_amount), SqlValue::Integer(input.w_id as i64)],
        )?;

        // Update district YTD.
        execute_dml(
            &self.cache,
            self.db,
            "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
            &[
                SqlValue::Numeric(input.h_amount),
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
            ],
        )?;

        // Resolve the customer id (by id, or by last name lookup).
        let c_id = if let Some(c_id) = input.c_id {
            c_id as i64
        } else {
            execute_query_for_int(
                &self.cache,
                &*self.db,
                "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first LIMIT 1",
                &[
                    SqlValue::Integer(input.c_w_id as i64),
                    SqlValue::Integer(input.c_d_id as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(input.c_last.as_ref().unwrap().as_str())),
                ],
            )?
        };

        // Update customer balance / ytd payment / payment count.
        execute_dml(
            &self.cache,
            self.db,
            "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, \
             c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &[
                SqlValue::Numeric(input.h_amount),
                SqlValue::Numeric(input.h_amount),
                SqlValue::Integer(input.c_w_id as i64),
                SqlValue::Integer(input.c_d_id as i64),
                SqlValue::Integer(c_id),
            ],
        )?;

        // Insert the history row.
        execute_dml(
            &self.cache,
            self.db,
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                SqlValue::Integer(c_id),
                SqlValue::Integer(input.c_d_id as i64),
                SqlValue::Integer(input.c_w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(WRITE_FAITHFUL_TS)),
                SqlValue::Numeric(input.h_amount),
                SqlValue::Varchar(arcstr::ArcStr::from(WRITE_FAITHFUL_DIST_INFO)),
            ],
        )?;

        Ok(())
    }

    /// Write-faithful Delivery: for each district, find the oldest undelivered order, delete
    /// its new_order row, and stamp the carrier id / delivery date on orders and order_line.
    /// The `DELETE FROM new_order` is what exercises delete compaction and delete-time index
    /// maintenance. Districts with no pending new_order are skipped (no panic).
    fn delivery_impl(&mut self, input: &DeliveryInput) -> Result<(), String> {
        for d_id in 1..=10 {
            // Find the oldest new_order for this district (read).
            let o_id = match execute_query_for_int(
                &self.cache,
                &*self.db,
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
                &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(d_id as i64)],
            ) {
                Ok(id) => id,
                // No pending new order for this district — skip it (spec-faithful, no panic).
                Err(_) => continue,
            };

            // Delete the new_order marker row (exercises compaction + index maintenance).
            execute_dml(
                &self.cache,
                self.db,
                "DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?",
                &[
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(d_id as i64),
                    SqlValue::Integer(o_id),
                ],
            )?;

            // Stamp the carrier id on the order header.
            execute_dml(
                &self.cache,
                self.db,
                "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
                &[
                    SqlValue::Integer(input.o_carrier_id as i64),
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(d_id as i64),
                    SqlValue::Integer(o_id),
                ],
            )?;

            // Stamp the delivery date on the order's lines.
            execute_dml(
                &self.cache,
                self.db,
                "UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
                &[
                    SqlValue::Varchar(arcstr::ArcStr::from(WRITE_FAITHFUL_TS)),
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(d_id as i64),
                    SqlValue::Integer(o_id),
                ],
            )?;
        }

        Ok(())
    }

    /// Read-only Order-Status (unchanged from the read-only executor — Order-Status is a read
    /// in real TPC-C).
    fn order_status_impl(&self, input: &OrderStatusInput) -> Result<(), String> {
        let c_id = if let Some(c_id) = input.c_id {
            execute_query(
                &self.cache,
                &*self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(input.d_id as i64),
                    SqlValue::Integer(c_id as i64),
                ],
            )?;
            c_id as i64
        } else {
            execute_query(
                &self.cache,
                &*self.db,
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                &[
                    SqlValue::Integer(input.w_id as i64),
                    SqlValue::Integer(input.d_id as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(input.c_last.as_ref().unwrap().as_str())),
                ],
            )?;
            1
        };

        execute_query(
            &self.cache,
            &*self.db,
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(c_id),
            ],
        )?;

        Ok(())
    }

    /// Read-only Stock-Level (unchanged — Stock-Level is a read in real TPC-C).
    fn stock_level_impl(&self, input: &StockLevelInput) -> Result<(), String> {
        let d_next_o_id = execute_query_for_int(
            &self.cache,
            &*self.db,
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlValue::Integer(input.w_id as i64), SqlValue::Integer(input.d_id as i64)],
        )?;

        let ol_o_id_min = d_next_o_id - 20;
        execute_query(
            &self.cache,
            &*self.db,
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
             WHERE ol_w_id = ? AND ol_d_id = ? \
             AND ol_o_id >= ? AND ol_o_id < ? \
             AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)",
            &[
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(ol_o_id_min),
                SqlValue::Integer(d_next_o_id),
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.threshold as i64),
            ],
        )?;

        Ok(())
    }

    /// Expose cache statistics for benchmark reporting.
    pub fn cache_stats(&self) -> vibesql_executor::PreparedStatementCacheStats {
        self.cache.stats()
    }
}

impl<'a> TPCCWriteExecutor for VibesqlWriteExecutor<'a> {
    fn new_order(&mut self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.new_order_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn payment(&mut self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.payment_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn order_status(&mut self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.order_status_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn delivery(&mut self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.delivery_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn stock_level(&mut self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.stock_level_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }
}

/// TPC-C transaction executor for SQLite
#[cfg(feature = "sqlite")]
pub struct SqliteTransactionExecutor<'a> {
    pub conn: &'a rusqlite::Connection,
}

#[cfg(feature = "sqlite")]
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

#[cfg(feature = "sqlite")]
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

/// Write-faithful TPC-C transaction executor for SQLite.
///
/// Issues the SAME logical INSERT/UPDATE/DELETE write set as [`VibesqlWriteExecutor`] so the
/// per-transaction VibeSQL/SQLite ratio reflects engine cost, not differing work
/// (apples-to-apples). SQLite's `Connection` methods take `&self`, but this implements the
/// `&mut self` [`TPCCWriteExecutor`] trait so both engines share the serial
/// `run_write_benchmark` runner.
#[cfg(feature = "sqlite")]
pub struct SqliteWriteExecutor<'a> {
    pub conn: &'a rusqlite::Connection,
}

#[cfg(feature = "sqlite")]
impl<'a> SqliteWriteExecutor<'a> {
    pub fn new(conn: &'a rusqlite::Connection) -> Self {
        Self { conn }
    }

    fn new_order_impl(&self, input: &NewOrderInput) -> Result<(), String> {
        let map = |e: rusqlite::Error| e.to_string();

        // Read warehouse tax (read-only lookup).
        let _: f64 = self
            .conn
            .query_row(
                "SELECT w_tax FROM warehouse WHERE w_id = ?",
                rusqlite::params![input.w_id],
                |r| r.get(0),
            )
            .map_err(map)?;

        // Read the district's next order id; this becomes the new order's o_id.
        let o_id: i64 = self
            .conn
            .query_row(
                "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
                rusqlite::params![input.w_id, input.d_id],
                |r| r.get(0),
            )
            .map_err(map)?;

        // Read customer info (read-only lookup).
        let _: f64 = self
            .conn
            .query_row(
                "SELECT c_discount FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                rusqlite::params![input.w_id, input.d_id, input.c_id],
                |r| r.get(0),
            )
            .map_err(map)?;

        // Advance the district's next order id (write).
        self.conn
            .execute(
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?",
                rusqlite::params![input.w_id, input.d_id],
            )
            .map_err(map)?;

        // Insert order header (o_carrier_id NULL until delivered).
        self.conn
            .execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, NULL, ?, 1)",
                rusqlite::params![
                    o_id,
                    input.d_id,
                    input.w_id,
                    input.c_id,
                    WRITE_FAITHFUL_TS,
                    input.ol_cnt
                ],
            )
            .map_err(map)?;

        // Insert new_order marker row.
        self.conn
            .execute(
                "INSERT INTO new_order VALUES (?, ?, ?)",
                rusqlite::params![o_id, input.d_id, input.w_id],
            )
            .map_err(map)?;

        for (idx, item) in input.items.iter().enumerate() {
            // Read item + stock (read-only lookups).
            let _: f64 = self
                .conn
                .query_row(
                    "SELECT i_price FROM item WHERE i_id = ?",
                    rusqlite::params![item.ol_i_id],
                    |r| r.get(0),
                )
                .map_err(map)?;
            let _: i64 = self
                .conn
                .query_row(
                    "SELECT s_quantity FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                    rusqlite::params![item.ol_i_id, item.ol_supply_w_id],
                    |r| r.get(0),
                )
                .map_err(map)?;

            self.conn
                .execute(
                    "UPDATE stock SET s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, \
                     s_quantity = s_quantity - ? WHERE s_i_id = ? AND s_w_id = ?",
                    rusqlite::params![
                        item.ol_quantity,
                        item.ol_quantity,
                        item.ol_i_id,
                        item.ol_supply_w_id
                    ],
                )
                .map_err(map)?;

            self.conn
                .execute(
                    "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
                    rusqlite::params![
                        o_id,
                        input.d_id,
                        input.w_id,
                        (idx + 1) as i64,
                        item.ol_i_id,
                        item.ol_supply_w_id,
                        item.ol_quantity,
                        item.ol_quantity as f64 * 10.0,
                        WRITE_FAITHFUL_DIST_INFO
                    ],
                )
                .map_err(map)?;
        }

        Ok(())
    }

    fn payment_impl(&self, input: &PaymentInput) -> Result<(), String> {
        let map = |e: rusqlite::Error| e.to_string();

        self.conn
            .execute(
                "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
                rusqlite::params![input.h_amount, input.w_id],
            )
            .map_err(map)?;

        self.conn
            .execute(
                "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
                rusqlite::params![input.h_amount, input.w_id, input.d_id],
            )
            .map_err(map)?;

        let c_id: i64 = if let Some(c_id) = input.c_id {
            c_id as i64
        } else {
            self.conn
                .query_row(
                    "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first LIMIT 1",
                    rusqlite::params![input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()],
                    |r| r.get(0),
                )
                .map_err(map)?
        };

        self.conn
            .execute(
                "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, \
                 c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                rusqlite::params![input.h_amount, input.h_amount, input.c_w_id, input.c_d_id, c_id],
            )
            .map_err(map)?;

        self.conn
            .execute(
                "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    c_id,
                    input.c_d_id,
                    input.c_w_id,
                    input.d_id,
                    input.w_id,
                    WRITE_FAITHFUL_TS,
                    input.h_amount,
                    WRITE_FAITHFUL_DIST_INFO
                ],
            )
            .map_err(map)?;

        Ok(())
    }

    fn delivery_impl(&self, input: &DeliveryInput) -> Result<(), String> {
        let map = |e: rusqlite::Error| e.to_string();

        for d_id in 1..=10 {
            let o_id: i64 = match self.conn.query_row(
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
                rusqlite::params![input.w_id, d_id],
                |r| r.get(0),
            ) {
                Ok(id) => id,
                Err(rusqlite::Error::QueryReturnedNoRows) => continue,
                Err(e) => return Err(map(e)),
            };

            self.conn
                .execute(
                    "DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?",
                    rusqlite::params![input.w_id, d_id, o_id],
                )
                .map_err(map)?;

            self.conn
                .execute(
                    "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?",
                    rusqlite::params![input.o_carrier_id, input.w_id, d_id, o_id],
                )
                .map_err(map)?;

            self.conn
                .execute(
                    "UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
                    rusqlite::params![WRITE_FAITHFUL_TS, input.w_id, d_id, o_id],
                )
                .map_err(map)?;
        }

        Ok(())
    }

    fn order_status_impl(&self, input: &OrderStatusInput) -> Result<(), String> {
        // Read-only — reuse the read-only SQLite executor's behavior.
        let exec = SqliteTransactionExecutor::new(self.conn);
        let r = exec.order_status(input);
        if r.success {
            Ok(())
        } else {
            Err(r.error.unwrap_or_default())
        }
    }

    fn stock_level_impl(&self, input: &StockLevelInput) -> Result<(), String> {
        let exec = SqliteTransactionExecutor::new(self.conn);
        let r = exec.stock_level(input);
        if r.success {
            Ok(())
        } else {
            Err(r.error.unwrap_or_default())
        }
    }
}

#[cfg(feature = "sqlite")]
impl<'a> TPCCWriteExecutor for SqliteWriteExecutor<'a> {
    fn new_order(&mut self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.new_order_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn payment(&mut self, input: &PaymentInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.payment_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn order_status(&mut self, input: &OrderStatusInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.order_status_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn delivery(&mut self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.delivery_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }

    fn stock_level(&mut self, input: &StockLevelInput) -> TransactionResult {
        let start = Instant::now();
        let result = self.stock_level_impl(input);
        TransactionResult {
            success: result.is_ok(),
            duration_us: start.elapsed().as_micros() as u64,
            error: result.err(),
        }
    }
}

/// TPC-C transaction executor for DuckDB
#[cfg(feature = "duckdb")]
pub struct DuckdbTransactionExecutor<'a> {
    pub conn: &'a duckdb::Connection,
}

#[cfg(feature = "duckdb")]
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

#[cfg(feature = "duckdb")]
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
#[cfg(feature = "mysql")]
pub struct MysqlTransactionExecutor<'a> {
    pub conn: std::cell::RefCell<&'a mut mysql::PooledConn>,
}

#[cfg(feature = "mysql")]
impl<'a> MysqlTransactionExecutor<'a> {
    pub fn new(conn: &'a mut mysql::PooledConn) -> Self {
        Self { conn: std::cell::RefCell::new(conn) }
    }

    pub fn new_order_impl(&self, input: &NewOrderInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();
        let mut conn = self.conn.borrow_mut();

        // Get warehouse tax rate
        if let Err(e) = conn.exec_drop("SELECT w_tax FROM warehouse WHERE w_id = ?", (input.w_id,))
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

#[cfg(feature = "mysql")]
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
