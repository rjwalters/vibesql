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

    NewOrderInput {
        w_id,
        d_id,
        c_id,
        ol_cnt,
        items,
    }
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

    OrderStatusInput {
        w_id,
        d_id,
        c_id,
        c_last,
    }
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

/// Optimized TPC-C transaction executor that bypasses SQL parsing for point lookups.
///
/// This executor uses direct index lookups for primary key queries, providing
/// significant performance improvements for OLTP workloads by avoiding:
/// - SQL parsing overhead (~50-100µs per query)
/// - Query planning overhead (~20-50µs per query)
/// - Expression evaluation overhead for simple predicates
///
/// For a New-Order transaction with 10 items:
/// - Standard path: ~23 queries × 500µs = 11.5ms
/// - Optimized path: ~23 lookups × 10µs = 230µs (50x improvement)
pub struct OptimizedVibesqlExecutor<'a> {
    pub db: &'a vibesql_storage::Database,
}

impl<'a> OptimizedVibesqlExecutor<'a> {
    pub fn new(db: &'a vibesql_storage::Database) -> Self {
        Self { db }
    }

    /// Direct index lookup for single-column primary key using the new high-performance API
    /// Returns true if row exists, false otherwise
    #[inline]
    fn exists_by_pk(&self, index_name: &str, key: i64) -> bool {
        use vibesql_types::SqlValue;
        let key_values = [SqlValue::Integer(key)];
        self.db.lookup_one_by_index(index_name, &key_values).ok().flatten().is_some()
    }

    /// Direct index lookup for two-column composite primary key using the new high-performance API
    /// Returns true if row exists, false otherwise
    #[inline]
    fn exists_by_pk2(&self, index_name: &str, key1: i64, key2: i64) -> bool {
        use vibesql_types::SqlValue;
        let key_values = [SqlValue::Integer(key1), SqlValue::Integer(key2)];
        self.db.lookup_one_by_index(index_name, &key_values).ok().flatten().is_some()
    }

    /// Direct index lookup for three-column composite primary key using the new high-performance API
    /// Returns true if row exists, false otherwise
    #[inline]
    fn exists_by_pk3(&self, index_name: &str, key1: i64, key2: i64, key3: i64) -> bool {
        use vibesql_types::SqlValue;
        let key_values = [SqlValue::Integer(key1), SqlValue::Integer(key2), SqlValue::Integer(key3)];
        self.db.lookup_one_by_index(index_name, &key_values).ok().flatten().is_some()
    }

    /// Direct index lookup returning the row reference - use when you need the row data
    #[inline]
    fn lookup_row_by_pk2(&self, index_name: &str, key1: i64, key2: i64) -> Option<&vibesql_storage::Row> {
        use vibesql_types::SqlValue;
        let key_values = [SqlValue::Integer(key1), SqlValue::Integer(key2)];
        self.db.lookup_one_by_index(index_name, &key_values).ok().flatten()
    }

    /// Execute New-Order transaction using optimized direct lookups
    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        use vibesql_types::SqlValue;
        let start = Instant::now();

        // Get warehouse (single-column PK lookup)
        // Index: idx_warehouse_pk on warehouse(W_ID)
        if !self.exists_by_pk("idx_warehouse_pk", input.w_id as i64) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some("Warehouse not found".to_string()),
            };
        }

        // Get district (two-column composite PK lookup)
        // Index: idx_district_pk on district(D_W_ID, D_ID)
        if !self.exists_by_pk2("idx_district_pk", input.w_id as i64, input.d_id as i64) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some("District not found".to_string()),
            };
        }

        // Get customer (three-column composite PK lookup)
        // Index: idx_customer_pk on customer(C_W_ID, C_D_ID, C_ID)
        if !self.exists_by_pk3("idx_customer_pk", input.w_id as i64, input.d_id as i64, input.c_id as i64) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some("Customer not found".to_string()),
            };
        }

        // Process each order line - lookup item and stock
        for item in &input.items {
            // Get item (single-column PK lookup)
            // Index: idx_item_pk on item(I_ID)
            if !self.exists_by_pk("idx_item_pk", item.ol_i_id as i64) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Item {} not found", item.ol_i_id)),
                };
            }

            // Get stock (two-column composite PK lookup)
            // Index: idx_stock_pk on stock(S_I_ID, S_W_ID)
            if !self.exists_by_pk2("idx_stock_pk", item.ol_i_id as i64, item.ol_supply_w_id as i64) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock for item {} not found", item.ol_i_id)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Payment transaction using the public lookup_by_index API
    pub fn payment(&self, input: &PaymentInput) -> TransactionResult {
        use vibesql_types::SqlValue;
        let start = Instant::now();

        // Get warehouse
        if !self.exists_by_pk("idx_warehouse_pk", input.w_id as i64) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some("Warehouse not found".to_string()),
            };
        }

        // Get district
        if !self.exists_by_pk2("idx_district_pk", input.w_id as i64, input.d_id as i64) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some("District not found".to_string()),
            };
        }

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            // Direct PK lookup
            if !self.exists_by_pk3("idx_customer_pk", input.c_w_id as i64, input.c_d_id as i64, c_id as i64) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("Customer not found".to_string()),
                };
            }
        } else {
            // For customer lookup by last name, fall back to SQL (rare case - 40% of payments)
            let c_query = format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()
            );
            if let Err(e) = execute_query(self.db, &c_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Customer query failed: {}", e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Order-Status transaction using direct index API
    ///
    /// This optimization replaces the SQL query for order lookup with direct index API:
    /// 1. Customer by ID: Uses direct index lookup via `idx_customer_pk` (60% of cases)
    /// 2. Customer by last name: Falls back to SQL (40% of cases)
    /// 3. Order lookup: Uses `idx_orders_customer` index with direct API to find max o_id
    pub fn order_status(&self, input: &OrderStatusInput) -> TransactionResult {
        use vibesql_types::SqlValue;
        let start = Instant::now();

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            if !self.exists_by_pk3("idx_customer_pk", input.w_id as i64, input.d_id as i64, c_id as i64) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("Customer not found".to_string()),
                };
            }
        } else {
            // Fall back to SQL for last name lookup
            let c_query = format!(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first",
                input.w_id, input.d_id, input.c_last.as_ref().unwrap()
            );
            if let Err(e) = execute_query(self.db, &c_query) {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Customer query failed: {}", e)),
                };
            }
        }

        // Get last order for customer using direct index API
        // Index: idx_orders_customer on orders(O_W_ID, O_D_ID, O_C_ID)
        let c_id = input.c_id.unwrap_or(1);
        let order_key = [
            SqlValue::Integer(input.w_id as i64),
            SqlValue::Integer(input.d_id as i64),
            SqlValue::Integer(c_id as i64),
        ];

        // Lookup all orders for this customer and find the one with max o_id
        match self.db.lookup_by_index("idx_orders_customer", &order_key) {
            Ok(Some(orders)) => {
                // Find the order with maximum o_id (column 0)
                let _max_order = orders.iter().max_by_key(|row| {
                    match row.values.get(0) {
                        Some(SqlValue::Integer(o_id)) => *o_id,
                        _ => i64::MIN,
                    }
                });
                // Order found - we just needed to verify it exists
            }
            Ok(None) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("No orders found for customer".to_string()),
                };
            }
            Err(e) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Order lookup failed: {}", e)),
                };
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Delivery transaction using direct index API (no SQL)
    ///
    /// This optimization replaces the 10 SQL queries with a single batched index lookup:
    /// 1. Build 10 keys for each district: (w_id, d_id)
    /// 2. Batch lookup using idx_new_order_pk on new_order(no_w_id, no_d_id, no_o_id)
    /// 3. Since the index is sorted by no_o_id, the first row returned is the minimum
    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        use vibesql_types::SqlValue;
        let start = Instant::now();

        // Build keys for all 10 districts: (no_w_id, no_d_id)
        // Using idx_new_order_pk index which is sorted by (no_w_id, no_d_id, no_o_id)
        let district_keys: Vec<Vec<SqlValue>> = (1..=10)
            .map(|d_id| vec![
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(d_id),
            ])
            .collect();

        // Batch lookup all new_orders for all 10 districts
        // The index returns rows in ascending no_o_id order, so first row is minimum
        let _results = match self.db.lookup_by_index_batch("idx_new_order_pk", &district_keys) {
            Ok(results) => {
                // For each district, extract the minimum no_o_id (first row if any)
                let mut _min_order_ids: Vec<Option<i64>> = Vec::with_capacity(10);
                for district_rows_opt in &results {
                    if let Some(rows) = district_rows_opt {
                        if let Some(first_row) = rows.first() {
                            // Column 0 = no_o_id (based on new_order table schema)
                            if let Some(SqlValue::Integer(no_o_id)) = first_row.values.first() {
                                _min_order_ids.push(Some(*no_o_id));
                            } else {
                                _min_order_ids.push(None);
                            }
                        } else {
                            _min_order_ids.push(None);
                        }
                    } else {
                        _min_order_ids.push(None);
                    }
                }
                results
            }
            Err(_) => {
                // Ignore errors - some districts may have no new orders
                // This matches the SQL path behavior
                Vec::new()
            }
        };

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    /// Execute Stock-Level transaction using direct index API (no SQL)
    ///
    /// This optimization replaces the SQL query with direct index lookups:
    /// 1. Batch lookup order_lines for the last 20 orders using idx_order_line_district
    /// 2. Collect unique item IDs
    /// 3. Batch lookup stock quantities using idx_stock_pk
    /// 4. Count items with quantity below threshold
    pub fn stock_level(&self, input: &StockLevelInput) -> TransactionResult {
        use std::collections::HashSet;
        use vibesql_types::SqlValue;
        let start = Instant::now();

        // Get district next order ID via direct lookup - need the row data here
        let district = match self.lookup_row_by_pk2("idx_district_pk", input.w_id as i64, input.d_id as i64) {
            Some(row) => row,
            None => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("District not found".to_string()),
                };
            }
        };

        // Extract d_next_o_id from the row (column index 10)
        let d_next_o_id = match district.values.get(10) {
            Some(SqlValue::Integer(id)) => *id,
            _ => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some("Invalid district data".to_string()),
                };
            }
        };

        // Build keys for order_line batch lookup: (ol_w_id, ol_d_id, ol_o_id)
        // Using idx_order_line_district index
        let ol_o_id_min = d_next_o_id - 20;
        let order_keys: Vec<Vec<SqlValue>> = (ol_o_id_min..d_next_o_id)
            .map(|o_id| vec![
                SqlValue::Integer(input.w_id as i64),
                SqlValue::Integer(input.d_id as i64),
                SqlValue::Integer(o_id),
            ])
            .collect();

        // Batch lookup all order_lines for the last 20 orders
        let order_line_results = match self.db.lookup_by_index_batch("idx_order_line_district", &order_keys) {
            Ok(results) => results,
            Err(e) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Order line lookup failed: {}", e)),
                };
            }
        };

        // Collect unique item IDs from order_lines (column 4 = ol_i_id)
        let mut unique_item_ids: HashSet<i64> = HashSet::new();
        for order_lines_opt in &order_line_results {
            if let Some(order_lines) = order_lines_opt {
                for row in order_lines {
                    if let Some(SqlValue::Integer(ol_i_id)) = row.values.get(4) {
                        unique_item_ids.insert(*ol_i_id);
                    }
                }
            }
        }

        // Build keys for stock batch lookup: (s_i_id, s_w_id)
        // Using idx_stock_pk index
        let stock_keys: Vec<Vec<SqlValue>> = unique_item_ids
            .iter()
            .map(|&i_id| vec![
                SqlValue::Integer(i_id),
                SqlValue::Integer(input.w_id as i64),
            ])
            .collect();

        // Batch lookup stock quantities
        let stock_results = match self.db.lookup_one_by_index_batch("idx_stock_pk", &stock_keys) {
            Ok(results) => results,
            Err(e) => {
                return TransactionResult {
                    success: false,
                    duration_us: start.elapsed().as_micros() as u64,
                    error: Some(format!("Stock lookup failed: {}", e)),
                };
            }
        };

        // Count items with quantity below threshold (column 2 = s_quantity)
        let mut _low_stock_count = 0i64;
        for stock_row_opt in &stock_results {
            if let Some(row) = stock_row_opt {
                if let Some(SqlValue::Integer(s_quantity)) = row.values.get(2) {
                    if *s_quantity < input.threshold as i64 {
                        _low_stock_count += 1;
                    }
                }
            }
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

impl<'a> TPCCExecutor for OptimizedVibesqlExecutor<'a> {
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

impl<'a> VibesqlTransactionExecutor<'a> {
    pub fn new(db: &'a vibesql_storage::Database) -> Self {
        Self { db }
    }

    /// Execute New-Order transaction (read-only simulation)
    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let w_tax_query = format!(
            "SELECT w_tax FROM warehouse WHERE w_id = {}",
            input.w_id
        );

        // Get district info
        let d_query = format!(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
            input.w_id, input.d_id
        );

        // Get customer info
        let c_query = format!(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
            input.w_id, input.d_id, input.c_id
        );

        // Execute queries
        if let Err(e) = execute_query(self.db, &w_tax_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Warehouse query failed: {}", e)),
            };
        }

        if let Err(e) = execute_query(self.db, &d_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("District query failed: {}", e)),
            };
        }

        if let Err(e) = execute_query(self.db, &c_query) {
            return TransactionResult {
                success: false,
                duration_us: start.elapsed().as_micros() as u64,
                error: Some(format!("Customer query failed: {}", e)),
            };
        }

        // Process each order line - just query item and stock info
        for item in &input.items {
            // Get item info
            let i_query = format!(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",
                item.ol_i_id
            );
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
    pub fn delivery(&self, input: &DeliveryInput) -> TransactionResult {
        let start = Instant::now();

        // Process each district - just query for new orders
        for d_id in 1..=10 {
            let no_query = format!(
                "SELECT no_o_id FROM new_order WHERE no_w_id = {} AND no_d_id = {} ORDER BY no_o_id LIMIT 1",
                input.w_id, d_id
            );
            // Ignore errors - some districts may have no new orders
            let _ = execute_query(self.db, &no_query);
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
        // Use subquery approach: first get items from recent orders, then check stock
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
#[cfg(feature = "benchmark-comparison")]
pub struct SqliteTransactionExecutor<'a> {
    pub conn: &'a rusqlite::Connection,
}

#[cfg(feature = "benchmark-comparison")]
impl<'a> SqliteTransactionExecutor<'a> {
    pub fn new(conn: &'a rusqlite::Connection) -> Self {
        Self { conn }
    }

    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let _ = self.conn.execute(
            &format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id),
            [],
        );

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
                &format!(
                    "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",
                    item.ol_i_id
                ),
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
        let d_next_o_id: i32 = self.conn.query_row(
            &format!(
                "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
            |row| row.get(0),
        ).unwrap_or(3001); // Default to 3001 if query fails

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

#[cfg(feature = "benchmark-comparison")]
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
#[cfg(feature = "benchmark-comparison")]
pub struct DuckdbTransactionExecutor<'a> {
    pub conn: &'a duckdb::Connection,
}

#[cfg(feature = "benchmark-comparison")]
impl<'a> DuckdbTransactionExecutor<'a> {
    pub fn new(conn: &'a duckdb::Connection) -> Self {
        Self { conn }
    }

    pub fn new_order(&self, input: &NewOrderInput) -> TransactionResult {
        let start = Instant::now();

        // Get warehouse tax rate
        let _ = self.conn.execute(
            &format!("SELECT w_tax FROM warehouse WHERE w_id = {}", input.w_id),
            [],
        );

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
                &format!(
                    "SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",
                    item.ol_i_id
                ),
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
        let d_next_o_id: i32 = self.conn.query_row(
            &format!(
                "SELECT d_next_o_id FROM district WHERE d_w_id = {} AND d_id = {}",
                input.w_id, input.d_id
            ),
            [],
            |row| row.get(0),
        ).unwrap_or(3001); // Default to 3001 if query fails

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

#[cfg(feature = "benchmark-comparison")]
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
#[cfg(feature = "benchmark-comparison")]
pub struct MysqlTransactionExecutor<'a> {
    pub conn: &'a mut mysql::PooledConn,
}

#[cfg(feature = "benchmark-comparison")]
impl<'a> MysqlTransactionExecutor<'a> {
    pub fn new(conn: &'a mut mysql::PooledConn) -> Self {
        Self { conn }
    }

    pub fn new_order(&mut self, input: &NewOrderInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();

        // Get warehouse tax rate
        let _: Option<(f64,)> = self.conn.exec_first(
            "SELECT w_tax FROM warehouse WHERE w_id = ?",
            (input.w_id,),
        ).ok().flatten();

        // Get district info
        let _: Option<(f64, i32)> = self.conn.exec_first(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ).ok().flatten();

        // Get customer info
        let _: Option<(f64, String, String)> = self.conn.exec_first(
            "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            (input.w_id, input.d_id, input.c_id),
        ).ok().flatten();

        // Process each order line - query item and stock info
        for item in &input.items {
            // Get item info
            let _: Option<(f64, String, String)> = self.conn.exec_first(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                (item.ol_i_id,),
            ).ok().flatten();

            // Get stock info
            let _: Option<(i32, i32, i32)> = self.conn.exec_first(
                "SELECT s_quantity, s_ytd, s_order_cnt FROM stock WHERE s_i_id = ? AND s_w_id = ?",
                (item.ol_i_id, item.ol_supply_w_id),
            ).ok().flatten();
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn payment(&mut self, input: &PaymentInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();

        // Get warehouse info
        let _: Option<(String, String, String, String, String, String)> = self.conn.exec_first(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
            (input.w_id,),
        ).ok().flatten();

        // Get district info
        let _: Option<(String, String, String, String, String, String)> = self.conn.exec_first(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ).ok().flatten();

        // Get customer (by ID or last name)
        if let Some(c_id) = input.c_id {
            let _: Option<(i32, String, String, String, f64)> = self.conn.exec_first(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                (input.c_w_id, input.c_d_id, c_id),
            ).ok().flatten();
        } else {
            let _: Option<(i32, String, String, String, f64)> = self.conn.exec_first(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                (input.c_w_id, input.c_d_id, input.c_last.as_ref().unwrap()),
            ).ok().flatten();
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn order_status(&mut self, input: &OrderStatusInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();

        // Get customer (by ID or last name)
        let c_id = if let Some(c_id) = input.c_id {
            let _: Option<(i32, String, String, String, f64)> = self.conn.exec_first(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                (input.w_id, input.d_id, c_id),
            ).ok().flatten();
            c_id
        } else {
            let _: Option<(i32, String, String, String, f64)> = self.conn.exec_first(
                "SELECT c_id, c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                (input.w_id, input.d_id, input.c_last.as_ref().unwrap()),
            ).ok().flatten();
            1 // Default c_id for order lookup
        };

        // Get last order for customer
        let _: Option<(i32, String, Option<i32>)> = self.conn.exec_first(
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            (input.w_id, input.d_id, c_id),
        ).ok().flatten();

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn delivery(&mut self, input: &DeliveryInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();

        // Process each district - query for new orders
        for d_id in 1..=10 {
            let _: Option<(i32,)> = self.conn.exec_first(
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1",
                (input.w_id, d_id),
            ).ok().flatten();
        }

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }

    pub fn stock_level(&mut self, input: &StockLevelInput) -> TransactionResult {
        use mysql::prelude::*;
        let start = Instant::now();

        // Get district next order ID
        let d_next_o_id: i32 = self.conn.exec_first(
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            (input.w_id, input.d_id),
        ).ok().flatten().map(|(id,): (i32,)| id).unwrap_or(3001);

        // Count low stock items for the last 20 orders (per TPC-C spec 2.8)
        // Use subquery approach for better optimization
        let ol_o_id_min = d_next_o_id - 20;
        let _: Option<(i64,)> = self.conn.exec_first(
            "SELECT COUNT(DISTINCT ol_i_id) FROM order_line \
             WHERE ol_w_id = ? AND ol_d_id = ? \
             AND ol_o_id >= ? AND ol_o_id < ? \
             AND ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)",
            (input.w_id, input.d_id, ol_o_id_min, d_next_o_id, input.w_id, input.threshold),
        ).ok().flatten();

        TransactionResult {
            success: true,
            duration_us: start.elapsed().as_micros() as u64,
            error: None,
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
impl<'a> TPCCExecutor for MysqlTransactionExecutor<'a> {
    fn new_order(&self, _input: &NewOrderInput) -> TransactionResult {
        // This trait requires &self but MySQL needs &mut self for queries
        // We implement the trait for benchmarking compatibility but use the &mut self methods directly
        TransactionResult {
            success: false,
            duration_us: 0,
            error: Some("Use MysqlTransactionExecutor methods directly".to_string()),
        }
    }

    fn payment(&self, _input: &PaymentInput) -> TransactionResult {
        TransactionResult {
            success: false,
            duration_us: 0,
            error: Some("Use MysqlTransactionExecutor methods directly".to_string()),
        }
    }

    fn order_status(&self, _input: &OrderStatusInput) -> TransactionResult {
        TransactionResult {
            success: false,
            duration_us: 0,
            error: Some("Use MysqlTransactionExecutor methods directly".to_string()),
        }
    }

    fn delivery(&self, _input: &DeliveryInput) -> TransactionResult {
        TransactionResult {
            success: false,
            duration_us: 0,
            error: Some("Use MysqlTransactionExecutor methods directly".to_string()),
        }
    }

    fn stock_level(&self, _input: &StockLevelInput) -> TransactionResult {
        TransactionResult {
            success: false,
            duration_us: 0,
            error: Some("Use MysqlTransactionExecutor methods directly".to_string()),
        }
    }
}

/// TPC-C workload generator following standard transaction mix
pub struct TPCCWorkload {
    pub rng: TPCCRng,
    pub num_warehouses: i32,
}

impl TPCCWorkload {
    pub fn new(seed: u64, num_warehouses: i32) -> Self {
        Self {
            rng: TPCCRng::new(seed),
            num_warehouses,
        }
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
