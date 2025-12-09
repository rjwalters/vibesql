//! TPC-C Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-C
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).

use super::data::TPCCData;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "mysql-comparison")]
use mysql::prelude::*;
#[cfg(feature = "mysql-comparison")]
use mysql::{Pool, PooledConn};
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

// =============================================================================
// Database Loaders
// =============================================================================

/// Load TPC-C database into VibeSQL with specified scale factor (number of warehouses)
pub fn load_vibesql(scale_factor: f64) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = TPCCData::new(scale_factor);

    // Enable case-insensitive identifier lookups for MySQL compatibility.
    // The TPC-C schema uses lowercase table/column names, but the SQL parser
    // normalizes unquoted identifiers to uppercase. This setting allows queries
    // to find tables regardless of case.
    db.catalog.set_case_sensitive_identifiers(false);

    // Create schema
    create_tpcc_schema_vibesql(&mut db);

    // Create indexes BEFORE loading data - this avoids slow bulk B+ tree operations.
    // Incremental index maintenance during inserts is faster than bulk building after
    // loading 100K+ rows.
    create_tpcc_indexes_vibesql(&mut db);

    // Load data
    load_item_vibesql(&mut db, &mut data);

    let districts_per_warehouse = data.districts_per_warehouse();
    for w_id in 1..=data.num_warehouses() {
        load_warehouse_vibesql(&mut db, &mut data, w_id);
        load_stock_vibesql(&mut db, &mut data, w_id);

        for d_id in 1..=districts_per_warehouse {
            load_district_vibesql(&mut db, &mut data, d_id, w_id);
            load_customer_vibesql(&mut db, &mut data, d_id, w_id);
            load_orders_vibesql(&mut db, &mut data, d_id, w_id);
        }
    }

    // Compute statistics for join order optimization
    for table_name in [
        "warehouse",
        "district",
        "customer",
        "history",
        "orders",
        "new_order",
        "order_line",
        "item",
        "stock",
    ] {
        if let Some(table) = db.get_table_mut(table_name) {
            table.analyze();
        }
    }

    db
}

#[cfg(feature = "sqlite-comparison")]
pub fn load_sqlite(scale_factor: f64) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = TPCCData::new(scale_factor);

    create_tpcc_schema_sqlite(&conn);

    load_item_sqlite(&conn, &mut data);

    let districts_per_warehouse = data.districts_per_warehouse();
    for w_id in 1..=data.num_warehouses() {
        load_warehouse_sqlite(&conn, &mut data, w_id);
        load_stock_sqlite(&conn, &mut data, w_id);

        for d_id in 1..=districts_per_warehouse {
            load_district_sqlite(&conn, &mut data, d_id, w_id);
            load_customer_sqlite(&conn, &mut data, d_id, w_id);
            load_orders_sqlite(&conn, &mut data, d_id, w_id);
        }
    }

    create_tpcc_indexes_sqlite(&conn);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.execute("ANALYZE", []).unwrap();

    conn
}

#[cfg(feature = "duckdb-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCCData::new(scale_factor);

    create_tpcc_schema_duckdb(&conn);

    load_item_duckdb(&conn, &mut data);

    let districts_per_warehouse = data.districts_per_warehouse();
    for w_id in 1..=data.num_warehouses() {
        load_warehouse_duckdb(&conn, &mut data, w_id);
        load_stock_duckdb(&conn, &mut data, w_id);

        for d_id in 1..=districts_per_warehouse {
            load_district_duckdb(&conn, &mut data, d_id, w_id);
            load_customer_duckdb(&conn, &mut data, d_id, w_id);
            load_orders_duckdb(&conn, &mut data, d_id, w_id);
        }
    }

    create_tpcc_indexes_duckdb(&conn);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.execute_batch("ANALYZE").unwrap();

    conn
}

/// Get a MySQL connection pool for parallel execution.
/// Returns None if MYSQL_URL is not set.
/// Note: This assumes the database has already been loaded by `load_mysql`.
#[cfg(feature = "mysql-comparison")]
pub fn get_mysql_pool() -> Option<Pool> {
    use mysql::{Opts, OptsBuilder};

    let url = std::env::var("MYSQL_URL").ok()?;
    let base_opts = Opts::from_url(&url).ok()?;

    // Build options with init statement to disable HeatWave secondary engine
    let opts = OptsBuilder::from_opts(base_opts)
        .init(vec!["SET SESSION use_secondary_engine=OFF"]);

    Pool::new(opts).ok()
}

/// Load MySQL TPC-C database
/// Requires MYSQL_URL environment variable (e.g., mysql://root:password@localhost:3306/tpcc)
/// Returns None if MYSQL_URL is not set or connection fails
#[cfg(feature = "mysql-comparison")]
pub fn load_mysql(scale_factor: f64) -> Option<PooledConn> {
    let url = std::env::var("MYSQL_URL").ok()?;
    let pool = Pool::new(url.as_str()).ok()?;
    let mut conn = pool.get_conn().ok()?;

    // Disable HeatWave secondary engine to avoid errors on complex queries
    let _ = conn.query_drop("SET SESSION use_secondary_engine=OFF");

    let mut data = TPCCData::new(scale_factor);

    // Create schema (drops and recreates tables)
    create_tpcc_schema_mysql(&mut conn);

    // Load data
    load_item_mysql(&mut conn, &mut data);

    let districts_per_warehouse = data.districts_per_warehouse();
    for w_id in 1..=data.num_warehouses() {
        load_warehouse_mysql(&mut conn, &mut data, w_id);
        load_stock_mysql(&mut conn, &mut data, w_id);

        for d_id in 1..=districts_per_warehouse {
            load_district_mysql(&mut conn, &mut data, d_id, w_id);
            load_customer_mysql(&mut conn, &mut data, d_id, w_id);
            load_orders_mysql(&mut conn, &mut data, d_id, w_id);
        }
    }

    create_tpcc_indexes_mysql(&mut conn);

    // Compute statistics for query optimization (ensures fair comparison with VibeSQL)
    conn.query_drop("ANALYZE TABLE warehouse, district, customer, history, new_order, orders, order_line, item, stock").unwrap();

    Some(conn)
}

// =============================================================================
// VibeSQL Schema and Loading
// =============================================================================

fn create_tpcc_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // Helper for varchar columns (using None = default max length)
    let varchar = || DataType::Varchar { max_length: None };

    // WAREHOUSE table
    db.create_table(TableSchema::with_primary_key(
        "warehouse".to_string(),
        vec![
            ColumnSchema::new("w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("w_name".to_string(), varchar(), false),
            ColumnSchema::new("w_street_1".to_string(), varchar(), false),
            ColumnSchema::new("w_street_2".to_string(), varchar(), false),
            ColumnSchema::new("w_city".to_string(), varchar(), false),
            ColumnSchema::new("w_state".to_string(), varchar(), false),
            ColumnSchema::new("w_zip".to_string(), varchar(), false),
            ColumnSchema::new(
                "w_tax".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new(
                "w_ytd".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
        ],
        vec!["w_id".to_string()],
    ))
    .unwrap();

    // DISTRICT table
    db.create_table(TableSchema::with_primary_key(
        "district".to_string(),
        vec![
            ColumnSchema::new("d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("d_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("d_name".to_string(), varchar(), false),
            ColumnSchema::new("d_street_1".to_string(), varchar(), false),
            ColumnSchema::new("d_street_2".to_string(), varchar(), false),
            ColumnSchema::new("d_city".to_string(), varchar(), false),
            ColumnSchema::new("d_state".to_string(), varchar(), false),
            ColumnSchema::new("d_zip".to_string(), varchar(), false),
            ColumnSchema::new(
                "d_tax".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new(
                "d_ytd".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new("d_next_o_id".to_string(), DataType::Integer, false),
        ],
        vec!["d_w_id".to_string(), "d_id".to_string()],
    ))
    .unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::with_primary_key(
        "customer".to_string(),
        vec![
            ColumnSchema::new("c_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_first".to_string(), varchar(), false),
            ColumnSchema::new("c_middle".to_string(), varchar(), false),
            ColumnSchema::new("c_last".to_string(), varchar(), false),
            ColumnSchema::new("c_street_1".to_string(), varchar(), false),
            ColumnSchema::new("c_street_2".to_string(), varchar(), false),
            ColumnSchema::new("c_city".to_string(), varchar(), false),
            ColumnSchema::new("c_state".to_string(), varchar(), false),
            ColumnSchema::new("c_zip".to_string(), varchar(), false),
            ColumnSchema::new("c_phone".to_string(), varchar(), false),
            ColumnSchema::new("c_since".to_string(), varchar(), false),
            ColumnSchema::new("c_credit".to_string(), varchar(), false),
            ColumnSchema::new(
                "c_credit_lim".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new(
                "c_discount".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new(
                "c_balance".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new(
                "c_ytd_payment".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new("c_payment_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_delivery_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_data".to_string(), varchar(), false),
        ],
        vec!["c_w_id".to_string(), "c_d_id".to_string(), "c_id".to_string()],
    ))
    .unwrap();

    // HISTORY table
    db.create_table(TableSchema::new(
        "history".to_string(),
        vec![
            ColumnSchema::new("h_c_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("h_c_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("h_c_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("h_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("h_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("h_date".to_string(), varchar(), false),
            ColumnSchema::new(
                "h_amount".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new("h_data".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // NEW_ORDER table
    db.create_table(TableSchema::with_primary_key(
        "new_order".to_string(),
        vec![
            ColumnSchema::new("no_o_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("no_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("no_w_id".to_string(), DataType::Integer, false),
        ],
        vec!["no_w_id".to_string(), "no_d_id".to_string(), "no_o_id".to_string()],
    ))
    .unwrap();

    // ORDERS table
    db.create_table(TableSchema::with_primary_key(
        "orders".to_string(),
        vec![
            ColumnSchema::new("o_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_c_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_entry_d".to_string(), varchar(), false),
            ColumnSchema::new("o_carrier_id".to_string(), DataType::Integer, true),
            ColumnSchema::new("o_ol_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("o_all_local".to_string(), DataType::Integer, false),
        ],
        vec!["o_w_id".to_string(), "o_d_id".to_string(), "o_id".to_string()],
    ))
    .unwrap();

    // ORDER_LINE table
    db.create_table(TableSchema::with_primary_key(
        "order_line".to_string(),
        vec![
            ColumnSchema::new("ol_o_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_number".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_i_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_supply_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ol_delivery_d".to_string(), varchar(), true),
            ColumnSchema::new("ol_quantity".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "ol_amount".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new("ol_dist_info".to_string(), varchar(), false),
        ],
        vec![
            "ol_w_id".to_string(),
            "ol_d_id".to_string(),
            "ol_o_id".to_string(),
            "ol_number".to_string(),
        ],
    ))
    .unwrap();

    // ITEM table
    db.create_table(TableSchema::with_primary_key(
        "item".to_string(),
        vec![
            ColumnSchema::new("i_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("i_im_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("i_name".to_string(), varchar(), false),
            ColumnSchema::new(
                "i_price".to_string(),
                DataType::Decimal { precision: 15, scale: 2 },
                false,
            ),
            ColumnSchema::new("i_data".to_string(), varchar(), false),
        ],
        vec!["i_id".to_string()],
    ))
    .unwrap();

    // STOCK table
    db.create_table(TableSchema::with_primary_key(
        "stock".to_string(),
        vec![
            ColumnSchema::new("s_i_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_quantity".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_dist_01".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_02".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_03".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_04".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_05".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_06".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_07".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_08".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_09".to_string(), varchar(), false),
            ColumnSchema::new("s_dist_10".to_string(), varchar(), false),
            ColumnSchema::new("s_ytd".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_order_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_remote_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("s_data".to_string(), varchar(), false),
        ],
        vec!["s_w_id".to_string(), "s_i_id".to_string()],
    ))
    .unwrap();
}

fn create_tpcc_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Helper to create index columns
    // Column names are uppercased to match the SQL parser's normalization.
    // The parser converts unquoted identifiers to uppercase, so index columns
    // must use uppercase names for case-sensitive matching in cost-based selection.
    fn col(name: &str) -> IndexColumn {
        IndexColumn {
            column_name: name.to_uppercase(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }
    }

    // Primary key indexes for core tables - TPC-C uses storage layer directly
    // so we need to manually create these (normally auto-created by executor in #3202)
    // The pk_new_order index enables prefix_scan_first optimization for Delivery (#3242)
    db.create_index(
        "pk_new_order".to_string(),
        "new_order".to_string(),
        true,
        vec![col("no_w_id"), col("no_d_id"), col("no_o_id")],
    )
    .ok();

    db.create_index(
        "pk_orders".to_string(),
        "orders".to_string(),
        true,
        vec![col("o_w_id"), col("o_d_id"), col("o_id")],
    )
    .ok();

    db.create_index(
        "pk_order_line".to_string(),
        "order_line".to_string(),
        true,
        vec![col("ol_w_id"), col("ol_d_id"), col("ol_o_id"), col("ol_number")],
    )
    .ok();

    // Secondary indexes for queries - these match the indexes created by
    // SQLite, DuckDB, and MySQL for fair benchmark comparison.
    db.create_index(
        "idx_customer_name".to_string(),
        "customer".to_string(),
        false,
        vec![col("c_w_id"), col("c_d_id"), col("c_last"), col("c_first")],
    )
    .ok();

    // Include o_id in the index to support ORDER BY o_id DESC LIMIT 1 efficiently.
    // This allows the Order-Status transaction to find a customer's most recent order
    // via a single index scan rather than fetching all orders and sorting.
    db.create_index(
        "idx_orders_customer".to_string(),
        "orders".to_string(),
        false,
        vec![col("o_w_id"), col("o_d_id"), col("o_c_id"), col("o_id")],
    )
    .ok();

    // Stock-Level transaction index: enables efficient range scans on order_line
    // for the last 20 orders per TPC-C spec 2.8. This matches the idx_order_line_district
    // index added to SQLite, DuckDB, and MySQL for benchmark consistency.
    db.create_index(
        "idx_order_line_district".to_string(),
        "order_line".to_string(),
        false,
        vec![col("ol_w_id"), col("ol_d_id"), col("ol_o_id")],
    )
    .ok();

    // Stock-Level optimization: index for filtering low-quantity stock items (#3221)
    // The Stock-Level transaction has a subquery:
    //   SELECT s_i_id FROM stock WHERE s_w_id = $1 AND s_quantity < $2
    // This index allows efficient range scan on (s_w_id, s_quantity) to find
    // low-quantity items, with s_i_id included for a covering index.
    db.create_index(
        "idx_stock_quantity".to_string(),
        "stock".to_string(),
        false,
        vec![col("s_w_id"), col("s_quantity"), col("s_i_id")],
    )
    .ok();
}

fn load_item_vibesql(db: &mut VibeDB, data: &mut TPCCData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let item = data.gen_item(i_id);
        let row = Row::new(vec![
            SqlValue::Integer(item.i_id as i64),
            SqlValue::Integer(item.i_im_id as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(item.i_name)),
            SqlValue::Numeric(item.i_price),
            SqlValue::Varchar(arcstr::ArcStr::from(item.i_data)),
        ]);
        db.insert_row("item", row).unwrap();
    }
}

fn load_warehouse_vibesql(db: &mut VibeDB, data: &mut TPCCData, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let warehouse = data.gen_warehouse(w_id);
    let row = Row::new(vec![
        SqlValue::Integer(warehouse.w_id as i64),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_name)),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_street_1)),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_street_2)),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_city)),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_state)),
        SqlValue::Varchar(arcstr::ArcStr::from(warehouse.w_zip)),
        SqlValue::Numeric(warehouse.w_tax),
        SqlValue::Numeric(warehouse.w_ytd),
    ]);
    db.insert_row("warehouse", row).unwrap();
}

fn load_stock_vibesql(db: &mut VibeDB, data: &mut TPCCData, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let stock = data.gen_stock(i_id, w_id);
        let row = Row::new(vec![
            SqlValue::Integer(stock.s_i_id as i64),
            SqlValue::Integer(stock.s_w_id as i64),
            SqlValue::Integer(stock.s_quantity as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_01)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_02)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_03)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_04)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_05)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_06)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_07)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_08)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_09)),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_dist_10)),
            SqlValue::Integer(stock.s_ytd as i64),
            SqlValue::Integer(stock.s_order_cnt as i64),
            SqlValue::Integer(stock.s_remote_cnt as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(stock.s_data)),
        ]);
        db.insert_row("stock", row).unwrap();
    }
}

fn load_district_vibesql(db: &mut VibeDB, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let district = data.gen_district(d_id, w_id);
    let row = Row::new(vec![
        SqlValue::Integer(district.d_id as i64),
        SqlValue::Integer(district.d_w_id as i64),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_name)),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_street_1)),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_street_2)),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_city)),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_state)),
        SqlValue::Varchar(arcstr::ArcStr::from(district.d_zip)),
        SqlValue::Numeric(district.d_tax),
        SqlValue::Numeric(district.d_ytd),
        SqlValue::Integer(district.d_next_o_id as i64),
    ]);
    db.insert_row("district", row).unwrap();
}

fn load_customer_vibesql(db: &mut VibeDB, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let customers_per_district = data.customers_per_district();
    for c_id in 1..=customers_per_district {
        let customer = data.gen_customer(c_id, d_id, w_id);
        let row = Row::new(vec![
            SqlValue::Integer(customer.c_id as i64),
            SqlValue::Integer(customer.c_d_id as i64),
            SqlValue::Integer(customer.c_w_id as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_first)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_middle)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_last)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_street_1)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_street_2)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_city)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_state)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_zip)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_phone)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_since)),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_credit)),
            SqlValue::Numeric(customer.c_credit_lim),
            SqlValue::Numeric(customer.c_discount),
            SqlValue::Numeric(customer.c_balance),
            SqlValue::Numeric(customer.c_ytd_payment),
            SqlValue::Integer(customer.c_payment_cnt as i64),
            SqlValue::Integer(customer.c_delivery_cnt as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(customer.c_data)),
        ]);
        db.insert_row("customer", row).unwrap();

        // Insert corresponding history record
        let history = data.gen_history(c_id, d_id, w_id);
        let history_row = Row::new(vec![
            SqlValue::Integer(history.h_c_id as i64),
            SqlValue::Integer(history.h_c_d_id as i64),
            SqlValue::Integer(history.h_c_w_id as i64),
            SqlValue::Integer(history.h_d_id as i64),
            SqlValue::Integer(history.h_w_id as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(history.h_date)),
            SqlValue::Numeric(history.h_amount),
            SqlValue::Varchar(arcstr::ArcStr::from(history.h_data)),
        ]);
        db.insert_row("history", history_row).unwrap();
    }
}

fn load_orders_vibesql(db: &mut VibeDB, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let customers_per_district = data.customers_per_district();
    let orders_per_district = data.orders_per_district();

    // Generate customer IDs in random order for orders
    let mut c_ids: Vec<i32> = (1..=customers_per_district).collect();
    // Simple shuffle using the RNG
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    // Threshold for delivered orders (70% of orders)
    let delivered_threshold = (orders_per_district as f64 * 0.7) as i32;

    for o_id in 1..=orders_per_district {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        let order_row = Row::new(vec![
            SqlValue::Integer(order.o_id as i64),
            SqlValue::Integer(order.o_d_id as i64),
            SqlValue::Integer(order.o_w_id as i64),
            SqlValue::Integer(order.o_c_id as i64),
            SqlValue::Varchar(arcstr::ArcStr::from(order.o_entry_d)),
            order.o_carrier_id.map(|v| SqlValue::Integer(v as i64)).unwrap_or(SqlValue::Null),
            SqlValue::Integer(order.o_ol_cnt as i64),
            SqlValue::Integer(order.o_all_local as i64),
        ]);
        db.insert_row("orders", order_row).unwrap();

        // Generate order lines
        let delivered = o_id <= delivered_threshold;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            let ol_row = Row::new(vec![
                SqlValue::Integer(ol.ol_o_id as i64),
                SqlValue::Integer(ol.ol_d_id as i64),
                SqlValue::Integer(ol.ol_w_id as i64),
                SqlValue::Integer(ol.ol_number as i64),
                SqlValue::Integer(ol.ol_i_id as i64),
                SqlValue::Integer(ol.ol_supply_w_id as i64),
                ol.ol_delivery_d.map(|s| SqlValue::Varchar(arcstr::ArcStr::from(s))).unwrap_or(SqlValue::Null),
                SqlValue::Integer(ol.ol_quantity as i64),
                SqlValue::Numeric(ol.ol_amount),
                SqlValue::Varchar(arcstr::ArcStr::from(ol.ol_dist_info)),
            ]);
            db.insert_row("order_line", ol_row).unwrap();
        }

        // New orders are the remaining 30%
        if o_id > delivered_threshold {
            let no = data.gen_new_order(o_id, d_id, w_id);
            let no_row = Row::new(vec![
                SqlValue::Integer(no.no_o_id as i64),
                SqlValue::Integer(no.no_d_id as i64),
                SqlValue::Integer(no.no_w_id as i64),
            ]);
            db.insert_row("new_order", no_row).unwrap();
        }
    }
}

// =============================================================================
// SQLite Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "sqlite-comparison")]
fn create_tpcc_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        "
        CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_name TEXT NOT NULL,
            w_street_1 TEXT NOT NULL,
            w_street_2 TEXT NOT NULL,
            w_city TEXT NOT NULL,
            w_state TEXT NOT NULL,
            w_zip TEXT NOT NULL,
            w_tax REAL NOT NULL,
            w_ytd REAL NOT NULL
        );

        CREATE TABLE district (
            d_id INTEGER NOT NULL,
            d_w_id INTEGER NOT NULL,
            d_name TEXT NOT NULL,
            d_street_1 TEXT NOT NULL,
            d_street_2 TEXT NOT NULL,
            d_city TEXT NOT NULL,
            d_state TEXT NOT NULL,
            d_zip TEXT NOT NULL,
            d_tax REAL NOT NULL,
            d_ytd REAL NOT NULL,
            d_next_o_id INTEGER NOT NULL,
            PRIMARY KEY (d_w_id, d_id)
        );

        CREATE TABLE customer (
            c_id INTEGER NOT NULL,
            c_d_id INTEGER NOT NULL,
            c_w_id INTEGER NOT NULL,
            c_first TEXT NOT NULL,
            c_middle TEXT NOT NULL,
            c_last TEXT NOT NULL,
            c_street_1 TEXT NOT NULL,
            c_street_2 TEXT NOT NULL,
            c_city TEXT NOT NULL,
            c_state TEXT NOT NULL,
            c_zip TEXT NOT NULL,
            c_phone TEXT NOT NULL,
            c_since TEXT NOT NULL,
            c_credit TEXT NOT NULL,
            c_credit_lim REAL NOT NULL,
            c_discount REAL NOT NULL,
            c_balance REAL NOT NULL,
            c_ytd_payment REAL NOT NULL,
            c_payment_cnt INTEGER NOT NULL,
            c_delivery_cnt INTEGER NOT NULL,
            c_data TEXT NOT NULL,
            PRIMARY KEY (c_w_id, c_d_id, c_id)
        );

        CREATE TABLE history (
            h_c_id INTEGER NOT NULL,
            h_c_d_id INTEGER NOT NULL,
            h_c_w_id INTEGER NOT NULL,
            h_d_id INTEGER NOT NULL,
            h_w_id INTEGER NOT NULL,
            h_date TEXT NOT NULL,
            h_amount REAL NOT NULL,
            h_data TEXT NOT NULL
        );

        CREATE TABLE new_order (
            no_o_id INTEGER NOT NULL,
            no_d_id INTEGER NOT NULL,
            no_w_id INTEGER NOT NULL,
            PRIMARY KEY (no_w_id, no_d_id, no_o_id)
        );

        CREATE TABLE orders (
            o_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_w_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_entry_d TEXT NOT NULL,
            o_carrier_id INTEGER,
            o_ol_cnt INTEGER NOT NULL,
            o_all_local INTEGER NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id)
        );

        CREATE TABLE order_line (
            ol_o_id INTEGER NOT NULL,
            ol_d_id INTEGER NOT NULL,
            ol_w_id INTEGER NOT NULL,
            ol_number INTEGER NOT NULL,
            ol_i_id INTEGER NOT NULL,
            ol_supply_w_id INTEGER NOT NULL,
            ol_delivery_d TEXT,
            ol_quantity INTEGER NOT NULL,
            ol_amount REAL NOT NULL,
            ol_dist_info TEXT NOT NULL,
            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
        );

        CREATE TABLE item (
            i_id INTEGER PRIMARY KEY,
            i_im_id INTEGER NOT NULL,
            i_name TEXT NOT NULL,
            i_price REAL NOT NULL,
            i_data TEXT NOT NULL
        );

        CREATE TABLE stock (
            s_i_id INTEGER NOT NULL,
            s_w_id INTEGER NOT NULL,
            s_quantity INTEGER NOT NULL,
            s_dist_01 TEXT NOT NULL,
            s_dist_02 TEXT NOT NULL,
            s_dist_03 TEXT NOT NULL,
            s_dist_04 TEXT NOT NULL,
            s_dist_05 TEXT NOT NULL,
            s_dist_06 TEXT NOT NULL,
            s_dist_07 TEXT NOT NULL,
            s_dist_08 TEXT NOT NULL,
            s_dist_09 TEXT NOT NULL,
            s_dist_10 TEXT NOT NULL,
            s_ytd INTEGER NOT NULL,
            s_order_cnt INTEGER NOT NULL,
            s_remote_cnt INTEGER NOT NULL,
            s_data TEXT NOT NULL,
            PRIMARY KEY (s_w_id, s_i_id)
        );
        ",
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn create_tpcc_indexes_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        "
        CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
        CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id, o_id);
        CREATE INDEX idx_order_line_district ON order_line (ol_w_id, ol_d_id, ol_o_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn load_item_sqlite(conn: &SqliteConn, data: &mut TPCCData) {
    let mut stmt = conn.prepare("INSERT INTO item VALUES (?, ?, ?, ?, ?)").unwrap();

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let item = data.gen_item(i_id);
        stmt.execute(rusqlite::params![
            item.i_id,
            item.i_im_id,
            item.i_name,
            item.i_price,
            item.i_data
        ])
        .unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_warehouse_sqlite(conn: &SqliteConn, data: &mut TPCCData, w_id: i32) {
    let warehouse = data.gen_warehouse(w_id);
    conn.execute(
        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rusqlite::params![
            warehouse.w_id,
            warehouse.w_name,
            warehouse.w_street_1,
            warehouse.w_street_2,
            warehouse.w_city,
            warehouse.w_state,
            warehouse.w_zip,
            warehouse.w_tax,
            warehouse.w_ytd
        ],
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn load_stock_sqlite(conn: &SqliteConn, data: &mut TPCCData, w_id: i32) {
    let mut stmt = conn
        .prepare("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let stock = data.gen_stock(i_id, w_id);
        stmt.execute(rusqlite::params![
            stock.s_i_id,
            stock.s_w_id,
            stock.s_quantity,
            stock.s_dist_01,
            stock.s_dist_02,
            stock.s_dist_03,
            stock.s_dist_04,
            stock.s_dist_05,
            stock.s_dist_06,
            stock.s_dist_07,
            stock.s_dist_08,
            stock.s_dist_09,
            stock.s_dist_10,
            stock.s_ytd,
            stock.s_order_cnt,
            stock.s_remote_cnt,
            stock.s_data
        ])
        .unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_district_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let district = data.gen_district(d_id, w_id);
    conn.execute(
        "INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rusqlite::params![
            district.d_id,
            district.d_w_id,
            district.d_name,
            district.d_street_1,
            district.d_street_2,
            district.d_city,
            district.d_state,
            district.d_zip,
            district.d_tax,
            district.d_ytd,
            district.d_next_o_id
        ],
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn load_customer_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut cust_stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut hist_stmt =
        conn.prepare("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    let customers_per_district = data.customers_per_district();
    for c_id in 1..=customers_per_district {
        let customer = data.gen_customer(c_id, d_id, w_id);
        cust_stmt
            .execute(rusqlite::params![
                customer.c_id,
                customer.c_d_id,
                customer.c_w_id,
                customer.c_first,
                customer.c_middle,
                customer.c_last,
                customer.c_street_1,
                customer.c_street_2,
                customer.c_city,
                customer.c_state,
                customer.c_zip,
                customer.c_phone,
                customer.c_since,
                customer.c_credit,
                customer.c_credit_lim,
                customer.c_discount,
                customer.c_balance,
                customer.c_ytd_payment,
                customer.c_payment_cnt,
                customer.c_delivery_cnt,
                customer.c_data
            ])
            .unwrap();

        let history = data.gen_history(c_id, d_id, w_id);
        hist_stmt
            .execute(rusqlite::params![
                history.h_c_id,
                history.h_c_d_id,
                history.h_c_w_id,
                history.h_d_id,
                history.h_w_id,
                history.h_date,
                history.h_amount,
                history.h_data
            ])
            .unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_orders_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let customers_per_district = data.customers_per_district();
    let orders_per_district = data.orders_per_district();
    let delivered_threshold = (orders_per_district as f64 * 0.7) as i32;

    let mut c_ids: Vec<i32> = (1..=customers_per_district).collect();
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    let mut order_stmt =
        conn.prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
    let mut ol_stmt =
        conn.prepare("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
    let mut no_stmt = conn.prepare("INSERT INTO new_order VALUES (?, ?, ?)").unwrap();

    for o_id in 1..=orders_per_district {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        order_stmt
            .execute(rusqlite::params![
                order.o_id,
                order.o_d_id,
                order.o_w_id,
                order.o_c_id,
                order.o_entry_d,
                order.o_carrier_id,
                order.o_ol_cnt,
                order.o_all_local
            ])
            .unwrap();

        let delivered = o_id <= delivered_threshold;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            ol_stmt
                .execute(rusqlite::params![
                    ol.ol_o_id,
                    ol.ol_d_id,
                    ol.ol_w_id,
                    ol.ol_number,
                    ol.ol_i_id,
                    ol.ol_supply_w_id,
                    ol.ol_delivery_d,
                    ol.ol_quantity,
                    ol.ol_amount,
                    ol.ol_dist_info
                ])
                .unwrap();
        }

        if o_id > delivered_threshold {
            let no = data.gen_new_order(o_id, d_id, w_id);
            no_stmt.execute(rusqlite::params![no.no_o_id, no.no_d_id, no.no_w_id]).unwrap();
        }
    }
}

// =============================================================================
// DuckDB Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "duckdb-comparison")]
fn create_tpcc_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        "
        CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_name VARCHAR NOT NULL,
            w_street_1 VARCHAR NOT NULL,
            w_street_2 VARCHAR NOT NULL,
            w_city VARCHAR NOT NULL,
            w_state VARCHAR NOT NULL,
            w_zip VARCHAR NOT NULL,
            w_tax DOUBLE NOT NULL,
            w_ytd DOUBLE NOT NULL
        );

        CREATE TABLE district (
            d_id INTEGER NOT NULL,
            d_w_id INTEGER NOT NULL,
            d_name VARCHAR NOT NULL,
            d_street_1 VARCHAR NOT NULL,
            d_street_2 VARCHAR NOT NULL,
            d_city VARCHAR NOT NULL,
            d_state VARCHAR NOT NULL,
            d_zip VARCHAR NOT NULL,
            d_tax DOUBLE NOT NULL,
            d_ytd DOUBLE NOT NULL,
            d_next_o_id INTEGER NOT NULL,
            PRIMARY KEY (d_w_id, d_id)
        );

        CREATE TABLE customer (
            c_id INTEGER NOT NULL,
            c_d_id INTEGER NOT NULL,
            c_w_id INTEGER NOT NULL,
            c_first VARCHAR NOT NULL,
            c_middle VARCHAR NOT NULL,
            c_last VARCHAR NOT NULL,
            c_street_1 VARCHAR NOT NULL,
            c_street_2 VARCHAR NOT NULL,
            c_city VARCHAR NOT NULL,
            c_state VARCHAR NOT NULL,
            c_zip VARCHAR NOT NULL,
            c_phone VARCHAR NOT NULL,
            c_since VARCHAR NOT NULL,
            c_credit VARCHAR NOT NULL,
            c_credit_lim DOUBLE NOT NULL,
            c_discount DOUBLE NOT NULL,
            c_balance DOUBLE NOT NULL,
            c_ytd_payment DOUBLE NOT NULL,
            c_payment_cnt INTEGER NOT NULL,
            c_delivery_cnt INTEGER NOT NULL,
            c_data VARCHAR NOT NULL,
            PRIMARY KEY (c_w_id, c_d_id, c_id)
        );

        CREATE TABLE history (
            h_c_id INTEGER NOT NULL,
            h_c_d_id INTEGER NOT NULL,
            h_c_w_id INTEGER NOT NULL,
            h_d_id INTEGER NOT NULL,
            h_w_id INTEGER NOT NULL,
            h_date VARCHAR NOT NULL,
            h_amount DOUBLE NOT NULL,
            h_data VARCHAR NOT NULL
        );

        CREATE TABLE new_order (
            no_o_id INTEGER NOT NULL,
            no_d_id INTEGER NOT NULL,
            no_w_id INTEGER NOT NULL,
            PRIMARY KEY (no_w_id, no_d_id, no_o_id)
        );

        CREATE TABLE orders (
            o_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_w_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_entry_d VARCHAR NOT NULL,
            o_carrier_id INTEGER,
            o_ol_cnt INTEGER NOT NULL,
            o_all_local INTEGER NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id)
        );

        CREATE TABLE order_line (
            ol_o_id INTEGER NOT NULL,
            ol_d_id INTEGER NOT NULL,
            ol_w_id INTEGER NOT NULL,
            ol_number INTEGER NOT NULL,
            ol_i_id INTEGER NOT NULL,
            ol_supply_w_id INTEGER NOT NULL,
            ol_delivery_d VARCHAR,
            ol_quantity INTEGER NOT NULL,
            ol_amount DOUBLE NOT NULL,
            ol_dist_info VARCHAR NOT NULL,
            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
        );

        CREATE TABLE item (
            i_id INTEGER PRIMARY KEY,
            i_im_id INTEGER NOT NULL,
            i_name VARCHAR NOT NULL,
            i_price DOUBLE NOT NULL,
            i_data VARCHAR NOT NULL
        );

        CREATE TABLE stock (
            s_i_id INTEGER NOT NULL,
            s_w_id INTEGER NOT NULL,
            s_quantity INTEGER NOT NULL,
            s_dist_01 VARCHAR NOT NULL,
            s_dist_02 VARCHAR NOT NULL,
            s_dist_03 VARCHAR NOT NULL,
            s_dist_04 VARCHAR NOT NULL,
            s_dist_05 VARCHAR NOT NULL,
            s_dist_06 VARCHAR NOT NULL,
            s_dist_07 VARCHAR NOT NULL,
            s_dist_08 VARCHAR NOT NULL,
            s_dist_09 VARCHAR NOT NULL,
            s_dist_10 VARCHAR NOT NULL,
            s_ytd INTEGER NOT NULL,
            s_order_cnt INTEGER NOT NULL,
            s_remote_cnt INTEGER NOT NULL,
            s_data VARCHAR NOT NULL,
            PRIMARY KEY (s_w_id, s_i_id)
        );
        ",
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn create_tpcc_indexes_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        "
        CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
        CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id, o_id);
        CREATE INDEX idx_order_line_district ON order_line (ol_w_id, ol_d_id, ol_o_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn load_item_duckdb(conn: &DuckDBConn, data: &mut TPCCData) {
    let mut stmt = conn.prepare("INSERT INTO item VALUES (?, ?, ?, ?, ?)").unwrap();

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let item = data.gen_item(i_id);
        stmt.execute(duckdb::params![
            item.i_id,
            item.i_im_id,
            item.i_name,
            item.i_price,
            item.i_data
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_warehouse_duckdb(conn: &DuckDBConn, data: &mut TPCCData, w_id: i32) {
    let warehouse = data.gen_warehouse(w_id);
    conn.execute(
        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            warehouse.w_id,
            warehouse.w_name,
            warehouse.w_street_1,
            warehouse.w_street_2,
            warehouse.w_city,
            warehouse.w_state,
            warehouse.w_zip,
            warehouse.w_tax,
            warehouse.w_ytd
        ],
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn load_stock_duckdb(conn: &DuckDBConn, data: &mut TPCCData, w_id: i32) {
    let mut stmt = conn
        .prepare("INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let stock = data.gen_stock(i_id, w_id);
        stmt.execute(duckdb::params![
            stock.s_i_id,
            stock.s_w_id,
            stock.s_quantity,
            stock.s_dist_01,
            stock.s_dist_02,
            stock.s_dist_03,
            stock.s_dist_04,
            stock.s_dist_05,
            stock.s_dist_06,
            stock.s_dist_07,
            stock.s_dist_08,
            stock.s_dist_09,
            stock.s_dist_10,
            stock.s_ytd,
            stock.s_order_cnt,
            stock.s_remote_cnt,
            stock.s_data
        ])
        .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_district_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let district = data.gen_district(d_id, w_id);
    conn.execute(
        "INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            district.d_id,
            district.d_w_id,
            district.d_name,
            district.d_street_1,
            district.d_street_2,
            district.d_city,
            district.d_state,
            district.d_zip,
            district.d_tax,
            district.d_ytd,
            district.d_next_o_id
        ],
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn load_customer_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut cust_stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut hist_stmt =
        conn.prepare("INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();

    let customers_per_district = data.customers_per_district();
    for c_id in 1..=customers_per_district {
        let customer = data.gen_customer(c_id, d_id, w_id);
        cust_stmt
            .execute(duckdb::params![
                customer.c_id,
                customer.c_d_id,
                customer.c_w_id,
                customer.c_first,
                customer.c_middle,
                customer.c_last,
                customer.c_street_1,
                customer.c_street_2,
                customer.c_city,
                customer.c_state,
                customer.c_zip,
                customer.c_phone,
                customer.c_since,
                customer.c_credit,
                customer.c_credit_lim,
                customer.c_discount,
                customer.c_balance,
                customer.c_ytd_payment,
                customer.c_payment_cnt,
                customer.c_delivery_cnt,
                customer.c_data
            ])
            .unwrap();

        let history = data.gen_history(c_id, d_id, w_id);
        hist_stmt
            .execute(duckdb::params![
                history.h_c_id,
                history.h_c_d_id,
                history.h_c_w_id,
                history.h_d_id,
                history.h_w_id,
                history.h_date,
                history.h_amount,
                history.h_data
            ])
            .unwrap();
    }
}

#[cfg(feature = "duckdb-comparison")]
fn load_orders_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let customers_per_district = data.customers_per_district();
    let orders_per_district = data.orders_per_district();
    let delivered_threshold = (orders_per_district as f64 * 0.7) as i32;

    let mut c_ids: Vec<i32> = (1..=customers_per_district).collect();
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    let mut order_stmt =
        conn.prepare("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
    let mut ol_stmt =
        conn.prepare("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").unwrap();
    let mut no_stmt = conn.prepare("INSERT INTO new_order VALUES (?, ?, ?)").unwrap();

    for o_id in 1..=orders_per_district {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        order_stmt
            .execute(duckdb::params![
                order.o_id,
                order.o_d_id,
                order.o_w_id,
                order.o_c_id,
                order.o_entry_d,
                order.o_carrier_id,
                order.o_ol_cnt,
                order.o_all_local
            ])
            .unwrap();

        let delivered = o_id <= delivered_threshold;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            ol_stmt
                .execute(duckdb::params![
                    ol.ol_o_id,
                    ol.ol_d_id,
                    ol.ol_w_id,
                    ol.ol_number,
                    ol.ol_i_id,
                    ol.ol_supply_w_id,
                    ol.ol_delivery_d,
                    ol.ol_quantity,
                    ol.ol_amount,
                    ol.ol_dist_info
                ])
                .unwrap();
        }

        if o_id > delivered_threshold {
            let no = data.gen_new_order(o_id, d_id, w_id);
            no_stmt.execute(duckdb::params![no.no_o_id, no.no_d_id, no.no_w_id]).unwrap();
        }
    }
}

// =============================================================================
// MySQL Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "mysql-comparison")]
fn create_tpcc_schema_mysql(conn: &mut PooledConn) {
    // Drop tables if they exist (in reverse dependency order)
    let drop_tables = [
        "DROP TABLE IF EXISTS order_line",
        "DROP TABLE IF EXISTS new_order",
        "DROP TABLE IF EXISTS orders",
        "DROP TABLE IF EXISTS history",
        "DROP TABLE IF EXISTS customer",
        "DROP TABLE IF EXISTS stock",
        "DROP TABLE IF EXISTS district",
        "DROP TABLE IF EXISTS warehouse",
        "DROP TABLE IF EXISTS item",
    ];
    for stmt in drop_tables {
        conn.query_drop(stmt).unwrap();
    }

    conn.query_drop(
        r#"
        CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_name VARCHAR(10) NOT NULL,
            w_street_1 VARCHAR(20) NOT NULL,
            w_street_2 VARCHAR(20) NOT NULL,
            w_city VARCHAR(20) NOT NULL,
            w_state VARCHAR(2) NOT NULL,
            w_zip VARCHAR(9) NOT NULL,
            w_tax DECIMAL(4,4) NOT NULL,
            w_ytd DECIMAL(12,2) NOT NULL
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE district (
            d_id INTEGER NOT NULL,
            d_w_id INTEGER NOT NULL,
            d_name VARCHAR(10) NOT NULL,
            d_street_1 VARCHAR(20) NOT NULL,
            d_street_2 VARCHAR(20) NOT NULL,
            d_city VARCHAR(20) NOT NULL,
            d_state VARCHAR(2) NOT NULL,
            d_zip VARCHAR(9) NOT NULL,
            d_tax DECIMAL(4,4) NOT NULL,
            d_ytd DECIMAL(12,2) NOT NULL,
            d_next_o_id INTEGER NOT NULL,
            PRIMARY KEY (d_w_id, d_id)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE customer (
            c_id INTEGER NOT NULL,
            c_d_id INTEGER NOT NULL,
            c_w_id INTEGER NOT NULL,
            c_first VARCHAR(16) NOT NULL,
            c_middle VARCHAR(2) NOT NULL,
            c_last VARCHAR(16) NOT NULL,
            c_street_1 VARCHAR(20) NOT NULL,
            c_street_2 VARCHAR(20) NOT NULL,
            c_city VARCHAR(20) NOT NULL,
            c_state VARCHAR(2) NOT NULL,
            c_zip VARCHAR(9) NOT NULL,
            c_phone VARCHAR(16) NOT NULL,
            c_since VARCHAR(25) NOT NULL,
            c_credit VARCHAR(2) NOT NULL,
            c_credit_lim DECIMAL(12,2) NOT NULL,
            c_discount DECIMAL(4,4) NOT NULL,
            c_balance DECIMAL(12,2) NOT NULL,
            c_ytd_payment DECIMAL(12,2) NOT NULL,
            c_payment_cnt INTEGER NOT NULL,
            c_delivery_cnt INTEGER NOT NULL,
            c_data VARCHAR(500) NOT NULL,
            PRIMARY KEY (c_w_id, c_d_id, c_id)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE history (
            h_c_id INTEGER NOT NULL,
            h_c_d_id INTEGER NOT NULL,
            h_c_w_id INTEGER NOT NULL,
            h_d_id INTEGER NOT NULL,
            h_w_id INTEGER NOT NULL,
            h_date VARCHAR(25) NOT NULL,
            h_amount DECIMAL(6,2) NOT NULL,
            h_data VARCHAR(24) NOT NULL
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE new_order (
            no_o_id INTEGER NOT NULL,
            no_d_id INTEGER NOT NULL,
            no_w_id INTEGER NOT NULL,
            PRIMARY KEY (no_w_id, no_d_id, no_o_id)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE orders (
            o_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_w_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_entry_d VARCHAR(25) NOT NULL,
            o_carrier_id INTEGER,
            o_ol_cnt INTEGER NOT NULL,
            o_all_local INTEGER NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE order_line (
            ol_o_id INTEGER NOT NULL,
            ol_d_id INTEGER NOT NULL,
            ol_w_id INTEGER NOT NULL,
            ol_number INTEGER NOT NULL,
            ol_i_id INTEGER NOT NULL,
            ol_supply_w_id INTEGER NOT NULL,
            ol_delivery_d VARCHAR(25),
            ol_quantity INTEGER NOT NULL,
            ol_amount DECIMAL(6,2) NOT NULL,
            ol_dist_info VARCHAR(24) NOT NULL,
            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE item (
            i_id INTEGER PRIMARY KEY,
            i_im_id INTEGER NOT NULL,
            i_name VARCHAR(24) NOT NULL,
            i_price DECIMAL(5,2) NOT NULL,
            i_data VARCHAR(50) NOT NULL
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();

    conn.query_drop(
        r#"
        CREATE TABLE stock (
            s_i_id INTEGER NOT NULL,
            s_w_id INTEGER NOT NULL,
            s_quantity INTEGER NOT NULL,
            s_dist_01 VARCHAR(24) NOT NULL,
            s_dist_02 VARCHAR(24) NOT NULL,
            s_dist_03 VARCHAR(24) NOT NULL,
            s_dist_04 VARCHAR(24) NOT NULL,
            s_dist_05 VARCHAR(24) NOT NULL,
            s_dist_06 VARCHAR(24) NOT NULL,
            s_dist_07 VARCHAR(24) NOT NULL,
            s_dist_08 VARCHAR(24) NOT NULL,
            s_dist_09 VARCHAR(24) NOT NULL,
            s_dist_10 VARCHAR(24) NOT NULL,
            s_ytd INTEGER NOT NULL,
            s_order_cnt INTEGER NOT NULL,
            s_remote_cnt INTEGER NOT NULL,
            s_data VARCHAR(50) NOT NULL,
            PRIMARY KEY (s_w_id, s_i_id)
        ) ENGINE=InnoDB
    "#,
    )
    .unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn create_tpcc_indexes_mysql(conn: &mut PooledConn) {
    conn.query_drop("CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)")
        .unwrap();
    conn.query_drop("CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id, o_id)")
        .unwrap();
    conn.query_drop(
        "CREATE INDEX idx_order_line_district ON order_line (ol_w_id, ol_d_id, ol_o_id)",
    )
    .unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn load_item_mysql(conn: &mut PooledConn, data: &mut TPCCData) {
    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let item = data.gen_item(i_id);
        conn.exec_drop(
            "INSERT INTO item VALUES (?, ?, ?, ?, ?)",
            (item.i_id, item.i_im_id, &item.i_name, item.i_price, &item.i_data),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_warehouse_mysql(conn: &mut PooledConn, data: &mut TPCCData, w_id: i32) {
    let warehouse = data.gen_warehouse(w_id);
    conn.exec_drop(
        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            warehouse.w_id,
            &warehouse.w_name,
            &warehouse.w_street_1,
            &warehouse.w_street_2,
            &warehouse.w_city,
            &warehouse.w_state,
            &warehouse.w_zip,
            warehouse.w_tax,
            warehouse.w_ytd,
        ),
    )
    .unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn load_stock_mysql(conn: &mut PooledConn, data: &mut TPCCData, w_id: i32) {
    use mysql::Value;

    let num_items = data.num_items();
    for i_id in 1..=num_items {
        let stock = data.gen_stock(i_id, w_id);
        // MySQL Params doesn't support 17-element tuples, so use Vec<Value>
        let params: Vec<Value> = vec![
            Value::from(stock.s_i_id),
            Value::from(stock.s_w_id),
            Value::from(stock.s_quantity),
            Value::from(stock.s_dist_01),
            Value::from(stock.s_dist_02),
            Value::from(stock.s_dist_03),
            Value::from(stock.s_dist_04),
            Value::from(stock.s_dist_05),
            Value::from(stock.s_dist_06),
            Value::from(stock.s_dist_07),
            Value::from(stock.s_dist_08),
            Value::from(stock.s_dist_09),
            Value::from(stock.s_dist_10),
            Value::from(stock.s_ytd),
            Value::from(stock.s_order_cnt),
            Value::from(stock.s_remote_cnt),
            Value::from(stock.s_data),
        ];
        conn.exec_drop(
            "INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_district_mysql(conn: &mut PooledConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use mysql::Value;

    let district = data.gen_district(d_id, w_id);
    let params: Vec<Value> = vec![
        Value::from(district.d_id),
        Value::from(district.d_w_id),
        Value::from(district.d_name),
        Value::from(district.d_street_1),
        Value::from(district.d_street_2),
        Value::from(district.d_city),
        Value::from(district.d_state),
        Value::from(district.d_zip),
        Value::from(district.d_tax),
        Value::from(district.d_ytd),
        Value::from(district.d_next_o_id),
    ];
    conn.exec_drop("INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", params)
        .unwrap();
}

#[cfg(feature = "mysql-comparison")]
fn load_customer_mysql(conn: &mut PooledConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use mysql::Value;

    let customers_per_district = data.customers_per_district();
    for c_id in 1..=customers_per_district {
        let customer = data.gen_customer(c_id, d_id, w_id);
        let params: Vec<Value> = vec![
            Value::from(customer.c_id),
            Value::from(customer.c_d_id),
            Value::from(customer.c_w_id),
            Value::from(customer.c_first),
            Value::from(customer.c_middle),
            Value::from(customer.c_last),
            Value::from(customer.c_street_1),
            Value::from(customer.c_street_2),
            Value::from(customer.c_city),
            Value::from(customer.c_state),
            Value::from(customer.c_zip),
            Value::from(customer.c_phone),
            Value::from(customer.c_since),
            Value::from(customer.c_credit),
            Value::from(customer.c_credit_lim),
            Value::from(customer.c_discount),
            Value::from(customer.c_balance),
            Value::from(customer.c_ytd_payment),
            Value::from(customer.c_payment_cnt),
            Value::from(customer.c_delivery_cnt),
            Value::from(customer.c_data),
        ];
        conn.exec_drop(
            "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
        ).unwrap();

        let history = data.gen_history(c_id, d_id, w_id);
        conn.exec_drop(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                history.h_c_id,
                history.h_c_d_id,
                history.h_c_w_id,
                history.h_d_id,
                history.h_w_id,
                &history.h_date,
                history.h_amount,
                &history.h_data,
            ),
        )
        .unwrap();
    }
}

#[cfg(feature = "mysql-comparison")]
fn load_orders_mysql(conn: &mut PooledConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use mysql::Value;

    let customers_per_district = data.customers_per_district();
    let orders_per_district = data.orders_per_district();
    let delivered_threshold = (orders_per_district as f64 * 0.7) as i32;

    let mut c_ids: Vec<i32> = (1..=customers_per_district).collect();
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    for o_id in 1..=orders_per_district {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        conn.exec_drop(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order.o_id,
                order.o_d_id,
                order.o_w_id,
                order.o_c_id,
                &order.o_entry_d,
                order.o_carrier_id,
                order.o_ol_cnt,
                order.o_all_local,
            ),
        )
        .unwrap();

        let delivered = o_id <= delivered_threshold;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            let params: Vec<Value> = vec![
                Value::from(ol.ol_o_id),
                Value::from(ol.ol_d_id),
                Value::from(ol.ol_w_id),
                Value::from(ol.ol_number),
                Value::from(ol.ol_i_id),
                Value::from(ol.ol_supply_w_id),
                ol.ol_delivery_d.map(Value::from).unwrap_or(Value::NULL),
                Value::from(ol.ol_quantity),
                Value::from(ol.ol_amount),
                Value::from(ol.ol_dist_info),
            ];
            conn.exec_drop("INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", params)
                .unwrap();
        }

        if o_id > delivered_threshold {
            let no = data.gen_new_order(o_id, d_id, w_id);
            conn.exec_drop(
                "INSERT INTO new_order VALUES (?, ?, ?)",
                (no.no_o_id, no.no_d_id, no.no_w_id),
            )
            .unwrap();
        }
    }
}
