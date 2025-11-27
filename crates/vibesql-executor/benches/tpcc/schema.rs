//! TPC-C Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-C
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).

use super::data::TPCCData;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
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

    // Load data
    load_item_vibesql(&mut db, &mut data);

    for w_id in 1..=data.num_warehouses() {
        load_warehouse_vibesql(&mut db, &mut data, w_id);
        load_stock_vibesql(&mut db, &mut data, w_id);

        for d_id in 1..=TPCCData::DISTRICTS_PER_WAREHOUSE {
            load_district_vibesql(&mut db, &mut data, d_id, w_id);
            load_customer_vibesql(&mut db, &mut data, d_id, w_id);
            load_orders_vibesql(&mut db, &mut data, d_id, w_id);
        }
    }

    // Create indexes for performance
    create_tpcc_indexes_vibesql(&mut db);

    // Compute statistics for join order optimization
    for table_name in [
        "warehouse", "district", "customer", "history",
        "orders", "new_order", "order_line", "item", "stock",
    ] {
        if let Some(table) = db.get_table_mut(table_name) {
            table.analyze();
        }
    }

    db
}

#[cfg(feature = "benchmark-comparison")]
pub fn load_sqlite(scale_factor: f64) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = TPCCData::new(scale_factor);

    create_tpcc_schema_sqlite(&conn);

    load_item_sqlite(&conn, &mut data);

    for w_id in 1..=data.num_warehouses() {
        load_warehouse_sqlite(&conn, &mut data, w_id);
        load_stock_sqlite(&conn, &mut data, w_id);

        for d_id in 1..=TPCCData::DISTRICTS_PER_WAREHOUSE {
            load_district_sqlite(&conn, &mut data, d_id, w_id);
            load_customer_sqlite(&conn, &mut data, d_id, w_id);
            load_orders_sqlite(&conn, &mut data, d_id, w_id);
        }
    }

    create_tpcc_indexes_sqlite(&conn);
    conn
}

#[cfg(feature = "benchmark-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCCData::new(scale_factor);

    create_tpcc_schema_duckdb(&conn);

    load_item_duckdb(&conn, &mut data);

    for w_id in 1..=data.num_warehouses() {
        load_warehouse_duckdb(&conn, &mut data, w_id);
        load_stock_duckdb(&conn, &mut data, w_id);

        for d_id in 1..=TPCCData::DISTRICTS_PER_WAREHOUSE {
            load_district_duckdb(&conn, &mut data, d_id, w_id);
            load_customer_duckdb(&conn, &mut data, d_id, w_id);
            load_orders_duckdb(&conn, &mut data, d_id, w_id);
        }
    }

    create_tpcc_indexes_duckdb(&conn);
    conn
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
    db.create_table(TableSchema::new(
        "warehouse".to_string(),
        vec![
            ColumnSchema::new("w_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("w_name".to_string(), varchar(), false),
            ColumnSchema::new("w_street_1".to_string(), varchar(), false),
            ColumnSchema::new("w_street_2".to_string(), varchar(), false),
            ColumnSchema::new("w_city".to_string(), varchar(), false),
            ColumnSchema::new("w_state".to_string(), varchar(), false),
            ColumnSchema::new("w_zip".to_string(), varchar(), false),
            ColumnSchema::new("w_tax".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("w_ytd".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
        ],
    ))
    .unwrap();

    // DISTRICT table
    db.create_table(TableSchema::new(
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
            ColumnSchema::new("d_tax".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("d_ytd".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("d_next_o_id".to_string(), DataType::Integer, false),
        ],
    ))
    .unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::new(
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
            ColumnSchema::new("c_credit_lim".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("c_discount".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("c_balance".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("c_ytd_payment".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("c_payment_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_delivery_cnt".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_data".to_string(), varchar(), false),
        ],
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
            ColumnSchema::new("h_amount".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("h_data".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // NEW_ORDER table
    db.create_table(TableSchema::new(
        "new_order".to_string(),
        vec![
            ColumnSchema::new("no_o_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("no_d_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("no_w_id".to_string(), DataType::Integer, false),
        ],
    ))
    .unwrap();

    // ORDERS table
    db.create_table(TableSchema::new(
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
    ))
    .unwrap();

    // ORDER_LINE table
    db.create_table(TableSchema::new(
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
            ColumnSchema::new("ol_amount".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("ol_dist_info".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // ITEM table
    db.create_table(TableSchema::new(
        "item".to_string(),
        vec![
            ColumnSchema::new("i_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("i_im_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("i_name".to_string(), varchar(), false),
            ColumnSchema::new("i_price".to_string(), DataType::Decimal { precision: 15, scale: 2 }, false),
            ColumnSchema::new("i_data".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // STOCK table
    db.create_table(TableSchema::new(
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
    ))
    .unwrap();
}

fn create_tpcc_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Helper to create index columns
    fn col(name: &str) -> IndexColumn {
        IndexColumn {
            column_name: name.to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }
    }

    // Primary key indexes (only for tables with < 100k rows to avoid slow disk-backed indexing)
    db.create_index(
        "idx_warehouse_pk".to_string(),
        "warehouse".to_string(),
        true,
        vec![col("w_id")],
    ).ok();

    db.create_index(
        "idx_district_pk".to_string(),
        "district".to_string(),
        true,
        vec![col("d_w_id"), col("d_id")],
    ).ok();

    db.create_index(
        "idx_customer_pk".to_string(),
        "customer".to_string(),
        true,
        vec![col("c_w_id"), col("c_d_id"), col("c_id")],
    ).ok();

    db.create_index(
        "idx_orders_pk".to_string(),
        "orders".to_string(),
        true,
        vec![col("o_w_id"), col("o_d_id"), col("o_id")],
    ).ok();

    db.create_index(
        "idx_new_order_pk".to_string(),
        "new_order".to_string(),
        true,
        vec![col("no_w_id"), col("no_d_id"), col("no_o_id")],
    ).ok();

    // NOTE: The following indexes are skipped because their tables have >= 100k rows,
    // which triggers disk-backed indexing that is currently very slow (causes benchmark
    // to hang indefinitely). The disk-backed B+ tree bulk_load needs performance
    // optimization. For now, we skip these indexes to allow the benchmark to complete.
    // The transactions will still work correctly, just without optimal index performance.
    //
    // Skipped indexes (tables with >= 100k rows trigger disk-backed mode):
    // - idx_order_line_pk: order_line has ~300k rows
    // - idx_item_pk: item has 100k rows
    // - idx_stock_pk: stock has 100k rows per warehouse
    //
    // See: https://github.com/rjwalters/vibesql/issues/2793

    // Secondary indexes for queries (on smaller tables)
    db.create_index(
        "idx_customer_name".to_string(),
        "customer".to_string(),
        false,
        vec![col("c_w_id"), col("c_d_id"), col("c_last"), col("c_first")],
    ).ok();

    db.create_index(
        "idx_orders_customer".to_string(),
        "orders".to_string(),
        false,
        vec![col("o_w_id"), col("o_d_id"), col("o_c_id")],
    ).ok();
}

fn load_item_vibesql(db: &mut VibeDB, data: &mut TPCCData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let item = data.gen_item(i_id);
        let row = Row::new(vec![
            SqlValue::Integer(item.i_id as i64),
            SqlValue::Integer(item.i_im_id as i64),
            SqlValue::Varchar(item.i_name),
            SqlValue::Numeric(item.i_price),
            SqlValue::Varchar(item.i_data),
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
        SqlValue::Varchar(warehouse.w_name),
        SqlValue::Varchar(warehouse.w_street_1),
        SqlValue::Varchar(warehouse.w_street_2),
        SqlValue::Varchar(warehouse.w_city),
        SqlValue::Varchar(warehouse.w_state),
        SqlValue::Varchar(warehouse.w_zip),
        SqlValue::Numeric(warehouse.w_tax),
        SqlValue::Numeric(warehouse.w_ytd),
    ]);
    db.insert_row("warehouse", row).unwrap();
}

fn load_stock_vibesql(db: &mut VibeDB, data: &mut TPCCData, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let stock = data.gen_stock(i_id, w_id);
        let row = Row::new(vec![
            SqlValue::Integer(stock.s_i_id as i64),
            SqlValue::Integer(stock.s_w_id as i64),
            SqlValue::Integer(stock.s_quantity as i64),
            SqlValue::Varchar(stock.s_dist_01),
            SqlValue::Varchar(stock.s_dist_02),
            SqlValue::Varchar(stock.s_dist_03),
            SqlValue::Varchar(stock.s_dist_04),
            SqlValue::Varchar(stock.s_dist_05),
            SqlValue::Varchar(stock.s_dist_06),
            SqlValue::Varchar(stock.s_dist_07),
            SqlValue::Varchar(stock.s_dist_08),
            SqlValue::Varchar(stock.s_dist_09),
            SqlValue::Varchar(stock.s_dist_10),
            SqlValue::Integer(stock.s_ytd as i64),
            SqlValue::Integer(stock.s_order_cnt as i64),
            SqlValue::Integer(stock.s_remote_cnt as i64),
            SqlValue::Varchar(stock.s_data),
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
        SqlValue::Varchar(district.d_name),
        SqlValue::Varchar(district.d_street_1),
        SqlValue::Varchar(district.d_street_2),
        SqlValue::Varchar(district.d_city),
        SqlValue::Varchar(district.d_state),
        SqlValue::Varchar(district.d_zip),
        SqlValue::Numeric(district.d_tax),
        SqlValue::Numeric(district.d_ytd),
        SqlValue::Integer(district.d_next_o_id as i64),
    ]);
    db.insert_row("district", row).unwrap();
}

fn load_customer_vibesql(db: &mut VibeDB, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for c_id in 1..=TPCCData::CUSTOMERS_PER_DISTRICT {
        let customer = data.gen_customer(c_id, d_id, w_id);
        let row = Row::new(vec![
            SqlValue::Integer(customer.c_id as i64),
            SqlValue::Integer(customer.c_d_id as i64),
            SqlValue::Integer(customer.c_w_id as i64),
            SqlValue::Varchar(customer.c_first),
            SqlValue::Varchar(customer.c_middle),
            SqlValue::Varchar(customer.c_last),
            SqlValue::Varchar(customer.c_street_1),
            SqlValue::Varchar(customer.c_street_2),
            SqlValue::Varchar(customer.c_city),
            SqlValue::Varchar(customer.c_state),
            SqlValue::Varchar(customer.c_zip),
            SqlValue::Varchar(customer.c_phone),
            SqlValue::Varchar(customer.c_since),
            SqlValue::Varchar(customer.c_credit),
            SqlValue::Numeric(customer.c_credit_lim),
            SqlValue::Numeric(customer.c_discount),
            SqlValue::Numeric(customer.c_balance),
            SqlValue::Numeric(customer.c_ytd_payment),
            SqlValue::Integer(customer.c_payment_cnt as i64),
            SqlValue::Integer(customer.c_delivery_cnt as i64),
            SqlValue::Varchar(customer.c_data),
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
            SqlValue::Varchar(history.h_date),
            SqlValue::Numeric(history.h_amount),
            SqlValue::Varchar(history.h_data),
        ]);
        db.insert_row("history", history_row).unwrap();
    }
}

fn load_orders_vibesql(db: &mut VibeDB, data: &mut TPCCData, d_id: i32, w_id: i32) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Generate customer IDs in random order for orders
    let mut c_ids: Vec<i32> = (1..=TPCCData::CUSTOMERS_PER_DISTRICT).collect();
    // Simple shuffle using the RNG
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    for o_id in 1..=TPCCData::ORDERS_PER_DISTRICT {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        let order_row = Row::new(vec![
            SqlValue::Integer(order.o_id as i64),
            SqlValue::Integer(order.o_d_id as i64),
            SqlValue::Integer(order.o_w_id as i64),
            SqlValue::Integer(order.o_c_id as i64),
            SqlValue::Varchar(order.o_entry_d),
            order.o_carrier_id.map(|v| SqlValue::Integer(v as i64)).unwrap_or(SqlValue::Null),
            SqlValue::Integer(order.o_ol_cnt as i64),
            SqlValue::Integer(order.o_all_local as i64),
        ]);
        db.insert_row("orders", order_row).unwrap();

        // Generate order lines
        let delivered = o_id <= 2100;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            let ol_row = Row::new(vec![
                SqlValue::Integer(ol.ol_o_id as i64),
                SqlValue::Integer(ol.ol_d_id as i64),
                SqlValue::Integer(ol.ol_w_id as i64),
                SqlValue::Integer(ol.ol_number as i64),
                SqlValue::Integer(ol.ol_i_id as i64),
                SqlValue::Integer(ol.ol_supply_w_id as i64),
                ol.ol_delivery_d.map(|v| SqlValue::Varchar(v)).unwrap_or(SqlValue::Null),
                SqlValue::Integer(ol.ol_quantity as i64),
                SqlValue::Numeric(ol.ol_amount),
                SqlValue::Varchar(ol.ol_dist_info),
            ]);
            db.insert_row("order_line", ol_row).unwrap();
        }

        // New orders are orders 2101-3000
        if o_id > 2100 {
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

#[cfg(feature = "benchmark-comparison")]
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

#[cfg(feature = "benchmark-comparison")]
fn create_tpcc_indexes_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        "
        CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
        CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_item_sqlite(conn: &SqliteConn, data: &mut TPCCData) {
    let mut stmt = conn.prepare(
        "INSERT INTO item VALUES (?, ?, ?, ?, ?)"
    ).unwrap();

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let item = data.gen_item(i_id);
        stmt.execute(rusqlite::params![
            item.i_id, item.i_im_id, item.i_name, item.i_price, item.i_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_warehouse_sqlite(conn: &SqliteConn, data: &mut TPCCData, w_id: i32) {
    let warehouse = data.gen_warehouse(w_id);
    conn.execute(
        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rusqlite::params![
            warehouse.w_id, warehouse.w_name, warehouse.w_street_1, warehouse.w_street_2,
            warehouse.w_city, warehouse.w_state, warehouse.w_zip, warehouse.w_tax, warehouse.w_ytd
        ],
    ).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_stock_sqlite(conn: &SqliteConn, data: &mut TPCCData, w_id: i32) {
    let mut stmt = conn.prepare(
        "INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let stock = data.gen_stock(i_id, w_id);
        stmt.execute(rusqlite::params![
            stock.s_i_id, stock.s_w_id, stock.s_quantity,
            stock.s_dist_01, stock.s_dist_02, stock.s_dist_03, stock.s_dist_04, stock.s_dist_05,
            stock.s_dist_06, stock.s_dist_07, stock.s_dist_08, stock.s_dist_09, stock.s_dist_10,
            stock.s_ytd, stock.s_order_cnt, stock.s_remote_cnt, stock.s_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_district_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let district = data.gen_district(d_id, w_id);
    conn.execute(
        "INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rusqlite::params![
            district.d_id, district.d_w_id, district.d_name, district.d_street_1, district.d_street_2,
            district.d_city, district.d_state, district.d_zip, district.d_tax, district.d_ytd, district.d_next_o_id
        ],
    ).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut cust_stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut hist_stmt = conn.prepare(
        "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for c_id in 1..=TPCCData::CUSTOMERS_PER_DISTRICT {
        let customer = data.gen_customer(c_id, d_id, w_id);
        cust_stmt.execute(rusqlite::params![
            customer.c_id, customer.c_d_id, customer.c_w_id,
            customer.c_first, customer.c_middle, customer.c_last,
            customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip,
            customer.c_phone, customer.c_since, customer.c_credit, customer.c_credit_lim,
            customer.c_discount, customer.c_balance, customer.c_ytd_payment,
            customer.c_payment_cnt, customer.c_delivery_cnt, customer.c_data
        ]).unwrap();

        let history = data.gen_history(c_id, d_id, w_id);
        hist_stmt.execute(rusqlite::params![
            history.h_c_id, history.h_c_d_id, history.h_c_w_id,
            history.h_d_id, history.h_w_id, history.h_date, history.h_amount, history.h_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_orders_sqlite(conn: &SqliteConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut c_ids: Vec<i32> = (1..=TPCCData::CUSTOMERS_PER_DISTRICT).collect();
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    let mut order_stmt = conn.prepare(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut ol_stmt = conn.prepare(
        "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut no_stmt = conn.prepare(
        "INSERT INTO new_order VALUES (?, ?, ?)"
    ).unwrap();

    for o_id in 1..=TPCCData::ORDERS_PER_DISTRICT {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        order_stmt.execute(rusqlite::params![
            order.o_id, order.o_d_id, order.o_w_id, order.o_c_id,
            order.o_entry_d, order.o_carrier_id, order.o_ol_cnt, order.o_all_local
        ]).unwrap();

        let delivered = o_id <= 2100;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            ol_stmt.execute(rusqlite::params![
                ol.ol_o_id, ol.ol_d_id, ol.ol_w_id, ol.ol_number,
                ol.ol_i_id, ol.ol_supply_w_id, ol.ol_delivery_d,
                ol.ol_quantity, ol.ol_amount, ol.ol_dist_info
            ]).unwrap();
        }

        if o_id > 2100 {
            let no = data.gen_new_order(o_id, d_id, w_id);
            no_stmt.execute(rusqlite::params![no.no_o_id, no.no_d_id, no.no_w_id]).unwrap();
        }
    }
}

// =============================================================================
// DuckDB Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
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

#[cfg(feature = "benchmark-comparison")]
fn create_tpcc_indexes_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        "
        CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first);
        CREATE INDEX idx_orders_customer ON orders (o_w_id, o_d_id, o_c_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_item_duckdb(conn: &DuckDBConn, data: &mut TPCCData) {
    let mut stmt = conn.prepare(
        "INSERT INTO item VALUES (?, ?, ?, ?, ?)"
    ).unwrap();

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let item = data.gen_item(i_id);
        stmt.execute(duckdb::params![
            item.i_id, item.i_im_id, item.i_name, item.i_price, item.i_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_warehouse_duckdb(conn: &DuckDBConn, data: &mut TPCCData, w_id: i32) {
    let warehouse = data.gen_warehouse(w_id);
    conn.execute(
        "INSERT INTO warehouse VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            warehouse.w_id, warehouse.w_name, warehouse.w_street_1, warehouse.w_street_2,
            warehouse.w_city, warehouse.w_state, warehouse.w_zip, warehouse.w_tax, warehouse.w_ytd
        ],
    ).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_stock_duckdb(conn: &DuckDBConn, data: &mut TPCCData, w_id: i32) {
    let mut stmt = conn.prepare(
        "INSERT INTO stock VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i_id in 1..=TPCCData::NUM_ITEMS {
        let stock = data.gen_stock(i_id, w_id);
        stmt.execute(duckdb::params![
            stock.s_i_id, stock.s_w_id, stock.s_quantity,
            stock.s_dist_01, stock.s_dist_02, stock.s_dist_03, stock.s_dist_04, stock.s_dist_05,
            stock.s_dist_06, stock.s_dist_07, stock.s_dist_08, stock.s_dist_09, stock.s_dist_10,
            stock.s_ytd, stock.s_order_cnt, stock.s_remote_cnt, stock.s_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_district_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let district = data.gen_district(d_id, w_id);
    conn.execute(
        "INSERT INTO district VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![
            district.d_id, district.d_w_id, district.d_name, district.d_street_1, district.d_street_2,
            district.d_city, district.d_state, district.d_zip, district.d_tax, district.d_ytd, district.d_next_o_id
        ],
    ).unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut cust_stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut hist_stmt = conn.prepare(
        "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for c_id in 1..=TPCCData::CUSTOMERS_PER_DISTRICT {
        let customer = data.gen_customer(c_id, d_id, w_id);
        cust_stmt.execute(duckdb::params![
            customer.c_id, customer.c_d_id, customer.c_w_id,
            customer.c_first, customer.c_middle, customer.c_last,
            customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip,
            customer.c_phone, customer.c_since, customer.c_credit, customer.c_credit_lim,
            customer.c_discount, customer.c_balance, customer.c_ytd_payment,
            customer.c_payment_cnt, customer.c_delivery_cnt, customer.c_data
        ]).unwrap();

        let history = data.gen_history(c_id, d_id, w_id);
        hist_stmt.execute(duckdb::params![
            history.h_c_id, history.h_c_d_id, history.h_c_w_id,
            history.h_d_id, history.h_w_id, history.h_date, history.h_amount, history.h_data
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_orders_duckdb(conn: &DuckDBConn, data: &mut TPCCData, d_id: i32, w_id: i32) {
    let mut c_ids: Vec<i32> = (1..=TPCCData::CUSTOMERS_PER_DISTRICT).collect();
    for i in (1..c_ids.len()).rev() {
        let j = data.rng.random_int(0, i as i64) as usize;
        c_ids.swap(i, j);
    }

    let mut order_stmt = conn.prepare(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut ol_stmt = conn.prepare(
        "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();
    let mut no_stmt = conn.prepare(
        "INSERT INTO new_order VALUES (?, ?, ?)"
    ).unwrap();

    for o_id in 1..=TPCCData::ORDERS_PER_DISTRICT {
        let c_id = c_ids[(o_id - 1) as usize];
        let order = data.gen_order(o_id, d_id, w_id, c_id);

        order_stmt.execute(duckdb::params![
            order.o_id, order.o_d_id, order.o_w_id, order.o_c_id,
            order.o_entry_d, order.o_carrier_id, order.o_ol_cnt, order.o_all_local
        ]).unwrap();

        let delivered = o_id <= 2100;
        for ol_number in 1..=order.o_ol_cnt {
            let ol = data.gen_order_line(o_id, d_id, w_id, ol_number, delivered);
            ol_stmt.execute(duckdb::params![
                ol.ol_o_id, ol.ol_d_id, ol.ol_w_id, ol.ol_number,
                ol.ol_i_id, ol.ol_supply_w_id, ol.ol_delivery_d,
                ol.ol_quantity, ol.ol_amount, ol.ol_dist_info
            ]).unwrap();
        }

        if o_id > 2100 {
            let no = data.gen_new_order(o_id, d_id, w_id);
            no_stmt.execute(duckdb::params![no.no_o_id, no.no_d_id, no.no_w_id]).unwrap();
        }
    }
}
