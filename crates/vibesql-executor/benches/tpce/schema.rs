//! TPC-E Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-E
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).
//!
//! TPC-E has 33 tables organized into 4 categories:
//! - Fixed Tables (9): Reference data that doesn't change
//! - Customer Tables (8): Scale with customer count
//! - Market Tables (8): Securities and market data
//! - Trade Tables (8): Grow during benchmark run

use super::data::TPCEData;
use vibesql_storage::Database as VibeDB;

#[cfg(feature = "duckdb-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "sqlite-comparison")]
use rusqlite::Connection as SqliteConn;

// =============================================================================
// Database Loaders
// =============================================================================

/// Load TPC-E database into VibeSQL with specified scale factor
pub fn load_vibesql(scale_factor: f64) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = TPCEData::new(scale_factor);

    // Enable case-insensitive identifier lookups
    db.catalog.set_case_sensitive_identifiers(false);

    // Create schema (all 33 tables)
    create_tpce_schema_vibesql(&mut db);

    // Create indexes before loading data
    create_tpce_indexes_vibesql(&mut db);

    // Load fixed tables
    load_fixed_tables_vibesql(&mut db, &mut data);

    // Load customer-related tables
    load_customer_tables_vibesql(&mut db, &mut data);

    // Load market tables
    load_market_tables_vibesql(&mut db, &mut data);

    // Load trade tables
    load_trade_tables_vibesql(&mut db, &mut data);

    // Compute statistics for query optimization
    let tables = [
        "charge",
        "commission_rate",
        "exchange",
        "industry",
        "sector",
        "status_type",
        "taxrate",
        "trade_type",
        "zip_code",
        "customer",
        "customer_account",
        "customer_taxrate",
        "account_permission",
        "watch_list",
        "watch_item",
        "address",
        "broker",
        "company",
        "company_competitor",
        "security",
        "daily_market",
        "last_trade",
        "financial",
        "news_item",
        "news_xref",
        "trade",
        "trade_history",
        "trade_request",
        "settlement",
        "cash_transaction",
        "holding",
        "holding_history",
        "holding_summary",
    ];
    for table_name in tables {
        if let Some(table) = db.get_table_mut(table_name) {
            table.analyze();
        }
    }

    db
}

#[cfg(feature = "sqlite-comparison")]
pub fn load_sqlite(scale_factor: f64) -> SqliteConn {
    let conn = SqliteConn::open_in_memory().unwrap();
    let mut data = TPCEData::new(scale_factor);

    create_tpce_schema_sqlite(&conn);
    load_fixed_tables_sqlite(&conn, &mut data);
    load_customer_tables_sqlite(&conn, &mut data);
    load_market_tables_sqlite(&conn, &mut data);
    load_trade_tables_sqlite(&conn, &mut data);
    create_tpce_indexes_sqlite(&conn);

    conn
}

#[cfg(feature = "duckdb-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCEData::new(scale_factor);

    create_tpce_schema_duckdb(&conn);
    load_fixed_tables_duckdb(&conn, &mut data);
    load_customer_tables_duckdb(&conn, &mut data);
    load_market_tables_duckdb(&conn, &mut data);
    load_trade_tables_duckdb(&conn, &mut data);
    create_tpce_indexes_duckdb(&conn);

    conn
}

// =============================================================================
// VibeSQL Schema Creation
// =============================================================================

fn create_tpce_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    let varchar = || DataType::Varchar { max_length: None };
    let decimal = || DataType::Decimal { precision: 15, scale: 2 };

    // =========================================================================
    // Fixed Tables (9)
    // =========================================================================

    // CHARGE table
    db.create_table(TableSchema::new(
        "charge".to_string(),
        vec![
            ColumnSchema::new("ch_tt_id".to_string(), varchar(), false),
            ColumnSchema::new("ch_c_tier".to_string(), DataType::Integer, false),
            ColumnSchema::new("ch_chrg".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // COMMISSION_RATE table
    db.create_table(TableSchema::new(
        "commission_rate".to_string(),
        vec![
            ColumnSchema::new("cr_c_tier".to_string(), DataType::Integer, false),
            ColumnSchema::new("cr_tt_id".to_string(), varchar(), false),
            ColumnSchema::new("cr_ex_id".to_string(), varchar(), false),
            ColumnSchema::new("cr_from_qty".to_string(), DataType::Integer, false),
            ColumnSchema::new("cr_to_qty".to_string(), DataType::Integer, false),
            ColumnSchema::new("cr_rate".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // EXCHANGE table
    db.create_table(TableSchema::new(
        "exchange".to_string(),
        vec![
            ColumnSchema::new("ex_id".to_string(), varchar(), false),
            ColumnSchema::new("ex_name".to_string(), varchar(), false),
            ColumnSchema::new("ex_num_symb".to_string(), DataType::Integer, false),
            ColumnSchema::new("ex_open".to_string(), DataType::Integer, false),
            ColumnSchema::new("ex_close".to_string(), DataType::Integer, false),
            ColumnSchema::new("ex_desc".to_string(), varchar(), false),
            ColumnSchema::new("ex_ad_id".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // INDUSTRY table
    db.create_table(TableSchema::new(
        "industry".to_string(),
        vec![
            ColumnSchema::new("in_id".to_string(), varchar(), false),
            ColumnSchema::new("in_name".to_string(), varchar(), false),
            ColumnSchema::new("in_sc_id".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // SECTOR table
    db.create_table(TableSchema::new(
        "sector".to_string(),
        vec![
            ColumnSchema::new("sc_id".to_string(), varchar(), false),
            ColumnSchema::new("sc_name".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // STATUS_TYPE table
    db.create_table(TableSchema::new(
        "status_type".to_string(),
        vec![
            ColumnSchema::new("st_id".to_string(), varchar(), false),
            ColumnSchema::new("st_name".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // TAXRATE table
    db.create_table(TableSchema::new(
        "taxrate".to_string(),
        vec![
            ColumnSchema::new("tx_id".to_string(), varchar(), false),
            ColumnSchema::new("tx_name".to_string(), varchar(), false),
            ColumnSchema::new("tx_rate".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // TRADE_TYPE table
    db.create_table(TableSchema::new(
        "trade_type".to_string(),
        vec![
            ColumnSchema::new("tt_id".to_string(), varchar(), false),
            ColumnSchema::new("tt_name".to_string(), varchar(), false),
            ColumnSchema::new("tt_is_sell".to_string(), DataType::Boolean, false),
            ColumnSchema::new("tt_is_mrkt".to_string(), DataType::Boolean, false),
        ],
    ))
    .unwrap();

    // ZIP_CODE table
    db.create_table(TableSchema::new(
        "zip_code".to_string(),
        vec![
            ColumnSchema::new("zc_code".to_string(), varchar(), false),
            ColumnSchema::new("zc_town".to_string(), varchar(), false),
            ColumnSchema::new("zc_div".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // =========================================================================
    // Customer Tables (8)
    // =========================================================================

    // ADDRESS table
    db.create_table(TableSchema::new(
        "address".to_string(),
        vec![
            ColumnSchema::new("ad_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ad_line1".to_string(), varchar(), false),
            ColumnSchema::new("ad_line2".to_string(), varchar(), true),
            ColumnSchema::new("ad_zc_code".to_string(), varchar(), false),
            ColumnSchema::new("ad_ctry".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::new(
        "customer".to_string(),
        vec![
            ColumnSchema::new("c_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("c_tax_id".to_string(), varchar(), false),
            ColumnSchema::new("c_st_id".to_string(), varchar(), false),
            ColumnSchema::new("c_l_name".to_string(), varchar(), false),
            ColumnSchema::new("c_f_name".to_string(), varchar(), false),
            ColumnSchema::new("c_m_name".to_string(), varchar(), true),
            ColumnSchema::new("c_gndr".to_string(), varchar(), true),
            ColumnSchema::new("c_tier".to_string(), DataType::Integer, false),
            ColumnSchema::new("c_dob".to_string(), varchar(), false),
            ColumnSchema::new("c_ad_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("c_ctry_1".to_string(), varchar(), true),
            ColumnSchema::new("c_area_1".to_string(), varchar(), true),
            ColumnSchema::new("c_local_1".to_string(), varchar(), true),
            ColumnSchema::new("c_ext_1".to_string(), varchar(), true),
            ColumnSchema::new("c_ctry_2".to_string(), varchar(), true),
            ColumnSchema::new("c_area_2".to_string(), varchar(), true),
            ColumnSchema::new("c_local_2".to_string(), varchar(), true),
            ColumnSchema::new("c_ext_2".to_string(), varchar(), true),
            ColumnSchema::new("c_ctry_3".to_string(), varchar(), true),
            ColumnSchema::new("c_area_3".to_string(), varchar(), true),
            ColumnSchema::new("c_local_3".to_string(), varchar(), true),
            ColumnSchema::new("c_ext_3".to_string(), varchar(), true),
            ColumnSchema::new("c_email_1".to_string(), varchar(), true),
            ColumnSchema::new("c_email_2".to_string(), varchar(), true),
        ],
    ))
    .unwrap();

    // CUSTOMER_ACCOUNT table
    db.create_table(TableSchema::new(
        "customer_account".to_string(),
        vec![
            ColumnSchema::new("ca_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ca_b_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ca_c_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ca_name".to_string(), varchar(), true),
            ColumnSchema::new("ca_tax_st".to_string(), DataType::Integer, false),
            ColumnSchema::new("ca_bal".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // CUSTOMER_TAXRATE table
    db.create_table(TableSchema::new(
        "customer_taxrate".to_string(),
        vec![
            ColumnSchema::new("cx_tx_id".to_string(), varchar(), false),
            ColumnSchema::new("cx_c_id".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // ACCOUNT_PERMISSION table
    db.create_table(TableSchema::new(
        "account_permission".to_string(),
        vec![
            ColumnSchema::new("ap_ca_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ap_acl".to_string(), varchar(), false),
            ColumnSchema::new("ap_tax_id".to_string(), varchar(), false),
            ColumnSchema::new("ap_l_name".to_string(), varchar(), false),
            ColumnSchema::new("ap_f_name".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // WATCH_LIST table
    db.create_table(TableSchema::new(
        "watch_list".to_string(),
        vec![
            ColumnSchema::new("wl_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("wl_c_id".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // WATCH_ITEM table
    db.create_table(TableSchema::new(
        "watch_item".to_string(),
        vec![
            ColumnSchema::new("wi_wl_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("wi_s_symb".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // BROKER table
    db.create_table(TableSchema::new(
        "broker".to_string(),
        vec![
            ColumnSchema::new("b_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("b_st_id".to_string(), varchar(), false),
            ColumnSchema::new("b_name".to_string(), varchar(), false),
            ColumnSchema::new("b_num_trades".to_string(), DataType::Integer, false),
            ColumnSchema::new("b_comm_total".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // =========================================================================
    // Market Tables (8)
    // =========================================================================

    // COMPANY table
    db.create_table(TableSchema::new(
        "company".to_string(),
        vec![
            ColumnSchema::new("co_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("co_st_id".to_string(), varchar(), false),
            ColumnSchema::new("co_name".to_string(), varchar(), false),
            ColumnSchema::new("co_in_id".to_string(), varchar(), false),
            ColumnSchema::new("co_sp_rate".to_string(), varchar(), true),
            ColumnSchema::new("co_ceo".to_string(), varchar(), true),
            ColumnSchema::new("co_ad_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("co_desc".to_string(), varchar(), true),
            ColumnSchema::new("co_open_date".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // COMPANY_COMPETITOR table
    db.create_table(TableSchema::new(
        "company_competitor".to_string(),
        vec![
            ColumnSchema::new("cp_co_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("cp_comp_co_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("cp_in_id".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // SECURITY table
    db.create_table(TableSchema::new(
        "security".to_string(),
        vec![
            ColumnSchema::new("s_symb".to_string(), varchar(), false),
            ColumnSchema::new("s_issue".to_string(), varchar(), false),
            ColumnSchema::new("s_st_id".to_string(), varchar(), false),
            ColumnSchema::new("s_name".to_string(), varchar(), false),
            ColumnSchema::new("s_ex_id".to_string(), varchar(), false),
            ColumnSchema::new("s_co_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("s_num_out".to_string(), DataType::Bigint, false),
            ColumnSchema::new("s_start_date".to_string(), varchar(), false),
            ColumnSchema::new("s_exch_date".to_string(), varchar(), false),
            ColumnSchema::new("s_pe".to_string(), decimal(), true),
            ColumnSchema::new("s_52wk_high".to_string(), decimal(), false),
            ColumnSchema::new("s_52wk_high_date".to_string(), varchar(), false),
            ColumnSchema::new("s_52wk_low".to_string(), decimal(), false),
            ColumnSchema::new("s_52wk_low_date".to_string(), varchar(), false),
            ColumnSchema::new("s_dividend".to_string(), decimal(), false),
            ColumnSchema::new("s_yield".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // DAILY_MARKET table
    db.create_table(TableSchema::new(
        "daily_market".to_string(),
        vec![
            ColumnSchema::new("dm_date".to_string(), varchar(), false),
            ColumnSchema::new("dm_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("dm_close".to_string(), decimal(), false),
            ColumnSchema::new("dm_high".to_string(), decimal(), false),
            ColumnSchema::new("dm_low".to_string(), decimal(), false),
            ColumnSchema::new("dm_vol".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // LAST_TRADE table
    db.create_table(TableSchema::new(
        "last_trade".to_string(),
        vec![
            ColumnSchema::new("lt_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("lt_dts".to_string(), varchar(), false),
            ColumnSchema::new("lt_price".to_string(), decimal(), false),
            ColumnSchema::new("lt_open_price".to_string(), decimal(), false),
            ColumnSchema::new("lt_vol".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // FINANCIAL table
    db.create_table(TableSchema::new(
        "financial".to_string(),
        vec![
            ColumnSchema::new("fi_co_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("fi_year".to_string(), DataType::Integer, false),
            ColumnSchema::new("fi_qtr".to_string(), DataType::Integer, false),
            ColumnSchema::new("fi_qtr_start_date".to_string(), varchar(), false),
            ColumnSchema::new("fi_revenue".to_string(), decimal(), false),
            ColumnSchema::new("fi_net_earn".to_string(), decimal(), false),
            ColumnSchema::new("fi_basic_eps".to_string(), decimal(), false),
            ColumnSchema::new("fi_dilut_eps".to_string(), decimal(), false),
            ColumnSchema::new("fi_margin".to_string(), decimal(), false),
            ColumnSchema::new("fi_inventory".to_string(), decimal(), false),
            ColumnSchema::new("fi_assets".to_string(), decimal(), false),
            ColumnSchema::new("fi_liability".to_string(), decimal(), false),
            ColumnSchema::new("fi_out_basic".to_string(), DataType::Bigint, false),
            ColumnSchema::new("fi_out_dilut".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // NEWS_ITEM table
    db.create_table(TableSchema::new(
        "news_item".to_string(),
        vec![
            ColumnSchema::new("ni_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ni_headline".to_string(), varchar(), false),
            ColumnSchema::new("ni_summary".to_string(), varchar(), false),
            ColumnSchema::new("ni_item".to_string(), varchar(), false),
            ColumnSchema::new("ni_dts".to_string(), varchar(), false),
            ColumnSchema::new("ni_source".to_string(), varchar(), false),
            ColumnSchema::new("ni_author".to_string(), varchar(), true),
        ],
    ))
    .unwrap();

    // NEWS_XREF table
    db.create_table(TableSchema::new(
        "news_xref".to_string(),
        vec![
            ColumnSchema::new("nx_ni_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("nx_co_id".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // =========================================================================
    // Trade Tables (8)
    // =========================================================================

    // TRADE table
    db.create_table(TableSchema::new(
        "trade".to_string(),
        vec![
            ColumnSchema::new("t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("t_dts".to_string(), varchar(), false),
            ColumnSchema::new("t_st_id".to_string(), varchar(), false),
            ColumnSchema::new("t_tt_id".to_string(), varchar(), false),
            ColumnSchema::new("t_is_cash".to_string(), DataType::Boolean, false),
            ColumnSchema::new("t_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("t_qty".to_string(), DataType::Integer, false),
            ColumnSchema::new("t_bid_price".to_string(), decimal(), false),
            ColumnSchema::new("t_ca_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("t_exec_name".to_string(), varchar(), false),
            ColumnSchema::new("t_trade_price".to_string(), decimal(), true),
            ColumnSchema::new("t_chrg".to_string(), decimal(), false),
            ColumnSchema::new("t_comm".to_string(), decimal(), false),
            ColumnSchema::new("t_tax".to_string(), decimal(), false),
            ColumnSchema::new("t_lifo".to_string(), DataType::Boolean, false),
        ],
    ))
    .unwrap();

    // TRADE_HISTORY table
    db.create_table(TableSchema::new(
        "trade_history".to_string(),
        vec![
            ColumnSchema::new("th_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("th_dts".to_string(), varchar(), false),
            ColumnSchema::new("th_st_id".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // TRADE_REQUEST table
    db.create_table(TableSchema::new(
        "trade_request".to_string(),
        vec![
            ColumnSchema::new("tr_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("tr_tt_id".to_string(), varchar(), false),
            ColumnSchema::new("tr_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("tr_qty".to_string(), DataType::Integer, false),
            ColumnSchema::new("tr_bid_price".to_string(), decimal(), false),
            ColumnSchema::new("tr_b_id".to_string(), DataType::Bigint, false),
        ],
    ))
    .unwrap();

    // SETTLEMENT table
    db.create_table(TableSchema::new(
        "settlement".to_string(),
        vec![
            ColumnSchema::new("se_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("se_cash_type".to_string(), varchar(), false),
            ColumnSchema::new("se_cash_due_date".to_string(), varchar(), false),
            ColumnSchema::new("se_amt".to_string(), decimal(), false),
        ],
    ))
    .unwrap();

    // CASH_TRANSACTION table
    db.create_table(TableSchema::new(
        "cash_transaction".to_string(),
        vec![
            ColumnSchema::new("ct_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("ct_dts".to_string(), varchar(), false),
            ColumnSchema::new("ct_amt".to_string(), decimal(), false),
            ColumnSchema::new("ct_name".to_string(), varchar(), false),
        ],
    ))
    .unwrap();

    // HOLDING table
    db.create_table(TableSchema::new(
        "holding".to_string(),
        vec![
            ColumnSchema::new("h_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("h_ca_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("h_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("h_dts".to_string(), varchar(), false),
            ColumnSchema::new("h_price".to_string(), decimal(), false),
            ColumnSchema::new("h_qty".to_string(), DataType::Integer, false),
        ],
    ))
    .unwrap();

    // HOLDING_HISTORY table
    db.create_table(TableSchema::new(
        "holding_history".to_string(),
        vec![
            ColumnSchema::new("hh_h_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("hh_t_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("hh_before_qty".to_string(), DataType::Integer, false),
            ColumnSchema::new("hh_after_qty".to_string(), DataType::Integer, false),
        ],
    ))
    .unwrap();

    // HOLDING_SUMMARY table
    db.create_table(TableSchema::new(
        "holding_summary".to_string(),
        vec![
            ColumnSchema::new("hs_ca_id".to_string(), DataType::Bigint, false),
            ColumnSchema::new("hs_s_symb".to_string(), varchar(), false),
            ColumnSchema::new("hs_qty".to_string(), DataType::Integer, false),
        ],
    ))
    .unwrap();
}

// =============================================================================
// VibeSQL Index Creation
// =============================================================================

fn create_tpce_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    fn col(name: &str) -> IndexColumn {
        IndexColumn {
            column_name: name.to_uppercase(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }
    }

    // Primary key indexes
    db.create_index(
        "idx_exchange_pk".to_string(),
        "exchange".to_string(),
        true,
        vec![col("ex_id")],
    )
    .ok();
    db.create_index("idx_sector_pk".to_string(), "sector".to_string(), true, vec![col("sc_id")])
        .ok();
    db.create_index(
        "idx_industry_pk".to_string(),
        "industry".to_string(),
        true,
        vec![col("in_id")],
    )
    .ok();
    db.create_index(
        "idx_status_type_pk".to_string(),
        "status_type".to_string(),
        true,
        vec![col("st_id")],
    )
    .ok();
    db.create_index(
        "idx_trade_type_pk".to_string(),
        "trade_type".to_string(),
        true,
        vec![col("tt_id")],
    )
    .ok();
    db.create_index("idx_taxrate_pk".to_string(), "taxrate".to_string(), true, vec![col("tx_id")])
        .ok();
    db.create_index(
        "idx_zip_code_pk".to_string(),
        "zip_code".to_string(),
        true,
        vec![col("zc_code")],
    )
    .ok();
    db.create_index("idx_address_pk".to_string(), "address".to_string(), true, vec![col("ad_id")])
        .ok();
    db.create_index("idx_customer_pk".to_string(), "customer".to_string(), true, vec![col("c_id")])
        .ok();
    db.create_index(
        "idx_customer_account_pk".to_string(),
        "customer_account".to_string(),
        true,
        vec![col("ca_id")],
    )
    .ok();
    db.create_index("idx_broker_pk".to_string(), "broker".to_string(), true, vec![col("b_id")])
        .ok();
    db.create_index("idx_company_pk".to_string(), "company".to_string(), true, vec![col("co_id")])
        .ok();
    db.create_index(
        "idx_security_pk".to_string(),
        "security".to_string(),
        true,
        vec![col("s_symb")],
    )
    .ok();
    db.create_index(
        "idx_last_trade_pk".to_string(),
        "last_trade".to_string(),
        true,
        vec![col("lt_s_symb")],
    )
    .ok();
    db.create_index("idx_trade_pk".to_string(), "trade".to_string(), true, vec![col("t_id")]).ok();
    db.create_index("idx_holding_pk".to_string(), "holding".to_string(), true, vec![col("h_t_id")])
        .ok();
    db.create_index(
        "idx_settlement_pk".to_string(),
        "settlement".to_string(),
        true,
        vec![col("se_t_id")],
    )
    .ok();

    // Secondary indexes for common query patterns
    db.create_index(
        "idx_customer_account_c_id".to_string(),
        "customer_account".to_string(),
        false,
        vec![col("ca_c_id")],
    )
    .ok();
    db.create_index(
        "idx_customer_account_b_id".to_string(),
        "customer_account".to_string(),
        false,
        vec![col("ca_b_id")],
    )
    .ok();
    db.create_index(
        "idx_trade_ca_id".to_string(),
        "trade".to_string(),
        false,
        vec![col("t_ca_id")],
    )
    .ok();
    db.create_index(
        "idx_trade_s_symb".to_string(),
        "trade".to_string(),
        false,
        vec![col("t_s_symb")],
    )
    .ok();
    db.create_index(
        "idx_holding_ca_id".to_string(),
        "holding".to_string(),
        false,
        vec![col("h_ca_id"), col("h_s_symb")],
    )
    .ok();
    db.create_index(
        "idx_holding_summary_pk".to_string(),
        "holding_summary".to_string(),
        true,
        vec![col("hs_ca_id"), col("hs_s_symb")],
    )
    .ok();
    db.create_index(
        "idx_security_co_id".to_string(),
        "security".to_string(),
        false,
        vec![col("s_co_id")],
    )
    .ok();
    db.create_index(
        "idx_company_in_id".to_string(),
        "company".to_string(),
        false,
        vec![col("co_in_id")],
    )
    .ok();
    db.create_index(
        "idx_watch_list_c_id".to_string(),
        "watch_list".to_string(),
        false,
        vec![col("wl_c_id")],
    )
    .ok();
    db.create_index(
        "idx_watch_item_wl_id".to_string(),
        "watch_item".to_string(),
        false,
        vec![col("wi_wl_id")],
    )
    .ok();
}

// =============================================================================
// VibeSQL Data Loading
// =============================================================================

fn load_fixed_tables_vibesql(db: &mut VibeDB, data: &mut TPCEData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Load exchanges
    for ex in data.gen_exchanges() {
        db.insert_row(
            "exchange",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(ex.ex_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(ex.ex_name)),
                SqlValue::Integer(ex.ex_num_symb as i64),
                SqlValue::Integer(ex.ex_open as i64),
                SqlValue::Integer(ex.ex_close as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(ex.ex_desc)),
                SqlValue::Bigint(ex.ex_ad_id),
            ]),
        )
        .unwrap();
    }

    // Load sectors
    for sc in data.gen_sectors() {
        db.insert_row(
            "sector",
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(sc.sc_id)), SqlValue::Varchar(arcstr::ArcStr::from(sc.sc_name))]),
        )
        .unwrap();
    }

    // Load industries
    for ind in data.gen_industries() {
        db.insert_row(
            "industry",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(ind.in_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(ind.in_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(ind.in_sc_id)),
            ]),
        )
        .unwrap();
    }

    // Load status types
    for st in data.gen_status_types() {
        db.insert_row(
            "status_type",
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from(st.st_id)), SqlValue::Varchar(arcstr::ArcStr::from(st.st_name))]),
        )
        .unwrap();
    }

    // Load trade types
    for tt in data.gen_trade_types() {
        db.insert_row(
            "trade_type",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(tt.tt_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(tt.tt_name)),
                SqlValue::Boolean(tt.tt_is_sell),
                SqlValue::Boolean(tt.tt_is_mrkt),
            ]),
        )
        .unwrap();
    }

    // Load charges
    for ch in data.gen_charges() {
        db.insert_row(
            "charge",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(ch.ch_tt_id)),
                SqlValue::Integer(ch.ch_c_tier as i64),
                SqlValue::Numeric(ch.ch_chrg),
            ]),
        )
        .unwrap();
    }

    // Load tax rates
    for tx in data.gen_taxrates() {
        db.insert_row(
            "taxrate",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(tx.tx_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(tx.tx_name)),
                SqlValue::Numeric(tx.tx_rate),
            ]),
        )
        .unwrap();
    }

    // Load ZIP codes
    for zc in data.gen_zip_codes() {
        db.insert_row(
            "zip_code",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(zc.zc_code)),
                SqlValue::Varchar(arcstr::ArcStr::from(zc.zc_town)),
                SqlValue::Varchar(arcstr::ArcStr::from(zc.zc_div)),
            ]),
        )
        .unwrap();
    }
}

fn load_customer_tables_vibesql(db: &mut VibeDB, data: &mut TPCEData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_customers = data.num_customers();
    let num_brokers = data.num_brokers();

    // Load brokers first
    for b_id in 1..=num_brokers as i64 {
        let broker = data.gen_broker(b_id);
        db.insert_row(
            "broker",
            Row::new(vec![
                SqlValue::Bigint(broker.b_id),
                SqlValue::Varchar(arcstr::ArcStr::from(broker.b_st_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(broker.b_name)),
                SqlValue::Integer(broker.b_num_trades as i64),
                SqlValue::Numeric(broker.b_comm_total),
            ]),
        )
        .unwrap();
    }

    // Track symbols for watch items
    let symbols: Vec<String> =
        (0..data.num_securities().min(100)).map(|i| data.gen_symbol(i)).collect();

    // Load customers and related data
    let mut account_id = 1_i64;
    let mut watch_list_id = 1_i64;

    for c_id in 1..=num_customers as i64 {
        // Customer address
        let addr = data.gen_address();
        db.insert_row(
            "address",
            Row::new(vec![
                SqlValue::Bigint(addr.ad_id),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_line1)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_line2)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_zc_code)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_ctry)),
            ]),
        )
        .unwrap();

        // Customer
        let cust = data.gen_customer(c_id);
        db.insert_row(
            "customer",
            Row::new(vec![
                SqlValue::Bigint(cust.c_id),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_tax_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_st_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_l_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_f_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_m_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_gndr)),
                SqlValue::Integer(cust.c_tier as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_dob)),
                SqlValue::Bigint(cust.c_ad_id),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ctry_1)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_area_1)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_local_1)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ext_1)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ctry_2)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_area_2)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_local_2)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ext_2)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ctry_3)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_area_3)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_local_3)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_ext_3)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_email_1)),
                SqlValue::Varchar(arcstr::ArcStr::from(cust.c_email_2)),
            ]),
        )
        .unwrap();

        // Customer accounts (5 per customer)
        for _ in 0..TPCEData::ACCOUNTS_PER_CUSTOMER {
            let b_id = (account_id % num_brokers as i64) + 1;
            let acct = data.gen_customer_account(account_id, c_id, b_id);
            db.insert_row(
                "customer_account",
                Row::new(vec![
                    SqlValue::Bigint(acct.ca_id),
                    SqlValue::Bigint(acct.ca_b_id),
                    SqlValue::Bigint(acct.ca_c_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(acct.ca_name)),
                    SqlValue::Integer(acct.ca_tax_st as i64),
                    SqlValue::Numeric(acct.ca_bal),
                ]),
            )
            .unwrap();
            account_id += 1;
        }

        // Watch list
        let wl = data.gen_watch_list(watch_list_id, c_id);
        db.insert_row(
            "watch_list",
            Row::new(vec![SqlValue::Bigint(wl.wl_id), SqlValue::Bigint(wl.wl_c_id)]),
        )
        .unwrap();

        // Watch items (random securities)
        let num_items = data.rng.random_int(5, 20) as usize;
        for symbol in symbols.iter().take(num_items) {
            db.insert_row(
                "watch_item",
                Row::new(vec![SqlValue::Bigint(watch_list_id), SqlValue::Varchar(arcstr::ArcStr::from(symbol.clone()))]),
            )
            .unwrap();
        }
        watch_list_id += 1;
    }
}

fn load_market_tables_vibesql(db: &mut VibeDB, data: &mut TPCEData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_companies = data.num_companies();
    let num_securities = data.num_securities();

    // Load companies
    for co_id in 1..=num_companies as i64 {
        // Company address
        let addr = data.gen_address();
        db.insert_row(
            "address",
            Row::new(vec![
                SqlValue::Bigint(addr.ad_id),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_line1)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_line2)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_zc_code)),
                SqlValue::Varchar(arcstr::ArcStr::from(addr.ad_ctry)),
            ]),
        )
        .unwrap();

        let company = data.gen_company(co_id);
        db.insert_row(
            "company",
            Row::new(vec![
                SqlValue::Bigint(company.co_id),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_st_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_in_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_sp_rate)),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_ceo)),
                SqlValue::Bigint(company.co_ad_id),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_desc)),
                SqlValue::Varchar(arcstr::ArcStr::from(company.co_open_date)),
            ]),
        )
        .unwrap();

        // Financial data (last 4 quarters)
        for year in [2023, 2024] {
            for qtr in 1..=4 {
                let fin = data.gen_financial(co_id, year, qtr);
                db.insert_row(
                    "financial",
                    Row::new(vec![
                        SqlValue::Bigint(fin.fi_co_id),
                        SqlValue::Integer(fin.fi_year as i64),
                        SqlValue::Integer(fin.fi_qtr as i64),
                        SqlValue::Varchar(arcstr::ArcStr::from(fin.fi_qtr_start_date)),
                        SqlValue::Numeric(fin.fi_revenue),
                        SqlValue::Numeric(fin.fi_net_earn),
                        SqlValue::Numeric(fin.fi_basic_eps),
                        SqlValue::Numeric(fin.fi_dilut_eps),
                        SqlValue::Numeric(fin.fi_margin),
                        SqlValue::Numeric(fin.fi_inventory),
                        SqlValue::Numeric(fin.fi_assets),
                        SqlValue::Numeric(fin.fi_liability),
                        SqlValue::Bigint(fin.fi_out_basic),
                        SqlValue::Bigint(fin.fi_out_dilut),
                    ]),
                )
                .unwrap();
            }
        }
    }

    // Load securities
    for i in 0..num_securities {
        let symb = data.gen_symbol(i);
        let co_id = ((i % num_companies) + 1) as i64;
        let sec = data.gen_security(&symb, co_id);

        db.insert_row(
            "security",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_symb.clone())),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_issue)),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_st_id)),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_name)),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_ex_id)),
                SqlValue::Bigint(sec.s_co_id),
                SqlValue::Bigint(sec.s_num_out),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_start_date)),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_exch_date)),
                SqlValue::Numeric(sec.s_pe),
                SqlValue::Numeric(sec.s_52wk_high),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_52wk_high_date)),
                SqlValue::Numeric(sec.s_52wk_low),
                SqlValue::Varchar(arcstr::ArcStr::from(sec.s_52wk_low_date)),
                SqlValue::Numeric(sec.s_dividend),
                SqlValue::Numeric(sec.s_yield),
            ]),
        )
        .unwrap();

        // Last trade
        let lt = data.gen_last_trade(&symb);
        db.insert_row(
            "last_trade",
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(lt.lt_s_symb)),
                SqlValue::Varchar(arcstr::ArcStr::from(lt.lt_dts)),
                SqlValue::Numeric(lt.lt_price),
                SqlValue::Numeric(lt.lt_open_price),
                SqlValue::Bigint(lt.lt_vol),
            ]),
        )
        .unwrap();
    }
}

fn load_trade_tables_vibesql(db: &mut VibeDB, data: &mut TPCEData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_accounts = data.num_accounts();
    let num_securities = data.num_securities();
    let trades_per_account = if data.is_micro_mode() { 5 } else { 10 };
    let holdings_per_account = if data.is_micro_mode() { 3 } else { 5 };

    // Generate trades and holdings for each account
    for ca_id in 1..=num_accounts as i64 {
        // Holdings
        for h in 0..holdings_per_account {
            let s_idx = (ca_id as i32 + h) % num_securities;
            let symb = data.gen_symbol(s_idx);
            let holding = data.gen_holding(ca_id, &symb);

            db.insert_row(
                "holding",
                Row::new(vec![
                    SqlValue::Bigint(holding.h_t_id),
                    SqlValue::Bigint(holding.h_ca_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(holding.h_s_symb.clone())),
                    SqlValue::Varchar(arcstr::ArcStr::from(holding.h_dts)),
                    SqlValue::Numeric(holding.h_price),
                    SqlValue::Integer(holding.h_qty as i64),
                ]),
            )
            .unwrap();

            // Holding summary
            let hs = data.gen_holding_summary(ca_id, &symb, holding.h_qty);
            db.insert_row(
                "holding_summary",
                Row::new(vec![
                    SqlValue::Bigint(hs.hs_ca_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(hs.hs_s_symb)),
                    SqlValue::Integer(hs.hs_qty as i64),
                ]),
            )
            .unwrap();
        }

        // Trades
        for _ in 0..trades_per_account {
            let s_idx = (data.rng.random_int(0, num_securities as i64 - 1)) as i32;
            let symb = data.gen_symbol(s_idx);
            let trade = data.gen_trade(ca_id, &symb);

            db.insert_row(
                "trade",
                Row::new(vec![
                    SqlValue::Bigint(trade.t_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(trade.t_dts.clone())),
                    SqlValue::Varchar(arcstr::ArcStr::from(trade.t_st_id.clone())),
                    SqlValue::Varchar(arcstr::ArcStr::from(trade.t_tt_id)),
                    SqlValue::Boolean(trade.t_is_cash),
                    SqlValue::Varchar(arcstr::ArcStr::from(trade.t_s_symb)),
                    SqlValue::Integer(trade.t_qty as i64),
                    SqlValue::Numeric(trade.t_bid_price),
                    SqlValue::Bigint(trade.t_ca_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(trade.t_exec_name)),
                    SqlValue::Numeric(trade.t_trade_price),
                    SqlValue::Numeric(trade.t_chrg),
                    SqlValue::Numeric(trade.t_comm),
                    SqlValue::Numeric(trade.t_tax),
                    SqlValue::Boolean(trade.t_lifo),
                ]),
            )
            .unwrap();

            // Trade history
            let th = data.gen_trade_history(trade.t_id, &trade.t_st_id);
            db.insert_row(
                "trade_history",
                Row::new(vec![
                    SqlValue::Bigint(th.th_t_id),
                    SqlValue::Varchar(arcstr::ArcStr::from(th.th_dts)),
                    SqlValue::Varchar(arcstr::ArcStr::from(th.th_st_id)),
                ]),
            )
            .unwrap();

            // Settlement for completed trades
            if trade.t_st_id == "CMPT" {
                let amount = trade.t_trade_price * trade.t_qty as f64;
                let se = data.gen_settlement(trade.t_id, amount);
                db.insert_row(
                    "settlement",
                    Row::new(vec![
                        SqlValue::Bigint(se.se_t_id),
                        SqlValue::Varchar(arcstr::ArcStr::from(se.se_cash_type)),
                        SqlValue::Varchar(arcstr::ArcStr::from(se.se_cash_due_date)),
                        SqlValue::Numeric(se.se_amt),
                    ]),
                )
                .unwrap();

                // Cash transaction
                if trade.t_is_cash {
                    let ct = data.gen_cash_transaction(trade.t_id, amount);
                    db.insert_row(
                        "cash_transaction",
                        Row::new(vec![
                            SqlValue::Bigint(ct.ct_t_id),
                            SqlValue::Varchar(arcstr::ArcStr::from(ct.ct_dts)),
                            SqlValue::Numeric(ct.ct_amt),
                            SqlValue::Varchar(arcstr::ArcStr::from(ct.ct_name)),
                        ]),
                    )
                    .unwrap();
                }
            }
        }
    }
}

// =============================================================================
// SQLite Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "sqlite-comparison")]
fn create_tpce_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(include_str!("sqlite_schema.sql")).unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn create_tpce_indexes_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_customer_account_c_id ON customer_account (ca_c_id);
        CREATE INDEX IF NOT EXISTS idx_customer_account_b_id ON customer_account (ca_b_id);
        CREATE INDEX IF NOT EXISTS idx_trade_ca_id ON trade (t_ca_id);
        CREATE INDEX IF NOT EXISTS idx_trade_s_symb ON trade (t_s_symb);
        CREATE INDEX IF NOT EXISTS idx_holding_ca_id ON holding (h_ca_id, h_s_symb);
        CREATE INDEX IF NOT EXISTS idx_security_co_id ON security (s_co_id);
        CREATE INDEX IF NOT EXISTS idx_company_in_id ON company (co_in_id);
        CREATE INDEX IF NOT EXISTS idx_watch_list_c_id ON watch_list (wl_c_id);
        CREATE INDEX IF NOT EXISTS idx_watch_item_wl_id ON watch_item (wi_wl_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "sqlite-comparison")]
fn load_fixed_tables_sqlite(conn: &SqliteConn, data: &mut TPCEData) {
    // Load exchanges
    for ex in data.gen_exchanges() {
        conn.execute(
            "INSERT INTO exchange VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                ex.ex_id,
                ex.ex_name,
                ex.ex_num_symb,
                ex.ex_open,
                ex.ex_close,
                ex.ex_desc,
                ex.ex_ad_id
            ],
        )
        .unwrap();
    }

    // Load sectors
    for sc in data.gen_sectors() {
        conn.execute("INSERT INTO sector VALUES (?, ?)", rusqlite::params![sc.sc_id, sc.sc_name])
            .unwrap();
    }

    // Load industries
    for ind in data.gen_industries() {
        conn.execute(
            "INSERT INTO industry VALUES (?, ?, ?)",
            rusqlite::params![ind.in_id, ind.in_name, ind.in_sc_id],
        )
        .unwrap();
    }

    // Load status types
    for st in data.gen_status_types() {
        conn.execute(
            "INSERT INTO status_type VALUES (?, ?)",
            rusqlite::params![st.st_id, st.st_name],
        )
        .unwrap();
    }

    // Load trade types
    for tt in data.gen_trade_types() {
        conn.execute(
            "INSERT INTO trade_type VALUES (?, ?, ?, ?)",
            rusqlite::params![tt.tt_id, tt.tt_name, tt.tt_is_sell, tt.tt_is_mrkt],
        )
        .unwrap();
    }

    // Load charges
    for ch in data.gen_charges() {
        conn.execute(
            "INSERT INTO charge VALUES (?, ?, ?)",
            rusqlite::params![ch.ch_tt_id, ch.ch_c_tier, ch.ch_chrg],
        )
        .unwrap();
    }

    // Load tax rates
    for tx in data.gen_taxrates() {
        conn.execute(
            "INSERT INTO taxrate VALUES (?, ?, ?)",
            rusqlite::params![tx.tx_id, tx.tx_name, tx.tx_rate],
        )
        .unwrap();
    }

    // Load ZIP codes
    for zc in data.gen_zip_codes() {
        conn.execute(
            "INSERT INTO zip_code VALUES (?, ?, ?)",
            rusqlite::params![zc.zc_code, zc.zc_town, zc.zc_div],
        )
        .unwrap();
    }
}

#[cfg(feature = "sqlite-comparison")]
fn load_customer_tables_sqlite(_conn: &SqliteConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

#[cfg(feature = "sqlite-comparison")]
fn load_market_tables_sqlite(_conn: &SqliteConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

#[cfg(feature = "sqlite-comparison")]
fn load_trade_tables_sqlite(_conn: &SqliteConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

// =============================================================================
// DuckDB Schema and Loading (for comparison)
// =============================================================================

#[cfg(feature = "duckdb-comparison")]
fn create_tpce_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(include_str!("duckdb_schema.sql")).unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn create_tpce_indexes_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_customer_account_c_id ON customer_account (ca_c_id);
        CREATE INDEX IF NOT EXISTS idx_customer_account_b_id ON customer_account (ca_b_id);
        CREATE INDEX IF NOT EXISTS idx_trade_ca_id ON trade (t_ca_id);
        CREATE INDEX IF NOT EXISTS idx_trade_s_symb ON trade (t_s_symb);
        CREATE INDEX IF NOT EXISTS idx_holding_ca_id ON holding (h_ca_id, h_s_symb);
        CREATE INDEX IF NOT EXISTS idx_security_co_id ON security (s_co_id);
        CREATE INDEX IF NOT EXISTS idx_company_in_id ON company (co_in_id);
        CREATE INDEX IF NOT EXISTS idx_watch_list_c_id ON watch_list (wl_c_id);
        CREATE INDEX IF NOT EXISTS idx_watch_item_wl_id ON watch_item (wi_wl_id);
        ",
    )
    .unwrap();
}

#[cfg(feature = "duckdb-comparison")]
fn load_fixed_tables_duckdb(_conn: &DuckDBConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

#[cfg(feature = "duckdb-comparison")]
fn load_customer_tables_duckdb(_conn: &DuckDBConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

#[cfg(feature = "duckdb-comparison")]
fn load_market_tables_duckdb(_conn: &DuckDBConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}

#[cfg(feature = "duckdb-comparison")]
fn load_trade_tables_duckdb(_conn: &DuckDBConn, _data: &mut TPCEData) {
    // Simplified - full implementation would mirror VibeSQL version
}
