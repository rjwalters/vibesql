//! TPC-DS Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-DS
//! benchmark tables.
//!
//! ## Phase 1 Tables (Core):
//!
//! Dimension Tables:
//! - date_dim: Date dimension with calendar attributes
//! - time_dim: Time dimension with time-of-day attributes
//! - item: Product/item dimension
//! - customer: Customer dimension
//! - customer_address: Customer address dimension
//! - store: Store dimension
//!
//! Fact Tables:
//! - store_sales: Store sales transactions
//!
//! ## Phase 2 Tables:
//!
//! Dimension Tables:
//! - promotion: Promotion campaigns
//! - warehouse: Warehouse locations
//! - ship_mode: Shipping methods
//! - reason: Return reasons
//!
//! Fact Tables:
//! - store_returns: Store return transactions
//!
//! ## Phase 3 Tables (Full E-Commerce):
//!
//! Dimension Tables:
//! - catalog_page: Catalog page information
//! - web_page: Web page information
//! - web_site: Web site information
//!
//! Fact Tables:
//! - catalog_sales: Catalog sales transactions
//! - catalog_returns: Catalog return transactions
//! - web_sales: Web sales transactions
//! - web_returns: Web return transactions
//!
//! ## Phase 4 Tables (Demographics & Inventory):
//!
//! Dimension Tables:
//! - customer_demographics: Customer demographic attributes
//! - household_demographics: Household demographic attributes
//! - income_band: Income range buckets
//! - call_center: Call center information
//!
//! Fact Tables:
//! - inventory: Inventory levels

use super::data::{
    TPCDSData, BRANDS, BUY_POTENTIALS, CATEGORIES, CLASSES, CREDIT_RATINGS, DEP_COUNTS,
    EDUCATION_STATUS, EMP_COUNTS, GENDERS, INCOME_BANDS, ITEM_COLORS, ITEM_SIZES, MARITAL_STATUS,
    STATES, VEHICLE_COUNTS,
};
use vibesql_storage::Database as VibeDB;
use vibesql_types::Date;

#[cfg(feature = "benchmark-comparison")]
use duckdb::Connection as DuckDBConn;
#[cfg(feature = "benchmark-comparison")]
use rusqlite::Connection as SqliteConn;

use std::str::FromStr;

// =============================================================================
// Database Loaders
// =============================================================================

pub fn load_vibesql(scale_factor: f64) -> VibeDB {
    let mut db = VibeDB::new();
    let mut data = TPCDSData::new(scale_factor);

    // Create schema
    create_tpcds_schema_vibesql(&mut db);

    // Load dimension tables first (fact tables reference them)
    load_date_dim_vibesql(&mut db, &mut data);
    load_time_dim_vibesql(&mut db, &mut data);
    load_item_vibesql(&mut db, &mut data);
    load_customer_address_vibesql(&mut db, &mut data);
    load_customer_vibesql(&mut db, &mut data);
    load_store_vibesql(&mut db, &mut data);

    // Phase 2 dimension tables
    load_promotion_vibesql(&mut db, &mut data);
    load_warehouse_vibesql(&mut db, &mut data);
    load_ship_mode_vibesql(&mut db, &mut data);
    load_reason_vibesql(&mut db, &mut data);

    // Phase 3 dimension tables
    load_catalog_page_vibesql(&mut db, &mut data);
    load_web_page_vibesql(&mut db, &mut data);
    load_web_site_vibesql(&mut db, &mut data);

    // Phase 4 dimension tables (demographics and call center)
    load_customer_demographics_vibesql(&mut db, &mut data);
    load_household_demographics_vibesql(&mut db, &mut data);
    load_income_band_vibesql(&mut db, &mut data);
    load_call_center_vibesql(&mut db, &mut data);

    // Load fact tables
    load_store_sales_vibesql(&mut db, &mut data);
    load_store_returns_vibesql(&mut db, &mut data);

    // Phase 3 fact tables
    load_catalog_sales_vibesql(&mut db, &mut data);
    load_catalog_returns_vibesql(&mut db, &mut data);
    load_web_sales_vibesql(&mut db, &mut data);
    load_web_returns_vibesql(&mut db, &mut data);

    // Phase 4 fact table (inventory)
    load_inventory_vibesql(&mut db, &mut data);

    // Create indexes for join optimization
    create_tpcds_indexes_vibesql(&mut db);

    // Compute statistics for query optimization
    for table_name in [
        "date_dim",
        "time_dim",
        "item",
        "customer",
        "customer_address",
        "store",
        "promotion",
        "warehouse",
        "ship_mode",
        "reason",
        "catalog_page",
        "web_page",
        "web_site",
        "customer_demographics",
        "household_demographics",
        "income_band",
        "call_center",
        "store_sales",
        "store_returns",
        "catalog_sales",
        "catalog_returns",
        "web_sales",
        "web_returns",
        "inventory",
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
    let mut data = TPCDSData::new(scale_factor);

    create_tpcds_schema_sqlite(&conn);

    load_date_dim_sqlite(&conn, &mut data);
    load_time_dim_sqlite(&conn, &mut data);
    load_item_sqlite(&conn, &mut data);
    load_customer_address_sqlite(&conn, &mut data);
    load_customer_sqlite(&conn, &mut data);
    load_store_sqlite(&conn, &mut data);
    load_store_sales_sqlite(&conn, &mut data);

    conn
}

#[cfg(feature = "benchmark-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCDSData::new(scale_factor);

    create_tpcds_schema_duckdb(&conn);

    load_date_dim_duckdb(&conn, &mut data);
    load_time_dim_duckdb(&conn, &mut data);
    load_item_duckdb(&conn, &mut data);
    load_customer_address_duckdb(&conn, &mut data);
    load_customer_duckdb(&conn, &mut data);
    load_store_duckdb(&conn, &mut data);
    load_store_sales_duckdb(&conn, &mut data);

    conn
}

// =============================================================================
// Schema Creation - VibeSQL
// =============================================================================

fn create_tpcds_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // DATE_DIM table - Calendar dimension
    db.create_table(TableSchema::new(
        "date_dim".to_string(),
        vec![
            ColumnSchema {
                name: "d_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "d_date_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "d_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_month_seq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_week_seq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_quarter_seq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_year".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_dow".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_moy".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_dom".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_qoy".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_fy_year".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_fy_quarter_seq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_fy_week_seq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_day_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(9) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_quarter_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(6) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_holiday".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_weekend".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_following_holiday".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_first_dom".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_last_dom".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_same_day_ly".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_same_day_lq".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_current_day".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_current_week".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_current_month".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_current_quarter".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "d_current_year".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // TIME_DIM table - Time of day dimension
    db.create_table(TableSchema::new(
        "time_dim".to_string(),
        vec![
            ColumnSchema {
                name: "t_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "t_time_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "t_time".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_hour".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_minute".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_second".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_am_pm".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_shift".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_sub_shift".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "t_meal_time".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // ITEM table - Product dimension
    db.create_table(TableSchema::new(
        "item".to_string(),
        vec![
            ColumnSchema {
                name: "i_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "i_item_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "i_rec_start_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_rec_end_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_item_desc".to_string(),
                data_type: DataType::Varchar { max_length: Some(200) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_current_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_brand_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_brand".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_class_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_category_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_category".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_manufact_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_manufact".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_size".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_formulation".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_color".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_units".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_container".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_manager_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "i_product_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CUSTOMER_ADDRESS table
    db.create_table(TableSchema::new(
        "customer_address".to_string(),
        vec![
            ColumnSchema {
                name: "ca_address_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_address_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_street_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_street_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_street_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_suite_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_city".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_county".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_state".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_zip".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_gmt_offset".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ca_location_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CUSTOMER table
    db.create_table(TableSchema::new(
        "customer".to_string(),
        vec![
            ColumnSchema {
                name: "c_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "c_customer_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "c_current_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_current_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_current_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_first_shipto_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_first_sales_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_salutation".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_first_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_last_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_preferred_cust_flag".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_birth_day".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_birth_month".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_birth_year".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_birth_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_login".to_string(),
                data_type: DataType::Varchar { max_length: Some(13) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_email_address".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "c_last_review_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // STORE table
    db.create_table(TableSchema::new(
        "store".to_string(),
        vec![
            ColumnSchema {
                name: "s_store_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "s_store_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "s_rec_start_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_rec_end_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_closed_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_store_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_number_employees".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_floor_space".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_hours".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_market_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_geography_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_market_desc".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_market_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_division_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_division_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_company_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_company_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_street_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_street_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_street_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_suite_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_city".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_county".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_state".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_zip".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_gmt_offset".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "s_tax_percentage".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // STORE_SALES fact table
    db.create_table(TableSchema::new(
        "store_sales".to_string(),
        vec![
            ColumnSchema {
                name: "ss_sold_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_sold_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_store_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_promo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ticket_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ext_discount_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ext_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ext_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ext_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_ext_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_coupon_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_net_paid".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_net_paid_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ss_net_profit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // =========================================================================
    // Phase 2 Tables
    // =========================================================================

    // PROMOTION table
    db.create_table(TableSchema::new(
        "promotion".to_string(),
        vec![
            ColumnSchema {
                name: "p_promo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "p_promo_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "p_start_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_end_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_cost".to_string(),
                data_type: DataType::Decimal { precision: 15, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_response_target".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_promo_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_dmail".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_email".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_catalog".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_tv".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_radio".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_press".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_event".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_demo".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_channel_details".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_purpose".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "p_discount_active".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // WAREHOUSE table
    db.create_table(TableSchema::new(
        "warehouse".to_string(),
        vec![
            ColumnSchema {
                name: "w_warehouse_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "w_warehouse_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "w_warehouse_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_warehouse_sq_ft".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_street_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_street_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_street_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_suite_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_city".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_county".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_state".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_zip".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "w_gmt_offset".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // SHIP_MODE table
    db.create_table(TableSchema::new(
        "ship_mode".to_string(),
        vec![
            ColumnSchema {
                name: "sm_ship_mode_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "sm_ship_mode_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "sm_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sm_code".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sm_carrier".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sm_contract".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // REASON table
    db.create_table(TableSchema::new(
        "reason".to_string(),
        vec![
            ColumnSchema {
                name: "r_reason_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "r_reason_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "r_reason_desc".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // STORE_RETURNS fact table
    db.create_table(TableSchema::new(
        "store_returns".to_string(),
        vec![
            ColumnSchema {
                name: "sr_returned_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_store_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_reason_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_ticket_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_amt_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_fee".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_return_ship_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_refunded_cash".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_reversed_charge".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_store_credit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "sr_net_loss".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // =========================================================================
    // Phase 3 Tables
    // =========================================================================

    // CATALOG_PAGE table
    db.create_table(TableSchema::new(
        "catalog_page".to_string(),
        vec![
            ColumnSchema {
                name: "cp_catalog_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_catalog_page_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_start_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_end_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_department".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_catalog_number".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_catalog_page_number".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_description".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cp_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // WEB_PAGE table
    db.create_table(TableSchema::new(
        "web_page".to_string(),
        vec![
            ColumnSchema {
                name: "wp_web_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_web_page_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_rec_start_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_rec_end_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_creation_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_access_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_autogen_flag".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_url".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_char_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_link_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_image_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wp_max_ad_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // WEB_SITE table
    db.create_table(TableSchema::new(
        "web_site".to_string(),
        vec![
            ColumnSchema {
                name: "web_site_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "web_site_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "web_rec_start_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_rec_end_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_open_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_close_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_mkt_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_mkt_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_mkt_desc".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_market_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_company_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_company_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_street_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_street_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_street_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_suite_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_city".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_county".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_state".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_zip".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_gmt_offset".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "web_tax_percentage".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CATALOG_SALES fact table
    db.create_table(TableSchema::new(
        "catalog_sales".to_string(),
        vec![
            ColumnSchema {
                name: "cs_sold_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_sold_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_bill_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_bill_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_bill_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_bill_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_call_center_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_catalog_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ship_mode_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_warehouse_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_promo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_order_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_discount_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_coupon_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_ext_ship_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_net_paid".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_net_paid_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_net_paid_inc_ship".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_net_paid_inc_ship_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cs_net_profit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CATALOG_RETURNS fact table
    db.create_table(TableSchema::new(
        "catalog_returns".to_string(),
        vec![
            ColumnSchema {
                name: "cr_returned_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_returned_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_refunded_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_refunded_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_refunded_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_refunded_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_returning_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_returning_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_returning_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_returning_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_call_center_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_catalog_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_ship_mode_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_warehouse_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_reason_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_order_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_return_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_return_amount".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_return_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_return_amt_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_fee".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_return_ship_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_refunded_cash".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_reversed_charge".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_store_credit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cr_net_loss".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // WEB_SALES fact table
    db.create_table(TableSchema::new(
        "web_sales".to_string(),
        vec![
            ColumnSchema {
                name: "ws_sold_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_sold_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_bill_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_bill_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_bill_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_bill_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_web_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_web_site_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ship_mode_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_warehouse_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_promo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_order_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_discount_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_sales_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_wholesale_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_list_price".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_coupon_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_ext_ship_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_net_paid".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_net_paid_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_net_paid_inc_ship".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_net_paid_inc_ship_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ws_net_profit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // WEB_RETURNS fact table
    db.create_table(TableSchema::new(
        "web_returns".to_string(),
        vec![
            ColumnSchema {
                name: "wr_returned_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_returned_time_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_refunded_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_refunded_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_refunded_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_refunded_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_returning_customer_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_returning_cdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_returning_hdemo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_returning_addr_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_web_page_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_reason_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_order_number".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_return_quantity".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_return_amt".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_return_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_return_amt_inc_tax".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_fee".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_return_ship_cost".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_refunded_cash".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_reversed_charge".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_account_credit".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "wr_net_loss".to_string(),
                data_type: DataType::Decimal { precision: 7, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CUSTOMER_DEMOGRAPHICS table - Customer demographic attributes
    db.create_table(TableSchema::new(
        "customer_demographics".to_string(),
        vec![
            ColumnSchema {
                name: "cd_demo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_gender".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_marital_status".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_education_status".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_purchase_estimate".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_credit_rating".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_dep_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_dep_employed_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cd_dep_college_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // HOUSEHOLD_DEMOGRAPHICS table - Household demographic attributes
    db.create_table(TableSchema::new(
        "household_demographics".to_string(),
        vec![
            ColumnSchema {
                name: "hd_demo_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "hd_income_band_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "hd_buy_potential".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "hd_dep_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "hd_vehicle_count".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // INCOME_BAND table - Income range buckets
    db.create_table(TableSchema::new(
        "income_band".to_string(),
        vec![
            ColumnSchema {
                name: "ib_income_band_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "ib_lower_bound".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "ib_upper_bound".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CALL_CENTER table - Call center information
    db.create_table(TableSchema::new(
        "call_center".to_string(),
        vec![
            ColumnSchema {
                name: "cc_call_center_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_call_center_id".to_string(),
                data_type: DataType::Varchar { max_length: Some(16) },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_rec_start_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_rec_end_date".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_closed_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_open_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_employees".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_sq_ft".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_hours".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_mkt_id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_mkt_class".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_mkt_desc".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_market_manager".to_string(),
                data_type: DataType::Varchar { max_length: Some(40) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_division".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_division_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_company".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_company_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_street_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_street_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_street_type".to_string(),
                data_type: DataType::Varchar { max_length: Some(15) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_suite_number".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_city".to_string(),
                data_type: DataType::Varchar { max_length: Some(60) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_county".to_string(),
                data_type: DataType::Varchar { max_length: Some(30) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_state".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_zip".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_country".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_gmt_offset".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "cc_tax_percentage".to_string(),
                data_type: DataType::Decimal { precision: 5, scale: 2 },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // INVENTORY table - Inventory levels fact table
    db.create_table(TableSchema::new(
        "inventory".to_string(),
        vec![
            ColumnSchema {
                name: "inv_date_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "inv_item_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "inv_warehouse_sk".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "inv_quantity_on_hand".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();
}

// =============================================================================
// Index Creation - VibeSQL
// =============================================================================

fn create_tpcds_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // Primary key indexes for dimension tables
    db.create_index(
        "idx_date_dim_pk".to_string(),
        "date_dim".to_string(),
        true,
        vec![IndexColumn {
            column_name: "d_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_time_dim_pk".to_string(),
        "time_dim".to_string(),
        true,
        vec![IndexColumn {
            column_name: "t_time_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_item_pk".to_string(),
        "item".to_string(),
        true,
        vec![IndexColumn {
            column_name: "i_item_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_customer_pk".to_string(),
        "customer".to_string(),
        true,
        vec![IndexColumn {
            column_name: "c_customer_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_customer_address_pk".to_string(),
        "customer_address".to_string(),
        true,
        vec![IndexColumn {
            column_name: "ca_address_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_store_pk".to_string(),
        "store".to_string(),
        true,
        vec![IndexColumn {
            column_name: "s_store_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Composite primary key for store_sales
    db.create_index(
        "idx_store_sales_pk".to_string(),
        "store_sales".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "ss_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "ss_ticket_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    // Foreign key indexes for store_sales (common join columns)
    db.create_index(
        "idx_store_sales_date".to_string(),
        "store_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "ss_sold_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_store_sales_customer".to_string(),
        "store_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "ss_customer_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_store_sales_store".to_string(),
        "store_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "ss_store_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Phase 2 indexes
    db.create_index(
        "idx_promotion_pk".to_string(),
        "promotion".to_string(),
        true,
        vec![IndexColumn {
            column_name: "p_promo_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_warehouse_pk".to_string(),
        "warehouse".to_string(),
        true,
        vec![IndexColumn {
            column_name: "w_warehouse_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_ship_mode_pk".to_string(),
        "ship_mode".to_string(),
        true,
        vec![IndexColumn {
            column_name: "sm_ship_mode_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_reason_pk".to_string(),
        "reason".to_string(),
        true,
        vec![IndexColumn {
            column_name: "r_reason_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // store_returns indexes
    db.create_index(
        "idx_store_returns_pk".to_string(),
        "store_returns".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "sr_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "sr_ticket_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    db.create_index(
        "idx_store_returns_date".to_string(),
        "store_returns".to_string(),
        false,
        vec![IndexColumn {
            column_name: "sr_returned_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_store_returns_customer".to_string(),
        "store_returns".to_string(),
        false,
        vec![IndexColumn {
            column_name: "sr_customer_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // Phase 3 indexes - Dimension tables
    db.create_index(
        "idx_catalog_page_pk".to_string(),
        "catalog_page".to_string(),
        true,
        vec![IndexColumn {
            column_name: "cp_catalog_page_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_web_page_pk".to_string(),
        "web_page".to_string(),
        true,
        vec![IndexColumn {
            column_name: "wp_web_page_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_web_site_pk".to_string(),
        "web_site".to_string(),
        true,
        vec![IndexColumn {
            column_name: "web_site_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // catalog_sales indexes
    db.create_index(
        "idx_catalog_sales_pk".to_string(),
        "catalog_sales".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "cs_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "cs_order_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    db.create_index(
        "idx_catalog_sales_date".to_string(),
        "catalog_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "cs_sold_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_catalog_sales_customer".to_string(),
        "catalog_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "cs_bill_customer_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // catalog_returns indexes
    db.create_index(
        "idx_catalog_returns_pk".to_string(),
        "catalog_returns".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "cr_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "cr_order_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    db.create_index(
        "idx_catalog_returns_date".to_string(),
        "catalog_returns".to_string(),
        false,
        vec![IndexColumn {
            column_name: "cr_returned_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // web_sales indexes
    db.create_index(
        "idx_web_sales_pk".to_string(),
        "web_sales".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "ws_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "ws_order_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    db.create_index(
        "idx_web_sales_date".to_string(),
        "web_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "ws_sold_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    db.create_index(
        "idx_web_sales_customer".to_string(),
        "web_sales".to_string(),
        false,
        vec![IndexColumn {
            column_name: "ws_bill_customer_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // web_returns indexes
    db.create_index(
        "idx_web_returns_pk".to_string(),
        "web_returns".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "wr_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "wr_order_number".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();

    db.create_index(
        "idx_web_returns_date".to_string(),
        "web_returns".to_string(),
        false,
        vec![IndexColumn {
            column_name: "wr_returned_date_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // customer_demographics index
    db.create_index(
        "idx_customer_demographics_pk".to_string(),
        "customer_demographics".to_string(),
        true,
        vec![IndexColumn {
            column_name: "cd_demo_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // household_demographics index
    db.create_index(
        "idx_household_demographics_pk".to_string(),
        "household_demographics".to_string(),
        true,
        vec![IndexColumn {
            column_name: "hd_demo_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // income_band index
    db.create_index(
        "idx_income_band_pk".to_string(),
        "income_band".to_string(),
        true,
        vec![IndexColumn {
            column_name: "ib_income_band_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // call_center index
    db.create_index(
        "idx_call_center_pk".to_string(),
        "call_center".to_string(),
        true,
        vec![IndexColumn {
            column_name: "cc_call_center_sk".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // inventory indexes (composite primary key)
    db.create_index(
        "idx_inventory_pk".to_string(),
        "inventory".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "inv_date_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "inv_item_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "inv_warehouse_sk".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();
}

// =============================================================================
// Data Loading - DATE_DIM
// =============================================================================

fn load_date_dim_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let day_names = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];

    // Generate dates from 1998-01-01 to 2003-12-31 (~2191 days)
    // Using a reduced set for benchmarking
    let num_dates = 2191.min(data.date_dim_count);

    for d_date_sk in 1..=num_dates {
        let days_since_base = d_date_sk as i64 - 1;

        // Calculate date components (simplified, ignoring leap years for benchmark)
        let year = 1998 + (days_since_base / 365) as i32;
        let day_of_year = (days_since_base % 365) as i32;
        let month = (day_of_year / 30).min(11) + 1;
        let day = (day_of_year % 30) + 1;

        let date_str = format!("{:04}-{:02}-{:02}", year, month, day);
        let d_date_id = format!("AAAAAA{:010}", d_date_sk);

        let d_dow = (days_since_base % 7) as i32;
        let d_week_seq = (days_since_base / 7) as i32 + 1;
        let d_month_seq = (year - 1998) * 12 + month;
        let d_quarter_seq = (year - 1998) * 4 + ((month - 1) / 3) + 1;
        let d_qoy = ((month - 1) / 3) + 1;
        let quarter_name = format!("{}Q{}", year, d_qoy);

        let is_weekend = d_dow == 0 || d_dow == 6;

        let row = Row::new(vec![
            SqlValue::Integer(d_date_sk as i64),
            SqlValue::Varchar(d_date_id),
            SqlValue::Date(Date::from_str(&date_str).unwrap_or_else(|_| Date::from_str("1998-01-01").unwrap())),
            SqlValue::Integer(d_month_seq as i64),
            SqlValue::Integer(d_week_seq as i64),
            SqlValue::Integer(d_quarter_seq as i64),
            SqlValue::Integer(year as i64),
            SqlValue::Integer(d_dow as i64),
            SqlValue::Integer(month as i64),
            SqlValue::Integer(day as i64),
            SqlValue::Integer(d_qoy as i64),
            SqlValue::Integer(year as i64),  // d_fy_year
            SqlValue::Integer(d_quarter_seq as i64),  // d_fy_quarter_seq
            SqlValue::Integer(d_week_seq as i64),  // d_fy_week_seq
            SqlValue::Varchar(day_names[d_dow as usize].to_string()),
            SqlValue::Varchar(quarter_name),
            SqlValue::Varchar("N".to_string()),  // d_holiday
            SqlValue::Varchar(if is_weekend { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar("N".to_string()),  // d_following_holiday
            SqlValue::Integer(((d_month_seq - 1) * 30 + 1) as i64),  // d_first_dom
            SqlValue::Integer((d_month_seq * 30) as i64),  // d_last_dom
            SqlValue::Integer((d_date_sk as i64 - 365).max(1)),  // d_same_day_ly
            SqlValue::Integer((d_date_sk as i64 - 91).max(1)),  // d_same_day_lq
            SqlValue::Varchar("N".to_string()),  // d_current_day
            SqlValue::Varchar("N".to_string()),  // d_current_week
            SqlValue::Varchar("N".to_string()),  // d_current_month
            SqlValue::Varchar("N".to_string()),  // d_current_quarter
            SqlValue::Varchar("N".to_string()),  // d_current_year
        ]);
        db.insert_row("date_dim", row).unwrap();
    }
}

// =============================================================================
// Data Loading - TIME_DIM
// =============================================================================

fn load_time_dim_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Generate time dimension for every hour (24 entries for benchmark, full would be 86400)
    let num_times = 24.min(data.time_dim_count / 3600).max(24);

    for hour in 0..num_times {
        let t_time_sk = hour * 3600; // Seconds since midnight
        let t_time_id = format!("AAAAAA{:010}", t_time_sk);

        let am_pm = if hour < 12 { "AM" } else { "PM" };
        let shift = if hour < 8 {
            "third"
        } else if hour < 16 {
            "first"
        } else {
            "second"
        };
        let sub_shift = if hour % 8 < 4 { "night" } else { "day" };
        let meal_time = if hour >= 7 && hour < 9 {
            "breakfast"
        } else if hour >= 12 && hour < 14 {
            "lunch"
        } else if hour >= 18 && hour < 20 {
            "dinner"
        } else {
            ""
        };

        let row = Row::new(vec![
            SqlValue::Integer(t_time_sk as i64),
            SqlValue::Varchar(t_time_id),
            SqlValue::Integer(t_time_sk as i64),
            SqlValue::Integer(hour as i64),
            SqlValue::Integer(0),  // t_minute
            SqlValue::Integer(0),  // t_second
            SqlValue::Varchar(am_pm.to_string()),
            SqlValue::Varchar(shift.to_string()),
            SqlValue::Varchar(sub_shift.to_string()),
            SqlValue::Varchar(meal_time.to_string()),
        ]);
        db.insert_row("time_dim", row).unwrap();
    }
}

// =============================================================================
// Data Loading - ITEM
// =============================================================================

fn load_item_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.item_count {
        let i_item_id = format!("AAAAAA{:010}", i);
        let category_idx = i % CATEGORIES.len();
        let class_idx = i % CLASSES.len();
        let brand_idx = i % BRANDS.len();
        let color_idx = i % ITEM_COLORS.len();
        let size_idx = i % ITEM_SIZES.len();

        let current_price = data.random_f64(1.0, 200.0);
        let wholesale_cost = current_price * 0.5;

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(i_item_id),
            SqlValue::Date(Date::from_str("1998-01-01").unwrap()),  // i_rec_start_date
            SqlValue::Null,  // i_rec_end_date
            SqlValue::Varchar(format!("{} {} item", CATEGORIES[category_idx], CLASSES[class_idx])),
            SqlValue::Numeric(current_price),
            SqlValue::Numeric(wholesale_cost),
            SqlValue::Integer((brand_idx + 1) as i64),
            SqlValue::Varchar(BRANDS[brand_idx].to_string()),
            SqlValue::Integer((class_idx + 1) as i64),
            SqlValue::Varchar(CLASSES[class_idx].to_string()),
            SqlValue::Integer((category_idx + 1) as i64),
            SqlValue::Varchar(CATEGORIES[category_idx].to_string()),
            SqlValue::Integer(((i % 10) + 1) as i64),
            SqlValue::Varchar(format!("Manufacturer#{}", (i % 10) + 1)),
            SqlValue::Varchar(ITEM_SIZES[size_idx].to_string()),
            SqlValue::Varchar(format!("formula{}", i % 20)),
            SqlValue::Varchar(ITEM_COLORS[color_idx].to_string()),
            SqlValue::Varchar("Each".to_string()),
            SqlValue::Varchar("Unknown".to_string()),
            SqlValue::Integer(((i % 100) + 1) as i64),
            SqlValue::Varchar(format!("Product#{}", i)),
        ]);
        db.insert_row("item", row).unwrap();
    }
}

// =============================================================================
// Data Loading - CUSTOMER_ADDRESS
// =============================================================================

fn load_customer_address_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.customer_address_count {
        let ca_address_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(ca_address_id),
            SqlValue::Varchar(format!("{}", data.random_i32(1, 999))),
            SqlValue::Varchar(data.random_varchar(30)),
            SqlValue::Varchar("Street".to_string()),
            SqlValue::Varchar(format!("Suite {}", data.random_i32(100, 999))),
            SqlValue::Varchar(data.random_city()),
            SqlValue::Varchar(format!("{} County", STATES[state_idx])),
            SqlValue::Varchar(STATES[state_idx].to_string()),
            SqlValue::Varchar(data.random_zip()),
            SqlValue::Varchar("United States".to_string()),
            SqlValue::Numeric(-5.0 + (state_idx as f64 * 0.1)),  // ca_gmt_offset
            SqlValue::Varchar("residential".to_string()),
        ]);
        db.insert_row("customer_address", row).unwrap();
    }
}

// =============================================================================
// Data Loading - CUSTOMER
// =============================================================================

fn load_customer_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let salutations = ["Mr.", "Mrs.", "Ms.", "Dr.", ""];

    for i in 1..=data.customer_count {
        let c_customer_id = format!("AAAAAA{:010}", i);
        let _gender_idx = i % GENDERS.len();
        let _marital_idx = i % MARITAL_STATUS.len();
        let sal_idx = i % salutations.len();

        let birth_year = data.random_i32(1930, 1990);
        let birth_month = data.random_i32(1, 12);
        let birth_day = data.random_i32(1, 28);

        // Link to customer_address (each customer has ~2 addresses on average)
        let addr_sk = ((i - 1) % data.customer_address_count) + 1;

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(c_customer_id),
            SqlValue::Integer((i % 1920 + 1) as i64),  // c_current_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // c_current_hdemo_sk
            SqlValue::Integer(addr_sk as i64),
            SqlValue::Integer(data.random_i32(1, 2191) as i64),  // c_first_shipto_date_sk
            SqlValue::Integer(data.random_i32(1, 2191) as i64),  // c_first_sales_date_sk
            SqlValue::Varchar(salutations[sal_idx].to_string()),
            SqlValue::Varchar(format!("FirstName{}", i % 1000)),
            SqlValue::Varchar(format!("LastName{}", i % 2000)),
            SqlValue::Varchar(if i % 3 == 0 { "Y" } else { "N" }.to_string()),
            SqlValue::Integer(birth_day as i64),
            SqlValue::Integer(birth_month as i64),
            SqlValue::Integer(birth_year as i64),
            SqlValue::Varchar("UNITED STATES".to_string()),
            SqlValue::Varchar(format!("user{}", i)),
            SqlValue::Varchar(data.random_email()),
            SqlValue::Integer(data.random_i32(1, 2191) as i64),
        ]);
        db.insert_row("customer", row).unwrap();
    }
}

// =============================================================================
// Data Loading - STORE
// =============================================================================

fn load_store_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.store_count {
        let s_store_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(s_store_id),
            SqlValue::Date(Date::from_str("1998-01-01").unwrap()),
            SqlValue::Null,  // s_rec_end_date
            SqlValue::Null,  // s_closed_date_sk
            SqlValue::Varchar(format!("Store#{}", i)),
            SqlValue::Integer(data.random_i32(50, 500) as i64),
            SqlValue::Integer(data.random_i32(5000, 50000) as i64),
            SqlValue::Varchar("8AM-10PM".to_string()),
            SqlValue::Varchar(format!("Manager{}", i % 100)),
            SqlValue::Integer((i % 10 + 1) as i64),
            SqlValue::Varchar("Unknown".to_string()),
            SqlValue::Varchar("Market description".to_string()),
            SqlValue::Varchar(format!("MarketManager{}", i % 50)),
            SqlValue::Integer((i % 5 + 1) as i64),
            SqlValue::Varchar(format!("Division{}", i % 5 + 1)),
            SqlValue::Integer((i % 3 + 1) as i64),
            SqlValue::Varchar(format!("Company{}", i % 3 + 1)),
            SqlValue::Varchar(format!("{}", data.random_i32(1, 999))),
            SqlValue::Varchar(data.random_varchar(30)),
            SqlValue::Varchar("Avenue".to_string()),
            SqlValue::Varchar(format!("Suite {}", data.random_i32(100, 999))),
            SqlValue::Varchar(data.random_city()),
            SqlValue::Varchar(format!("{} County", STATES[state_idx])),
            SqlValue::Varchar(STATES[state_idx].to_string()),
            SqlValue::Varchar(data.random_zip()),
            SqlValue::Varchar("United States".to_string()),
            SqlValue::Numeric(-5.0 + (state_idx as f64 * 0.1)),
            SqlValue::Numeric(data.random_f64(0.0, 0.11)),
        ]);
        db.insert_row("store", row).unwrap();
    }
}

// =============================================================================
// Data Loading - STORE_SALES
// =============================================================================

fn load_store_sales_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.store_sales_count {
        let ss_sold_date_sk = (i % num_dates) + 1;
        let ss_sold_time_sk = (i % 24) * 3600;
        let ss_item_sk = (i % data.item_count) + 1;
        let ss_customer_sk = (i % data.customer_count) + 1;
        let ss_store_sk = (i % data.store_count) + 1;
        let ss_ticket_number = (i / 5) + 1;  // ~5 items per ticket

        let quantity = data.random_i32(1, 100);
        let wholesale_cost = data.random_f64(1.0, 50.0);
        let list_price = wholesale_cost * data.random_f64(1.5, 3.0);
        let sales_price = list_price * data.random_f64(0.8, 1.0);
        let ext_sales_price = sales_price * quantity as f64;
        let ext_wholesale_cost = wholesale_cost * quantity as f64;
        let ext_list_price = list_price * quantity as f64;
        let ext_discount_amt = ext_list_price - ext_sales_price;
        let ext_tax = ext_sales_price * 0.08;
        let coupon_amt = if i % 10 == 0 {
            ext_sales_price * 0.05
        } else {
            0.0
        };
        let net_paid = ext_sales_price - coupon_amt;
        let net_paid_inc_tax = net_paid + ext_tax;
        let net_profit = net_paid - ext_wholesale_cost;

        let row = Row::new(vec![
            SqlValue::Integer(ss_sold_date_sk as i64),
            SqlValue::Integer(ss_sold_time_sk as i64),
            SqlValue::Integer(ss_item_sk as i64),
            SqlValue::Integer(ss_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // ss_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // ss_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // ss_addr_sk
            SqlValue::Integer(ss_store_sk as i64),
            SqlValue::Integer((i % 300 + 1) as i64),  // ss_promo_sk
            SqlValue::Integer(ss_ticket_number as i64),
            SqlValue::Integer(quantity as i64),
            SqlValue::Numeric(wholesale_cost),
            SqlValue::Numeric(list_price),
            SqlValue::Numeric(sales_price),
            SqlValue::Numeric(ext_discount_amt),
            SqlValue::Numeric(ext_sales_price),
            SqlValue::Numeric(ext_wholesale_cost),
            SqlValue::Numeric(ext_list_price),
            SqlValue::Numeric(ext_tax),
            SqlValue::Numeric(coupon_amt),
            SqlValue::Numeric(net_paid),
            SqlValue::Numeric(net_paid_inc_tax),
            SqlValue::Numeric(net_profit),
        ]);
        db.insert_row("store_sales", row).unwrap();
    }
}

// =============================================================================
// Data Loading - Phase 2 Tables (VibeSQL)
// =============================================================================

fn load_promotion_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::PROMO_PURPOSES;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.promotion_count {
        let p_promo_id = format!("AAAAAA{:010}", i);
        let purpose_idx = i % PROMO_PURPOSES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(p_promo_id),
            SqlValue::Integer(data.random_i32(1, 2191) as i64),  // p_start_date_sk
            SqlValue::Integer(data.random_i32(1, 2191) as i64),  // p_end_date_sk
            SqlValue::Integer(data.random_key(data.item_count) as i64),  // p_item_sk
            SqlValue::Numeric(data.random_f64(100.0, 10000.0)),  // p_cost
            SqlValue::Integer(data.random_i32(1, 100) as i64),   // p_response_target
            SqlValue::Varchar(format!("Promo#{}", i)),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
            SqlValue::Varchar(format!("Channel details for promo {}", i)),
            SqlValue::Varchar(PROMO_PURPOSES[purpose_idx].to_string()),
            SqlValue::Varchar(if data.random_bool() { "Y" } else { "N" }.to_string()),
        ]);
        db.insert_row("promotion", row).unwrap();
    }
}

fn load_warehouse_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.warehouse_count {
        let w_warehouse_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(w_warehouse_id),
            SqlValue::Varchar(format!("Warehouse#{}", i)),
            SqlValue::Integer(data.random_i32(50000, 500000) as i64),  // sq_ft
            SqlValue::Varchar(format!("{}", data.random_i32(1, 999))),
            SqlValue::Varchar(data.random_varchar(30)),
            SqlValue::Varchar("Boulevard".to_string()),
            SqlValue::Varchar(format!("Suite {}", data.random_i32(100, 999))),
            SqlValue::Varchar(data.random_city()),
            SqlValue::Varchar(format!("{} County", STATES[state_idx])),
            SqlValue::Varchar(STATES[state_idx].to_string()),
            SqlValue::Varchar(data.random_zip()),
            SqlValue::Varchar("United States".to_string()),
            SqlValue::Numeric(-5.0 + (state_idx as f64 * 0.1)),
        ]);
        db.insert_row("warehouse", row).unwrap();
    }
}

fn load_ship_mode_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::SHIP_MODES;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let carriers = ["FedEx", "UPS", "USPS", "DHL", "Freight"];
    let types = ["EXPRESS", "REGULAR", "OVERNIGHT", "LIBRARY", "TWO DAY"];

    for i in 1..=data.ship_mode_count.min(20) {
        let sm_ship_mode_id = format!("AAAAAA{:010}", i);
        let mode_idx = i % SHIP_MODES.len();
        let carrier_idx = i % carriers.len();
        let type_idx = i % types.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(sm_ship_mode_id),
            SqlValue::Varchar(types[type_idx].to_string()),
            SqlValue::Varchar(SHIP_MODES[mode_idx].to_string()),
            SqlValue::Varchar(carriers[carrier_idx].to_string()),
            SqlValue::Varchar(format!("Contract{}", i)),
        ]);
        db.insert_row("ship_mode", row).unwrap();
    }
}

fn load_reason_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::REASONS;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.reason_count.min(REASONS.len()) {
        let r_reason_id = format!("AAAAAA{:010}", i);

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(r_reason_id),
            SqlValue::Varchar(REASONS[i - 1].to_string()),
        ]);
        db.insert_row("reason", row).unwrap();
    }
}

fn load_store_returns_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.store_returns_count {
        let sr_returned_date_sk = (i % num_dates) + 1;
        let sr_return_time_sk = (i % 24) * 3600;
        let sr_item_sk = (i % data.item_count) + 1;
        let sr_customer_sk = (i % data.customer_count) + 1;
        let sr_store_sk = (i % data.store_count) + 1;
        let sr_reason_sk = (i % data.reason_count.min(15)) + 1;
        let sr_ticket_number = (i / 3) + 1;  // ~3 items per return ticket

        let return_quantity = data.random_i32(1, 10);
        let return_amt = data.random_f64(10.0, 500.0) * return_quantity as f64;
        let return_tax = return_amt * 0.08;
        let return_amt_inc_tax = return_amt + return_tax;
        let fee = data.random_f64(5.0, 25.0);
        let return_ship_cost = data.random_f64(5.0, 50.0);
        let refunded_cash = return_amt * 0.7;
        let reversed_charge = return_amt * 0.1;
        let store_credit = return_amt * 0.2;
        let net_loss = return_amt - refunded_cash - reversed_charge - store_credit + fee;

        let row = Row::new(vec![
            SqlValue::Integer(sr_returned_date_sk as i64),
            SqlValue::Integer(sr_return_time_sk as i64),
            SqlValue::Integer(sr_item_sk as i64),
            SqlValue::Integer(sr_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // sr_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // sr_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // sr_addr_sk
            SqlValue::Integer(sr_store_sk as i64),
            SqlValue::Integer(sr_reason_sk as i64),
            SqlValue::Integer(sr_ticket_number as i64),
            SqlValue::Integer(return_quantity as i64),
            SqlValue::Numeric(return_amt),
            SqlValue::Numeric(return_tax),
            SqlValue::Numeric(return_amt_inc_tax),
            SqlValue::Numeric(fee),
            SqlValue::Numeric(return_ship_cost),
            SqlValue::Numeric(refunded_cash),
            SqlValue::Numeric(reversed_charge),
            SqlValue::Numeric(store_credit),
            SqlValue::Numeric(net_loss),
        ]);
        db.insert_row("store_returns", row).unwrap();
    }
}

// =============================================================================
// Data Loading - Phase 3 Dimension Tables
// =============================================================================

fn load_catalog_page_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::CATALOG_PAGE_TYPES;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.catalog_page_count {
        let cp_catalog_page_id = format!("AAAAAA{:010}", i);
        let type_idx = i % CATALOG_PAGE_TYPES.len();
        let catalog_number = (i / 100) + 1;
        let page_number = (i % 100) + 1;

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(cp_catalog_page_id),
            SqlValue::Integer(((i - 1) / 1000 + 1) as i64),  // cp_start_date_sk
            SqlValue::Integer(((i - 1) / 1000 + 365) as i64),  // cp_end_date_sk
            SqlValue::Varchar(format!("{}", data.random_varchar(80))),  // cp_department
            SqlValue::Integer(catalog_number as i64),
            SqlValue::Integer(page_number as i64),
            SqlValue::Varchar(format!("Catalog page {}", i)),  // cp_description
            SqlValue::Varchar(CATALOG_PAGE_TYPES[type_idx].to_string()),
        ]);
        db.insert_row("catalog_page", row).unwrap();
    }
}

fn load_web_page_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::WEB_PAGE_TYPES;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.web_page_count {
        let wp_web_page_id = format!("AAAAAA{:010}", i);
        let type_idx = i % WEB_PAGE_TYPES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(wp_web_page_id),
            SqlValue::Integer(((i - 1) / 100 + 1) as i64),  // wp_rec_start_date_sk (date_sk)
            SqlValue::Null,  // wp_rec_end_date_sk
            SqlValue::Integer(((i - 1) / 20 + 1) as i64),  // wp_creation_date_sk
            SqlValue::Integer(((i - 1) / 10 + 1) as i64),  // wp_access_date_sk
            SqlValue::Varchar("N".to_string()),  // wp_autogen_flag
            SqlValue::Integer((i % data.customer_count + 1) as i64),  // wp_customer_sk
            SqlValue::Varchar(format!("/page/{}", i)),  // wp_url
            SqlValue::Varchar(WEB_PAGE_TYPES[type_idx].to_string()),
            SqlValue::Integer(data.random_i32(1, 10) as i64),  // wp_char_count
            SqlValue::Integer(data.random_i32(1, 5) as i64),  // wp_link_count
            SqlValue::Integer(data.random_i32(1, 20) as i64),  // wp_image_count
            SqlValue::Integer(data.random_i32(5, 100) as i64),  // wp_max_ad_count
        ]);
        db.insert_row("web_page", row).unwrap();
    }
}

fn load_web_site_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use super::data::WEB_SITE_CLASSES;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 1..=data.web_site_count {
        let web_site_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();
        let class_idx = i % WEB_SITE_CLASSES.len();

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(web_site_id),
            SqlValue::Integer(((i - 1) / 10 + 1) as i64),  // web_rec_start_date_sk
            SqlValue::Null,  // web_rec_end_date_sk
            SqlValue::Varchar(format!("site_{}", i)),  // web_name
            SqlValue::Integer(((i - 1) / 5 + 1) as i64),  // web_open_date_sk
            SqlValue::Null,  // web_close_date_sk
            SqlValue::Varchar(WEB_SITE_CLASSES[class_idx].to_string()),
            SqlValue::Varchar(format!("Manager{}", i % 50)),  // web_manager
            SqlValue::Integer((i % 10 + 1) as i64),  // web_mkt_id
            SqlValue::Varchar(format!("Market class {}", i % 5)),  // web_mkt_class
            SqlValue::Varchar(format!("Market description {}", i)),  // web_mkt_desc
            SqlValue::Varchar(format!("MarketManager{}", i % 20)),  // web_market_manager
            SqlValue::Integer((i % 5 + 1) as i64),  // web_company_id
            SqlValue::Varchar(format!("Company{}", i % 5 + 1)),
            SqlValue::Varchar(format!("{}", data.random_i32(1, 999))),  // web_street_number
            SqlValue::Varchar(data.random_varchar(30)),  // web_street_name
            SqlValue::Varchar("Street".to_string()),
            SqlValue::Varchar(format!("Suite {}", data.random_i32(100, 999))),
            SqlValue::Varchar(data.random_city()),
            SqlValue::Varchar(format!("{} County", STATES[state_idx])),
            SqlValue::Varchar(STATES[state_idx].to_string()),
            SqlValue::Varchar(data.random_zip()),
            SqlValue::Varchar("United States".to_string()),
            SqlValue::Numeric(-5.0 + (state_idx as f64 * 0.1)),  // web_gmt_offset
            SqlValue::Numeric(data.random_f64(0.0, 0.12)),  // web_tax_percentage
        ]);
        db.insert_row("web_site", row).unwrap();
    }
}

// =============================================================================
// Data Loading - Phase 3 Fact Tables
// =============================================================================

fn load_catalog_sales_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.catalog_sales_count {
        let cs_sold_date_sk = (i % num_dates) + 1;
        let cs_sold_time_sk = (i % 24) * 3600;
        let cs_ship_date_sk = cs_sold_date_sk + data.random_i32(1, 30) as usize;
        let cs_bill_customer_sk = (i % data.customer_count) + 1;
        let cs_ship_customer_sk = cs_bill_customer_sk;
        let cs_item_sk = (i % data.item_count) + 1;
        let cs_promo_sk = (i % data.promotion_count) + 1;
        let cs_warehouse_sk = (i % data.warehouse_count) + 1;
        let cs_ship_mode_sk = (i % data.ship_mode_count.min(20)) + 1;
        let cs_catalog_page_sk = (i % data.catalog_page_count) + 1;
        let cs_order_number = (i / 5) + 1;  // ~5 items per order

        let quantity = data.random_i32(1, 20);
        let wholesale_cost = data.random_f64(10.0, 100.0);
        let list_price = wholesale_cost * 1.5;
        let sales_price = list_price * data.random_f64(0.8, 1.0);
        let ext_discount_amt = (list_price - sales_price) * quantity as f64;
        let ext_sales_price = sales_price * quantity as f64;
        let ext_wholesale_cost = wholesale_cost * quantity as f64;
        let ext_list_price = list_price * quantity as f64;
        let ext_tax = ext_sales_price * 0.08;
        let coupon_amt = data.random_f64(0.0, 20.0);
        let ext_ship_cost = data.random_f64(5.0, 30.0);
        let net_paid = ext_sales_price - coupon_amt;
        let net_paid_inc_tax = net_paid + ext_tax;
        let net_paid_inc_ship = net_paid + ext_ship_cost;
        let net_paid_inc_ship_tax = net_paid_inc_ship + ext_tax;
        let net_profit = net_paid - ext_wholesale_cost;

        let row = Row::new(vec![
            SqlValue::Integer(cs_sold_date_sk as i64),
            SqlValue::Integer(cs_sold_time_sk as i64),
            SqlValue::Integer(cs_ship_date_sk as i64),
            SqlValue::Integer(cs_bill_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // cs_bill_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // cs_bill_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // cs_bill_addr_sk
            SqlValue::Integer(cs_ship_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // cs_ship_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // cs_ship_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // cs_ship_addr_sk
            SqlValue::Integer((i % 6 + 1) as i64),  // cs_call_center_sk
            SqlValue::Integer(cs_catalog_page_sk as i64),
            SqlValue::Integer(cs_ship_mode_sk as i64),
            SqlValue::Integer(cs_warehouse_sk as i64),
            SqlValue::Integer(cs_item_sk as i64),
            SqlValue::Integer(cs_promo_sk as i64),
            SqlValue::Integer(cs_order_number as i64),
            SqlValue::Integer(quantity as i64),
            SqlValue::Numeric(wholesale_cost),
            SqlValue::Numeric(list_price),
            SqlValue::Numeric(sales_price),
            SqlValue::Numeric(ext_discount_amt),
            SqlValue::Numeric(ext_sales_price),
            SqlValue::Numeric(ext_wholesale_cost),
            SqlValue::Numeric(ext_list_price),
            SqlValue::Numeric(ext_tax),
            SqlValue::Numeric(coupon_amt),
            SqlValue::Numeric(ext_ship_cost),
            SqlValue::Numeric(net_paid),
            SqlValue::Numeric(net_paid_inc_tax),
            SqlValue::Numeric(net_paid_inc_ship),
            SqlValue::Numeric(net_paid_inc_ship_tax),
            SqlValue::Numeric(net_profit),
        ]);
        db.insert_row("catalog_sales", row).unwrap();
    }
}

fn load_catalog_returns_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.catalog_returns_count {
        let cr_returned_date_sk = (i % num_dates) + 1;
        let cr_returned_time_sk = (i % 24) * 3600;
        let cr_item_sk = (i % data.item_count) + 1;
        let cr_refunded_customer_sk = (i % data.customer_count) + 1;
        let cr_returning_customer_sk = cr_refunded_customer_sk;
        let cr_reason_sk = (i % data.reason_count.min(15)) + 1;
        let cr_catalog_page_sk = (i % data.catalog_page_count) + 1;
        let cr_ship_mode_sk = (i % data.ship_mode_count.min(20)) + 1;
        let cr_warehouse_sk = (i % data.warehouse_count) + 1;
        let cr_order_number = (i / 3) + 1;  // ~3 items per return

        let return_quantity = data.random_i32(1, 10);
        let return_amt = data.random_f64(10.0, 500.0) * return_quantity as f64;
        let return_tax = return_amt * 0.08;
        let return_amt_inc_tax = return_amt + return_tax;
        let fee = data.random_f64(5.0, 25.0);
        let return_ship_cost = data.random_f64(5.0, 50.0);
        let refunded_cash = return_amt * 0.7;
        let reversed_charge = return_amt * 0.1;
        let store_credit = return_amt * 0.2;
        let net_loss = return_amt - refunded_cash - reversed_charge - store_credit + fee;

        let row = Row::new(vec![
            SqlValue::Integer(cr_returned_date_sk as i64),
            SqlValue::Integer(cr_returned_time_sk as i64),
            SqlValue::Integer(cr_item_sk as i64),
            SqlValue::Integer(cr_refunded_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // cr_refunded_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // cr_refunded_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // cr_refunded_addr_sk
            SqlValue::Integer(cr_returning_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // cr_returning_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // cr_returning_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // cr_returning_addr_sk
            SqlValue::Integer((i % 6 + 1) as i64),  // cr_call_center_sk
            SqlValue::Integer(cr_catalog_page_sk as i64),
            SqlValue::Integer(cr_ship_mode_sk as i64),
            SqlValue::Integer(cr_warehouse_sk as i64),
            SqlValue::Integer(cr_reason_sk as i64),
            SqlValue::Integer(cr_order_number as i64),
            SqlValue::Integer(return_quantity as i64),
            SqlValue::Numeric(return_amt),
            SqlValue::Numeric(return_tax),
            SqlValue::Numeric(return_amt_inc_tax),
            SqlValue::Numeric(fee),
            SqlValue::Numeric(return_ship_cost),
            SqlValue::Numeric(refunded_cash),
            SqlValue::Numeric(reversed_charge),
            SqlValue::Numeric(store_credit),
            SqlValue::Numeric(net_loss),
        ]);
        db.insert_row("catalog_returns", row).unwrap();
    }
}

fn load_web_sales_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.web_sales_count {
        let ws_sold_date_sk = (i % num_dates) + 1;
        let ws_sold_time_sk = (i % 24) * 3600;
        let ws_ship_date_sk = ws_sold_date_sk + data.random_i32(1, 30) as usize;
        let ws_item_sk = (i % data.item_count) + 1;
        let ws_bill_customer_sk = (i % data.customer_count) + 1;
        let ws_ship_customer_sk = ws_bill_customer_sk;
        let ws_promo_sk = (i % data.promotion_count) + 1;
        let ws_warehouse_sk = (i % data.warehouse_count) + 1;
        let ws_ship_mode_sk = (i % data.ship_mode_count.min(20)) + 1;
        let ws_web_page_sk = (i % data.web_page_count) + 1;
        let ws_web_site_sk = (i % data.web_site_count) + 1;
        let ws_order_number = (i / 5) + 1;  // ~5 items per order

        let quantity = data.random_i32(1, 20);
        let wholesale_cost = data.random_f64(10.0, 100.0);
        let list_price = wholesale_cost * 1.5;
        let sales_price = list_price * data.random_f64(0.8, 1.0);
        let ext_discount_amt = (list_price - sales_price) * quantity as f64;
        let ext_sales_price = sales_price * quantity as f64;
        let ext_wholesale_cost = wholesale_cost * quantity as f64;
        let ext_list_price = list_price * quantity as f64;
        let ext_tax = ext_sales_price * 0.08;
        let coupon_amt = data.random_f64(0.0, 20.0);
        let ext_ship_cost = data.random_f64(5.0, 30.0);
        let net_paid = ext_sales_price - coupon_amt;
        let net_paid_inc_tax = net_paid + ext_tax;
        let net_paid_inc_ship = net_paid + ext_ship_cost;
        let net_paid_inc_ship_tax = net_paid_inc_ship + ext_tax;
        let net_profit = net_paid - ext_wholesale_cost;

        let row = Row::new(vec![
            SqlValue::Integer(ws_sold_date_sk as i64),
            SqlValue::Integer(ws_sold_time_sk as i64),
            SqlValue::Integer(ws_ship_date_sk as i64),
            SqlValue::Integer(ws_item_sk as i64),
            SqlValue::Integer(ws_bill_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // ws_bill_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // ws_bill_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // ws_bill_addr_sk
            SqlValue::Integer(ws_ship_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // ws_ship_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // ws_ship_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // ws_ship_addr_sk
            SqlValue::Integer(ws_web_page_sk as i64),
            SqlValue::Integer(ws_web_site_sk as i64),
            SqlValue::Integer(ws_ship_mode_sk as i64),
            SqlValue::Integer(ws_warehouse_sk as i64),
            SqlValue::Integer(ws_promo_sk as i64),
            SqlValue::Integer(ws_order_number as i64),
            SqlValue::Integer(quantity as i64),
            SqlValue::Numeric(wholesale_cost),
            SqlValue::Numeric(list_price),
            SqlValue::Numeric(sales_price),
            SqlValue::Numeric(ext_discount_amt),
            SqlValue::Numeric(ext_sales_price),
            SqlValue::Numeric(ext_wholesale_cost),
            SqlValue::Numeric(ext_list_price),
            SqlValue::Numeric(ext_tax),
            SqlValue::Numeric(coupon_amt),
            SqlValue::Numeric(ext_ship_cost),
            SqlValue::Numeric(net_paid),
            SqlValue::Numeric(net_paid_inc_tax),
            SqlValue::Numeric(net_paid_inc_ship),
            SqlValue::Numeric(net_paid_inc_ship_tax),
            SqlValue::Numeric(net_profit),
        ]);
        db.insert_row("web_sales", row).unwrap();
    }
}

fn load_web_returns_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let num_dates = 2191.min(data.date_dim_count);

    for i in 0..data.web_returns_count {
        let wr_returned_date_sk = (i % num_dates) + 1;
        let wr_returned_time_sk = (i % 24) * 3600;
        let wr_item_sk = (i % data.item_count) + 1;
        let wr_refunded_customer_sk = (i % data.customer_count) + 1;
        let wr_returning_customer_sk = wr_refunded_customer_sk;
        let wr_reason_sk = (i % data.reason_count.min(15)) + 1;
        let wr_web_page_sk = (i % data.web_page_count) + 1;
        let wr_order_number = (i / 3) + 1;  // ~3 items per return

        let return_quantity = data.random_i32(1, 10);
        let return_amt = data.random_f64(10.0, 500.0) * return_quantity as f64;
        let return_tax = return_amt * 0.08;
        let return_amt_inc_tax = return_amt + return_tax;
        let fee = data.random_f64(5.0, 25.0);
        let return_ship_cost = data.random_f64(5.0, 50.0);
        let refunded_cash = return_amt * 0.7;
        let reversed_charge = return_amt * 0.1;
        let account_credit = return_amt * 0.2;
        let net_loss = return_amt - refunded_cash - reversed_charge - account_credit + fee;

        let row = Row::new(vec![
            SqlValue::Integer(wr_returned_date_sk as i64),
            SqlValue::Integer(wr_returned_time_sk as i64),
            SqlValue::Integer(wr_item_sk as i64),
            SqlValue::Integer(wr_refunded_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // wr_refunded_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // wr_refunded_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // wr_refunded_addr_sk
            SqlValue::Integer(wr_returning_customer_sk as i64),
            SqlValue::Integer((i % 1920 + 1) as i64),  // wr_returning_cdemo_sk
            SqlValue::Integer((i % 7200 + 1) as i64),  // wr_returning_hdemo_sk
            SqlValue::Integer(((i % data.customer_address_count) + 1) as i64),  // wr_returning_addr_sk
            SqlValue::Integer(wr_web_page_sk as i64),
            SqlValue::Integer(wr_reason_sk as i64),
            SqlValue::Integer(wr_order_number as i64),
            SqlValue::Integer(return_quantity as i64),
            SqlValue::Numeric(return_amt),
            SqlValue::Numeric(return_tax),
            SqlValue::Numeric(return_amt_inc_tax),
            SqlValue::Numeric(fee),
            SqlValue::Numeric(return_ship_cost),
            SqlValue::Numeric(refunded_cash),
            SqlValue::Numeric(reversed_charge),
            SqlValue::Numeric(account_credit),
            SqlValue::Numeric(net_loss),
        ]);
        db.insert_row("web_returns", row).unwrap();
    }
}

// =============================================================================
// Data Loading - CUSTOMER_DEMOGRAPHICS
// =============================================================================

fn load_customer_demographics_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Generate all combinations of demographic attributes
    // TPC-DS spec: 1920 combinations (2 genders * 5 marital * 7 education * ~28 other combos)
    let mut sk = 0;
    for gender in GENDERS.iter() {
        for marital in MARITAL_STATUS.iter() {
            for education in EDUCATION_STATUS.iter() {
                for &credit in CREDIT_RATINGS.iter() {
                    for &dep_count in &DEP_COUNTS[0..3] {
                        // Limit combinations
                        sk += 1;
                        if sk > data.customer_demographics_count {
                            return;
                        }

                        let purchase_estimate = data.random_i32(500, 10000);
                        let dep_employed = data.random_i32(0, dep_count);
                        let dep_college = data.random_i32(0, dep_count);

                        let row = Row::new(vec![
                            SqlValue::Integer(sk as i64),
                            SqlValue::Varchar(gender.to_string()),
                            SqlValue::Varchar(marital.to_string()),
                            SqlValue::Varchar(education.to_string()),
                            SqlValue::Integer(purchase_estimate as i64),
                            SqlValue::Varchar(credit.to_string()),
                            SqlValue::Integer(dep_count as i64),
                            SqlValue::Integer(dep_employed as i64),
                            SqlValue::Integer(dep_college as i64),
                        ]);
                        db.insert_row("customer_demographics", row).unwrap();
                    }
                }
            }
        }
    }
}

// =============================================================================
// Data Loading - HOUSEHOLD_DEMOGRAPHICS
// =============================================================================

fn load_household_demographics_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Generate combinations: income_band * buy_potential * dep_count * vehicle_count
    // TPC-DS spec: 7200 combinations
    let mut sk = 0;
    for income_band_sk in 1..=data.income_band_count.min(INCOME_BANDS.len()) {
        for &buy_potential in BUY_POTENTIALS.iter() {
            for &dep_count in DEP_COUNTS.iter() {
                for &vehicle_count in VEHICLE_COUNTS.iter() {
                    sk += 1;
                    if sk > data.household_demographics_count {
                        return;
                    }

                    let row = Row::new(vec![
                        SqlValue::Integer(sk as i64),
                        SqlValue::Integer(income_band_sk as i64),
                        SqlValue::Varchar(buy_potential.to_string()),
                        SqlValue::Integer(dep_count as i64),
                        SqlValue::Integer(vehicle_count as i64),
                    ]);
                    db.insert_row("household_demographics", row).unwrap();
                }
            }
        }
    }
}

// =============================================================================
// Data Loading - INCOME_BAND
// =============================================================================

fn load_income_band_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    for i in 0..data.income_band_count.min(INCOME_BANDS.len()) {
        let (lower, upper) = INCOME_BANDS[i];

        let row = Row::new(vec![
            SqlValue::Integer((i + 1) as i64),
            SqlValue::Integer(lower as i64),
            SqlValue::Integer(upper as i64),
        ]);
        db.insert_row("income_band", row).unwrap();
    }
}

// =============================================================================
// Data Loading - CALL_CENTER
// =============================================================================

fn load_call_center_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let classes = ["small", "medium", "large", "unknown"];
    let hours = ["8AM-4PM", "8AM-8PM", "8AM-12AM"];

    for i in 1..=data.call_center_count {
        let cc_call_center_id = format!("AAAAAA{:010}", i);
        let class_idx = i % classes.len();
        let hours_idx = i % hours.len();

        let open_date_sk = data.random_i32(1, 500);
        let employees = data.random_i32(10, 500);
        let sq_ft = data.random_i32(1000, 50000);
        let state_idx = i % STATES.len();
        let gmt_offset = -5.0 - (state_idx % 4) as f64;

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Varchar(cc_call_center_id),
            SqlValue::Null, // cc_rec_start_date
            SqlValue::Null, // cc_rec_end_date
            SqlValue::Null, // cc_closed_date_sk
            SqlValue::Integer(open_date_sk as i64),
            SqlValue::Varchar(format!("Call Center {}", i)),
            SqlValue::Varchar(classes[class_idx].to_string()),
            SqlValue::Integer(employees as i64),
            SqlValue::Integer(sq_ft as i64),
            SqlValue::Varchar(hours[hours_idx].to_string()),
            SqlValue::Varchar(data.random_varchar(30)),
            SqlValue::Integer((i % 6 + 1) as i64), // cc_mkt_id
            SqlValue::Varchar(format!("Market Class {}", i % 10)),
            SqlValue::Varchar(format!("Market Description {}", i)),
            SqlValue::Varchar(data.random_varchar(30)),
            SqlValue::Integer((i % 3 + 1) as i64), // cc_division
            SqlValue::Varchar(format!("Division {}", i % 3 + 1)),
            SqlValue::Integer((i % 2 + 1) as i64), // cc_company
            SqlValue::Varchar(format!("Company {}", i % 2 + 1)),
            SqlValue::Varchar(format!("{}", data.random_i32(100, 9999))),
            SqlValue::Varchar(data.random_varchar(40)),
            SqlValue::Varchar("Street".to_string()),
            SqlValue::Varchar(format!("Suite {}", data.random_i32(100, 999))),
            SqlValue::Varchar(data.random_city()),
            SqlValue::Varchar(format!("{} County", STATES[state_idx])),
            SqlValue::Varchar(STATES[state_idx].to_string()),
            SqlValue::Varchar(data.random_zip()),
            SqlValue::Varchar("United States".to_string()),
            SqlValue::Numeric(gmt_offset),
            SqlValue::Numeric(data.random_f64(0.0, 0.11)),
        ]);
        db.insert_row("call_center", row).unwrap();
    }
}

// =============================================================================
// Data Loading - INVENTORY
// =============================================================================

fn load_inventory_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // Inventory is a fact table with date_sk x item_sk x warehouse_sk
    // Generate inventory snapshots for a subset of dates
    let num_dates = 52.min(data.date_dim_count); // Weekly snapshots for 1 year
    let num_items = data.item_count;
    let num_warehouses = data.warehouse_count;

    let mut count = 0;
    for week in 0..num_dates {
        let date_sk = week * 7 + 1; // Weekly snapshots

        // Sample items and warehouses to control data size
        let items_per_week = (data.inventory_count / num_dates).min(num_items * num_warehouses);
        let items_sample = (items_per_week / num_warehouses).max(1);

        for item_offset in 0..items_sample {
            let item_sk = (item_offset % num_items) + 1;

            for warehouse_sk in 1..=num_warehouses {
                count += 1;
                if count > data.inventory_count {
                    return;
                }

                let quantity = data.random_i32(0, 1000);

                let row = Row::new(vec![
                    SqlValue::Integer(date_sk as i64),
                    SqlValue::Integer(item_sk as i64),
                    SqlValue::Integer(warehouse_sk as i64),
                    SqlValue::Integer(quantity as i64),
                ]);
                db.insert_row("inventory", row).unwrap();
            }
        }
    }
}

// =============================================================================
// SQLite Schema and Data Loading (for benchmark comparison)
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_tpcds_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE date_dim (
            d_date_sk INTEGER PRIMARY KEY,
            d_date_id TEXT NOT NULL,
            d_date TEXT,
            d_month_seq INTEGER,
            d_week_seq INTEGER,
            d_quarter_seq INTEGER,
            d_year INTEGER,
            d_dow INTEGER,
            d_moy INTEGER,
            d_dom INTEGER,
            d_qoy INTEGER,
            d_fy_year INTEGER,
            d_fy_quarter_seq INTEGER,
            d_fy_week_seq INTEGER,
            d_day_name TEXT,
            d_quarter_name TEXT,
            d_holiday TEXT,
            d_weekend TEXT,
            d_following_holiday TEXT,
            d_first_dom INTEGER,
            d_last_dom INTEGER,
            d_same_day_ly INTEGER,
            d_same_day_lq INTEGER,
            d_current_day TEXT,
            d_current_week TEXT,
            d_current_month TEXT,
            d_current_quarter TEXT,
            d_current_year TEXT
        );

        CREATE TABLE time_dim (
            t_time_sk INTEGER PRIMARY KEY,
            t_time_id TEXT NOT NULL,
            t_time INTEGER,
            t_hour INTEGER,
            t_minute INTEGER,
            t_second INTEGER,
            t_am_pm TEXT,
            t_shift TEXT,
            t_sub_shift TEXT,
            t_meal_time TEXT
        );

        CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY,
            i_item_id TEXT NOT NULL,
            i_rec_start_date TEXT,
            i_rec_end_date TEXT,
            i_item_desc TEXT,
            i_current_price REAL,
            i_wholesale_cost REAL,
            i_brand_id INTEGER,
            i_brand TEXT,
            i_class_id INTEGER,
            i_class TEXT,
            i_category_id INTEGER,
            i_category TEXT,
            i_manufact_id INTEGER,
            i_manufact TEXT,
            i_size TEXT,
            i_formulation TEXT,
            i_color TEXT,
            i_units TEXT,
            i_container TEXT,
            i_manager_id INTEGER,
            i_product_name TEXT
        );

        CREATE TABLE customer_address (
            ca_address_sk INTEGER PRIMARY KEY,
            ca_address_id TEXT NOT NULL,
            ca_street_number TEXT,
            ca_street_name TEXT,
            ca_street_type TEXT,
            ca_suite_number TEXT,
            ca_city TEXT,
            ca_county TEXT,
            ca_state TEXT,
            ca_zip TEXT,
            ca_country TEXT,
            ca_gmt_offset REAL,
            ca_location_type TEXT
        );

        CREATE TABLE customer (
            c_customer_sk INTEGER PRIMARY KEY,
            c_customer_id TEXT NOT NULL,
            c_current_cdemo_sk INTEGER,
            c_current_hdemo_sk INTEGER,
            c_current_addr_sk INTEGER,
            c_first_shipto_date_sk INTEGER,
            c_first_sales_date_sk INTEGER,
            c_salutation TEXT,
            c_first_name TEXT,
            c_last_name TEXT,
            c_preferred_cust_flag TEXT,
            c_birth_day INTEGER,
            c_birth_month INTEGER,
            c_birth_year INTEGER,
            c_birth_country TEXT,
            c_login TEXT,
            c_email_address TEXT,
            c_last_review_date_sk INTEGER
        );

        CREATE TABLE store (
            s_store_sk INTEGER PRIMARY KEY,
            s_store_id TEXT NOT NULL,
            s_rec_start_date TEXT,
            s_rec_end_date TEXT,
            s_closed_date_sk INTEGER,
            s_store_name TEXT,
            s_number_employees INTEGER,
            s_floor_space INTEGER,
            s_hours TEXT,
            s_manager TEXT,
            s_market_id INTEGER,
            s_geography_class TEXT,
            s_market_desc TEXT,
            s_market_manager TEXT,
            s_division_id INTEGER,
            s_division_name TEXT,
            s_company_id INTEGER,
            s_company_name TEXT,
            s_street_number TEXT,
            s_street_name TEXT,
            s_street_type TEXT,
            s_suite_number TEXT,
            s_city TEXT,
            s_county TEXT,
            s_state TEXT,
            s_zip TEXT,
            s_country TEXT,
            s_gmt_offset REAL,
            s_tax_percentage REAL
        );

        CREATE TABLE store_sales (
            ss_sold_date_sk INTEGER,
            ss_sold_time_sk INTEGER,
            ss_item_sk INTEGER NOT NULL,
            ss_customer_sk INTEGER,
            ss_cdemo_sk INTEGER,
            ss_hdemo_sk INTEGER,
            ss_addr_sk INTEGER,
            ss_store_sk INTEGER,
            ss_promo_sk INTEGER,
            ss_ticket_number INTEGER NOT NULL,
            ss_quantity INTEGER,
            ss_wholesale_cost REAL,
            ss_list_price REAL,
            ss_sales_price REAL,
            ss_ext_discount_amt REAL,
            ss_ext_sales_price REAL,
            ss_ext_wholesale_cost REAL,
            ss_ext_list_price REAL,
            ss_ext_tax REAL,
            ss_coupon_amt REAL,
            ss_net_paid REAL,
            ss_net_paid_inc_tax REAL,
            ss_net_profit REAL,
            PRIMARY KEY (ss_item_sk, ss_ticket_number)
        );

        CREATE INDEX idx_ss_date ON store_sales(ss_sold_date_sk);
        CREATE INDEX idx_ss_customer ON store_sales(ss_customer_sk);
        CREATE INDEX idx_ss_store ON store_sales(ss_store_sk);
    "#,
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_date_dim_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    let num_dates = 2191.min(data.date_dim_count);

    let mut stmt = conn.prepare(
        "INSERT INTO date_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for d_date_sk in 1..=num_dates {
        let days_since_base = d_date_sk as i64 - 1;
        let year = 1998 + (days_since_base / 365) as i32;
        let day_of_year = (days_since_base % 365) as i32;
        let month = (day_of_year / 30).min(11) + 1;
        let day = (day_of_year % 30) + 1;
        let date_str = format!("{:04}-{:02}-{:02}", year, month, day);
        let d_date_id = format!("AAAAAA{:010}", d_date_sk);
        let d_dow = (days_since_base % 7) as i32;
        let d_week_seq = (days_since_base / 7) as i32 + 1;
        let d_month_seq = (year - 1998) * 12 + month;
        let d_quarter_seq = (year - 1998) * 4 + ((month - 1) / 3) + 1;
        let d_qoy = ((month - 1) / 3) + 1;
        let quarter_name = format!("{}Q{}", year, d_qoy);
        let is_weekend = d_dow == 0 || d_dow == 6;

        stmt.execute(rusqlite::params![
            d_date_sk,
            d_date_id,
            date_str,
            d_month_seq,
            d_week_seq,
            d_quarter_seq,
            year,
            d_dow,
            month,
            day,
            d_qoy,
            year,
            d_quarter_seq,
            d_week_seq,
            day_names[d_dow as usize],
            quarter_name,
            "N",
            if is_weekend { "Y" } else { "N" },
            "N",
            (d_month_seq - 1) * 30 + 1,
            d_month_seq * 30,
            (d_date_sk as i64 - 365).max(1),
            (d_date_sk as i64 - 91).max(1),
            "N", "N", "N", "N", "N"
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_time_dim_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let num_times = 24.min(data.time_dim_count / 3600).max(24);

    let mut stmt = conn.prepare(
        "INSERT INTO time_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for hour in 0..num_times {
        let t_time_sk = hour * 3600;
        let t_time_id = format!("AAAAAA{:010}", t_time_sk);
        let am_pm = if hour < 12 { "AM" } else { "PM" };
        let shift = if hour < 8 { "third" } else if hour < 16 { "first" } else { "second" };
        let sub_shift = if hour % 8 < 4 { "night" } else { "day" };
        let meal_time = if hour >= 7 && hour < 9 { "breakfast" }
            else if hour >= 12 && hour < 14 { "lunch" }
            else if hour >= 18 && hour < 20 { "dinner" }
            else { "" };

        stmt.execute(rusqlite::params![
            t_time_sk, t_time_id, t_time_sk, hour, 0, 0, am_pm, shift, sub_shift, meal_time
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_item_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.item_count {
        let i_item_id = format!("AAAAAA{:010}", i);
        let category_idx = i % CATEGORIES.len();
        let class_idx = i % CLASSES.len();
        let brand_idx = i % BRANDS.len();
        let color_idx = i % ITEM_COLORS.len();
        let size_idx = i % ITEM_SIZES.len();
        let current_price = data.random_f64(1.0, 200.0);
        let wholesale_cost = current_price * 0.5;

        stmt.execute(rusqlite::params![
            i,
            i_item_id,
            "1998-01-01",
            rusqlite::types::Null,
            format!("{} {} item", CATEGORIES[category_idx], CLASSES[class_idx]),
            current_price,
            wholesale_cost,
            brand_idx + 1,
            BRANDS[brand_idx],
            class_idx + 1,
            CLASSES[class_idx],
            category_idx + 1,
            CATEGORIES[category_idx],
            (i % 10) + 1,
            format!("Manufacturer#{}", (i % 10) + 1),
            ITEM_SIZES[size_idx],
            format!("formula{}", i % 20),
            ITEM_COLORS[color_idx],
            "Each",
            "Unknown",
            (i % 100) + 1,
            format!("Product#{}", i)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_address_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO customer_address VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.customer_address_count {
        let ca_address_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        stmt.execute(rusqlite::params![
            i,
            ca_address_id,
            format!("{}", data.random_i32(1, 999)),
            data.random_varchar(30),
            "Street",
            format!("Suite {}", data.random_i32(100, 999)),
            data.random_city(),
            format!("{} County", STATES[state_idx]),
            STATES[state_idx],
            data.random_zip(),
            "United States",
            -5.0 + (state_idx as f64 * 0.1),
            "residential"
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let salutations = ["Mr.", "Mrs.", "Ms.", "Dr.", ""];

    let mut stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.customer_count {
        let c_customer_id = format!("AAAAAA{:010}", i);
        let sal_idx = i % salutations.len();
        let birth_year = data.random_i32(1930, 1990);
        let birth_month = data.random_i32(1, 12);
        let birth_day = data.random_i32(1, 28);
        let addr_sk = ((i - 1) % data.customer_address_count) + 1;

        stmt.execute(rusqlite::params![
            i,
            c_customer_id,
            i % 1920 + 1,
            i % 7200 + 1,
            addr_sk,
            data.random_i32(1, 2191),
            data.random_i32(1, 2191),
            salutations[sal_idx],
            format!("FirstName{}", i % 1000),
            format!("LastName{}", i % 2000),
            if i % 3 == 0 { "Y" } else { "N" },
            birth_day,
            birth_month,
            birth_year,
            "UNITED STATES",
            format!("user{}", i),
            data.random_email(),
            data.random_i32(1, 2191)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_store_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO store VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.store_count {
        let s_store_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        stmt.execute(rusqlite::params![
            i,
            s_store_id,
            "1998-01-01",
            rusqlite::types::Null,
            rusqlite::types::Null,
            format!("Store#{}", i),
            data.random_i32(50, 500),
            data.random_i32(5000, 50000),
            "8AM-10PM",
            format!("Manager{}", i % 100),
            i % 10 + 1,
            "Unknown",
            "Market description",
            format!("MarketManager{}", i % 50),
            i % 5 + 1,
            format!("Division{}", i % 5 + 1),
            i % 3 + 1,
            format!("Company{}", i % 3 + 1),
            format!("{}", data.random_i32(1, 999)),
            data.random_varchar(30),
            "Avenue",
            format!("Suite {}", data.random_i32(100, 999)),
            data.random_city(),
            format!("{} County", STATES[state_idx]),
            STATES[state_idx],
            data.random_zip(),
            "United States",
            -5.0 + (state_idx as f64 * 0.1),
            data.random_f64(0.0, 0.11)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_store_sales_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let num_dates = 2191.min(data.date_dim_count);

    let mut stmt = conn.prepare(
        "INSERT INTO store_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.store_sales_count {
        let ss_sold_date_sk = (i % num_dates) + 1;
        let ss_sold_time_sk = (i % 24) * 3600;
        let ss_item_sk = (i % data.item_count) + 1;
        let ss_customer_sk = (i % data.customer_count) + 1;
        let ss_store_sk = (i % data.store_count) + 1;
        let ss_ticket_number = (i / 5) + 1;

        let quantity = data.random_i32(1, 100);
        let wholesale_cost = data.random_f64(1.0, 50.0);
        let list_price = wholesale_cost * data.random_f64(1.5, 3.0);
        let sales_price = list_price * data.random_f64(0.8, 1.0);
        let ext_sales_price = sales_price * quantity as f64;
        let ext_wholesale_cost = wholesale_cost * quantity as f64;
        let ext_list_price = list_price * quantity as f64;
        let ext_discount_amt = ext_list_price - ext_sales_price;
        let ext_tax = ext_sales_price * 0.08;
        let coupon_amt = if i % 10 == 0 { ext_sales_price * 0.05 } else { 0.0 };
        let net_paid = ext_sales_price - coupon_amt;
        let net_paid_inc_tax = net_paid + ext_tax;
        let net_profit = net_paid - ext_wholesale_cost;

        stmt.execute(rusqlite::params![
            ss_sold_date_sk,
            ss_sold_time_sk,
            ss_item_sk,
            ss_customer_sk,
            i % 1920 + 1,
            i % 7200 + 1,
            (i % data.customer_address_count) + 1,
            ss_store_sk,
            i % 300 + 1,
            ss_ticket_number,
            quantity,
            wholesale_cost,
            list_price,
            sales_price,
            ext_discount_amt,
            ext_sales_price,
            ext_wholesale_cost,
            ext_list_price,
            ext_tax,
            coupon_amt,
            net_paid,
            net_paid_inc_tax,
            net_profit
        ]).unwrap();
    }
}

// =============================================================================
// DuckDB Schema and Data Loading (for benchmark comparison)
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_tpcds_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE date_dim (
            d_date_sk INTEGER PRIMARY KEY,
            d_date_id VARCHAR(16) NOT NULL,
            d_date DATE,
            d_month_seq INTEGER,
            d_week_seq INTEGER,
            d_quarter_seq INTEGER,
            d_year INTEGER,
            d_dow INTEGER,
            d_moy INTEGER,
            d_dom INTEGER,
            d_qoy INTEGER,
            d_fy_year INTEGER,
            d_fy_quarter_seq INTEGER,
            d_fy_week_seq INTEGER,
            d_day_name VARCHAR(9),
            d_quarter_name VARCHAR(6),
            d_holiday VARCHAR(1),
            d_weekend VARCHAR(1),
            d_following_holiday VARCHAR(1),
            d_first_dom INTEGER,
            d_last_dom INTEGER,
            d_same_day_ly INTEGER,
            d_same_day_lq INTEGER,
            d_current_day VARCHAR(1),
            d_current_week VARCHAR(1),
            d_current_month VARCHAR(1),
            d_current_quarter VARCHAR(1),
            d_current_year VARCHAR(1)
        );

        CREATE TABLE time_dim (
            t_time_sk INTEGER PRIMARY KEY,
            t_time_id VARCHAR(16) NOT NULL,
            t_time INTEGER,
            t_hour INTEGER,
            t_minute INTEGER,
            t_second INTEGER,
            t_am_pm VARCHAR(2),
            t_shift VARCHAR(20),
            t_sub_shift VARCHAR(20),
            t_meal_time VARCHAR(20)
        );

        CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY,
            i_item_id VARCHAR(16) NOT NULL,
            i_rec_start_date DATE,
            i_rec_end_date DATE,
            i_item_desc VARCHAR(200),
            i_current_price DECIMAL(7,2),
            i_wholesale_cost DECIMAL(7,2),
            i_brand_id INTEGER,
            i_brand VARCHAR(50),
            i_class_id INTEGER,
            i_class VARCHAR(50),
            i_category_id INTEGER,
            i_category VARCHAR(50),
            i_manufact_id INTEGER,
            i_manufact VARCHAR(50),
            i_size VARCHAR(20),
            i_formulation VARCHAR(20),
            i_color VARCHAR(20),
            i_units VARCHAR(10),
            i_container VARCHAR(10),
            i_manager_id INTEGER,
            i_product_name VARCHAR(50)
        );

        CREATE TABLE customer_address (
            ca_address_sk INTEGER PRIMARY KEY,
            ca_address_id VARCHAR(16) NOT NULL,
            ca_street_number VARCHAR(10),
            ca_street_name VARCHAR(60),
            ca_street_type VARCHAR(15),
            ca_suite_number VARCHAR(10),
            ca_city VARCHAR(60),
            ca_county VARCHAR(30),
            ca_state VARCHAR(2),
            ca_zip VARCHAR(10),
            ca_country VARCHAR(20),
            ca_gmt_offset DECIMAL(5,2),
            ca_location_type VARCHAR(20)
        );

        CREATE TABLE customer (
            c_customer_sk INTEGER PRIMARY KEY,
            c_customer_id VARCHAR(16) NOT NULL,
            c_current_cdemo_sk INTEGER,
            c_current_hdemo_sk INTEGER,
            c_current_addr_sk INTEGER,
            c_first_shipto_date_sk INTEGER,
            c_first_sales_date_sk INTEGER,
            c_salutation VARCHAR(10),
            c_first_name VARCHAR(20),
            c_last_name VARCHAR(30),
            c_preferred_cust_flag VARCHAR(1),
            c_birth_day INTEGER,
            c_birth_month INTEGER,
            c_birth_year INTEGER,
            c_birth_country VARCHAR(20),
            c_login VARCHAR(13),
            c_email_address VARCHAR(50),
            c_last_review_date_sk INTEGER
        );

        CREATE TABLE store (
            s_store_sk INTEGER PRIMARY KEY,
            s_store_id VARCHAR(16) NOT NULL,
            s_rec_start_date DATE,
            s_rec_end_date DATE,
            s_closed_date_sk INTEGER,
            s_store_name VARCHAR(50),
            s_number_employees INTEGER,
            s_floor_space INTEGER,
            s_hours VARCHAR(20),
            s_manager VARCHAR(40),
            s_market_id INTEGER,
            s_geography_class VARCHAR(100),
            s_market_desc VARCHAR(100),
            s_market_manager VARCHAR(40),
            s_division_id INTEGER,
            s_division_name VARCHAR(50),
            s_company_id INTEGER,
            s_company_name VARCHAR(50),
            s_street_number VARCHAR(10),
            s_street_name VARCHAR(60),
            s_street_type VARCHAR(15),
            s_suite_number VARCHAR(10),
            s_city VARCHAR(60),
            s_county VARCHAR(30),
            s_state VARCHAR(2),
            s_zip VARCHAR(10),
            s_country VARCHAR(20),
            s_gmt_offset DECIMAL(5,2),
            s_tax_percentage DECIMAL(5,2)
        );

        CREATE TABLE store_sales (
            ss_sold_date_sk INTEGER,
            ss_sold_time_sk INTEGER,
            ss_item_sk INTEGER NOT NULL,
            ss_customer_sk INTEGER,
            ss_cdemo_sk INTEGER,
            ss_hdemo_sk INTEGER,
            ss_addr_sk INTEGER,
            ss_store_sk INTEGER,
            ss_promo_sk INTEGER,
            ss_ticket_number INTEGER NOT NULL,
            ss_quantity INTEGER,
            ss_wholesale_cost DECIMAL(7,2),
            ss_list_price DECIMAL(7,2),
            ss_sales_price DECIMAL(7,2),
            ss_ext_discount_amt DECIMAL(7,2),
            ss_ext_sales_price DECIMAL(7,2),
            ss_ext_wholesale_cost DECIMAL(7,2),
            ss_ext_list_price DECIMAL(7,2),
            ss_ext_tax DECIMAL(7,2),
            ss_coupon_amt DECIMAL(7,2),
            ss_net_paid DECIMAL(7,2),
            ss_net_paid_inc_tax DECIMAL(7,2),
            ss_net_profit DECIMAL(7,2),
            PRIMARY KEY (ss_item_sk, ss_ticket_number)
        );

        CREATE INDEX idx_ss_date ON store_sales(ss_sold_date_sk);
        CREATE INDEX idx_ss_customer ON store_sales(ss_customer_sk);
        CREATE INDEX idx_ss_store ON store_sales(ss_store_sk);
    "#,
    )
    .unwrap();
}

#[cfg(feature = "benchmark-comparison")]
fn load_date_dim_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    let num_dates = 2191.min(data.date_dim_count);

    let mut stmt = conn.prepare(
        "INSERT INTO date_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for d_date_sk in 1..=num_dates {
        let days_since_base = d_date_sk as i64 - 1;
        let year = 1998 + (days_since_base / 365) as i32;
        let day_of_year = (days_since_base % 365) as i32;
        let month = (day_of_year / 30).min(11) + 1;
        let day = (day_of_year % 30) + 1;
        let date_str = format!("{:04}-{:02}-{:02}", year, month, day);
        let d_date_id = format!("AAAAAA{:010}", d_date_sk);
        let d_dow = (days_since_base % 7) as i32;
        let d_week_seq = (days_since_base / 7) as i32 + 1;
        let d_month_seq = (year - 1998) * 12 + month;
        let d_quarter_seq = (year - 1998) * 4 + ((month - 1) / 3) + 1;
        let d_qoy = ((month - 1) / 3) + 1;
        let quarter_name = format!("{}Q{}", year, d_qoy);
        let is_weekend = d_dow == 0 || d_dow == 6;

        stmt.execute(duckdb::params![
            d_date_sk,
            d_date_id,
            date_str,
            d_month_seq,
            d_week_seq,
            d_quarter_seq,
            year,
            d_dow,
            month,
            day,
            d_qoy,
            year,
            d_quarter_seq,
            d_week_seq,
            day_names[d_dow as usize],
            quarter_name,
            "N",
            if is_weekend { "Y" } else { "N" },
            "N",
            (d_month_seq - 1) * 30 + 1,
            d_month_seq * 30,
            (d_date_sk as i64 - 365).max(1),
            (d_date_sk as i64 - 91).max(1),
            "N", "N", "N", "N", "N"
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_time_dim_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let num_times = 24.min(data.time_dim_count / 3600).max(24);

    let mut stmt = conn.prepare(
        "INSERT INTO time_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for hour in 0..num_times {
        let t_time_sk = hour * 3600;
        let t_time_id = format!("AAAAAA{:010}", t_time_sk);
        let am_pm = if hour < 12 { "AM" } else { "PM" };
        let shift = if hour < 8 { "third" } else if hour < 16 { "first" } else { "second" };
        let sub_shift = if hour % 8 < 4 { "night" } else { "day" };
        let meal_time = if hour >= 7 && hour < 9 { "breakfast" }
            else if hour >= 12 && hour < 14 { "lunch" }
            else if hour >= 18 && hour < 20 { "dinner" }
            else { "" };

        stmt.execute(duckdb::params![
            t_time_sk, t_time_id, t_time_sk, hour, 0, 0, am_pm, shift, sub_shift, meal_time
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_item_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO item VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.item_count {
        let i_item_id = format!("AAAAAA{:010}", i);
        let category_idx = i % CATEGORIES.len();
        let class_idx = i % CLASSES.len();
        let brand_idx = i % BRANDS.len();
        let color_idx = i % ITEM_COLORS.len();
        let size_idx = i % ITEM_SIZES.len();
        let current_price = data.random_f64(1.0, 200.0);
        let wholesale_cost = current_price * 0.5;

        stmt.execute(duckdb::params![
            i as i64,
            i_item_id,
            "1998-01-01",
            duckdb::types::Null,
            format!("{} {} item", CATEGORIES[category_idx], CLASSES[class_idx]),
            current_price,
            wholesale_cost,
            brand_idx + 1,
            BRANDS[brand_idx],
            class_idx + 1,
            CLASSES[class_idx],
            category_idx + 1,
            CATEGORIES[category_idx],
            (i % 10) + 1,
            format!("Manufacturer#{}", (i % 10) + 1),
            ITEM_SIZES[size_idx],
            format!("formula{}", i % 20),
            ITEM_COLORS[color_idx],
            "Each",
            "Unknown",
            (i % 100) + 1,
            format!("Product#{}", i)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_address_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO customer_address VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.customer_address_count {
        let ca_address_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        stmt.execute(duckdb::params![
            i as i64,
            ca_address_id,
            format!("{}", data.random_i32(1, 999)),
            data.random_varchar(30),
            "Street",
            format!("Suite {}", data.random_i32(100, 999)),
            data.random_city(),
            format!("{} County", STATES[state_idx]),
            STATES[state_idx],
            data.random_zip(),
            "United States",
            -5.0 + (state_idx as f64 * 0.1),
            "residential"
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let salutations = ["Mr.", "Mrs.", "Ms.", "Dr.", ""];

    let mut stmt = conn.prepare(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.customer_count {
        let c_customer_id = format!("AAAAAA{:010}", i);
        let sal_idx = i % salutations.len();
        let birth_year = data.random_i32(1930, 1990);
        let birth_month = data.random_i32(1, 12);
        let birth_day = data.random_i32(1, 28);
        let addr_sk = ((i - 1) % data.customer_address_count) + 1;

        stmt.execute(duckdb::params![
            i as i64,
            c_customer_id,
            i % 1920 + 1,
            i % 7200 + 1,
            addr_sk,
            data.random_i32(1, 2191),
            data.random_i32(1, 2191),
            salutations[sal_idx],
            format!("FirstName{}", i % 1000),
            format!("LastName{}", i % 2000),
            if i % 3 == 0 { "Y" } else { "N" },
            birth_day,
            birth_month,
            birth_year,
            "UNITED STATES",
            format!("user{}", i),
            data.random_email(),
            data.random_i32(1, 2191)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_store_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn.prepare(
        "INSERT INTO store VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 1..=data.store_count {
        let s_store_id = format!("AAAAAA{:010}", i);
        let state_idx = i % STATES.len();

        stmt.execute(duckdb::params![
            i as i64,
            s_store_id,
            "1998-01-01",
            duckdb::types::Null,
            duckdb::types::Null,
            format!("Store#{}", i),
            data.random_i32(50, 500),
            data.random_i32(5000, 50000),
            "8AM-10PM",
            format!("Manager{}", i % 100),
            i % 10 + 1,
            "Unknown",
            "Market description",
            format!("MarketManager{}", i % 50),
            i % 5 + 1,
            format!("Division{}", i % 5 + 1),
            i % 3 + 1,
            format!("Company{}", i % 3 + 1),
            format!("{}", data.random_i32(1, 999)),
            data.random_varchar(30),
            "Avenue",
            format!("Suite {}", data.random_i32(100, 999)),
            data.random_city(),
            format!("{} County", STATES[state_idx]),
            STATES[state_idx],
            data.random_zip(),
            "United States",
            -5.0 + (state_idx as f64 * 0.1),
            data.random_f64(0.0, 0.11)
        ]).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_store_sales_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let num_dates = 2191.min(data.date_dim_count);

    let mut stmt = conn.prepare(
        "INSERT INTO store_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).unwrap();

    for i in 0..data.store_sales_count {
        let ss_sold_date_sk = (i % num_dates) + 1;
        let ss_sold_time_sk = (i % 24) * 3600;
        let ss_item_sk = (i % data.item_count) + 1;
        let ss_customer_sk = (i % data.customer_count) + 1;
        let ss_store_sk = (i % data.store_count) + 1;
        let ss_ticket_number = (i / 5) + 1;

        let quantity = data.random_i32(1, 100);
        let wholesale_cost = data.random_f64(1.0, 50.0);
        let list_price = wholesale_cost * data.random_f64(1.5, 3.0);
        let sales_price = list_price * data.random_f64(0.8, 1.0);
        let ext_sales_price = sales_price * quantity as f64;
        let ext_wholesale_cost = wholesale_cost * quantity as f64;
        let ext_list_price = list_price * quantity as f64;
        let ext_discount_amt = ext_list_price - ext_sales_price;
        let ext_tax = ext_sales_price * 0.08;
        let coupon_amt = if i % 10 == 0 { ext_sales_price * 0.05 } else { 0.0 };
        let net_paid = ext_sales_price - coupon_amt;
        let net_paid_inc_tax = net_paid + ext_tax;
        let net_profit = net_paid - ext_wholesale_cost;

        stmt.execute(duckdb::params![
            ss_sold_date_sk as i64,
            ss_sold_time_sk as i64,
            ss_item_sk as i64,
            ss_customer_sk as i64,
            (i % 1920 + 1) as i64,
            (i % 7200 + 1) as i64,
            ((i % data.customer_address_count) + 1) as i64,
            ss_store_sk as i64,
            (i % 300 + 1) as i64,
            ss_ticket_number as i64,
            quantity,
            wholesale_cost,
            list_price,
            sales_price,
            ext_discount_amt,
            ext_sales_price,
            ext_wholesale_cost,
            ext_list_price,
            ext_tax,
            coupon_amt,
            net_paid,
            net_paid_inc_tax,
            net_profit
        ]).unwrap();
    }
}
