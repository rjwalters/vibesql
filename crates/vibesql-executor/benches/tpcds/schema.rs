//! TPC-DS Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-DS
//! benchmark tables. Currently implements Phase 1 tables:
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
//! Additional tables will be added in future phases.

use super::data::{
    TPCDSData, BRANDS, CATEGORIES, CLASSES, CREDIT_RATINGS, GENDERS, ITEM_COLORS, ITEM_SIZES,
    MARITAL_STATUS, STATES,
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

    // Load fact tables
    load_store_sales_vibesql(&mut db, &mut data);

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
        "store_sales",
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
        let gender_idx = i % GENDERS.len();
        let marital_idx = i % MARITAL_STATUS.len();
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
