//! TPC-DS Schema Creation and Data Loading
//!
//! This module provides schema creation and data loading functions for TPC-DS
//! benchmark tables across multiple database engines (VibeSQL, SQLite, DuckDB).
//!
//! This implementation focuses on the demographic dimension tables:
//! - INCOME_BAND: Income band reference table (20 rows)
//! - CUSTOMER_DEMOGRAPHICS: Customer demographic combinations (~1,920 rows)
//! - HOUSEHOLD_DEMOGRAPHICS: Household demographic combinations (~7,200 rows)
//! - CALL_CENTER: Call center information (~6 rows at SF=1)
//! - INVENTORY: Inventory snapshots (fact table with dimension-like usage)

use super::data::{
    TPCDSData, CC_CLASSES, CC_HOURS, CD_CREDIT_RATINGS, CD_EDUCATION_STATUS, CD_GENDERS,
    CD_MARITAL_STATUS, HD_BUY_POTENTIALS,
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

    // Load data
    load_income_band_vibesql(&mut db);
    load_customer_demographics_vibesql(&mut db, &mut data);
    load_household_demographics_vibesql(&mut db, &mut data);
    load_call_center_vibesql(&mut db, &mut data);
    load_inventory_vibesql(&mut db, &mut data);

    // Create indexes for primary keys
    create_tpcds_indexes_vibesql(&mut db);

    // Compute statistics for join order optimization
    for table_name in [
        "INCOME_BAND",
        "CUSTOMER_DEMOGRAPHICS",
        "HOUSEHOLD_DEMOGRAPHICS",
        "CALL_CENTER",
        "INVENTORY",
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

    // Create schema
    create_tpcds_schema_sqlite(&conn);

    // Load data
    load_income_band_sqlite(&conn);
    load_customer_demographics_sqlite(&conn, &mut data);
    load_household_demographics_sqlite(&conn, &mut data);
    load_call_center_sqlite(&conn, &mut data);
    load_inventory_sqlite(&conn, &mut data);

    conn
}

#[cfg(feature = "benchmark-comparison")]
pub fn load_duckdb(scale_factor: f64) -> DuckDBConn {
    let conn = DuckDBConn::open_in_memory().unwrap();
    let mut data = TPCDSData::new(scale_factor);

    // Create schema
    create_tpcds_schema_duckdb(&conn);

    // Load data
    load_income_band_duckdb(&conn);
    load_customer_demographics_duckdb(&conn, &mut data);
    load_household_demographics_duckdb(&conn, &mut data);
    load_call_center_duckdb(&conn, &mut data);
    load_inventory_duckdb(&conn, &mut data);

    conn
}

// =============================================================================
// Schema Creation - VibeSQL
// =============================================================================

fn create_tpcds_schema_vibesql(db: &mut VibeDB) {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    // INCOME_BAND table (20 rows)
    db.create_table(TableSchema::new(
        "INCOME_BAND".to_string(),
        vec![
            ColumnSchema {
                name: "IB_INCOME_BAND_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "IB_LOWER_BOUND".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "IB_UPPER_BOUND".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CUSTOMER_DEMOGRAPHICS table (~1,920 rows)
    db.create_table(TableSchema::new(
        "CUSTOMER_DEMOGRAPHICS".to_string(),
        vec![
            ColumnSchema {
                name: "CD_DEMO_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_GENDER".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_MARITAL_STATUS".to_string(),
                data_type: DataType::Varchar { max_length: Some(1) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_EDUCATION_STATUS".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(20),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_PURCHASE_ESTIMATE".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_CREDIT_RATING".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(10),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_DEP_COUNT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_DEP_EMPLOYED_COUNT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CD_DEP_COLLEGE_COUNT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // HOUSEHOLD_DEMOGRAPHICS table (~7,200 rows)
    db.create_table(TableSchema::new(
        "HOUSEHOLD_DEMOGRAPHICS".to_string(),
        vec![
            ColumnSchema {
                name: "HD_DEMO_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "HD_INCOME_BAND_SK".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "HD_BUY_POTENTIAL".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(15),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "HD_DEP_COUNT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "HD_VEHICLE_COUNT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // CALL_CENTER table (~6 rows at SF=1)
    db.create_table(TableSchema::new(
        "CALL_CENTER".to_string(),
        vec![
            ColumnSchema {
                name: "CC_CALL_CENTER_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_CALL_CENTER_ID".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(16),
                },
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_REC_START_DATE".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_REC_END_DATE".to_string(),
                data_type: DataType::Date,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_CLOSED_DATE_SK".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_OPEN_DATE_SK".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_NAME".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(50),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_CLASS".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(50),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_EMPLOYEES".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_SQ_FT".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_HOURS".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(20),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_MANAGER".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(40),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_MKT_ID".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_MKT_CLASS".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(50),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_MKT_DESC".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(100),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_MARKET_MANAGER".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(40),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_DIVISION".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_DIVISION_NAME".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(50),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_COMPANY".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_COMPANY_NAME".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(50),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_STREET_NUMBER".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(10),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_STREET_NAME".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(60),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_STREET_TYPE".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(15),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_SUITE_NUMBER".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(10),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_CITY".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(60),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_COUNTY".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(30),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_STATE".to_string(),
                data_type: DataType::Varchar { max_length: Some(2) },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_ZIP".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(10),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_COUNTRY".to_string(),
                data_type: DataType::Varchar {
                    max_length: Some(20),
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_GMT_OFFSET".to_string(),
                data_type: DataType::Decimal {
                    precision: 5,
                    scale: 2,
                },
                nullable: true,
                default_value: None,
            },
            ColumnSchema {
                name: "CC_TAX_PERCENTAGE".to_string(),
                data_type: DataType::Decimal {
                    precision: 5,
                    scale: 2,
                },
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();

    // INVENTORY table (fact table with dimension-like usage)
    db.create_table(TableSchema::new(
        "INVENTORY".to_string(),
        vec![
            ColumnSchema {
                name: "INV_DATE_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "INV_ITEM_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "INV_WAREHOUSE_SK".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
            },
            ColumnSchema {
                name: "INV_QUANTITY_ON_HAND".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
            },
        ],
    ))
    .unwrap();
}

/// Create indexes on TPC-DS tables for primary keys
fn create_tpcds_indexes_vibesql(db: &mut VibeDB) {
    use vibesql_ast::{IndexColumn, OrderDirection};

    // INCOME_BAND: PRIMARY KEY (IB_INCOME_BAND_SK)
    db.create_index(
        "idx_income_band_pk".to_string(),
        "INCOME_BAND".to_string(),
        true,
        vec![IndexColumn {
            column_name: "IB_INCOME_BAND_SK".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // CUSTOMER_DEMOGRAPHICS: PRIMARY KEY (CD_DEMO_SK)
    db.create_index(
        "idx_customer_demographics_pk".to_string(),
        "CUSTOMER_DEMOGRAPHICS".to_string(),
        true,
        vec![IndexColumn {
            column_name: "CD_DEMO_SK".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // HOUSEHOLD_DEMOGRAPHICS: PRIMARY KEY (HD_DEMO_SK)
    db.create_index(
        "idx_household_demographics_pk".to_string(),
        "HOUSEHOLD_DEMOGRAPHICS".to_string(),
        true,
        vec![IndexColumn {
            column_name: "HD_DEMO_SK".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // CALL_CENTER: PRIMARY KEY (CC_CALL_CENTER_SK)
    db.create_index(
        "idx_call_center_pk".to_string(),
        "CALL_CENTER".to_string(),
        true,
        vec![IndexColumn {
            column_name: "CC_CALL_CENTER_SK".to_string(),
            direction: OrderDirection::Asc,
            prefix_length: None,
        }],
    )
    .unwrap();

    // INVENTORY: PRIMARY KEY (INV_DATE_SK, INV_ITEM_SK, INV_WAREHOUSE_SK)
    db.create_index(
        "idx_inventory_pk".to_string(),
        "INVENTORY".to_string(),
        true,
        vec![
            IndexColumn {
                column_name: "INV_DATE_SK".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "INV_ITEM_SK".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
            IndexColumn {
                column_name: "INV_WAREHOUSE_SK".to_string(),
                direction: OrderDirection::Asc,
                prefix_length: None,
            },
        ],
    )
    .unwrap();
}

// =============================================================================
// Schema Creation - SQLite
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_tpcds_schema_sqlite(conn: &SqliteConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE income_band (
            ib_income_band_sk INTEGER PRIMARY KEY,
            ib_lower_bound INTEGER,
            ib_upper_bound INTEGER
        );

        CREATE TABLE customer_demographics (
            cd_demo_sk INTEGER PRIMARY KEY,
            cd_gender TEXT,
            cd_marital_status TEXT,
            cd_education_status TEXT,
            cd_purchase_estimate INTEGER,
            cd_credit_rating TEXT,
            cd_dep_count INTEGER,
            cd_dep_employed_count INTEGER,
            cd_dep_college_count INTEGER
        );

        CREATE TABLE household_demographics (
            hd_demo_sk INTEGER PRIMARY KEY,
            hd_income_band_sk INTEGER,
            hd_buy_potential TEXT,
            hd_dep_count INTEGER,
            hd_vehicle_count INTEGER
        );

        CREATE TABLE call_center (
            cc_call_center_sk INTEGER PRIMARY KEY,
            cc_call_center_id TEXT NOT NULL,
            cc_rec_start_date TEXT,
            cc_rec_end_date TEXT,
            cc_closed_date_sk INTEGER,
            cc_open_date_sk INTEGER,
            cc_name TEXT,
            cc_class TEXT,
            cc_employees INTEGER,
            cc_sq_ft INTEGER,
            cc_hours TEXT,
            cc_manager TEXT,
            cc_mkt_id INTEGER,
            cc_mkt_class TEXT,
            cc_mkt_desc TEXT,
            cc_market_manager TEXT,
            cc_division INTEGER,
            cc_division_name TEXT,
            cc_company INTEGER,
            cc_company_name TEXT,
            cc_street_number TEXT,
            cc_street_name TEXT,
            cc_street_type TEXT,
            cc_suite_number TEXT,
            cc_city TEXT,
            cc_county TEXT,
            cc_state TEXT,
            cc_zip TEXT,
            cc_country TEXT,
            cc_gmt_offset REAL,
            cc_tax_percentage REAL
        );

        CREATE TABLE inventory (
            inv_date_sk INTEGER NOT NULL,
            inv_item_sk INTEGER NOT NULL,
            inv_warehouse_sk INTEGER NOT NULL,
            inv_quantity_on_hand INTEGER,
            PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
        );
    "#,
    )
    .unwrap();
}

// =============================================================================
// Schema Creation - DuckDB
// =============================================================================

#[cfg(feature = "benchmark-comparison")]
fn create_tpcds_schema_duckdb(conn: &DuckDBConn) {
    conn.execute_batch(
        r#"
        CREATE TABLE income_band (
            ib_income_band_sk INTEGER PRIMARY KEY,
            ib_lower_bound INTEGER,
            ib_upper_bound INTEGER
        );

        CREATE TABLE customer_demographics (
            cd_demo_sk INTEGER PRIMARY KEY,
            cd_gender VARCHAR(1),
            cd_marital_status VARCHAR(1),
            cd_education_status VARCHAR(20),
            cd_purchase_estimate INTEGER,
            cd_credit_rating VARCHAR(10),
            cd_dep_count INTEGER,
            cd_dep_employed_count INTEGER,
            cd_dep_college_count INTEGER
        );

        CREATE TABLE household_demographics (
            hd_demo_sk INTEGER PRIMARY KEY,
            hd_income_band_sk INTEGER,
            hd_buy_potential VARCHAR(15),
            hd_dep_count INTEGER,
            hd_vehicle_count INTEGER
        );

        CREATE TABLE call_center (
            cc_call_center_sk INTEGER PRIMARY KEY,
            cc_call_center_id VARCHAR(16) NOT NULL,
            cc_rec_start_date DATE,
            cc_rec_end_date DATE,
            cc_closed_date_sk INTEGER,
            cc_open_date_sk INTEGER,
            cc_name VARCHAR(50),
            cc_class VARCHAR(50),
            cc_employees INTEGER,
            cc_sq_ft INTEGER,
            cc_hours VARCHAR(20),
            cc_manager VARCHAR(40),
            cc_mkt_id INTEGER,
            cc_mkt_class VARCHAR(50),
            cc_mkt_desc VARCHAR(100),
            cc_market_manager VARCHAR(40),
            cc_division INTEGER,
            cc_division_name VARCHAR(50),
            cc_company INTEGER,
            cc_company_name VARCHAR(50),
            cc_street_number VARCHAR(10),
            cc_street_name VARCHAR(60),
            cc_street_type VARCHAR(15),
            cc_suite_number VARCHAR(10),
            cc_city VARCHAR(60),
            cc_county VARCHAR(30),
            cc_state VARCHAR(2),
            cc_zip VARCHAR(10),
            cc_country VARCHAR(20),
            cc_gmt_offset DECIMAL(5,2),
            cc_tax_percentage DECIMAL(5,2)
        );

        CREATE TABLE inventory (
            inv_date_sk INTEGER NOT NULL,
            inv_item_sk INTEGER NOT NULL,
            inv_warehouse_sk INTEGER NOT NULL,
            inv_quantity_on_hand INTEGER,
            PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
        );
    "#,
    )
    .unwrap();
}

// =============================================================================
// Data Loading - INCOME_BAND (fixed 20 rows)
// =============================================================================

fn load_income_band_vibesql(db: &mut VibeDB) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    // TPC-DS defines 20 income bands with $10,000 increments
    for i in 1..=20 {
        let lower_bound = ((i - 1) * 10000) as i64;
        let upper_bound = if i == 20 {
            200000
        } else {
            (i * 10000) as i64
        };

        let row = Row::new(vec![
            SqlValue::Integer(i as i64),
            SqlValue::Integer(lower_bound),
            SqlValue::Integer(upper_bound),
        ]);
        db.insert_row("INCOME_BAND", row).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_income_band_sqlite(conn: &SqliteConn) {
    let mut stmt = conn
        .prepare("INSERT INTO income_band VALUES (?, ?, ?)")
        .unwrap();

    for i in 1..=20 {
        let lower_bound = ((i - 1) * 10000) as i64;
        let upper_bound = if i == 20 { 200000 } else { (i * 10000) as i64 };

        stmt.execute(rusqlite::params![i as i64, lower_bound, upper_bound])
            .unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_income_band_duckdb(conn: &DuckDBConn) {
    let mut stmt = conn
        .prepare("INSERT INTO income_band VALUES (?, ?, ?)")
        .unwrap();

    for i in 1..=20 {
        let lower_bound = ((i - 1) * 10000) as i64;
        let upper_bound = if i == 20 { 200000 } else { (i * 10000) as i64 };

        stmt.execute(duckdb::params![i as i64, lower_bound, upper_bound])
            .unwrap();
    }
}

// =============================================================================
// Data Loading - CUSTOMER_DEMOGRAPHICS (~1,920 rows)
// =============================================================================

fn load_customer_demographics_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut sk = 1;
    for &gender in CD_GENDERS {
        for &marital_status in CD_MARITAL_STATUS {
            for &education in CD_EDUCATION_STATUS {
                for dep_count in 0..=6 {
                    let purchase_estimate = ((sk * 500) % 10000 + 500) as i64;
                    let credit_rating = CD_CREDIT_RATINGS[sk % CD_CREDIT_RATINGS.len()];
                    let dep_employed = data.random_integer(0, dep_count + 1) as i64;
                    let dep_college = data.random_integer(0, dep_count + 1) as i64;

                    let row = Row::new(vec![
                        SqlValue::Integer(sk as i64),
                        SqlValue::Varchar(gender.to_string()),
                        SqlValue::Varchar(marital_status.to_string()),
                        SqlValue::Varchar(education.to_string()),
                        SqlValue::Integer(purchase_estimate),
                        SqlValue::Varchar(credit_rating.to_string()),
                        SqlValue::Integer(dep_count as i64),
                        SqlValue::Integer(dep_employed),
                        SqlValue::Integer(dep_college),
                    ]);
                    db.insert_row("CUSTOMER_DEMOGRAPHICS", row).unwrap();
                    sk += 1;

                    if sk > data.customer_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_demographics_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO customer_demographics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let mut sk = 1;
    for &gender in CD_GENDERS {
        for &marital_status in CD_MARITAL_STATUS {
            for &education in CD_EDUCATION_STATUS {
                for dep_count in 0..=6 {
                    let purchase_estimate = ((sk * 500) % 10000 + 500) as i64;
                    let credit_rating = CD_CREDIT_RATINGS[sk % CD_CREDIT_RATINGS.len()];
                    let dep_employed = data.random_integer(0, dep_count + 1) as i64;
                    let dep_college = data.random_integer(0, dep_count + 1) as i64;

                    stmt.execute(rusqlite::params![
                        sk as i64,
                        gender,
                        marital_status,
                        education,
                        purchase_estimate,
                        credit_rating,
                        dep_count as i64,
                        dep_employed,
                        dep_college,
                    ])
                    .unwrap();
                    sk += 1;

                    if sk > data.customer_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_customer_demographics_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO customer_demographics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    let mut sk = 1;
    for &gender in CD_GENDERS {
        for &marital_status in CD_MARITAL_STATUS {
            for &education in CD_EDUCATION_STATUS {
                for dep_count in 0..=6 {
                    let purchase_estimate = ((sk * 500) % 10000 + 500) as i64;
                    let credit_rating = CD_CREDIT_RATINGS[sk % CD_CREDIT_RATINGS.len()];
                    let dep_employed = data.random_integer(0, dep_count + 1) as i64;
                    let dep_college = data.random_integer(0, dep_count + 1) as i64;

                    stmt.execute(duckdb::params![
                        sk as i64,
                        gender,
                        marital_status,
                        education,
                        purchase_estimate,
                        credit_rating,
                        dep_count as i64,
                        dep_employed,
                        dep_college,
                    ])
                    .unwrap();
                    sk += 1;

                    if sk > data.customer_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

// =============================================================================
// Data Loading - HOUSEHOLD_DEMOGRAPHICS (~7,200 rows)
// =============================================================================

fn load_household_demographics_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut sk = 1;
    for income_band_sk in 1..=20 {
        for &buy_potential in HD_BUY_POTENTIALS {
            for dep_count in 0..=9 {
                for vehicle_count in 0..=5 {
                    let row = Row::new(vec![
                        SqlValue::Integer(sk as i64),
                        SqlValue::Integer(income_band_sk as i64),
                        SqlValue::Varchar(buy_potential.to_string()),
                        SqlValue::Integer(dep_count as i64),
                        SqlValue::Integer(vehicle_count as i64),
                    ]);
                    db.insert_row("HOUSEHOLD_DEMOGRAPHICS", row).unwrap();
                    sk += 1;

                    if sk > data.household_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_household_demographics_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO household_demographics VALUES (?, ?, ?, ?, ?)")
        .unwrap();

    let mut sk = 1;
    for income_band_sk in 1..=20 {
        for &buy_potential in HD_BUY_POTENTIALS {
            for dep_count in 0..=9 {
                for vehicle_count in 0..=5 {
                    stmt.execute(rusqlite::params![
                        sk as i64,
                        income_band_sk as i64,
                        buy_potential,
                        dep_count as i64,
                        vehicle_count as i64,
                    ])
                    .unwrap();
                    sk += 1;

                    if sk > data.household_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_household_demographics_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO household_demographics VALUES (?, ?, ?, ?, ?)")
        .unwrap();

    let mut sk = 1;
    for income_band_sk in 1..=20 {
        for &buy_potential in HD_BUY_POTENTIALS {
            for dep_count in 0..=9 {
                for vehicle_count in 0..=5 {
                    stmt.execute(duckdb::params![
                        sk as i64,
                        income_band_sk as i64,
                        buy_potential,
                        dep_count as i64,
                        vehicle_count as i64,
                    ])
                    .unwrap();
                    sk += 1;

                    if sk > data.household_demographics_count {
                        return;
                    }
                }
            }
        }
    }
}

// =============================================================================
// Data Loading - CALL_CENTER (~6 rows at SF=1)
// =============================================================================

fn load_call_center_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let cities = ["Midway", "Fairview", "Salem", "Oakland", "Springfield", "Georgetown"];
    let states = ["TN", "IN", "OH", "NY", "IL", "TX"];
    let counties = [
        "Williamson County",
        "Grant County",
        "Marion County",
        "Monroe County",
        "Sangamon County",
        "Travis County",
    ];

    for i in 0..data.call_center_count {
        let sk = i + 1;
        let call_center_id = format!("AAAAAAAAB{:07}AA", sk);
        let rec_start_date = data.random_date("1998-01-01", "2000-12-31");
        let name = format!("call center #{}", sk);
        let cc_class = CC_CLASSES[i % CC_CLASSES.len()];
        let employees = (50 + (i * 25) % 200) as i64;
        let sq_ft = (5000 + (i * 1000) % 10000) as i64;
        let hours = CC_HOURS[i % CC_HOURS.len()];
        let manager = format!("Manager #{}", sk);
        let mkt_id = ((i % 6) + 1) as i64;
        let mkt_class = format!("Market Class {}", (i % 4) + 1);
        let mkt_desc = data.random_varchar(100);
        let market_manager = format!("Market Manager #{}", sk);
        let division = ((i % 3) + 1) as i64;
        let division_name = format!("Division {}", (i % 3) + 1);
        let company = 1_i64;
        let company_name = "Call Center Corp".to_string();
        let street_number = format!("{}", 100 + i * 10);
        let street_name = format!("{} Street", cities[i % cities.len()]);
        let street_type = "Street".to_string();
        let suite_number = format!("Suite {}", i + 1);
        let city = cities[i % cities.len()].to_string();
        let county = counties[i % counties.len()].to_string();
        let state = states[i % states.len()].to_string();
        let zip = data.random_zip();
        let country = "United States".to_string();
        let gmt_offset = -5.0 - (i % 4) as f64;
        let tax_percentage = 0.05 + (i as f64 * 0.005);

        let row = Row::new(vec![
            SqlValue::Integer(sk as i64),
            SqlValue::Varchar(call_center_id),
            SqlValue::Date(Date::from_str(&rec_start_date).unwrap()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Integer(((sk * 100) % 1000) as i64),
            SqlValue::Varchar(name),
            SqlValue::Varchar(cc_class.to_string()),
            SqlValue::Integer(employees),
            SqlValue::Integer(sq_ft),
            SqlValue::Varchar(hours.to_string()),
            SqlValue::Varchar(manager),
            SqlValue::Integer(mkt_id),
            SqlValue::Varchar(mkt_class),
            SqlValue::Varchar(mkt_desc),
            SqlValue::Varchar(market_manager),
            SqlValue::Integer(division),
            SqlValue::Varchar(division_name),
            SqlValue::Integer(company),
            SqlValue::Varchar(company_name),
            SqlValue::Varchar(street_number),
            SqlValue::Varchar(street_name),
            SqlValue::Varchar(street_type),
            SqlValue::Varchar(suite_number),
            SqlValue::Varchar(city),
            SqlValue::Varchar(county),
            SqlValue::Varchar(state),
            SqlValue::Varchar(zip),
            SqlValue::Varchar(country),
            SqlValue::Numeric(gmt_offset),
            SqlValue::Numeric(tax_percentage),
        ]);
        db.insert_row("CALL_CENTER", row).unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_call_center_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare(
            "INSERT INTO call_center VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();

    let cities = ["Midway", "Fairview", "Salem", "Oakland", "Springfield", "Georgetown"];
    let states = ["TN", "IN", "OH", "NY", "IL", "TX"];
    let counties = [
        "Williamson County",
        "Grant County",
        "Marion County",
        "Monroe County",
        "Sangamon County",
        "Travis County",
    ];

    for i in 0..data.call_center_count {
        let sk = i + 1;
        let call_center_id = format!("AAAAAAAAB{:07}AA", sk);
        let rec_start_date = data.random_date("1998-01-01", "2000-12-31");
        let name = format!("call center #{}", sk);
        let cc_class = CC_CLASSES[i % CC_CLASSES.len()];
        let employees = (50 + (i * 25) % 200) as i64;
        let sq_ft = (5000 + (i * 1000) % 10000) as i64;
        let hours = CC_HOURS[i % CC_HOURS.len()];
        let manager = format!("Manager #{}", sk);
        let mkt_id = ((i % 6) + 1) as i64;
        let mkt_class = format!("Market Class {}", (i % 4) + 1);
        let mkt_desc = data.random_varchar(100);
        let market_manager = format!("Market Manager #{}", sk);
        let division = ((i % 3) + 1) as i64;
        let division_name = format!("Division {}", (i % 3) + 1);
        let company = 1_i64;
        let company_name = "Call Center Corp";
        let street_number = format!("{}", 100 + i * 10);
        let street_name = format!("{} Street", cities[i % cities.len()]);
        let street_type = "Street";
        let suite_number = format!("Suite {}", i + 1);
        let city = cities[i % cities.len()];
        let county = counties[i % counties.len()];
        let state = states[i % states.len()];
        let zip = data.random_zip();
        let country = "United States";
        let gmt_offset = -5.0 - (i % 4) as f64;
        let tax_percentage = 0.05 + (i as f64 * 0.005);

        stmt.execute(rusqlite::params![
            sk as i64,
            call_center_id,
            rec_start_date,
            Option::<String>::None,
            Option::<i64>::None,
            ((sk * 100) % 1000) as i64,
            name,
            cc_class,
            employees,
            sq_ft,
            hours,
            manager,
            mkt_id,
            mkt_class,
            mkt_desc,
            market_manager,
            division,
            division_name,
            company,
            company_name,
            street_number,
            street_name,
            street_type,
            suite_number,
            city,
            county,
            state,
            zip,
            country,
            gmt_offset,
            tax_percentage,
        ])
        .unwrap();
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_call_center_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare(
            "INSERT INTO call_center VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();

    let cities = ["Midway", "Fairview", "Salem", "Oakland", "Springfield", "Georgetown"];
    let states = ["TN", "IN", "OH", "NY", "IL", "TX"];
    let counties = [
        "Williamson County",
        "Grant County",
        "Marion County",
        "Monroe County",
        "Sangamon County",
        "Travis County",
    ];

    for i in 0..data.call_center_count {
        let sk = i + 1;
        let call_center_id = format!("AAAAAAAAB{:07}AA", sk);
        let rec_start_date = data.random_date("1998-01-01", "2000-12-31");
        let name = format!("call center #{}", sk);
        let cc_class = CC_CLASSES[i % CC_CLASSES.len()];
        let employees = (50 + (i * 25) % 200) as i64;
        let sq_ft = (5000 + (i * 1000) % 10000) as i64;
        let hours = CC_HOURS[i % CC_HOURS.len()];
        let manager = format!("Manager #{}", sk);
        let mkt_id = ((i % 6) + 1) as i64;
        let mkt_class = format!("Market Class {}", (i % 4) + 1);
        let mkt_desc = data.random_varchar(100);
        let market_manager = format!("Market Manager #{}", sk);
        let division = ((i % 3) + 1) as i64;
        let division_name = format!("Division {}", (i % 3) + 1);
        let company = 1_i64;
        let company_name = "Call Center Corp";
        let street_number = format!("{}", 100 + i * 10);
        let street_name = format!("{} Street", cities[i % cities.len()]);
        let street_type = "Street";
        let suite_number = format!("Suite {}", i + 1);
        let city = cities[i % cities.len()];
        let county = counties[i % counties.len()];
        let state = states[i % states.len()];
        let zip = data.random_zip();
        let country = "United States";
        let gmt_offset = -5.0 - (i % 4) as f64;
        let tax_percentage = 0.05 + (i as f64 * 0.005);

        stmt.execute(duckdb::params![
            sk as i64,
            call_center_id,
            rec_start_date,
            Option::<String>::None,
            Option::<i64>::None,
            ((sk * 100) % 1000) as i64,
            name,
            cc_class,
            employees,
            sq_ft,
            hours,
            manager,
            mkt_id,
            mkt_class,
            mkt_desc,
            market_manager,
            division,
            division_name,
            company,
            company_name,
            street_number,
            street_name,
            street_type,
            suite_number,
            city,
            county,
            state,
            zip,
            country,
            gmt_offset,
            tax_percentage,
        ])
        .unwrap();
    }
}

// =============================================================================
// Data Loading - INVENTORY (fact table)
// =============================================================================

fn load_inventory_vibesql(db: &mut VibeDB, data: &mut TPCDSData) {
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    let mut count = 0;
    for week in 0..52 {
        let date_sk = (week * 7 + 1) as i64;

        for warehouse_sk in 1..=data.warehouse_count {
            for item_sk in 1..=data.item_count.min(100) {
                if count >= data.inventory_count {
                    return;
                }

                let quantity = data.random_integer(0, 1000) as i64;

                let row = Row::new(vec![
                    SqlValue::Integer(date_sk),
                    SqlValue::Integer(item_sk as i64),
                    SqlValue::Integer(warehouse_sk as i64),
                    SqlValue::Integer(quantity),
                ]);
                db.insert_row("INVENTORY", row).unwrap();
                count += 1;
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_inventory_sqlite(conn: &SqliteConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO inventory VALUES (?, ?, ?, ?)")
        .unwrap();

    let mut count = 0;
    for week in 0..52 {
        let date_sk = (week * 7 + 1) as i64;

        for warehouse_sk in 1..=data.warehouse_count {
            for item_sk in 1..=data.item_count.min(100) {
                if count >= data.inventory_count {
                    return;
                }

                let quantity = data.random_integer(0, 1000) as i64;

                stmt.execute(rusqlite::params![
                    date_sk,
                    item_sk as i64,
                    warehouse_sk as i64,
                    quantity,
                ])
                .unwrap();
                count += 1;
            }
        }
    }
}

#[cfg(feature = "benchmark-comparison")]
fn load_inventory_duckdb(conn: &DuckDBConn, data: &mut TPCDSData) {
    let mut stmt = conn
        .prepare("INSERT INTO inventory VALUES (?, ?, ?, ?)")
        .unwrap();

    let mut count = 0;
    for week in 0..52 {
        let date_sk = (week * 7 + 1) as i64;

        for warehouse_sk in 1..=data.warehouse_count {
            for item_sk in 1..=data.item_count.min(100) {
                if count >= data.inventory_count {
                    return;
                }

                let quantity = data.random_integer(0, 1000) as i64;

                stmt.execute(duckdb::params![
                    date_sk,
                    item_sk as i64,
                    warehouse_sk as i64,
                    quantity,
                ])
                .unwrap();
                count += 1;
            }
        }
    }
}
