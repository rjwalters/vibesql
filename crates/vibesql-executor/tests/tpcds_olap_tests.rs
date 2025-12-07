//! TPC-DS OLAP Query Validation Tests
//!
//! Tests validating ROLLUP, CUBE, and GROUPING SETS functionality using
//! TPC-DS-style queries. These tests verify the SQL:1999 OLAP extensions
//! implementation against realistic analytical query patterns.
//!
//! TPC-DS queries using these features:
//! - ROLLUP: Q5, Q14, Q18, Q22, Q27, Q70, Q77, Q80, Q86
//! - CUBE: Q67
//! - GROUPING SETS: Q36
//!
//! Reference: https://www.tpc.org/tpcds/

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// ============================================================================
// Test Database Setup
// ============================================================================

/// Creates a simplified TPC-DS schema with sample data for OLAP testing
fn setup_tpcds_olap_db() -> Database {
    let mut db = Database::new();

    // Create store_sales fact table
    execute_ddl(
        &mut db,
        r#"CREATE TABLE store_sales (
            ss_sold_date_sk INTEGER,
            ss_item_sk INTEGER,
            ss_customer_sk INTEGER,
            ss_store_sk INTEGER,
            ss_ext_sales_price FLOAT,
            ss_net_profit FLOAT,
            ss_quantity INTEGER
        )"#,
    );

    // Create date_dim dimension table
    execute_ddl(
        &mut db,
        r#"CREATE TABLE date_dim (
            d_date_sk INTEGER PRIMARY KEY,
            d_year INTEGER,
            d_moy INTEGER,
            d_qoy INTEGER,
            d_week_seq INTEGER
        )"#,
    );

    // Create item dimension table
    execute_ddl(
        &mut db,
        r#"CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY,
            i_category TEXT,
            i_class TEXT,
            i_brand TEXT,
            i_manufact_id INTEGER
        )"#,
    );

    // Create store dimension table
    execute_ddl(
        &mut db,
        r#"CREATE TABLE store (
            s_store_sk INTEGER PRIMARY KEY,
            s_store_name TEXT,
            s_state TEXT,
            s_county TEXT
        )"#,
    );

    // Create web_sales fact table (for Q5, Q77, Q80)
    execute_ddl(
        &mut db,
        r#"CREATE TABLE web_sales (
            ws_sold_date_sk INTEGER,
            ws_item_sk INTEGER,
            ws_ext_sales_price FLOAT,
            ws_net_profit FLOAT
        )"#,
    );

    // Create catalog_sales fact table (for Q5, Q77, Q80)
    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_sales (
            cs_sold_date_sk INTEGER,
            cs_item_sk INTEGER,
            cs_ext_sales_price FLOAT,
            cs_net_profit FLOAT
        )"#,
    );

    // Populate dimension tables
    populate_date_dim(&mut db);
    populate_item(&mut db);
    populate_store(&mut db);

    // Populate fact tables
    populate_store_sales(&mut db);
    populate_web_sales(&mut db);
    populate_catalog_sales(&mut db);

    db
}

fn execute_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse DDL");
    match stmt {
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute(&create, db).expect("Failed to create table");
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

fn execute_insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse INSERT");
    match stmt {
        Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert).expect("Failed to insert");
        }
        _ => panic!("Expected INSERT statement"),
    }
}

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse query");
    match stmt {
        Statement::Select(select) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select).expect("Failed to execute query")
        }
        _ => panic!("Expected SELECT statement"),
    }
}

fn populate_date_dim(db: &mut Database) {
    // Years 2000-2002, quarters and months
    let inserts = [
        // 2000 data
        "INSERT INTO date_dim VALUES (1, 2000, 1, 1, 1)",
        "INSERT INTO date_dim VALUES (2, 2000, 2, 1, 5)",
        "INSERT INTO date_dim VALUES (3, 2000, 3, 1, 9)",
        "INSERT INTO date_dim VALUES (4, 2000, 4, 2, 14)",
        "INSERT INTO date_dim VALUES (5, 2000, 5, 2, 18)",
        "INSERT INTO date_dim VALUES (6, 2000, 6, 2, 22)",
        "INSERT INTO date_dim VALUES (7, 2000, 7, 3, 27)",
        "INSERT INTO date_dim VALUES (8, 2000, 8, 3, 31)",
        "INSERT INTO date_dim VALUES (9, 2000, 9, 3, 35)",
        "INSERT INTO date_dim VALUES (10, 2000, 10, 4, 40)",
        "INSERT INTO date_dim VALUES (11, 2000, 11, 4, 44)",
        "INSERT INTO date_dim VALUES (12, 2000, 12, 4, 48)",
        // 2001 data
        "INSERT INTO date_dim VALUES (13, 2001, 1, 1, 53)",
        "INSERT INTO date_dim VALUES (14, 2001, 2, 1, 57)",
        "INSERT INTO date_dim VALUES (15, 2001, 3, 1, 61)",
        "INSERT INTO date_dim VALUES (16, 2001, 4, 2, 66)",
        "INSERT INTO date_dim VALUES (17, 2001, 5, 2, 70)",
        "INSERT INTO date_dim VALUES (18, 2001, 6, 2, 74)",
        "INSERT INTO date_dim VALUES (19, 2001, 7, 3, 79)",
        "INSERT INTO date_dim VALUES (20, 2001, 8, 3, 83)",
        "INSERT INTO date_dim VALUES (21, 2001, 9, 3, 87)",
        "INSERT INTO date_dim VALUES (22, 2001, 10, 4, 92)",
        "INSERT INTO date_dim VALUES (23, 2001, 11, 4, 96)",
        "INSERT INTO date_dim VALUES (24, 2001, 12, 4, 100)",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

fn populate_item(db: &mut Database) {
    let inserts = [
        "INSERT INTO item VALUES (1, 'Electronics', 'Computers', 'BrandA', 1)",
        "INSERT INTO item VALUES (2, 'Electronics', 'Computers', 'BrandB', 1)",
        "INSERT INTO item VALUES (3, 'Electronics', 'Phones', 'BrandA', 2)",
        "INSERT INTO item VALUES (4, 'Clothing', 'Shirts', 'BrandC', 3)",
        "INSERT INTO item VALUES (5, 'Clothing', 'Pants', 'BrandC', 3)",
        "INSERT INTO item VALUES (6, 'Clothing', 'Shirts', 'BrandD', 4)",
        "INSERT INTO item VALUES (7, 'Home', 'Furniture', 'BrandE', 5)",
        "INSERT INTO item VALUES (8, 'Home', 'Kitchen', 'BrandF', 5)",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

fn populate_store(db: &mut Database) {
    let inserts = [
        "INSERT INTO store VALUES (1, 'Store#1', 'CA', 'Los Angeles')",
        "INSERT INTO store VALUES (2, 'Store#2', 'CA', 'San Francisco')",
        "INSERT INTO store VALUES (3, 'Store#3', 'NY', 'New York')",
        "INSERT INTO store VALUES (4, 'Store#4', 'TX', 'Houston')",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

fn populate_store_sales(db: &mut Database) {
    // Generate sales data across years, items, and stores
    let inserts = [
        // 2000 Q1 sales
        "INSERT INTO store_sales VALUES (1, 1, 1, 1, 100.00, 20.00, 2)",
        "INSERT INTO store_sales VALUES (1, 2, 2, 1, 150.00, 30.00, 3)",
        "INSERT INTO store_sales VALUES (2, 1, 3, 2, 120.00, 25.00, 2)",
        "INSERT INTO store_sales VALUES (3, 3, 4, 2, 80.00, 15.00, 1)",
        // 2000 Q2 sales
        "INSERT INTO store_sales VALUES (4, 4, 1, 1, 200.00, 50.00, 4)",
        "INSERT INTO store_sales VALUES (5, 5, 2, 3, 180.00, 45.00, 3)",
        "INSERT INTO store_sales VALUES (6, 6, 3, 3, 90.00, 20.00, 2)",
        // 2000 Q3 sales
        "INSERT INTO store_sales VALUES (7, 7, 1, 4, 300.00, 80.00, 2)",
        "INSERT INTO store_sales VALUES (8, 8, 2, 4, 250.00, 60.00, 3)",
        // 2000 Q4 sales
        "INSERT INTO store_sales VALUES (10, 1, 1, 1, 180.00, 35.00, 3)",
        "INSERT INTO store_sales VALUES (11, 2, 2, 2, 220.00, 55.00, 4)",
        "INSERT INTO store_sales VALUES (12, 3, 3, 3, 140.00, 30.00, 2)",
        // 2001 Q1 sales
        "INSERT INTO store_sales VALUES (13, 1, 1, 1, 130.00, 25.00, 2)",
        "INSERT INTO store_sales VALUES (14, 4, 2, 2, 190.00, 40.00, 3)",
        "INSERT INTO store_sales VALUES (15, 5, 3, 3, 160.00, 35.00, 2)",
        // 2001 Q2 sales
        "INSERT INTO store_sales VALUES (16, 6, 1, 1, 110.00, 22.00, 2)",
        "INSERT INTO store_sales VALUES (17, 7, 2, 4, 280.00, 70.00, 3)",
        "INSERT INTO store_sales VALUES (18, 8, 3, 4, 240.00, 55.00, 2)",
        // 2001 Q3-Q4 sales
        "INSERT INTO store_sales VALUES (19, 1, 1, 1, 170.00, 38.00, 3)",
        "INSERT INTO store_sales VALUES (22, 2, 2, 2, 195.00, 42.00, 3)",
        "INSERT INTO store_sales VALUES (24, 3, 3, 3, 165.00, 36.00, 2)",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

fn populate_web_sales(db: &mut Database) {
    let inserts = [
        "INSERT INTO web_sales VALUES (1, 1, 50.00, 10.00)",
        "INSERT INTO web_sales VALUES (2, 2, 75.00, 15.00)",
        "INSERT INTO web_sales VALUES (13, 1, 60.00, 12.00)",
        "INSERT INTO web_sales VALUES (14, 3, 85.00, 17.00)",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

fn populate_catalog_sales(db: &mut Database) {
    let inserts = [
        "INSERT INTO catalog_sales VALUES (1, 1, 80.00, 16.00)",
        "INSERT INTO catalog_sales VALUES (3, 4, 120.00, 24.00)",
        "INSERT INTO catalog_sales VALUES (13, 2, 95.00, 19.00)",
        "INSERT INTO catalog_sales VALUES (15, 5, 110.00, 22.00)",
    ];

    for sql in inserts {
        execute_insert(db, sql);
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn get_f64(val: &SqlValue) -> f64 {
    match val {
        SqlValue::Float(f) => *f as f64,
        SqlValue::Real(f) => *f as f64,
        SqlValue::Double(f) => *f,
        SqlValue::Numeric(f) => *f,
        SqlValue::Integer(i) => *i as f64,
        SqlValue::Bigint(i) => *i as f64,
        SqlValue::Null => f64::NAN,
        _ => panic!("Expected numeric value, got {:?}", val),
    }
}

fn get_i64(val: &SqlValue) -> i64 {
    match val {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Null => -999999, // Sentinel
        _ => panic!("Expected integer, got {:?}", val),
    }
}

fn is_null(val: &SqlValue) -> bool {
    matches!(val, SqlValue::Null)
}

#[allow(dead_code)]
fn get_str(val: &SqlValue) -> &str {
    match val {
        SqlValue::Varchar(s) => s,
        SqlValue::Character(s) => s,
        SqlValue::Null => "<NULL>",
        _ => panic!("Expected string, got {:?}", val),
    }
}

// ============================================================================
// TPC-DS Q5 Style: Channel Sales by Year with ROLLUP
// ============================================================================
// Original Q5 calculates channel sales with ROLLUP across multiple dimensions

#[test]
fn test_tpcds_q5_rollup_channel_sales() {
    let db = setup_tpcds_olap_db();

    // Q5-style: Sales by year and channel with ROLLUP for subtotals
    let query = r#"
        SELECT
            d_year,
            'Store' AS channel,
            SUM(ss_ext_sales_price) AS total_sales,
            GROUPING(d_year) AS year_grouping
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(d_year)
        ORDER BY d_year
    "#;

    let rows = execute_query(&db, query);

    // Should have year 2000, year 2001, and grand total (NULL year)
    assert!(rows.len() >= 3, "Expected at least 3 rows (2 years + grand total)");

    // Verify grand total exists (NULL year, GROUPING = 1)
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]));
    assert!(grand_total.is_some(), "Missing grand total row");

    // GROUPING(d_year) should be 1 for grand total
    let gt = grand_total.unwrap();
    assert_eq!(get_i64(&gt.values[3]), 1, "GROUPING should be 1 for grand total");
}

#[test]
fn test_tpcds_q5_rollup_multi_channel() {
    let db = setup_tpcds_olap_db();

    // Combine store, web, and catalog sales with ROLLUP
    let query = r#"
        WITH all_sales AS (
            SELECT d_year, 'Store' as channel, ss_ext_sales_price as sales
            FROM store_sales, date_dim
            WHERE ss_sold_date_sk = d_date_sk
            UNION ALL
            SELECT d_year, 'Web' as channel, ws_ext_sales_price as sales
            FROM web_sales, date_dim
            WHERE ws_sold_date_sk = d_date_sk
            UNION ALL
            SELECT d_year, 'Catalog' as channel, cs_ext_sales_price as sales
            FROM catalog_sales, date_dim
            WHERE cs_sold_date_sk = d_date_sk
        )
        SELECT d_year, channel, SUM(sales) as total
        FROM all_sales
        GROUP BY ROLLUP(d_year, channel)
        ORDER BY d_year, channel
    "#;

    let rows = execute_query(&db, query);

    // Should have detail rows, year subtotals, and grand total
    assert!(rows.len() > 5, "Expected multiple rows with rollup");

    // Verify grand total
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some(), "Missing grand total row");
}

// ============================================================================
// TPC-DS Q14 Style: Cross-Channel Sales with ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q14_rollup_cross_sell_analysis() {
    let db = setup_tpcds_olap_db();

    // Q14-style: Items sold in both store and web channels with ROLLUP
    let query = r#"
        SELECT
            i_category,
            i_class,
            SUM(ss_ext_sales_price) as store_sales,
            GROUPING(i_category) as cat_grp,
            GROUPING(i_class) as class_grp
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY ROLLUP(i_category, i_class)
        ORDER BY i_category, i_class
    "#;

    let rows = execute_query(&db, query);

    // Should have category/class detail, category subtotals, and grand total
    assert!(rows.len() >= 4, "Expected multiple rows");

    // Verify category subtotals (NULL class, non-NULL category)
    let category_subtotals: Vec<_> =
        rows.iter().filter(|r| !is_null(&r.values[0]) && is_null(&r.values[1])).collect();
    assert!(!category_subtotals.is_empty(), "Missing category subtotals");

    // For category subtotals, GROUPING(i_category)=0, GROUPING(i_class)=1
    for row in category_subtotals {
        assert_eq!(get_i64(&row.values[3]), 0, "GROUPING(i_category) should be 0");
        assert_eq!(get_i64(&row.values[4]), 1, "GROUPING(i_class) should be 1");
    }
}

// ============================================================================
// TPC-DS Q18 Style: Customer Demographics with ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q18_rollup_customer_analysis() {
    let db = setup_tpcds_olap_db();

    // Q18-style: Sales analysis with ROLLUP on category and year
    let query = r#"
        SELECT
            i_category,
            d_year,
            SUM(ss_ext_sales_price) as sum_sales,
            AVG(ss_ext_sales_price) as avg_sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(i_category, d_year)
        ORDER BY i_category, d_year
    "#;

    let rows = execute_query(&db, query);

    assert!(rows.len() >= 5, "Expected multiple rows with rollup");

    // Verify grand total
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some(), "Missing grand total row");

    // Grand total should have valid aggregates
    let gt = grand_total.unwrap();
    let sum_sales = get_f64(&gt.values[2]);
    assert!(sum_sales > 0.0, "Grand total sum should be positive");
}

// ============================================================================
// TPC-DS Q22 Style: Inventory Analysis with ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q22_rollup_quarterly_analysis() {
    let db = setup_tpcds_olap_db();

    // Q22-style: Quarterly sales with ROLLUP
    let query = r#"
        SELECT
            d_qoy,
            i_category,
            COUNT(*) as cnt,
            SUM(ss_quantity) as sum_qty
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(d_qoy, i_category)
        ORDER BY d_qoy, i_category
    "#;

    let rows = execute_query(&db, query);

    assert!(rows.len() > 4, "Expected multiple rows");

    // Verify quarter subtotals exist
    let q_subtotals: Vec<_> =
        rows.iter().filter(|r| !is_null(&r.values[0]) && is_null(&r.values[1])).collect();
    assert!(!q_subtotals.is_empty(), "Missing quarter subtotals");
}

// ============================================================================
// TPC-DS Q27 Style: Store Profit with ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q27_rollup_store_profit() {
    let db = setup_tpcds_olap_db();

    // Q27-style: Store profit analysis with ROLLUP
    let query = r#"
        SELECT
            s_state,
            s_county,
            SUM(ss_net_profit) as total_profit,
            AVG(ss_net_profit) as avg_profit,
            GROUPING(s_state) as state_grp,
            GROUPING(s_county) as county_grp
        FROM store_sales, store, date_dim
        WHERE ss_store_sk = s_store_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY ROLLUP(s_state, s_county)
        ORDER BY s_state, s_county
    "#;

    let rows = execute_query(&db, query);

    assert!(rows.len() >= 3, "Expected multiple rows");

    // Verify state subtotals (GROUPING(s_state)=0, GROUPING(s_county)=1)
    let state_subtotals: Vec<_> =
        rows.iter().filter(|r| get_i64(&r.values[4]) == 0 && get_i64(&r.values[5]) == 1).collect();
    assert!(!state_subtotals.is_empty(), "Missing state subtotals");
}

// ============================================================================
// TPC-DS Q36 Style: GROUPING SETS
// ============================================================================

#[test]
fn test_tpcds_q36_grouping_sets() {
    let db = setup_tpcds_olap_db();

    // Q36-style: Gross profit using GROUPING SETS
    // Using separate GROUPING columns instead of arithmetic for compatibility
    let query = r#"
        SELECT
            i_category,
            s_state,
            SUM(ss_net_profit) as gross_profit,
            GROUPING(i_category) as cat_grp,
            GROUPING(s_state) as state_grp
        FROM store_sales, store, item, date_dim
        WHERE ss_store_sk = s_store_sk
            AND ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY GROUPING SETS(
            (i_category, s_state),
            (i_category),
            (s_state),
            ()
        )
        ORDER BY cat_grp, state_grp, i_category, s_state
    "#;

    let rows = execute_query(&db, query);

    // Should have: detail rows, category subtotals, state subtotals, grand total
    assert!(rows.len() >= 4, "Expected at least 4 rows");

    // Verify different grouping levels exist
    // Detail level: cat_grp=0, state_grp=0
    let detail_rows: Vec<_> =
        rows.iter().filter(|r| get_i64(&r.values[3]) == 0 && get_i64(&r.values[4]) == 0).collect();
    assert!(!detail_rows.is_empty(), "Missing detail rows (both groupings=0)");

    // Category-only: cat_grp=0, state_grp=1
    let category_only: Vec<_> =
        rows.iter().filter(|r| get_i64(&r.values[3]) == 0 && get_i64(&r.values[4]) == 1).collect();
    assert!(!category_only.is_empty(), "Missing category-only subtotals");

    // State-only: cat_grp=1, state_grp=0
    let state_only: Vec<_> =
        rows.iter().filter(|r| get_i64(&r.values[3]) == 1 && get_i64(&r.values[4]) == 0).collect();
    assert!(!state_only.is_empty(), "Missing state-only subtotals");

    // Grand total: cat_grp=1, state_grp=1
    let grand_total: Vec<_> =
        rows.iter().filter(|r| get_i64(&r.values[3]) == 1 && get_i64(&r.values[4]) == 1).collect();
    assert_eq!(grand_total.len(), 1, "Should have exactly 1 grand total");
}

#[test]
fn test_tpcds_q36_grouping_sets_explicit() {
    let db = setup_tpcds_olap_db();

    // Explicit GROUPING SETS equivalent to Q36
    let query = r#"
        SELECT
            i_category,
            i_class,
            SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY GROUPING SETS(
            (i_category, i_class),
            (i_category),
            ()
        )
        ORDER BY i_category, i_class
    "#;

    let rows = execute_query(&db, query);

    // Verify structure matches ROLLUP(i_category, i_class)
    assert!(rows.len() >= 3, "Expected multiple rows");

    // Verify grand total
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some(), "Missing grand total");
}

// ============================================================================
// TPC-DS Q67 Style: CUBE for Multi-Dimensional Analysis
// ============================================================================

#[test]
fn test_tpcds_q67_cube() {
    let db = setup_tpcds_olap_db();

    // Q67-style: Multi-dimensional analysis with CUBE
    let query = r#"
        SELECT
            i_category,
            i_class,
            i_brand,
            SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY CUBE(i_category, i_class, i_brand)
        LIMIT 50
    "#;

    let rows = execute_query(&db, query);

    // CUBE(3 cols) = 2^3 = 8 grouping combinations
    // With data: many detail rows + all 8 grouping combinations
    assert!(rows.len() >= 8, "Expected at least 8 rows (all cube groupings)");

    // Verify different cube levels exist
    // Level 0: all non-null (detail)
    let detail_rows = rows
        .iter()
        .filter(|r| !is_null(&r.values[0]) && !is_null(&r.values[1]) && !is_null(&r.values[2]))
        .count();
    assert!(detail_rows > 0, "Missing detail rows");

    // Grand total: all null
    let grand_total = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(grand_total.is_some(), "Missing grand total");

    // Category-only subtotal (CUBE feature, not in ROLLUP)
    let category_only = rows
        .iter()
        .find(|r| !is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(category_only.is_some(), "Missing category-only subtotal (CUBE feature)");
}

#[test]
fn test_tpcds_q67_cube_two_dimensions() {
    let db = setup_tpcds_olap_db();

    // Simpler CUBE with 2 dimensions
    let query = r#"
        SELECT
            d_year,
            i_category,
            SUM(ss_quantity) as qty,
            SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY CUBE(d_year, i_category)
        ORDER BY d_year, i_category
    "#;

    let rows = execute_query(&db, query);

    // CUBE(2) = 4 groupings: (year, cat), (year), (cat), ()
    // With 2 years and 3 categories, should have many rows
    assert!(rows.len() >= 4, "Expected at least 4 groupings");

    // Verify category-only grouping exists (distinguishes CUBE from ROLLUP)
    let category_only = rows.iter().find(|r| is_null(&r.values[0]) && !is_null(&r.values[1]));
    assert!(
        category_only.is_some(),
        "Missing category-only subtotal (distinguishes CUBE from ROLLUP)"
    );
}

// ============================================================================
// TPC-DS Q70 Style: State-level ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q70_rollup_state() {
    let db = setup_tpcds_olap_db();

    // Q70-style: Sales by state with ROLLUP
    let query = r#"
        SELECT
            s_state,
            SUM(ss_ext_sales_price) as total_sales,
            SUM(ss_net_profit) as total_profit,
            COUNT(*) as sale_count
        FROM store_sales, store, date_dim
        WHERE ss_store_sk = s_store_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(s_state)
        ORDER BY total_sales DESC
    "#;

    let rows = execute_query(&db, query);

    // Should have state rows + grand total
    assert!(rows.len() >= 2, "Expected at least 2 rows");

    // Grand total should have highest total sales (all states combined)
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]));
    assert!(grand_total.is_some(), "Missing grand total");

    let gt_sales = get_f64(&grand_total.unwrap().values[1]);
    for row in &rows {
        if !is_null(&row.values[0]) {
            let state_sales = get_f64(&row.values[1]);
            assert!(gt_sales >= state_sales, "Grand total should be >= all state totals");
        }
    }
}

// ============================================================================
// TPC-DS Q77 Style: Multi-Channel Profit with ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q77_rollup_channel_profit() {
    let db = setup_tpcds_olap_db();

    // Q77-style: Channel profit analysis with ROLLUP
    let query = r#"
        WITH channel_profit AS (
            SELECT 'Store' as channel, d_year, SUM(ss_net_profit) as profit
            FROM store_sales, date_dim
            WHERE ss_sold_date_sk = d_date_sk
            GROUP BY d_year
            UNION ALL
            SELECT 'Web' as channel, d_year, SUM(ws_net_profit) as profit
            FROM web_sales, date_dim
            WHERE ws_sold_date_sk = d_date_sk
            GROUP BY d_year
            UNION ALL
            SELECT 'Catalog' as channel, d_year, SUM(cs_net_profit) as profit
            FROM catalog_sales, date_dim
            WHERE cs_sold_date_sk = d_date_sk
            GROUP BY d_year
        )
        SELECT channel, d_year, SUM(profit) as total_profit
        FROM channel_profit
        GROUP BY ROLLUP(channel, d_year)
        ORDER BY channel, d_year
    "#;

    let rows = execute_query(&db, query);

    assert!(rows.len() >= 4, "Expected multiple rows");

    // Verify channel subtotals
    let channel_subtotals: Vec<_> =
        rows.iter().filter(|r| !is_null(&r.values[0]) && is_null(&r.values[1])).collect();
    assert!(!channel_subtotals.is_empty(), "Missing channel subtotals");
}

// ============================================================================
// TPC-DS Q80 Style: Combined Store/Web/Catalog Analysis
// ============================================================================

#[test]
fn test_tpcds_q80_rollup_combined_analysis() {
    let db = setup_tpcds_olap_db();

    // Q80-style: Combined channel analysis with ROLLUP
    let query = r#"
        SELECT
            d_year,
            i_category,
            SUM(ss_ext_sales_price) as store_sales,
            SUM(ss_net_profit) as store_profit
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(d_year, i_category)
        HAVING SUM(ss_ext_sales_price) > 50
        ORDER BY d_year, i_category
    "#;

    let rows = execute_query(&db, query);

    // All returned rows should meet HAVING condition
    for row in &rows {
        let sales = get_f64(&row.values[2]);
        assert!(sales > 50.0, "HAVING clause not applied correctly");
    }
}

// ============================================================================
// TPC-DS Q86 Style: Web Sales ROLLUP
// ============================================================================

#[test]
fn test_tpcds_q86_rollup_web_analysis() {
    let db = setup_tpcds_olap_db();

    // Q86-style: Web sales with category rollup
    let query = r#"
        SELECT
            i_category,
            d_year,
            SUM(ws_ext_sales_price) as web_sales
        FROM web_sales, item, date_dim
        WHERE ws_item_sk = i_item_sk
            AND ws_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(i_category, d_year)
        ORDER BY i_category, d_year
    "#;

    let rows = execute_query(&db, query);

    // Should have detail, category subtotals, and grand total
    assert!(rows.len() >= 2, "Expected at least 2 rows");

    // Verify grand total
    let grand_total = rows.iter().find(|r| is_null(&r.values[0]) && is_null(&r.values[1]));
    assert!(grand_total.is_some(), "Missing grand total");
}

// ============================================================================
// Edge Cases and Complex Patterns
// ============================================================================

#[test]
fn test_rollup_with_composite_key() {
    let db = setup_tpcds_olap_db();

    // ROLLUP with composite grouping element
    let query = r#"
        SELECT
            d_year,
            d_qoy,
            i_category,
            SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP((d_year, d_qoy), i_category)
        ORDER BY d_year, d_qoy, i_category
    "#;

    let rows = execute_query(&db, query);

    // (d_year, d_qoy) is treated as a single unit
    // So we get: ((year,qoy), category), ((year,qoy)), ()
    assert!(rows.len() >= 3, "Expected multiple rows");

    // Grand total should have all NULLs
    let grand_total = rows
        .iter()
        .find(|r| is_null(&r.values[0]) && is_null(&r.values[1]) && is_null(&r.values[2]));
    assert!(grand_total.is_some(), "Missing grand total");
}

#[test]
fn test_grouping_sets_union_behavior() {
    let db = setup_tpcds_olap_db();

    // GROUPING SETS should produce same result as explicit UNION ALL
    let gs_query = r#"
        SELECT i_category, d_year, SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk
        GROUP BY GROUPING SETS((i_category, d_year), (i_category))
        ORDER BY i_category, d_year
    "#;

    let gs_rows = execute_query(&db, gs_query);

    // Count rows with category subtotals (NULL year)
    let subtotals = gs_rows.iter().filter(|r| is_null(&r.values[1])).count();
    assert!(subtotals > 0, "Missing category subtotals");
}

#[test]
fn test_cube_with_having_filter() {
    let db = setup_tpcds_olap_db();

    // CUBE with HAVING filters applied after grouping
    let query = r#"
        SELECT
            i_category,
            d_year,
            SUM(ss_ext_sales_price) as sales,
            COUNT(*) as cnt
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY CUBE(i_category, d_year)
        HAVING COUNT(*) >= 2
        ORDER BY sales DESC
    "#;

    let rows = execute_query(&db, query);

    // All rows should have count >= 2
    for row in &rows {
        let cnt = get_i64(&row.values[3]);
        assert!(cnt >= 2, "HAVING clause not applied: count={}", cnt);
    }
}

#[test]
fn test_rollup_null_handling() {
    let db = setup_tpcds_olap_db();

    // Verify NULL in ROLLUP is correctly identified by GROUPING()
    let query = r#"
        SELECT
            i_category,
            d_year,
            GROUPING(i_category) as g_cat,
            GROUPING(d_year) as g_year,
            SUM(ss_ext_sales_price) as sales
        FROM store_sales, item, date_dim
        WHERE ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
        GROUP BY ROLLUP(i_category, d_year)
    "#;

    let rows = execute_query(&db, query);

    // For each row, check GROUPING function correctness
    for row in &rows {
        let cat_is_null = is_null(&row.values[0]);
        let year_is_null = is_null(&row.values[1]);
        let g_cat = get_i64(&row.values[2]);
        let g_year = get_i64(&row.values[3]);

        // GROUPING should be 1 when NULL from rollup, 0 otherwise
        if cat_is_null {
            assert_eq!(g_cat, 1, "GROUPING(i_category) should be 1 when NULL");
        } else {
            assert_eq!(g_cat, 0, "GROUPING(i_category) should be 0 when not NULL");
        }

        if year_is_null {
            assert_eq!(g_year, 1, "GROUPING(d_year) should be 1 when NULL");
        } else {
            assert_eq!(g_year, 0, "GROUPING(d_year) should be 0 when not NULL");
        }
    }
}

// ============================================================================
// Pivot Aggregate Optimization Tests (Issue #3136)
// ============================================================================

/// Test the TPC-DS Q2-style pivot pattern with multiple SUM(CASE...) aggregates
/// This is the specific pattern optimized by PivotAggregateGroup
#[test]
fn test_pivot_aggregate_pattern_tpcds_q2() {
    let mut db = Database::new();

    // Create a simplified weekly_sales table similar to TPC-DS
    execute_ddl(
        &mut db,
        r#"CREATE TABLE weekly_sales (
            week_seq INTEGER,
            d_day_name TEXT,
            sales_price FLOAT
        )"#,
    );

    // Insert test data across different days and weeks
    let inserts = [
        // Week 1
        "INSERT INTO weekly_sales VALUES (1, 'Sunday', 100.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Monday', 150.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Tuesday', 120.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Wednesday', 130.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Thursday', 140.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Friday', 200.0)",
        "INSERT INTO weekly_sales VALUES (1, 'Saturday', 250.0)",
        // Week 2
        "INSERT INTO weekly_sales VALUES (2, 'Sunday', 110.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Monday', 160.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Tuesday', 125.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Wednesday', 135.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Thursday', 145.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Friday', 210.0)",
        "INSERT INTO weekly_sales VALUES (2, 'Saturday', 260.0)",
    ];

    for sql in inserts {
        execute_insert(&mut db, sql);
    }

    // TPC-DS Q2 style query with 7 pivot aggregates
    let query = r#"
        SELECT
            week_seq,
            SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) as sun_sales,
            SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) as mon_sales,
            SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) as tue_sales,
            SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) as wed_sales,
            SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) as thu_sales,
            SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) as fri_sales,
            SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) as sat_sales
        FROM weekly_sales
        GROUP BY week_seq
        ORDER BY week_seq
    "#;

    let rows = execute_query(&db, query);

    // Verify results
    assert_eq!(rows.len(), 2, "Should have 2 weeks of results");

    // Week 1 results
    assert_eq!(get_i64(&rows[0].values[0]), 1, "First row should be week 1");
    assert_eq!(get_f64(&rows[0].values[1]), 100.0, "Sunday sales week 1");
    assert_eq!(get_f64(&rows[0].values[2]), 150.0, "Monday sales week 1");
    assert_eq!(get_f64(&rows[0].values[3]), 120.0, "Tuesday sales week 1");
    assert_eq!(get_f64(&rows[0].values[4]), 130.0, "Wednesday sales week 1");
    assert_eq!(get_f64(&rows[0].values[5]), 140.0, "Thursday sales week 1");
    assert_eq!(get_f64(&rows[0].values[6]), 200.0, "Friday sales week 1");
    assert_eq!(get_f64(&rows[0].values[7]), 250.0, "Saturday sales week 1");

    // Week 2 results
    assert_eq!(get_i64(&rows[1].values[0]), 2, "Second row should be week 2");
    assert_eq!(get_f64(&rows[1].values[1]), 110.0, "Sunday sales week 2");
    assert_eq!(get_f64(&rows[1].values[2]), 160.0, "Monday sales week 2");
    assert_eq!(get_f64(&rows[1].values[3]), 125.0, "Tuesday sales week 2");
    assert_eq!(get_f64(&rows[1].values[4]), 135.0, "Wednesday sales week 2");
    assert_eq!(get_f64(&rows[1].values[5]), 145.0, "Thursday sales week 2");
    assert_eq!(get_f64(&rows[1].values[6]), 210.0, "Friday sales week 2");
    assert_eq!(get_f64(&rows[1].values[7]), 260.0, "Saturday sales week 2");
}

/// Test pivot aggregates without GROUP BY (single group)
#[test]
fn test_pivot_aggregate_no_group_by() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE day_sales (
            day_name TEXT,
            amount FLOAT
        )"#,
    );

    let inserts = [
        "INSERT INTO day_sales VALUES ('Monday', 100.0)",
        "INSERT INTO day_sales VALUES ('Monday', 50.0)",
        "INSERT INTO day_sales VALUES ('Tuesday', 200.0)",
        "INSERT INTO day_sales VALUES ('Wednesday', 150.0)",
    ];

    for sql in inserts {
        execute_insert(&mut db, sql);
    }

    // Pivot query without GROUP BY
    let query = r#"
        SELECT
            SUM(CASE WHEN day_name = 'Monday' THEN amount ELSE NULL END) as mon_total,
            SUM(CASE WHEN day_name = 'Tuesday' THEN amount ELSE NULL END) as tue_total,
            SUM(CASE WHEN day_name = 'Wednesday' THEN amount ELSE NULL END) as wed_total
        FROM day_sales
    "#;

    let rows = execute_query(&db, query);

    assert_eq!(rows.len(), 1, "Should have exactly one result row");
    assert_eq!(get_f64(&rows[0].values[0]), 150.0, "Monday total (100 + 50)");
    assert_eq!(get_f64(&rows[0].values[1]), 200.0, "Tuesday total");
    assert_eq!(get_f64(&rows[0].values[2]), 150.0, "Wednesday total");
}
