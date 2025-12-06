//! Issue #3731: TPC-DS validation failures for Q64 and Q75
//!
//! Q64: Returns 2 rows vs DuckDB's 1 row
//! Q75: Returns 35 rows vs DuckDB's 40 rows
//!
//! This test file helps debug these discrepancies by comparing actual results.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};

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

fn execute_query(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse query");
    match stmt {
        Statement::Select(select) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select).expect("Failed to execute query")
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// Minimal test case for Q64
/// Q64 uses a join on TWO conditions: cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
/// This is important because it's testing composite key matching
#[test]
fn test_q64_minimal() {
    let mut db = Database::new();

    // Create tables
    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_sales (
            cs_item_sk INTEGER,
            cs_order_number INTEGER,
            cs_ext_list_price DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_returns (
            cr_item_sk INTEGER,
            cr_order_number INTEGER,
            cr_refunded_cash DECIMAL(10,2),
            cr_reversed_charge DECIMAL(10,2),
            cr_store_credit DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY
        )"#,
    );

    // Insert test data
    // Scenario: Item 1 with order 100 - should match
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 100, 200.00)");
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 100, 100.00)"); // Same item, same order
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (1, 100, 10.00, 5.00, 5.00)");

    // Item 2 with order 200 - different scenario
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (2, 200, 500.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (2, 200, 50.00, 25.00, 25.00)");

    // Item 3 - no return (shouldn't match the join)
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (3, 300, 1000.00)");

    // Items
    execute_insert(&mut db, "INSERT INTO item VALUES (1)");
    execute_insert(&mut db, "INSERT INTO item VALUES (2)");
    execute_insert(&mut db, "INSERT INTO item VALUES (3)");

    // Q64 query (simplified)
    let results = execute_query(
        &db,
        r#"
        WITH cs_ui AS (
            SELECT
                cs_item_sk,
                SUM(cs_ext_list_price) AS sale,
                SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
            FROM catalog_sales, catalog_returns
            WHERE cs_item_sk = cr_item_sk
                AND cs_order_number = cr_order_number
            GROUP BY cs_item_sk
            HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
        )
        SELECT
            cs_ui.cs_item_sk,
            sale,
            refund
        FROM cs_ui, item
        WHERE i_item_sk = cs_item_sk
        ORDER BY cs_item_sk
        "#,
    );

    println!("Q64 minimal test results:");
    for row in &results {
        println!("  {:?}", row);
    }

    // The test is to understand the behavior
    // For item 1: sale = 300.00, refund = 20.00, 300 > 2*20 = 40? Yes
    // For item 2: sale = 500.00, refund = 100.00, 500 > 2*100 = 200? Yes
    // Both should pass HAVING
    assert_eq!(results.len(), 2, "Expected 2 rows");
}

/// Test case focusing on the cross join multiplication issue
/// When there are multiple sales rows and multiple returns rows for the same (item, order),
/// the cross join can multiply the rows incorrectly
#[test]
fn test_q64_cross_join_multiplication() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_sales (
            cs_item_sk INTEGER,
            cs_order_number INTEGER,
            cs_ext_list_price DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_returns (
            cr_item_sk INTEGER,
            cr_order_number INTEGER,
            cr_refunded_cash DECIMAL(10,2),
            cr_reversed_charge DECIMAL(10,2),
            cr_store_credit DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY
        )"#,
    );

    // Single sale, single return - no multiplication
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 100, 100.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (1, 100, 10.00, 0.00, 0.00)");

    // Multiple sales, single return - should sum sales correctly
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (2, 200, 50.00)");
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (2, 200, 50.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (2, 200, 10.00, 0.00, 0.00)");

    // Single sale, multiple returns - should sum returns correctly
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (3, 300, 100.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (3, 300, 5.00, 0.00, 0.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (3, 300, 5.00, 0.00, 0.00)");

    execute_insert(&mut db, "INSERT INTO item VALUES (1)");
    execute_insert(&mut db, "INSERT INTO item VALUES (2)");
    execute_insert(&mut db, "INSERT INTO item VALUES (3)");

    // First let's check the raw join to understand what's happening
    let join_results = execute_query(
        &db,
        r#"
        SELECT
            cs_item_sk,
            cs_order_number,
            cs_ext_list_price,
            cr_refunded_cash
        FROM catalog_sales, catalog_returns
        WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
        ORDER BY cs_item_sk, cs_order_number
        "#,
    );

    println!("Raw join results (expecting cross product within matched groups):");
    for row in &join_results {
        println!("  {:?}", row);
    }

    // Item 1: 1 sale x 1 return = 1 row
    // Item 2: 2 sales x 1 return = 2 rows
    // Item 3: 1 sale x 2 returns = 2 rows
    // Total = 5 rows

    // Now check the aggregated results
    let agg_results = execute_query(
        &db,
        r#"
        SELECT
            cs_item_sk,
            SUM(cs_ext_list_price) AS sale,
            SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
        FROM catalog_sales, catalog_returns
        WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
        GROUP BY cs_item_sk
        ORDER BY cs_item_sk
        "#,
    );

    println!("\nAggregated results:");
    for row in &agg_results {
        println!("  {:?}", row);
    }

    // CRITICAL ISSUE: When doing a cross join and then aggregating:
    // Item 1: sale = 100, refund = 10 (correct - 1x1)
    // Item 2: sale = 50+50 (from 2 sales) BUT each is counted once per return
    //         Since there's 1 return, sale = 100, refund = 10 (correct)
    // Item 3: sale = 100 (from 1 sale) BUT it's counted once per return (2 returns)
    //         So sale = 100*2 = 200 (INCORRECT!), refund = 5+5 = 10

    // This is the fundamental problem with cross joins + aggregation
}

/// Minimal test case for Q75
/// Q75 uses LEFT JOINs to include sales that don't have returns
/// The issue is UNION ALL + LEFT JOIN combination
#[test]
fn test_q75_minimal() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY,
            i_brand_id INTEGER,
            i_class_id INTEGER,
            i_category_id INTEGER,
            i_manufact_id INTEGER,
            i_category TEXT
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE date_dim (
            d_date_sk INTEGER PRIMARY KEY,
            d_year INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_sales (
            cs_item_sk INTEGER,
            cs_sold_date_sk INTEGER,
            cs_order_number INTEGER,
            cs_quantity INTEGER,
            cs_ext_sales_price DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_returns (
            cr_item_sk INTEGER,
            cr_order_number INTEGER,
            cr_return_quantity INTEGER,
            cr_return_amount DECIMAL(10,2)
        )"#,
    );

    // Insert test data
    execute_insert(&mut db, "INSERT INTO item VALUES (1, 1, 1, 1, 1, 'Books')");
    execute_insert(&mut db, "INSERT INTO date_dim VALUES (1, 2000)");
    execute_insert(&mut db, "INSERT INTO date_dim VALUES (2, 2001)");

    // Sale in 2000 with no return
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 1, 100, 5, 50.00)");
    // Sale in 2001 with no return
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 2, 101, 3, 30.00)");
    // Sale in 2000 with return
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 1, 102, 10, 100.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (1, 102, 2, 20.00)");

    // Test just the catalog_sales part with LEFT JOIN
    let results = execute_query(
        &db,
        r#"
        SELECT
            d_year,
            i_brand_id,
            cs_quantity,
            COALESCE(cr_return_quantity, 0) as cr_qty,
            cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
            cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
        FROM catalog_sales
        JOIN item ON i_item_sk = cs_item_sk
        JOIN date_dim ON d_date_sk = cs_sold_date_sk
        LEFT JOIN catalog_returns ON cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk
        WHERE i_category = 'Books'
        ORDER BY d_year, cs_order_number
        "#,
    );

    println!("Q75 catalog_sales part (with LEFT JOIN):");
    for row in &results {
        println!("  {:?}", row);
    }

    // Expected:
    // Order 100 (2000): qty=5, no return → sales_cnt=5
    // Order 101 (2001): qty=3, no return → sales_cnt=3
    // Order 102 (2000): qty=10, return qty=2 → sales_cnt=8
}

/// Test to verify that the cross-join behavior matches expected SQL semantics.
/// The issue in Q64 might be that the HAVING clause filters differently due to
/// data-dependent behavior from cross-join multiplication.
#[test]
fn test_q64_having_filter_edge_case() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_sales (
            cs_item_sk INTEGER,
            cs_order_number INTEGER,
            cs_ext_list_price DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE catalog_returns (
            cr_item_sk INTEGER,
            cr_order_number INTEGER,
            cr_refunded_cash DECIMAL(10,2),
            cr_reversed_charge DECIMAL(10,2),
            cr_store_credit DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE item (
            i_item_sk INTEGER PRIMARY KEY
        )"#,
    );

    // Scenario where cross-join multiplication changes HAVING outcome:
    // Item 1: 1 sale of 100, 2 returns of 25 each = 50 total refund
    // Correct: sale=100, refund=50, 100 > 2*50? NO (fails HAVING)
    // Multiplied: sale=200, refund=50, 200 > 2*50=100? YES (passes HAVING incorrectly)
    execute_insert(&mut db, "INSERT INTO catalog_sales VALUES (1, 100, 100.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (1, 100, 25.00, 0.00, 0.00)");
    execute_insert(&mut db, "INSERT INTO catalog_returns VALUES (1, 100, 25.00, 0.00, 0.00)");
    execute_insert(&mut db, "INSERT INTO item VALUES (1)");

    // First, check what the aggregation returns BEFORE the HAVING filter
    let before_having = execute_query(
        &db,
        r#"
        SELECT
            cs_item_sk,
            SUM(cs_ext_list_price) AS sale,
            SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
        FROM catalog_sales, catalog_returns
        WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
        GROUP BY cs_item_sk
        "#,
    );

    println!("Before HAVING (should show sale=200 due to cross-join multiplication):");
    for row in &before_having {
        println!("  {:?}", row);
    }

    // Now check with the HAVING filter
    let after_having = execute_query(
        &db,
        r#"
        WITH cs_ui AS (
            SELECT
                cs_item_sk,
                SUM(cs_ext_list_price) AS sale,
                SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
            FROM catalog_sales, catalog_returns
            WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
            GROUP BY cs_item_sk
            HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
        )
        SELECT cs_ui.cs_item_sk, sale, refund
        FROM cs_ui, item
        WHERE i_item_sk = cs_item_sk
        "#,
    );

    println!("\nAfter HAVING (200 > 100 is true, so should pass HAVING but with wrong sale value):");
    for row in &after_having {
        println!("  {:?}", row);
    }

    // If cross-join multiplication is happening, we get 1 row with sale=200, refund=50
    // This incorrectly passes the HAVING because 200 > 100
    // But the "correct" semantic would be sale=100, refund=50, which fails 100 > 100
    assert_eq!(
        after_having.len(),
        1,
        "With cross-join multiplication, item passes HAVING with inflated sale"
    );
}

/// Test Q75 division filter edge cases
/// Q75 has this condition: CAST(curr_yr.sales_cnt AS DECIMAL) / CAST(prev_yr.sales_cnt AS DECIMAL) < 0.9
/// If prev_yr.sales_cnt is 0, this could cause issues
#[test]
fn test_q75_division_edge_cases() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE test_data (
            year INTEGER,
            sales_cnt INTEGER
        )"#,
    );

    // Test case 1: Division where result < 0.9
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2000, 100)");
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2001, 80)");

    // Test case 2: Division where result = 0.9 (edge case)
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2000, 100)");
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2001, 90)");

    // Test case 3: Division where result > 0.9
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2000, 100)");
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2001, 95)");

    // Test case 4: Zero in denominator
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2000, 0)");
    execute_insert(&mut db, "INSERT INTO test_data VALUES (2001, 50)");

    let results = execute_query(
        &db,
        r#"
        SELECT
            curr.year,
            prev.sales_cnt AS prev_cnt,
            curr.sales_cnt AS curr_cnt,
            CAST(curr.sales_cnt AS DECIMAL) / CAST(prev.sales_cnt AS DECIMAL) AS ratio
        FROM test_data curr, test_data prev
        WHERE curr.year = 2001 AND prev.year = 2000
        ORDER BY prev.sales_cnt
        "#,
    );

    println!("Division test results:");
    for row in &results {
        println!("  {:?}", row);
    }

    // Now test with the filter
    let filtered_results = execute_query(
        &db,
        r#"
        SELECT
            curr.year,
            prev.sales_cnt AS prev_cnt,
            curr.sales_cnt AS curr_cnt
        FROM test_data curr, test_data prev
        WHERE curr.year = 2001 AND prev.year = 2000
            AND CAST(curr.sales_cnt AS DECIMAL) / CAST(prev.sales_cnt AS DECIMAL) < 0.9
        ORDER BY prev.sales_cnt
        "#,
    );

    println!("\nFiltered results (ratio < 0.9):");
    for row in &filtered_results {
        println!("  {:?}", row);
    }
}

/// Test COALESCE with NULL from LEFT JOIN
#[test]
fn test_q75_left_join_coalesce() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE lj_sales (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER,
            price DECIMAL(10,2)
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE lj_returns (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER,
            return_amt DECIMAL(10,2)
        )"#,
    );

    // Sale with no return
    execute_insert(&mut db, "INSERT INTO lj_sales VALUES (1, 100, 10, 100.00)");

    // Sale with return
    execute_insert(&mut db, "INSERT INTO lj_sales VALUES (2, 200, 20, 200.00)");
    execute_insert(&mut db, "INSERT INTO lj_returns VALUES (2, 200, 5, 50.00)");

    let results = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity,
            COALESCE(return_qty, 0) AS return_qty,
            quantity - COALESCE(return_qty, 0) AS net_qty,
            price - COALESCE(return_amt, 0.0) AS net_amt
        FROM lj_sales
        LEFT JOIN lj_returns ON lj_sales.item_sk = lj_returns.item_sk AND lj_sales.order_num = lj_returns.order_num
        ORDER BY item_sk
        "#,
    );

    println!("LEFT JOIN with COALESCE results:");
    for row in &results {
        println!("  {:?}", row);
    }

    // Item 1: net_qty = 10 - 0 = 10, net_amt = 100 - 0 = 100
    // Item 2: net_qty = 20 - 5 = 15, net_amt = 200 - 50 = 150
    assert_eq!(results.len(), 2);
}

/// Test LEFT JOIN column preservation
/// The left side columns should NEVER be NULL unless the source data is NULL
#[test]
fn test_left_join_preserves_left_columns() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE left_table (
            id INTEGER PRIMARY KEY,
            value TEXT
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE right_table (
            id INTEGER,
            extra TEXT
        )"#,
    );

    execute_insert(&mut db, "INSERT INTO left_table VALUES (1, 'a')");
    execute_insert(&mut db, "INSERT INTO left_table VALUES (2, 'b')");
    execute_insert(&mut db, "INSERT INTO left_table VALUES (3, 'c')");
    
    // Only match id=2
    execute_insert(&mut db, "INSERT INTO right_table VALUES (2, 'matched')");

    let results = execute_query(
        &db,
        r#"
        SELECT left_table.id, left_table.value, right_table.id, right_table.extra
        FROM left_table
        LEFT JOIN right_table ON left_table.id = right_table.id
        ORDER BY left_table.id
        "#,
    );

    println!("LEFT JOIN column preservation test:");
    for row in &results {
        println!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows (all from left table)");
    
    // Check that left.id is NEVER null
    for (i, row) in results.iter().enumerate() {
        let left_id = &row.values[0];
        assert!(
            !matches!(left_id, vibesql_types::SqlValue::Null),
            "Row {}: left_table.id should NOT be NULL, got {:?}",
            i,
            left_id
        );
    }
}

/// Test that unqualified column references in LEFT JOIN
/// resolve to the LEFT table, not the RIGHT table (which would be NULL)
#[test]
fn test_unqualified_column_in_left_join() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE sales_uk (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE returns_uk (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER
        )"#,
    );

    // Sale with no return
    execute_insert(&mut db, "INSERT INTO sales_uk VALUES (1, 100, 10)");

    // Sale with return
    execute_insert(&mut db, "INSERT INTO sales_uk VALUES (2, 200, 20)");
    execute_insert(&mut db, "INSERT INTO returns_uk VALUES (2, 200, 5)");

    // Test 1: Qualified reference (should work correctly)
    let results_qualified = execute_query(
        &db,
        r#"
        SELECT
            sales_uk.item_sk,
            quantity
        FROM sales_uk
        LEFT JOIN returns_uk ON sales_uk.item_sk = returns_uk.item_sk AND sales_uk.order_num = returns_uk.order_num
        ORDER BY sales_uk.item_sk
        "#,
    );

    println!("QUALIFIED column reference results:");
    for row in &results_qualified {
        println!("  {:?}", row);
    }

    // Test 2: Unqualified reference (BUG: may incorrectly resolve to right table)
    let results_unqualified = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity
        FROM sales_uk
        LEFT JOIN returns_uk ON sales_uk.item_sk = returns_uk.item_sk AND sales_uk.order_num = returns_uk.order_num
        ORDER BY item_sk
        "#,
    );

    println!("UNQUALIFIED column reference results:");
    for row in &results_unqualified {
        println!("  {:?}", row);
    }

    // Both should produce the same results!
    // Specifically, item_sk should NEVER be NULL (it's from the left table)
    assert_eq!(results_qualified.len(), 2);
    assert_eq!(results_unqualified.len(), 2);

    // Check item_sk is not null in unqualified case
    for (i, row) in results_unqualified.iter().enumerate() {
        let item_sk = &row.values[0];
        println!("Row {}: item_sk = {:?}", i, item_sk);
        // BUG: This may fail if item_sk resolves to returns_uk.item_sk instead of sales_uk.item_sk
        assert!(
            !matches!(item_sk, vibesql_types::SqlValue::Null),
            "Row {}: item_sk should NOT be NULL (should resolve to sales_uk.item_sk), got {:?}",
            i,
            item_sk
        );
    }
}

/// Test column resolution with COALESCE in expressions
/// This is more like the actual Q75 query
#[test]
fn test_column_resolution_with_coalesce() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE sales_crc (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE returns_crc (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER
        )"#,
    );

    // Sale with no return - item 1
    execute_insert(&mut db, "INSERT INTO sales_crc VALUES (1, 100, 10)");

    // Sale with return - item 2
    execute_insert(&mut db, "INSERT INTO sales_crc VALUES (2, 200, 20)");
    execute_insert(&mut db, "INSERT INTO returns_crc VALUES (2, 200, 5)");

    // Query with COALESCE (like Q75)
    let results = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity,
            COALESCE(return_qty, 0) AS return_qty
        FROM sales_crc
        LEFT JOIN returns_crc ON sales_crc.item_sk = returns_crc.item_sk AND sales_crc.order_num = returns_crc.order_num
        ORDER BY item_sk
        "#,
    );

    println!("Column resolution with COALESCE:");
    for row in &results {
        println!("  {:?}", row);
    }

    // item_sk should be 1, 2 - NOT null for item 1
    for (i, row) in results.iter().enumerate() {
        let item_sk = &row.values[0];
        println!("Row {}: item_sk = {:?}", i, item_sk);
        assert!(
            !matches!(item_sk, vibesql_types::SqlValue::Null),
            "Row {}: item_sk should NOT be NULL, got {:?}",
            i,
            item_sk
        );
    }
}

/// Test column resolution: 3 columns without expressions
#[test]
fn test_column_resolution_three_cols_no_expr() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE sales_3c (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE returns_3c (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER
        )"#,
    );

    // Sale with no return - item 1
    execute_insert(&mut db, "INSERT INTO sales_3c VALUES (1, 100, 10)");

    // Sale with return - item 2
    execute_insert(&mut db, "INSERT INTO sales_3c VALUES (2, 200, 20)");
    execute_insert(&mut db, "INSERT INTO returns_3c VALUES (2, 200, 5)");

    // Query with 3 simple columns, no expressions
    let results = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity,
            return_qty
        FROM sales_3c
        LEFT JOIN returns_3c ON sales_3c.item_sk = returns_3c.item_sk AND sales_3c.order_num = returns_3c.order_num
        ORDER BY item_sk
        "#,
    );

    println!("Three columns, no expression:");
    for row in &results {
        println!("  {:?}", row);
    }

    // item_sk should be 1, 2 - NOT null for item 1
    for (i, row) in results.iter().enumerate() {
        let item_sk = &row.values[0];
        println!("Row {}: item_sk = {:?}", i, item_sk);
        assert!(
            !matches!(item_sk, vibesql_types::SqlValue::Null),
            "Row {}: item_sk should NOT be NULL, got {:?}",
            i,
            item_sk
        );
    }
}

/// Test column resolution: 3 columns with expression on right-table column
#[test]
fn test_column_resolution_expr_on_right_col() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE sales_roe (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE returns_roe (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER
        )"#,
    );

    // Sale with no return - item 1
    execute_insert(&mut db, "INSERT INTO sales_roe VALUES (1, 100, 10)");

    // Sale with return - item 2
    execute_insert(&mut db, "INSERT INTO sales_roe VALUES (2, 200, 20)");
    execute_insert(&mut db, "INSERT INTO returns_roe VALUES (2, 200, 5)");

    // Query with expression on RIGHT table column
    let results = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity,
            return_qty + 1 AS return_qty_plus
        FROM sales_roe
        LEFT JOIN returns_roe ON sales_roe.item_sk = returns_roe.item_sk AND sales_roe.order_num = returns_roe.order_num
        ORDER BY item_sk
        "#,
    );

    println!("Expression on RIGHT table column:");
    for row in &results {
        println!("  {:?}", row);
    }

    // item_sk should be 1, 2 - NOT null for item 1
    for (i, row) in results.iter().enumerate() {
        let item_sk = &row.values[0];
        println!("Row {}: item_sk = {:?}", i, item_sk);
        assert!(
            !matches!(item_sk, vibesql_types::SqlValue::Null),
            "Row {}: item_sk should NOT be NULL, got {:?}",
            i,
            item_sk
        );
    }
}

/// Test column resolution with just arithmetic expression (no COALESCE)
#[test]
fn test_column_resolution_with_arithmetic() {
    let mut db = Database::new();

    execute_ddl(
        &mut db,
        r#"CREATE TABLE sales_arith (
            item_sk INTEGER,
            order_num INTEGER,
            quantity INTEGER
        )"#,
    );

    execute_ddl(
        &mut db,
        r#"CREATE TABLE returns_arith (
            item_sk INTEGER,
            order_num INTEGER,
            return_qty INTEGER
        )"#,
    );

    // Sale with no return - item 1
    execute_insert(&mut db, "INSERT INTO sales_arith VALUES (1, 100, 10)");

    // Sale with return - item 2
    execute_insert(&mut db, "INSERT INTO sales_arith VALUES (2, 200, 20)");
    execute_insert(&mut db, "INSERT INTO returns_arith VALUES (2, 200, 5)");

    // Query with arithmetic expression (but no COALESCE)
    let results = execute_query(
        &db,
        r#"
        SELECT
            item_sk,
            quantity,
            quantity + 1 AS qty_plus_one
        FROM sales_arith
        LEFT JOIN returns_arith ON sales_arith.item_sk = returns_arith.item_sk AND sales_arith.order_num = returns_arith.order_num
        ORDER BY item_sk
        "#,
    );

    println!("Column resolution with arithmetic expression:");
    for row in &results {
        println!("  {:?}", row);
    }

    // item_sk should be 1, 2 - NOT null for item 1
    for (i, row) in results.iter().enumerate() {
        let item_sk = &row.values[0];
        println!("Row {}: item_sk = {:?}", i, item_sk);
        assert!(
            !matches!(item_sk, vibesql_types::SqlValue::Null),
            "Row {}: item_sk should NOT be NULL, got {:?}",
            i,
            item_sk
        );
    }
}
