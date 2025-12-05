//! Test for TPC-H Q15 with CTE support
//!
//! This test verifies basic CTE functionality needed for TPC-H Q15.
//! Note: Full Q15 support requires CTEs to be accessible from scalar subqueries,
//! which is tracked as a separate enhancement.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute SQL
fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::Select(select_stmt) => {
            let select_executor = SelectExecutor::new(db);
            select_executor.execute(&select_stmt).map_err(|e| format!("Select error: {:?}", e))
        }
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute(&create, db)
                .map_err(|e| format!("Create error: {:?}", e))?;
            Ok(vec![])
        }
        Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert).map_err(|e| format!("Insert error: {:?}", e))?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

fn setup_q15_tables(db: &mut Database) {
    // Create tables
    execute_sql(
        db,
        "CREATE TABLE lineitem_q15 (
            l_orderkey INTEGER,
            l_suppkey INTEGER,
            l_extendedprice FLOAT,
            l_discount FLOAT,
            l_shipdate DATE
        )",
    )
    .unwrap();

    execute_sql(
        db,
        "CREATE TABLE supplier_q15 (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25),
            s_address VARCHAR(40),
            s_phone VARCHAR(15)
        )",
    )
    .unwrap();

    // Insert supplier data
    execute_sql(
        db,
        "INSERT INTO supplier_q15 VALUES (1, 'Supplier#1', 'Address1', '111-111-1111')",
    )
    .unwrap();
    execute_sql(
        db,
        "INSERT INTO supplier_q15 VALUES (2, 'Supplier#2', 'Address2', '222-222-2222')",
    )
    .unwrap();

    // Insert lineitem data
    // Supplier #1 total: 100*(1-0.1) + 200*(1-0.05) = 90 + 190 = 280
    execute_sql(db, "INSERT INTO lineitem_q15 VALUES (1, 1, 100.0, 0.1, '1996-02-01')").unwrap();
    execute_sql(db, "INSERT INTO lineitem_q15 VALUES (2, 1, 200.0, 0.05, '1996-02-15')").unwrap();
    // Supplier #2 total: 150*(1-0.1) = 135
    execute_sql(db, "INSERT INTO lineitem_q15 VALUES (3, 2, 150.0, 0.1, '1996-03-01')").unwrap();
}

#[test]
fn test_basic_cte_with_group_by() {
    let mut db = Database::new();
    setup_q15_tables(&mut db);

    // Test basic CTE with GROUP BY (core Q15 pattern)
    let cte_query = r#"
WITH revenue AS (
    SELECT l_suppkey, SUM(l_extendedprice) as total
    FROM lineitem_q15
    GROUP BY l_suppkey
)
SELECT l_suppkey, total
FROM revenue
ORDER BY total DESC
"#;

    let result = execute_sql(&mut db, cte_query);
    assert!(result.is_ok(), "CTE with GROUP BY should work: {:?}", result);
    let rows = result.unwrap();
    assert_eq!(rows.len(), 2, "Should return 2 suppliers");

    // Verify the first row (highest total should be supplier 1 with 300.0)
    if let SqlValue::Integer(suppkey) = &rows[0].values[0] {
        assert_eq!(*suppkey, 1, "Supplier 1 should have highest total");
    }
}

#[test]
fn test_cte_join_with_table() {
    let mut db = Database::new();
    setup_q15_tables(&mut db);

    // Test CTE joined with a table (like Q15 main query part)
    let cte_query = r#"
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem_q15
    GROUP BY l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    total_revenue
FROM supplier_q15, revenue
WHERE s_suppkey = supplier_no
ORDER BY total_revenue DESC
"#;

    let result = execute_sql(&mut db, cte_query);
    assert!(result.is_ok(), "CTE joined with table should work: {:?}", result);
    let rows = result.unwrap();
    assert_eq!(rows.len(), 2, "Should return 2 suppliers");

    // First row should be supplier 1 with revenue 280.0
    if let SqlValue::Integer(suppkey) = &rows[0].values[0] {
        assert_eq!(*suppkey, 1, "Supplier 1 should have highest revenue");
    }
}

#[test]
fn test_q15_without_max_subquery() {
    let mut db = Database::new();
    setup_q15_tables(&mut db);

    // Test Q15-like query without the MAX scalar subquery
    // Uses simplified filters without date comparison to test core CTE functionality
    let q15_simplified = r#"
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem_q15
    GROUP BY l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier_q15, revenue
WHERE s_suppkey = supplier_no
ORDER BY total_revenue DESC
LIMIT 1
"#;

    let result = execute_sql(&mut db, q15_simplified);
    assert!(result.is_ok(), "Simplified Q15 should work: {:?}", result);
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1, "Should return top 1 supplier");

    // First column should be s_suppkey = 1 (Supplier#1 has higher revenue: 280 vs 135)
    if let SqlValue::Integer(suppkey) = &rows[0].values[0] {
        assert_eq!(*suppkey, 1, "Top supplier should be Supplier#1 (suppkey=1)");
    }
}

#[test]
fn test_cte_without_column_list_empty_result() {
    // Regression test for issue #2417
    // Tests that CTEs without explicit column lists can infer schema
    // even when the result set is empty
    let mut db = Database::new();
    setup_q15_tables(&mut db);

    // CTE with no explicit column list, filtered to return empty result
    let empty_cte_query = r#"
WITH revenue AS (
    SELECT
        l_suppkey as supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) as total_revenue
    FROM lineitem_q15
    WHERE l_shipdate > '2099-01-01'
    GROUP BY l_suppkey
)
SELECT supplier_no, total_revenue
FROM revenue
"#;

    let result = execute_sql(&mut db, empty_cte_query);
    assert!(
        result.is_ok(),
        "CTE without column list should work even with empty results: {:?}",
        result
    );
    let rows = result.unwrap();
    assert_eq!(rows.len(), 0, "Should return 0 rows (empty result set)");
}
