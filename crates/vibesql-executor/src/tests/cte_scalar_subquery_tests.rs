//! Tests for CTE references in scalar subqueries (issue #2602)
//!
//! This module tests the TPC-H Q15 pattern where a CTE is defined once
//! and referenced both in the main FROM clause and in a scalar subquery.
//! This pattern enables finding rows that match an aggregate computed from the CTE.

use super::super::*;
use vibesql_ast::Statement;
use vibesql_parser::Parser;

fn execute_sql(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| ExecutorError::ParseError(format!("{:?}", e)))?;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
            Ok(vec![])
        }
        Statement::Select(select_stmt) => {
            let result = SelectExecutor::new(db).execute(&select_stmt)?;
            Ok(result)
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Unsupported statement type: {:?}",
            stmt
        ))),
    }
}

/// Test the basic TPC-H Q15 pattern: CTE referenced in scalar subquery
#[test]
fn test_cte_in_scalar_subquery_basic() {
    let mut db = vibesql_storage::Database::new();

    // Create test tables
    execute_sql(&mut db, "CREATE TABLE supplier(s_suppkey INT, s_name VARCHAR(25))").unwrap();
    execute_sql(&mut db, "CREATE TABLE lineitem(l_suppkey INT, l_amount DECIMAL(10,2))").unwrap();

    // Insert test data
    execute_sql(&mut db, "INSERT INTO supplier VALUES (1, 'Supplier A')").unwrap();
    execute_sql(&mut db, "INSERT INTO supplier VALUES (2, 'Supplier B')").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (1, 100.00)").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (1, 200.00)").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (2, 150.00)").unwrap();

    // Query: Find supplier(s) with maximum total revenue
    // Supplier 1: 100 + 200 = 300
    // Supplier 2: 150
    // MAX = 300, so only Supplier 1 should be returned
    let query = "
        WITH revenue AS (
            SELECT l_suppkey as supplier_no,
                   SUM(l_amount) as total_revenue
            FROM lineitem
            GROUP BY l_suppkey
        )
        SELECT s_suppkey, s_name, total_revenue
        FROM supplier, revenue
        WHERE s_suppkey = supplier_no
          AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
    ";

    let result = execute_sql(&mut db, query).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one supplier with max revenue");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar("Supplier A".to_string()));
}

/// Test tie-handling: multiple suppliers with the same maximum revenue
#[test]
fn test_cte_in_scalar_subquery_ties() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE supplier(s_suppkey INT, s_name VARCHAR(25))").unwrap();
    execute_sql(&mut db, "CREATE TABLE lineitem(l_suppkey INT, l_amount DECIMAL(10,2))").unwrap();

    // Insert data where two suppliers have the same max revenue
    execute_sql(&mut db, "INSERT INTO supplier VALUES (1, 'Supplier A')").unwrap();
    execute_sql(&mut db, "INSERT INTO supplier VALUES (2, 'Supplier B')").unwrap();
    execute_sql(&mut db, "INSERT INTO supplier VALUES (3, 'Supplier C')").unwrap();

    // Supplier 1: 150 + 130 = 280
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (1, 150.00)").unwrap();
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (1, 130.00)").unwrap();
    // Supplier 2: 280 (tied for max)
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (2, 280.00)").unwrap();
    // Supplier 3: 200 (below max)
    execute_sql(&mut db, "INSERT INTO lineitem VALUES (3, 200.00)").unwrap();

    let query = "
        WITH revenue AS (
            SELECT l_suppkey as supplier_no,
                   SUM(l_amount) as total_revenue
            FROM lineitem
            GROUP BY l_suppkey
        )
        SELECT s_suppkey, s_name, total_revenue
        FROM supplier, revenue
        WHERE s_suppkey = supplier_no
          AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
        ORDER BY s_suppkey
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Both suppliers 1 and 2 have total_revenue = 280
    assert_eq!(result.len(), 2, "Should return both suppliers tied for max revenue");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
}

/// Test CTE referenced multiple times in scalar subqueries
#[test]
fn test_cte_multiple_scalar_subquery_refs() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE items(id INT, value INT)").unwrap();
    execute_sql(&mut db, "INSERT INTO items VALUES (1, 10)").unwrap();
    execute_sql(&mut db, "INSERT INTO items VALUES (2, 20)").unwrap();
    execute_sql(&mut db, "INSERT INTO items VALUES (3, 30)").unwrap();
    execute_sql(&mut db, "INSERT INTO items VALUES (4, 40)").unwrap();

    // CTE referenced twice in WHERE clause with different aggregates
    let query = "
        WITH stats AS (
            SELECT value FROM items
        )
        SELECT id, value
        FROM items
        WHERE value >= (SELECT AVG(value) FROM stats)
          AND value <= (SELECT MAX(value) FROM stats)
        ORDER BY id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // AVG = 25, MAX = 40
    // Values >= 25 AND <= 40: 30, 40
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(4));
}

/// Test CTE with MIN aggregate in scalar subquery
#[test]
fn test_cte_min_in_scalar_subquery() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE prices(product_id INT, price INT)").unwrap();
    execute_sql(&mut db, "INSERT INTO prices VALUES (1, 100)").unwrap();
    execute_sql(&mut db, "INSERT INTO prices VALUES (2, 50)").unwrap();
    execute_sql(&mut db, "INSERT INTO prices VALUES (3, 75)").unwrap();

    // Find product(s) with minimum price
    let query = "
        WITH price_list AS (
            SELECT product_id, price FROM prices
        )
        SELECT product_id, price
        FROM price_list
        WHERE price = (SELECT MIN(price) FROM price_list)
    ";

    let result = execute_sql(&mut db, query).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(50));
}

/// Test CTE in SELECT list scalar subquery
#[test]
fn test_cte_in_select_list_scalar_subquery() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE sales(region TEXT, amount INT)").unwrap();
    execute_sql(&mut db, "INSERT INTO sales VALUES ('North', 100)").unwrap();
    execute_sql(&mut db, "INSERT INTO sales VALUES ('South', 200)").unwrap();
    execute_sql(&mut db, "INSERT INTO sales VALUES ('East', 150)").unwrap();

    // Each row shows its amount compared to the total
    let query = "
        WITH totals AS (
            SELECT SUM(amount) as total FROM sales
        )
        SELECT region, amount, (SELECT total FROM totals) as grand_total
        FROM sales
        ORDER BY region
    ";

    let result = execute_sql(&mut db, query).unwrap();

    assert_eq!(result.len(), 3);
    // Grand total = 100 + 200 + 150 = 450
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(450));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(450));
    assert_eq!(result[2].values[2], vibesql_types::SqlValue::Integer(450));
}

/// Test CTE referenced in IN subquery (issue #3044)
/// This tests that CTE context is properly propagated to IN subquery evaluation.
/// Note: Uses correlated subquery to avoid thread-local cache interference.
#[test]
fn test_cte_in_in_subquery() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE orders_cte_in(order_id INT, customer_id INT, amount INT)")
        .unwrap();
    execute_sql(&mut db, "INSERT INTO orders_cte_in VALUES (1, 1, 100)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders_cte_in VALUES (2, 2, 200)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders_cte_in VALUES (3, 1, 150)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders_cte_in VALUES (4, 3, 50)").unwrap();

    // Find orders from customers who have high-value orders (>= 150)
    // CTE defines the high-value customers, correlated IN subquery references the CTE
    // Making it correlated avoids the thread-local cache issue
    let query = "
        WITH high_value AS (
            SELECT customer_id, amount FROM orders_cte_in WHERE amount >= 150
        )
        SELECT o.order_id, o.customer_id, o.amount
        FROM orders_cte_in o
        WHERE o.customer_id IN (SELECT h.customer_id FROM high_value h WHERE h.customer_id = o.customer_id)
        ORDER BY o.order_id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Customers with orders >= 150: customer 1 (150), customer 2 (200)
    // All orders from these customers: 1, 2, 3 (order 4 is customer 3 with only 50)
    assert_eq!(result.len(), 3, "Expected 3 orders from customers with high-value orders");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
}

/// Test CTE referenced in EXISTS subquery (issue #3044)
/// This tests that CTE context is properly propagated to EXISTS evaluation.
#[test]
fn test_cte_in_exists_subquery() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE products(product_id INT, name TEXT)").unwrap();
    execute_sql(&mut db, "CREATE TABLE order_items(order_id INT, product_id INT)").unwrap();

    execute_sql(&mut db, "INSERT INTO products VALUES (1, 'Widget')").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES (2, 'Gadget')").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES (3, 'Gizmo')").unwrap();

    execute_sql(&mut db, "INSERT INTO order_items VALUES (100, 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO order_items VALUES (101, 2)").unwrap();

    // Find products that have been ordered, using CTE for order items
    let query = "
        WITH ordered_products AS (
            SELECT DISTINCT product_id FROM order_items
        )
        SELECT product_id, name
        FROM products p
        WHERE EXISTS (SELECT 1 FROM ordered_products op WHERE op.product_id = p.product_id)
        ORDER BY product_id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Products 1 and 2 have been ordered
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
}

/// Test CTE referenced in NOT EXISTS subquery (issue #3044)
#[test]
fn test_cte_in_not_exists_subquery() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE products(product_id INT, name TEXT)").unwrap();
    execute_sql(&mut db, "CREATE TABLE order_items(order_id INT, product_id INT)").unwrap();

    execute_sql(&mut db, "INSERT INTO products VALUES (1, 'Widget')").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES (2, 'Gadget')").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES (3, 'Gizmo')").unwrap();

    execute_sql(&mut db, "INSERT INTO order_items VALUES (100, 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO order_items VALUES (101, 2)").unwrap();

    // Find products that have NOT been ordered, using CTE for order items
    let query = "
        WITH ordered_products AS (
            SELECT DISTINCT product_id FROM order_items
        )
        SELECT product_id, name
        FROM products p
        WHERE NOT EXISTS (SELECT 1 FROM ordered_products op WHERE op.product_id = p.product_id)
        ORDER BY product_id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Only product 3 (Gizmo) has not been ordered
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar("Gizmo".to_string()));
}
