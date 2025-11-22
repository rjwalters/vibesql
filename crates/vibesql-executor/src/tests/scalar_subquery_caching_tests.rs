//! Tests for scalar subquery caching optimization (issue #2425)
//!
//! This module tests the performance optimization that caches uncorrelated
//! scalar subqueries to avoid redundant execution, especially in HAVING clauses.

use super::super::*;
use vibesql_parser::Parser;
use vibesql_ast::Statement;

fn execute_sql(db: &mut vibesql_storage::Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
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
        _ => Err(ExecutorError::UnsupportedFeature(format!("Unsupported statement type: {:?}", stmt))),
    }
}

#[test]
fn test_uncorrelated_scalar_in_having_correctness() {
    // Test that uncorrelated scalar subqueries in HAVING produce correct results
    // This is the TPC-H Q11 pattern
    let mut db = vibesql_storage::Database::new();

    // Create test tables
    execute_sql(&mut db, "CREATE TABLE products (category TEXT, value INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES ('A', 100)").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES ('A', 200)").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES ('B', 50)").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES ('B', 150)").unwrap();
    execute_sql(&mut db, "INSERT INTO products VALUES ('C', 300)").unwrap();

    // Query with uncorrelated scalar subquery in HAVING
    // Only groups with total value > (global average) should be returned
    let query = "
        SELECT category, SUM(value) as total_value
        FROM products
        GROUP BY category
        HAVING SUM(value) > (SELECT AVG(value) FROM products)
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Global average is (100+200+50+150+300)/5 = 160
    // Category A: 300 > 160 ✓
    // Category B: 200 > 160 ✓
    // Category C: 300 > 160 ✓
    assert_eq!(result.len(), 3, "All three categories should pass threshold of 160");
}

#[test]
fn test_same_uncorrelated_scalar_multiple_times() {
    // Test that the same uncorrelated scalar subquery works correctly when used multiple times
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE numbers (value INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO numbers VALUES (1)").unwrap();
    execute_sql(&mut db, "INSERT INTO numbers VALUES (2)").unwrap();
    execute_sql(&mut db, "INSERT INTO numbers VALUES (3)").unwrap();
    execute_sql(&mut db, "INSERT INTO numbers VALUES (4)").unwrap();
    execute_sql(&mut db, "INSERT INTO numbers VALUES (5)").unwrap();

    // Use the same scalar subquery multiple times in different contexts
    // This tests that the cache is working correctly by using the same subquery twice
    let query = "
        SELECT value
        FROM numbers
        WHERE value > (SELECT AVG(value) FROM numbers)
           OR value = (SELECT AVG(value) FROM numbers)
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Average is 3
    // value > 3: 4, 5
    // value = 3: 3
    // Total: 3, 4, 5
    assert_eq!(result.len(), 3);
}
