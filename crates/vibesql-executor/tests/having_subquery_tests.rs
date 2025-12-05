//! Tests for HAVING clause with subqueries (Issue #366)
//!
//! SQL:1999 Section 7.8: HAVING clause
//! Tests various subquery types in HAVING: scalar, IN, EXISTS, quantified comparisons

use vibesql_executor::SelectExecutor;

/// Helper to setup test database with sales and targets tables
fn setup_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    // Sales table
    let sales_schema = vibesql_catalog::TableSchema::new(
        "SALES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "ID".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "DEPT".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "AMOUNT".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(sales_schema).unwrap();

    // Insert sales data
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1), // dept 1
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1), // dept 1
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2), // dept 2
            vibesql_types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(2), // dept 2
            vibesql_types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(5),
            vibesql_types::SqlValue::Integer(3), // dept 3
            vibesql_types::SqlValue::Integer(500),
        ]),
    )
    .unwrap();

    // Targets table for comparison
    let targets_schema = vibesql_catalog::TableSchema::new(
        "TARGETS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "ID".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "TARGET_AMOUNT".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(targets_schema).unwrap();

    // Insert target data
    db.insert_row(
        "TARGETS",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_having_with_scalar_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where total sales > average of all target amounts
    // Expected: dept 1 (300 > 200), dept 3 (500 > 200)
    // Not: dept 2 (125 < 200)
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) > (SELECT AVG(target_amount) FROM targets) \
         ORDER BY dept",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Should return dept 1 (300) and dept 3 (500)
    assert_eq!(rows.len(), 2);

    // Check dept 1
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(1)));
    assert_eq!(rows[0].get(1), Some(&vibesql_types::SqlValue::Integer(300)));

    // Check dept 3
    assert_eq!(rows[1].get(0), Some(&vibesql_types::SqlValue::Integer(3)));
    assert_eq!(rows[1].get(1), Some(&vibesql_types::SqlValue::Integer(500)))
}

#[test]
fn test_having_with_scalar_subquery_comparison() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where count of sales < maximum target amount
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, COUNT(*) as cnt \
         FROM sales \
         GROUP BY dept \
         HAVING COUNT(*) < (SELECT MAX(target_amount) FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // All departments have count < 200: dept 1 (2), dept 2 (2), dept 3 (1)
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_having_with_exists_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where there exists a target
    // (This will match all departments since targets table has at least one row)
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING EXISTS (SELECT 1 FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // All 3 departments should be returned
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_having_with_not_exists_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where there does NOT exist a target > 1000
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING NOT EXISTS (SELECT 1 FROM targets WHERE target_amount > 1000)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // All departments should be returned (no targets > 1000)
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_having_with_in_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where total sales is in the targets table
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) IN (SELECT target_amount FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // No department has total = 200 (the only target value)
    // dept 1: 300, dept 2: 125, dept 3: 500
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_having_with_not_in_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where total sales is NOT in the targets table
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) NOT IN (SELECT target_amount FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // All departments have totals NOT in targets
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_having_with_complex_boolean_expression() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Complex HAVING with subquery and boolean operators
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) > (SELECT AVG(target_amount) FROM targets) \
            AND COUNT(*) >= 1",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // dept 1 (300) and dept 3 (500) both > 200 and have count >= 1
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_having_without_group_by_with_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // HAVING without GROUP BY treats entire table as one group
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT COUNT(*) as cnt \
         FROM sales \
         HAVING COUNT(*) > (SELECT COUNT(*) FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // sales has 5 rows, targets has 1 row, so 5 > 1 is true
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(5)));
}

#[test]
fn test_having_with_quantified_all() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where total sales > ALL target amounts
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) > ALL (SELECT target_amount FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // dept 1 (300 > 200), dept 3 (500 > 200) should match
    // dept 2 (125 < 200) should not match
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_having_with_quantified_any() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Find departments where total sales < ANY target amounts
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) < ANY (SELECT target_amount FROM targets)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Only dept 2 (125) is < 200
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&vibesql_types::SqlValue::Integer(2)));
}

#[test]
fn test_having_subquery_returns_empty() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Subquery returns no rows - scalar subquery should return NULL
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) > (SELECT target_amount FROM targets WHERE target_amount > 1000)",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Comparison with NULL returns NULL, HAVING filters out non-TRUE results
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_having_with_multiple_aggregates_and_subquery() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Multiple aggregates in HAVING with subquery
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT dept, SUM(amount) as total, AVG(amount) as avg_amt \
         FROM sales \
         GROUP BY dept \
         HAVING SUM(amount) > (SELECT AVG(target_amount) FROM targets) \
            AND AVG(amount) > 100",
    )
    .unwrap();

    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // dept 1: sum=300 (>200), avg=150 (>100) ✓
    // dept 2: sum=125 (<200) ✗
    // dept 3: sum=500 (>200), avg=500 (>100) ✓
    assert_eq!(rows.len(), 2);
}
