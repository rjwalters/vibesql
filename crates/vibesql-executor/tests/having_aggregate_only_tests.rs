//! Tests for HAVING clause with aggregates only in HAVING (Issue #4198)
//!
//! This tests the case where aggregate functions appear ONLY in the HAVING clause,
//! not in the SELECT list. This is valid SQL that should work.
//!
//! Example:
//! ```sql
//! SELECT dept_id
//! FROM sales
//! GROUP BY dept_id
//! HAVING AVG(amount) > 100  -- AVG only in HAVING, not SELECT
//! ```

use vibesql_executor::SelectExecutor;

/// Helper to setup test database
fn setup_test_database() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    // Sales table with dept_id and amount
    let schema = vibesql_catalog::TableSchema::new(
        "SALES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "DEPT_ID".to_string(),
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
    db.create_table(schema).unwrap();

    // Insert test data:
    // Dept 1: amounts 150, 200 -> avg 175, sum 350, count 2, min 150, max 200
    // Dept 2: amounts 50, 70 -> avg 60, sum 120, count 2, min 50, max 70
    // Dept 3: amounts 120 -> avg 120, sum 120, count 1, min 120, max 120
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(70),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(120),
        ]),
    )
    .unwrap();

    db
}

/// Test: Aggregate in HAVING that is NOT in SELECT list (Issue #4198)
///
/// This is the main test case from the issue:
/// SELECT dept_id FROM sales GROUP BY dept_id HAVING AVG(amount) > 100
#[test]
fn test_having_aggregate_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // AVG(amount) appears ONLY in HAVING, not in SELECT
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING AVG(amount) > 100";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 avg=175 (>100), Dept 2 avg=60 (<100), Dept 3 avg=120 (>100)
    // Should return dept 1 and dept 3
    assert_eq!(rows.len(), 2, "Should return 2 rows (dept 1 and 3 have avg > 100)");

    // Check the results contain dept 1 and 3 (order may vary)
    let dept_ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        })
        .collect();

    assert!(dept_ids.contains(&1), "Should contain dept 1");
    assert!(dept_ids.contains(&3), "Should contain dept 3");
    assert!(!dept_ids.contains(&2), "Should not contain dept 2");
}

/// Test: Aggregate in HAVING that IS also in SELECT list (baseline - should already work)
#[test]
fn test_having_aggregate_also_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // AVG(amount) appears in both SELECT and HAVING
    let sql =
        "SELECT dept_id, AVG(amount) as avg_amt FROM sales GROUP BY dept_id HAVING AVG(amount) > 100";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 avg=175 (>100), Dept 2 avg=60 (<100), Dept 3 avg=120 (>100)
    // Should return dept 1 and dept 3
    assert_eq!(rows.len(), 2, "Should return 2 rows (dept 1 and 3 have avg > 100)");
}

/// Test: Multiple different aggregates - one in SELECT, different one in HAVING
#[test]
fn test_having_different_aggregate_than_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // COUNT(*) in SELECT, SUM(amount) in HAVING
    let sql = "SELECT dept_id, COUNT(*) as cnt FROM sales GROUP BY dept_id HAVING SUM(amount) > 100";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 sum=350 (>100), Dept 2 sum=120 (>100), Dept 3 sum=120 (>100)
    // All should pass the HAVING filter
    assert_eq!(rows.len(), 3, "Should return 3 rows (all have sum > 100)");
}

/// Test: Complex HAVING expression with aggregate not in SELECT
#[test]
fn test_having_complex_expression_aggregate_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // AVG in HAVING arithmetic expression, not in SELECT
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING AVG(amount) * 2 > 200";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 avg*2=350 (>200), Dept 2 avg*2=120 (<200), Dept 3 avg*2=240 (>200)
    // Should return dept 1 and dept 3
    assert_eq!(rows.len(), 2, "Should return 2 rows (dept 1 and 3 have avg*2 > 200)");
}

/// Test: COUNT(*) in HAVING, no aggregates in SELECT
#[test]
fn test_having_count_star_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // COUNT(*) only in HAVING
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING COUNT(*) > 1";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 count=2 (>1), Dept 2 count=2 (>1), Dept 3 count=1 (not >1)
    // Should return dept 1 and dept 2
    assert_eq!(rows.len(), 2, "Should return 2 rows (dept 1 and 2 have count > 1)");

    let dept_ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer value"),
        })
        .collect();

    assert!(dept_ids.contains(&1), "Should contain dept 1");
    assert!(dept_ids.contains(&2), "Should contain dept 2");
    assert!(!dept_ids.contains(&3), "Should not contain dept 3");
}

/// Test: Multiple aggregates in HAVING, none in SELECT
#[test]
fn test_having_multiple_aggregates_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // Both AVG and COUNT in HAVING, neither in SELECT
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING AVG(amount) > 100 AND COUNT(*) > 1";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1: avg=175 (>100) AND count=2 (>1) -> PASS
    // Dept 2: avg=60 (<100) -> FAIL
    // Dept 3: avg=120 (>100) AND count=1 (not >1) -> FAIL
    // Should return only dept 1
    assert_eq!(rows.len(), 1, "Should return 1 row (only dept 1 passes both conditions)");

    let dept_id = match &rows[0].values[0] {
        vibesql_types::SqlValue::Integer(i) => *i,
        _ => panic!("Expected integer value"),
    };
    assert_eq!(dept_id, 1, "Should be dept 1");
}

/// Test: MIN/MAX in HAVING, not in SELECT
#[test]
fn test_having_min_max_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // MIN and MAX in HAVING, neither in SELECT
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING MAX(amount) - MIN(amount) > 20";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1: max=200, min=150, diff=50 (>20) -> PASS
    // Dept 2: max=70, min=50, diff=20 (not >20) -> FAIL
    // Dept 3: max=120, min=120, diff=0 (not >20) -> FAIL
    // Should return only dept 1
    assert_eq!(rows.len(), 1, "Should return 1 row (only dept 1 has diff > 20)");

    let dept_id = match &rows[0].values[0] {
        vibesql_types::SqlValue::Integer(i) => *i,
        _ => panic!("Expected integer value"),
    };
    assert_eq!(dept_id, 1, "Should be dept 1");
}

/// Test: SUM in HAVING, not in SELECT
#[test]
fn test_having_sum_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // SUM only in HAVING
    let sql = "SELECT dept_id FROM sales GROUP BY dept_id HAVING SUM(amount) > 200";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Dept 1 sum=350 (>200), Dept 2 sum=120 (<200), Dept 3 sum=120 (<200)
    // Should return only dept 1
    assert_eq!(rows.len(), 1, "Should return 1 row (only dept 1 has sum > 200)");

    let dept_id = match &rows[0].values[0] {
        vibesql_types::SqlValue::Integer(i) => *i,
        _ => panic!("Expected integer value"),
    };
    assert_eq!(dept_id, 1, "Should be dept 1");
}

/// Test: HAVING without GROUP BY with aggregate not in SELECT (edge case)
/// SQL allows HAVING without GROUP BY - treats entire table as one group
#[test]
fn test_having_without_group_by_aggregate_not_in_select() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // COUNT(*) only in HAVING, no GROUP BY
    // This treats the entire table as one group
    let sql = "SELECT 1 as marker FROM sales HAVING COUNT(*) > 3";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Total 5 rows > 3, so should return 1 row
    assert_eq!(rows.len(), 1, "Should return 1 row (count 5 > 3)");
}

/// Test: HAVING without GROUP BY, aggregate in SELECT and HAVING (baseline)
#[test]
fn test_having_without_group_by_aggregate_in_both() {
    let db = setup_test_database();
    let executor = SelectExecutor::new(&db);

    // COUNT(*) in both SELECT and HAVING, no GROUP BY
    let sql = "SELECT COUNT(*) as cnt FROM sales HAVING COUNT(*) > 3";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Total 5 rows > 3, so should return 1 row with count=5
    assert_eq!(rows.len(), 1, "Should return 1 row (count 5 > 3)");

    let count = match &rows[0].values[0] {
        vibesql_types::SqlValue::Integer(i) => *i,
        _ => panic!("Expected integer value"),
    };
    assert_eq!(count, 5, "Should have count = 5");
}

/// Test: Compare two different aggregates in HAVING (Issue #4198 example)
/// HAVING AVG(a) > AVG(b) * 2
#[test]
fn test_having_compare_two_aggregates() {
    let mut db = vibesql_storage::Database::new();

    // Create table with two numeric columns to compare
    let schema = vibesql_catalog::TableSchema::new(
        "DATA".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "GROUP_ID".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "VALUE_A".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "VALUE_B".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Group 1: avg_a=200, avg_b=50 -> avg_a > avg_b * 2 (200 > 100) TRUE
    db.insert_row("DATA", vibesql_storage::Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Integer(200),
        vibesql_types::SqlValue::Integer(50),
    ])).unwrap();

    // Group 2: avg_a=100, avg_b=100 -> avg_a > avg_b * 2 (100 > 200) FALSE
    db.insert_row("DATA", vibesql_storage::Row::new(vec![
        vibesql_types::SqlValue::Integer(2),
        vibesql_types::SqlValue::Integer(100),
        vibesql_types::SqlValue::Integer(100),
    ])).unwrap();

    let executor = SelectExecutor::new(&db);

    // Compare two aggregates in HAVING
    let sql = "SELECT group_id FROM data GROUP BY group_id HAVING AVG(value_a) > AVG(value_b) * 2";
    println!("Test SQL: {}", sql);

    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement");
    };

    let rows = executor.execute(&select_stmt).unwrap();

    // Only group 1 should pass
    assert_eq!(rows.len(), 1, "Should return 1 row (only group 1 passes)");

    let group_id = match &rows[0].values[0] {
        vibesql_types::SqlValue::Integer(i) => *i,
        _ => panic!("Expected integer value"),
    };
    assert_eq!(group_id, 1, "Should be group 1");
}
