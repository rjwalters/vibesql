//! Integration tests for streaming aggregation fast path (#3815)
//!
//! Tests that streaming aggregate queries (e.g., `SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?`)
//! return correct aggregate values.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor, Session};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute a SQL statement and setup tables
fn execute_sql(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).unwrap();
        }
        _ => panic!("Unexpected statement type"),
    }
}

/// Helper to create a simple test table with primary key and integer column
fn setup_test_table() -> Database {
    let mut db = Database::new();

    // Create table: sbtest (id INTEGER PRIMARY KEY, k INTEGER, c VARCHAR)
    execute_sql(
        &mut db,
        "CREATE TABLE sbtest (id INTEGER PRIMARY KEY, k INTEGER, c VARCHAR(100))",
    );

    // Insert test data: id 1-10 with k = id * 10
    // SUM(k) for id 1-10 = 10 + 20 + 30 + ... + 100 = 550
    for i in 1..=10 {
        execute_sql(
            &mut db,
            &format!("INSERT INTO sbtest VALUES ({}, {}, 'row{}')", i, i * 10, i),
        );
    }

    db
}

/// Helper to parse a SELECT statement and return the Box contents
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql).unwrap() {
        vibesql_ast::Statement::Select(stmt) => *stmt,
        _ => panic!("Expected SELECT statement"),
    }
}

// =============================================================================
// SUM Aggregate Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_sum_full_range() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // SUM(k) for id 1-10 = 10 + 20 + 30 + ... + 100 = 550
    let stmt = parse_select("SELECT SUM(k) FROM sbtest WHERE id BETWEEN 1 AND 10");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(550),
        "SUM(k) for id 1-10 should be 550"
    );
}

#[test]
fn test_streaming_aggregate_sum_partial_range() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // SUM(k) for id 3-7 = 30 + 40 + 50 + 60 + 70 = 250
    let stmt = parse_select("SELECT SUM(k) FROM sbtest WHERE id BETWEEN 3 AND 7");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(250),
        "SUM(k) for id 3-7 should be 250"
    );
}

#[test]
fn test_streaming_aggregate_sum_single_row() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // SUM(k) for id 5-5 = 50
    let stmt = parse_select("SELECT SUM(k) FROM sbtest WHERE id BETWEEN 5 AND 5");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(50),
        "SUM(k) for id 5-5 should be 50"
    );
}

// =============================================================================
// COUNT Aggregate Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_count_column() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // COUNT(k) for id 1-10 = 10
    let stmt = parse_select("SELECT COUNT(k) FROM sbtest WHERE id BETWEEN 1 AND 10");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(10),
        "COUNT(k) for id 1-10 should be 10"
    );
}

#[test]
fn test_streaming_aggregate_count_partial_range() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // COUNT(k) for id 3-7 = 5
    let stmt = parse_select("SELECT COUNT(k) FROM sbtest WHERE id BETWEEN 3 AND 7");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(5),
        "COUNT(k) for id 3-7 should be 5"
    );
}

// =============================================================================
// AVG Aggregate Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_avg() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // AVG(k) for id 1-10 = 550/10 = 55
    let stmt = parse_select("SELECT AVG(k) FROM sbtest WHERE id BETWEEN 1 AND 10");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    // AVG typically returns a double/numeric
    match &result[0].values[0] {
        SqlValue::Double(v) => assert!((v - 55.0).abs() < 0.001, "AVG(k) should be 55.0"),
        SqlValue::Numeric(v) => assert!((v - 55.0).abs() < 0.001, "AVG(k) should be 55.0"),
        SqlValue::Integer(v) => assert_eq!(*v, 55, "AVG(k) should be 55"),
        other => panic!("Unexpected AVG result type: {:?}", other),
    }
}

// =============================================================================
// MIN/MAX Aggregate Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_min() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // MIN(k) for id 3-7 = 30
    let stmt = parse_select("SELECT MIN(k) FROM sbtest WHERE id BETWEEN 3 AND 7");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(30),
        "MIN(k) for id 3-7 should be 30"
    );
}

#[test]
fn test_streaming_aggregate_max() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // MAX(k) for id 3-7 = 70
    let stmt = parse_select("SELECT MAX(k) FROM sbtest WHERE id BETWEEN 3 AND 7");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(70),
        "MAX(k) for id 3-7 should be 70"
    );
}

// =============================================================================
// Multiple Aggregates Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_multiple() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // Multiple aggregates in one query
    let stmt =
        parse_select("SELECT SUM(k), COUNT(k), MIN(k), MAX(k) FROM sbtest WHERE id BETWEEN 1 AND 5");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    // SUM(k) for id 1-5 = 10 + 20 + 30 + 40 + 50 = 150
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(150),
        "SUM(k) for id 1-5 should be 150"
    );
    // COUNT(k) = 5
    assert_eq!(
        result[0].values[1],
        SqlValue::Integer(5),
        "COUNT(k) for id 1-5 should be 5"
    );
    // MIN(k) = 10
    assert_eq!(
        result[0].values[2],
        SqlValue::Integer(10),
        "MIN(k) for id 1-5 should be 10"
    );
    // MAX(k) = 50
    assert_eq!(
        result[0].values[3],
        SqlValue::Integer(50),
        "MAX(k) for id 1-5 should be 50"
    );
}

// =============================================================================
// Empty Range Tests
// =============================================================================

#[test]
fn test_streaming_aggregate_empty_range() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // Range with no matching rows (id 100-200)
    let stmt = parse_select("SELECT SUM(k) FROM sbtest WHERE id BETWEEN 100 AND 200");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    // SUM of empty set is NULL
    assert_eq!(
        result[0].values[0],
        SqlValue::Null,
        "SUM of empty set should be NULL"
    );
}

#[test]
fn test_streaming_aggregate_count_empty_range() {
    let db = setup_test_table();
    let executor = SelectExecutor::new(&db);

    // COUNT of empty range
    let stmt = parse_select("SELECT COUNT(k) FROM sbtest WHERE id BETWEEN 100 AND 200");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return exactly one row");
    // COUNT of empty set is 0
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(0),
        "COUNT of empty set should be 0"
    );
}

// =============================================================================
// Prepared Statement Tests (ensures streaming aggregate works with prepared stmts)
// =============================================================================

#[test]
fn test_streaming_aggregate_prepared_statement() {
    let db = setup_test_table();
    let session = Session::new(&db);

    // Prepare a streaming aggregate query
    let stmt = session
        .prepare("SELECT SUM(k) FROM sbtest WHERE id BETWEEN ? AND ?")
        .unwrap();

    // Execute with different parameters
    let result1 = session
        .execute_prepared(&stmt, &[SqlValue::Integer(1), SqlValue::Integer(5)])
        .unwrap();
    let rows1 = result1.rows().unwrap();
    assert_eq!(rows1.len(), 1);
    assert_eq!(
        rows1[0].values[0],
        SqlValue::Integer(150),
        "SUM(k) for id 1-5 should be 150"
    );

    let result2 = session
        .execute_prepared(&stmt, &[SqlValue::Integer(6), SqlValue::Integer(10)])
        .unwrap();
    let rows2 = result2.rows().unwrap();
    assert_eq!(rows2.len(), 1);
    // SUM(k) for id 6-10 = 60 + 70 + 80 + 90 + 100 = 400
    assert_eq!(
        rows2[0].values[0],
        SqlValue::Integer(400),
        "SUM(k) for id 6-10 should be 400"
    );
}
