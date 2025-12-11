//! Tests for GROUP BY with always-false WHERE clause
//!
//! SQL standard behavior:
//! - GROUP BY with empty input (after WHERE filtering) should return 0 rows
//! - Aggregates without GROUP BY on empty input should return 1 row (COUNT=0, others=NULL)
//!
//! Issue: WHERE NULL IS NOT NULL with GROUP BY was incorrectly returning 1 row

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup_db() -> Database {
    let mut db = Database::new();

    // Create test table
    let create = Parser::parse_sql("CREATE TABLE tab1 (col0 INTEGER, col1 INTEGER)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Insert test data
    let inserts = [
        "INSERT INTO tab1 VALUES (1, 10)",
        "INSERT INTO tab1 VALUES (2, 20)",
        "INSERT INTO tab1 VALUES (3, 30)",
    ];

    for sql in inserts {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
        }
    }

    db
}

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_group_by_with_null_is_not_null_returns_empty() {
    let db = setup_db();

    // SQL: WHERE NULL IS NOT NULL is always false
    // With GROUP BY, this should return 0 rows (no groups when input is empty)
    let rows = execute_query(
        &db,
        "SELECT AVG(col0) AS col2 FROM tab1 WHERE (NULL) IS NOT NULL GROUP BY col0",
    );

    assert_eq!(
        rows.len(),
        0,
        "GROUP BY with always-false WHERE should return 0 rows, got {} rows with {:?}",
        rows.len(),
        rows.first().map(|r| &r.values)
    );
}

#[test]
fn test_aggregate_without_group_by_with_null_is_not_null_returns_one_row() {
    let db = setup_db();

    // SQL: Aggregate WITHOUT GROUP BY on empty input returns 1 row
    // COUNT(*) returns 0, other aggregates return NULL
    let rows = execute_query(&db, "SELECT AVG(col0), COUNT(*) FROM tab1 WHERE (NULL) IS NOT NULL");

    assert_eq!(
        rows.len(),
        1,
        "Aggregate without GROUP BY should return 1 row, got {} rows",
        rows.len()
    );

    // AVG should be NULL, COUNT should be 0
    assert_eq!(rows[0].values[0], SqlValue::Null, "AVG on empty input should be NULL");
    assert_eq!(rows[0].values[1], SqlValue::Integer(0), "COUNT(*) on empty input should be 0");
}

#[test]
fn test_group_by_with_col_greater_than_100_returns_empty() {
    let db = setup_db();

    // All values in col0 are 1, 2, 3 - none > 100
    // With GROUP BY, this should return 0 rows
    let rows = execute_query(&db, "SELECT AVG(col0) FROM tab1 WHERE col0 > 100 GROUP BY col0");

    assert_eq!(
        rows.len(),
        0,
        "GROUP BY with filtering all rows should return 0 rows, got {}",
        rows.len()
    );
}

#[test]
fn test_aggregate_without_group_by_with_col_greater_than_100_returns_one_row() {
    let db = setup_db();

    // All values in col0 are 1, 2, 3 - none > 100
    // Without GROUP BY, aggregate on empty should return 1 row
    let rows = execute_query(&db, "SELECT AVG(col0), COUNT(*) FROM tab1 WHERE col0 > 100");

    assert_eq!(
        rows.len(),
        1,
        "Aggregate without GROUP BY should return 1 row, got {} rows",
        rows.len()
    );

    assert_eq!(rows[0].values[0], SqlValue::Null, "AVG on empty input should be NULL");
    assert_eq!(rows[0].values[1], SqlValue::Integer(0), "COUNT(*) on empty input should be 0");
}

#[test]
fn test_group_by_on_empty_table_returns_empty() {
    let mut db = Database::new();

    // Create empty table
    let create = Parser::parse_sql("CREATE TABLE empty_tab (col0 INTEGER)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // GROUP BY on empty table should return 0 rows
    let rows = execute_query(&db, "SELECT AVG(col0) FROM empty_tab GROUP BY col0");

    assert_eq!(rows.len(), 0, "GROUP BY on empty table should return 0 rows, got {}", rows.len());
}

#[test]
fn test_aggregate_without_group_by_on_empty_table_returns_one_row() {
    let mut db = Database::new();

    // Create empty table
    let create = Parser::parse_sql("CREATE TABLE empty_tab (col0 INTEGER)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Aggregate without GROUP BY on empty table should return 1 row
    let rows = execute_query(&db, "SELECT AVG(col0), COUNT(*) FROM empty_tab");

    assert_eq!(
        rows.len(),
        1,
        "Aggregate without GROUP BY on empty table should return 1 row, got {}",
        rows.len()
    );

    assert_eq!(rows[0].values[0], SqlValue::Null, "AVG on empty table should be NULL");
    assert_eq!(rows[0].values[1], SqlValue::Integer(0), "COUNT(*) on empty table should be 0");
}

#[test]
fn test_count_with_group_by_always_false_where_returns_empty() {
    let db = setup_db();

    // COUNT with GROUP BY and always-false WHERE should return 0 rows
    let rows =
        execute_query(&db, "SELECT COUNT(*) FROM tab1 WHERE (NULL) IS NOT NULL GROUP BY col0");

    assert_eq!(
        rows.len(),
        0,
        "COUNT(*) with GROUP BY and always-false WHERE should return 0 rows, got {} with {:?}",
        rows.len(),
        rows.first().map(|r| &r.values)
    );
}

#[test]
fn test_group_by_with_false_literal_returns_empty() {
    let db = setup_db();

    // WHERE FALSE should filter all rows
    // With GROUP BY, this should return 0 rows
    let rows = execute_query(&db, "SELECT AVG(col0) FROM tab1 WHERE FALSE GROUP BY col0");

    assert_eq!(rows.len(), 0, "GROUP BY with WHERE FALSE should return 0 rows, got {}", rows.len());
}
