//! Tests for sqlite_search_count diagnostic variable (TCL test compatibility)
//!
//! The sqlite_search_count variable tracks the number of rows examined during
//! query execution. This is used by SQLite TCL tests to verify query optimization
//! behavior (e.g., verifying index usage by checking row counts).
//!
//! Note: VibeSQL's execute_with_columns scans the FROM clause twice (once for
//! schema building, once for actual execution), so counts may be higher than
//! expected compared to a single-pass implementation.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::SelectExecutor;

/// Helper to execute SQL and return rows
fn execute_sql(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::Select(select) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute_with_columns(&select).expect("Failed to execute SELECT");
        result.rows
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Helper to execute SQL for DML
fn execute_dml(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            crate::CreateTableExecutor::execute(&create, db).expect("Failed to create table");
        }
        vibesql_ast::Statement::Insert(insert) => {
            crate::InsertExecutor::execute(db, &insert).expect("Failed to insert");
        }
        vibesql_ast::Statement::CreateIndex(create_idx) => {
            crate::CreateIndexExecutor::execute(&create_idx, db).expect("Failed to create index");
        }
        _ => panic!("Unexpected statement type"),
    }
}

#[test]
fn test_search_count_basic() {
    let db = Database::new();

    // Initial search count should be 0
    assert_eq!(db.search_count(), 0);

    // Reset should keep it at 0
    db.reset_search_count();
    assert_eq!(db.search_count(), 0);

    // Increment should work
    db.increment_search_count(5);
    assert_eq!(db.search_count(), 5);

    // Another increment adds to existing
    db.increment_search_count(3);
    assert_eq!(db.search_count(), 8);

    // Reset clears the count
    db.reset_search_count();
    assert_eq!(db.search_count(), 0);
}

#[test]
fn test_search_count_function() {
    let db = Database::new();

    // Query sqlite_search_count() - should return 0 initially
    let rows = execute_sql(&db, "SELECT sqlite_search_count()");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Bigint(0));
}

#[test]
fn test_search_count_reset_function() {
    let db = Database::new();

    // Set some initial count
    db.increment_search_count(42);
    assert_eq!(db.search_count(), 42);

    // Reset via function
    let rows = execute_sql(&db, "SELECT sqlite_search_count_reset()");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Bigint(0));

    // Verify count is now 0
    assert_eq!(db.search_count(), 0);
}

#[test]
fn test_search_count_tracks_table_scan() {
    let mut db = Database::new();

    // Create a simple table
    execute_dml(&mut db, "CREATE TABLE t1 (x INT, y INT)");

    // Insert some rows
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1, 10)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2, 20)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (3, 30)");

    // Reset the counter before the query
    db.reset_search_count();

    // Execute a table scan query
    let rows = execute_sql(&db, "SELECT * FROM t1");
    assert_eq!(rows.len(), 3);

    // Rows were examined (at least 3 due to double-scan in execute_with_columns)
    // The count should be >= 3 (rows scanned at least once)
    let count = db.search_count();
    assert!(count >= 3, "Expected at least 3 rows examined, got {}", count);
    // And should be a multiple of 3 (due to double scanning)
    assert!(count % 3 == 0, "Expected count to be multiple of 3, got {}", count);
}

#[test]
fn test_search_count_with_where_clause() {
    let mut db = Database::new();

    // Create a table and insert rows
    execute_dml(&mut db, "CREATE TABLE t1 (x INT, y INT)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1, 10)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2, 20)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (3, 30)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (4, 40)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (5, 50)");

    // Reset the counter
    db.reset_search_count();

    // Query with WHERE clause - still scans all rows (no index)
    let rows = execute_sql(&db, "SELECT * FROM t1 WHERE x = 3");
    assert_eq!(rows.len(), 1); // Only 1 row matches

    // All 5 rows were examined during the scan (at least once, possibly twice)
    let count = db.search_count();
    assert!(count >= 5, "Expected at least 5 rows examined, got {}", count);
    assert!(count % 5 == 0, "Expected count to be multiple of 5, got {}", count);
}

#[test]
fn test_search_count_with_index() {
    let mut db = Database::new();

    // Create table with a secondary index (not primary key, to avoid point lookup)
    execute_dml(&mut db, "CREATE TABLE t1 (id INT PRIMARY KEY, x INT, y INT)");
    execute_dml(&mut db, "CREATE INDEX idx_x ON t1 (x)");

    // Insert rows
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1, 10, 100)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2, 20, 200)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (3, 30, 300)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (4, 40, 400)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (5, 50, 500)");

    // Reset counter
    db.reset_search_count();

    // Query using indexed column - should use index scan
    let rows = execute_sql(&db, "SELECT * FROM t1 WHERE x = 30");
    assert_eq!(rows.len(), 1);

    // With index, fewer rows should be examined compared to full scan
    // Due to double-scan in execute_with_columns, expect count >= 1
    let count = db.search_count();
    // Index scan returns only matching rows (1 in this case)
    assert!(count >= 1, "Expected at least 1 row examined via index, got {}", count);
    // Should be much less than 5 * 2 = 10 (full scan would examine all rows)
    assert!(count <= 4, "Expected fewer rows with index scan, got {}", count);
}

#[test]
fn test_search_count_reset_between_queries() {
    let mut db = Database::new();

    // Create table
    execute_dml(&mut db, "CREATE TABLE t1 (x INT)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2)");

    // First query
    db.reset_search_count();
    execute_sql(&db, "SELECT * FROM t1");
    let first_count = db.search_count();
    assert!(first_count >= 2, "Expected at least 2 rows examined, got {}", first_count);

    // Reset and second query
    db.reset_search_count();
    execute_sql(&db, "SELECT * FROM t1 WHERE x = 1");
    let second_count = db.search_count();
    // Still scans all rows (no index), count should be similar to first query
    assert!(second_count >= 2, "Expected at least 2 rows examined, got {}", second_count);
}

#[test]
fn test_search_count_via_sql_reset() {
    let mut db = Database::new();

    // Create table
    execute_dml(&mut db, "CREATE TABLE t1 (x INT)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (3)");

    // Use SQL function to reset (simulating TCL test pattern)
    execute_sql(&db, "SELECT sqlite_search_count_reset()");

    // Execute query
    execute_sql(&db, "SELECT * FROM t1");

    // Check count via SQL function
    let rows = execute_sql(&db, "SELECT sqlite_search_count()");
    // Expect at least 3 rows examined (due to double-scan, likely 6)
    if let SqlValue::Bigint(count) = &rows[0].values[0] {
        assert!(*count >= 3, "Expected at least 3 rows examined, got {}", count);
        assert!(*count % 3 == 0, "Expected count to be multiple of 3, got {}", count);
    } else {
        panic!("Expected Bigint value");
    }
}

#[test]
fn test_search_count_multiple_tables() {
    let mut db = Database::new();

    // Create two tables
    execute_dml(&mut db, "CREATE TABLE t1 (x INT)");
    execute_dml(&mut db, "CREATE TABLE t2 (y INT)");

    // Insert rows
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2)");
    execute_dml(&mut db, "INSERT INTO t2 VALUES (10)");
    execute_dml(&mut db, "INSERT INTO t2 VALUES (20)");
    execute_dml(&mut db, "INSERT INTO t2 VALUES (30)");

    // Reset and execute a query with join
    db.reset_search_count();
    let rows = execute_sql(&db, "SELECT * FROM t1, t2");
    assert_eq!(rows.len(), 6); // 2 * 3 = 6 rows (cross join)

    // Both tables should be scanned: 2 rows from t1 + 3 rows from t2
    // Note: The exact count depends on the join implementation
    // For cross join, both tables are scanned
    let count = db.search_count();
    assert!(count >= 5, "Expected at least 5 rows examined, got {}", count);
}

#[test]
fn test_search_count_empty_table() {
    let mut db = Database::new();

    // Create empty table
    execute_dml(&mut db, "CREATE TABLE t1 (x INT)");

    // Reset and query empty table
    db.reset_search_count();
    let rows = execute_sql(&db, "SELECT * FROM t1");
    assert_eq!(rows.len(), 0);

    // No rows to examine
    assert_eq!(db.search_count(), 0);
}

#[test]
fn test_search_count_secondary_index() {
    let mut db = Database::new();

    // Create table
    execute_dml(&mut db, "CREATE TABLE t1 (id INT PRIMARY KEY, val INT)");

    // Create secondary index on val
    execute_dml(&mut db, "CREATE INDEX idx_val ON t1 (val)");

    // Insert rows
    execute_dml(&mut db, "INSERT INTO t1 VALUES (1, 100)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (2, 200)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (3, 100)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (4, 300)");
    execute_dml(&mut db, "INSERT INTO t1 VALUES (5, 100)");

    // Reset and query using indexed column
    db.reset_search_count();
    let rows = execute_sql(&db, "SELECT * FROM t1 WHERE val = 100");
    assert_eq!(rows.len(), 3); // 3 rows match

    // With index, only the matching rows should be examined
    let count = db.search_count();
    assert_eq!(count, 3, "Expected 3 rows examined via index, got {}", count);
}
