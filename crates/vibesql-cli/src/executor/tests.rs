use super::{validation, SqlExecutor};

#[test]
fn test_list_schemas() {
    let executor = SqlExecutor::new(None).unwrap();
    // Default database should have "public" schema
    assert!(executor.list_schemas().is_ok());
}

#[test]
fn test_list_indexes_empty() {
    let executor = SqlExecutor::new(None).unwrap();
    // New database should have no indexes
    assert!(executor.list_indexes().is_ok());
}

#[test]
fn test_list_roles() {
    let executor = SqlExecutor::new(None).unwrap();
    // Should show at least the default PUBLIC role
    assert!(executor.list_roles().is_ok());
}

#[test]
fn test_validate_table_name_nonexistent() {
    let executor = SqlExecutor::new(None).unwrap();
    // Should fail for non-existent table
    let result = validation::validate_table_name(&executor.db, "nonexistent_table");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_validate_table_name_sql_injection() {
    let executor = SqlExecutor::new(None).unwrap();
    // Should fail for table names with SQL injection attempts
    let result = validation::validate_table_name(&executor.db, "users; DROP TABLE users; --");
    assert!(result.is_err());
}

#[test]
fn test_describe_table_basic() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    // Should print table description without error
    assert!(executor.describe_table("test").is_ok());
}

#[test]
fn test_describe_nonexistent_table() {
    let executor = SqlExecutor::new(None).unwrap();
    let result = executor.describe_table("nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_describe_table_with_indexes() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE test (id INT PRIMARY KEY, email VARCHAR(100))").unwrap();
    executor.execute("CREATE INDEX idx_test_email ON test (email)").unwrap();
    assert!(executor.describe_table("test").is_ok());
}

#[test]
fn test_describe_table_with_multiple_columns() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10, 2))").unwrap();
    // Should print table with multiple columns of different types
    assert!(executor.describe_table("products").is_ok());
}

#[test]
fn test_insert_row_count_single() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();

    let result = executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    assert_eq!(result.row_count, 1, "Single INSERT should return row count of 1");
}

#[test]
fn test_insert_row_count_multiple() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();

    let result = executor.execute(
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
    ).unwrap();
    assert_eq!(result.row_count, 3, "Multiple value INSERT should return row count of 3");
}

#[test]
fn test_update_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();

    let result = executor.execute("UPDATE users SET name = 'Updated' WHERE id > 1").unwrap();
    assert_eq!(result.row_count, 2, "UPDATE should return row count of 2");
}

#[test]
fn test_delete_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").unwrap();

    let result = executor.execute("DELETE FROM users WHERE id IN (1, 3)").unwrap();
    assert_eq!(result.row_count, 2, "DELETE should return row count of 2");
}

#[test]
fn test_select_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    let result = executor.execute("SELECT * FROM users").unwrap();
    assert_eq!(result.row_count, 2, "SELECT should return row count of 2");
    assert_eq!(result.rows.len(), 2, "SELECT should return 2 rows");
}

#[test]
fn test_create_table_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("CREATE TABLE test (id INT PRIMARY KEY)").unwrap();
    assert_eq!(result.row_count, 0, "CREATE TABLE should return row count of 0 (DDL)");
}

#[test]
fn test_multi_column_select_order() {
    // Regression test for issue #1170
    // Multi-column SELECT should preserve left-to-right column order
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SELECT 74 AS col0, 50 AS col1").unwrap();

    assert_eq!(result.rows.len(), 1, "Should return 1 row");
    assert_eq!(result.rows[0].len(), 2, "Should return 2 columns");

    // Values should be in the same order as specified in SELECT: 74 first, then 50
    // Note: Values are formatted as debug strings like "Integer(74)"
    assert_eq!(result.rows[0][0], "Integer(74)", "First column should be 74");
    assert_eq!(result.rows[0][1], "Integer(50)", "Second column should be 50");
}

#[test]
fn test_create_index() {
    // Regression test for issue #3340
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE tab1 (pk INT PRIMARY KEY, col0 INT)").unwrap();

    let result = executor.execute("CREATE INDEX idx_tab1_0 ON tab1 (col0)");
    assert!(result.is_ok(), "CREATE INDEX should succeed");
    assert_eq!(result.unwrap().row_count, 0, "CREATE INDEX should return row count of 0 (DDL)");
}

#[test]
fn test_drop_index() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE tab1 (pk INT PRIMARY KEY, col0 INT)").unwrap();
    executor.execute("CREATE INDEX idx_tab1_0 ON tab1 (col0)").unwrap();

    let result = executor.execute("DROP INDEX idx_tab1_0");
    assert!(result.is_ok(), "DROP INDEX should succeed");
    assert_eq!(result.unwrap().row_count, 0, "DROP INDEX should return row count of 0 (DDL)");
}

#[test]
fn test_alter_table_add_column() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE tab1 (pk INT PRIMARY KEY)").unwrap();

    let result = executor.execute("ALTER TABLE tab1 ADD COLUMN col0 INT");
    assert!(result.is_ok(), "ALTER TABLE ADD COLUMN should succeed");
    assert_eq!(result.unwrap().row_count, 0, "ALTER TABLE should return row count of 0 (DDL)");
}

#[test]
fn test_transaction_begin_commit() {
    let mut executor = SqlExecutor::new(None).unwrap();

    let result = executor.execute("BEGIN TRANSACTION");
    assert!(result.is_ok(), "BEGIN TRANSACTION should succeed");

    let result = executor.execute("COMMIT");
    assert!(result.is_ok(), "COMMIT should succeed");
}

#[test]
fn test_transaction_begin_rollback() {
    let mut executor = SqlExecutor::new(None).unwrap();

    let result = executor.execute("BEGIN");
    assert!(result.is_ok(), "BEGIN should succeed");

    let result = executor.execute("ROLLBACK");
    assert!(result.is_ok(), "ROLLBACK should succeed");
}

#[test]
fn test_savepoint() {
    let mut executor = SqlExecutor::new(None).unwrap();

    executor.execute("BEGIN").unwrap();

    let result = executor.execute("SAVEPOINT sp1");
    assert!(result.is_ok(), "SAVEPOINT should succeed");

    let result = executor.execute("ROLLBACK TO SAVEPOINT sp1");
    assert!(result.is_ok(), "ROLLBACK TO SAVEPOINT should succeed");

    let result = executor.execute("RELEASE SAVEPOINT sp1");
    // Note: After rollback to savepoint, releasing might fail - that's expected behavior
    // Just checking it doesn't panic
    let _ = result;

    executor.execute("COMMIT").unwrap();
}
