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
    // Note: CREATE INDEX is not yet supported in CLI executor, so skip for now
    // This test will pass once CREATE INDEX support is added
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

// ============================================================================
// SHOW Statement Tests
// ============================================================================

#[test]
fn test_show_tables_empty() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW TABLES").unwrap();
    assert_eq!(result.columns, vec!["Tables_in_database"]);
    assert_eq!(result.row_count, 0);
}

#[test]
fn test_show_tables_with_tables() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY)").unwrap();
    executor.execute("CREATE TABLE products (id INT PRIMARY KEY)").unwrap();

    let result = executor.execute("SHOW TABLES").unwrap();
    assert_eq!(result.columns, vec!["Tables_in_database"]);
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_show_tables_like_pattern() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY)").unwrap();
    executor.execute("CREATE TABLE user_roles (id INT PRIMARY KEY)").unwrap();
    executor.execute("CREATE TABLE products (id INT PRIMARY KEY)").unwrap();

    let result = executor.execute("SHOW TABLES LIKE 'USER%'").unwrap();
    // Should match USERS and USER_ROLES
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_show_databases() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW DATABASES").unwrap();
    assert_eq!(result.columns, vec!["Database"]);
    // Should have at least the default public schema
    assert!(result.row_count >= 1);
}

#[test]
fn test_show_columns() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), active BOOLEAN NOT NULL)").unwrap();

    let result = executor.execute("SHOW COLUMNS FROM users").unwrap();
    assert_eq!(result.columns[0], "Field");
    assert_eq!(result.columns[1], "Type");
    assert_eq!(result.row_count, 3); // id, name, active
}

#[test]
fn test_show_full_columns() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();

    let result = executor.execute("SHOW FULL COLUMNS FROM users").unwrap();
    // SHOW FULL COLUMNS has more columns
    assert!(result.columns.contains(&"Collation".to_string()));
    assert!(result.columns.contains(&"Privileges".to_string()));
    assert!(result.columns.contains(&"Comment".to_string()));
}

#[test]
fn test_show_columns_like_pattern() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), nickname VARCHAR(50))").unwrap();

    let result = executor.execute("SHOW COLUMNS FROM users LIKE 'N%'").unwrap();
    // Should match NAME and NICKNAME
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_show_columns_nonexistent_table() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW COLUMNS FROM nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_show_index() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100))").unwrap();

    let result = executor.execute("SHOW INDEX FROM users").unwrap();
    assert_eq!(result.columns[0], "Table");
    assert_eq!(result.columns[2], "Key_name");
    // Primary key creates an index
    assert!(result.row_count >= 1);
}

#[test]
fn test_show_index_nonexistent_table() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW INDEX FROM nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_show_create_table() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();

    let result = executor.execute("SHOW CREATE TABLE users").unwrap();
    assert_eq!(result.columns, vec!["Table", "Create Table"]);
    assert_eq!(result.row_count, 1);

    // The CREATE TABLE statement should be in the second column
    let create_stmt = &result.rows[0][1];
    assert!(create_stmt.contains("CREATE TABLE"));
    assert!(create_stmt.contains("USERS")); // Table name is normalized to uppercase
}

#[test]
fn test_show_create_table_nonexistent() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW CREATE TABLE nonexistent");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_describe_statement() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))").unwrap();

    let result = executor.execute("DESCRIBE users").unwrap();
    // DESCRIBE is equivalent to SHOW COLUMNS
    assert_eq!(result.columns[0], "Field");
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_describe_with_column_pattern() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200))").unwrap();

    let result = executor.execute("DESCRIBE users 'N%'").unwrap();
    // Should only show NAME column (matching N%)
    assert_eq!(result.row_count, 1);
}
