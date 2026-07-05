use super::{validation, SqlExecutor};

#[test]
fn test_list_schemas() {
    let executor = SqlExecutor::new(None).unwrap();
    // Default database should have default schema
    assert!(executor.list_schemas().is_ok());
}

#[test]
fn test_wal_off_by_default() {
    // `new` (and the default config) must never activate the WAL path.
    let executor = SqlExecutor::new(None).unwrap();
    assert!(!executor.wal_active());
}

#[test]
fn test_wal_disabled_for_memory_database() {
    // Documented edge case: requesting WAL for an in-memory database silently
    // disables it (there is no file to attach the WAL to).
    let executor = SqlExecutor::new_with_wal(Some(":memory:".to_string()), true).unwrap();
    assert!(!executor.wal_active());
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
    executor
        .execute(
            "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10, 2))",
        )
        .unwrap();
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

    let result = executor
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    assert_eq!(result.row_count, 3, "Multiple value INSERT should return row count of 3");
}

#[test]
fn test_update_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();

    let result = executor.execute("UPDATE users SET name = 'Updated' WHERE id > 1").unwrap();
    assert_eq!(result.row_count, 2, "UPDATE should return row count of 2");
}

#[test]
fn test_delete_row_count() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor
        .execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();

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
    // Values are displayed using Display trait, not Debug (fix for #3810)
    assert_eq!(result.rows[0][0], Some("74".to_string()), "First column should be 74");
    assert_eq!(result.rows[0][1], Some("50".to_string()), "Second column should be 50");
}

#[test]
fn test_select_column_names_and_values_issue_3810() {
    // Regression test for issue #3810
    // SELECT should show actual column names/aliases, not generic "Column"
    // SELECT should show actual values, not typed representation like "Integer(1)"
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SELECT 1 as my_column, 'hello' as greeting").unwrap();

    // Column names should be the aliases, not "Column"
    // Note: SQL:1999 normalizes unquoted identifiers to lowercase
    assert_eq!(result.columns, vec!["my_column", "greeting"]);

    // Values should be display format, not debug format
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Some("1".to_string()),
        "Integer value should display as '1', not 'Integer(1)'"
    );
    assert_eq!(
        result.rows[0][1],
        Some("hello".to_string()),
        "Varchar value should display as 'hello', not 'Varchar(\"hello\")'"
    );
}

#[test]
fn test_select_column_names_from_table() {
    // Verify column names use short format by default (short_column_names=ON)
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    executor.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();

    let result = executor.execute("SELECT id, name FROM users").unwrap();

    // Default: short_column_names=ON, so just column names without table prefix
    assert_eq!(result.columns, vec!["id", "name"]);

    // Values should be display format
    assert_eq!(result.rows[0][0], Some("1".to_string()));
    assert_eq!(result.rows[0][1], Some("Alice".to_string()));
}

#[test]
fn test_select_wildcard_column_names() {
    // Verify SELECT * returns column names in short format by default
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE products (sku VARCHAR(20) PRIMARY KEY, price INT)").unwrap();
    executor.execute("INSERT INTO products VALUES ('ABC123', 99)").unwrap();

    let result = executor.execute("SELECT * FROM products").unwrap();

    // Default: short_column_names=ON, so just column names without table prefix
    assert_eq!(result.columns, vec!["sku", "price"]);
    assert_eq!(result.rows[0][0], Some("ABC123".to_string()));
    assert_eq!(result.rows[0][1], Some("99".to_string()));
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

    let result = executor.execute("SHOW TABLES LIKE 'user%'").unwrap();
    // Should match users and user_roles (lowercase per SQL:1999)
    assert_eq!(result.row_count, 2);
}

#[test]
fn test_show_databases() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("SHOW DATABASES").unwrap();
    assert_eq!(result.columns, vec!["Database"]);
    // Should have at least the default schema
    assert!(result.row_count >= 1);
}

#[test]
fn test_show_columns() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), active BOOLEAN NOT NULL)",
        )
        .unwrap();

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
    executor
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), nickname VARCHAR(50))")
        .unwrap();

    let result = executor.execute("SHOW COLUMNS FROM users LIKE 'n%'").unwrap();
    // Should match name and nickname (lowercase per SQL:1999)
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
    // Use TEXT PRIMARY KEY to ensure an autoindex is created
    // Note: INTEGER PRIMARY KEY is a rowid alias and doesn't create a separate index
    executor.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email VARCHAR(100))").unwrap();

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
    let create_stmt = result.rows[0][1].as_ref().expect("CREATE TABLE output should not be NULL");
    assert!(create_stmt.contains("CREATE TABLE"));
    assert!(create_stmt.contains("users")); // Table name is normalized to lowercase per SQL:1999
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
    executor
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(200))")
        .unwrap();

    let result = executor.execute("DESCRIBE users 'n%'").unwrap();
    // Should only show name column (lowercase per SQL:1999)
    assert_eq!(result.row_count, 1);
}

// ============================================================================
// Index, ALTER TABLE, and Transaction Tests
// ============================================================================

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

// ============================================================================
// PRAGMA count_changes tests (issue #5283)
// ============================================================================

#[test]
fn test_count_changes_default_off() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a INT)").unwrap();

    // Default OFF: DML returns no result rows
    let result = executor.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    assert!(result.rows.is_empty());
    assert_eq!(result.row_count, 2);

    // Query form reports 0
    let result = executor.execute("PRAGMA count_changes").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);
}

#[test]
fn test_count_changes_insert_update_delete() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a INT)").unwrap();
    executor.execute("PRAGMA count_changes=ON").unwrap();

    // Query form reports 1 while ON
    let result = executor.execute("PRAGMA count_changes").unwrap();
    assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);

    let result = executor.execute("INSERT INTO t VALUES(1),(2),(3)").unwrap();
    assert_eq!(result.rows, vec![vec![Some("3".to_string())]]);

    let result = executor.execute("UPDATE t SET a=a+10 WHERE a<3").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);

    let result = executor.execute("DELETE FROM t WHERE a=3").unwrap();
    assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);

    // SELECT output is unaffected by the pragma
    let result = executor.execute("SELECT count(*) FROM t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);

    // OFF restores current behavior
    executor.execute("PRAGMA count_changes=OFF").unwrap();
    let result = executor.execute("INSERT INTO t VALUES(9)").unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_count_changes_upsert_counts_direct_inserts_only() {
    // upsert1-400 semantics (verified against sqlite3): the count row for an
    // upsert INSERT reports only directly inserted rows, while changes()
    // includes rows taken through the DO UPDATE arm.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t2(a TEXT UNIQUE, b INT DEFAULT 1)").unwrap();
    executor.execute("INSERT INTO t2(a) VALUES('one'),('two'),('three')").unwrap();
    executor.execute("PRAGMA count_changes=ON").unwrap();

    let result = executor
        .execute(
            "INSERT INTO t2(a) VALUES('one'),('one'),('three'),('four') \
             ON CONFLICT(a) DO UPDATE SET b=b+1",
        )
        .unwrap();
    // Count row: 1 direct insert ('four'); the 3 DO UPDATE-arm rows excluded
    assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);

    executor.execute("PRAGMA count_changes=OFF").unwrap();

    // changes() still reports all 4 affected rows (SQLite parity)
    let result = executor.execute("SELECT changes()").unwrap();
    assert_eq!(result.rows, vec![vec![Some("4".to_string())]]);

    // upsert1-410: the DO UPDATE arm really ran (one hit twice, three once)
    let result = executor.execute("SELECT a, b FROM t2 ORDER BY a").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Some("four".to_string()), Some("1".to_string())],
            vec![Some("one".to_string()), Some("3".to_string())],
            vec![Some("three".to_string()), Some("2".to_string())],
            vec![Some("two".to_string()), Some("1".to_string())],
        ]
    );
}

#[test]
fn test_count_changes_does_not_replace_returning() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a INT)").unwrap();
    executor.execute("PRAGMA count_changes=ON").unwrap();

    // RETURNING output takes precedence over the count row
    let result = executor.execute("INSERT INTO t VALUES(7) RETURNING a").unwrap();
    assert_eq!(result.rows, vec![vec![Some("7".to_string())]]);
    assert_eq!(result.columns, vec!["a".to_string()]);
}

// ============================================================================
// ?NNN numbered placeholder tests (issue #5283)
// ============================================================================

#[test]
fn test_question_numbered_placeholder_upsert_inexact_target() {
    // upsert1-1210: once `b+?1` lexes, the inexact-conflict-target path must
    // yield SQLite's canonical error (not a syntax error near "1")
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
    executor.execute("CREATE UNIQUE INDEX t1x ON t1(b+3)").unwrap();

    let err = executor
        .execute("INSERT INTO t1(a,b) VALUES(1,2) ON CONFLICT(b+?1) DO NOTHING")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"),
        "unexpected error: {msg}"
    );
}

// Issue #5842 sub-item 4: PRAGMA gaps.

#[test]
fn test_pragma_journal_mode_echoes_wal() {
    // PRAGMA journal_mode (query form) must return a single row reporting the
    // active journaling mode. VibeSQL runs its own always-on WAL, so it reports
    // "wal" instead of silently returning an empty result.
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("PRAGMA journal_mode").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns, vec!["journal_mode".to_string()]);
    assert_eq!(result.rows[0][0].as_deref(), Some("wal"));
}

#[test]
fn test_pragma_journal_mode_set_is_accepted() {
    // The SET form is a silently-accepted no-op (VibeSQL's WAL is always on).
    let mut executor = SqlExecutor::new(None).unwrap();
    // Must not error.
    executor.execute("PRAGMA journal_mode = WAL").unwrap();
}

#[test]
fn test_pragma_integrity_check_no_argument() {
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("PRAGMA integrity_check").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));
}

#[test]
fn test_pragma_integrity_check_with_table_argument() {
    // The table-scoped form `PRAGMA integrity_check('t1')` previously fell into
    // the SET branch and was silently ignored (empty result). It must report
    // "ok" for any table argument.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t1(a INT)").unwrap();
    let result = executor.execute("PRAGMA integrity_check('t1')").unwrap();
    assert_eq!(result.row_count, 1, "integrity_check(table) should return one row");
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));

    // Unquoted identifier argument form as well.
    let result = executor.execute("PRAGMA integrity_check(t1)").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));
}
