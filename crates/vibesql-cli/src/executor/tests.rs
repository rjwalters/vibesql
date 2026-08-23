use super::{validation, DbOpenOptions, SqlExecutor};

#[test]
fn test_columnar_cache_budget_applied_on_open() {
    // A configured budget is applied to the Database on open (#6200).
    let budget = 8 * 1024 * 1024; // 8MB
    let executor = SqlExecutor::new_with_options(
        None,
        DbOpenOptions { columnar_cache_budget: budget, ..DbOpenOptions::default() },
    )
    .unwrap();
    assert_eq!(executor.db.columnar_cache_budget(), budget);
}

#[test]
fn test_columnar_cache_budget_zero_disables_cache() {
    // `columnar_cache_budget = 0` disables the cache: the Database reports a
    // 0-byte budget after open (#6200).
    let executor = SqlExecutor::new_with_options(
        None,
        DbOpenOptions { columnar_cache_budget: 0, ..DbOpenOptions::default() },
    )
    .unwrap();
    assert_eq!(executor.db.columnar_cache_budget(), 0);
}

#[test]
fn test_columnar_cache_budget_default_is_256mb() {
    // The default open options carry the 256MB budget through to the Database.
    let executor = SqlExecutor::new(None).unwrap();
    assert_eq!(executor.db.columnar_cache_budget(), 256 * 1024 * 1024);
}

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

#[test]
fn test_pragma_integrity_check_argument_taxonomy() {
    // SQLite distinguishes a numeric error-count *limit* from a table/schema
    // *name* argument (pragma-3.5.2 / pragma-3.6):
    //   PRAGMA integrity_check=4    -- limit 4 errors, whole db -> "ok"
    //   PRAGMA integrity_check='4'  -- table named "4" -> "no such table: 4"
    //   PRAGMA integrity_check=xyz  -- table named "xyz" -> "no such table: xyz"
    // An existing table (or a schema table such as sqlite_schema) is a valid
    // target and reports "ok".
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t2(a INT)").unwrap();

    // Numeric argument is an error-count limit, not a table name.
    let result = executor.execute("PRAGMA integrity_check=4").unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));

    // Existing table -> ok.
    let result = executor.execute("PRAGMA integrity_check=t2").unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));

    // Schema table is always a valid target.
    let result = executor.execute("PRAGMA integrity_check=sqlite_schema").unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("ok"));

    // Quoted string that is not a table -> "no such table: 4".
    let err = executor.execute("PRAGMA integrity_check='4'").unwrap_err();
    assert_eq!(err.to_string(), "no such table: 4");

    // Bare identifier that is not a table -> "no such table: xyz".
    let err = executor.execute("PRAGMA integrity_check=xyz").unwrap_err();
    assert_eq!(err.to_string(), "no such table: xyz");

    // quick_check shares the same argument handling.
    let err = executor.execute("PRAGMA quick_check=nope").unwrap_err();
    assert_eq!(err.to_string(), "no such table: nope");
}

#[test]
fn test_pragma_foreign_key_check_missing_table_errors() {
    // SQLite: `PRAGMA foreign_key_check(NAME)` on a table that does not exist
    // raises "no such table: NAME" (pragma4-4.6.5, fkey5). This differs from
    // foreign_key_list / table_info, which return an empty result for a
    // missing table.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t1(a)").unwrap();
    executor.execute("CREATE UNIQUE INDEX i1 ON t1(a)").unwrap();
    executor.execute("CREATE TABLE c1(a, b, c REFERENCES t1(a))").unwrap();
    executor.execute("INSERT INTO c1 VALUES(1, 2, 3)").unwrap();

    // Named argument that is not a table -> "no such table: NAME" (both the
    // quoted-string and bare-identifier spellings).
    let err = executor.execute("PRAGMA foreign_key_check('c2')").unwrap_err();
    assert_eq!(err.to_string(), "no such table: c2");
    let err = executor.execute("PRAGMA foreign_key_check(nope)").unwrap_err();
    assert_eq!(err.to_string(), "no such table: nope");

    // An existing table with a violated FK still reports the violation row
    // (row 1 of c1 references t1(a)=3, which does not exist): table, rowid,
    // parent, fkid.
    let result = executor.execute("PRAGMA foreign_key_check('c1')").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("c1"));
    assert_eq!(result.rows[0][1].as_deref(), Some("1"));
    assert_eq!(result.rows[0][2].as_deref(), Some("t1"));
    assert_eq!(result.rows[0][3].as_deref(), Some("0"));

    // An existing table with no violations returns an empty result (no error).
    executor.execute("CREATE TABLE t2(a)").unwrap();
    let result = executor.execute("PRAGMA foreign_key_check('t2')").unwrap();
    assert_eq!(result.row_count, 0);

    // The whole-database form (no argument) never errors on a missing table.
    let result = executor.execute("PRAGMA foreign_key_check").unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("c1"));

    // Schema tables are always valid targets and never error.
    let result = executor.execute("PRAGMA foreign_key_check('sqlite_master')").unwrap();
    assert_eq!(result.row_count, 0);
}

#[test]
fn test_pragma_table_info_verbatim_type_and_default() {
    // PRAGMA table_info echoes the declared type verbatim (only bracket/quote
    // delimiters stripped) and the verbatim DEFAULT source text, matching
    // SQLite (pragma-6.7). Columns: cid, name, type, notnull, dflt_value, pk.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor
        .execute(
            "CREATE TABLE test_table(\
                one INT NOT NULL DEFAULT -1, \
                two text, \
                three VARCHAR(45, 65) DEFAULT 'abcde', \
                four REAL DEFAULT X'abcdef', \
                five DEFAULT CURRENT_TIME)",
        )
        .unwrap();
    let result = executor.execute("PRAGMA table_info(test_table)").unwrap();
    let expect: Vec<(&str, &str, &str, Option<&str>)> = vec![
        ("one", "INT", "1", Some("-1")),
        // `text` (lowercase) canonicalizes to `TEXT`; a column with no DEFAULT
        // reports NULL.
        ("two", "TEXT", "0", None),
        // Two-argument VARCHAR the affinity mapping cannot round-trip is echoed
        // verbatim, and the string default keeps its quotes.
        ("three", "VARCHAR(45, 65)", "0", Some("'abcde'")),
        // Blob-literal default preserves SQLite's `X'..'` spelling (not the
        // ToSql `x'ABCDEF'` re-render).
        ("four", "REAL", "0", Some("X'abcdef'")),
        // Typeless column reports an empty type; CURRENT_TIME default verbatim.
        ("five", "", "0", Some("CURRENT_TIME")),
    ];
    assert_eq!(result.row_count, expect.len());
    for (i, (name, ty, notnull, dflt)) in expect.into_iter().enumerate() {
        assert_eq!(result.rows[i][1].as_deref(), Some(name), "name row {i}");
        assert_eq!(result.rows[i][2].as_deref(), Some(ty), "type row {i}");
        assert_eq!(result.rows[i][3].as_deref(), Some(notnull), "notnull row {i}");
        assert_eq!(result.rows[i][4].as_deref(), dflt, "dflt_value row {i}");
    }
}

#[test]
fn test_pragma_table_info_strips_type_delimiters() {
    // Bracketed / double-quoted type names report the inner name only
    // (pragma-6.2): `[TYPE_Y]` -> `TYPE_Y`, `"TYPE_Z"` -> `TYPE_Z`. A plain
    // user type is echoed unchanged.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t2(a TYPE_X, b [TYPE_Y], c \"TYPE_Z\")").unwrap();
    let result = executor.execute("PRAGMA table_info(t2)").unwrap();
    assert_eq!(result.rows[0][2].as_deref(), Some("TYPE_X"));
    assert_eq!(result.rows[1][2].as_deref(), Some("TYPE_Y"));
    assert_eq!(result.rows[2][2].as_deref(), Some("TYPE_Z"));
}

#[test]
fn test_pragma_table_info_default_strips_outer_parens() {
    // A parenthesized DEFAULT expression reports without its single outer paren
    // pair, matching SQLite (`DEFAULT (5+3)` -> `5+3`, pragma-6.2.2).
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t9(b DEFAULT (5+3))").unwrap();
    let result = executor.execute("PRAGMA table_info(t9)").unwrap();
    assert_eq!(result.rows[0][4].as_deref(), Some("5+3"));
}

#[test]
fn test_pragma_database_list_memory_no_temp() {
    // An in-memory session with no temp objects reports exactly one row:
    // seq=0, name=main, file="" — matching sqlite3 3.51.0, which omits the
    // `temp` row until a temp object exists.
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.columns, vec!["seq", "name", "file"]);
    assert_eq!(result.row_count, 1, "no temp object yet -> only main");
    assert_eq!(result.rows[0][0].as_deref(), Some("0"));
    assert_eq!(result.rows[0][1].as_deref(), Some("main"));
    assert_eq!(result.rows[0][2].as_deref(), Some(""), "in-memory main has empty file");
}

#[test]
fn test_pragma_database_list_temp_table_adds_temp_row() {
    // Creating a temp table materializes the session temp schema; the `temp`
    // database then appears as seq=1, name=temp, file="".
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TEMP TABLE t(x INT)").unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.row_count, 2, "temp table -> main + temp");
    assert_eq!(result.rows[0][1].as_deref(), Some("main"));
    assert_eq!(result.rows[1][0].as_deref(), Some("1"));
    assert_eq!(result.rows[1][1].as_deref(), Some("temp"));
    assert_eq!(result.rows[1][2].as_deref(), Some(""), "temp file is always empty");
}

#[test]
fn test_pragma_database_list_temp_view_adds_temp_row() {
    // A temp view (no temp table) also triggers the temp database row.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TEMP VIEW v AS SELECT 1").unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.row_count, 2, "temp view -> main + temp");
    assert_eq!(result.rows[1][1].as_deref(), Some("temp"));
}

#[test]
fn test_pragma_database_list_temp_trigger_adds_temp_row() {
    // A temp trigger (fired on a persistent table) also triggers the temp row.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE base(a INT)").unwrap();
    executor.execute("CREATE TEMP TRIGGER tr AFTER INSERT ON base BEGIN SELECT 1; END").unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.row_count, 2, "temp trigger -> main + temp");
    assert_eq!(result.rows[1][1].as_deref(), Some("temp"));
}

#[test]
fn test_pragma_database_list_temp_row_sticky_after_drop() {
    // Once the temp database has been touched, it stays reported even after
    // every temp object created in it is dropped — verified against real
    // sqlite3 3.51.0 (`CREATE TEMP TABLE t1(...); DROP TABLE temp.t1;` still
    // reports a `temp` row). See #6406 / e_createtable-1.3..1.6, which
    // create-then-drop temp objects across a test group and still expect
    // `X(temp)` present (as an empty list) in every later `table_list`
    // snapshot.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TEMP TABLE t1(x INT)").unwrap();
    executor.execute("DROP TABLE temp.t1").unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.row_count, 2, "temp row must stick around after drop");
    assert_eq!(result.rows[1][1].as_deref(), Some("temp"));
}

#[test]
fn test_pragma_database_list_persistent_objects_no_temp_row() {
    // Persistent tables/views must NOT cause the temp database to appear.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a INT)").unwrap();
    executor.execute("CREATE VIEW v AS SELECT 1").unwrap();
    let result = executor.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.row_count, 1, "persistent objects only -> just main");
    assert_eq!(result.rows[0][1].as_deref(), Some("main"));
}

#[test]
fn test_pragma_data_version_returns_one() {
    // PRAGMA data_version reports 1 for a connection that has observed no
    // external commit (SQLite's initial value). The read-only-write form
    // `= N` is a no-op that still reports the current value.
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("PRAGMA data_version").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.columns, vec!["data_version".to_string()]);
    assert_eq!(result.rows[0][0].as_deref(), Some("1"));

    // Read-only-write form still reports 1.
    let result = executor.execute("PRAGMA data_version = 1234").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0].as_deref(), Some("1"));

    // Schema-qualified form is accepted and ignored.
    let result = executor.execute("PRAGMA main.data_version").unwrap();
    assert_eq!(result.rows[0][0].as_deref(), Some("1"));
}

#[test]
fn test_pragma_collation_list_builtins() {
    // PRAGMA collation_list reports the three built-in collating sequences,
    // most-recently-registered first: RTRIM, NOCASE, BINARY.
    let mut executor = SqlExecutor::new(None).unwrap();
    let result = executor.execute("PRAGMA collation_list").unwrap();
    assert_eq!(result.columns, vec!["seq".to_string(), "name".to_string()]);
    assert_eq!(result.row_count, 3);
    assert_eq!(result.rows[0][0].as_deref(), Some("0"));
    assert_eq!(result.rows[0][1].as_deref(), Some("RTRIM"));
    assert_eq!(result.rows[1][1].as_deref(), Some("NOCASE"));
    assert_eq!(result.rows[2][1].as_deref(), Some("BINARY"));
}

#[test]
fn test_pragma_table_info_typeless_column_reports_empty_type() {
    // A column declared without a datatype (`CREATE TABLE t(a)`) has BLOB
    // affinity internally, but SQLite's table_info reports an *empty* declared
    // type for it, not "BLOB". Regression guard for #6175 (pragma-6.2.2/6.2.3).
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a, b TEXT, c)").unwrap();

    let result = executor.execute("PRAGMA table_info(t)").unwrap();
    assert_eq!(result.row_count, 3);
    // type column is index 2.
    assert_eq!(result.rows[0][2].as_deref(), Some(""), "typeless column a -> empty type");
    assert_eq!(result.rows[1][2].as_deref(), Some("TEXT"), "typed column b keeps its type");
    assert_eq!(result.rows[2][2].as_deref(), Some(""), "typeless column c -> empty type");
}

#[test]
fn test_pragma_table_info_integer_primary_key_notnull_is_zero() {
    // An INTEGER PRIMARY KEY rowid alias is internally non-nullable, but
    // SQLite's table_info reports notnull=0 for it because there is no
    // *explicit* NOT NULL clause. An explicit NOT NULL still reports 1.
    // Regression guard for #6175 (pragma-6.2.3).
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t(a, b INTEGER PRIMARY KEY, c TEXT NOT NULL)").unwrap();

    let result = executor.execute("PRAGMA table_info(t)").unwrap();
    assert_eq!(result.row_count, 3);
    // Columns are cid, name, type, notnull, dflt_value, pk (notnull is index 3).
    assert_eq!(result.rows[1][3].as_deref(), Some("0"), "INTEGER PRIMARY KEY notnull=0");
    assert_eq!(result.rows[1][5].as_deref(), Some("1"), "INTEGER PRIMARY KEY pk=1");
    assert_eq!(result.rows[2][3].as_deref(), Some("1"), "explicit NOT NULL notnull=1");
}

#[test]
fn test_pragma_table_info_composite_pk_positions() {
    // A normal composite PRIMARY KEY reports 1-based positions in declared order.
    // Regression guard for #6175 (pragma-6.8, no-duplicate case).
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE tk(a, b, c, PRIMARY KEY(a, b, c))").unwrap();
    let result = executor.execute("PRAGMA table_info(tk)").unwrap();
    assert_eq!(result.row_count, 3);
    // pk column is index 5.
    assert_eq!(result.rows[0][5].as_deref(), Some("1"), "a is pk position 1");
    assert_eq!(result.rows[1][5].as_deref(), Some("2"), "b is pk position 2");
    assert_eq!(result.rows[2][5].as_deref(), Some("3"), "c is pk position 3");
}

#[test]
fn test_pragma_table_info_composite_pk_duplicate_column_gap() {
    // SQLite keys pk position off each column's *first* occurrence in the
    // declared PRIMARY KEY list, but a repeated column still consumes an
    // ordinal. `PRIMARY KEY(a,b,a,c)` therefore yields a=1, b=2, c=4 (the
    // duplicate `a` consumes position 3). Regression guard for #6175
    // (pragma-6.8).
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t68(a, b, c, PRIMARY KEY(a, b, a, c))").unwrap();
    let result = executor.execute("PRAGMA table_info(t68)").unwrap();
    assert_eq!(result.row_count, 3);
    // pk column is index 5.
    assert_eq!(result.rows[0][5].as_deref(), Some("1"), "a is pk position 1");
    assert_eq!(result.rows[1][5].as_deref(), Some("2"), "b is pk position 2");
    assert_eq!(result.rows[2][5].as_deref(), Some("4"), "c is pk position 4 (dup a consumed 3)");
}

#[test]
fn test_pragma_index_info_reports_key_columns() {
    // PRAGMA index_info(idx) returns one row per key column: seqno, cid (table
    // column rank), name. The `= idx` form is accepted the same as `(idx)`.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t3(a, b, c)").unwrap();
    executor.execute("CREATE INDEX t3i2 ON t3(b, a)").unwrap();

    let result = executor.execute("PRAGMA index_info(t3i2)").unwrap();
    assert_eq!(result.columns, vec!["seqno".to_string(), "cid".to_string(), "name".to_string()]);
    assert_eq!(result.row_count, 2);
    // b is table column 1, a is table column 0.
    assert_eq!(result.rows[0], vec![Some("0".into()), Some("1".into()), Some("b".into())]);
    assert_eq!(result.rows[1], vec![Some("1".into()), Some("0".into()), Some("a".into())]);

    // `= idx` form.
    let result = executor.execute("PRAGMA index_info = t3i2").unwrap();
    assert_eq!(result.row_count, 2);

    // Unknown index -> empty result (no error).
    let result = executor.execute("PRAGMA index_info(nope)").unwrap();
    assert_eq!(result.row_count, 0);
}

#[test]
fn test_pragma_index_xinfo_appends_rowid_aux_column() {
    // PRAGMA index_xinfo(idx) adds desc/coll/key columns and appends the
    // auxiliary rowid entry (cid -1, name NULL, key 0) that index_info omits.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t3(a, b)").unwrap();
    executor.execute("CREATE INDEX t3i1 ON t3(a, b)").unwrap();

    let result = executor.execute("PRAGMA index_xinfo(t3i1)").unwrap();
    assert_eq!(
        result.columns,
        vec![
            "seqno".to_string(),
            "cid".to_string(),
            "name".to_string(),
            "desc".to_string(),
            "coll".to_string(),
            "key".to_string()
        ]
    );
    assert_eq!(result.row_count, 3);
    // Two key columns, then the auxiliary rowid column.
    assert_eq!(result.rows[0][5].as_deref(), Some("1"), "a is a key column");
    assert_eq!(result.rows[1][5].as_deref(), Some("1"), "b is a key column");
    assert_eq!(result.rows[2][1].as_deref(), Some("-1"), "aux rowid cid = -1");
    assert_eq!(result.rows[2][2], None, "aux rowid name is NULL");
    assert_eq!(result.rows[2][5].as_deref(), Some("0"), "aux column key = 0");
}

#[test]
fn test_pragma_index_list_origins() {
    // PRAGMA index_list(table) reports seq, name, unique, origin, partial. An
    // explicit CREATE INDEX has origin 'c'; a UNIQUE-constraint autoindex has
    // origin 'u'; a PRIMARY KEY autoindex has origin 'pk'.
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t3(a, b UNIQUE)").unwrap();
    executor.execute("CREATE INDEX t3i1 ON t3(a, b)").unwrap();

    let result = executor.execute("PRAGMA index_list(t3)").unwrap();
    assert_eq!(
        result.columns,
        vec![
            "seq".to_string(),
            "name".to_string(),
            "unique".to_string(),
            "origin".to_string(),
            "partial".to_string()
        ]
    );
    // Newest-first ordering: the explicit index appears before the autoindex.
    let names: Vec<Option<&str>> = result.rows.iter().map(|r| r[1].as_deref()).collect();
    assert!(names.contains(&Some("t3i1")));
    assert!(names.contains(&Some("sqlite_autoindex_t3_1")));
    for row in &result.rows {
        match row[1].as_deref() {
            Some("t3i1") => {
                assert_eq!(row[2].as_deref(), Some("0"), "explicit index not unique");
                assert_eq!(row[3].as_deref(), Some("c"), "explicit -> origin c");
            }
            Some("sqlite_autoindex_t3_1") => {
                assert_eq!(row[2].as_deref(), Some("1"), "UNIQUE autoindex is unique");
                assert_eq!(row[3].as_deref(), Some("u"), "UNIQUE -> origin u");
            }
            other => panic!("unexpected index {other:?}"),
        }
    }

    // PRIMARY KEY autoindex -> origin pk.
    executor.execute("CREATE TABLE tp(a, b, PRIMARY KEY(a, b))").unwrap();
    let result = executor.execute("PRAGMA index_list(tp)").unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][3].as_deref(), Some("pk"));

    // Unknown table -> empty result (no error).
    let result = executor.execute("PRAGMA index_list(nope)").unwrap();
    assert_eq!(result.row_count, 0);
}

// ============================================================================
// PRAGMA auto_vacuum / temp_store parse-normalize-echo tests (issue #6175,
// pragma.test pragma-17 / pragma-18). VibeSQL has no pager auto-vacuum and
// demotes TEMP tables to persistent, but it parses/normalizes/echoes both
// settings exactly like SQLite so introspection round-trips.
// ============================================================================

#[test]
fn test_pragma_auto_vacuum_default_and_normalization() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 0 (NONE).
    let result = executor.execute("PRAGMA auto_vacuum").unwrap();
    assert_eq!(result.columns, vec!["auto_vacuum".to_string()]);
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // Numeric + symbolic spellings normalize to the canonical code, and the
    // value round-trips through a subsequent read. (setting, expected-echo)
    for (set, want) in [
        ("0", "0"),
        ("1", "1"),
        ("2", "2"),
        ("3", "0"),  // out-of-range -> NONE
        ("-1", "0"), // negative -> NONE
        ("1234", "0"),
        ("-1234", "0"),
        ("none", "0"),
        ("NONE", "0"),
        ("NoNe", "0"),
        ("full", "1"),
        ("FULL", "1"),
        ("incremental", "2"),
        ("INCREMENTAL", "2"),
    ] {
        executor.execute(&format!("PRAGMA auto_vacuum={set}")).unwrap();
        let result = executor.execute("PRAGMA auto_vacuum").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some(want.to_string())]],
            "auto_vacuum={set} should echo {want}"
        );
    }
}

#[test]
fn test_pragma_temp_store_default_and_normalization() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 0 (DEFAULT).
    let result = executor.execute("PRAGMA temp_store").unwrap();
    assert_eq!(result.columns, vec!["temp_store".to_string()]);
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    for (set, want) in [
        ("0", "0"),
        ("1", "1"),
        ("2", "2"),
        ("3", "0"),  // out-of-range -> DEFAULT
        ("-1", "0"), // negative -> DEFAULT
        ("file", "1"),
        ("FILE", "1"),
        ("fIlE", "1"),
        ("memory", "2"),
        ("MEMORY", "2"),
        ("MeMoRy", "2"),
        ("default", "0"),
    ] {
        executor.execute(&format!("PRAGMA temp_store={set}")).unwrap();
        let result = executor.execute("PRAGMA temp_store").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some(want.to_string())]],
            "temp_store={set} should echo {want}"
        );
    }
}

#[test]
fn test_pragma_synchronous_default_and_arithmetic() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 2 (FULL).
    let result = executor.execute("PRAGMA synchronous").unwrap();
    assert_eq!(result.columns, vec!["synchronous".to_string()]);
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);

    // SQLite's exact getSafetyLevel()+mask arithmetic (pragma.test
    // pragma-1.6/1.10/1.11.x/1.13/1.14.x): keyword and numeric spellings,
    // including out-of-range numbers that wrap via `(raw+1) & 0x07`.
    for (set, want) in [
        ("OFF", "0"),
        ("ON", "1"),
        ("NORMAL", "1"), // unlisted keyword falls through to NORMAL's value
        ("FULL", "2"),
        ("EXTRA", "3"),
        ("0", "0"),
        ("2", "2"),
        ("4", "4"),
        ("3", "3"),
        ("8", "0"),  // wraps
        ("10", "2"), // wraps
    ] {
        executor.execute(&format!("PRAGMA synchronous={set}")).unwrap();
        let result = executor.execute("PRAGMA synchronous").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some(want.to_string())]],
            "synchronous={set} should echo {want}"
        );
    }
}

#[test]
fn test_pragma_synchronous_rejected_inside_transaction() {
    let mut executor = SqlExecutor::new(None).unwrap();

    executor.execute("BEGIN").unwrap();
    let result = executor.execute("PRAGMA synchronous = OFF");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Safety level may not be changed inside a transaction"));

    // The rejected SET must not have taken effect.
    executor.execute("ROLLBACK").unwrap();
    let result = executor.execute("PRAGMA synchronous").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);
}

#[test]
fn test_pragma_cache_size_and_default_cache_size() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Both default to -2000 (SQLITE_DEFAULT_CACHE_SIZE) before anything is set.
    let result = executor.execute("PRAGMA cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-2000".to_string())]]);
    let result = executor.execute("PRAGMA default_cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-2000".to_string())]]);

    // `cache_size=N` stores the raw signed value verbatim and does NOT touch
    // default_cache_size (pragma.test pragma-1.2/1.5).
    executor.execute("PRAGMA cache_size=-4321").unwrap();
    let result = executor.execute("PRAGMA cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-4321".to_string())]]);
    let result = executor.execute("PRAGMA default_cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-2000".to_string())]]);

    // `default_cache_size=N` normalizes to abs(N) and updates BOTH pragmas
    // immediately (pragma.test pragma-1.8).
    executor.execute("PRAGMA default_cache_size=-123").unwrap();
    let result = executor.execute("PRAGMA cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("123".to_string())]]);
    let result = executor.execute("PRAGMA default_cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("123".to_string())]]);
}

#[test]
fn test_pragma_cache_spill_default_and_toggle() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default: enabled, no explicit size -> mirrors cache_size.
    executor.execute("PRAGMA cache_size=2000").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2000".to_string())]]);

    // Disabling reads back 0 regardless of cache_size.
    executor.execute("PRAGMA cache_spill=OFF").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);
}

#[test]
fn test_pragma_page_size_default_and_set() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is SQLITE_DEFAULT_PAGE_SIZE (4096).
    let result = executor.execute("PRAGMA page_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("4096".to_string())]]);

    // A valid power-of-two size in [512, 65536] is accepted.
    executor.execute("PRAGMA page_size=16384").unwrap();
    let result = executor.execute("PRAGMA page_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("16384".to_string())]]);

    // Out-of-range / non-power-of-two sizes are silently ignored, exactly like
    // SQLite's `sqlite3BtreeSetPageSize` guard (pragma4.test 1.18 vs 1.19).
    for bad in ["511", "1000", "0", "131072"] {
        executor.execute(&format!("PRAGMA page_size={bad}")).unwrap();
        let result = executor.execute("PRAGMA page_size").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("16384".to_string())]],
            "PRAGMA page_size={bad} should have been ignored"
        );
    }

    // Setting the pragma returns no rows.
    let result = executor.execute("PRAGMA page_size=512").unwrap();
    assert_eq!(result.row_count, 0);
    assert!(result.rows.is_empty());
}

#[test]
fn test_pragma_cache_spill_reads_max_of_cache_pages_and_threshold() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // SQLite's `sqlite3PcacheSetSpillsize` returns
    // `max(numberOfCachePages(cache_size), szSpill)`, so a spill threshold
    // *below* the cache size reads back as the cache size
    // (pragma2.test pragma2-4.5.3: cache_size=50, cache_spill=25 -> 50).
    executor.execute("PRAGMA cache_size=50").unwrap();
    executor.execute("PRAGMA cache_spill=25").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("50".to_string())]]);

    // A threshold above the cache size wins (pragma2-4.5.2).
    executor.execute("PRAGMA cache_spill=100000").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("100000".to_string())]]);
}

#[test]
fn test_pragma_cache_spill_negative_argument_is_kib_scaled_by_page_size() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // pragma2.test pragma2-5.1..5.3, verbatim.
    executor.execute("PRAGMA page_size=16384").unwrap();
    executor.execute("PRAGMA cache_size=2").unwrap();
    executor.execute("PRAGMA cache_spill=YES").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);

    executor.execute("PRAGMA cache_spill=NO").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // A negative argument is a KiB budget: -51 KiB / 16384 B per page = 3
    // pages, which beats the 2-page cache size. It also re-enables spilling
    // (SQLite's `sqlite3GetBoolean("-51", 1)` is true).
    executor.execute("PRAGMA cache_spill(-51)").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("3".to_string())]]);
}

#[test]
fn test_pragma_cache_spill_negative_argument_keeps_spilling_enabled() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // SQLite's `getSafetyLevel()` gates its numeric branch on
    // `sqlite3Isdigit(*z)` — an ASCII digit in the *first* position, no sign
    // — so a negative argument matches neither the digit branch nor any
    // keyword and returns `dflt`, which `pragma.c` supplies as
    // `(size != 0)`. Every nonzero negative therefore leaves spilling
    // *enabled*. Verified against SQLite 3.53.4:
    //   PRAGMA cache_spill=-1024; PRAGMA cache_spill;  -> enabled (nonzero)
    //
    // Regression guard for the `-256` / `-1024` / `-2048` family (the natural
    // "N MiB budget" spellings): their low byte is zero, so treating a
    // leading `-` as "numeric" and testing `(parsed as u8) != 0` reports
    // spilling *off*. `-51` — the only negative the other tests cover —
    // happens to mask the bug because its low byte is nonzero.
    //
    // page_size 4096 and the default cache_size of -2000 give
    // numberOfCachePages = 2048000 / 4096 = 500, and the read-back is
    // max(500, szSpill).
    for (arg, expected) in [("-256", "500"), ("-1024", "500"), ("-2048", "512")] {
        executor.execute("PRAGMA cache_spill=OFF").unwrap();
        let result = executor.execute("PRAGMA cache_spill").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("0".to_string())]],
            "cache_spill=OFF should disable spilling"
        );

        executor.execute(&format!("PRAGMA cache_spill({arg})")).unwrap();
        let result = executor.execute("PRAGMA cache_spill").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some(expected.to_string())]],
            "PRAGMA cache_spill({arg}) must leave spilling enabled (SQLite dflt = size != 0)"
        );
    }

    // The positive side of the same gate is deliberately unchanged: `256`
    // *does* take the digit branch, and SQLite's `(u8)sqlite3Atoi("256") == 0`
    // disables spilling (3.53.4: `PRAGMA cache_spill=256` reads back 0).
    executor.execute("PRAGMA cache_spill=256").unwrap();
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);
}

#[test]
fn test_pragma_cache_spill_negative_cache_size_is_kib_scaled() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // A negative `cache_size` is likewise a KiB budget when cache_spill
    // resolves it to a page count: -2000 KiB / 4096 B = 500 pages
    // (SQLite's `numberOfCachePages`).
    let result = executor.execute("PRAGMA cache_spill").unwrap();
    assert_eq!(result.rows, vec![vec![Some("500".to_string())]]);

    // ... but `PRAGMA cache_size` itself still echoes the raw signed value.
    let result = executor.execute("PRAGMA cache_size").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-2000".to_string())]]);
}

#[test]
fn test_pragma_user_version_default_set_and_negative() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 0 (pragma.test pragma-8.2.1, #6175).
    let result = executor.execute("PRAGMA user_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // `= N` form.
    executor.execute("PRAGMA user_version = 2").unwrap();
    let result = executor.execute("PRAGMA user_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("2".to_string())]]);

    // Negative values round-trip (pragma-8.2.14/8.2.15).
    executor.execute("PRAGMA user_version = -450").unwrap();
    let result = executor.execute("PRAGMA user_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("-450".to_string())]]);
}

#[test]
fn test_pragma_application_id_default_and_function_style_set() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 0 (pragma.test pragma-8.3.1, #6175).
    let result = executor.execute("PRAGMA application_id").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // Function-style `(N)` argument (pragma-8.3.2: `PRAGMA Application_ID(12345)`).
    executor.execute("PRAGMA application_id(12345)").unwrap();
    let result = executor.execute("PRAGMA application_id").unwrap();
    assert_eq!(result.rows, vec![vec![Some("12345".to_string())]]);
}

#[test]
fn test_pragma_schema_version_default_set_and_ddl_autoincrement() {
    let mut executor = SqlExecutor::new(None).unwrap();

    // Default is 0.
    let result = executor.execute("PRAGMA schema_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // Explicit `= N` set (pragma.test pragma-8.1.1/8.1.2).
    executor.execute("PRAGMA schema_version = 105").unwrap();
    let result = executor.execute("PRAGMA schema_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("105".to_string())]]);

    // A successful DDL statement bumps the cookie by 1 (pragma-8.1.5/8.1.6:
    // schema_version 106 -> CREATE TABLE -> 107).
    executor.execute("PRAGMA schema_version = 106").unwrap();
    executor.execute("CREATE TABLE t4(a, b, c)").unwrap();
    let result = executor.execute("PRAGMA schema_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("107".to_string())]]);

    // VACUUM also bumps the cookie (pragma-8.2.4.2/8.2.4.3: 108 -> VACUUM -> 109).
    executor.execute("PRAGMA schema_version = 108").unwrap();
    executor.execute("VACUUM").unwrap();
    let result = executor.execute("PRAGMA schema_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("109".to_string())]]);

    // A plain read (no DDL) leaves the cookie unchanged.
    let result = executor.execute("PRAGMA schema_version").unwrap();
    assert_eq!(result.rows, vec![vec![Some("109".to_string())]]);
}

#[test]
fn test_pragma_index_xinfo_expression_column_cid_and_explicit_collation() {
    let mut executor = SqlExecutor::new(None).unwrap();
    executor.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, d)").unwrap();
    executor.execute("CREATE INDEX i2x ON t1(d COLLATE nocase, c DESC)").unwrap();
    executor.execute("CREATE INDEX i3 ON t1(d, b+c, c)").unwrap();

    // Explicit COLLATE on an index column is echoed verbatim, not hardcoded
    // BINARY (pragma.test 23.2d, #6175).
    let result = executor.execute("PRAGMA index_xinfo(i2x)").unwrap();
    // Columns: seqno, cid, name, desc, coll, key
    assert_eq!(result.rows[0][4], Some("nocase".to_string()));
    // The second (non-collated) key column still defaults to BINARY.
    assert_eq!(result.rows[1][4], Some("BINARY".to_string()));

    // An expression index column reports cid -2 (not -1, which is reserved
    // for a rowid reference) (pragma.test 23.2e, #6175).
    let result = executor.execute("PRAGMA index_xinfo(i3)").unwrap();
    assert_eq!(result.rows[1][1], Some("-2".to_string()));
    assert_eq!(result.rows[1][2], None);
}

// ============================================================================
// ATTACH DATABASE / DETACH DATABASE (#6310, Phase 1 — session-scoped)
// ============================================================================

/// Helper: a fresh in-memory executor for ATTACH tests.
fn attach_test_executor() -> SqlExecutor {
    SqlExecutor::new(None).unwrap()
}

#[test]
fn test_attach_memory_lifecycle_cross_schema() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();

    let result = ex.execute("SELECT * FROM aux.t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);

    // Cross-schema join between main and aux.
    ex.execute("CREATE TABLE m(a INTEGER)").unwrap();
    ex.execute("INSERT INTO m VALUES (10)").unwrap();
    let result = ex.execute("SELECT a, x FROM m, aux.t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("10".to_string()), Some("1".to_string())]]);
}

#[test]
fn test_attach_nonexistent_file_behaves_like_memory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("newfile.db");
    let mut ex = attach_test_executor();
    ex.execute(&format!("ATTACH '{}' AS aux", path.display())).unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
    let result = ex.execute("SELECT x FROM aux.t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("7".to_string())]]);
    // Phase 1 is session-scoped: nothing is written to the declared path.
    assert!(!path.exists(), "Phase 1 must not create the attached file");
}

#[test]
fn test_attach_existing_invalid_file_errors_and_rolls_back() {
    // Phase 2 (#6362) removed the Phase 1 "not yet supported" guard: an
    // existing non-empty file is now loaded. A file that isn't a recognized
    // VibeSQL/SQLite/SQL-dump format surfaces a load error instead — and the
    // failed attachment must roll back cleanly (no half-registered schema
    // left behind; the name is free to retry).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("real.db");
    std::fs::write(&path, b"not a recognized database format").unwrap();
    let mut ex = attach_test_executor();
    let err = ex.execute(&format!("ATTACH '{}' AS aux", path.display())).unwrap_err();
    assert!(
        !err.to_string().contains("not yet supported"),
        "Phase 1 guard message should be gone: {err}"
    );
    // No half-registered schema survives the failed load.
    assert!(ex.execute("SELECT * FROM aux.t").is_err());
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
}

#[test]
fn test_attach_duplicate_and_reserved_names_rejected() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    for (sql, expected) in [
        ("ATTACH ':memory:' AS aux", "database aux is already in use"),
        ("ATTACH ':memory:' AS AUX", "database AUX is already in use"),
        ("ATTACH ':memory:' AS main", "database main is already in use"),
        ("ATTACH ':memory:' AS temp", "database temp is already in use"),
    ] {
        let err = ex.execute(sql).unwrap_err();
        assert_eq!(err.to_string(), expected, "for {sql}");
    }
}

#[test]
fn test_attach_max_limit() {
    let mut ex = attach_test_executor();
    for i in 0..10 {
        ex.execute(&format!("ATTACH ':memory:' AS db{i}")).unwrap();
    }
    let err = ex.execute("ATTACH ':memory:' AS one_more").unwrap_err();
    assert_eq!(err.to_string(), "too many attached databases - max 10");
}

#[test]
fn test_detach_removes_schema_and_reattach_works() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
    ex.execute("DETACH aux").unwrap();

    // Subsequent references fail.
    assert!(ex.execute("SELECT * FROM aux.t").is_err());
    assert!(ex.execute("INSERT INTO aux.t VALUES (2)").is_err());

    // Re-attach after detach works and starts empty.
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    assert!(ex.execute("SELECT * FROM aux.t").is_err(), "re-attached schema must be empty");
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
}

#[test]
fn test_detach_unknown_database_errors() {
    let mut ex = attach_test_executor();
    let err = ex.execute("DETACH nosuch").unwrap_err();
    assert_eq!(err.to_string(), "no such database: nosuch");
    // DETACH DATABASE noise word accepted too.
    let err = ex.execute("DETACH DATABASE nosuch").unwrap_err();
    assert_eq!(err.to_string(), "no such database: nosuch");
}

#[test]
fn test_attach_detach_rejected_inside_transaction() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS pre").unwrap();
    ex.execute("BEGIN").unwrap();
    let err = ex.execute("ATTACH ':memory:' AS aux").unwrap_err();
    assert_eq!(err.to_string(), "cannot ATTACH database within transaction");
    let err = ex.execute("DETACH pre").unwrap_err();
    assert_eq!(err.to_string(), "cannot DETACH database within transaction");
    ex.execute("COMMIT").unwrap();
    // Both work again outside the transaction.
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("DETACH pre").unwrap();
}

#[test]
fn test_pragma_database_list_enumerates_attachments() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS a1").unwrap();
    ex.execute("ATTACH 'somefile.db' AS a2").unwrap();

    let result = ex.execute("PRAGMA database_list").unwrap();
    assert_eq!(result.columns, vec!["seq", "name", "file"]);
    // main (seq 0) + two attachments starting at seq 2 (no temp objects yet).
    assert_eq!(
        result.rows,
        vec![
            vec![Some("0".to_string()), Some("main".to_string()), Some(String::new())],
            vec![Some("2".to_string()), Some("a1".to_string()), Some(String::new())],
            vec![Some("3".to_string()), Some("a2".to_string()), Some("somefile.db".to_string())],
        ]
    );

    // Detach shifts the remaining attachment's seq.
    ex.execute("DETACH a1").unwrap();
    let result = ex.execute("PRAGMA database_list").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Some("0".to_string()), Some("main".to_string()), Some(String::new())],
            vec![Some("2".to_string()), Some("a2".to_string()), Some("somefile.db".to_string())],
        ]
    );
}

#[test]
fn test_unqualified_resolution_order_temp_main_attached() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS a1").unwrap();
    ex.execute("ATTACH ':memory:' AS a2").unwrap();

    // Table only in attached schemas: attach order decides (a1 wins).
    ex.execute("CREATE TABLE a1.s(x INTEGER)").unwrap();
    ex.execute("CREATE TABLE a2.s(x INTEGER)").unwrap();
    ex.execute("INSERT INTO a1.s VALUES (1)").unwrap();
    ex.execute("INSERT INTO a2.s VALUES (2)").unwrap();
    let result = ex.execute("SELECT x FROM s").unwrap();
    assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);

    // main shadows attached.
    ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO t VALUES (0)").unwrap();
    ex.execute("CREATE TABLE a1.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO a1.t VALUES (5)").unwrap();
    let result = ex.execute("SELECT x FROM t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("0".to_string())]]);

    // temp shadows main (and attached).
    ex.execute("CREATE TEMP TABLE t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO temp.t VALUES (99)").unwrap();
    let result = ex.execute("SELECT x FROM t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("99".to_string())]]);
}

#[test]
fn test_attach_names_case_insensitive() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS AuxDB").unwrap();
    ex.execute("CREATE TABLE auxdb.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO AUXDB.t VALUES (3)").unwrap();
    let result = ex.execute("SELECT x FROM \"AuxDB\".t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("3".to_string())]]);
    ex.execute("DETACH \"AUXDB\"").unwrap();
    assert!(ex.execute("SELECT x FROM auxdb.t").is_err());
}

#[test]
fn test_attached_qualified_ddl_and_drop_forms() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("CREATE TABLE aux.t(z INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();

    // Index on the attached table (index follows the table's schema).
    ex.execute("CREATE INDEX i1 ON t(z)").unwrap();

    // Qualified view + trigger.
    ex.execute("CREATE VIEW aux.v1 AS SELECT z FROM t").unwrap();
    let result = ex.execute("SELECT * FROM aux.v1").unwrap();
    assert_eq!(result.rows, vec![vec![Some("7".to_string())]]);
    ex.execute("CREATE TRIGGER aux.tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (8)").unwrap();

    // Corresponding DROP forms.
    ex.execute("DROP TRIGGER aux.tr1").unwrap();
    ex.execute("DROP VIEW aux.v1").unwrap();
    ex.execute("DROP INDEX i1").unwrap();
    ex.execute("DROP TABLE aux.t").unwrap();
    ex.execute("DETACH aux").unwrap();
}

#[test]
fn test_qualified_drop_trigger_is_schema_scoped() {
    let mut ex = attach_test_executor();
    ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
    ex.execute("CREATE TRIGGER tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();

    // Wrong-schema qualified drop does not remove the main trigger.
    let err = ex.execute("DROP TRIGGER temp.tr1").unwrap_err();
    assert!(err.to_string().contains("tr1"), "got: {err}");
    // Unknown database qualifier errors with SQLite wording.
    let err = ex.execute("DROP TRIGGER nosuch.tr1").unwrap_err();
    assert_eq!(err.to_string(), "unknown database nosuch");

    // main-qualified drop removes it.
    ex.execute("DROP TRIGGER main.tr1").unwrap();
    assert!(ex.execute("DROP TRIGGER tr1").is_err(), "trigger should be gone");
}

#[test]
fn test_create_trigger_unknown_database_errors_at_execution() {
    let mut ex = attach_test_executor();
    ex.execute("CREATE TABLE t1(x INTEGER)").unwrap();
    let err = ex
        .execute("CREATE TRIGGER temporary.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END")
        .unwrap_err();
    assert_eq!(err.to_string(), "unknown database temporary");
    // An arbitrary unknown qualifier errors the same way…
    let err =
        ex.execute("CREATE TRIGGER auxdb.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END").unwrap_err();
    assert_eq!(err.to_string(), "unknown database auxdb");
    // …and succeeds once a database of that name is attached.
    ex.execute("ATTACH ':memory:' AS auxdb").unwrap();
    ex.execute("CREATE TABLE auxdb.t1(x INTEGER)").unwrap();
    ex.execute("CREATE TRIGGER auxdb.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END").unwrap();
}

#[test]
fn test_detach_cleans_up_views_triggers_indexes() {
    let mut ex = attach_test_executor();
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("CREATE TABLE aux.t(z INTEGER)").unwrap();
    ex.execute("CREATE INDEX iz ON t(z)").unwrap();
    ex.execute("CREATE VIEW aux.v1 AS SELECT z FROM t").unwrap();
    ex.execute("CREATE TRIGGER aux.tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
    ex.execute("DETACH aux").unwrap();

    assert!(ex.execute("SELECT * FROM aux.v1").is_err());
    assert!(ex.execute("SELECT * FROM aux.t").is_err());

    // Re-attaching gives a clean schema — the old objects are gone.
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
    ex.execute("CREATE TABLE aux.t(z INTEGER)").unwrap();
    ex.execute("CREATE VIEW aux.v1 AS SELECT z FROM t").unwrap();
    ex.execute("CREATE TRIGGER aux.tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
}

#[test]
fn test_attached_schema_not_persisted_to_main_snapshot() {
    // ATTACH is session-scoped in Phase 1: saving the main database must not
    // capture attached schemas or their objects, and a fresh session on the
    // same file must reopen without them.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("main.vbsql");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(db_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE keep(x INTEGER)").unwrap();
        ex.execute("INSERT INTO keep VALUES (42)").unwrap();
        ex.execute("ATTACH ':memory:' AS aux").unwrap();
        ex.execute("CREATE TABLE aux.gone(y INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.gone VALUES (1)").unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT y FROM gone").unwrap();
        ex.save_database(&db_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(db_path_str.clone())).unwrap();
        // Main data survived.
        let result = ex.execute("SELECT x FROM keep").unwrap();
        assert_eq!(result.rows, vec![vec![Some("42".to_string())]]);
        // Attached schema and its objects did not.
        assert!(ex.execute("SELECT y FROM aux.gone").is_err());
        assert!(ex.execute("SELECT * FROM aux.v1").is_err());
        // The name is free to attach again.
        ex.execute("ATTACH ':memory:' AS aux").unwrap();
    }
}

#[test]
fn test_attached_table_index_not_persisted_to_main_snapshot() {
    // Regression test for the Judge-reported #6310 leak: `CREATE INDEX i1 ON
    // t(z)` with an *unqualified* table target that resolves to an attached
    // table (`aux.t`) stores the bare `"t"` as the index's table_name. The
    // persistence filters must key off the index's owning schema — not a
    // qualifier embedded in table_name — or the index leaks into the binary
    // checkpoint and the main database refuses to open in the next session
    // ("Failed to create index: Table 't' not found").
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("main_idx.vbsql");
    let db_path_str = db_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(db_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE keep(x INTEGER)").unwrap();
        ex.execute("INSERT INTO keep VALUES (42)").unwrap();
        ex.execute("ATTACH ':memory:' AS aux").unwrap();
        // Attached-only table: the unqualified index target below can only
        // resolve to aux.t via the attached fallback.
        ex.execute("CREATE TABLE aux.t(z INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
        ex.execute("CREATE INDEX i1 ON t(z)").unwrap();
        ex.save_database(&db_path_str).unwrap();
    }

    {
        // The main database must reopen cleanly — an unopenable database here
        // is exactly the reported bug.
        let mut ex = SqlExecutor::new(Some(db_path_str.clone())).unwrap();
        // Main data survived.
        let result = ex.execute("SELECT x FROM keep").unwrap();
        assert_eq!(result.rows, vec![vec![Some("42".to_string())]]);
        // No attached-schema artifacts survived: the attached table is gone…
        assert!(ex.execute("SELECT z FROM aux.t").is_err());
        // …and the leaked index name is free for reuse in main.
        ex.execute("CREATE TABLE t_main(z INTEGER)").unwrap();
        ex.execute("CREATE INDEX i1 ON t_main(z)").unwrap();
        // The name is free to attach again.
        ex.execute("ATTACH ':memory:' AS aux").unwrap();
    }
}

// ============================================================================
// ATTACH DATABASE / DETACH DATABASE (#6362, Phase 2 — file-backed load/persist)
// ============================================================================

#[test]
fn test_attach_save_exit_reattach_round_trip_with_own_file() {
    // Core Phase 2 acceptance scenario: session A attaches a real file,
    // creates and populates a table, and exits cleanly; a fresh session B
    // attaches the same file and reads the row back.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        // Session A.
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE keep(x INTEGER)").unwrap();
        ex.execute("INSERT INTO keep VALUES (42)").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
        // Clean exit: the main database and every file-backed attachment
        // are saved.
        ex.save_database(&main_path_str).unwrap();
    }

    assert!(aux_path.exists(), "clean exit must have written the attached file");

    {
        // Session B: a fresh executor, both files reopened independently.
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        let result = ex.execute("SELECT x FROM keep").unwrap();
        assert_eq!(result.rows, vec![vec![Some("42".to_string())]]);
        // aux isn't attached yet in this fresh session.
        assert!(ex.execute("SELECT * FROM aux.t").is_err());

        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        let result = ex.execute("SELECT x FROM aux.t").unwrap();
        assert_eq!(result.rows, vec![vec![Some("1".to_string())]]);
    }

    // No cross-contamination in either direction: the aux file must not
    // contain main's table, and the main file must not contain aux's.
    let aux_contents = std::fs::read_to_string(&aux_path).unwrap();
    assert!(!aux_contents.to_lowercase().contains("keep"), "aux file leaked main's table");
    let main_contents = std::fs::read_to_string(&main_path).unwrap();
    assert!(!main_contents.to_lowercase().contains("aux"), "main file leaked the attachment");
}

#[test]
fn test_attach_reattach_typeless_column_reports_empty_type() {
    // Issue #6481: a column with NO declared type (`CREATE TABLE
    // aux.t2(d, e, f)`) must still report an empty declared type from
    // `PRAGMA table_info`, not "BLOB", after the attached schema's DDL
    // round-trips through save/reload across a fresh session — exactly like
    // `test_pragma_table_info_typeless_column_reports_empty_type` above
    // already guards for the *main* schema. Before the fix, the attached
    // schema's on-disk DDL reconstruction (`write_create_table_ddl`) always
    // emitted a concrete type token, permanently losing the "no declared
    // type" distinction for every column that didn't have one.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        // Session A: create the attached table with a mix of typeless and
        // explicitly-typed columns, then exit cleanly (writes the attached
        // file via `save_attached_schema_sql_dump`).
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t2(d, e TEXT, f)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    assert!(aux_path.exists(), "clean exit must have written the attached file");

    // The on-disk attached-schema dump must never contain a bare "BLOB" type
    // token for the typeless columns — assert this directly on the file
    // contents so a regression is caught even if a future PRAGMA change
    // masked it at the query layer.
    let aux_contents = std::fs::read_to_string(&aux_path).unwrap();
    assert!(
        !aux_contents.to_uppercase().contains("BLOB"),
        "attached-schema dump must not fabricate a BLOB type for typeless columns, got:\n{aux_contents}"
    );

    {
        // Session B: a fresh executor, re-attaching the same file — the
        // process boundary the bug is specifically tied to.
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

        let result = ex.execute("PRAGMA table_info(t2)").unwrap();
        assert_eq!(result.row_count, 3);
        // Columns are cid, name, type, notnull, dflt_value, pk (type is index 2).
        assert_eq!(result.rows[0][2].as_deref(), Some(""), "typeless column d -> empty type");
        // The attached-schema DDL reconstruction (`write_create_table_ddl`)
        // normalizes an explicitly-typed column through `format_column_type`
        // rather than preserving the original spelling — `TEXT` (Varchar
        // affinity with no length) round-trips as `VARCHAR`. That
        // normalization is pre-existing and independent of #6481's fix
        // (which only changes the *typeless* branch below it); this
        // assertion just pins that a typed column still gets a concrete,
        // non-empty type, not that its spelling is preserved verbatim.
        assert_eq!(
            result.rows[1][2].as_deref(),
            Some("VARCHAR"),
            "typed column e keeps a concrete type"
        );
        assert_eq!(result.rows[2][2].as_deref(), Some(""), "typeless column f -> empty type");
    }
}

#[test]
fn test_attach_reattach_explicit_blob_and_any_columns_still_round_trip_as_blob() {
    // Regression guard alongside #6481's typeless-column fix: an explicit
    // `BLOB` declaration (`DataType::BinaryLargeObject` — the same internal
    // representation a *typeless* column also collapses to) must NOT be
    // reclassified as typeless and must still report "BLOB" from `PRAGMA
    // table_info` after an attached-schema save/reattach round trip.
    //
    // `ANY` is included as a second explicitly-typed column for the same
    // "must not go empty" guard, but it is a distinct case: `ANY` is not
    // special-cased to `BinaryLargeObject` in the parser (SQLite gives it
    // NUMERIC affinity, not BLOB affinity — see
    // `crates/vibesql-parser/src/parser/create/types.rs`, #6191) and instead
    // parses to `DataType::UserDefined { type_name: "any" }`, which
    // round-trips verbatim (lowercased, matching the lexer's identifier
    // normalization) rather than through the BLOB-affinity formatting path.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t3(a BLOB, b ANY)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

        let result = ex.execute("PRAGMA table_info(t3)").unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0][2].as_deref(), Some("BLOB"), "explicit BLOB must round-trip");
        assert_eq!(result.rows[1][2].as_deref(), Some("any"), "explicit ANY must not go empty");
    }
}

#[test]
fn test_detach_flushes_pending_state_before_removing_schema() {
    // DETACH itself must persist the attached schema's data before removing
    // it — without a prior explicit `\save`, the data must still survive.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aux_detach.vbsql");
    let path_str = path.to_str().unwrap().to_string();

    let mut ex = attach_test_executor();
    ex.execute(&format!("ATTACH '{}' AS aux", path_str)).unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (11)").unwrap();
    // No explicit save_database call: DETACH itself must flush.
    ex.execute("DETACH aux").unwrap();

    assert!(path.exists(), "DETACH must have written the attached file");

    // Re-attach (same session) and confirm the data survived the flush.
    ex.execute(&format!("ATTACH '{}' AS aux", path_str)).unwrap();
    let result = ex.execute("SELECT x FROM aux.t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("11".to_string())]]);
}

#[test]
fn test_attach_newer_format_version_is_hard_error() {
    // Attaching a file written by a newer VibeSQL binary must hard-error via
    // the existing recovery failure policy — never silently present an empty
    // schema (see CLAUDE.md "Recovery failure policy").
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("future.vbsql");
    let path_str = path.to_str().unwrap().to_string();

    {
        // `save_binary` (not `save`, which defaults to zstd-compressed
        // output via the `compression` feature) so the on-disk file starts
        // with the uncompressed 16-byte header (5-byte "VBSQL" magic + 1-byte
        // version) that the byte patch below targets.
        let mut builder = SqlExecutor::new(None).unwrap();
        builder.execute("CREATE TABLE t(x INTEGER)").unwrap();
        builder.db.save_binary(&path_str).unwrap();
    }
    // Patch the format-version byte (offset 5, right after the 5-byte magic)
    // to simulate a file written by a newer VibeSQL binary — mirrors
    // `persistence::binary::format`'s own
    // `test_read_header_forward_version_is_typed_error`.
    {
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[5] = bytes[5].wrapping_add(1);
        std::fs::write(&path, bytes).unwrap();
    }

    let mut ex = attach_test_executor();
    let err = ex.execute(&format!("ATTACH '{}' AS aux", path_str)).unwrap_err();
    assert!(err.to_string().contains("newer version of VibeSQL"), "got: {err}");
    // Rolled back cleanly: the name is free to attach again.
    ex.execute("ATTACH ':memory:' AS aux").unwrap();
}

#[test]
fn test_pragma_database_list_canonicalizes_existing_attached_file_path() {
    // A file-backed attachment that actually exists on disk reports its
    // canonicalized absolute path, matching the `main` precedent.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aux_canon.vbsql");
    let path_str = path.to_str().unwrap().to_string();
    {
        let mut ex = SqlExecutor::new(Some(path_str.clone())).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.save_database(&path_str).unwrap();
    }

    let mut ex = attach_test_executor();
    ex.execute(&format!("ATTACH '{}' AS aux", path_str)).unwrap();
    let result = ex.execute("PRAGMA database_list").unwrap();
    let expected = std::fs::canonicalize(&path).unwrap().to_str().unwrap().to_string();
    assert_eq!(
        result.rows,
        vec![
            vec![Some("0".to_string()), Some("main".to_string()), Some(String::new())],
            vec![Some("2".to_string()), Some("aux".to_string()), Some(expected)],
        ]
    );
}

// ============================================================================
// ATTACH DATABASE views/triggers/indexes round-trip (#6407)
// ============================================================================

#[test]
fn test_attach_reattach_round_trips_view() {
    // A view defined inside an attached schema — with its captured SQL text
    // referencing the attachment's own qualifier throughout (`aux.v1`,
    // `FROM t` resolving against `aux`) — must survive a clean exit and a
    // fresh session's re-attach of the same file, per the issue #6407
    // acceptance criteria.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_view.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (2)").unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT x FROM t").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        let result = ex.execute("SELECT x FROM aux.v1 ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
            "view must round-trip through a save/exit/reattach cycle"
        );
    }
}

#[test]
fn test_attach_reattach_round_trips_trigger() {
    // A trigger defined inside an attached schema must likewise round-trip
    // (issue #6407 acceptance criteria).
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_trigger.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.log(msg TEXT)").unwrap();
        ex.execute(
            "CREATE TRIGGER aux.tr1 AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES ('inserted'); END",
        )
        .unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        // Firing the trigger proves it round-tripped, not just that its
        // catalog entry exists.
        ex.execute("INSERT INTO aux.t VALUES (99)").unwrap();
        let result = ex.execute("SELECT msg FROM aux.log").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("inserted".to_string())]],
            "trigger must round-trip and fire after a save/exit/reattach cycle"
        );
    }
}

#[test]
fn test_attach_reattach_views_preserve_creation_order_fresh_session() {
    // Regression test for #6508: views created in a known order (v1, v2, v3)
    // must list back in that same order from `<alias>.sqlite_master` after a
    // fresh `ATTACH` in a brand-new session — not the non-deterministic order
    // a `HashMap`-backed enumeration would produce. Tables are the control
    // case (already correct via `IndexMap`); this pins the view analogue.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_view_order.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT 1").unwrap();
        ex.execute("CREATE VIEW aux.v2 AS SELECT 2").unwrap();
        ex.execute("CREATE VIEW aux.v3 AS SELECT 3").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    // Fresh session, same files on disk.
    let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    let result = ex.execute("SELECT name FROM aux.sqlite_master WHERE type='view'").unwrap();
    let names: Vec<Option<String>> = result.rows.into_iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        names,
        vec![Some("v1".to_string()), Some("v2".to_string()), Some("v3".to_string())],
        "views must list back in creation order after a fresh re-attach"
    );
}

#[test]
fn test_attach_detach_reattach_views_preserve_creation_order_same_session() {
    // Same guarantee as above, but exercised via DETACH/re-ATTACH within a
    // single session rather than a fresh process (#6508 acceptance criteria).
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_view_order_same_session.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    ex.execute("CREATE VIEW aux.v1 AS SELECT 1").unwrap();
    ex.execute("CREATE VIEW aux.v2 AS SELECT 2").unwrap();
    ex.execute("CREATE VIEW aux.v3 AS SELECT 3").unwrap();
    ex.execute("DETACH aux").unwrap();

    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    let result = ex.execute("SELECT name FROM aux.sqlite_master WHERE type='view'").unwrap();
    let names: Vec<Option<String>> = result.rows.into_iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        names,
        vec![Some("v1".to_string()), Some("v2".to_string()), Some("v3".to_string())],
        "views must list back in creation order after a same-session DETACH/re-ATTACH"
    );
}

#[test]
fn test_attach_reattach_triggers_preserve_creation_order_fresh_session() {
    // Regression test for #6508: same guarantee as the view test above, for
    // triggers created in a known order (tr1, tr2, tr3).
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_trigger_order.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(a INTEGER)").unwrap();
        ex.execute("CREATE TRIGGER aux.tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
        ex.execute("CREATE TRIGGER aux.tr2 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
        ex.execute("CREATE TRIGGER aux.tr3 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    // Fresh session, same files on disk.
    let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    let result = ex.execute("SELECT name FROM aux.sqlite_master WHERE type='trigger'").unwrap();
    let names: Vec<Option<String>> = result.rows.into_iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        names,
        vec![Some("tr1".to_string()), Some("tr2".to_string()), Some("tr3".to_string())],
        "triggers must list back in creation order after a fresh re-attach"
    );
}

#[test]
fn test_attach_detach_reattach_triggers_preserve_creation_order_same_session() {
    // Same guarantee as above, but exercised via DETACH/re-ATTACH within a
    // single session rather than a fresh process (#6508 acceptance criteria).
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_trigger_order_same_session.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    ex.execute("CREATE TABLE aux.t(a INTEGER)").unwrap();
    ex.execute("CREATE TRIGGER aux.tr1 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
    ex.execute("CREATE TRIGGER aux.tr2 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
    ex.execute("CREATE TRIGGER aux.tr3 AFTER INSERT ON t BEGIN SELECT 1; END").unwrap();
    ex.execute("DETACH aux").unwrap();

    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    let result = ex.execute("SELECT name FROM aux.sqlite_master WHERE type='trigger'").unwrap();
    let names: Vec<Option<String>> = result.rows.into_iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        names,
        vec![Some("tr1".to_string()), Some("tr2".to_string()), Some("tr3".to_string())],
        "triggers must list back in creation order after a same-session DETACH/re-ATTACH"
    );
}

#[test]
fn test_attach_reattach_tables_preserve_creation_order_regression() {
    // Control case from #6508: tables were already correct (re-homed via the
    // order-preserving `IndexMap`-backed `Schema.tables`). Pinned explicitly
    // here — per the issue's acceptance criteria — so a future change cannot
    // silently regress table ordering while "fixing" views/triggers.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_table_order.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t1(a INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.t2(a INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.t3(a INTEGER)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    let result = ex.execute("SELECT name FROM aux.sqlite_master WHERE type='table'").unwrap();
    let names: Vec<Option<String>> = result.rows.into_iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        names,
        vec![Some("t1".to_string()), Some("t2".to_string()), Some("t3".to_string())],
        "tables must list back in creation order after a fresh re-attach (control case)"
    );
}

#[test]
fn test_attach_reattach_view_binds_to_attached_schema_despite_main_collision() {
    // The round trip must preserve the view body's *schema binding*, not just
    // the view's existence (#6476 review). The writer persists the body
    // schema-relative, so an unqualified table reference in the reloaded body
    // would late-bind through `Catalog::get_table`'s temp → main → attached
    // search order and read `main`'s same-named table instead — returning
    // another database's rows with no error at all.
    //
    // Both spellings of the defining body must converge on the attachment:
    //   * `FROM aux.t` — explicitly qualified, unambiguous at definition time.
    //   * `FROM t`     — bare, resolved to `aux.t` in the defining session.
    // SQLite's rule (an unqualified name in a view body resolves within the
    // schema containing the view) makes both mean `aux.t` forever after.
    for body in ["SELECT x FROM aux.t", "SELECT x FROM t"] {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main.vbsql");
        let main_path_str = main_path.to_str().unwrap().to_string();
        let aux_path = dir.path().join("aux_view_collision.vbsql");
        let aux_path_str = aux_path.to_str().unwrap().to_string();

        {
            let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
            ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
            ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
            ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
            ex.execute("INSERT INTO aux.t VALUES (2)").unwrap();
            ex.execute(&format!("CREATE VIEW aux.v1 AS {}", body)).unwrap();

            let before = ex.execute("SELECT x FROM aux.v1 ORDER BY x").unwrap();
            assert_eq!(
                before.rows,
                vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
                "sanity: `{}` must read the attachment in the defining session",
                body
            );
            ex.save_database(&main_path_str).unwrap();
        }

        {
            let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
            // The collision that every other round-trip test is missing: a
            // table in `main` with the same bare name as the attachment's.
            // Without it there is nothing for a mis-bound body to resolve to,
            // so the wrong binding is unobservable.
            ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
            ex.execute("INSERT INTO t VALUES (999)").unwrap();
            ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

            let result = ex.execute("SELECT x FROM aux.v1 ORDER BY x").unwrap();
            assert_eq!(
                result.rows,
                vec![vec![Some("1".to_string())], vec![Some("2".to_string())]],
                "view body `{}` must still read aux.t after reload, not main.t",
                body
            );

            // And the collision itself still resolves normally either way.
            let main_rows = ex.execute("SELECT x FROM t").unwrap();
            assert_eq!(main_rows.rows, vec![vec![Some("999".to_string())]]);
        }
    }
}

#[test]
fn test_attach_reattach_view_binds_to_attached_schema_under_a_different_alias() {
    // The binding must follow the alias *this* session attached under, not
    // the one the file was saved with — the whole reason the writer persists
    // schema-relative in the first place. With a same-named `main.t` present,
    // a body that failed to re-qualify would silently read main's rows.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_alias_collision.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT x FROM aux.t").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO t VALUES (999)").unwrap();
        // Saved as `aux`, reloaded as `other`.
        ex.execute(&format!("ATTACH '{}' AS other", aux_path_str)).unwrap();

        let result = ex.execute("SELECT x FROM other.v1").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("7".to_string())]],
            "view body must re-bind to the *current* alias's schema, not main"
        );

        // The save-time alias must not leak back into the live session.
        assert!(
            ex.execute("SELECT x FROM aux.v1").is_err(),
            "the save-time alias `aux` must not be resolvable in this session"
        );
    }
}

#[test]
fn test_attach_reattach_view_preserves_an_explicit_main_reference() {
    // `strip_schema_qualifier` only removes the attachment's *own* qualifier,
    // so an explicit `main.` reference survives into the persisted body. The
    // reader must leave it alone: bare means "this attachment", qualified
    // means "that schema". Rewriting it to `aux.` would be the mirror image
    // of the bug this whole path exists to prevent.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_main_ref.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO t VALUES (999)").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
        ex.execute("CREATE VIEW aux.vmain AS SELECT x FROM main.t").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        let result = ex.execute("SELECT x FROM aux.vmain").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("999".to_string())]],
            "an explicit main.-qualified reference must keep pointing at main"
        );
    }
}

#[test]
fn test_attach_reattach_view_binds_a_joined_body_to_the_attached_schema() {
    // A multi-table body exercises the recursion (both join arms) and the
    // aliased column qualifiers that survive the rewrite: `a.x`/`b.y` must
    // still bind after `t`/`u` become `aux.t`/`aux.u`.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_join.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.u(x INTEGER, y TEXT)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (1)").unwrap();
        ex.execute("INSERT INTO aux.u VALUES (1, 'aux-row')").unwrap();
        ex.execute("CREATE VIEW aux.vj AS SELECT b.y FROM aux.t AS a JOIN aux.u AS b ON a.x = b.x")
            .unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE u(x INTEGER, y TEXT)").unwrap();
        ex.execute("INSERT INTO t VALUES (1)").unwrap();
        ex.execute("INSERT INTO u VALUES (1, 'main-row')").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

        let result = ex.execute("SELECT y FROM aux.vj").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("aux-row".to_string())]],
            "both join arms must re-bind to the attached schema, not main"
        );
    }
}

#[test]
fn test_attach_reattach_trigger_body_binds_to_attachment_on_name_collision() {
    // Regression test for #6477: an unqualified table name inside a trigger
    // body must resolve within the trigger's *own* schema, never through the
    // connection-wide temp → main → attached search order.
    //
    // A trigger body is stored as `TriggerAction::RawSql` and re-parsed when
    // the trigger fires, and the parser rejects a qualified table name inside
    // a trigger body ("qualified table names are not allowed on INSERT,
    // UPDATE, and DELETE statements within triggers", matching SQLite). So
    // the view-body rewrite in `schema_qualify` has nothing to operate on
    // here — the fix instead scopes the executor's unqualified table-name
    // resolution to the firing trigger's own schema
    // (`Catalog::set_restrict_unqualified_resolution_to_schema`), so a
    // trigger owned by an attached schema `aux` writes to `aux.log`, not a
    // same-named `main.log`.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_trigger_collision.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.log(msg TEXT)").unwrap();
        ex.execute(
            "CREATE TRIGGER aux.tr1 AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES ('fired'); END",
        )
        .unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE log(msg TEXT)").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("INSERT INTO aux.t VALUES (5)").unwrap();

        let main_log = ex.execute("SELECT msg FROM main.log").unwrap();
        let aux_log = ex.execute("SELECT msg FROM aux.log").unwrap();

        assert!(
            main_log.rows.is_empty(),
            "#6477: main.log must stay empty — the trigger body's unqualified `log` must not \
             bind to a same-named table in main"
        );
        assert_eq!(
            aux_log.rows,
            vec![vec![Some("fired".to_string())]],
            "#6477: the attachment's own log must receive the row — the trigger body's \
             unqualified `log` must bind within its own (aux) schema"
        );
    }
}

#[test]
fn test_attach_trigger_body_binds_to_attachment_on_name_collision_single_session() {
    // Regression test for #6477: the defect is pre-existing and independent
    // of persistence — it reproduces in a single live session with no
    // save/reload at all, whenever `main` holds a table with the same name
    // as one in the attached schema. This is the exact repro from the issue
    // body's "Problem" section (single-session variant), kept separate from
    // the save/exit/re-attach variant above so both code paths stay pinned.
    let dir = tempfile::tempdir().unwrap();
    let aux_path = dir.path().join("aux_trigger_collision_live.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
    ex.execute("CREATE TABLE log(msg TEXT)").unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("CREATE TABLE aux.log(msg TEXT)").unwrap();
    ex.execute(
        "CREATE TRIGGER aux.tr1 AFTER INSERT ON t \
         BEGIN INSERT INTO log VALUES ('fired'); END",
    )
    .unwrap();
    ex.execute("INSERT INTO aux.t VALUES (5)").unwrap();

    let main_log = ex.execute("SELECT msg FROM main.log").unwrap();
    let aux_log = ex.execute("SELECT msg FROM aux.log").unwrap();

    assert!(
        main_log.rows.is_empty(),
        "#6477: main.log must stay empty in a single live session (no save/reload) — the \
         trigger body's unqualified `log` must not bind to a same-named table in main"
    );
    assert_eq!(
        aux_log.rows,
        vec![vec![Some("fired".to_string())]],
        "#6477: the attachment's own log must receive the row in a single live session — the \
         trigger body's unqualified `log` must bind within its own (aux) schema"
    );
}

#[test]
fn test_trigger_body_resolves_with_case_mismatched_main_schema_qualifier() {
    // Regression test for the case-sensitivity gap in #6477's trigger-body
    // resolution restriction: the parser stores a `CREATE TRIGGER` schema
    // qualifier verbatim as written (`MAIN`) because schema comparisons are
    // case-insensitive everywhere downstream, but the storage-layer mirror of
    // the restriction used it as a raw `"<schema>.<table>"` HashMap key, so
    // any case-varied qualifier turned a previously-working trigger into a
    // hard `Table 'MAIN.log' not found` error on every firing. Every spelling
    // of `main.` must fire and write into `main`.
    for qualifier in ["main", "MAIN", "MaIn"] {
        let mut ex = SqlExecutor::new(None).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE log(msg TEXT)").unwrap();
        ex.execute(&format!(
            "CREATE TRIGGER {}.tr AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES ('fired'); END",
            qualifier
        ))
        .unwrap();

        ex.execute("INSERT INTO t VALUES (1)").unwrap_or_else(|e| {
            panic!(
                "firing a trigger created as `{}.tr` must not error (case-varied schema \
                 qualifiers are valid, previously-working SQL): {}",
                qualifier, e
            )
        });

        assert_eq!(
            ex.execute("SELECT msg FROM main.log").unwrap().rows,
            vec![vec![Some("fired".to_string())]],
            "a trigger created as `{}.tr` must write into main.log when it fires",
            qualifier
        );
    }
}

#[test]
fn test_trigger_body_binds_to_attachment_with_case_mismatched_schema_qualifier() {
    // Companion to the `MAIN.` case above, for an ATTACHed schema: attachment
    // names are canonically lowercased (`store/attachments.rs`), so
    // `CREATE TRIGGER AUX.tr1 ...` against a database attached as `aux` must
    // still resolve — and, per #6477, must still bind the body's unqualified
    // `log` inside `aux`, never the same-named table in `main`.
    for qualifier in ["aux", "AUX"] {
        let dir = tempfile::tempdir().unwrap();
        let aux_path = dir.path().join("aux_trigger_qualifier_case.vbsql");
        let aux_path_str = aux_path.to_str().unwrap().to_string();

        let mut ex = SqlExecutor::new(None).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE log(msg TEXT)").unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.log(msg TEXT)").unwrap();
        ex.execute(&format!(
            "CREATE TRIGGER {}.tr1 AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES ('fired'); END",
            qualifier
        ))
        .unwrap();

        ex.execute("INSERT INTO aux.t VALUES (5)").unwrap_or_else(|e| {
            panic!(
                "firing a trigger created as `{}.tr1` must not error (the attachment is known \
                 as `aux` regardless of the qualifier's case): {}",
                qualifier, e
            )
        });

        assert!(
            ex.execute("SELECT msg FROM main.log").unwrap().rows.is_empty(),
            "#6477: main.log must stay empty for a trigger created as `{}.tr1`",
            qualifier
        );
        assert_eq!(
            ex.execute("SELECT msg FROM aux.log").unwrap().rows,
            vec![vec![Some("fired".to_string())]],
            "#6477: aux.log must receive the row for a trigger created as `{}.tr1`",
            qualifier
        );
    }
}

#[test]
fn test_attach_reattach_view_body_may_qualify_columns_with_the_bare_table_name() {
    // Regression guard for the re-qualification itself: a body written
    // `SELECT t.x FROM t` names its columns after the table's *unqualified*
    // name, and the executor does not match a bare column qualifier against a
    // schema-qualified table. Rewriting the FROM entry to `aux.t` without
    // keeping `t` as a correlation name turns a working view into
    // "Column 'x' not found (searched tables: aux.t)" — a fresh breakage
    // introduced by the fix rather than by the bug it fixes. Asserted with
    // and without a colliding `main.t` so neither the binding nor the
    // resolution can regress silently.
    for with_main_collision in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let main_path = dir.path().join("main.vbsql");
        let main_path_str = main_path.to_str().unwrap().to_string();
        let aux_path = dir.path().join("aux_bare_qualifier.vbsql");
        let aux_path_str = aux_path.to_str().unwrap().to_string();

        {
            let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
            ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
            ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
            ex.execute("INSERT INTO aux.t VALUES (5)").unwrap();
            ex.execute("CREATE VIEW aux.v AS SELECT t.x FROM t WHERE t.x > 0").unwrap();
            assert_eq!(
                ex.execute("SELECT x FROM aux.v").unwrap().rows,
                vec![vec![Some("5".to_string())]],
                "sanity: the body must work in the defining session"
            );
            ex.save_database(&main_path_str).unwrap();
        }

        {
            let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
            if with_main_collision {
                ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
                ex.execute("INSERT INTO t VALUES (999)").unwrap();
            }
            ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
            assert_eq!(
                ex.execute("SELECT x FROM aux.v").unwrap().rows,
                vec![vec![Some("5".to_string())]],
                "bare-table-name column qualifiers must survive re-qualification \
                 (main collision: {})",
                with_main_collision
            );
        }
    }
}

#[test]
fn test_attach_reattach_view_on_view_binds_to_the_attached_schema() {
    // A view whose body references *another view* in the same attachment is
    // rewritten by the same rule (`v1` -> `aux.v1`), so it exercises the
    // reader's view-creation ordering as well as the re-binding. With a
    // same-named `main.t` under the inner view, a mis-bound chain would
    // silently surface main's rows through two levels of indirection.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_view_on_view.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (3)").unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT x FROM aux.t").unwrap();
        ex.execute("CREATE VIEW aux.v2 AS SELECT x FROM aux.v1").unwrap();
        assert_eq!(
            ex.execute("SELECT x FROM aux.v2").unwrap().rows,
            vec![vec![Some("3".to_string())]],
            "sanity: the view chain must read the attachment in the defining session"
        );
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO t VALUES (999)").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        assert_eq!(
            ex.execute("SELECT x FROM aux.v2").unwrap().rows,
            vec![vec![Some("3".to_string())]],
            "a view-on-view chain must re-bind to the attached schema, not main"
        );
    }
}

#[test]
fn test_attach_reattach_round_trips_partial_and_expression_index() {
    // Partial (WHERE-predicate) and expression indexes on an attached
    // schema's table must round-trip — including the physical index body,
    // not just the catalog metadata (issue #6407 acceptance criteria).
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_index.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (-1)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (5)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (10)").unwrap();
        ex.execute("CREATE INDEX aux_idx ON t(x) WHERE x > 0").unwrap();
        ex.execute("CREATE INDEX aux_expr_idx ON t(abs(x))").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

        // Data itself is intact regardless of the index.
        let result = ex.execute("SELECT x FROM aux.t ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Some("-1".to_string())],
                vec![Some("5".to_string())],
                vec![Some("10".to_string())],
            ]
        );

        // The partial index's WHERE predicate survived in the catalog.
        let where_clause_present = ex
            .db
            .catalog
            .find_index_by_name("aux_idx")
            .and_then(|m| m.where_clause.as_ref())
            .is_some();
        assert!(where_clause_present, "partial index's WHERE predicate must survive the reload");

        // The physical index body was correctly rebuilt from the
        // now-populated attached table's live rows — not just its catalog
        // metadata. A stale/empty body (e.g. if the predicate were silently
        // dropped during rebuild) would either contain 0 or 3 entries; the
        // correct partial body contains exactly the 2 rows matching `x > 0`
        // (5 and 10, not -1). This is a more direct proof than EXPLAIN QUERY
        // PLAN, whose index-vs-scan choice is a cost-based decision that a
        // 3-row table may decline regardless of round-trip correctness.
        match ex.db.get_index_data("aux_idx") {
            Some(vibesql_storage::IndexData::InMemory { data }) => {
                let total_entries: usize = data.values().map(|rows| rows.len()).sum();
                assert_eq!(
                    total_entries, 2,
                    "partial index body must contain exactly the 2 rows matching \
                     x > 0 after rebuild, got: {:?}",
                    data
                );
            }
            other => panic!("expected an in-memory partial index body, got: {:?}", other),
        }

        // The expression index also round-tripped and returns correct data
        // when queried by its indexed expression.
        let result = ex.execute("SELECT x FROM aux.t WHERE abs(x) = 1").unwrap();
        assert_eq!(result.rows, vec![vec![Some("-1".to_string())]]);
    }
}

#[test]
fn test_attach_reattach_index_skipped_on_main_table_name_collision() {
    // The missing fixture axis for indexes: a *populated* `main` holding a
    // same-named table (the configuration the view tests already cover).
    //
    // The storage-side index body build binds by BARE table name, so with
    // `main.t` present the rebuild would resolve `t` to `main.t` instead of
    // `aux.t` (#6487). Here `main.t` has no `z` column at all, so the build
    // errors with `Column 'z' not found in table 't'`. That error must NOT
    // propagate: an attachment that merely *contains* an index would then be
    // impossible to re-`ATTACH`, which is strictly worse than pre-#6407
    // (where indexes were never persisted at all). The documented behavior is
    // that the `ATTACH` succeeds, all data is readable, and only the index is
    // missing.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_idx_collide.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x TEXT, z INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('a', 10)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('b', -1)").unwrap();
        ex.execute("CREATE INDEX aux_plain_idx ON t(z)").unwrap();
        ex.execute("CREATE INDEX aux_partial_idx ON t(x) WHERE z > 0").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        // A same-named table in `main`, WITHOUT the indexed `z` column.
        ex.execute("CREATE TABLE t(x TEXT)").unwrap();
        ex.execute("INSERT INTO t VALUES ('MAIN-ROW')").unwrap();

        // The ATTACH must succeed — this is the whole point of the guard.
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str))
            .expect("ATTACH of an index-bearing attachment must succeed despite a main collision");

        // The attachment's data is fully readable.
        let result = ex.execute("SELECT x, z FROM aux.t ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Some("a".to_string()), Some("10".to_string())],
                vec![Some("b".to_string()), Some("-1".to_string())],
            ]
        );

        // `main.t` is untouched: no index body was built from its rows, and
        // no stray `main`-schema index was registered under the aux name.
        let result = ex.execute("SELECT x FROM t").unwrap();
        assert_eq!(result.rows, vec![vec![Some("MAIN-ROW".to_string())]]);
        assert!(
            ex.db.get_index_data("aux_plain_idx").is_none(),
            "the shadowed index must be skipped entirely, not built against main.t"
        );
        assert!(
            ex.db.get_index_data("aux_partial_idx").is_none(),
            "the shadowed partial index must be skipped entirely, not built against main.t"
        );

        // Queries that could have used the missing index still return correct
        // results via a full scan.
        let result = ex.execute("SELECT x FROM aux.t WHERE z = 10").unwrap();
        assert_eq!(result.rows, vec![vec![Some("a".to_string())]]);
    }
}

#[test]
fn test_attach_reattach_index_skipped_on_main_collision_with_matching_column() {
    // The second half of the same fixture axis: `main.t` collides on name AND
    // happens to carry the indexed column, so the storage-side bare-name bind
    // (#6487) would NOT error — it would silently build the index body from
    // `main.t`'s rows and register it as a `main`-schema index. That is the
    // more dangerous branch (wrong data rather than a loud failure), so it is
    // pinned separately: the index must be skipped here too, and `main.t`
    // must be left without a phantom index.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_idx_collide2.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x TEXT, z INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('AUX-A', 10)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('AUX-B', 20)").unwrap();
        ex.execute("CREATE UNIQUE INDEX aux_uniq_idx ON t(z)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        // Same name AND a compatible `z` column — the silent-wrong-body case.
        ex.execute("CREATE TABLE t(x TEXT, z INTEGER)").unwrap();
        ex.execute("INSERT INTO t VALUES ('MAIN-A', 100)").unwrap();
        ex.execute("INSERT INTO t VALUES ('MAIN-B', 100)").unwrap();

        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str))
            .expect("ATTACH must succeed even when main.t is index-build compatible");

        // Both tables read back their own rows.
        let result = ex.execute("SELECT x FROM aux.t ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("AUX-A".to_string())], vec![Some("AUX-B".to_string())]]
        );
        let result = ex.execute("SELECT x FROM t ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("MAIN-A".to_string())], vec![Some("MAIN-B".to_string())]]
        );

        // No index body was built at all — in particular not one filled with
        // main.t's rows (which, being duplicate 100s, a UNIQUE index over
        // main.t could not even have represented).
        assert!(
            ex.db.get_index_data("aux_uniq_idx").is_none(),
            "the shadowed unique index must be skipped, not built against main.t"
        );

        // main.t keeps its duplicate `z` values: no phantom UNIQUE constraint
        // leaked onto it from the attachment's index.
        ex.execute("INSERT INTO t VALUES ('MAIN-C', 100)")
            .expect("main.t must not have acquired the attachment's UNIQUE index");
    }
}

#[test]
fn test_attach_reattach_index_rebuilds_when_main_has_a_differently_named_table() {
    // Control for the two collision tests above: the guard must be narrow. A
    // populated `main` that does NOT shadow the attachment's table name still
    // gets a fully rebuilt index body — the skip is keyed on name resolution,
    // not merely on "main is non-empty".
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_idx_nocollide.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x TEXT, z INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('a', 10)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('b', -1)").unwrap();
        ex.execute("CREATE INDEX aux_idx ON t(z) WHERE z > 0").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute("CREATE TABLE mt(x TEXT)").unwrap();
        ex.execute("INSERT INTO mt VALUES ('MAIN-ROW')").unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();

        let result = ex.execute("SELECT x FROM aux.t ORDER BY x").unwrap();
        assert_eq!(result.rows, vec![vec![Some("a".to_string())], vec![Some("b".to_string())]]);

        // The partial index body was rebuilt from aux.t's rows: exactly the
        // one row matching `z > 0`.
        match ex.db.get_index_data("aux_idx") {
            Some(vibesql_storage::IndexData::InMemory { data }) => {
                let total_entries: usize = data.values().map(|rows| rows.len()).sum();
                assert_eq!(
                    total_entries, 1,
                    "partial index body must contain exactly the 1 row matching z > 0, got: {:?}",
                    data
                );
            }
            other => panic!("expected a rebuilt in-memory partial index body, got: {:?}", other),
        }
    }
}

#[test]
fn test_attach_round_trips_view_and_trigger_under_a_different_alias() {
    // The on-disk attached-schema dump is *standalone*: the writer strips the
    // saving session's schema qualifier, and the loader re-qualifies with
    // whatever alias the new session attached under. So a file saved as `aux`
    // must reload correctly as `other` — this is the property that makes the
    // qualifier rewrite (rather than verbatim replay) necessary at all.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let side_path = dir.path().join("side.vbsql");
    let side_path_str = side_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", side_path_str)).unwrap();
        ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
        ex.execute("CREATE TABLE aux.log(msg TEXT)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES (-3)").unwrap();
        ex.execute("CREATE VIEW aux.v1 AS SELECT x FROM t").unwrap();
        ex.execute(
            "CREATE TRIGGER aux.tr1 AFTER INSERT ON t \
             BEGIN INSERT INTO log VALUES ('fired'); END",
        )
        .unwrap();
        ex.execute("CREATE INDEX aux_idx ON t(x) WHERE x > 0").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        // Deliberately a *different* alias than the one used at save time.
        ex.execute(&format!("ATTACH '{}' AS other", side_path_str)).unwrap();

        let result = ex.execute("SELECT x FROM other.v1 ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("-3".to_string())], vec![Some("7".to_string())]],
            "view must re-home under the new alias, not the saved one"
        );

        // The partial index body was rebuilt against the new alias's table:
        // exactly the one saved row matching `x > 0` (7, not -3). A dropped
        // predicate would give 2.
        //
        // Asserted *before* the trigger-firing INSERT below on purpose:
        // indexes on attached-schema tables are not maintained by DML at all
        // (a pre-existing gap that reproduces with no save/reload involved —
        // #6474), so checking afterwards would measure that bug rather than
        // this round-trip. Tighten to include post-reload DML once #6474 lands.
        match ex.db.get_index_data("aux_idx") {
            Some(vibesql_storage::IndexData::InMemory { data }) => {
                let total_entries: usize = data.values().map(|rows| rows.len()).sum();
                assert_eq!(
                    total_entries, 1,
                    "partial index must be rebuilt under the new alias with its \
                     WHERE predicate intact, got: {:?}",
                    data
                );
            }
            other => panic!("expected an in-memory partial index body, got: {:?}", other),
        }

        ex.execute("INSERT INTO other.t VALUES (8)").unwrap();
        let result = ex.execute("SELECT msg FROM other.log").unwrap();
        assert_eq!(
            result.rows,
            vec![vec![Some("fired".to_string())]],
            "trigger must re-home under the new alias and still fire"
        );

        // The saved alias must NOT leak back into the live session.
        assert!(
            ex.execute("SELECT x FROM aux.v1").is_err(),
            "the save-time alias `aux` must not resolve in a session that \
             attached the file as `other`"
        );
    }
}

// ----------------------------------------------------------------------------
// Auto-generated indexes must never reach an attached schema's dump (#6476
// review). `IndexManager::list_indexes()` yields storage *map keys*, and
// `make_index_key` prefixes every non-`main` schema onto the key — so an
// attached table's implicit index is keyed `aux.sqlite_autoindex_t_1` /
// `aux._withoutrowidinternalpk_t`, and a `starts_with(...)` test against the
// key silently fails to exclude exactly the indexes the filter exists for.
// `CREATE TABLE` recreates those indexes on reload anyway, and their names are
// reserved, so emitting them turns the attachment into a write-then-cannot-
// reopen file:
//
//     object name reserved for internal use: sqlite_autoindex_t_1
//
// Every other round-trip test above uses a bare `x INTEGER` column — and an
// `INTEGER PRIMARY KEY` is a rowid alias that gets no implicit index — which is
// why the whole suite stayed green while a `TEXT PRIMARY KEY`, a `UNIQUE`
// constraint, or `WITHOUT ROWID` broke. The two tests below are that missing
// axis; they must filter on the metadata's own bare `index_name`, never the
// map key, to pass.
// ----------------------------------------------------------------------------

#[test]
fn test_attach_reattach_round_trips_a_non_rowid_primary_key_and_unique_table() {
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_pk.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        // `x TEXT PRIMARY KEY` is NOT a rowid alias, so it gets an implicit
        // `sqlite_autoindex_t_1`; the `UNIQUE` column adds `..._t_2`.
        ex.execute("CREATE TABLE aux.t(x TEXT PRIMARY KEY, y INTEGER UNIQUE, z INTEGER)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('a', 1, 10)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('b', 2, 20)").unwrap();
        // A genuine user index alongside them: the fix must exclude only the
        // auto-generated ones, not stop emitting indexes altogether.
        ex.execute("CREATE INDEX aux_user_idx ON t(z)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        // The primary regression: the persisted file must still be openable.
        // Before the fix this fails here with
        // `object name reserved for internal use: sqlite_autoindex_t_1`.
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        let result = ex.execute("SELECT x, y, z FROM aux.t ORDER BY x").unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Some("a".to_string()), Some("1".to_string()), Some("10".to_string())],
                vec![Some("b".to_string()), Some("2".to_string()), Some("20".to_string())],
            ]
        );
        // The constraints themselves survived (they came back via CREATE
        // TABLE, which is why re-emitting their indexes was redundant).
        assert!(
            ex.execute("INSERT INTO aux.t VALUES ('a', 3, 30)").is_err(),
            "the PRIMARY KEY constraint must still be enforced after re-attach"
        );
        assert!(
            ex.execute("INSERT INTO aux.t VALUES ('c', 1, 30)").is_err(),
            "the UNIQUE constraint must still be enforced after re-attach"
        );
    }

    // …and the dump is clean at the source, so the re-attach above is not
    // merely surviving a tolerated duplicate.
    let dump = std::fs::read_to_string(&aux_path).unwrap();
    assert!(
        !dump.to_lowercase().contains("sqlite_autoindex_"),
        "the attached dump must not emit constraint-generated indexes \
         (CREATE TABLE recreates them, and the name is reserved); got:\n{dump}"
    );
    assert!(
        dump.contains("aux_user_idx"),
        "the user-defined index must still be emitted; got:\n{dump}"
    );
}

#[test]
fn test_attach_reattach_round_trips_a_without_rowid_table() {
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql");
    let main_path_str = main_path.to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux_wor.vbsql");
    let aux_path_str = aux_path.to_str().unwrap().to_string();

    {
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        // A WITHOUT ROWID table's PK index is keyed
        // `aux._withoutrowidinternalpk_t` (#5882) and slips the same filter.
        ex.execute("CREATE TABLE aux.t(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('k1', 1)").unwrap();
        ex.execute("INSERT INTO aux.t VALUES ('k2', 2)").unwrap();
        ex.save_database(&main_path_str).unwrap();
    }

    {
        // Same primary regression as above, via the `_withoutrowidinternalpk_`
        // prefix rather than `sqlite_autoindex_`.
        let mut ex = SqlExecutor::new(Some(main_path_str.clone())).unwrap();
        ex.execute(&format!("ATTACH '{}' AS aux", aux_path_str)).unwrap();
        let result = ex.execute("SELECT k, v FROM aux.t ORDER BY k").unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![Some("k1".to_string()), Some("1".to_string())],
                vec![Some("k2".to_string()), Some("2".to_string())],
            ]
        );
        assert!(
            ex.execute("INSERT INTO aux.t VALUES ('k1', 9)").is_err(),
            "the WITHOUT ROWID PRIMARY KEY must still be enforced after re-attach"
        );
    }

    let dump = std::fs::read_to_string(&aux_path).unwrap();
    assert!(
        !dump.to_lowercase().contains(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX),
        "the attached dump must not emit the WITHOUT ROWID internal PK index \
         (it is regenerated from the CREATE TABLE DDL); got:\n{dump}"
    );
    assert!(
        !dump.to_lowercase().contains("sqlite_autoindex_"),
        "the attached dump must not emit constraint-generated indexes; got:\n{dump}"
    );
    assert!(
        dump.to_uppercase().contains("WITHOUT ROWID"),
        "the WITHOUT ROWID clause itself must survive; got:\n{dump}"
    );
}

// ============================================================================
// Index maintenance on ATTACHed-schema tables (#6474)
//
// An index on an attached schema's table used to be built once by CREATE
// INDEX and then never maintained: schema-qualified DML (`INSERT INTO aux.t`)
// reached the storage index-maintenance code as `"aux.t"` while the index's
// own metadata recorded a bare `table_name = "t"` (with `schema = "aux"`
// alongside), so the name compare never matched and every INSERT / UPDATE /
// DELETE silently left the index body stale.
// ============================================================================

/// Total number of `(key -> row) `entries in an in-memory index body.
fn index_entry_count(ex: &SqlExecutor, index_name: &str) -> usize {
    match ex.db.get_index_data(index_name) {
        Some(vibesql_storage::IndexData::InMemory { data }) => {
            data.values().map(|rows| rows.len()).sum()
        }
        other => panic!("expected an in-memory index body for '{}', got: {:?}", index_name, other),
    }
}

/// Number of distinct keys in an in-memory index body.
fn index_key_count(ex: &SqlExecutor, index_name: &str) -> usize {
    match ex.db.get_index_data(index_name) {
        Some(vibesql_storage::IndexData::InMemory { data }) => data.len(),
        other => panic!("expected an in-memory index body for '{}', got: {:?}", index_name, other),
    }
}

/// `true` when the index body holds a key equal to the single numeric `value`.
///
/// Keys are normalized for comparison at maintenance time, so the stored
/// representation is matched structurally rather than by exact `SqlValue`
/// variant.
fn index_has_numeric_key(ex: &SqlExecutor, index_name: &str, value: f64) -> bool {
    match ex.db.get_index_data(index_name) {
        Some(vibesql_storage::IndexData::InMemory { data }) => data.keys().any(|key| {
            key.len() == 1
                && match &key[0] {
                    vibesql_types::SqlValue::Integer(n) => *n as f64 == value,
                    vibesql_types::SqlValue::Bigint(n) => *n as f64 == value,
                    vibesql_types::SqlValue::Smallint(n) => *n as f64 == value,
                    vibesql_types::SqlValue::Double(f) => *f == value,
                    vibesql_types::SqlValue::Real(f) => *f == value,
                    vibesql_types::SqlValue::Float(f) => f64::from(*f) == value,
                    vibesql_types::SqlValue::Numeric(f) => *f == value,
                    _ => false,
                }
        }),
        other => panic!("expected an in-memory index body for '{}', got: {:?}", index_name, other),
    }
}

/// Build a fresh main database with `aux` attached and an indexed `aux.t`.
///
/// Returns the executor plus the `TempDir` that owns both files (the caller
/// must keep it alive for the duration of the test).
fn attached_schema_with_indexes() -> (SqlExecutor, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql").to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql").to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path)).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path)).unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
    // `main` holds no `t`, so the unqualified CREATE INDEX target resolves to
    // the attached schema through the catalog search path.
    ex.execute("CREATE INDEX aux_plain_idx ON t(x)").unwrap();
    ex.execute("CREATE INDEX aux_partial_idx ON t(x) WHERE x > 0").unwrap();
    ex.execute("CREATE INDEX aux_expr_idx ON t(abs(x))").unwrap();

    // Baseline: CREATE INDEX itself already worked before this fix.
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 1);
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 1);
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 1);

    (ex, dir)
}

#[test]
fn test_attached_schema_indexes_maintained_by_qualified_dml() {
    // Schema-qualified DML (`INSERT INTO aux.t ...`) — the issue's own repro.
    let (mut ex, _dir) = attached_schema_with_indexes();

    // INSERT: a row satisfying the partial predicate lands in every index.
    ex.execute("INSERT INTO aux.t VALUES (8)").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 2, "plain index must see the new row");
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 2, "8 satisfies x > 0");
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 2, "abs(8) must be indexed");
    assert!(index_has_numeric_key(&ex, "aux_plain_idx", 8.0));
    assert!(index_has_numeric_key(&ex, "aux_partial_idx", 8.0));

    // INSERT: a row failing the partial predicate is indexed everywhere but
    // the partial index — the predicate must still be honored.
    ex.execute("INSERT INTO aux.t VALUES (-5)").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 3);
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 2, "-5 must not enter the partial index");
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 3);
    assert!(index_has_numeric_key(&ex, "aux_expr_idx", 5.0), "abs(-5) = 5 must be the stored key");

    // UPDATE: the old key leaves and the new key arrives.
    ex.execute("UPDATE aux.t SET x = 9 WHERE x = 8").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 3);
    assert!(index_has_numeric_key(&ex, "aux_plain_idx", 9.0), "updated key must be indexed");
    assert!(!index_has_numeric_key(&ex, "aux_plain_idx", 8.0), "stale key must be removed");
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 2);
    assert!(index_has_numeric_key(&ex, "aux_partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "aux_expr_idx", 9.0));

    // UPDATE across the partial predicate boundary: 9 -> -9 leaves the
    // partial index entirely but stays in the full and expression indexes.
    ex.execute("UPDATE aux.t SET x = -9 WHERE x = 9").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 3);
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 1, "-9 must drop out of x > 0");
    assert!(!index_has_numeric_key(&ex, "aux_partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "aux_expr_idx", 9.0), "abs(-9) is still 9");

    // DELETE: entries are withdrawn from every index the row was in.
    ex.execute("DELETE FROM aux.t WHERE x = 7").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 2);
    assert!(!index_has_numeric_key(&ex, "aux_plain_idx", 7.0));
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 0, "the last x > 0 row is gone");
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 2);
    assert!(!index_has_numeric_key(&ex, "aux_expr_idx", 7.0));
}

#[test]
fn test_attached_schema_indexes_maintained_by_unqualified_dml() {
    // The same DML written *unqualified*, resolving to the attached table
    // only through the catalog search path (no `main.t` / `temp.t` shadows
    // it). This spelling reaches storage with a bare table name and behaved
    // differently from the qualified spelling before the fix.
    let (mut ex, _dir) = attached_schema_with_indexes();

    ex.execute("INSERT INTO t VALUES (8)").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 2);
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 2);
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 2);
    assert!(index_has_numeric_key(&ex, "aux_plain_idx", 8.0));

    ex.execute("UPDATE t SET x = 9 WHERE x = 8").unwrap();
    assert!(index_has_numeric_key(&ex, "aux_plain_idx", 9.0));
    assert!(!index_has_numeric_key(&ex, "aux_plain_idx", 8.0));
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 2);

    ex.execute("DELETE FROM t WHERE x = 9").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 1);
    assert!(!index_has_numeric_key(&ex, "aux_plain_idx", 9.0));
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 1);
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 1);

    // The rows themselves agree with the index bodies.
    let result = ex.execute("SELECT x FROM aux.t").unwrap();
    assert_eq!(result.rows, vec![vec![Some("7".to_string())]]);
}

#[test]
fn test_attached_schema_dml_does_not_maintain_main_schema_indexes() {
    // Cross-schema contamination guard: `main.t` and `aux.t` both exist with
    // the same bare name and each has its own index. DML against `aux.t` must
    // maintain only `aux.t`'s index — matching purely on the bare table name
    // would update both.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql").to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql").to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path)).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path)).unwrap();

    // Create the attached table and its index BEFORE `main.t` exists, so the
    // unqualified CREATE INDEX target unambiguously resolves to `aux.t`.
    ex.execute("CREATE TABLE aux.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (7)").unwrap();
    ex.execute("CREATE INDEX aux_idx ON t(x)").unwrap();

    // Now a same-named main-schema table with its own index.
    ex.execute("CREATE TABLE main.t(x INTEGER)").unwrap();
    ex.execute("INSERT INTO main.t VALUES (100)").unwrap();
    // `CREATE INDEX ... ON <schema>.<table>` is not accepted by the parser, so
    // the main-schema index is created unqualified — which now resolves to
    // `main.t` (main precedes attached databases in the search path).
    ex.execute("CREATE INDEX main_idx ON t(x)").unwrap();

    assert_eq!(index_entry_count(&ex, "aux_idx"), 1);
    assert_eq!(index_entry_count(&ex, "main_idx"), 1);

    // DML on the attached table only.
    ex.execute("INSERT INTO aux.t VALUES (8)").unwrap();
    assert_eq!(index_entry_count(&ex, "aux_idx"), 2, "aux.t's index must see the new row");
    assert_eq!(
        index_entry_count(&ex, "main_idx"),
        1,
        "main.t's index must NOT be touched by DML on aux.t"
    );
    assert!(!index_has_numeric_key(&ex, "main_idx", 8.0));
    assert_eq!(index_key_count(&ex, "main_idx"), 1);

    // DML on the main table only, the mirror image.
    ex.execute("INSERT INTO main.t VALUES (200)").unwrap();
    assert_eq!(index_entry_count(&ex, "main_idx"), 2);
    assert_eq!(
        index_entry_count(&ex, "aux_idx"),
        2,
        "aux.t's index must NOT be touched by DML on main.t"
    );
    assert!(!index_has_numeric_key(&ex, "aux_idx", 200.0));

    // Both tables still read back exactly their own rows.
    let result = ex.execute("SELECT x FROM aux.t ORDER BY x").unwrap();
    assert_eq!(result.rows, vec![vec![Some("7".to_string())], vec![Some("8".to_string())]]);
    let result = ex.execute("SELECT x FROM main.t ORDER BY x").unwrap();
    assert_eq!(result.rows, vec![vec![Some("100".to_string())], vec![Some("200".to_string())]]);
}

#[test]
fn test_attached_schema_index_answered_query_matches_unindexed_query() {
    // A stale index is worse than no index: a query the planner can answer
    // from the index body must return exactly the rows a full scan returns.
    // Compare the indexed answer against the same query after DROP INDEX.
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql").to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql").to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path)).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path)).unwrap();
    ex.execute("CREATE TABLE aux.t(x INTEGER, label TEXT)").unwrap();
    for i in 0..40 {
        ex.execute(&format!("INSERT INTO aux.t VALUES ({}, 'seed-{}')", i, i)).unwrap();
    }
    ex.execute("CREATE INDEX aux_q_idx ON t(x)").unwrap();
    ex.execute("CREATE INDEX aux_q_partial_idx ON t(x) WHERE x > 20").unwrap();

    // DML *after* the index was built — the rows that used to go missing.
    for i in 40..60 {
        ex.execute(&format!("INSERT INTO aux.t VALUES ({}, 'post-{}')", i, i)).unwrap();
    }
    ex.execute("UPDATE aux.t SET label = 'updated' WHERE x = 45").unwrap();
    ex.execute("DELETE FROM aux.t WHERE x = 3").unwrap();

    // `INDEXED BY` forces the named index scan (#6475), so these really are
    // index-answered rather than at the planner's cost-based discretion — a
    // plain `WHERE` on a 59-row table may well be answered by a full scan,
    // which would hide a stale index instead of exposing it.
    let indexed_queries = [
        "SELECT x, label FROM aux.t INDEXED BY aux_q_idx WHERE x = 45",
        "SELECT x, label FROM aux.t INDEXED BY aux_q_idx WHERE x = 3",
        "SELECT x, label FROM aux.t INDEXED BY aux_q_idx WHERE x = 55",
        "SELECT x, label FROM aux.t INDEXED BY aux_q_idx WHERE x > 20 ORDER BY x",
        "SELECT count(*) FROM aux.t INDEXED BY aux_q_idx WHERE x >= 40",
    ];
    // The same queries with no index available at all (run after DROP INDEX).
    let scan_queries = [
        "SELECT x, label FROM aux.t WHERE x = 45",
        "SELECT x, label FROM aux.t WHERE x = 3",
        "SELECT x, label FROM aux.t WHERE x = 55",
        "SELECT x, label FROM aux.t WHERE x > 20 ORDER BY x",
        "SELECT count(*) FROM aux.t WHERE x >= 40",
    ];

    let with_indexes: Vec<_> =
        indexed_queries.iter().map(|q| ex.execute(q).unwrap().rows).collect::<Vec<_>>();

    // Sanity-check a couple of answers directly, so an "equal but both wrong"
    // outcome cannot pass.
    assert_eq!(
        with_indexes[0],
        vec![vec![Some("45".to_string()), Some("updated".to_string())]],
        "the updated row must be findable through the index"
    );
    assert!(with_indexes[1].is_empty(), "the deleted row must not be findable through the index");
    assert_eq!(
        with_indexes[2],
        vec![vec![Some("55".to_string()), Some("post-55".to_string())]],
        "a row inserted after CREATE INDEX must be findable through the index"
    );

    ex.execute("DROP INDEX aux_q_idx").unwrap();
    ex.execute("DROP INDEX aux_q_partial_idx").unwrap();

    for (query, indexed_rows) in scan_queries.iter().zip(with_indexes) {
        let scanned_rows = ex.execute(query).unwrap().rows;
        assert_eq!(
            indexed_rows, scanned_rows,
            "index-answered result diverged from the full-scan result for: {}",
            query
        );
    }
}

// ============================================================================
// ON CONFLICT DO UPDATE (upsert) index maintenance (issue #6493)
//
// `INSERT ... ON CONFLICT(...) DO UPDATE SET ...` correctly updated the row's
// column values but never called any index-maintenance function, leaving
// every user-defined index on the table silently stale (the old key was
// never removed and the new key never added). Unlike #6474/#6492, this
// reproduces on an ordinary, non-attached table too — it is a distinct bug in
// the upsert dispatch path (`insert/on_conflict_update.rs`), not an
// attached-schema table-name-resolution issue.
// ============================================================================

#[test]
fn test_upsert_do_update_maintains_plain_index_exact_repro() {
    // The issue's own repro: querying by the new indexed value must find the
    // row, and querying by the old (pre-update) indexed value must not.
    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)").unwrap();
    ex.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    ex.execute("CREATE INDEX idx ON t(x)").unwrap();

    ex.execute("INSERT INTO t VALUES (1, 999) ON CONFLICT(id) DO UPDATE SET x = excluded.x")
        .unwrap();

    // The row's column values are updated correctly (this half already worked).
    let rows = ex.execute("SELECT id, x FROM t").unwrap().rows;
    assert_eq!(rows, vec![vec![Some("1".to_string()), Some("999".to_string())]]);

    // `INDEXED BY` forces an index-answered scan (#6475), so these exercise
    // the index body directly rather than the planner's cost-based discretion.
    let new_key_hit = ex.execute("SELECT id, x FROM t INDEXED BY idx WHERE x = 999").unwrap().rows;
    assert_eq!(
        new_key_hit,
        vec![vec![Some("1".to_string()), Some("999".to_string())]],
        "the new key must be findable through the index"
    );
    let old_key_hit = ex.execute("SELECT id, x FROM t INDEXED BY idx WHERE x = 100").unwrap().rows;
    assert!(old_key_hit.is_empty(), "the stale key must not remain in the index");
}

#[test]
fn test_upsert_do_update_maintains_plain_partial_and_expression_indexes() {
    // Non-attached counterpart of `test_attached_schema_indexes_maintained_by_qualified_dml`'s
    // UPDATE coverage, exercised through the upsert DO UPDATE arm instead of a
    // plain UPDATE: old key removed, new key added, partial predicate
    // re-evaluated in both directions (row leaves the partial index, then
    // re-enters it).
    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER)").unwrap();
    ex.execute("INSERT INTO t VALUES (1, 8)").unwrap();
    ex.execute("CREATE INDEX plain_idx ON t(x)").unwrap();
    ex.execute("CREATE INDEX partial_idx ON t(x) WHERE x > 0").unwrap();
    ex.execute("CREATE INDEX expr_idx ON t(abs(x))").unwrap();
    assert_eq!(index_entry_count(&ex, "plain_idx"), 1);
    assert_eq!(index_entry_count(&ex, "partial_idx"), 1);
    assert_eq!(index_entry_count(&ex, "expr_idx"), 1);

    // Upsert DO UPDATE: the old key (8) leaves and the new key (9) arrives.
    ex.execute("INSERT INTO t VALUES (1, 9) ON CONFLICT(id) DO UPDATE SET x = excluded.x").unwrap();
    assert_eq!(index_entry_count(&ex, "plain_idx"), 1);
    assert!(index_has_numeric_key(&ex, "plain_idx", 9.0), "updated key must be indexed");
    assert!(!index_has_numeric_key(&ex, "plain_idx", 8.0), "stale key must be removed");
    assert_eq!(index_entry_count(&ex, "partial_idx"), 1);
    assert!(index_has_numeric_key(&ex, "partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "expr_idx", 9.0));

    // Upsert DO UPDATE across the partial predicate boundary: 9 -> -9 leaves
    // the partial index entirely but stays in the full and expression indexes
    // (the "row moves out of the partial index" edge case).
    ex.execute("INSERT INTO t VALUES (1, -9) ON CONFLICT(id) DO UPDATE SET x = excluded.x")
        .unwrap();
    assert_eq!(index_entry_count(&ex, "plain_idx"), 1);
    assert_eq!(index_entry_count(&ex, "partial_idx"), 0, "-9 must drop out of x > 0");
    assert!(!index_has_numeric_key(&ex, "partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "expr_idx", 9.0), "abs(-9) is still 9");

    // Upsert DO UPDATE back across the boundary: -9 -> 5 re-enters the
    // partial index (the "row moves into the partial index" edge case).
    ex.execute("INSERT INTO t VALUES (1, 5) ON CONFLICT(id) DO UPDATE SET x = excluded.x").unwrap();
    assert_eq!(index_entry_count(&ex, "plain_idx"), 1);
    assert_eq!(index_entry_count(&ex, "partial_idx"), 1, "5 satisfies x > 0 again");
    assert!(index_has_numeric_key(&ex, "partial_idx", 5.0));
    assert!(index_has_numeric_key(&ex, "expr_idx", 5.0));
}

/// Build a fresh main database with `aux` attached, an attached-schema table
/// with a PRIMARY KEY (needed as an upsert conflict target), and plain,
/// partial, and expression indexes on it.
fn attached_schema_with_indexes_and_pk() -> (SqlExecutor, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let main_path = dir.path().join("main.vbsql").to_str().unwrap().to_string();
    let aux_path = dir.path().join("aux.vbsql").to_str().unwrap().to_string();

    let mut ex = SqlExecutor::new(Some(main_path)).unwrap();
    ex.execute(&format!("ATTACH '{}' AS aux", aux_path)).unwrap();
    ex.execute("CREATE TABLE aux.t(id INTEGER PRIMARY KEY, x INTEGER)").unwrap();
    ex.execute("INSERT INTO aux.t VALUES (1, 8)").unwrap();
    ex.execute("CREATE INDEX aux_plain_idx ON t(x)").unwrap();
    ex.execute("CREATE INDEX aux_partial_idx ON t(x) WHERE x > 0").unwrap();
    ex.execute("CREATE INDEX aux_expr_idx ON t(abs(x))").unwrap();

    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 1);
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 1);
    assert_eq!(index_entry_count(&ex, "aux_expr_idx"), 1);

    (ex, dir)
}

#[test]
fn test_upsert_do_update_maintains_indexes_attached_schema() {
    // Attached-schema counterpart (issue #6493): the upsert DO UPDATE arm
    // must maintain indexes on an attached-schema table exactly as it does on
    // a main-schema table. This is distinct from #6474/#6492 (attached-schema
    // table-name resolution for plain INSERT/UPDATE/DELETE) — it exercises
    // the ON CONFLICT DO UPDATE dispatch path specifically.
    let (mut ex, _dir) = attached_schema_with_indexes_and_pk();

    // Schema-qualified upsert.
    ex.execute("INSERT INTO aux.t VALUES (1, 9) ON CONFLICT(id) DO UPDATE SET x = excluded.x")
        .unwrap();
    assert_eq!(index_entry_count(&ex, "aux_plain_idx"), 1);
    assert!(index_has_numeric_key(&ex, "aux_plain_idx", 9.0), "updated key must be indexed");
    assert!(!index_has_numeric_key(&ex, "aux_plain_idx", 8.0), "stale key must be removed");
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 1);
    assert!(index_has_numeric_key(&ex, "aux_partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "aux_expr_idx", 9.0));

    // Unqualified upsert, resolving through the catalog search path (no
    // `main.t` shadows it) — matching
    // `test_attached_schema_indexes_maintained_by_unqualified_dml`'s coverage
    // of the unqualified spelling.
    ex.execute("INSERT INTO t VALUES (1, -9) ON CONFLICT(id) DO UPDATE SET x = excluded.x")
        .unwrap();
    assert_eq!(index_entry_count(&ex, "aux_partial_idx"), 0, "-9 must drop out of x > 0");
    assert!(!index_has_numeric_key(&ex, "aux_partial_idx", 9.0));
    assert!(index_has_numeric_key(&ex, "aux_expr_idx", 9.0), "abs(-9) is still 9");
}

#[test]
fn test_create_view_unqualified_body_prefers_main_over_temp() {
    // Regression test for #6485 (Gap 2): a `main` view's body must resolve
    // an unqualified table name against `main`, never falling back to a
    // same-named TEMP table — matching real SQLite. VibeSQL previously
    // resolved the view body with the ordinary temp-first search order
    // (`Catalog::get_table`'s default), so a TEMP table created *after* the
    // view silently hijacked every future reference to the view, both at
    // CREATE VIEW time (column derivation) and at query time (view-body
    // re-execution). Exercises the exact repro from the issue body: a
    // self-join of the view's own table, so a mixup would surface not just
    // as wrong data but as duplicated rows from the wrong schema.
    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute("CREATE TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO t1 VALUES('a main','b main')").unwrap();
    ex.execute("CREATE TEMP TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO temp.t1 VALUES('a temp','b temp')").unwrap();
    ex.execute("CREATE VIEW nv AS SELECT * FROM t1 AS x, t1 AS y").unwrap();

    let result = ex.execute("SELECT * FROM nv").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            Some("a main".to_string()),
            Some("b main".to_string()),
            Some("a main".to_string()),
            Some("b main".to_string()),
        ]],
        "#6485: an unqualified `t1` inside a main view's body must resolve to main.t1, not \
         temp.t1"
    );
}

#[test]
fn test_create_view_unqualified_body_prefers_main_over_temp_created_after_view() {
    // Companion to the above: the TEMP table is created *after* the view, so
    // this also pins the query-time (view-body re-execution) half of the fix
    // in `select/scan/table.rs`, not just CREATE VIEW's eager column
    // derivation. A regression here would mean the view's *rows* still come
    // from `main` right after creation but silently start coming from `temp`
    // the moment a colliding TEMP table shows up later in the session.
    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute("CREATE TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO t1 VALUES('a main','b main')").unwrap();
    ex.execute("CREATE VIEW nv AS SELECT * FROM t1").unwrap();

    // Sanity: before the TEMP collision exists, the view reads main as expected.
    assert_eq!(
        ex.execute("SELECT * FROM nv").unwrap().rows,
        vec![vec![Some("a main".to_string()), Some("b main".to_string())]]
    );

    ex.execute("CREATE TEMP TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO temp.t1 VALUES('a temp','b temp')").unwrap();

    assert_eq!(
        ex.execute("SELECT * FROM nv").unwrap().rows,
        vec![vec![Some("a main".to_string()), Some("b main".to_string())]],
        "#6485: nv must keep reading main.t1 after a colliding TEMP table is created, not \
         silently switch to it"
    );
}

#[test]
fn test_create_temp_view_unqualified_body_still_prefers_temp() {
    // A TEMP view is left unrestricted (unlike a main view): its body keeps
    // ordinary temp-first resolution, so an unqualified name inside a TEMP
    // view's body resolving to a colliding TEMP table (shadowing main) is
    // correct SQLite behavior and must not regress into an over-broad
    // "always main" restriction.
    let mut ex = SqlExecutor::new(None).unwrap();
    ex.execute("CREATE TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO t1 VALUES('a main','b main')").unwrap();
    ex.execute("CREATE TEMP TABLE t1(a,b)").unwrap();
    ex.execute("INSERT INTO temp.t1 VALUES('a temp','b temp')").unwrap();
    ex.execute("CREATE TEMP VIEW tv AS SELECT * FROM t1").unwrap();

    assert_eq!(
        ex.execute("SELECT * FROM tv").unwrap().rows,
        vec![vec![Some("a temp".to_string()), Some("b temp".to_string())]],
        "a TEMP view's unqualified body must keep resolving against temp (shadowing main)"
    );
}
