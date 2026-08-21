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
    let err = ex
        .execute("CREATE TRIGGER auxdb.r1 AFTER INSERT ON t1 BEGIN SELECT 1; END")
        .unwrap_err();
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
