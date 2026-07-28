//! Integration tests for sqlite_schema/sqlite_master virtual table
//!
//! Issue #4577: These tests verify that schema introspection via the
//! sqlite_schema (and sqlite_master alias) virtual table works correctly.
//!
//! The sqlite_schema table provides SQLite-compatible schema introspection:
//! ```sql
//! SELECT * FROM sqlite_schema;
//! SELECT name FROM sqlite_master WHERE type = 'table';
//! ```

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, SelectExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper to execute a CREATE TABLE statement
fn execute_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, db).expect("Failed to execute CREATE TABLE");
    } else {
        panic!("Expected CREATE TABLE statement");
    }
}

/// Helper to execute a CREATE TABLE statement while preserving the verbatim
/// original source text (issue #5619), mirroring how the CLI/load paths call
/// `execute_with_source`.
fn execute_create_table_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute_with_source(&create_stmt, db, Some(sql))
            .expect("Failed to execute CREATE TABLE");
    } else {
        panic!("Expected CREATE TABLE statement");
    }
}

/// Extract the single text value from a one-row, one-column query result.
fn single_text(db: &Database, query: &str) -> String {
    let (_columns, rows) = execute_select(db, query);
    assert_eq!(rows.len(), 1, "expected exactly one row for query: {}", query);
    match &rows[0].values[0] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_string()
        }
        other => panic!("expected text sql value, got {:?}", other),
    }
}

/// Helper to execute a CREATE INDEX statement
fn execute_create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateIndex(create_stmt) = stmt {
        CreateIndexExecutor::execute(&create_stmt, db).expect("Failed to execute CREATE INDEX");
    } else {
        panic!("Expected CREATE INDEX statement");
    }
}

/// Helper to execute a CREATE VIEW statement
fn execute_create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateView(create_stmt) = stmt {
        ViewExecutor::execute_create_view(&create_stmt, db).expect("Failed to execute CREATE VIEW");
    } else {
        panic!("Expected CREATE VIEW statement");
    }
}

/// Helper to execute a SELECT and return rows
fn execute_select(db: &Database, sql: &str) -> (Vec<String>, Vec<vibesql_storage::Row>) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute_with_columns(&select_stmt).expect("Failed to execute SELECT");
        (result.columns, result.rows)
    } else {
        panic!("Expected SELECT statement");
    }
}

fn setup_database_with_table() -> Database {
    let mut db = Database::new();
    execute_create_table(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(100))");
    db
}

/// Test basic SELECT * FROM sqlite_schema
#[test]
fn test_sqlite_schema_select_all() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_schema");

    // Should have 5 columns: type, name, tbl_name, rootpage, sql
    assert_eq!(columns.len(), 5);
    assert_eq!(columns[0], "type");
    assert_eq!(columns[1], "name");
    assert_eq!(columns[2], "tbl_name");
    assert_eq!(columns[3], "rootpage");
    assert_eq!(columns[4], "sql");

    // Should have at least one row for the users table
    assert!(!rows.is_empty(), "Should have at least one row for the users table");
}

/// Test SELECT * FROM sqlite_master (alias for sqlite_schema)
#[test]
fn test_sqlite_master_select_all() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_master");

    assert_eq!(columns.len(), 5);
    assert!(!rows.is_empty());
}

/// Test SELECT specific columns FROM sqlite_schema
#[test]
fn test_sqlite_schema_select_columns() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT name, type FROM sqlite_schema");

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0], "name");
    assert_eq!(columns[1], "type");
    assert!(!rows.is_empty());
}

/// Test SELECT FROM sqlite_schema with WHERE clause
#[test]
fn test_sqlite_schema_where_clause() {
    let db = setup_database_with_table();

    let (columns, rows) =
        execute_select(&db, "SELECT name FROM sqlite_schema WHERE type = 'table'");

    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0], "name");
    // Should find the users table
    assert!(!rows.is_empty());
}

/// Test case-insensitive access to sqlite_schema
#[test]
fn test_sqlite_schema_case_insensitive() {
    let db = setup_database_with_table();

    // Test various case combinations
    let cases = [
        "SELECT * FROM SQLITE_SCHEMA",
        "SELECT * FROM Sqlite_Schema",
        "SELECT * FROM SQLITE_MASTER",
        "SELECT * FROM Sqlite_Master",
    ];

    for sql in cases {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute_select(&db, sql)));
        assert!(result.is_ok(), "Query '{}' should succeed", sql);
    }
}

/// Test sqlite_schema includes indexes
#[test]
fn test_sqlite_schema_includes_indexes() {
    let mut db = setup_database_with_table();

    // Create an index
    execute_create_index(&mut db, "CREATE INDEX idx_users_name ON users(name)");

    let (_columns, rows) =
        execute_select(&db, "SELECT name, type FROM sqlite_schema WHERE type = 'index'");

    assert!(!rows.is_empty(), "Should have at least one index row");
}

/// Test sqlite_schema includes views
#[test]
fn test_sqlite_schema_includes_views() {
    let mut db = setup_database_with_table();

    // Create a view
    execute_create_view(&mut db, "CREATE VIEW user_names AS SELECT name FROM users");

    let (_columns, rows) =
        execute_select(&db, "SELECT name, type FROM sqlite_schema WHERE type = 'view'");

    assert!(!rows.is_empty(), "Should have at least one view row");
}

/// Test sqlite_schema with aliased table reference
#[test]
fn test_sqlite_schema_with_alias() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT s.name, s.type FROM sqlite_schema AS s");

    assert_eq!(columns.len(), 2);
    assert!(!rows.is_empty());
}

/// Test sqlite_schema ORDER BY
#[test]
fn test_sqlite_schema_order_by() {
    let mut db = setup_database_with_table();

    // Create additional objects
    execute_create_index(&mut db, "CREATE INDEX idx_users_id ON users(id)");
    execute_create_view(&mut db, "CREATE VIEW v_users AS SELECT * FROM users");

    let (_columns, rows) =
        execute_select(&db, "SELECT type, name FROM sqlite_schema ORDER BY type, name");

    // Should have multiple rows sorted by type, then by name
    assert!(rows.len() >= 3, "Should have table, index, and view");
}

/// Test sqlite_schema with empty database
#[test]
fn test_sqlite_schema_empty_database() {
    let db = Database::new();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_schema");

    assert_eq!(columns.len(), 5);
    assert!(rows.is_empty(), "Empty database should have no schema rows");
}

/// Test sqlite_schema in subquery (Issue #4577 acceptance criteria)
#[test]
fn test_sqlite_schema_in_subquery() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(
        &db,
        "SELECT * FROM (SELECT name FROM sqlite_schema WHERE type = 'table') AS t",
    );

    assert_eq!(columns.len(), 1);
    assert!(!rows.is_empty());
}

/// Issue #5619 / table-1.1: SQLite stores the original CREATE TABLE statement
/// byte-verbatim in sqlite_master.sql, preserving the user's exact whitespace
/// and formatting. When the source text is captured at CREATE time, the engine
/// must return it as-is rather than a normalized reconstruction.
#[test]
fn test_sqlite_master_sql_preserves_verbatim_whitespace() {
    let mut db = Database::new();
    // This is the exact statement from SQLite's table.test table-1.1, including
    // the multi-line layout and leading indentation on the column definitions.
    let create_sql = "CREATE TABLE test1 (\n      one varchar(10),\n      two text\n    )";
    execute_create_table_with_source(&mut db, create_sql);

    let stored = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(
        stored, create_sql,
        "sqlite_master.sql must preserve the verbatim CREATE TABLE text (issue #5619)"
    );
}

/// A trailing semicolon on the original statement must not be stored: SQLite's
/// sqlite_master.sql excludes the terminating `;`.
#[test]
fn test_sqlite_master_sql_strips_trailing_semicolon() {
    let mut db = Database::new();
    execute_create_table_with_source(&mut db, "CREATE TABLE   t  (  a   INT ,  b  TEXT ) ;");

    let stored = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(stored, "CREATE TABLE   t  (  a   INT ,  b  TEXT )");
}

/// Issue #5634: `ALTER TABLE ... RENAME TO` edits the verbatim CREATE TABLE
/// text in place, rewriting the table name to the double-quoted new name and
/// preserving all other formatting — byte-for-byte matching sqlite3 3.51.0
/// (previously deferred to invalidate-and-reconstruct, issue #5625).
#[test]
fn test_sqlite_master_sql_rename_table_edits_text_in_place() {
    let mut db = Database::new();
    let create_sql = "CREATE TABLE oldname (\n      a INTEGER,\n      b TEXT\n    )";
    execute_create_table_with_source(&mut db, create_sql);

    // Sanity: verbatim text is returned before any ALTER.
    let before = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(before, create_sql);

    let stmt = Parser::parse_sql("ALTER TABLE oldname RENAME TO newname").expect("parse");
    if let vibesql_ast::Statement::AlterTable(alter) = stmt {
        AlterTableExecutor::execute(&alter, &mut db).expect("rename");
    } else {
        panic!("expected ALTER TABLE");
    }

    // sqlite3 3.51.0 emits the new name double-quoted, preserving everything
    // else verbatim: `CREATE TABLE "newname" (\n      a INTEGER,\n      b TEXT\n    )`.
    let after = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(
        after, "CREATE TABLE \"newname\" (\n      a INTEGER,\n      b TEXT\n    )",
        "RENAME TO must rewrite the table name in place, double-quoted (issue #5634)"
    );
}

/// Issue #5625 part (1): after `ALTER TABLE ... ADD COLUMN`, the catalog schema
/// copy — read by sqlite_master, PRAGMA table_info, and DML column resolution —
/// must reflect the added column (previously only the storage copy was updated,
/// leaving the catalog stale). And part (2): the verbatim CREATE TABLE text is
/// edited in place (`, c INTEGER` appended) to match sqlite3 3.51.0.
#[test]
fn test_sqlite_master_sql_add_column_syncs_catalog_and_edits_text() {
    let mut db = Database::new();
    let create_sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
    execute_create_table_with_source(&mut db, create_sql);

    let stmt = Parser::parse_sql("ALTER TABLE t ADD COLUMN c INTEGER").expect("parse");
    if let vibesql_ast::Statement::AlterTable(alter) = stmt {
        AlterTableExecutor::execute_with_source(
            &alter,
            &mut db,
            Some("ALTER TABLE t ADD COLUMN c INTEGER"),
        )
        .expect("add column");
    } else {
        panic!("expected ALTER TABLE");
    }

    // Part (2): verbatim text edited in place, exactly like sqlite3.
    let sql = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(
        sql, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n, c INTEGER)",
        "ADD COLUMN must append the verbatim column def before the closing paren (issue #5625)"
    );

    // Part (1): the catalog copy now lists the added column.
    let catalog_schema = db.catalog.get_table("t").expect("catalog has table t");
    let col_names: Vec<&str> = catalog_schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        col_names,
        vec!["a", "b", "c"],
        "catalog schema must include the added column after ADD COLUMN (issue #5625)"
    );
}

/// Issue #5634: after `ALTER TABLE ... DROP COLUMN`, the verbatim CREATE TABLE
/// text is edited in place (the dropped column's definition span removed,
/// byte-for-byte matching sqlite3 3.51.0), and the catalog schema copy drops the
/// column too (issue #5625 part 1).
#[test]
fn test_drop_column_edits_text_and_syncs_catalog() {
    let mut db = Database::new();
    let create_sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT,\n  c   INTEGER\n)";
    execute_create_table_with_source(&mut db, create_sql);

    let stmt = Parser::parse_sql("ALTER TABLE t DROP COLUMN c").expect("parse");
    if let vibesql_ast::Statement::AlterTable(alter) = stmt {
        AlterTableExecutor::execute(&alter, &mut db).expect("drop column");
    } else {
        panic!("expected ALTER TABLE");
    }

    // sqlite3 3.51.0: removes `,\n  c   INTEGER` (preceding comma to the `)`).
    let sql = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(
        sql, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT)",
        "DROP COLUMN must remove the column-def span in place (issue #5634)"
    );

    let catalog_schema = db.catalog.get_table("t").expect("catalog has table t");
    let col_names: Vec<&str> = catalog_schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        col_names,
        vec!["a", "b"],
        "catalog schema must drop the column after DROP COLUMN (issue #5625)"
    );
}

/// Issue #5625: `ALTER TABLE ... RENAME COLUMN` rewrites the column name in its
/// definition position in the verbatim text (matching sqlite3) AND syncs the
/// catalog so the renamed column resolves.
#[test]
fn test_rename_column_edits_text_and_syncs_catalog() {
    let mut db = Database::new();
    let create_sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
    execute_create_table_with_source(&mut db, create_sql);

    let stmt = Parser::parse_sql("ALTER TABLE t RENAME COLUMN b TO bb").expect("parse");
    if let vibesql_ast::Statement::AlterTable(alter) = stmt {
        AlterTableExecutor::execute(&alter, &mut db).expect("rename column");
    } else {
        panic!("expected ALTER TABLE");
    }

    let sql = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert_eq!(
        sql, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  bb   TEXT\n)",
        "RENAME COLUMN must rewrite the column name in place (issue #5625)"
    );

    let catalog_schema = db.catalog.get_table("t").expect("catalog has table t");
    let col_names: Vec<&str> = catalog_schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(col_names, vec!["a", "bb"], "catalog schema must reflect the renamed column");
}

/// Issue #6175: `sqlite_master`/`sqlite_schema` must list objects in the order
/// they were created (SQLite's schema-table rowid order), interleaving tables
/// and indexes — not the historical "all tables first, then all indexes"
/// emission order. Here indexes are created *between* table creations, so the
/// creation order (`alpha, beta, idx_alpha, gamma, idx_beta`) differs from any
/// tables-first grouping (`alpha, beta, gamma, idx_alpha, idx_beta`); only the
/// creation-ordered result is correct (pragma.test 23.1).
#[test]
fn test_sqlite_schema_lists_objects_in_creation_order() {
    let mut db = Database::new();

    // Interleave table and index creation. Non-alphabetical, and with each
    // index landing before a later table, so neither an alphabetical sort nor a
    // "tables first, then indexes" grouping reproduces this order.
    execute_create_table(&mut db, "CREATE TABLE alpha (x INTEGER)");
    execute_create_table(&mut db, "CREATE TABLE beta (x INTEGER)");
    execute_create_index(&mut db, "CREATE INDEX idx_alpha ON alpha(x)");
    execute_create_table(&mut db, "CREATE TABLE gamma (x INTEGER)");
    execute_create_index(&mut db, "CREATE INDEX idx_beta ON beta(x)");

    // Default order (no ORDER BY) must be creation order.
    let (_columns, rows) = execute_select(&db, "SELECT name FROM sqlite_schema");
    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.to_string()
            }
            other => panic!("expected text name value, got {:?}", other),
        })
        .collect();

    assert_eq!(
        names,
        vec!["alpha", "beta", "idx_alpha", "gamma", "idx_beta"],
        "sqlite_schema must list objects in interleaved creation order, not tables-first (#6175)"
    );
}

/// Without captured source text (e.g. AST built programmatically), the engine
/// falls back to reconstructing a valid CREATE TABLE statement.
#[test]
fn test_sqlite_master_sql_reconstructs_without_source() {
    let mut db = Database::new();
    // execute_create_table uses the plain `execute` path (no source text).
    execute_create_table(&mut db, "CREATE TABLE noverb (a INTEGER, b TEXT)");

    let stored = single_text(&db, "SELECT sql FROM sqlite_master WHERE type='table'");
    assert!(
        stored.to_uppercase().starts_with("CREATE TABLE"),
        "fallback reconstruction should still produce CREATE TABLE, got: {}",
        stored
    );
    assert!(stored.contains("noverb"), "reconstruction should name the table, got: {}", stored);
}
