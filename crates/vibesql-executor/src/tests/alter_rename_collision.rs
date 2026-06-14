//! Tests for `ALTER TABLE ... RENAME TO <existing>` error parity with
//! sqlite3 3.51.0 (#5554).
//!
//! sqlite3 3.51.0 reports a single message spanning both the table and index
//! namespaces when the rename target already exists:
//!
//! ```text
//! sqlite> CREATE TABLE a(x); CREATE TABLE b(y);
//! sqlite> ALTER TABLE a RENAME TO b;
//! Error: there is already another table or index with this name: b
//! ```
//!
//! Renaming onto an existing INDEX name yields the same message:
//!
//! ```text
//! sqlite> CREATE TABLE a(x); CREATE INDEX idx ON a(x);
//! sqlite> ALTER TABLE a RENAME TO idx;
//! Error: there is already another table or index with this name: idx
//! ```

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Parse and execute a statement, returning the typed executor error so tests
/// can assert both the variant and the rendered (SQLite-compatible) message.
fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateIndex(s) => crate::CreateIndexExecutor::execute(&s, db),
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn rename_onto_existing_table_matches_sqlite_message() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE a(x)").unwrap();
    exec(&mut db, "CREATE TABLE b(y)").unwrap();

    let err = exec(&mut db, "ALTER TABLE a RENAME TO b").unwrap_err();
    assert_eq!(err, ExecutorError::RenameTargetExists("b".to_string()));
    assert_eq!(
        err.to_string(),
        "there is already another table or index with this name: b"
    );

    // The failed rename must leave the original table intact.
    assert!(db.get_table("a").is_some(), "source table should survive a failed rename");
}

#[test]
fn rename_onto_existing_index_matches_sqlite_message() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE a(x)").unwrap();
    exec(&mut db, "CREATE INDEX idx ON a(x)").unwrap();

    let err = exec(&mut db, "ALTER TABLE a RENAME TO idx").unwrap_err();
    assert_eq!(err, ExecutorError::RenameTargetExists("idx".to_string()));
    assert_eq!(
        err.to_string(),
        "there is already another table or index with this name: idx"
    );

    assert!(db.get_table("a").is_some(), "source table should survive a failed rename");
}

#[test]
fn valid_rename_still_succeeds() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE a(x)").unwrap();

    exec(&mut db, "ALTER TABLE a RENAME TO c").unwrap();
    assert!(db.get_table("c").is_some(), "renamed table should exist under the new name");
    assert!(db.get_table("a").is_none(), "old table name should no longer exist");
}
