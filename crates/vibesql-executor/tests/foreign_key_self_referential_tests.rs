//! Tests for self-referential FOREIGN KEY enforcement on UPDATE / DELETE
//! (issue #6170, mirroring SQLite's `fkey2-16.*` matrix).
//!
//! A self-referential FK is evaluated against the table's *post-mutation*
//! state: the row being changed contributes its NEW key, and a row that is
//! being deleted takes its own self-reference with it. Three behaviours
//! follow, all previously wrong:
//!
//! 1. `UPDATE self SET a=?, b=?` that moves both the parent key and the self-reference in lock-step
//!    must succeed (self-rescue).
//! 2. `UPDATE self SET a=?` (or `SET b=?`) that breaks the self-reference must fail — the stale OLD
//!    key still sitting in the table must not rescue it (old-row exclusion).
//! 3. `DELETE FROM self` of a row that references itself must succeed.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| String::new()).map_err(|e| e.to_string())
        }
        other => Err(format!("unsupported statement type in test helper: {:?}", other)),
    }
}

/// Build a fresh FK-enabled database holding a single self-referential
/// table `self(a, b REFERENCES self(a))` created from `schema`.
fn setup(schema: &str) -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, schema).unwrap();
    run(&mut db, "INSERT INTO self VALUES(13, 13)").unwrap();
    db
}

/// Each of SQLite's three fkey2-16 schema variants must behave identically.
fn schemas() -> Vec<&'static str> {
    vec![
        "CREATE TABLE self(a INTEGER PRIMARY KEY, b REFERENCES self(a))",
        "CREATE TABLE self(a PRIMARY KEY, b REFERENCES self(a))",
        "CREATE TABLE self(a UNIQUE, b INTEGER PRIMARY KEY REFERENCES self(a))",
    ]
}

#[test]
fn self_ref_update_both_columns_in_lockstep_succeeds() {
    for schema in schemas() {
        let mut db = setup(schema);
        // Moves the parent key (a) and the self-reference (b) together.
        run(&mut db, "UPDATE self SET a = 14, b = 14")
            .unwrap_or_else(|e| panic!("[{schema}] first lock-step update failed: {e}"));
        run(&mut db, "UPDATE self SET a = 17, b = 17")
            .unwrap_or_else(|e| panic!("[{schema}] second lock-step update failed: {e}"));
    }
}

#[test]
fn self_ref_update_parent_key_only_fails() {
    for schema in schemas() {
        let mut db = setup(schema);
        run(&mut db, "UPDATE self SET a = 14, b = 14").unwrap();
        // Moving `a` alone orphans the row's own self-reference (b still 14).
        let res = run(&mut db, "UPDATE self SET a = 15");
        assert!(res.is_err(), "[{schema}] moving parent key alone must violate FK, got {res:?}");
    }
}

#[test]
fn self_ref_update_child_key_only_fails() {
    for schema in schemas() {
        let mut db = setup(schema);
        run(&mut db, "UPDATE self SET a = 14, b = 14").unwrap();
        // Moving `b` alone points the self-reference at a non-existent key.
        let res = run(&mut db, "UPDATE self SET b = 15");
        assert!(res.is_err(), "[{schema}] moving child key alone must violate FK, got {res:?}");
    }
}

#[test]
fn self_ref_delete_of_self_referencing_row_succeeds() {
    for schema in schemas() {
        let mut db = setup(schema);
        run(&mut db, "UPDATE self SET a = 17, b = 17").unwrap();
        // The row references itself; deleting it removes the reference too.
        run(&mut db, "DELETE FROM self")
            .unwrap_or_else(|e| panic!("[{schema}] delete of self-referencing row failed: {e}"));
    }
}

#[test]
fn self_ref_insert_without_matching_key_still_fails() {
    // Guard against over-relaxing: an INSERT whose FK value matches neither
    // an existing row nor its own key must still be rejected.
    for schema in schemas() {
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);
        run(&mut db, schema).unwrap();
        let res = run(&mut db, "INSERT INTO self VALUES(20, 21)");
        assert!(res.is_err(), "[{schema}] INSERT with unmatched self-FK must fail, got {res:?}");
    }
}
