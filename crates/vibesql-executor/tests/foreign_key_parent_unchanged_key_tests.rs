//! Tests for FOREIGN KEY enforcement when an UPDATE does not actually change
//! the parent key (issue #6170, mirroring SQLite's `fkey2-1.*.13` cases).
//!
//! SQLite fires no referential action and raises no violation when an UPDATE
//! rewrites a parent-key column to the value it already holds. Re-assigning a
//! parent key to itself (e.g. `UPDATE t1 SET a = 1` when `a` is already 1)
//! leaves every existing child reference valid, so there is nothing to
//! cascade, set-null/default, or restrict.
//!
//! Before the fix, VibeSQL's parent-side check (`check_no_child_references`)
//! saw the child rows still matching the unchanged old key and raised a
//! spurious "cannot update a parent row" violation. These tests lock in the
//! corrected behaviour while guarding against over-relaxing (a real key change
//! that orphans a child must still fail).

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, UpdateExecutor};
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
        other => Err(format!("unsupported statement type in test helper: {:?}", other)),
    }
}

/// Fresh FK-enabled parent/child pair with one referenced parent row and one
/// child row pointing at it.
fn setup(parent_schema: &str) -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, parent_schema).unwrap();
    run(&mut db, "CREATE TABLE t2(c REFERENCES t1(a), d)").unwrap();
    run(&mut db, "INSERT INTO t1 VALUES(1, 2)").unwrap();
    run(&mut db, "INSERT INTO t2 VALUES(1, 3)").unwrap();
    db
}

#[test]
fn update_parent_key_to_same_literal_value_succeeds() {
    // fkey2-1.1.1.13 / fkey2-1.2.1.13 etc.: `UPDATE t1 SET a = 1` when a is
    // already 1 leaves the child reference valid and must succeed.
    let mut db = setup("CREATE TABLE t1(a PRIMARY KEY, b)");
    run(&mut db, "UPDATE t1 SET a = 1")
        .unwrap_or_else(|e| panic!("no-op parent key update must succeed: {e}"));
}

#[test]
fn update_parent_key_to_itself_succeeds() {
    // `UPDATE t1 SET a = a` is also a no-op on the key column.
    let mut db = setup("CREATE TABLE t1(a PRIMARY KEY, b)");
    run(&mut db, "UPDATE t1 SET a = a")
        .unwrap_or_else(|e| panic!("self-assigning parent key update must succeed: {e}"));
}

#[test]
fn update_parent_nonkey_column_only_succeeds() {
    // Changing a non-key column of a referenced parent row must not trip FK
    // enforcement.
    let mut db = setup("CREATE TABLE t1(a PRIMARY KEY, b)");
    run(&mut db, "UPDATE t1 SET b = 99")
        .unwrap_or_else(|e| panic!("updating parent non-key column must succeed: {e}"));
}

#[test]
fn update_parent_ipk_to_same_value_succeeds() {
    // fkey2-1.1.4.13: same behaviour when the parent key is an INTEGER
    // PRIMARY KEY (IPK).
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE t7(a, b INTEGER PRIMARY KEY)").unwrap();
    run(&mut db, "CREATE TABLE t8(c REFERENCES t7, d)").unwrap();
    run(&mut db, "INSERT INTO t7 VALUES(2, 1)").unwrap();
    run(&mut db, "INSERT INTO t8 VALUES(1, 3)").unwrap();
    run(&mut db, "UPDATE t7 SET b = 1")
        .unwrap_or_else(|e| panic!("no-op IPK parent key update must succeed: {e}"));
}

#[test]
fn update_parent_key_to_new_value_still_fails() {
    // Guard against over-relaxing: actually changing the referenced key
    // orphans the child and must still raise a violation (fkey2-1.*.12:
    // `UPDATE t1 SET a = 2`).
    let mut db = setup("CREATE TABLE t1(a PRIMARY KEY, b)");
    let res = run(&mut db, "UPDATE t1 SET a = 2");
    assert!(res.is_err(), "changing a referenced parent key must violate FK, got {res:?}");
}
