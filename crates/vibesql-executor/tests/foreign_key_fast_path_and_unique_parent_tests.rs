//! Regression tests for two silent "wrong answer" FK-enforcement bugs fixed
//! under issue #6170. Both are the kind of correctness regression that is easy
//! to reintroduce via an unrelated refactor, so they are pinned here in the
//! path that gates every PR (`cargo test -p vibesql-executor`), independent of
//! the (dispatch-only) TCL conformance job.
//!
//! Bug #1 — single-row UPDATE super-fast path bypassed FK validation.
//!   `try_super_fast_path` (in-place column writes, no row cloning) checked
//!   that an assigned column was not the PK / not UNIQUE / not independently
//!   indexed, but never checked whether it was a FOREIGN KEY *child-side*
//!   column. `UPDATE child SET fk_col=<literal> WHERE pk=<literal>` therefore
//!   silently skipped referential-integrity enforcement whenever `fk_col`
//!   carried no index of its own — a dangling reference was written and the
//!   UPDATE returned success. These tests deliberately shape the UPDATE to hit
//!   that path (`WHERE <ipk> = <literal>` → single row, `fk_col` unindexed).
//!
//! Bug #2 — UPDATE-triggered cascade checked the wrong parent key.
//!   `ForeignKeyValidator::check_no_child_references` hardcoded the parent
//!   table's PRIMARY KEY as "the key that changed". An FK whose parent key is a
//!   UNIQUE constraint/index rather than the PK was silently skipped by every
//!   CASCADE/SET NULL/SET DEFAULT/RESTRICT/NO ACTION check whenever the PK was
//!   left untouched. These tests change only the UNIQUE parent key and assert
//!   the configured referential action actually fires.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

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

/// All rows of a table as raw value vectors, in physical scan order.
fn rows(db: &Database, table: &str) -> Vec<Vec<SqlValue>> {
    let t = db.get_table(table).expect("table not found");
    t.scan().iter().map(|r| r.values.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// Bug #1: single-row super-fast-path FK bypass.
// ---------------------------------------------------------------------------

/// `UPDATE child SET fk_col=<literal> WHERE id=<literal>` where `fk_col` has no
/// index of its own and points at a non-existent parent row must be rejected —
/// not silently applied by the in-place fast path.
#[test]
fn fast_path_update_of_fk_column_to_missing_parent_is_rejected() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    // `id INTEGER PRIMARY KEY` gives a rowid so `WHERE id = 1` resolves a single
    // row (the trigger for the super-fast path). `fk_col` is a plain FK column
    // with no UNIQUE/PK/index of its own — exactly the shape that used to slip
    // past FK validation on the in-place path.
    run(&mut db, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b)").unwrap();
    run(&mut db, "CREATE TABLE child(id INTEGER PRIMARY KEY, fk_col REFERENCES parent(a), payload)")
        .unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 10)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(1, 1, 100)").unwrap();

    // 999 has no matching parent row: this must raise an FK violation.
    let res = run(&mut db, "UPDATE child SET fk_col = 999 WHERE id = 1");
    assert!(
        res.is_err(),
        "single-row fast-path UPDATE of an FK column to a missing parent must be rejected, got {res:?}"
    );

    // And it must not have been silently applied: the row still references 1.
    let child = rows(&db, "child");
    assert_eq!(child.len(), 1, "child row count changed unexpectedly");
    assert_eq!(
        child[0][1],
        SqlValue::Integer(1),
        "FK column was silently mutated to a dangling value despite the violation: {child:?}"
    );
}

/// Companion guard: the same fast-path UPDATE to a *valid* parent key must
/// still succeed (the fix must not over-reject legitimate FK-column updates).
#[test]
fn fast_path_update_of_fk_column_to_existing_parent_succeeds() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b)").unwrap();
    run(&mut db, "CREATE TABLE child(id INTEGER PRIMARY KEY, fk_col REFERENCES parent(a), payload)")
        .unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 10)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(2, 20)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(1, 1, 100)").unwrap();

    run(&mut db, "UPDATE child SET fk_col = 2 WHERE id = 1")
        .unwrap_or_else(|e| panic!("valid FK-column update via fast path must succeed: {e}"));

    let child = rows(&db, "child");
    assert_eq!(child[0][1], SqlValue::Integer(2), "valid FK update did not apply: {child:?}");
}

// ---------------------------------------------------------------------------
// Bug #2: UPDATE-triggered cascade against a UNIQUE (non-PK) parent key.
// ---------------------------------------------------------------------------

/// Child FK references a UNIQUE column (not the PK) with ON UPDATE CASCADE.
/// Changing only that UNIQUE parent key must cascade into the child — before
/// the fix, the parent check keyed off the (untouched) PRIMARY KEY and silently
/// skipped the cascade, leaving the child pointing at the old value.
#[test]
fn update_of_unique_parent_key_cascades_to_child() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, u UNIQUE)").unwrap();
    run(&mut db, "CREATE TABLE child(x REFERENCES parent(u) ON UPDATE CASCADE, y)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 100)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(100, 5)").unwrap();

    // Change only the UNIQUE parent key; the PK (id) is untouched.
    run(&mut db, "UPDATE parent SET u = 200 WHERE id = 1")
        .unwrap_or_else(|e| panic!("cascading UNIQUE-parent-key update must succeed: {e}"));

    let child = rows(&db, "child");
    assert_eq!(
        child[0][0],
        SqlValue::Integer(200),
        "ON UPDATE CASCADE against a UNIQUE parent key did not fire — child left dangling: {child:?}"
    );
}

/// Same schema shape but with the default RESTRICT action: changing the UNIQUE
/// parent key while a child still references the old value must be rejected —
/// before the fix this referential check was silently skipped and the update
/// wrongly succeeded, orphaning the child.
#[test]
fn update_of_unique_parent_key_with_referencing_child_is_restricted() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    run(&mut db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, u UNIQUE)").unwrap();
    // No ON UPDATE action → NO ACTION / RESTRICT-style enforcement.
    run(&mut db, "CREATE TABLE child(x REFERENCES parent(u), y)").unwrap();
    run(&mut db, "INSERT INTO parent VALUES(1, 100)").unwrap();
    run(&mut db, "INSERT INTO child VALUES(100, 5)").unwrap();

    let res = run(&mut db, "UPDATE parent SET u = 200 WHERE id = 1");
    assert!(
        res.is_err(),
        "changing a UNIQUE parent key still referenced by a child must raise an FK violation, got {res:?}"
    );

    // The child must remain untouched and still reference the (unchanged) key.
    let parent = rows(&db, "parent");
    assert_eq!(parent[0][1], SqlValue::Integer(100), "parent key changed despite restriction: {parent:?}");
    let child = rows(&db, "child");
    assert_eq!(child[0][0], SqlValue::Integer(100), "child reference changed unexpectedly: {child:?}");
}
