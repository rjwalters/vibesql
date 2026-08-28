//! Tests for INSERT-source-sensitive FK schema-mismatch validation (Part of #6170).
//!
//! `validate_fk_schema_for_dml` (statement-prepare-time FK schema validation,
//! EVIDENCE-OF R-45488-08504 / R-48391-38472) walks the transitive closure of
//! tables that reference the DML target to catch a broken *descendant* FK
//! definition (e.g. a child whose parent-key columns are not backed by a
//! PK/UNIQUE/non-partial UNIQUE INDEX) even when the DML statement itself
//! never touches that descendant.
//!
//! Empirically verified against real `sqlite3` 3.51.0: this closure walk only
//! fires for INSERT when the row source is a `SELECT` — `INSERT INTO parent
//! SELECT ...` reports "foreign key mismatch" for a broken child exactly like
//! `UPDATE`/`DELETE` do, but `INSERT INTO parent VALUES(...)` and `INSERT INTO
//! parent DEFAULT VALUES` do not, because SQLite's fast single-row VALUES
//! insert path never builds the FK change-mask machinery a broken
//! descendant's definition would be resolved through (a new parent row can
//! never invalidate an existing child row). This exact contrast is asserted
//! side-by-side by the SQLite conformance suite itself: e_fkey-19.2 (`INSERT
//! INTO parent VALUES(...)` succeeds despite a broken descendant) vs.
//! e_fkey-20.$tn.6 (`INSERT INTO $ptbl SELECT ?, ?` fails) in
//! `docs/reference/sqlite/test/e_fkey.test`.
//!
//! A child's own outgoing FK is still checked unconditionally on INSERT,
//! regardless of statement form — INSERT into a broken child must always fail.

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn new_db() -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    db
}

fn exec_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .unwrap_or_else(|e| panic!("exec {sql:?}: {e}"));
        }
        vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
            CreateIndexExecutor::execute(&create_index_stmt, db)
                .unwrap_or_else(|e| panic!("exec {sql:?}: {e}"));
        }
        other => panic!("expected CREATE TABLE/INDEX, got {other:?}"),
    }
}

fn exec_insert(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e}"));
    match stmt {
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).map(|_| ())
        }
        other => panic!("expected INSERT, got {other:?}"),
    }
}

/// Builds the e_fkey-19 schema: `parent` has a mix of well-formed children
/// (child1 references the PK) and a broken descendant, `child_bad`, whose FK
/// targets a plain (non-unique) indexed column — a classic "foreign key
/// mismatch" case (SQLite EVIDENCE-OF R-51039-44840).
fn setup_parent_with_broken_descendant(db: &mut Database) {
    exec_ddl(db, "CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER, e INTEGER)");
    exec_ddl(db, "CREATE INDEX i2 ON parent(e)"); // non-unique: makes child_bad's FK broken
    exec_ddl(db, "CREATE TABLE child1(f INTEGER, g INTEGER REFERENCES parent(a))"); // well-formed
    exec_ddl(db, "CREATE TABLE child_bad(l INTEGER, m INTEGER REFERENCES parent(e))"); // broken: e is not unique
}

#[test]
fn insert_values_into_parent_of_broken_descendant_succeeds() {
    // e_fkey-19.2: INSERT INTO parent VALUES(...) must succeed even though a
    // descendant child (child_bad) has a broken FK definition, because
    // VALUES-form INSERT never needs the FK change-mask for `parent`'s
    // descendants.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    exec_insert(&mut db, "INSERT INTO parent VALUES(1, 2, 3)")
        .expect("INSERT ... VALUES into the parent side of a broken descendant must succeed");
}

#[test]
fn insert_default_values_into_parent_of_broken_descendant_succeeds() {
    // Same rule applies to DEFAULT VALUES (also a non-SELECT source).
    let mut db = new_db();
    exec_ddl(&mut db, "CREATE TABLE p3(a INTEGER PRIMARY KEY, b INTEGER)");
    exec_ddl(&mut db, "CREATE TABLE c3(c INTEGER REFERENCES p3(b))"); // broken: b has no unique key

    exec_insert(&mut db, "INSERT INTO p3 DEFAULT VALUES")
        .expect("INSERT ... DEFAULT VALUES into the parent side of a broken child must succeed");
}

#[test]
fn insert_select_into_parent_of_broken_descendant_fails() {
    // e_fkey-20.$tn.6: INSERT INTO $ptbl SELECT ... must fail with "foreign
    // key mismatch" for the same broken descendant that a VALUES-form INSERT
    // tolerates.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    let err = exec_insert(&mut db, "INSERT INTO parent SELECT 1, 2, 3")
        .expect_err("INSERT ... SELECT into the parent side of a broken descendant must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("foreign key mismatch") && msg.contains("child_bad"),
        "expected a foreign key mismatch naming child_bad, got: {msg}"
    );
}

#[test]
fn insert_values_into_the_broken_child_itself_still_fails() {
    // Step 1 of validate_fk_schema_for_dml ("this table's own outgoing FKs")
    // is unconditional: INSERT directly into the broken child must fail
    // whether the row source is VALUES or SELECT.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);
    exec_insert(&mut db, "INSERT INTO parent VALUES(1, 2, 3)").unwrap();

    let err = exec_insert(&mut db, "INSERT INTO child_bad VALUES('xxx', 3)")
        .expect_err("INSERT ... VALUES into the broken child itself must still fail");
    assert!(err.to_string().contains("foreign key mismatch"));
}

#[test]
fn insert_select_into_the_broken_child_itself_still_fails() {
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);
    exec_insert(&mut db, "INSERT INTO parent VALUES(1, 2, 3)").unwrap();

    let err = exec_insert(&mut db, "INSERT INTO child_bad SELECT 'xxx', 3")
        .expect_err("INSERT ... SELECT into the broken child itself must fail");
    assert!(err.to_string().contains("foreign key mismatch"));
}

#[test]
fn insert_values_into_parent_with_only_well_formed_children_succeeds() {
    // Sanity check: with no broken descendant at all, both INSERT forms
    // succeed against the parent.
    let mut db = new_db();
    exec_ddl(&mut db, "CREATE TABLE parent(a INTEGER PRIMARY KEY)");
    exec_ddl(&mut db, "CREATE TABLE child1(g INTEGER REFERENCES parent(a))");

    exec_insert(&mut db, "INSERT INTO parent VALUES(1)").unwrap();
    exec_insert(&mut db, "INSERT INTO parent SELECT 2").unwrap();
}
