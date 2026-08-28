//! Tests for INSERT-source-sensitive FK schema-mismatch validation (Part of #6170).
//!
//! `validate_fk_schema_for_dml` (statement-prepare-time FK schema validation,
//! EVIDENCE-OF R-45488-08504 / R-48391-38472) walks the transitive closure of
//! tables that reference the DML target to catch a broken *descendant* FK
//! definition (e.g. a child whose parent-key columns are not backed by a
//! PK/UNIQUE/non-partial UNIQUE INDEX) even when the DML statement itself
//! never touches that descendant.
//!
//! Empirically verified against real `sqlite3` 3.51.0, this mirrors the skip
//! condition at the head of SQLite's parent-side loop in `sqlite3FkCheck()`:
//!
//! ```c
//! if( !pFKey->isDeferred && !(db->flags & SQLITE_DeferFKs)
//!  && !pParse->pToplevel && !pParse->isMultiWrite
//! ){
//!   /* Inserting a single row into a parent table cannot cause (or fix)
//!   ** an immediate foreign key violation. So do nothing in this case.  */
//!   continue;
//! }
//! ```
//!
//! The `continue` skips the whole loop body — including the
//! `sqlite3FkLocateIndex()` call that raises `foreign key mismatch` — so the
//! descendant walk is skipped **only** for a single-row, top-level,
//! non-multi-write INSERT whose relevant child FKs are all immediate. Measured
//! matrix against `CREATE TABLE p(a PRIMARY KEY, b); CREATE TABLE c(x
//! REFERENCES p(b));` (broken: `p.b` has no PK/UNIQUE backing it):
//!
//! | statement against the parent `p`                             | real SQLite |
//! |--------------------------------------------------------------|-------------|
//! | `INSERT INTO p VALUES(1,2)`                                  | ok          |
//! | `INSERT INTO p DEFAULT VALUES`                               | ok          |
//! | `INSERT OR IGNORE`/`OR ABORT`/`OR FAIL`/`OR ROLLBACK`        | ok          |
//! | `INSERT INTO p VALUES(1,2) ON CONFLICT DO NOTHING`           | ok          |
//! | `INSERT INTO p VALUES(1,2),(3,4)`                            | mismatch    |
//! | `INSERT INTO p SELECT 1,2`                                   | mismatch    |
//! | `INSERT OR REPLACE INTO p VALUES(1,2)`                       | mismatch    |
//! | `INSERT INTO p VALUES(1,2) ON CONFLICT(a) DO UPDATE SET b=3` | mismatch    |
//! | `INSERT INTO p VALUES(1,2) RETURNING a`                      | mismatch    |
//! | `INSERT INTO p VALUES(1,2)` from inside a trigger body       | mismatch    |
//! | single-row VALUES, child FK `DEFERRABLE INITIALLY DEFERRED`  | mismatch    |
//! | single-row VALUES with `PRAGMA defer_foreign_keys=ON`        | mismatch    |
//!
//! The VALUES-vs-SELECT half of this contrast is asserted side-by-side by the
//! SQLite conformance suite itself: e_fkey-19.2 / e_fkey-21.2 (`INSERT INTO
//! parent VALUES(...)` succeeds despite a broken descendant) vs.
//! e_fkey-20.$tn.6 (`INSERT INTO $ptbl SELECT ?, ?` fails) in
//! `docs/reference/sqlite/test/e_fkey.test`.
//!
//! A child's own outgoing FK is still checked unconditionally on INSERT,
//! regardless of statement form — INSERT into a broken child must always fail.

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor, TriggerExecutor};
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
        vibesql_ast::Statement::CreateTrigger(create_trigger_stmt) => {
            TriggerExecutor::create_trigger(db, &create_trigger_stmt)
                .unwrap_or_else(|e| panic!("exec {sql:?}: {e}"));
        }
        other => panic!("expected CREATE TABLE/INDEX/TRIGGER, got {other:?}"),
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

// ---------------------------------------------------------------------------
// The rest of SQLite's `pParse->isMultiWrite` / `pParse->pToplevel` /
// `pFKey->isDeferred` / `SQLITE_DeferFKs` conditions. `InsertSource::Select`
// is not the only form that leaves the skip path — each case below was
// measured against real `sqlite3` 3.51.0 (see the module doc matrix).
// ---------------------------------------------------------------------------

#[test]
fn insert_multi_row_values_into_parent_of_broken_descendant_fails() {
    // A multi-row VALUES list is parsed by SQLite as a compound SELECT of
    // VALUES, which makes the statement multi-write — so unlike the single-row
    // form it *does* report the broken descendant.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    let err = exec_insert(&mut db, "INSERT INTO parent VALUES(1, 2, 3), (4, 5, 6)")
        .expect_err("multi-row INSERT ... VALUES into the parent must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("foreign key mismatch") && msg.contains("child_bad"),
        "expected a foreign key mismatch naming child_bad, got: {msg}"
    );
}

#[test]
fn insert_or_replace_values_into_parent_of_broken_descendant_fails() {
    // REPLACE conflict handling deletes conflicting rows before inserting, so
    // SQLite marks the statement multi-write. (The other conflict clauses —
    // IGNORE/ABORT/FAIL/ROLLBACK — do not, and stay on the skip path.)
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    let err = exec_insert(&mut db, "INSERT OR REPLACE INTO parent VALUES(1, 2, 3)")
        .expect_err("INSERT OR REPLACE into the parent must fail");
    assert!(err.to_string().contains("foreign key mismatch"), "got: {err}");
}

#[test]
fn insert_or_ignore_values_into_parent_of_broken_descendant_succeeds() {
    // Counterpart to the REPLACE case above: OR IGNORE is not multi-write in
    // SQLite, so it stays on the single-row skip path and succeeds.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    exec_insert(&mut db, "INSERT OR IGNORE INTO parent VALUES(1, 2, 3)")
        .expect("INSERT OR IGNORE into the parent side of a broken descendant must succeed");
}

#[test]
fn insert_values_with_returning_into_parent_of_broken_descendant_fails() {
    // RETURNING compiles to a coroutine, which SQLite flags as multi-write.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);

    let err = exec_insert(&mut db, "INSERT INTO parent VALUES(1, 2, 3) RETURNING a")
        .expect_err("INSERT ... VALUES ... RETURNING into the parent must fail");
    assert!(err.to_string().contains("foreign key mismatch"), "got: {err}");
}

#[test]
fn insert_values_from_inside_a_trigger_body_into_parent_of_broken_descendant_fails() {
    // `pParse->pToplevel != 0`: an INSERT compiled as part of a trigger
    // sub-program is never on the skip path, even in single-row VALUES form.
    let mut db = new_db();
    setup_parent_with_broken_descendant(&mut db);
    exec_ddl(&mut db, "CREATE TABLE t(v INTEGER)");
    exec_ddl(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO parent VALUES(1, 2, 3); END",
    );

    let err = exec_insert(&mut db, "INSERT INTO t VALUES(9)")
        .expect_err("a trigger body's INSERT ... VALUES into the parent must fail");
    assert!(err.to_string().contains("foreign key mismatch"), "got: {err}");
}

#[test]
fn insert_values_into_parent_of_deferred_broken_child_fails() {
    // `pFKey->isDeferred`: even the single-row VALUES skip path still resolves
    // a child FK declared DEFERRABLE INITIALLY DEFERRED, so the mismatch in its
    // definition is reported.
    let mut db = new_db();
    exec_ddl(&mut db, "CREATE TABLE p4(a INTEGER PRIMARY KEY, b INTEGER)");
    exec_ddl(&mut db, "CREATE TABLE c4(c INTEGER REFERENCES p4(b) DEFERRABLE INITIALLY DEFERRED)");

    let err = exec_insert(&mut db, "INSERT INTO p4 VALUES(1, 2)")
        .expect_err("a deferred broken child FK must still be reported on single-row VALUES");
    assert!(err.to_string().contains("foreign key mismatch"), "got: {err}");
}

#[test]
fn insert_values_into_parent_of_broken_child_fails_under_defer_foreign_keys_pragma() {
    // `SQLITE_DeferFKs`: with PRAGMA defer_foreign_keys on, *every* child FK is
    // treated as deferred, so the skip path is abandoned entirely.
    let mut db = new_db();
    exec_ddl(&mut db, "CREATE TABLE p5(a INTEGER PRIMARY KEY, b INTEGER)");
    exec_ddl(&mut db, "CREATE TABLE c5(c INTEGER REFERENCES p5(b))");
    db.set_defer_foreign_keys(true);

    let err = exec_insert(&mut db, "INSERT INTO p5 VALUES(1, 2)")
        .expect_err("PRAGMA defer_foreign_keys must re-arm the parent-side mismatch check");
    assert!(err.to_string().contains("foreign key mismatch"), "got: {err}");
}
