//! Tests for FK ON DELETE/UPDATE CASCADE firing child-table row triggers (#5440).
//!
//! sqlite3 3.51 fires the child table's BEFORE/AFTER DELETE/UPDATE row
//! triggers for every row removed/updated via a parent cascade. These tests
//! lock in the matching behavior: per-row trigger firing, RAISE(ABORT)
//! aborting the whole statement, RAISE(IGNORE) skipping a child row's cascade,
//! and multi-level cascade ordering. All expectations were verified live
//! against sqlite3 3.51.0.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single DDL/DML statement.
fn exec(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger(db, &s).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::Delete(s) => crate::delete::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
            .map_err(|e| e.to_string()),
        Statement::Update(s) => crate::update::UpdateExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) updated", count))
            .map_err(|e| e.to_string()),
        Statement::DropTable(s) => {
            crate::DropTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        other => Err(format!("Unsupported statement type: {:?}", other)),
    }
}

/// Run a SELECT and return the first column of every row as a `Vec<SqlValue>`.
fn query_col(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let result = crate::SelectExecutor::new(db).execute_with_columns(&select).expect("run select");
    result.rows.iter().map(|r| r.values[0].clone()).collect()
}

/// Collect the `msg` audit column as a Vec<String> in insertion order.
fn audit_msgs(db: &Database) -> Vec<String> {
    query_col(db, "SELECT msg FROM audit ORDER BY seq")
        .into_iter()
        .map(|v| match v {
            SqlValue::Varchar(s) => s.to_string(),
            other => format!("{:?}", other),
        })
        .collect()
}

fn fresh_db() -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    db
}

fn create_audit(db: &mut Database) {
    exec(db, "CREATE TABLE audit (seq INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT)").unwrap();
}

/// Parent DELETE with ON DELETE CASCADE fires the child's AFTER DELETE
/// trigger once per cascaded row.
#[test]
fn cascade_delete_fires_child_after_delete_trigger() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_ad AFTER DELETE ON child BEGIN INSERT INTO audit(msg) VALUES('deleted ' || OLD.id); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1), (2)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1), (20, 2)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    // sqlite3 3.51: AFTER DELETE fires for each cascaded child row.
    assert_eq!(audit_msgs(&db), vec!["deleted 10", "deleted 11"]);
    // Child rows referencing parent 1 are gone; row 20 (parent 2) remains.
    assert_eq!(query_col(&db, "SELECT id FROM child ORDER BY id"), vec![SqlValue::Integer(20)]);
}

/// Both BEFORE and AFTER DELETE child triggers fire, in sqlite3 order
/// (BEFORE before the cascaded delete, AFTER after).
#[test]
fn cascade_delete_fires_before_and_after_child_triggers_in_order() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_bd BEFORE DELETE ON child BEGIN INSERT INTO audit(msg) VALUES('before ' || OLD.id); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_ad AFTER DELETE ON child BEGIN INSERT INTO audit(msg) VALUES('after ' || OLD.id); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    // Verified against sqlite3 3.51.0.
    assert_eq!(audit_msgs(&db), vec!["before 10", "after 10", "before 11", "after 11"]);
}

/// A child BEFORE DELETE trigger RAISE(ABORT) on a cascaded row aborts the
/// whole statement: the cascade's already-applied partial deletes are rolled
/// back. Observable statement-level rollback for RAISE(ABORT) is implemented
/// via the statement savepoint, which is only armed inside an explicit
/// transaction (matching how the direct-DML RAISE(ABORT) atomicity is tested,
/// see raise_trigger_tests.rs). #5440 broadens the savepoint-arming gate so a
/// cascade-reachable child trigger arms it even when the parent table has no
/// triggers of its own.
#[test]
fn cascade_delete_child_before_raise_abort_rolls_back_statement() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_guard BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(ABORT, 'no delete 11'); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");
    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("no delete 11"), "unexpected error: {err}");
    // ABORT keeps the transaction open (only the statement is rolled back).
    assert!(db.in_transaction(), "ABORT must keep the transaction open");
    db.commit_transaction().expect("COMMIT");

    // sqlite3 aborts the whole statement: parent and both child rows survive,
    // including the child[10] that the cascade had already deleted.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );
}

/// #5464: the same cascade-fired RAISE(ABORT), but in AUTO-COMMIT (no explicit
/// BEGIN). SQLite wraps every statement in an implicit transaction, so the
/// partial cascade deletes are rolled back here too: parent + both children
/// survive. Before #5464 the statement savepoint was never armed in auto-commit
/// and the already-cascaded child[10] stayed deleted.
///
/// sqlite3 3.51.0 (no explicit txn): `DELETE FROM parent WHERE id=1` -> error
/// `no delete 11`; SELECT shows parent 1, child 10 and 11 all intact.
#[test]
fn cascade_delete_child_before_raise_abort_rolls_back_in_auto_commit() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_guard BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(ABORT, 'no delete 11'); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    // No explicit transaction — pure auto-commit.
    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("no delete 11"), "unexpected error: {err}");
    assert!(!db.in_transaction(), "auto-commit must not leak an open transaction");

    // Whole statement rolled back: parent and both children survive, including
    // child[10] that the cascade had already deleted before the abort.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );
}

/// #5464: cascade UPDATE child RAISE(ABORT) in auto-commit rolls back the whole
/// statement — parent key and both child FKs are restored (including child[10]
/// whose cascade update had already been applied).
///
/// sqlite3 3.51.0 (no explicit txn): `UPDATE parent SET id=99 WHERE id=1` ->
/// error `no update 11`; parent still 1, both child pids still 1.
#[test]
fn cascade_update_child_before_raise_abort_rolls_back_in_auto_commit() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_guard BEFORE UPDATE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(ABORT, 'no update 11'); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    let err = exec(&mut db, "UPDATE parent SET id = 99 WHERE id = 1").unwrap_err();
    assert!(err.contains("no update 11"), "unexpected error: {err}");
    assert!(!db.in_transaction());

    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT pid FROM child ORDER BY id"),
        vec![SqlValue::Integer(1), SqlValue::Integer(1)]
    );
}

/// Parent PK UPDATE with ON UPDATE CASCADE fires the child's BEFORE/AFTER
/// UPDATE triggers for each cascaded row, with OLD/NEW reflecting the FK
/// rewrite.
#[test]
fn cascade_update_fires_child_update_triggers() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_bu BEFORE UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('before ' || OLD.id); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_au AFTER UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('after ' || OLD.pid || '->' || NEW.pid || ' id=' || NEW.id); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    exec(&mut db, "UPDATE parent SET id = 99 WHERE id = 1").unwrap();

    // Verified against sqlite3 3.51.0.
    assert_eq!(
        audit_msgs(&db),
        vec!["before 10", "after 1->99 id=10", "before 11", "after 1->99 id=11"]
    );
    // Child FK columns were rewritten to the new parent key.
    assert_eq!(
        query_col(&db, "SELECT pid FROM child ORDER BY id"),
        vec![SqlValue::Integer(99), SqlValue::Integer(99)]
    );
}

/// A child BEFORE UPDATE RAISE(ABORT) on a cascaded row rolls back the whole
/// statement (including the cascade's already-applied partial updates). As
/// with the DELETE case, observable statement rollback is exercised inside an
/// explicit transaction.
#[test]
fn cascade_update_child_before_raise_abort_rolls_back_statement() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_guard BEFORE UPDATE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(ABORT, 'no update 11'); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");
    let err = exec(&mut db, "UPDATE parent SET id = 99 WHERE id = 1").unwrap_err();
    assert!(err.contains("no update 11"), "unexpected error: {err}");
    assert!(db.in_transaction(), "ABORT must keep the transaction open");
    db.commit_transaction().expect("COMMIT");

    // Statement aborted: parent key unchanged, child FKs unchanged (including
    // child[10] whose cascade update had already been applied).
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT pid FROM child ORDER BY id"),
        vec![SqlValue::Integer(1), SqlValue::Integer(1)]
    );
}

/// Multi-level cascade fires triggers at every level, interleaved per row in
/// sqlite3 order: child BEFORE -> grandchild cascade -> child AFTER.
#[test]
fn multi_level_cascade_delete_fires_triggers_at_each_level() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE a (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE b (id INTEGER PRIMARY KEY, aid INTEGER REFERENCES a(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TABLE c (id INTEGER PRIMARY KEY, bid INTEGER REFERENCES b(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER b_bd BEFORE DELETE ON b BEGIN INSERT INTO audit(msg) VALUES('b before ' || OLD.id); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER b_ad AFTER DELETE ON b BEGIN INSERT INTO audit(msg) VALUES('b after ' || OLD.id); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER c_bd BEFORE DELETE ON c BEGIN INSERT INTO audit(msg) VALUES('c before ' || OLD.id); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER c_ad AFTER DELETE ON c BEGIN INSERT INTO audit(msg) VALUES('c after ' || OLD.id); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO a VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO b VALUES (100, 1)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES (1000, 100)").unwrap();

    exec(&mut db, "DELETE FROM a WHERE id = 1").unwrap();

    // Verified against sqlite3 3.51.0: the grandchild (c) triggers fire
    // between b's BEFORE and AFTER triggers.
    assert_eq!(
        audit_msgs(&db),
        vec!["b before 100", "c before 1000", "c after 1000", "b after 100"]
    );
    assert_eq!(query_col(&db, "SELECT id FROM b"), Vec::<SqlValue>::new());
    assert_eq!(query_col(&db, "SELECT id FROM c"), Vec::<SqlValue>::new());
}

/// #5465: a cascade-fired RAISE(IGNORE) skips the child row's CASCADE delete,
/// but the surviving child is now an orphan (its parent is being deleted). For
/// an immediate FK this trips the statement-end FK check, rolling the whole
/// statement back — exactly as sqlite3 3.51.0:
///
/// ```text
/// DELETE FROM parent WHERE id=1;  -- Error: FOREIGN KEY constraint failed
/// -- parent 1, child 10 and child 11 all survive (statement rolled back)
/// ```
///
/// Before #5465 VibeSQL honored the skip but never re-ran the orphan check, so
/// it deleted the parent + child 10 and left child 11 orphaned silently.
#[test]
fn cascade_delete_raise_ignore_orphan_trips_statement_end_fk_check() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    // Auto-commit: the implicit statement savepoint rolls the whole statement
    // back on the FK violation.
    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "expected FK violation, got: {err}");
    assert!(!db.in_transaction(), "auto-commit must not leak an open transaction");

    // sqlite3 3.51.0: statement rolls back — parent 1 and BOTH children
    // survive (including child 10, which the cascade had already deleted).
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );
}

/// #5465: the same orphan check fires for ON UPDATE CASCADE. A cascade-fired
/// BEFORE UPDATE RAISE(IGNORE) leaves the child pointing at the OLD parent key,
/// which the parent UPDATE is about to rewrite — an orphan.
///
/// sqlite3 3.51.0: `UPDATE parent SET id=2 WHERE id=1` -> error
/// `FOREIGN KEY constraint failed`; parent stays 1, both child pids stay 1.
#[test]
fn cascade_update_raise_ignore_orphan_trips_statement_end_fk_check() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE UPDATE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    let err = exec(&mut db, "UPDATE parent SET id = 2 WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "expected FK violation, got: {err}");
    assert!(!db.in_transaction());

    // Statement rolled back: parent key unchanged, both child FKs unchanged.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT pid FROM child ORDER BY id"),
        vec![SqlValue::Integer(1), SqlValue::Integer(1)]
    );
}

/// #5465 control: RAISE(IGNORE) inside a cascade that leaves a *consistent*
/// state must NOT raise a false FK violation. Here the trigger's WHEN clause
/// never matches, so every child cascade-deletes normally and the parent is
/// removed — no orphan, no error. Locks in that the new orphan check only
/// fires when a row is actually skipped.
#[test]
fn cascade_delete_raise_ignore_not_fired_leaves_consistent_state() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    // WHEN OLD.id = 999 never matches the rows we delete, so RAISE(IGNORE)
    // never fires and no orphan is created.
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE DELETE ON child WHEN OLD.id = 999 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    // sqlite3 3.51.0: no error; parent + all children removed.
    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(query_col(&db, "SELECT id FROM parent"), Vec::<SqlValue>::new());
    assert_eq!(query_col(&db, "SELECT id FROM child ORDER BY id"), Vec::<SqlValue>::new());
}

/// #5465 control: a DEFERRABLE INITIALLY DEFERRED FK does NOT raise the orphan
/// at statement end — the surviving child is queued and the violation only
/// surfaces at COMMIT, matching sqlite3 3.51.0:
///
/// ```text
/// BEGIN;
/// DELETE FROM parent WHERE id=1;  -- no error yet; child 11 orphaned, visible
/// COMMIT;                         -- Error: FOREIGN KEY constraint failed
/// ```
#[test]
fn cascade_delete_raise_ignore_deferred_fk_defers_orphan_to_commit() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");
    // Deferred: the DELETE itself succeeds mid-transaction (no immediate error).
    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    // Mid-txn state: parent + child 10 gone, child 11 survives orphaned.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), Vec::<SqlValue>::new());
    assert_eq!(query_col(&db, "SELECT id FROM child ORDER BY id"), vec![SqlValue::Integer(11)]);

    // COMMIT catches the deferred orphan. Use the executor's CommitExecutor
    // (not the raw storage commit) so the deferred FK re-check runs.
    let commit = crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db);
    assert!(commit.is_err(), "COMMIT must fail on the deferred orphan");
    let msg = format!("{:?}", commit.unwrap_err());
    assert!(
        msg.contains("FOREIGN KEY constraint failed"),
        "expected FK violation at COMMIT, got: {msg}"
    );
}

/// Sanity: cascade delete still works (and deletes the right rows) when the
/// child table has no triggers — no behavioral regression.
#[test]
fn cascade_delete_without_child_triggers_unaffected() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1), (2)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1), (20, 2)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    assert_eq!(query_col(&db, "SELECT id FROM child ORDER BY id"), vec![SqlValue::Integer(20)]);
}

// ---------------------------------------------------------------------------
// #5501: ON DELETE SET NULL / SET DEFAULT must fire the child's BEFORE/AFTER
// UPDATE row triggers (the action is an UPDATE on the child), and a child
// BEFORE UPDATE RAISE(IGNORE) must skip that child's rewrite — re-using the
// statement-end orphan FK check from #5465. All expectations verified live
// against sqlite3 3.51.0.
// ---------------------------------------------------------------------------

/// Parent DELETE with ON DELETE SET NULL fires the child's BEFORE and AFTER
/// UPDATE triggers once per rewritten row, with OLD/NEW reflecting the FK
/// being nulled.
///
/// sqlite3 3.51.0 audit for two children referencing parent 1:
/// ```text
/// before update 10 old.pid=1 new.pid=NULL
/// after update 10
/// before update 11 old.pid=1 new.pid=NULL
/// after update 11
/// ```
#[test]
fn set_null_fires_child_before_and_after_update_triggers() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_bu BEFORE UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('before update ' || OLD.id || ' old.pid=' || IFNULL(OLD.pid,'NULL') || ' new.pid=' || IFNULL(NEW.pid,'NULL')); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_au AFTER UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('after update ' || OLD.id); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    // Verified against sqlite3 3.51.0.
    assert_eq!(
        audit_msgs(&db),
        vec![
            "before update 10 old.pid=1 new.pid=NULL",
            "after update 10",
            "before update 11 old.pid=1 new.pid=NULL",
            "after update 11",
        ]
    );
    // Both child FK columns nulled out.
    assert_eq!(
        query_col(&db, "SELECT IFNULL(pid, -1) FROM child ORDER BY id"),
        vec![SqlValue::Integer(-1), SqlValue::Integer(-1)]
    );
}

/// Parent DELETE with ON DELETE SET DEFAULT fires the child's BEFORE/AFTER
/// UPDATE triggers with NEW reflecting the column default.
///
/// sqlite3 3.51.0: child pid default 99; audit shows `bu 10 new.pid=99` /
/// `au 10`; final child pid = 99.
#[test]
fn set_default_fires_child_before_and_after_update_triggers() {
    let mut db = fresh_db();
    create_audit(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_bu BEFORE UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('bu ' || OLD.id || ' new.pid=' || IFNULL(NEW.pid,'NULL')); END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_au AFTER UPDATE ON child BEGIN INSERT INTO audit(msg) VALUES('au ' || OLD.id); END",
    )
    .unwrap();
    // Parent 99 must exist so the SET DEFAULT rewrite (pid -> 99) is a valid FK.
    exec(&mut db, "INSERT INTO parent VALUES (1), (99)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    // Verified against sqlite3 3.51.0.
    assert_eq!(audit_msgs(&db), vec!["bu 10 new.pid=99", "au 10"]);
    assert_eq!(query_col(&db, "SELECT pid FROM child"), vec![SqlValue::Integer(99)]);
}

/// #5501 + #5465: a SET NULL child BEFORE UPDATE RAISE(IGNORE) skips that
/// child's rewrite, leaving it still pointing at the parent being deleted —
/// an orphan that trips the immediate statement-end FK check and rolls the
/// whole statement back.
///
/// sqlite3 3.51.0 (auto-commit): `DELETE FROM parent WHERE id=1` -> error
/// `FOREIGN KEY constraint failed`; parent 1 stays, child 10 keeps pid=1.
#[test]
fn set_null_raise_ignore_orphan_trips_statement_end_fk_check() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE UPDATE ON child WHEN OLD.id = 10 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1)").unwrap();

    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "expected FK violation, got: {err}");
    assert!(!db.in_transaction(), "auto-commit must not leak an open transaction");

    // sqlite3 3.51.0: statement rolled back — parent and child both intact,
    // child still references the parent (pid unchanged).
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(query_col(&db, "SELECT pid FROM child"), vec![SqlValue::Integer(1)]);
}

/// #5501 + #5465: the same RAISE(IGNORE) skip on a SET DEFAULT child trips
/// the orphan check (the skipped child keeps its OLD FK to the deleted parent).
#[test]
fn set_default_raise_ignore_orphan_trips_statement_end_fk_check() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER DEFAULT 99 REFERENCES parent(id) ON DELETE SET DEFAULT)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE UPDATE ON child WHEN OLD.id = 10 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1), (99)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1)").unwrap();

    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "expected FK violation, got: {err}");
    assert!(!db.in_transaction());

    // Statement rolled back: parent 1 and child 10 (pid=1) both intact.
    assert_eq!(
        query_col(&db, "SELECT id FROM parent ORDER BY id"),
        vec![SqlValue::Integer(1), SqlValue::Integer(99)]
    );
    assert_eq!(query_col(&db, "SELECT pid FROM child"), vec![SqlValue::Integer(1)]);
}

/// #5501 + #5465 control: a SET NULL RAISE(IGNORE) under a DEFERRABLE
/// INITIALLY DEFERRED FK does NOT raise at statement end. The skipped child is
/// queued and the violation surfaces only at COMMIT, matching sqlite3 3.51.0.
#[test]
fn set_null_raise_ignore_deferred_fk_defers_orphan_to_commit() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE UPDATE ON child WHEN OLD.id = 10 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");
    // Deferred: the DELETE itself succeeds mid-transaction (no immediate error).
    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    // Mid-txn: parent gone, child 10 survives orphaned with its OLD pid=1.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), Vec::<SqlValue>::new());
    assert_eq!(query_col(&db, "SELECT pid FROM child"), vec![SqlValue::Integer(1)]);

    let commit = crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db);
    assert!(commit.is_err(), "COMMIT must fail on the deferred orphan");
    let msg = format!("{:?}", commit.unwrap_err());
    assert!(
        msg.contains("FOREIGN KEY constraint failed"),
        "expected FK violation at COMMIT, got: {msg}"
    );
}

/// #5501 sanity: SET NULL with no child triggers still rewrites the FK to
/// NULL — no behavioral regression on the no-trigger path.
#[test]
fn set_null_without_child_triggers_unaffected() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1), (2)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1), (20, 2)").unwrap();

    exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();

    // Children of parent 1 nulled; child of parent 2 unchanged.
    assert_eq!(
        query_col(&db, "SELECT IFNULL(pid, -1) FROM child ORDER BY id"),
        vec![SqlValue::Integer(-1), SqlValue::Integer(-1), SqlValue::Integer(2)]
    );
}

// ---------------------------------------------------------------------------
// #5502: immediate-FK ConstraintViolation inside an EXPLICIT transaction must
// roll the offending STATEMENT back (undoing every partial change it applied,
// including an already-cascaded child DELETE) while leaving the enclosing
// transaction OPEN. Before #5502 the non-RAISE `Err` arm of
// `run_inside_transaction` *released* the statement savepoint instead of
// rolling it back, leaving inconsistent partial state (#5498 found
// `parent={1}, child={11}` — the parent DELETE reverted but child[10]'s
// already-cascaded delete persisted).
//
// Scope note vs sqlite3 3.51.0: in an explicit txn sqlite3 leaves the partial
// state `parent={}, child={11}` for *this* cascade-orphan case (it does not
// undo the already-cascaded child[10] delete). VibeSQL's statement-savepoint
// rolls the whole statement back atomically — `parent={1}, child={10,11}` —
// which the savepoint mechanism cannot make selective. This is the resolution
// the issue explicitly accepts: "internally consistent and recoverable
// without manual ROLLBACK", matching the same statement-rollback scope as
// RAISE(ABORT) and the auto-commit path. For *plain* constraint violations
// (UNIQUE/CHECK/NOT NULL/plain FK) this is exact sqlite3 parity — see
// raise_trigger_tests.rs.
// ---------------------------------------------------------------------------

/// #5502: cascade RAISE(IGNORE) orphan immediate-FK error inside an explicit
/// transaction rolls the whole DELETE statement back (statement savepoint),
/// leaving the transaction OPEN so the application can COMMIT or ROLLBACK.
#[test]
fn cascade_delete_raise_ignore_orphan_in_explicit_txn_rolls_back_statement_keeps_txn_open() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");

    // The cascade fires, child 11's BEFORE DELETE RAISE(IGNORE) leaves it as an
    // orphan, the immediate FK check raises. The statement savepoint must roll
    // the WHOLE statement back (including child 10's already-cascaded delete).
    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "expected FK violation, got: {err}");

    // The transaction stays OPEN (a constraint violation is statement-scoped,
    // not transaction-scoped — SQLite's default).
    assert!(db.in_transaction(), "constraint violation must keep the transaction open");

    // Statement fully rolled back: parent 1 and BOTH children survive, with NO
    // inconsistent partial state (the #5502 bug left parent gone OR child 10
    // lost). Verified via a LIVE SELECT against the in-transaction state.
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );

    // The transaction is recoverable: a clean COMMIT now succeeds because the
    // state is consistent (no orphan was left behind).
    crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db)
        .expect("COMMIT should succeed — no orphan persisted after statement rollback");
    assert!(!db.in_transaction());
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );
}

/// #5502: an EARLIER statement in the same explicit transaction must survive
/// when a later statement trips the cascade-orphan immediate FK. Only the
/// offending statement is rolled back; the transaction stays open.
#[test]
fn cascade_orphan_in_explicit_txn_preserves_earlier_statements() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    exec(&mut db, "CREATE TABLE log (id INTEGER PRIMARY KEY)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER child_skip BEFORE DELETE ON child WHEN OLD.id = 11 BEGIN SELECT RAISE(IGNORE); END",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES (1)").unwrap();
    exec(&mut db, "INSERT INTO child VALUES (10, 1), (11, 1)").unwrap();

    db.begin_transaction().expect("BEGIN");

    // Earlier statement: a plain insert into an unrelated table.
    exec(&mut db, "INSERT INTO log VALUES (100)").unwrap();

    // Offending statement: cascade orphan immediate FK.
    let err = exec(&mut db, "DELETE FROM parent WHERE id = 1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint failed"), "got: {err}");
    assert!(db.in_transaction(), "txn must stay open");

    // Earlier statement survives; offending statement fully undone.
    assert_eq!(query_col(&db, "SELECT id FROM log"), vec![SqlValue::Integer(100)]);
    assert_eq!(query_col(&db, "SELECT id FROM parent"), vec![SqlValue::Integer(1)]);
    assert_eq!(
        query_col(&db, "SELECT id FROM child ORDER BY id"),
        vec![SqlValue::Integer(10), SqlValue::Integer(11)]
    );

    crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db).expect("COMMIT ok");
    assert_eq!(query_col(&db, "SELECT id FROM log"), vec![SqlValue::Integer(100)]);
}

/// fkey2-3.1.3 (#6170): a multi-level `ON UPDATE CASCADE` chain (`ab` ->
/// `cd` -> `ef`) that lands on a value forbidden by the grandchild's own
/// CHECK constraint must abort the whole outer UPDATE — the cascade rewrite
/// is itself an UPDATE on `ef` and must satisfy `ef`'s own constraints, not
/// silently write the row anyway. Also verifies the multi-level propagation
/// itself: `cd`'s cascaded PK change must further cascade to `ef`.
#[test]
fn multi_level_cascade_update_checks_grandchild_constraints() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE ab(a PRIMARY KEY, b)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE cd(c PRIMARY KEY REFERENCES ab ON UPDATE CASCADE ON DELETE CASCADE, d)",
    )
    .unwrap();
    exec(&mut db, "CREATE TABLE ef(e REFERENCES cd ON UPDATE CASCADE, f, CHECK (e!=5))").unwrap();
    exec(&mut db, "INSERT INTO ab VALUES(1, 'b')").unwrap();
    exec(&mut db, "INSERT INTO cd VALUES(1, 'd')").unwrap();
    exec(&mut db, "INSERT INTO ef VALUES(1, 'e')").unwrap();

    // Cascading ab.a: 1->5 propagates to cd.c (1->5), which must further
    // cascade to ef.e (1->5) -- but ef has CHECK(e!=5), so the whole UPDATE
    // must fail and leave every table unchanged.
    let err = exec(&mut db, "UPDATE ab SET a = 5").unwrap_err();
    assert!(err.contains("CHECK constraint failed"), "got: {err}");
    assert_eq!(query_col(&db, "SELECT a FROM ab"), vec![SqlValue::Integer(1)]);
    assert_eq!(query_col(&db, "SELECT c FROM cd"), vec![SqlValue::Integer(1)]);
    assert_eq!(query_col(&db, "SELECT e FROM ef"), vec![SqlValue::Integer(1)]);

    // A value that clears the grandchild's CHECK constraint cascades cleanly
    // through both levels.
    exec(&mut db, "UPDATE ab SET a = 2").unwrap();
    assert_eq!(query_col(&db, "SELECT a FROM ab"), vec![SqlValue::Integer(2)]);
    assert_eq!(query_col(&db, "SELECT c FROM cd"), vec![SqlValue::Integer(2)]);
    assert_eq!(query_col(&db, "SELECT e FROM ef"), vec![SqlValue::Integer(2)]);
}

/// fkey2-9.1.5 (#6170): `ON DELETE SET DEFAULT` must re-validate that the
/// column's default value is itself a valid parent key. If the default no
/// longer resolves to an existing parent row, the whole DELETE must fail
/// with a FOREIGN KEY violation instead of silently writing an orphaned
/// default value into the child row.
#[test]
fn on_delete_set_default_revalidates_default_against_parent() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)").unwrap();
    exec(
        &mut db,
        "CREATE TABLE t2(c INTEGER PRIMARY KEY, d INTEGER DEFAULT 1 REFERENCES t1 ON DELETE SET DEFAULT)",
    )
    .unwrap();
    // Deliberately do NOT insert a row with a=1, so the default (1) does not
    // resolve to any parent row once t1's only row (a=2) is deleted.
    exec(&mut db, "INSERT INTO t1 VALUES(2, 'two')").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(1, 2)").unwrap();

    let err = exec(&mut db, "DELETE FROM t1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "got: {err}");
    // Nothing was mutated: the child row keeps its original (valid) value
    // and the parent row was not deleted.
    assert_eq!(query_col(&db, "SELECT a FROM t1"), vec![SqlValue::Integer(2)]);
    assert_eq!(query_col(&db, "SELECT d FROM t2"), vec![SqlValue::Integer(2)]);
}

// ---------------------------------------------------------------------------
// DROP TABLE performs SQLite's implicit FK-enforcing DELETE FROM (fkey3-1.3/1.5,
// e_fkey-57/58/61.3). EVIDENCE-OF R-14208-23986 / R-11078-03945: the implicit
// DELETE removes all rows and may invoke FK actions or constraint violations.
// ---------------------------------------------------------------------------

/// DROP TABLE of a parent still referenced (immediate NO ACTION) by a child
/// row raises FOREIGN KEY constraint failed and leaves the table in place
/// (EVIDENCE-OF R-32768-47925; fkey3-1.3).
#[test]
fn drop_table_referenced_parent_raises_and_keeps_table() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(100), (101)").unwrap();
    exec(&mut db, "CREATE TABLE t2(y INTEGER REFERENCES t1(x))").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(100), (101)").unwrap();

    let err = exec(&mut db, "DROP TABLE t1").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "got: {err}");
    // Table t1 must still exist and retain its rows.
    assert!(db.catalog.table_exists("t1"));
    assert_eq!(
        query_col(&db, "SELECT x FROM t1 ORDER BY x"),
        vec![SqlValue::Integer(100), SqlValue::Integer(101)]
    );

    // After dropping the child, the parent drops cleanly (fkey3-1.4/1.5).
    exec(&mut db, "DROP TABLE t2").unwrap();
    exec(&mut db, "DROP TABLE t1").unwrap();
    assert!(!db.catalog.table_exists("t1"));
}

/// DROP TABLE of a parent with an ON DELETE CASCADE child empties the child
/// via the implicit DELETE, then drops the parent (e_fkey-57.4).
#[test]
fn drop_table_cascades_into_child() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE p(a, b, PRIMARY KEY(a, b))").unwrap();
    exec(&mut db, "CREATE TABLE c3(c, d, FOREIGN KEY(c, d) REFERENCES p ON DELETE CASCADE)")
        .unwrap();
    exec(&mut db, "INSERT INTO p VALUES(1, 2)").unwrap();
    exec(&mut db, "INSERT INTO c3 VALUES(1, 2)").unwrap();

    exec(&mut db, "DROP TABLE p").unwrap();
    assert!(!db.catalog.table_exists("p"));
    // The cascade emptied the child.
    assert_eq!(query_col(&db, "SELECT count(*) FROM c3"), vec![SqlValue::Integer(0)]);
}

/// DROP TABLE of a parent with an ON DELETE SET NULL child nulls the child's
/// FK columns via the implicit DELETE, then drops the parent (e_fkey-61.3.1).
#[test]
fn drop_table_set_null_on_child() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE p(a UNIQUE)").unwrap();
    exec(&mut db, "CREATE TABLE c(b REFERENCES p(a) ON DELETE SET NULL)").unwrap();
    exec(&mut db, "INSERT INTO p VALUES('x')").unwrap();
    exec(&mut db, "INSERT INTO c VALUES('x')").unwrap();

    exec(&mut db, "DROP TABLE p").unwrap();
    assert!(!db.catalog.table_exists("p"));
    // The child's FK column was set to NULL by the SET NULL action.
    assert_eq!(query_col(&db, "SELECT b FROM c"), vec![SqlValue::Null]);
}

/// A self-referential FK never blocks a DROP TABLE: once the whole table is
/// removed both sides of every self-reference disappear together, so the
/// implicit DELETE can never leave an orphan (fkey3-3.* families rely on this).
#[test]
fn drop_table_self_referential_fk_does_not_block() {
    let mut db = fresh_db();
    exec(
        &mut db,
        "CREATE TABLE t3(a, b, c, d, UNIQUE(a, b), FOREIGN KEY(c, d) REFERENCES t3(a, b))",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO t3 VALUES(1, 2, 1, 2)").unwrap();

    exec(&mut db, "DROP TABLE t3").unwrap();
    assert!(!db.catalog.table_exists("t3"));
}

/// The special DROP-TABLE FK behavior only applies when foreign keys are
/// enabled: with PRAGMA foreign_keys OFF a referenced parent drops freely
/// (EVIDENCE-OF R-54142-41346).
#[test]
fn drop_table_referenced_parent_allowed_when_fk_disabled() {
    let mut db = Database::new(); // foreign keys default OFF
    exec(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY)").unwrap();
    exec(&mut db, "INSERT INTO t1 VALUES(100)").unwrap();
    exec(&mut db, "CREATE TABLE t2(y INTEGER REFERENCES t1(x))").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(100)").unwrap();

    exec(&mut db, "DROP TABLE t1").unwrap();
    assert!(!db.catalog.table_exists("t1"));
}

/// A `NOT DEFERRABLE INITIALLY DEFERRED` foreign key (SQLite grammar allows
/// this contradictory-looking combination) must still be checked immediately
/// on DELETE, even inside an open transaction: `NOT DEFERRABLE` always wins
/// and the `INITIALLY DEFERRED` clause is a no-op unless the constraint is
/// actually `DEFERRABLE` (e_fkey-34.*, #6170). Regression test for a bug
/// where the DELETE-side `should_defer` computation only consulted
/// `initially_deferred`, silently deferring a NOT DEFERRABLE constraint to
/// COMMIT instead of raising immediately.
#[test]
fn delete_not_deferrable_initially_deferred_violates_immediately_in_txn() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent(x, y, z, PRIMARY KEY(x, y, z))").unwrap();
    exec(
        &mut db,
        "CREATE TABLE c1(a, b, c, FOREIGN KEY(a, b, c) REFERENCES parent NOT DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES('a', 'b', 'c')").unwrap();
    exec(&mut db, "INSERT INTO c1 VALUES('a', 'b', 'c')").unwrap();

    db.begin_transaction().expect("BEGIN");
    let result = exec(&mut db, "DELETE FROM parent WHERE x = 'a'");
    assert!(
        result.is_err(),
        "NOT DEFERRABLE INITIALLY DEFERRED must violate immediately, not defer to COMMIT"
    );
    assert!(db.in_transaction(), "the failed DELETE must keep the transaction open");
    db.rollback_transaction().expect("ROLLBACK");
}

/// Sibling of the above for a genuinely `DEFERRABLE INITIALLY DEFERRED`
/// constraint: the DELETE must succeed inside the transaction (the
/// violation is queued), and only surface at COMMIT.
#[test]
fn delete_deferrable_initially_deferred_defers_to_commit() {
    let mut db = fresh_db();
    exec(&mut db, "CREATE TABLE parent(x, y, z, PRIMARY KEY(x, y, z))").unwrap();
    exec(
        &mut db,
        "CREATE TABLE c7(a, b, c, FOREIGN KEY(a, b, c) REFERENCES parent DEFERRABLE INITIALLY DEFERRED)",
    )
    .unwrap();
    exec(&mut db, "INSERT INTO parent VALUES('s', 't', 'u')").unwrap();
    exec(&mut db, "INSERT INTO c7 VALUES('s', 't', 'u')").unwrap();

    db.begin_transaction().expect("BEGIN");
    exec(&mut db, "DELETE FROM parent WHERE x = 's'")
        .expect("DEFERRABLE INITIALLY DEFERRED must not violate immediately");
    assert!(db.in_transaction());
    // The deferred-FK re-check runs in `CommitExecutor` (not inside
    // `Database::commit_transaction` itself), so COMMIT must go through the
    // same executor a real `COMMIT` statement would use.
    let commit_result = crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db);
    assert!(commit_result.is_err(), "the queued violation must surface at COMMIT");
}
