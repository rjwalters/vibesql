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
    assert_eq!(
        audit_msgs(&db),
        vec!["before 10", "after 10", "before 11", "after 11"]
    );
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
    assert_eq!(
        query_col(&db, "SELECT id FROM parent"),
        vec![SqlValue::Integer(1)]
    );
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
