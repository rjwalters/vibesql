//! Execution tests for the SQLite `RAISE()` trigger-program expression (#5409).
//!
//! Verified against sqlite3 3.51.x:
//! - `RAISE(ABORT, msg)` / `RAISE(FAIL, msg)` / `RAISE(ROLLBACK, msg)` abort the
//!   firing statement and report `msg` (SQLite error code 19); the message is
//!   surfaced verbatim.
//! - `RAISE(IGNORE)` abandons just the current row and continues with no error.
//!
//! For a *single* aborting statement the three abort variants leave identical
//! visible state (the failing statement's changes vanish either way). Their
//! rollback-scope differences are only observable inside an explicit
//! multi-statement transaction — those are covered by the
//! "Multi-statement transaction scope (#5417)" section below, where ABORT
//! rolls back just the statement, FAIL keeps the statement's partial changes,
//! and ROLLBACK rolls back the whole transaction (all verified against
//! sqlite3 3.51.0).

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use super::super::*;
use crate::errors::ExecutorError;

/// Execute setup SQL that is expected to succeed.
fn exec_ok(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("Unsupported setup statement: {:?}", other),
    }
}

/// Execute a DML statement and return the Result so the caller can assert on a
/// RAISE-driven error.
fn exec_dml(db: &mut vibesql_storage::Database, sql: &str) -> Result<usize, ExecutorError> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Insert(s) => InsertExecutor::execute(db, &s),
        Statement::Update(s) => UpdateExecutor::execute(&s, db),
        Statement::Delete(s) => DeleteExecutor::execute(&s, db),
        other => panic!("Expected DML, got {:?}", other),
    }
}

/// Read column `col` of every row in `table`, ordered by physical position.
fn column_values(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<SqlValue> {
    let schema = db.catalog.get_table(table).expect("table exists");
    let idx = schema.columns.iter().position(|c| c.name == col).expect("column exists");
    db.get_table(table)
        .expect("table exists")
        .scan()
        .iter()
        .map(|row| row.values[idx].clone())
        .collect()
}

fn ints(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<i64> {
    column_values(db, table, col)
        .into_iter()
        .map(|v| match v {
            SqlValue::Integer(i) => i,
            SqlValue::Bigint(i) => i,
            other => panic!("expected integer, got {:?}", other),
        })
        .collect()
}

fn new_db_with_table() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (3, 30)");
    db
}

#[test]
fn raise_abort_aborts_update_with_message() {
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v > 100 \
         BEGIN SELECT raise(ABORT, 'value too big'); END",
    );

    let result = exec_dml(&mut db, "UPDATE t SET v = 200 WHERE id = 2");
    match result {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
            assert_eq!(message, "value too big");
        }
        other => panic!("expected RAISE(ABORT) error, got {:?}", other),
    }

    // The aborted statement made no change: row 2 keeps v=20.
    assert_eq!(ints(&db, "t", "v"), vec![10, 20, 30]);
}

#[test]
fn raise_abort_message_is_surfaced_verbatim() {
    // SQLite reports the message text directly (no "constraint" prefix).
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t \
         BEGIN SELECT raise(ABORT, 'custom error text'); END",
    );

    let err = exec_dml(&mut db, "UPDATE t SET v = 99 WHERE id = 1").unwrap_err();
    // Display should be exactly the rendered message.
    assert_eq!(err.to_string(), "custom error text");
}

#[test]
fn raise_abort_coerces_non_string_message_to_text() {
    // SQLite coerces the message to text (integer 42 -> "42").
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t \
         BEGIN SELECT raise(ABORT, 42); END",
    );

    let err = exec_dml(&mut db, "UPDATE t SET v = 99 WHERE id = 1").unwrap_err();
    assert_eq!(err.to_string(), "42");
}

#[test]
fn raise_fail_aborts_with_message() {
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v > 100 \
         BEGIN SELECT raise(FAIL, 'fail msg'); END",
    );

    match exec_dml(&mut db, "UPDATE t SET v = 200 WHERE id = 2") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
            assert_eq!(message, "fail msg");
        }
        other => panic!("expected RAISE(FAIL) error, got {:?}", other),
    }
}

#[test]
fn raise_rollback_aborts_with_message() {
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v > 100 \
         BEGIN SELECT raise(ROLLBACK, 'undo all'); END",
    );

    match exec_dml(&mut db, "UPDATE t SET v = 200 WHERE id = 2") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
            assert_eq!(message, "undo all");
        }
        other => panic!("expected RAISE(ROLLBACK) error, got {:?}", other),
    }
    // No row changed.
    assert_eq!(ints(&db, "t", "v"), vec![10, 20, 30]);
}

#[test]
fn raise_ignore_skips_only_the_matching_update_row() {
    let mut db = new_db_with_table();
    // Skip the row whose NEW value would be the sentinel 999, update the rest.
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    // Row 2 maps to 999 (ignored); rows 1 and 3 get +100. No error.
    let affected = exec_dml(&mut db, "UPDATE t SET v = CASE WHEN id = 2 THEN 999 ELSE v + 100 END")
        .expect("RAISE(IGNORE) must not error");

    // Rows 1 and 3 updated; row 2 unchanged at 20.
    assert_eq!(ints(&db, "t", "v"), vec![110, 20, 130]);
    // The ignored row is not counted as affected.
    assert_eq!(affected, 2, "ignored row must not be counted as updated");
}

#[test]
fn raise_ignore_skips_only_the_matching_insert_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE INSERT ON t WHEN NEW.v = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");
    // This insert is ignored by the trigger.
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 999)")
        .expect("RAISE(IGNORE) must not error");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (3, 30)");

    // Only rows 1 and 3 made it in.
    assert_eq!(ints(&db, "t", "id"), vec![1, 3]);
    assert_eq!(ints(&db, "t", "v"), vec![10, 30]);
}

#[test]
fn raise_ignore_skips_only_the_matching_delete_row() {
    let mut db = new_db_with_table();
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE DELETE ON t WHEN OLD.v = 20 \
         BEGIN SELECT raise(IGNORE); END",
    );

    // Attempt to delete everything; row 2 (v=20) is protected by IGNORE.
    let affected = exec_dml(&mut db, "DELETE FROM t").expect("RAISE(IGNORE) must not error");

    assert_eq!(ints(&db, "t", "id"), vec![2]);
    assert_eq!(affected, 2, "only the non-ignored rows are deleted/counted");
}

// ===========================================================================
// Multi-statement transaction scope (#5417)
//
// The per-variant rollback scope (ABORT vs FAIL vs ROLLBACK) is only
// observable inside an explicit BEGIN...COMMIT. Each test below mirrors a
// scenario verified against sqlite3 3.51.0:
//
//   CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
//   CREATE TRIGGER trg <BEFORE|AFTER> INSERT ON t WHEN NEW.v = 'BAD'
//     BEGIN SELECT RAISE(<action>, 'boom'); END;
//   BEGIN;
//   INSERT INTO t VALUES (1,'before');         -- earlier statement
//   INSERT INTO t VALUES (...);                -- offending statement (RAISEs)
//   ...                                        -- (txn still open for ABORT/FAIL)
//   COMMIT;
//
// sqlite3 reference results are documented inline per test.
// ===========================================================================

/// Build a db with table `t(id INTEGER PRIMARY KEY, v TEXT)` and a trigger.
fn db_with_trigger(timing_event: &str, action: &str) -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec_ok(
        &mut db,
        &format!(
            "CREATE TRIGGER trg {timing_event} ON t WHEN NEW.v = 'BAD' \
             BEGIN SELECT raise({action}, 'boom'); END"
        ),
    );
    db
}

fn texts(db: &vibesql_storage::Database, table: &str, col: &str) -> Vec<String> {
    column_values(db, table, col)
        .into_iter()
        .map(|v| match v {
            SqlValue::Varchar(s) => s.to_string(),
            SqlValue::Null => "<null>".to_string(),
            other => panic!("expected text, got {:?}", other),
        })
        .collect()
}

/// RAISE(ABORT): the offending statement is rolled back, but earlier and
/// later statements in the same transaction survive, and the transaction
/// stays open.
///
/// sqlite3 3.51.0 final rows for `{before}, {BAD}, {after}`: `{before, after}`.
#[test]
fn raise_abort_keeps_transaction_open_and_prior_statements_survive() {
    let mut db = db_with_trigger("BEFORE INSERT", "ABORT");

    db.begin_transaction().expect("BEGIN");

    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    // Offending statement: BEFORE trigger fires RAISE(ABORT).
    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
            assert_eq!(message, "boom");
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    // Transaction is STILL OPEN (ABORT only rolls back the statement).
    assert!(db.in_transaction(), "ABORT must keep the transaction open");

    // A later statement still runs inside the same transaction.
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (3, 'after')").expect("stmt 3 ok");

    db.commit_transaction().expect("COMMIT");

    // Row 1 (before) and row 3 (after) survive; row 2 (BAD) was aborted.
    assert_eq!(ints(&db, "t", "id"), vec![1, 3]);
    assert_eq!(texts(&db, "t", "v"), vec!["before".to_string(), "after".to_string()]);
}

/// RAISE(ROLLBACK): the entire transaction is rolled back and closed.
///
/// sqlite3 3.51.0: after the RAISE, `no transaction is active` — even the
/// earlier `before` row is gone.
#[test]
fn raise_rollback_aborts_entire_transaction() {
    let mut db = db_with_trigger("BEFORE INSERT", "ROLLBACK");

    db.begin_transaction().expect("BEGIN");

    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");
    assert_eq!(ints(&db, "t", "id"), vec![1], "row 1 visible before the RAISE");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
            assert_eq!(message, "boom");
        }
        other => panic!("expected RAISE(ROLLBACK), got {:?}", other),
    }

    // The WHOLE transaction was rolled back: no longer in a transaction, and
    // even the earlier `before` row is gone.
    assert!(!db.in_transaction(), "ROLLBACK must end the transaction");
    assert!(db.get_table("t").unwrap().scan().is_empty(), "all rows rolled back");
}

/// RAISE(ABORT) vs RAISE(FAIL): with an AFTER trigger and a multi-row
/// statement, the offending statement has already applied some rows when the
/// trigger fires. ABORT undoes the whole statement; FAIL keeps the rows the
/// statement already applied (including the offending row, inserted before the
/// AFTER trigger ran).
///
/// Scenario: txn has row 1; statement inserts (2,'okA'),(3,'BAD'),(4,'okB').
///   - ABORT  → final rows `{1}`            (whole statement undone)
///   - FAIL   → final rows `{1, 2, 3}`      (rows applied so far kept; row 4
///                                           never reached)
/// Both verified against sqlite3 3.51.0.
#[test]
fn raise_abort_undoes_whole_statement_with_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "ABORT");

    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT keeps the transaction open");
    db.commit_transaction().expect("COMMIT");

    // ABORT undoes the entire offending statement — only row 1 survives.
    assert_eq!(ints(&db, "t", "id"), vec![1]);
}

#[test]
fn raise_fail_keeps_partial_statement_changes_with_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "FAIL");

    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
        }
        other => panic!("expected RAISE(FAIL), got {:?}", other),
    }

    assert!(db.in_transaction(), "FAIL keeps the transaction open");
    db.commit_transaction().expect("COMMIT");

    // FAIL keeps the rows applied before the trigger fired: row 1 (earlier
    // statement), row 2 ('okA'), and row 3 ('BAD' — inserted before its AFTER
    // trigger raised). Row 4 ('okB') was never reached.
    assert_eq!(ints(&db, "t", "id"), vec![1, 2, 3]);
    assert_eq!(
        texts(&db, "t", "v"),
        vec!["before".to_string(), "okA".to_string(), "BAD".to_string()]
    );
}

/// Outside an explicit transaction, all three variants behave identically:
/// the offending statement leaves no trace (auto-commit unit) and no
/// transaction is opened. Guards against the savepoint machinery leaking an
/// implicit transaction.
#[test]
fn raise_outside_transaction_leaves_no_open_transaction() {
    for action in ["ABORT", "FAIL", "ROLLBACK"] {
        let mut db = db_with_trigger("BEFORE INSERT", action);
        exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'ok')").expect("seed row");

        let err = exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')").unwrap_err();
        assert!(matches!(err, ExecutorError::Raise { .. }), "{action}: expected RAISE");
        assert!(!db.in_transaction(), "{action}: must not leave a transaction open");
        // Only the seeded row remains; the BAD insert never applied.
        assert_eq!(ints(&db, "t", "id"), vec![1], "{action}");
    }
}

#[test]
fn raise_abort_in_insert_trigger_aborts() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE INSERT ON t WHEN NEW.v < 0 \
         BEGIN SELECT raise(ABORT, 'no negatives'); END",
    );

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, -5)") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
            assert_eq!(message, "no negatives");
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }
    // Nothing inserted.
    assert!(db.get_table("t").unwrap().scan().is_empty());
}
