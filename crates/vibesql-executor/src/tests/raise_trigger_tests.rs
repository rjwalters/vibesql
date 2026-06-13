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
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).expect("CREATE VIEW failed");
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

// ===========================================================================
// RAISE(IGNORE) in secondary trigger paths (#5418)
//
// PR #5415 wired RAISE(IGNORE) -> TriggerOutcome::SkipRow through the primary
// INSERT/UPDATE/DELETE row loops. #5418 extends it to the REPLACE
// conflict-resolution path, the UPDATE ... FROM secondary-update path, and the
// INSTEAD OF (view) trigger paths. Each test below mirrors a scenario verified
// against sqlite3 3.51.0.
// ===========================================================================

/// REPLACE / INSERT OR REPLACE: a BEFORE DELETE trigger that RAISE(IGNORE)s the
/// conflicting row's delete leaves the row in place, so the REPLACE's insert
/// then fails with a UNIQUE constraint error and the table is unchanged.
///
/// sqlite3 3.51.0 (PRAGMA recursive_triggers=ON):
///   CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
///   INSERT INTO t VALUES(1,'a'),(2,'b');
///   CREATE TRIGGER trg BEFORE DELETE ON t BEGIN SELECT RAISE(IGNORE); END;
///   REPLACE INTO t VALUES(1,'NEW');  -- Error: UNIQUE constraint failed: t.id
///   -- table unchanged: 1|a, 2|b
#[test]
fn raise_ignore_in_before_delete_blocks_replace_conflict() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 20)");
    // BEFORE DELETE always ignores: the conflicting row can never be deleted.
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE DELETE ON t \
         BEGIN SELECT raise(IGNORE); END",
    );

    // REPLACE on id=1 must resolve the conflict by deleting the old row, but
    // the BEFORE DELETE trigger IGNOREs that delete. The surviving row then
    // collides with the new row on the PK -> UNIQUE constraint error.
    let result = exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (1, 99)");
    assert!(
        result.is_err(),
        "REPLACE must fail: the IGNORE'd conflicting row still collides on the PK"
    );

    // Table unchanged: the old row survives, the new row was not inserted.
    assert_eq!(ints(&db, "t", "id"), vec![1, 2]);
    assert_eq!(ints(&db, "t", "v"), vec![10, 20]);
}

// The companion "REPLACE where the BEFORE DELETE trigger is present but does
// NOT veto the conflicting delete" scenarios — including the WHEN-false /
// non-firing trigger that #5437 flagged — are covered in the "REPLACE
// conflict-delete with a BEFORE DELETE trigger present (#5437)" section at the
// end of this file. (The #5437 "duplicate row" report turned out to be a read
// artifact of `Table::scan()` surfacing tombstoned rows; the conflict-delete
// itself was already correct, and those tests assert it through a live
// `SELECT`.)

/// UPDATE ... FROM (secondary-update path): a BEFORE UPDATE trigger that
/// RAISE(IGNORE)s one matched row skips just that row; the rest update.
///
/// sqlite3 3.51.0:
///   UPDATE t SET v=d.nv FROM d WHERE t.id=d.id;  -- d maps id 2 -> 999
///   -- trigger ignores NEW.v=999, so row 2 keeps its old value, 1 and 3 update.
#[test]
fn raise_ignore_skips_only_matching_row_in_update_from() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (3, 30)");
    exec_ok(&mut db, "CREATE TABLE d (id INTEGER PRIMARY KEY, nv INTEGER)");
    exec_ok(&mut db, "INSERT INTO d (id, nv) VALUES (1, 111)");
    exec_ok(&mut db, "INSERT INTO d (id, nv) VALUES (2, 999)");
    exec_ok(&mut db, "INSERT INTO d (id, nv) VALUES (3, 333)");
    // The sentinel 999 is ignored.
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    let affected = exec_dml(&mut db, "UPDATE t SET v = d.nv FROM d WHERE t.id = d.id")
        .expect("RAISE(IGNORE) must not error");

    // Rows 1 and 3 updated; row 2 unchanged at 20 (its NEW value 999 ignored).
    assert_eq!(ints(&db, "t", "v"), vec![111, 20, 333]);
    assert_eq!(affected, 2, "the ignored row must not be counted as updated");
}

// INSTEAD OF (view) trigger tests.
//
// These use the trigger-level WHEN guard plus an unconditional `SELECT
// raise(IGNORE)` placed BEFORE the base-table write, rather than an in-body
// `CASE WHEN ... THEN raise(IGNORE) END`. VibeSQL's trigger-body statement
// splitter currently treats the `END` of a `CASE ... END` as the trigger-body
// terminator (a pre-existing splitter limitation, separate from #5418), so a
// `CASE` inside a trigger body fails to parse. The WHEN-guard form exercises
// the same INSTEAD OF SkipRow plumbing without tripping that limitation.
//
// SkipRow semantics for INSTEAD OF (verified against sqlite3 3.51.0): a
// `SELECT raise(IGNORE)` aborts the rest of the current trigger's program for
// that row — so a base-table write placed after the RAISE does not run — while
// other rows proceed normally.

/// INSTEAD OF INSERT: when the trigger fires and RAISE(IGNORE)s before its base
/// insert, the base table is not written for that view row.
#[test]
fn raise_ignore_in_instead_of_insert_skips_base_write() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    // The IGNORE comes before the base insert, so the base insert is abandoned.
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF INSERT ON vw \
         BEGIN \
           SELECT raise(IGNORE); \
           INSERT INTO base (id, v) VALUES (NEW.id, NEW.v); \
         END",
    );

    exec_dml(&mut db, "INSERT INTO vw (id, v) VALUES (1, 10)")
        .expect("RAISE(IGNORE) must not error");

    // The view insert was abandoned: nothing reached the base table.
    assert!(db.get_table("base").unwrap().scan().is_empty());
}

/// Control: the same INSTEAD OF INSERT trigger WITHOUT the RAISE writes the base
/// table normally (confirms the path applies the operation when not skipped).
#[test]
fn instead_of_insert_without_ignore_writes_base() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF INSERT ON vw \
         BEGIN \
           INSERT INTO base (id, v) VALUES (NEW.id, NEW.v); \
         END",
    );

    exec_dml(&mut db, "INSERT INTO vw (id, v) VALUES (1, 10)").expect("view insert must succeed");

    assert_eq!(ints(&db, "base", "id"), vec![1]);
    assert_eq!(ints(&db, "base", "v"), vec![10]);
}

// NOTE: the INSTEAD OF UPDATE/DELETE tests immediately below use a single,
// *unconditional* trigger (no trigger-level WHEN clause). They exercise the
// INSTEAD OF SkipRow path: a `SELECT raise(IGNORE)` placed before the base
// write abandons the operation. INSTEAD OF triggers that *do* carry a WHEN
// clause are covered separately in the "INSTEAD OF + WHEN clause (#5438)"
// section further down — previously such triggers failed with TableNotFound
// because `evaluate_when_condition` resolved the schema via `catalog.get_table`
// (None for a view); they now fall back to the view pseudo-schema.

/// INSTEAD OF UPDATE: RAISE(IGNORE) before the base UPDATE abandons the view
/// update, so the base table is not written.
#[test]
fn raise_ignore_in_instead_of_update_abandons_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF UPDATE ON vw \
         BEGIN \
           SELECT raise(IGNORE); \
           UPDATE base SET v = NEW.v WHERE id = OLD.id; \
         END",
    );

    exec_dml(&mut db, "UPDATE vw SET v = 999")
        .expect("RAISE(IGNORE) in INSTEAD OF UPDATE must not error");

    // Base unchanged: every row's view update was abandoned.
    assert_eq!(ints(&db, "base", "id"), vec![1, 2]);
    assert_eq!(ints(&db, "base", "v"), vec![10, 20]);
}

/// Control: the same INSTEAD OF UPDATE trigger WITHOUT the RAISE writes the base
/// table for every matched view row.
#[test]
fn instead_of_update_without_ignore_writes_base() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF UPDATE ON vw \
         BEGIN \
           UPDATE base SET v = NEW.v WHERE id = OLD.id; \
         END",
    );

    exec_dml(&mut db, "UPDATE vw SET v = 999").expect("INSTEAD OF UPDATE must succeed");

    assert_eq!(ints(&db, "base", "v"), vec![999, 999]);
}

/// INSTEAD OF DELETE: RAISE(IGNORE) before the base DELETE abandons the view
/// delete, so no base rows are removed.
#[test]
fn raise_ignore_in_instead_of_delete_abandons_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF DELETE ON vw \
         BEGIN \
           SELECT raise(IGNORE); \
           DELETE FROM base WHERE id = OLD.id; \
         END",
    );

    exec_dml(&mut db, "DELETE FROM vw")
        .expect("RAISE(IGNORE) in INSTEAD OF DELETE must not error");

    // Base unchanged: every row's view delete was abandoned.
    assert_eq!(ints(&db, "base", "id"), vec![1, 2]);
    assert_eq!(ints(&db, "base", "v"), vec![10, 20]);
}

/// Control: the same INSTEAD OF DELETE trigger WITHOUT the RAISE deletes the
/// base rows.
#[test]
fn instead_of_delete_without_ignore_deletes_base() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF DELETE ON vw \
         BEGIN \
           DELETE FROM base WHERE id = OLD.id; \
         END",
    );

    exec_dml(&mut db, "DELETE FROM vw").expect("INSTEAD OF DELETE must succeed");

    assert!(db.get_table("base").unwrap().scan().is_empty());
}

// ===========================================================================
// INSTEAD OF + WHEN clause (#5438)
//
// An INSTEAD OF trigger (on a VIEW) that carries a `WHEN` condition previously
// failed with `TableNotFound`: `evaluate_when_condition` resolved the OLD/NEW
// schema via `catalog.get_table`, which returns None for a view. The fix
// mirrors the trigger-body path — when the target is a view, the schema is
// built from the view definition (`resolve_trigger_schema`). The WHEN clause
// is evaluated against the NEW/OLD pseudo-rows, and the trigger fires only when
// the condition is true (skipped otherwise), matching sqlite3 3.51.x.
//
// sqlite3 3.51.0, INSTEAD OF UPDATE ON vw WHEN OLD.id = 1 over rows (1,10),(2,20)
// after `UPDATE vw SET v = 999`: row id=1 -> v=999, row id=2 -> v=20 (skipped).
// ===========================================================================

/// INSTEAD OF UPDATE with `WHEN OLD.id = 1`: the trigger body fires only for the
/// row whose OLD.id is 1; the other row is left untouched (WHEN false skips it).
#[test]
fn instead_of_update_when_old_fires_selectively() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF UPDATE ON vw WHEN OLD.id = 1 \
         BEGIN \
           UPDATE base SET v = NEW.v WHERE id = OLD.id; \
         END",
    );

    // Reproduces the original bug report: this used to error with
    // TableNotFound("vw").
    exec_dml(&mut db, "UPDATE vw SET v = 999")
        .expect("INSTEAD OF UPDATE with WHEN must not error on a view");

    // Only the WHEN-true row (id=1) had its body fire; id=2 is unchanged.
    assert_eq!(ints(&db, "base", "id"), vec![1, 2]);
    assert_eq!(ints(&db, "base", "v"), vec![999, 20]);
}

/// INSTEAD OF INSERT with `WHEN NEW.v > 0`: the body fires for the positive row
/// and is skipped for the non-positive row (matching sqlite3 — a WHEN-false
/// INSTEAD OF INSERT performs no base write either).
#[test]
fn instead_of_insert_when_new_fires_selectively() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF INSERT ON vw WHEN NEW.v > 0 \
         BEGIN \
           INSERT INTO base (id, v) VALUES (NEW.id, NEW.v); \
         END",
    );

    exec_dml(&mut db, "INSERT INTO vw (id, v) VALUES (1, 10)")
        .expect("INSTEAD OF INSERT with WHEN must not error on a view");
    exec_dml(&mut db, "INSERT INTO vw (id, v) VALUES (2, -5)")
        .expect("WHEN-false INSTEAD OF INSERT must not error");

    // Only the WHEN-true insert (v=10) reached the base table.
    assert_eq!(ints(&db, "base", "id"), vec![1]);
    assert_eq!(ints(&db, "base", "v"), vec![10]);
}

/// INSTEAD OF DELETE with `WHEN OLD.v > 15`: only the row whose OLD.v exceeds 15
/// is deleted from the base table; the other view row is skipped.
#[test]
fn instead_of_delete_when_old_fires_selectively() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE base (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (1, 10)");
    exec_ok(&mut db, "INSERT INTO base (id, v) VALUES (2, 20)");
    exec_ok(&mut db, "CREATE VIEW vw AS SELECT id, v FROM base");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg INSTEAD OF DELETE ON vw WHEN OLD.v > 15 \
         BEGIN \
           DELETE FROM base WHERE id = OLD.id; \
         END",
    );

    exec_dml(&mut db, "DELETE FROM vw")
        .expect("INSTEAD OF DELETE with WHEN must not error on a view");

    // Only id=2 (v=20 > 15) was deleted; id=1 (v=10) survives. Asserted through
    // the live SELECT path — `Table::scan()` still surfaces tombstoned rows, so
    // a deletion check must read via SELECT (matches sqlite3 3.51.0: `1|10`).
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM base ORDER BY id"),
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(10)]],
    );
}

/// Regression guard for the get_table path: a WHEN clause on a *base-table*
/// trigger must still work (the view fallback must not disturb real tables).
#[test]
fn base_table_trigger_when_clause_still_works() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (id INTEGER, v INTEGER)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg AFTER INSERT ON t WHEN NEW.v > 0 \
         BEGIN \
           INSERT INTO log (id, v) VALUES (NEW.id, NEW.v); \
         END",
    );

    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)").expect("base insert ok");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, -5)").expect("base insert ok");

    // log only captured the WHEN-true insert.
    assert_eq!(ints(&db, "log", "id"), vec![1]);
    assert_eq!(ints(&db, "log", "v"), vec![10]);
}

// ===========================================================================
// REPLACE conflict-delete with a BEFORE DELETE trigger present (#5437)
//
// #5437 reported that `INSERT OR REPLACE` left a DUPLICATE row whenever ANY
// BEFORE DELETE trigger existed on the table — even a never-firing (WHEN-false)
// one. The duplicate, however, was a *read artifact*: the bug report inspected
// raw storage (`Table::scan()`), which intentionally still contains
// bitmap-deleted (tombstoned) rows that have not yet been compacted. Through
// any live read — `Table::scan_live()` or a real `SELECT` — the REPLACE
// conflict-delete path correctly removes the conflicting row, so there is
// exactly one row per key.
//
// These tests assert the invariant through the user-facing `SELECT` path (the
// path a client query actually takes) so a future regression that leaves a
// conflicting row live would fail. Every expected result below is verified
// against sqlite3 3.51.0 with `PRAGMA recursive_triggers=ON`.
// ===========================================================================

/// Run `SELECT <cols> FROM <table> ORDER BY <order_by>` and return the rows'
/// raw `SqlValue` cells — exercising the live read path a client would see.
fn select_rows(
    db: &vibesql_storage::Database,
    sql: &str,
) -> Vec<Vec<SqlValue>> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Select(s) => {
            let executor = crate::SelectExecutor::new(db);
            let result = executor.execute_with_columns(&s).expect("SELECT failed");
            result.rows.into_iter().map(|row| row.values.to_vec()).collect()
        }
        other => panic!("Expected SELECT, got {:?}", other),
    }
}

fn ipk(i: i64) -> SqlValue {
    SqlValue::Integer(i)
}

/// A BEFORE DELETE trigger that never fires (its WHEN can never match) must not
/// prevent the REPLACE conflict-delete: the conflicting row is removed and the
/// new row replaces it, leaving exactly one row.
///
/// sqlite3 3.51.0: `1|10, 2|222`.
#[test]
fn replace_with_when_false_before_delete_trigger_leaves_one_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");
    // WHEN can never be true, so the trigger never fires and never raises.
    exec_ok(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t WHEN OLD.id = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (2, 222)")
        .expect("REPLACE must resolve the conflict");

    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(10)], vec![ipk(2), ipk(222)]],
        "the conflicting row must be deleted, not duplicated"
    );
}

/// A plain (non-RAISE) BEFORE DELETE trigger fires exactly once during the
/// REPLACE conflict-delete and the row is then deleted normally — no duplicate.
///
/// sqlite3 3.51.0: rows `1|10, 2|222`; the trigger fired once (one log row).
#[test]
fn replace_with_non_raise_before_delete_trigger_fires_once_and_replaces() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "CREATE TABLE log (n INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");
    // Side-effecting trigger that fires (no WHEN) but never raises.
    exec_ok(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t \
         BEGIN INSERT INTO log (n) VALUES (OLD.id); END",
    );

    exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (2, 222)")
        .expect("REPLACE must resolve the conflict");

    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(10)], vec![ipk(2), ipk(222)]],
        "exactly one row per id after REPLACE"
    );
    // The BEFORE DELETE trigger fired exactly once, for the conflicting row.
    assert_eq!(
        select_rows(&db, "SELECT n FROM log"),
        vec![vec![ipk(2)]],
        "BEFORE DELETE trigger must fire exactly once for the conflicting row"
    );
}

/// REPLACE conflicting on a (multi-column) UNIQUE constraint, with a non-firing
/// BEFORE DELETE trigger present, resolves the conflict to a single row.
///
/// sqlite3 3.51.0: `1|100|10, 2|200|222`.
#[test]
fn replace_unique_conflict_with_before_delete_trigger_leaves_one_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, u, v) VALUES (1, 100, 10), (2, 200, 20)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t WHEN OLD.id = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    // Conflicts on the PK (id=2) — replace it.
    exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, u, v) VALUES (2, 200, 222)")
        .expect("REPLACE must resolve the UNIQUE conflict");

    assert_eq!(
        select_rows(&db, "SELECT id, u, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(100), ipk(10)], vec![ipk(2), ipk(200), ipk(222)]],
    );
}

/// A single REPLACE that conflicts with TWO distinct existing rows (one on the
/// PK, one on a UNIQUE column) deletes BOTH conflicting rows, with a non-firing
/// BEFORE DELETE trigger present.
///
/// sqlite3 3.51.0: new row (2, 300, 222) collides with id=2 (PK) and u=300
/// (id=3's UNIQUE value); both are removed, leaving `1|100|10, 2|300|222`.
#[test]
fn replace_multiple_conflicts_with_before_delete_trigger_deletes_all() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, u, v) VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t WHEN OLD.id = 999 \
         BEGIN SELECT raise(IGNORE); END",
    );

    exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, u, v) VALUES (2, 300, 222)")
        .expect("REPLACE must resolve both conflicts");

    assert_eq!(
        select_rows(&db, "SELECT id, u, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(100), ipk(10)], vec![ipk(2), ipk(300), ipk(222)]],
        "both conflicting rows (id=2 and id=3) must be deleted"
    );
}

/// Control: REPLACE with NO trigger present is unaffected (one row per key).
///
/// sqlite3 3.51.0: `1|10, 2|222`.
#[test]
fn replace_without_trigger_leaves_one_row() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");

    exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (2, 222)")
        .expect("REPLACE must resolve the conflict");

    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(10)], vec![ipk(2), ipk(222)]],
    );
}

/// Regression for the #5418 SkipRow wiring: a BEFORE DELETE trigger that ALWAYS
/// RAISE(IGNORE)s the conflicting delete leaves the row in place, so the
/// REPLACE's insert collides on the PK and errors; the table is unchanged.
///
/// sqlite3 3.51.0: `UNIQUE constraint failed: t.id`; rows stay `1|10, 2|20`.
#[test]
fn replace_with_raise_ignore_before_delete_errors_and_leaves_table_unchanged() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");
    exec_ok(&mut db, "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN SELECT raise(IGNORE); END");

    let result = exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (1, 99)");
    assert!(
        result.is_err(),
        "the IGNORE'd conflicting row still collides on the PK -> error"
    );

    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(10)], vec![ipk(2), ipk(20)]],
        "table must be unchanged after the failed REPLACE"
    );
}

/// Regression: a BEFORE DELETE trigger that RAISE(ABORT)s the conflicting
/// delete aborts the REPLACE; the table is unchanged.
///
/// sqlite3 3.51.0: error `no delete`; rows stay `1|10, 2|20`.
#[test]
fn replace_with_raise_abort_before_delete_aborts() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER bd BEFORE DELETE ON t BEGIN SELECT raise(ABORT, 'no delete'); END",
    );

    match exec_dml(&mut db, "INSERT OR REPLACE INTO t (id, v) VALUES (1, 99)") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
            assert_eq!(message, "no delete");
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(1), ipk(10)], vec![ipk(2), ipk(20)]],
        "table must be unchanged after the aborted REPLACE"
    );
}
