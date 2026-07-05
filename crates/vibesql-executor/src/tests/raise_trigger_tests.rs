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
    // trigger raised). Row 4 ('okB') was never reached. Asserted through the
    // live SELECT path (not raw `Table::scan()`): the offending 'BAD' row must
    // be genuinely live, not a bitmap tombstone (#5474). sqlite3 3.51.0:
    // `1|before, 2|okA, 3|BAD`.
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("before".into())],
            vec![ipk(2), SqlValue::Varchar("okA".into())],
            vec![ipk(3), SqlValue::Varchar("BAD".into())],
        ],
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

// ===========================================================================
// Auto-commit statement atomicity (#5464)
//
// SQLite wraps every top-level statement in an implicit transaction, so a
// single statement that applies rows incrementally and then aborts mid-way
// rolls back ALL of that statement's changes — even outside an explicit
// BEGIN...COMMIT. The statement savepoint (#5417) is only armed *inside* an
// explicit transaction; #5464 adds an implicit per-statement transaction for
// the auto-commit case. Every expectation below is verified live against
// sqlite3 3.51.0 with no explicit transaction (pure auto-commit).
// ===========================================================================

/// AFTER INSERT trigger that RAISE(ABORT)s on a later row of a multi-row
/// INSERT, in auto-commit: rows applied before the abort must also be undone.
///
/// sqlite3 3.51.0:
///   INSERT INTO t VALUES (2,'okA'),(3,'BAD'),(4,'okB');  -- Error: boom
///   -- no rows inserted (whole statement rolled back)
#[test]
fn autocommit_after_insert_abort_rolls_back_whole_statement() {
    let mut db = db_with_trigger("AFTER INSERT", "ABORT");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    // sqlite3: the whole statement is rolled back — no rows survive.
    assert!(!db.in_transaction(), "auto-commit must not leak an open transaction");
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        Vec::<Vec<SqlValue>>::new(),
        "ABORT must undo even the rows applied before the offending row"
    );
}

/// RAISE(FAIL) keeps the rows the statement applied before the abort, even in
/// auto-commit — the implicit transaction commits (rather than rolls back) for
/// FAIL. This is the FAIL-vs-ABORT distinction made observable in auto-commit by
/// #5464, with the offending-row visibility corrected in #5474.
///
/// sqlite3 3.51.0 (live read):
///   INSERT INTO t VALUES (2,'okA'),(3,'BAD'),(4,'okB');  -- Error: boom
///   SELECT id FROM t;  -- 2, 3  (okA + the BAD row inserted before its AFTER
///                                trigger raised; okB never reached)
///
/// Both rows must be present through the live SELECT path: okA (the row applied
/// before the offending one) AND BAD (the offending row itself — an AFTER INSERT
/// trigger fires *after* the row is inserted, so SQLite keeps it under FAIL).
/// Before #5474 the per-row AFTER-trigger handler unconditionally tombstoned the
/// offending row on any trigger error, so a live SELECT saw only okA; #5474
/// removes that ad-hoc tombstone and lets `run_top_level_dml` keep the FAIL
/// changes intact.
#[test]
fn autocommit_after_insert_fail_keeps_pre_offending_and_offending_rows() {
    let mut db = db_with_trigger("AFTER INSERT", "FAIL");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
        }
        other => panic!("expected RAISE(FAIL), got {:?}", other),
    }

    assert!(!db.in_transaction(), "auto-commit must not leak an open transaction");
    // FAIL keeps the row applied before the offending row (okA) AND the offending
    // row itself (BAD — inserted before its AFTER trigger fired); okB was never
    // reached. Crucially this is NOT empty — FAIL did not roll back the whole
    // statement the way ABORT does (see
    // `autocommit_after_insert_abort_rolls_back_whole_statement`).
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(2), SqlValue::Varchar("okA".into())],
            vec![ipk(3), SqlValue::Varchar("BAD".into())],
        ],
    );
}

/// BEFORE INSERT trigger that RAISE(ABORT)s on a later row of a multi-row
/// INSERT, in auto-commit: rows applied before the offending row are undone.
///
/// sqlite3 3.51.0:
///   INSERT INTO t VALUES (2,'okA'),(3,'BAD'),(4,'okB');  -- Error: boom
///   -- no rows inserted.
#[test]
fn autocommit_before_insert_abort_rolls_back_whole_statement() {
    let mut db = db_with_trigger("BEFORE INSERT", "ABORT");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(!db.in_transaction());
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        Vec::<Vec<SqlValue>>::new(),
        "ABORT undoes okA (applied before the BAD row's BEFORE trigger raised)"
    );
}

/// BEFORE INSERT trigger that RAISE(FAIL)s on a later row of a multi-row INSERT,
/// in auto-commit: rows applied before the offending row are KEPT, but the
/// offending row itself is NOT — a BEFORE trigger fires *before* the row is
/// inserted, so the FAILing row never lands. This is the BEFORE/AFTER contrast
/// of #5474: under FAIL an AFTER trigger keeps the offending row, a BEFORE
/// trigger does not.
///
/// sqlite3 3.51.0 (live read):
///   INSERT INTO t VALUES (2,'okA'),(3,'BAD'),(4,'okB');  -- Error: boom
///   SELECT id,v FROM t;  -- 2|okA  (okB never reached; BAD never inserted)
#[test]
fn autocommit_before_insert_fail_keeps_pre_offending_rows_only() {
    let mut db = db_with_trigger("BEFORE INSERT", "FAIL");

    match exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
        }
        other => panic!("expected RAISE(FAIL), got {:?}", other),
    }

    assert!(!db.in_transaction());
    // Only okA survives: the BEFORE trigger raised before the BAD row was
    // inserted, so unlike the AFTER case the offending row is absent.
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![ipk(2), SqlValue::Varchar("okA".into())]],
    );
}

/// A plain multi-row INSERT (no triggers) whose later row violates UNIQUE rolls
/// back the whole statement — SQLite statement atomicity for a non-RAISE error.
///
/// sqlite3 3.51.0:
///   INSERT INTO t VALUES (1,10); -- seed
///   INSERT INTO t VALUES (2,20),(3,30),(1,99),(4,40); -- UNIQUE constraint
///   SELECT id FROM t; -- 1  (rows 2,3 NOT inserted)
#[test]
fn autocommit_multirow_unique_violation_rolls_back_whole_statement() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");

    let err = exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2,20),(3,30),(1,99),(4,40)")
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "expected UNIQUE error, got {err}");

    assert!(!db.in_transaction());
    // Only the pre-existing seed row remains; rows 2 and 3 were rolled back.
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);
}

/// INSERT OR IGNORE skips the conflicting row and KEEPS the others — a
/// conflict-clause that intentionally applies partially is NOT an abort and
/// must not trigger a statement rollback.
///
/// sqlite3 3.51.0:
///   INSERT INTO t VALUES (1,10); -- seed
///   INSERT OR IGNORE INTO t VALUES (2,20),(1,99),(3,30);
///   SELECT id FROM t; -- 1, 2, 3
#[test]
fn autocommit_insert_or_ignore_keeps_non_conflicting_rows() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)");

    let affected = exec_dml(&mut db, "INSERT OR IGNORE INTO t (id, v) VALUES (2,20),(1,99),(3,30)")
        .expect("OR IGNORE must not error on a conflict");
    assert_eq!(affected, 2, "two non-conflicting rows inserted");

    assert!(!db.in_transaction());
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![ipk(1)], vec![ipk(2)], vec![ipk(3)]],
    );
}

/// Single-row INSERT success in auto-commit with a (non-firing) trigger present
/// is unaffected — the implicit-transaction wrapper commits cleanly.
#[test]
fn autocommit_single_row_success_with_trigger_unaffected() {
    let mut db = db_with_trigger("BEFORE INSERT", "ABORT");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'ok')").expect("non-BAD insert succeeds");
    assert!(!db.in_transaction());
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t"),
        vec![vec![ipk(1), SqlValue::Varchar("ok".into())]],
    );
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
    // REPLACE fires conflict-delete triggers only with recursive_triggers ON
    // (SQLite lang_conflict.html; default is now OFF, #5840). Enable it so the
    // BEFORE DELETE trigger runs and its RAISE(IGNORE) can block the conflict.
    db.set_recursive_triggers(true);
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
    // REPLACE fires conflict-delete triggers only with recursive_triggers ON
    // (SQLite lang_conflict.html; default is now OFF, #5840).
    db.set_recursive_triggers(true);
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
    // REPLACE fires conflict-delete triggers only with recursive_triggers ON
    // (SQLite lang_conflict.html; default is now OFF, #5840).
    db.set_recursive_triggers(true);
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
    // REPLACE fires conflict-delete triggers only with recursive_triggers ON
    // (SQLite lang_conflict.html; default is now OFF, #5840).
    db.set_recursive_triggers(true);
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

// ===========================================================================
// Per-variant RAISE scope for the RETURNING and procedural entry points (#5432)
//
// #5417 wrapped the bare `execute` DML entry points in `run_top_level_dml`.
// The RETURNING-clause entry points (`*::execute_returning`) and the
// procedural-context entry points (`*::execute_with_procedural_context`) were
// left unwrapped, so a RAISE fired through them did not get the per-variant
// statement-savepoint scope. #5432 wraps both. Each test below mirrors a
// scenario verified against sqlite3 3.51.0 and asserts final table state via a
// LIVE SELECT (not raw `Table::scan()`), exactly as the #5417 tests do.
//
// Reference scenario (same as #5417): a transaction already holds row 1; a
// single offending statement targets the `BAD` row with a trigger that RAISEs.
//   - ABORT    -> the offending statement is undone; the txn stays open and
//                 earlier statements survive.
//   - FAIL     -> the statement's partial changes are kept; the txn stays open.
//   - ROLLBACK -> the whole transaction is rolled back and closed.
// For RETURNING, when the RAISE aborts the statement the error propagates and
// NO rows are returned.
// ===========================================================================

/// Execute an INSERT ... RETURNING and surface the Result (so a RAISE can be
/// asserted on). On success returns the projected RETURNING rows.
fn exec_insert_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<Vec<SqlValue>>, ExecutorError> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Insert(s) => InsertExecutor::execute_returning(db, &s).map(|outcome| {
            outcome
                .returning
                .map(|r| r.rows.into_iter().map(|row| row.values.to_vec()).collect())
                .unwrap_or_default()
        }),
        other => panic!("Expected INSERT, got {:?}", other),
    }
}

/// Execute an UPDATE ... RETURNING and surface the Result.
fn exec_update_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<Vec<SqlValue>>, ExecutorError> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Update(s) => UpdateExecutor::execute_returning(&s, db).map(|(_, returning)| {
            returning
                .map(|r| r.rows.into_iter().map(|row| row.values.to_vec()).collect())
                .unwrap_or_default()
        }),
        other => panic!("Expected UPDATE, got {:?}", other),
    }
}

/// Execute a DELETE ... RETURNING and surface the Result.
fn exec_delete_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<Vec<SqlValue>>, ExecutorError> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Delete(s) => DeleteExecutor::execute_returning(&s, db).map(|(_, returning)| {
            returning
                .map(|r| r.rows.into_iter().map(|row| row.values.to_vec()).collect())
                .unwrap_or_default()
        }),
        other => panic!("Expected DELETE, got {:?}", other),
    }
}

/// Execute a DML statement through the procedural-context entry point (the path
/// a stored-procedure / script body takes) and surface the Result.
fn exec_dml_procedural(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<usize, ExecutorError> {
    let ctx = crate::procedural::ExecutionContext::new();
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Insert(s) => InsertExecutor::execute_with_procedural_context(db, &s, &ctx),
        Statement::Update(s) => UpdateExecutor::execute_with_procedural_context(&s, db, &ctx),
        Statement::Delete(s) => DeleteExecutor::execute_with_procedural_context(&s, db, &ctx),
        other => panic!("Expected DML, got {:?}", other),
    }
}

// --- RETURNING path ---------------------------------------------------------

/// RAISE(ABORT) in a RETURNING INSERT: the offending statement is undone, the
/// txn stays open and earlier statements survive, and NO rows are returned.
///
/// sqlite3 3.51.0: row 1 ('before') and row 3 ('after') survive; the RETURNING
/// INSERT errors with no rows.
#[test]
fn returning_insert_raise_abort_undoes_statement_keeps_txn() {
    let mut db = db_with_trigger("BEFORE INSERT", "ABORT");
    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_insert_returning(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD') RETURNING id") {
        Err(ExecutorError::Raise { action, message }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
            assert_eq!(message, "boom");
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT must keep the transaction open");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (3, 'after')").expect("stmt 3 ok");
    db.commit_transaction().expect("COMMIT");

    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![ipk(1)], vec![ipk(3)]],
        "ABORT undoes the offending RETURNING INSERT but keeps the txn's other rows"
    );
}

/// RAISE(ROLLBACK) in a RETURNING INSERT: the whole transaction is rolled back
/// and closed.
///
/// sqlite3 3.51.0: no transaction active afterward; even row 1 is gone.
#[test]
fn returning_insert_raise_rollback_aborts_whole_txn() {
    let mut db = db_with_trigger("BEFORE INSERT", "ROLLBACK");
    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_insert_returning(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD') RETURNING id") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
        }
        other => panic!("expected RAISE(ROLLBACK), got {:?}", other),
    }

    assert!(!db.in_transaction(), "ROLLBACK must end the transaction");
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        Vec::<Vec<SqlValue>>::new(),
        "ROLLBACK discards the entire transaction, including earlier rows"
    );
}

/// RAISE(ABORT) vs RAISE(FAIL) in a multi-row RETURNING INSERT with an AFTER
/// trigger: ABORT undoes the whole statement; FAIL keeps the rows applied
/// before the trigger fired (including the offending row, inserted before its
/// AFTER trigger ran). Verified against sqlite3 3.51.0.
#[test]
fn returning_insert_raise_abort_undoes_whole_statement_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "ABORT");
    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_insert_returning(
        &mut db,
        "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB') RETURNING id",
    ) {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![ipk(1)]],
        "ABORT undoes the entire offending RETURNING statement"
    );
}

#[test]
fn returning_insert_raise_fail_keeps_partial_statement_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "FAIL");
    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_insert_returning(
        &mut db,
        "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB') RETURNING id",
    ) {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
        }
        other => panic!("expected RAISE(FAIL), got {:?}", other),
    }

    assert!(db.in_transaction(), "FAIL keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    // FAIL keeps row 1 (earlier stmt), row 2 ('okA') and row 3 ('BAD' — inserted
    // before its AFTER trigger raised); row 4 ('okB') never reached.
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("before".into())],
            vec![ipk(2), SqlValue::Varchar("okA".into())],
            vec![ipk(3), SqlValue::Varchar("BAD".into())],
        ],
    );
}

/// RAISE(ABORT) in a RETURNING UPDATE: the offending statement is undone, the
/// txn stays open, earlier statements survive.
///
/// sqlite3 3.51.0: row 1's update is undone (keeps v='keep'); the prior
/// statement's change to a different row survives.
#[test]
fn returning_update_raise_abort_undoes_statement_keeps_txn() {
    let mut db = db_with_trigger("BEFORE UPDATE", "ABORT");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 'keep')");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 'other')");

    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "UPDATE t SET v = 'changed' WHERE id = 2").expect("prior stmt ok");

    match exec_update_returning(&mut db, "UPDATE t SET v = 'BAD' WHERE id = 1 RETURNING id") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("keep".into())],
            vec![ipk(2), SqlValue::Varchar("changed".into())],
        ],
        "ABORT undoes the offending RETURNING UPDATE; the prior statement survives"
    );
}

/// RAISE(ROLLBACK) in a RETURNING DELETE: the whole transaction is rolled back.
///
/// sqlite3 3.51.0: no transaction active; the prior statement's change is gone
/// and both rows remain at their pre-transaction values.
#[test]
fn returning_delete_raise_rollback_aborts_whole_txn() {
    // A DELETE trigger must key on OLD.v (NEW is undefined for DELETE), so build
    // the schema directly rather than via `db_with_trigger` (which uses NEW.v).
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE DELETE ON t WHEN OLD.v = 'BAD' \
         BEGIN SELECT raise(ROLLBACK, 'boom'); END",
    );
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 'keep')");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')");

    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "UPDATE t SET v = 'changed' WHERE id = 1").expect("prior stmt ok");

    match exec_delete_returning(&mut db, "DELETE FROM t WHERE id = 2 RETURNING id") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
        }
        other => panic!("expected RAISE(ROLLBACK), got {:?}", other),
    }

    assert!(!db.in_transaction(), "ROLLBACK must end the transaction");
    // The prior UPDATE is undone (v back to 'keep'); the BAD row still exists.
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("keep".into())],
            vec![ipk(2), SqlValue::Varchar("BAD".into())],
        ],
        "ROLLBACK discards the whole transaction including the prior UPDATE"
    );
}

// --- Procedural-context path ------------------------------------------------

/// RAISE(ABORT) through the procedural-context INSERT entry point: the offending
/// statement is undone, the txn stays open, earlier statements survive.
#[test]
fn procedural_insert_raise_abort_undoes_statement_keeps_txn() {
    let mut db = db_with_trigger("BEFORE INSERT", "ABORT");
    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT must keep the transaction open");
    exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (3, 'after')").expect("stmt 3 ok");
    db.commit_transaction().expect("COMMIT");

    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        vec![vec![ipk(1)], vec![ipk(3)]],
    );
}

/// RAISE(ROLLBACK) through the procedural-context INSERT entry point: the whole
/// transaction is rolled back and closed.
#[test]
fn procedural_insert_raise_rollback_aborts_whole_txn() {
    let mut db = db_with_trigger("BEFORE INSERT", "ROLLBACK");
    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
        }
        other => panic!("expected RAISE(ROLLBACK), got {:?}", other),
    }

    assert!(!db.in_transaction(), "ROLLBACK must end the transaction");
    assert_eq!(
        select_rows(&db, "SELECT id FROM t ORDER BY id"),
        Vec::<Vec<SqlValue>>::new(),
    );
}

/// RAISE(ABORT) vs RAISE(FAIL) through the procedural-context INSERT entry point
/// with an AFTER trigger and a multi-row statement: ABORT undoes the whole
/// statement; FAIL keeps the rows applied before the trigger fired.
#[test]
fn procedural_insert_raise_abort_undoes_whole_statement_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "ABORT");
    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml_procedural(
        &mut db,
        "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')",
    ) {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);
}

#[test]
fn procedural_insert_raise_fail_keeps_partial_statement_after_trigger() {
    let mut db = db_with_trigger("AFTER INSERT", "FAIL");
    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "INSERT INTO t (id, v) VALUES (1, 'before')").expect("stmt 1 ok");

    match exec_dml_procedural(
        &mut db,
        "INSERT INTO t (id, v) VALUES (2, 'okA'), (3, 'BAD'), (4, 'okB')",
    ) {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Fail);
        }
        other => panic!("expected RAISE(FAIL), got {:?}", other),
    }

    assert!(db.in_transaction(), "FAIL keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("before".into())],
            vec![ipk(2), SqlValue::Varchar("okA".into())],
            vec![ipk(3), SqlValue::Varchar("BAD".into())],
        ],
    );
}

/// RAISE(ABORT) through the procedural-context UPDATE entry point: statement
/// undone, txn stays open, prior statement survives.
#[test]
fn procedural_update_raise_abort_undoes_statement_keeps_txn() {
    let mut db = db_with_trigger("BEFORE UPDATE", "ABORT");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 'keep')");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 'other')");

    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "UPDATE t SET v = 'changed' WHERE id = 2").expect("prior stmt ok");

    match exec_dml_procedural(&mut db, "UPDATE t SET v = 'BAD' WHERE id = 1") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Abort);
        }
        other => panic!("expected RAISE(ABORT), got {:?}", other),
    }

    assert!(db.in_transaction(), "ABORT keeps the transaction open");
    db.commit_transaction().expect("COMMIT");
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("keep".into())],
            vec![ipk(2), SqlValue::Varchar("changed".into())],
        ],
    );
}

/// RAISE(ROLLBACK) through the procedural-context DELETE entry point: the whole
/// transaction is rolled back.
#[test]
fn procedural_delete_raise_rollback_aborts_whole_txn() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER trg BEFORE DELETE ON t WHEN OLD.v = 'BAD' \
         BEGIN SELECT raise(ROLLBACK, 'boom'); END",
    );
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (1, 'keep')");
    exec_ok(&mut db, "INSERT INTO t (id, v) VALUES (2, 'BAD')");

    db.begin_transaction().expect("BEGIN");
    exec_dml_procedural(&mut db, "UPDATE t SET v = 'changed' WHERE id = 1").expect("prior stmt ok");

    match exec_dml_procedural(&mut db, "DELETE FROM t WHERE id = 2") {
        Err(ExecutorError::Raise { action, .. }) => {
            assert_eq!(action, vibesql_ast::RaiseAction::Rollback);
        }
        other => panic!("expected RAISE(ROLLBACK), got {:?}", other),
    }

    assert!(!db.in_transaction(), "ROLLBACK must end the transaction");
    assert_eq!(
        select_rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![ipk(1), SqlValue::Varchar("keep".into())],
            vec![ipk(2), SqlValue::Varchar("BAD".into())],
        ],
    );
}

// ===========================================================================
// #5502: plain constraint violations (UNIQUE / CHECK) inside an explicit
// transaction roll the offending STATEMENT back and keep the transaction open
// — SQLite's default conflict resolution (ABORT scope). This is the same
// `run_inside_transaction` `Err(other)` path the cascade-orphan immediate FK
// flows through; before #5502 it *released* the statement savepoint (leaving
// partial multi-row changes) instead of rolling it back.
//
// These exercise the *trigger-armed* statement-savepoint path: a trigger on
// the table makes `table_may_fire_trigger` true so the statement is wrapped in
// a savepoint. (A constraint violation on a trigger-free table never arms a
// savepoint — it takes the fast pass-through — so it is out of scope here.)
//
// End state verified live against sqlite3 3.51.0: the earlier statement's row
// survives, the violating multi-row statement is fully undone, the txn stays
// open, and a subsequent COMMIT succeeds. This is EXACT sqlite3 parity for the
// plain-constraint case (unlike the cascade-orphan case, where the savepoint's
// whole-statement rollback is a coarser-but-consistent superset of sqlite3's
// partial state).
// ===========================================================================

/// #5502: a multi-row INSERT whose later row violates UNIQUE, on a table that
/// carries a trigger (so the statement savepoint is armed), inside an explicit
/// transaction rolls the whole statement back and keeps the txn open.
#[test]
fn unique_violation_in_explicit_txn_rolls_back_statement_keeps_txn_open() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER UNIQUE)");
    // A trigger anywhere on the table arms the statement savepoint for its DML.
    exec_ok(&mut db, "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

    db.begin_transaction().expect("BEGIN");

    // Earlier statement: survives the later violation.
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 100)").expect("stmt 1 ok");

    // Offending statement: row (3,100) duplicates v=100 -> UNIQUE violation.
    let err = exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 200), (3, 100)")
        .expect_err("expected UNIQUE violation");
    assert!(err.to_string().contains("UNIQUE"), "expected UNIQUE error, got: {err}");

    // Constraint violation is statement-scoped: txn stays open.
    assert!(db.in_transaction(), "UNIQUE violation must keep the transaction open");

    // Whole offending statement undone (row 2 NOT left behind); earlier row 1
    // survives. Matches sqlite3 3.51.0.
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);

    // Recoverable: COMMIT succeeds and persists exactly the earlier statement.
    crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db).expect("COMMIT ok");
    assert!(!db.in_transaction());
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);
}

/// #5502: same statement-rollback scope for a CHECK constraint violation on a
/// later row of a multi-row INSERT inside an explicit transaction.
#[test]
fn check_violation_in_explicit_txn_rolls_back_statement_keeps_txn_open() {
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK (v < 50))");
    exec_ok(&mut db, "CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END");

    db.begin_transaction().expect("BEGIN");
    exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (1, 10)").expect("stmt 1 ok");

    // Row (3,99) violates CHECK (v < 50).
    let err = exec_dml(&mut db, "INSERT INTO t (id, v) VALUES (2, 20), (3, 99)")
        .expect_err("expected CHECK violation");
    assert!(err.to_string().contains("CHECK"), "expected CHECK error, got: {err}");

    assert!(db.in_transaction(), "CHECK violation must keep the transaction open");
    // Whole statement undone (row 2 NOT kept); earlier row 1 survives.
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);

    crate::CommitExecutor::execute(&vibesql_ast::CommitStmt, &mut db).expect("COMMIT ok");
    assert_eq!(select_rows(&db, "SELECT id FROM t ORDER BY id"), vec![vec![ipk(1)]]);
}
