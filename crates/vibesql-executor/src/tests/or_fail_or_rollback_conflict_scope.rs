//! Tests for the `OR FAIL` / `OR ROLLBACK` conflict-resolution algorithms on
//! INSERT/UPDATE (issue #6193, e_insert-4.1.1.7/1.9, e_update-1.8.x).
//!
//! Per SQLite (lang_conflict.html):
//! - `OR FAIL` stops the statement at the first constraint violation but
//!   KEEPS every row change the statement already applied before that row.
//! - `OR ROLLBACK` stops the statement at the first constraint violation and
//!   rolls back the ENTIRE enclosing transaction (autocommit is re-enabled
//!   even when the violation happened inside an explicit `BEGIN`).
//! - Both are exempt from a FOREIGN KEY constraint violation, which always
//!   behaves as the default ABORT regardless of the statement's own
//!   conflict-resolution clause (fkey2-20.2.6.x / 20.3.6.x, mirroring the
//!   existing `OR IGNORE` carve-out in `or_ignore_fk_enforcement.rs`).

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single DDL/DML/transaction-control statement,
/// returning a display string on success or the error message on failure.
fn exec(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::Update(s) => crate::UpdateExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) updated", count))
            .map_err(|e| e.to_string()),
        Statement::BeginTransaction(_) => {
            db.begin_transaction().map_err(|e| e.to_string())?;
            Ok("began".to_string())
        }
        Statement::Commit(_) => {
            db.commit_transaction().map_err(|e| e.to_string())?;
            Ok("committed".to_string())
        }
        other => Err(format!("Unsupported statement type: {:?}", other)),
    }
}

/// Run a SELECT and return every value of every row flattened in row order.
fn query_all(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let result = crate::SelectExecutor::new(db).execute_with_columns(&select).expect("run select");
    result.rows.iter().flat_map(|r| r.values.clone()).collect()
}

fn setup_a4(db: &mut Database) {
    exec(db, "CREATE TABLE a4(c UNIQUE, d)").unwrap();
    exec(db, "INSERT INTO a4 VALUES(1, 'a')").unwrap();
}

#[test]
fn insert_or_fail_keeps_rows_applied_before_the_conflict() {
    // e_insert-4.1.1.9: the first SELECT row (4, 'e') has no conflict and must
    // survive even though the statement as a whole errors on the second row.
    let mut db = Database::new();
    setup_a4(&mut db);

    let err =
        exec(&mut db, "INSERT OR FAIL INTO a4 SELECT 4, 'e' UNION ALL SELECT 1, 'e'").unwrap_err();
    assert!(err.contains("UNIQUE constraint failed"), "expected a UNIQUE error, got: {err}");

    assert_eq!(
        query_all(&db, "SELECT c, d FROM a4 ORDER BY d, c"),
        vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("a")),
            SqlValue::Integer(4),
            SqlValue::Varchar(arcstr::ArcStr::from("e")),
        ]
    );
}

#[test]
fn update_or_fail_surfaces_not_null_violation_on_a_later_row() {
    // Doctor regression for #6193: `UPDATE OR FAIL` that hits a NOT NULL
    // violation on a NON-first row must (a) keep the rows updated before it and
    // (b) still surface the NOT NULL error — not silently report success.
    //
    // The bug: the collect loop stashed `fail_error = Some(NOT NULL...)` and
    // broke, but the unconditional `fail_error = truncate_updates_for_or_fail(..)`
    // that follows overwrote it back to `None` whenever the collected prefix had
    // no PK/UNIQUE conflict among itself (the common case) — discarding the real
    // error. Here row 1's update is applied, row 2 (b -> NULL) is the offending
    // row, and there is no PK/UNIQUE conflict in the kept prefix.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER NOT NULL)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1,1),(2,2),(3,3)").unwrap();

    let err = exec(
        &mut db,
        "UPDATE OR FAIL t SET b = CASE a WHEN 1 THEN 100 WHEN 2 THEN NULL ELSE 300 END",
    )
    .unwrap_err();
    assert!(err.contains("NOT NULL constraint failed"), "expected a NOT NULL error, got: {err}");

    // Row 1's update (b = 100) was applied before the offending row and is kept;
    // rows 2 and 3 are untouched (the statement stopped at row 2).
    assert_eq!(
        query_all(&db, "SELECT a, b FROM t ORDER BY a"),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(100),
            SqlValue::Integer(2),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
            SqlValue::Integer(3),
        ]
    );
}

#[test]
fn update_or_fail_surfaces_not_null_violation_without_a_primary_key() {
    // Same as above but with no PRIMARY KEY / UNIQUE key space at all, so
    // `truncate_updates_for_or_fail` returns `None` via its empty-key-spaces
    // early-out — the pre-existing NOT NULL error must survive that path too.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(a INTEGER, b INTEGER NOT NULL)").unwrap();
    exec(&mut db, "INSERT INTO t2 VALUES(1,1),(2,2),(3,3)").unwrap();

    let err = exec(
        &mut db,
        "UPDATE OR FAIL t2 SET b = CASE a WHEN 1 THEN 100 WHEN 2 THEN NULL ELSE 300 END",
    )
    .unwrap_err();
    assert!(err.contains("NOT NULL constraint failed"), "expected a NOT NULL error, got: {err}");

    assert_eq!(
        query_all(&db, "SELECT a, b FROM t2 ORDER BY a"),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(100),
            SqlValue::Integer(2),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
            SqlValue::Integer(3),
        ]
    );
}

#[test]
fn insert_default_abort_still_discards_the_whole_statement() {
    // Regression guard: without an OR clause (default ABORT), a multi-row
    // conflict must still roll back every row the statement applied,
    // matching the pre-existing behavior this change must not disturb.
    let mut db = Database::new();
    setup_a4(&mut db);

    let err = exec(&mut db, "INSERT INTO a4 SELECT 4, 'e' UNION ALL SELECT 1, 'e'").unwrap_err();
    assert!(err.contains("UNIQUE constraint failed"), "expected a UNIQUE error, got: {err}");

    assert_eq!(query_all(&db, "SELECT count(*) FROM a4"), vec![SqlValue::Integer(1)]);
}

#[test]
fn insert_or_rollback_aborts_the_enclosing_explicit_transaction() {
    // e_insert-4.1.1.7: `OR ROLLBACK` inside an explicit transaction must
    // terminate the whole transaction (a subsequent COMMIT has nothing left
    // to commit), not just undo the offending statement.
    let mut db = Database::new();
    setup_a4(&mut db);

    exec(&mut db, "BEGIN").unwrap();
    let err = exec(&mut db, "INSERT OR ROLLBACK INTO a4 VALUES(1, 'd')").unwrap_err();
    assert!(err.contains("UNIQUE constraint failed"), "expected a UNIQUE error, got: {err}");

    // The transaction was already rolled back by the conflict, so COMMIT has
    // nothing to commit and must fail rather than silently succeeding.
    assert!(exec(&mut db, "COMMIT").is_err(), "COMMIT should fail: no active transaction");

    // Only the original row survives.
    assert_eq!(query_all(&db, "SELECT count(*) FROM a4"), vec![SqlValue::Integer(1)]);
}

#[test]
fn insert_or_fail_does_not_apply_to_a_foreign_key_violation() {
    // A FOREIGN KEY violation is exempt from `OR FAIL` (as from every other
    // conflict clause) — SQLite always aborts the whole statement for it,
    // never treating it as an "ignorable"/"keep-partial" conflict
    // (fkey2-20.2.6.x).
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec(&mut db, "CREATE TABLE pp(a PRIMARY KEY, b)").unwrap();
    exec(&mut db, "CREATE TABLE cc(c PRIMARY KEY, d REFERENCES pp)").unwrap();

    let err = exec(&mut db, "INSERT OR FAIL INTO cc VALUES(1, 2)").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "expected a FOREIGN KEY error, got: {err}");
    assert_eq!(query_all(&db, "SELECT count(*) FROM cc"), vec![SqlValue::Integer(0)]);
}

#[test]
fn insert_or_rollback_does_not_apply_to_a_foreign_key_violation() {
    // Same carve-out for `OR ROLLBACK`: a FK violation inside an explicit
    // transaction must NOT terminate the transaction — it gets the default
    // (statement-only) ABORT scope, leaving earlier statements intact.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec(&mut db, "CREATE TABLE pp(a PRIMARY KEY, b)").unwrap();
    exec(&mut db, "CREATE TABLE cc(c PRIMARY KEY, d REFERENCES pp)").unwrap();

    exec(&mut db, "BEGIN").unwrap();
    exec(&mut db, "INSERT INTO pp VALUES(2, 'two')").unwrap();
    let err = exec(&mut db, "INSERT OR ROLLBACK INTO cc VALUES(1, 5)").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "expected a FOREIGN KEY error, got: {err}");

    // The transaction is still open: COMMIT must succeed and the earlier
    // INSERT INTO pp survives.
    exec(&mut db, "COMMIT").expect("COMMIT should still succeed: transaction stayed open");
    assert_eq!(query_all(&db, "SELECT count(*) FROM pp"), vec![SqlValue::Integer(1)]);
}
