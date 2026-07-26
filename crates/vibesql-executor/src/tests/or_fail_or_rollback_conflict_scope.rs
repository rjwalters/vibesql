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
