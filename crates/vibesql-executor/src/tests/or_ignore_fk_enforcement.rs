//! Tests that the `OR IGNORE` conflict-resolution algorithm does NOT suppress
//! FOREIGN KEY constraint enforcement (fkey2-20.2.*/20.3.*, issue #6170).
//!
//! Per SQLite, an ON CONFLICT clause (`OR IGNORE`, untargeted DO NOTHING, ...)
//! applies only to NOT NULL / UNIQUE / PRIMARY KEY / CHECK constraints. It is
//! explicitly documented to have **no effect on FOREIGN KEY constraints**: an
//! immediate FK violation always raises "FOREIGN KEY constraint failed" and a
//! DEFERRABLE one is still queued for COMMIT-time checking. Previously VibeSQL
//! treated a missing parent key as an "ignorable conflict" and silently dropped
//! the row (and did so even when `PRAGMA foreign_keys` was OFF, wrongly skipping
//! a perfectly valid row). These tests lock in the corrected behaviour.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single DDL/DML statement, returning a display string on
/// success or the error message on failure.
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

fn setup(db: &mut Database) {
    exec(db, "CREATE TABLE pp(a PRIMARY KEY, b)").unwrap();
    exec(db, "CREATE TABLE cc(c PRIMARY KEY, d REFERENCES pp)").unwrap();
}

#[test]
fn insert_or_ignore_still_raises_immediate_fk_violation() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    setup(&mut db);

    // Parent key 2 does not exist: OR IGNORE must NOT swallow the FK error.
    let err = exec(&mut db, "INSERT OR IGNORE INTO cc VALUES(1, 2)").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "expected a FOREIGN KEY error, got: {err}");

    // Nothing was inserted (statement aborted).
    assert_eq!(query_all(&db, "SELECT count(*) FROM cc"), vec![SqlValue::Integer(0)]);
}

#[test]
fn update_or_ignore_still_raises_immediate_fk_violation() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    setup(&mut db);

    exec(&mut db, "INSERT INTO pp VALUES(2, 'two')").unwrap();
    exec(&mut db, "INSERT INTO cc VALUES(1, 2)").unwrap();

    // Repointing the child at a non-existent parent key must error even under
    // OR IGNORE (fkey2-20.3.*).
    let err = exec(&mut db, "UPDATE OR IGNORE cc SET d = 5").unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint"), "expected a FOREIGN KEY error, got: {err}");

    // The row is unchanged (still points at parent key 2).
    assert_eq!(query_all(&db, "SELECT d FROM cc WHERE c = 1"), vec![SqlValue::Integer(2)]);
}

#[test]
fn insert_or_ignore_inserts_valid_row_when_foreign_keys_disabled() {
    // With PRAGMA foreign_keys = OFF (the default), a missing parent key is not
    // a violation at all, so OR IGNORE must insert the row rather than treating
    // the absent parent as an ignorable conflict and silently dropping it.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(false);
    setup(&mut db);

    exec(&mut db, "INSERT OR IGNORE INTO cc VALUES(1, 2)").unwrap();
    assert_eq!(query_all(&db, "SELECT count(*) FROM cc"), vec![SqlValue::Integer(1)]);
}

#[test]
fn insert_or_ignore_still_ignores_unique_conflicts() {
    // Regression guard: OR IGNORE must keep suppressing genuine PRIMARY KEY /
    // UNIQUE conflicts (the FK carve-out must not disable ordinary conflict
    // resolution).
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    setup(&mut db);

    exec(&mut db, "INSERT INTO pp VALUES(2, 'two')").unwrap();
    exec(&mut db, "INSERT INTO cc VALUES(1, 2)").unwrap();
    // Second row has the same PRIMARY KEY (c = 1) and a valid parent key: the
    // PK conflict is silently ignored, leaving exactly one row.
    exec(&mut db, "INSERT OR IGNORE INTO cc VALUES(1, 2)").unwrap();

    assert_eq!(query_all(&db, "SELECT count(*) FROM cc"), vec![SqlValue::Integer(1)]);
}
