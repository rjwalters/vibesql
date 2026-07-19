//! Tests that REPLACE performs foreign-key processing on the rows it deletes
//! to clear a PRIMARY KEY / UNIQUE conflict (fkey2-13.*, issue #6170).
//!
//! When a REPLACE deletes a conflicting parent row, any child row that
//! referenced that parent's key would be orphaned. For a NO ACTION foreign
//! key SQLite checks this at statement end — after the REPLACE re-inserts its
//! new row — so an orphan is repaired when the re-inserted row restores the
//! same parent-key value. These tests lock in that behaviour (verified against
//! the SQLite fkey2.test expectations).

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
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
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

/// pp(a UNIQUE, b, c, PRIMARY KEY(b, c)); cc(d, e, f UNIQUE, FK(d,e) -> pp).
fn setup(db: &mut Database) {
    db.set_foreign_keys_enabled(true);
    exec(db, "CREATE TABLE pp(a UNIQUE, b, c, PRIMARY KEY(b, c))").unwrap();
    exec(db, "CREATE TABLE cc(d, e, f UNIQUE, FOREIGN KEY(d, e) REFERENCES pp)").unwrap();
    exec(db, "INSERT INTO pp VALUES(1, 2, 3)").unwrap();
    exec(db, "INSERT INTO cc VALUES(2, 3, 1)").unwrap();
}

#[test]
fn replace_dropping_referenced_key_raises_fk_error() {
    // fkey2-13.1.1: REPLACE INTO pp VALUES(1, 4, 5) conflicts on UNIQUE(a=1),
    // so the parent row (b=2,c=3) is deleted. The new row provides (b=4,c=5),
    // which does NOT restore the key cc references (2,3) -> orphan -> error.
    let mut db = Database::new();
    setup(&mut db);

    let err = exec(&mut db, "REPLACE INTO pp VALUES(1, 4, 5)").unwrap_err();
    assert!(
        err.contains("FOREIGN KEY") || err.to_lowercase().contains("foreign key"),
        "expected a foreign-key violation, got: {err}"
    );

    // The failed REPLACE must leave both tables untouched.
    assert_eq!(
        query_all(&db, "SELECT a, b, c FROM pp"),
        vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)],
    );
    assert_eq!(
        query_all(&db, "SELECT d, e, f FROM cc"),
        vec![SqlValue::Integer(2), SqlValue::Integer(3), SqlValue::Integer(1)],
    );
}

#[test]
fn replace_restoring_referenced_key_succeeds() {
    // fkey2-13.1.3 variant: REPLACE the parent row on its PRIMARY KEY(b, c)
    // with a new row that carries the SAME (b, c) = (2, 3). The child's
    // reference is repaired by the re-inserted row, so the REPLACE succeeds.
    let mut db = Database::new();
    setup(&mut db);

    // Conflicts on PK(b, c) = (2, 3); new row keeps (b, c) = (2, 3) but
    // changes a from 1 to 9. cc(2, 3) still finds a parent afterwards.
    exec(&mut db, "REPLACE INTO pp VALUES(9, 2, 3)").expect("REPLACE should succeed");

    assert_eq!(
        query_all(&db, "SELECT a, b, c FROM pp"),
        vec![SqlValue::Integer(9), SqlValue::Integer(2), SqlValue::Integer(3)],
    );
    assert_eq!(
        query_all(&db, "SELECT d, e, f FROM cc"),
        vec![SqlValue::Integer(2), SqlValue::Integer(3), SqlValue::Integer(1)],
    );
}

#[test]
fn replace_without_children_referencing_deleted_key_succeeds() {
    // A REPLACE that deletes a parent row no child references must still work.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec(&mut db, "CREATE TABLE pp(a UNIQUE, b, c, PRIMARY KEY(b, c))").unwrap();
    exec(&mut db, "CREATE TABLE cc(d, e, f UNIQUE, FOREIGN KEY(d, e) REFERENCES pp)").unwrap();
    exec(&mut db, "INSERT INTO pp VALUES(1, 2, 3)").unwrap();
    exec(&mut db, "INSERT INTO pp VALUES(7, 8, 9)").unwrap();
    // Only (2,3) is referenced; deleting (8,9) via REPLACE is fine.
    exec(&mut db, "INSERT INTO cc VALUES(2, 3, 1)").unwrap();

    exec(&mut db, "REPLACE INTO pp VALUES(7, 10, 11)").expect("REPLACE should succeed");

    // (8,9) replaced by (10,11); the referenced (2,3) is untouched.
    assert_eq!(
        query_all(&db, "SELECT a, b, c FROM pp ORDER BY a"),
        vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
            SqlValue::Integer(7),
            SqlValue::Integer(10),
            SqlValue::Integer(11),
        ],
    );
}

#[test]
fn replace_ignored_when_foreign_keys_disabled() {
    // With PRAGMA foreign_keys = off, the REPLACE-delete is not FK-checked.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(false);
    exec(&mut db, "CREATE TABLE pp(a UNIQUE, b, c, PRIMARY KEY(b, c))").unwrap();
    exec(&mut db, "CREATE TABLE cc(d, e, f UNIQUE, FOREIGN KEY(d, e) REFERENCES pp)").unwrap();
    exec(&mut db, "INSERT INTO pp VALUES(1, 2, 3)").unwrap();
    exec(&mut db, "INSERT INTO cc VALUES(2, 3, 1)").unwrap();

    // Would orphan cc(2,3) but FK enforcement is off -> succeeds.
    exec(&mut db, "REPLACE INTO pp VALUES(1, 4, 5)").expect("REPLACE should succeed with FK off");

    assert_eq!(
        query_all(&db, "SELECT a, b, c FROM pp"),
        vec![SqlValue::Integer(1), SqlValue::Integer(4), SqlValue::Integer(5)],
    );
}
