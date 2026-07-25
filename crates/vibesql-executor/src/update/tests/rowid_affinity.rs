//! Rowid (INTEGER) affinity on `UPDATE` assignments to the rowid — either the
//! INTEGER PRIMARY KEY alias column or the virtual `rowid` pseudo-column
//! (trigger1-15.1, issue #6176).
//!
//! sqlite3 3.51.0 semantics (verified against the reference binary):
//!   - a TEXT/REAL value that is losslessly an integer is coerced
//!     (`SET a='5'` stores 5; `SET a=7.0` stores 7);
//!   - anything else — non-numeric TEXT, BLOB, fractional REAL, and (unlike
//!     INSERT, where it means auto-assign) NULL — raises `datatype mismatch`.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use crate::*;

/// Execute a non-query statement (CREATE/INSERT/CREATE TRIGGER).
fn ddl(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT");
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER");
        }
        other => panic!("unsupported ddl: {:?}", other),
    }
}

/// Run an UPDATE, returning the executor result (so affinity errors are observable).
fn update(db: &mut vibesql_storage::Database, sql: &str) -> Result<usize, crate::ExecutorError> {
    match Parser::parse_sql(sql).expect("parse update") {
        Statement::Update(s) => UpdateExecutor::execute(&s, db),
        other => panic!("expected UPDATE, got {:?}", other),
    }
}

/// Run a SELECT and return the single column of the single row.
fn select_one(db: &mut vibesql_storage::Database, sql: &str) -> SqlValue {
    match Parser::parse_sql(sql).expect("parse select") {
        Statement::Select(s) => {
            let rows = SelectExecutor::new(db).execute(&s).expect("SELECT");
            assert_eq!(rows.len(), 1, "expected one row from {sql}");
            rows[0].values[0].clone()
        }
        other => panic!("expected SELECT, got {:?}", other),
    }
}

fn assert_datatype_mismatch(result: Result<usize, crate::ExecutorError>, context: &str) {
    let err = result.expect_err(context);
    assert!(
        err.to_string().contains("datatype mismatch"),
        "{context}: expected datatype mismatch, got: {err}"
    );
}

fn ipk_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE tA(a INTEGER PRIMARY KEY, b, c)");
    ddl(&mut db, "INSERT INTO tA VALUES(1, 2, 3)");
    db
}

/// trigger1-15.1: `UPDATE tA SET a='abc'` on an INTEGER PRIMARY KEY column is
/// `datatype mismatch`, including when a BEFORE UPDATE trigger exists (the
/// trigger must not mask the affinity check).
#[test]
fn update_ipk_to_nonnumeric_text_errors() {
    let mut db = ipk_db();
    ddl(&mut db, "CREATE TRIGGER tA_trigger BEFORE UPDATE ON tA BEGIN SELECT 1; END");
    assert_datatype_mismatch(update(&mut db, "UPDATE tA SET a = 'abc'"), "SET a='abc'");
    // Row unchanged.
    assert_eq!(select_one(&mut db, "SELECT a FROM tA"), SqlValue::Integer(1));
}

/// NULL is valid on INSERT (auto-assign) but a `datatype mismatch` on UPDATE.
#[test]
fn update_ipk_to_null_errors() {
    let mut db = ipk_db();
    assert_datatype_mismatch(update(&mut db, "UPDATE tA SET a = NULL"), "SET a=NULL");
}

/// Fractional REAL cannot be a rowid.
#[test]
fn update_ipk_to_fractional_real_errors() {
    let mut db = ipk_db();
    assert_datatype_mismatch(update(&mut db, "UPDATE tA SET a = 7.5"), "SET a=7.5");
}

/// Lossless TEXT / REAL integers coerce (SQLite INTEGER affinity).
#[test]
fn update_ipk_lossless_coercions() {
    let mut db = ipk_db();
    assert_eq!(update(&mut db, "UPDATE tA SET a = '5'").unwrap(), 1);
    assert_eq!(select_one(&mut db, "SELECT a FROM tA"), SqlValue::Integer(5));

    assert_eq!(update(&mut db, "UPDATE tA SET a = 7.0").unwrap(), 1);
    assert_eq!(select_one(&mut db, "SELECT a FROM tA"), SqlValue::Integer(7));
}

/// `SET rowid = ...` routed through the rowid keyword on an IPK table applies
/// the same affinity rules.
#[test]
fn update_rowid_keyword_on_ipk_table() {
    let mut db = ipk_db();
    assert_datatype_mismatch(update(&mut db, "UPDATE tA SET rowid = 'abc'"), "SET rowid='abc'");
    assert_eq!(update(&mut db, "UPDATE tA SET rowid = '9'").unwrap(), 1);
    assert_eq!(select_one(&mut db, "SELECT a FROM tA"), SqlValue::Integer(9));
}

/// Virtual-rowid tables (no INTEGER PRIMARY KEY): same rules for
/// `SET rowid = ...`, including the NULL rejection.
#[test]
fn update_virtual_rowid_affinity() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE tB(a, b)");
    ddl(&mut db, "INSERT INTO tB VALUES(1, 2)");

    assert_datatype_mismatch(update(&mut db, "UPDATE tB SET rowid = 'abc'"), "SET rowid='abc'");
    assert_datatype_mismatch(update(&mut db, "UPDATE tB SET rowid = NULL"), "SET rowid=NULL");
    assert_datatype_mismatch(update(&mut db, "UPDATE tB SET rowid = 2.5"), "SET rowid=2.5");

    // Lossless TEXT integer relocates the row.
    assert_eq!(update(&mut db, "UPDATE tB SET rowid = '42'").unwrap(), 1);
    match select_one(&mut db, "SELECT rowid FROM tB") {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => assert_eq!(i, 42),
        other => panic!("expected integer rowid, got {other:?}"),
    }
}
