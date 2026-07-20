//! End-to-end regression tests for issue #6173: STRICT tables must enforce
//! their rigid datatype rules on the *computed* value of a generated column
//! (STORED or VIRTUAL), not only on directly-supplied column values.
//!
//! Before the fix, a generated column whose expression evaluated to a value of
//! the wrong storage class was silently coerced by ordinary affinity rules, so
//! INSERTs that SQLite rejects with `cannot store <T> value in <T> column
//! <tbl>.<col>` succeeded. Mirrors SQLite's `strict1-9.x` tests.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Execute an INSERT, returning Ok(()) or the error's Display text.
fn insert(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {e:?}"))?;
    let Statement::Insert(s) = stmt else {
        panic!("expected INSERT");
    };
    InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
}

/// STORED generated columns whose expression yields a value of the wrong
/// storage class are rejected in a STRICT table (SQLite strict1-9.2.x).
#[test]
fn strict_stored_generated_column_type_mismatch_rejected() {
    let mut db = Database::new();
    create_with_source(
        &mut db,
        "CREATE TABLE gc (
            k INTEGER PRIMARY KEY,
            c1 REAL AS(if(k=13,'x', k=14,x'34', 0.0))   STORED,
            c2 INT  AS(if(k=21,1.5, k=23,'x', 0))       STORED,
            c4 BLOB AS(if(k=42,2, x'00'))               STORED
        ) STRICT;",
    );

    // Valid computed values succeed.
    insert(&mut db, "INSERT INTO gc(k) VALUES(1)").expect("baseline insert should succeed");

    // TEXT computed into a REAL column.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(13)"),
        Err("cannot store TEXT value in REAL column gc.c1".to_string())
    );
    // BLOB computed into a REAL column.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(14)"),
        Err("cannot store BLOB value in REAL column gc.c1".to_string())
    );
    // Non-lossless REAL computed into an INT column.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(21)"),
        Err("cannot store REAL value in INT column gc.c2".to_string())
    );
    // TEXT computed into an INT column.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(23)"),
        Err("cannot store TEXT value in INT column gc.c2".to_string())
    );
    // INT computed into a BLOB column.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(42)"),
        Err("cannot store INT value in BLOB column gc.c4".to_string())
    );
}

/// VIRTUAL generated columns get the same enforcement (SQLite strict1-9.4.x).
#[test]
fn strict_virtual_generated_column_type_mismatch_rejected() {
    let mut db = Database::new();
    create_with_source(
        &mut db,
        "CREATE TABLE gc (
            k INTEGER PRIMARY KEY,
            c3 TEXT AS(if(k=34,x'34', 'x'))   VIRTUAL
        ) STRICT;",
    );

    insert(&mut db, "INSERT INTO gc(k) VALUES(1)").expect("baseline insert should succeed");

    // BLOB computed into a TEXT column is the only rejected case for TEXT.
    assert_eq!(
        insert(&mut db, "INSERT INTO gc(k) VALUES(34)"),
        Err("cannot store BLOB value in TEXT column gc.c3".to_string())
    );
}

/// A non-STRICT table applies ordinary affinity coercion to generated columns
/// and never raises the strict `cannot store` error — the fix must not leak
/// strict enforcement into ordinary tables.
#[test]
fn non_strict_generated_column_uses_affinity() {
    let mut db = Database::new();
    create_with_source(
        &mut db,
        "CREATE TABLE t (
            k INTEGER PRIMARY KEY,
            g REAL AS('x') STORED
        );",
    );
    // 'x' is not numeric; affinity leaves it as-is rather than erroring.
    insert(&mut db, "INSERT INTO t(k) VALUES(1)")
        .expect("non-strict generated column must not raise a strict error");
}
