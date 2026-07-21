//! Cross-update UNIQUE/PK conflict error-message format.
//!
//! When a single UPDATE would land multiple rows on the same PRIMARY KEY or
//! UNIQUE value, sqlite3 3.51.0 reports `UNIQUE constraint failed: t1.a` — each
//! offending column qualified by its table name, with no parenthetical suffix.
//! Before the fix VibeSQL emitted `UNIQUE constraint failed: a (multiple rows
//! would have same key)`, which failed triggerC-1.15. Part of #6176.

use vibesql_executor::{CreateTableExecutor, ExecutorError, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn run_update_err(db: &mut Database, sql: &str) -> ExecutorError {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse UPDATE");
    let Statement::Update(update) = stmt else { panic!("expected UPDATE") };
    UpdateExecutor::execute(&update, db).expect_err("expected constraint violation")
}

/// triggerC-1.15: collapsing every row onto one INTEGER PRIMARY KEY value names
/// the column as `t1.a`.
#[test]
fn cross_update_primary_key_collision_message_is_table_qualified() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE, c, d, e)");
    exec(&mut db, "INSERT INTO t1 VALUES(1,2,3,4,5)");
    exec(&mut db, "INSERT INTO t1 VALUES(6,7,8,9,10)");
    exec(&mut db, "INSERT INTO t1 VALUES(11,12,13,14,15)");

    match run_update_err(&mut db, "UPDATE t1 SET a=100") {
        ExecutorError::ConstraintViolation(msg) => {
            assert_eq!(msg, "UNIQUE constraint failed: t1.a", "got: {msg}");
        }
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }
}

/// A UNIQUE (non-PK) collision names the column as `t1.b`.
#[test]
fn cross_update_unique_collision_message_is_table_qualified() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE)");
    exec(&mut db, "INSERT INTO t1 VALUES(1,2)");
    exec(&mut db, "INSERT INTO t1 VALUES(6,7)");

    match run_update_err(&mut db, "UPDATE t1 SET b=50") {
        ExecutorError::ConstraintViolation(msg) => {
            assert_eq!(msg, "UNIQUE constraint failed: t1.b", "got: {msg}");
        }
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }
}

/// A composite PRIMARY KEY qualifies every column: `t3.c, t3.d`.
#[test]
fn cross_update_composite_key_collision_message_qualifies_each_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t3(c, d, PRIMARY KEY(c, d))");
    exec(&mut db, "INSERT INTO t3 VALUES(1, 2)");
    exec(&mut db, "INSERT INTO t3 VALUES(3, 4)");

    match run_update_err(&mut db, "UPDATE t3 SET c=9, d=9") {
        ExecutorError::ConstraintViolation(msg) => {
            assert_eq!(msg, "UNIQUE constraint failed: t3.c, t3.d", "got: {msg}");
        }
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }
}
