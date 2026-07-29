//! Integration tests for per-database `sqlite_sequence` AUTOINCREMENT
//! bookkeeping (issue #6173).
//!
//! SQLite keeps a SEPARATE `sqlite_sequence` table in every database — `main`
//! plus each connection's `temp` schema. A main table's high-water mark lives
//! in `main.sqlite_sequence`; a TEMP table's lives in `temp.sqlite_sequence`.
//! The two never cross-contaminate, even when a main and a temp AUTOINCREMENT
//! table coexist (autoinc-4.x).
//!
//! Before the fix, the bookkeeping resolved the unqualified name
//! `sqlite_sequence`, which follows SQLite's temp-shadows-main lookup — so once
//! any temp AUTOINCREMENT table existed, EVERY table's counter (main tables
//! included) landed in `temp.sqlite_sequence`, leaving `main.sqlite_sequence`
//! wrongly empty. These tests pin the corrected, schema-qualified routing.
//!
//! NOTE: SQLite's canonical `autoinc.test` section 4 exercises exactly this
//! behavior, but the multi-process VibeSQL TCL shim demotes `CREATE TEMP TABLE`
//! to a persistent main-schema table (there is no long-lived connection to hold
//! a session-scoped temp table across the shim's per-statement CLI processes),
//! so it cannot observe the main-vs-temp `sqlite_sequence` split. These
//! in-process engine tests verify the behavior directly instead.

use vibesql_executor::{CreateTableExecutor, DropTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    match stmt {
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT");
        }
        other => panic!("expected INSERT, got {other:?}"),
    }
}

fn exec_drop_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse DROP TABLE");
    match stmt {
        vibesql_ast::Statement::DropTable(s) => {
            DropTableExecutor::execute(&s, db).expect("DROP TABLE")
        }
        other => panic!("expected DROP TABLE, got {other:?}"),
    };
}

/// Read `(name, seq)` pairs from the `sqlite_sequence` table in `schema`
/// (`"main"` or `"temp"`), sorted by name. Returns an empty vec when the table
/// has no rows.
fn sequence_rows(db: &Database, schema: &str) -> Vec<(String, i64)> {
    let sql = format!("SELECT name, seq FROM {schema}.sqlite_sequence ORDER BY name");
    let stmt = Parser::parse_sql(&sql).expect("parse SELECT sqlite_sequence");
    let rows = match stmt {
        vibesql_ast::Statement::Select(s) => {
            SelectExecutor::new(db).execute_with_columns(&s).expect("SELECT sqlite_sequence").rows
        }
        other => panic!("expected SELECT, got {other:?}"),
    };
    rows.iter()
        .map(|r| {
            let name = match &r.values[0] {
                SqlValue::Varchar(s) => s.to_string(),
                other => panic!("expected text name, got {other:?}"),
            };
            let seq = match &r.values[1] {
                SqlValue::Integer(v) | SqlValue::Bigint(v) => *v,
                SqlValue::Smallint(v) => *v as i64,
                other => panic!("expected integer seq, got {other:?}"),
            };
            (name, seq)
        })
        .collect()
}

/// A main AUTOINCREMENT table's counter stays in `main.sqlite_sequence` and a
/// coexisting TEMP AUTOINCREMENT table's counter stays in
/// `temp.sqlite_sequence` — neither leaks into the other (autoinc-4.4/4.5).
#[test]
fn main_and_temp_autoincrement_sequences_are_isolated() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t3(a INTEGER PRIMARY KEY AUTOINCREMENT, b)");

    exec_insert(&mut db, "INSERT INTO t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO t3 VALUES(20, 2)");
    // NULL rowids auto-allocate to 11 / 21, advancing each high-water mark.
    exec_insert(&mut db, "INSERT INTO t1 VALUES(NULL, 3)");
    exec_insert(&mut db, "INSERT INTO t3 VALUES(NULL, 4)");

    assert_eq!(
        sequence_rows(&db, "main"),
        vec![("t1".to_string(), 11)],
        "main.sqlite_sequence must hold ONLY the main table's high-water mark"
    );
    assert_eq!(
        sequence_rows(&db, "temp"),
        vec![("t3".to_string(), 21)],
        "temp.sqlite_sequence must hold ONLY the temp table's high-water mark"
    );
}

/// Dropping the TEMP table removes only its `temp.sqlite_sequence` row; the main
/// table's `main.sqlite_sequence` row is untouched (autoinc-4.8).
#[test]
fn dropping_temp_table_leaves_main_sequence_intact() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t3(a INTEGER PRIMARY KEY AUTOINCREMENT, b)");
    exec_insert(&mut db, "INSERT INTO t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO t3 VALUES(20, 2)");

    exec_drop_table(&mut db, "DROP TABLE t3");

    assert_eq!(
        sequence_rows(&db, "temp"),
        Vec::<(String, i64)>::new(),
        "dropping the temp table must clear its temp.sqlite_sequence row"
    );
    assert_eq!(
        sequence_rows(&db, "main"),
        vec![("t1".to_string(), 10)],
        "the main table's sqlite_sequence row must survive the temp DROP"
    );
}

/// Dropping the main table removes only its `main.sqlite_sequence` row while the
/// temp table's `temp.sqlite_sequence` row is untouched — the inverse of the
/// case above, and the one the pre-fix temp-shadowing got wrong (autoinc-4.9).
#[test]
fn dropping_main_table_leaves_temp_sequence_intact() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t3(a INTEGER PRIMARY KEY AUTOINCREMENT, b)");
    exec_insert(&mut db, "INSERT INTO t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO t3 VALUES(20, 2)");

    exec_drop_table(&mut db, "DROP TABLE t1");

    assert_eq!(
        sequence_rows(&db, "main"),
        Vec::<(String, i64)>::new(),
        "dropping the main table must clear its main.sqlite_sequence row"
    );
    assert_eq!(
        sequence_rows(&db, "temp"),
        vec![("t3".to_string(), 20)],
        "the temp table's sqlite_sequence row must survive the main DROP"
    );
}
