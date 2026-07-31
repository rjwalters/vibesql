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

use vibesql_executor::{
    CreateTableExecutor, DropTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
};
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

// ---------------------------------------------------------------------------
// Same-name collision: a table named `t1` in BOTH main and temp (issue #6350).
// An explicitly-qualified INSERT must route its sqlite_sequence bookkeeping to
// the qualified schema, not re-resolve the bare name via temp-shadows-main.
// ---------------------------------------------------------------------------

/// Helper: read the `x` (INTEGER PRIMARY KEY) column of `schema.t1`, sorted.
fn t1_rowids(db: &Database, schema: &str) -> Vec<i64> {
    let sql = format!("SELECT x FROM {schema}.t1 ORDER BY x");
    let stmt = Parser::parse_sql(&sql).expect("parse SELECT t1");
    let rows = match stmt {
        vibesql_ast::Statement::Select(s) => {
            SelectExecutor::new(db).execute_with_columns(&s).expect("SELECT t1").rows
        }
        other => panic!("expected SELECT, got {other:?}"),
    };
    rows.iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(v) | SqlValue::Bigint(v) => *v,
            other => panic!("expected integer rowid, got {other:?}"),
        })
        .collect()
}

/// The issue #6350 repro: with same-named AUTOINCREMENT tables in main and
/// temp, `INSERT INTO main.t1` bumps `main.sqlite_sequence` and
/// `INSERT INTO temp.t1` bumps `temp.sqlite_sequence` — the main insert's
/// high-water mark is never absorbed into (or overwritten by) the temp one.
#[test]
fn qualified_inserts_with_same_named_tables_route_to_own_schema() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");

    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO temp.t1 VALUES(20, 2)");

    assert_eq!(
        sequence_rows(&db, "main"),
        vec![("t1".to_string(), 10)],
        "main.sqlite_sequence must hold the qualified main insert's high-water mark"
    );
    assert_eq!(
        sequence_rows(&db, "temp"),
        vec![("t1".to_string(), 20)],
        "temp.sqlite_sequence must hold ONLY the qualified temp insert's high-water mark"
    );
}

/// The READ path: a NULL-IPK insert into `main.t1` must consult
/// `main.sqlite_sequence`, not the same-named temp table's (higher) counter —
/// and vice versa (issue #6350).
#[test]
fn qualified_null_ipk_insert_reads_own_schema_counter() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO temp.t1 VALUES(100, 2)");

    // Must allocate 11 (main's counter + 1), NOT 101 (temp's counter + 1).
    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(NULL, 3)");
    assert_eq!(
        t1_rowids(&db, "main"),
        vec![10, 11],
        "NULL IPK into main.t1 must continue main's own sequence, not temp's"
    );

    // And the temp side continues from its own counter.
    exec_insert(&mut db, "INSERT INTO temp.t1 VALUES(NULL, 4)");
    assert_eq!(t1_rowids(&db, "temp"), vec![100, 101]);

    assert_eq!(sequence_rows(&db, "main"), vec![("t1".to_string(), 11)]);
    assert_eq!(sequence_rows(&db, "temp"), vec![("t1".to_string(), 101)]);
}

/// Unqualified INSERT behavior is unchanged: with both tables present,
/// temp still shadows main for a bare `t1`, so the bookkeeping lands in
/// `temp.sqlite_sequence` (SQLite name-resolution order).
#[test]
fn unqualified_insert_still_temp_shadows_main() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");

    exec_insert(&mut db, "INSERT INTO t1 VALUES(30, 1)");

    assert_eq!(
        sequence_rows(&db, "main"),
        Vec::<(String, i64)>::new(),
        "an unqualified insert resolves to the temp table; main's sequence stays empty"
    );
    assert_eq!(sequence_rows(&db, "temp"), vec![("t1".to_string(), 30)]);
    assert_eq!(t1_rowids(&db, "temp"), vec![30]);
    assert_eq!(t1_rowids(&db, "main"), Vec::<i64>::new());
}

/// A qualified `INSERT ... SELECT` whose source is empty still creates the
/// `(t1, 0)` sqlite_sequence row (autoinc-9.1) — in the QUALIFIED schema, not
/// the temp-shadowed one (issue #6350).
#[test]
fn qualified_empty_insert_select_creates_zero_row_in_own_schema() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TABLE src(x INTEGER, y)");

    exec_insert(&mut db, "INSERT INTO main.t1 SELECT * FROM src");

    assert_eq!(
        sequence_rows(&db, "main"),
        vec![("t1".to_string(), 0)],
        "the (t1, 0) row for an empty qualified INSERT ... SELECT belongs to main"
    );
    assert_eq!(
        sequence_rows(&db, "temp"),
        Vec::<(String, i64)>::new(),
        "temp.sqlite_sequence must not absorb main.t1's bookkeeping"
    );
}

/// The post-BEFORE-trigger rowid recompute (`execution.rs`) must also consult
/// the qualified schema's counter: a NULL-IPK insert into `main.t1` whose
/// BEFORE INSERT trigger fires still allocates from main's sequence, not the
/// same-named temp table's higher counter (issue #6350).
#[test]
fn before_insert_trigger_recompute_uses_qualified_schema_counter() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TABLE log(n INTEGER)");
    // Bind the trigger to main.t1 BEFORE the same-named temp table exists.
    let trigger_sql = "CREATE TRIGGER t1_before BEFORE INSERT ON t1 \
                       BEGIN INSERT INTO log VALUES(1); END";
    let stmt = Parser::parse_sql(trigger_sql).expect("parse CREATE TRIGGER");
    match stmt {
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(&mut db, &s, Some(trigger_sql))
                .expect("CREATE TRIGGER");
        }
        other => panic!("expected CREATE TRIGGER, got {other:?}"),
    }
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");

    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO temp.t1 VALUES(100, 2)");

    // The trigger forces the post-trigger recompute path; the recomputed
    // rowid must be 11 (main's counter + 1), NOT 101 (temp's counter + 1).
    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(NULL, 3)");

    assert_eq!(
        t1_rowids(&db, "main"),
        vec![10, 11],
        "post-trigger recompute must continue main's own sequence, not temp's"
    );
    assert_eq!(sequence_rows(&db, "main"), vec![("t1".to_string(), 11)]);
    assert_eq!(sequence_rows(&db, "temp"), vec![("t1".to_string(), 100)]);
}

/// Regression pin for the already-correct DROP path: `DROP TABLE main.t1`
/// removes only main's sequence row; the same-named temp table's row survives.
#[test]
fn dropping_qualified_main_table_leaves_same_named_temp_sequence() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y)");
    exec_insert(&mut db, "INSERT INTO main.t1 VALUES(10, 1)");
    exec_insert(&mut db, "INSERT INTO temp.t1 VALUES(20, 2)");

    exec_drop_table(&mut db, "DROP TABLE main.t1");

    assert_eq!(
        sequence_rows(&db, "main"),
        Vec::<(String, i64)>::new(),
        "dropping main.t1 must clear only main's sequence row"
    );
    assert_eq!(
        sequence_rows(&db, "temp"),
        vec![("t1".to_string(), 20)],
        "the same-named temp table's sequence row must survive DROP TABLE main.t1"
    );
}
