//! Regression tests for issue #6174 (altertab.test 5.x / 13.2): the "after
//! rename" schema re-check SQLite runs once `ALTER TABLE` has rewritten its
//! dependents, and `temp.`-qualified RENAME / trigger targets.
//!
//! - `ALTER TABLE t2 RENAME TO one` must abort with `error in view v after rename: ambiguous column
//!   name: one.a` when a dependent view already aliases another relation `one` (the qualified
//!   reference then matches two FROM items), leaving the schema untouched.
//! - `ALTER TABLE t2 RENAME b TO y` must abort with `error in trigger tr1 after rename: ambiguous
//!   column name: y` when a trigger body's untouched unqualified `y` becomes ambiguous.
//! - `CREATE TRIGGER ... ON temp.t9` must fire for inserts into the temp table, and `ALTER TABLE
//!   temp.t9 RENAME TO ...` must succeed, carrying that trigger along while leaving a same-named
//!   main table (and its trigger) alone.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, ExecutorError, InsertExecutor, SelectExecutor,
    TriggerExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn try_exec(db: &mut Database, sql: &str) -> Result<(), ExecutorError> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            ViewExecutor::execute_create_view(&s, db).map(|_| ())
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql)).map(|_| ())
        }
        Statement::AlterTable(s) => {
            AlterTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn exec(db: &mut Database, sql: &str) {
    try_exec(db, sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let Statement::Select(select) = Parser::parse_sql(sql).expect("parse SELECT") else {
        panic!("expected SELECT");
    };
    let rows = SelectExecutor::new(db).execute(&select).unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter().map(|r| r.values.to_vec()).collect()
}

fn ints(rows: &[Vec<SqlValue>]) -> Vec<Vec<i64>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|v| match v {
                    SqlValue::Integer(i) => *i,
                    SqlValue::Bigint(i) => *i,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

fn setup_alias_collision(db: &mut Database, temp_view: bool) {
    exec(db, "CREATE TABLE t1(a, b)");
    exec(db, "CREATE TABLE t2(a, b)");
    exec(db, "INSERT INTO t1 VALUES(1, 2)");
    exec(db, "INSERT INTO t2 VALUES(3, 4)");
    let view = if temp_view { "temp.vv" } else { "v" };
    exec(db, &format!("CREATE VIEW {view} AS SELECT one.a, one.b, t2.a, t2.b FROM t1 AS one, t2"));
}

/// altertab.test 5.3/5.4: the rename is rejected and the view keeps working.
#[test]
fn table_rename_colliding_with_view_alias_is_rejected_after_rename() {
    let mut db = Database::new();
    setup_alias_collision(&mut db, false);

    let err = try_exec(&mut db, "ALTER TABLE t2 RENAME TO one").expect_err("must abort");
    assert!(
        err.to_string().contains("error in view v after rename: ambiguous column name: one.a"),
        "unexpected error: {err}"
    );
    assert_eq!(ints(&query(&db, "SELECT * FROM v")), vec![vec![1, 2, 3, 4]]);
    assert_eq!(ints(&query(&db, "SELECT a FROM t2")), vec![vec![3]]);
}

/// altertab.test 5.5/5.6: a temp view over main tables is re-checked too.
#[test]
fn table_rename_colliding_with_temp_view_alias_is_rejected_after_rename() {
    let mut db = Database::new();
    setup_alias_collision(&mut db, true);

    let err = try_exec(&mut db, "ALTER TABLE t2 RENAME TO one").expect_err("must abort");
    assert!(
        err.to_string().contains("error in view vv after rename: ambiguous column name: one.a"),
        "unexpected error: {err}"
    );
    assert_eq!(ints(&query(&db, "SELECT * FROM vv")), vec![vec![1, 2, 3, 4]]);
}

/// A rename that introduces no collision is unaffected by the re-check.
#[test]
fn table_rename_without_collision_still_succeeds() {
    let mut db = Database::new();
    setup_alias_collision(&mut db, false);

    exec(&mut db, "ALTER TABLE t2 RENAME TO two");
    assert_eq!(ints(&query(&db, "SELECT * FROM v")), vec![vec![1, 2, 3, 4]]);
}

/// altertab.test 13.2: `y` in the trigger body becomes ambiguous once
/// `t2.b` is renamed to `y`; the ALTER aborts and the column keeps its name.
#[test]
fn column_rename_making_trigger_reference_ambiguous_is_rejected() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x, y)");
    exec(&mut db, "CREATE TABLE t2(a, b)");
    exec(&mut db, "CREATE TABLE log(c)");
    exec(
        &mut db,
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO log SELECT y FROM t1, t2; END",
    );

    let err = try_exec(&mut db, "ALTER TABLE t2 RENAME b TO y").expect_err("must abort");
    assert!(
        err.to_string().contains("error in trigger tr1 after rename: ambiguous column name: y"),
        "unexpected error: {err}"
    );
    // Rolled back: `b` still exists under its old name.
    exec(&mut db, "INSERT INTO t2 VALUES(5, 6)");
    assert_eq!(ints(&query(&db, "SELECT b FROM t2")), vec![vec![6]]);
}

/// A column rename whose new name collides with nothing the trigger reads
/// is still allowed.
#[test]
fn column_rename_without_trigger_ambiguity_still_succeeds() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x, y)");
    exec(&mut db, "CREATE TABLE t2(a, b)");
    exec(&mut db, "CREATE TABLE log(c)");
    exec(
        &mut db,
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO log SELECT y FROM t1, t2; END",
    );

    exec(&mut db, "ALTER TABLE t2 RENAME b TO z");
    exec(&mut db, "INSERT INTO t2 VALUES(5, 6)");
    exec(&mut db, "INSERT INTO t1 VALUES(1, 2)");
    assert_eq!(ints(&query(&db, "SELECT c FROM log")), vec![vec![2]]);
}

/// altertab.test 5.0/5.1: a trigger created `ON temp.t9` fires, and
/// `ALTER TABLE temp.t9 RENAME TO ...` succeeds and carries it along, while
/// the same-named main table and its own trigger are left alone.
#[test]
fn temp_qualified_trigger_target_and_rename() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t9(a, b, c)");
    exec(&mut db, "CREATE TABLE t10(a, b, c)");
    exec(&mut db, "CREATE TABLE mlog(a)");
    exec(&mut db, "CREATE TEMP TABLE t9(a, b, c)");
    exec(
        &mut db,
        "CREATE TRIGGER temp.t9t AFTER INSERT ON temp.t9 BEGIN \
         INSERT INTO t10 VALUES(new.a, new.b, new.c); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER main.m9t AFTER INSERT ON t9 BEGIN INSERT INTO mlog VALUES(new.a); END",
    );

    exec(&mut db, "INSERT INTO temp.t9 VALUES(1, 2, 3)");
    assert_eq!(ints(&query(&db, "SELECT * FROM t10")), vec![vec![1, 2, 3]]);
    assert!(query(&db, "SELECT * FROM mlog").is_empty(), "main trigger must not fire");

    exec(&mut db, "ALTER TABLE temp.t9 RENAME TO t1234567890");

    // The temp trigger followed the rename.
    exec(&mut db, "INSERT INTO t1234567890 VALUES(4, 5, 6)");
    assert_eq!(ints(&query(&db, "SELECT * FROM t10")), vec![vec![1, 2, 3], vec![4, 5, 6]]);
    let rows = query(&db, "SELECT tbl_name FROM sqlite_temp_master WHERE name='t9t'");
    assert_eq!(rows, vec![vec![SqlValue::Varchar("t1234567890".into())]]);

    // The main table and its trigger are untouched.
    exec(&mut db, "INSERT INTO t9 VALUES(7, 8, 9)");
    assert_eq!(ints(&query(&db, "SELECT a FROM mlog")), vec![vec![7]]);
    let rows = query(&db, "SELECT tbl_name FROM sqlite_master WHERE name='m9t'");
    assert_eq!(rows, vec![vec![SqlValue::Varchar("t9".into())]]);
}

/// altertab.test 33.1: a dangling bare column only inside the derived table of an
/// expression subquery is caught by the post-rename re-parse, so the error carries
/// the `after rename` suffix and the schema is left untouched.
#[test]
fn rename_column_trigger_expr_subquery_derived_table_after_rename() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a TEXT)");
    exec(&mut db, "CREATE TABLE t2(b TEXT)");
    exec(
        &mut db,
        "CREATE TRIGGER r3 AFTER INSERT ON t1 BEGIN \
         UPDATE t2 SET (b,a)=(SELECT 1) FROM t1 JOIN t2 ON (SELECT * FROM (SELECT a)); END",
    );
    let err = try_exec(&mut db, "ALTER TABLE t1 RENAME COLUMN a TO b").unwrap_err();
    assert!(
        err.to_string().contains("error in trigger r3 after rename: no such column: a"),
        "got: {err}"
    );
}
