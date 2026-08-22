//! Tests for `PRAGMA recursive_triggers` semantics (#5535).
//!
//! With `recursive_triggers = OFF`, a trigger already executing on the stack is
//! not re-fired by DML performed within its own body — directly- and
//! mutually-recursive trigger firing is suppressed. A *different* trigger
//! reached via nested DML still fires, and a trigger fired at the top level
//! still fires (it is not yet on the stack). With `recursive_triggers = ON`,
//! triggers recurse up to the depth cap (#5479). OFF is the default, matching
//! SQLite's `pragma.c` (#5840).
//!
//! Every expectation below was verified against sqlite3 3.51.0. The headline
//! case (`off_suppresses_self_refiring_trigger3_6_shape`) reproduces the exact
//! `trigger3-6` scenario from SQLite's `trigger3.test`, which runs the whole
//! file under `PRAGMA recursive_triggers = off`.

use vibesql_executor::{
    CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a single SQL statement, dispatching on statement type.
fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            // Preserve the raw SQL so the trigger body re-parses faithfully.
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Run a SELECT and return its rows as `Vec<Vec<SqlValue>>` (live rows only).
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db)
        .execute(&select)
        .expect("SELECT failed")
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

fn i(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

/// `recursive_triggers = off`: the `trigger3-6` self-refiring shape. An AFTER
/// INSERT trigger on `tbl2` inserts back into `tbl2`; with the pragma off that
/// nested INSERT must NOT re-fire the trigger, so `tbl2` ends with exactly two
/// rows (without suppression this recurses to the depth cap and errors). The
/// body's `UPDATE tbl SET c = 10` runs once.
///
/// sqlite3 3.51.0 (recursive_triggers off):
///   tbl2 -> {1 2 3 , 1 2 3} ; tbl -> {1 2 10}
#[test]
fn off_suppresses_self_refiring_trigger3_6_shape() {
    let mut db = Database::new();
    db.set_recursive_triggers(false);

    exec(&mut db, "CREATE TABLE tbl(a, b, c)");
    exec(&mut db, "INSERT INTO tbl VALUES(1, 2, 3)");
    exec(&mut db, "CREATE TABLE tbl2(a, b, c)");
    exec(
        &mut db,
        "CREATE TRIGGER after_tbl2_insert AFTER INSERT ON tbl2 BEGIN \
         UPDATE tbl SET c = 10; \
         INSERT INTO tbl2 VALUES (new.a, new.b, new.c); END",
    );

    // Without suppression this recurses until the depth cap and errors.
    exec(&mut db, "INSERT INTO tbl2 VALUES (1, 2, 3)");

    assert_eq!(
        query(&db, "SELECT * FROM tbl2"),
        vec![vec![i(1), i(2), i(3)], vec![i(1), i(2), i(3)]],
        "self-recursive trigger must fire exactly once with recursive_triggers off",
    );
    assert_eq!(
        query(&db, "SELECT * FROM tbl"),
        vec![vec![i(1), i(2), i(10)]],
        "the trigger body's UPDATE runs exactly once",
    );
}

/// `recursive_triggers = off` is *per trigger*, not a blanket disable: a
/// DIFFERENT trigger reached via nested DML still fires. Trigger `ta` (on `a`)
/// inserts into `b`, which fires trigger `tb` (on `b`) once.
///
/// sqlite3 3.51.0: log -> {tb , ta}  (tb runs to completion before ta's body
/// finishes; both fire exactly once).
#[test]
fn off_still_fires_distinct_nested_trigger() {
    let mut db = Database::new();
    db.set_recursive_triggers(false);

    exec(&mut db, "CREATE TABLE a(x)");
    exec(&mut db, "CREATE TABLE b(x)");
    exec(&mut db, "CREATE TABLE log(msg)");
    exec(
        &mut db,
        "CREATE TRIGGER ta AFTER INSERT ON a BEGIN \
         INSERT INTO b VALUES(new.x); INSERT INTO log VALUES('ta'); END",
    );
    exec(&mut db, "CREATE TRIGGER tb AFTER INSERT ON b BEGIN INSERT INTO log VALUES('tb'); END");

    exec(&mut db, "INSERT INTO a VALUES(1)");

    let txt = |s: &str| SqlValue::Varchar(arcstr::ArcStr::from(s));
    assert_eq!(
        query(&db, "SELECT msg FROM log"),
        vec![vec![txt("tb")], vec![txt("ta")]],
        "a distinct nested trigger must still fire when recursive_triggers is off",
    );
}

/// `recursive_triggers = off` suppresses MUTUAL recursion too: `ta` (on `a`)
/// inserts into `b`, `tb` (on `b`) inserts back into `a` — but `ta` is already
/// on the stack, so its re-entry is suppressed. Each fires once.
///
/// sqlite3 3.51.0: log -> {ta , tb} (and no infinite loop / cap error).
#[test]
fn off_suppresses_mutual_recursion() {
    let mut db = Database::new();
    db.set_recursive_triggers(false);

    exec(&mut db, "CREATE TABLE a(x)");
    exec(&mut db, "CREATE TABLE b(x)");
    exec(&mut db, "CREATE TABLE log(msg)");
    exec(
        &mut db,
        "CREATE TRIGGER ta AFTER INSERT ON a BEGIN \
         INSERT INTO log VALUES('ta'); INSERT INTO b VALUES(new.x); END",
    );
    exec(
        &mut db,
        "CREATE TRIGGER tb AFTER INSERT ON b BEGIN \
         INSERT INTO log VALUES('tb'); INSERT INTO a VALUES(new.x); END",
    );

    exec(&mut db, "INSERT INTO a VALUES(1)");

    let txt = |s: &str| SqlValue::Varchar(arcstr::ArcStr::from(s));
    assert_eq!(
        query(&db, "SELECT msg FROM log"),
        vec![vec![txt("ta")], vec![txt("tb")]],
        "mutual trigger recursion must be suppressed (each fires once) with recursive_triggers off",
    );
}

/// `recursive_triggers = on` (the default) keeps recursive firing: a guarded
/// self-recursive countdown trigger fires once per level.
///
/// sqlite3 3.51.0 (recursive_triggers on, the modern default): inserting 5
/// yields rows 5,4,3,2,1,0 — six rows.
#[test]
fn on_recurses_to_completion() {
    let mut db = Database::new();
    // Default is OFF (matches SQLite); enable recursion explicitly for this case.
    db.set_recursive_triggers(true);
    assert!(db.recursive_triggers(), "recursive_triggers explicitly enabled");

    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY)");
    exec(
        &mut db,
        "CREATE TRIGGER t_cd AFTER INSERT ON t WHEN (new.a > 0) BEGIN \
         INSERT INTO t VALUES(new.a - 1); END",
    );

    exec(&mut db, "INSERT INTO t VALUES(5)");

    assert_eq!(
        query(&db, "SELECT a FROM t ORDER BY a").len(),
        6,
        "recursive_triggers on must recurse: rows 5..=0",
    );
}

/// Toggling the pragma mid-session takes effect. Each phase uses a disjoint
/// value band so the same self-recursive countdown trigger never hits a PRIMARY
/// KEY collision, isolating the on/off behavior:
///   - ON: inserting 5 recurses down its band (5,4,3,2,1 — stops at the band floor) producing 5
///     rows.
///   - OFF: inserting 25 fires once (no re-fire), adding 25 and its single decrement 24 — 2 rows.
///   - ON: inserting 45 recurses again down its band (45..=41) — 5 rows.
///
/// The trigger is guarded `new.a > floor(a)` per band via `new.a % 10 != 1`
/// so each band yields a fixed, non-overlapping set of rows. Counts verified to
/// match the equivalent sqlite3 3.51.0 runs (with the pragma set explicitly per
/// phase, since the sqlite3 CLI defaults the pragma to off).
#[test]
fn toggle_mid_session_takes_effect() {
    let mut db = Database::new();

    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY)");
    // Recurse downward but stop at any value ending in 1, so a start of N0..N9
    // recurses only within its own ten-band (e.g. 5 -> 4,3,2,1).
    exec(
        &mut db,
        "CREATE TRIGGER t_cd AFTER INSERT ON t WHEN (new.a % 10 > 1) BEGIN \
         INSERT INTO t VALUES(new.a - 1); END",
    );

    // ON (explicitly enabled): inserting 5 recurses to 5,4,3,2,1 = 5 rows.
    db.set_recursive_triggers(true);
    assert!(db.recursive_triggers(), "recursive_triggers explicitly enabled");
    exec(&mut db, "INSERT INTO t VALUES(5)");
    assert_eq!(query(&db, "SELECT a FROM t").len(), 5, "ON: 5,4,3,2,1");

    // Switch OFF: inserting 25 fires the trigger once; its body inserts 24 but
    // that nested INSERT does NOT re-fire the trigger. Adds {25, 24} = 2 rows.
    db.set_recursive_triggers(false);
    exec(&mut db, "INSERT INTO t VALUES(25)");
    assert_eq!(
        query(&db, "SELECT a FROM t").len(),
        7,
        "OFF: only the direct row 25 and its single decrement 24 are added",
    );

    // Switch back ON: inserting 45 recurses down its band to 45,44,43,42,41.
    db.set_recursive_triggers(true);
    exec(&mut db, "INSERT INTO t VALUES(45)");
    assert_eq!(
        query(&db, "SELECT a FROM t").len(),
        12,
        "ON again: 45,44,43,42,41 added by recursion (5 rows)",
    );
}
