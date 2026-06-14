//! `UPDATE ... SET rowid = <expr>` on base tables (issue #5517).
//!
//! SQLite lets a rowid table relocate a row by assigning its rowid:
//! `UPDATE t SET rowid=99 WHERE rowid=1` moves the row to rowid 99, fires the
//! table's BEFORE/AFTER UPDATE triggers with OLD.rowid=1 / NEW.rowid=99, and
//! raises `UNIQUE constraint failed: t.rowid` if the target rowid is already
//! taken. `_rowid_` / `oid` are aliases. For an INTEGER PRIMARY KEY table the
//! rowid IS the IPK column, so `SET rowid=` writes the IPK (and a collision is
//! reported against the IPK column name). These behaviors were verified against
//! sqlite3 3.51.0; see triggerC-7.x.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use crate::*;

/// Execute a non-query statement (CREATE/INSERT/CREATE INDEX/CREATE TRIGGER).
fn ddl(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE");
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).expect("CREATE INDEX");
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

/// Run an UPDATE, returning the executor result (so conflict errors are observable).
fn update(db: &mut vibesql_storage::Database, sql: &str) -> Result<usize, crate::ExecutorError> {
    match Parser::parse_sql(sql).expect("parse update") {
        Statement::Update(s) => UpdateExecutor::execute(&s, db),
        other => panic!("expected UPDATE, got {:?}", other),
    }
}

/// Run a SELECT and return rows (each row carries its resolved row_id).
fn select(db: &mut vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    match Parser::parse_sql(sql).expect("parse select") {
        Statement::Select(s) => SelectExecutor::new(db).execute(&s).expect("SELECT"),
        other => panic!("expected SELECT, got {:?}", other),
    }
}

fn int(v: &SqlValue) -> i64 {
    match v {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        other => panic!("expected integer, got {:?}", other),
    }
}

fn text(v: &SqlValue) -> String {
    match v {
        SqlValue::Varchar(s) => s.to_string(),
        other => panic!("expected text, got {:?}", other),
    }
}

/// `UPDATE t SET rowid=N` relocates the row on a virtual-rowid table.
#[test]
fn set_rowid_relocates_row() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");
    ddl(&mut db, "INSERT INTO t VALUES(2)");
    ddl(&mut db, "INSERT INTO t VALUES(3)");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=99 WHERE rowid=1").unwrap(), 1);

    // sqlite3: rows are (rowid=2,x=2), (rowid=3,x=3), (rowid=99,x=1).
    let rows = select(&mut db, "SELECT rowid, x FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(2, 2), (3, 3), (99, 1)]);
}

/// `_rowid_` is an alias for `rowid`.
#[test]
fn set_underscore_rowid_alias() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");

    assert_eq!(update(&mut db, "UPDATE t SET _rowid_=50 WHERE rowid=1").unwrap(), 1);

    let rows = select(&mut db, "SELECT rowid, x FROM t");
    assert_eq!(int(&rows[0].values[0]), 50);
    assert_eq!(int(&rows[0].values[1]), 1);
}

/// Relocating onto an occupied rowid is `UNIQUE constraint failed: t.rowid`.
#[test]
fn set_rowid_conflict_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x)");
    ddl(&mut db, "INSERT INTO t VALUES(1)");
    ddl(&mut db, "INSERT INTO t VALUES(2)");
    ddl(&mut db, "INSERT INTO t VALUES(3)");

    let err = update(&mut db, "UPDATE t SET rowid=2 WHERE rowid=1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.rowid"),
        "expected rowid UNIQUE error, got: {}",
        err
    );

    // The table must be unchanged after the aborted UPDATE.
    let rows = select(&mut db, "SELECT rowid, x FROM t ORDER BY rowid");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 1), (2, 2), (3, 3)]);
}

/// On an INTEGER PRIMARY KEY table, `SET rowid=` writes the IPK column.
#[test]
fn set_rowid_aliases_ipk() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=7 WHERE k=1").unwrap(), 1);

    let rows = select(&mut db, "SELECT rowid, k, v FROM t");
    assert_eq!(int(&rows[0].values[0]), 7); // rowid
    assert_eq!(int(&rows[0].values[1]), 7); // k (IPK)
    assert_eq!(text(&rows[0].values[2]), "a");
}

/// IPK collision via `SET rowid=` reports against the IPK column, not `.rowid`.
#[test]
fn set_rowid_ipk_conflict_errors() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'a')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'b')");

    let err = update(&mut db, "UPDATE t SET rowid=2 WHERE k=1").unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t.k"),
        "expected IPK UNIQUE error, got: {}",
        err
    );
}

/// An index on another column still finds the row after a rowid relocation.
#[test]
fn index_consistent_after_relocate() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t(x, y)");
    ddl(&mut db, "CREATE INDEX iy ON t(y)");
    ddl(&mut db, "INSERT INTO t VALUES(1, 'aa')");
    ddl(&mut db, "INSERT INTO t VALUES(2, 'bb')");

    assert_eq!(update(&mut db, "UPDATE t SET rowid=99 WHERE rowid=1").unwrap(), 1);

    // Index scan on y must report the relocated rowid (99), not the slot (1).
    let rows = select(&mut db, "SELECT rowid, x, y FROM t WHERE y='aa'");
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0].values[0]), 99);
    assert_eq!(int(&rows[0].values[1]), 1);
    assert_eq!(text(&rows[0].values[2]), "aa");
}

/// BEFORE/AFTER UPDATE triggers see OLD.rowid (old) and NEW.rowid (new).
#[test]
fn triggers_see_old_and_new_rowid() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t8(x)");
    ddl(&mut db, "CREATE TABLE t7(a, b)");
    ddl(&mut db, "INSERT INTO t7 VALUES(1, 2)");
    ddl(
        &mut db,
        "CREATE TRIGGER t7tb BEFORE UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('before ' || old.rowid || '->' || new.rowid); END",
    );
    ddl(
        &mut db,
        "CREATE TRIGGER t7ta AFTER UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('after ' || old.rowid || '->' || new.rowid); END",
    );

    assert_eq!(update(&mut db, "UPDATE t7 SET rowid=8 WHERE rowid=1").unwrap(), 1);

    let t7 = select(&mut db, "SELECT rowid, a, b FROM t7");
    assert_eq!(int(&t7[0].values[0]), 8);
    assert_eq!(int(&t7[0].values[1]), 1);
    assert_eq!(int(&t7[0].values[2]), 2);

    let t8 = select(&mut db, "SELECT x FROM t8 ORDER BY rowid");
    let log: Vec<String> = t8.iter().map(|r| text(&r.values[0])).collect();
    assert_eq!(log, vec!["before 1->8".to_string(), "after 1->8".to_string()]);
}

/// triggerC-7.5: a BEFORE UPDATE trigger relocates *another* row via
/// `SET rowid=`, then the AFTER UPDATE trigger logs old/new rowid. Verified
/// against sqlite3 3.51.0:
///   T7 = (2,3,4) (3,5,7) (8,1,2)
///   T8 = "after fired 1->8", "after fired 3->3"
///
/// This issue's SET-rowid relocation makes the *table* state (T7) match sqlite3
/// exactly: the nested `UPDATE t7 SET rowid=8` fired inside the BEFORE trigger
/// relocates rowid 1 -> 8.
///
/// The T8 AFTER-trigger log is only PARTIALLY correct: the outer UPDATE's AFTER
/// trigger fires ("after fired 3->3"), but the nested UPDATE (run inside the
/// BEFORE trigger, i.e. with a trigger context) does NOT fire its own AFTER
/// trigger, so "after fired 1->8" is missing. That gap is in the trigger
/// execution path — `execute_internal` forces `has_triggers=false` whenever a
/// `trigger_context` is present, suppressing all nested-DML triggers — which is
/// separate from rowid relocation and tracked as a follow-on (see PR body,
/// "Part of #5517"). This test pins the relocation behavior we DO fix and
/// documents the residual triggerC-7.5/7.6 delta.
#[test]
fn triggerc_7_5_relocate_in_before_trigger() {
    let mut db = vibesql_storage::Database::new();
    ddl(&mut db, "CREATE TABLE t8(x)");
    ddl(&mut db, "CREATE TABLE t7(a, b)");
    ddl(&mut db, "INSERT INTO t7 VALUES(1, 2)");
    ddl(&mut db, "INSERT INTO t7 VALUES(3, 4)");
    ddl(&mut db, "INSERT INTO t7 VALUES(5, 6)");
    ddl(
        &mut db,
        "CREATE TRIGGER t7t BEFORE UPDATE ON t7 WHEN (old.rowid!=1 OR new.rowid!=8) \
         BEGIN UPDATE t7 SET rowid = 8 WHERE rowid=1; END",
    );
    ddl(
        &mut db,
        "CREATE TRIGGER t7ta AFTER UPDATE ON t7 BEGIN \
         INSERT INTO t8 VALUES('after fired ' || old.rowid || '->' || new.rowid); END",
    );

    update(&mut db, "UPDATE t7 SET b=7 WHERE a = 5").unwrap();

    // Table state matches sqlite3 exactly: the nested SET-rowid relocate applied.
    let t7 = select(&mut db, "SELECT rowid, a, b FROM t7 ORDER BY rowid");
    let got: Vec<(i64, i64, i64)> = t7
        .iter()
        .map(|r| (int(&r.values[0]), int(&r.values[1]), int(&r.values[2])))
        .collect();
    assert_eq!(got, vec![(2, 3, 4), (3, 5, 7), (8, 1, 2)]);

    // Outer UPDATE's AFTER trigger fires; the nested UPDATE's AFTER trigger does
    // not (documented follow-on). sqlite3 also logs "after fired 1->8" first.
    let t8 = select(&mut db, "SELECT x FROM t8 ORDER BY rowid");
    let log: Vec<String> = t8.iter().map(|r| text(&r.values[0])).collect();
    assert_eq!(log, vec!["after fired 3->3".to_string()]);
}
