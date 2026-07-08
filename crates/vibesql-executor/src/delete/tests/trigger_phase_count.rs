//! Regression tests for #5523: a combined BEFORE+AFTER DELETE trigger pair must
//! observe the *live* `count(*)` of the table at each phase of a per-row DELETE.
//!
//! SQLite fires DELETE row triggers interleaved per row (#5486): for each
//! affected row R it runs BEFORE(R) (R still present -> count includes it),
//! removes R, then runs AFTER(R) (R gone -> count excludes it). A trigger body
//! that reads `(SELECT count(*) FROM t)` must therefore see the running count.
//!
//! Before this fix the database-level columnar cache (which serves the columnar
//! aggregate path that computes `count(*)` for row-oriented tables) was only
//! invalidated *after* the whole DELETE loop. The AFTER trigger then observed
//! the pre-delete count. All expected sequences below were verified live against
//! sqlite3 3.51.0.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger(db, &s).unwrap();
        }
        Statement::Insert(s) => {
            crate::InsertExecutor::execute(db, &s).unwrap();
        }
        Statement::Delete(s) => {
            crate::delete::DeleteExecutor::execute(&s, db).unwrap();
        }
        other => panic!("unsupported statement: {other:?}"),
    }
}

/// Read the `t` column of the `log` table, in insertion (rowid) order, as a
/// `Vec<String>`. Uses a LIVE SELECT through the executor so the assertion
/// exercises the same read path triggers observe.
fn log_values(db: &Database) -> Vec<String> {
    let stmt = vibesql_parser::Parser::parse_sql("SELECT t FROM log").expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {other:?}"),
    };
    let result = crate::SelectExecutor::new(db).execute_with_columns(&select).expect("run select");
    result
        .rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Create `t1` plus a `log` table and the combined BEFORE+AFTER DELETE triggers
/// that each log `old.a || old.b || (SELECT count(*) FROM t1)`.
fn setup(db: &mut Database) {
    exec(db, "CREATE TABLE t1(a INT PRIMARY KEY, b)");
    exec(db, "CREATE TABLE log(t)");
    exec(
        db,
        "CREATE TRIGGER tr1 AFTER DELETE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || old.b || (SELECT count(*) FROM t1)); END",
    );
    exec(
        db,
        "CREATE TRIGGER tr2 BEFORE DELETE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || old.b || (SELECT count(*) FROM t1)); END",
    );
    exec(db, "INSERT INTO t1 VALUES(1,'one'),(2,'two'),(3,'three')");
}

/// Single-row DELETE: BEFORE sees the pre-delete count (3), AFTER sees the
/// post-delete count (2). Matches sqlite3 3.51.0 `1one3 1one2`.
#[test]
fn before_after_delete_trigger_phase_count_single_row() {
    let mut db = Database::new();
    setup(&mut db);

    exec(&mut db, "DELETE FROM t1 WHERE a=1");

    assert_eq!(
        log_values(&db),
        vec!["1one3".to_string(), "1one2".to_string()],
        "BEFORE must see count=3 (row present), AFTER count=2 (row gone)"
    );
}

/// Multi-row DELETE: the running count must decrement per row across the
/// interleaved loop. Matches sqlite3 3.51.0 `2two2 2two1 3three1 3three0`
/// after row a=1 is removed first.
#[test]
fn before_after_delete_trigger_phase_count_multi_row() {
    let mut db = Database::new();
    setup(&mut db);

    // Remove a=1 so the remaining rows are a=2, a=3 (two live rows).
    exec(&mut db, "DELETE FROM t1 WHERE a=1");
    exec(&mut db, "DELETE FROM log");

    // DELETE all remaining rows: row a=2 then a=3 (PK order).
    exec(&mut db, "DELETE FROM t1");

    assert_eq!(
        log_values(&db),
        vec![
            "2two2".to_string(),
            "2two1".to_string(),
            "3three1".to_string(),
            "3three0".to_string(),
        ],
        "each phase must observe the live running count"
    );
}

/// With ONLY an AFTER DELETE trigger the count was already correct; lock it in
/// so the per-row invalidation does not regress the single-trigger case.
#[test]
fn after_only_delete_trigger_phase_count() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b)");
    exec(&mut db, "CREATE TABLE log(t)");
    exec(
        &mut db,
        "CREATE TRIGGER tr1 AFTER DELETE ON t1 BEGIN \
         INSERT INTO log VALUES(old.a || old.b || (SELECT count(*) FROM t1)); END",
    );
    exec(&mut db, "INSERT INTO t1 VALUES(1,'one'),(2,'two'),(3,'three')");

    exec(&mut db, "DELETE FROM t1 WHERE a=1");

    assert_eq!(log_values(&db), vec!["1one2".to_string()], "AFTER sees count=2");
}
