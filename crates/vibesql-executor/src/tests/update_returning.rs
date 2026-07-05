//! Regression tests for issue #5233 (window1.test 73.2/73.4):
//!
//! - INSTEAD OF UPDATE view WHERE truthiness: comparison expressions return
//!   SQLite-style `Integer(1)`, which previously failed the Boolean-only
//!   match arm and fired the trigger 0 times.
//! - UPDATE ... FROM on a view fires the INSTEAD OF trigger once per join
//!   match (SQLite semantics) — the previous per-view-row dedup is gone.
//! - UPDATE ... RETURNING returns the NEW rows (after SET assignments), for
//!   base tables and for views routed through INSTEAD OF UPDATE triggers.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use super::super::*;

fn execute_sql(db: &mut vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            vec![]
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
            vec![]
        }
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).expect("CREATE VIEW failed");
            vec![]
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER failed");
            vec![]
        }
        Statement::Select(s) => SelectExecutor::new(db).execute(&s).expect("SELECT failed"),
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
            vec![]
        }
        other => panic!("Unsupported statement in test helper: {:?}", other),
    }
}

/// Run an UPDATE and return (count, RETURNING result).
fn execute_update_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> (usize, Option<crate::select::SelectResult>) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Update(s) => UpdateExecutor::execute_returning(&s, db).expect("UPDATE failed"),
        other => panic!("Expected UPDATE, got {:?}", other),
    }
}

fn int_rows(result: &crate::select::SelectResult) -> Vec<Vec<i64>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| match v {
                    SqlValue::Integer(n) => *n,
                    SqlValue::Bigint(n) => *n,
                    SqlValue::Smallint(n) => *n as i64,
                    other => panic!("Expected integer value, got {:?}", other),
                })
                .collect()
        })
        .collect()
}

// ------------------------------------------------------------------------
// Root cause 1: WHERE truthiness on the no-FROM view path (73.2 fire count)
// ------------------------------------------------------------------------

#[test]
fn test_instead_of_update_view_where_comparison_fires() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1), (2), (4)");
    execute_sql(&mut db, "CREATE TABLE log(x INT)");
    execute_sql(&mut db, "CREATE VIEW v3 AS SELECT a AS b FROM t1");
    execute_sql(
        &mut db,
        "CREATE TRIGGER y1 INSTEAD OF UPDATE ON v3 BEGIN INSERT INTO log VALUES(1); END",
    );

    // Comparison results surface as Integer(1) in this evaluator context;
    // before the fix the trigger fired 0 times (Boolean-only match arm).
    let (count, _) = execute_update_returning(&mut db, "UPDATE v3 SET b=9 WHERE b=4");
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the single INSTEAD OF fire is verified via the log table below.
    assert_eq!(count, 0, "changes() is 0 for a view UPDATE");

    let log = execute_sql(&mut db, "SELECT x FROM log");
    assert_eq!(log.len(), 1, "INSTEAD OF trigger should fire once for WHERE b=4");
}

// ------------------------------------------------------------------------
// Root cause 2: UPDATE...FROM view path fires once per join match (73.4)
// ------------------------------------------------------------------------

#[test]
fn test_instead_of_update_view_from_fires_per_join_match() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE base(b INT, c INT)");
    execute_sql(&mut db, "INSERT INTO base VALUES (4, 10)");
    execute_sql(&mut db, "CREATE TABLE m(k INT)");
    execute_sql(&mut db, "INSERT INTO m VALUES (1), (2), (3)");
    execute_sql(&mut db, "CREATE TABLE log(x INT)");
    execute_sql(&mut db, "CREATE VIEW v2 AS SELECT b, c FROM base");
    execute_sql(
        &mut db,
        "CREATE TRIGGER y2 INSTEAD OF UPDATE ON v2 BEGIN INSERT INTO log VALUES(1); END",
    );

    // One view row, three FROM rows, no join condition restricting them:
    // SQLite fires the INSTEAD OF trigger once per join match (3 times).
    // The previous implementation deduped to one fire per view row.
    let (count, _) = execute_update_returning(&mut db, "UPDATE v2 SET c=99 FROM m WHERE b=4");
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the three per-join-match fires are verified via the log table below.
    assert_eq!(count, 0, "changes() is 0 for a view UPDATE");

    let log = execute_sql(&mut db, "SELECT x FROM log");
    assert_eq!(log.len(), 3, "INSTEAD OF trigger should fire once per join match");
}

// ------------------------------------------------------------------------
// Root cause 3: UPDATE ... RETURNING
// ------------------------------------------------------------------------

#[test]
fn test_update_returning_base_table_new_values() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1), (2), (4)");

    // SQLite: RETURNING evaluates against the NEW row (5|6), not the old one.
    let (count, returning) =
        execute_update_returning(&mut db, "UPDATE t1 SET a=5 WHERE a=4 RETURNING *, a+1");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns.len(), 2);
    assert_eq!(int_rows(&returning), vec![vec![5, 6]]);
}

#[test]
fn test_update_returning_zero_matched_rows_is_empty_not_error() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1)");

    let (count, returning) =
        execute_update_returning(&mut db, "UPDATE t1 SET a=5 WHERE a=100 RETURNING *");
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a (empty) result");
    assert_eq!(returning.rows.len(), 0);
    assert_eq!(returning.columns, vec!["a".to_string()]);
}

#[test]
fn test_update_without_returning_yields_none() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (4)");

    let (count, returning) = execute_update_returning(&mut db, "UPDATE t1 SET a=5 WHERE a=4");
    assert_eq!(count, 1);
    assert!(returning.is_none());
}

/// window1.test 73.2 shape: RETURNING through an INSTEAD OF UPDATE trigger
/// returns the NEW view rows (old row with SET applied), one per fire,
/// regardless of what the trigger body does.
#[test]
fn test_update_returning_through_instead_of_trigger() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE base(b INT, c INT)");
    execute_sql(&mut db, "INSERT INTO base VALUES (4, 8), (4, 9), (4, 10), (5, 11)");
    execute_sql(&mut db, "CREATE TABLE log(x INT)");
    execute_sql(&mut db, "CREATE VIEW t2 AS SELECT b, c FROM base");
    execute_sql(
        &mut db,
        "CREATE TRIGGER t2_tr INSTEAD OF UPDATE ON t2 BEGIN INSERT INTO log VALUES(1); END",
    );

    let (count, returning) =
        execute_update_returning(&mut db, "UPDATE t2 SET c=99 WHERE b=4 RETURNING *");
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the three fires are verified via the RETURNING rows below.
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["b".to_string(), "c".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![4, 99], vec![4, 99], vec![4, 99]]);
}

/// window1.test 73.4 shape: UPDATE ... FROM ... RETURNING on a view fires
/// (and returns a row) once per join match — 1 view row x 3 FROM rows here.
#[test]
fn test_update_returning_through_instead_of_trigger_with_from() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE base(b INT, c INT)");
    execute_sql(&mut db, "INSERT INTO base VALUES (4, 10)");
    execute_sql(&mut db, "CREATE TABLE m(k INT)");
    execute_sql(&mut db, "INSERT INTO m VALUES (1), (2), (3)");
    execute_sql(&mut db, "CREATE VIEW t2 AS SELECT b, c FROM base");
    execute_sql(&mut db, "CREATE TABLE log(x INT)");
    execute_sql(
        &mut db,
        "CREATE TRIGGER t2_tr INSTEAD OF UPDATE ON t2 BEGIN INSERT INTO log VALUES(1); END",
    );

    let (count, returning) =
        execute_update_returning(&mut db, "UPDATE t2 SET c=15 FROM m WHERE b=4 RETURNING *");
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the three per-join-match fires are verified via the RETURNING rows below.
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![4, 15], vec![4, 15], vec![4, 15]]);
}
