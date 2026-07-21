//! Regression tests for issue #5262: DELETE ... RETURNING (SQLite 3.35.0+).
//!
//! SQLite semantics: RETURNING yields one result row per deleted row,
//! evaluated against the OLD row values (before deletion). The truncate
//! fast path (no WHERE) and the single-row PK fast path are gated when a
//! RETURNING clause is present so the OLD rows can be captured.
//!
//! Modeled on `update_returning.rs` (#5233 / #5260).

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
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
            vec![]
        }
        other => panic!("Unsupported statement in test helper: {:?}", other),
    }
}

/// Run a DELETE and return (count, RETURNING result).
fn execute_delete_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> (usize, Option<crate::select::SelectResult>) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::Delete(s) => DeleteExecutor::execute_returning(&s, db).expect("DELETE failed"),
        other => panic!("Expected DELETE, got {:?}", other),
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

#[test]
fn test_delete_returning_old_values() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10), (2, 20), (4, 40)");

    // SQLite: RETURNING evaluates against the OLD (deleted) row.
    let (count, returning) =
        execute_delete_returning(&mut db, "DELETE FROM t1 WHERE a=4 RETURNING *, a+1");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["a".to_string(), "b".to_string(), "a+1".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![4, 40, 5]]);

    // The row is actually gone.
    let remaining = execute_sql(&mut db, "SELECT a FROM t1");
    assert_eq!(remaining.len(), 2);
}

#[test]
fn test_delete_returning_no_where_gates_truncate_fast_path() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1), (2), (3)");

    // No WHERE clause: the truncate fast path must be gated so the OLD
    // rows can still be projected.
    let (count, returning) = execute_delete_returning(&mut db, "DELETE FROM t1 RETURNING *");
    assert_eq!(count, 3);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![1], vec![2], vec![3]]);

    let remaining = execute_sql(&mut db, "SELECT a FROM t1");
    assert_eq!(remaining.len(), 0, "all rows should be deleted");
}

#[test]
fn test_delete_returning_single_row_pk_gates_fast_path() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1, 10), (2, 20)");

    // Single-row PK delete: delete_by_pk_fast must be gated so the OLD
    // row can be captured.
    let (count, returning) =
        execute_delete_returning(&mut db, "DELETE FROM t1 WHERE a=2 RETURNING b");
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["b".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![20]]);

    let remaining = execute_sql(&mut db, "SELECT a FROM t1");
    assert_eq!(remaining.len(), 1);
}

#[test]
fn test_delete_returning_order_by_limit() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (3), (1), (4), (2)");

    // SQLite extension: DELETE ... ORDER BY ... LIMIT n deletes exactly the n
    // rows selected by the sort. The ORDER BY only picks *which* rows fall
    // within the LIMIT; the rows are then deleted — and RETURNING emitted — in
    // rowid order, not ORDER BY order (SQLite lang_delete.html R-07548-13422;
    // conformance test e_delete-3.10). Values (3),(1),(4),(2) occupy rowids
    // 1..4, so `ORDER BY a DESC LIMIT 2` selects a=4 (rowid 3) and a=3 (rowid
    // 1); emitted in rowid order that is [3, 4].
    let (count, returning) =
        execute_delete_returning(&mut db, "DELETE FROM t1 ORDER BY a DESC LIMIT 2 RETURNING a");
    assert_eq!(count, 2);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(int_rows(&returning), vec![vec![3], vec![4]]);

    let remaining = execute_sql(&mut db, "SELECT a FROM t1");
    assert_eq!(remaining.len(), 2);
}

#[test]
fn test_delete_returning_zero_matched_rows_is_empty_not_error() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1)");

    let (count, returning) =
        execute_delete_returning(&mut db, "DELETE FROM t1 WHERE a=100 RETURNING *");
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a (empty) result");
    assert_eq!(returning.rows.len(), 0);
    assert_eq!(returning.columns, vec!["a".to_string()]);
}

#[test]
fn test_delete_without_returning_yields_none() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (4)");

    let (count, returning) = execute_delete_returning(&mut db, "DELETE FROM t1 WHERE a=4");
    assert_eq!(count, 1);
    assert!(returning.is_none());
}

#[test]
fn test_delete_returning_alias_and_qualified_wildcard() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (7)");

    let (count, returning) = execute_delete_returning(
        &mut db,
        "DELETE FROM t1 WHERE a=7 RETURNING t1.*, a*2 AS doubled",
    );
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["a".to_string(), "doubled".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![7, 14]]);
}

#[test]
fn test_delete_returning_wrong_qualified_wildcard_errors() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a INT)");
    execute_sql(&mut db, "INSERT INTO t1 VALUES (1)");

    let stmt = Parser::parse_sql("DELETE FROM t1 RETURNING other.*").unwrap();
    match stmt {
        Statement::Delete(s) => {
            let result = DeleteExecutor::execute_returning(&s, &mut db);
            assert!(result.is_err(), "qualified wildcard for wrong table must error");
        }
        other => panic!("Expected DELETE, got {:?}", other),
    }
}

/// DELETE on a view through an INSTEAD OF trigger returns the OLD view
/// rows, one per trigger fire, regardless of what the trigger body does.
#[test]
fn test_delete_returning_through_instead_of_trigger() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE base(b INT, c INT)");
    execute_sql(&mut db, "INSERT INTO base VALUES (4, 8), (4, 9), (5, 11)");
    execute_sql(&mut db, "CREATE TABLE log(x INT)");
    execute_sql(&mut db, "CREATE VIEW v1 AS SELECT b, c FROM base");
    execute_sql(
        &mut db,
        "CREATE TRIGGER v1_tr INSTEAD OF DELETE ON v1 BEGIN INSERT INTO log VALUES(1); END",
    );

    let (count, returning) =
        execute_delete_returning(&mut db, "DELETE FROM v1 WHERE b=4 RETURNING *");
    // changes() after DML on a view is always 0 (SQLite R-09813-48563, #5840);
    // the INSTEAD OF trigger's fire count is still verified via RETURNING below.
    assert_eq!(count, 0);
    let returning = returning.expect("RETURNING clause should produce a result");
    assert_eq!(returning.columns, vec!["b".to_string(), "c".to_string()]);
    assert_eq!(int_rows(&returning), vec![vec![4, 8], vec![4, 9]]);

    let log = execute_sql(&mut db, "SELECT x FROM log");
    assert_eq!(log.len(), 2, "INSTEAD OF trigger should fire once per matching view row");
}
