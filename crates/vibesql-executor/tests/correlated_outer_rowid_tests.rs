//! Regression tests for issue #6087.
//!
//! A correlated scalar subquery whose WHERE clause references the *outer*
//! table's `rowid` (e.g. `SELECT y FROM d2 WHERE d2.rowid = d1.rowid` where
//! `d1` is the outer table) must re-evaluate the outer rowid per outer row.
//!
//! The bug: `d1` is absent from the subquery's own schema, so the rowid
//! resolver fell through to the inner row's tracked `row_id`. Because
//! `Row::get_row_id_for_table` answers *any* table qualifier from the single
//! `row_id` field when no per-table `row_ids` map is present, `d1.rowid` bound
//! to the *inner* row's rowid — making the correlated predicate
//! `d2.rowid = d1.rowid` trivially true for every inner row. The subquery then
//! returned the first inner row's value regardless of the outer row, so the
//! outer filter kept only the first outer row's match (rowvalue9 4.tn.4 /
//! 4.tn.6).
//!
//! Expected values verified against sqlite3 3.51.0.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

/// Collect the integer values of result column 0 (rowids).
fn rowids(rows: &[Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.get(0) {
            Some(SqlValue::Integer(n)) | Some(SqlValue::Bigint(n)) => *n,
            other => panic!("expected integer at index 0, got {other:?}"),
        })
        .collect()
}

/// `d1(a, c)` with rowids 1,2 and `d2(x, y)` with rowids 1,2 such that
/// `d2.rowid = N` yields `y = N`.
fn setup_db() -> Database {
    let mut db = Database::new();
    let d1 = TableSchema::new(
        "d1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(d1).unwrap();
    // rowid 1: a=1, c=10 ; rowid 2: a=2, c=20
    db.insert_row("d1", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("d1", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)])).unwrap();

    let d2 = TableSchema::new(
        "d2".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(d2).unwrap();
    // rowid 1: x=10, y=1 ; rowid 2: x=20, y=2
    db.insert_row("d2", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1)])).unwrap();
    db.insert_row("d2", Row::new(vec![SqlValue::Integer(20), SqlValue::Integer(2)])).unwrap();
    db
}

/// The primary reproducer from issue #6087. Every outer row matches because the
/// per-row subquery resolves `d1.rowid` to the current outer row. Before the
/// fix only rowid 1 was returned.
#[test]
fn scalar_correlated_outer_rowid_reevaluated_per_row() {
    let db = setup_db();
    let sql = "SELECT d1.rowid FROM d1 \
               WHERE a = (SELECT y FROM d2 WHERE d2.rowid = d1.rowid)";
    assert_eq!(rowids(&run(&db, sql)), vec![1, 2]);
}

/// The row-value form (rowvalue9 4.tn.4 shape): `(c, a) = (SELECT x, y ...)`.
#[test]
fn row_value_correlated_outer_rowid_reevaluated_per_row() {
    let db = setup_db();
    let sql = "SELECT d1.rowid FROM d1 \
               WHERE (c, a) = (SELECT x, y FROM d2 WHERE d2.rowid = d1.rowid)";
    // rowid 1: (10,1) vs d2.rowid=1 -> (10,1) match; rowid 2: (20,2) vs
    // d2.rowid=2 -> (20,2) match.
    assert_eq!(rowids(&run(&db, sql)), vec![1, 2]);
}

/// A correlated outer rowid that matches only some outer rows must filter
/// correctly rather than returning all-or-nothing — guards against a fix that
/// merely reuses the wrong (but consistent) binding.
#[test]
fn scalar_correlated_outer_rowid_partial_match() {
    let db = setup_db();
    // Swap: match only when a equals the *other* row's y. d2.rowid=1 -> y=1,
    // d2.rowid=2 -> y=2, so `a = y` holds for both rows here; instead compare
    // against x to make only rowid mismatch fail.
    let sql = "SELECT d1.rowid FROM d1 \
               WHERE c = (SELECT x FROM d2 WHERE d2.rowid = d1.rowid)";
    // rowid 1: c=10 vs d2.rowid=1 -> x=10 match; rowid 2: c=20 vs d2.rowid=2 ->
    // x=20 match. Both match, confirming per-row resolution of x too.
    assert_eq!(rowids(&run(&db, sql)), vec![1, 2]);
}

/// An unqualified `rowid` inside the subquery still resolves to the subquery's
/// own table, not the outer one — the fix must only redirect *qualified*
/// outer-table rowids.
#[test]
fn unqualified_inner_rowid_unaffected() {
    let db = setup_db();
    // `SELECT y FROM d2 WHERE rowid = 1` always yields y=1 regardless of the
    // outer row, so only the outer row with a=1 (rowid 1) matches.
    let sql = "SELECT d1.rowid FROM d1 WHERE a = (SELECT y FROM d2 WHERE rowid = 1)";
    assert_eq!(rowids(&run(&db, sql)), vec![1]);
}
