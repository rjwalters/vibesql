//! Regression tests for issue #6047 (root cause 1).
//!
//! A qualified column reference (`t1.y`) inside a *doubly*-nested correlated
//! scalar subquery must resolve against the outer table even when that table
//! lives several levels up the `outer_schema` chain.
//!
//! The single-level table-existence pre-check in the combined evaluator
//! (`eval_column_ref`, added for issue #4510) only inspected the *immediate*
//! `outer_schema`'s `table_schemas` map. For an N-level-nested subquery the
//! innermost evaluator's immediate outer schema is an empty merged schema whose
//! own `outer_schema` field links to the real outer table, so the pre-check
//! spuriously raised `no such column: t1.y` — even though the actual column
//! resolver (`CombinedSchema::get_column_index`) walks the whole chain and would
//! have found it. This is the qualified-column analogue of #4618 (which fixed
//! the *unqualified* double-nesting case).
//!
//! Expected values verified against sqlite3 3.51.

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

/// One scalar integer from the single result row/column.
fn scalar(rows: &[Row]) -> i64 {
    assert_eq!(rows.len(), 1, "expected exactly one result row");
    match rows[0].get(0) {
        Some(SqlValue::Integer(n)) => *n,
        Some(SqlValue::Bigint(n)) => *n,
        other => panic!("expected integer at index 0, got {other:?}"),
    }
}

/// `CREATE TABLE t1(x, y, z); INSERT INTO t1 VALUES(1000, 2000, 3000);`
fn setup_db() -> Database {
    let mut db = Database::new();
    let t1 = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
            ColumnSchema::new("z".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t1).unwrap();
    db.insert_row(
        "t1",
        Row::new(vec![SqlValue::Integer(1000), SqlValue::Integer(2000), SqlValue::Integer(3000)]),
    )
    .unwrap();
    db
}

/// Single-level nesting already worked before the fix — keep it as a control.
#[test]
fn single_level_qualified_correlation() {
    let db = setup_db();
    assert_eq!(scalar(&run(&db, "SELECT (SELECT t1.y) FROM t1")), 2000);
}

/// The primary reproducer: two-level nesting with a qualified outer reference.
/// Used to fail with `no such column: t1.y`.
#[test]
fn double_nested_qualified_correlation() {
    let db = setup_db();
    assert_eq!(scalar(&run(&db, "SELECT (SELECT (SELECT t1.y)) FROM t1")), 2000);
}

/// Three levels deep resolves through the full chain.
#[test]
fn triple_nested_qualified_correlation() {
    let db = setup_db();
    assert_eq!(scalar(&run(&db, "SELECT (SELECT (SELECT (SELECT t1.y))) FROM t1")), 2000);
}

/// The nested reference can point at any column of the outer table.
#[test]
fn double_nested_qualified_correlation_other_column() {
    let db = setup_db();
    assert_eq!(scalar(&run(&db, "SELECT (SELECT (SELECT t1.x)) FROM t1")), 1000);
    assert_eq!(scalar(&run(&db, "SELECT (SELECT (SELECT t1.z)) FROM t1")), 3000);
}

/// A genuinely-missing qualified table still errors — the chain walk must not
/// mask an out-of-scope reference.
#[test]
fn missing_qualified_table_still_errors() {
    let db = setup_db();
    let select = parse_select("SELECT (SELECT (SELECT nosuch.y)) FROM t1");
    let result = SelectExecutor::new(&db).execute(&select);
    assert!(result.is_err(), "reference to a non-existent table must error");
}
