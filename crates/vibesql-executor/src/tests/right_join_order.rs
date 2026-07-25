//! Regression tests for issue #6190: RIGHT JOIN row ordering.
//!
//! VibeSQL previously implemented `RIGHT OUTER JOIN` by swapping the operands
//! into a `LEFT OUTER JOIN` and reversing the output columns. That produced the
//! correct result *set* but a right-driven row *order*, which disagreed with
//! SQLite (values.test 8.1.3 / 8.1.4). SQLite drives a RIGHT JOIN from the left
//! table in the nested loop — matched rows come out in left-to-right order and
//! the unmatched right rows are appended at the end. These tests pin that order.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::select::SelectExecutor;
use crate::{CreateTableExecutor, InsertExecutor};

fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap();
        }
        other => panic!("unexpected statement type for exec(): {other:?}"),
    }
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("expected SELECT statement: {sql:?}");
    };
    let executor = SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .unwrap_or_else(|e| panic!("execute {sql:?}: {e:?}"))
        .into_iter()
        .map(|r| r.values.to_vec())
        .collect()
}

fn i(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

/// RIGHT JOIN with no ON clause is a cross product. SQLite emits it in
/// left-outer / right-inner order (values.test 8.1.3).
#[test]
fn right_join_no_condition_is_left_driven_order() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    exec(&mut db, "CREATE TABLE t1(x)");
    exec(&mut db, "INSERT INTO t1 VALUES(10), (20)");

    let rows = query(&db, "SELECT * FROM t1 RIGHT JOIN ( VALUES(1, 2), (3, 4) )");

    // Expected order: each left row (10, then 20) paired with every right row
    // in order — NOT right-driven (which would interleave differently).
    assert_eq!(
        rows,
        vec![
            vec![i(10), i(1), i(2)],
            vec![i(10), i(3), i(4)],
            vec![i(20), i(1), i(2)],
            vec![i(20), i(3), i(4)],
        ]
    );
}

/// RIGHT JOIN with an ON clause: matched rows first (in left order), then the
/// unmatched right rows appended with NULL left columns (values.test 8.1.4).
#[test]
fn right_join_with_condition_appends_unmatched_right_last() {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    exec(&mut db, "CREATE TABLE t1(x)");
    exec(&mut db, "INSERT INTO t1 VALUES(10), (20), (123)");

    let rows = query(
        &db,
        "SELECT * FROM t1 RIGHT JOIN ( VALUES(1, 2), (3, 4), (123, 99) ) ON (column1 = x)",
    );

    // 123 is the only left value with a matching right column1 -> matched row
    // comes first; the two unmatched right rows are appended with NULL x.
    assert_eq!(
        rows,
        vec![
            vec![i(123), i(123), i(99)],
            vec![SqlValue::Null, i(1), i(2)],
            vec![SqlValue::Null, i(3), i(4)],
        ]
    );
}
