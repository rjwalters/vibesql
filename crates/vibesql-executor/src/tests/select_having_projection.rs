//! SELECT documentation-evidence semantics for HAVING projection and
//! GROUP BY validation (issue #6192, SQLite `e_select.test`).
//!
//! Two behaviors are pinned here independent of the TCL harness:
//!
//! 1. An aggregate that appears *only* in a HAVING predicate must not leak into the result set as
//!    an extra column. The native columnar aggregation path appended the HAVING-only aggregate to
//!    the row layout so the predicate could be evaluated, but never stripped it from the output
//!    projection (`e_select-4.13.1.*`).
//!
//! 2. An aggregate function used directly in a GROUP BY expression is a compile-time error in
//!    SQLite with the fixed wording "aggregate functions are not allowed in the GROUP BY clause"
//!    (`e_select-4.12.*`), not the generic "misuse of aggregate" message.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

/// Execute a DDL/DML statement, panicking on any error.
fn exec_setup(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db).unwrap();
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).unwrap();
        }
        other => panic!("unexpected setup statement: {other:?}"),
    }
}

/// Run a SELECT and return the resulting rows.
fn run_select(
    db: &vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, crate::ExecutorError> {
    let stmt = Parser::parse_sql(sql).unwrap();
    match stmt {
        Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt)
        }
        other => panic!("expected SELECT, got {other:?}"),
    }
}

fn c1_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    exec_setup(&mut db, "CREATE TABLE c1(up, down)");
    for (up, down) in [("x", 1), ("x", 2), ("x", 4), ("x", 8), ("y", 16), ("y", 32)] {
        exec_setup(&mut db, &format!("INSERT INTO c1 VALUES('{up}', {down})"));
    }
    db
}

/// e_select-4.13.1.1: a HAVING-only aggregate (`count(*)`) must not appear in
/// the projected result — the query selects a single column `up`.
#[test]
fn having_only_count_star_does_not_leak_column() {
    let db = c1_db();
    let rows = run_select(&db, "SELECT up FROM c1 GROUP BY up HAVING count(*)>3").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 1, "HAVING count(*) leaked into projection");
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar("x".into()));
}

/// e_select-4.13.1.2 / 4.13.1.3: a HAVING-only `sum()` aggregate must not leak.
#[test]
fn having_only_sum_does_not_leak_column() {
    let db = c1_db();

    let rows = run_select(&db, "SELECT up FROM c1 GROUP BY up HAVING sum(down)>16").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 1, "HAVING sum(down) leaked into projection");
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar("y".into()));

    let rows = run_select(&db, "SELECT up FROM c1 GROUP BY up HAVING sum(down)<16").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 1, "HAVING sum(down) leaked into projection");
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar("x".into()));
}

/// A HAVING aggregate that is *also* selected must still appear exactly once.
#[test]
fn having_aggregate_also_in_select_is_not_double_counted() {
    let db = c1_db();
    let rows =
        run_select(&db, "SELECT up, count(*) FROM c1 GROUP BY up HAVING count(*)>3").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 2, "shared HAVING/SELECT aggregate changed column count");
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar("x".into()));
}

/// A non-aggregate HAVING predicate keeps the projection intact (control).
#[test]
fn non_aggregate_having_keeps_projection() {
    let db = c1_db();
    let rows = run_select(&db, "SELECT up FROM c1 GROUP BY up HAVING up='y'").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar("y".into()));
}

fn b3_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    exec_setup(&mut db, "CREATE TABLE b3(a COLLATE nocase, b COLLATE binary)");
    for (a, b) in [("abc", "abc"), ("aBC", "aBC"), ("Def", "Def"), ("dEF", "dEF")] {
        exec_setup(&mut db, &format!("INSERT INTO b3 VALUES('{a}', '{b}')"));
    }
    db
}

/// e_select-4.12.*: an aggregate function used directly in GROUP BY is a
/// compile-time error with SQLite's exact wording.
#[test]
fn aggregate_in_group_by_is_rejected_with_sqlite_wording() {
    let db = b3_db();

    for sql in [
        "SELECT * FROM b3 GROUP BY count(*)",
        "SELECT max(a) FROM b3 GROUP BY max(b)",
        "SELECT group_concat(a) FROM b3 GROUP BY a, max(b)",
    ] {
        let err = run_select(&db, sql).expect_err(sql);
        assert_eq!(
            err.to_string(),
            "aggregate functions are not allowed in the GROUP BY clause",
            "wrong error for: {sql}"
        );
    }
}

/// A non-aggregate expression in GROUP BY (including unary `+`) is still legal.
#[test]
fn non_aggregate_group_by_expression_is_allowed() {
    let db = b3_db();
    // `+a` is a unary-plus expression, not an aggregate — must not be rejected.
    let rows = run_select(&db, "SELECT count(*) FROM b3 GROUP BY +a").unwrap();
    assert!(!rows.is_empty(), "GROUP BY +a should be a legal grouping key");
}
