//! Regression tests for aggregate `FILTER (WHERE ...)` clause semantics.
//!
//! Covers the filter1.test edge cases fixed under issue #6191:
//! - a bare `count()` (zero-arg, == `count(*)`) still honors a FILTER clause
//!   (filter1-8.0);
//! - a bare column reference with GROUP BY follows the row that produced the
//!   *filtered* MIN/MAX — and falls back to the first row of the group when the
//!   FILTER excludes every row (filter1-3.3 / 3.5);
//! - an aggregate nested inside another aggregate's FILTER is rejected with
//!   SQLite's "misuse of aggregate function X()" wording (filter1-2.3).

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn int(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

fn text(s: &str) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(s))
}

/// Execute one DDL/DML statement against `db`.
fn exec(db: &mut vibesql_storage::Database, sql: &str) {
    match vibesql_parser::Parser::parse_sql(sql).unwrap() {
        vibesql_ast::Statement::CreateTable(ct) => {
            vibesql_executor::CreateTableExecutor::execute(&ct, db).unwrap();
        }
        vibesql_ast::Statement::Insert(ins) => {
            vibesql_executor::InsertExecutor::execute(db, &ins).unwrap();
        }
        other => panic!("unexpected statement for `{sql}`: {other:?}"),
    }
}

/// Run a SELECT and return its rows as nested value vectors.
fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let executor = SelectExecutor::new(db);
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let result = executor.execute_with_columns(&select).unwrap();
        result.rows.into_iter().map(|r| r.values.to_vec()).collect()
    } else {
        panic!("expected SELECT for `{sql}`");
    }
}

// filter1-8.0: a bare `count()` with a FILTER counts only the matching rows.
#[test]
fn test_bare_count_with_filter() {
    let mut db = vibesql_storage::Database::new();
    exec(&mut db, "CREATE TABLE t(a, b)");
    exec(&mut db, "INSERT INTO t(a, b) VALUES (1, NULL), (2, 3), (4, NULL)");

    // count() == count(*), so with no FILTER it counts every row.
    assert_eq!(query(&db, "SELECT count() FROM t"), vec![vec![int(3)]]);
    // With a FILTER only the two NULL-b rows qualify.
    assert_eq!(query(&db, "SELECT count() FILTER (WHERE b IS NULL) FROM t"), vec![vec![int(2)]]);
}

// filter1-3.3: `max(b) FILTER (WHERE c='x')` matches no rows in either group,
// so the bare column `c` falls back to the *first* row of each group (3 and 6),
// not the last.
#[test]
fn test_bare_column_falls_back_to_first_row_when_filter_empty() {
    let mut db = vibesql_storage::Database::new();
    exec(&mut db, "CREATE TABLE t2(a, b, c)");
    exec(&mut db, "INSERT INTO t2(a, b, c) VALUES (1, 2, 3), (1, 3, 4), (2, 5, 6), (2, 7, 8)");

    let rows = query(&db, "SELECT a, c, max(b) FILTER (WHERE c='x') FROM t2 GROUP BY a");
    assert_eq!(
        rows,
        vec![vec![int(1), int(3), SqlValue::Null], vec![int(2), int(6), SqlValue::Null],]
    );
}

// filter1-3.5: the bare column follows the row producing the *filtered* max.
// Group a=1 has c='x' rows, so max(b) FILTER picks b=5 from the ('x') row and
// the bare c reports 'x'. Group a=2 has no c='x' row, so it falls back to the
// first row (c=6) with a NULL max.
#[test]
fn test_bare_column_follows_filtered_max_row() {
    let mut db = vibesql_storage::Database::new();
    exec(&mut db, "CREATE TABLE t2(a, b, c)");
    exec(
        &mut db,
        "INSERT INTO t2(a, b, c) VALUES (1, 5, 'x'), (1, 2, 3), (1, 4, 'x'), (2, 5, 6), (2, 7, 8)",
    );

    let rows = query(&db, "SELECT a, c, max(b) FILTER (WHERE c='x') FROM t2 GROUP BY a");
    assert_eq!(rows, vec![vec![int(1), text("x"), int(5)], vec![int(2), int(6), SqlValue::Null],]);
}

// filter1-2.3: an aggregate nested inside another aggregate's FILTER is a misuse
// reported as "misuse of aggregate function count()".
#[test]
fn test_aggregate_nested_in_filter_is_misuse() {
    let mut db = vibesql_storage::Database::new();
    exec(&mut db, "CREATE TABLE t1(a)");
    exec(&mut db, "INSERT INTO t1(a) VALUES (1), (2), (3)");

    let executor = SelectExecutor::new(&db);
    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT sum(a) FILTER (WHERE 1 - count(a)) FROM t1")
            .unwrap();
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let err = executor.execute_with_columns(&select).expect_err("nested aggregate must error");
    assert!(
        err.to_string().contains("misuse of aggregate function count()"),
        "unexpected error: {err}"
    );
}
