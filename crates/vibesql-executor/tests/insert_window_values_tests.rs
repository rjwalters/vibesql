//! Regression tests for issue #6190 — window functions inside INSERT VALUES rows.
//!
//! SQLite treats a `VALUES` row that contains a window function as its own
//! single-row `SELECT` coroutine, so `(7, row_number() OVER ())` evaluates to
//! `(7, 1)` (`row_number()` over a single constant row is 1). VibeSQL used to
//! reject such an INSERT with "misuse of window function row_number()" because
//! the INSERT path evaluated each VALUES expression with the plain scalar
//! evaluator. These cases mirror `docs/reference/sqlite/test/values.test`
//! sections 3 and 16; all expected values were verified against SQLite.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("INSERT failed: {} -- {:?}", sql, e));
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
            .into_iter()
            .map(|row| row.values.to_vec())
            .collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn int(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

/// values.test 3.1.1 / 3.1.2 — a single window row in the middle of the VALUES
/// list. `row_number() OVER ()` over its own one-row coroutine is 1.
#[test]
fn test_insert_values_single_window_row() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE y1(x, y)");
    run_stmt(&mut db, "INSERT INTO y1 VALUES(1, 2), (3, 4), (row_number() OVER (), 5)");

    let rows = query(&db, "SELECT * FROM y1");
    assert_eq!(rows, vec![vec![int(1), int(2)], vec![int(3), int(4)], vec![int(1), int(5)],]);
}

/// values.test 3.2.1 — two separate window rows. Each is its own single-row
/// coroutine, so both `row_number() OVER ()` values are 1 (they do not
/// accumulate across rows).
#[test]
fn test_insert_values_two_window_rows_each_reset() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE y1(x, y)");
    run_stmt(
        &mut db,
        "INSERT INTO y1 VALUES(1, 2), (3, 4), (row_number() OVER (), 6), (row_number() OVER (), 7)",
    );

    let rows = query(&db, "SELECT * FROM y1");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(2)],
            vec![int(3), int(4)],
            vec![int(1), int(6)],
            vec![int(1), int(7)],
        ]
    );
}

/// values.test 16.2 — a window row surrounded by plain rows on both sides.
#[test]
fn test_insert_values_window_row_between_plain_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(a, b)");
    run_stmt(
        &mut db,
        "INSERT INTO t1 VALUES(1,2),(3,4),(5,6),(7,row_number()OVER()),\
         (9,10), (11,12), (13,14), (15,16)",
    );

    let rows = query(&db, "SELECT * FROM t1 ORDER BY a, b");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(2)],
            vec![int(3), int(4)],
            vec![int(5), int(6)],
            vec![int(7), int(1)],
            vec![int(9), int(10)],
            vec![int(11), int(12)],
            vec![int(13), int(14)],
            vec![int(15), int(16)],
        ]
    );
}

/// values.test 16.4 — the leading row carries the window function.
#[test]
fn test_insert_values_leading_window_row() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(a, b)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES(1,row_number()OVER()),(2,3), (4,5), (6,7)");

    let rows = query(&db, "SELECT * FROM t1 ORDER BY a, b");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(1)],
            vec![int(2), int(3)],
            vec![int(4), int(5)],
            vec![int(6), int(7)],
        ]
    );
}

/// A plain INSERT VALUES with no window function must be entirely unaffected by
/// the window pre-evaluation fast path (regression guard for the common case).
#[test]
fn test_insert_values_no_window_unchanged() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(a, b)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES(1,2),(3,4),(5,6)");

    let rows = query(&db, "SELECT * FROM t1");
    assert_eq!(rows, vec![vec![int(1), int(2)], vec![int(3), int(4)], vec![int(5), int(6)],]);
}
