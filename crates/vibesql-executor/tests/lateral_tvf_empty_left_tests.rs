//! Regression tests for issue #5989 (PR #5998 judge feedback): the empty-left
//! side of a lateral TVF dependent join.
//!
//! When the left sibling of `FROM t, json_each(t.j)` produces **zero rows**
//! (either a statically-empty base table, or a subquery/WHERE that filters the
//! left side down to nothing at runtime), the correct result is 0 rows — the
//! lateral TVF is never evaluated because there is no left row to correlate its
//! argument against.
//!
//! The original implementation, on the empty-left path, re-invoked
//! `execute_table_function(..., None, None)` purely to recover the fixed
//! 8-column output schema. But `execute_table_function` evaluates the TVF
//! argument (`t.j`) *before* building the schema, and with a `None` outer
//! context the correlated column `t.j` cannot resolve, so the query errored:
//!
//! ```text
//! Error evaluating json_each() argument: no such column: t.j
//! ```
//!
//! sqlite3 3.51.0 returns 0 rows (exit 0) for all cases below. The fix builds
//! the fixed schema directly (no argument evaluation) on the empty-left path.
//!
//! ```sql
//! -- statically-empty base table
//! CREATE TABLE t(id INTEGER, j TEXT);   -- no rows
//! SELECT t.id, je.value FROM t, json_each(t.j) AS je;   -- sqlite3: 0 rows
//!
//! -- dynamically-empty left side (WHERE eliminates all rows)
//! CREATE TABLE t(id INTEGER, j TEXT); INSERT INTO t VALUES(1,'[1,2]');
//! SELECT x.id, je.value FROM (SELECT * FROM t WHERE id>99) x, json_each(x.j) AS je;
//! -- sqlite3: 0 rows
//! ```
//!
//! All expected values below were verified against sqlite3 3.51.0.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
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

/// Statically-empty base table: `SELECT * FROM t, json_each(t.j)` over a table
/// with no rows returns 0 rows (not an error). sqlite3 3.51.0: 0 rows.
#[test]
fn lateral_tvf_over_statically_empty_table_returns_zero_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(id INTEGER, j TEXT)");

    // No inserts: t is empty.
    let rows = query(&db, "SELECT t.id, je.value FROM t, json_each(t.j) AS je");
    assert!(rows.is_empty(), "expected 0 rows, got {:?}", rows);

    // The bare `SELECT *` form (which relies on the merged left+TVF schema being
    // recoverable without a left row) must also succeed and return 0 rows.
    let rows_star = query(&db, "SELECT * FROM t, json_each(t.j) AS je");
    assert!(rows_star.is_empty(), "expected 0 rows, got {:?}", rows_star);
}

/// Dynamically-empty left side: the base table has rows, but a WHERE clause in
/// a subquery filters the left side to zero rows before the lateral join. The
/// correlated argument `x.j` still must not be evaluated against a NULL context.
/// sqlite3 3.51.0: 0 rows.
#[test]
fn lateral_tvf_over_dynamically_empty_left_returns_zero_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(id INTEGER, j TEXT)");
    run_stmt(&mut db, "INSERT INTO t VALUES (1, '[1,2]')");

    // `id > 99` eliminates the only row, so the left side is empty at runtime.
    let rows = query(
        &db,
        "SELECT x.id, je.value FROM (SELECT * FROM t WHERE id > 99) x, json_each(x.j) AS je",
    );
    assert!(rows.is_empty(), "expected 0 rows, got {:?}", rows);
}

/// Dynamically-empty left via a top-level WHERE on the base table (no subquery),
/// exercising the same empty-left schema-recovery path. sqlite3 3.51.0: 0 rows.
#[test]
fn lateral_tvf_top_level_where_empties_left_returns_zero_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(id INTEGER, j TEXT)");
    run_stmt(&mut db, "INSERT INTO t VALUES (1, '[1,2]')");

    let rows = query(&db, "SELECT t.id, je.value FROM t, json_each(t.j) AS je WHERE t.id > 99");
    assert!(rows.is_empty(), "expected 0 rows, got {:?}", rows);
}

/// Non-empty control: confirms the fix does not regress the happy path — the
/// TVF is still evaluated per left row when the left side has rows.
/// sqlite3 3.51.0: (1,10),(1,20).
#[test]
fn lateral_tvf_non_empty_left_still_expands() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(id INTEGER, j TEXT)");
    run_stmt(&mut db, "INSERT INTO t VALUES (1, '[10,20]')");

    let rows = query(&db, "SELECT t.id, je.value FROM t, json_each(t.j) AS je");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![SqlValue::Integer(1), SqlValue::Integer(20)],
        ],
        "expected the TVF to expand per left row"
    );
}
