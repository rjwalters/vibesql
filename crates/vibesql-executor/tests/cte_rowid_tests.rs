//! Regression tests for issue #5516
//!
//! A CTE, like a view or derived table, has no implicit `rowid`. SQLite
//! 3.51.0 errors with `no such column: rowid` (also `oid` / `_rowid_`) when
//! the pseudo-column is referenced against a CTE:
//!
//! ```sql
//! WITH c AS (SELECT a,b FROM t) SELECT rowid FROM c;  -- Error: no such column: rowid
//! ```
//!
//! Before #5516, VibeSQL resolved `rowid` against the CTE and returned rows.
//! The fix marks CTE pseudo-schemas with `is_view = true` so the shared
//! rowid-resolution paths (#5492) reject the pseudo-column.
//!
//! A CTE column *genuinely named* `rowid` must still resolve, and `rowid`
//! against a base table must still work.

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

/// Run a SELECT and return its rows, panicking on error.
fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
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

/// Run a SELECT and return Err if execution fails.
fn try_query(
    db: &vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("{:?}", e))?;
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).map_err(|e| format!("{:?}", e))
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER, b INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(10, 20), (30, 40)");
    db
}

/// Assert the error message mentions a missing rowid pseudo-column.
fn assert_no_such_rowid(err: &str, pseudo: &str) {
    let lower = err.to_lowercase();
    assert!(
        lower.contains(&pseudo.to_lowercase())
            && (lower.contains("not found")
                || lower.contains("no such column")
                || lower.contains("columnnotfound")),
        "expected a 'no such column: {}' style error, got: {}",
        pseudo,
        err
    );
}

#[test]
fn test_rowid_against_cte_errors() {
    // sqlite3 3.51.0: Error: no such column: rowid
    let db = setup_db();
    let err = try_query(&db, "WITH c AS (SELECT a, b FROM t) SELECT rowid FROM c")
        .expect_err("rowid against a CTE must error (no such column: rowid)");
    assert_no_such_rowid(&err, "rowid");
}

#[test]
fn test_oid_against_cte_errors() {
    // sqlite3 3.51.0: Error: no such column: oid
    let db = setup_db();
    let err = try_query(&db, "WITH c AS (SELECT a, b FROM t) SELECT oid FROM c")
        .expect_err("oid against a CTE must error (no such column: oid)");
    assert_no_such_rowid(&err, "oid");
}

#[test]
fn test_underscore_rowid_against_cte_errors() {
    // sqlite3 3.51.0: Error: no such column: _rowid_
    let db = setup_db();
    let err = try_query(&db, "WITH c AS (SELECT a, b FROM t) SELECT _rowid_ FROM c")
        .expect_err("_rowid_ against a CTE must error (no such column: _rowid_)");
    assert_no_such_rowid(&err, "_rowid_");
}

#[test]
fn test_qualified_rowid_against_cte_errors() {
    // Qualified form: SELECT c.rowid FROM c
    let db = setup_db();
    let err = try_query(&db, "WITH c AS (SELECT a, b FROM t) SELECT c.rowid FROM c")
        .expect_err("qualified c.rowid against a CTE must error");
    assert_no_such_rowid(&err, "rowid");
}

#[test]
fn test_cte_with_explicit_columns_rowid_errors() {
    // CTE with an explicit column list (no column named rowid) still has no rowid.
    let db = setup_db();
    let err = try_query(&db, "WITH c(x, y) AS (SELECT a, b FROM t) SELECT rowid FROM c")
        .expect_err("rowid against an explicit-column CTE must error");
    assert_no_such_rowid(&err, "rowid");
}

#[test]
fn test_cte_column_named_rowid_resolves() {
    // CAUTION case: a CTE column genuinely named `rowid` must still resolve to
    // that column (real columns shadow the pseudo-column).
    // sqlite3 3.51.0: WITH c(rowid) AS (SELECT a FROM t) SELECT rowid FROM c -> 10, 30
    let db = setup_db();
    let rows = query(&db, "WITH c(rowid) AS (SELECT a FROM t) SELECT rowid FROM c ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(10)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(30)]);
}

#[test]
fn test_cte_column_aliased_rowid_resolves() {
    // A column aliased to `rowid` in the CTE body must also resolve.
    let db = setup_db();
    let rows =
        query(&db, "WITH c AS (SELECT a AS rowid FROM t) SELECT rowid FROM c ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(10)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(30)]);
}

#[test]
fn test_rowid_against_base_table_still_works() {
    // Base tables still expose the rowid pseudo-column (no regression).
    let db = setup_db();
    let rows = query(&db, "SELECT rowid FROM t ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    // Implicit rowids start at 1 (returned as Bigint).
    assert_eq!(rows[0], vec![SqlValue::Bigint(1)]);
    assert_eq!(rows[1], vec![SqlValue::Bigint(2)]);
}

#[test]
fn test_cte_selecting_base_rowid_then_referencing_works() {
    // A CTE may explicitly SELECT the base table's rowid; the resulting CTE
    // column (named `rowid`) is then referenceable, but it is a real column,
    // not the (now absent) CTE pseudo-column.
    let db = setup_db();
    let rows = query(&db, "WITH c AS (SELECT rowid FROM t) SELECT rowid FROM c ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Bigint(1)]);
    assert_eq!(rows[1], vec![SqlValue::Bigint(2)]);
}

#[test]
fn test_recursive_cte_rowid_errors() {
    // sqlite3 3.51.0: WITH RECURSIVE c(x) AS (...) SELECT rowid FROM c -> no such column: rowid
    let db = setup_db();
    let err = try_query(
        &db,
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<3) SELECT rowid FROM c",
    )
    .expect_err("rowid against a recursive CTE must error");
    assert_no_such_rowid(&err, "rowid");
}

#[test]
fn test_non_rowid_columns_from_cte_still_work() {
    // Sanity: ordinary columns from a CTE are unaffected by the is_view marker.
    let db = setup_db();
    let rows = query(&db, "WITH c AS (SELECT a, b FROM t) SELECT a, b FROM c ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(10), SqlValue::Integer(20)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(30), SqlValue::Integer(40)]);
}
