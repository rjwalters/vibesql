//! Regression tests for the window1.test quick-win fixes (issue #5190)
//!
//! Covers:
//! - 65.2/65.3: COLLATE propagating through an aggregate into an IN comparison
//! - 15.0: exact SQLite error for window functions in recursive CTEs
//! - 15.2: FROM-less SELECT whose WHERE references a select-list alias
//! - 32.10: CREATE VIEW errors prefixed with "error in view <name>: "
//! - 61.1: CAST to an unknown type name applies SQLite affinity rules

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

fn execute_sql(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| ExecutorError::ParseError(format!("{:?}", e)))?;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
            Ok(vec![])
        }
        Statement::CreateView(view_stmt) => {
            crate::advanced_objects::execute_create_view(&view_stmt, db)?;
            Ok(vec![])
        }
        Statement::Select(select_stmt) => Ok(SelectExecutor::new(db).execute(&select_stmt)?),
        other => {
            Err(ExecutorError::UnsupportedFeature(format!("Unsupported statement: {:?}", other)))
        }
    }
}

// ------------------------------------------------------------------------
// 65.2 / 65.3 — COLLATE through aggregate into IN
// ------------------------------------------------------------------------

#[test]
fn test_collate_nocase_through_aggregate_into_in_subquery() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(c1 VARCHAR(10))").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES('abcd')").unwrap();

    // window1.test 65.2: max(c1 COLLATE nocase) must compare case-insensitively
    let rows =
        execute_sql(&mut db, "SELECT max(c1 COLLATE nocase) IN (SELECT 'aBCd') FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(
            rows[0].values[0],
            vibesql_types::SqlValue::Boolean(true) | vibesql_types::SqlValue::Integer(1)
        ),
        "expected truthy IN result, got {:?}",
        rows[0].values[0]
    );
}

#[test]
fn test_aggregate_in_subquery_without_collate_stays_binary() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(c1 VARCHAR(10))").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES('abcd')").unwrap();

    // Without COLLATE nocase, BINARY comparison must NOT match
    let rows = execute_sql(&mut db, "SELECT max(c1) IN (SELECT 'aBCd') FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(
            rows[0].values[0],
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Integer(0)
        ),
        "expected falsy IN result, got {:?}",
        rows[0].values[0]
    );
}

// ------------------------------------------------------------------------
// 15.0 — window functions in recursive CTEs
// ------------------------------------------------------------------------

#[test]
fn test_window_function_in_recursive_cte_errors_with_sqlite_message() {
    let mut db = vibesql_storage::Database::new();
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE q(x, rn) AS (
            SELECT 1, 1
            UNION ALL
            SELECT x+1, ROW_NUMBER() OVER (ORDER BY x) FROM q WHERE x < 3
         )
         SELECT * FROM q",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "cannot use window functions in recursive queries");
}

// ------------------------------------------------------------------------
// 15.2 — FROM-less SELECT with alias referenced from WHERE
// ------------------------------------------------------------------------

#[test]
fn test_fromless_select_alias_in_where_falsy() {
    let mut db = vibesql_storage::Database::new();
    // ''+'' coerces to 0 → WHERE is false → no rows (window1.test 15.2)
    let rows = execute_sql(&mut db, "SELECT (SELECT '') x WHERE x+x").unwrap();
    assert!(rows.is_empty(), "expected no rows, got {:?}", rows);
}

#[test]
fn test_fromless_select_alias_in_where_truthy() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(&mut db, "SELECT (SELECT 1) x WHERE x+x").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_fromless_select_where_unknown_column_still_errors() {
    let mut db = vibesql_storage::Database::new();
    let result = execute_sql(&mut db, "SELECT 1 x WHERE y");
    assert!(result.is_err(), "unknown column in WHERE should still error");
}

// ------------------------------------------------------------------------
// 32.10 — CREATE VIEW error prefix
// ------------------------------------------------------------------------

#[test]
fn test_create_view_error_is_prefixed_with_view_name() {
    let mut db = vibesql_storage::Database::new();
    // Invalid ORDER BY term in a compound view query (window1.test 32.10 shape)
    let err = execute_sql(
        &mut db,
        "CREATE VIEW a AS SELECT NULL INTERSECT SELECT NULL ORDER BY s() OVER R",
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.starts_with("error in view a: "),
        "expected 'error in view a: ' prefix, got: {msg}"
    );
}

#[test]
fn test_create_view_set_operation_mismatch_is_not_prefixed() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t01(x INTEGER, y INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t02(x INTEGER)").unwrap();

    // select7.test 8.2 shape: SQLite surfaces set-operation arity mismatches at
    // query time with NO "error in view" prefix; VibeSQL compiles views eagerly,
    // so the bare message must surface at CREATE VIEW time.
    let err =
        execute_sql(&mut db, "CREATE VIEW v0 AS SELECT x, y FROM t01 UNION SELECT x FROM t02")
            .unwrap_err();
    assert_eq!(
        err.to_string(),
        "SELECTs to the left and right of UNION do not have the same number of result columns"
    );
}

#[test]
fn test_create_view_intersect_mismatch_is_not_prefixed() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t01(x INTEGER, y INTEGER)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t02(x INTEGER)").unwrap();

    let err =
        execute_sql(&mut db, "CREATE VIEW v1 AS SELECT x, y FROM t01 INTERSECT SELECT x FROM t02")
            .unwrap_err();
    let msg = err.to_string();
    assert!(
        !msg.starts_with("error in view"),
        "INTERSECT arity mismatch must not carry the view prefix, got: {msg}"
    );
    assert!(msg.contains("INTERSECT"), "expected INTERSECT in message, got: {msg}");
}

// ------------------------------------------------------------------------
// 61.1 — CAST to unknown type names (SQLite affinity rules)
// ------------------------------------------------------------------------

#[test]
fn test_cast_to_unknown_typename_numeric_affinity() {
    let mut db = vibesql_storage::Database::new();

    // Non-numeric text → 0 (NUMERIC affinity)
    let rows = execute_sql(&mut db, "SELECT CAST('seventeen' AS banana)").unwrap();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(0));

    // Integral text → INTEGER
    let rows = execute_sql(&mut db, "SELECT CAST('5' AS banana)").unwrap();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(5));

    // Fractional text → REAL
    let rows = execute_sql(&mut db, "SELECT CAST('1.5' AS banana)").unwrap();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Double(1.5));

    // Numeric values keep their storage class
    let rows = execute_sql(&mut db, "SELECT CAST(7 AS banana)").unwrap();
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(7));
}

#[test]
fn test_cast_empty_typename_passes_value_through_sum() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(5),(NULL),('seventeen')").unwrap();

    // window1.test 61.1 inner fragment: sum over CAST(a AS ) must execute
    let rows = execute_sql(&mut db, "SELECT sum(CAST(a AS )) FROM t1").unwrap();
    assert_eq!(rows.len(), 1, "sum(CAST(a AS )) should produce one row");
}
