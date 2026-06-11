//! Regression tests for issue #5363
//!
//! `UPDATE ... FROM` (the multi-table UPDATE extension) takes a separate
//! execution path (`execute_update_from`) from plain UPDATE. After #5362
//! threaded WITH-clause CTE context into plain UPDATE/DELETE RETURNING,
//! the UPDATE ... FROM path still passed `None` to
//! `dml_returning::project_returning`, so a CTE-referencing subquery in
//! RETURNING failed with "Table not found" even though the same CTE was
//! already visible to SET and WHERE subqueries (those run inside a
//! synthetic SELECT that carries the statement's `with_clause`).
//!
//! Repro from the issue:
//!
//! ```sql
//! CREATE TABLE t(a INTEGER, b INTEGER);
//! INSERT INTO t VALUES(1, 0);
//! CREATE TABLE s(a INTEGER, v INTEGER);
//! INSERT INTO s VALUES(1, 7);
//! WITH c AS (SELECT 100 AS bonus)
//! UPDATE t SET b = s.v FROM s WHERE t.a = s.a
//! RETURNING b, (SELECT bonus FROM c);
//! -- sqlite3 3.51.0: returns 7|100
//! -- VibeSQL used to fail: Table 'c' not found
//! ```
//!
//! CTE precedence matches #5350/#5352 semantics: CTE names shadow same-named
//! catalog tables/views and resolve ASCII case-insensitively.
//!
//! All expected values below were verified against sqlite3 3.51.0.

use vibesql_executor::{SelectExecutor, UpdateExecutor};
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("Insert failed: {} -- {:?}", sql, e));
        }
        vibesql_ast::Statement::Update(update) => {
            UpdateExecutor::execute(&update, db)
                .unwrap_or_else(|e| panic!("Update failed: {} -- {:?}", sql, e));
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

/// Run an UPDATE and return (affected_count, RETURNING rows).
fn run_update_returning(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> (usize, Vec<Vec<SqlValue>>) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    let vibesql_ast::Statement::Update(update) = stmt else {
        panic!("Expected UPDATE statement: {}", sql);
    };
    let (count, returning) = UpdateExecutor::execute_returning(&update, db)
        .unwrap_or_else(|e| panic!("Update failed: {} -- {:?}", sql, e));
    let rows = returning
        .expect("RETURNING result expected")
        .rows
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect();
    (count, rows)
}

/// Standard two-table fixture: t(a, b) = (1, 0); s(a, v) = (1, 7).
fn setup_t_and_s(db: &mut vibesql_storage::Database) {
    run_stmt(db, "CREATE TABLE t(a INTEGER, b INTEGER)");
    run_stmt(db, "INSERT INTO t VALUES(1, 0)");
    run_stmt(db, "CREATE TABLE s(a INTEGER, v INTEGER)");
    run_stmt(db, "INSERT INTO s VALUES(1, 7)");
}

/// The exact repro from issue #5363 (sqlite3: returns 7|100).
#[test]
fn test_update_from_returning_cte_subquery() {
    let mut db = vibesql_storage::Database::new();
    setup_t_and_s(&mut db);

    let (count, rows) = run_update_returning(
        &mut db,
        "WITH c AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v FROM s WHERE t.a = s.a \
         RETURNING b, (SELECT bonus FROM c)",
    );
    assert_eq!(count, 1);
    assert_eq!(rows, vec![vec![SqlValue::Integer(7), SqlValue::Integer(100)]]);
    assert_eq!(query(&db, "SELECT b FROM t"), vec![vec![SqlValue::Integer(7)]]);
}

/// CTE referenced from both a SET subquery and a RETURNING subquery in the
/// same UPDATE ... FROM statement (sqlite3: returns 107|101).
#[test]
fn test_update_from_cte_in_set_and_returning() {
    let mut db = vibesql_storage::Database::new();
    setup_t_and_s(&mut db);

    let (count, rows) = run_update_returning(
        &mut db,
        "WITH c AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v + (SELECT bonus FROM c) FROM s WHERE t.a = s.a \
         RETURNING b, (SELECT bonus FROM c) + 1",
    );
    assert_eq!(count, 1);
    assert_eq!(rows, vec![vec![SqlValue::Integer(107), SqlValue::Integer(101)]]);
}

/// The CTE itself is the FROM source of UPDATE ... FROM, and RETURNING also
/// references it (sqlite3: 1|50|60 and 2|60|60).
#[test]
fn test_update_from_cte_as_from_source() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER, b INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1, 0)");
    run_stmt(&mut db, "INSERT INTO t VALUES(2, 0)");

    let (count, mut rows) = run_update_returning(
        &mut db,
        "WITH src(a, v) AS (VALUES(1, 50), (2, 60)) \
         UPDATE t SET b = src.v FROM src WHERE t.a = src.a \
         RETURNING a, b, (SELECT max(v) FROM src)",
    );
    assert_eq!(count, 2);
    rows.sort_by(|x, y| x[0].partial_cmp(&y[0]).unwrap());
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(50), SqlValue::Integer(60)],
            vec![SqlValue::Integer(2), SqlValue::Integer(60), SqlValue::Integer(60)],
        ]
    );
    assert_eq!(
        query(&db, "SELECT a, b FROM t ORDER BY a"),
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(50)],
            vec![SqlValue::Integer(2), SqlValue::Integer(60)],
        ]
    );
}

/// CTE names resolve ASCII case-insensitively in UPDATE ... FROM RETURNING
/// (sqlite3: returns 7|100|100).
#[test]
fn test_update_from_returning_cte_case_insensitive() {
    let mut db = vibesql_storage::Database::new();
    setup_t_and_s(&mut db);

    let (count, rows) = run_update_returning(
        &mut db,
        "WITH MyCte AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v FROM s WHERE t.a = s.a \
         RETURNING b, (SELECT bonus FROM MYCTE), (SELECT bonus FROM mycte)",
    );
    assert_eq!(count, 1);
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(7), SqlValue::Integer(100), SqlValue::Integer(100)]]
    );
}

/// A CTE shadows a same-named catalog table inside the RETURNING subquery
/// (sqlite3: returns 7|100, not the catalog table's 999).
#[test]
fn test_update_from_returning_cte_shadows_catalog_table() {
    let mut db = vibesql_storage::Database::new();
    setup_t_and_s(&mut db);
    run_stmt(&mut db, "CREATE TABLE c(bonus INTEGER)");
    run_stmt(&mut db, "INSERT INTO c VALUES(999)");

    let (count, rows) = run_update_returning(
        &mut db,
        "WITH c AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v FROM s WHERE t.a = s.a \
         RETURNING b, (SELECT bonus FROM c)",
    );
    assert_eq!(count, 1);
    assert_eq!(rows, vec![vec![SqlValue::Integer(7), SqlValue::Integer(100)]]);

    // The catalog table is untouched.
    assert_eq!(query(&db, "SELECT bonus FROM c"), vec![vec![SqlValue::Integer(999)]]);
}

/// Zero rows matched: RETURNING yields no rows and the CTE reference must
/// not error (sqlite3: empty result, no error).
#[test]
fn test_update_from_returning_cte_zero_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER, b INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1, 0)");
    run_stmt(&mut db, "CREATE TABLE s(a INTEGER, v INTEGER)");
    // s is empty: the join matches nothing.

    let (count, rows) = run_update_returning(
        &mut db,
        "WITH c AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v FROM s WHERE t.a = s.a \
         RETURNING b, (SELECT bonus FROM c)",
    );
    assert_eq!(count, 0);
    assert!(rows.is_empty());
    assert_eq!(query(&db, "SELECT b FROM t"), vec![vec![SqlValue::Integer(0)]]);
}

/// Regression guard for the already-working paths: CTE subqueries in SET and
/// WHERE of UPDATE ... FROM (sqlite3: b = 107, then b = 7 for the WHERE case).
#[test]
fn test_update_from_cte_in_set_and_where_regression() {
    let mut db = vibesql_storage::Database::new();
    setup_t_and_s(&mut db);

    // SET subquery referencing the CTE (sqlite3: b = 107).
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 100 AS bonus) \
         UPDATE t SET b = s.v + (SELECT bonus FROM c) FROM s WHERE t.a = s.a",
    );
    assert_eq!(query(&db, "SELECT b FROM t"), vec![vec![SqlValue::Integer(107)]]);

    // WHERE subquery referencing the CTE (sqlite3: b = 7).
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 1 AS k) \
         UPDATE t SET b = s.v FROM s WHERE t.a = s.a AND t.a = (SELECT k FROM c)",
    );
    assert_eq!(query(&db, "SELECT b FROM t"), vec![vec![SqlValue::Integer(7)]]);
}
