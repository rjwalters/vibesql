//! Regression tests for issue #5350
//!
//! Subqueries in expression position (scalar subqueries, EXISTS, IN) inside a
//! FROM-less SELECT did not inherit the enclosing statement's CTE bindings, so
//! a name bound by an outer `WITH` clause resolved to a same-named catalog
//! object (view/table) instead of the CTE. SQLite gives the CTE precedence.
//!
//! Repro from the issue:
//!
//! ```sql
//! CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER);
//! INSERT INTO t1 VALUES (1,2),(2,3),(3,2);
//! CREATE VIEW lll AS
//!   SELECT row_number() OVER (PARTITION BY grp_id) AS rn, grp_id, id FROM t1;
//!
//! WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id)
//! SELECT (SELECT max(id) FROM lll WHERE grp_id = 2);
//! -- sqlite3: 100 (CTE row); VibeSQL used to return 3 (view rows)
//! ```
//!
//! All expected values below were verified against sqlite3.

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
        vibesql_ast::Statement::CreateView(create_view) => {
            vibesql_executor::ViewExecutor::execute_create_view(&create_view, db).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

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

/// Setup from the issue body: a view `lll` over `t1`, shadowed by a CTE.
fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES (1,2),(2,3),(3,2)");
    run_stmt(
        &mut db,
        "CREATE VIEW lll AS \
         SELECT row_number() OVER (PARTITION BY grp_id) AS rn, grp_id, id FROM t1",
    );
    db
}

/// The exact repro from issue #5350: sqlite3 returns 100 (CTE shadows view).
#[test]
fn test_scalar_subquery_sees_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT (SELECT max(id) FROM lll WHERE grp_id = 2)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100)]]);
}

/// EXISTS in expression position must also see the CTE (sqlite3: 1).
#[test]
fn test_exists_subquery_sees_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT EXISTS(SELECT 1 FROM lll WHERE id = 100)",
    );
    // VibeSQL represents EXISTS results as Boolean (rendered as 1 by the CLI)
    assert_eq!(rows, vec![vec![SqlValue::Boolean(true)]]);
}

/// IN (SELECT ...) in expression position must also see the CTE (sqlite3: 1).
#[test]
fn test_in_subquery_sees_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT 100 IN (SELECT id FROM lll)",
    );
    // VibeSQL represents IN results as Boolean (rendered as 1 by the CLI)
    assert_eq!(rows, vec![vec![SqlValue::Boolean(true)]]);
}

/// Scalar subquery in the WHERE clause of a FROM-less SELECT (sqlite3: 5).
#[test]
fn test_where_clause_subquery_sees_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT 5 WHERE (SELECT max(id) FROM lll) = 100",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(5)]]);
}

/// CTE names are ASCII case-insensitive, like catalog names (sqlite3: 100).
#[test]
fn test_case_insensitive_cte_reference() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
         SELECT (SELECT max(id) FROM LLL)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100)]]);
}

/// A WITH clause inside the subquery shadows the outer CTE (sqlite3: 200).
#[test]
fn test_inner_with_shadows_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 100 AS id) \
         SELECT (WITH lll AS (SELECT 200 AS id) SELECT max(id) FROM lll)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(200)]]);
}

/// Control: without a CTE the same name still resolves to the catalog view
/// (sqlite3: 3 = max(id) over the view rows).
#[test]
fn test_no_cte_still_resolves_catalog_view() {
    let db = setup_db();
    let rows = query(&db, "SELECT (SELECT max(id) FROM lll WHERE grp_id = 2)");
    assert_eq!(rows, vec![vec![SqlValue::Integer(3)]]);
}

/// CTE context must compose with correlation bindings, not replace them
/// (sqlite3: 11, 12, 13).
#[test]
fn test_cte_composes_with_correlated_subquery() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH c AS (SELECT 10 AS v) \
         SELECT (SELECT t1.id + (SELECT v FROM c)) FROM t1 ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(11)], vec![SqlValue::Integer(12)], vec![SqlValue::Integer(13)],]
    );
}

/// Subqueries on the right side of a set operation see the CTE (sqlite3: 1, 100).
#[test]
fn test_union_branch_subquery_sees_outer_cte() {
    let db = setup_db();
    let mut rows = query(
        &db,
        "WITH lll AS (SELECT 100 AS id) \
         SELECT 1 UNION SELECT (SELECT max(id) FROM lll)",
    );
    rows.sort();
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(100)]]);
}

/// FROM-less SELECT with a window function alongside a CTE-referencing
/// scalar subquery (sqlite3: 100|1).
#[test]
fn test_window_path_subquery_sees_outer_cte() {
    let db = setup_db();
    let rows = query(
        &db,
        "WITH lll AS (SELECT 100 AS id) \
         SELECT (SELECT max(id) FROM lll), min(1) OVER ()",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100), SqlValue::Integer(1)]]);
}
