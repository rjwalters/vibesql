//! Regression tests for issue #5359
//!
//! SQLite allows a `WITH` clause on INSERT (and UPDATE/DELETE), with CTE
//! names visible to subqueries in the VALUES rows, the `INSERT ... SELECT`
//! source, the upsert `ON CONFLICT DO UPDATE` arm, and RETURNING
//! expressions. VibeSQL parsed `WITH ... INSERT` but the INSERT executor
//! dropped the statement's `with_clause` on the floor, so subqueries inside
//! VALUES rows failed with "Table not found".
//!
//! Repro from the issue:
//!
//! ```sql
//! CREATE TABLE t(a INTEGER);
//! WITH c AS (SELECT 8) INSERT INTO t VALUES((SELECT * FROM c));
//! -- sqlite3 3.51.0: inserts 8
//! -- VibeSQL used to fail: Table 'c' not found
//! ```
//!
//! CTE precedence matches #5350/#5352 semantics: CTE names shadow same-named
//! catalog tables/views and resolve ASCII case-insensitively.
//!
//! All expected values below were verified against sqlite3 3.51.0.

use vibesql_executor::{DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor};
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("Insert failed: {} -- {:?}", sql, e));
        }
        vibesql_ast::Statement::Update(update) => {
            UpdateExecutor::execute(&update, db)
                .unwrap_or_else(|e| panic!("Update failed: {} -- {:?}", sql, e));
        }
        vibesql_ast::Statement::Delete(delete) => {
            DeleteExecutor::execute(&delete, db)
                .unwrap_or_else(|e| panic!("Delete failed: {} -- {:?}", sql, e));
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

/// The exact repro from issue #5359 (sqlite3: inserts 8).
#[test]
fn test_with_insert_values_scalar_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(&mut db, "WITH c AS (SELECT 8) INSERT INTO t VALUES((SELECT * FROM c))");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(8)]]);
}

/// Multiple VALUES rows, each with a CTE-referencing subquery (sqlite3: 5, 6).
#[test]
fn test_with_insert_values_multi_row() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 5 AS v) \
         INSERT INTO t VALUES((SELECT v FROM c)), ((SELECT v+1 FROM c))",
    );
    assert_eq!(
        query(&db, "SELECT a FROM t ORDER BY a"),
        vec![vec![SqlValue::Integer(5)], vec![SqlValue::Integer(6)]]
    );
}

/// `WITH ... INSERT INTO t SELECT ... FROM cte` (sqlite3: 8).
#[test]
fn test_with_insert_select_from_cte() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(&mut db, "WITH c AS (SELECT 8 AS v) INSERT INTO t SELECT v FROM c");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(8)]]);
}

/// CTE referenced from the WHERE clause of an INSERT ... SELECT
/// (sqlite3: inserts 20, 30).
#[test]
fn test_with_insert_select_cte_in_where() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE src(x INTEGER)");
    run_stmt(&mut db, "INSERT INTO src VALUES(10),(20),(30)");
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 15 AS lim) \
         INSERT INTO t SELECT x FROM src WHERE x > (SELECT lim FROM c)",
    );
    assert_eq!(
        query(&db, "SELECT a FROM t ORDER BY a"),
        vec![vec![SqlValue::Integer(20)], vec![SqlValue::Integer(30)]]
    );
}

/// A CTE shadows a same-named catalog table, both as the INSERT ... SELECT
/// source and inside a VALUES subquery (sqlite3: 8, then 9).
#[test]
fn test_with_insert_cte_shadows_catalog_table() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE c(v INTEGER)");
    run_stmt(&mut db, "INSERT INTO c VALUES(111)");
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");

    // CTE shadows table `c` as the SELECT source.
    run_stmt(&mut db, "WITH c AS (SELECT 8 AS v) INSERT INTO t SELECT v FROM c");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(8)]]);

    // CTE shadows table `c` inside a VALUES subquery.
    run_stmt(&mut db, "DELETE FROM t");
    run_stmt(&mut db, "WITH c AS (SELECT 9 AS v) INSERT INTO t VALUES((SELECT v FROM c))");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(9)]]);

    // The catalog table is untouched.
    assert_eq!(query(&db, "SELECT v FROM c"), vec![vec![SqlValue::Integer(111)]]);
}

/// CTE names resolve ASCII case-insensitively (sqlite3: inserts 20, 30).
#[test]
fn test_with_insert_cte_case_insensitive() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE src(x INTEGER)");
    run_stmt(&mut db, "INSERT INTO src VALUES(10),(20),(30)");
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(
        &mut db,
        "WITH Costs AS (SELECT 15 AS lim) \
         INSERT INTO t SELECT x FROM src WHERE x > (SELECT lim FROM cOSTS)",
    );
    assert_eq!(
        query(&db, "SELECT a FROM t ORDER BY a"),
        vec![vec![SqlValue::Integer(20)], vec![SqlValue::Integer(30)]]
    );

    // Case-insensitive in a VALUES subquery too.
    run_stmt(&mut db, "DELETE FROM t");
    run_stmt(&mut db, "WITH C AS (SELECT 9 AS v) INSERT INTO t VALUES((SELECT v FROM c))");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(9)]]);
}

/// DEFAULT VALUES with a WITH clause is a no-op for CTE visibility but must
/// still execute (sqlite3: inserts the default).
#[test]
fn test_with_insert_default_values() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER DEFAULT 7)");
    run_stmt(&mut db, "WITH c AS (SELECT 8) INSERT INTO t DEFAULT VALUES");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(7)]]);
}

/// Upsert: CTE visible in the ON CONFLICT DO UPDATE SET expression
/// (sqlite3: a=1, b=8 after the upsert takes the update arm).
#[test]
fn test_with_insert_on_conflict_do_update_cte() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1, 0)");
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 8 AS v) INSERT INTO t VALUES(1, 99) \
         ON CONFLICT(a) DO UPDATE SET b=(SELECT v FROM c)",
    );
    assert_eq!(
        query(&db, "SELECT a, b FROM t"),
        vec![vec![SqlValue::Integer(1), SqlValue::Integer(8)]]
    );
}

/// RETURNING with a CTE-referencing subquery (sqlite3: returns 8).
#[test]
fn test_with_insert_returning_cte_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");

    let sql = "WITH c AS (SELECT 8 AS v) INSERT INTO t VALUES(1) RETURNING (SELECT v FROM c)";
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Insert(insert) = stmt else {
        panic!("Expected INSERT statement");
    };
    let outcome = InsertExecutor::execute_returning(&mut db, &insert).unwrap();
    assert_eq!(outcome.affected_rows, 1);
    let returning = outcome.returning.expect("RETURNING result expected");
    assert_eq!(returning.rows.len(), 1);
    assert_eq!(returning.rows[0].values.to_vec(), vec![SqlValue::Integer(8)]);
}

/// UPDATE with WITH: CTE visible in SET and WHERE subqueries
/// (sqlite3: rows 2, 10 after `a*10` where a matches the CTE key).
#[test]
fn test_with_update_cte_in_set_and_where() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1),(2)");
    run_stmt(
        &mut db,
        "WITH c AS (SELECT 1 AS k) UPDATE t SET a = a*10 WHERE a = (SELECT k FROM c)",
    );
    assert_eq!(
        query(&db, "SELECT a FROM t ORDER BY a"),
        vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(10)]]
    );
}

/// UPDATE ... RETURNING with a CTE-referencing subquery (sqlite3: returns 8).
#[test]
fn test_with_update_returning_cte_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(1)");

    let sql = "WITH c AS (SELECT 8 AS v) UPDATE t SET a=2 RETURNING (SELECT v FROM c)";
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Update(update) = stmt else {
        panic!("Expected UPDATE statement");
    };
    let (count, returning) = UpdateExecutor::execute_returning(&update, &mut db).unwrap();
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING result expected");
    assert_eq!(returning.rows.len(), 1);
    assert_eq!(returning.rows[0].values.to_vec(), vec![SqlValue::Integer(8)]);
}

/// DELETE with WITH: CTE visible in the WHERE subquery (sqlite3: only row 10
/// remains), and DELETE ... RETURNING sees the CTE too (sqlite3: returns 9).
#[test]
fn test_with_delete_cte_in_where_and_returning() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(a INTEGER)");
    run_stmt(&mut db, "INSERT INTO t VALUES(2),(10)");
    run_stmt(&mut db, "WITH c AS (SELECT 2 AS k) DELETE FROM t WHERE a = (SELECT k FROM c)");
    assert_eq!(query(&db, "SELECT a FROM t"), vec![vec![SqlValue::Integer(10)]]);

    let sql = "WITH c AS (SELECT 9 AS v) DELETE FROM t RETURNING (SELECT v FROM c)";
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Delete(delete) = stmt else {
        panic!("Expected DELETE statement");
    };
    let (count, returning) = DeleteExecutor::execute_returning(&delete, &mut db).unwrap();
    assert_eq!(count, 1);
    let returning = returning.expect("RETURNING result expected");
    assert_eq!(returning.rows.len(), 1);
    assert_eq!(returning.rows[0].values.to_vec(), vec![SqlValue::Integer(9)]);
}
