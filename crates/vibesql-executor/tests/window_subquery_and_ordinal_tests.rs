//! Regression tests for issue #5231 (window1.test 6.2, 25.1, 25.2)
//!
//! Covers two independent fixes:
//!
//! 1. IN/NOT IN subqueries whose SELECT list contains a window function must
//!    NOT be converted to semi/anti joins (the window function is computed
//!    over the subquery's whole result set and cannot be hoisted into a
//!    per-row join condition). They fall back to row-by-row IN evaluation.
//!
//! 2. Ordinal ORDER BY terms that land inside a `SELECT *` wildcard expansion
//!    must resolve to a table-qualified column reference so the ambiguity
//!    check is not tripped when multiple tables expose the same column name.

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

/// Setup matching the curator's minimal reproducers for 25.1 / 25.2
fn setup_in_subquery_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(t1_id INTEGER PRIMARY KEY)");
    run_stmt(&mut db, "CREATE TABLE t3(t3_id INTEGER PRIMARY KEY)");
    run_stmt(&mut db, "INSERT INTO t1 VALUES(1),(3),(5)");
    run_stmt(&mut db, "INSERT INTO t3 VALUES(10),(11),(12)");
    db
}

#[test]
fn test_in_subquery_with_uncorrelated_window_function() {
    // window1.test 25.2 reproducer: row_number() over t3 yields {1, 2, 3},
    // so t1 ids 1 and 3 match. Previously errored with
    // "misuse of window function row_number()".
    let db = setup_in_subquery_db();
    let rows = query(
        &db,
        "SELECT * FROM t1 WHERE t1_id IN (SELECT row_number() OVER (ORDER BY t3_id) FROM t3) \
         ORDER BY t1_id",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(3)]]);
}

#[test]
fn test_in_subquery_with_correlated_window_function() {
    // window1.test 25.1 reproducer: for each outer row the subquery yields
    // {t1_id+1, t1_id+2, t1_id+3}, which never contains t1_id, so the result
    // is empty. Previously errored with "Column 't1_id' not found".
    let db = setup_in_subquery_db();
    let rows = query(
        &db,
        "SELECT * FROM t1 WHERE t1_id IN \
         (SELECT t1_id + row_number() OVER (ORDER BY t1_id) FROM t3)",
    );
    assert!(rows.is_empty(), "Expected empty result, got {:?}", rows);
}

#[test]
fn test_not_in_subquery_with_window_function() {
    // The ANTI-join path shares the same guard: row_number() over t3 yields
    // {1, 2, 3}, so only t1_id = 5 survives NOT IN.
    let db = setup_in_subquery_db();
    let rows = query(
        &db,
        "SELECT * FROM t1 WHERE t1_id NOT IN \
         (SELECT row_number() OVER (ORDER BY t3_id) FROM t3)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(5)]]);
}

/// Setup matching the curator's minimal reproducer for 6.2
fn setup_ordinal_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE a(x INTEGER)");
    run_stmt(&mut db, "CREATE TABLE b(x INTEGER)");
    run_stmt(&mut db, "INSERT INTO a VALUES(2),(1)");
    run_stmt(&mut db, "INSERT INTO b VALUES(20),(10)");
    db
}

#[test]
fn test_order_by_ordinal_through_wildcard_with_duplicate_column_names() {
    // window1.test 6.2 reproducer (window-free): ordinal ORDER BY through
    // SELECT * must not be re-resolved as a bare column name. Previously
    // errored with "ambiguous column name: x" because both b and the derived
    // table expose column x.
    let db = setup_ordinal_db();
    let rows = query(&db, "SELECT * FROM b, (SELECT x FROM a) ORDER BY 1, 2");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(10), SqlValue::Integer(1)],
            vec![SqlValue::Integer(10), SqlValue::Integer(2)],
            vec![SqlValue::Integer(20), SqlValue::Integer(1)],
            vec![SqlValue::Integer(20), SqlValue::Integer(2)],
        ]
    );
}

#[test]
fn test_order_by_ordinal_through_qualified_wildcard() {
    // Same defect through the table.* arm: the qualified wildcard must also
    // emit a table-qualified reference.
    let db = setup_ordinal_db();
    let rows = query(&db, "SELECT b.x, sub.x FROM b, (SELECT x FROM a) AS sub ORDER BY 1, 2");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(10), SqlValue::Integer(1)],
            vec![SqlValue::Integer(10), SqlValue::Integer(2)],
            vec![SqlValue::Integer(20), SqlValue::Integer(1)],
            vec![SqlValue::Integer(20), SqlValue::Integer(2)],
        ]
    );

    let rows = query(&db, "SELECT b.*, sub.* FROM b, (SELECT x FROM a) AS sub ORDER BY 1, 2");
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::Integer(10), SqlValue::Integer(1)],
            vec![SqlValue::Integer(10), SqlValue::Integer(2)],
            vec![SqlValue::Integer(20), SqlValue::Integer(1)],
            vec![SqlValue::Integer(20), SqlValue::Integer(2)],
        ]
    );
}

#[test]
fn test_order_by_ordinal_wildcard_with_window_in_derived_table() {
    // Full shape of window1.test 6.2: the derived table contains a window
    // function and the second ordinal lands on its (unnamed) output column.
    let db = setup_ordinal_db();
    let rows =
        query(&db, "SELECT * FROM b, (SELECT x, count(*) OVER (ORDER BY x) FROM a) ORDER BY 1, 2");
    assert_eq!(rows.len(), 4);
    // First column (b.x) must be the primary sort key
    assert_eq!(rows[0][0], SqlValue::Integer(10));
    assert_eq!(rows[1][0], SqlValue::Integer(10));
    assert_eq!(rows[2][0], SqlValue::Integer(20));
    assert_eq!(rows[3][0], SqlValue::Integer(20));
    // Second column (derived x) must be the secondary sort key
    assert_eq!(rows[0][1], SqlValue::Integer(1));
    assert_eq!(rows[1][1], SqlValue::Integer(2));
}
