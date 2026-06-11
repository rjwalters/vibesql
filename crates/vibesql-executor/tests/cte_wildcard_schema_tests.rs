//! Regression tests for issue #5293 (window8.test 8.3)
//!
//! `WITH cte AS (SELECT * FROM t)` used to materialize a schema with a single
//! `col{i}` column instead of expanding the wildcard to the underlying
//! table's columns. This caused:
//!
//! - `SELECT t FROM cte` to fail with "Column 't' not found"
//! - `SELECT * FROM cte` to silently DROP all but the first column
//! - correlated subqueries against the CTE to fail (and, in window
//!   PARTITION BY position, to silently collapse into one partition because
//!   the evaluation error was swallowed as NULL)
//!
//! The fix statically expands wildcard SELECT items in `derive_cte_schema`
//! using the database catalog and prior CTE schemas.

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

/// Setup from the issue body (window8.test 8.x)
fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE tx(a INTEGER PRIMARY KEY)");
    run_stmt(&mut db, "INSERT INTO tx VALUES(1),(2),(3),(4),(5),(6)");
    run_stmt(&mut db, "CREATE TABLE map(v INTEGER PRIMARY KEY, t TEXT)");
    run_stmt(
        &mut db,
        "INSERT INTO map VALUES (1,'odd'),(2,'even'),(3,'odd'),(4,'even'),(5,'odd'),(6,'even')",
    );
    db
}

fn odd_even(v: i64) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(if v % 2 == 0 { "even" } else { "odd" }))
}

#[test]
fn test_select_column_by_name_from_wildcard_cte() {
    // Previously: Error: Column 't' not found ... Available columns: col0
    let db = setup_db();
    let rows = query(&db, "WITH map2 AS (SELECT * FROM map) SELECT t FROM map2 ORDER BY t");
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[0], vec![odd_even(2)]);
    assert_eq!(rows[5], vec![odd_even(1)]);
}

#[test]
fn test_select_star_from_wildcard_cte_returns_all_columns() {
    // Previously returned a single col0 column (silent data loss)
    let db = setup_db();
    let rows = query(&db, "WITH map2 AS (SELECT * FROM map) SELECT * FROM map2 ORDER BY v");
    assert_eq!(rows.len(), 6);
    for (i, row) in rows.iter().enumerate() {
        let v = (i + 1) as i64;
        assert_eq!(row.len(), 2, "expected both v and t columns, got {:?}", row);
        assert_eq!(row[0], SqlValue::Integer(v));
        assert_eq!(row[1], odd_even(v));
    }
}

#[test]
fn test_qualified_wildcard_cte() {
    // Previously: same failure via SELECT map.* (qualified wildcard)
    let db = setup_db();
    let rows = query(&db, "WITH map2 AS (SELECT map.* FROM map) SELECT t FROM map2 WHERE v = 2");
    assert_eq!(rows, vec![vec![odd_even(2)]]);
}

#[test]
fn test_correlated_subquery_against_wildcard_cte() {
    // Issue reproducer: correlated scalar subquery in the SELECT list
    let db = setup_db();
    let rows = query(
        &db,
        "WITH map2 AS (SELECT * FROM map) \
         SELECT a, (SELECT t FROM map2 WHERE v=a) FROM tx ORDER BY a",
    );
    assert_eq!(rows.len(), 6);
    for (i, row) in rows.iter().enumerate() {
        let a = (i + 1) as i64;
        assert_eq!(row[0], SqlValue::Integer(a));
        assert_eq!(row[1], odd_even(a), "wrong subquery result for a={}", a);
    }
}

#[test]
fn test_window_partition_by_correlated_subquery_against_wildcard_cte() {
    // window8.test 8.3: the partition key used to silently evaluate to NULL
    // for every row, collapsing the window into one partition
    // (got 1 3 6 10 15 21 instead of per-parity running sums)
    let db = setup_db();
    let rows = query(
        &db,
        "WITH map2 AS (SELECT * FROM map) \
         SELECT sum(a) OVER (PARTITION BY (SELECT t FROM map2 WHERE v=a) ORDER BY a) \
         FROM tx ORDER BY a",
    );
    // odd partition: 1, 1+3=4, 1+3+5=9; even partition: 2, 2+4=6, 2+4+6=12
    let expected: Vec<Vec<SqlValue>> = vec![
        vec![SqlValue::Integer(1)],
        vec![SqlValue::Integer(2)],
        vec![SqlValue::Integer(4)],
        vec![SqlValue::Integer(6)],
        vec![SqlValue::Integer(9)],
        vec![SqlValue::Integer(12)],
    ];
    assert_eq!(rows, expected);
}

#[test]
fn test_wildcard_cte_referencing_prior_wildcard_cte() {
    // Later CTEs must expand wildcards against prior CTE schemas
    let db = setup_db();
    let rows = query(
        &db,
        "WITH c1 AS (SELECT * FROM map), c2 AS (SELECT * FROM c1) \
         SELECT t FROM c2 WHERE v = 3",
    );
    assert_eq!(rows, vec![vec![odd_even(3)]]);
}

#[test]
fn test_mixed_wildcard_and_expression_type_alignment() {
    // `SELECT *, expr` from a 2-column table puts expr at value index 2;
    // the schema's running value offset must track wildcard expansion
    let db = setup_db();
    let rows = query(
        &db,
        "WITH c AS (SELECT *, v + 100 AS vplus FROM map) \
         SELECT v, t, vplus FROM c WHERE v = 4",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(4), odd_even(4), SqlValue::Integer(104)]]);
}

#[test]
fn test_empty_result_wildcard_cte_still_exposes_column_names() {
    // With no rows, types cannot be inferred, but names must still expand
    let db = setup_db();
    let rows = query(&db, "WITH c AS (SELECT * FROM map WHERE v > 100) SELECT t FROM c");
    assert!(rows.is_empty(), "expected empty result, got {:?}", rows);
}

#[test]
fn test_table_alias_qualified_wildcard_cte() {
    // Qualified wildcard must match the FROM alias, not just the table name
    let db = setup_db();
    let rows = query(&db, "WITH c AS (SELECT mp.* FROM map mp) SELECT t FROM c WHERE v = 5");
    assert_eq!(rows, vec![vec![odd_even(5)]]);
}

#[test]
fn test_explicit_column_list_still_works() {
    // Explicit CTE column lists take precedence over expansion
    let db = setup_db();
    let rows = query(&db, "WITH c(x, y) AS (SELECT * FROM map) SELECT y FROM c WHERE x = 6");
    assert_eq!(rows, vec![vec![odd_even(6)]]);
}
