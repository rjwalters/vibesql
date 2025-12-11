//! Test for HAVING clause NULL handling regression tests
//!
//! These tests ensure proper three-valued logic (TRUE, FALSE, NULL) in HAVING clauses.
//! SQL standard requires that NULL in a HAVING clause filters out the row (treated as FALSE).
//!
//! Issues covered:
//! - HAVING with NOT BETWEEN NULL AND NULL (sqllogictest random/groupby/slt_good_10.test:1119)
//! - HAVING with NULL IN (...) (sqllogictest random/groupby/slt_good_10.test:32633)

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper to execute SELECT and return rows
fn select_rows(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Helper to create the test table (matching sqllogictest tab2)
fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "TAB2".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL2".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("TAB2").unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(91), SqlValue::Integer(23), SqlValue::Integer(79)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(42), SqlValue::Integer(87)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(92), SqlValue::Integer(31), SqlValue::Integer(58)]))
        .unwrap();
}

#[test]
fn test_having_not_between_null_and_null() {
    // Regression test for sqllogictest random/groupby/slt_good_10.test:1119
    // Query: SELECT + col0 * - col0 AS col0 FROM tab2 GROUP BY col0
    //        HAVING AVG ( ALL col2 ) NOT BETWEEN NULL AND ( NULL )
    // Expected: 0 rows (because NOT BETWEEN NULL AND NULL evaluates to NULL,
    //           and NULL in HAVING filters out the row)

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(
        &db,
        "SELECT + col0 * - col0 AS col0 FROM tab2 GROUP BY col0 HAVING AVG ( ALL col2 ) NOT BETWEEN NULL AND ( NULL )"
    );

    // SQL three-valued logic:
    // - x NOT BETWEEN NULL AND NULL = (x < NULL) OR (x > NULL)
    // - Any comparison with NULL returns NULL
    // - NULL OR NULL = NULL
    // - NULL in HAVING filters out the row (treated as FALSE)
    assert_eq!(rows.len(), 0, "HAVING NOT BETWEEN NULL AND NULL should filter all rows");
}

#[test]
fn test_having_null_in_list() {
    // Regression test for sqllogictest random/groupby/slt_good_10.test:32633
    // Query: SELECT ALL AVG ( ALL col2 ) FROM tab0 GROUP BY col2
    //        HAVING NULL IN ( - col2 * col2 )
    // Expected: 0 rows (because NULL IN (...) evaluates to NULL,
    //           and NULL in HAVING filters out the row)

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT 1 AS x FROM tab2 GROUP BY col2 HAVING NULL IN (1, 2, 3)");

    // SQL three-valued logic:
    // - NULL IN (non-empty list) = NULL
    // - NULL in HAVING filters out the row (treated as FALSE)
    assert_eq!(rows.len(), 0, "HAVING NULL IN (...) should filter all rows");
}

#[test]
fn test_having_between_null() {
    // Test regular BETWEEN with NULL bounds
    let mut db = Database::new();
    create_test_table(&mut db);

    // BETWEEN with NULL low bound
    let rows =
        select_rows(&db, "SELECT col0 FROM tab2 GROUP BY col0 HAVING col0 BETWEEN NULL AND 100");
    assert_eq!(rows.len(), 0, "HAVING x BETWEEN NULL AND 100 should filter all rows");

    // BETWEEN with NULL high bound
    let rows =
        select_rows(&db, "SELECT col0 FROM tab2 GROUP BY col0 HAVING col0 BETWEEN 0 AND NULL");
    assert_eq!(rows.len(), 0, "HAVING x BETWEEN 0 AND NULL should filter all rows");
}

#[test]
fn test_having_in_with_null_in_list() {
    // Test IN where the list contains NULL
    let mut db = Database::new();
    create_test_table(&mut db);

    // When the test value matches a non-NULL item, it should return that row
    let rows = select_rows(&db, "SELECT col0 FROM tab2 GROUP BY col0 HAVING col0 IN (91, NULL)");
    assert_eq!(rows.len(), 1, "HAVING col0 IN (91, NULL) should return 1 row");
    assert_eq!(rows[0].values[0], SqlValue::Integer(91));

    // When the test value doesn't match any non-NULL item, result is NULL (row filtered)
    let rows = select_rows(&db, "SELECT col0 FROM tab2 GROUP BY col0 HAVING col0 IN (999, NULL)");
    assert_eq!(rows.len(), 0, "HAVING col0 IN (999, NULL) should filter all rows");
}

#[test]
fn test_having_null_literal() {
    // Simple test: HAVING NULL should filter all rows
    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT col0 FROM tab2 GROUP BY col0 HAVING NULL");
    assert_eq!(rows.len(), 0, "HAVING NULL should filter all rows");
}

#[test]
fn test_having_aggregate_not_between() {
    // Test HAVING with aggregate function and NOT BETWEEN
    let mut db = Database::new();
    create_test_table(&mut db);

    // This should work correctly - aggregate compared to known bounds
    let rows = select_rows(
        &db,
        "SELECT col0 FROM tab2 GROUP BY col0 HAVING AVG(col2) NOT BETWEEN 0 AND 50",
    );
    // AVG(col2) values are 79, 87, 58 - all are NOT BETWEEN 0 AND 50, so all 3 rows
    assert_eq!(rows.len(), 3, "HAVING AVG(col2) NOT BETWEEN 0 AND 50 should return all rows");
}
