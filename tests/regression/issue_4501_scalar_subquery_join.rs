//! Test for scalar subquery in WHERE clause with JOIN (#4501)
//!
//! This test ensures that scalar subqueries in WHERE clauses are correctly
//! evaluated when combined with JOINs.
//!
//! The bug was in the columnar join execution path, which silently dropped
//! WHERE predicates containing scalar subqueries because they couldn't be
//! converted to SIMD-accelerated column predicates.
//!
//! For example:
//! ```sql
//! SELECT t1.id, t2.category
//! FROM t1 JOIN t2 ON t1.id = t2.t1_id
//! WHERE t1.id = (SELECT MAX(id) FROM t1)
//! ```
//!
//! Before the fix: returned ALL joined rows (scalar subquery ignored)
//! After the fix: returns only rows where t1.id matches the MAX value

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

/// Helper to create test tables
fn create_test_tables(db: &mut Database) {
    // Create t1 (id, val)
    let schema1 = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("val".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema1).unwrap();

    let t1 = db.get_table_mut("t1").unwrap();
    t1.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]))
        .unwrap();
    t1.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)]))
        .unwrap();
    t1.insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(300)]))
        .unwrap();

    // Create t2 (id, t1_id, category)
    let schema2 = TableSchema::new(
        "t2".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("t1_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("category".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema2).unwrap();

    let t2 = db.get_table_mut("t2").unwrap();
    // t1_id=1 has categories 10, 20
    // t1_id=2 has category 10
    // t1_id=3 has category 20
    t2.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(10)]))
        .unwrap();
    t2.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(1), SqlValue::Integer(20)]))
        .unwrap();
    t2.insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(2), SqlValue::Integer(10)]))
        .unwrap();
    t2.insert(Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(3), SqlValue::Integer(20)]))
        .unwrap();
}

#[test]
fn test_scalar_subquery_with_join() {
    // Regression test for issue #4501
    // SELECT t1.id, t1.val, t2.category FROM t1 JOIN t2 ON t1.id = t2.t1_id
    // WHERE t1.id = (SELECT MAX(id) FROM t1)
    //
    // Expected: only rows where t1.id = 3 (the MAX)
    // Bug: returned all 4 joined rows (scalar subquery filter was ignored)

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT t1.id, t1.val, t2.category FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.id = (SELECT MAX(id) FROM t1)",
    );

    // Should return 1 row (t1.id=3 with t2.category=20)
    assert_eq!(rows.len(), 1, "Should return 1 row where t1.id = MAX(id) = 3");

    let id = match &rows[0].values[0] {
        SqlValue::Integer(n) => *n,
        other => panic!("Expected Integer, got {:?}", other),
    };
    assert_eq!(id, 3, "t1.id should be 3 (MAX)");

    let category = match &rows[0].values[2] {
        SqlValue::Integer(n) => *n,
        other => panic!("Expected Integer, got {:?}", other),
    };
    assert_eq!(category, 20, "category should be 20");
}

#[test]
fn test_scalar_subquery_without_join() {
    // Sanity check: scalar subquery works without JOIN
    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT * FROM t1 WHERE id = (SELECT MAX(id) FROM t1)");

    assert_eq!(rows.len(), 1, "Should return 1 row");
    let id = match &rows[0].values[0] {
        SqlValue::Integer(n) => *n,
        other => panic!("Expected Integer, got {:?}", other),
    };
    assert_eq!(id, 3, "id should be 3 (MAX)");
}

#[test]
fn test_scalar_subquery_with_join_and_other_conditions() {
    // Test scalar subquery combined with other WHERE conditions
    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT t1.id, t2.category FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.id >= (SELECT MIN(id) FROM t1) AND t2.category = 10",
    );

    // Should return 2 rows: (1, 10) and (2, 10)
    assert_eq!(rows.len(), 2, "Should return 2 rows with category=10");

    let mut ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2], "Should have t1.id values 1 and 2");
}

#[test]
fn test_in_subquery_with_join() {
    // Test IN subquery with JOIN
    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT t1.id, t2.category FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.id IN (SELECT id FROM t1 WHERE id > 1)",
    );

    // Should return 2 rows: (2, 10) and (3, 20)
    assert_eq!(rows.len(), 2, "Should return 2 rows where t1.id > 1");

    let mut ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3], "Should have t1.id values 2 and 3");
}

#[test]
fn test_exists_subquery_with_join() {
    // Test EXISTS subquery with JOIN
    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(
        &db,
        "SELECT t1.id, t2.category FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE EXISTS (SELECT 1 FROM t1 WHERE t1.id = 3)",
    );

    // EXISTS is true (there is a row with id=3), so all 4 joined rows should be returned
    assert_eq!(rows.len(), 4, "Should return all 4 rows since EXISTS is true");
}
