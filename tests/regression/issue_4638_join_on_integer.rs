//! Test for JOIN ON clause accepting integer values (#4638)
//!
//! SQLite allows any expression in JOIN ON clauses, treating 0 as false
//! and non-zero values as true. This test ensures VibeSQL maintains
//! SQLite compatibility for this behavior.
//!
//! Example:
//! ```sql
//! SELECT * FROM t1 JOIN t2 ON 1  -- Always matches (1 = true)
//! SELECT * FROM t1 JOIN t2 ON 0  -- Never matches (0 = false)
//! ```
//!
//! Before the fix: "JOIN condition must evaluate to boolean, got: Integer(0)"
//! After the fix: Integer values are coerced to boolean (0=false, non-zero=true)

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
    t1.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)])).unwrap();
    t1.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)])).unwrap();

    // Create t2 (id, category)
    let schema2 = TableSchema::new(
        "t2".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("category".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema2).unwrap();

    let t2 = db.get_table_mut("t2").unwrap();
    t2.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    t2.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)])).unwrap();
}

#[test]
fn test_join_on_integer_one_always_matches() {
    // Regression test for issue #4638
    // SELECT * FROM t1 JOIN t2 ON 1
    //
    // The literal 1 should be treated as true, producing a cross join.
    // Before the fix: "JOIN condition must evaluate to boolean, got: Integer(1)"
    // After the fix: Returns cross product (2 * 2 = 4 rows)

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON 1");

    // Should return 4 rows (cross product: 2 rows in t1 * 2 rows in t2)
    assert_eq!(rows.len(), 4, "JOIN ON 1 should produce cross product (4 rows)");
}

#[test]
fn test_join_on_integer_zero_never_matches() {
    // SELECT * FROM t1 JOIN t2 ON 0
    //
    // The literal 0 should be treated as false, producing no matches.
    // Before the fix: "JOIN condition must evaluate to boolean, got: Integer(0)"
    // After the fix: Returns 0 rows

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON 0");

    // Should return 0 rows (0 = false, never matches)
    assert_eq!(rows.len(), 0, "JOIN ON 0 should produce no rows");
}

#[test]
fn test_join_on_integer_expression() {
    // SELECT * FROM t1 JOIN t2 ON t1.id - t1.id
    //
    // The expression evaluates to 0, which should be treated as false.
    // This tests that integer expression results are also coerced.

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id - t1.id");

    // t1.id - t1.id = 0 (false), should return 0 rows
    assert_eq!(rows.len(), 0, "JOIN ON (expression=0) should produce no rows");
}

#[test]
fn test_join_on_nonzero_integer_expression() {
    // SELECT * FROM t1 JOIN t2 ON t1.id
    //
    // When t1.id is non-zero (1 or 2), it should be treated as true.

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id");

    // Both t1.id values (1 and 2) are non-zero = true, so cross join for each row
    // 2 rows in t1 * 2 rows in t2 = 4 rows
    assert_eq!(rows.len(), 4, "JOIN ON non-zero integer should match");
}

#[test]
fn test_left_join_on_integer_zero() {
    // SELECT * FROM t1 LEFT JOIN t2 ON 0
    //
    // The 0 should be treated as false, so no matches.
    // LEFT JOIN preserves all left rows with NULLs for right columns.

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON 0");

    // Should return 2 rows (both t1 rows with NULL for t2.id)
    assert_eq!(rows.len(), 2, "LEFT JOIN ON 0 should return left rows with NULLs");

    for row in &rows {
        assert!(
            matches!(row.values[1], SqlValue::Null),
            "t2.id should be NULL when ON condition is false"
        );
    }
}

#[test]
fn test_left_join_on_integer_one() {
    // SELECT * FROM t1 LEFT JOIN t2 ON 1
    //
    // The 1 should be treated as true, producing cross join behavior.

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 LEFT JOIN t2 ON 1");

    // Should return 4 rows (cross product)
    assert_eq!(rows.len(), 4, "LEFT JOIN ON 1 should produce cross product");
}

#[test]
fn test_join_on_boolean_still_works() {
    // Verify that standard boolean conditions still work correctly

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.id = t2.id");

    // Should return 2 rows (id=1 match and id=2 match)
    assert_eq!(rows.len(), 2, "Standard boolean JOIN ON should work");

    // Verify the matches
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();

    assert!(ids.contains(&1) && ids.contains(&2), "Should match on id 1 and 2");
}

#[test]
fn test_join_on_null_is_false() {
    // SELECT * FROM t1 JOIN t2 ON NULL
    //
    // NULL in a JOIN condition should be treated as false (no match).

    let mut db = Database::new();
    create_test_tables(&mut db);

    let rows = select_rows(&db, "SELECT t1.id, t2.id FROM t1 JOIN t2 ON NULL");

    // NULL = false in boolean context, should return 0 rows
    assert_eq!(rows.len(), 0, "JOIN ON NULL should produce no rows");
}
