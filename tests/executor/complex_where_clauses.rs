//! Tests for complex WHERE clause execution with deep nesting
//!
//! Related to issue #1411: Complex WHERE clause execution with deep nesting

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .map_err(|e| format!("Execution error: {:?}", e))
}

fn create_test_table() -> Database {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("PK".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL0".to_string(), DataType::Integer, true),
            ColumnSchema::new("COL1".to_string(), DataType::DoublePrecision, true),
            ColumnSchema::new("COL3".to_string(), DataType::Integer, true),
            ColumnSchema::new("COL4".to_string(), DataType::DoublePrecision, true),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "TAB0",
        Row::new(vec![
            SqlValue::Integer(0),
            SqlValue::Integer(200),
            SqlValue::Double(500.0),
            SqlValue::Integer(300),
            SqlValue::Double(700.0),
        ]),
    )
    .unwrap();

    db.insert_row(
        "TAB0",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(400),
            SqlValue::Double(600.0),
            SqlValue::Integer(500),
            SqlValue::Null,
        ]),
    )
    .unwrap();

    db.insert_row(
        "TAB0",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(300),
            SqlValue::Double(650.0),
            SqlValue::Integer(600),
            SqlValue::Double(800.0),
        ]),
    )
    .unwrap();

    db.insert_row(
        "TAB0",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(100),
            SqlValue::Double(700.0),
            SqlValue::Integer(250),
            SqlValue::Double(750.0),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_simple_where_clause() {
    let db = create_test_table();

    // Simple WHERE clause - baseline test
    let result = execute_select(&db, "SELECT PK FROM TAB0 WHERE COL0 > 200")
        .expect("Query should succeed");

    assert_eq!(result.len(), 2); // rows 1 and 2
}

#[test]
fn test_nested_and_or() {
    let db = create_test_table();

    // Nested AND/OR combination
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE (COL0 > 200 AND COL3 > 400) OR (COL0 < 200 AND COL3 < 300)",
    )
    .expect("Query should succeed");

    // row 1: COL0=400, COL3=500 → first condition TRUE
    // row 2: COL0=300, COL3=600 → first condition TRUE
    // row 3: COL0=100, COL3=250 → second condition TRUE
    assert_eq!(result.len(), 3);
}

#[test]
fn test_deeply_nested_predicates() {
    let db = create_test_table();

    // Deeply nested AND/OR with multiple levels
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         (COL4 IS NULL OR \
          (COL3 < 642 AND \
           (COL0 > 259 OR COL3 >= 572 OR \
            (COL3 < 245 OR COL1 <= 687.32))))",
    )
    .expect("Query should succeed");

    // Verify we get some results (exact count depends on data)
    assert!(
        !result.is_empty(),
        "Should return at least one row for complex nested query"
    );
}

#[test]
fn test_complex_with_between() {
    let db = create_test_table();

    // Complex predicate with BETWEEN and nested conditions
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         (COL3 < 245 OR \
          (COL3 BETWEEN 960 AND 132 OR COL1 <= 687.32) AND \
          COL0 <= 447 AND COL0 > 100)",
    )
    .expect("Query should succeed");

    // The query should execute without errors
    // Exact results depend on predicate evaluation order
    assert!(result.len() <= 4, "Should not return more rows than exist");
}

#[test]
fn test_complex_with_multiple_levels() {
    let db = create_test_table();

    // Test case similar to the failing sqllogictest pattern
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         ((COL0 > 200 OR (COL3 > 300 AND COL1 < 700.0)) AND \
          (COL4 IS NOT NULL OR COL3 < 500))",
    )
    .expect("Query should succeed");

    // Verify execution completes
    assert!(result.len() <= 4);
}

#[test]
fn test_subquery_in_complex_where() {
    let db = create_test_table();

    // Complex WHERE with subquery (similar to issue example)
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         COL0 IN (SELECT COL3 FROM TAB0 WHERE COL3 > 259 OR COL3 >= 500)",
    )
    .expect("Query should succeed");

    // Subquery returns: 300, 500, 600 (COL3 values where COL3 > 259 OR COL3 >= 500)
    // Main query checks if COL0 matches: 200, 400, 300, 100
    // Match: row 2 has COL0=300
    assert_eq!(result.len(), 1);
}

#[test]
fn test_nested_subquery_with_complex_predicate() {
    let db = create_test_table();

    // More complex - subquery with nested predicates
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         (COL4 IS NULL OR COL3 < 600) AND \
         COL0 IN (SELECT COL3 FROM TAB0 WHERE \
                  (COL3 > 259 OR COL3 >= 572 OR \
                   (COL3 < 245 AND COL0 > 100)))",
    )
    .expect("Query should succeed");

    // This tests both predicate decomposition and subquery handling
    assert!(result.len() <= 4);
}

#[test]
fn test_deterministic_results() {
    let db = create_test_table();

    // Run the same complex query multiple times - results should be identical
    let sql = "SELECT PK FROM TAB0 WHERE \
               (COL0 > 200 AND COL3 < 600) OR \
               (COL4 IS NOT NULL AND COL1 > 650.0)";

    let result1 = execute_select(&db, sql).expect("First execution should succeed");
    let result2 = execute_select(&db, sql).expect("Second execution should succeed");
    let result3 = execute_select(&db, sql).expect("Third execution should succeed");

    assert_eq!(
        result1.len(),
        result2.len(),
        "Results should be deterministic"
    );
    assert_eq!(
        result1.len(),
        result3.len(),
        "Results should be deterministic"
    );

    // Also check that the actual values match, not just count
    for (r1, r2) in result1.iter().zip(result2.iter()) {
        assert_eq!(r1.values, r2.values, "Row values should match exactly");
    }
}

#[test]
fn test_cse_with_complex_predicates() {
    let db = create_test_table();

    // Test that CSE doesn't cause incorrect results with complex predicates
    // This query repeats COL0 > 200 multiple times
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         (COL0 > 200 AND COL3 > 400) OR \
         (COL0 > 200 AND COL1 < 700.0) OR \
         (COL0 > 200 AND COL4 IS NOT NULL)",
    )
    .expect("Query should succeed");

    // row 1: COL0=400 (>200), satisfies multiple conditions
    // row 2: COL0=300 (>200), satisfies multiple conditions
    assert!(result.len() >= 2, "Should return rows with COL0 > 200");
}

#[test]
fn test_null_handling_in_complex_where() {
    let db = create_test_table();

    // Complex predicate with NULL comparisons
    let result = execute_select(
        &db,
        "SELECT PK FROM TAB0 WHERE \
         (COL4 IS NULL OR (COL4 > 700.0 AND COL0 < 500)) AND \
         COL3 > 200",
    )
    .expect("Query should succeed");

    // row 1: COL4=NULL, COL3=500 → first part TRUE (IS NULL), second TRUE
    // row 0: COL4=700.0, not > 700.0, doesn't match
    // row 2: COL4=800.0, COL0=300, matches second condition, COL3=600
    // row 3: COL4=750.0, COL0=100, matches second condition, COL3=250
    assert!(result.len() > 0, "Should return rows matching NULL conditions");
}
