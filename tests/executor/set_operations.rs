//! End-to-end integration tests for SQL set operations.
//!
//! Tests UNION, INTERSECT, and EXCEPT operations (with ALL variants).

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// Set Operations Tests (UNION, INTERSECT, EXCEPT)
// ========================================================================

#[test]
fn test_e2e_set_operations() {
    let mut db = Database::new();

    // Create two tables with overlapping data
    let table_a_schema = TableSchema::new(
        "TABLE_A".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(table_a_schema).unwrap();

    let table_b_schema = TableSchema::new(
        "TABLE_B".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(table_b_schema).unwrap();

    // Insert data: table_a has 1, 2, 3, 4; table_b has 3, 4, 5, 6
    db.insert_row(
        "TABLE_A",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(StringValue::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_A",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(StringValue::from("Bob"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_A",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(StringValue::from("Charlie"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_A",
        Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(StringValue::from("David"))]),
    )
    .unwrap();

    db.insert_row(
        "TABLE_B",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(StringValue::from("Charlie"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_B",
        Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(StringValue::from("David"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_B",
        Row::new(vec![SqlValue::Integer(5), SqlValue::Varchar(StringValue::from("Eve"))]),
    )
    .unwrap();
    db.insert_row(
        "TABLE_B",
        Row::new(vec![SqlValue::Integer(6), SqlValue::Varchar(StringValue::from("Frank"))]),
    )
    .unwrap();

    // Test 1: UNION (distinct) - should return 1,2,3,4,5,6
    let results =
        execute_select(&db, "SELECT id FROM table_a UNION SELECT id FROM table_b;").unwrap();
    assert_eq!(results.len(), 6);
    let ids: Vec<i64> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
    assert!(ids.contains(&6));

    // Test 2: UNION ALL - should return 8 rows (4+4)
    let results =
        execute_select(&db, "SELECT id FROM table_a UNION ALL SELECT id FROM table_b;").unwrap();
    assert_eq!(results.len(), 8);

    // Test 3: INTERSECT (distinct) - should return only 3 and 4
    let results =
        execute_select(&db, "SELECT id FROM table_a INTERSECT SELECT id FROM table_b;").unwrap();
    assert_eq!(results.len(), 2);
    let ids: Vec<i64> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));

    // Test 4: EXCEPT (distinct) - should return only 1 and 2
    let results =
        execute_select(&db, "SELECT id FROM table_a EXCEPT SELECT id FROM table_b;").unwrap();
    assert_eq!(results.len(), 2);
    let ids: Vec<i64> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));

    // Test 5: Multiple UNION operations
    db.insert_row(
        "TABLE_A",
        Row::new(vec![SqlValue::Integer(7), SqlValue::Varchar(StringValue::from("Grace"))]),
    )
    .unwrap();
    let results = execute_select(&db,
        "SELECT id FROM table_a WHERE id <= 2 UNION SELECT id FROM table_a WHERE id >= 4 UNION SELECT id FROM table_b WHERE id = 3;"
    ).unwrap();
    // Should get: 1, 2 from first, 4, 7 from second, 3 from third = 1,2,3,4,7
    assert_eq!(results.len(), 5);

    // Test 6: UNION with ORDER BY
    let results = execute_select(&db, "SELECT id FROM table_a WHERE id <= 3 UNION SELECT id FROM table_b WHERE id >= 5 ORDER BY id;").unwrap();
    assert_eq!(results.len(), 5); // 1,2,3,5,6
                                  // Verify ordering
    let ids: Vec<i64> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 5, 6]);

    // Test 7: Set operation with WHERE clauses
    let results = execute_select(
        &db,
        "SELECT name FROM table_a WHERE id < 3 UNION SELECT name FROM table_b WHERE id > 5;",
    )
    .unwrap();
    assert_eq!(results.len(), 3); // Alice, Bob, Frank

    // Test 8: INTERSECT ALL with duplicates
    // Create tables with duplicate values
    let dup_a_schema = TableSchema::new(
        "DUP_A".to_string(),
        vec![ColumnSchema::new("VAL".to_string(), DataType::Integer, false)],
    );
    db.create_table(dup_a_schema).unwrap();

    let dup_b_schema = TableSchema::new(
        "DUP_B".to_string(),
        vec![ColumnSchema::new("VAL".to_string(), DataType::Integer, false)],
    );
    db.create_table(dup_b_schema).unwrap();

    // dup_a: 1, 1, 2, 2, 2
    db.insert_row("DUP_A", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("DUP_A", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("DUP_A", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("DUP_A", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("DUP_A", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // dup_b: 1, 2, 2
    db.insert_row("DUP_B", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("DUP_B", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("DUP_B", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // INTERSECT ALL should return: 1 (once), 2 (twice)
    let results =
        execute_select(&db, "SELECT val FROM dup_a INTERSECT ALL SELECT val FROM dup_b;").unwrap();
    assert_eq!(results.len(), 3);

    // Test 9: EXCEPT ALL with duplicates
    // dup_a: 1, 1, 2, 2, 2
    // dup_b: 1, 2, 2
    // EXCEPT ALL should return: 1 (once), 2 (once)
    let results =
        execute_select(&db, "SELECT val FROM dup_a EXCEPT ALL SELECT val FROM dup_b;").unwrap();
    assert_eq!(results.len(), 2);

    // Test 10: UNION with LIMIT
    let results =
        execute_select(&db, "SELECT id FROM table_a UNION SELECT id FROM table_b LIMIT 3;")
            .unwrap();
    assert_eq!(results.len(), 3);
}

/// Regression test for issue #5105 (window9.test 8.4):
/// UNION dedup of cross-class numerics must follow SQLite's "last-occurrence
/// wins" rule when the operands have the same numeric magnitude but different
/// storage classes. SQLite's ephemeral B-tree uses `OP_IdxInsert` semantics
/// that overwrite existing entries with the new (right-arm) value.
///
/// Concretely: `SELECT 0 UNION SELECT 0.0` returns `0.0` (REAL), not `0` (INTEGER).
#[test]
fn test_union_dedup_storage_class_last_wins() {
    let db = Database::new();

    // Right-arm REAL should win over left-arm INTEGER for same magnitude.
    let results = execute_select(&db, "SELECT 0 UNION SELECT 0.0;").unwrap();
    assert_eq!(results.len(), 1);
    match &results[0].values[0] {
        SqlValue::Real(_) | SqlValue::Float(_) | SqlValue::Double(_) | SqlValue::Numeric(_) => {}
        other => panic!(
            "Expected REAL-class value for `SELECT 0 UNION SELECT 0.0`, got {:?}",
            other
        ),
    }

    // Right-arm INTEGER should win over left-arm REAL for same magnitude.
    let results = execute_select(&db, "SELECT 0.0 UNION SELECT 0;").unwrap();
    assert_eq!(results.len(), 1);
    match &results[0].values[0] {
        SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => {}
        other => panic!(
            "Expected INTEGER-class value for `SELECT 0.0 UNION SELECT 0`, got {:?}",
            other
        ),
    }

    // Three-arm chain: ((0 UNION 0.0) UNION 0) → final right arm INTEGER wins.
    let results = execute_select(&db, "SELECT 0 UNION SELECT 0.0 UNION SELECT 0;").unwrap();
    assert_eq!(results.len(), 1);
    match &results[0].values[0] {
        SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => {}
        other => panic!("Expected INTEGER-class value for chained UNION, got {:?}", other),
    }
}
