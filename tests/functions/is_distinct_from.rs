// Test for IS [NOT] DISTINCT FROM syntax (SQL:1999)
// NULL-safe comparison operator for JOIN conditions and predicates.

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
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn setup_test_db() -> Database {
    let schema = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    // Row 1: a=1, b=1 (equal non-NULL values)
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1)])).unwrap();
    // Row 2: a=1, b=2 (different non-NULL values)
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();
    // Row 3: a=NULL, b=NULL (both NULL)
    db.insert_row("t1", Row::new(vec![SqlValue::Null, SqlValue::Null])).unwrap();
    // Row 4: a=NULL, b=1 (one NULL, one not)
    db.insert_row("t1", Row::new(vec![SqlValue::Null, SqlValue::Integer(1)])).unwrap();
    // Row 5: a=1, b=NULL (one NULL, one not)
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();
    db
}

#[test]
fn test_is_distinct_from_non_null_equal() {
    let db = setup_test_db();
    // Two equal non-NULL values: NOT distinct
    let results = execute_select(&db, "SELECT a, b FROM t1 WHERE a IS DISTINCT FROM b")
        .expect("Query should succeed");
    // Should find: (1,2), (NULL,1), (1,NULL) - values that ARE distinct
    // Should NOT find: (1,1), (NULL,NULL)
    assert_eq!(results.len(), 3, "Should find 3 rows where a IS DISTINCT FROM b");
}

#[test]
fn test_is_not_distinct_from_non_null_equal() {
    let db = setup_test_db();
    // Two equal non-NULL values: NOT distinct (so IS NOT DISTINCT returns true)
    let results = execute_select(&db, "SELECT a, b FROM t1 WHERE a IS NOT DISTINCT FROM b")
        .expect("Query should succeed");
    // Should find: (1,1), (NULL,NULL) - values that are NOT distinct
    assert_eq!(results.len(), 2, "Should find 2 rows where a IS NOT DISTINCT FROM b");
}

#[test]
fn test_is_distinct_from_both_null() {
    let db = setup_test_db();
    // NULL IS DISTINCT FROM NULL → FALSE (they're considered equal for this comparison)
    let results = execute_select(&db, "SELECT a FROM t1 WHERE NULL IS DISTINCT FROM NULL")
        .expect("Query should succeed");
    assert_eq!(results.len(), 0, "NULL IS DISTINCT FROM NULL should be FALSE");
}

#[test]
fn test_is_not_distinct_from_both_null() {
    let db = setup_test_db();
    // NULL IS NOT DISTINCT FROM NULL → TRUE (they're considered equal)
    let results = execute_select(&db, "SELECT a FROM t1 WHERE NULL IS NOT DISTINCT FROM NULL")
        .expect("Query should succeed");
    assert_eq!(results.len(), 5, "NULL IS NOT DISTINCT FROM NULL should be TRUE for all rows");
}

#[test]
fn test_is_distinct_from_one_null() {
    let db = setup_test_db();
    // NULL IS DISTINCT FROM non-NULL → TRUE
    let results = execute_select(&db, "SELECT a FROM t1 WHERE NULL IS DISTINCT FROM 1")
        .expect("Query should succeed");
    assert_eq!(results.len(), 5, "NULL IS DISTINCT FROM 1 should be TRUE for all rows");
}

#[test]
fn test_is_not_distinct_from_one_null() {
    let db = setup_test_db();
    // NULL IS NOT DISTINCT FROM non-NULL → FALSE
    let results = execute_select(&db, "SELECT a FROM t1 WHERE NULL IS NOT DISTINCT FROM 1")
        .expect("Query should succeed");
    assert_eq!(results.len(), 0, "NULL IS NOT DISTINCT FROM 1 should be FALSE");
}

#[test]
fn test_is_distinct_from_non_null_different() {
    let db = setup_test_db();
    // 1 IS DISTINCT FROM 2 → TRUE (different values)
    let results = execute_select(&db, "SELECT a FROM t1 WHERE 1 IS DISTINCT FROM 2")
        .expect("Query should succeed");
    assert_eq!(results.len(), 5, "1 IS DISTINCT FROM 2 should be TRUE for all rows");
}

#[test]
fn test_is_not_distinct_from_non_null_different() {
    let db = setup_test_db();
    // 1 IS NOT DISTINCT FROM 2 → FALSE (different values)
    let results = execute_select(&db, "SELECT a FROM t1 WHERE 1 IS NOT DISTINCT FROM 2")
        .expect("Query should succeed");
    assert_eq!(results.len(), 0, "1 IS NOT DISTINCT FROM 2 should be FALSE");
}

#[test]
fn test_is_distinct_from_in_join_condition() {
    // This test verifies the syntax parses correctly in a JOIN context
    let schema1 = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
    );
    let schema2 = TableSchema::new(
        "t2".to_string(),
        vec![ColumnSchema::new("b".to_string(), DataType::Integer, true)],
    );

    let mut db = Database::new();
    db.create_table(schema1).unwrap();
    db.create_table(schema2).unwrap();
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("t1", Row::new(vec![SqlValue::Null])).unwrap();
    db.insert_row("t2", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("t2", Row::new(vec![SqlValue::Null])).unwrap();

    // IS NOT DISTINCT FROM is commonly used for NULL-safe joins
    let results =
        execute_select(&db, "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.a IS NOT DISTINCT FROM t2.b")
            .expect("Query should succeed");
    // Should match: (1,1) and (NULL,NULL)
    assert_eq!(results.len(), 2, "Should find 2 matching rows in NULL-safe join");
}

#[test]
fn test_is_distinct_from_with_expressions() {
    let db = setup_test_db();
    // Test with arithmetic expressions
    let results =
        execute_select(&db, "SELECT a FROM t1 WHERE (a + 1) IS NOT DISTINCT FROM (b + 1)")
            .expect("Query should succeed");
    // (a+1) IS NOT DISTINCT FROM (b+1) when a=b (for row 1 where a=1,b=1)
    // For NULL values, NULL+1=NULL, and NULL IS NOT DISTINCT FROM NULL
    assert_eq!(results.len(), 2, "Should find 2 rows where (a+1) IS NOT DISTINCT FROM (b+1)");
}

#[test]
fn test_pretty_print_is_distinct_from() {
    use vibesql_ast::pretty_print::ToSql;

    let sql = "SELECT * FROM t1 WHERE a IS DISTINCT FROM b";
    let stmt = Parser::parse_sql(sql).expect("Parse should succeed");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let printed = select_stmt.to_sql();
    assert!(
        printed.contains("IS DISTINCT FROM"),
        "Pretty print should contain 'IS DISTINCT FROM': {}",
        printed
    );
}

#[test]
fn test_pretty_print_is_not_distinct_from() {
    use vibesql_ast::pretty_print::ToSql;

    let sql = "SELECT * FROM t1 WHERE a IS NOT DISTINCT FROM b";
    let stmt = Parser::parse_sql(sql).expect("Parse should succeed");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let printed = select_stmt.to_sql();
    assert!(
        printed.contains("IS NOT DISTINCT FROM"),
        "Pretty print should contain 'IS NOT DISTINCT FROM': {}",
        printed
    );
}
