//! Tests for aggregate patterns from random/aggregates SQLLogicTest suite
//! These tests reproduce specific patterns that cause failures in the random aggregate tests

use super::super::*;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute a SQL statement
fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("Execution error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("Execution error: {:?}", e))?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

#[test]
fn test_aggregate_with_cross_join() {
    // Pattern from slt_good_0.test: MIN(ALL -32), COUNT(*) * COUNT(*)
    // FROM (tab0 CROSS JOIN tab0)

    let mut db = Database::new();

    // Create simple test tables
    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(97,1,99)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(15,81,47)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(87,21,10)").unwrap();

    // Test MIN with constant in CROSS JOIN context
    let rows = execute_sql(&mut db,
        "SELECT MIN(ALL -32) AS col0, COUNT(*) * COUNT(*) FROM tab0 AS cor0 CROSS JOIN tab0 AS cor1"
    ).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(-32)); // MIN of constant is the constant
    assert_eq!(rows[0].values[1], SqlValue::Integer(81)); // 9 rows * 9 rows = 81
}

#[test]
fn test_aggregate_count_all_in_cross_join() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(97,1,99)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(15,81,47)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(87,21,10)").unwrap();

    // Test COUNT(*) in CROSS JOIN - should count cartesian product
    let rows =
        execute_sql(&mut db, "SELECT COUNT(*) FROM tab0 AS cor0 CROSS JOIN tab0 cor1").unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(9)); // 3 * 3 = 9
}

#[test]
fn test_aggregate_with_all_keyword() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(97)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(15)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(87)").unwrap();

    // Test MIN(ALL expr) - ALL is default, should work same as MIN(expr)
    let rows = execute_sql(&mut db, "SELECT MIN(ALL col0) FROM tab0").unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(15));
}

#[test]
fn test_aggregate_with_arithmetic_in_cross_join() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(51,14,96)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(85,5,59)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(91,47,68)").unwrap();

    // Test aggregate with arithmetic: COUNT(*) * COUNT(*)
    let rows =
        execute_sql(&mut db, "SELECT COUNT(*) * COUNT(*) FROM (tab1 AS cor0 CROSS JOIN tab1 cor1)")
            .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(81)); // (3*3) * (3*3) = 9 * 9 = 81
}

#[test]
fn test_div_operator_in_aggregate_context() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(20)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(40)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(60)").unwrap();

    // Test DIV operator (integer division) in aggregate context
    let rows = execute_sql(&mut db, "SELECT DISTINCT 20 DIV -97 FROM tab0").unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(0)); // 20 / -97 = 0 (integer division)
}

#[test]
fn test_multiple_aggregates_same_query() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(64,77,40)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(75,67,58)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(46,51,23)").unwrap();

    // Test multiple aggregates in same query
    let rows = execute_sql(
        &mut db,
        "SELECT MIN(col0), MAX(col1), AVG(col2), COUNT(*), SUM(col0) FROM tab2",
    )
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(46)); // MIN(col0)
    assert_eq!(rows[0].values[1], SqlValue::Integer(77)); // MAX(col1)
                                                          // AVG(col2) = (40 + 58 + 23) / 3 = 121 / 3 = 40.333...
    assert_eq!(rows[0].values[3], SqlValue::Integer(3)); // COUNT(*)
    assert_eq!(rows[0].values[4], SqlValue::Integer(185)); // SUM(col0) = 64+75+46
}

#[test]
fn test_aggregate_with_constant_expression() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab1(col0 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(1)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(2)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab1 VALUES(3)").unwrap();

    // Test aggregate of constant expression across multiple rows
    let rows = execute_sql(&mut db, "SELECT ALL 39 * 15 FROM tab1").unwrap();

    assert_eq!(rows.len(), 3); // Should return constant for each row
    for row in rows {
        assert_eq!(row.values[0], SqlValue::Integer(585)); // 39 * 15
    }
}

#[test]
fn test_aggregate_min_with_expression() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(10, 20)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(30, 40)").unwrap();

    // Test MIN with negative expression
    let rows = execute_sql(&mut db, "SELECT MIN(ALL -32) FROM tab0").unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(-32)); // MIN of constant
}

#[test]
fn test_aggregate_rowsort_multiple_columns() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab2(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(64,77,40)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(75,67,58)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab2 VALUES(46,51,23)").unwrap();

    // Test query that returns multiple columns (tests hash verification for >8 values)
    let rows =
        execute_sql(&mut db, "SELECT DISTINCT * FROM tab2 WHERE -col2 + col1 IS NOT NULL").unwrap();

    assert_eq!(rows.len(), 3);
    // Results should contain all 3 rows (9 values total)
    // This would be hashed in SQLLogicTest with hash-threshold 8
}

#[test]
fn test_aggregate_with_null_arithmetic() {
    let mut db = Database::new();

    execute_sql(&mut db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(NULL, 10)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(20, NULL)").unwrap();
    execute_sql(&mut db, "INSERT INTO tab0 VALUES(30, 40)").unwrap();

    // Test aggregate with NULL in arithmetic
    let rows = execute_sql(&mut db, "SELECT COUNT(*), SUM(col0), SUM(col1) FROM tab0").unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(3)); // COUNT(*) includes NULLs
    assert_eq!(rows[0].values[1], SqlValue::Integer(50)); // SUM(col0) = 20+30, ignores NULL
    assert_eq!(rows[0].values[2], SqlValue::Integer(50)); // SUM(col1) = 10+40, ignores NULL
}
