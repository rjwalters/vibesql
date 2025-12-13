//! Test for issue #4436: ORDER BY alias references in expressions
//!
//! When using expressions like `ORDER BY -x` or `ORDER BY abs(x)` where `x` is an alias
//! defined in the SELECT clause, VibeSQL should resolve the alias and use the underlying
//! expression for sorting.
//!
//! SQLite allows referencing SELECT clause aliases in ORDER BY expressions.

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

/// Helper to create the test table (matches select1.test test1 table)
fn create_test1_table(db: &mut Database) {
    let schema = TableSchema::new(
        "test1".to_string(),
        vec![ColumnSchema::new("f1".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("test1").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(11)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(33)])).unwrap();
}

/// Helper to create the t3 table for boolean test
fn create_t3_table(db: &mut Database) {
    let schema = TableSchema::new(
        "t3".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t3").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(3)])).unwrap();
}

/// Helper to create the t4 table for multi-column alias test
fn create_t4_table(db: &mut Database) {
    let schema = TableSchema::new(
        "t4".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t4").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(3)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(2)])).unwrap();
}

/// select1-10.2: SELECT f1 AS x FROM test1 ORDER BY -x
#[test]
fn test_order_by_negated_alias() {
    let mut db = Database::new();
    create_test1_table(&mut db);

    // ORDER BY -x where x is an alias for f1
    // -11 = -11, -33 = -33
    // Sorted ascending: -33, -11 → so rows are: 33, 11
    let rows = select_rows(&db, "SELECT f1 AS x FROM test1 ORDER BY -x");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(33));
    assert_eq!(rows[1].values[0], SqlValue::Integer(11));
}

/// select1-10.3: SELECT f1-23 AS x FROM test1 ORDER BY abs(x)
#[test]
fn test_order_by_function_of_alias() {
    let mut db = Database::new();
    create_test1_table(&mut db);

    // f1-23: 11-23=-12, 33-23=10
    // abs(x): abs(-12)=12, abs(10)=10
    // Sorted by abs(x) ascending: 10, 12 → rows are: 33-23=10, 11-23=-12
    let rows = select_rows(&db, "SELECT f1-23 AS x FROM test1 ORDER BY abs(x)");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(10)); // 33-23
    assert_eq!(rows[1].values[0], SqlValue::Integer(-12)); // 11-23
}

/// select1-10.4: SELECT a=1 AS x FROM t3 ORDER BY x
/// This tests ordering by a boolean alias result
#[test]
fn test_order_by_boolean_alias() {
    let mut db = Database::new();
    create_t3_table(&mut db);

    // a=1 returns: TRUE (for a=1), FALSE (for a=2), FALSE (for a=3)
    // SQLite sorts FALSE (0) before TRUE (1)
    // Sorted ascending: 0, 0, 1
    let rows = select_rows(&db, "SELECT a=1 AS x FROM t3 ORDER BY x");
    assert_eq!(rows.len(), 3);
    // FALSE (0) values come first, then TRUE (1)
    assert_eq!(rows[0].values[0], SqlValue::Integer(0)); // a=2 or a=3
    assert_eq!(rows[1].values[0], SqlValue::Integer(0)); // a=2 or a=3
    assert_eq!(rows[2].values[0], SqlValue::Integer(1)); // a=1
}

/// select1-10.6: SELECT a AS x, b AS y FROM t4 ORDER BY x, y
/// This tests ordering by multiple aliases
#[test]
fn test_order_by_multiple_aliases() {
    let mut db = Database::new();
    create_t4_table(&mut db);

    // Order by x (a), then y (b)
    // Expected order: (1,1), (1,2), (2,2), (2,3)
    let rows = select_rows(&db, "SELECT a AS x, b AS y FROM t4 ORDER BY x, y");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].values, vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1].values, vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
    assert_eq!(rows[2].values, vec![SqlValue::Integer(2), SqlValue::Integer(2)]);
    assert_eq!(rows[3].values, vec![SqlValue::Integer(2), SqlValue::Integer(3)]);
}

/// select1-10.7: SELECT a AS x FROM t4 ORDER BY 10-x
#[test]
fn test_order_by_expression_with_alias() {
    let mut db = Database::new();
    create_t4_table(&mut db);

    // 10-x values: 10-1=9, 10-1=9, 10-2=8, 10-2=8
    // Sorted ascending by 10-x: 8, 8, 9, 9
    // So rows where a=2 come first, then a=1
    let rows = select_rows(&db, "SELECT a AS x FROM t4 ORDER BY 10-x");
    assert_eq!(rows.len(), 4);
    // First two should have x=2 (10-2=8)
    assert_eq!(rows[0].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    // Last two should have x=1 (10-1=9)
    assert_eq!(rows[2].values[0], SqlValue::Integer(1));
    assert_eq!(rows[3].values[0], SqlValue::Integer(1));
}

/// Test ORDER BY with alias used inside CASE expression
#[test]
fn test_order_by_case_with_alias() {
    let mut db = Database::new();
    create_t4_table(&mut db);

    // CASE WHEN x > 1 THEN 0 ELSE 1 END
    // x=1 → 1, x=2 → 0
    // Sorted ascending: 0, 0, 1, 1
    let rows = select_rows(&db, "SELECT a AS x FROM t4 ORDER BY CASE WHEN x > 1 THEN 0 ELSE 1 END");
    assert_eq!(rows.len(), 4);
    // First two should have x=2 (CASE returns 0)
    assert_eq!(rows[0].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    // Last two should have x=1 (CASE returns 1)
    assert_eq!(rows[2].values[0], SqlValue::Integer(1));
    assert_eq!(rows[3].values[0], SqlValue::Integer(1));
}

/// Test ORDER BY alias with DESC
#[test]
fn test_order_by_alias_expr_desc() {
    let mut db = Database::new();
    create_test1_table(&mut db);

    // ORDER BY -x DESC
    // -11 = -11, -33 = -33
    // Sorted descending: -11, -33 → rows are: 11, 33
    let rows = select_rows(&db, "SELECT f1 AS x FROM test1 ORDER BY -x DESC");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(11));
    assert_eq!(rows[1].values[0], SqlValue::Integer(33));
}
