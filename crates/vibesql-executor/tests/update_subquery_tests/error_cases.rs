use vibesql_ast::{Assignment, Expression, UpdateStmt};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{ExecutorError, UpdateExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_update_with_subquery_multiple_rows_error() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (amount INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(60000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(70000)])).unwrap();

    // UPDATE employees SET salary = (SELECT amount FROM salaries) -- ERROR: multiple rows
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,

        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "amount".to_string() },
            alias: None,
        }],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "salaries".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryReturnedMultipleRows { .. } => {
            // Expected error
        }
        other => panic!("Expected SubqueryReturnedMultipleRows, got {:?}", other),
    }
}

#[test]
fn test_update_with_subquery_multiple_columns_error() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (min_amt INT, max_amt INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![
            ColumnSchema::new("min_amt".to_string(), DataType::Integer, false),
            ColumnSchema::new("max_amt".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(50000), SqlValue::Integer(100000)]))
        .unwrap();

    // UPDATE employees SET salary = (SELECT min_amt, max_amt FROM salaries) -- ERROR: 2 columns
    let subquery = Box::new(vibesql_ast::SelectStmt {
        with_clause: None,

        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "min_amt".to_string() },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "max_amt".to_string() },
                alias: None,
            },
        ],
        into_table: None,
        into_variables: None,
        from: Some(vibesql_ast::FromClause::Table {
            name: "salaries".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected SubqueryColumnCountMismatch, got {:?}", other),
    }
}
