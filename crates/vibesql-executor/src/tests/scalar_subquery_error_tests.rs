//! Scalar subquery error handling tests
//!
//! Tests for scalar subquery error cases and validation:
//! - Multiple rows returned (cardinality violation)
//! - Multiple columns returned (column count violation)

use super::super::*;

#[test]
fn test_scalar_subquery_error_multiple_rows() {
    // Test: Scalar subquery returns multiple rows - should error
    let mut db = vibesql_storage::Database::new();

    // Create employees table
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert multiple rows
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]),
    )
    .unwrap();

    // Build subquery that returns multiple rows: SELECT id FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(subquery),
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // Should error with SubqueryReturnedMultipleRows
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryReturnedMultipleRows { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryReturnedMultipleRows error"),
    }
}

#[test]
fn test_scalar_subquery_error_multiple_columns() {
    // Test: Scalar subquery returns multiple columns - should error
    let mut db = vibesql_storage::Database::new();

    // Create employees table with multiple columns
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert one row
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    // Build subquery that returns multiple columns: SELECT id, name FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(subquery),
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // Should error with SubqueryColumnCountMismatch
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryColumnCountMismatch error"),
    }
}
