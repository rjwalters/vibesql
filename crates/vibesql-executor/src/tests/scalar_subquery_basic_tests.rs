//! Basic scalar subquery tests
//!
//! Tests for basic (non-correlated) scalar subquery functionality:
//! - Scalar subqueries in WHERE clause
//! - Scalar subqueries in SELECT list
//! - Empty result set handling (returns NULL)

use super::super::*;

#[test]
fn test_scalar_subquery_in_where_clause() {
    // Test: SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)
    let mut db = vibesql_storage::Database::new();

    // Create employees table
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
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data: salaries are 50000, 60000, 70000 (avg = 60000)
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Integer(60000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            vibesql_types::SqlValue::Integer(70000),
        ]),
    )
    .unwrap();

    // Build subquery: SELECT AVG(salary) FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
                character_unit: None,
            },
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

    // Build main query: SELECT * FROM employees WHERE salary > (subquery)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "employees".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "salary".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::ScalarSubquery(subquery)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Only Charlie (70000) should be returned
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(70000));
}

#[test]
fn test_scalar_subquery_in_select_list() {
    // Test: SELECT name, salary, (SELECT MAX(salary) FROM employees) as max_sal FROM employees
    let mut db = vibesql_storage::Database::new();

    // Create employees table
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
            vibesql_catalog::ColumnSchema::new(
                "salary".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            vibesql_types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            vibesql_types::SqlValue::Integer(70000),
        ]),
    )
    .unwrap();

    // Build subquery: SELECT MAX(salary) FROM employees
    let subquery = Box::new(vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
                character_unit: None,
            },
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

    // Build main query: SELECT name, salary, (subquery) as max_sal FROM employees
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ScalarSubquery(subquery),
                alias: Some("max_sal".to_string()),
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
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should have 2 rows, each with max_sal = 70000
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(50000));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(70000)); // max_sal
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(result[1].values[1], vibesql_types::SqlValue::Integer(70000));
    assert_eq!(result[1].values[2], vibesql_types::SqlValue::Integer(70000)); // max_sal
}

#[test]
fn test_scalar_subquery_returns_null_when_empty() {
    // Test: SELECT (SELECT id FROM employees WHERE id = 999) as missing_id
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

    // Insert one row with id=1
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]),
    )
    .unwrap();

    // Build subquery that returns no rows: SELECT id FROM employees WHERE id = 999
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
        where_clause: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                999,
            ))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) as missing_id FROM employees
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(subquery),
            alias: Some("missing_id".to_string()),
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
    let result = executor.execute(&stmt).unwrap();

    // Should return 1 row with NULL
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Null);
}
