//! Tests for SELECT without FROM clause
//!
//! Tests queries that evaluate expressions without any table context

use super::super::*;

#[test]
fn test_select_literal_integers() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: Some("one".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                alias: Some("two".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                alias: Some("three".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Single row
    assert_eq!(result[0].values.len(), 3);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_select_literal_mixed_types() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42)),
                alias: Some("num".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
                alias: Some("text".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
                alias: Some("flag".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(42));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Boolean(true));
}

#[test]
fn test_select_arithmetic_expression() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(1),
                    )),
                    op: vibesql_ast::BinaryOperator::Plus,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(1),
                    )),
                },
                alias: Some("sum".to_string()),
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(2),
                    )),
                    op: vibesql_ast::BinaryOperator::Multiply,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(3),
                    )),
                },
                alias: Some("product".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Note: +, -, * return Integer; only / returns Numeric currently
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2)); // 1 + 1
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(6)); // 2 * 3
}

#[test]
fn test_select_function_call() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "UPPER".to_string(),
                args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))],
                character_unit: None,
            },
            alias: Some("upper".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HELLO")));
}

#[test]
fn test_select_star_without_from_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("require FROM clause") || msg.contains("requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_column_reference_without_from_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "some_column".to_string(),
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_is_null_with_column_reference_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::IsNull {
                expr: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_between_with_column_reference_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Between {
                expr: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                low: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    10,
                ))),
                high: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    20,
                ))),
                negated: false,
                symmetric: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_cast_with_column_reference_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Cast {
                expr: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                data_type: vibesql_types::DataType::Varchar { max_length: Some(255) },
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_like_with_column_reference_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Like {
                expr: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                pattern: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("A%")),
                )),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_in_list_with_column_reference_fails() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::InList {
                expr: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                values: vec![
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
                ],
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_hex_literal_in_subquery_without_from() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT x'303132' IN (SELECT * FROM t1)
    // This should work now that hex literals are properly parsed as literals, not column references
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::In {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("012")))),
                subquery: Box::new(vibesql_ast::SelectStmt {
                    with_clause: None,
                    set_operation: None,
                    distinct: false,
                    select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                    from: Some(vibesql_ast::FromClause::Table {
                        name: "t1".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    where_clause: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                    into_table: None,
                    into_variables: None,
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Empty subquery returns FALSE for IN
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Boolean(false));
}

#[test]
fn test_hex_literal_simple() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT x'48656C6C6F' (should return "Hello")
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
            alias: Some("hex_value".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")));
}

#[test]
fn test_binary_literal_in_subquery_without_from() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Varchar { max_length: None },
            false,
        )],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT b'01010101' IN (SELECT * FROM t1)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::In {
                expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("U")))),
                subquery: Box::new(vibesql_ast::SelectStmt {
                    with_clause: None,
                    set_operation: None,
                    distinct: false,
                    select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                    from: Some(vibesql_ast::FromClause::Table {
                        name: "t1".to_string(),
                        alias: None,
                        column_aliases: None,
                    }),
                    where_clause: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                    into_table: None,
                    into_variables: None,
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Empty subquery returns FALSE for IN
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Boolean(false));
}

/// Test for issue #1612: Scalar subquery validation must work even for empty results
/// SQL standard R-35033-20570: The subquery on the right of an IN or NOT IN operator
/// must be a scalar subquery (1 column) if the left expression is not a row value expression.
#[test]
fn test_in_subquery_multi_column_empty_table_should_error() {
    let mut db = vibesql_storage::Database::new();

    // Create empty table with TWO columns (x and y)
    let schema = vibesql_catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "x".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "y".to_string(),
                vibesql_types::DataType::Varchar { max_length: None },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT 1 FROM t1 WHERE 1 IN (SELECT * FROM t1)
    // This should ERROR because SELECT * FROM t1 returns 2 columns, not 1
    // Even though t1 is empty, the column count validation must still run
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
        }),
        where_clause: Some(vibesql_ast::Expression::In {
            expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            subquery: Box::new(vibesql_ast::SelectStmt {
                with_clause: None,
                set_operation: None,
                distinct: false,
                select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                from: Some(vibesql_ast::FromClause::Table {
                    name: "t1".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                into_table: None,
                into_variables: None,
            }),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::SubqueryColumnCountMismatch { expected, actual }) => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryColumnCountMismatch error, got: {:?}", result),
    }
}
