//! Tests for issue #1530: IN subquery must validate actual column count after wildcard expansion
//!
//! These tests verify that IN subqueries correctly reject multi-column subqueries,
//! including cases where wildcards expand to multiple columns.

use super::super::super::*;

#[test]
fn test_in_subquery_wildcard_multi_column_rejected() {
    let mut db = vibesql_storage::Database::new();

    // Create table with 2 columns
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
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(99),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT 1 FROM t1 WHERE 1 IN (SELECT * FROM t1)
    // This should FAIL because SELECT * expands to 2 columns
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
                select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }], // * expands to 2 columns!
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
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        e => panic!("Expected SubqueryColumnCountMismatch, got: {:?}", e),
    }
}

#[test]
fn test_in_subquery_explicit_multi_column_rejected() {
    let mut db = vibesql_storage::Database::new();

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
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(99),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT 1 FROM t1 WHERE 1 IN (SELECT x, y FROM t1)
    // This should FAIL because subquery returns 2 columns
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
                select_list: vec![
                    vibesql_ast::SelectItem::Expression {
                        expr: vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "x".to_string(),
                        },
                        alias: None,
                    },
                    vibesql_ast::SelectItem::Expression {
                        expr: vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "y".to_string(),
                        },
                        alias: None,
                    },
                ],
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
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        e => panic!("Expected SubqueryColumnCountMismatch, got: {:?}", e),
    }
}

#[test]
fn test_in_subquery_single_column_accepted() {
    let mut db = vibesql_storage::Database::new();

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
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(99),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT 1 FROM t1 WHERE 1 IN (SELECT x FROM t1)
    // This should SUCCEED because subquery returns 1 column
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
                select_list: vec![vibesql_ast::SelectItem::Expression {
                    expr: vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "x".to_string(),
                    },
                    alias: None,
                }],
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
    assert!(result.is_ok());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 1); // One row matches
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_scalar_subquery_wildcard_multi_column_rejected() {
    let mut db = vibesql_storage::Database::new();

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
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "t1",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(99),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT (SELECT * FROM t1) FROM t1
    // This should FAIL because scalar subquery expands to 2 columns
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
                with_clause: None,
                set_operation: None,
                distinct: false,
                select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }], // * expands to 2 columns!
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
            })),
            alias: None,
        }],
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
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        e => panic!("Expected SubqueryColumnCountMismatch, got: {:?}", e),
    }
}
