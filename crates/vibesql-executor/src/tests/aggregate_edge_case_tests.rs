//! Edge case tests for aggregate functions
//!
//! Tests for decimal precision, mixed numeric types, and CASE expressions with aggregates.

use super::super::*;

#[test]
fn test_avg_precision_decimal() {
    // Edge case: AVG should preserve DECIMAL precision, not truncate to INTEGER
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "prices".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "price".to_string(),
                vibesql_types::DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "prices",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Numeric("10.50".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "prices",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Numeric("20.75".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "prices",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Numeric("15.25".parse().unwrap()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
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
                    column: "price".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "prices".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG(10.50, 20.75, 15.25) = 46.50 / 3 = 15.50
    match &result[0].values[0] {
        vibesql_types::SqlValue::Numeric(value) => {
            assert!((value - 15.50).abs() < 0.01, "Expected 15.50, got {}", value);
        }
        other => panic!("Expected Numeric value, got {:?}", other),
    }
}

#[test]
fn test_sum_mixed_numeric_types() {
    // Edge case: SUM on NUMERIC values should work
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "mixed_amounts".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "mixed_amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Numeric("100.50".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "mixed_amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Numeric("200.25".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "mixed_amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Numeric("150.00".parse().unwrap()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "mixed_amounts".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM(100.50, 200.25, 150.00) = 450.75
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Numeric("450.75".parse().unwrap()));
}

#[test]
fn test_aggregate_with_case_expression() {
    // Edge case: Aggregates with CASE expressions - common real-world pattern
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "transactions".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "type".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(10) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "amount".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "transactions",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("credit")),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "transactions",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("debit")),
            vibesql_types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "transactions",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("credit")),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END)
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![vibesql_ast::Expression::Case {
                    operand: None,
                    when_clauses: vec![vibesql_ast::CaseWhen {
                        conditions: vec![vibesql_ast::Expression::BinaryOp {
                            left: Box::new(vibesql_ast::Expression::ColumnRef {
                                table: None,
                                column: "type".to_string(),
                            }),
                            op: vibesql_ast::BinaryOperator::Equal,
                            right: Box::new(vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("credit")),
                            )),
                        }],
                        result: vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        },
                    }],
                    else_result: Some(Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(0),
                    ))),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "transactions".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM of credits only: 100 + 200 = 300 (debit of 50 becomes 0)
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(300));
}

#[test]
fn test_max_with_unary_plus() {
    // Test MAX(+column) - unary plus operator
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "col0".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "MAX".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Plus,
                    expr: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "col0".to_string(),
                    }),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_max_with_unary_minus() {
    // Test MAX(-column) - unary minus operator
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "col0".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "MAX".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "col0".to_string(),
                    }),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // MAX(-col0) where col0 = {1, 5, 3} → {-1, -5, -3} → max is -1
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(-1));
}

#[test]
fn test_count_with_not() {
    // Test COUNT(NOT column) - unary NOT operator
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "tab0".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "col1".to_string(),
            vibesql_types::DataType::Boolean,
            true, // nullable
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Boolean(true)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Boolean(false)]))
        .unwrap();
    db.insert_row("tab0", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Boolean(true)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::AggregateFunction {
                name: "COUNT".to_string(),
                distinct: false,
                args: vec![vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Not,
                    expr: Box::new(vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "col1".to_string(),
                    }),
                }],
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "tab0".to_string(),
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

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // COUNT counts non-NULL values
    // NOT true = false, NOT false = true, NOT true = false
    // All non-NULL, so COUNT = 3
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
}
