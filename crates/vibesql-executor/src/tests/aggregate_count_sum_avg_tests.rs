//! COUNT, SUM, and AVG aggregate function tests
//!
//! Tests for basic aggregate functions and their behavior with NULL values.

use super::super::*;

#[test]
fn test_count_star_no_group_by() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "age".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(35),
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
                name: "COUNT".to_string(),
                args: vec![vibesql_ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "users".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_sum_no_group_by() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
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
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(150),
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
            name: "sales".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(450));
}

#[test]
fn test_count_with_nulls() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "data".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // COUNT(*) counts all rows including NULL
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![vibesql_ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "data".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3)); // Counts all 3 rows
}

#[test]
fn test_sum_with_nulls() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "amounts".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(200),
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
                    column: "value".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "amounts".to_string(),
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
    // SUM ignores NULL values, so 100 + 200 = 300
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(300));
}

#[test]
fn test_avg_function() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "scores".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "score".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "scores",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(80),
        ]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(90),
        ]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(70),
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
                    column: "score".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "scores".to_string(),
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
    // AVG(80, 90, 70) = 240 / 3 = 80
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Numeric(80.0));
}

#[test]
fn test_avg_with_nulls() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ratings".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "rating".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "ratings",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(3),
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
                    column: "rating".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "ratings".to_string(),
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
    // AVG ignores NULL, so (5 + 3) / 2 = 4
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Numeric(4.0));
}

#[test]
fn test_count_column_all_nulls() {
    // Edge case: COUNT(column) with ALL NULL values should return 0, not row count
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "null_data".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "value".to_string(),
                vibesql_types::DataType::Integer,
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "null_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Null,
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // COUNT(column) should return 0 when all values are NULL
    let stmt_count_col = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "value".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "null_data".to_string(),
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

    let result = executor.execute(&stmt_count_col).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(0)); // COUNT(col) with all NULLs = 0

    // COUNT(*) should still return row count
    let stmt_count_star = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![vibesql_ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "null_data".to_string(),
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

    let result = executor.execute(&stmt_count_star).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(3)); // COUNT(*) counts rows
}

// ============================================================================
// Tests for COUNT(*) in CASE expressions (Issue #1150)
// ============================================================================

#[test]
fn test_count_star_in_simple_case_expression() {
    // Test: CASE COUNT(*) WHEN 3 THEN 'three' ELSE 'other' END
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "data".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("data", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("data", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("data", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Case {
                operand: Some(Box::new(vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                })),
                when_clauses: vec![vibesql_ast::CaseWhen {
                    conditions: vec![vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(3),
                    )],
                    result: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three"))),
                }],
                else_result: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("other")),
                ))),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "data".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("three")));
}

#[test]
fn test_count_star_in_searched_case_expression() {
    // Test: CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "items".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("items", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("items", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("items", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Case {
                operand: None,
                when_clauses: vec![vibesql_ast::CaseWhen {
                    conditions: vec![vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::AggregateFunction {
                            name: "COUNT".to_string(),
                            distinct: false,
                            args: vec![vibesql_ast::Expression::Wildcard],
                        }),
                        op: vibesql_ast::BinaryOperator::GreaterThan,
                        right: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(2),
                        )),
                    }],
                    result: vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("many"))),
                }],
                else_result: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("few")),
                ))),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "items".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("many")));
}

#[test]
fn test_count_star_in_arithmetic_expression() {
    // Test: COUNT(*) * 10
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "records".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("records", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("records", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("records", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(3)]))
        .unwrap();
    db.insert_row("records", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(4)]))
        .unwrap();
    db.insert_row("records", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(5)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                }),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(10),
                )),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "records".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(50)); // 5 * 10
}

#[test]
fn test_count_star_in_case_then_clause() {
    // Test: CASE WHEN 1=1 THEN COUNT(*) ELSE 0 END
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Case {
                operand: None,
                when_clauses: vec![vibesql_ast::CaseWhen {
                    conditions: vec![vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(1),
                        )),
                        op: vibesql_ast::BinaryOperator::Equal,
                        right: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(1),
                        )),
                    }],
                    result: vibesql_ast::Expression::AggregateFunction {
                        name: "COUNT".to_string(),
                        distinct: false,
                        args: vec![vibesql_ast::Expression::Wildcard],
                    },
                }],
                else_result: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(0),
                ))),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "test".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_count_star_in_nested_case_expression() {
    // Test: CASE COUNT(*) WHEN 2 THEN CASE WHEN 1=1 THEN 'two' END ELSE 'other' END
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "nested".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nested", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("nested", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::Case {
                operand: Some(Box::new(vibesql_ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![vibesql_ast::Expression::Wildcard],
                })),
                when_clauses: vec![vibesql_ast::CaseWhen {
                    conditions: vec![vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(2),
                    )],
                    result: vibesql_ast::Expression::Case {
                        operand: None,
                        when_clauses: vec![vibesql_ast::CaseWhen {
                            conditions: vec![vibesql_ast::Expression::BinaryOp {
                                left: Box::new(vibesql_ast::Expression::Literal(
                                    vibesql_types::SqlValue::Integer(1),
                                )),
                                op: vibesql_ast::BinaryOperator::Equal,
                                right: Box::new(vibesql_ast::Expression::Literal(
                                    vibesql_types::SqlValue::Integer(1),
                                )),
                            }],
                            result: vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")),
                            ),
                        }],
                        else_result: None,
                    },
                }],
                else_result: Some(Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("other")),
                ))),
            },
            alias: None,
        }],
        from: Some(vibesql_ast::FromClause::Table {
            name: "nested".to_string(),
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
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("two")));
}
