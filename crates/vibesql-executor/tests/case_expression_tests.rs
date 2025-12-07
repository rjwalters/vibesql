use vibesql_ast::Expression;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::ExpressionEvaluator;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

/// Helper to create a simple schema for testing
fn create_test_schema() -> TableSchema {
    TableSchema::new(
        "test_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "status".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("value".to_string(), DataType::Integer, false),
        ],
    )
}

#[test]
fn test_simple_case_basic_match() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE status WHEN 'active' THEN 'Active User' WHEN 'inactive' THEN 'Inactive User' ELSE
    // 'Unknown' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef {
            table: None,
            column: "status".to_string(),
        })),
        when_clauses: vec![
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("active")))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Active User"))),
            },
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("inactive")))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Inactive User"))),
            },
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Unknown"))))),
    };

    // Test with 'active' status
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("active")),
        SqlValue::Integer(100),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Active User")));

    // Test with 'inactive' status
    let row2 = Row::new(vec![
        SqlValue::Integer(2),
        SqlValue::Varchar(arcstr::ArcStr::from("inactive")),
        SqlValue::Integer(50),
    ]);
    let result2 = evaluator.eval(&case_expr, &row2).unwrap();
    assert_eq!(result2, SqlValue::Varchar(arcstr::ArcStr::from("Inactive User")));

    // Test with unmatched value (should return ELSE)
    let row3 = Row::new(vec![
        SqlValue::Integer(3),
        SqlValue::Varchar(arcstr::ArcStr::from("pending")),
        SqlValue::Integer(75),
    ]);
    let result3 = evaluator.eval(&case_expr, &row3).unwrap();
    assert_eq!(result3, SqlValue::Varchar(arcstr::ArcStr::from("Unknown")));
}

#[test]
fn test_simple_case_null_handling() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE status WHEN NULL THEN 'Null Status' ELSE 'Not Null' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef {
            table: None,
            column: "status".to_string(),
        })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![Expression::Literal(SqlValue::Null)],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Null Status"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Not Null"))))),
    };

    // Test with NULL status
    // In standard SQL, NULL = NULL returns NULL (not TRUE), so the WHEN clause
    // does NOT match. The ELSE result should be returned.
    let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Integer(100)]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Not Null")));
}

#[test]
fn test_searched_case_basic() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE WHEN value < 50 THEN 'Low' WHEN value < 100 THEN 'Medium' ELSE 'High' END
    let case_expr = Expression::Case {
        operand: None, // Searched CASE
        when_clauses: vec![
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::LessThan,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "value".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Integer(50))),
                }],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Low"))),
            },
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::LessThan,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "value".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Integer(100))),
                }],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Medium"))),
            },
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("High"))))),
    };

    // Test value = 25 (should match first condition)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(25),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Low")));

    // Test value = 75 (should match second condition)
    let row2 = Row::new(vec![
        SqlValue::Integer(2),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(75),
    ]);
    let result2 = evaluator.eval(&case_expr, &row2).unwrap();
    assert_eq!(result2, SqlValue::Varchar(arcstr::ArcStr::from("Medium")));

    // Test value = 150 (should use ELSE)
    let row3 = Row::new(vec![
        SqlValue::Integer(3),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(150),
    ]);
    let result3 = evaluator.eval(&case_expr, &row3).unwrap();
    assert_eq!(result3, SqlValue::Varchar(arcstr::ArcStr::from("High")));
}

#[test]
fn test_searched_case_null_condition() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE WHEN NULL THEN 'yes' ELSE 'no' END
    // NULL condition should be FALSE, not TRUE
    let case_expr = Expression::Case {
        operand: None,
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![Expression::Literal(SqlValue::Null)],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("yes"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("no"))))),
    };

    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(100),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    // Should return ELSE because NULL is not TRUE
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("no")));
}

#[test]
fn test_case_no_else_defaults_to_null() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 1 THEN 'one' WHEN 2 THEN 'two' END
    // No ELSE clause, value = 3 -> should return NULL
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(1))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("one"))),
            },
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(2))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("two"))),
            },
        ],
        else_result: None, // No ELSE clause
    };

    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(3),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_case_lazy_evaluation() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 1 THEN 'first' WHEN 1 THEN 'second' ELSE 'other' END
    // Should return 'first' and not evaluate second WHEN
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(1))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("first"))),
            },
            vibesql_ast::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(1))],
                result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("second"))),
            },
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("other"))))),
    };

    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(1),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    // Should match first WHEN clause
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("first")));
}

// Tests for comma-separated WHEN values (Issue #409)

#[test]
fn test_case_comma_separated_matching_first() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 2, 3, 4 THEN 'match' ELSE 'no' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
                Expression::Literal(SqlValue::Integer(4)),
            ],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("match"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("no"))))),
    };

    // Test with value = 2 (matches first condition in list)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(2),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("match")));
}

#[test]
fn test_case_comma_separated_matching_last() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 2, 3, 4 THEN 'match' ELSE 'no' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
                Expression::Literal(SqlValue::Integer(4)),
            ],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("match"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("no"))))),
    };

    // Test with value = 4 (matches last condition in list)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(4),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("match")));
}

#[test]
fn test_case_comma_separated_no_match() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 2, 3, 4 THEN 'match' ELSE 'no' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
                Expression::Literal(SqlValue::Integer(4)),
            ],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("match"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("no"))))),
    };

    // Test with value = 5 (no match, should return ELSE)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(5),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("no")));
}

#[test]
fn test_case_comma_separated_duplicate_values() {
    // Test from spec: CASE 0 WHEN 2, 2 THEN 1 ELSE 1 END
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 2, 2 THEN 1 ELSE 1 END (duplicate values allowed)
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(2)),
            ],
            result: Expression::Literal(SqlValue::Integer(1)),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(1)))),
    };

    // Test with value = 0 (no match, should return ELSE)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(0),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Integer(1));
}

#[test]
fn test_case_comma_separated_with_null() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE value WHEN 2, 2 THEN 1 ELSE NULL END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef { table: None, column: "value".to_string() })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(2)),
            ],
            result: Expression::Literal(SqlValue::Integer(1)),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    // Test with value = 2 (matches, should return 1)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("test")),
        SqlValue::Integer(2),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Integer(1));
}

#[test]
fn test_case_comma_separated_varchar() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    // CASE status WHEN 'active', 'pending', 'new' THEN 'open' ELSE 'closed' END
    let case_expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef {
            table: None,
            column: "status".to_string(),
        })),
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("active"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("pending"))),
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("new"))),
            ],
            result: Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("open"))),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("closed"))))),
    };

    // Test with status = 'pending' (matches second condition in list)
    let row = Row::new(vec![
        SqlValue::Integer(1),
        SqlValue::Varchar(arcstr::ArcStr::from("pending")),
        SqlValue::Integer(100),
    ]);
    let result = evaluator.eval(&case_expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("open")));
}
