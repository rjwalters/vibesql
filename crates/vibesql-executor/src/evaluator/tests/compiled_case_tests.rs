// Unit tests for compiled_case.rs - CompiledCaseExpression optimization

use vibesql_ast::{BinaryOperator, CaseWhen, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::evaluator::compiled_case::CompiledCaseExpression;
use crate::schema::CombinedSchema;

fn create_test_schema() -> CombinedSchema {
    let table_schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new(
                "day_name".to_string(),
                DataType::Varchar { max_length: Some(20) },
                false,
            ),
            ColumnSchema::new("sales_price".to_string(), DataType::Integer, false),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, false),
        ],
    );
    CombinedSchema::from_table("test".to_string(), table_schema)
}

fn create_simple_case_expression(
    col_name: &str,
    match_value: SqlValue,
    result_col: &str,
) -> Expression {
    // CASE WHEN col_name = match_value THEN result_col ELSE NULL END
    Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: col_name.to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(match_value)),
            }],
            result: Expression::ColumnRef { table: None, column: result_col.to_string() },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    }
}

#[test]
fn test_compile_when_equals_then_column() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN sales_price ELSE NULL END
    let expr = create_simple_case_expression(
        "day_name",
        SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
        "sales_price",
    );

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_some(), "Should compile simple CASE WHEN col = literal THEN col");

    match compiled.unwrap() {
        CompiledCaseExpression::WhenEqualsThenColumn {
            condition_col_idx,
            condition_value,
            result_col_idx,
        } => {
            assert_eq!(condition_col_idx, 0, "day_name is at index 0");
            assert_eq!(condition_value, SqlValue::Varchar(arcstr::ArcStr::from("Sunday")));
            assert_eq!(result_col_idx, 1, "sales_price is at index 1");
        }
        _ => panic!("Expected WhenEqualsThenColumn variant"),
    }
}

#[test]
fn test_compile_when_equals_then_literal() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN 1 ELSE NULL END
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
            }],
            result: Expression::Literal(SqlValue::Integer(1)),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_some(), "Should compile CASE WHEN col = literal THEN literal");

    match compiled.unwrap() {
        CompiledCaseExpression::WhenEqualsThenLiteral {
            condition_col_idx,
            condition_value,
            result_value,
        } => {
            assert_eq!(condition_col_idx, 0);
            assert_eq!(condition_value, SqlValue::Varchar(arcstr::ArcStr::from("Sunday")));
            assert_eq!(result_value, SqlValue::Integer(1));
        }
        _ => panic!("Expected WhenEqualsThenLiteral variant"),
    }
}

#[test]
fn test_evaluate_when_condition_matches() {
    let schema = create_test_schema();
    let expr = create_simple_case_expression(
        "day_name",
        SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
        "sales_price",
    );

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema).unwrap();

    // Row where day_name = 'Sunday', sales_price = 9999
    let row = Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
        SqlValue::Integer(9999),
        SqlValue::Integer(10),
    ]);

    let result = compiled.evaluate(&row);
    assert_eq!(result, SqlValue::Integer(9999));
}

#[test]
fn test_evaluate_when_condition_does_not_match() {
    let schema = create_test_schema();
    let expr = create_simple_case_expression(
        "day_name",
        SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
        "sales_price",
    );

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema).unwrap();

    // Row where day_name = 'Monday' (not Sunday)
    let row = Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("Monday")),
        SqlValue::Integer(9999),
        SqlValue::Integer(10),
    ]);

    let result = compiled.evaluate(&row);
    assert_eq!(result, SqlValue::Null, "Should return NULL when condition doesn't match");
}

#[test]
fn test_evaluate_literal_result() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN 1 ELSE NULL END
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
            }],
            result: Expression::Literal(SqlValue::Integer(1)),
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema).unwrap();

    // Row where day_name = 'Sunday'
    let row_match = Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("Sunday")),
        SqlValue::Null,
        SqlValue::Integer(10),
    ]);
    assert_eq!(compiled.evaluate(&row_match), SqlValue::Integer(1));

    // Row where day_name = 'Monday'
    let row_no_match = Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("Monday")),
        SqlValue::Null,
        SqlValue::Integer(10),
    ]);
    assert_eq!(compiled.evaluate(&row_no_match), SqlValue::Null);
}

#[test]
fn test_does_not_compile_simple_case_with_operand() {
    let schema = create_test_schema();

    // Simple CASE day_name WHEN 'Sunday' THEN sales_price END (has operand)
    let expr = Expression::Case {
        operand: Some(Box::new(Expression::ColumnRef {
            table: None,
            column: "day_name".to_string(),
        })),
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))],
            result: Expression::ColumnRef { table: None, column: "sales_price".to_string() },
        }],
        else_result: None,
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile simple CASE (with operand)");
}

#[test]
fn test_does_not_compile_multiple_when_clauses() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN 1 WHEN day_name = 'Monday' THEN 2 ELSE 0 END
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![
            CaseWhen {
                conditions: vec![Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "day_name".to_string(),
                    }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
                }],
                result: Expression::Literal(SqlValue::Integer(1)),
            },
            CaseWhen {
                conditions: vec![Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "day_name".to_string(),
                    }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Monday")))),
                }],
                result: Expression::Literal(SqlValue::Integer(2)),
            },
        ],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile CASE with multiple WHEN clauses");
}

#[test]
fn test_does_not_compile_non_null_else() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN sales_price ELSE 0 END (non-null ELSE)
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
            }],
            result: Expression::ColumnRef { table: None, column: "sales_price".to_string() },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile CASE with non-null ELSE");
}

#[test]
fn test_does_not_compile_non_equality_condition() {
    let schema = create_test_schema();

    // CASE WHEN quantity > 5 THEN sales_price ELSE NULL END
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "quantity".to_string(),
                }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }],
            result: Expression::ColumnRef { table: None, column: "sales_price".to_string() },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile CASE with non-equality condition");
}

#[test]
fn test_does_not_compile_complex_result() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN sales_price * 2 ELSE NULL END
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
            }],
            result: Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "sales_price".to_string(),
                }),
                op: BinaryOperator::Multiply,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile CASE with complex result expression");
}

#[test]
fn test_compiles_with_absent_else() {
    let schema = create_test_schema();

    // CASE WHEN day_name = 'Sunday' THEN sales_price END (no ELSE clause)
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
            }],
            result: Expression::ColumnRef { table: None, column: "sales_price".to_string() },
        }],
        else_result: None,
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_some(), "Should compile CASE without ELSE clause (implicit NULL)");
}

#[test]
fn test_literal_on_left_side_of_equality() {
    let schema = create_test_schema();

    // CASE WHEN 'Sunday' = day_name THEN sales_price ELSE NULL END (literal on left)
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Sunday")))),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
            }],
            result: Expression::ColumnRef { table: None, column: "sales_price".to_string() },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_some(), "Should compile when literal is on left side of equality");
}

#[test]
fn test_does_not_compile_non_case_expression() {
    let schema = create_test_schema();

    // Plain column reference - not a CASE expression
    let expr = Expression::ColumnRef { table: None, column: "day_name".to_string() };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile non-CASE expressions");
}

#[test]
fn test_does_not_compile_column_to_column_comparison() {
    let schema = create_test_schema();

    // CASE WHEN day_name = sales_price THEN quantity ELSE NULL END (column = column)
    let expr = Expression::Case {
        operand: None,
        when_clauses: vec![CaseWhen {
            conditions: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "day_name".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "sales_price".to_string(),
                }),
            }],
            result: Expression::ColumnRef { table: None, column: "quantity".to_string() },
        }],
        else_result: Some(Box::new(Expression::Literal(SqlValue::Null))),
    };

    let compiled = CompiledCaseExpression::try_compile(&expr, &schema);
    assert!(compiled.is_none(), "Should not compile column = column comparisons");
}
