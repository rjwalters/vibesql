//! Expression analysis for SIMD eligibility
//!
//! This module determines whether expressions can benefit from SIMD evaluation
//! by analyzing their structure, operand types, and complexity.

use vibesql_ast::{BinaryOperator, Expression};

use super::null_handling::has_null_values;
use super::SIMD_THRESHOLD;

/// Check if an expression can benefit from SIMD evaluation
///
/// Returns true if:
/// - Row count >= SIMD_THRESHOLD (enough rows to amortize conversion overhead)
/// - Expression is simple binary arithmetic (+, -, *, /)
/// - Operands are column references or literals (no complex sub-expressions)
/// - No subqueries, aggregates, or other complex operations
/// - No NULL values present (graceful fallback to scalar evaluation)
#[cfg(feature = "simd")]
pub fn can_use_simd_for_expression(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> bool {
    // Must have enough rows to amortize conversion overhead
    if rows.len() < SIMD_THRESHOLD {
        return false;
    }

    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check if operator is SIMD-compatible
            let is_simd_op = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            );

            if !is_simd_op {
                return false;
            }

            // Check if operands are simple (column refs or literals)
            if !is_simple_operand(left) || !is_simple_operand(right) {
                return false;
            }

            // Check for NULL values - if present, fall back to scalar evaluation
            // This enables true graceful fallback instead of throwing errors
            !has_null_values(expr, rows, evaluator)
        }
        _ => false,
    }
}

/// Check if an expression is a simple operand (column reference or literal)
#[cfg(feature = "simd")]
pub fn is_simple_operand(expr: &Expression) -> bool {
    match expr {
        // Column references are simple
        Expression::ColumnRef { .. } => true,

        // Literals are simple
        Expression::Literal(_) => true,

        // Nested binary ops are simple if their operands are simple
        Expression::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) && is_simple_operand(left)
                && is_simple_operand(right)
        }

        // Everything else is complex (subqueries, aggregates, functions, etc.)
        _ => false,
    }
}

/// Extract all column references from an expression tree
#[cfg(feature = "simd")]
pub fn extract_column_refs(expr: &Expression) -> Vec<Expression> {
    let mut columns = Vec::new();
    match expr {
        Expression::ColumnRef { .. } => {
            columns.push(expr.clone());
        }
        Expression::BinaryOp { left, right, .. } => {
            columns.extend(extract_column_refs(left));
            columns.extend(extract_column_refs(right));
        }
        // Other expression types (literals, etc.) don't contain column refs
        _ => {}
    }
    columns
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    // Helper to create a mock evaluator for testing
    fn create_test_evaluator() -> crate::evaluator::CombinedExpressionEvaluator<'static> {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        // Create a minimal schema for testing
        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Bigint, false),
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
            ColumnSchema::new("c".to_string(), DataType::Bigint, false),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema = Box::leak(Box::new(CombinedSchema::from_table(
            "test".to_string(),
            table_schema,
        )));
        crate::evaluator::CombinedExpressionEvaluator::new(schema)
    }

    // Helper to create test rows with numeric values
    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Bigint(i as i64),
                    SqlValue::Bigint((i * 2) as i64),
                    SqlValue::Bigint((i * 3) as i64),
                ])
            })
            .collect()
    }

    // ===== Expression Detection Tests =====

    #[test]
    fn test_can_use_simd_returns_false_for_small_row_count() {
        let evaluator = create_test_evaluator();
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };

        // Below threshold
        let rows_99 = create_test_rows(99);
        let rows_50 = create_test_rows(50);
        let rows_0 = create_test_rows(0);
        assert!(!can_use_simd_for_expression(&expr, &rows_99, &evaluator));
        assert!(!can_use_simd_for_expression(&expr, &rows_50, &evaluator));
        assert!(!can_use_simd_for_expression(&expr, &rows_0, &evaluator));

        // At threshold
        let rows_100 = create_test_rows(100);
        assert!(can_use_simd_for_expression(&expr, &rows_100, &evaluator));

        // Above threshold
        let rows_101 = create_test_rows(101);
        let rows_1000 = create_test_rows(1000);
        assert!(can_use_simd_for_expression(&expr, &rows_101, &evaluator));
        assert!(can_use_simd_for_expression(&expr, &rows_1000, &evaluator));
    }

    #[test]
    fn test_can_use_simd_returns_true_for_simple_arithmetic() {
        let evaluator = create_test_evaluator();
        let rows = create_test_rows(100);

        // Test each arithmetic operator
        let operators = vec![
            BinaryOperator::Plus,
            BinaryOperator::Minus,
            BinaryOperator::Multiply,
            BinaryOperator::Divide,
        ];

        for op in operators {
            let expr = Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            };

            assert!(
                can_use_simd_for_expression(&expr, &rows, &evaluator),
                "Should support SIMD for operator: {:?}",
                op
            );
        }
    }

    #[test]
    fn test_can_use_simd_returns_false_for_unsupported_operators() {
        let evaluator = create_test_evaluator();
        let rows = create_test_rows(100);

        let unsupported_ops = vec![
            BinaryOperator::Modulo,
            BinaryOperator::Concat,
            BinaryOperator::And,
            BinaryOperator::Or,
            BinaryOperator::Equal,
            BinaryOperator::NotEqual,
            BinaryOperator::LessThan,
            BinaryOperator::LessThanOrEqual,
            BinaryOperator::GreaterThan,
            BinaryOperator::GreaterThanOrEqual,
        ];

        for op in unsupported_ops {
            let expr = Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op,
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            };

            assert!(
                !can_use_simd_for_expression(&expr, &rows, &evaluator),
                "Should NOT support SIMD for operator: {:?}",
                op
            );
        }
    }

    #[test]
    fn test_can_use_simd_returns_false_for_complex_operands() {
        let evaluator = create_test_evaluator();
        let rows = create_test_rows(100);

        // ScalarSubquery
        let expr_subquery = Expression::BinaryOp {
            left: Box::new(Expression::ScalarSubquery(Box::new(
                vibesql_ast::SelectStmt {
                    with_clause: None,
                    distinct: false,
                    select_list: vec![],
                    into_table: None,
                    into_variables: None,
                    from: None,
                    where_clause: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                    set_operation: None,
                },
            ))),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        assert!(!can_use_simd_for_expression(&expr_subquery, &rows, &evaluator));

        // AggregateFunction
        let expr_aggregate = Expression::BinaryOp {
            left: Box::new(Expression::AggregateFunction {
                name: "SUM".to_string(),
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "x".to_string(),
                }],
                distinct: false,
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        assert!(!can_use_simd_for_expression(&expr_aggregate, &rows, &evaluator));

        // Function
        let expr_function = Expression::BinaryOp {
            left: Box::new(Expression::Function {
                name: "ABS".to_string(),
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "x".to_string(),
                }],
                character_unit: None,
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        assert!(!can_use_simd_for_expression(&expr_function, &rows, &evaluator));

        // CASE
        let expr_case = Expression::BinaryOp {
            left: Box::new(Expression::Case {
                operand: None,
                when_clauses: vec![],
                else_result: None,
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        assert!(!can_use_simd_for_expression(&expr_case, &rows, &evaluator));
    }

    #[test]
    fn test_can_use_simd_returns_true_for_nested_binary_operations() {
        let evaluator = create_test_evaluator();
        let rows = create_test_rows(100);

        // (a + b) * c
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "a".to_string(),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "b".to_string(),
                }),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "c".to_string(),
            }),
        };

        assert!(can_use_simd_for_expression(&expr, &rows, &evaluator));
    }

    // ===== Operand Simplicity Tests =====

    #[test]
    fn test_is_simple_operand_for_column_refs() {
        let expr = Expression::ColumnRef {
            table: None,
            column: "x".to_string(),
        };
        assert!(is_simple_operand(&expr));

        let expr_qualified = Expression::ColumnRef {
            table: Some("t".to_string()),
            column: "x".to_string(),
        };
        assert!(is_simple_operand(&expr_qualified));
    }

    #[test]
    fn test_is_simple_operand_for_literals() {
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Integer(
            42
        ))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Double(
            3.14
        ))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Varchar(
            "test".to_string()
        ))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Boolean(
            true
        ))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Null)));
    }

    #[test]
    fn test_is_simple_operand_for_nested_arithmetic() {
        // a + b is simple
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "b".to_string(),
            }),
        };
        assert!(is_simple_operand(&expr));

        // (a + b) * 2 is simple
        let nested = Expression::BinaryOp {
            left: Box::new(expr),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };
        assert!(is_simple_operand(&nested));
    }

    #[test]
    fn test_is_simple_operand_returns_false_for_complex_expressions() {
        // ScalarSubquery is not simple
        let subquery = Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));
        assert!(!is_simple_operand(&subquery));

        // AggregateFunction is not simple
        let aggregate = Expression::AggregateFunction {
            name: "SUM".to_string(),
            args: vec![],
            distinct: false,
        };
        assert!(!is_simple_operand(&aggregate));

        // Function is not simple
        let function = Expression::Function {
            name: "ABS".to_string(),
            args: vec![],
            character_unit: None,
        };
        assert!(!is_simple_operand(&function));

        // CASE is not simple
        let case_expr = Expression::Case {
            operand: None,
            when_clauses: vec![],
            else_result: None,
        };
        assert!(!is_simple_operand(&case_expr));
    }
}
