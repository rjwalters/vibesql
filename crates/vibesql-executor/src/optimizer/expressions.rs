//! Expression optimization logic for constant folding and dead code elimination

use vibesql_ast::{BinaryOperator, CaseWhen, Expression};
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::{casting::cast_value, CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Reverse a comparison operator for predicate normalization
///
/// When we swap operands (e.g., `5 > x` -> `x < 5`), we need to reverse the operator.
fn reverse_comparison_op(op: &BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::LessThan => Some(BinaryOperator::GreaterThan),
        BinaryOperator::LessThanOrEqual => Some(BinaryOperator::GreaterThanOrEqual),
        BinaryOperator::GreaterThan => Some(BinaryOperator::LessThan),
        BinaryOperator::GreaterThanOrEqual => Some(BinaryOperator::LessThanOrEqual),
        BinaryOperator::Equal => Some(BinaryOperator::Equal),
        BinaryOperator::NotEqual => Some(BinaryOperator::NotEqual),
        _ => None, // Not a comparison operator
    }
}

/// Check if an expression is a constant (literal or evaluatable at plan time)
///
/// This includes literals, casts of literals, and simple arithmetic on literals.
fn is_constant_expr(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(_) => true,
        Expression::UnaryOp { expr: inner, .. } => is_constant_expr(inner),
        Expression::BinaryOp { left, right, .. } => {
            is_constant_expr(left) && is_constant_expr(right)
        }
        Expression::Cast { expr: inner, .. } => is_constant_expr(inner),
        _ => false,
    }
}

/// Result of WHERE clause optimization
#[derive(Debug, PartialEq)]
pub enum WhereOptimization {
    /// WHERE clause was optimized to always true - can be removed
    AlwaysTrue,
    /// WHERE clause was optimized to always false - return empty result
    AlwaysFalse,
    /// WHERE clause was optimized but still needs evaluation
    Optimized(Expression),
    /// WHERE clause was not optimized
    Unchanged(Option<Expression>),
}

/// Optimize a SELECT statement's WHERE clause
///
/// Performs constant folding and dead code elimination on WHERE expressions.
/// Returns information about whether the WHERE clause can be eliminated entirely.
pub fn optimize_where_clause(
    where_expr: Option<&Expression>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<WhereOptimization, ExecutorError> {
    match where_expr {
        None => Ok(WhereOptimization::Unchanged(None)),
        Some(expr) => {
            let optimized = optimize_expression(expr, evaluator)?;

            match optimized {
                Expression::Literal(SqlValue::Boolean(true)) => Ok(WhereOptimization::AlwaysTrue),
                Expression::Literal(SqlValue::Boolean(false)) => Ok(WhereOptimization::AlwaysFalse),
                Expression::Literal(SqlValue::Null) => {
                    // WHERE NULL is treated as false
                    Ok(WhereOptimization::AlwaysFalse)
                }
                optimized_expr => {
                    if optimized_expr == *expr {
                        Ok(WhereOptimization::Unchanged(Some(optimized_expr)))
                    } else {
                        Ok(WhereOptimization::Optimized(optimized_expr))
                    }
                }
            }
        }
    }
}

/// Optimize an expression by performing constant folding
///
/// Recursively walks the expression tree and evaluates any subexpressions
/// that don't reference columns (constants).
#[allow(clippy::only_used_in_recursion)]
pub fn optimize_expression(
    expr: &Expression,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Expression, ExecutorError> {
    match expr {
        // Literals are already optimized
        Expression::Literal(_) => Ok(expr.clone()),

        // Column references, pseudo-variables, and session variables cannot be optimized
        Expression::ColumnRef { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => Ok(expr.clone()),

        // Binary operations - try to fold constants and normalize predicates
        Expression::BinaryOp { left, op, right } => {
            let left_opt = optimize_expression(left, evaluator)?;
            let right_opt = optimize_expression(right, evaluator)?;

            // If both sides are literals, evaluate the operation
            if let (Expression::Literal(left_val), Expression::Literal(right_val)) =
                (&left_opt, &right_opt)
            {
                // Use the evaluator's SQL mode for constant folding to ensure correct behavior
                // across different database modes (MySQL, SQLite, etc.)
                let sql_mode = evaluator.database().map(|db| db.sql_mode()).unwrap_or_default();
                match ExpressionEvaluator::eval_binary_op_static(left_val, op, right_val, sql_mode)
                {
                    Ok(result) => return Ok(Expression::Literal(result)),
                    Err(_) => {
                        // If evaluation fails, continue with normalization below
                    }
                }
            }

            // Predicate normalization: Ensure constants are on the right side
            // Transform: `constant op column` -> `column reverse_op constant`
            // This enables better plan caching and index selection
            if let Some(reversed_op) = reverse_comparison_op(op) {
                let left_is_const = is_constant_expr(&left_opt);
                let right_is_const = is_constant_expr(&right_opt);

                // If left is constant and right is not, swap them
                if left_is_const && !right_is_const {
                    return Ok(Expression::BinaryOp {
                        left: Box::new(right_opt),
                        op: reversed_op,
                        right: Box::new(left_opt),
                    });
                }
            }

            Ok(Expression::BinaryOp {
                left: Box::new(left_opt),
                op: *op,
                right: Box::new(right_opt),
            })
        }

        // Unary operations - try to fold if operand is literal
        Expression::UnaryOp { op, expr: inner_expr } => {
            let inner_opt = optimize_expression(inner_expr, evaluator)?;

            // If the operand is a literal, evaluate the unary operation
            if let Expression::Literal(val) = &inner_opt {
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let unary_expr = Expression::UnaryOp {
                    op: *op,
                    expr: Box::new(Expression::Literal(val.clone())),
                };

                match evaluator.eval(&unary_expr, &dummy_row) {
                    Ok(result) => Ok(Expression::Literal(result)),
                    Err(_) => Ok(unary_expr),
                }
            } else {
                Ok(Expression::UnaryOp { op: *op, expr: Box::new(inner_opt) })
            }
        }

        // Function calls - cannot optimize generally
        Expression::Function { .. } => Ok(expr.clone()),

        // Aggregate functions - cannot optimize
        Expression::AggregateFunction { .. } => Ok(expr.clone()),

        // IS NULL/NOT NULL - try to fold if operand is literal
        Expression::IsNull { expr: inner_expr, negated } => {
            let inner_opt = optimize_expression(inner_expr, evaluator)?;

            if let Expression::Literal(val) = &inner_opt {
                let is_null = val.is_null();
                let result = if *negated { !is_null } else { is_null };
                Ok(Expression::Literal(SqlValue::Boolean(result)))
            } else {
                Ok(Expression::IsNull { expr: Box::new(inner_opt), negated: *negated })
            }
        }

        // Wildcard - cannot optimize
        Expression::Wildcard => Ok(expr.clone()),

        // CASE expressions - optimize recursively
        Expression::Case { operand, when_clauses, else_result } => {
            let operand_opt =
                operand.as_ref().map(|op| optimize_expression(op, evaluator)).transpose()?;
            let when_clauses_opt: Result<Vec<CaseWhen>, ExecutorError> = when_clauses
                .iter()
                .map(|wc| {
                    let conditions: Result<Vec<Expression>, ExecutorError> = wc
                        .conditions
                        .iter()
                        .map(|cond| optimize_expression(cond, evaluator))
                        .collect();
                    Ok(CaseWhen {
                        conditions: conditions?,
                        result: optimize_expression(&wc.result, evaluator)?,
                    })
                })
                .collect();
            let else_opt =
                else_result.as_ref().map(|er| optimize_expression(er, evaluator)).transpose()?;

            Ok(Expression::Case {
                operand: operand_opt.map(Box::new),
                when_clauses: when_clauses_opt?,
                else_result: else_opt.map(Box::new),
            })
        }

        // IN with subquery - cannot optimize
        Expression::In { .. } => Ok(expr.clone()),
        Expression::InList { .. } => Ok(expr.clone()),

        // BETWEEN - try to optimize operands and fold if all are literals
        Expression::Between { expr: inner_expr, low, high, negated, symmetric } => {
            let expr_opt = optimize_expression(inner_expr, evaluator)?;
            let low_opt = optimize_expression(low, evaluator)?;
            let high_opt = optimize_expression(high, evaluator)?;

            // If all operands are literals, evaluate at compile time
            if let (
                Expression::Literal(expr_val),
                Expression::Literal(low_val),
                Expression::Literal(high_val),
            ) = (&expr_opt, &low_opt, &high_opt)
            {
                // Use the evaluator's SQL mode for constant folding to ensure correct behavior
                // across different database modes (MySQL, SQLite, etc.)
                let sql_mode = evaluator.database().map(|db| db.sql_mode()).unwrap_or_default();
                match ExpressionEvaluator::eval_between_static(
                    expr_val, low_val, high_val, *negated, *symmetric, sql_mode,
                ) {
                    Ok(result) => return Ok(Expression::Literal(result)),
                    Err(_) => {
                        // If evaluation fails, keep the BETWEEN expression to fail at runtime with proper error
                    }
                }
            }

            Ok(Expression::Between {
                expr: Box::new(expr_opt),
                low: Box::new(low_opt),
                high: Box::new(high_opt),
                negated: *negated,
                symmetric: *symmetric,
            })
        }

        // CAST - try to optimize operand and evaluate if it's a literal
        Expression::Cast { expr: inner_expr, data_type } => {
            let expr_opt = optimize_expression(inner_expr, evaluator)?;

            // If the operand is a literal, we can evaluate the cast at plan time
            if let Expression::Literal(val) = &expr_opt {
                // Get sql_mode from the evaluator's database if available
                let sql_mode = evaluator.database().map(|db| db.sql_mode()).unwrap_or_default();
                match cast_value(val, data_type, &sql_mode) {
                    Ok(result) => Ok(Expression::Literal(result)),
                    Err(_) => {
                        // If cast fails, keep the CAST expression to fail at runtime with proper error
                        Ok(Expression::Cast {
                            expr: Box::new(expr_opt),
                            data_type: data_type.clone(),
                        })
                    }
                }
            } else {
                Ok(Expression::Cast { expr: Box::new(expr_opt), data_type: data_type.clone() })
            }
        }

        // String functions - cannot optimize generally
        Expression::Position { .. } | Expression::Trim { .. } | Expression::Extract { .. } => {
            Ok(expr.clone())
        }

        // LIKE - cannot optimize generally
        Expression::Like { .. } => Ok(expr.clone()),

        // EXISTS - cannot optimize
        Expression::Exists { .. } => Ok(expr.clone()),

        // Quantified comparison - cannot optimize
        Expression::QuantifiedComparison { .. } => Ok(expr.clone()),

        // Current date/time - cannot optimize
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => Ok(expr.clone()),

        // INTERVAL - optimize the value expression
        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            let optimized_value = optimize_expression(value, evaluator)?;
            Ok(Expression::Interval {
                value: Box::new(optimized_value),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            })
        }

        // DEFAULT - cannot optimize
        Expression::Default => Ok(expr.clone()),

        // DuplicateKeyValue - cannot optimize
        Expression::DuplicateKeyValue { .. } => Ok(expr.clone()),

        // Window functions - cannot optimize
        Expression::WindowFunction { .. } => Ok(expr.clone()),

        // NEXT VALUE - cannot optimize
        Expression::NextValue { .. } => Ok(expr.clone()),

        // Scalar subquery - cannot optimize
        Expression::ScalarSubquery(_) => Ok(expr.clone()),

        // MATCH AGAINST - cannot optimize
        Expression::MatchAgainst { .. } => Ok(expr.clone()),

        // Placeholders - cannot optimize (they need to be bound first)
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => Ok(expr.clone()),

        // Conjunction and Disjunction - optimize children
        Expression::Conjunction(children) => {
            let optimized: Result<Vec<_>, _> =
                children.iter().map(|child| optimize_expression(child, evaluator)).collect();
            Ok(Expression::Conjunction(optimized?))
        }

        Expression::Disjunction(children) => {
            let optimized: Result<Vec<_>, _> =
                children.iter().map(|child| optimize_expression(child, evaluator)).collect();
            Ok(Expression::Disjunction(optimized?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::Expression;
    use vibesql_catalog::TableSchema;
    use vibesql_types::{DataType, SqlValue};

    #[test]
    fn test_cast_folding_integer_to_varchar() {
        // CAST(42 AS VARCHAR) should fold to '42'
        let expr = Expression::Cast {
            expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
            data_type: DataType::Varchar { max_length: None },
        };

        // Create a minimal evaluator (we don't need a real one for literal optimization)
        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        // Should be folded to a literal VARCHAR
        match optimized {
            Expression::Literal(SqlValue::Varchar(s)) => assert_eq!(s.as_str(), "42"),
            _ => panic!("Expected folded VARCHAR literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_cast_folding_varchar_to_integer() {
        // CAST('123' AS INTEGER) should fold to 123
        let expr = Expression::Cast {
            expr: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("123")))),
            data_type: DataType::Integer,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Integer(n)) => assert_eq!(n, 123),
            _ => panic!("Expected folded INTEGER literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_cast_folding_preserves_failed_casts() {
        // CAST('abc' AS INTEGER) should fail at runtime, not plan time
        // Keep as CAST expression to get proper error message at runtime
        let expr = Expression::Cast {
            expr: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("abc")))),
            data_type: DataType::Integer,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        // Should keep as CAST expression (not folded due to error)
        match optimized {
            Expression::Cast { .. } => {} // Good, preserved for runtime error
            _ => panic!("Expected CAST to be preserved for failed cast, got {:?}", optimized),
        }
    }

    #[test]
    fn test_cast_non_literal_not_folded() {
        // CAST(column AS INTEGER) should not be folded
        let expr = Expression::Cast {
            expr: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
            data_type: DataType::Integer,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        // Should remain as CAST expression
        match optimized {
            Expression::Cast { .. } => {} // Good, not folded
            _ => panic!("Expected CAST to be preserved for non-literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_cast_null_folding() {
        // CAST(NULL AS INTEGER) should fold to NULL
        let expr = Expression::Cast {
            expr: Box::new(Expression::Literal(SqlValue::Null)),
            data_type: DataType::Integer,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Null) => {} // Good, folded to NULL
            _ => panic!("Expected folded NULL literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_between_constant_folding_true() {
        // 5 BETWEEN 1 AND 10 should fold to TRUE
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(SqlValue::Integer(5))),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::Literal(SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Boolean(true)) => {} // Good, folded to TRUE
            _ => panic!("Expected folded TRUE literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_between_constant_folding_false() {
        // 15 BETWEEN 1 AND 10 should fold to FALSE
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(SqlValue::Integer(15))),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::Literal(SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Boolean(false)) => {} // Good, folded to FALSE
            _ => panic!("Expected folded FALSE literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_between_with_column_not_folded() {
        // x BETWEEN 1 AND 10 should not be folded
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::Literal(SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        // Should remain as BETWEEN expression
        match optimized {
            Expression::Between { .. } => {} // Good, not folded
            _ => panic!("Expected BETWEEN to be preserved for non-literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_not_between_constant_folding() {
        // 15 NOT BETWEEN 1 AND 10 should fold to TRUE
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(SqlValue::Integer(15))),
            low: Box::new(Expression::Literal(SqlValue::Integer(1))),
            high: Box::new(Expression::Literal(SqlValue::Integer(10))),
            negated: true,
            symmetric: false,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Boolean(true)) => {} // Good, folded to TRUE
            _ => panic!("Expected folded TRUE literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_between_symmetric_constant_folding() {
        // 5 BETWEEN SYMMETRIC 10 AND 1 should fold to TRUE (bounds are swapped)
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(SqlValue::Integer(5))),
            low: Box::new(Expression::Literal(SqlValue::Integer(10))),
            high: Box::new(Expression::Literal(SqlValue::Integer(1))),
            negated: false,
            symmetric: true,
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::Literal(SqlValue::Boolean(true)) => {} // Good, folded to TRUE
            _ => panic!("Expected folded TRUE literal, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_greater_than_or_equal() {
        // 9933 >= col0 should normalize to col0 <= 9933
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(9933))),
            op: vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            right: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should be: col0 <= 9933
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "col0")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::LessThanOrEqual);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(9933))));
            }
            _ => panic!("Expected normalized BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_less_than_or_equal() {
        // 8524 <= col3 should normalize to col3 >= 8524
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(8524))),
            op: vibesql_ast::BinaryOperator::LessThanOrEqual,
            right: Box::new(Expression::ColumnRef { table: None, column: "col3".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should be: col3 >= 8524
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "col3")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::GreaterThanOrEqual);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(8524))));
            }
            _ => panic!("Expected normalized BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_already_normalized() {
        // col0 <= 9933 should remain unchanged (already normalized)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "col0".to_string() }),
            op: vibesql_ast::BinaryOperator::LessThanOrEqual,
            right: Box::new(Expression::Literal(SqlValue::Integer(9933))),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should remain: col0 <= 9933
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "col0")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::LessThanOrEqual);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(9933))));
            }
            _ => panic!("Expected unchanged BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_equal() {
        // 100 = x should normalize to x = 100
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(100))),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should be: x = 100
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "x")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::Equal);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(100))));
            }
            _ => panic!("Expected normalized BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_non_comparison_unchanged() {
        // 5 + x should not be normalized (not a comparison operator)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(5))),
            op: vibesql_ast::BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should remain: 5 + x
                assert!(matches!(left.as_ref(), Expression::Literal(SqlValue::Integer(5))));
                assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
                assert!(
                    matches!(right.as_ref(), Expression::ColumnRef { column, .. } if column == "x")
                );
            }
            _ => panic!("Expected unchanged BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_less_than() {
        // 100 < x should normalize to x > 100
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(100))),
            op: vibesql_ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should be: x > 100
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "x")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::GreaterThan);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(100))));
            }
            _ => panic!("Expected normalized BinaryOp, got {:?}", optimized),
        }
    }

    #[test]
    fn test_predicate_normalization_greater_than() {
        // 200 > x should normalize to x < 200
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Integer(200))),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
        };

        let db = vibesql_storage::Database::new();
        let schema = TableSchema::new("test".to_string(), vec![]);
        let combined = crate::schema::CombinedSchema::from_table("test".to_string(), schema);
        let evaluator = CombinedExpressionEvaluator::with_database(&combined, &db);

        let optimized = optimize_expression(&expr, &evaluator).unwrap();

        match optimized {
            Expression::BinaryOp { left, op, right } => {
                // Should be: x < 200
                assert!(
                    matches!(left.as_ref(), Expression::ColumnRef { column, .. } if column == "x")
                );
                assert_eq!(op, vibesql_ast::BinaryOperator::LessThan);
                assert!(matches!(right.as_ref(), Expression::Literal(SqlValue::Integer(200))));
            }
            _ => panic!("Expected normalized BinaryOp, got {:?}", optimized),
        }
    }
}
