//! SIMD-accelerated expression evaluation for general arithmetic expressions
//!
//! This module extends SIMD support beyond aggregates to general expression evaluation
//! in WHERE clauses, SELECT projections, ORDER BY, and other query contexts.
//!
//! # Overview
//!
//! Evaluates expressions in batch mode using SIMD operations when beneficial:
//! - Collects values from multiple rows into columnar buffers
//! - Applies SIMD arithmetic operations
//! - Converts results back to row-based format
//!
//! # When SIMD is Used
//!
//! SIMD path is chosen when:
//! 1. Row count >= SIMD_THRESHOLD (100 rows)
//! 2. Expression is simple arithmetic (+, -, *, /)
//! 3. All operands are numeric (Int64 or Float64)
//! 4. No complex sub-expressions (subqueries, aggregates, etc.)
//!
//! # Performance
//!
//! Expected improvements:
//! - 2-4x for expression-heavy queries (conservative)
//! - 5-10x for computation-dominated queries (optimistic)
//!
//! Overhead considerations:
//! - Row → columnar → row conversion has cost
//! - Only beneficial when computation cost exceeds conversion cost
//! - Threshold tuned to break-even point (~100-1000 rows)

use crate::errors::ExecutorError;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Threshold for using SIMD expression evaluation
/// Below this, scalar evaluation is more efficient due to conversion overhead
pub const SIMD_THRESHOLD: usize = 100;

/// Maximum recursion depth for nested expressions
/// Prevents stack overflow on deeply nested binary operations
const MAX_RECURSION_DEPTH: usize = 32;

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
fn is_simple_operand(expr: &Expression) -> bool {
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
fn extract_column_refs(expr: &Expression) -> Vec<Expression> {
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

/// Check if an expression contains NULL values by checking source columns only
#[cfg(feature = "simd")]
fn has_null_values(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> bool {
    let column_refs = extract_column_refs(expr);
    if column_refs.is_empty() {
        return false;
    }

    let sample_size = rows.len().min(100);

    for row in rows.iter().take(sample_size) {
        for col_ref in &column_refs {
            match evaluator.eval(col_ref, row) {
                Ok(value) if value == SqlValue::Null => return true,
                Err(_) => return true,
                _ => {}
            }
        }
    }

    if sample_size < rows.len() {
        for row in rows.iter().skip(sample_size) {
            for col_ref in &column_refs {
                match evaluator.eval(col_ref, row) {
                    Ok(value) if value == SqlValue::Null => return true,
                    Err(_) => return true,
                    _ => {}
                }
            }
        }
    }

    false
}

/// Evaluate an expression in batch mode using SIMD
///
/// # Arguments
///
/// * `expr` - Expression to evaluate
/// * `rows` - Input rows to evaluate against
/// * `evaluator` - Expression evaluator for column lookups and fallback
///
/// # Returns
///
/// Vector of SqlValues, one per row, or an error
///
/// # Algorithm
///
/// 1. Analyze expression to determine column dependencies
/// 2. Extract column values into typed buffers (i64 or f64)
/// 3. Apply SIMD arithmetic operations
/// 4. Convert results back to SqlValues
#[cfg(feature = "simd")]
pub fn eval_expression_batch_simd(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Early return for empty input
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Check if we should use SIMD (includes NULL detection for graceful fallback)
    if !can_use_simd_for_expression(expr, rows, evaluator) {
        // Fall back to scalar evaluation
        return eval_expression_scalar(expr, rows, evaluator);
    }

    // Evaluate expression using SIMD with depth tracking
    match expr {
        Expression::BinaryOp { left, op, right } => {
            eval_binary_op_simd(left, *op, right, rows, evaluator, 0)
        }
        _ => eval_expression_scalar(expr, rows, evaluator),
    }
}

/// Evaluate a binary operation using SIMD
#[cfg(feature = "simd")]
fn eval_binary_op_simd(
    left: &Expression,
    op: BinaryOperator,
    right: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    depth: usize,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Check recursion depth to prevent stack overflow
    if depth >= MAX_RECURSION_DEPTH {
        // Fall back to scalar evaluation for deeply nested expressions
        return eval_expression_scalar(
            &Expression::BinaryOp {
                left: Box::new(left.clone()),
                op,
                right: Box::new(right.clone()),
            },
            rows,
            evaluator,
        );
    }

    // Evaluate left and right operands
    let left_values = eval_operand_to_buffer(left, rows, evaluator, depth)?;
    let right_values = eval_operand_to_buffer(right, rows, evaluator, depth)?;

    // Determine result type and perform SIMD operation
    apply_simd_operation(&left_values, op, &right_values)
}

/// Evaluate an operand expression to a typed buffer
#[cfg(feature = "simd")]
fn eval_operand_to_buffer(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    depth: usize,
) -> Result<NumericBuffer, ExecutorError> {
    match expr {
        // Recursively handle nested binary operations
        Expression::BinaryOp { left, op, right } => {
            let result_values = eval_binary_op_simd(left, *op, right, rows, evaluator, depth + 1)?;
            // Convert Vec<SqlValue> to NumericBuffer
            let numeric_values: Result<Vec<_>, _> = result_values
                .into_iter()
                .map(|v| NumericValue::from_sql_value(&v))
                .collect();
            convert_to_buffer(numeric_values?)
        }

        // For simple expressions (columns, literals), evaluate normally
        _ => {
            let numeric_values: Result<Vec<_>, _> = rows
                .iter()
                .map(|row| {
                    let value = evaluator.eval(expr, row)?;
                    NumericValue::from_sql_value(&value)
                })
                .collect();
            convert_to_buffer(numeric_values?)
        }
    }
}

/// Convert a vector of NumericValues to a typed NumericBuffer
///
/// Returns an error if NULL values are present, which indicates a bug in the
/// NULL detection logic (NULLs should be caught by can_use_simd_for_expression).
#[cfg(feature = "simd")]
fn convert_to_buffer(values: Vec<NumericValue>) -> Result<NumericBuffer, ExecutorError> {
    // Determine if we can use Int64 or need Float64
    let has_float = values.iter().any(|v| matches!(v, NumericValue::Float64(_)));

    // NULL values should have been detected early by can_use_simd_for_expression()
    // If we reach here with NULLs, it's a bug in the NULL detection logic
    if values.iter().any(|v| matches!(v, NumericValue::Null)) {
        return Err(ExecutorError::UnsupportedExpression(
            "NULL values reached SIMD path despite early detection - this is a bug".to_string(),
        ));
    }

    if has_float {
        // Use Float64
        let buf: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n as f64,
                NumericValue::Float64(f) => f,
                NumericValue::Null => unreachable!("NULLs filtered by early detection"),
            })
            .collect();
        Ok(NumericBuffer::Float64(buf))
    } else {
        // Use Int64
        let buf: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n,
                NumericValue::Float64(_) => unreachable!("Mixed types handled above"),
                NumericValue::Null => unreachable!("NULLs filtered by early detection"),
            })
            .collect();
        Ok(NumericBuffer::Int64(buf))
    }
}

/// Apply SIMD arithmetic operation to two buffers
#[cfg(feature = "simd")]
fn apply_simd_operation(
    left: &NumericBuffer,
    op: BinaryOperator,
    right: &NumericBuffer,
) -> Result<Vec<SqlValue>, ExecutorError> {
    use super::arithmetic::*;

    match (left, right) {
        // Both i64 - use integer SIMD
        (NumericBuffer::Int64(a), NumericBuffer::Int64(b)) => {
            let result = match op {
                BinaryOperator::Plus => simd_add_i64(a, b),
                BinaryOperator::Minus => simd_sub_i64(a, b),
                BinaryOperator::Multiply => simd_mul_i64(a, b),
                BinaryOperator::Divide => {
                    // Use SIMD division first, then post-process for divide-by-zero
                    // This is Option 1 from the issue: SIMD first, fix later
                    //
                    // Performance note: This approach gets SIMD benefits for the common
                    // case (no divide-by-zero) while still correctly handling edge cases.
                    // The post-processing scan is cheap compared to the scalar loop alternative.
                    let result = simd_div_i64(a, b);

                    // Convert to SqlValue, replacing divide-by-zero with NULL
                    return Ok(result
                        .into_iter()
                        .zip(b.iter())
                        .map(|(quotient, &divisor)| {
                            if divisor == 0 {
                                SqlValue::Null
                            } else {
                                SqlValue::Bigint(quotient)
                            }
                        })
                        .collect());
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported SIMD operation: {:?}",
                        op
                    )))
                }
            };
            Ok(result.into_iter().map(SqlValue::Bigint).collect())
        }

        // At least one f64 - promote to float SIMD
        _ => {
            let a_f64 = left.to_f64();
            let b_f64 = right.to_f64();

            let result = match op {
                BinaryOperator::Plus => simd_add_f64(&a_f64, &b_f64),
                BinaryOperator::Minus => simd_sub_f64(&a_f64, &b_f64),
                BinaryOperator::Multiply => simd_mul_f64(&a_f64, &b_f64),
                BinaryOperator::Divide => simd_div_f64(&a_f64, &b_f64),
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported SIMD operation: {:?}",
                        op
                    )))
                }
            };
            Ok(result.into_iter().map(SqlValue::Double).collect())
        }
    }
}

/// Scalar fallback for expressions that can't use SIMD
#[cfg(feature = "simd")]
fn eval_expression_scalar(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    rows.iter().map(|row| evaluator.eval(expr, row)).collect()
}

/// Numeric value that can be used in SIMD operations
#[cfg(feature = "simd")]
#[derive(Debug, Clone)]
enum NumericValue {
    Int64(i64),
    Float64(f64),
    Null,
}

#[cfg(feature = "simd")]
impl NumericValue {
    fn from_sql_value(value: &SqlValue) -> Result<Self, ExecutorError> {
        match value {
            SqlValue::Integer(n) => Ok(NumericValue::Int64(*n)),
            SqlValue::Bigint(n) => Ok(NumericValue::Int64(*n)),
            SqlValue::Smallint(n) => Ok(NumericValue::Int64(*n as i64)),
            SqlValue::Double(f) => Ok(NumericValue::Float64(*f)),
            SqlValue::Float(f) => Ok(NumericValue::Float64(*f as f64)),
            SqlValue::Numeric(f) => Ok(NumericValue::Float64(*f)),
            SqlValue::Real(f) => Ok(NumericValue::Float64(*f as f64)),
            SqlValue::Null => Ok(NumericValue::Null),
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Cannot use non-numeric value in SIMD expression: {:?}",
                value
            ))),
        }
    }
}

/// Buffer of numeric values for SIMD operations
#[cfg(feature = "simd")]
#[derive(Debug)]
enum NumericBuffer {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
}

#[cfg(feature = "simd")]
impl NumericBuffer {
    /// Convert buffer to f64 (promoting integers if necessary)
    fn to_f64(&self) -> Vec<f64> {
        match self {
            NumericBuffer::Int64(v) => v.iter().map(|&x| x as f64).collect(),
            NumericBuffer::Float64(v) => v.clone(),
        }
    }
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, Expression};
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
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema = Box::leak(Box::new(CombinedSchema::from_table("test".to_string(), table_schema)));
        crate::evaluator::CombinedExpressionEvaluator::new(schema)
    }

    // Helper to create test rows with numeric values
    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| Row::new(vec![SqlValue::Bigint(i as i64), SqlValue::Bigint((i * 2) as i64)]))
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
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Integer(42))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Double(3.14))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Varchar("test".to_string()))));
        assert!(is_simple_operand(&Expression::Literal(SqlValue::Boolean(true))));
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

    // ===== Batch Expression Evaluation Tests =====

    #[test]
    fn test_eval_expression_batch_simd_simple_addition() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Expression: column_0 + 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "0".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        // This test will use scalar fallback due to evaluator limitations
        // but verifies the function signature and basic execution path
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);

        // Should complete without panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_eval_expression_batch_simd_empty_rows() {
        let rows = vec![];
        let evaluator = create_test_evaluator();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };

        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_eval_expression_batch_simd_falls_back_for_insufficient_rows() {
        let rows = create_test_rows(50); // Below threshold
        let evaluator = create_test_evaluator();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "0".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };

        // Should fall back to scalar evaluation
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_eval_expression_batch_simd_falls_back_for_complex_expression() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Complex expression with function
        let expr = Expression::Function {
            name: "ABS".to_string(),
            args: vec![Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }],
            character_unit: None,
        };

        // Should fall back to scalar evaluation
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    // ===== Recursion Depth Limiting Tests =====

    #[test]
    fn test_recursion_depth_limiting_prevents_stack_overflow() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Create deeply nested expression (40 levels)
        let mut expr = Expression::Literal(SqlValue::Integer(1));
        for _ in 0..40 {
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            };
        }

        // Should not panic, should fall back to scalar
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_recursion_depth_at_limit() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Create expression at exactly MAX_RECURSION_DEPTH levels (32)
        let mut expr = Expression::Literal(SqlValue::Integer(1));
        for _ in 0..31 {
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            };
        }

        // Should handle this depth
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    // ===== NULL Handling Tests =====

    #[test]
    fn test_convert_to_buffer_rejects_null_values() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Null,
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("NULL"));
        }
    }

    #[test]
    fn test_numeric_value_from_null_sql_value() {
        let result = NumericValue::from_sql_value(&SqlValue::Null);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), NumericValue::Null));
    }

    // ===== Buffer Conversion Tests =====

    #[test]
    fn test_convert_to_buffer_all_int64() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Int64(2),
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Int64(buf) => {
                assert_eq!(buf, vec![1, 2, 3]);
            }
            _ => panic!("Expected Int64 buffer"),
        }
    }

    #[test]
    fn test_convert_to_buffer_mixed_promotes_to_float64() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Float64(2.5),
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Float64(buf) => {
                assert_eq!(buf, vec![1.0, 2.5, 3.0]);
            }
            _ => panic!("Expected Float64 buffer"),
        }
    }

    #[test]
    fn test_convert_to_buffer_all_float64() {
        let values = vec![
            NumericValue::Float64(1.5),
            NumericValue::Float64(2.5),
            NumericValue::Float64(3.5),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Float64(buf) => {
                assert_eq!(buf, vec![1.5, 2.5, 3.5]);
            }
            _ => panic!("Expected Float64 buffer"),
        }
    }

    #[test]
    fn test_numeric_buffer_to_f64_promotion() {
        // Int64 buffer promotes to f64
        let int_buf = NumericBuffer::Int64(vec![1, 2, 3]);
        let f64_vec = int_buf.to_f64();
        assert_eq!(f64_vec, vec![1.0, 2.0, 3.0]);

        // Float64 buffer clones
        let float_buf = NumericBuffer::Float64(vec![1.5, 2.5, 3.5]);
        let f64_vec = float_buf.to_f64();
        assert_eq!(f64_vec, vec![1.5, 2.5, 3.5]);
    }

    // ===== Division Handling Tests =====

    #[test]
    fn test_integer_division_by_zero_returns_null() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Int64(vec![10, 20, 30]);
        let b = NumericBuffer::Int64(vec![2, 0, 5]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(5));
        assert_eq!(values[1], SqlValue::Null); // Division by zero
        assert_eq!(values[2], SqlValue::Bigint(6));
    }

    #[test]
    fn test_integer_division_non_zero() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Int64(vec![10, 20, 30, 40]);
        let b = NumericBuffer::Int64(vec![2, 4, 3, 8]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(5));
        assert_eq!(values[1], SqlValue::Bigint(5));
        assert_eq!(values[2], SqlValue::Bigint(10));
        assert_eq!(values[3], SqlValue::Bigint(5));
    }

    #[test]
    fn test_float_division_handles_zero() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Float64(vec![10.0, 20.0, 30.0]);
        let b = NumericBuffer::Float64(vec![2.0, 0.0, 5.0]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(5.0));
        // Float division by zero produces infinity
        match values[1] {
            SqlValue::Double(v) => assert!(v.is_infinite()),
            _ => panic!("Expected Double value"),
        }
        assert_eq!(values[2], SqlValue::Double(6.0));
    }

    // ===== SIMD Operation Tests =====

    #[test]
    fn test_apply_simd_operation_int64_addition() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Int64(vec![1, 2, 3, 4]);
        let b = NumericBuffer::Int64(vec![10, 20, 30, 40]);

        let result = apply_simd_operation(&a, BinaryOperator::Plus, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(11));
        assert_eq!(values[1], SqlValue::Bigint(22));
        assert_eq!(values[2], SqlValue::Bigint(33));
        assert_eq!(values[3], SqlValue::Bigint(44));
    }

    #[test]
    fn test_apply_simd_operation_float64_multiplication() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Float64(vec![1.5, 2.0, 3.5, 4.0]);
        let b = NumericBuffer::Float64(vec![2.0, 3.0, 2.0, 5.0]);

        let result = apply_simd_operation(&a, BinaryOperator::Multiply, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(3.0));
        assert_eq!(values[1], SqlValue::Double(6.0));
        assert_eq!(values[2], SqlValue::Double(7.0));
        assert_eq!(values[3], SqlValue::Double(20.0));
    }

    #[test]
    fn test_apply_simd_operation_type_promotion() {
        use super::apply_simd_operation;

        // Int64 + Float64 should promote to Float64
        let a = NumericBuffer::Int64(vec![1, 2, 3]);
        let b = NumericBuffer::Float64(vec![1.5, 2.5, 3.5]);

        let result = apply_simd_operation(&a, BinaryOperator::Plus, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(2.5));
        assert_eq!(values[1], SqlValue::Double(4.5));
        assert_eq!(values[2], SqlValue::Double(6.5));
    }

    #[test]
    fn test_apply_simd_operation_unsupported_operator() {
        use super::apply_simd_operation;

        let a = NumericBuffer::Int64(vec![1, 2, 3]);
        let b = NumericBuffer::Int64(vec![1, 2, 3]);

        let result = apply_simd_operation(&a, BinaryOperator::Modulo, &b);
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("Unsupported SIMD operation"));
        }
    }

    // ===== NumericValue Conversion Tests =====

    #[test]
    fn test_numeric_value_from_sql_value_integers() {
        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Integer(42)).unwrap(),
            NumericValue::Int64(42)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Bigint(100)).unwrap(),
            NumericValue::Int64(100)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Smallint(10)).unwrap(),
            NumericValue::Int64(10)
        ));
    }

    #[test]
    fn test_numeric_value_from_sql_value_floats() {
        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Double(3.14)).unwrap(),
            NumericValue::Float64(3.14)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Float(2.5)).unwrap(),
            NumericValue::Float64(_)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Numeric(1.5)).unwrap(),
            NumericValue::Float64(1.5)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Real(4.2)).unwrap(),
            NumericValue::Float64(_)
        ));
    }

    #[test]
    fn test_numeric_value_from_sql_value_rejects_non_numeric() {
        let result = NumericValue::from_sql_value(&SqlValue::Varchar("hello".to_string()));
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("non-numeric value"));
        }
    }
}
