//! Expression aggregates - aggregating over expressions rather than simple columns
//!
//! This module handles aggregates over complex expressions like SUM(a * b),
//! where we need to evaluate the expression for each row before aggregating.
//!
//! For large datasets (>= 100 rows), this module automatically uses SIMD-accelerated
//! evaluation via Apache Arrow, providing 4-8x performance improvement.
//!
//! ## Batch-Native Path
//!
//! The `compute_batch_expression_aggregate` function provides SIMD-accelerated
//! expression evaluation directly on `ColumnarBatch` without converting to rows.
//! This is ~20-30% faster than the row-based path for large batches.
//!
//! ## Module Structure
//!
//! - `evaluator` - Simple expression evaluation for single rows
//! - `vectorized` - Vectorized binary operations with optional filtering
//! - `simd` - SIMD-accelerated computation via Arrow
//! - `batch` - Batch-native expression processing on ColumnarBatch

mod batch;
mod evaluator;
mod simd;
mod vectorized;

#[cfg(test)]
mod tests;

use crate::schema::CombinedSchema;
use vibesql_ast::Expression;

use super::{AggregateOp, AggregateSource, AggregateSpec};

// Re-export public API
pub(super) use batch::compute_batch_expression_aggregate;
pub use batch::evaluate_expression_to_column;
pub use batch::evaluate_expression_with_cached_column;
pub(super) use vectorized::compute_expression_aggregate;

/// Extract aggregate operations from AST expressions
///
/// Converts aggregate function expressions to AggregateSpec objects
/// that can be used with columnar execution.
///
/// Currently supports:
/// - SUM(column) → Column aggregate (fast path)
/// - SUM(a * b) → Expression aggregate (evaluates expression per row)
/// - COUNT(*) or COUNT(column) → Column aggregate
/// - AVG(column) or AVG(expr) → Column/Expression aggregate
/// - MIN(column) or MIN(expr) → Column/Expression aggregate
/// - MAX(column) or MAX(expr) → Column/Expression aggregate
///
/// Supported expression types:
/// - Simple column references (fast path)
/// - Binary operations (+, -, *, /) with column references
///
/// Returns None if the expression contains unsupported patterns:
/// - DISTINCT aggregates
/// - Multiple arguments
/// - Complex expressions (subqueries, function calls, etc.)
/// - Non-aggregate expressions
///
/// # Arguments
///
/// * `exprs` - The SELECT list expressions
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(aggregates) if all expressions can be converted to aggregates,
/// None if any expression is too complex for columnar optimization.
pub fn extract_aggregates(
    exprs: &[Expression],
    schema: &CombinedSchema,
) -> Option<Vec<AggregateSpec>> {
    let mut aggregates = Vec::new();

    for expr in exprs.iter() {
        match expr {
            Expression::AggregateFunction { name, distinct, args } => {
                // DISTINCT not supported for columnar optimization
                if *distinct {
                    return None;
                }

                let op = match name.to_uppercase().as_str() {
                    "SUM" => AggregateOp::Sum,
                    "COUNT" => AggregateOp::Count,
                    "AVG" => AggregateOp::Avg,
                    "MIN" => AggregateOp::Min,
                    "MAX" => AggregateOp::Max,
                    _ => return None, // Unsupported aggregate function
                };

                // Handle COUNT(*)
                if op == AggregateOp::Count && args.is_empty() {
                    // For COUNT(*), use column 0 (the column index is ignored by compute_count)
                    aggregates.push(AggregateSpec { op, source: AggregateSource::Column(0) });
                    continue;
                }

                // Handle COUNT(*) with wildcard argument (Expression::Wildcard or ColumnRef { column: "*" })
                if op == AggregateOp::Count && args.len() == 1 {
                    match &args[0] {
                        Expression::Wildcard => {
                            aggregates
                                .push(AggregateSpec { op, source: AggregateSource::CountStar });
                            continue;
                        }
                        Expression::ColumnRef { table: _, column } if column == "*" => {
                            aggregates
                                .push(AggregateSpec { op, source: AggregateSource::CountStar });
                            continue;
                        }
                        _ => {}
                    }
                }

                // Extract source (column or expression) for other aggregates
                if args.len() != 1 {
                    return None; // Multiple arguments not supported
                }

                let source = match &args[0] {
                    // Fast path: simple column reference
                    Expression::ColumnRef { table, column } => {
                        let column_idx = schema.get_column_index(table.as_deref(), column)?;
                        AggregateSource::Column(column_idx)
                    }
                    // New: support binary operations like a * b
                    Expression::BinaryOp { .. } => {
                        // Check if this is a simple binary operation we can handle
                        if is_simple_arithmetic_expr(&args[0], schema).is_some() {
                            AggregateSource::Expression(args[0].clone())
                        } else {
                            return None; // Complex expression not supported
                        }
                    }
                    _ => return None, // Other expression types not supported
                };

                aggregates.push(AggregateSpec { op, source });
            }
            _ => {
                return None; // Non-aggregate expressions not supported
            }
        }
    }

    Some(aggregates)
}

/// Check if an expression is a simple arithmetic expression we can optimize
///
/// Returns Some(()) if the expression only contains column references and
/// arithmetic operations (+, -, *, /), which we can efficiently evaluate.
/// Returns None if the expression contains unsupported operations.
fn is_simple_arithmetic_expr(expr: &Expression, schema: &CombinedSchema) -> Option<()> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Verify column exists
            schema.get_column_index(table.as_deref(), column)?;
            Some(())
        }
        Expression::Literal(_) => Some(()),
        Expression::BinaryOp { left, op, right } => {
            // Only support arithmetic operations
            use vibesql_ast::BinaryOperator::*;
            match op {
                Plus | Minus | Multiply | Divide => {
                    is_simple_arithmetic_expr(left, schema)?;
                    is_simple_arithmetic_expr(right, schema)?;
                    Some(())
                }
                _ => None, // Comparison ops, logical ops, etc. not supported
            }
        }
        _ => None, // Function calls, subqueries, etc. not supported
    }
}
