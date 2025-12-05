//! Vectorized binary aggregate operations
//!
//! This module provides vectorized evaluation for simple binary operations
//! (e.g., col_a * col_b) with optional filter bitmap support. It serves as
//! an intermediate optimization layer between scalar row-by-row evaluation
//! and full SIMD processing.

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::evaluator::{eval_simple_expr, sql_value_to_f64};
use super::simd::try_simd_aggregate;
use crate::select::columnar::aggregate::functions::compare_for_min_max;
use crate::select::columnar::aggregate::AggregateOp;
use crate::select::columnar::scan::ColumnarScan;

/// Threshold for using SIMD acceleration (same as vectorized filter threshold)
const SIMD_THRESHOLD: usize = 100;

/// Try vectorized evaluation for simple binary operations (e.g., col_a * col_b)
///
/// Returns Some(result) if the expression can be vectorized (simple column * column),
/// or None if we need to fall back to row-by-row evaluation.
fn try_vectorized_binary_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
    schema: &CombinedSchema,
) -> Result<Option<SqlValue>, ExecutorError> {
    log::trace!(
        "[VecBinary] Checking vectorized binary aggregate for {} rows, op={:?}",
        rows.len(),
        op
    );

    // Only optimize SUM and AVG for now
    if !matches!(op, AggregateOp::Sum | AggregateOp::Avg) {
        log::trace!("[VecBinary] Op {:?} not supported for vectorized binary", op);
        return Ok(None);
    }

    // Check if expression is a simple binary multiply: col_a * col_b
    if let Expression::BinaryOp { left, op: bin_op, right } = expr {
        use vibesql_ast::BinaryOperator;

        // Only handle multiplication for now (most common in TPC-H)
        if *bin_op != BinaryOperator::Multiply {
            return Ok(None);
        }

        // Both operands must be simple column references
        let (left_col, right_col) = match (left.as_ref(), right.as_ref()) {
            (
                Expression::ColumnRef { table: t1, column: c1 },
                Expression::ColumnRef { table: t2, column: c2 },
            ) => {
                let idx1 = schema.get_column_index(t1.as_deref(), c1).ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!("Column not found: {}", c1))
                })?;
                let idx2 = schema.get_column_index(t2.as_deref(), c2).ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!("Column not found: {}", c2))
                })?;
                (idx1, idx2)
            }
            _ => return Ok(None), // Not simple col * col pattern
        };

        // Vectorized path: extract both columns, multiply, sum
        let scan = ColumnarScan::new(rows);
        let mut sum = 0.0;
        let mut count = 0;

        // Batch processing: accumulate products in batches for better cache locality
        const BATCH_SIZE: usize = 1024;
        let mut batch_products = Vec::with_capacity(BATCH_SIZE);

        for row_idx in 0..rows.len() {
            // Check filter
            if let Some(bitmap) = filter_bitmap {
                if !bitmap.get(row_idx).copied().unwrap_or(false) {
                    continue;
                }
            }

            // Get values from both columns
            let val1 =
                scan.row(row_idx).and_then(|row| row.get(left_col)).unwrap_or(&SqlValue::Null);
            let val2 =
                scan.row(row_idx).and_then(|row| row.get(right_col)).unwrap_or(&SqlValue::Null);

            // Convert to f64 and multiply
            if let (Some(v1), Some(v2)) = (sql_value_to_f64(val1), sql_value_to_f64(val2)) {
                batch_products.push(v1 * v2);
                count += 1;

                // Process batch when full
                if batch_products.len() >= BATCH_SIZE {
                    sum += batch_products.iter().sum::<f64>();
                    batch_products.clear();
                }
            }
        }

        // Process remaining batch
        if !batch_products.is_empty() {
            sum += batch_products.iter().sum::<f64>();
        }

        let result = if count > 0 {
            match op {
                AggregateOp::Sum => SqlValue::Double(sum),
                AggregateOp::Avg => SqlValue::Double(sum / count as f64),
                _ => unreachable!(),
            }
        } else {
            SqlValue::Null
        };

        return Ok(Some(result));
    }

    Ok(None)
}

/// Compute an aggregate over an expression (e.g., SUM(a * b))
///
/// Evaluates the expression for each row, then aggregates the results.
/// For large datasets (>= SIMD_THRESHOLD rows), uses SIMD-accelerated evaluation.
pub fn compute_expression_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    log::debug!(
        "[ExprAgg] compute_expression_aggregate: {} rows, op={:?}, has_filter={}, threshold={}",
        rows.len(),
        op,
        filter_bitmap.is_some(),
        SIMD_THRESHOLD
    );

    // Try main branch's vectorized path first for simple binary operations
    // This is optimized for column × column multiplication with optional filtering
    if let Some(result) = try_vectorized_binary_aggregate(rows, expr, op, filter_bitmap, schema)? {
        log::debug!("[ExprAgg] Used vectorized binary aggregate path");
        return Ok(result);
    }

    // Try SIMD path for large datasets (more general than vectorized binary)
    // Only when no filter bitmap (vectorized binary handles filtered case)
    if rows.len() >= SIMD_THRESHOLD && filter_bitmap.is_none() {
        log::debug!(
            "[ExprAgg] Attempting Arrow SIMD path ({} >= {} rows)",
            rows.len(),
            SIMD_THRESHOLD
        );
        if let Ok(result) = try_simd_aggregate(rows, expr, op, schema) {
            log::debug!("[ExprAgg] Arrow SIMD path succeeded");
            return Ok(result);
        }
        log::debug!("[ExprAgg] Arrow SIMD path failed, falling back to scalar");
        // Fall through to scalar path if SIMD fails
    } else if rows.len() < SIMD_THRESHOLD {
        log::debug!(
            "[ExprAgg] Below SIMD threshold ({} < {}), using vectorized/scalar path",
            rows.len(),
            SIMD_THRESHOLD
        );
    }

    // Scalar path (for small datasets, complex expressions, or when SIMD not applicable)
    log::debug!("[ExprAgg] Using scalar aggregate path");
    compute_scalar_aggregate(rows, expr, op, filter_bitmap, schema)
}

/// Scalar (row-by-row) aggregate computation
///
/// This is the fallback path for small datasets, complex expressions,
/// or when vectorized/SIMD paths are not applicable.
fn compute_scalar_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => {
            let mut int_sum: i64 = 0;
            let mut float_sum = 0.0;
            let mut count = 0;
            let mut has_float = false;

            for (row_idx, row) in rows.iter().enumerate() {
                // Check filter bitmap
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }

                // Evaluate expression for this row
                let value = eval_simple_expr(expr, row, schema)?;

                // Add to sum
                if !matches!(value, SqlValue::Null) {
                    match value {
                        SqlValue::Integer(v) => {
                            if has_float {
                                float_sum += v as f64;
                            } else {
                                int_sum += v;
                            }
                        }
                        SqlValue::Bigint(v) => {
                            if has_float {
                                float_sum += v as f64;
                            } else {
                                int_sum += v;
                            }
                        }
                        SqlValue::Smallint(v) => {
                            if has_float {
                                float_sum += v as f64;
                            } else {
                                int_sum += v as i64;
                            }
                        }
                        SqlValue::Float(v) => {
                            if !has_float {
                                // Convert accumulated integer sum to float
                                float_sum = int_sum as f64;
                                has_float = true;
                            }
                            float_sum += v as f64;
                        }
                        SqlValue::Double(v) => {
                            if !has_float {
                                // Convert accumulated integer sum to float
                                float_sum = int_sum as f64;
                                has_float = true;
                            }
                            float_sum += v;
                        }
                        SqlValue::Numeric(v) => {
                            if !has_float {
                                // Convert accumulated integer sum to float
                                float_sum = int_sum as f64;
                                has_float = true;
                            }
                            float_sum += v;
                        }
                        SqlValue::Null => {} // Already checked above
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Cannot compute SUM on non-numeric value: {:?}",
                                value
                            )))
                        }
                    }
                    count += 1;
                }
            }

            Ok(if count > 0 {
                if has_float {
                    SqlValue::Double(float_sum)
                } else {
                    SqlValue::Integer(int_sum)
                }
            } else {
                SqlValue::Null
            })
        }
        AggregateOp::Count => {
            // COUNT of expression counts non-NULL results
            let mut count = 0;
            for (row_idx, row) in rows.iter().enumerate() {
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }
                let value = eval_simple_expr(expr, row, schema)?;
                if !matches!(value, SqlValue::Null) {
                    count += 1;
                }
            }
            Ok(SqlValue::Integer(count))
        }
        AggregateOp::Avg => {
            // AVG(expr) = SUM(expr) / COUNT(expr)
            let sum_result =
                compute_expression_aggregate(rows, expr, AggregateOp::Sum, filter_bitmap, schema)?;
            let count_result = compute_expression_aggregate(
                rows,
                expr,
                AggregateOp::Count,
                filter_bitmap,
                schema,
            )?;

            match (sum_result, count_result) {
                (SqlValue::Integer(sum), SqlValue::Integer(count)) if count > 0 => {
                    Ok(SqlValue::Double(sum as f64 / count as f64))
                }
                (SqlValue::Double(sum), SqlValue::Integer(count)) if count > 0 => {
                    Ok(SqlValue::Double(sum / count as f64))
                }
                _ => Ok(SqlValue::Null),
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let mut result_value: Option<SqlValue> = None;

            for (row_idx, row) in rows.iter().enumerate() {
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }

                let value = eval_simple_expr(expr, row, schema)?;
                if !matches!(value, SqlValue::Null) {
                    result_value = Some(match &result_value {
                        None => value,
                        Some(current) => {
                            let should_update = if op == AggregateOp::Min {
                                compare_for_min_max(&value, current)
                            } else {
                                compare_for_min_max(current, &value)
                            };
                            if should_update {
                                value
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }

            Ok(result_value.unwrap_or(SqlValue::Null))
        }
    }
}
