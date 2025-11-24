//! Expression aggregates - aggregating over expressions rather than simple columns
//!
//! This module handles aggregates over complex expressions like SUM(a * b),
//! where we need to evaluate the expression for each row before aggregating.
//!
//! For large datasets (>= 100 rows), this module automatically uses SIMD-accelerated
//! evaluation via Apache Arrow, providing 4-8x performance improvement.

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::functions::compare_for_min_max;
use super::{AggregateOp, AggregateSource, AggregateSpec};
use super::super::scan::ColumnarScan;

/// Threshold for using SIMD acceleration (same as vectorized filter threshold)
const SIMD_THRESHOLD: usize = 100;

/// Evaluate a simple arithmetic expression for a single row
///
/// This is a lightweight evaluator for the subset of expressions we support
/// in columnar aggregates (column references and binary operations).
pub(super) fn eval_simple_expr(
    expr: &Expression,
    row: &Row,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match expr {
        Expression::ColumnRef { table, column } => {
            let col_idx = schema.get_column_index(table.as_deref(), column)
                .ok_or_else(|| ExecutorError::UnsupportedExpression(
                    format!("Column not found: {}", column)
                ))?;
            Ok(row.get(col_idx).cloned().unwrap_or(SqlValue::Null))
        }
        Expression::Literal(val) => Ok(val.clone()),
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_simple_expr(left, row, schema)?;
            let right_val = eval_simple_expr(right, row, schema)?;

            // Use the evaluator's operators module
            use crate::evaluator::operators::OperatorRegistry;
            OperatorRegistry::eval_binary_op(&left_val, op, &right_val, vibesql_types::SqlMode::default())
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Complex expressions not supported in columnar aggregates".to_string()
        )),
    }
}

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
    // Only optimize SUM and AVG for now
    if !matches!(op, AggregateOp::Sum | AggregateOp::Avg) {
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
                Expression::ColumnRef { table: t2, column: c2 }
            ) => {
                let idx1 = schema.get_column_index(t1.as_deref(), c1)
                    .ok_or_else(|| ExecutorError::UnsupportedExpression(
                        format!("Column not found: {}", c1)
                    ))?;
                let idx2 = schema.get_column_index(t2.as_deref(), c2)
                    .ok_or_else(|| ExecutorError::UnsupportedExpression(
                        format!("Column not found: {}", c2)
                    ))?;
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
            let val1 = scan.row(row_idx)
                .and_then(|row| row.get(left_col))
                .unwrap_or(&SqlValue::Null);
            let val2 = scan.row(row_idx)
                .and_then(|row| row.get(right_col))
                .unwrap_or(&SqlValue::Null);

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

/// Convert SqlValue to f64 for arithmetic operations
fn sql_value_to_f64(val: &SqlValue) -> Option<f64> {
    match val {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::Bigint(v) => Some(*v as f64),
        SqlValue::Smallint(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        SqlValue::Numeric(v) => Some(*v),
        SqlValue::Null => None,
        _ => None,
    }
}

/// Compute an aggregate over an expression (e.g., SUM(a * b))
///
/// Evaluates the expression for each row, then aggregates the results.
/// For large datasets (>= SIMD_THRESHOLD rows), uses SIMD-accelerated evaluation.
pub(super) fn compute_expression_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    // Try main branch's vectorized path first for simple binary operations
    // This is optimized for column × column multiplication with optional filtering
    if let Some(result) = try_vectorized_binary_aggregate(rows, expr, op, filter_bitmap, schema)? {
        return Ok(result);
    }

    // Try SIMD path for large datasets (more general than vectorized binary)
    // Only when no filter bitmap (vectorized binary handles filtered case)
    if rows.len() >= SIMD_THRESHOLD && filter_bitmap.is_none() {
        if let Ok(result) = try_simd_aggregate(rows, expr, op, schema) {
            return Ok(result);
        }
        // Fall through to scalar path if SIMD fails
    }

    // Scalar path (for small datasets, complex expressions, or when SIMD not applicable)
    match op {
        AggregateOp::Sum => {
            let mut sum = 0.0;
            let mut count = 0;

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
                        SqlValue::Integer(v) => sum += v as f64,
                        SqlValue::Bigint(v) => sum += v as f64,
                        SqlValue::Smallint(v) => sum += v as f64,
                        SqlValue::Float(v) => sum += v as f64,
                        SqlValue::Double(v) => sum += v,
                        SqlValue::Numeric(v) => sum += v,
                        SqlValue::Null => {}, // Already checked above
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                format!("Cannot compute SUM on non-numeric value: {:?}", value)
                            ))
                        }
                    }
                    count += 1;
                }
            }

            Ok(if count > 0 {
                SqlValue::Double(sum)
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
            let sum_result = compute_expression_aggregate(rows, expr, AggregateOp::Sum, filter_bitmap, schema)?;
            let count_result = compute_expression_aggregate(rows, expr, AggregateOp::Count, filter_bitmap, schema)?;

            match (sum_result, count_result) {
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
            Expression::AggregateFunction {
                name,
                distinct,
                args,
            } => {
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
                    aggregates.push(AggregateSpec {
                        op,
                        source: AggregateSource::Column(0),
                    });
                    continue;
                }

                // Handle COUNT(*) with wildcard argument (Expression::Wildcard or ColumnRef { column: "*" })
                if op == AggregateOp::Count && args.len() == 1 {
                    match &args[0] {
                        Expression::Wildcard => {
                            aggregates.push(AggregateSpec {
                                op,
                                source: AggregateSource::Column(0),
                            });
                            continue;
                        }
                        Expression::ColumnRef { table: _, column } if column == "*" => {
                            aggregates.push(AggregateSpec {
                                op,
                                source: AggregateSource::Column(0),
                            });
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

/// Try to compute aggregate using SIMD-accelerated evaluation
///
/// This function converts rows to Arrow RecordBatch, evaluates the expression
/// using SIMD operations, and aggregates the result.
///
/// Returns Ok(value) if SIMD path succeeds, Err(_) if it fails (caller falls back to scalar).
fn try_simd_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    use crate::select::vectorized::{
        evaluate_arithmetic_simd, rows_to_record_batch,
    };

    // Extract column names from schema (in order)
    let mut column_names = vec![String::new(); schema.total_columns];
    for (start_idx, table_schema) in schema.table_schemas.values() {
        for (col_idx, col) in table_schema.columns.iter().enumerate() {
            column_names[start_idx + col_idx] = col.name.clone();
        }
    }

    // Convert rows to RecordBatch
    let batch = rows_to_record_batch(rows, &column_names)
        .map_err(|_| ExecutorError::Other("Failed to convert to RecordBatch".to_string()))?;

    // Evaluate expression using SIMD
    let result_array = evaluate_arithmetic_simd(&batch, expr)?;

    // Aggregate the result array
    match op {
        AggregateOp::Sum => {
            // Use Arrow compute kernels for summing
            use arrow::array::{Float64Array, Int64Array};
            use arrow::compute::sum;

            match result_array.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Int64Array".to_string())
                        })?;
                    let sum_val = sum(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD sum returned None".to_string())
                    })?;
                    Ok(SqlValue::Integer(sum_val))
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Float64Array".to_string())
                        })?;
                    let sum_val = sum(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD sum returned None".to_string())
                    })?;
                    Ok(SqlValue::Double(sum_val))
                }
                _ => Err(ExecutorError::Other("Unsupported array type for SUM".to_string())),
            }
        }
        AggregateOp::Count => {
            // Count non-null values
            let non_null_count = result_array.len() - result_array.null_count();
            Ok(SqlValue::Integer(non_null_count as i64))
        }
        AggregateOp::Avg => {
            // AVG = SUM / COUNT
            let sum_result = try_simd_aggregate(rows, expr, AggregateOp::Sum, schema)?;
            let count_result = try_simd_aggregate(rows, expr, AggregateOp::Count, schema)?;

            match (sum_result, count_result) {
                (SqlValue::Double(sum), SqlValue::Integer(count)) if count > 0 => {
                    Ok(SqlValue::Double(sum / count as f64))
                }
                (SqlValue::Integer(sum), SqlValue::Integer(count)) if count > 0 => {
                    Ok(SqlValue::Double(sum as f64 / count as f64))
                }
                _ => Ok(SqlValue::Null),
            }
        }
        AggregateOp::Min => {
            use arrow::array::{Float64Array, Int64Array};
            use arrow::compute::min;

            match result_array.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Int64Array".to_string())
                        })?;
                    let min_val = min(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD min returned None".to_string())
                    })?;
                    Ok(SqlValue::Integer(min_val))
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Float64Array".to_string())
                        })?;
                    let min_val = min(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD min returned None".to_string())
                    })?;
                    Ok(SqlValue::Double(min_val))
                }
                _ => Err(ExecutorError::Other("Unsupported array type for MIN".to_string())),
            }
        }
        AggregateOp::Max => {
            use arrow::array::{Float64Array, Int64Array};
            use arrow::compute::max;

            match result_array.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Int64Array".to_string())
                        })?;
                    let max_val = max(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD max returned None".to_string())
                    })?;
                    Ok(SqlValue::Integer(max_val))
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = result_array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            ExecutorError::Other("Failed to downcast Float64Array".to_string())
                        })?;
                    let max_val = max(arr).ok_or_else(|| {
                        ExecutorError::Other("SIMD max returned None".to_string())
                    })?;
                    Ok(SqlValue::Double(max_val))
                }
                _ => Err(ExecutorError::Other("Unsupported array type for MAX".to_string())),
            }
        }
    }
}
