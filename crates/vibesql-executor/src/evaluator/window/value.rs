//! Value window functions
//!
//! Implements LAG, LEAD, FIRST_VALUE, and LAST_VALUE for accessing values from other rows in the
//! partition.

use vibesql_ast::Expression;
use vibesql_types::SqlValue;

use super::{partitioning::Partition, utils::evaluate_default_value};

/// Evaluate LAG() value window function
///
/// Accesses a value from a previous row in the partition (offset rows back).
/// Returns the value from (current_row_idx - offset).
/// If offset goes before partition start, returns default value (or NULL).
///
/// Signature: LAG(expr [, offset [, default]])
/// - expr: Expression to evaluate on the offset row
/// - offset: Number of rows back (default: 1)
/// - default: Value to return when offset is out of bounds (default: NULL)
///
/// Example: LAG(price, 1) OVER (ORDER BY date)
/// Example: LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month)
///
/// Requires: ORDER BY in window spec (partition must be sorted)
pub fn evaluate_lag<F>(
    partition: &Partition,
    current_row_idx: usize,
    value_expr: &Expression,
    offset: Option<i64>,
    default: Option<&Expression>,
    eval_fn: F,
) -> Result<SqlValue, String>
where
    F: Fn(&Expression, &vibesql_storage::Row) -> Result<SqlValue, String>,
{
    let offset_val = offset.unwrap_or(1);

    // Validate offset is non-negative
    if offset_val < 0 {
        return Err(format!("LAG offset must be non-negative, got {}", offset_val));
    }

    // Calculate target row index (current_row_idx - offset)
    let target_idx = if offset_val as usize > current_row_idx {
        // Offset goes before partition start - return default
        return evaluate_default_value(default);
    } else {
        current_row_idx - offset_val as usize
    };

    // Get value from target row
    if let Some(target_row) = partition.rows.get(target_idx) {
        eval_fn(value_expr, target_row)
    } else {
        // Should not happen if bounds check is correct
        Ok(evaluate_default_value(default)?)
    }
}

/// Evaluate LEAD() value window function
///
/// Accesses a value from a subsequent row in the partition (offset rows forward).
/// Returns the value from (current_row_idx + offset).
/// If offset goes past partition end, returns default value (or NULL).
///
/// Signature: LEAD(expr [, offset [, default]])
/// - expr: Expression to evaluate on the offset row
/// - offset: Number of rows forward (default: 1)
/// - default: Value to return when offset is out of bounds (default: NULL)
///
/// Example: LEAD(price, 1) OVER (ORDER BY date)
/// Example: LEAD(sales, 3, 0) OVER (PARTITION BY product ORDER BY quarter)
///
/// Requires: ORDER BY in window spec (partition must be sorted)
pub fn evaluate_lead<F>(
    partition: &Partition,
    current_row_idx: usize,
    value_expr: &Expression,
    offset: Option<i64>,
    default: Option<&Expression>,
    eval_fn: F,
) -> Result<SqlValue, String>
where
    F: Fn(&Expression, &vibesql_storage::Row) -> Result<SqlValue, String>,
{
    let offset_val = offset.unwrap_or(1);

    // Validate offset is non-negative
    if offset_val < 0 {
        return Err(format!("LEAD offset must be non-negative, got {}", offset_val));
    }

    // Calculate target row index (current_row_idx + offset)
    let target_idx = current_row_idx + offset_val as usize;

    // Check if target is beyond partition end
    if target_idx >= partition.len() {
        return evaluate_default_value(default);
    }

    // Get value from target row
    if let Some(target_row) = partition.rows.get(target_idx) {
        eval_fn(value_expr, target_row)
    } else {
        // Should not happen if bounds check is correct
        Ok(evaluate_default_value(default)?)
    }
}

/// Evaluate FIRST_VALUE() value window function
///
/// Returns the value of the expression evaluated on the first row of the partition.
/// Useful for getting the initial value in an ordered partition.
///
/// Signature: FIRST_VALUE(expr)
/// - expr: Expression to evaluate on the first row
///
/// Example: FIRST_VALUE(price) OVER (PARTITION BY product ORDER BY date)
/// Example: FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date)
///
/// Note: Typically used with ORDER BY to get the "earliest" value according to ordering.
pub fn evaluate_first_value<F>(
    partition: &Partition,
    value_expr: &Expression,
    eval_fn: F,
) -> Result<SqlValue, String>
where
    F: Fn(&Expression, &vibesql_storage::Row) -> Result<SqlValue, String>,
{
    // Get the first row in the partition
    if let Some(first_row) = partition.rows.first() {
        eval_fn(value_expr, first_row)
    } else {
        // Empty partition - return NULL
        Ok(SqlValue::Null)
    }
}

/// Evaluate LAST_VALUE() value window function
///
/// Returns the value of the expression evaluated on the last row of the partition.
/// Useful for getting the final value in an ordered partition.
///
/// Signature: LAST_VALUE(expr)
/// - expr: Expression to evaluate on the last row
///
/// Example: LAST_VALUE(price) OVER (PARTITION BY product ORDER BY date)
/// Example: LAST_VALUE(status) OVER (PARTITION BY order_id ORDER BY timestamp)
///
/// Note: With default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
/// LAST_VALUE returns the current row's value. To get the actual last value in the
/// partition, use frame: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
/// This implementation always returns the last row in the partition.
pub fn evaluate_last_value<F>(
    partition: &Partition,
    value_expr: &Expression,
    eval_fn: F,
) -> Result<SqlValue, String>
where
    F: Fn(&Expression, &vibesql_storage::Row) -> Result<SqlValue, String>,
{
    // Get the last row in the partition
    if let Some(last_row) = partition.rows.last() {
        eval_fn(value_expr, last_row)
    } else {
        // Empty partition - return NULL
        Ok(SqlValue::Null)
    }
}

/// Evaluate NTH_VALUE() value window function
///
/// Returns the value of the expression from the Nth row in the window frame.
/// N is 1-based (NTH_VALUE(expr, 1) returns the first row's value).
///
/// Signature: NTH_VALUE(expr, n)
/// - expr: Expression to evaluate on the Nth row
/// - n: 1-based row position (must be positive integer)
///
/// Example: NTH_VALUE(price, 2) OVER (ORDER BY date) -- returns 2nd row's price
/// Example: NTH_VALUE(name, 1) OVER (PARTITION BY dept ORDER BY salary DESC)
///
/// Returns NULL if N is out of bounds or if the partition has fewer than N rows.
pub fn evaluate_nth_value<F>(
    partition: &Partition,
    n: i64,
    value_expr: &Expression,
    eval_fn: F,
) -> Result<SqlValue, String>
where
    F: Fn(&Expression, &vibesql_storage::Row) -> Result<SqlValue, String>,
{
    // N must be positive (1-based indexing)
    if n < 1 {
        return Err(format!("NTH_VALUE n must be a positive integer, got {}", n));
    }

    // Convert to 0-based index
    let idx = (n - 1) as usize;

    // Check if index is within partition bounds
    if idx >= partition.len() {
        // Out of bounds - return NULL
        return Ok(SqlValue::Null);
    }

    // Get value from the Nth row
    if let Some(row) = partition.rows.get(idx) {
        eval_fn(value_expr, row)
    } else {
        // Should not happen if bounds check is correct
        Ok(SqlValue::Null)
    }
}
