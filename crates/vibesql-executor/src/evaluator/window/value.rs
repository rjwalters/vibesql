//! Value window functions
//!
//! Implements LAG and LEAD for accessing values from other rows in the partition.
//! FIRST_VALUE, LAST_VALUE, and NTH_VALUE are evaluated inline at the dispatch sites
//! with frame-aware logic (see evaluation.rs and aggregation/window.rs).

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

// Note: FIRST_VALUE, LAST_VALUE, and NTH_VALUE are now evaluated inline at the
// dispatch sites (evaluation.rs and aggregation/window.rs) using frame-aware logic
// via calculate_frame_with_exclusion + included_indices. The previous implementations
// here operated on the entire partition without respecting frame boundaries.
