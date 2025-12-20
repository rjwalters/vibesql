//! Aggregate window functions
//!
//! Implements COUNT, SUM, AVG, MIN, MAX with frame support.
//! SQL:2003 FILTER clause support for conditional aggregation in window functions.

use std::{cmp::Ordering, ops::Range};

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{partitioning::Partition, sorting::compare_values};

/// Helper function to check if a row passes the FILTER condition
/// Returns true if there's no filter, or if the filter evaluates to TRUE
#[inline]
fn passes_filter<F>(filter: Option<&Expression>, row: &Row, eval_fn: &F) -> bool
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    if let Some(filter_expr) = filter {
        if let Ok(filter_result) = eval_fn(filter_expr, row) {
            matches!(filter_result, SqlValue::Boolean(true))
        } else {
            false // Evaluation error = skip row
        }
    } else {
        true // No filter = include all rows
    }
}

/// Evaluate COUNT aggregate window function over a frame
///
/// Counts rows in the frame. Two variants:
/// - COUNT(*): counts all rows in frame
/// - COUNT(expr): counts rows where expr is not NULL
///
/// Supports FILTER clause for conditional aggregation.
///
/// Example: COUNT(*) FILTER (WHERE x > 0) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
pub fn evaluate_count_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: Option<&Expression>,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut count = 0i64;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        // COUNT(*) - count all rows
        if arg_expr.is_none() {
            count += 1;
            continue;
        }

        // COUNT(expr) - count non-NULL values
        if let Some(expr) = arg_expr {
            if let Ok(val) = eval_fn(expr, row) {
                if !matches!(val, SqlValue::Null) {
                    count += 1;
                }
            }
        }
    }

    SqlValue::Integer(count)
}

/// Evaluate SUM aggregate window function over a frame
///
/// Sums numeric values in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
///
/// Example: SUM(amount) FILTER (WHERE status = 'paid') OVER (ORDER BY date) for filtered running totals
pub fn evaluate_sum_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut sum = 0.0f64;
    let mut has_value = false;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Smallint(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Bigint(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Numeric(n) => {
                    sum += n;
                    has_value = true;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Double(n) => {
                    sum += n;
                    has_value = true;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    if has_value {
        // Return Integer if sum is a whole number, otherwise Numeric
        if sum.fract() == 0.0 && sum >= i64::MIN as f64 && sum <= i64::MAX as f64 {
            SqlValue::Integer(sum as i64)
        } else {
            SqlValue::Numeric(sum)
        }
    } else {
        SqlValue::Null
    }
}

/// Evaluate AVG aggregate window function over a frame
///
/// Computes average of numeric values in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
///
/// Example: AVG(temperature) FILTER (WHERE valid = 1) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
/// for 7-day moving average of valid readings
pub fn evaluate_avg_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut sum = 0.0f64;
    let mut count = 0i64;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Smallint(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Bigint(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Numeric(n) => {
                    sum += n;
                    count += 1;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Double(n) => {
                    sum += n;
                    count += 1;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    if count > 0 {
        SqlValue::Numeric(sum / count as f64)
    } else {
        SqlValue::Null
    }
}

/// Evaluate MIN aggregate window function over a frame
///
/// Finds minimum value in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
///
/// Example: MIN(salary) FILTER (WHERE active = 1) OVER (PARTITION BY department)
pub fn evaluate_min_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut min_val: Option<SqlValue> = None;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            if matches!(val, SqlValue::Null) {
                continue; // Ignore NULL
            }

            if let Some(ref current_min) = min_val {
                if compare_values(&val, current_min) == Ordering::Less {
                    min_val = Some(val);
                }
            } else {
                min_val = Some(val);
            }
        }
    }

    min_val.unwrap_or(SqlValue::Null)
}

/// Evaluate MAX aggregate window function over a frame
///
/// Finds maximum value in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
///
/// Example: MAX(salary) FILTER (WHERE active = 1) OVER (PARTITION BY department)
pub fn evaluate_max_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut max_val: Option<SqlValue> = None;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            if matches!(val, SqlValue::Null) {
                continue; // Ignore NULL
            }

            if let Some(ref current_max) = max_val {
                if compare_values(&val, current_max) == Ordering::Greater {
                    max_val = Some(val);
                }
            } else {
                max_val = Some(val);
            }
        }
    }

    max_val.unwrap_or(SqlValue::Null)
}

/// Evaluate GROUP_CONCAT aggregate window function over a frame
///
/// Concatenates string values in the frame using the specified separator.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
///
/// Example: GROUP_CONCAT(name, ',') FILTER (WHERE active = 1) OVER (ORDER BY date)
pub fn evaluate_group_concat_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    separator: &str,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut values: Vec<String> = Vec::new();

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Null => {} // Ignore NULL
                SqlValue::Varchar(s) | SqlValue::Character(s) => {
                    values.push(s.to_string());
                }
                SqlValue::Integer(n) => {
                    values.push(n.to_string());
                }
                SqlValue::Bigint(n) => {
                    values.push(n.to_string());
                }
                SqlValue::Smallint(n) => {
                    values.push(n.to_string());
                }
                SqlValue::Numeric(n) => {
                    // Format as integer if whole number
                    if n.fract() == 0.0 {
                        values.push((n as i64).to_string());
                    } else {
                        values.push(n.to_string());
                    }
                }
                SqlValue::Float(n) => {
                    if n.fract() == 0.0 {
                        values.push((n as i64).to_string());
                    } else {
                        values.push(n.to_string());
                    }
                }
                SqlValue::Real(n) => {
                    if n.fract() == 0.0 {
                        values.push((n as i64).to_string());
                    } else {
                        values.push(n.to_string());
                    }
                }
                SqlValue::Double(n) => {
                    if n.fract() == 0.0 {
                        values.push((n as i64).to_string());
                    } else {
                        values.push(n.to_string());
                    }
                }
                other => {
                    // Convert other types to string
                    values.push(format!("{}", other));
                }
            }
        }
    }

    if values.is_empty() {
        SqlValue::Null
    } else {
        SqlValue::Varchar(values.join(separator).into())
    }
}
