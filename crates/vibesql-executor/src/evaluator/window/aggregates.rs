//! Aggregate window functions
//!
//! Implements COUNT, SUM, AVG, MIN, MAX with frame support.
//! SQL:2003 FILTER clause support for conditional aggregation in window functions.
//! SQL:2011 EXCLUDE clause support for frame exclusion.

use std::cmp::Ordering;

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
            // SQLite uses general truthiness: any non-zero, non-NULL value is true
            is_truthy_value(&filter_result)
        } else {
            false // Evaluation error = skip row
        }
    } else {
        true // No filter = include all rows
    }
}

/// Check if a SQL value is truthy (SQLite semantics)
fn is_truthy_value(value: &SqlValue) -> bool {
    match value {
        SqlValue::Boolean(b) => *b,
        SqlValue::Null => false,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Smallint(n) => *n != 0,
        SqlValue::Bigint(n) => *n != 0,
        SqlValue::Unsigned(n) => *n != 0,
        SqlValue::Float(f) => *f != 0.0,
        SqlValue::Real(f) => *f != 0.0,
        SqlValue::Double(f) => *f != 0.0,
        SqlValue::Numeric(f) => *f != 0.0,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try parsing as number; if it parses as non-zero, it's truthy
            s.parse::<f64>().map_or(false, |n| n != 0.0)
        }
        _ => false,
    }
}

/// Evaluate COUNT aggregate window function over a frame
///
/// Counts rows in the frame. Two variants:
/// - COUNT(*): counts all rows in frame
/// - COUNT(expr): counts rows where expr is not NULL
///
/// Supports FILTER clause for conditional aggregation.
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: COUNT(*) FILTER (WHERE x > 0) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
pub fn evaluate_count_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: Option<&Expression>,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut count = 0i64;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: SUM(amount) FILTER (WHERE status = 'paid') OVER (ORDER BY date) for filtered running totals
pub fn evaluate_sum_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut sum = 0.0f64;
    let mut has_value = false;
    // Track whether any addend was a real/float so we preserve SQLite's behaviour:
    // sum() returns INTEGER only when every input was an integer; one float input
    // yields a REAL (Numeric) result, even if the total happens to be a whole number.
    let mut saw_real = false;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
                    saw_real = true;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                    has_value = true;
                    saw_real = true;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                    has_value = true;
                    saw_real = true;
                }
                SqlValue::Double(n) => {
                    sum += n;
                    has_value = true;
                    saw_real = true;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    if has_value {
        if saw_real {
            SqlValue::Numeric(sum)
        } else if sum.fract() == 0.0 && sum >= i64::MIN as f64 && sum <= i64::MAX as f64 {
            SqlValue::Integer(sum as i64)
        } else {
            SqlValue::Numeric(sum)
        }
    } else {
        SqlValue::Null
    }
}

/// Evaluate TOTAL aggregate window function over a frame
///
/// Like SUM but returns 0.0 instead of NULL for empty sets (SQLite compatible).
/// Always returns a real (float) value.
/// Supports FILTER clause for conditional aggregation.
/// Supports EXCLUDE clause via frame_indices iterator.
pub fn evaluate_total_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut sum = 0.0f64;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
                }
                SqlValue::Smallint(n) => {
                    sum += n as f64;
                }
                SqlValue::Bigint(n) => {
                    sum += n as f64;
                }
                SqlValue::Numeric(n) => {
                    sum += n;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                }
                SqlValue::Double(n) => {
                    sum += n;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    // TOTAL always returns a real value, 0.0 for empty sets (never NULL)
    SqlValue::Numeric(sum)
}

/// Evaluate AVG aggregate window function over a frame
///
/// Computes average of numeric values in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
/// Supports FILTER clause for conditional aggregation.
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: AVG(temperature) FILTER (WHERE valid = 1) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
/// for 7-day moving average of valid readings
pub fn evaluate_avg_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut sum = 0.0f64;
    let mut count = 0i64;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: MIN(salary) FILTER (WHERE active = 1) OVER (PARTITION BY department)
pub fn evaluate_min_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut min_val: Option<SqlValue> = None;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: MAX(salary) FILTER (WHERE active = 1) OVER (PARTITION BY department)
pub fn evaluate_max_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    let mut max_val: Option<SqlValue> = None;

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
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
/// Supports EXCLUDE clause via frame_indices iterator.
///
/// Example: GROUP_CONCAT(name, ',') FILTER (WHERE active = 1) OVER (ORDER BY date)
pub fn evaluate_group_concat_window<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    separator: &str,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    evaluate_group_concat_window_with_expr(
        partition,
        frame_indices,
        arg_expr,
        None,
        separator,
        filter,
        eval_fn,
    )
}

/// Evaluate GROUP_CONCAT with per-row separator expression support.
///
/// When `separator_expr` is Some, the separator is evaluated per-row against each
/// row in the frame. The separator between values[i] and values[i+1] uses the
/// separator evaluated at the row that produced values[i+1].
/// When `separator_expr` is None, falls back to the static `default_separator`.
pub fn evaluate_group_concat_window_with_expr<F, I>(
    partition: &Partition,
    frame_indices: I,
    arg_expr: &Expression,
    separator_expr: Option<&Expression>,
    default_separator: &str,
    filter: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    I: IntoIterator<Item = usize>,
{
    // Collect (value_string, separator_string) pairs
    let mut entries: Vec<(String, String)> = Vec::new();

    for idx in frame_indices {
        if idx >= partition.len() {
            continue;
        }

        let row = &partition.rows[idx];

        // Check FILTER condition first
        if !passes_filter(filter, row, &eval_fn) {
            continue;
        }

        if let Ok(val) = eval_fn(arg_expr, row) {
            let val_str = match val {
                SqlValue::Null => continue, // Ignore NULL
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                SqlValue::Integer(n) => n.to_string(),
                SqlValue::Bigint(n) => n.to_string(),
                SqlValue::Smallint(n) => n.to_string(),
                SqlValue::Numeric(n) => {
                    if n.fract() == 0.0 {
                        (n as i64).to_string()
                    } else {
                        n.to_string()
                    }
                }
                SqlValue::Float(n) => {
                    if n.fract() == 0.0 {
                        (n as i64).to_string()
                    } else {
                        n.to_string()
                    }
                }
                SqlValue::Real(n) => {
                    if n.fract() == 0.0 {
                        (n as i64).to_string()
                    } else {
                        n.to_string()
                    }
                }
                SqlValue::Double(n) => {
                    if n.fract() == 0.0 {
                        (n as i64).to_string()
                    } else {
                        n.to_string()
                    }
                }
                other => format!("{}", other),
            };

            // Evaluate separator per-row if expression is provided
            let sep = if let Some(sep_expr) = separator_expr {
                if let Ok(sep_val) = eval_fn(sep_expr, row) {
                    match sep_val {
                        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                        SqlValue::Null => default_separator.to_string(),
                        other => format!("{}", other),
                    }
                } else {
                    default_separator.to_string()
                }
            } else {
                default_separator.to_string()
            };

            entries.push((val_str, sep));
        }
    }

    if entries.is_empty() {
        SqlValue::Null
    } else {
        let mut result = String::new();
        for (i, (val, sep)) in entries.iter().enumerate() {
            if i > 0 {
                // Use the separator from this row (the row that produced this value)
                result.push_str(sep);
            }
            result.push_str(val);
        }
        SqlValue::Varchar(result.into())
    }
}
