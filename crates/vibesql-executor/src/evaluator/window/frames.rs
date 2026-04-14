//! Frame calculation for window functions
//!
//! Calculates frame boundaries (ROWS mode) for window function evaluation.
//! Supports SQL:2011 EXCLUDE clause for excluding rows from the frame.

use std::cmp::Ordering;
use std::ops::Range;

use vibesql_ast::{
    Expression, FrameBound, FrameExclude, FrameUnit, OrderByItem, UnaryOperator, WindowFrame,
};
use vibesql_types::SqlValue;

use vibesql_storage::Row;

use super::partitioning::Partition;
use super::sorting::compare_values;

/// Validate a window frame specification, returning an error if invalid.
///
/// This checks that frame offsets are non-negative numbers.
pub fn validate_frame(frame_spec: &Option<WindowFrame>) -> Result<(), String> {
    let frame = match frame_spec {
        Some(f) => f,
        None => return Ok(()),
    };

    // Validate start bound
    validate_frame_bound(&frame.start, true)?;

    // Validate end bound if present
    if let Some(ref end_bound) = frame.end {
        validate_frame_bound(end_bound, false)?;
    }

    Ok(())
}

/// Validate that RANGE frames with offset PRECEDING/FOLLOWING have exactly one ORDER BY column.
///
/// Per the SQL standard and SQLite behavior, RANGE with numeric offsets (N PRECEDING or
/// N FOLLOWING) requires exactly one ORDER BY expression. Without ORDER BY, or with
/// multiple ORDER BY columns, an error is returned.
pub fn validate_range_order_by(
    frame_spec: &Option<WindowFrame>,
    order_by: &Option<Vec<OrderByItem>>,
) -> Result<(), String> {
    let frame = match frame_spec {
        Some(f) => f,
        None => return Ok(()),
    };

    // Only applies to RANGE frames
    if !matches!(frame.unit, FrameUnit::Range) {
        return Ok(());
    }

    // Check if either bound uses a numeric offset (N PRECEDING or N FOLLOWING)
    let has_offset_bound = matches!(
        frame.start,
        FrameBound::Preceding(_) | FrameBound::Following(_)
    ) || frame
        .end
        .as_ref()
        .is_some_and(|end| matches!(end, FrameBound::Preceding(_) | FrameBound::Following(_)));

    if !has_offset_bound {
        return Ok(());
    }

    // RANGE with offset requires exactly one ORDER BY column
    let order_by_count = order_by.as_ref().map_or(0, |items| items.len());
    if order_by_count != 1 {
        return Err(
            "RANGE with offset PRECEDING/FOLLOWING requires one ORDER BY expression".to_string(),
        );
    }

    Ok(())
}

/// Validate a single frame bound
fn validate_frame_bound(bound: &FrameBound, is_start: bool) -> Result<(), String> {
    match bound {
        FrameBound::UnboundedPreceding
        | FrameBound::UnboundedFollowing
        | FrameBound::CurrentRow => Ok(()),

        FrameBound::Preceding(offset_expr) | FrameBound::Following(offset_expr) => {
            let bound_type = if is_start { "starting" } else { "ending" };
            match evaluate_offset_expr(offset_expr)? {
                Some(offset) if offset < 0.0 => {
                    Err(format!("frame {} offset must be a non-negative number", bound_type))
                }
                None => {
                    // NULL, invalid string, blob - all treated as invalid numbers
                    Err(format!("frame {} offset must be a non-negative number", bound_type))
                }
                Some(_) => Ok(()), // Valid non-negative number
            }
        }
    }
}

/// Evaluate an offset expression to get its numeric value.
///
/// Handles literal integers, floats, strings (parsed as numbers), and unary minus expressions.
/// Returns None to indicate the value should be treated as invalid (for error reporting).
fn evaluate_offset_expr(expr: &Expression) -> Result<Option<f64>, String> {
    match expr {
        Expression::Literal(SqlValue::Integer(n)) => Ok(Some(*n as f64)),
        Expression::Literal(SqlValue::Smallint(n)) => Ok(Some(*n as f64)),
        Expression::Literal(SqlValue::Bigint(n)) => Ok(Some(*n as f64)),
        Expression::Literal(SqlValue::Unsigned(n)) => Ok(Some(*n as f64)),
        Expression::Literal(SqlValue::Float(f)) => Ok(Some(*f as f64)),
        Expression::Literal(SqlValue::Real(f)) => Ok(Some(*f)),
        Expression::Literal(SqlValue::Double(f)) => Ok(Some(*f)),
        Expression::Literal(SqlValue::Numeric(f)) => Ok(Some(*f)),
        Expression::Literal(SqlValue::Null) => {
            // NULL is treated as invalid (not a number)
            Ok(None)
        }
        Expression::Literal(SqlValue::Character(s)) | Expression::Literal(SqlValue::Varchar(s)) => {
            // Try to parse string as a number (SQLite-compatible behavior)
            let s_str = s.as_str();
            if s_str.is_empty() {
                // Empty string is invalid
                Ok(None)
            } else if let Ok(n) = s_str.parse::<f64>() {
                Ok(Some(n))
            } else {
                // String that can't be parsed as number is invalid
                Ok(None)
            }
        }
        Expression::Literal(SqlValue::Blob(_)) => {
            // Blob is invalid
            Ok(None)
        }
        Expression::UnaryOp { op: UnaryOperator::Minus, expr } => {
            match evaluate_offset_expr(expr)? {
                Some(inner) => Ok(Some(-inner)),
                None => Ok(None),
            }
        }
        Expression::UnaryOp { op: UnaryOperator::Plus, expr } => evaluate_offset_expr(expr),
        _ => Err("frame offset must be a constant expression".to_string()),
    }
}

/// Calculate frame boundaries for a given row in a partition
///
/// Returns a `Range<usize>` representing the [start, end) indices of rows in the frame.
/// Supports ROWS, RANGE, and GROUPS frame semantics.
///
/// The `eval_fn` closure evaluates ORDER BY expressions against rows,
/// supporting complex expressions (BinaryOp, Function, etc.).
pub fn calculate_frame<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
    eval_fn: &F,
) -> Range<usize>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let partition_size = partition.len();

    // Default frame depends on whether there's an ORDER BY:
    // - Without ORDER BY: entire partition (all rows)
    // - With ORDER BY: RANGE UNBOUNDED PRECEDING to CURRENT ROW
    let frame = match frame_spec {
        Some(f) => f,
        None => {
            // Check if there's an ORDER BY clause
            let has_order_by = order_by.as_ref().is_some_and(|items| !items.is_empty());

            if has_order_by {
                // Default with ORDER BY: RANGE UNBOUNDED PRECEDING TO CURRENT ROW
                // This means all rows from start to current row's peer group (inclusive)
                return calculate_range_frame(
                    partition,
                    current_row_idx,
                    order_by,
                    &FrameBound::UnboundedPreceding,
                    &Some(FrameBound::CurrentRow),
                    eval_fn,
                );
            } else {
                // Default without ORDER BY: entire partition
                return 0..partition_size;
            }
        }
    };

    // Dispatch based on frame unit
    match frame.unit {
        FrameUnit::Rows => {
            calculate_rows_frame(partition, current_row_idx, &frame.start, &frame.end)
        }
        FrameUnit::Range => calculate_range_frame(
            partition,
            current_row_idx,
            order_by,
            &frame.start,
            &frame.end,
            eval_fn,
        ),
        FrameUnit::Groups => calculate_groups_frame(
            partition,
            current_row_idx,
            order_by,
            &frame.start,
            &frame.end,
            eval_fn,
        ),
    }
}

/// Calculate frame boundaries for ROWS mode (physical row-based)
fn calculate_rows_frame(
    partition: &Partition,
    current_row_idx: usize,
    start: &FrameBound,
    end: &Option<FrameBound>,
) -> Range<usize> {
    let partition_size = partition.len();

    // Calculate start boundary
    let start_idx = calculate_rows_boundary(start, current_row_idx, partition_size, true);

    // Calculate end boundary
    let end_idx = match end {
        Some(end_bound) => {
            calculate_rows_boundary(end_bound, current_row_idx, partition_size, false)
        }
        None => current_row_idx + 1, // Default: CURRENT ROW (inclusive, so +1 for Range)
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate frame boundaries for RANGE mode (value-based)
///
/// RANGE frames are based on the ORDER BY expression values:
/// - UNBOUNDED PRECEDING/FOLLOWING: Start/end of partition
/// - CURRENT ROW: All rows with same ORDER BY values (peers)
/// - N PRECEDING: Rows where ORDER BY value >= current_value - N
/// - N FOLLOWING: Rows where ORDER BY value <= current_value + N
fn calculate_range_frame<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    start: &FrameBound,
    end: &Option<FrameBound>,
    eval_fn: &F,
) -> Range<usize>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let partition_size = partition.len();

    if partition_size == 0 || current_row_idx >= partition_size {
        return 0..0;
    }

    // Without ORDER BY, RANGE mode treats entire partition as the frame
    let order_items = match order_by {
        Some(items) if !items.is_empty() => items,
        _ => return 0..partition_size,
    };

    // Get the current row's ORDER BY value (use first ORDER BY expression)
    let current_row = &partition.rows[current_row_idx];
    let current_value = eval_fn(&order_items[0].expr, current_row).unwrap_or(SqlValue::Null);

    // Calculate start boundary
    let start_idx = calculate_range_boundary(
        partition,
        current_row_idx,
        order_items,
        &current_value,
        start,
        true,
        eval_fn,
    );

    // Calculate end boundary
    let end_idx = match end {
        Some(end_bound) => calculate_range_boundary(
            partition,
            current_row_idx,
            order_items,
            &current_value,
            end_bound,
            false,
            eval_fn,
        ),
        None => {
            // Default: CURRENT ROW - find last peer
            find_last_peer(partition, current_row_idx, order_items, eval_fn) + 1
        }
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate a single RANGE mode boundary
///
/// Note: The meaning of PRECEDING/FOLLOWING depends on sort direction:
/// - ASC: PRECEDING = smaller values, FOLLOWING = larger values
/// - DESC: PRECEDING = larger values, FOLLOWING = smaller values
fn calculate_range_boundary<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_items: &[OrderByItem],
    current_value: &SqlValue,
    bound: &FrameBound,
    is_start: bool,
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    use vibesql_ast::OrderDirection;

    let partition_size = partition.len();

    // Check if we have DESC ordering (affects PRECEDING/FOLLOWING semantics)
    let is_desc =
        order_items.first().is_some_and(|item| matches!(item.direction, OrderDirection::Desc));

    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => partition_size,

        FrameBound::CurrentRow => {
            if is_start {
                // Find first peer (row with same ORDER BY values)
                find_first_peer(partition, current_row_idx, order_items, eval_fn)
            } else {
                // Find last peer + 1 (exclusive end)
                find_last_peer(partition, current_row_idx, order_items, eval_fn) + 1
            }
        }

        FrameBound::Preceding(offset_expr) => {
            let offset = get_numeric_offset(offset_expr);

            // For DESC order, PRECEDING means larger values (add offset)
            // For ASC order, PRECEDING means smaller values (subtract offset)
            let target_value = if is_desc {
                add_to_value(current_value, offset)
            } else {
                subtract_from_value(current_value, offset)
            };

            if is_start {
                // For DESC: find first row where value <= target (target is larger, comes first)
                // For ASC: find first row where value >= target (target is smaller, comes first)
                if is_desc {
                    find_first_row_le_desc(partition, order_items, &target_value, eval_fn)
                } else {
                    find_first_row_ge(partition, order_items, &target_value, eval_fn)
                }
            } else {
                // Find the boundary row and return exclusive end
                if is_desc {
                    find_last_row_ge_desc(partition, order_items, &target_value, eval_fn) + 1
                } else {
                    find_last_row_le(partition, order_items, &target_value, eval_fn) + 1
                }
            }
        }

        FrameBound::Following(offset_expr) => {
            let offset = get_numeric_offset(offset_expr);

            // For DESC order, FOLLOWING means smaller values (subtract offset)
            // For ASC order, FOLLOWING means larger values (add offset)
            let target_value = if is_desc {
                subtract_from_value(current_value, offset)
            } else {
                add_to_value(current_value, offset)
            };

            if is_start {
                // For DESC: find first row where value <= target (target is smaller, comes later)
                // For ASC: find first row where value >= target (target is larger, comes later)
                if is_desc {
                    find_first_row_le_desc(partition, order_items, &target_value, eval_fn)
                } else {
                    find_first_row_ge(partition, order_items, &target_value, eval_fn)
                }
            } else {
                // Find the boundary row and return exclusive end
                if is_desc {
                    find_last_row_ge_desc(partition, order_items, &target_value, eval_fn) + 1
                } else {
                    find_last_row_le(partition, order_items, &target_value, eval_fn) + 1
                }
            }
        }
    }
}

/// Calculate frame boundaries for GROUPS mode (peer group-based)
///
/// GROUPS frames count peer groups (rows with same ORDER BY values):
/// - UNBOUNDED PRECEDING/FOLLOWING: Start/end of partition
/// - CURRENT ROW: Current peer group
/// - N PRECEDING: N peer groups before current
/// - N FOLLOWING: N peer groups after current
fn calculate_groups_frame<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    start: &FrameBound,
    end: &Option<FrameBound>,
    eval_fn: &F,
) -> Range<usize>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let partition_size = partition.len();

    if partition_size == 0 || current_row_idx >= partition_size {
        return 0..0;
    }

    // Without ORDER BY, GROUPS mode treats entire partition as one group
    let order_items = match order_by {
        Some(items) if !items.is_empty() => items,
        _ => return 0..partition_size,
    };

    // Build peer group boundaries
    let group_boundaries = build_group_boundaries(partition, order_items, eval_fn);
    let current_group = find_group_for_row(&group_boundaries, current_row_idx);

    // Calculate start boundary
    let start_idx =
        calculate_groups_boundary(&group_boundaries, current_group, start, true, partition_size);

    // Calculate end boundary
    let end_idx = match end {
        Some(end_bound) => calculate_groups_boundary(
            &group_boundaries,
            current_group,
            end_bound,
            false,
            partition_size,
        ),
        None => {
            // Default: CURRENT ROW - end of current group
            if current_group + 1 < group_boundaries.len() {
                group_boundaries[current_group + 1]
            } else {
                partition_size
            }
        }
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate a single GROUPS mode boundary
fn calculate_groups_boundary(
    group_boundaries: &[usize],
    current_group: usize,
    bound: &FrameBound,
    is_start: bool,
    partition_size: usize,
) -> usize {
    let num_groups = group_boundaries.len();

    match bound {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::UnboundedFollowing => partition_size,

        FrameBound::CurrentRow => {
            if is_start {
                group_boundaries[current_group]
            } else {
                if current_group + 1 < num_groups {
                    group_boundaries[current_group + 1]
                } else {
                    partition_size
                }
            }
        }

        FrameBound::Preceding(offset_expr) => {
            let offset = get_numeric_offset(offset_expr) as usize;

            if offset > current_group {
                // Target group is before the partition start
                if is_start {
                    // Start at beginning of partition (include everything from start)
                    0
                } else {
                    // End bound is before the partition start => empty frame
                    0
                }
            } else {
                let target_group = current_group - offset;
                if is_start {
                    group_boundaries[target_group]
                } else {
                    if target_group + 1 < num_groups {
                        group_boundaries[target_group + 1]
                    } else {
                        partition_size
                    }
                }
            }
        }

        FrameBound::Following(offset_expr) => {
            let offset = get_numeric_offset(offset_expr) as usize;
            let target_group = current_group + offset;

            if target_group >= num_groups {
                // Target group is beyond the partition end
                if is_start {
                    // Start bound is beyond partition => empty frame
                    partition_size
                } else {
                    // End at end of partition (include everything to end)
                    partition_size
                }
            } else {
                if is_start {
                    group_boundaries[target_group]
                } else {
                    if target_group + 1 < num_groups {
                        group_boundaries[target_group + 1]
                    } else {
                        partition_size
                    }
                }
            }
        }
    }
}

/// Build a list of peer group start indices
fn build_group_boundaries<F>(
    partition: &Partition,
    order_items: &[OrderByItem],
    eval_fn: &F,
) -> Vec<usize>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut boundaries = vec![0];

    for i in 1..partition.len() {
        let prev_row = &partition.rows[i - 1];
        let curr_row = &partition.rows[i];

        // Check if this row starts a new group (different ORDER BY values)
        let mut is_new_group = false;
        for item in order_items {
            let prev_val = eval_fn(&item.expr, prev_row).unwrap_or(SqlValue::Null);
            let curr_val = eval_fn(&item.expr, curr_row).unwrap_or(SqlValue::Null);
            if compare_values(&prev_val, &curr_val) != Ordering::Equal {
                is_new_group = true;
                break;
            }
        }

        if is_new_group {
            boundaries.push(i);
        }
    }

    boundaries
}

/// Find which group a row belongs to
fn find_group_for_row(group_boundaries: &[usize], row_idx: usize) -> usize {
    for (i, &boundary) in group_boundaries.iter().enumerate().rev() {
        if row_idx >= boundary {
            return i;
        }
    }
    0
}

/// Find the first peer (row with same ORDER BY values as current)
fn find_first_peer<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_items: &[OrderByItem],
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let current_row = &partition.rows[current_row_idx];

    for i in (0..current_row_idx).rev() {
        let row = &partition.rows[i];
        let mut is_peer = true;

        for item in order_items {
            let curr_val = eval_fn(&item.expr, current_row).unwrap_or(SqlValue::Null);
            let row_val = eval_fn(&item.expr, row).unwrap_or(SqlValue::Null);
            if compare_values(&curr_val, &row_val) != Ordering::Equal {
                is_peer = false;
                break;
            }
        }

        if !is_peer {
            return i + 1;
        }
    }

    0
}

/// Find the last peer (row with same ORDER BY values as current)
fn find_last_peer<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_items: &[OrderByItem],
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let current_row = &partition.rows[current_row_idx];

    for i in (current_row_idx + 1)..partition.len() {
        let row = &partition.rows[i];
        let mut is_peer = true;

        for item in order_items {
            let curr_val = eval_fn(&item.expr, current_row).unwrap_or(SqlValue::Null);
            let row_val = eval_fn(&item.expr, row).unwrap_or(SqlValue::Null);
            if compare_values(&curr_val, &row_val) != Ordering::Equal {
                is_peer = false;
                break;
            }
        }

        if !is_peer {
            return i - 1;
        }
    }

    partition.len() - 1
}

/// Find the first row where ORDER BY value >= target
fn find_first_row_ge<F>(
    partition: &Partition,
    order_items: &[OrderByItem],
    target: &SqlValue,
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    for (i, row) in partition.rows.iter().enumerate() {
        let val = eval_fn(&order_items[0].expr, row).unwrap_or(SqlValue::Null);
        if compare_values(&val, target) != Ordering::Less {
            return i;
        }
    }
    partition.len()
}

/// Find the last row where ORDER BY value <= target
fn find_last_row_le<F>(
    partition: &Partition,
    order_items: &[OrderByItem],
    target: &SqlValue,
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    for i in (0..partition.len()).rev() {
        let val = eval_fn(&order_items[0].expr, &partition.rows[i]).unwrap_or(SqlValue::Null);
        if compare_values(&val, target) != Ordering::Greater {
            return i;
        }
    }
    0
}

/// Find the first row where ORDER BY value <= target (for DESC sorted partitions)
///
/// In a DESC-sorted partition, values decrease as we go through the rows.
/// This finds the first row (smallest index) where value <= target.
fn find_first_row_le_desc<F>(
    partition: &Partition,
    order_items: &[OrderByItem],
    target: &SqlValue,
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    for (i, row) in partition.rows.iter().enumerate() {
        let val = eval_fn(&order_items[0].expr, row).unwrap_or(SqlValue::Null);
        if compare_values(&val, target) != Ordering::Greater {
            return i;
        }
    }
    partition.len()
}

/// Find the last row where ORDER BY value >= target (for DESC sorted partitions)
///
/// In a DESC-sorted partition, values decrease as we go through the rows.
/// This finds the last row (largest index) where value >= target.
fn find_last_row_ge_desc<F>(
    partition: &Partition,
    order_items: &[OrderByItem],
    target: &SqlValue,
    eval_fn: &F,
) -> usize
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    for i in (0..partition.len()).rev() {
        let val = eval_fn(&order_items[0].expr, &partition.rows[i]).unwrap_or(SqlValue::Null);
        if compare_values(&val, target) != Ordering::Less {
            return i;
        }
    }
    0
}

/// Get numeric offset from expression
fn get_numeric_offset(expr: &Expression) -> f64 {
    match evaluate_offset_expr(expr) {
        Ok(Some(n)) => n,
        _ => 0.0,
    }
}

/// Subtract a numeric offset from a SqlValue
fn subtract_from_value(value: &SqlValue, offset: f64) -> SqlValue {
    match value {
        SqlValue::Integer(n) => SqlValue::Real(*n as f64 - offset),
        SqlValue::Smallint(n) => SqlValue::Real(*n as f64 - offset),
        SqlValue::Bigint(n) => SqlValue::Real(*n as f64 - offset),
        SqlValue::Unsigned(n) => SqlValue::Real(*n as f64 - offset),
        SqlValue::Float(f) => SqlValue::Real(*f as f64 - offset),
        SqlValue::Real(f) => SqlValue::Real(*f - offset),
        SqlValue::Double(f) => SqlValue::Real(*f - offset),
        SqlValue::Numeric(f) => SqlValue::Real(*f - offset),
        _ => value.clone(), // For non-numeric, return as-is
    }
}

/// Add a numeric offset to a SqlValue
fn add_to_value(value: &SqlValue, offset: f64) -> SqlValue {
    match value {
        SqlValue::Integer(n) => SqlValue::Real(*n as f64 + offset),
        SqlValue::Smallint(n) => SqlValue::Real(*n as f64 + offset),
        SqlValue::Bigint(n) => SqlValue::Real(*n as f64 + offset),
        SqlValue::Unsigned(n) => SqlValue::Real(*n as f64 + offset),
        SqlValue::Float(f) => SqlValue::Real(*f as f64 + offset),
        SqlValue::Real(f) => SqlValue::Real(*f + offset),
        SqlValue::Double(f) => SqlValue::Real(*f + offset),
        SqlValue::Numeric(f) => SqlValue::Real(*f + offset),
        _ => value.clone(), // For non-numeric, return as-is
    }
}

/// Calculate a single ROWS mode boundary (physical row-based)
///
/// Returns the index for the boundary.
/// For start boundaries, returns inclusive index.
/// For end boundaries, returns exclusive index (Range semantics).
fn calculate_rows_boundary(
    bound: &FrameBound,
    current_row_idx: usize,
    partition_size: usize,
    is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,

        FrameBound::UnboundedFollowing => partition_size,

        FrameBound::CurrentRow => {
            if is_start {
                current_row_idx
            } else {
                current_row_idx + 1 // Exclusive end
            }
        }

        FrameBound::Preceding(offset_expr) => {
            let offset = get_numeric_offset(offset_expr) as usize;
            if is_start {
                current_row_idx.saturating_sub(offset)
            } else {
                // For end boundary, +1 for exclusive end (Range semantics)
                // If offset > current_row_idx, the position is before the partition start,
                // so return 0 (empty frame when combined with any start boundary)
                if offset > current_row_idx {
                    0
                } else {
                    current_row_idx - offset + 1
                }
            }
        }

        FrameBound::Following(offset_expr) => {
            let offset = get_numeric_offset(offset_expr) as usize;
            let result = current_row_idx + offset;

            if is_start {
                result.min(partition_size)
            } else {
                (result + 1).min(partition_size) // Exclusive end, +1 for inclusive offset
            }
        }
    }
}

/// Result of frame calculation including exclusion information
#[derive(Debug, Clone)]
pub struct FrameResult {
    /// The frame range [start, end)
    pub range: Range<usize>,
    /// Exclusion mode
    pub exclude: Option<FrameExclude>,
    /// Index of the current row being evaluated
    pub current_row_idx: usize,
}

impl FrameResult {
    /// Check if a row index should be included in the frame calculation
    ///
    /// Takes into account both the frame range and the EXCLUDE clause.
    pub fn includes<F>(
        &self,
        row_idx: usize,
        partition: &Partition,
        order_by: &Option<Vec<OrderByItem>>,
        eval_fn: &F,
    ) -> bool
    where
        F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    {
        // First check if row is in the frame range
        if !self.range.contains(&row_idx) {
            return false;
        }

        // Then apply EXCLUDE logic
        match self.exclude {
            None | Some(FrameExclude::NoOthers) => true,

            Some(FrameExclude::CurrentRow) => row_idx != self.current_row_idx,

            Some(FrameExclude::Group) => {
                // Exclude current row and all its peers (rows with same ORDER BY values)
                !is_peer(row_idx, self.current_row_idx, partition, order_by, eval_fn)
            }

            Some(FrameExclude::Ties) => {
                // Exclude peers of current row, but include current row itself
                row_idx == self.current_row_idx
                    || !is_peer(row_idx, self.current_row_idx, partition, order_by, eval_fn)
            }
        }
    }

    /// Get an iterator over all included row indices
    ///
    /// This filters out excluded rows based on the EXCLUDE clause.
    pub fn included_indices<'a, F>(
        &'a self,
        partition: &'a Partition,
        order_by: &'a Option<Vec<OrderByItem>>,
        eval_fn: &'a F,
    ) -> impl Iterator<Item = usize> + 'a
    where
        F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
    {
        self.range.clone().filter(move |&idx| self.includes(idx, partition, order_by, eval_fn))
    }
}

/// Calculate frame with exclusion information
///
/// Returns a FrameResult that includes both the range and exclusion info.
pub fn calculate_frame_with_exclusion<F>(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
    eval_fn: &F,
) -> FrameResult
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let range = calculate_frame(partition, current_row_idx, order_by, frame_spec, eval_fn);
    let exclude = frame_spec.as_ref().and_then(|f| f.exclude);

    FrameResult { range, exclude, current_row_idx }
}

/// Check if two rows are peers (have same ORDER BY values)
///
/// Rows are considered peers if they have identical values for all ORDER BY expressions.
/// If there's no ORDER BY clause, all rows are considered peers.
fn is_peer<F>(
    row_idx_a: usize,
    row_idx_b: usize,
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
    eval_fn: &F,
) -> bool
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    // Same row is always a peer of itself
    if row_idx_a == row_idx_b {
        return true;
    }

    // Without ORDER BY, all rows are considered peers
    let order_items = match order_by {
        Some(items) if !items.is_empty() => items,
        _ => return true,
    };

    // Ensure indices are valid
    if row_idx_a >= partition.len() || row_idx_b >= partition.len() {
        return false;
    }

    let row_a = &partition.rows[row_idx_a];
    let row_b = &partition.rows[row_idx_b];

    // Compare all ORDER BY expressions
    for order_item in order_items {
        let val_a = eval_fn(&order_item.expr, row_a).unwrap_or(SqlValue::Null);
        let val_b = eval_fn(&order_item.expr, row_b).unwrap_or(SqlValue::Null);

        if compare_values(&val_a, &val_b) != Ordering::Equal {
            return false;
        }
    }

    true
}
