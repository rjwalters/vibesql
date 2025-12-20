//! Frame calculation for window functions
//!
//! Calculates frame boundaries (ROWS mode) for window function evaluation.
//! Supports SQL:2011 EXCLUDE clause for excluding rows from the frame.

use std::cmp::Ordering;
use std::ops::Range;

use vibesql_ast::{Expression, FrameBound, FrameExclude, FrameUnit, OrderByItem, WindowFrame};
use vibesql_types::SqlValue;

use super::partitioning::Partition;
use super::sorting::compare_values;
use super::utils::evaluate_expression;

/// Calculate frame boundaries for a given row in a partition
///
/// Returns a `Range<usize>` representing the [start, end) indices of rows in the frame.
/// Implements ROWS mode frame semantics.
pub fn calculate_frame(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
) -> Range<usize> {
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
                // Default with ORDER BY: start of partition to current row (inclusive)
                let result = 0..(current_row_idx + 1);
                return result;
            } else {
                // Default without ORDER BY: entire partition
                let result = 0..partition_size;
                return result;
            }
        }
    };

    // Only support ROWS mode for now
    if !matches!(frame.unit, FrameUnit::Rows) {
        // Fallback to default for unsupported RANGE mode
        return 0..(current_row_idx + 1);
    }

    // Calculate start boundary
    let start_idx = calculate_frame_boundary(&frame.start, current_row_idx, partition_size, true);

    // Calculate end boundary
    let end_idx = match &frame.end {
        Some(end_bound) => {
            calculate_frame_boundary(end_bound, current_row_idx, partition_size, false)
        }
        None => current_row_idx + 1, // Default: CURRENT ROW (inclusive, so +1 for Range)
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate a single frame boundary (start or end)
///
/// Returns the index for the boundary.
/// For start boundaries, returns inclusive index.
/// For end boundaries, returns exclusive index (Range semantics).
fn calculate_frame_boundary(
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
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0, // Fallback for non-constant (should not happen after validation)
            };

            current_row_idx.saturating_sub(offset)
        }

        FrameBound::Following(offset_expr) => {
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0,
            };

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
    pub fn includes(
        &self,
        row_idx: usize,
        partition: &Partition,
        order_by: &Option<Vec<OrderByItem>>,
    ) -> bool {
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
                !is_peer(row_idx, self.current_row_idx, partition, order_by)
            }

            Some(FrameExclude::Ties) => {
                // Exclude peers of current row, but include current row itself
                row_idx == self.current_row_idx
                    || !is_peer(row_idx, self.current_row_idx, partition, order_by)
            }
        }
    }

    /// Get an iterator over all included row indices
    ///
    /// This filters out excluded rows based on the EXCLUDE clause.
    pub fn included_indices<'a>(
        &'a self,
        partition: &'a Partition,
        order_by: &'a Option<Vec<OrderByItem>>,
    ) -> impl Iterator<Item = usize> + 'a {
        self.range.clone().filter(move |&idx| self.includes(idx, partition, order_by))
    }
}

/// Calculate frame with exclusion information
///
/// Returns a FrameResult that includes both the range and exclusion info.
pub fn calculate_frame_with_exclusion(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
) -> FrameResult {
    let range = calculate_frame(partition, current_row_idx, order_by, frame_spec);
    let exclude = frame_spec.as_ref().and_then(|f| f.exclude);

    FrameResult { range, exclude, current_row_idx }
}

/// Check if two rows are peers (have same ORDER BY values)
///
/// Rows are considered peers if they have identical values for all ORDER BY expressions.
/// If there's no ORDER BY clause, all rows are considered peers.
fn is_peer(
    row_idx_a: usize,
    row_idx_b: usize,
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
) -> bool {
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
        let val_a = evaluate_expression(&order_item.expr, row_a).unwrap_or(SqlValue::Null);
        let val_b = evaluate_expression(&order_item.expr, row_b).unwrap_or(SqlValue::Null);

        if compare_values(&val_a, &val_b) != Ordering::Equal {
            return false;
        }
    }

    true
}
