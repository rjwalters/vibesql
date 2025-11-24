//! Hash-based GROUP BY aggregation
//!
//! This module implements efficient hash-based grouping for columnar data,
//! enabling queries like TPC-H Q1 to use the columnar execution path.

use std::collections::HashMap;

use crate::errors::ExecutorError;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::super::scan::ColumnarScan;
use super::functions::compute_columnar_aggregate_impl;
use super::AggregateOp;

/// Compute aggregates with GROUP BY using columnar execution
///
/// This function implements hash-based grouping on columnar data, enabling
/// TPC-H Q1 and similar queries to use the columnar execution path.
///
/// # Algorithm
///
/// 1. Build hash table mapping group keys → (row indices in that group)
/// 2. For each group, compute aggregates over the grouped rows
/// 3. Return results as rows with (group_key_cols, aggregate_cols)
///
/// # Arguments
///
/// * `rows` - Input rows to group and aggregate
/// * `group_cols` - Indices of columns to group by
/// * `agg_cols` - List of (column_index, aggregate_op) pairs to compute
/// * `filter_bitmap` - Optional filter to apply before grouping
///
/// # Returns
///
/// Vec of Row objects, each containing group key values followed by aggregate results
///
/// # Example
///
/// ```rust,ignore
/// // SELECT l_returnflag, SUM(l_extendedprice)
/// // FROM lineitem
/// // GROUP BY l_returnflag
///
/// let rows = vec![
///     Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(100.0)]),
///     Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Double(200.0)]),
///     Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(150.0)]),
/// ];
///
/// let group_cols = vec![0]; // Group by first column (l_returnflag)
/// let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(l_extendedprice)
///
/// let result = columnar_group_by(&rows, &group_cols, &agg_cols, None)?;
/// // Returns:
/// // Row["A", 250.0]
/// // Row["B", 200.0]
/// ```
pub fn columnar_group_by(
    rows: &[Row],
    group_cols: &[usize],
    agg_cols: &[(usize, AggregateOp)],
    filter_bitmap: Option<&[bool]>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Create columnar scan for efficient column access
    let scan = ColumnarScan::new(rows);

    // Phase 1: Build hash table mapping group keys to row indices
    // HashMap<Vec<SqlValue>, Vec<usize>>
    // Key: group key values, Value: indices of rows in that group
    let mut groups: HashMap<Vec<SqlValue>, Vec<usize>> = HashMap::new();

    for row_idx in 0..rows.len() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract group key values for this row
        let mut group_key = Vec::with_capacity(group_cols.len());
        for &col_idx in group_cols {
            let value = scan.row(row_idx)
                .and_then(|row| row.get(col_idx))
                .unwrap_or(&SqlValue::Null);
            group_key.push(value.clone());
        }

        // Add row index to this group
        groups.entry(group_key).or_default().push(row_idx);
    }

    // Phase 2: Compute aggregates for each group
    let mut result_rows = Vec::with_capacity(groups.len());

    // Reuse a single bitmap buffer to avoid repeated allocations
    // This is much more efficient than allocating rows.len() booleans per group
    let mut group_bitmap = vec![false; rows.len()];

    for (group_key, row_indices) in groups {
        // Set bits for this group's rows
        for &idx in &row_indices {
            group_bitmap[idx] = true;
        }

        // Compute aggregates for this group
        let mut result_values = Vec::with_capacity(group_key.len() + agg_cols.len());

        // First, add group key values
        result_values.extend(group_key);

        // Then, compute each aggregate
        for (col_idx, agg_op) in agg_cols {
            let agg_result = compute_columnar_aggregate_impl(&scan, *col_idx, *agg_op, Some(&group_bitmap))?;
            result_values.push(agg_result);
        }

        result_rows.push(Row::new(result_values));

        // Clear bitmap for next group (faster than allocating a new one)
        for &idx in &row_indices {
            group_bitmap[idx] = false;
        }
    }

    Ok(result_rows)
}
