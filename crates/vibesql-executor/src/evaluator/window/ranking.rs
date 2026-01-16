//! Ranking window functions
//!
//! Implements ROW_NUMBER, RANK, DENSE_RANK, and NTILE.

use std::cmp::Ordering;

use vibesql_ast::OrderByItem;
use vibesql_types::SqlValue;

use super::{partitioning::Partition, sorting::compare_values, utils::evaluate_expression_with_map};

/// Evaluate ROW_NUMBER() window function
///
/// Returns unique sequential integers starting from 1 for each row in the partition.
/// Ties (based on ORDER BY) get arbitrary but consistent ordering.
///
/// Example: [1, 2, 3, 4, 5] regardless of duplicate values
pub fn evaluate_row_number(partition: &Partition) -> Vec<SqlValue> {
    // ROW_NUMBER is simple: just assign 1, 2, 3, ... to each row
    (1..=partition.len()).map(|n| SqlValue::Integer(n as i64)).collect()
}

/// Evaluate RANK() window function
///
/// Returns rank with gaps when there are ties (based on ORDER BY).
/// Rows with equal ORDER BY values get the same rank.
/// Next rank after tie skips numbers.
///
/// Example for scores [95, 90, 90, 85]: ranks are [1, 2, 2, 4]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_rank(partition: &Partition, order_by: &Option<Vec<OrderByItem>>) -> Vec<SqlValue> {
    // RANK requires ORDER BY
    if order_by.is_none() || order_by.as_ref().unwrap().is_empty() {
        // Without ORDER BY, all rows get rank 1
        return vec![SqlValue::Integer(1); partition.len()];
    }

    let order_items = order_by.as_ref().unwrap();
    let mut ranks = Vec::new();
    let mut current_rank = 1i64;

    for (idx, row) in partition.rows.iter().enumerate() {
        if idx > 0 {
            // Compare current row with previous row
            let prev_row = &partition.rows[idx - 1];

            // Check if ORDER BY values differ
            let mut values_differ = false;
            for order_item in order_items {
                let val_curr = evaluate_expression_with_map(&order_item.expr, row, &partition.column_map).unwrap_or(SqlValue::Null);
                let val_prev =
                    evaluate_expression_with_map(&order_item.expr, prev_row, &partition.column_map).unwrap_or(SqlValue::Null);

                if compare_values(&val_curr, &val_prev) != Ordering::Equal {
                    values_differ = true;
                    break;
                }
            }

            if values_differ {
                // New rank group - rank becomes row number (1-indexed)
                current_rank = (idx + 1) as i64;
            }
            // else: same rank as previous row
        }

        ranks.push(SqlValue::Integer(current_rank));
    }

    ranks
}

/// Evaluate DENSE_RANK() window function
///
/// Returns rank without gaps when there are ties (based on ORDER BY).
/// Rows with equal ORDER BY values get the same rank.
/// Next rank continues sequentially (no gaps).
///
/// Example for scores [95, 90, 90, 85]: ranks are [1, 2, 2, 3]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_dense_rank(
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
) -> Vec<SqlValue> {
    // DENSE_RANK requires ORDER BY
    if order_by.is_none() || order_by.as_ref().unwrap().is_empty() {
        // Without ORDER BY, all rows get rank 1
        return vec![SqlValue::Integer(1); partition.len()];
    }

    let order_items = order_by.as_ref().unwrap();
    let mut ranks = Vec::new();
    let mut current_rank = 1i64;

    for (idx, row) in partition.rows.iter().enumerate() {
        if idx > 0 {
            // Compare current row with previous row
            let prev_row = &partition.rows[idx - 1];

            // Check if ORDER BY values differ
            let mut values_differ = false;
            for order_item in order_items {
                let val_curr = evaluate_expression_with_map(&order_item.expr, row, &partition.column_map).unwrap_or(SqlValue::Null);
                let val_prev =
                    evaluate_expression_with_map(&order_item.expr, prev_row, &partition.column_map).unwrap_or(SqlValue::Null);

                if compare_values(&val_curr, &val_prev) != Ordering::Equal {
                    values_differ = true;
                    break;
                }
            }

            if values_differ {
                // New rank group - increment rank by 1 (dense, no gaps)
                current_rank += 1;
            }
            // else: same rank as previous row
        }

        ranks.push(SqlValue::Integer(current_rank));
    }

    ranks
}

/// Evaluate PERCENT_RANK() window function
///
/// Returns the relative rank of the current row: (rank - 1) / (partition_size - 1).
/// The first row always has percent_rank of 0.0.
/// The last row (or last tie group) has percent_rank of 1.0.
/// Returns 0.0 for single-row partitions.
///
/// Example for scores [95, 90, 90, 85]:
///   - ranks: [1, 2, 2, 4]
///   - percent_rank: [0.0, 0.333..., 0.333..., 1.0]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_percent_rank(
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
) -> Vec<SqlValue> {
    let n = partition.len();

    // Single row partition - return 0.0
    if n <= 1 {
        return vec![SqlValue::Numeric(0.0); n];
    }

    // Get ranks first (reuse RANK logic)
    let ranks = evaluate_rank(partition, order_by);

    // Convert ranks to percent_rank: (rank - 1) / (n - 1)
    let denominator = (n - 1) as f64;

    ranks
        .into_iter()
        .map(|rank| {
            if let SqlValue::Integer(r) = rank {
                SqlValue::Numeric((r - 1) as f64 / denominator)
            } else {
                SqlValue::Numeric(0.0)
            }
        })
        .collect()
}

/// Evaluate CUME_DIST() window function
///
/// Returns the cumulative distribution: the fraction of partition rows
/// that are less than or equal to the current row.
///
/// Formula: (number of rows <= current row) / (total rows in partition)
/// Value range: (0, 1] - always > 0, maximum is 1.0
///
/// Example for scores [95, 90, 90, 85] (sorted DESC):
///   - row 1 (95): 1/4 = 0.25
///   - rows 2-3 (90): 3/4 = 0.75 (3 rows are <= 90)
///   - row 4 (85): 4/4 = 1.0
///   - cume_dist: [0.25, 0.75, 0.75, 1.0]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_cume_dist(
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
) -> Vec<SqlValue> {
    let n = partition.len();

    // Empty partition - shouldn't happen but handle gracefully
    if n == 0 {
        return vec![];
    }

    // Without ORDER BY, all rows are considered equal - cume_dist = 1.0 for all
    if order_by.is_none() || order_by.as_ref().unwrap().is_empty() {
        return vec![SqlValue::Numeric(1.0); n];
    }

    let order_items = order_by.as_ref().unwrap();
    let mut results = Vec::with_capacity(n);

    // For each row, count how many rows are <= current row
    // Due to sorting, we can track where the "group" ends
    let mut group_end_indices = Vec::new();
    let mut current_group_start = 0;

    for idx in 0..n {
        let is_last_row = idx == n - 1;
        let is_new_group = if is_last_row {
            true // Last row always ends a group
        } else {
            // Check if next row differs from current
            let curr_row = &partition.rows[idx];
            let next_row = &partition.rows[idx + 1];

            let mut differs = false;
            for order_item in order_items {
                let val_curr =
                    evaluate_expression_with_map(&order_item.expr, curr_row, &partition.column_map).unwrap_or(SqlValue::Null);
                let val_next =
                    evaluate_expression_with_map(&order_item.expr, next_row, &partition.column_map).unwrap_or(SqlValue::Null);

                if compare_values(&val_curr, &val_next) != Ordering::Equal {
                    differs = true;
                    break;
                }
            }
            differs
        };

        if is_new_group {
            // End of a tie group at index idx
            let group_end = idx + 1; // 1-based count of rows <= current

            // All rows from current_group_start to idx get the same cume_dist
            for _ in current_group_start..=idx {
                group_end_indices.push(group_end);
            }
            current_group_start = idx + 1;
        }
    }

    // Convert to cumulative distribution values
    let denominator = n as f64;
    for group_end in group_end_indices {
        results.push(SqlValue::Numeric(group_end as f64 / denominator));
    }

    results
}

/// Evaluate NTILE(n) window function
///
/// Divides partition into n approximately equal groups (buckets/tiles).
/// Returns the group number (1 to n) for each row.
///
/// If rows don't divide evenly, earlier groups get one extra row.
/// Example: 10 rows with NTILE(4) → groups of [3, 3, 2, 2] rows
///
/// Example: NTILE(4) on 8 rows → [1, 1, 2, 2, 3, 3, 4, 4]
/// Example: NTILE(4) on 10 rows → [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
pub fn evaluate_ntile(partition: &Partition, n: i64) -> Result<Vec<SqlValue>, String> {
    if n <= 0 {
        return Err("argument of ntile must be a positive integer".to_string());
    }

    let total_rows = partition.len();
    let n_buckets = n as usize;

    // If n >= total_rows, each row gets its own bucket
    if n_buckets >= total_rows {
        return Ok((1..=total_rows).map(|i| SqlValue::Integer(i as i64)).collect());
    }

    // Calculate bucket sizes
    // Base size: total_rows / n_buckets (integer division)
    // Remainder: total_rows % n_buckets
    // First 'remainder' buckets get (base_size + 1) rows
    // Remaining buckets get base_size rows

    let base_size = total_rows / n_buckets;
    let remainder = total_rows % n_buckets;

    let mut bucket_numbers = Vec::with_capacity(total_rows);
    let mut current_bucket = 1i64;
    let mut rows_in_current_bucket = 0;

    for _ in 0..total_rows {
        bucket_numbers.push(SqlValue::Integer(current_bucket));
        rows_in_current_bucket += 1;

        // Calculate size of current bucket
        // First 'remainder' buckets get (base_size + 1) rows
        let bucket_size =
            if (current_bucket as usize) <= remainder { base_size + 1 } else { base_size };

        // Move to next bucket if current one is full
        if rows_in_current_bucket >= bucket_size {
            current_bucket += 1;
            rows_in_current_bucket = 0;
        }
    }

    Ok(bucket_numbers)
}
