//! Columnar Hash Join Implementation
//!
//! This module provides a high-performance hash join that operates entirely on
//! columnar data, avoiding row materialization overhead.
//!
//! ## Performance Characteristics
//!
//! - **Build phase**: O(n) with SIMD hash computation on contiguous arrays
//! - **Probe phase**: O(m) with SIMD hash computation and vectorized lookups
//! - **Memory**: O(n) for hash table + O(result_size) for output
//!
//! ## Key Optimizations
//!
//! 1. **No SqlValue enum dispatch**: Operates directly on typed arrays (i64, f64, etc.)
//! 2. **SIMD hashing**: Hashes 4-8 values simultaneously
//! 3. **Cache-friendly**: Contiguous memory access patterns
//! 4. **Deferred materialization**: Only creates rows at output if needed
//!
//! Note: This module is experimental/research code. Some functions are not yet
//! integrated into the main execution path.
//!
//! ## Module Structure
//!
//! - `hash_table`: Hash table implementations for single and multi-column keys
//! - `probe`: Probe phase implementations for inner and outer joins
//! - `output`: Result construction and column gathering
//! - `row_extract`: Utilities for extracting columnar data from row-based inputs

#![allow(dead_code)]

mod hash_table;
mod output;
mod probe;
mod row_extract;

use crate::errors::ExecutorError;
use crate::select::columnar::ColumnarBatch;

// Re-export public types
pub use row_extract::{hash_join_indices_columnar, hash_join_indices_columnar_multi};

/// Execute a columnar inner hash join
///
/// This function operates entirely on columnar data without materializing rows.
///
/// # Arguments
/// * `left_batch` - Left (probe) side columnar batch
/// * `right_batch` - Right (build) side columnar batch
/// * `left_key_idx` - Column index of join key in left batch
/// * `right_key_idx` - Column index of join key in right batch
///
/// # Returns
/// A new ColumnarBatch containing joined columns
pub fn columnar_hash_join_inner(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    left_key_idx: usize,
    right_key_idx: usize,
) -> Result<ColumnarBatch, ExecutorError> {
    // Get key columns
    let left_key =
        left_batch.column(left_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_key_idx,
            batch_columns: left_batch.column_count(),
        })?;
    let right_key =
        right_batch.column(right_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_key_idx,
            batch_columns: right_batch.column_count(),
        })?;

    // Build hash table on right (smaller side ideally)
    // For now, always build on right - could optimize to choose smaller side
    let hash_table = hash_table::ColumnarHashTable::build_from_column(right_key)?;

    // Probe and collect matching indices
    let join_indices = probe::probe_columnar(&hash_table, left_key, right_key)?;

    // Gather result columns
    output::gather_join_result(left_batch, right_batch, &join_indices)
}

/// Execute a columnar LEFT OUTER hash join
///
/// This function operates entirely on columnar data without materializing rows.
/// LEFT OUTER JOIN preserves all rows from the left (probe) side, outputting
/// NULL values for right columns when there's no match.
///
/// # Arguments
/// * `left_batch` - Left (probe) side columnar batch - all rows preserved
/// * `right_batch` - Right (build) side columnar batch
/// * `left_key_idx` - Column index of join key in left batch
/// * `right_key_idx` - Column index of join key in right batch
///
/// # Returns
/// A new ColumnarBatch containing joined columns with left rows preserved
pub fn columnar_hash_join_left_outer(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    left_key_idx: usize,
    right_key_idx: usize,
) -> Result<ColumnarBatch, ExecutorError> {
    // Get key columns
    let left_key =
        left_batch.column(left_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_key_idx,
            batch_columns: left_batch.column_count(),
        })?;
    let right_key =
        right_batch.column(right_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_key_idx,
            batch_columns: right_batch.column_count(),
        })?;

    // Build hash table on right side
    let hash_table = hash_table::ColumnarHashTable::build_from_column(right_key)?;

    // Probe and collect matching indices, tracking unmatched left rows
    let join_indices =
        probe::probe_columnar_left_outer(&hash_table, left_key, right_key, left_batch.row_count())?;

    // Gather result columns with NULL handling for unmatched rows
    output::gather_left_outer_result(left_batch, right_batch, &join_indices)
}

/// Execute a columnar RIGHT OUTER hash join
///
/// This function operates entirely on columnar data without materializing rows.
/// RIGHT OUTER JOIN preserves all rows from the right (build) side, outputting
/// NULL values for left columns when there's no match.
///
/// # Arguments
/// * `left_batch` - Left (probe) side columnar batch
/// * `right_batch` - Right (build) side columnar batch - all rows preserved
/// * `left_key_idx` - Column index of join key in left batch
/// * `right_key_idx` - Column index of join key in right batch
///
/// # Returns
/// A new ColumnarBatch containing joined columns with right rows preserved
pub fn columnar_hash_join_right_outer(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    left_key_idx: usize,
    right_key_idx: usize,
) -> Result<ColumnarBatch, ExecutorError> {
    // Get key columns
    let left_key =
        left_batch.column(left_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_key_idx,
            batch_columns: left_batch.column_count(),
        })?;
    let right_key =
        right_batch.column(right_key_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_key_idx,
            batch_columns: right_batch.column_count(),
        })?;

    // Build hash table on left side (reverse of inner join)
    let hash_table = hash_table::ColumnarHashTable::build_from_column(left_key)?;

    // Probe with right side and track unmatched right rows
    let join_indices = probe::probe_columnar_right_outer(
        &hash_table,
        right_key,
        left_key,
        right_batch.row_count(),
    )?;

    // Gather result columns with NULL handling for unmatched rows
    output::gather_right_outer_result(left_batch, right_batch, &join_indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::columnar::ColumnArray;
    use std::sync::Arc;

    #[test]
    fn test_columnar_hash_join() {
        // Create left batch: customer_id, name
        let left_columns = vec![
            ColumnArray::Int64(Arc::new(vec![1, 2, 3, 4]), None),
            ColumnArray::String(
                Arc::new(vec!["Alice".into(), "Bob".into(), "Carol".into(), "Dave".into()]),
                None,
            ),
        ];
        let left_batch = ColumnarBatch::from_columns(
            left_columns,
            Some(vec!["customer_id".into(), "name".into()]),
        )
        .unwrap();

        // Create right batch: order_id, customer_id, amount
        let right_columns = vec![
            ColumnArray::Int64(Arc::new(vec![101, 102, 103, 104, 105]), None),
            ColumnArray::Int64(Arc::new(vec![1, 2, 1, 3, 2]), None),
            ColumnArray::Float64(Arc::new(vec![100.0, 200.0, 150.0, 300.0, 250.0]), None),
        ];
        let right_batch = ColumnarBatch::from_columns(
            right_columns,
            Some(vec!["order_id".into(), "customer_id".into(), "amount".into()]),
        )
        .unwrap();

        // Join on customer_id (left col 0 = right col 1)
        let result = columnar_hash_join_inner(&left_batch, &right_batch, 0, 1).unwrap();

        // Should have 5 result rows (Alice has 2 orders, Bob has 2, Carol has 1)
        assert_eq!(result.row_count(), 5);

        // Should have 5 columns (2 from left + 3 from right)
        assert_eq!(result.column_count(), 5);
    }

    #[test]
    fn test_columnar_hash_join_left_outer() {
        // Create left batch: customer_id, name (4 customers)
        let left_columns = vec![
            ColumnArray::Int64(Arc::new(vec![1, 2, 3, 4]), None),
            ColumnArray::String(
                Arc::new(vec!["Alice".into(), "Bob".into(), "Carol".into(), "Dave".into()]),
                None,
            ),
        ];
        let left_batch = ColumnarBatch::from_columns(
            left_columns,
            Some(vec!["customer_id".into(), "name".into()]),
        )
        .unwrap();

        // Create right batch: order_id, customer_id (only customers 1, 2, 3 have orders)
        let right_columns = vec![
            ColumnArray::Int64(Arc::new(vec![101, 102, 103]), None),
            ColumnArray::Int64(Arc::new(vec![1, 2, 1]), None), // Dave (id=4) has no orders
        ];
        let right_batch = ColumnarBatch::from_columns(
            right_columns,
            Some(vec!["order_id".into(), "customer_id".into()]),
        )
        .unwrap();

        // LEFT OUTER JOIN on customer_id (left col 0 = right col 1)
        let result = columnar_hash_join_left_outer(&left_batch, &right_batch, 0, 1).unwrap();

        // Should have 5 result rows:
        // - Alice (1): 2 matches (101, 103)
        // - Bob (2): 1 match (102)
        // - Carol (3): 0 matches, but preserved with NULLs
        // - Dave (4): 0 matches, but preserved with NULLs
        assert_eq!(result.row_count(), 5);
        assert_eq!(result.column_count(), 4); // 2 left + 2 right

        // Verify that all left rows are preserved
        let rows = result.to_rows().unwrap();

        // Count customers preserved
        let mut customer_counts = std::collections::HashMap::new();
        for row in &rows {
            if let Some(vibesql_types::SqlValue::Integer(id)) = row.get(0) {
                *customer_counts.entry(*id).or_insert(0) += 1;
            }
        }

        // Alice should appear 2 times, Bob 1 time, Carol 1 time, Dave 1 time
        assert_eq!(customer_counts.get(&1), Some(&2)); // Alice
        assert_eq!(customer_counts.get(&2), Some(&1)); // Bob
        assert_eq!(customer_counts.get(&3), Some(&1)); // Carol (preserved with NULL)
        assert_eq!(customer_counts.get(&4), Some(&1)); // Dave (preserved with NULL)
    }

    #[test]
    fn test_columnar_hash_join_right_outer() {
        // Create left batch: customer_id, name (2 customers)
        let left_columns = vec![
            ColumnArray::Int64(Arc::new(vec![1, 2]), None),
            ColumnArray::String(Arc::new(vec!["Alice".into(), "Bob".into()]), None),
        ];
        let left_batch = ColumnarBatch::from_columns(
            left_columns,
            Some(vec!["customer_id".into(), "name".into()]),
        )
        .unwrap();

        // Create right batch: order_id, customer_id (customer 3 has no matching left row)
        let right_columns = vec![
            ColumnArray::Int64(Arc::new(vec![101, 102, 103, 104]), None),
            ColumnArray::Int64(Arc::new(vec![1, 2, 3, 1]), None), // Order 103 has customer_id=3, not in left
        ];
        let right_batch = ColumnarBatch::from_columns(
            right_columns,
            Some(vec!["order_id".into(), "customer_id".into()]),
        )
        .unwrap();

        // RIGHT OUTER JOIN on customer_id (left col 0 = right col 1)
        let result = columnar_hash_join_right_outer(&left_batch, &right_batch, 0, 1).unwrap();

        // Should have 4 result rows (all right rows preserved):
        // - Order 101 (customer 1): matches Alice
        // - Order 102 (customer 2): matches Bob
        // - Order 103 (customer 3): no match, left columns are NULL
        // - Order 104 (customer 1): matches Alice
        assert_eq!(result.row_count(), 4);
        assert_eq!(result.column_count(), 4); // 2 left + 2 right

        // Verify that all right rows are preserved
        let rows = result.to_rows().unwrap();

        // Count by order_id (column 2 in result)
        let mut order_found = std::collections::HashSet::new();
        let mut null_customer_count = 0;

        for row in &rows {
            if let Some(vibesql_types::SqlValue::Integer(order_id)) = row.get(2) {
                order_found.insert(*order_id);
            }
            if let Some(vibesql_types::SqlValue::Null) = row.get(0) {
                null_customer_count += 1;
            }
        }

        // All 4 orders should be present
        assert!(order_found.contains(&101));
        assert!(order_found.contains(&102));
        assert!(order_found.contains(&103));
        assert!(order_found.contains(&104));

        // One row should have NULL customer (order 103)
        assert_eq!(null_customer_count, 1);
    }

    #[test]
    fn test_columnar_hash_join_with_nulls() {
        // Create left batch with NULL key
        let left_columns = vec![
            ColumnArray::Int64(
                Arc::new(vec![1, 0, 3]),                  // 0 is placeholder for NULL
                Some(Arc::new(vec![false, true, false])), // Index 1 is NULL
            ),
            ColumnArray::String(Arc::new(vec!["Alice".into(), "Bob".into(), "Carol".into()]), None),
        ];
        let left_batch =
            ColumnarBatch::from_columns(left_columns, Some(vec!["id".into(), "name".into()]))
                .unwrap();

        // Create right batch
        let right_columns = vec![
            ColumnArray::Int64(Arc::new(vec![1, 3]), None),
            ColumnArray::String(Arc::new(vec!["Order1".into(), "Order3".into()]), None),
        ];
        let right_batch =
            ColumnarBatch::from_columns(right_columns, Some(vec!["id".into(), "desc".into()]))
                .unwrap();

        // LEFT OUTER JOIN - Bob (NULL key) should be preserved with NULL right columns
        let result = columnar_hash_join_left_outer(&left_batch, &right_batch, 0, 0).unwrap();

        // Should have 3 rows: Alice matches, Bob (NULL key) preserved, Carol matches
        assert_eq!(result.row_count(), 3);

        // Verify Bob's row has NULL for right side
        let rows = result.to_rows().unwrap();
        let bob_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Some(vibesql_types::SqlValue::Varchar(s)) if s.as_str() == "Bob"));

        assert!(bob_row.is_some());
        let bob = bob_row.unwrap();
        // Bob's right-side columns should be NULL
        assert!(matches!(bob.get(2), Some(vibesql_types::SqlValue::Null)));
        assert!(matches!(bob.get(3), Some(vibesql_types::SqlValue::Null)));
    }
}
