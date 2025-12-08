// ============================================================================
// Reverse Scan - Reverse iteration for DESC ORDER BY optimization
// ============================================================================
//
// This module provides reverse iteration methods for BTreeMap indexes to
// efficiently support `ORDER BY col DESC LIMIT N` queries.
//
// Instead of fetching all matching rows and reversing the vector, these methods
// iterate through the index in descending key order, allowing early termination
// when combined with LIMIT pushdown.

use std::ops::Bound;
use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::range_bounds::try_increment_sqlvalue;
use super::value_normalization::normalize_for_comparison;

/// Helper to compute the exclusive upper bound for a prefix scan
fn compute_prefix_upper_bound(prefix: &[SqlValue]) -> Option<Vec<SqlValue>> {
    if prefix.is_empty() {
        return None;
    }

    let mut upper_bound = prefix.to_vec();
    let last_idx = upper_bound.len() - 1;

    match try_increment_sqlvalue(&upper_bound[last_idx]) {
        Some(incremented) => {
            upper_bound[last_idx] = incremented;
            Some(upper_bound)
        }
        None => None,
    }
}

impl IndexData {
    /// Lookup rows matching a multi-column prefix in reverse (descending) order
    ///
    /// This is the reverse counterpart of `prefix_scan()`. It returns rows in
    /// descending key order, which is essential for efficiently handling
    /// `ORDER BY col DESC LIMIT N` queries.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns
    ///
    /// # Returns
    /// Vector of row indices matching the prefix, in descending key order
    ///
    /// # Performance
    /// For InMemory indexes, uses BTreeMap's efficient `range().rev()` for O(log n + k)
    /// complexity. For DiskBacked indexes, falls back to forward scan + reverse (less
    /// optimal but correct).
    ///
    /// # Example
    /// ```text
    /// // Index on (w_id, d_id, o_id) - 3 columns
    /// // Find all rows where w_id=1 AND d_id=5 in DESCENDING o_id order
    /// let rows = index_data.prefix_scan_reverse(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    /// // rows[0] has the highest o_id, rows[last] has the lowest
    /// ```
    pub fn prefix_scan_reverse(&self, prefix: &[SqlValue]) -> Vec<usize> {
        // Normalize prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Handle empty prefix - return all rows in reverse order
                if normalized_prefix.is_empty() {
                    let mut all_rows: Vec<usize> = Vec::new();
                    for row_indices in data.values().rev() {
                        all_rows.extend(row_indices.iter().rev());
                    }
                    // Apply lazy adjustment for pending deletions
                    if !pending_deletions.is_empty() {
                        for row_idx in &mut all_rows {
                            let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                            *row_idx -= decrement;
                        }
                    }
                    return all_rows;
                }

                // Calculate upper bound by incrementing the last element of the prefix
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let mut matching_row_indices = Vec::new();

                // Use .rev() to iterate in descending order
                for (key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound)).rev()
                {
                    // Double-check prefix match (needed for Unbounded end bound case)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        // Also reverse the row_indices within each key for full DESC order
                        matching_row_indices.extend(row_indices.iter().rev());
                    }
                }

                // Apply lazy adjustment for pending deletions
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
                    }
                }

                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Use the efficient range_scan_reverse with prev_leaf pointers
                let (start_key, end_key) = if normalized_prefix.is_empty() {
                    (None, None)
                } else {
                    (
                        Some(normalized_prefix.clone()),
                        compute_prefix_upper_bound(&normalized_prefix),
                    )
                };

                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        guard
                            .range_scan_reverse(
                                start_key.as_ref(),
                                end_key.as_ref(),
                                true,  // Inclusive start
                                false, // Exclusive end
                            )
                            .unwrap_or_else(|_| vec![])
                    }
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_scan_reverse: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support reverse scans - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support reverse scans - use search() method instead
                vec![]
            }
        }
    }

    /// Lookup rows matching a multi-column prefix in reverse order with a limit
    ///
    /// This method combines reverse iteration with early termination, making it
    /// highly efficient for `ORDER BY col DESC LIMIT N` queries.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns
    /// * `limit` - Maximum number of rows to return
    ///
    /// # Returns
    /// Vector of up to `limit` row indices matching the prefix, in descending key order
    ///
    /// # Performance
    /// For InMemory indexes, this is O(log n + limit) instead of O(log n + k) where k
    /// is total matching rows. For queries like `ORDER BY o_id DESC LIMIT 1`, this
    /// returns after finding just the first (highest) matching key.
    ///
    /// # Example
    /// ```text
    /// // Find the most recent order for customer (w_id=1, d_id=2, c_id=3)
    /// // Index on (o_w_id, o_d_id, o_c_id, o_id) - composite index
    /// let rows = index_data.prefix_scan_reverse_limit(
    ///     &[SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)],
    ///     1  // Only need the first (highest o_id)
    /// );
    /// ```
    pub fn prefix_scan_reverse_limit(&self, prefix: &[SqlValue], limit: usize) -> Vec<usize> {
        if limit == 0 {
            return vec![];
        }

        // Normalize prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Helper closure to apply pending deletions adjustment
                let apply_adjustment = |result: &mut Vec<usize>| {
                    if !pending_deletions.is_empty() {
                        for row_idx in result.iter_mut() {
                            let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                            *row_idx -= decrement;
                        }
                    }
                };

                // Handle empty prefix - take last `limit` rows from entire index
                if normalized_prefix.is_empty() {
                    let mut result = Vec::with_capacity(limit);
                    for row_indices in data.values().rev() {
                        for &row_idx in row_indices.iter().rev() {
                            result.push(row_idx);
                            if result.len() >= limit {
                                apply_adjustment(&mut result);
                                return result;
                            }
                        }
                    }
                    apply_adjustment(&mut result);
                    return result;
                }

                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let mut matching_row_indices = Vec::with_capacity(limit);

                // Iterate in reverse with early termination
                for (key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound)).rev()
                {
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        // Reverse row_indices and take only what we need
                        for &row_idx in row_indices.iter().rev() {
                            matching_row_indices.push(row_idx);
                            if matching_row_indices.len() >= limit {
                                apply_adjustment(&mut matching_row_indices);
                                return matching_row_indices;
                            }
                        }
                    }
                }

                apply_adjustment(&mut matching_row_indices);
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Use the efficient range_scan_reverse with prev_leaf pointers
                let (start_key, end_key) = if normalized_prefix.is_empty() {
                    (None, None)
                } else {
                    (
                        Some(normalized_prefix.clone()),
                        compute_prefix_upper_bound(&normalized_prefix),
                    )
                };

                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        let results = guard
                            .range_scan_reverse(start_key.as_ref(), end_key.as_ref(), true, false)
                            .unwrap_or_else(|_| vec![]);

                        // Take first `limit` elements (they have the highest keys in DESC order)
                        results.into_iter().take(limit).collect()
                    }
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_scan_reverse_limit: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support reverse scans with limit - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support reverse scans with limit - use search() method instead
                vec![]
            }
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Helper to create an InMemory IndexData with test data
    fn create_test_index_data(entries: Vec<(Vec<SqlValue>, Vec<usize>)>) -> IndexData {
        let mut data = BTreeMap::new();
        for (key, row_indices) in entries {
            let normalized_key: Vec<SqlValue> = key.iter().map(normalize_for_comparison).collect();
            data.insert(normalized_key, row_indices);
        }
        IndexData::InMemory {
            data,
            pending_deletions: Vec::new(),
        }
    }

    // ========================================================================
    // prefix_scan_reverse() Tests
    // ========================================================================

    #[test]
    fn test_prefix_scan_reverse_basic() {
        // Index on (a, b) - look for rows where a=1 in reverse order
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![3]),
        ]);

        // prefix_scan returns [0, 1, 2] (ascending b order)
        let forward = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert_eq!(forward, vec![0, 1, 2]);

        // prefix_scan_reverse returns [2, 1, 0] (descending b order)
        let reverse = index.prefix_scan_reverse(&[SqlValue::Integer(1)]);
        assert_eq!(reverse, vec![2, 1, 0]);
    }

    #[test]
    fn test_prefix_scan_reverse_two_column_prefix() {
        // Index on (w_id, d_id, o_id) - like TPC-C ORDER table
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(200)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(300)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(6), SqlValue::Integer(100)], vec![3]),
        ]);

        // Look for w_id=1, d_id=5, expecting o_id in descending order
        let reverse = index.prefix_scan_reverse(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
        assert_eq!(reverse, vec![2, 1, 0]); // o_id 300, 200, 100
    }

    #[test]
    fn test_prefix_scan_reverse_no_match() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![0],
        )]);

        let reverse = index.prefix_scan_reverse(&[SqlValue::Integer(999)]);
        assert!(reverse.is_empty());
    }

    #[test]
    fn test_prefix_scan_reverse_multiple_rows_per_key() {
        // Non-unique index: multiple row indices per key
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0, 1, 2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![3, 4]),
        ]);

        let reverse = index.prefix_scan_reverse(&[SqlValue::Integer(1)]);
        // Key [1,20] reversed: [4, 3], then key [1,10] reversed: [2, 1, 0]
        assert_eq!(reverse, vec![4, 3, 2, 1, 0]);
    }

    // ========================================================================
    // prefix_scan_reverse_limit() Tests
    // ========================================================================

    #[test]
    fn test_prefix_scan_reverse_limit_basic() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(40)], vec![3]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(50)], vec![4]),
        ]);

        // Get only top 2 (highest keys)
        let result = index.prefix_scan_reverse_limit(&[SqlValue::Integer(1)], 2);
        assert_eq!(result, vec![4, 3]); // o_id 50, 40
    }

    #[test]
    fn test_prefix_scan_reverse_limit_one() {
        // Simulates TPC-C Order Status: find highest o_id for customer
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(3),
                    SqlValue::Integer(100),
                ],
                vec![0],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(3),
                    SqlValue::Integer(101),
                ],
                vec![1],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(3),
                    SqlValue::Integer(102),
                ],
                vec![2],
            ),
        ]);

        // Find most recent order for customer (w_id=1, d_id=2, c_id=3)
        let result = index.prefix_scan_reverse_limit(
            &[SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)],
            1,
        );
        assert_eq!(result, vec![2]); // o_id 102 (highest)
    }

    #[test]
    fn test_prefix_scan_reverse_limit_exceeds_matches() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        ]);

        // Request more than available
        let result = index.prefix_scan_reverse_limit(&[SqlValue::Integer(1)], 100);
        assert_eq!(result, vec![1, 0]); // All matches, in reverse order
    }

    #[test]
    fn test_prefix_scan_reverse_limit_zero() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![0],
        )]);

        let result = index.prefix_scan_reverse_limit(&[SqlValue::Integer(1)], 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_prefix_scan_reverse_limit_with_duplicates() {
        // Non-unique index with limit
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0, 1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![2, 3, 4]),
        ]);

        // Limit=3: should get [4, 3, 2] from key [1,20]
        let result = index.prefix_scan_reverse_limit(&[SqlValue::Integer(1)], 3);
        assert_eq!(result, vec![4, 3, 2]);

        // Limit=4: should get [4, 3, 2, 1] (all from [1,20] + first from [1,10])
        let result = index.prefix_scan_reverse_limit(&[SqlValue::Integer(1)], 4);
        assert_eq!(result, vec![4, 3, 2, 1]);
    }

    #[test]
    fn test_prefix_scan_reverse_tpcc_order_status() {
        // Realistic TPC-C scenario: ORDER table index on (O_W_ID, O_D_ID, O_C_ID, O_ID)
        // Customer has orders with o_id: 2991, 2992, 2993, 2994, 2995
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Integer(1),  // w_id
                    SqlValue::Integer(2),  // d_id
                    SqlValue::Integer(42), // c_id
                    SqlValue::Integer(2991),
                ],
                vec![100],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(42),
                    SqlValue::Integer(2992),
                ],
                vec![101],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(42),
                    SqlValue::Integer(2993),
                ],
                vec![102],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(42),
                    SqlValue::Integer(2994),
                ],
                vec![103],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(42),
                    SqlValue::Integer(2995),
                ],
                vec![104],
            ),
        ]);

        // ORDER STATUS query: find most recent order for customer
        let result = index.prefix_scan_reverse_limit(
            &[SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(42)],
            1,
        );

        // Should return row 104 (o_id=2995, the most recent)
        assert_eq!(result, vec![104]);
    }
}
