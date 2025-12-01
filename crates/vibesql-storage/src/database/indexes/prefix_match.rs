// ============================================================================
// Prefix Match - Multi-column index prefix matching
// ============================================================================

use std::ops::Bound;
use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::value_normalization::normalize_for_comparison;

impl IndexData {
    /// Lookup multiple values using prefix matching for multi-column indexes
    ///
    /// This method is designed for multi-column indexes where we want to match on the
    /// first column only. For example, with index on (a, b) and query `WHERE a IN (10, 20)`,
    /// this will find all rows where `a=10` OR `a=20`, regardless of the value of `b`.
    ///
    /// # Arguments
    /// * `values` - List of values for the first indexed column
    ///
    /// # Returns
    /// Vector of row indices where the first column matches any of the values
    ///
    /// # Implementation Notes
    /// This uses the existing `range_scan()` method with start==end (equality check),
    /// which already has built-in prefix matching support for multi-column indexes.
    /// See `range_scan()` implementation for the prefix matching logic.
    ///
    /// This solves the issue where `multi_lookup([10])` would fail to match index keys
    /// like `[10, 20]` because BTreeMap requires exact key matches.
    pub fn prefix_multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        // Deduplicate values to avoid returning duplicate rows
        // For example, WHERE a IN (10, 10, 20) should only look up 10 once
        let mut unique_values: Vec<&SqlValue> = values.iter().collect();
        unique_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        unique_values.dedup();

        let mut matching_row_indices = Vec::new();

        for value in unique_values {
            // Use range_scan with start==end (both inclusive) to trigger prefix matching
            // The range_scan() implementation automatically handles multi-column indexes
            // by iterating through all keys where the first column matches 'value'
            let range_indices = self.range_scan(
                Some(value),  // start
                Some(value),  // end (same as start for equality/prefix matching)
                true,         // inclusive_start
                true,         // inclusive_end
            );

            matching_row_indices.extend(range_indices);
        }

        matching_row_indices
    }

    /// Scan index using multi-column prefix matching
    ///
    /// This method is designed for multi-column indexes where we want to match on
    /// the first N columns. For example, with index on (a, b, c) and query
    /// `WHERE a = 1 AND b = 2`, this will find all rows where `a=1` AND `b=2`,
    /// regardless of the value of `c`.
    ///
    /// # Arguments
    /// * `prefix` - Key prefix to match (e.g., [a_val, b_val] for 2-column prefix)
    ///
    /// # Returns
    /// Vector of row indices where the key prefix matches
    ///
    /// # Example
    /// ```rust,ignore
    /// // Index on (w_id, d_id, o_id) - 3 columns
    /// // Find all rows where w_id=1 AND d_id=5 (2-column prefix)
    /// let rows = index_data.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    /// ```
    ///
    /// # Implementation Notes
    /// Uses BTreeMap's lexicographic ordering: [1, 5] < [1, 5, 1] < [1, 5, 2] < [1, 6]
    /// We start at the prefix key and iterate while all prefix columns match.
    pub fn prefix_scan(&self, prefix: &[SqlValue]) -> Vec<usize> {
        if prefix.is_empty() {
            return Vec::new();
        }

        // Normalize all prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> = prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data } => {
                let mut matching_row_indices = Vec::new();

                // Start iteration at the prefix key using BTreeMap's efficient range()
                // Lexicographic ordering means [1, 5] < [1, 5, x] < [1, 6] for any x
                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());

                for (key_values, row_indices) in data.range::<[SqlValue], _>((start_bound, Bound::Unbounded)) {
                    // Check if the key starts with our prefix
                    if key_values.len() < normalized_prefix.len() {
                        // Key has fewer columns than prefix - can't match
                        break;
                    }

                    // Compare prefix columns
                    let matches = key_values[..normalized_prefix.len()]
                        .iter()
                        .zip(normalized_prefix.iter())
                        .all(|(k, p)| k == p);

                    if !matches {
                        // Prefix no longer matches - we've passed all matching keys
                        break;
                    }

                    matching_row_indices.extend(row_indices);
                }

                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed indexes, we need to use range_scan with calculated bounds
                // Strategy: Calculate the next prefix to use as exclusive upper bound
                //
                // For example, to find all rows where (a, b) = (1, 5) in index (a, b, c):
                //   Range: [1, 5] (inclusive) to [1, 6] (exclusive)
                //   This captures all keys like [1, 5, 1], [1, 5, 2], ..., [1, 5, 999]
                //   But excludes [1, 6, x] for any x

                // Calculate end bound by incrementing the last prefix element
                let end_key = {
                    let mut end = normalized_prefix.clone();
                    if let Some(last) = end.last_mut() {
                        // Try to increment the last value
                        if let Some(incremented) = super::range_bounds::try_increment_sqlvalue(last) {
                            *last = incremented;
                            Some(end)
                        } else {
                            // Can't increment (e.g., max integer) - use unbounded
                            None
                        }
                    } else {
                        None
                    }
                };

                match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .range_scan(
                            Some(&normalized_prefix),
                            end_key.as_ref(),
                            true,  // Inclusive start
                            false, // Exclusive end
                        )
                        .unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in prefix_scan: {}", e);
                        vec![]
                    }
                }
            }
        }
    }

    /// Batch prefix scan - look up multiple prefixes in a single call
    ///
    /// This method is optimized for batch prefix lookups where you need to retrieve
    /// rows matching multiple key prefixes. It's more efficient than calling
    /// `prefix_scan` in a loop.
    ///
    /// # Arguments
    /// * `prefixes` - List of key prefixes to look up
    ///
    /// # Returns
    /// Vector of (prefix_index, row_indices) pairs for each prefix that has matches
    ///
    /// # Example
    /// ```rust,ignore
    /// // Index on (w_id, d_id, o_id) - look up all orders for districts 1-10
    /// let prefixes: Vec<Vec<SqlValue>> = (1..=10)
    ///     .map(|d| vec![SqlValue::Integer(1), SqlValue::Integer(d)])
    ///     .collect();
    /// let results = index_data.prefix_scan_batch(&prefixes);
    /// ```
    pub fn prefix_scan_batch(&self, prefixes: &[Vec<SqlValue>]) -> Vec<(usize, Vec<usize>)> {
        let mut results = Vec::new();

        for (idx, prefix) in prefixes.iter().enumerate() {
            let row_indices = self.prefix_scan(prefix);
            if !row_indices.is_empty() {
                results.push((idx, row_indices));
            }
        }

        results
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
    /// Note: Keys are normalized to match how real indexes store data
    fn create_test_index_data(entries: Vec<(Vec<SqlValue>, Vec<usize>)>) -> IndexData {
        let mut data = BTreeMap::new();
        for (key, row_indices) in entries {
            // Normalize keys like real index insertion does
            let normalized_key: Vec<SqlValue> = key.iter().map(normalize_for_comparison).collect();
            data.insert(normalized_key, row_indices);
        }
        IndexData::InMemory { data }
    }

    // ========================================================================
    // prefix_scan() Tests - InMemory
    // ========================================================================

    #[test]
    fn test_prefix_scan_single_column_match() {
        // Index on (a, b) - look for rows where a=1
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![3]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![4]),
        ]);

        // Prefix [1] should match rows 0, 1, 2
        let results = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert_eq!(results, vec![0, 1, 2]);
    }

    #[test]
    fn test_prefix_scan_two_column_prefix() {
        // Index on (a, b, c) - look for rows where a=1 AND b=5
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(5),
                    SqlValue::Integer(100),
                ],
                vec![0],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(5),
                    SqlValue::Integer(200),
                ],
                vec![1],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(5),
                    SqlValue::Integer(300),
                ],
                vec![2],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(6),
                    SqlValue::Integer(100),
                ],
                vec![3],
            ),
            (
                vec![
                    SqlValue::Integer(2),
                    SqlValue::Integer(5),
                    SqlValue::Integer(100),
                ],
                vec![4],
            ),
        ]);

        // Prefix [1, 5] should match rows 0, 1, 2
        let results = index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
        assert_eq!(results, vec![0, 1, 2]);
    }

    #[test]
    fn test_prefix_scan_exact_match() {
        // When prefix length equals key length, it's an exact match
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        ]);

        // Exact match [1, 10]
        let results = index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(10)]);
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_prefix_scan_no_match() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
        ]);

        // No rows where a=3
        let results = index.prefix_scan(&[SqlValue::Integer(3)]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_single_row() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![0],
        )]);

        let results = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_prefix_scan_multiple_rows_per_key() {
        // Non-unique index: multiple row indices per key
        let index = create_test_index_data(vec![
            (
                vec![SqlValue::Integer(1), SqlValue::Integer(10)],
                vec![0, 5, 10],
            ),
            (
                vec![SqlValue::Integer(1), SqlValue::Integer(20)],
                vec![1, 6],
            ),
        ]);

        let results = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert_eq!(results, vec![0, 5, 10, 1, 6]);
    }

    // ========================================================================
    // Edge Cases
    // ========================================================================

    #[test]
    fn test_prefix_scan_empty_prefix() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
        ]);

        // Empty prefix returns nothing (by design)
        let results = index.prefix_scan(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_prefix_longer_than_key() {
        // Index has 2-column keys, but we search with 3-column prefix
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        ]);

        // Prefix longer than key cannot match
        let results = index.prefix_scan(&[
            SqlValue::Integer(1),
            SqlValue::Integer(10),
            SqlValue::Integer(100),
        ]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_empty_index() {
        let index = create_test_index_data(vec![]);

        let results = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_with_string_keys() {
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Varchar("a".to_string()),
                    SqlValue::Integer(1),
                ],
                vec![0],
            ),
            (
                vec![
                    SqlValue::Varchar("a".to_string()),
                    SqlValue::Integer(2),
                ],
                vec![1],
            ),
            (
                vec![
                    SqlValue::Varchar("b".to_string()),
                    SqlValue::Integer(1),
                ],
                vec![2],
            ),
        ]);

        let results = index.prefix_scan(&[SqlValue::Varchar("a".to_string())]);
        assert_eq!(results, vec![0, 1]);
    }

    #[test]
    fn test_prefix_scan_with_mixed_types() {
        // Multi-column index with different types
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar("x".to_string()),
                    SqlValue::Boolean(true),
                ],
                vec![0],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar("x".to_string()),
                    SqlValue::Boolean(false),
                ],
                vec![1],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Varchar("y".to_string()),
                    SqlValue::Boolean(true),
                ],
                vec![2],
            ),
        ]);

        // Match on [1, "x"] - order depends on BTreeMap key ordering (false < true)
        let results = index.prefix_scan(&[
            SqlValue::Integer(1),
            SqlValue::Varchar("x".to_string()),
        ]);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0));
        assert!(results.contains(&1));
    }

    #[test]
    fn test_prefix_scan_numeric_type_normalization() {
        // Test that different numeric types are normalized correctly
        // Index uses Integer, but we search with a different numeric type
        let index = create_test_index_data(vec![
            (vec![SqlValue::Double(1.0), SqlValue::Double(10.0)], vec![0]),
            (vec![SqlValue::Double(1.0), SqlValue::Double(20.0)], vec![1]),
            (vec![SqlValue::Double(2.0), SqlValue::Double(10.0)], vec![2]),
        ]);

        // Search with Integer(1) should match Double(1.0) after normalization
        let results = index.prefix_scan(&[SqlValue::Integer(1)]);
        assert_eq!(results, vec![0, 1]);
    }

    // ========================================================================
    // prefix_scan_batch() Tests
    // ========================================================================

    #[test]
    fn test_prefix_scan_batch_basic() {
        // Index on (w_id, d_id, o_id) - like TPC-C NEW_ORDER table
        let index = create_test_index_data(vec![
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(1),
                    SqlValue::Integer(100),
                ],
                vec![0],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(1),
                    SqlValue::Integer(101),
                ],
                vec![1],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(100),
                ],
                vec![2],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(2),
                    SqlValue::Integer(101),
                ],
                vec![3],
            ),
            (
                vec![
                    SqlValue::Integer(1),
                    SqlValue::Integer(3),
                    SqlValue::Integer(100),
                ],
                vec![4],
            ),
        ]);

        // Batch lookup for districts 1 and 2
        let prefixes = vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
        ];

        let results = index.prefix_scan_batch(&prefixes);

        // Should have 2 results (one for each prefix that has matches)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (0, vec![0, 1])); // prefix 0 matches rows 0, 1
        assert_eq!(results[1], (1, vec![2, 3])); // prefix 1 matches rows 2, 3
    }

    #[test]
    fn test_prefix_scan_batch_some_empty() {
        let index = create_test_index_data(vec![
            (
                vec![SqlValue::Integer(1), SqlValue::Integer(1)],
                vec![0],
            ),
            (
                vec![SqlValue::Integer(1), SqlValue::Integer(3)],
                vec![2],
            ),
        ]);

        // Batch lookup - prefix at index 1 has no matches
        let prefixes = vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![SqlValue::Integer(1), SqlValue::Integer(2)], // No match
            vec![SqlValue::Integer(1), SqlValue::Integer(3)],
        ];

        let results = index.prefix_scan_batch(&prefixes);

        // Only prefixes 0 and 2 have matches
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (0, vec![0]));
        assert_eq!(results[1], (2, vec![2]));
    }

    #[test]
    fn test_prefix_scan_batch_all_empty() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![0],
        )]);

        let prefixes = vec![
            vec![SqlValue::Integer(2), SqlValue::Integer(1)],
            vec![SqlValue::Integer(3), SqlValue::Integer(1)],
        ];

        let results = index.prefix_scan_batch(&prefixes);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_batch_empty_input() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![0],
        )]);

        let results = index.prefix_scan_batch(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_batch_tpcc_like() {
        // Simulate TPC-C Delivery transaction: lookup all districts for a warehouse
        // Index: (NO_W_ID, NO_D_ID, NO_O_ID)
        let mut entries = Vec::new();
        let w_id = 1;

        // Create data for 10 districts, each with varying number of new orders
        for d_id in 1..=10 {
            for o_id in 1..=(d_id * 2) {
                // District 1 has 2 orders, district 2 has 4, etc.
                let key = vec![
                    SqlValue::Integer(w_id),
                    SqlValue::Integer(d_id),
                    SqlValue::Integer(o_id),
                ];
                entries.push((key, vec![((d_id - 1) * 10 + o_id - 1) as usize]));
            }
        }

        let index = create_test_index_data(entries);

        // Batch prefix lookup for all 10 districts
        let prefixes: Vec<Vec<SqlValue>> = (1..=10)
            .map(|d| vec![SqlValue::Integer(w_id), SqlValue::Integer(d)])
            .collect();

        let results = index.prefix_scan_batch(&prefixes);

        // All 10 districts should have matches
        assert_eq!(results.len(), 10);

        // Verify each district has the expected number of rows
        for (idx, rows) in &results {
            let d_id = *idx as i64 + 1;
            let expected_count = (d_id * 2) as usize;
            assert_eq!(rows.len(), expected_count, "District {} should have {} orders", d_id, expected_count);
        }
    }

    // ========================================================================
    // prefix_multi_lookup() Tests
    // ========================================================================

    #[test]
    fn test_prefix_multi_lookup_basic() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![2]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![3]),
            (vec![SqlValue::Integer(3), SqlValue::Integer(10)], vec![4]),
        ]);

        // Look up a=1 OR a=2
        let results = index.prefix_multi_lookup(&[SqlValue::Integer(1), SqlValue::Integer(2)]);

        // Should find rows 0, 1 (a=1) and 2, 3 (a=2)
        assert_eq!(results.len(), 4);
        assert!(results.contains(&0));
        assert!(results.contains(&1));
        assert!(results.contains(&2));
        assert!(results.contains(&3));
    }

    #[test]
    fn test_prefix_multi_lookup_with_duplicates() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
        ]);

        // Duplicates in input should be deduplicated
        let results = index.prefix_multi_lookup(&[
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(2),
        ]);

        assert_eq!(results.len(), 2);
        assert!(results.contains(&0));
        assert!(results.contains(&1));
    }
}
