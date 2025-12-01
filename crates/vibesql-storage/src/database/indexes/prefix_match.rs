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
