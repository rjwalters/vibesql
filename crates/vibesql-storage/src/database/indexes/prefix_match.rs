// ============================================================================
// Prefix Match - Multi-column index prefix matching
// ============================================================================

use std::ops::Bound;
use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::range_bounds::try_increment_sqlvalue;
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

    /// Lookup rows matching a multi-column prefix in a composite index
    ///
    /// For example, with index `[c_w_id, c_d_id, c_id]` and prefix `[1, 2]`,
    /// this returns all rows where `c_w_id = 1 AND c_d_id = 2`, regardless of `c_id`.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns (N < total columns)
    ///
    /// # Returns
    /// Vector of row indices matching the prefix
    ///
    /// # Performance
    /// Uses BTreeMap's efficient range() method with computed bounds for O(log n + k)
    /// complexity, where n is the number of unique keys and k is matching keys.
    ///
    /// # How it works
    /// BTreeMap orders Vec<SqlValue> lexicographically:
    ///   [1, 2] < [1, 2, 0] < [1, 2, 99] < [1, 3] < [1, 3, 0]
    ///
    /// So prefix_scan([1, 2]) scans from [1, 2] (inclusive) to [1, 3) (exclusive).
    ///
    /// # Example
    /// ```rust,ignore
    /// // Index on (w_id, d_id, o_id) - 3 columns
    /// // Find all rows where w_id=1 AND d_id=5 (2-column prefix)
    /// let rows = index_data.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    /// ```
    pub fn prefix_scan(&self, prefix: &[SqlValue]) -> Vec<usize> {
        if prefix.is_empty() {
            // Empty prefix matches everything - return all rows
            return self.values().flatten().collect();
        }

        // Normalize prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> = prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data } => {
                // Calculate upper bound by incrementing the last element of the prefix
                // For prefix [1, 2], upper bound is [1, 3)
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded, // Couldn't increment, use unbounded
                };

                let mut matching_row_indices = Vec::new();

                for (key_values, row_indices) in data.range::<[SqlValue], _>((start_bound, end_bound)) {
                    // Double-check prefix match (needed for Unbounded end bound case)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        matching_row_indices.extend(row_indices);
                    }
                }

                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Calculate upper bound for disk-backed index
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

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

/// Compute the exclusive upper bound for a prefix scan
///
/// For prefix [1, 2], returns [1, 3] (incrementing the last element).
/// This allows BTreeMap range scan to efficiently find all keys starting with [1, 2].
///
/// Returns None if the last element cannot be incremented (e.g., max value overflow).
fn compute_prefix_upper_bound(prefix: &[SqlValue]) -> Option<Vec<SqlValue>> {
    if prefix.is_empty() {
        return None;
    }

    // Clone prefix and try to increment the last element
    let mut upper_bound = prefix.to_vec();
    let last_idx = upper_bound.len() - 1;

    match try_increment_sqlvalue(&upper_bound[last_idx]) {
        Some(incremented) => {
            upper_bound[last_idx] = incremented;
            Some(upper_bound)
        }
        None => None, // Couldn't increment (overflow), caller should use unbounded
    }
}
