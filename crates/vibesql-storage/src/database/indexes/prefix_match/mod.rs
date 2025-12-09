// ============================================================================
// Prefix Match - Multi-column index prefix matching
// ============================================================================
//
// This module has been refactored into focused submodules for better
// maintainability and code organization:
//
// - mod.rs (this file): Core prefix operations (prefix_scan, prefix_multi_lookup, etc.)
// - covering.rs: Covering index operations that return key values + row indices
// - distinct.rs: Distinct value extraction for skip-scan optimization
// - skip_scan.rs: Skip-scan operations for non-prefix column filters
// - tests.rs: Comprehensive test suite

mod covering;
mod distinct;
mod skip_scan;

#[cfg(test)]
mod tests;

use std::ops::Bound;
use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::range_bounds::{try_increment_sqlvalue, try_increment_sqlvalue_prefix};
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
                Some(value), // start
                Some(value), // end (same as start for equality/prefix matching)
                true,        // inclusive_start
                true,        // inclusive_end
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
    /// ```text
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
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Calculate upper bound by incrementing the last element of the prefix
                // For prefix [1, 2], upper bound is [1, 3)
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded, // Couldn't increment, use unbounded
                };

                let mut matching_row_indices = Vec::new();

                for (key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound))
                {
                    // Double-check prefix match (needed for Unbounded end bound case)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Apply pending deletions adjustment
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
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
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support prefix scans - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support prefix scans - use search() method instead
                vec![]
            }
        }
    }

    /// Lookup the first row matching a multi-column prefix in a composite index
    ///
    /// This is an optimized version of `prefix_scan` that returns only the first matching row.
    /// For queries with `ORDER BY <remaining_pk_column> LIMIT 1`, this avoids fetching all
    /// matching rows when we only need the minimum/first one.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns (N < total columns)
    ///
    /// # Returns
    /// The row index of the first matching row, or None if no match
    ///
    /// # Performance
    /// O(log n) - only accesses the first matching entry in the BTreeMap range
    ///
    /// # Example
    /// ```text
    /// // TPC-C Delivery: find oldest new_order for warehouse 1, district 5
    /// // Index: (no_w_id, no_d_id, no_o_id)
    /// // Returns the row with minimum no_o_id for the given warehouse/district
    /// let first_row = index_data.prefix_scan_first(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    /// ```
    pub fn prefix_scan_first(&self, prefix: &[SqlValue]) -> Option<usize> {
        if prefix.is_empty() {
            // Empty prefix - return first row in index
            return self.values().flatten().next();
        }

        // Normalize prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Calculate upper bound by incrementing the last element of the prefix
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                // Get just the first matching entry
                for (key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound))
                {
                    // Verify prefix match (needed for Unbounded end bound case)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        // Return the first row index from this key, adjusted for pending deletions
                        return row_indices.first().copied().map(|row_idx| {
                            if pending_deletions.is_empty() {
                                row_idx
                            } else {
                                let decrement = pending_deletions.partition_point(|&d| d < row_idx);
                                row_idx - decrement
                            }
                        });
                    }
                }

                None
            }
            IndexData::DiskBacked { btree, .. } => {
                // Calculate upper bound for disk-backed index
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .range_scan_first(
                            Some(&normalized_prefix),
                            end_key.as_ref(),
                            true,  // Inclusive start
                            false, // Exclusive end
                        )
                        .unwrap_or(None),
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_scan_first: {}",
                            e
                        );
                        None
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support prefix scans - use search() method instead
                None
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support prefix scans - use search() method instead
                None
            }
        }
    }

    /// Bounded prefix scan - look up rows matching a prefix with an optional upper bound
    /// on the next column
    ///
    /// This method is designed for queries like `WHERE col1 = 1 AND col2 < 10` on a
    /// composite index `(col1, col2, col3)`. It's more efficient than `prefix_scan`
    /// because it avoids scanning all rows with `col1 = 1` and only scans up to the bound.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns (equality predicates)
    /// * `upper_bound` - Upper bound for the (N+1)th column (exclusive)
    ///
    /// # Returns
    /// Vector of row indices matching the prefix and bound
    ///
    /// # Performance
    /// Uses BTreeMap's efficient range() method with computed bounds for O(log n + k)
    /// complexity, where n is the number of unique keys and k is matching keys.
    ///
    /// # Example
    /// ```text
    /// // Index on (s_w_id, s_quantity, s_i_id)
    /// // Find all rows where s_w_id = 1 AND s_quantity < 10
    /// let rows = index_data.prefix_bounded_scan(
    ///     &[SqlValue::Integer(1)],         // prefix: s_w_id = 1
    ///     &SqlValue::Integer(10),           // upper_bound: s_quantity < 10
    ///     false,                            // exclusive upper bound
    /// );
    /// ```
    pub fn prefix_bounded_scan(
        &self,
        prefix: &[SqlValue],
        upper_bound: &SqlValue,
        inclusive_upper: bool,
    ) -> Vec<usize> {
        if prefix.is_empty() {
            // Empty prefix with upper bound is not well-defined - fall back to full scan
            return self.values().flatten().collect();
        }

        // Normalize values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();
        let normalized_bound = normalize_for_comparison(upper_bound);

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                use std::ops::Bound;

                // Start bound: [prefix] (inclusive)
                let start_key = normalized_prefix.clone();
                let start_bound: Bound<Vec<SqlValue>> = Bound::Included(start_key);

                // End bound: [prefix, upper_bound] (exclusive or inclusive depending on flag)
                let mut end_key = normalized_prefix.clone();
                end_key.push(normalized_bound);
                let end_bound: Bound<Vec<SqlValue>> = if inclusive_upper {
                    // For inclusive upper bound, we need to find the next value
                    // to make it effectively inclusive
                    let last_idx = end_key.len() - 1;
                    match try_increment_sqlvalue(&end_key[last_idx]) {
                        Some(next_val) => {
                            end_key[last_idx] = next_val;
                            Bound::Excluded(end_key)
                        }
                        None => {
                            // Can't increment, use included bound
                            Bound::Included(end_key)
                        }
                    }
                } else {
                    Bound::Excluded(end_key)
                };

                let mut matching_row_indices = Vec::new();

                for (key_values, row_indices) in data.range((start_bound, end_bound)) {
                    // Verify prefix match (needed for safety)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Apply pending deletions adjustment
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
                    }
                }

                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed indexes, construct start and end keys
                let start_key = normalized_prefix.clone();

                let mut end_key = normalized_prefix.clone();
                end_key.push(normalized_bound);

                match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .range_scan(
                            Some(&start_key),
                            Some(&end_key),
                            true,            // Inclusive start
                            inclusive_upper, // Inclusive/exclusive end
                        )
                        .unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_bounded_scan: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support prefix bounded scans - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support prefix bounded scans - use search() method instead
                vec![]
            }
        }
    }

    /// Prefix + range scan with both lower and upper bounds on the trailing column
    ///
    /// This method combines prefix matching with a range scan on the next column,
    /// supporting both lower and upper bounds. Essential for queries like:
    /// `WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id >= 2981 AND ol_o_id < 3001`
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns (equality)
    /// * `lower_bound` - Lower bound for the (N+1)th column, if any
    /// * `inclusive_lower` - Whether lower bound is inclusive (>=) or exclusive (>)
    /// * `upper_bound` - Upper bound for the (N+1)th column, if any
    /// * `inclusive_upper` - Whether upper bound is inclusive (<=) or exclusive (<)
    ///
    /// # Returns
    /// Vector of row indices matching the prefix and range constraint
    ///
    /// # Example
    /// ```text
    /// // Index on (ol_w_id, ol_d_id, ol_o_id, ol_number)
    /// // Find all rows where ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id >= 2981 AND ol_o_id < 3001
    /// let rows = index_data.prefix_range_scan(
    ///     &[SqlValue::Integer(1), SqlValue::Integer(1)],  // prefix
    ///     Some(&SqlValue::Integer(2981)),                  // lower_bound
    ///     true,                                            // inclusive_lower (>=)
    ///     Some(&SqlValue::Integer(3001)),                  // upper_bound
    ///     false,                                           // exclusive upper (<)
    /// );
    /// ```
    pub fn prefix_range_scan(
        &self,
        prefix: &[SqlValue],
        lower_bound: Option<&SqlValue>,
        inclusive_lower: bool,
        upper_bound: Option<&SqlValue>,
        inclusive_upper: bool,
    ) -> Vec<usize> {
        if prefix.is_empty() {
            // Empty prefix with range is not well-defined - fall back to full scan
            return self.values().flatten().collect();
        }

        // If no bounds are specified, fall back to regular prefix scan
        if lower_bound.is_none() && upper_bound.is_none() {
            return self.prefix_scan(prefix);
        }

        // Check for inverted range (lower > upper) - this is valid SQL but returns no rows
        // e.g., WHERE col BETWEEN 10 AND 5 should return empty, not panic
        if let (Some(lb), Some(ub)) = (lower_bound, upper_bound) {
            let normalized_lb = normalize_for_comparison(lb);
            let normalized_ub = normalize_for_comparison(ub);
            if normalized_lb > normalized_ub {
                // Inverted range - no rows can match
                return Vec::new();
            }
        }

        // Normalize values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                use std::ops::Bound;

                // Build start key: [prefix, lower_bound?]
                let start_bound: Bound<Vec<SqlValue>> = if let Some(lb) = lower_bound {
                    let normalized_lb = normalize_for_comparison(lb);
                    let mut start_key = normalized_prefix.clone();
                    start_key.push(normalized_lb);
                    if inclusive_lower {
                        Bound::Included(start_key)
                    } else {
                        Bound::Excluded(start_key)
                    }
                } else {
                    Bound::Included(normalized_prefix.clone())
                };

                // Build end key: [prefix, upper_bound?]
                let end_bound: Bound<Vec<SqlValue>> = if let Some(ub) = upper_bound {
                    let normalized_ub = normalize_for_comparison(ub);
                    let mut end_key = normalized_prefix.clone();
                    end_key.push(normalized_ub);
                    if inclusive_upper {
                        // For inclusive upper bound, we need to find the next value
                        let last_idx = end_key.len() - 1;
                        match try_increment_sqlvalue(&end_key[last_idx]) {
                            Some(next_val) => {
                                end_key[last_idx] = next_val;
                                Bound::Excluded(end_key)
                            }
                            None => {
                                // Can't increment, use included bound
                                Bound::Included(end_key)
                            }
                        }
                    } else {
                        Bound::Excluded(end_key)
                    }
                } else {
                    // No upper bound - need to scan up to the end of the prefix range
                    // Create a key that is just past the end of the prefix
                    let end_key = normalized_prefix.clone();
                    // For unbounded upper, we need to capture all values with this prefix
                    // We do this by constructing a key just past the prefix range
                    match try_increment_sqlvalue_prefix(&end_key) {
                        Some(next_prefix) => Bound::Excluded(next_prefix),
                        None => Bound::Unbounded,
                    }
                };

                let mut matching_row_indices = Vec::new();

                for (key_values, row_indices) in data.range((start_bound, end_bound)) {
                    // Verify prefix match (needed for safety when we have a lower bound)
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Apply pending deletions adjustment
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
                    }
                }

                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed indexes, construct start and end keys
                let start_key = if let Some(lb) = lower_bound {
                    let normalized_lb = normalize_for_comparison(lb);
                    let mut key = normalized_prefix.clone();
                    key.push(normalized_lb);
                    Some(key)
                } else {
                    Some(normalized_prefix.clone())
                };

                let end_key = if let Some(ub) = upper_bound {
                    let normalized_ub = normalize_for_comparison(ub);
                    let mut key = normalized_prefix.clone();
                    key.push(normalized_ub);
                    Some(key)
                } else {
                    // For unbounded upper, construct a key just past the prefix
                    try_increment_sqlvalue_prefix(&normalized_prefix)
                };

                match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .range_scan(
                            start_key.as_ref(),
                            end_key.as_ref(),
                            inclusive_lower || lower_bound.is_none(), // Inclusive start if no lower bound
                            inclusive_upper,
                        )
                        .unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_range_scan: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support prefix range scans - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support prefix range scans - use search() method instead
                vec![]
            }
        }
    }

    /// Prefix scan with limit and optional reverse iteration
    ///
    /// This method is optimized for ORDER BY with LIMIT queries where the index
    /// satisfies the ORDER BY clause. Instead of fetching all matching rows and
    /// then applying LIMIT, this stops early after collecting enough rows.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns
    /// * `limit` - Maximum number of rows to return (None means no limit)
    /// * `reverse` - If true, scan in reverse order (for DESC ORDER BY)
    ///
    /// # Returns
    /// Vector of row indices matching the prefix, limited to `limit` rows
    ///
    /// # Performance
    /// For ORDER BY ... LIMIT 1 queries on a customer with 30 orders:
    /// - Without limit: Fetch all 30 rows, reverse, take 1 = O(30)
    /// - With limit+reverse: Scan from end, stop after 1 = O(1)
    ///
    /// # Example
    /// ```text
    /// // Find the most recent order for customer (ORDER BY o_id DESC LIMIT 1)
    /// let prefix = vec![SqlValue::Integer(w_id), SqlValue::Integer(d_id), SqlValue::Integer(c_id)];
    /// let rows = index_data.prefix_scan_limit(&prefix, Some(1), true);
    /// ```
    pub fn prefix_scan_limit(
        &self,
        prefix: &[SqlValue],
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<usize> {
        // If no limit and not reverse, use the regular prefix_scan
        if limit.is_none() && !reverse {
            return self.prefix_scan(prefix);
        }

        if prefix.is_empty() {
            // Empty prefix - either return all or first N rows
            let all_rows: Vec<usize> = self.values().flatten().collect();
            return match limit {
                Some(n) if reverse => all_rows.into_iter().rev().take(n).collect(),
                Some(n) => all_rows.into_iter().take(n).collect(),
                None if reverse => all_rows.into_iter().rev().collect(),
                None => all_rows,
            };
        }

        // Normalize prefix values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Calculate upper bound by incrementing the last element of the prefix
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let mut matching_row_indices = Vec::new();
                let max_rows = limit.unwrap_or(usize::MAX);

                // Closure to apply pending deletions adjustment
                let apply_adjustment = |result: &mut Vec<usize>| {
                    if !pending_deletions.is_empty() {
                        for row_idx in result.iter_mut() {
                            let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                            *row_idx -= decrement;
                        }
                    }
                };

                if reverse {
                    // Reverse iteration: collect all matching keys first, then iterate in reverse
                    // BTreeMap's range doesn't support reverse iteration directly, so we collect
                    // and reverse. For small result sets (typical with LIMIT), this is efficient.
                    let matching_entries: Vec<_> = data
                        .range::<[SqlValue], _>((start_bound, end_bound))
                        .filter(|(key_values, _)| {
                            key_values.len() >= normalized_prefix.len()
                                && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                        })
                        .collect();

                    // Iterate in reverse order
                    for (_, row_indices) in matching_entries.into_iter().rev() {
                        // For each key, row indices are in insertion order
                        // For DESC order, we want the last inserted rows first
                        for &row_idx in row_indices.iter().rev() {
                            matching_row_indices.push(row_idx);
                            if matching_row_indices.len() >= max_rows {
                                apply_adjustment(&mut matching_row_indices);
                                return matching_row_indices;
                            }
                        }
                    }
                } else {
                    // Forward iteration with early termination
                    for (key_values, row_indices) in
                        data.range::<[SqlValue], _>((start_bound, end_bound))
                    {
                        // Double-check prefix match (needed for Unbounded end bound case)
                        if key_values.len() >= normalized_prefix.len()
                            && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                        {
                            for &row_idx in row_indices {
                                matching_row_indices.push(row_idx);
                                if matching_row_indices.len() >= max_rows {
                                    apply_adjustment(&mut matching_row_indices);
                                    return matching_row_indices;
                                }
                            }
                        }
                    }
                }

                apply_adjustment(&mut matching_row_indices);
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed, fall back to regular scan then limit
                // TODO: Implement reverse scanning in BTreeIndex for better performance
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let all_indices = match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .range_scan(Some(&normalized_prefix), end_key.as_ref(), true, false)
                        .unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in prefix_scan_limit: {}",
                            e
                        );
                        vec![]
                    }
                };

                match limit {
                    Some(n) if reverse => all_indices.into_iter().rev().take(n).collect(),
                    Some(n) => all_indices.into_iter().take(n).collect(),
                    None if reverse => all_indices.into_iter().rev().collect(),
                    None => all_indices,
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support prefix scan with limit - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support prefix scan with limit - use search() method instead
                vec![]
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
    /// ```text
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
pub(super) fn compute_prefix_upper_bound(prefix: &[SqlValue]) -> Option<Vec<SqlValue>> {
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
