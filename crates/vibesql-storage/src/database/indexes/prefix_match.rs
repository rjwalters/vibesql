// ============================================================================
// Prefix Match - Multi-column index prefix matching
// ============================================================================

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
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

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
    /// ```rust,ignore
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
            IndexData::InMemory { data } => {
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
                        // Return the first row index from this key
                        return row_indices.first().copied();
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
    /// ```rust,ignore
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
            IndexData::InMemory { data } => {
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
    /// ```rust,ignore
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
            IndexData::InMemory { data } => {
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
    /// ```rust,ignore
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
            IndexData::InMemory { data } => {
                // Calculate upper bound by incrementing the last element of the prefix
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let mut matching_row_indices = Vec::new();
                let max_rows = limit.unwrap_or(usize::MAX);

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
                                    return matching_row_indices;
                                }
                            }
                        }
                    }
                }

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
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(200)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(300)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(6), SqlValue::Integer(100)], vec![3]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(5), SqlValue::Integer(100)], vec![4]),
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
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0, 5, 10]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1, 6]),
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

        // Empty prefix matches everything - returns all rows
        let results = index.prefix_scan(&[]);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&0));
        assert!(results.contains(&1));
    }

    #[test]
    fn test_prefix_scan_prefix_longer_than_key() {
        // Index has 2-column keys, but we search with 3-column prefix
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![0],
        )]);

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
            (vec![SqlValue::Varchar("a".to_string()), SqlValue::Integer(1)], vec![0]),
            (vec![SqlValue::Varchar("a".to_string()), SqlValue::Integer(2)], vec![1]),
            (vec![SqlValue::Varchar("b".to_string()), SqlValue::Integer(1)], vec![2]),
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
        let results =
            index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Varchar("x".to_string())]);
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
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(101)], vec![3]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(3), SqlValue::Integer(100)], vec![4]),
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
            (vec![SqlValue::Integer(1), SqlValue::Integer(1)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(3)], vec![2]),
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
                let key =
                    vec![SqlValue::Integer(w_id), SqlValue::Integer(d_id), SqlValue::Integer(o_id)];
                entries.push((key, vec![((d_id - 1) * 10 + o_id - 1) as usize]));
            }
        }

        let index = create_test_index_data(entries);

        // Batch prefix lookup for all 10 districts
        let prefixes: Vec<Vec<SqlValue>> =
            (1..=10).map(|d| vec![SqlValue::Integer(w_id), SqlValue::Integer(d)]).collect();

        let results = index.prefix_scan_batch(&prefixes);

        // All 10 districts should have matches
        assert_eq!(results.len(), 10);

        // Verify each district has the expected number of rows
        for (idx, rows) in &results {
            let d_id = *idx as i64 + 1;
            let expected_count = (d_id * 2) as usize;
            assert_eq!(
                rows.len(),
                expected_count,
                "District {} should have {} orders",
                d_id,
                expected_count
            );
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

    // ========================================================================
    // prefix_scan_first() Tests - InMemory
    // ========================================================================

    #[test]
    fn test_prefix_scan_first_basic() {
        // Index on (w_id, d_id, o_id) - TPC-C style
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(200)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(300)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![3]),
        ]);

        // Prefix [1, 1] should return first matching row (row 0)
        let result = index.prefix_scan_first(&[SqlValue::Integer(1), SqlValue::Integer(1)]);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_prefix_scan_first_no_match() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![0],
        )]);

        // Prefix [2, 1] should return None (no match)
        let result = index.prefix_scan_first(&[SqlValue::Integer(2), SqlValue::Integer(1)]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_prefix_scan_first_empty_prefix() {
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1)], vec![0]),
            (vec![SqlValue::Integer(2)], vec![1]),
        ]);

        // Empty prefix should return first row in index
        let result = index.prefix_scan_first(&[]);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_prefix_scan_first_returns_minimum_third_column() {
        // Index on (w_id, d_id, o_id)
        // This tests the TPC-C Delivery query pattern:
        // SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 5 ORDER BY no_o_id LIMIT 1
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(300)], vec![3]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(100)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(200)], vec![2]),
        ]);

        // Prefix [1, 5] should return row with minimum o_id (100) = row 1
        // BTreeMap stores in sorted order, so [1, 5, 100] comes first
        let result = index.prefix_scan_first(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
        assert_eq!(result, Some(1)); // Row index for o_id=100
    }

    // ========================================================================
    // prefix_scan_limit() Tests - LIMIT pushdown optimization (#3253)
    // ========================================================================

    #[test]
    fn test_prefix_scan_limit_forward_with_limit() {
        // Index on (w_id, d_id, o_id) - find first 2 orders for a customer
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(102)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(103)], vec![3]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![4]),
        ]);

        // LIMIT 2 in forward order (ASC)
        let results = index.prefix_scan_limit(
            &[SqlValue::Integer(1), SqlValue::Integer(1)],
            Some(2),
            false, // ASC
        );
        assert_eq!(results, vec![0, 1]); // First 2 orders
    }

    #[test]
    fn test_prefix_scan_limit_reverse_with_limit() {
        // Index on (w_id, d_id, o_id) - find most recent order (DESC LIMIT 1)
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(102)], vec![2]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(103)], vec![3]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![4]),
        ]);

        // LIMIT 1 in reverse order (DESC) - most recent order
        let results = index.prefix_scan_limit(
            &[SqlValue::Integer(1), SqlValue::Integer(1)],
            Some(1),
            true, // DESC
        );
        assert_eq!(results, vec![3]); // Last order (o_id=103)
    }

    #[test]
    fn test_prefix_scan_limit_reverse_all() {
        // Get all rows in reverse order (no limit)
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
        ]);

        // No limit, reverse order
        let results = index.prefix_scan_limit(
            &[SqlValue::Integer(1)],
            None,
            true, // DESC
        );
        assert_eq!(results, vec![2, 1, 0]); // Reverse order
    }

    #[test]
    fn test_prefix_scan_limit_no_limit_no_reverse() {
        // Falls back to regular prefix_scan
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
            (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![2]),
        ]);

        let results = index.prefix_scan_limit(&[SqlValue::Integer(1)], None, false);
        assert_eq!(results, vec![0, 1]);
    }

    #[test]
    fn test_prefix_scan_limit_tpcc_order_status() {
        // Simulate TPC-C Order-Status: Find most recent order for a customer
        // Customer has 30 orders, we need just the last one (ORDER BY o_id DESC LIMIT 1)
        let mut entries = Vec::new();
        let w_id = 1;
        let d_id = 5;
        let c_id = 42;

        // Create 30 orders for the customer
        for o_id in 1..=30 {
            let key = vec![
                SqlValue::Integer(w_id),
                SqlValue::Integer(d_id),
                SqlValue::Integer(c_id),
                SqlValue::Integer(o_id),
            ];
            entries.push((key, vec![o_id as usize - 1]));
        }

        let index = create_test_index_data(entries);

        // Most recent order (ORDER BY o_id DESC LIMIT 1)
        let results = index.prefix_scan_limit(
            &[SqlValue::Integer(w_id), SqlValue::Integer(d_id), SqlValue::Integer(c_id)],
            Some(1),
            true, // DESC
        );

        // Should get the last order (o_id=30, row_idx=29)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 29);
    }

    #[test]
    fn test_prefix_scan_limit_empty_result() {
        let index = create_test_index_data(vec![(
            vec![SqlValue::Integer(1), SqlValue::Integer(10)],
            vec![0],
        )]);

        // No match
        let results = index.prefix_scan_limit(&[SqlValue::Integer(2)], Some(5), true);
        assert!(results.is_empty());
    }

    #[test]
    fn test_prefix_scan_limit_less_than_limit() {
        // When there are fewer matching rows than the limit
        let index = create_test_index_data(vec![
            (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
            (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        ]);

        // LIMIT 10 but only 2 rows match
        let results = index.prefix_scan_limit(&[SqlValue::Integer(1)], Some(10), false);
        assert_eq!(results, vec![0, 1]); // All matching rows
    }
}
