// ============================================================================
// Range Scan - Range query implementation
// ============================================================================

use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::range_bounds::{calculate_next_value, smart_increment_value, try_increment_sqlvalue};
use super::streaming::OwnedStreamingRangeScan;
use super::value_normalization::normalize_for_comparison;

impl IndexData {
    /// Scan index for rows matching range predicate
    ///
    /// # Arguments
    /// * `start` - Lower bound (None = unbounded)
    /// * `end` - Upper bound (None = unbounded)
    /// * `inclusive_start` - Include rows equal to start value
    /// * `inclusive_end` - Include rows equal to end value
    ///
    /// # Returns
    /// Vector of row indices that match the range predicate
    ///
    /// # Performance
    /// Uses BTreeMap's efficient range() method for O(log n + k) complexity,
    /// where n is the number of unique keys and k is the number of matching keys.
    /// This is significantly faster than the previous O(n) full scan approach.
    pub fn range_scan(
        &self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Vec<usize> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                use std::ops::Bound;

                let mut matching_row_indices = Vec::new();

                // Normalize bounds for consistent numeric comparison
                // This allows Real, Numeric, Integer, etc. to be compared correctly
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Special handling for prefix matching on multi-column indexes
                // This handles both equality queries (start == end) and range queries (start != end)
                // on the first column of multi-column indexes.
                //
                // The key insight: For multi-column indexes, we can't use single-element keys
                // like [value] because the index stores composite keys like [col1_val, col2_val].
                // Instead, we need to iterate and check the first column of each key.
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    // Equality check (prefix matching for multi-column IN clauses)
                    if start_val == end_val && inclusive_start && inclusive_end {
                        // Prefix matching using efficient BTreeMap::range() - O(log n + k) instead of O(n)
                        // Strategy: Start iteration at [target_value] and continue while first column matches
                        //
                        // For example, to find all rows where column `a` = 10 in index (a, b):
                        //   Start: Bound::Included([10])
                        //   Continue iterating while key[0] == 10
                        //   Stop when key[0] != 10
                        //
                        // This works because BTreeMap orders Vec<SqlValue> lexicographically:
                        //   [10] < [10, 1] < [10, 2] < [10, 99] < [11] < [11, 1]

                        let start_key = vec![start_val.clone()];
                        let start_bound: Bound<&[SqlValue]> = Bound::Included(start_key.as_slice());

                        // Iterate from start_key to end of map, stopping when first column changes
                        for (key_values, row_indices) in
                            data.range::<[SqlValue], _>((start_bound, Bound::Unbounded))
                        {
                            // Check if first column still matches target
                            if key_values.is_empty() || &key_values[0] != start_val {
                                break; // Stop iteration when prefix no longer matches
                            }
                            matching_row_indices.extend(row_indices);
                        }
                        // Apply lazy adjustment for pending deletions
                        if !pending_deletions.is_empty() {
                            for row_idx in &mut matching_row_indices {
                                let decrement =
                                    pending_deletions.partition_point(|&d| d < *row_idx);
                                *row_idx -= decrement;
                            }
                        }
                        return matching_row_indices;
                    }
                }

                // Edge case: Check for invalid/empty ranges
                // 1. start == end with at least one exclusive bound: col > 5 AND col < 5
                // 2. start > end (inverted range): col BETWEEN 100 AND 50
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    if start_val == end_val && (!inclusive_start || !inclusive_end) {
                        // Empty range: no values can satisfy this condition
                        return Vec::new();
                    }
                    // Check for inverted range: start > end
                    // This handles cases like BETWEEN 9128.11 AND 4747.32 where min > max
                    if start_val > end_val {
                        // Inverted range: no values can satisfy this condition
                        return Vec::new();
                    }
                }

                // Special handling for range queries that might use multi-column indexes
                // If we have both start and end bounds, we need to handle multi-column indexes specially
                // by checking if keys in the map have multiple elements (indicating multi-column index)
                if normalized_start.is_some() || normalized_end.is_some() {
                    // Peek at first key to determine if this is a multi-column index
                    // Multi-column indexes have keys with > 1 element
                    let is_multi_column = data.keys().next().is_some_and(|k| k.len() > 1);

                    if is_multi_column {
                        // Multi-column index range query: iterate and compare first column only
                        // For example, col3 BETWEEN 24 AND 90 on index (col3, col0):
                        //   Start at keys with col3 >= 24, continue while col3 <= 90

                        // For multi-column indexes, Excluded([v]) doesn't exclude keys like [v, x]
                        // because lexicographically [v] < [v, x]. So for `col > 40`, we need to
                        // increment to 41 and use Included([41]) instead of Excluded([40]).
                        // Use smart_increment_value which chooses the right increment strategy:
                        // - Integer values (40.0): add 1.0 → 41.0
                        // - Float values (3952.75): add epsilon → 3952.750...001
                        let start_key = normalized_start.as_ref().map(|v| {
                            if inclusive_start {
                                // For >= predicates, use value as-is with Included
                                (vec![v.clone()], true)
                            } else {
                                // For > predicates, increment value and use Included
                                // This ensures we exclude ALL keys starting with the original value
                                match smart_increment_value(v) {
                                    Some(incremented) => (vec![incremented], true),
                                    // If increment fails (overflow), fall back to Excluded
                                    None => (vec![v.clone()], false),
                                }
                            }
                        });
                        let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                            Some((key, true)) => Bound::Included(key.as_slice()),
                            Some((key, false)) => Bound::Excluded(key.as_slice()),
                            None => Bound::Unbounded,
                        };

                        // Calculate upper bound efficiently instead of using Unbounded + manual checking
                        // For multi-column indexes, we need an upper bound that stops after all keys
                        // starting with end_val (if inclusive) or before them (if exclusive)
                        let end_key = normalized_end.as_ref().and_then(|v| {
                            if inclusive_end {
                                // For inclusive: try to increment the value to get next prefix
                                // If successful, use as Excluded bound; otherwise use Unbounded
                                try_increment_sqlvalue(v)
                                    .map(|incremented| (vec![incremented], false))
                            } else {
                                // For exclusive: use end_val itself as Excluded bound
                                Some((vec![v.clone()], false))
                            }
                        });

                        let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                            Some((key, _)) => Bound::Excluded(key.as_slice()),
                            None => Bound::Unbounded,
                        };

                        // Edge case: Check for invalid range (both bounds excluded at same value)
                        // This can happen with multi-column indexes when start == end and both exclusive
                        // Example: col > 5 AND col < 5 would create Excluded([5]) .. Excluded([5]) → panic
                        // BTreeMap::range() panics on this case, so we return empty result instead
                        if let (Bound::Excluded(start_slice), Bound::Excluded(end_slice)) =
                            (&start_bound, &end_bound)
                        {
                            if start_slice == end_slice {
                                // Invalid range: no values can satisfy this condition
                                return Vec::new();
                            }
                        }

                        // Iterate through BTreeMap with proper bounds - no manual checking needed!
                        for (_key_values, row_indices) in
                            data.range::<[SqlValue], _>((start_bound, end_bound))
                        {
                            matching_row_indices.extend(row_indices);
                        }
                        // Apply lazy adjustment for pending deletions
                        if !pending_deletions.is_empty() {
                            for row_idx in &mut matching_row_indices {
                                let decrement =
                                    pending_deletions.partition_point(|&d| d < *row_idx);
                                *row_idx -= decrement;
                            }
                        }
                        return matching_row_indices;
                    }
                }

                // Standard range scan for single-column indexes or actual range queries
                // Convert to single-element keys (for single-column indexes)
                let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                // Build bounds for BTreeMap::range()
                // Note: BTreeMap<Vec<SqlValue>, _> requires &[SqlValue] (slice) for range bounds
                // Type annotation is needed to help Rust's type inference
                let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                    Some(key) if inclusive_start => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) if inclusive_end => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                // Edge case: Check for invalid range (both bounds excluded at same value)
                // Example: col > 5 AND col < 5 would create Excluded([5]) .. Excluded([5]) → panic
                // BTreeMap::range() panics on this case, so we return empty result instead
                if let (Bound::Excluded(start_slice), Bound::Excluded(end_slice)) =
                    (&start_bound, &end_bound)
                {
                    if start_slice == end_slice {
                        // Invalid range: no values can satisfy this condition
                        return Vec::new();
                    }
                }

                // Use BTreeMap's efficient range() method instead of full iteration
                // This is O(log n + k) instead of O(n) where n = total keys, k = matching keys
                // Explicit type parameter needed due to Borrow trait ambiguity
                for (_key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound))
                {
                    matching_row_indices.extend(row_indices);
                }

                // Apply lazy adjustment for pending deletions
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
                    }
                }

                // Return row indices in the order established by BTreeMap iteration
                // BTreeMap gives us results sorted by index key value, which is the
                // expected order for indexed queries. We should NOT sort by row index
                // as that would destroy the index-based ordering.
                matching_row_indices
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support range scans - use search() method instead
                Vec::new()
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support range scans - use search() method instead
                Vec::new()
            }
            IndexData::DiskBacked { btree, .. } => {
                // Normalize bounds for consistent numeric comparison (same as InMemory)
                // This ensures Real, Numeric, Integer, etc. can be compared correctly
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Special handling for prefix matching (multi-column IN clauses) - same as InMemory
                // When start == end with inclusive bounds, we're doing an equality check on the
                // first column of a multi-column index.
                let is_prefix_match = if let (Some(start_val), Some(end_val)) =
                    (&normalized_start, &normalized_end)
                {
                    start_val == end_val && inclusive_start && inclusive_end
                } else {
                    false
                };

                if is_prefix_match {
                    // Prefix matching for DiskBacked - optimized using calculated upper bound
                    // Strategy: Calculate the next value to use as exclusive upper bound
                    //
                    // For example, to find all rows where column `a` = 10 in index (a, b):
                    //   Range: [10] (inclusive) to [11] (exclusive)
                    //   This captures all keys like [10, 1], [10, 2], ... [10, 999]
                    //   But excludes [11, x] for any x
                    //
                    // This works because BTreeMap orders Vec<SqlValue> lexicographically:
                    //   [10] < [10, 1] < [10, 2] < [10, 99] < [11] < [11, 1]

                    let target_value = normalized_start.as_ref().unwrap(); // Safe: checked above
                    let start_key = vec![target_value.clone()];

                    // Calculate upper bound: next value after target
                    // For numeric types, we can increment; for others, use unbounded
                    let end_key = calculate_next_value(target_value).map(|next_val| vec![next_val]);

                    // Range scan from [target] to [next_value) exclusive
                    match acquire_btree_lock(btree) {
                        Ok(guard) => guard
                            .range_scan(
                                Some(&start_key),
                                end_key.as_ref(), // Bounded end (or unbounded if can't increment)
                                true,             // Inclusive start
                                false,            // Exclusive end
                            )
                            .unwrap_or_else(|_| vec![]),
                        Err(e) => {
                            log::warn!("BTreeIndex lock acquisition failed in range_scan: {}", e);
                            vec![]
                        }
                    }
                } else {
                    // Standard range scan for single-column indexes or actual range queries
                    // NOTE: We apply the smart increment fix conservatively for exclusive start bounds
                    // to handle potential multi-column cases. This is safe for single-column indexes too.

                    // For exclusive start bounds with multi-column indexes, we need to increment the value
                    // to avoid missing rows. Apply smart_increment_value to choose the right strategy.
                    let (start_key, final_inclusive_start) =
                        if let Some(start_val) = normalized_start.as_ref() {
                            if inclusive_start {
                                (Some(vec![start_val.clone()]), true)
                            } else {
                                // Apply smart increment for exclusive start bounds
                                match smart_increment_value(start_val) {
                                    Some(incremented) => (Some(vec![incremented]), true),
                                    None => (Some(vec![start_val.clone()]), false),
                                }
                            }
                        } else {
                            (None, inclusive_start)
                        };

                    let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                    // Safely acquire lock and call BTreeIndex::range_scan
                    match acquire_btree_lock(btree) {
                        Ok(guard) => guard
                            .range_scan(
                                start_key.as_ref(),
                                end_key.as_ref(),
                                final_inclusive_start,
                                inclusive_end,
                            )
                            .unwrap_or_else(|_| vec![]),
                        Err(e) => {
                            // Log error and return empty result set
                            log::warn!("BTreeIndex lock acquisition failed in range_scan: {}", e);
                            vec![]
                        }
                    }
                }
            }
        }
    }

    /// Scan index for rows matching range predicate with early termination at limit
    ///
    /// This is an optimization for LIMIT queries - stops scanning after collecting
    /// enough rows instead of scanning the entire range.
    ///
    /// # Arguments
    /// * `start` - Lower bound (None = unbounded)
    /// * `end` - Upper bound (None = unbounded)
    /// * `inclusive_start` - Include rows equal to start value
    /// * `inclusive_end` - Include rows equal to end value
    /// * `limit` - Maximum number of rows to return (None = no limit)
    ///
    /// # Returns
    /// Vector of row indices that match the range predicate, up to limit
    ///
    /// # Performance
    /// For queries with small LIMIT relative to matching rows, this is O(log n + limit)
    /// instead of O(log n + k) where k = all matching keys.
    pub fn range_scan_limit(
        &self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
        limit: Option<usize>,
    ) -> Vec<usize> {
        // If no limit, fall back to regular range_scan
        let Some(limit) = limit else {
            return self.range_scan(start, end, inclusive_start, inclusive_end);
        };

        if limit == 0 {
            return vec![];
        }

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                use std::ops::Bound;

                let mut matching_row_indices = Vec::with_capacity(limit);

                // Normalize bounds for consistent numeric comparison
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Edge case: Check for invalid/empty ranges
                if let (Some(start_val), Some(end_val)) = (&normalized_start, &normalized_end) {
                    if start_val == end_val && (!inclusive_start || !inclusive_end) {
                        return Vec::new();
                    }
                    if start_val > end_val {
                        return Vec::new();
                    }
                }

                // Build bounds for BTreeMap::range()
                let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                let start_bound: Bound<&[SqlValue]> = match start_key.as_ref() {
                    Some(key) if inclusive_start => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) if inclusive_end => Bound::Included(key.as_slice()),
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                // Edge case: both bounds excluded at same value
                if let (Bound::Excluded(start_slice), Bound::Excluded(end_slice)) =
                    (&start_bound, &end_bound)
                {
                    if start_slice == end_slice {
                        return Vec::new();
                    }
                }

                // Iterate with early termination at limit
                for (_key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound))
                {
                    for &row_idx in row_indices {
                        matching_row_indices.push(row_idx);
                        if matching_row_indices.len() >= limit {
                            // Apply lazy adjustment for pending deletions before returning
                            if !pending_deletions.is_empty() {
                                for idx in &mut matching_row_indices {
                                    let decrement =
                                        pending_deletions.partition_point(|&d| d < *idx);
                                    *idx -= decrement;
                                }
                            }
                            return matching_row_indices;
                        }
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
            IndexData::IVFFlat { .. } => Vec::new(),
            IndexData::Hnsw { .. } => Vec::new(),
            IndexData::DiskBacked { btree, .. } => {
                // Normalize bounds
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                let start_key = normalized_start.as_ref().map(|v| vec![v.clone()]);
                let end_key = normalized_end.as_ref().map(|v| vec![v.clone()]);

                // For DiskBacked, fall back to regular range_scan with post-limit
                // TODO: Implement early termination in BTreeIndex::range_scan
                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        let all_rows = guard
                            .range_scan(
                                start_key.as_ref(),
                                end_key.as_ref(),
                                inclusive_start,
                                inclusive_end,
                            )
                            .unwrap_or_else(|_| vec![]);
                        all_rows.into_iter().take(limit).collect()
                    }
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in range_scan_limit: {}", e);
                        vec![]
                    }
                }
            }
        }
    }

    /// Create a streaming iterator for range scan without materialization.
    ///
    /// Unlike `range_scan()` which returns a `Vec<usize>`, this method returns an
    /// iterator that yields row indices one at a time. This is more efficient for:
    /// - Large result sets where allocating a Vec is expensive
    /// - Queries that may not consume all results
    /// - Better cache locality during processing
    ///
    /// # Arguments
    /// * `start` - Lower bound (None = unbounded)
    /// * `end` - Upper bound (None = unbounded)
    /// * `inclusive_start` - Include rows equal to start value
    /// * `inclusive_end` - Include rows equal to end value
    ///
    /// # Returns
    /// An iterator yielding row indices, or None if the index type doesn't support
    /// streaming (e.g., DiskBacked, IVFFlat, HNSW) or if the range is invalid.
    ///
    /// # Performance
    /// This method has O(log n) setup time and O(1) per-element iteration.
    /// It avoids the O(k) allocation overhead of `range_scan()` where k = matching keys.
    ///
    /// # Limitations
    /// - Only supported for InMemory single-column indexes
    /// - Multi-column indexes fall back to `range_scan()` internally
    /// - DiskBacked indexes don't support streaming yet
    pub fn range_scan_streaming<'a>(
        &'a self,
        start: Option<&SqlValue>,
        end: Option<&SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Option<OwnedStreamingRangeScan<'a>> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Check if this is a multi-column index (streaming not optimized for these)
                let is_multi_column = data.keys().next().is_some_and(|k| k.len() > 1);
                if is_multi_column {
                    // Multi-column indexes need special handling that doesn't fit streaming
                    // Fall back to caller using range_scan() instead
                    return None;
                }

                // Normalize bounds for consistent numeric comparison
                let normalized_start = start.map(normalize_for_comparison);
                let normalized_end = end.map(normalize_for_comparison);

                // Use OwnedStreamingRangeScan which handles bounds and uses BTreeMap::range()
                OwnedStreamingRangeScan::new(
                    data,
                    pending_deletions,
                    normalized_start,
                    normalized_end,
                    inclusive_start,
                    inclusive_end,
                )
            }
            // DiskBacked, IVFFlat, HNSW don't support streaming yet
            _ => None,
        }
    }
}
