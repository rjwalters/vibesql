// ============================================================================
// Skip Scan Operations - Filter on non-prefix columns
// ============================================================================
//
// These methods implement skip-scan optimization for queries that filter on
// columns that are not the prefix of a composite index.

use vibesql_types::SqlValue;

use crate::database::indexes::{
    index_metadata::{acquire_btree_lock, IndexData},
    value_normalization::{normalize_bound_for_range_scan, normalize_for_comparison},
};

impl IndexData {
    /// Skip-scan: Lookup rows matching a non-prefix column filter
    ///
    /// This implements the skip-scan optimization for queries that filter on
    /// non-prefix columns of a composite index. Instead of a full table scan,
    /// it:
    /// 1. Gets distinct values of the prefix column(s)
    /// 2. For each prefix value, seeks to (prefix, filter_value) in the index
    /// 3. Returns matching rows
    ///
    /// # Arguments
    /// * `filter_column_idx` - Index of the column being filtered (1 = second column, etc.)
    /// * `filter_value` - Value to filter on (equality predicate)
    ///
    /// # Returns
    /// Vector of row indices matching the filter across all prefix values
    ///
    /// # Example
    /// ```text
    /// // Index on (region, date) - query: WHERE date = '2024-01-01'
    /// // Skip-scan visits each region and looks up '2024-01-01' entries
    /// let rows = index_data.skip_scan_equality(1, &SqlValue::Varchar("2024-01-01".into()));
    /// ```
    ///
    /// # Performance
    /// Cost = O(prefix_cardinality * log(n) + k) where:
    /// - prefix_cardinality = number of distinct prefix values
    /// - n = total index entries
    /// - k = matching rows
    ///
    /// This is beneficial when prefix_cardinality is low and the filter is selective.
    pub fn skip_scan_equality(
        &self,
        filter_column_idx: usize,
        filter_value: &SqlValue,
    ) -> Vec<usize> {
        if filter_column_idx == 0 {
            // Not a skip-scan - use regular prefix lookup
            return self.prefix_multi_lookup(std::slice::from_ref(filter_value));
        }

        let normalized_filter = normalize_for_comparison(filter_value);

        match self {
            IndexData::InMemory { data } => {
                let mut matching_rows = Vec::new();

                // Group keys by their prefix and find those matching the filter
                for (key, row_indices) in data.iter() {
                    // Check if the filter column exists and matches
                    if key.len() > filter_column_idx {
                        let key_filter_val = &key[filter_column_idx];
                        if *key_filter_val == normalized_filter {
                            matching_rows.extend(row_indices);
                        }
                    }
                }

                matching_rows
            }
            IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                Ok(guard) => guard
                    .skip_scan_equality(filter_column_idx, &normalized_filter)
                    .unwrap_or_else(|_| vec![]),
                Err(e) => {
                    log::warn!("BTreeIndex lock acquisition failed in skip_scan_equality: {}", e);
                    vec![]
                }
            },
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                // Vector indexes don't support skip-scan
                vec![]
            }
        }
    }

    /// Skip-scan with range filter on non-prefix column
    ///
    /// Similar to `skip_scan_equality` but supports range predicates
    /// (>, >=, <, <=, BETWEEN) on the non-prefix column.
    ///
    /// # Arguments
    /// * `filter_column_idx` - Index of the column being filtered
    /// * `lower_bound` - Optional lower bound for the range
    /// * `inclusive_lower` - Whether lower bound is inclusive
    /// * `upper_bound` - Optional upper bound for the range
    /// * `inclusive_upper` - Whether upper bound is inclusive
    ///
    /// # Returns
    /// Vector of row indices matching the range filter across all prefix values
    pub fn skip_scan_range(
        &self,
        filter_column_idx: usize,
        lower_bound: Option<&SqlValue>,
        inclusive_lower: bool,
        upper_bound: Option<&SqlValue>,
        inclusive_upper: bool,
    ) -> Vec<usize> {
        if filter_column_idx == 0 {
            // Not a skip-scan - use regular range scan
            return self.range_scan(lower_bound, upper_bound, inclusive_lower, inclusive_upper);
        }

        // Normalize bounds for consistent numeric comparison, correcting
        // inclusive/exclusive flags for out-of-f64-precision integer
        // literals compared against REAL-affinity columns (issue #6575),
        // same as `range_scan`'s use of `normalize_range_bounds` (#6587).
        // `skip_scan_range` evaluates each side's predicate manually rather
        // than via `BTreeMap::range()`, so each bound is corrected
        // independently with `normalize_bound_for_range_scan`.
        let (normalized_lower, inclusive_lower) = match lower_bound {
            Some(v) => {
                let (nv, inc) = normalize_bound_for_range_scan(v, inclusive_lower, true);
                (Some(nv), inc)
            }
            None => (None, inclusive_lower),
        };
        let (normalized_upper, inclusive_upper) = match upper_bound {
            Some(v) => {
                let (nv, inc) = normalize_bound_for_range_scan(v, inclusive_upper, false);
                (Some(nv), inc)
            }
            None => (None, inclusive_upper),
        };

        match self {
            IndexData::InMemory { data } => {
                let mut matching_rows = Vec::new();

                for (key, row_indices) in data.iter() {
                    if key.len() > filter_column_idx {
                        let key_filter_val = &key[filter_column_idx];

                        // Check range bounds
                        let passes_lower = match &normalized_lower {
                            None => true,
                            Some(lb) => {
                                if inclusive_lower {
                                    key_filter_val >= lb
                                } else {
                                    key_filter_val > lb
                                }
                            }
                        };

                        let passes_upper = match &normalized_upper {
                            None => true,
                            Some(ub) => {
                                if inclusive_upper {
                                    key_filter_val <= ub
                                } else {
                                    key_filter_val < ub
                                }
                            }
                        };

                        if passes_lower && passes_upper {
                            matching_rows.extend(row_indices);
                        }
                    }
                }

                matching_rows
            }
            IndexData::DiskBacked { btree, .. } => match acquire_btree_lock(btree) {
                Ok(guard) => guard
                    .skip_scan_range(
                        filter_column_idx,
                        normalized_lower.as_ref(),
                        inclusive_lower,
                        normalized_upper.as_ref(),
                        inclusive_upper,
                    )
                    .unwrap_or_else(|_| vec![]),
                Err(e) => {
                    log::warn!("BTreeIndex lock acquisition failed in skip_scan_range: {}", e);
                    vec![]
                }
            },
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                // Vector indexes don't support skip-scan
                vec![]
            }
        }
    }
}
