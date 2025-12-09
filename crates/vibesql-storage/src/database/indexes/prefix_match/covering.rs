// ============================================================================
// Covering Index Operations - Index-only scans returning key values
// ============================================================================
//
// These methods return both key values AND row indices, enabling index-only
// queries where all SELECT columns are part of the index key.

use std::ops::Bound;
use vibesql_types::SqlValue;

use crate::database::indexes::index_metadata::IndexData;
use crate::database::indexes::range_bounds::{try_increment_sqlvalue, try_increment_sqlvalue_prefix};
use crate::database::indexes::value_normalization::normalize_for_comparison;

use super::compute_prefix_upper_bound;

impl IndexData {
    /// Covering index scan - returns both key values AND row indices for index-only queries
    ///
    /// This is the key method for covering index optimization. Instead of returning
    /// just row indices (which would require fetching rows from the table), this method
    /// returns the full index key values along with their row indices.
    ///
    /// For queries where all SELECT columns are part of the index key, the caller can
    /// construct result rows directly from the returned key values without any table access.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for equality predicates (e.g., [s_w_id])
    /// * `upper_bound` - Upper bound for the trailing column (e.g., s_quantity < 10)
    /// * `inclusive_upper` - Whether the upper bound is inclusive (<=) or exclusive (<)
    ///
    /// # Returns
    /// Vector of (key_values, row_indices) pairs. Each key_values is the full index key
    /// (e.g., [s_w_id, s_quantity, s_i_id]) that matched the predicate.
    ///
    /// # Example
    /// ```text
    /// // Index on (s_w_id, s_quantity, s_i_id)
    /// // Query: SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_quantity < 10
    /// // This is a covering index scan - s_i_id is in the index key!
    /// let results = index_data.prefix_bounded_scan_covering(
    ///     &[SqlValue::Integer(1)],  // prefix: s_w_id = 1
    ///     &SqlValue::Integer(10),   // upper_bound: s_quantity < 10
    ///     false,                     // exclusive (<)
    /// );
    /// // results: [(key=[1, 5, 100], rows=[0]), (key=[1, 7, 200], rows=[1]), ...]
    /// // Caller can extract s_i_id from key[2] without table fetch!
    /// ```
    ///
    /// # Performance
    /// O(log n + k) where k is matching keys. Eliminates table fetches for covered queries,
    /// which is critical for TPC-C Stock-Level query where ~300 items may need lookup.
    pub fn prefix_bounded_scan_covering(
        &self,
        prefix: &[SqlValue],
        upper_bound: &SqlValue,
        inclusive_upper: bool,
    ) -> Vec<(Vec<SqlValue>, Vec<usize>)> {
        if prefix.is_empty() {
            // Empty prefix with upper bound is not well-defined
            return Vec::new();
        }

        // Normalize values for consistent comparison
        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();
        let normalized_bound = normalize_for_comparison(upper_bound);

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Start bound: [prefix] (inclusive)
                let start_key = normalized_prefix.clone();
                let start_bound: Bound<Vec<SqlValue>> = Bound::Included(start_key);

                // End bound: [prefix, upper_bound] (exclusive or inclusive depending on flag)
                let mut end_key = normalized_prefix.clone();
                end_key.push(normalized_bound);
                let end_bound: Bound<Vec<SqlValue>> = if inclusive_upper {
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
                };

                let mut results = Vec::new();

                for (key_values, row_indices) in data.range((start_bound, end_bound)) {
                    // Verify prefix match
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        // Apply pending deletions adjustment to row indices
                        let adjusted_indices = if pending_deletions.is_empty() {
                            row_indices.clone()
                        } else {
                            row_indices
                                .iter()
                                .map(|&row_idx| {
                                    let decrement =
                                        pending_deletions.partition_point(|&d| d < row_idx);
                                    row_idx - decrement
                                })
                                .collect()
                        };
                        results.push((key_values.clone(), adjusted_indices));
                    }
                }

                results
            }
            IndexData::DiskBacked { .. } => {
                // For disk-backed indexes, fall back to non-covering scan for now
                // TODO: Implement covering scan for disk-backed indexes
                Vec::new()
            }
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                // Vector indexes don't support covering scans
                Vec::new()
            }
        }
    }

    /// Covering index range scan with both lower and upper bounds
    ///
    /// Similar to `prefix_bounded_scan_covering` but supports both lower and upper bounds
    /// on the trailing column. Returns key values along with row indices for index-only queries.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for equality predicates
    /// * `lower_bound` - Lower bound for the trailing column (optional)
    /// * `inclusive_lower` - Whether lower bound is inclusive
    /// * `upper_bound` - Upper bound for the trailing column (optional)
    /// * `inclusive_upper` - Whether upper bound is inclusive
    ///
    /// # Returns
    /// Vector of (key_values, row_indices) pairs for covering index scans
    pub fn prefix_range_scan_covering(
        &self,
        prefix: &[SqlValue],
        lower_bound: Option<&SqlValue>,
        inclusive_lower: bool,
        upper_bound: Option<&SqlValue>,
        inclusive_upper: bool,
    ) -> Vec<(Vec<SqlValue>, Vec<usize>)> {
        if prefix.is_empty() {
            return Vec::new();
        }

        // If no bounds specified, fall back to prefix_scan_covering
        if lower_bound.is_none() && upper_bound.is_none() {
            return self.prefix_scan_covering(prefix);
        }

        // Check for inverted range
        if let (Some(lb), Some(ub)) = (lower_bound, upper_bound) {
            let normalized_lb = normalize_for_comparison(lb);
            let normalized_ub = normalize_for_comparison(ub);
            if normalized_lb > normalized_ub {
                return Vec::new();
            }
        }

        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
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
                        let last_idx = end_key.len() - 1;
                        match try_increment_sqlvalue(&end_key[last_idx]) {
                            Some(next_val) => {
                                end_key[last_idx] = next_val;
                                Bound::Excluded(end_key)
                            }
                            None => Bound::Included(end_key),
                        }
                    } else {
                        Bound::Excluded(end_key)
                    }
                } else {
                    let end_key = normalized_prefix.clone();
                    match try_increment_sqlvalue_prefix(&end_key) {
                        Some(next_prefix) => Bound::Excluded(next_prefix),
                        None => Bound::Unbounded,
                    }
                };

                let mut results = Vec::new();

                for (key_values, row_indices) in data.range((start_bound, end_bound)) {
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        let adjusted_indices = if pending_deletions.is_empty() {
                            row_indices.clone()
                        } else {
                            row_indices
                                .iter()
                                .map(|&row_idx| {
                                    let decrement =
                                        pending_deletions.partition_point(|&d| d < row_idx);
                                    row_idx - decrement
                                })
                                .collect()
                        };
                        results.push((key_values.clone(), adjusted_indices));
                    }
                }

                results
            }
            IndexData::DiskBacked { .. } | IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                Vec::new()
            }
        }
    }

    /// Covering prefix scan - returns key values and row indices for index-only queries
    ///
    /// This is a covering version of `prefix_scan` that returns the full index keys
    /// along with row indices, enabling index-only scans without table access.
    ///
    /// # Arguments
    /// * `prefix` - Prefix values for the first N index columns
    ///
    /// # Returns
    /// Vector of (key_values, row_indices) pairs
    pub fn prefix_scan_covering(&self, prefix: &[SqlValue]) -> Vec<(Vec<SqlValue>, Vec<usize>)> {
        if prefix.is_empty() {
            // Empty prefix - return all entries with their keys
            return self.keys_and_values();
        }

        let normalized_prefix: Vec<SqlValue> =
            prefix.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                let end_key = compute_prefix_upper_bound(&normalized_prefix);

                let start_bound: Bound<&[SqlValue]> = Bound::Included(normalized_prefix.as_slice());
                let end_bound: Bound<&[SqlValue]> = match end_key.as_ref() {
                    Some(key) => Bound::Excluded(key.as_slice()),
                    None => Bound::Unbounded,
                };

                let mut results = Vec::new();

                for (key_values, row_indices) in
                    data.range::<[SqlValue], _>((start_bound, end_bound))
                {
                    if key_values.len() >= normalized_prefix.len()
                        && key_values[..normalized_prefix.len()] == normalized_prefix[..]
                    {
                        let adjusted_indices = if pending_deletions.is_empty() {
                            row_indices.clone()
                        } else {
                            row_indices
                                .iter()
                                .map(|&row_idx| {
                                    let decrement =
                                        pending_deletions.partition_point(|&d| d < row_idx);
                                    row_idx - decrement
                                })
                                .collect()
                        };
                        results.push((key_values.clone(), adjusted_indices));
                    }
                }

                results
            }
            IndexData::DiskBacked { .. } | IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                Vec::new()
            }
        }
    }

    /// Returns all keys and their row indices (for covering scans on empty prefix)
    pub(super) fn keys_and_values(&self) -> Vec<(Vec<SqlValue>, Vec<usize>)> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                data.iter()
                    .map(|(k, v)| {
                        let adjusted = if pending_deletions.is_empty() {
                            v.clone()
                        } else {
                            v.iter()
                                .map(|&row_idx| {
                                    let decrement =
                                        pending_deletions.partition_point(|&d| d < row_idx);
                                    row_idx - decrement
                                })
                                .collect()
                        };
                        (k.clone(), adjusted)
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}
