// ============================================================================
// Streaming Range Scan - Iterator-based range scan without materialization
// ============================================================================
//
// This module provides streaming iterators for range scans that avoid
// materializing all matching row indices into a Vec. This is critical for
// performance on range queries without LIMIT.
//
// Key benefits:
// - No upfront Vec allocation for matching indices
// - Memory usage proportional to output, not total matches
// - Better cache locality (process rows as they're found)

use std::collections::btree_map;
use std::ops::Bound;

use vibesql_types::SqlValue;

/// Self-contained streaming range scan that owns its bounds.
///
/// This iterator owns the normalized start/end keys and uses BTreeMap::range()
/// for efficient seeking instead of iterating through all entries.
///
/// The key insight is that we can store the bound keys in the struct and
/// create the range iterator lazily on first access, avoiding lifetime issues.
pub struct OwnedStreamingRangeScan<'a> {
    /// The BTreeMap data reference
    data: &'a std::collections::BTreeMap<Vec<SqlValue>, Vec<usize>>,
    /// Current position within the current key's row indices
    current_indices: Option<std::slice::Iter<'a, usize>>,
    /// Reference to pending deletions for adjustment
    pending_deletions: &'a [usize],
    /// Normalized start bound key (owned)
    start_key: Option<Vec<SqlValue>>,
    /// Normalized end bound key (owned)
    end_key: Option<Vec<SqlValue>>,
    /// Whether start is inclusive
    inclusive_start: bool,
    /// Whether end is inclusive
    inclusive_end: bool,
    /// The range iterator (created lazily)
    range_iter: Option<btree_map::Range<'a, Vec<SqlValue>, Vec<usize>>>,
    /// Whether we've initialized the range iterator
    initialized: bool,
}

impl<'a> OwnedStreamingRangeScan<'a> {
    /// Create a new owned streaming range scan.
    ///
    /// Returns None if the range is empty or invalid.
    pub fn new(
        data: &'a std::collections::BTreeMap<Vec<SqlValue>, Vec<usize>>,
        pending_deletions: &'a [usize],
        start: Option<SqlValue>,
        end: Option<SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Option<Self> {
        // Check for empty/invalid ranges
        if let (Some(start_val), Some(end_val)) = (&start, &end) {
            if start_val == end_val && (!inclusive_start || !inclusive_end) {
                return None; // Empty range
            }
            if start_val > end_val {
                return None; // Inverted range
            }
        }

        // Convert bounds to key format
        let start_key = start.map(|v| vec![v]);
        let end_key = end.map(|v| vec![v]);

        Some(Self {
            data,
            current_indices: None,
            pending_deletions,
            start_key,
            end_key,
            inclusive_start,
            inclusive_end,
            range_iter: None,
            initialized: false,
        })
    }

    /// Adjust a row index by accounting for pending deletions.
    #[inline]
    fn adjust_row_index(&self, row_idx: usize) -> usize {
        if self.pending_deletions.is_empty() {
            row_idx
        } else {
            let decrement = self.pending_deletions.partition_point(|&d| d < row_idx);
            row_idx - decrement
        }
    }

    /// Initialize the range iterator if not already done.
    /// This uses unsafe to extend the lifetime of the keys, which is safe
    /// because the keys are stored in self and won't be modified.
    fn ensure_initialized(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Build bounds for BTreeMap::range()
        // We need to create bounds that reference our stored keys
        let start_bound: Bound<&[SqlValue]> = match &self.start_key {
            Some(key) if self.inclusive_start => Bound::Included(key.as_slice()),
            Some(key) => Bound::Excluded(key.as_slice()),
            None => Bound::Unbounded,
        };

        let end_bound: Bound<&[SqlValue]> = match &self.end_key {
            Some(key) if self.inclusive_end => Bound::Included(key.as_slice()),
            Some(key) => Bound::Excluded(key.as_slice()),
            None => Bound::Unbounded,
        };

        // Check for invalid range (both bounds excluded at same value)
        if let (Bound::Excluded(s), Bound::Excluded(e)) = (&start_bound, &end_bound) {
            if s == e {
                return; // Invalid range - leave range_iter as None
            }
        }

        // Create the range iterator
        // SAFETY: The bounds reference self.start_key and self.end_key which are
        // stored in self. The range iterator only needs these references to be
        // valid during iteration, and since we're storing the keys in self,
        // they will outlive the iterator.
        self.range_iter = Some(self.data.range::<[SqlValue], _>((start_bound, end_bound)));
    }
}

impl Iterator for OwnedStreamingRangeScan<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Ensure we've initialized the range iterator
        self.ensure_initialized();

        loop {
            // Try to get the next index from the current key's indices
            if let Some(ref mut indices) = self.current_indices {
                if let Some(&row_idx) = indices.next() {
                    return Some(self.adjust_row_index(row_idx));
                }
            }

            // Move to the next key in the range
            let range_iter = self.range_iter.as_mut()?;
            match range_iter.next() {
                Some((_key, row_indices)) => {
                    self.current_indices = Some(row_indices.iter());
                    // Loop back to try getting from this key's indices
                }
                None => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_owned_streaming_range_scan_basic() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(2)], vec![1, 2]);
        data.insert(vec![SqlValue::Integer(3)], vec![3]);
        data.insert(vec![SqlValue::Integer(4)], vec![4, 5, 6]);
        data.insert(vec![SqlValue::Integer(5)], vec![7]);

        let pending_deletions: Vec<usize> = vec![];

        // Range scan for values 2..=4
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            Some(SqlValue::Integer(2)),
            Some(SqlValue::Integer(4)),
            true,
            true,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_owned_streaming_range_scan_with_pending_deletions() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(2)], vec![1]);
        data.insert(vec![SqlValue::Integer(3)], vec![2]);
        data.insert(vec![SqlValue::Integer(4)], vec![3]);
        data.insert(vec![SqlValue::Integer(5)], vec![4]);

        // Row at index 1 was deleted
        let pending_deletions: Vec<usize> = vec![1];

        // Full scan
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            None,
            None,
            true,
            true,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        // Original: [0, 1, 2, 3, 4]
        // After adjusting for deletion at 1:
        // - 0 stays 0 (no deletions before it)
        // - 1 stays 1 (deletion at 1 is not < 1)
        // - 2 becomes 1 (1 deletion before it)
        // - 3 becomes 2 (1 deletion before it)
        // - 4 becomes 3 (1 deletion before it)
        assert_eq!(results, vec![0, 1, 1, 2, 3]);
    }

    #[test]
    fn test_owned_streaming_range_scan_empty_range() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(5)], vec![4]);

        let pending_deletions: Vec<usize> = vec![];

        // Range 3..=4 has no matching keys
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            Some(SqlValue::Integer(3)),
            Some(SqlValue::Integer(4)),
            true,
            true,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        let expected: Vec<usize> = vec![];
        assert_eq!(results, expected);
    }

    #[test]
    fn test_owned_streaming_range_scan_inverted_range() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);

        let pending_deletions: Vec<usize> = vec![];

        // Inverted range: 5..=1
        let result = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            Some(SqlValue::Integer(5)),
            Some(SqlValue::Integer(1)),
            true,
            true,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_owned_streaming_range_scan_exclusive_bounds() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(2)], vec![1]);
        data.insert(vec![SqlValue::Integer(3)], vec![2]);
        data.insert(vec![SqlValue::Integer(4)], vec![3]);
        data.insert(vec![SqlValue::Integer(5)], vec![4]);

        let pending_deletions: Vec<usize> = vec![];

        // Range 2 < x < 4 (exclusive both ends)
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            Some(SqlValue::Integer(2)),
            Some(SqlValue::Integer(4)),
            false,
            false,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        assert_eq!(results, vec![2]); // Only key 3
    }

    #[test]
    fn test_owned_streaming_unbounded_start() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(2)], vec![1]);
        data.insert(vec![SqlValue::Integer(3)], vec![2]);

        let pending_deletions: Vec<usize> = vec![];

        // Range ..=2
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            None,
            Some(SqlValue::Integer(2)),
            true,
            true,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        assert_eq!(results, vec![0, 1]); // Keys 1 and 2
    }

    #[test]
    fn test_owned_streaming_unbounded_end() {
        let mut data: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
        data.insert(vec![SqlValue::Integer(1)], vec![0]);
        data.insert(vec![SqlValue::Integer(2)], vec![1]);
        data.insert(vec![SqlValue::Integer(3)], vec![2]);

        let pending_deletions: Vec<usize> = vec![];

        // Range 2..
        let iter = OwnedStreamingRangeScan::new(
            &data,
            &pending_deletions,
            Some(SqlValue::Integer(2)),
            None,
            true,
            true,
        )
        .unwrap();

        let results: Vec<usize> = iter.collect();
        assert_eq!(results, vec![1, 2]); // Keys 2 and 3
    }
}
