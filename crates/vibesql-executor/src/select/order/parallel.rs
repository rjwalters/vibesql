//! Parallel sorting implementation for ORDER BY.
//!
//! This module provides parallel sorting capabilities using rayon when the
//! `parallel` feature is enabled. It automatically chooses between parallel
//! and sequential sorting based on dataset size.

#[cfg(feature = "parallel")]
use rayon::slice::ParallelSliceMut;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

use super::comparison::compare_rows_by_sort_keys;
use super::RowWithSortKeys;

/// Sort rows using either parallel or sequential sorting based on dataset size.
///
/// When the `parallel` feature is enabled, this function will use rayon's
/// parallel sort for large datasets and fall back to sequential sort for
/// smaller ones based on the global ParallelConfig thresholds.
///
/// When the `parallel` feature is disabled, this always uses sequential sort.
///
/// # Tie-Breaking with rowid
///
/// When ORDER BY expressions are equal, SQLite uses rowid as a secondary sort key
/// to maintain deterministic ordering. This function implements the same behavior
/// by comparing row_id values when sort keys are equal (issue #4893).
pub(super) fn sort_rows(rows: &mut [RowWithSortKeys]) {
    let comparison_fn = |(row_a, keys_a): &RowWithSortKeys, (row_b, keys_b): &RowWithSortKeys| {
        let keys_a = keys_a.as_ref().unwrap();
        let keys_b = keys_b.as_ref().unwrap();
        let sort_cmp = compare_rows_by_sort_keys(keys_a, keys_b);
        if sort_cmp != std::cmp::Ordering::Equal {
            return sort_cmp;
        }
        // Use rowid as tie-breaker for deterministic ordering matching SQLite behavior
        // When sort keys are equal, order by rowid (insertion order) ascending.
        // Rowids are SIGNED (issue #5835): row_id stores the two's-complement
        // bit pattern of an i64, so compare in signed space — a rowid of -1
        // (u64::MAX) must sort before 0. Identical to u64 order for
        // non-negative rowids.
        row_a.row_id.map(|r| r as i64).cmp(&row_b.row_id.map(|r| r as i64))
    };

    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_sort(rows.len()) {
            // Parallel sort for large datasets
            rows.par_sort_by(comparison_fn);
        } else {
            // Sequential sort for small datasets
            rows.sort_by(comparison_fn);
        }
    }

    #[cfg(not(feature = "parallel"))]
    {
        // Always use sequential sort when parallel feature is disabled
        rows.sort_by(comparison_fn);
    }
}
