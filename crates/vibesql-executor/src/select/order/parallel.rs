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
pub(super) fn sort_rows(rows: &mut [RowWithSortKeys]) {
    let comparison_fn = |(_, keys_a): &RowWithSortKeys, (_, keys_b): &RowWithSortKeys| {
        let keys_a = keys_a.as_ref().unwrap();
        let keys_b = keys_b.as_ref().unwrap();
        compare_rows_by_sort_keys(keys_a, keys_b)
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
