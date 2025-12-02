//! COUNT aggregate operations for columnar data processing.
//!
//! This module provides vectorized COUNT operations with both filtered and
//! non-filtered variants using packed and boolean masks.

#![allow(clippy::needless_range_loop)]

use super::super::PackedMask;

// ============================================================================
// FILTERED COUNT OPERATIONS (boolean mask)
// ============================================================================

/// Count of true values in filter mask (number of rows passing filter).
#[inline]
pub fn count_filtered(filter_mask: &[bool]) -> i64 {
    let len = filter_mask.len();
    if len == 0 {
        return 0;
    }

    // Use 4-accumulator pattern for count
    let (mut c0, mut c1, mut c2, mut c3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        c0 += filter_mask[off] as i64;
        c1 += filter_mask[off + 1] as i64;
        c2 += filter_mask[off + 2] as i64;
        c3 += filter_mask[off + 3] as i64;
    }

    let mut count = c0 + c1 + c2 + c3;
    for i in (chunks * 4)..len {
        count += filter_mask[i] as i64;
    }
    count
}

// ============================================================================
// PACKED FILTERED COUNT OPERATIONS
// ============================================================================

/// Count of set bits in filter mask (number of rows passing filter).
///
/// This is a convenience wrapper around PackedMask::count_ones().
#[inline]
pub fn count_packed_filtered(filter_mask: &PackedMask) -> i64 {
    filter_mask.count_ones() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_filtered() {
        let filter = vec![true, false, true, false, true, false, true, false];
        assert_eq!(count_filtered(&filter), 4);

        let filter_all = vec![true; 8];
        assert_eq!(count_filtered(&filter_all), 8);

        let filter_none = vec![false; 8];
        assert_eq!(count_filtered(&filter_none), 0);

        assert_eq!(count_filtered(&[]), 0);
    }

    #[test]
    fn test_count_packed_filtered() {
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        assert_eq!(count_packed_filtered(&mask), 4);
    }
}
