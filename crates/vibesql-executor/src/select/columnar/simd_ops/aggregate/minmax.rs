//! MIN/MAX aggregate operations for columnar data processing.
//!
//! This module provides vectorized MIN/MAX operations with both filtered and
//! non-filtered variants using packed and boolean masks.

#![allow(clippy::needless_range_loop)]

use super::super::PackedMask;

// ============================================================================
// BASIC MIN/MAX OPERATIONS
// ============================================================================

/// Minimum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn min_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Minimum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn min_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Maximum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn max_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

/// Maximum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn max_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

// ============================================================================
// FILTERED MIN/MAX OPERATIONS (boolean mask)
// ============================================================================

/// Min of f64 values where filter_mask[i] == true.
#[inline]
pub fn min_f64_filtered(values: &[f64], filter_mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            m0 = m0.min(values[off]);
        }
        if filter_mask[off + 1] {
            m1 = m1.min(values[off + 1]);
        }
        if filter_mask[off + 2] {
            m2 = m2.min(values[off + 2]);
        }
        if filter_mask[off + 3] {
            m3 = m3.min(values[off + 3]);
        }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.min(values[i]);
        }
    }

    if result == f64::INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Max of f64 values where filter_mask[i] == true.
#[inline]
pub fn max_f64_filtered(values: &[f64], filter_mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            m0 = m0.max(values[off]);
        }
        if filter_mask[off + 1] {
            m1 = m1.max(values[off + 1]);
        }
        if filter_mask[off + 2] {
            m2 = m2.max(values[off + 2]);
        }
        if filter_mask[off + 3] {
            m3 = m3.max(values[off + 3]);
        }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.max(values[i]);
        }
    }

    if result == f64::NEG_INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Min of i64 values where filter_mask[i] == true.
#[inline]
pub fn min_i64_filtered(values: &[i64], filter_mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            m0 = m0.min(values[off]);
        }
        if filter_mask[off + 1] {
            m1 = m1.min(values[off + 1]);
        }
        if filter_mask[off + 2] {
            m2 = m2.min(values[off + 2]);
        }
        if filter_mask[off + 3] {
            m3 = m3.min(values[off + 3]);
        }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.min(values[i]);
        }
    }

    if result == i64::MAX {
        None
    } else {
        Some(result)
    }
}

/// Max of i64 values where filter_mask[i] == true.
#[inline]
pub fn max_i64_filtered(values: &[i64], filter_mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            m0 = m0.max(values[off]);
        }
        if filter_mask[off + 1] {
            m1 = m1.max(values[off + 1]);
        }
        if filter_mask[off + 2] {
            m2 = m2.max(values[off + 2]);
        }
        if filter_mask[off + 3] {
            m3 = m3.max(values[off + 3]);
        }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.max(values[i]);
        }
    }

    if result == i64::MIN {
        None
    } else {
        Some(result)
    }
}

// ============================================================================
// PACKED FILTERED MIN/MAX OPERATIONS
// ============================================================================

/// Min of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn min_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = f64::INFINITY;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.min(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == f64::INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Max of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn max_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = f64::NEG_INFINITY;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.max(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == f64::NEG_INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Min of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn min_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = i64::MAX;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.min(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == i64::MAX {
        None
    } else {
        Some(result)
    }
}

/// Max of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn max_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = i64::MIN;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.max(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == i64::MIN {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_max_i64() {
        let values = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(9));
        assert_eq!(min_i64(&[]), None);
        assert_eq!(max_i64(&[]), None);
    }

    #[test]
    fn test_min_max_f64() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        assert_eq!(min_f64(&values), Some(1.0));
        assert_eq!(max_f64(&values), Some(9.0));
        assert_eq!(min_f64(&[]), None);
        assert_eq!(max_f64(&[]), None);
    }

    #[test]
    fn test_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<i64> = (1..=7).collect(); // 7 elements
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(7));
    }

    #[test]
    fn test_min_max_f64_filtered() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0];
        let filter = vec![true, false, true, false, true, false, true, false];

        // Only indices 0, 2, 4, 6: values 5, 8, 9, 7
        assert_eq!(min_f64_filtered(&values, &filter), Some(5.0));
        assert_eq!(max_f64_filtered(&values, &filter), Some(9.0));

        // No rows pass filter
        let filter_none = vec![false; 8];
        assert_eq!(min_f64_filtered(&values, &filter_none), None);
        assert_eq!(max_f64_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_min_max_i64_filtered() {
        let values = vec![5i64, 2, 8, 1, 9, 3, 7, 4];
        let filter = vec![true, false, true, false, true, false, true, false];

        assert_eq!(min_i64_filtered(&values, &filter), Some(5));
        assert_eq!(max_i64_filtered(&values, &filter), Some(9));

        let filter_none = vec![false; 8];
        assert_eq!(min_i64_filtered(&values, &filter_none), None);
        assert_eq!(max_i64_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_min_max_f64_packed_filtered() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0];
        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        // Only indices 0, 2, 4, 6: values 5, 8, 9, 7
        assert_eq!(min_f64_packed_filtered(&values, &filter), Some(5.0));
        assert_eq!(max_f64_packed_filtered(&values, &filter), Some(9.0));

        // No rows pass filter
        let filter_none = PackedMask::new_all_clear(8);
        assert_eq!(min_f64_packed_filtered(&values, &filter_none), None);
        assert_eq!(max_f64_packed_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_min_max_i64_packed_filtered() {
        let values = vec![5i64, 2, 8, 1, 9, 3, 7, 4];
        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        assert_eq!(min_i64_packed_filtered(&values, &filter), Some(5));
        assert_eq!(max_i64_packed_filtered(&values, &filter), Some(9));

        let filter_none = PackedMask::new_all_clear(8);
        assert_eq!(min_i64_packed_filtered(&values, &filter_none), None);
        assert_eq!(max_i64_packed_filtered(&values, &filter_none), None);
    }
}
