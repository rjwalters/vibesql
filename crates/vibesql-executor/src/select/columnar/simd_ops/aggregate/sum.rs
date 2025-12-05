//! SUM aggregate operations for columnar data processing.
//!
//! This module provides vectorized SUM operations with both filtered and
//! non-filtered variants using packed and boolean masks.

#![allow(clippy::needless_range_loop)]

use super::super::PackedMask;

// ============================================================================
// BASIC SUM OPERATIONS
// ============================================================================

/// Sum of i64 values using 4-accumulator auto-vectorization pattern.
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements).
#[inline]
pub fn sum_i64(values: &[i64]) -> i64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 = s0.wrapping_add(values[off]);
        s1 = s1.wrapping_add(values[off + 1]);
        s2 = s2.wrapping_add(values[off + 2]);
        s3 = s3.wrapping_add(values[off + 3]);
    }

    let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
    for i in (chunks * 4)..values.len() {
        sum = sum.wrapping_add(values[i]);
    }
    sum
}

/// Sum of f64 values using 4-accumulator auto-vectorization pattern.
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements).
/// WARNING: Do NOT replace with `.iter().sum()` - it's 4x slower!
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 += values[off];
        s1 += values[off + 1];
        s2 += values[off + 2];
        s3 += values[off + 3];
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..values.len() {
        sum += values[i];
    }
    sum
}

// ============================================================================
// FILTERED SUM OPERATIONS (boolean mask)
// ============================================================================

/// Sum of f64 values where filter_mask[i] == true, using 4-accumulator pattern.
///
/// This fuses filtering and aggregation to avoid intermediate allocations.
/// For simple aggregates like Q6, this can provide 2-3x speedup.
///
/// # Arguments
/// * `values` - Array of values to aggregate
/// * `filter_mask` - Boolean mask (true = include in aggregate)
///
/// # Returns
/// Sum of values where filter_mask is true
#[inline]
pub fn sum_f64_filtered(values: &[f64], filter_mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return 0.0;
    }

    // Use 4-accumulator pattern with conditional adds
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        // 4-accumulator pattern: reduces loop-carried dependencies
        // The compiler may auto-vectorize these conditional adds
        if filter_mask[off] {
            s0 += values[off];
        }
        if filter_mask[off + 1] {
            s1 += values[off + 1];
        }
        if filter_mask[off + 2] {
            s2 += values[off + 2];
        }
        if filter_mask[off + 3] {
            s3 += values[off + 3];
        }
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum += values[i];
        }
    }
    sum
}

/// Sum of i64 values where filter_mask[i] == true.
#[inline]
pub fn sum_i64_filtered(values: &[i64], filter_mask: &[bool]) -> i64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return 0;
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            s0 = s0.wrapping_add(values[off]);
        }
        if filter_mask[off + 1] {
            s1 = s1.wrapping_add(values[off + 1]);
        }
        if filter_mask[off + 2] {
            s2 = s2.wrapping_add(values[off + 2]);
        }
        if filter_mask[off + 3] {
            s3 = s3.wrapping_add(values[off + 3]);
        }
    }

    let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum = sum.wrapping_add(values[i]);
        }
    }
    sum
}

// ============================================================================
// PACKED FILTERED SUM OPERATIONS
// ============================================================================

/// Sum of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn sum_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> f64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return 0.0;
    }

    // Process word by word for better cache utilization
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue; // Skip entirely zero words
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        // Process set bits in this word
        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                // Distribute across 4 accumulators based on bit position
                match bit_pos % 4 {
                    0 => s0 += values[idx],
                    1 => s1 += values[idx],
                    2 => s2 += values[idx],
                    _ => s3 += values[idx],
                }
            }
            bits &= bits - 1; // Clear lowest set bit
        }
    }

    s0 + s1 + s2 + s3
}

/// Sum of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn sum_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> i64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return 0;
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
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
                match bit_pos % 4 {
                    0 => s0 = s0.wrapping_add(values[idx]),
                    1 => s1 = s1.wrapping_add(values[idx]),
                    2 => s2 = s2.wrapping_add(values[idx]),
                    _ => s3 = s3.wrapping_add(values[idx]),
                }
            }
            bits &= bits - 1;
        }
    }

    s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i64() {
        let values: Vec<i64> = (1..=100).collect();
        assert_eq!(sum_i64(&values), 5050);
        assert_eq!(sum_i64(&[]), 0);
        assert_eq!(sum_i64(&[42]), 42);
    }

    #[test]
    fn test_sum_f64() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert!((sum_f64(&values) - 5050.0).abs() < 0.001);
        assert_eq!(sum_f64(&[]), 0.0);
    }

    #[test]
    fn test_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<i64> = (1..=7).collect(); // 7 elements
        assert_eq!(sum_i64(&values), 28);

        let values: Vec<i64> = (1..=5).collect(); // 5 elements
        assert_eq!(sum_i64(&values), 15);
    }

    #[test]
    fn test_sum_f64_filtered() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let filter = vec![true, false, true, false, true, false, true, false];
        // Sum only odd positions: 1 + 3 + 5 + 7 = 16
        assert!((sum_f64_filtered(&values, &filter) - 16.0).abs() < 0.001);

        // All true
        let filter_all = vec![true; 8];
        assert!((sum_f64_filtered(&values, &filter_all) - 36.0).abs() < 0.001);

        // All false
        let filter_none = vec![false; 8];
        assert_eq!(sum_f64_filtered(&values, &filter_none), 0.0);

        // Empty
        assert_eq!(sum_f64_filtered(&[], &[]), 0.0);
    }

    #[test]
    fn test_sum_i64_filtered() {
        let values = vec![1i64, 2, 3, 4, 5, 6, 7, 8];
        let filter = vec![true, false, true, false, true, false, true, false];
        assert_eq!(sum_i64_filtered(&values, &filter), 16);

        let filter_all = vec![true; 8];
        assert_eq!(sum_i64_filtered(&values, &filter_all), 36);
    }

    #[test]
    fn test_filtered_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<f64> = (1..=7).map(|x| x as f64).collect(); // 7 elements
        let filter = vec![true, false, true, false, true, false, true]; // 1, 3, 5, 7 = 16
        assert!((sum_f64_filtered(&values, &filter) - 16.0).abs() < 0.001);
    }

    #[test]
    fn test_sum_f64_packed_filtered() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        // Sum of 1, 3, 5, 7 = 16
        assert!((sum_f64_packed_filtered(&values, &mask) - 16.0).abs() < 0.001);

        // All true
        let mask_all = PackedMask::new_all_set(8);
        assert!((sum_f64_packed_filtered(&values, &mask_all) - 36.0).abs() < 0.001);

        // All false
        let mask_none = PackedMask::new_all_clear(8);
        assert_eq!(sum_f64_packed_filtered(&values, &mask_none), 0.0);
    }

    #[test]
    fn test_sum_i64_packed_filtered() {
        let values = vec![1i64, 2, 3, 4, 5, 6, 7, 8];
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        assert_eq!(sum_i64_packed_filtered(&values, &mask), 16);
    }
}
