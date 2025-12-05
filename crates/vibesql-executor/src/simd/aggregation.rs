//! SIMD-accelerated masked aggregation functions for GROUP BY operations
//!
//! This module provides vectorized aggregation functions that operate on typed
//! arrays with boolean mask filtering. These are used by the columnar GROUP BY
//! implementation to compute per-group aggregates efficiently.
//!
//! # Performance
//!
//! The 4-accumulator pattern enables LLVM auto-vectorization:
//! - Breaks loop-carried dependencies
//! - Allows parallel SIMD lane execution
//! - Matches explicit SIMD performance without platform-specific code
//!
//! # Masking
//!
//! All functions accept a `mask` parameter where `true` indicates the value
//! should be included in the aggregate. This enables efficient per-group
//! computation without materializing filtered arrays.

#![allow(clippy::needless_range_loop)]

/// Count the number of true values in a mask
///
/// This is used for COUNT aggregates and to compute AVG denominators.
#[inline]
pub fn simd_count_masked(mask: &[bool]) -> usize {
    // Count is naturally vectorizable - each element is independent
    mask.iter().filter(|&&b| b).count()
}

/// Sum of i64 values where mask is true, using 4-accumulator pattern
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements)
#[inline]
pub fn simd_sum_i64_masked(values: &[i64], mask: &[bool]) -> i64 {
    debug_assert_eq!(values.len(), mask.len());

    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        // Branchless selection: multiply by mask (0 or 1)
        s0 = s0.wrapping_add(if mask[off] { values[off] } else { 0 });
        s1 = s1.wrapping_add(if mask[off + 1] { values[off + 1] } else { 0 });
        s2 = s2.wrapping_add(if mask[off + 2] { values[off + 2] } else { 0 });
        s3 = s3.wrapping_add(if mask[off + 3] { values[off + 3] } else { 0 });
    }

    let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            sum = sum.wrapping_add(values[i]);
        }
    }
    sum
}

/// Sum of f64 values where mask is true, using 4-accumulator pattern
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements)
/// WARNING: Do NOT replace with iterator pattern - it's 4x slower!
#[inline]
pub fn simd_sum_f64_masked(values: &[f64], mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), mask.len());

    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        // Branchless selection using multiplication
        s0 += if mask[off] { values[off] } else { 0.0 };
        s1 += if mask[off + 1] { values[off + 1] } else { 0.0 };
        s2 += if mask[off + 2] { values[off + 2] } else { 0.0 };
        s3 += if mask[off + 3] { values[off + 3] } else { 0.0 };
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            sum += values[i];
        }
    }
    sum
}

/// Minimum of i64 values where mask is true, using 4-lane parallel reduction
///
/// Returns None if no values match the mask.
#[inline]
pub fn simd_min_i64_masked(values: &[i64], mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), mask.len());

    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        if mask[off] {
            m0 = m0.min(values[off]);
        }
        if mask[off + 1] {
            m1 = m1.min(values[off + 1]);
        }
        if mask[off + 2] {
            m2 = m2.min(values[off + 2]);
        }
        if mask[off + 3] {
            m3 = m3.min(values[off + 3]);
        }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            result = result.min(values[i]);
        }
    }

    // Check if any value was actually found
    if result == i64::MAX {
        // Verify there was at least one masked value
        if !mask.iter().any(|&b| b) {
            return None;
        }
    }
    Some(result)
}

/// Minimum of f64 values where mask is true, using 4-lane parallel reduction
///
/// Returns None if no values match the mask.
#[inline]
pub fn simd_min_f64_masked(values: &[f64], mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), mask.len());

    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        if mask[off] {
            m0 = m0.min(values[off]);
        }
        if mask[off + 1] {
            m1 = m1.min(values[off + 1]);
        }
        if mask[off + 2] {
            m2 = m2.min(values[off + 2]);
        }
        if mask[off + 3] {
            m3 = m3.min(values[off + 3]);
        }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            result = result.min(values[i]);
        }
    }

    // Check if any value was actually found
    if result == f64::INFINITY && !mask.iter().any(|&b| b) {
        return None;
    }
    Some(result)
}

/// Maximum of i64 values where mask is true, using 4-lane parallel reduction
///
/// Returns None if no values match the mask.
#[inline]
pub fn simd_max_i64_masked(values: &[i64], mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), mask.len());

    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        if mask[off] {
            m0 = m0.max(values[off]);
        }
        if mask[off + 1] {
            m1 = m1.max(values[off + 1]);
        }
        if mask[off + 2] {
            m2 = m2.max(values[off + 2]);
        }
        if mask[off + 3] {
            m3 = m3.max(values[off + 3]);
        }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            result = result.max(values[i]);
        }
    }

    // Check if any value was actually found
    if result == i64::MIN && !mask.iter().any(|&b| b) {
        return None;
    }
    Some(result)
}

/// Maximum of f64 values where mask is true, using 4-lane parallel reduction
///
/// Returns None if no values match the mask.
#[inline]
pub fn simd_max_f64_masked(values: &[f64], mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), mask.len());

    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        if mask[off] {
            m0 = m0.max(values[off]);
        }
        if mask[off + 1] {
            m1 = m1.max(values[off + 1]);
        }
        if mask[off + 2] {
            m2 = m2.max(values[off + 2]);
        }
        if mask[off + 3] {
            m3 = m3.max(values[off + 3]);
        }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        if mask[i] {
            result = result.max(values[i]);
        }
    }

    // Check if any value was actually found
    if result == f64::NEG_INFINITY && !mask.iter().any(|&b| b) {
        return None;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_count_masked() {
        let mask = vec![true, false, true, false, true];
        assert_eq!(simd_count_masked(&mask), 3);

        let all_true = vec![true; 10];
        assert_eq!(simd_count_masked(&all_true), 10);

        let all_false = vec![false; 10];
        assert_eq!(simd_count_masked(&all_false), 0);
    }

    #[test]
    fn test_simd_sum_i64_masked() {
        let values: Vec<i64> = (1..=10).collect();
        let mask = vec![true, false, true, false, true, false, true, false, true, false];
        // Sum of 1 + 3 + 5 + 7 + 9 = 25
        assert_eq!(simd_sum_i64_masked(&values, &mask), 25);

        let all_true = vec![true; 10];
        assert_eq!(simd_sum_i64_masked(&values, &all_true), 55);

        let all_false = vec![false; 10];
        assert_eq!(simd_sum_i64_masked(&values, &all_false), 0);
    }

    #[test]
    fn test_simd_sum_f64_masked() {
        let values: Vec<f64> = (1..=10).map(|x| x as f64).collect();
        let mask = vec![true, false, true, false, true, false, true, false, true, false];
        assert!((simd_sum_f64_masked(&values, &mask) - 25.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_min_i64_masked() {
        let values = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let mask = vec![true, false, true, false, true, false, true, false, true];
        // Min of 5, 8, 9, 7, 6 = 5
        assert_eq!(simd_min_i64_masked(&values, &mask), Some(5));

        let all_false = vec![false; 9];
        assert_eq!(simd_min_i64_masked(&values, &all_false), None);
    }

    #[test]
    fn test_simd_max_i64_masked() {
        let values = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let mask = vec![true, false, true, false, true, false, true, false, true];
        // Max of 5, 8, 9, 7, 6 = 9
        assert_eq!(simd_max_i64_masked(&values, &mask), Some(9));

        let all_false = vec![false; 9];
        assert_eq!(simd_max_i64_masked(&values, &all_false), None);
    }

    #[test]
    fn test_simd_min_f64_masked() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0];
        let mask = vec![true, false, true, false, true];
        assert_eq!(simd_min_f64_masked(&values, &mask), Some(5.0));
    }

    #[test]
    fn test_simd_max_f64_masked() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0];
        let mask = vec![true, false, true, false, true];
        assert_eq!(simd_max_f64_masked(&values, &mask), Some(9.0));
    }

    #[test]
    fn test_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<i64> = (1..=7).collect();
        let mask = vec![true; 7];
        assert_eq!(simd_sum_i64_masked(&values, &mask), 28);
        assert_eq!(simd_min_i64_masked(&values, &mask), Some(1));
        assert_eq!(simd_max_i64_masked(&values, &mask), Some(7));
    }
}
