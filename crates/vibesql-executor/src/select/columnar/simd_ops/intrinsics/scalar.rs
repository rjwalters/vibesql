//! Scalar fallback implementations for SIMD operations.
//!
//! These functions are used on platforms without SIMD support or as a reference
//! implementation for correctness testing. They use the same 4-accumulator
//! pattern as the auto-vectorized versions.

#![allow(clippy::needless_range_loop)]

/// Scalar filtered SUM for f64 values.
///
/// Sums all `values[i]` where `mask[i]` is true, using a 4-accumulator
/// pattern to break loop-carried dependencies.
///
/// This is the reference implementation against which SIMD variants are tested.
#[inline]
pub fn sum_f64_filtered(values: &[f64], mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), mask.len());
    let len = values.len().min(mask.len());
    if len == 0 {
        return 0.0;
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if mask[off] {
            s0 += values[off];
        }
        if mask[off + 1] {
            s1 += values[off + 1];
        }
        if mask[off + 2] {
            s2 += values[off + 2];
        }
        if mask[off + 3] {
            s3 += values[off + 3];
        }
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..len {
        if mask[i] {
            sum += values[i];
        }
    }
    sum
}

/// Scalar masked SUM for f64 values (used by GROUP BY aggregation).
///
/// Identical semantics to `sum_f64_filtered` but kept as a separate function
/// for use in the `simd::aggregation` dispatch path.
#[inline]
pub fn sum_f64_masked(values: &[f64], mask: &[bool]) -> f64 {
    sum_f64_filtered(values, mask)
}

/// Scalar unfiltered SUM for f64 values.
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_f64_filtered_basic() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true, false, true, false, true, false, true, false];
        assert!((sum_f64_filtered(&values, &mask) - 16.0).abs() < 1e-10);
    }

    #[test]
    fn test_sum_f64_filtered_all_true() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true; 8];
        assert!((sum_f64_filtered(&values, &mask) - 36.0).abs() < 1e-10);
    }

    #[test]
    fn test_sum_f64_filtered_all_false() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![false; 8];
        assert_eq!(sum_f64_filtered(&values, &mask), 0.0);
    }

    #[test]
    fn test_sum_f64_filtered_empty() {
        assert_eq!(sum_f64_filtered(&[], &[]), 0.0);
    }

    #[test]
    fn test_sum_f64_filtered_remainder() {
        // Non-multiple of 4
        let values: Vec<f64> = (1..=7).map(|x| x as f64).collect();
        let mask = vec![true, false, true, false, true, false, true];
        assert!((sum_f64_filtered(&values, &mask) - 16.0).abs() < 1e-10);
    }

    #[test]
    fn test_sum_f64_filtered_single() {
        let values = vec![42.0];
        let mask = vec![true];
        assert!((sum_f64_filtered(&values, &mask) - 42.0).abs() < 1e-10);

        let mask_f = vec![false];
        assert_eq!(sum_f64_filtered(&values, &mask_f), 0.0);
    }

    #[test]
    fn test_sum_f64_unfiltered() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert!((sum_f64(&values) - 5050.0).abs() < 1e-10);
        assert_eq!(sum_f64(&[]), 0.0);
    }
}
