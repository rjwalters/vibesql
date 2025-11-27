//! ARM64 NEON-optimized SIMD comparison operations
//!
//! This module provides ARM64-specific comparison implementations using NEON intrinsics.
//! NEON comparisons return mask vectors that need to be converted to boolean results.

#![allow(clippy::needless_range_loop)]

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
use std::arch::aarch64::*;

/// NEON greater-than comparison for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_gt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let thresh = vdupq_n_f64(threshold);

            // NEON comparison returns a mask (0xFFFFFFFFFFFFFFFF for true, 0 for false)
            let mask = vcgtq_f64(values, thresh);

            // Extract mask to array and convert to booleans
            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] > threshold);
    }

    result
}

/// NEON greater-than-or-equal comparison for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_ge_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let thresh = vdupq_n_f64(threshold);
            let mask = vcgeq_f64(values, thresh);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] >= threshold);
    }

    result
}

/// NEON less-than comparison for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_lt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let thresh = vdupq_n_f64(threshold);
            let mask = vcltq_f64(values, thresh);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] < threshold);
    }

    result
}

/// NEON less-than-or-equal comparison for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_le_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let thresh = vdupq_n_f64(threshold);
            let mask = vcleq_f64(values, thresh);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] <= threshold);
    }

    result
}

/// NEON equality comparison for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_eq_f64(column: &[f64], value: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let val_vec = vdupq_n_f64(value);
            let mask = vceqq_f64(values, val_vec);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] == value);
    }

    result
}

/// NEON not-equal comparison for f64 columns (2 elements at a time)
///
/// Note: NEON doesn't have a direct "not equal" instruction,
/// so we use equality and negate the result.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_ne_f64(column: &[f64], value: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_f64(column.as_ptr().add(offset));
            let val_vec = vdupq_n_f64(value);

            // Get equality mask and invert it
            let eq_mask = vceqq_f64(values, val_vec);

            // Extract mask values and manually invert (NOT operation)
            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), eq_mask);

            // Invert the bits: eq mask is all 1s (0xFFFFFFFFFFFFFFFF) for true, 0 for false
            // So NOT equal is the opposite
            result.push(mask_arr[0] == 0);
            result.push(mask_arr[1] == 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] != value);
    }

    result
}

/// NEON greater-than comparison for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_gt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_s64(column.as_ptr().add(offset));
            let thresh = vdupq_n_s64(threshold);
            let mask = vcgtq_s64(values, thresh);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] > threshold);
    }

    result
}

/// NEON less-than comparison for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_lt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_s64(column.as_ptr().add(offset));
            let thresh = vdupq_n_s64(threshold);
            let mask = vcltq_s64(values, thresh);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] < threshold);
    }

    result
}

/// NEON equality comparison for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_eq_i64(column: &[i64], value: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let values = vld1q_s64(column.as_ptr().add(offset));
            let val_vec = vdupq_n_s64(value);
            let mask = vceqq_s64(values, val_vec);

            let mut mask_arr = [0u64; 2];
            vst1q_u64(mask_arr.as_mut_ptr(), mask);

            result.push(mask_arr[0] != 0);
            result.push(mask_arr[1] != 0);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..column.len() {
        result.push(column[i] == value);
    }

    result
}

#[cfg(all(test, feature = "simd", target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn test_neon_gt_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = neon_gt_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![false, false, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_neon_ge_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = neon_ge_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_neon_lt_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = neon_lt_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_neon_le_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = neon_le_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![true, true, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_neon_eq_f64() {
        let column = vec![1.0, 5.0, 3.0, 5.0, 2.0, 5.0, 4.0, 6.0, 9.0];
        let result = neon_eq_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, false, false]
        );
    }

    #[test]
    fn test_neon_ne_f64() {
        let column = vec![1.0, 5.0, 3.0, 5.0, 2.0, 5.0, 4.0, 6.0, 9.0];
        let result = neon_ne_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, true, true]
        );
    }

    #[test]
    fn test_neon_gt_i64() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = neon_gt_i64(&column, 5);
        assert_eq!(
            result,
            vec![false, false, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_neon_lt_i64() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = neon_lt_i64(&column, 5);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_neon_eq_i64() {
        let column = vec![1, 5, 3, 5, 2, 5, 4, 6, 9];
        let result = neon_eq_i64(&column, 5);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, false, false]
        );
    }
}
