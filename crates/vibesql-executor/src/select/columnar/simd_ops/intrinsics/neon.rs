//! NEON intrinsic implementations for ARM aarch64.
//!
//! NEON provides 128-bit vector registers (2x f64 lanes). On aarch64, NEON
//! is always available -- there is no runtime feature detection needed.
//!
//! # Safety
//!
//! All functions in this module use `unsafe` NEON intrinsics. They are marked
//! `#[target_feature(enable = "neon")]` and must be called through the dispatch
//! layer which verifies CPU support.

#![allow(clippy::needless_range_loop)]

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// NEON-accelerated filtered SUM for f64 values.
///
/// Processes 2 f64 values per iteration using 128-bit NEON registers.
/// Uses `vbslq_f64` (bitwise select) to blend masked values with zero,
/// avoiding branches in the inner loop.
///
/// # Safety
///
/// Requires aarch64 with NEON support (always available on aarch64).
/// Caller must ensure `values.len() == mask.len()`.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn sum_f64_filtered(values: &[f64], mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), mask.len());
    let len = values.len().min(mask.len());
    if len == 0 {
        return 0.0;
    }

    // Use 2 vector accumulators (each holds 2x f64 = 4 f64 lanes total)
    // This breaks loop-carried dependencies and maximizes throughput.
    let mut acc0 = vdupq_n_f64(0.0);
    let mut acc1 = vdupq_n_f64(0.0);
    let zero = vdupq_n_f64(0.0);

    // Process 4 elements per iteration (2 NEON registers x 2 lanes each)
    let chunks = len / 4;
    let values_ptr = values.as_ptr();
    let mask_ptr = mask.as_ptr();

    for i in 0..chunks {
        let off = i * 4;

        // Load 2x f64 values into each register
        // SAFETY: off + 3 < chunks * 4 <= len, so all accesses are in bounds
        let v0 = vld1q_f64(values_ptr.add(off));
        let v1 = vld1q_f64(values_ptr.add(off + 2));

        // Build mask vectors from bool array.
        // Each bool is 1 byte; we need to expand to 64-bit all-ones or all-zeros
        // for vbslq_f64 (bitwise select).
        //
        // SAFETY: mask_ptr.add(off)..mask_ptr.add(off+3) are in bounds
        let m0_lo = if *mask_ptr.add(off) { u64::MAX } else { 0u64 };
        let m0_hi = if *mask_ptr.add(off + 1) { u64::MAX } else { 0u64 };
        let m1_lo = if *mask_ptr.add(off + 2) { u64::MAX } else { 0u64 };
        let m1_hi = if *mask_ptr.add(off + 3) { u64::MAX } else { 0u64 };

        // Create NEON mask registers (reinterpret u64x2 as the mask for vbslq)
        let mask0: uint64x2_t = vcombine_u64(vcreate_u64(m0_lo), vcreate_u64(m0_hi));
        let mask1: uint64x2_t = vcombine_u64(vcreate_u64(m1_lo), vcreate_u64(m1_hi));

        // vbslq_f64: for each bit, select from v if mask bit is 1, else zero
        let masked0 = vbslq_f64(mask0, v0, zero);
        let masked1 = vbslq_f64(mask1, v1, zero);

        // Accumulate
        acc0 = vaddq_f64(acc0, masked0);
        acc1 = vaddq_f64(acc1, masked1);
    }

    // Combine the two accumulators
    let combined = vaddq_f64(acc0, acc1);

    // Horizontal sum: extract both lanes and add
    let sum = vgetq_lane_f64(combined, 0) + vgetq_lane_f64(combined, 1);

    // Handle remainder (0-3 elements)
    let mut remainder_sum = sum;
    for i in (chunks * 4)..len {
        if mask[i] {
            remainder_sum += values[i];
        }
    }
    remainder_sum
}

/// NEON-accelerated masked SUM for f64 values (GROUP BY path).
///
/// # Safety
///
/// Requires aarch64 with NEON support.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn sum_f64_masked(values: &[f64], mask: &[bool]) -> f64 {
    sum_f64_filtered(values, mask)
}

/// NEON-accelerated unfiltered SUM for f64 values.
///
/// # Safety
///
/// Requires aarch64 with NEON support.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
pub unsafe fn sum_f64(values: &[f64]) -> f64 {
    let len = values.len();
    if len == 0 {
        return 0.0;
    }

    let mut acc0 = vdupq_n_f64(0.0);
    let mut acc1 = vdupq_n_f64(0.0);

    let chunks = len / 4;
    let ptr = values.as_ptr();

    for i in 0..chunks {
        let off = i * 4;
        // SAFETY: off + 3 < chunks * 4 <= len
        let v0 = vld1q_f64(ptr.add(off));
        let v1 = vld1q_f64(ptr.add(off + 2));
        acc0 = vaddq_f64(acc0, v0);
        acc1 = vaddq_f64(acc1, v1);
    }

    let combined = vaddq_f64(acc0, acc1);
    let mut sum = vgetq_lane_f64(combined, 0) + vgetq_lane_f64(combined, 1);

    for i in (chunks * 4)..len {
        sum += values[i];
    }
    sum
}

#[cfg(all(test, target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn test_neon_sum_f64_filtered_basic() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true, false, true, false, true, false, true, false];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 16.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_neon_sum_f64_filtered_all_true() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true; 8];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 36.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_neon_sum_f64_filtered_all_false() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![false; 8];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_neon_sum_f64_filtered_empty() {
        let result = unsafe { sum_f64_filtered(&[], &[]) };
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_neon_sum_f64_filtered_remainder() {
        // Non-multiple of 4
        let values: Vec<f64> = (1..=7).map(|x| x as f64).collect();
        let mask = vec![true, false, true, false, true, false, true];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        // 1 + 3 + 5 + 7 = 16
        assert!((result - 16.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_neon_sum_f64_filtered_single() {
        let values = vec![42.0];
        let mask = vec![true];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 42.0).abs() < 1e-10);

        let mask_f = vec![false];
        let result = unsafe { sum_f64_filtered(&values, &mask_f) };
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_neon_sum_f64_filtered_three_elements() {
        // Sub-vector-width (3 < 4)
        let values = vec![10.0, 20.0, 30.0];
        let mask = vec![true, true, true];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 60.0).abs() < 1e-10);
    }

    #[test]
    fn test_neon_sum_f64_filtered_exact_four() {
        let values = vec![1.0, 2.0, 3.0, 4.0];
        let mask = vec![true, false, true, false];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_neon_sum_f64_filtered_five_elements() {
        // One remainder element
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mask = vec![true, true, true, true, true];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 15.0).abs() < 1e-10);
    }

    #[test]
    fn test_neon_sum_f64_filtered_large() {
        // 1024 elements (batch size)
        let values: Vec<f64> = (1..=1024).map(|x| x as f64).collect();
        let mask: Vec<bool> = (0..1024).map(|i| i % 2 == 0).collect();
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        // Sum of odd numbers 1,3,5,...,1023 = 512^2 = 262144
        let expected: f64 = (0..1024).filter(|i| i % 2 == 0).map(|i| (i + 1) as f64).sum();
        assert!((result - expected).abs() < 1e-6, "got {} expected {}", result, expected);
    }

    #[test]
    fn test_neon_matches_scalar() {
        // Property test: NEON and scalar must produce identical results
        let values: Vec<f64> = (0..100).map(|i| (i as f64) * 1.1 + 0.7).collect();
        let mask: Vec<bool> = (0..100).map(|i| i % 3 != 0).collect();

        let neon_result = unsafe { sum_f64_filtered(&values, &mask) };
        let scalar_result = super::super::scalar::sum_f64_filtered(&values, &mask);

        assert!(
            (neon_result - scalar_result).abs() < 1e-10,
            "NEON {} != scalar {}",
            neon_result,
            scalar_result
        );
    }

    #[test]
    fn test_neon_sum_f64_unfiltered() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let result = unsafe { sum_f64(&values) };
        assert!((result - 5050.0).abs() < 1e-10);
    }

    #[test]
    fn test_neon_sum_f64_unfiltered_empty() {
        let result = unsafe { sum_f64(&[]) };
        assert_eq!(result, 0.0);
    }
}
