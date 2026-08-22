//! Explicit SIMD intrinsic implementations for critical aggregation paths.
//!
//! This module provides platform-specific SIMD implementations that outperform
//! auto-vectorization for operations where branch-based masking prevents LLVM
//! from reliably generating optimal vector code.
//!
//! # Architecture
//!
//! ```text
//! intrinsics/
//! +-- mod.rs       # This file: dispatch macros and public API
//! +-- avx2.rs      # AVX2 intrinsics (x86_64, 256-bit = 4x f64)
//! +-- neon.rs      # NEON intrinsics (aarch64, 128-bit = 2x f64)
//! +-- scalar.rs    # Scalar fallback (all platforms)
//! ```
//!
//! # Dispatch Strategy
//!
//! - **aarch64**: NEON is always available, so we call NEON directly (no runtime check).
//! - **x86_64**: Runtime detection via `is_x86_feature_detected!("avx2")`. Falls back to scalar if
//!   AVX2 is not available.
//! - **Other**: Always uses scalar fallback.
//!
//! # Safety
//!
//! All platform-specific functions are `unsafe` with `#[target_feature]` annotations.
//! The public API in this module is **safe** -- the dispatch functions handle the
//! `unsafe` boundary internally, calling the appropriate implementation after
//! verifying CPU support.

pub mod avx2;
pub mod neon;
pub mod scalar;

// ============================================================================
// SAFE PUBLIC API (dispatched)
// ============================================================================

/// Filtered SUM of f64 values, dispatched to the best available SIMD.
///
/// Sums all `values[i]` where `mask[i]` is true. Uses explicit SIMD intrinsics
/// (NEON on aarch64, AVX2 on x86_64) to avoid the branch-per-element pattern
/// that prevents reliable auto-vectorization.
///
/// # Performance
///
/// This is the hottest path in TPC-H Q1 and Q6. Explicit SIMD eliminates
/// the branch-per-element overhead that makes auto-vectorization fragile.
///
/// # Panics
///
/// Debug-asserts that `values.len() == mask.len()`.
#[inline]
pub fn sum_f64_filtered(values: &[f64], mask: &[bool]) -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is always available on aarch64.
        // The #[target_feature(enable = "neon")] on the callee ensures the
        // compiler generates NEON instructions. We pass valid slices with
        // matching lengths (debug-asserted).
        unsafe { neon::sum_f64_filtered(values, mask) }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: We just verified AVX2 is available. We pass valid slices
            // with matching lengths (debug-asserted).
            unsafe { avx2::sum_f64_filtered(values, mask) }
        } else {
            scalar::sum_f64_filtered(values, mask)
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar::sum_f64_filtered(values, mask)
    }
}

/// Masked SUM of f64 values, dispatched to the best available SIMD.
///
/// This is the GROUP BY aggregation path. Same semantics as `sum_f64_filtered`
/// but kept as a separate entry point for the `simd::aggregation` module.
#[inline]
pub fn sum_f64_masked(values: &[f64], mask: &[bool]) -> f64 {
    sum_f64_filtered(values, mask)
}

/// Unfiltered SUM of f64 values, dispatched to the best available SIMD.
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: NEON is always available on aarch64.
        unsafe { neon::sum_f64(values) }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 verified above.
            unsafe { avx2::sum_f64(values) }
        } else {
            scalar::sum_f64(values)
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar::sum_f64(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Dispatch tests -- these exercise whichever backend the current platform
    // selects, ensuring the safe API works end-to-end.
    // ========================================================================

    #[test]
    fn test_dispatch_sum_f64_filtered_basic() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true, false, true, false, true, false, true, false];
        let result = sum_f64_filtered(&values, &mask);
        assert!((result - 16.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_all_true() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true; 8];
        let result = sum_f64_filtered(&values, &mask);
        assert!((result - 36.0).abs() < 1e-10);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_all_false() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![false; 8];
        assert_eq!(sum_f64_filtered(&values, &mask), 0.0);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_empty() {
        assert_eq!(sum_f64_filtered(&[], &[]), 0.0);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_single() {
        assert!((sum_f64_filtered(&[42.0], &[true]) - 42.0).abs() < 1e-10);
        assert_eq!(sum_f64_filtered(&[42.0], &[false]), 0.0);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_sub_vector_width() {
        // 3 elements: less than any SIMD width
        let values = vec![10.0, 20.0, 30.0];
        let mask = vec![true, false, true];
        assert!((sum_f64_filtered(&values, &mask) - 40.0).abs() < 1e-10);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_remainder() {
        let values: Vec<f64> = (1..=5).map(|x| x as f64).collect();
        let mask = vec![true, true, true, true, true];
        assert!((sum_f64_filtered(&values, &mask) - 15.0).abs() < 1e-10);
    }

    #[test]
    fn test_dispatch_sum_f64_filtered_large() {
        let values: Vec<f64> = (1..=1024).map(|x| x as f64).collect();
        let mask: Vec<bool> = (0..1024).map(|i| i % 2 == 0).collect();
        let result = sum_f64_filtered(&values, &mask);
        let expected: f64 = (0..1024).filter(|i| i % 2 == 0).map(|i| (i + 1) as f64).sum();
        assert!((result - expected).abs() < 1e-6);
    }

    #[test]
    fn test_dispatch_matches_scalar() {
        // Verify that the dispatched version matches scalar for a variety of inputs
        for len in [0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 1024] {
            let values: Vec<f64> = (0..len).map(|i| (i as f64) * 1.1 + 0.7).collect();
            let mask: Vec<bool> = (0..len).map(|i| i % 3 != 0).collect();

            let dispatched = sum_f64_filtered(&values, &mask);
            let scalar = scalar::sum_f64_filtered(&values, &mask);

            assert!(
                (dispatched - scalar).abs() < 1e-10,
                "len={}: dispatched {} != scalar {}",
                len,
                dispatched,
                scalar
            );
        }
    }

    #[test]
    fn test_dispatch_sum_f64_unfiltered() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert!((sum_f64(&values) - 5050.0).abs() < 1e-10);
        assert_eq!(sum_f64(&[]), 0.0);
    }

    #[test]
    fn test_dispatch_sum_f64_unfiltered_matches_scalar() {
        for len in [0, 1, 3, 4, 5, 7, 8, 9, 16, 17, 100, 1024] {
            let values: Vec<f64> = (0..len).map(|i| (i as f64) * 2.3 + 0.1).collect();
            let dispatched = sum_f64(&values);
            let scalar = scalar::sum_f64(&values);
            assert!(
                (dispatched - scalar).abs() < 1e-10,
                "len={}: dispatched {} != scalar {}",
                len,
                dispatched,
                scalar
            );
        }
    }
}
