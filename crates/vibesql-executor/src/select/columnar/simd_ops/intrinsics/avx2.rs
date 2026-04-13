//! AVX2 intrinsic implementations for x86_64.
//!
//! AVX2 provides 256-bit vector registers (4x f64 lanes). AVX2 requires
//! runtime feature detection via `is_x86_feature_detected!("avx2")`.
//!
//! # Safety
//!
//! All functions in this module use `unsafe` AVX2 intrinsics. They are marked
//! `#[target_feature(enable = "avx2")]` and must only be called after verifying
//! AVX2 support via the dispatch layer.

#![allow(clippy::needless_range_loop)]

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// AVX2-accelerated filtered SUM for f64 values.
///
/// Processes 4 f64 values per iteration using 256-bit AVX registers.
/// Uses `_mm256_blendv_pd` to conditionally select values based on
/// the boolean mask, avoiding branches in the inner loop.
///
/// The mask conversion works as follows:
/// - Load 4 bool bytes (each 0x00 or 0x01)
/// - Convert to 32-bit integers via `_mm_cvtepu8_epi32`
/// - Widen to 64-bit via `_mm256_cvtepi32_epi64`
/// - Compare against zero to produce all-1s / all-0s per lane
/// - Use as blend mask for `_mm256_blendv_pd`
///
/// # Safety
///
/// Requires x86_64 with AVX2 support. Caller must verify via
/// `is_x86_feature_detected!("avx2")` before calling.
/// Caller must ensure `values.len() == mask.len()`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn sum_f64_filtered(values: &[f64], mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), mask.len());
    let len = values.len().min(mask.len());
    if len == 0 {
        return 0.0;
    }

    // Two 256-bit accumulators (4x f64 each = 8 f64 lanes total)
    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();
    let zero = _mm256_setzero_pd();
    let ones_epi64 = _mm256_set1_epi64x(0); // For comparison: mask != 0

    // Process 8 elements per iteration (2 AVX2 registers x 4 lanes)
    let chunks = len / 8;
    let values_ptr = values.as_ptr();
    let mask_ptr = mask.as_ptr() as *const u8;

    for i in 0..chunks {
        let off = i * 8;

        // Load 4x f64 values into each register
        // SAFETY: off + 7 < chunks * 8 <= len
        let v0 = _mm256_loadu_pd(values_ptr.add(off));
        let v1 = _mm256_loadu_pd(values_ptr.add(off + 4));

        // Load 4 mask bytes, expand to 64-bit lanes for blending.
        // Each bool is 1 byte (0x00 or 0x01).
        //
        // Step 1: Load 4 bytes into a 128-bit register
        // SAFETY: mask_ptr + off .. mask_ptr + off + 7 are in bounds
        let m0_bytes = _mm_cvtsi32_si128(std::ptr::read_unaligned(mask_ptr.add(off) as *const i32));
        let m1_bytes =
            _mm_cvtsi32_si128(std::ptr::read_unaligned(mask_ptr.add(off + 4) as *const i32));

        // Step 2: Zero-extend bytes to 32-bit integers
        let m0_epi32 = _mm_cvtepu8_epi32(m0_bytes);
        let m1_epi32 = _mm_cvtepu8_epi32(m1_bytes);

        // Step 3: Zero-extend 32-bit to 64-bit
        let m0_epi64 = _mm256_cvtepi32_epi64(m0_epi32);
        let m1_epi64 = _mm256_cvtepi32_epi64(m1_epi32);

        // Step 4: Compare != 0 to get all-1s / all-0s per 64-bit lane
        // _mm256_cmpeq_epi64 returns all-1s where equal; we compare against zero
        // and invert by using the "not equal" logic: XOR with all-1s
        let eq0 = _mm256_cmpeq_epi64(m0_epi64, ones_epi64);
        let eq1 = _mm256_cmpeq_epi64(m1_epi64, ones_epi64);
        // eq is all-1s where mask was 0 (false); we want the opposite
        // NOT(eq) gives all-1s where mask was non-zero (true)
        let blend_mask0 = _mm256_castsi256_pd(_mm256_xor_si256(
            eq0,
            _mm256_set1_epi64x(-1), // all-1s
        ));
        let blend_mask1 = _mm256_castsi256_pd(_mm256_xor_si256(
            eq1,
            _mm256_set1_epi64x(-1),
        ));

        // Blend: select value where mask is true, zero where false
        // _mm256_blendv_pd checks the sign bit of each 64-bit lane in the mask
        let masked0 = _mm256_blendv_pd(zero, v0, blend_mask0);
        let masked1 = _mm256_blendv_pd(zero, v1, blend_mask1);

        // Accumulate
        acc0 = _mm256_add_pd(acc0, masked0);
        acc1 = _mm256_add_pd(acc1, masked1);
    }

    // Combine the two accumulators
    let combined = _mm256_add_pd(acc0, acc1);

    // Horizontal reduction: 4 lanes -> 1 scalar
    // Step 1: hadd pairs adjacent lanes: [a+b, c+d, a+b, c+d] (128-bit halves)
    let hadd = _mm256_hadd_pd(combined, combined);
    // Step 2: Extract high 128-bit half and add to low half
    let hi128 = _mm256_extractf128_pd(hadd, 1);
    let lo128 = _mm256_castpd256_pd128(hadd);
    let sum_vec = _mm_add_sd(lo128, hi128);
    let mut sum = _mm_cvtsd_f64(sum_vec);

    // Handle remaining elements that didn't fill a full 8-element chunk
    let remainder_start = chunks * 8;
    // Process remaining in groups of 4 if possible
    if remainder_start + 4 <= len {
        let v = _mm256_loadu_pd(values_ptr.add(remainder_start));
        let m_bytes = _mm_cvtsi32_si128(
            std::ptr::read_unaligned(mask_ptr.add(remainder_start) as *const i32),
        );
        let m_epi32 = _mm_cvtepu8_epi32(m_bytes);
        let m_epi64 = _mm256_cvtepi32_epi64(m_epi32);
        let eq = _mm256_cmpeq_epi64(m_epi64, ones_epi64);
        let blend = _mm256_castsi256_pd(_mm256_xor_si256(eq, _mm256_set1_epi64x(-1)));
        let masked = _mm256_blendv_pd(zero, v, blend);
        let hadd2 = _mm256_hadd_pd(masked, masked);
        let hi2 = _mm256_extractf128_pd(hadd2, 1);
        let lo2 = _mm256_castpd256_pd128(hadd2);
        let s2 = _mm_add_sd(lo2, hi2);
        sum += _mm_cvtsd_f64(s2);

        // Scalar tail for last 0-3 elements
        for i in (remainder_start + 4)..len {
            if mask[i] {
                sum += values[i];
            }
        }
    } else {
        // Scalar tail for last 0-7 elements
        for i in remainder_start..len {
            if mask[i] {
                sum += values[i];
            }
        }
    }

    sum
}

/// AVX2-accelerated masked SUM for f64 values (GROUP BY path).
///
/// # Safety
///
/// Requires x86_64 with AVX2 support.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn sum_f64_masked(values: &[f64], mask: &[bool]) -> f64 {
    sum_f64_filtered(values, mask)
}

/// AVX2-accelerated unfiltered SUM for f64 values.
///
/// # Safety
///
/// Requires x86_64 with AVX2 support.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn sum_f64(values: &[f64]) -> f64 {
    let len = values.len();
    if len == 0 {
        return 0.0;
    }

    let mut acc0 = _mm256_setzero_pd();
    let mut acc1 = _mm256_setzero_pd();

    let chunks = len / 8;
    let ptr = values.as_ptr();

    for i in 0..chunks {
        let off = i * 8;
        // SAFETY: off + 7 < chunks * 8 <= len
        let v0 = _mm256_loadu_pd(ptr.add(off));
        let v1 = _mm256_loadu_pd(ptr.add(off + 4));
        acc0 = _mm256_add_pd(acc0, v0);
        acc1 = _mm256_add_pd(acc1, v1);
    }

    let combined = _mm256_add_pd(acc0, acc1);
    let hadd = _mm256_hadd_pd(combined, combined);
    let hi128 = _mm256_extractf128_pd(hadd, 1);
    let lo128 = _mm256_castpd256_pd128(hadd);
    let sum_vec = _mm_add_sd(lo128, hi128);
    let mut sum = _mm_cvtsd_f64(sum_vec);

    for i in (chunks * 8)..len {
        sum += values[i];
    }
    sum
}

// Tests are gated to x86_64 only; on aarch64 (Apple Silicon) these won't compile.
#[cfg(all(test, target_arch = "x86_64"))]
mod tests {
    use super::*;

    fn has_avx2() -> bool {
        is_x86_feature_detected!("avx2")
    }

    #[test]
    fn test_avx2_sum_f64_filtered_basic() {
        if !has_avx2() {
            return;
        }
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true, false, true, false, true, false, true, false];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 16.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_avx2_sum_f64_filtered_all_true() {
        if !has_avx2() {
            return;
        }
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![true; 8];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 36.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_avx2_sum_f64_filtered_all_false() {
        if !has_avx2() {
            return;
        }
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mask = vec![false; 8];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_avx2_sum_f64_filtered_empty() {
        if !has_avx2() {
            return;
        }
        let result = unsafe { sum_f64_filtered(&[], &[]) };
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_avx2_sum_f64_filtered_remainder() {
        if !has_avx2() {
            return;
        }
        let values: Vec<f64> = (1..=7).map(|x| x as f64).collect();
        let mask = vec![true, false, true, false, true, false, true];
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        assert!((result - 16.0).abs() < 1e-10, "got {}", result);
    }

    #[test]
    fn test_avx2_sum_f64_filtered_large() {
        if !has_avx2() {
            return;
        }
        let values: Vec<f64> = (1..=1024).map(|x| x as f64).collect();
        let mask: Vec<bool> = (0..1024).map(|i| i % 2 == 0).collect();
        let result = unsafe { sum_f64_filtered(&values, &mask) };
        let expected: f64 = (0..1024).filter(|i| i % 2 == 0).map(|i| (i + 1) as f64).sum();
        assert!((result - expected).abs() < 1e-6, "got {} expected {}", result, expected);
    }

    #[test]
    fn test_avx2_matches_scalar() {
        if !has_avx2() {
            return;
        }
        let values: Vec<f64> = (0..100).map(|i| (i as f64) * 1.1 + 0.7).collect();
        let mask: Vec<bool> = (0..100).map(|i| i % 3 != 0).collect();

        let avx2_result = unsafe { sum_f64_filtered(&values, &mask) };
        let scalar_result = super::super::scalar::sum_f64_filtered(&values, &mask);

        assert!(
            (avx2_result - scalar_result).abs() < 1e-10,
            "AVX2 {} != scalar {}",
            avx2_result,
            scalar_result
        );
    }

    #[test]
    fn test_avx2_sum_f64_unfiltered() {
        if !has_avx2() {
            return;
        }
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let result = unsafe { sum_f64(&values) };
        assert!((result - 5050.0).abs() < 1e-10);
    }
}
