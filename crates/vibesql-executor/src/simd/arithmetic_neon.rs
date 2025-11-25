//! ARM64 NEON-optimized SIMD arithmetic operations
//!
//! This module provides ARM64-specific implementations using NEON intrinsics.
//! NEON uses 128-bit vectors (2 doubles or 2 longs) and provides unique
//! ARM-specific optimizations like fused multiply-add (FMLA).

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
use std::arch::aarch64::*;

/// NEON addition for f64 columns (2 elements at a time)
///
/// Uses ARM64 NEON intrinsics for 128-bit SIMD operations.
/// Processes 2 f64 values per iteration.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 2 elements with NEON (f64x2)
    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            // Load 2 f64 values from a and b
            let a_vec = vld1q_f64(a.as_ptr().add(offset));
            let b_vec = vld1q_f64(b.as_ptr().add(offset));

            // Perform SIMD addition
            let sum = vaddq_f64(a_vec, b_vec);

            // Store result
            let mut temp = [0.0; 2];
            vst1q_f64(temp.as_mut_ptr(), sum);
            result.extend_from_slice(&temp);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// NEON subtraction for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_f64(a.as_ptr().add(offset));
            let b_vec = vld1q_f64(b.as_ptr().add(offset));
            let diff = vsubq_f64(a_vec, b_vec);

            let mut temp = [0.0; 2];
            vst1q_f64(temp.as_mut_ptr(), diff);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// NEON multiplication for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_f64(a.as_ptr().add(offset));
            let b_vec = vld1q_f64(b.as_ptr().add(offset));
            let product = vmulq_f64(a_vec, b_vec);

            let mut temp = [0.0; 2];
            vst1q_f64(temp.as_mut_ptr(), product);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}

/// NEON division for f64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_f64(a.as_ptr().add(offset));
            let b_vec = vld1q_f64(b.as_ptr().add(offset));
            let quotient = vdivq_f64(a_vec, b_vec);

            let mut temp = [0.0; 2];
            vst1q_f64(temp.as_mut_ptr(), quotient);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] / b[i]);
    }

    result
}

/// NEON fused multiply-add for f64 columns (2 elements at a time)
///
/// Computes a * b + c in a single instruction, which is faster and more
/// accurate than separate multiply and add operations.
/// This is a NEON-specific optimization using the FMLA instruction.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_fma_f64(a: &[f64], b: &[f64], c: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_f64(a.as_ptr().add(offset));
            let b_vec = vld1q_f64(b.as_ptr().add(offset));
            let c_vec = vld1q_f64(c.as_ptr().add(offset));

            // Fused multiply-add: a * b + c
            let fma_result = vfmaq_f64(c_vec, a_vec, b_vec);

            let mut temp = [0.0; 2];
            vst1q_f64(temp.as_mut_ptr(), fma_result);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i] + c[i]);
    }

    result
}

/// NEON addition for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_s64(a.as_ptr().add(offset));
            let b_vec = vld1q_s64(b.as_ptr().add(offset));
            let sum = vaddq_s64(a_vec, b_vec);

            let mut temp = [0i64; 2];
            vst1q_s64(temp.as_mut_ptr(), sum);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// NEON subtraction for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    let chunks = a.len() / 2;
    for i in 0..chunks {
        let offset = i * 2;

        unsafe {
            let a_vec = vld1q_s64(a.as_ptr().add(offset));
            let b_vec = vld1q_s64(b.as_ptr().add(offset));
            let diff = vsubq_s64(a_vec, b_vec);

            let mut temp = [0i64; 2];
            vst1q_s64(temp.as_mut_ptr(), diff);
            result.extend_from_slice(&temp);
        }
    }

    let remainder_start = chunks * 2;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// NEON multiplication for i64 columns (2 elements at a time)
///
/// Note: NEON doesn't have native 64-bit integer multiplication,
/// so we fall back to scalar operations for now.
/// A future optimization could use 32-bit SIMD multiplication
/// and combine results, but that adds complexity.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_mul_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    // NEON doesn't have efficient 64-bit integer multiplication
    // Fall back to scalar implementation
    a.iter().zip(b.iter()).map(|(x, y)| x * y).collect()
}

#[cfg(all(test, feature = "simd", target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn test_neon_add_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let result = neon_add_f64(&a, &b);
        assert_eq!(
            result,
            vec![11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0, 99.0]
        );
    }

    #[test]
    fn test_neon_sub_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = neon_sub_f64(&a, &b);
        assert_eq!(
            result,
            vec![9.0, 18.0, 27.0, 36.0, 45.0, 54.0, 63.0, 72.0, 81.0]
        );
    }

    #[test]
    fn test_neon_mul_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let result = neon_mul_f64(&a, &b);
        assert_eq!(
            result,
            vec![2.0, 6.0, 12.0, 20.0, 30.0, 42.0, 56.0, 72.0, 90.0]
        );
    }

    #[test]
    fn test_neon_div_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![2.0, 4.0, 3.0, 8.0, 5.0, 6.0, 7.0, 10.0, 9.0];
        let result = neon_div_f64(&a, &b);
        assert_eq!(
            result,
            vec![5.0, 5.0, 10.0, 5.0, 10.0, 10.0, 10.0, 8.0, 10.0]
        );
    }

    #[test]
    fn test_neon_fma_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0];
        let c = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let result = neon_fma_f64(&a, &b, &c);
        // a * b + c
        assert_eq!(result, vec![12.0, 26.0, 42.0, 60.0, 80.0]);
    }

    #[test]
    fn test_neon_add_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![10, 20, 30, 40, 50, 60, 70, 80, 90];
        let result = neon_add_i64(&a, &b);
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88, 99]);
    }

    #[test]
    fn test_neon_sub_i64() {
        let a = vec![10, 20, 30, 40, 50, 60, 70, 80, 90];
        let b = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = neon_sub_i64(&a, &b);
        assert_eq!(result, vec![9, 18, 27, 36, 45, 54, 63, 72, 81]);
    }

    #[test]
    fn test_neon_mul_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = neon_mul_i64(&a, &b);
        assert_eq!(result, vec![2, 6, 12, 20, 30, 42, 56, 72, 90]);
    }
}
