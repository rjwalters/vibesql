//! ARM64 NEON-optimized SIMD aggregation operations
//!
//! This module provides ARM64-specific aggregation implementations using NEON intrinsics.
//! NEON's 128-bit vectors process 2 doubles or 2 longs at a time.

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
use std::arch::aarch64::*;

/// NEON sum for f64 columns (2 elements at a time)
///
/// Uses NEON horizontal operations to efficiently sum across vector lanes.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_sum_f64(column: &[f64]) -> f64 {
    let mut sum_vec;

    unsafe {
        // Initialize accumulator to zero
        sum_vec = vdupq_n_f64(0.0);

        // Process chunks of 2 elements with NEON
        let chunks = column.len() / 2;
        for i in 0..chunks {
            let offset = i * 2;
            let values = vld1q_f64(column.as_ptr().add(offset));
            sum_vec = vaddq_f64(sum_vec, values);
        }

        // Horizontal add: sum both lanes of the accumulator
        let sum_scalar = vaddvq_f64(sum_vec);

        // Handle remainder elements with scalar fallback
        let remainder_start = chunks * 2;
        let remainder_sum: f64 = column[remainder_start..].iter().sum();

        sum_scalar + remainder_sum
    }
}

/// NEON average for f64 columns
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_avg_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let sum = neon_sum_f64(column);
    Some(sum / column.len() as f64)
}

/// NEON minimum for f64 columns (2 elements at a time)
///
/// Uses NEON's pairwise min operation for efficient reduction.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_min_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    unsafe {
        let mut min_vec = vdupq_n_f64(f64::INFINITY);

        // Process chunks of 2 elements with NEON
        let chunks = column.len() / 2;
        for i in 0..chunks {
            let offset = i * 2;
            let values = vld1q_f64(column.as_ptr().add(offset));
            min_vec = vminq_f64(min_vec, values);
        }

        // Horizontal min: find minimum across both lanes
        let min_scalar = vminvq_f64(min_vec);

        // Handle remainder elements with scalar fallback
        let remainder_start = chunks * 2;
        let remainder_min = column[remainder_start..]
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(f64::INFINITY);

        Some(min_scalar.min(remainder_min))
    }
}

/// NEON maximum for f64 columns (2 elements at a time)
///
/// Uses NEON's pairwise max operation for efficient reduction.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_max_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    unsafe {
        let mut max_vec = vdupq_n_f64(f64::NEG_INFINITY);

        // Process chunks of 2 elements with NEON
        let chunks = column.len() / 2;
        for i in 0..chunks {
            let offset = i * 2;
            let values = vld1q_f64(column.as_ptr().add(offset));
            max_vec = vmaxq_f64(max_vec, values);
        }

        // Horizontal max: find maximum across both lanes
        let max_scalar = vmaxvq_f64(max_vec);

        // Handle remainder elements with scalar fallback
        let remainder_start = chunks * 2;
        let remainder_max = column[remainder_start..]
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(f64::NEG_INFINITY);

        Some(max_scalar.max(remainder_max))
    }
}

/// NEON sum for i64 columns (2 elements at a time)
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_sum_i64(column: &[i64]) -> i64 {
    let mut sum_vec;

    unsafe {
        // Initialize accumulator to zero
        sum_vec = vdupq_n_s64(0);

        // Process chunks of 2 elements with NEON
        let chunks = column.len() / 2;
        for i in 0..chunks {
            let offset = i * 2;
            let values = vld1q_s64(column.as_ptr().add(offset));
            sum_vec = vaddq_s64(sum_vec, values);
        }

        // Horizontal add: sum both lanes of the accumulator
        let sum_scalar = vaddvq_s64(sum_vec);

        // Handle remainder elements with scalar fallback
        let remainder_start = chunks * 2;
        let remainder_sum: i64 = column[remainder_start..].iter().sum();

        sum_scalar + remainder_sum
    }
}

/// NEON average for i64 columns
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_avg_i64(column: &[i64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let sum = neon_sum_i64(column);
    Some(sum as f64 / column.len() as f64)
}

/// NEON minimum for i64 columns (2 elements at a time)
///
/// Note: NEON doesn't have native min operations for signed 64-bit integers,
/// so this falls back to scalar implementation.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_min_i64(column: &[i64]) -> Option<i64> {
    if column.is_empty() {
        return None;
    }

    // NEON doesn't have efficient 64-bit signed integer min
    // Fall back to scalar implementation
    column.iter().copied().min()
}

/// NEON maximum for i64 columns (2 elements at a time)
///
/// Note: NEON doesn't have native max operations for signed 64-bit integers,
/// so this falls back to scalar implementation.
#[cfg(all(feature = "simd", target_arch = "aarch64"))]
pub fn neon_max_i64(column: &[i64]) -> Option<i64> {
    if column.is_empty() {
        return None;
    }

    // NEON doesn't have efficient 64-bit signed integer max
    // Fall back to scalar implementation
    column.iter().copied().max()
}

#[cfg(all(test, feature = "simd", target_arch = "aarch64"))]
mod tests {
    use super::*;

    #[test]
    fn test_neon_sum_f64() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = neon_sum_f64(&column);
        assert_eq!(result, 45.0);
    }

    #[test]
    fn test_neon_avg_f64() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = neon_avg_f64(&column);
        assert_eq!(result, Some(5.0));
    }

    #[test]
    fn test_neon_min_f64() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let result = neon_min_f64(&column);
        assert_eq!(result, Some(1.0));
    }

    #[test]
    fn test_neon_max_f64() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let result = neon_max_f64(&column);
        assert_eq!(result, Some(9.0));
    }

    #[test]
    fn test_neon_sum_i64() {
        let column = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = neon_sum_i64(&column);
        assert_eq!(result, 45);
    }

    #[test]
    fn test_neon_avg_i64() {
        let column = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = neon_avg_i64(&column);
        assert_eq!(result, Some(5.0));
    }

    #[test]
    fn test_neon_min_i64() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let result = neon_min_i64(&column);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_neon_max_i64() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let result = neon_max_i64(&column);
        assert_eq!(result, Some(9));
    }

    #[test]
    fn test_neon_empty_column() {
        let column: Vec<f64> = vec![];
        assert_eq!(neon_sum_f64(&column), 0.0);
        assert_eq!(neon_avg_f64(&column), None);
        assert_eq!(neon_min_f64(&column), None);
        assert_eq!(neon_max_f64(&column), None);
    }
}
