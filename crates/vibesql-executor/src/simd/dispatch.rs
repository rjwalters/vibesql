//! Runtime SIMD dispatch based on CPU features
//!
//! This module provides a trait-based interface for SIMD operations
//! with runtime dispatch to the best available implementation.

use super::cpu_features::{CpuFeatures, SimdLevel};
use std::sync::OnceLock;

/// Trait for SIMD arithmetic operations
pub trait SimdOperations: Send + Sync {
    /// SIMD level for this implementation
    fn simd_level(&self) -> SimdLevel;

    /// Add two f64 slices
    fn add_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64>;

    /// Subtract two f64 slices
    fn sub_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64>;

    /// Multiply two f64 slices
    fn mul_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64>;

    /// Divide two f64 slices
    fn div_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64>;

    /// Add two i64 slices
    fn add_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64>;

    /// Subtract two i64 slices
    fn sub_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64>;

    /// Multiply two i64 slices
    fn mul_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64>;

    // Comparison operations for f64
    fn gt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool>;
    fn ge_f64(&self, column: &[f64], threshold: f64) -> Vec<bool>;
    fn lt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool>;
    fn le_f64(&self, column: &[f64], threshold: f64) -> Vec<bool>;
    fn eq_f64(&self, column: &[f64], value: f64) -> Vec<bool>;
    fn ne_f64(&self, column: &[f64], value: f64) -> Vec<bool>;

    // Comparison operations for i64
    fn gt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool>;
    fn lt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool>;
    fn eq_i64(&self, column: &[i64], value: i64) -> Vec<bool>;

    // Aggregation operations for f64
    fn sum_f64(&self, column: &[f64]) -> f64;
    fn avg_f64(&self, column: &[f64]) -> Option<f64>;
    fn min_f64(&self, column: &[f64]) -> Option<f64>;
    fn max_f64(&self, column: &[f64]) -> Option<f64>;

    // Aggregation operations for i64
    fn sum_i64(&self, column: &[i64]) -> i64;
    fn avg_i64(&self, column: &[i64]) -> Option<f64>;
    fn min_i64(&self, column: &[i64]) -> Option<i64>;
    fn max_i64(&self, column: &[i64]) -> Option<i64>;
}

/// Scalar fallback implementation
struct ScalarOperations;

impl SimdOperations for ScalarOperations {
    fn simd_level(&self) -> SimdLevel {
        SimdLevel::Scalar
    }

    fn add_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
    }

    fn sub_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
    }

    fn mul_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).collect()
    }

    fn div_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        a.iter().zip(b.iter()).map(|(x, y)| x / y).collect()
    }

    fn add_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
    }

    fn sub_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
    }

    fn mul_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).collect()
    }

    fn gt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        column.iter().map(|&x| x > threshold).collect()
    }

    fn ge_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        column.iter().map(|&x| x >= threshold).collect()
    }

    fn lt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        column.iter().map(|&x| x < threshold).collect()
    }

    fn le_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        column.iter().map(|&x| x <= threshold).collect()
    }

    fn eq_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        column.iter().map(|&x| x == value).collect()
    }

    fn ne_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        column.iter().map(|&x| x != value).collect()
    }

    fn gt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        column.iter().map(|&x| x > threshold).collect()
    }

    fn lt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        column.iter().map(|&x| x < threshold).collect()
    }

    fn eq_i64(&self, column: &[i64], value: i64) -> Vec<bool> {
        column.iter().map(|&x| x == value).collect()
    }

    fn sum_f64(&self, column: &[f64]) -> f64 {
        column.iter().sum()
    }

    fn avg_f64(&self, column: &[f64]) -> Option<f64> {
        if column.is_empty() {
            None
        } else {
            Some(column.iter().sum::<f64>() / column.len() as f64)
        }
    }

    fn min_f64(&self, column: &[f64]) -> Option<f64> {
        column.iter().copied().min_by(|a, b| a.partial_cmp(b).unwrap())
    }

    fn max_f64(&self, column: &[f64]) -> Option<f64> {
        column.iter().copied().max_by(|a, b| a.partial_cmp(b).unwrap())
    }

    fn sum_i64(&self, column: &[i64]) -> i64 {
        column.iter().sum()
    }

    fn avg_i64(&self, column: &[i64]) -> Option<f64> {
        if column.is_empty() {
            None
        } else {
            Some(column.iter().sum::<i64>() as f64 / column.len() as f64)
        }
    }

    fn min_i64(&self, column: &[i64]) -> Option<i64> {
        column.iter().copied().min()
    }

    fn max_i64(&self, column: &[i64]) -> Option<i64> {
        column.iter().copied().max()
    }
}

/// AVX2 implementation (256-bit SIMD, 4 doubles)
#[cfg(feature = "simd")]
struct Avx2Operations;

#[cfg(feature = "simd")]
impl SimdOperations for Avx2Operations {
    fn simd_level(&self) -> SimdLevel {
        SimdLevel::Avx2
    }

    fn add_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        // Use existing wide-based implementation (currently f64x4)
        super::arithmetic::simd_add_f64(a, b)
    }

    fn sub_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_sub_f64(a, b)
    }

    fn mul_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_mul_f64(a, b)
    }

    fn div_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_div_f64(a, b)
    }

    fn add_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_add_i64(a, b)
    }

    fn sub_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_sub_i64(a, b)
    }

    fn mul_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_mul_i64(a, b)
    }

    fn gt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_gt_f64(column, threshold)
    }

    fn ge_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_ge_f64(column, threshold)
    }

    fn lt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_lt_f64(column, threshold)
    }

    fn le_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_le_f64(column, threshold)
    }

    fn eq_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        super::comparison::simd_eq_f64(column, value)
    }

    fn ne_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        super::comparison::simd_ne_f64(column, value)
    }

    fn gt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        super::comparison::simd_gt_i64(column, threshold)
    }

    fn lt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        super::comparison::simd_lt_i64(column, threshold)
    }

    fn eq_i64(&self, column: &[i64], value: i64) -> Vec<bool> {
        super::comparison::simd_eq_i64(column, value)
    }

    fn sum_f64(&self, column: &[f64]) -> f64 {
        super::aggregation::simd_sum_f64(column)
    }

    fn avg_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_avg_f64(column)
    }

    fn min_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_min_f64(column)
    }

    fn max_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_max_f64(column)
    }

    fn sum_i64(&self, column: &[i64]) -> i64 {
        super::aggregation::simd_sum_i64(column)
    }

    fn avg_i64(&self, column: &[i64]) -> Option<f64> {
        super::aggregation::simd_avg_i64(column)
    }

    fn min_i64(&self, column: &[i64]) -> Option<i64> {
        super::aggregation::simd_min_i64(column)
    }

    fn max_i64(&self, column: &[i64]) -> Option<i64> {
        super::aggregation::simd_max_i64(column)
    }
}

/// AVX-512 implementation (512-bit SIMD, 8 doubles)
///
/// Note: This is a placeholder for future AVX-512 implementation.
/// Currently falls back to AVX2 implementation until dedicated
/// AVX-512 intrinsics are added.
#[cfg(feature = "simd")]
struct Avx512Operations;

#[cfg(feature = "simd")]
impl SimdOperations for Avx512Operations {
    fn simd_level(&self) -> SimdLevel {
        SimdLevel::Avx512
    }

    fn add_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        // TODO: Implement dedicated AVX-512 path using f64x8
        // For now, fall back to AVX2 implementation
        super::arithmetic::simd_add_f64(a, b)
    }

    fn sub_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_sub_f64(a, b)
    }

    fn mul_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_mul_f64(a, b)
    }

    fn div_f64(&self, a: &[f64], b: &[f64]) -> Vec<f64> {
        super::arithmetic::simd_div_f64(a, b)
    }

    fn add_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_add_i64(a, b)
    }

    fn sub_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_sub_i64(a, b)
    }

    fn mul_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        super::arithmetic::simd_mul_i64(a, b)
    }

    fn gt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        // TODO: Implement dedicated AVX-512 path
        super::comparison::simd_gt_f64(column, threshold)
    }

    fn ge_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_ge_f64(column, threshold)
    }

    fn lt_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_lt_f64(column, threshold)
    }

    fn le_f64(&self, column: &[f64], threshold: f64) -> Vec<bool> {
        super::comparison::simd_le_f64(column, threshold)
    }

    fn eq_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        super::comparison::simd_eq_f64(column, value)
    }

    fn ne_f64(&self, column: &[f64], value: f64) -> Vec<bool> {
        super::comparison::simd_ne_f64(column, value)
    }

    fn gt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        super::comparison::simd_gt_i64(column, threshold)
    }

    fn lt_i64(&self, column: &[i64], threshold: i64) -> Vec<bool> {
        super::comparison::simd_lt_i64(column, threshold)
    }

    fn eq_i64(&self, column: &[i64], value: i64) -> Vec<bool> {
        super::comparison::simd_eq_i64(column, value)
    }

    fn sum_f64(&self, column: &[f64]) -> f64 {
        // TODO: Implement dedicated AVX-512 path
        super::aggregation::simd_sum_f64(column)
    }

    fn avg_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_avg_f64(column)
    }

    fn min_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_min_f64(column)
    }

    fn max_f64(&self, column: &[f64]) -> Option<f64> {
        super::aggregation::simd_max_f64(column)
    }

    fn sum_i64(&self, column: &[i64]) -> i64 {
        super::aggregation::simd_sum_i64(column)
    }

    fn avg_i64(&self, column: &[i64]) -> Option<f64> {
        super::aggregation::simd_avg_i64(column)
    }

    fn min_i64(&self, column: &[i64]) -> Option<i64> {
        super::aggregation::simd_min_i64(column)
    }

    fn max_i64(&self, column: &[i64]) -> Option<i64> {
        super::aggregation::simd_max_i64(column)
    }
}

/// Get the global SIMD operations implementation
///
/// This function returns a reference to a SIMD operations implementation
/// selected based on runtime CPU feature detection. The result is cached
/// after the first call.
pub fn get_simd_ops() -> &'static dyn SimdOperations {
    static OPS: OnceLock<Box<dyn SimdOperations>> = OnceLock::new();

    OPS.get_or_init(|| {
        let features = CpuFeatures::get();
        let level = features.best_simd_level();

        #[cfg(feature = "simd")]
        {
            match level {
                SimdLevel::Avx512 => {
                    log::info!("Using AVX-512 SIMD operations (512-bit vectors, 8 doubles)");
                    Box::new(Avx512Operations)
                }
                SimdLevel::Avx2 | SimdLevel::Sse42 => {
                    // Both AVX2 and SSE4.2 use the same implementation for now
                    // (wide crate handles the details)
                    log::info!(
                        "Using {} SIMD operations (256-bit vectors, 4 doubles)",
                        level.name()
                    );
                    Box::new(Avx2Operations)
                }
                SimdLevel::Neon => {
                    log::info!("Using NEON SIMD operations (128-bit vectors, 2 doubles)");
                    // NEON would use the same wide-based implementation
                    Box::new(Avx2Operations)
                }
                _ => {
                    log::info!("Using scalar fallback (no SIMD)");
                    Box::new(ScalarOperations)
                }
            }
        }

        #[cfg(not(feature = "simd"))]
        {
            log::info!("SIMD feature disabled, using scalar operations");
            Box::new(ScalarOperations)
        }
    })
    .as_ref()
}

/// Convenience functions that use runtime dispatch
pub mod dispatched {
    use super::get_simd_ops;

    /// Add two f64 slices using best available SIMD
    pub fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        get_simd_ops().add_f64(a, b)
    }

    /// Subtract two f64 slices using best available SIMD
    pub fn sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        get_simd_ops().sub_f64(a, b)
    }

    /// Multiply two f64 slices using best available SIMD
    pub fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        get_simd_ops().mul_f64(a, b)
    }

    /// Divide two f64 slices using best available SIMD
    pub fn div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        get_simd_ops().div_f64(a, b)
    }

    /// Add two i64 slices using best available SIMD
    pub fn add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
        get_simd_ops().add_i64(a, b)
    }

    /// Subtract two i64 slices using best available SIMD
    pub fn sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
        get_simd_ops().sub_i64(a, b)
    }

    /// Multiply two i64 slices using best available SIMD
    pub fn mul_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
        get_simd_ops().mul_i64(a, b)
    }

    // Comparison operations for f64
    pub fn gt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
        get_simd_ops().gt_f64(column, threshold)
    }

    pub fn ge_f64(column: &[f64], threshold: f64) -> Vec<bool> {
        get_simd_ops().ge_f64(column, threshold)
    }

    pub fn lt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
        get_simd_ops().lt_f64(column, threshold)
    }

    pub fn le_f64(column: &[f64], threshold: f64) -> Vec<bool> {
        get_simd_ops().le_f64(column, threshold)
    }

    pub fn eq_f64(column: &[f64], value: f64) -> Vec<bool> {
        get_simd_ops().eq_f64(column, value)
    }

    pub fn ne_f64(column: &[f64], value: f64) -> Vec<bool> {
        get_simd_ops().ne_f64(column, value)
    }

    // Comparison operations for i64
    pub fn gt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
        get_simd_ops().gt_i64(column, threshold)
    }

    pub fn lt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
        get_simd_ops().lt_i64(column, threshold)
    }

    pub fn eq_i64(column: &[i64], value: i64) -> Vec<bool> {
        get_simd_ops().eq_i64(column, value)
    }

    // Aggregation operations for f64
    pub fn sum_f64(column: &[f64]) -> f64 {
        get_simd_ops().sum_f64(column)
    }

    pub fn avg_f64(column: &[f64]) -> Option<f64> {
        get_simd_ops().avg_f64(column)
    }

    pub fn min_f64(column: &[f64]) -> Option<f64> {
        get_simd_ops().min_f64(column)
    }

    pub fn max_f64(column: &[f64]) -> Option<f64> {
        get_simd_ops().max_f64(column)
    }

    // Aggregation operations for i64
    pub fn sum_i64(column: &[i64]) -> i64 {
        get_simd_ops().sum_i64(column)
    }

    pub fn avg_i64(column: &[i64]) -> Option<f64> {
        get_simd_ops().avg_i64(column)
    }

    pub fn min_i64(column: &[i64]) -> Option<i64> {
        get_simd_ops().min_i64(column)
    }

    pub fn max_i64(column: &[i64]) -> Option<i64> {
        get_simd_ops().max_i64(column)
    }
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    #[test]
    fn test_dispatch_selection() {
        let ops = get_simd_ops();
        let level = ops.simd_level();

        println!("Selected SIMD level: {:?}", level);

        // Verify we got a valid implementation
        assert!(level >= SimdLevel::Scalar);
    }

    #[test]
    fn test_dispatched_add_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let result = dispatched::add_f64(&a, &b);
        assert_eq!(
            result,
            vec![11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0, 99.0]
        );
    }

    #[test]
    fn test_dispatched_mul_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = dispatched::mul_i64(&a, &b);
        assert_eq!(result, vec![2, 6, 12, 20, 30, 42, 56, 72, 90]);
    }

    #[test]
    fn test_scalar_fallback() {
        let scalar = ScalarOperations;
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];

        let result = scalar.add_f64(&a, &b);
        assert_eq!(result, vec![6.0, 8.0, 10.0, 12.0]);

        let result = scalar.mul_f64(&a, &b);
        assert_eq!(result, vec![5.0, 12.0, 21.0, 32.0]);
    }
}
