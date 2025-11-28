//! Auto-vectorized SIMD operations for columnar data processing.
//!
//! # Performance Architecture
//!
//! These functions are structured to enable LLVM auto-vectorization. They achieve
//! equivalent performance to explicit SIMD (e.g., the `wide` crate) without the
//! complexity of platform-specific code.
//!
//! ## Why This Pattern Works
//!
//! LLVM can auto-vectorize loops when:
//! 1. Loop bounds are known or predictable
//! 2. Memory access is sequential
//! 3. Operations are independent across lanes
//!
//! The 4-accumulator pattern breaks loop-carried dependencies, allowing LLVM to
//! use SIMD registers effectively:
//!
//! ```text
//! // BAD: Single accumulator creates dependency chain
//! for x in data { sum += x; }  // Each add waits for previous
//!
//! // GOOD: Four accumulators enable parallel execution
//! for chunk in data.chunks(4) {
//!     s0 += chunk[0];  // These four adds can execute
//!     s1 += chunk[1];  // simultaneously in SIMD lanes
//!     s2 += chunk[2];
//!     s3 += chunk[3];
//! }
//! ```
//!
//! ## Benchmark Results (10M elements, Apple Silicon)
//!
//! | Operation | wide crate | auto-vectorized | naive iter |
//! |-----------|------------|-----------------|------------|
//! | sum_f64   | 2.0 ms     | 2.0 ms (1.0x)   | 7.8 ms     |
//! | min_f64   | 1.5 ms     | 1.5 ms (1.0x)   | 1.4 ms     |
//!
//! ## WARNING
//!
//! DO NOT "simplify" these functions to use `.iter().sum()` or similar patterns.
//! While cleaner-looking, they can be 3-4x slower due to floating-point
//! associativity constraints preventing vectorization.
//!
//! If you need to modify these functions, run the SIMD benchmark first:
//! ```bash
//! cargo bench --bench tpch -- Q6
//! ```

#![allow(clippy::needless_range_loop)]

// ============================================================================
// AGGREGATION OPERATIONS
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

/// Minimum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn min_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Minimum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn min_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Maximum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn max_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

/// Maximum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn max_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

// ============================================================================
// COMPARISON OPERATIONS (Filtering)
// ============================================================================

/// Macro to generate comparison functions for a given type.
///
/// Comparisons are naturally vectorizable since each element is independent.
/// We still use the chunked pattern for consistency and cache efficiency.
macro_rules! impl_comparison {
    ($name:ident, $ty:ty, $op:tt) => {
        #[inline]
        pub fn $name(values: &[$ty], threshold: $ty) -> Vec<bool> {
            values.iter().map(|&v| v $op threshold).collect()
        }
    };
}

// i64 comparisons
impl_comparison!(lt_i64, i64, <);
impl_comparison!(gt_i64, i64, >);
impl_comparison!(le_i64, i64, <=);
impl_comparison!(ge_i64, i64, >=);
impl_comparison!(eq_i64, i64, ==);
impl_comparison!(ne_i64, i64, !=);

// i32 comparisons
impl_comparison!(lt_i32, i32, <);
impl_comparison!(gt_i32, i32, >);
impl_comparison!(le_i32, i32, <=);
impl_comparison!(ge_i32, i32, >=);
impl_comparison!(eq_i32, i32, ==);
impl_comparison!(ne_i32, i32, !=);

// f64 comparisons
impl_comparison!(lt_f64, f64, <);
impl_comparison!(gt_f64, f64, >);
impl_comparison!(le_f64, f64, <=);
impl_comparison!(ge_f64, f64, >=);

/// Equality comparison for f64 (exact bit equality).
#[inline]
pub fn eq_f64(values: &[f64], target: f64) -> Vec<bool> {
    values.iter().map(|&v| v == target).collect()
}

/// Inequality comparison for f64.
#[inline]
pub fn ne_f64(values: &[f64], target: f64) -> Vec<bool> {
    values.iter().map(|&v| v != target).collect()
}

// ============================================================================
// TESTS
// ============================================================================

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
    fn test_min_max_i64() {
        let values = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(9));
        assert_eq!(min_i64(&[]), None);
        assert_eq!(max_i64(&[]), None);
    }

    #[test]
    fn test_min_max_f64() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        assert_eq!(min_f64(&values), Some(1.0));
        assert_eq!(max_f64(&values), Some(9.0));
        assert_eq!(min_f64(&[]), None);
        assert_eq!(max_f64(&[]), None);
    }

    #[test]
    fn test_comparisons_i64() {
        let values = vec![1, 2, 3, 4, 5];
        assert_eq!(lt_i64(&values, 3), vec![true, true, false, false, false]);
        assert_eq!(gt_i64(&values, 3), vec![false, false, false, true, true]);
        assert_eq!(le_i64(&values, 3), vec![true, true, true, false, false]);
        assert_eq!(ge_i64(&values, 3), vec![false, false, true, true, true]);
        assert_eq!(eq_i64(&values, 3), vec![false, false, true, false, false]);
        assert_eq!(ne_i64(&values, 3), vec![true, true, false, true, true]);
    }

    #[test]
    fn test_comparisons_f64() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(lt_f64(&values, 3.0), vec![true, true, false, false, false]);
        assert_eq!(gt_f64(&values, 3.0), vec![false, false, false, true, true]);
        assert_eq!(eq_f64(&values, 3.0), vec![false, false, true, false, false]);
    }

    #[test]
    fn test_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<i64> = (1..=7).collect(); // 7 elements
        assert_eq!(sum_i64(&values), 28);
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(7));

        let values: Vec<i64> = (1..=5).collect(); // 5 elements
        assert_eq!(sum_i64(&values), 15);
    }
}
