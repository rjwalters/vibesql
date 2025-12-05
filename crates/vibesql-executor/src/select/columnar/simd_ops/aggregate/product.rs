//! Product/arithmetic aggregate operations for columnar data processing.
//!
//! This module provides vectorized sum-of-product operations for expression
//! aggregates like TPC-H Q6: SUM(l_extendedprice * l_discount).

#![allow(clippy::needless_range_loop)]

use super::super::PackedMask;

// ============================================================================
// BASIC PRODUCT OPERATIONS
// ============================================================================

/// Sum of element-wise product of two f64 arrays using 4-accumulator pattern.
///
/// This is the core operation for TPC-H Q6: SUM(l_extendedprice * l_discount).
///
/// Performance: Uses LLVM auto-vectorization for SIMD acceleration.
/// The 4-accumulator pattern enables parallel execution across SIMD lanes.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
///
/// # Returns
/// Sum of element-wise products: Σ(a[i] * b[i])
///
/// # Panics
/// Panics if arrays have different lengths (debug builds only).
#[inline]
pub fn sum_product_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");

    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }

    // 4-accumulator pattern for LLVM auto-vectorization
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 += a[off] * b[off];
        s1 += a[off + 1] * b[off + 1];
        s2 += a[off + 2] * b[off + 2];
        s3 += a[off + 3] * b[off + 3];
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..len {
        sum += a[i] * b[i];
    }
    sum
}

/// Sum of element-wise product with null mask using 4-accumulator pattern.
///
/// Handles NULL values efficiently by checking the null bitmap rather than
/// per-element Option checking. This enables SIMD auto-vectorization.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
/// * `null_a` - Null bitmap for array `a` (true = null, false = valid)
/// * `null_b` - Null bitmap for array `b` (true = null, false = valid)
///
/// # Returns
/// (sum, count) - Sum of products for non-null pairs and count of valid pairs
#[inline]
pub fn sum_product_f64_masked(
    a: &[f64],
    b: &[f64],
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls in either array
    let no_nulls_a = null_a.is_none_or(|n| !n.iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| !n.iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return (sum_product_f64(a, b), len as i64);
    }

    // Slow path with null handling - still use accumulator pattern where possible
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;

    for i in 0..len {
        let is_null_a = null_mask_a.get(i).copied().unwrap_or(false);
        let is_null_b = null_mask_b.get(i).copied().unwrap_or(false);

        if !is_null_a && !is_null_b {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

// ============================================================================
// FILTERED PRODUCT OPERATIONS (boolean mask)
// ============================================================================

/// Sum of element-wise product of two f64 arrays with filter mask.
///
/// This is the key operation for TPC-H Q6: SUM(l_extendedprice * l_discount)
/// with WHERE clause filtering.
///
/// Fuses filtering and aggregation to avoid:
/// 1. Creating intermediate filtered arrays
/// 2. Multiple passes over the data
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
/// * `filter_mask` - Boolean mask (true = include in aggregate)
///
/// # Returns
/// (sum, count) - Sum of products for filtered rows and count of passing rows
#[inline]
pub fn sum_product_f64_filtered(a: &[f64], b: &[f64], filter_mask: &[bool]) -> (f64, i64) {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");
    debug_assert_eq!(a.len(), filter_mask.len(), "Arrays and filter must have same length");

    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // 4-accumulator pattern with filter
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let (mut c0, mut c1, mut c2, mut c3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            s0 += a[off] * b[off];
            c0 += 1;
        }
        if filter_mask[off + 1] {
            s1 += a[off + 1] * b[off + 1];
            c1 += 1;
        }
        if filter_mask[off + 2] {
            s2 += a[off + 2] * b[off + 2];
            c2 += 1;
        }
        if filter_mask[off + 3] {
            s3 += a[off + 3] * b[off + 3];
            c3 += 1;
        }
    }

    let mut sum = s0 + s1 + s2 + s3;
    let mut count = c0 + c1 + c2 + c3;

    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

/// Sum of element-wise product with filter mask AND null masks.
///
/// Full version that handles both filter predicates and NULL values.
/// Used when columnar data has NULLs that also need to be excluded.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values
/// * `filter_mask` - Boolean mask from predicates (true = include)
/// * `null_a` - Null bitmap for array `a` (true = null, false = valid)
/// * `null_b` - Null bitmap for array `b` (true = null, false = valid)
///
/// # Returns
/// (sum, count) - Sum of products and count of valid rows
#[inline]
pub fn sum_product_f64_filtered_masked(
    a: &[f64],
    b: &[f64],
    filter_mask: &[bool],
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls
    let no_nulls_a = null_a.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return sum_product_f64_filtered(a, b, filter_mask);
    }

    // Slow path with null handling
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;

    for i in 0..len {
        let is_null_a = null_mask_a.get(i).copied().unwrap_or(false);
        let is_null_b = null_mask_b.get(i).copied().unwrap_or(false);

        if filter_mask[i] && !is_null_a && !is_null_b {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

// ============================================================================
// PACKED FILTERED PRODUCT OPERATIONS
// ============================================================================

/// Sum of element-wise product of two f64 arrays with packed filter mask.
///
/// This is the key operation for TPC-H Q6: SUM(l_extendedprice * l_discount)
/// with WHERE clause filtering.
#[inline]
pub fn sum_product_f64_packed_filtered(
    a: &[f64],
    b: &[f64],
    filter_mask: &PackedMask,
) -> (f64, i64) {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");
    debug_assert_eq!(a.len(), filter_mask.len(), "Arrays and filter must have same length");

    let len = a.len();
    if len == 0 {
        return (0.0, 0);
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let mut count = 0i64;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                let product = a[idx] * b[idx];
                match bit_pos % 4 {
                    0 => s0 += product,
                    1 => s1 += product,
                    2 => s2 += product,
                    _ => s3 += product,
                }
                count += 1;
            }
            bits &= bits - 1;
        }
    }

    (s0 + s1 + s2 + s3, count)
}

/// Sum of element-wise product with packed filter mask AND null masks.
///
/// Full version that handles both filter predicates and NULL values.
#[inline]
pub fn sum_product_f64_packed_filtered_masked(
    a: &[f64],
    b: &[f64],
    filter_mask: &PackedMask,
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls
    let no_nulls_a = null_a.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return sum_product_f64_packed_filtered(a, b, filter_mask);
    }

    // Slow path with null handling
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                let is_null_a = null_mask_a.get(idx).copied().unwrap_or(false);
                let is_null_b = null_mask_b.get(idx).copied().unwrap_or(false);

                if !is_null_a && !is_null_b {
                    sum += a[idx] * b[idx];
                    count += 1;
                }
            }
            bits &= bits - 1;
        }
    }

    (sum, count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_product_f64() {
        // Basic test: [1, 2, 3, 4] * [2, 2, 2, 2] = 2 + 4 + 6 + 8 = 20
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 2.0, 2.0, 2.0];
        assert!((sum_product_f64(&a, &b) - 20.0).abs() < 0.001);

        // Empty arrays
        assert_eq!(sum_product_f64(&[], &[]), 0.0);

        // Single element
        assert!((sum_product_f64(&[5.0], &[3.0]) - 15.0).abs() < 0.001);

        // Non-multiple-of-4 lengths (tests remainder handling)
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let b = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        assert!((sum_product_f64(&a, &b) - 28.0).abs() < 0.001);

        // TPC-H Q6 style: price * discount
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];
        // Expected: 100*0.05 + 200*0.06 + 300*0.07 + 400*0.05 + 500*0.06 + 600*0.07 + 700*0.05 + 800*0.06
        //         = 5 + 12 + 21 + 20 + 30 + 42 + 35 + 48 = 213
        assert!((sum_product_f64(&prices, &discounts) - 213.0).abs() < 0.001);
    }

    #[test]
    fn test_sum_product_f64_masked() {
        // No nulls - fast path
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 2.0, 2.0, 2.0];
        let (sum, count) = sum_product_f64_masked(&a, &b, None, None);
        assert!((sum - 20.0).abs() < 0.001);
        assert_eq!(count, 4);

        // With nulls in first array
        let null_a = vec![false, true, false, false];
        let (sum, count) = sum_product_f64_masked(&a, &b, Some(&null_a), None);
        // Only indices 0, 2, 3 counted: 1*2 + 3*2 + 4*2 = 2 + 6 + 8 = 16
        assert!((sum - 16.0).abs() < 0.001);
        assert_eq!(count, 3);

        // With nulls in second array
        let null_b = vec![false, false, true, false];
        let (sum, count) = sum_product_f64_masked(&a, &b, None, Some(&null_b));
        // Only indices 0, 1, 3 counted: 1*2 + 2*2 + 4*2 = 2 + 4 + 8 = 14
        assert!((sum - 14.0).abs() < 0.001);
        assert_eq!(count, 3);

        // With nulls in both arrays
        let (sum, count) = sum_product_f64_masked(&a, &b, Some(&null_a), Some(&null_b));
        // Only indices where both are not null: 0, 3 → 1*2 + 4*2 = 10
        assert!((sum - 10.0).abs() < 0.001);
        assert_eq!(count, 2);

        // Empty arrays
        let (sum, count) = sum_product_f64_masked(&[], &[], None, None);
        assert_eq!(sum, 0.0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_sum_product_f64_filtered() {
        // TPC-H Q6 scenario: SUM(price * discount) WHERE filter
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];
        let filter = vec![true, false, true, false, true, false, true, false];

        // Only indices 0, 2, 4, 6: 100*0.05 + 300*0.07 + 500*0.06 + 700*0.05
        //                        = 5 + 21 + 30 + 35 = 91
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter);
        assert!((sum - 91.0).abs() < 0.001);
        assert_eq!(count, 4);

        // All pass filter
        let filter_all = vec![true; 8];
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter_all);
        assert!((sum - 213.0).abs() < 0.001); // Same as unfiltered sum_product test
        assert_eq!(count, 8);

        // None pass filter
        let filter_none = vec![false; 8];
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter_none);
        assert_eq!(sum, 0.0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_sum_product_f64_filtered_masked() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0];
        let filter = vec![true, true, true, true, false, false, false, false];
        let null_a = vec![false, true, false, false, false, false, false, false];

        // Filter passes 0-3, null excludes 1, so: 0,2,3 → 1*2 + 3*2 + 4*2 = 16
        let (sum, count) = sum_product_f64_filtered_masked(&a, &b, &filter, Some(&null_a), None);
        assert!((sum - 16.0).abs() < 0.001);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_sum_product_f64_packed_filtered() {
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];

        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        // Only indices 0, 2, 4, 6: 100*0.05 + 300*0.07 + 500*0.06 + 700*0.05
        //                        = 5 + 21 + 30 + 35 = 91
        let (sum, count) = sum_product_f64_packed_filtered(&prices, &discounts, &filter);
        assert!((sum - 91.0).abs() < 0.001);
        assert_eq!(count, 4);

        // All pass filter
        let filter_all = PackedMask::new_all_set(8);
        let (sum, count) = sum_product_f64_packed_filtered(&prices, &discounts, &filter_all);
        assert!((sum - 213.0).abs() < 0.001);
        assert_eq!(count, 8);
    }
}
