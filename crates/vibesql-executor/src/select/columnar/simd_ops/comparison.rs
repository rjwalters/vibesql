//! Comparison operations for columnar data processing.
//!
//! This module provides vectorized comparison operations that return boolean masks
//! or packed bitmasks for filtering columnar data.

#![allow(clippy::needless_range_loop)]

use super::PackedMask;

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
// BETWEEN OPERATIONS (Range checks in single pass)
// ============================================================================

/// Check if i64 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_i64(values: &[i64], low: i64, high: i64) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

/// Check if i32 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_i32(values: &[i32], low: i32, high: i32) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

/// Check if f64 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_f64(values: &[f64], low: f64, high: f64) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

// ============================================================================
// PACKED COMPARISON OPERATIONS (Return PackedMask instead of Vec<bool>)
// ============================================================================
// These functions return packed bitmasks for 8x memory reduction and
// native SIMD bitwise operations.

/// Less-than comparison returning packed mask.
#[inline]
pub fn lt_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than comparison returning packed mask.
#[inline]
pub fn gt_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Less-than-or-equal comparison returning packed mask.
#[inline]
pub fn le_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than-or-equal comparison returning packed mask.
#[inline]
pub fn ge_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Equality comparison returning packed mask.
#[inline]
pub fn eq_i64_packed(values: &[i64], target: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Inequality comparison returning packed mask.
#[inline]
pub fn ne_i64_packed(values: &[i64], target: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Less-than comparison for i32 returning packed mask.
#[inline]
pub fn lt_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than comparison for i32 returning packed mask.
#[inline]
pub fn gt_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Less-than-or-equal comparison for i32 returning packed mask.
#[inline]
pub fn le_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than-or-equal comparison for i32 returning packed mask.
#[inline]
pub fn ge_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Equality comparison for i32 returning packed mask.
#[inline]
pub fn eq_i32_packed(values: &[i32], target: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Inequality comparison for i32 returning packed mask.
#[inline]
pub fn ne_i32_packed(values: &[i32], target: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Less-than comparison for f64 returning packed mask.
#[inline]
pub fn lt_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than comparison for f64 returning packed mask.
#[inline]
pub fn gt_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Less-than-or-equal comparison for f64 returning packed mask.
#[inline]
pub fn le_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Greater-than-or-equal comparison for f64 returning packed mask.
#[inline]
pub fn ge_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Equality comparison for f64 returning packed mask.
#[inline]
pub fn eq_f64_packed(values: &[f64], target: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Inequality comparison for f64 returning packed mask.
#[inline]
pub fn ne_f64_packed(values: &[f64], target: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

// ============================================================================
// PACKED BETWEEN OPERATIONS
// ============================================================================

/// Check if i64 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_i64_packed(values: &[i64], low: i64, high: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Check if i32 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_i32_packed(values: &[i32], low: i32, high: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

/// Check if f64 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_f64_packed(values: &[f64], low: f64, high: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = len.div_ceil(64);
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask::from_words(words, len)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_between_i64() {
        let values = vec![1, 5, 10, 15, 20, 25];
        assert_eq!(between_i64(&values, 5, 20), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_between_i32() {
        let values: Vec<i32> = vec![1, 5, 10, 15, 20, 25];
        assert_eq!(between_i32(&values, 5, 20), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_between_f64() {
        let values = vec![0.01, 0.05, 0.06, 0.07, 0.08];
        assert_eq!(between_f64(&values, 0.05, 0.07), vec![false, true, true, true, false]);
    }

    #[test]
    fn test_packed_comparison_i64() {
        let values = vec![1i64, 2, 3, 4, 5];

        let mask = lt_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, false, false]);

        let mask = gt_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, false, true, true]);

        let mask = le_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, true, false, false]);

        let mask = ge_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, true, true]);

        let mask = eq_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, false, false]);

        let mask = ne_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, true, true]);
    }

    #[test]
    fn test_packed_comparison_f64() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let mask = lt_f64_packed(&values, 3.0);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, false, false]);

        let mask = ge_f64_packed(&values, 3.0);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, true, true]);
    }

    #[test]
    fn test_packed_between_i64() {
        let values = vec![1i64, 5, 10, 15, 20, 25];
        let mask = between_i64_packed(&values, 5, 20);
        assert_eq!(mask.to_bool_vec(), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_packed_between_f64() {
        let values = vec![0.01, 0.05, 0.06, 0.07, 0.08];
        let mask = between_f64_packed(&values, 0.05, 0.07);
        assert_eq!(mask.to_bool_vec(), vec![false, true, true, true, false]);
    }

    #[test]
    fn test_packed_mask_large_dataset() {
        // Test with a large dataset to ensure correctness at scale
        let n = 10_000;
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();

        // Filter: values < 5000 (half the dataset)
        let mask = lt_f64_packed(&values, 5000.0);

        assert_eq!(mask.count_ones(), 5000);
    }
}
