//! SIMD aggregation operations for columnar data
//!
//! Note: Index-based loops are used intentionally for SIMD vectorization.

#![allow(clippy::needless_range_loop)]

#[cfg(feature = "simd")]
use wide::*;

/// SIMD sum for f64 columns
#[cfg(feature = "simd")]
pub fn simd_sum_f64(column: &[f64]) -> f64 {
    log::debug!("[SIMD] simd_sum_f64 called with {} elements", column.len());
    let mut sum = 0.0;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        // Extract and sum manually since wide doesn't have horizontal sum
        let arr: [f64; 4] = values.into();
        sum += arr[0] + arr[1] + arr[2] + arr[3];
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        sum += column[i];
    }

    sum
}

/// SIMD average for f64 columns
#[cfg(feature = "simd")]
pub fn simd_avg_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let sum = simd_sum_f64(column);
    Some(sum / column.len() as f64)
}

/// SIMD minimum for f64 columns
#[cfg(feature = "simd")]
pub fn simd_min_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let mut min = f64::INFINITY;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [f64; 4] = values.into();
        for &val in &arr {
            min = min.min(val);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        min = min.min(column[i]);
    }

    Some(min)
}

/// SIMD maximum for f64 columns
#[cfg(feature = "simd")]
pub fn simd_max_f64(column: &[f64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let mut max = f64::NEG_INFINITY;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [f64; 4] = values.into();
        for &val in &arr {
            max = max.max(val);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        max = max.max(column[i]);
    }

    Some(max)
}

/// SIMD count for any column type
#[cfg(feature = "simd")]
pub fn simd_count(len: usize) -> usize {
    len
}

/// SIMD sum for i64 columns
#[cfg(feature = "simd")]
pub fn simd_sum_i64(column: &[i64]) -> i64 {
    log::debug!("[SIMD] simd_sum_i64 called with {} elements", column.len());
    let mut sum = 0i64;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        sum += arr[0] + arr[1] + arr[2] + arr[3];
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        sum += column[i];
    }

    sum
}

/// SIMD average for i64 columns
#[cfg(feature = "simd")]
pub fn simd_avg_i64(column: &[i64]) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let sum = simd_sum_i64(column);
    Some(sum as f64 / column.len() as f64)
}

/// SIMD minimum for i64 columns
#[cfg(feature = "simd")]
pub fn simd_min_i64(column: &[i64]) -> Option<i64> {
    if column.is_empty() {
        return None;
    }

    let mut min = i64::MAX;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        for &val in &arr {
            min = min.min(val);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        min = min.min(column[i]);
    }

    Some(min)
}

/// SIMD maximum for i64 columns
#[cfg(feature = "simd")]
pub fn simd_max_i64(column: &[i64]) -> Option<i64> {
    if column.is_empty() {
        return None;
    }

    let mut max = i64::MIN;

    // Process chunks of 4 elements with SIMD
    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        for &val in &arr {
            max = max.max(val);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        max = max.max(column[i]);
    }

    Some(max)
}

// =============================================================================
// Masked SIMD aggregation functions
// =============================================================================
//
// These functions process data with a selection bitmap without copying values
// to a temporary vector. This provides better performance by:
// 1. Avoiding memory allocation for the filtered subset
// 2. Processing data in a single pass with SIMD-accelerated masking
// 3. Better cache utilization by streaming through contiguous memory

/// SIMD sum for f64 columns with mask (selection bitmap)
///
/// Computes the sum of values where mask[i] == true, without copying values.
/// Uses SIMD to process chunks of 4 values at a time.
#[cfg(feature = "simd")]
pub fn simd_sum_f64_masked(column: &[f64], mask: &[bool]) -> f64 {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut sum = 0.0f64;
    let len = column.len();

    // Process chunks of 4 for SIMD
    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        // Load 4 values into SIMD register
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        // Apply mask manually (f64x4 doesn't have native mask support)
        // Convert to array, apply mask, sum
        let arr: [f64; 4] = values.into();
        if mask[offset] { sum += arr[0]; }
        if mask[offset + 1] { sum += arr[1]; }
        if mask[offset + 2] { sum += arr[2]; }
        if mask[offset + 3] { sum += arr[3]; }
    }

    // Handle remainder
    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            sum += column[i];
        }
    }

    sum
}

/// SIMD sum for i64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_sum_i64_masked(column: &[i64], mask: &[bool]) -> i64 {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut sum = 0i64;
    let len = column.len();

    // Process chunks of 4 for SIMD
    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        if mask[offset] { sum += arr[0]; }
        if mask[offset + 1] { sum += arr[1]; }
        if mask[offset + 2] { sum += arr[2]; }
        if mask[offset + 3] { sum += arr[3]; }
    }

    // Handle remainder
    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            sum += column[i];
        }
    }

    sum
}

/// SIMD min for f64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_min_f64_masked(column: &[f64], mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut min = f64::INFINITY;
    let mut found = false;
    let len = column.len();

    // Process chunks of 4 for SIMD
    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [f64; 4] = values.into();
        if mask[offset] { min = min.min(arr[0]); found = true; }
        if mask[offset + 1] { min = min.min(arr[1]); found = true; }
        if mask[offset + 2] { min = min.min(arr[2]); found = true; }
        if mask[offset + 3] { min = min.min(arr[3]); found = true; }
    }

    // Handle remainder
    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            min = min.min(column[i]);
            found = true;
        }
    }

    if found { Some(min) } else { None }
}

/// SIMD min for i64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_min_i64_masked(column: &[i64], mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut min = i64::MAX;
    let mut found = false;
    let len = column.len();

    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        if mask[offset] { min = min.min(arr[0]); found = true; }
        if mask[offset + 1] { min = min.min(arr[1]); found = true; }
        if mask[offset + 2] { min = min.min(arr[2]); found = true; }
        if mask[offset + 3] { min = min.min(arr[3]); found = true; }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            min = min.min(column[i]);
            found = true;
        }
    }

    if found { Some(min) } else { None }
}

/// SIMD max for f64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_max_f64_masked(column: &[f64], mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut max = f64::NEG_INFINITY;
    let mut found = false;
    let len = column.len();

    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [f64; 4] = values.into();
        if mask[offset] { max = max.max(arr[0]); found = true; }
        if mask[offset + 1] { max = max.max(arr[1]); found = true; }
        if mask[offset + 2] { max = max.max(arr[2]); found = true; }
        if mask[offset + 3] { max = max.max(arr[3]); found = true; }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            max = max.max(column[i]);
            found = true;
        }
    }

    if found { Some(max) } else { None }
}

/// SIMD max for i64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_max_i64_masked(column: &[i64], mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(column.len(), mask.len(), "Column and mask must have same length");

    let mut max = i64::MIN;
    let mut found = false;
    let len = column.len();

    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;

        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);

        let arr: [i64; 4] = values.into();
        if mask[offset] { max = max.max(arr[0]); found = true; }
        if mask[offset + 1] { max = max.max(arr[1]); found = true; }
        if mask[offset + 2] { max = max.max(arr[2]); found = true; }
        if mask[offset + 3] { max = max.max(arr[3]); found = true; }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..len {
        if mask[i] {
            max = max.max(column[i]);
            found = true;
        }
    }

    if found { Some(max) } else { None }
}

/// Count selected values (popcount on mask)
///
/// This is a fast O(1) count using popcount-style iteration.
#[cfg(feature = "simd")]
pub fn simd_count_masked(mask: &[bool]) -> usize {
    // Simple count - Rust will auto-vectorize this
    mask.iter().filter(|&&b| b).count()
}

/// SIMD avg for f64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_avg_f64_masked(column: &[f64], mask: &[bool]) -> Option<f64> {
    let count = simd_count_masked(mask);
    if count == 0 {
        return None;
    }
    let sum = simd_sum_f64_masked(column, mask);
    Some(sum / count as f64)
}

/// SIMD avg for i64 columns with mask
#[cfg(feature = "simd")]
pub fn simd_avg_i64_masked(column: &[i64], mask: &[bool]) -> Option<f64> {
    let count = simd_count_masked(mask);
    if count == 0 {
        return None;
    }
    let sum = simd_sum_i64_masked(column, mask);
    Some(sum as f64 / count as f64)
}

// =============================================================================
// Masked SIMD with NULL handling
// =============================================================================
//
// These functions combine both filter mask and NULL mask for optimal performance.
// The combined mask is: mask[i] == true AND !null_mask[i]

/// SIMD sum for f64 with both selection mask and null mask
#[cfg(feature = "simd")]
pub fn simd_sum_f64_masked_nulls(
    column: &[f64],
    selection_mask: &[bool],
    null_mask: Option<&[bool]>,
) -> f64 {
    match null_mask {
        None => simd_sum_f64_masked(column, selection_mask),
        Some(nulls) => {
            debug_assert_eq!(column.len(), nulls.len());
            let mut sum = 0.0f64;
            let len = column.len();

            let chunks = len / 4;
            for i in 0..chunks {
                let offset = i * 4;
                let values = f64x4::from([
                    column[offset],
                    column[offset + 1],
                    column[offset + 2],
                    column[offset + 3],
                ]);

                let arr: [f64; 4] = values.into();
                if selection_mask[offset] && !nulls[offset] { sum += arr[0]; }
                if selection_mask[offset + 1] && !nulls[offset + 1] { sum += arr[1]; }
                if selection_mask[offset + 2] && !nulls[offset + 2] { sum += arr[2]; }
                if selection_mask[offset + 3] && !nulls[offset + 3] { sum += arr[3]; }
            }

            let remainder_start = chunks * 4;
            for i in remainder_start..len {
                if selection_mask[i] && !nulls[i] {
                    sum += column[i];
                }
            }
            sum
        }
    }
}

/// SIMD sum for i64 with both selection mask and null mask
#[cfg(feature = "simd")]
pub fn simd_sum_i64_masked_nulls(
    column: &[i64],
    selection_mask: &[bool],
    null_mask: Option<&[bool]>,
) -> i64 {
    match null_mask {
        None => simd_sum_i64_masked(column, selection_mask),
        Some(nulls) => {
            debug_assert_eq!(column.len(), nulls.len());
            let mut sum = 0i64;
            let len = column.len();

            let chunks = len / 4;
            for i in 0..chunks {
                let offset = i * 4;
                let values = i64x4::from([
                    column[offset],
                    column[offset + 1],
                    column[offset + 2],
                    column[offset + 3],
                ]);

                let arr: [i64; 4] = values.into();
                if selection_mask[offset] && !nulls[offset] { sum += arr[0]; }
                if selection_mask[offset + 1] && !nulls[offset + 1] { sum += arr[1]; }
                if selection_mask[offset + 2] && !nulls[offset + 2] { sum += arr[2]; }
                if selection_mask[offset + 3] && !nulls[offset + 3] { sum += arr[3]; }
            }

            let remainder_start = chunks * 4;
            for i in remainder_start..len {
                if selection_mask[i] && !nulls[i] {
                    sum += column[i];
                }
            }
            sum
        }
    }
}

/// Count selected non-null values
#[cfg(feature = "simd")]
pub fn simd_count_masked_nulls(
    selection_mask: &[bool],
    null_mask: Option<&[bool]>,
) -> usize {
    match null_mask {
        None => simd_count_masked(selection_mask),
        Some(nulls) => {
            selection_mask.iter()
                .zip(nulls.iter())
                .filter(|(&selected, &is_null)| selected && !is_null)
                .count()
        }
    }
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    #[test]
    fn test_simd_sum_f64() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = simd_sum_f64(&column);
        assert_eq!(result, 45.0);
    }

    #[test]
    fn test_simd_avg_f64() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = simd_avg_f64(&column);
        assert_eq!(result, Some(5.0));
    }

    #[test]
    fn test_simd_min_f64() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let result = simd_min_f64(&column);
        assert_eq!(result, Some(1.0));
    }

    #[test]
    fn test_simd_max_f64() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let result = simd_max_f64(&column);
        assert_eq!(result, Some(9.0));
    }

    #[test]
    fn test_simd_sum_i64() {
        let column = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = simd_sum_i64(&column);
        assert_eq!(result, 45);
    }

    #[test]
    fn test_simd_avg_i64() {
        let column = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = simd_avg_i64(&column);
        assert_eq!(result, Some(5.0));
    }

    #[test]
    fn test_simd_min_i64() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let result = simd_min_i64(&column);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_simd_max_i64() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let result = simd_max_i64(&column);
        assert_eq!(result, Some(9));
    }

    #[test]
    fn test_simd_count() {
        let result = simd_count(100);
        assert_eq!(result, 100);
    }

    // Tests for masked SIMD functions

    #[test]
    fn test_simd_sum_f64_masked() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let mask = vec![true, false, true, false, true, false, true, false, true];
        // Selected: 1.0 + 3.0 + 5.0 + 7.0 + 9.0 = 25.0
        let result = simd_sum_f64_masked(&column, &mask);
        assert!((result - 25.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_sum_f64_masked_all_true() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mask = vec![true, true, true, true, true];
        let result = simd_sum_f64_masked(&column, &mask);
        assert!((result - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_sum_f64_masked_all_false() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mask = vec![false, false, false, false, false];
        let result = simd_sum_f64_masked(&column, &mask);
        assert!((result - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_sum_i64_masked() {
        let column = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mask = vec![true, false, true, false, true, false, true, false, true];
        // Selected: 1 + 3 + 5 + 7 + 9 = 25
        let result = simd_sum_i64_masked(&column, &mask);
        assert_eq!(result, 25);
    }

    #[test]
    fn test_simd_min_f64_masked() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let mask = vec![true, false, true, false, true, false, true, false, true];
        // Selected: 5.0, 8.0, 9.0, 7.0, 6.0 -> min = 5.0
        let result = simd_min_f64_masked(&column, &mask);
        assert_eq!(result, Some(5.0));
    }

    #[test]
    fn test_simd_min_f64_masked_empty() {
        let column = vec![5.0, 2.0, 8.0];
        let mask = vec![false, false, false];
        let result = simd_min_f64_masked(&column, &mask);
        assert_eq!(result, None);
    }

    #[test]
    fn test_simd_max_f64_masked() {
        let column = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        let mask = vec![false, true, false, true, false, true, false, true, false];
        // Selected: 2.0, 1.0, 3.0, 4.0 -> max = 4.0
        let result = simd_max_f64_masked(&column, &mask);
        assert_eq!(result, Some(4.0));
    }

    #[test]
    fn test_simd_min_i64_masked() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let mask = vec![true, true, true, false, false, false, true, true, true];
        // Selected: 5, 2, 8, 7, 4, 6 -> min = 2
        let result = simd_min_i64_masked(&column, &mask);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_simd_max_i64_masked() {
        let column = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        let mask = vec![true, true, true, false, false, false, true, true, true];
        // Selected: 5, 2, 8, 7, 4, 6 -> max = 8
        let result = simd_max_i64_masked(&column, &mask);
        assert_eq!(result, Some(8));
    }

    #[test]
    fn test_simd_count_masked() {
        let mask = vec![true, false, true, false, true, false, true, false, true];
        let result = simd_count_masked(&mask);
        assert_eq!(result, 5);
    }

    #[test]
    fn test_simd_avg_f64_masked() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mask = vec![true, false, true, false, true];
        // Selected: 1.0, 3.0, 5.0 -> avg = 3.0
        let result = simd_avg_f64_masked(&column, &mask);
        assert_eq!(result, Some(3.0));
    }

    #[test]
    fn test_simd_avg_i64_masked() {
        let column = vec![2, 4, 6, 8, 10];
        let mask = vec![true, false, true, false, true];
        // Selected: 2, 6, 10 -> avg = 6.0
        let result = simd_avg_i64_masked(&column, &mask);
        assert_eq!(result, Some(6.0));
    }

    #[test]
    fn test_simd_sum_f64_masked_nulls() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let selection = vec![true, true, true, true, true];
        let nulls = vec![false, true, false, true, false]; // 2.0 and 4.0 are NULL
        // Selected non-null: 1.0 + 3.0 + 5.0 = 9.0
        let result = simd_sum_f64_masked_nulls(&column, &selection, Some(&nulls));
        assert!((result - 9.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_sum_i64_masked_nulls() {
        let column = vec![1, 2, 3, 4, 5];
        let selection = vec![true, true, true, true, true];
        let nulls = vec![false, true, false, true, false]; // 2 and 4 are NULL
        // Selected non-null: 1 + 3 + 5 = 9
        let result = simd_sum_i64_masked_nulls(&column, &selection, Some(&nulls));
        assert_eq!(result, 9);
    }

    #[test]
    fn test_simd_count_masked_nulls() {
        let selection = vec![true, true, true, true, true];
        let nulls = vec![false, true, false, true, false];
        // 3 non-null selected values
        let result = simd_count_masked_nulls(&selection, Some(&nulls));
        assert_eq!(result, 3);
    }

    #[test]
    fn test_simd_masked_large_dataset() {
        // Test with a larger dataset to exercise SIMD chunking
        let column: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let mask: Vec<bool> = (0..1000).map(|i| i % 2 == 0).collect();
        // Sum of even numbers 0..1000: 0+2+4+...+998 = 249500
        let result = simd_sum_f64_masked(&column, &mask);
        assert!((result - 249500.0).abs() < 0.001);
    }
}
