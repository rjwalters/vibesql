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
}
