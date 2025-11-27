//! NULL handling for SIMD operations using bitmasks

#![allow(clippy::needless_range_loop)]

#[cfg(feature = "simd")]
use wide::*;

/// Bitmask representing NULL values in a column
#[cfg(feature = "simd")]
pub struct NullBitmask {
    /// Bit vector where 1 indicates NULL, 0 indicates non-NULL
    bits: Vec<u8>,
    /// Number of elements this bitmask represents
    len: usize,
}

#[cfg(feature = "simd")]
impl NullBitmask {
    /// Create a new NULL bitmask for the given length
    pub fn new(len: usize) -> Self {
        let bytes = len.div_ceil(8); // Round up to nearest byte
        Self {
            bits: vec![0; bytes],
            len,
        }
    }

    /// Create a NULL bitmask from a boolean vector (true = NULL)
    pub fn from_nulls(nulls: &[bool]) -> Self {
        let mut bitmask = Self::new(nulls.len());
        for (i, &is_null) in nulls.iter().enumerate() {
            if is_null {
                bitmask.set_null(i);
            }
        }
        bitmask
    }

    /// Mark position as NULL
    pub fn set_null(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        self.bits[byte_index] |= 1 << bit_index;
    }

    /// Check if position is NULL
    pub fn is_null(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        (self.bits[byte_index] & (1 << bit_index)) != 0
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if bitmask is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// SIMD sum for f64 columns with NULL handling
#[cfg(feature = "simd")]
pub fn simd_sum_f64_with_nulls(column: &[f64], nulls: &NullBitmask) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let mut sum = 0.0;
    let mut count = 0;

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

        // Create mask for non-NULL values
        let mut mask_values = [0.0; 4];
        for j in 0..4 {
            if !nulls.is_null(offset + j) {
                mask_values[j] = 1.0;
                count += 1;
            }
        }
        let mask = f64x4::from(mask_values);

        // Multiply values by mask (NULL values become 0.0)
        let masked_values = values * mask;
        let arr: [f64; 4] = masked_values.into();
        sum += arr[0] + arr[1] + arr[2] + arr[3];
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        if !nulls.is_null(i) {
            sum += column[i];
            count += 1;
        }
    }

    if count == 0 {
        None
    } else {
        Some(sum)
    }
}

/// SIMD average for f64 columns with NULL handling
#[cfg(feature = "simd")]
pub fn simd_avg_f64_with_nulls(column: &[f64], nulls: &NullBitmask) -> Option<f64> {
    if column.is_empty() {
        return None;
    }

    let mut sum = 0.0;
    let mut count = 0;

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

        // Create mask for non-NULL values
        let mut mask_values = [0.0; 4];
        for j in 0..4 {
            if !nulls.is_null(offset + j) {
                mask_values[j] = 1.0;
                count += 1;
            }
        }
        let mask = f64x4::from(mask_values);

        // Multiply values by mask (NULL values become 0.0)
        let masked_values = values * mask;
        let arr: [f64; 4] = masked_values.into();
        sum += arr[0] + arr[1] + arr[2] + arr[3];
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        if !nulls.is_null(i) {
            sum += column[i];
            count += 1;
        }
    }

    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

/// SIMD count for columns with NULL handling
#[cfg(feature = "simd")]
pub fn simd_count_with_nulls(nulls: &NullBitmask) -> usize {
    let mut count = 0;

    for i in 0..nulls.len() {
        if !nulls.is_null(i) {
            count += 1;
        }
    }

    count
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    #[test]
    fn test_null_bitmask() {
        let mut bitmask = NullBitmask::new(10);
        assert_eq!(bitmask.len(), 10);
        assert!(!bitmask.is_null(0));

        bitmask.set_null(0);
        bitmask.set_null(5);
        bitmask.set_null(9);

        assert!(bitmask.is_null(0));
        assert!(!bitmask.is_null(1));
        assert!(bitmask.is_null(5));
        assert!(bitmask.is_null(9));
    }

    #[test]
    fn test_null_bitmask_from_nulls() {
        let nulls = vec![true, false, false, true, false, true, false, false, true];
        let bitmask = NullBitmask::from_nulls(&nulls);

        assert!(bitmask.is_null(0));
        assert!(!bitmask.is_null(1));
        assert!(!bitmask.is_null(2));
        assert!(bitmask.is_null(3));
        assert!(!bitmask.is_null(4));
        assert!(bitmask.is_null(5));
        assert!(!bitmask.is_null(6));
        assert!(!bitmask.is_null(7));
        assert!(bitmask.is_null(8));
    }

    #[test]
    fn test_simd_sum_f64_with_nulls() {
        let column = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let nulls = NullBitmask::from_nulls(&vec![
            false, true, false, false, true, false, false, false, true,
        ]);

        let result = simd_sum_f64_with_nulls(&column, &nulls);
        // Sum of 1.0, 3.0, 4.0, 6.0, 7.0, 8.0 = 29.0 (skipping NULL at indices 1, 4, 8)
        assert_eq!(result, Some(29.0));
    }

    #[test]
    fn test_simd_avg_f64_with_nulls() {
        let column = vec![2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0];
        let nulls = NullBitmask::from_nulls(&vec![
            false, true, false, false, true, false, false, false, true,
        ]);

        let result = simd_avg_f64_with_nulls(&column, &nulls);
        // Average of 2.0, 6.0, 8.0, 12.0, 14.0, 16.0 = 58.0 / 6 = 9.666...
        assert!(result.is_some());
        let avg = result.unwrap();
        assert!((avg - 9.666666666666666).abs() < 1e-10);
    }

    #[test]
    fn test_simd_count_with_nulls() {
        let nulls = NullBitmask::from_nulls(&vec![
            false, true, false, false, true, false, false, false, true,
        ]);

        let count = simd_count_with_nulls(&nulls);
        assert_eq!(count, 6); // 9 total - 3 NULLs
    }

    #[test]
    fn test_all_nulls() {
        let column = vec![1.0, 2.0, 3.0];
        let nulls = NullBitmask::from_nulls(&vec![true, true, true]);

        let sum = simd_sum_f64_with_nulls(&column, &nulls);
        let avg = simd_avg_f64_with_nulls(&column, &nulls);

        assert_eq!(sum, None);
        assert_eq!(avg, None);
    }
}
