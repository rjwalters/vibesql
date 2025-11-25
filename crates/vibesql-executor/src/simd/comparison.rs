//! SIMD comparison operations for columnar data

#[cfg(feature = "simd")]
use wide::*;

/// SIMD greater-than comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_gt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

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
        let thresh = f64x4::from([threshold; 4]);
        let mask = values.cmp_gt(thresh);

        // Extract individual boolean values from the mask
        let arr: [f64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0.0);
        }
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] > threshold);
    }

    result
}

/// SIMD greater-than-or-equal comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_ge_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

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
        let thresh = f64x4::from([threshold; 4]);
        let mask = values.cmp_ge(thresh);

        let arr: [f64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0.0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] >= threshold);
    }

    result
}

/// SIMD less-than comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_lt_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = f64x4::from([threshold; 4]);
        let mask = values.cmp_lt(thresh);

        let arr: [f64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0.0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] < threshold);
    }

    result
}

/// SIMD less-than-or-equal comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_le_f64(column: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = f64x4::from([threshold; 4]);
        let mask = values.cmp_le(thresh);

        let arr: [f64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0.0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] <= threshold);
    }

    result
}

/// SIMD equality comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_eq_f64(column: &[f64], value: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let val = f64x4::from([value; 4]);
        let mask = values.cmp_eq(val);

        let arr: [f64; 4] = mask.into();
        for &v in &arr {
            result.push(v != 0.0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] == value);
    }

    result
}

/// SIMD not-equal comparison for f64 columns
#[cfg(feature = "simd")]
pub fn simd_ne_f64(column: &[f64], value: f64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = f64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let val = f64x4::from([value; 4]);
        let mask = values.cmp_ne(val);

        let arr: [f64; 4] = mask.into();
        for &v in &arr {
            result.push(v != 0.0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] != value);
    }

    result
}

/// SIMD greater-than comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_gt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i64x4::from([threshold; 4]);
        let mask = values.cmp_gt(thresh);

        let arr: [i64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] > threshold);
    }

    result
}

/// SIMD less-than comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_lt_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i64x4::from([threshold; 4]);
        let mask = values.cmp_lt(thresh);

        let arr: [i64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] < threshold);
    }

    result
}

/// SIMD greater-than-or-equal comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_ge_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i64x4::from([threshold; 4]);
        // a >= b is equivalent to !(a < b)
        let mask = !values.cmp_lt(thresh);

        let arr: [i64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] >= threshold);
    }

    result
}

/// SIMD less-than-or-equal comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_le_i64(column: &[i64], threshold: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i64x4::from([threshold; 4]);
        // a <= b is equivalent to !(a > b)
        let mask = !values.cmp_gt(thresh);

        let arr: [i64; 4] = mask.into();
        for &val in &arr {
            result.push(val != 0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] <= threshold);
    }

    result
}

/// SIMD equality comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_eq_i64(column: &[i64], value: i64) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i64x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let val = i64x4::from([value; 4]);
        let mask = values.cmp_eq(val);

        let arr: [i64; 4] = mask.into();
        for &v in &arr {
            result.push(v != 0);
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] == value);
    }

    result
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    #[test]
    fn test_simd_gt_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = simd_gt_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![false, false, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_simd_lt_f64() {
        let column = vec![1.0, 5.0, 3.0, 8.0, 2.0, 10.0, 4.0, 6.0, 9.0];
        let result = simd_lt_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_simd_eq_f64() {
        let column = vec![1.0, 5.0, 3.0, 5.0, 2.0, 5.0, 4.0, 6.0, 9.0];
        let result = simd_eq_f64(&column, 5.0);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, false, false]
        );
    }

    #[test]
    fn test_simd_gt_i64() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_gt_i64(&column, 5);
        assert_eq!(
            result,
            vec![false, false, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_simd_eq_i64() {
        let column = vec![1, 5, 3, 5, 2, 5, 4, 6, 9];
        let result = simd_eq_i64(&column, 5);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, false, false]
        );
    }
}
