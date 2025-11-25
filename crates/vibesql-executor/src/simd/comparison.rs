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
/// SIMD not-equal comparison for i64 columns
#[cfg(feature = "simd")]
pub fn simd_ne_i64(column: &[i64], value: i64) -> Vec<bool> {
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
        // NE is equivalent to NOT(EQ)
        let mask_eq = values.cmp_eq(val);
        let arr: [i64; 4] = mask_eq.into();
        for &v in &arr {
            result.push(v == 0); // NOT EQ
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] != value);
    }

    result
}

/// SIMD greater-than comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_gt_i32(column: &[i32], threshold: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i32x4::from([threshold; 4]);
        let mask = values.cmp_gt(thresh);

        let arr: [i32; 4] = mask.into();
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

/// SIMD greater-than-or-equal comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_ge_i32(column: &[i32], threshold: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i32x4::from([threshold; 4]);
        // GE is equivalent to NOT(LT)
        let mask_lt = values.cmp_lt(thresh);
        let arr: [i32; 4] = mask_lt.into();
        for &val in &arr {
            result.push(val == 0); // NOT LT
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] >= threshold);
    }

    result
}

/// SIMD less-than comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_lt_i32(column: &[i32], threshold: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i32x4::from([threshold; 4]);
        let mask = values.cmp_lt(thresh);

        let arr: [i32; 4] = mask.into();
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

/// SIMD less-than-or-equal comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_le_i32(column: &[i32], threshold: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let thresh = i32x4::from([threshold; 4]);
        // LE is equivalent to NOT(GT)
        let mask_gt = values.cmp_gt(thresh);
        let arr: [i32; 4] = mask_gt.into();
        for &val in &arr {
            result.push(val == 0); // NOT GT
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] <= threshold);
    }

    result
}

/// SIMD equality comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_eq_i32(column: &[i32], value: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let val = i32x4::from([value; 4]);
        let mask = values.cmp_eq(val);

        let arr: [i32; 4] = mask.into();
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

/// SIMD not-equal comparison for i32 columns (dates)
#[cfg(feature = "simd")]
pub fn simd_ne_i32(column: &[i32], value: i32) -> Vec<bool> {
    let mut result = Vec::with_capacity(column.len());

    let chunks = column.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let values = i32x4::from([
            column[offset],
            column[offset + 1],
            column[offset + 2],
            column[offset + 3],
        ]);
        let val = i32x4::from([value; 4]);
        // NE is equivalent to NOT(EQ)
        let mask_eq = values.cmp_eq(val);
        let arr: [i32; 4] = mask_eq.into();
        for &v in &arr {
            result.push(v == 0); // NOT EQ
        }
    }

    let remainder_start = chunks * 4;
    for i in remainder_start..column.len() {
        result.push(column[i] != value);
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

    #[test]
    fn test_simd_ge_i64() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_ge_i64(&column, 5);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_simd_le_i64() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_le_i64(&column, 5);
        assert_eq!(
            result,
            vec![true, true, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_simd_ne_i64() {
        let column = vec![1, 5, 3, 5, 2, 5, 4, 6, 9];
        let result = simd_ne_i64(&column, 5);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, true, true]
        );
    }

    #[test]
    fn test_simd_gt_i32() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_gt_i32(&column, 5);
        assert_eq!(
            result,
            vec![false, false, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_simd_ge_i32() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_ge_i32(&column, 5);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, true, true]
        );
    }

    #[test]
    fn test_simd_lt_i32() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_lt_i32(&column, 5);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_simd_le_i32() {
        let column = vec![1, 5, 3, 8, 2, 10, 4, 6, 9];
        let result = simd_le_i32(&column, 5);
        assert_eq!(
            result,
            vec![true, true, true, false, true, false, true, false, false]
        );
    }

    #[test]
    fn test_simd_eq_i32() {
        let column = vec![1, 5, 3, 5, 2, 5, 4, 6, 9];
        let result = simd_eq_i32(&column, 5);
        assert_eq!(
            result,
            vec![false, true, false, true, false, true, false, false, false]
        );
    }

    #[test]
    fn test_simd_ne_i32() {
        let column = vec![1, 5, 3, 5, 2, 5, 4, 6, 9];
        let result = simd_ne_i32(&column, 5);
        assert_eq!(
            result,
            vec![true, false, true, false, true, false, true, true, true]
        );
    }

    #[test]
    fn test_simd_i32_dates() {
        // Test with date values (days since epoch)
        // Date: 2024-01-01 = 19723 days, 2024-01-15 = 19737 days, 2024-02-01 = 19754 days
        let dates = vec![19720, 19723, 19730, 19737, 19740, 19750, 19754, 19760, 19770];
        let threshold = 19737; // 2024-01-15

        // Test greater than 2024-01-15
        let result = simd_gt_i32(&dates, threshold);
        assert_eq!(
            result,
            vec![false, false, false, false, true, true, true, true, true]
        );

        // Test less than or equal to 2024-01-15
        let result = simd_le_i32(&dates, threshold);
        assert_eq!(
            result,
            vec![true, true, true, true, false, false, false, false, false]
        );
    }
}
