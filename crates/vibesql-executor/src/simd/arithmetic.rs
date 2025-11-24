//! SIMD arithmetic operations for columnar data

#[cfg(feature = "simd")]
use wide::*;

/// SIMD addition for f64 columns
#[cfg(feature = "simd")]
pub fn simd_add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD (f64x4)
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let sum = a_vec + b_vec;

        let arr: [f64; 4] = sum.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// SIMD subtraction for f64 columns
#[cfg(feature = "simd")]
pub fn simd_sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let diff = a_vec - b_vec;

        let arr: [f64; 4] = diff.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// SIMD multiplication for f64 columns
#[cfg(feature = "simd")]
pub fn simd_mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let product = a_vec * b_vec;

        let arr: [f64; 4] = product.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}

/// SIMD division for f64 columns
#[cfg(feature = "simd")]
pub fn simd_div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = f64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = f64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let quotient = a_vec / b_vec;

        let arr: [f64; 4] = quotient.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] / b[i]);
    }

    result
}

/// SIMD addition for i64 columns
#[cfg(feature = "simd")]
pub fn simd_add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let sum = a_vec + b_vec;

        let arr: [i64; 4] = sum.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// SIMD subtraction for i64 columns
#[cfg(feature = "simd")]
pub fn simd_sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let diff = a_vec - b_vec;

        let arr: [i64; 4] = diff.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// SIMD multiplication for i64 columns
#[cfg(feature = "simd")]
pub fn simd_mul_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 4 elements with SIMD
    let chunks = a.len() / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let a_vec = i64x4::from([a[offset], a[offset + 1], a[offset + 2], a[offset + 3]]);
        let b_vec = i64x4::from([b[offset], b[offset + 1], b[offset + 2], b[offset + 3]]);
        let product = a_vec * b_vec;

        let arr: [i64; 4] = product.into();
        result.extend_from_slice(&arr);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 4;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}


#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

    // ===== Basic functionality tests =====

    #[test]
    fn test_simd_add_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let result = simd_add_f64(&a, &b);
        assert_eq!(result, vec![11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0, 99.0]);
    }

    #[test]
    fn test_simd_sub_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let result = simd_sub_f64(&a, &b);
        assert_eq!(result, vec![9.0, 18.0, 27.0, 36.0, 45.0, 54.0, 63.0, 72.0, 81.0]);
    }

    #[test]
    fn test_simd_mul_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let result = simd_mul_f64(&a, &b);
        assert_eq!(result, vec![2.0, 6.0, 12.0, 20.0, 30.0, 42.0, 56.0, 72.0, 90.0]);
    }

    #[test]
    fn test_simd_div_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
        let b = vec![2.0, 4.0, 3.0, 8.0, 5.0, 6.0, 7.0, 10.0, 9.0];
        let result = simd_div_f64(&a, &b);
        assert_eq!(result, vec![5.0, 5.0, 10.0, 5.0, 10.0, 10.0, 10.0, 8.0, 10.0]);
    }

    #[test]
    fn test_simd_add_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![10, 20, 30, 40, 50, 60, 70, 80, 90];
        let result = simd_add_i64(&a, &b);
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88, 99]);
    }

    #[test]
    fn test_simd_mul_i64() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let b = vec![2, 3, 4, 5, 6, 7, 8, 9, 10];
        let result = simd_mul_i64(&a, &b);
        assert_eq!(result, vec![2, 6, 12, 20, 30, 42, 56, 72, 90]);
    }

    // ===== Edge case tests =====

    #[test]
    fn test_empty_array_f64() {
        let a: Vec<f64> = vec![];
        let b: Vec<f64> = vec![];

        assert_eq!(simd_add_f64(&a, &b), Vec::<f64>::new());
        assert_eq!(simd_sub_f64(&a, &b), Vec::<f64>::new());
        assert_eq!(simd_mul_f64(&a, &b), Vec::<f64>::new());
        assert_eq!(simd_div_f64(&a, &b), Vec::<f64>::new());
    }

    #[test]
    fn test_empty_array_i64() {
        let a: Vec<i64> = vec![];
        let b: Vec<i64> = vec![];

        assert_eq!(simd_add_i64(&a, &b), Vec::<i64>::new());
        assert_eq!(simd_sub_i64(&a, &b), Vec::<i64>::new());
        assert_eq!(simd_mul_i64(&a, &b), Vec::<i64>::new());
    }

    #[test]
    fn test_single_element_f64() {
        let a = vec![5.0];
        let b = vec![3.0];

        assert_eq!(simd_add_f64(&a, &b), vec![8.0]);
        assert_eq!(simd_sub_f64(&a, &b), vec![2.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![15.0]);
        assert_eq!(simd_div_f64(&a, &b), vec![5.0 / 3.0]);
    }

    #[test]
    fn test_single_element_i64() {
        let a = vec![5];
        let b = vec![3];

        assert_eq!(simd_add_i64(&a, &b), vec![8]);
        assert_eq!(simd_sub_i64(&a, &b), vec![2]);
        assert_eq!(simd_mul_i64(&a, &b), vec![15]);
    }

    #[test]
    fn test_two_elements_f64() {
        let a = vec![5.0, 10.0];
        let b = vec![3.0, 2.0];

        assert_eq!(simd_add_f64(&a, &b), vec![8.0, 12.0]);
        assert_eq!(simd_sub_f64(&a, &b), vec![2.0, 8.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![15.0, 20.0]);
    }

    #[test]
    fn test_three_elements_f64() {
        let a = vec![5.0, 10.0, 15.0];
        let b = vec![3.0, 2.0, 4.0];

        assert_eq!(simd_add_f64(&a, &b), vec![8.0, 12.0, 19.0]);
        assert_eq!(simd_sub_f64(&a, &b), vec![2.0, 8.0, 11.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![15.0, 20.0, 60.0]);
    }

    #[test]
    fn test_exactly_four_elements_f64() {
        // Test exactly one SIMD chunk (4 elements)
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![10.0, 20.0, 30.0, 40.0];

        assert_eq!(simd_add_f64(&a, &b), vec![11.0, 22.0, 33.0, 44.0]);
        assert_eq!(simd_sub_f64(&a, &b), vec![-9.0, -18.0, -27.0, -36.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![10.0, 40.0, 90.0, 160.0]);
    }

    #[test]
    fn test_five_elements_with_remainder_f64() {
        // Test SIMD path + 1 element remainder
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0];

        assert_eq!(simd_add_f64(&a, &b), vec![11.0, 22.0, 33.0, 44.0, 55.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![10.0, 40.0, 90.0, 160.0, 250.0]);
    }

    #[test]
    fn test_large_dataset_f64() {
        // Test with 1000 elements to ensure SIMD path works at scale
        let a: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let b: Vec<f64> = (0..1000).map(|i| (i * 2) as f64).collect();

        let result_add = simd_add_f64(&a, &b);
        let result_mul = simd_mul_f64(&a, &b);

        assert_eq!(result_add.len(), 1000);
        assert_eq!(result_mul.len(), 1000);

        // Verify a few spot checks
        assert_eq!(result_add[0], 0.0);
        assert_eq!(result_add[100], 300.0);
        assert_eq!(result_mul[10], 200.0);
    }

    #[test]
    fn test_large_dataset_i64() {
        // Test with 1000 elements for i64
        let a: Vec<i64> = (0..1000).collect();
        let b: Vec<i64> = (0..1000).map(|i| i * 2).collect();

        let result_add = simd_add_i64(&a, &b);
        let result_mul = simd_mul_i64(&a, &b);

        assert_eq!(result_add.len(), 1000);
        assert_eq!(result_mul.len(), 1000);

        // Verify spot checks
        assert_eq!(result_add[0], 0);
        assert_eq!(result_add[100], 300);
        assert_eq!(result_mul[10], 200);
    }

    #[test]
    fn test_all_same_values_f64() {
        let a = vec![5.5; 10];
        let b = vec![2.0; 10];

        let result_add = simd_add_f64(&a, &b);
        let result_mul = simd_mul_f64(&a, &b);

        for val in result_add {
            assert!((val - 7.5).abs() < 1e-10);
        }
        for val in result_mul {
            assert!((val - 11.0).abs() < 1e-10);
        }
    }

    #[test]
    fn test_negative_values_f64() {
        let a = vec![-1.0, -2.0, -3.0, -4.0, -5.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0];

        assert_eq!(simd_add_f64(&a, &b), vec![9.0, 18.0, 27.0, 36.0, 45.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![-10.0, -40.0, -90.0, -160.0, -250.0]);
    }

    #[test]
    fn test_negative_values_i64() {
        let a = vec![-1, -2, -3, -4, -5];
        let b = vec![10, 20, 30, 40, 50];

        assert_eq!(simd_add_i64(&a, &b), vec![9, 18, 27, 36, 45]);
        assert_eq!(simd_mul_i64(&a, &b), vec![-10, -40, -90, -160, -250]);
    }

    #[test]
    fn test_zero_values_f64() {
        let a = vec![0.0, 1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 0.0, 7.0, 0.0, 9.0];

        assert_eq!(simd_add_f64(&a, &b), vec![5.0, 1.0, 9.0, 3.0, 13.0]);
        assert_eq!(simd_mul_f64(&a, &b), vec![0.0, 0.0, 14.0, 0.0, 36.0]);
    }

    #[test]
    fn test_division_by_zero_f64() {
        let a = vec![10.0, 20.0, 30.0, 40.0];
        let b = vec![2.0, 0.0, 5.0, 0.0];

        let result = simd_div_f64(&a, &b);

        assert_eq!(result[0], 5.0);
        assert!(result[1].is_infinite());
        assert_eq!(result[2], 6.0);
        assert!(result[3].is_infinite());
    }

    #[test]
    fn test_very_small_numbers_f64() {
        let a = vec![1e-100, 2e-100, 3e-100, 4e-100, 5e-100];
        let b = vec![1e-100, 2e-100, 3e-100, 4e-100, 5e-100];

        let result_add = simd_add_f64(&a, &b);

        assert!((result_add[0] - 2e-100).abs() < 1e-110);
        assert!((result_add[1] - 4e-100).abs() < 1e-110);
    }

    #[test]
    fn test_very_large_numbers_f64() {
        let a = vec![1e100, 2e100, 3e100, 4e100, 5e100];
        let b = vec![1e100, 2e100, 3e100, 4e100, 5e100];

        let result_add = simd_add_f64(&a, &b);

        assert!((result_add[0] - 2e100).abs() < 1e90);
        assert!((result_add[1] - 4e100).abs() < 1e90);
    }

    #[test]
    fn test_mixed_positive_negative_i64() {
        let a = vec![1, -2, 3, -4, 5, -6, 7, -8];
        let b = vec![-1, 2, -3, 4, -5, 6, -7, 8];

        assert_eq!(simd_add_i64(&a, &b), vec![0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(simd_sub_i64(&a, &b), vec![2, -4, 6, -8, 10, -12, 14, -16]);
    }

    // ===== Correctness tests (SIMD vs Scalar) =====

    #[test]
    fn test_simd_matches_scalar_add_f64() {
        let a: Vec<f64> = (0..100).map(|i| (i as f64) * 1.5).collect();
        let b: Vec<f64> = (0..100).map(|i| (i as f64) * 2.5).collect();

        // SIMD path
        let simd_result = simd_add_f64(&a, &b);

        // Scalar path
        let scalar_result: Vec<f64> = a.iter().zip(b.iter())
            .map(|(x, y)| x + y)
            .collect();

        assert_eq!(simd_result.len(), scalar_result.len());
        for (simd_val, scalar_val) in simd_result.iter().zip(scalar_result.iter()) {
            assert!((simd_val - scalar_val).abs() < 1e-10,
                "SIMD: {}, Scalar: {}", simd_val, scalar_val);
        }
    }

    #[test]
    fn test_simd_matches_scalar_mul_f64() {
        let a: Vec<f64> = (0..100).map(|i| (i as f64) * 1.5).collect();
        let b: Vec<f64> = (0..100).map(|i| (i as f64) * 2.5).collect();

        let simd_result = simd_mul_f64(&a, &b);
        let scalar_result: Vec<f64> = a.iter().zip(b.iter())
            .map(|(x, y)| x * y)
            .collect();

        for (simd_val, scalar_val) in simd_result.iter().zip(scalar_result.iter()) {
            assert!((simd_val - scalar_val).abs() < 1e-10,
                "SIMD: {}, Scalar: {}", simd_val, scalar_val);
        }
    }

    #[test]
    fn test_simd_matches_scalar_div_f64() {
        let a: Vec<f64> = (1..101).map(|i| (i as f64) * 10.0).collect();
        let b: Vec<f64> = (1..101).map(|i| i as f64).collect();

        let simd_result = simd_div_f64(&a, &b);
        let scalar_result: Vec<f64> = a.iter().zip(b.iter())
            .map(|(x, y)| x / y)
            .collect();

        for (simd_val, scalar_val) in simd_result.iter().zip(scalar_result.iter()) {
            assert!((simd_val - scalar_val).abs() < 1e-10,
                "SIMD: {}, Scalar: {}", simd_val, scalar_val);
        }
    }

    #[test]
    fn test_simd_matches_scalar_add_i64() {
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (100..200).collect();

        let simd_result = simd_add_i64(&a, &b);
        let scalar_result: Vec<i64> = a.iter().zip(b.iter())
            .map(|(x, y)| x + y)
            .collect();

        assert_eq!(simd_result, scalar_result);
    }

    #[test]
    fn test_simd_matches_scalar_mul_i64() {
        let a: Vec<i64> = (0..100).collect();
        let b: Vec<i64> = (1..101).collect();

        let simd_result = simd_mul_i64(&a, &b);
        let scalar_result: Vec<i64> = a.iter().zip(b.iter())
            .map(|(x, y)| x * y)
            .collect();

        assert_eq!(simd_result, scalar_result);
    }

    // ===== Different data sizes =====

    #[test]
    fn test_various_sizes_f64() {
        let sizes = vec![0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 1000];

        for size in sizes {
            let a: Vec<f64> = (0..size).map(|i| i as f64).collect();
            let b: Vec<f64> = (0..size).map(|i| (i * 2) as f64).collect();

            let result_add = simd_add_f64(&a, &b);
            let result_mul = simd_mul_f64(&a, &b);

            assert_eq!(result_add.len(), size as usize, "Add failed for size {}", size);
            assert_eq!(result_mul.len(), size as usize, "Mul failed for size {}", size);
        }
    }

    #[test]
    fn test_various_sizes_i64() {
        let sizes = vec![0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 1000];

        for size in sizes {
            let a: Vec<i64> = (0..size).collect();
            let b: Vec<i64> = (0..size).map(|i| i * 2).collect();

            let result_add = simd_add_i64(&a, &b);
            let result_mul = simd_mul_i64(&a, &b);

            assert_eq!(result_add.len(), size as usize, "Add failed for size {}", size);
            assert_eq!(result_mul.len(), size as usize, "Mul failed for size {}", size);
        }
    }
}
