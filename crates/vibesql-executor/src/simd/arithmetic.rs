//! SIMD arithmetic operations for columnar data

#[cfg(feature = "simd")]
use wide::*;

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
use std::arch::x86_64::*;

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

// AVX-512 implementations (8 elements at a time)
// These provide 2x throughput on AVX-512 capable CPUs

/// AVX-512 addition for f64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_add_f64_avx512(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        // Load 8 f64 values from a and b
        let a_vec = _mm512_loadu_pd(a.as_ptr().add(offset));
        let b_vec = _mm512_loadu_pd(b.as_ptr().add(offset));

        // Perform SIMD addition
        let sum = _mm512_add_pd(a_vec, b_vec);

        // Store result
        let mut temp = [0.0; 8];
        _mm512_storeu_pd(temp.as_mut_ptr(), sum);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// AVX-512 subtraction for f64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_sub_f64_avx512(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_pd(a.as_ptr().add(offset));
        let b_vec = _mm512_loadu_pd(b.as_ptr().add(offset));
        let diff = _mm512_sub_pd(a_vec, b_vec);

        let mut temp = [0.0; 8];
        _mm512_storeu_pd(temp.as_mut_ptr(), diff);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// AVX-512 multiplication for f64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_mul_f64_avx512(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_pd(a.as_ptr().add(offset));
        let b_vec = _mm512_loadu_pd(b.as_ptr().add(offset));
        let product = _mm512_mul_pd(a_vec, b_vec);

        let mut temp = [0.0; 8];
        _mm512_storeu_pd(temp.as_mut_ptr(), product);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}

/// AVX-512 division for f64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_div_f64_avx512(a: &[f64], b: &[f64]) -> Vec<f64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_pd(a.as_ptr().add(offset));
        let b_vec = _mm512_loadu_pd(b.as_ptr().add(offset));
        let quotient = _mm512_div_pd(a_vec, b_vec);

        let mut temp = [0.0; 8];
        _mm512_storeu_pd(temp.as_mut_ptr(), quotient);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] / b[i]);
    }

    result
}

/// AVX-512 addition for i64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_add_i64_avx512(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_epi64(a.as_ptr().add(offset) as *const i64);
        let b_vec = _mm512_loadu_epi64(b.as_ptr().add(offset) as *const i64);
        let sum = _mm512_add_epi64(a_vec, b_vec);

        let mut temp = [0i64; 8];
        _mm512_storeu_epi64(temp.as_mut_ptr() as *mut i64, sum);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] + b[i]);
    }

    result
}

/// AVX-512 subtraction for i64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_sub_i64_avx512(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_epi64(a.as_ptr().add(offset) as *const i64);
        let b_vec = _mm512_loadu_epi64(b.as_ptr().add(offset) as *const i64);
        let diff = _mm512_sub_epi64(a_vec, b_vec);

        let mut temp = [0i64; 8];
        _mm512_storeu_epi64(temp.as_mut_ptr() as *mut i64, diff);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] - b[i]);
    }

    result
}

/// AVX-512 multiplication for i64 columns (8 elements at a time)
#[cfg(all(feature = "simd", target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
pub unsafe fn simd_mul_i64_avx512(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut result = Vec::with_capacity(a.len());

    // Process chunks of 8 elements with AVX-512
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;

        let a_vec = _mm512_loadu_epi64(a.as_ptr().add(offset) as *const i64);
        let b_vec = _mm512_loadu_epi64(b.as_ptr().add(offset) as *const i64);
        let product = _mm512_mullo_epi64(a_vec, b_vec);

        let mut temp = [0i64; 8];
        _mm512_storeu_epi64(temp.as_mut_ptr() as *mut i64, product);
        result.extend_from_slice(&temp);
    }

    // Handle remainder elements with scalar fallback
    let remainder_start = chunks * 8;
    for i in remainder_start..a.len() {
        result.push(a[i] * b[i]);
    }

    result
}


#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;

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

    // AVX-512 tests (only run on AVX-512 capable CPUs)
    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_add_f64_avx512() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0];
        let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0];
        let result = unsafe { simd_add_f64_avx512(&a, &b) };
        assert_eq!(result, vec![11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0, 99.0, 110.0, 121.0, 132.0, 143.0, 154.0, 165.0, 176.0, 187.0]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_sub_f64_avx512() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0];
        let b = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0];
        let result = unsafe { simd_sub_f64_avx512(&a, &b) };
        assert_eq!(result, vec![9.0, 18.0, 27.0, 36.0, 45.0, 54.0, 63.0, 72.0, 81.0, 90.0, 99.0, 108.0, 117.0, 126.0, 135.0, 144.0, 153.0]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_mul_f64_avx512() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0];
        let b = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0];
        let result = unsafe { simd_mul_f64_avx512(&a, &b) };
        assert_eq!(result, vec![2.0, 6.0, 12.0, 20.0, 30.0, 42.0, 56.0, 72.0, 90.0, 110.0, 132.0, 156.0, 182.0, 210.0, 240.0, 272.0, 306.0]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_div_f64_avx512() {
        let a = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0];
        let b = vec![2.0, 4.0, 3.0, 8.0, 5.0, 6.0, 7.0, 10.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0];
        let result = unsafe { simd_div_f64_avx512(&a, &b) };
        assert_eq!(result, vec![5.0, 5.0, 10.0, 5.0, 10.0, 10.0, 10.0, 8.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_add_i64_avx512() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
        let b = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170];
        let result = unsafe { simd_add_i64_avx512(&a, &b) };
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88, 99, 110, 121, 132, 143, 154, 165, 176, 187]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_sub_i64_avx512() {
        let a = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170];
        let b = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
        let result = unsafe { simd_sub_i64_avx512(&a, &b) };
        assert_eq!(result, vec![9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99, 108, 117, 126, 135, 144, 153]);
    }

    #[test]
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    fn test_simd_mul_i64_avx512() {
        let a = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
        let b = vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18];
        let result = unsafe { simd_mul_i64_avx512(&a, &b) };
        assert_eq!(result, vec![2, 6, 12, 20, 30, 42, 56, 72, 90, 110, 132, 156, 182, 210, 240, 272, 306]);
    }
}
