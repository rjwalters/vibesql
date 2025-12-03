//! Runtime CPU feature detection and SIMD dispatch
//!
//! This module provides runtime detection of CPU features and dispatch
//! of operations to the best available SIMD implementation.

/// Available SIMD levels for dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdLevel {
    /// No SIMD, scalar fallback
    Scalar,
    /// x86 SSE4.2
    Sse42,
    /// x86 AVX2
    Avx2,
    /// x86 AVX-512
    Avx512,
    /// ARM NEON
    Neon,
    /// ARM SVE
    Sve,
}

impl SimdLevel {
    /// Get a human-readable name for this SIMD level
    pub fn name(&self) -> &'static str {
        match self {
            SimdLevel::Scalar => "Scalar",
            SimdLevel::Sse42 => "SSE4.2",
            SimdLevel::Avx2 => "AVX2",
            SimdLevel::Avx512 => "AVX-512",
            SimdLevel::Neon => "NEON",
            SimdLevel::Sve => "SVE",
        }
    }

    /// Get the vector width in bits for this SIMD level
    pub fn vector_width_bits(&self) -> usize {
        match self {
            SimdLevel::Scalar => 64,
            SimdLevel::Sse42 => 128,
            SimdLevel::Avx2 => 256,
            SimdLevel::Avx512 => 512,
            SimdLevel::Neon => 128,
            SimdLevel::Sve => 128, // Variable, but 128 is minimum
        }
    }

    /// Get the number of f64 lanes for this SIMD level
    pub fn f64_lanes(&self) -> usize {
        self.vector_width_bits() / 64
    }

    /// Get the number of i64 lanes for this SIMD level
    pub fn i64_lanes(&self) -> usize {
        self.vector_width_bits() / 64
    }
}

/// Detected CPU features
#[derive(Debug, Clone)]
pub struct CpuFeatures {
    pub has_sse42: bool,
    pub has_avx2: bool,
    pub has_avx512f: bool,
    pub has_avx512dq: bool,
    pub has_neon: bool,
    pub has_sve: bool,
}

impl CpuFeatures {
    /// Detect CPU features at runtime
    pub fn get() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self {
                has_sse42: std::arch::is_x86_feature_detected!("sse4.2"),
                has_avx2: std::arch::is_x86_feature_detected!("avx2"),
                has_avx512f: std::arch::is_x86_feature_detected!("avx512f"),
                has_avx512dq: std::arch::is_x86_feature_detected!("avx512dq"),
                has_neon: false,
                has_sve: false,
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            Self {
                has_sse42: false,
                has_avx2: false,
                has_avx512f: false,
                has_avx512dq: false,
                has_neon: std::arch::is_aarch64_feature_detected!("neon"),
                has_sve: std::arch::is_aarch64_feature_detected!("sve"),
            }
        }

        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                has_sse42: false,
                has_avx2: false,
                has_avx512f: false,
                has_avx512dq: false,
                has_neon: false,
                has_sve: false,
            }
        }
    }

    /// Get the best available SIMD level for this CPU
    pub fn best_simd_level(&self) -> SimdLevel {
        if self.has_avx512f && self.has_avx512dq {
            SimdLevel::Avx512
        } else if self.has_avx2 {
            SimdLevel::Avx2
        } else if self.has_sse42 {
            SimdLevel::Sse42
        } else if self.has_sve {
            SimdLevel::Sve
        } else if self.has_neon {
            SimdLevel::Neon
        } else {
            SimdLevel::Scalar
        }
    }
}

/// Module containing dispatched SIMD operations
pub mod dispatched {
    use super::*;

    /// Compute element-wise sum of two f64 vectors
    pub fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        assert_eq!(a.len(), b.len());
        a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
    }

    /// Compute element-wise product of two f64 vectors
    pub fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
        assert_eq!(a.len(), b.len());
        a.iter().zip(b.iter()).map(|(x, y)| x * y).collect()
    }

    /// Sum all elements of a f64 vector
    pub fn sum_f64(values: &[f64]) -> f64 {
        // Use 4-accumulator pattern for LLVM auto-vectorization
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
        for value in &values[(chunks * 4)..] {
            sum += value;
        }
        sum
    }

    /// Get the currently active SIMD level
    pub fn active_level() -> SimdLevel {
        CpuFeatures::get().best_simd_level()
    }
}
