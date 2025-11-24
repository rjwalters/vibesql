//! Runtime CPU feature detection for SIMD dispatch
//!
//! This module provides runtime detection of CPU SIMD capabilities
//! to enable dynamic dispatch to the best available implementation.

use std::sync::OnceLock;

/// CPU features available for SIMD operations
#[derive(Debug, Clone, Copy)]
pub struct CpuFeatures {
    /// SSE4.2 support (x86_64, 128-bit, 2 doubles)
    pub has_sse42: bool,
    /// AVX2 support (x86_64, 256-bit, 4 doubles)
    pub has_avx2: bool,
    /// AVX-512F support (x86_64, 512-bit foundation)
    pub has_avx512f: bool,
    /// AVX-512DQ support (x86_64, double/quadword operations)
    pub has_avx512dq: bool,
    /// NEON support (ARM64, 128-bit, 2 doubles)
    pub has_neon: bool,
    /// SVE support (ARM64, variable width)
    pub has_sve: bool,
}

impl CpuFeatures {
    /// Detect CPU features at runtime
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self {
                has_sse42: is_x86_feature_detected!("sse4.2"),
                has_avx2: is_x86_feature_detected!("avx2"),
                has_avx512f: is_x86_feature_detected!("avx512f"),
                has_avx512dq: is_x86_feature_detected!("avx512dq"),
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
            // No SIMD support on this platform
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

    /// Get the best SIMD level available on this CPU
    pub fn best_simd_level(&self) -> SimdLevel {
        // x86_64 preference order: AVX-512 > AVX2 > SSE4.2
        if self.has_avx512f && self.has_avx512dq {
            SimdLevel::Avx512
        } else if self.has_avx2 {
            SimdLevel::Avx2
        } else if self.has_sse42 {
            SimdLevel::Sse42
        }
        // ARM64 preference order: SVE > NEON
        else if self.has_sve {
            SimdLevel::Sve
        } else if self.has_neon {
            SimdLevel::Neon
        } else {
            SimdLevel::Scalar
        }
    }

    /// Get global CPU features (cached)
    pub fn get() -> &'static CpuFeatures {
        static FEATURES: OnceLock<CpuFeatures> = OnceLock::new();
        FEATURES.get_or_init(CpuFeatures::detect)
    }
}

/// SIMD implementation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SimdLevel {
    /// No SIMD support (scalar fallback)
    Scalar,
    /// SSE4.2 (128-bit, 2 doubles)
    Sse42,
    /// AVX2 (256-bit, 4 doubles)
    Avx2,
    /// AVX-512 (512-bit, 8 doubles)
    Avx512,
    /// ARM NEON (128-bit, 2 doubles)
    Neon,
    /// ARM SVE (variable width)
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
            SimdLevel::Sve => 256, // Variable, but commonly 256
        }
    }

    /// Get the number of f64 elements that fit in a vector
    pub fn f64_lanes(&self) -> usize {
        self.vector_width_bits() / 64
    }

    /// Get the number of i64 elements that fit in a vector
    pub fn i64_lanes(&self) -> usize {
        self.vector_width_bits() / 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_feature_detection() {
        let features = CpuFeatures::detect();
        let level = features.best_simd_level();

        // Should detect at least one SIMD level on modern CPUs
        println!("Detected SIMD level: {:?}", level);
        println!("CPU features: {:?}", features);

        // Verify that we get consistent results
        let features2 = CpuFeatures::get();
        assert_eq!(features.has_avx2, features2.has_avx2);
        assert_eq!(features.has_avx512f, features2.has_avx512f);
    }

    #[test]
    fn test_simd_level_properties() {
        assert_eq!(SimdLevel::Scalar.f64_lanes(), 1);
        assert_eq!(SimdLevel::Sse42.f64_lanes(), 2);
        assert_eq!(SimdLevel::Avx2.f64_lanes(), 4);
        assert_eq!(SimdLevel::Avx512.f64_lanes(), 8);
        assert_eq!(SimdLevel::Neon.f64_lanes(), 2);

        assert_eq!(SimdLevel::Avx512.name(), "AVX-512");
        assert_eq!(SimdLevel::Avx2.name(), "AVX2");
    }

    #[test]
    fn test_simd_level_ordering() {
        assert!(SimdLevel::Avx512 > SimdLevel::Avx2);
        assert!(SimdLevel::Avx2 > SimdLevel::Sse42);
        assert!(SimdLevel::Sse42 > SimdLevel::Scalar);
    }
}
