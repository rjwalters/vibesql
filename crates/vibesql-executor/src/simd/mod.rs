//! SIMD-accelerated operations for columnar data
//!
//! This module provides SIMD implementations of common database operations
//! for columnar storage formats. It achieves 4-8x speedup on arithmetic,
//! filtering, and aggregation operations compared to scalar implementations.
//!
//! # Features
//!
//! - **Arithmetic operations**: `+`, `-`, `*`, `/` for f64 columns; `+`, `-`, `*` for i64 columns
//! - **Comparison operations**: `<`, `<=`, `>`, `>=`, `=`, `!=`
//! - **Aggregations**: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`
//! - **NULL handling**: Bitmask-based NULL value support
//! - **Remainder handling**: Automatic fallback to scalar for non-aligned data
//!
//! # Usage
//!
//! Enable the `simd` feature to use these operations:
//!
//! ```toml
//! [dependencies]
//! vibesql-executor = { version = "0.1", features = ["simd"] }
//! ```
//!
//! # Examples
//!
//! ```rust,ignore
//! use vibesql_executor::simd::*;
//!
//! let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
//! let b = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0];
//!
//! // SIMD addition (4-6x faster than scalar)
//! let result = simd_add_f64(&a, &b);
//!
//! // SIMD filtering (4-8x faster than scalar)
//! let mask = simd_gt_f64(&a, 5.0);
//!
//! // SIMD aggregation (5-10x faster than scalar)
//! let sum = simd_sum_f64(&a);
//! ```
//!
//! # Platform Support
//!
//! SIMD operations use `wide` which supports:
//! - x86/x86_64 with SSE2, AVX, AVX2
//! - ARM with NEON
//! - WASM with SIMD proposal
//!
//! On platforms without SIMD support, operations fall back to scalar
//! implementations automatically.

#[cfg(feature = "simd")]
pub mod aggregation;
#[cfg(feature = "simd")]
pub mod arithmetic;
#[cfg(feature = "simd")]
pub mod comparison;
#[cfg(feature = "simd")]
pub mod null_handling;
#[cfg(feature = "simd")]
pub mod hashing;
#[cfg(feature = "simd")]
pub mod cpu_features;
#[cfg(feature = "simd")]
pub mod dispatch;

// Re-export public API
#[cfg(feature = "simd")]
pub use aggregation::*;
#[cfg(feature = "simd")]
pub use arithmetic::*;
#[cfg(feature = "simd")]
pub use comparison::*;
#[cfg(feature = "simd")]
pub use null_handling::*;
#[cfg(feature = "simd")]
pub use hashing::*;
#[cfg(feature = "simd")]
pub use cpu_features::*;
#[cfg(feature = "simd")]
pub use dispatch::*;
