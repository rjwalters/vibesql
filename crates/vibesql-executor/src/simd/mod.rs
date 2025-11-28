//! SIMD-accelerated operations for columnar query execution
//!
//! This module provides auto-vectorized implementations of common database
//! operations. The functions are structured to enable LLVM auto-vectorization
//! without requiring explicit SIMD intrinsics.
//!
//! # Modules
//!
//! - `aggregation`: Masked aggregation functions for GROUP BY (SUM, COUNT, MIN, MAX)
//! - `dispatch`: Runtime CPU feature detection and SIMD dispatch

pub mod aggregation;
mod dispatch;

pub use dispatch::{dispatched, CpuFeatures, SimdLevel};
