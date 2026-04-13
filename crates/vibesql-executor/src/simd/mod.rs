//! SIMD-accelerated operations for columnar query execution
//!
//! This module provides both auto-vectorized and explicit SIMD implementations
//! of common database operations.
//!
//! # Modules
//!
//! - `aggregation`: Masked aggregation functions for GROUP BY (SUM, COUNT, MIN, MAX)
//! - `dispatch`: Runtime CPU feature detection and SIMD dispatch
//!
//! Explicit SIMD intrinsics live in `select::columnar::simd_ops::intrinsics` and
//! are wired into the aggregation functions automatically.

pub mod aggregation;
mod dispatch;

pub use dispatch::{dispatched, CpuFeatures, SimdLevel};
