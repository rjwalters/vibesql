//! Aggregate operations for columnar data processing.
//!
//! This module provides vectorized aggregate operations (sum, min, max, count)
//! with both filtered and non-filtered variants using packed and boolean masks.
//!
//! # Module Organization
//!
//! - [`sum`] - SUM aggregate operations
//! - [`count`] - COUNT aggregate operations
//! - [`minmax`] - MIN/MAX aggregate operations
//! - [`product`] - Product/arithmetic operations (e.g., SUM(a * b))

mod count;
mod minmax;
mod product;
mod sum;

// Re-export all public functions for backwards compatibility

// SUM operations
pub use sum::{
    sum_f64, sum_f64_filtered, sum_f64_packed_filtered, sum_i64, sum_i64_filtered,
    sum_i64_packed_filtered,
};

// COUNT operations
pub use count::{count_filtered, count_packed_filtered};

// MIN/MAX operations
pub use minmax::{
    max_f64, max_f64_filtered, max_f64_packed_filtered, max_i64, max_i64_filtered,
    max_i64_packed_filtered, min_f64, min_f64_filtered, min_f64_packed_filtered, min_i64,
    min_i64_filtered, min_i64_packed_filtered,
};

// Product/arithmetic operations
pub use product::{
    sum_product_f64, sum_product_f64_filtered, sum_product_f64_filtered_masked,
    sum_product_f64_masked, sum_product_f64_packed_filtered,
    sum_product_f64_packed_filtered_masked,
};
