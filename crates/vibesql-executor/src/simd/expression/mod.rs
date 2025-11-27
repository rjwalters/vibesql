//! SIMD-accelerated expression evaluation for general arithmetic expressions
//!
//! This module extends SIMD support beyond aggregates to general expression evaluation
//! in WHERE clauses, SELECT projections, ORDER BY, and other query contexts.
//!
//! # Overview
//!
//! Evaluates expressions in batch mode using SIMD operations when beneficial:
//! - Collects values from multiple rows into columnar buffers
//! - Applies SIMD arithmetic operations
//! - Converts results back to row-based format
//!
//! # When SIMD is Used
//!
//! SIMD path is chosen when:
//! 1. Row count >= SIMD_THRESHOLD (100 rows)
//! 2. Expression is simple arithmetic (+, -, *, /)
//! 3. All operands are numeric (Int64 or Float64)
//! 4. No complex sub-expressions (subqueries, aggregates, etc.)
//!
//! # Performance
//!
//! Expected improvements:
//! - 2-4x for expression-heavy queries (conservative)
//! - 5-10x for computation-dominated queries (optimistic)
//!
//! Overhead considerations:
//! - Row → columnar → row conversion has cost
//! - Only beneficial when computation cost exceeds conversion cost
//! - Threshold tuned to break-even point (~100-1000 rows)
//!
//! # Module Structure
//!
//! - [`analysis`]: Expression analysis to determine SIMD eligibility
//! - [`null_handling`]: NULL value detection for graceful fallback
//! - [`evaluation`]: Core SIMD evaluation logic and buffer management

#[cfg(feature = "simd")]
pub mod analysis;
#[cfg(feature = "simd")]
pub mod evaluation;
#[cfg(feature = "simd")]
pub mod null_handling;

/// Threshold for using SIMD expression evaluation
/// Below this, scalar evaluation is more efficient due to conversion overhead
pub const SIMD_THRESHOLD: usize = 100;

/// Maximum recursion depth for nested expressions
/// Prevents stack overflow on deeply nested binary operations
pub const MAX_RECURSION_DEPTH: usize = 32;

// Re-export public API
#[cfg(feature = "simd")]
pub use analysis::{can_use_simd_for_expression, extract_column_refs, is_simple_operand};
#[cfg(feature = "simd")]
pub use evaluation::{
    apply_simd_operation, convert_to_buffer, eval_expression_batch_simd, eval_expression_scalar,
    NumericBuffer, NumericValue,
};
#[cfg(feature = "simd")]
pub use null_handling::has_null_values;
