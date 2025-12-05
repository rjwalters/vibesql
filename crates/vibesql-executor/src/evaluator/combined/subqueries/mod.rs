//! Subquery evaluation for combined expressions
//!
//! This module provides subquery evaluation functionality split into focused submodules:
//! - correlation: Detecting and extracting correlated references
//! - schema_utils: Schema validation utilities
//! - scalar: Scalar subquery evaluation
//! - exists: EXISTS predicate evaluation
//! - quantified: ALL/ANY/SOME comparisons
//! - in_subquery: IN predicate evaluation with index optimization
//!
//! Note: Cache key computation is provided by the shared `crate::evaluator::caching` module.

// Utility modules (private)
mod correlation;
mod schema_utils;

// Evaluator modules (private - methods defined on CombinedExpressionEvaluator)
mod exists;
mod in_subquery;
mod quantified;
mod scalar;

// Re-export cache clearing function for benchmarks
pub use in_subquery::clear_in_subquery_cache;
