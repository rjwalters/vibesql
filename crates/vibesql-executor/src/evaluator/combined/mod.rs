//! Expression evaluation for combined schemas (JOINs)
//!
//! This module implements expression evaluation for combined row contexts, organized into:
//! - `eval` - Main evaluation entry point and column references
//! - `predicates` - BETWEEN, LIKE, IN list, IS NULL
//! - `subqueries` - Subquery evaluation (scalar, IN, EXISTS, quantified)
//! - `special` - CASE expressions, CAST, and function calls
//!
//! The evaluator uses the shared binary operation logic from `core::ExpressionEvaluator`.
mod eval;
mod predicates;
mod special;
mod subqueries;

// Re-export cache clearing function for benchmarks
pub use subqueries::clear_in_subquery_cache;

// Note: The CombinedExpressionEvaluator struct is defined in core.rs
// This module only contains the implementation methods
