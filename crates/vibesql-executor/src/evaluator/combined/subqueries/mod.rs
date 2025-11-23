//! Subquery evaluation for combined expressions
//!
//! This module provides subquery evaluation functionality split into focused submodules:
//! - cache: Cache key computation for performance
//! - correlation: Detecting and extracting correlated references
//! - schema_utils: Schema validation utilities
//! - scalar: Scalar subquery evaluation
//! - exists: EXISTS predicate evaluation
//! - quantified: ALL/ANY/SOME comparisons
//! - in_subquery: IN predicate evaluation with index optimization

// Utility modules (private)
mod cache;
mod correlation;
mod schema_utils;

// Evaluator modules (private - methods defined on CombinedExpressionEvaluator)
mod scalar;
mod exists;
mod quantified;
mod in_subquery;
