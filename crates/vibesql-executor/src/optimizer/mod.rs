//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Cost-based predicate ordering for optimal filter performance
//! - Centralized index planning and strategy selection
//! - Unified predicate classification and index strategy selection
//! - Subquery rewriting for IN predicate optimization
//! - Adaptive execution model selection (row-oriented vs columnar)

pub mod adaptive_execution;
mod expressions;
pub mod index_planner;
pub mod index_strategy;
pub mod predicate;
mod predicate_plan;
pub mod selectivity;
pub mod subquery_rewrite;
pub mod subquery_to_join;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use adaptive_execution::{choose_execution_model, ExecutionModel};
pub use expressions::*;
pub use predicate_plan::PredicatePlan;
pub use subquery_rewrite::rewrite_subquery_optimizations;
pub use subquery_to_join::transform_subqueries_to_joins;
pub use where_pushdown::{combine_with_and, PredicateDecomposition};
