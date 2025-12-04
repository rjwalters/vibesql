//! Expression optimization module for query planning
//!
//! This module implements:
//! - Constant folding and dead code elimination for expressions
//! - WHERE clause predicate pushdown for efficient join evaluation
//! - Cost-based predicate ordering for optimal filter performance
//! - Centralized index planning and strategy selection
//! - Subquery rewriting for IN predicate optimization
//! - Adaptive execution model selection (row-oriented vs columnar)
//! - Aggregate-aware query optimization for GROUP BY/HAVING performance
//! - Unused table elimination for cross join optimization

pub mod adaptive;
pub mod aggregate_analysis;
mod expressions;
pub mod index_planner;
mod predicate_plan;
pub mod selectivity;
pub mod subquery_rewrite;
pub mod subquery_to_join;
pub mod table_elimination;
#[cfg(test)]
mod tests;
pub mod where_pushdown;

pub use expressions::*;
pub use predicate_plan::PredicatePlan;
pub use subquery_rewrite::rewrite_subquery_optimizations;
pub use subquery_to_join::transform_subqueries_to_joins;
pub use table_elimination::eliminate_unused_tables;
pub use where_pushdown::combine_with_and;
