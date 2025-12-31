// Module declarations
pub mod arena;
pub(crate) mod caching;
pub(crate) mod casting;
pub mod coercion;
mod combined;
mod combined_core;
pub(crate) mod compiled;
pub(crate) mod compiled_case;
pub(crate) mod compiled_pivot;
mod core;
pub mod date_format;
mod expression_hash;
mod expressions;
pub(crate) mod functions;
pub(crate) mod operators;
#[cfg(feature = "parallel")]
pub(crate) mod parallel;
#[cfg(not(feature = "parallel"))]
mod parallel;
pub(crate) mod pattern;
mod single;
mod subqueries_shared;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export public API
pub use core::{CombinedExpressionEvaluator, ExpressionEvaluator};
// Re-export values_are_equal for cross-type hash join comparisons
pub(crate) use core::values_are_equal;

pub use arena::ArenaExpressionEvaluator;
// Re-export cache clearing function for benchmarks
pub use combined::clear_in_subquery_cache;
// Re-export eval_unary_op for use in other modules
pub(crate) use expressions::operators::eval_unary_op;
