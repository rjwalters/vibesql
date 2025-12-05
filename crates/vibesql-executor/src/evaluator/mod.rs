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
mod parallel;
pub(crate) mod pattern;
mod single;
mod subqueries_shared;
pub mod window;

#[cfg(test)]
mod tests;

// Re-export public API
pub use arena::ArenaExpressionEvaluator;
pub use core::{CombinedExpressionEvaluator, ExpressionEvaluator};
// Re-export eval_unary_op for use in other modules
pub(crate) use expressions::operators::eval_unary_op;
// Re-export cache clearing function for benchmarks
pub use combined::clear_in_subquery_cache;
