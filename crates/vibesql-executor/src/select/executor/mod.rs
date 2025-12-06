//! SelectExecutor implementation split across modules
//!
//! The SelectExecutor is organized into separate implementation files:
//! - `builder` - Struct definition and constructor methods
//! - `execute` - Main execution entry points
//! - `columns` - Column name derivation
//! - `aggregation` - Aggregation and GROUP BY execution
//! - `nonagg` - Non-aggregation execution path
//! - `utils` - Utility methods for expression analysis
//! - `index_optimization` - Index-based optimizations for WHERE and ORDER BY
//! - `arena_execution` - Arena-based execution for zero-allocation prepared statements

mod aggregation;
mod arena_execution;
mod builder;
mod columnar_execution;
mod columns;
mod execute;
mod fast_path;
pub use fast_path::{is_simple_point_query, is_streaming_aggregate_query};
mod index_optimization;
mod nonagg;
mod utils;
pub(crate) mod validation;

#[cfg(test)]
mod memory_tests;

pub use builder::SelectExecutor;
