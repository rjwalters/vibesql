//! Join reordering optimization
//!
//! Provides cost-based join reordering for multi-table queries:
//! - Analyzes join conditions and WHERE predicates
//! - Uses exhaustive search with pruning to find optimal join order
//! - Minimizes intermediate result sizes
//!
//! This optimization is enabled by default for 3-8 table INNER/CROSS joins.
//! Disabled for 9+ tables to prevent excessive search time (9! = 362,880).
//! Can be disabled via JOIN_REORDER_DISABLED environment variable.

mod graph;
mod optimizer;
mod predicates;
mod utils;

// Re-export public API
pub(crate) use optimizer::execute_with_join_reordering;
pub(crate) use utils::{
    all_joins_are_cross,
    count_tables_in_from,
    should_apply_join_reordering,
};
