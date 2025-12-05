//! Join reordering optimization
//!
//! Provides cost-based join reordering for multi-table queries:
//! - Analyzes join conditions and WHERE predicates
//! - Uses time-bounded search to find optimal join order
//! - Minimizes intermediate result sizes
//!
//! This optimization uses time-bounded anytime search (default: 1000ms budget)
//! to enable optimization for queries of all sizes, including 9+ tables.
//! Simple queries complete exhaustively in <1ms, complex queries use full budget.
//! Can be configured via JOIN_REORDER_TIME_BUDGET_MS or disabled via JOIN_REORDER_DISABLED.

mod graph;
mod optimizer;
mod predicates;
mod utils;

// Re-export public API
pub(crate) use optimizer::{execute_with_join_reordering, execute_with_semi_join_reordering};
pub(crate) use utils::{all_joins_are_cross, count_tables_in_from, should_apply_join_reordering};
