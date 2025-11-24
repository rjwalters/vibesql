//! Join order optimization via time-bounded search
//!
//! This module implements join order optimization using breadth-first search with
//! a configurable time budget. Unlike greedy heuristics that commit to the first choice,
//! search explores multiple orderings and selects one that minimizes estimated cost.
//!
//! ## Time-Bounded Search (Default)
//!
//! By default, the optimizer uses time-bounded anytime search that:
//! - Explores join orders using parallel BFS (breadth-first)
//! - Terminates when time budget is exceeded (default: 1000ms)
//! - Returns the best ordering found so far (anytime property)
//! - Falls back to greedy heuristic if no complete ordering found
//!
//! This approach enables optimization for large queries (9+ tables) while preventing
//! excessive search time. Small queries complete exhaustively within milliseconds.
//!
//! ## Configuration
//!
//! Time budget and behavior can be configured via environment variables:
//! - `JOIN_REORDER_TIME_BUDGET_MS`: Override default time budget (default: 1000ms)
//! - `JOIN_REORDER_VERBOSE`: Enable search statistics logging
//! - `JOIN_REORDER_DISABLED`: Disable optimization entirely
//!
//! ## Algorithm
//!
//! Uses parallel breadth-first search with branch-and-bound pruning:
//! 1. Start with empty set of joined tables
//! 2. At each depth level, expand all candidate states in parallel
//! 3. Estimate cost of each join
//! 4. Prune states with cost exceeding current best (with threshold)
//! 5. Check time budget at each layer
//! 6. Continue until all tables joined or time budget exceeded
//!
//! ## Example
//!
//! ```text
//! Query: SELECT * FROM t1, t2, t3, t4, t5
//! WHERE t1.id = t2.id AND t2.id = t3.id ...
//!
//! BFS search by depth:
//! Depth 0: {}
//! Depth 1: {t1}, {t2}, {t3}, {t4}, {t5}  (5 states)
//! Depth 2: {t1,t2}, {t1,t3}, {t2,t3}, ... (pruned to keep best N states)
//! Depth 3: {t1,t2,t3}, ...                (pruned, check time budget)
//! ...
//!
//! Returns best complete ordering found, or falls back if time exceeded
//! ```

mod bfs;
mod config;
mod context;
mod cost;
mod dfs;
mod greedy;
mod optimizer;
mod state;

#[cfg(test)]
mod tests;

// Re-export public types
pub use config::ParallelSearchConfig;
pub use optimizer::JoinOrderSearch;
pub use state::JoinCost;

// Re-export from parent module for internal use
use super::reorder;
