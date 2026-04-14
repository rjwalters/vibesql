//! Window Function Evaluator
//!
//! This module implements the core window function evaluation engine that:
//! - Partitions rows by PARTITION BY expressions
//! - Sorts partitions by ORDER BY clauses
//! - Calculates frame boundaries (ROWS mode)
//! - Evaluates window functions over frames
//!
//! # Module Organization
//!
//! The window function evaluator is split into logical modules:
//! - `partitioning` - Partition management and row grouping
//! - `sorting` - Partition sorting and value comparison
//! - `frames` - Frame boundary calculation (ROWS mode)
//! - `ranking` - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
//! - `aggregates` - Aggregate window functions (COUNT, SUM, AVG, MIN, MAX)
//! - `value` - Value access functions (LAG, LEAD)
//! - `utils` - Shared utility functions

mod aggregates;
mod frames;
mod partitioning;
mod ranking;
mod sorting;
mod utils;
mod value;

// Re-export public API
pub use aggregates::{
    evaluate_avg_window, evaluate_count_window, evaluate_group_concat_window,
    evaluate_group_concat_window_with_expr, evaluate_max_window, evaluate_min_window,
    evaluate_sum_window, evaluate_total_window,
};
pub use frames::{calculate_frame, calculate_frame_with_exclusion, validate_frame, FrameResult};
pub use partitioning::{partition_rows, Partition};
pub use ranking::{
    evaluate_cume_dist, evaluate_dense_rank, evaluate_ntile, evaluate_percent_rank, evaluate_rank,
    evaluate_row_number,
};
pub use sorting::{compare_values, sort_partition};
pub use value::{
    evaluate_lag, evaluate_lead,
};

#[cfg(test)]
mod tests;
