//! GROUP BY operations and aggregate function evaluation
//!
//! This module provides:
//! - Aggregate function accumulators (COUNT, SUM, AVG, MIN, MAX)
//! - Hash-based grouping implementation
//! - ROLLUP, CUBE, and GROUPING SETS expansion
//! - SQL value comparison and arithmetic helpers

mod aggregates;
mod grouping_sets;
mod hash;

// Re-export public API
pub(super) use aggregates::{AggregateAccumulator, compare_sql_values};
pub(super) use grouping_sets::{expand_group_by_clause, get_base_expressions, GroupingContext};
pub(super) use hash::group_rows;
