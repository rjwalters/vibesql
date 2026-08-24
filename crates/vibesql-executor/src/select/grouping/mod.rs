//! GROUP BY operations and aggregate function evaluation
//!
//! This module provides:
//! - Aggregate function accumulators (COUNT, SUM, AVG, MIN, MAX)
//! - Hash-based grouping implementation
//! - ROLLUP, CUBE, and GROUPING SETS expansion
//! - SQL value comparison and arithmetic helpers
//! - Specialized GROUP BY key types for efficient hashing

mod aggregates;
mod grouping_sets;
mod hash;
mod keys;

// Re-export public API
pub(crate) use aggregates::{
    compare_sql_values, compare_sql_values_with_collation, welford_finalize, welford_update,
    AggregateAccumulator,
};
pub(crate) use grouping_sets::expressions_equal;
pub(super) use grouping_sets::{
    expand_group_by_clause, get_base_expressions, resolve_base_expressions_aliases,
    resolve_group_by_alias, resolve_grouping_set_aliases, resolve_having_aliases_with_values,
    GroupingContext,
};
pub(super) use hash::group_rows;
pub(crate) use keys::{GroupKey, GroupKeySpec};
