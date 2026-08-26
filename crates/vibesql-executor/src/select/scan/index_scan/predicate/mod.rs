//! Index predicate extraction
//!
//! Extracts range and IN predicates from WHERE clauses for index optimization.
//! Supports both column-based and expression-based (functional) indexes.
//!
//! # Module Organization
//!
//! - `range`: Range predicate extraction (>, <, >=, <=, BETWEEN, AND combinations)
//! - `in_list`: IN predicate extraction and main entry point for column-based indexes
//! - `composite`: Multi-column composite index predicate extraction
//! - `expression_index`: Expression-based (functional) index support
//!
//! # Public API
//!
//! The main entry points are:
//! - `extract_index_predicate()`: Extract predicate for single-column index
//! - `extract_index_predicate_for_indexed_column()`: Unified entry for any index column type
//! - `extract_composite_predicates_with_in()`: Extract predicates for composite indexes
//! - `where_clause_fully_satisfied_by_indexed_column()`: Check if index covers WHERE clause

use vibesql_types::SqlValue;

mod affinity_coercion;
mod composite;
mod expression_index;
mod in_list;
pub(super) mod range;
mod temporal_coercion;

// Re-export types
pub(crate) use composite::{CompositePredicateType, PrefixPredicateResult, PrefixWithRangeResult};

/// Range predicate information extracted from WHERE clause
#[derive(Debug)]
pub(crate) struct RangePredicate {
    pub start: Option<SqlValue>,
    pub end: Option<SqlValue>,
    pub inclusive_start: bool,
    pub inclusive_end: bool,
    /// Whether NULL values should be excluded from the range scan
    /// This is true for all SQL inequality comparisons (<, <=, >, >=, BETWEEN)
    /// since NULL comparisons in SQL return NULL (not true)
    pub exclude_nulls: bool,
}

/// Index predicate types that can be pushed down to storage layer
#[derive(Debug)]
pub(crate) enum IndexPredicate {
    /// Range scan with optional bounds (>, <, >=, <=, BETWEEN)
    Range(RangePredicate),
    /// Multi-value lookup (IN predicate)
    In(Vec<SqlValue>),
}

// Re-export composite index functions
// Test-only re-export (function kept for backward compatibility with tests)
// Re-export WHERE-clause literal affinity coercion (issue #6555)
pub(crate) use affinity_coercion::{
    coerce_composite_predicates_for_affinity, coerce_index_predicate_for_affinity,
    coerce_lookup_keys_for_affinity, coerce_prefix_result_for_affinity,
    coerce_prefix_with_range_result_for_affinity,
};
#[cfg(test)]
pub(crate) use composite::extract_composite_equality_predicates;
pub(crate) use composite::{
    build_residual_where_clause, extract_composite_predicates_with_in,
    extract_prefix_equality_predicates, extract_prefix_with_trailing_range,
    generate_composite_keys, where_clause_fully_satisfied_by_composite_key,
};
// Re-export expression index functions
pub(crate) use expression_index::{
    extract_index_predicate_for_indexed_column, where_clause_fully_satisfied_by_indexed_column,
};
// Re-export main extraction function
pub(crate) use in_list::{extract_index_predicate, where_clause_fully_satisfied_by_index};
// Re-export temporal probe-bound coercion (issue #5333)
pub(crate) use temporal_coercion::{
    coerce_equality_key, coerce_index_predicate_for_temporal_keys, EqualityKeyCoercion,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
