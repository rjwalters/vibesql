//! Analytical pattern detection

use vibesql_ast::SelectStmt;

use super::query::{
    has_aggregate_functions, has_arithmetic_expressions, has_group_by, has_selective_projection,
    has_window_functions, is_single_table,
};

/// Detect if a query has analytical patterns suitable for columnar execution
///
/// Returns true if the query benefits from columnar execution based on:
/// - Has aggregation (aggregate functions like SUM, AVG, COUNT)
/// - Has arithmetic expressions (price * quantity, price * (1 - discount))
/// - Single table only (no JOINs yet - Phase 5 limitation)
/// - Selective projection (few columns, not SELECT *)
/// - No window functions
/// - No DISTINCT
///
/// # Rationale
///
/// Columnar execution excels at:
/// - Aggregations: SIMD can process multiple values per instruction
/// - Arithmetic: Vectorized operations on packed column data
/// - Scans: Better cache locality when accessing few columns
/// - GROUP BY: Hash-based grouping with vectorized aggregation
///
/// Row-oriented is better for:
/// - Point lookups: Single row access patterns
/// - Wide projections: Need all columns anyway
/// - Complex joins: Tuple-at-a-time processing more flexible
///
/// # Phase 6 Capabilities
///
/// Current columnar execution supports:
/// - Aggregation with or without GROUP BY (e.g., SELECT SUM(price) FROM orders GROUP BY category)
/// - Single table scans (no JOINs)
/// - Simple predicates (=, <, >, <=, >=, BETWEEN, AND)
/// - Arithmetic expressions in aggregates (e.g., SUM(a * b))
///
/// NOT supported yet (TODO: Future phases):
/// - JOIN operations
/// - DISTINCT
/// - Window functions
pub(super) fn has_analytical_pattern(query: &SelectStmt) -> bool {
    // Phase 5 limitation: Single table only (no joins)
    if !is_single_table(query) {
        return false;
    }

    // No window functions
    if has_window_functions(query) {
        return false;
    }

    // No DISTINCT for now
    if query.distinct {
        return false;
    }

    let has_aggregation = has_aggregate_functions(query);
    let has_arithmetic = has_arithmetic_expressions(query);
    let selective_projection = has_selective_projection(query);
    let has_grouping = has_group_by(query);

    // Columnar execution is beneficial if:
    // 1. Has aggregation (aggregate functions like SUM/AVG/COUNT), AND
    // 2. Either has arithmetic OR selective projection OR GROUP BY
    //
    // This ensures we only use columnar for queries that benefit from:
    // - Aggregation: Columnar aggregates are much faster with SIMD
    // - Arithmetic: Vectorized operations on column data
    // - Selective columns: Avoid conversion overhead for wide rows
    // - GROUP BY: Hash-based grouping with efficient aggregate computation
    has_aggregation && (has_arithmetic || selective_projection || has_grouping)
}
