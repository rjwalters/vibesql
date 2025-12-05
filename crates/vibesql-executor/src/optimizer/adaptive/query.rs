//! Query characteristic analysis

use vibesql_ast::{FromClause, SelectItem, SelectStmt};

use super::expression::{contains_aggregate, contains_arithmetic, contains_window_function};

/// Check if query has GROUP BY clause
pub(super) fn has_group_by(query: &SelectStmt) -> bool {
    query.group_by.is_some()
}

/// Check if query contains aggregate functions (SUM, AVG, MIN, MAX, COUNT)
pub(super) fn has_aggregate_functions(query: &SelectStmt) -> bool {
    query.select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_aggregate(expr),
        SelectItem::Wildcard { .. } => false,
        SelectItem::QualifiedWildcard { .. } => false,
    })
}

/// Check if query contains arithmetic expressions in SELECT list or WHERE clause
///
/// Arithmetic expressions like `price * (1 - discount)` benefit from SIMD
/// vectorization in columnar execution.
pub(super) fn has_arithmetic_expressions(query: &SelectStmt) -> bool {
    // Check SELECT list for arithmetic
    let select_has_arithmetic = query.select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_arithmetic(expr),
        _ => false,
    });

    // Check WHERE clause for arithmetic
    let where_has_arithmetic = query.where_clause.as_ref().is_some_and(contains_arithmetic);

    select_has_arithmetic || where_has_arithmetic
}

/// Check if query has selective projection (few columns, not SELECT *)
///
/// Selective projections benefit more from columnar execution because:
/// - Only needed columns are loaded (projection pushdown)
/// - Less conversion overhead between row/columnar formats
///
/// Returns true if:
/// - Not SELECT * (wildcard)
/// - Projected columns <= 10 (arbitrary threshold)
pub(super) fn has_selective_projection(query: &SelectStmt) -> bool {
    // Count non-wildcard items
    let non_wildcard_count = query
        .select_list
        .iter()
        .filter(|item| {
            !matches!(item, SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. })
        })
        .count();

    // If there are any wildcards, projection is NOT selective
    let has_wildcard = query.select_list.iter().any(|item| {
        matches!(item, SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. })
    });

    if has_wildcard {
        return false;
    }

    // Selective if <= 10 columns
    // This threshold allows for moderate projections like TPC-H queries
    // while avoiding columnar overhead for very wide projections
    non_wildcard_count > 0 && non_wildcard_count <= 10
}

/// Check if query is a single table (no JOINs, no subqueries)
///
/// Phase 5 limitation: Columnar execution only supports single table scans.
/// JOINs and subqueries will be added in future phases.
pub(super) fn is_single_table(query: &SelectStmt) -> bool {
    match &query.from {
        Some(FromClause::Table { .. }) => true,
        Some(FromClause::Join { .. }) | Some(FromClause::Subquery { .. }) => false,
        None => false, // No FROM clause (e.g., SELECT 1)
    }
}

/// Check if query contains window functions
///
/// Window functions are not yet supported in columnar execution.
pub(super) fn has_window_functions(query: &SelectStmt) -> bool {
    query.select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => contains_window_function(expr),
        _ => false,
    })
}
