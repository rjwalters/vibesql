//! Helper functions for SELECT query execution

use indexmap::IndexSet;

/// Estimate the memory size of a result set in bytes
///
/// Used for memory limit tracking during query execution.
/// Samples a subset of rows to avoid O(n) overhead on large result sets.
pub(super) fn estimate_result_size(rows: &[vibesql_storage::Row]) -> usize {
    if rows.is_empty() {
        return 0;
    }

    // For small result sets, measure exactly
    if rows.len() <= 100 {
        return rows.iter().map(|r| r.estimated_size_bytes()).sum();
    }

    // For large result sets, sample and extrapolate
    // Sample first 10, middle 10, and last 10 rows
    let sample_size = 30;
    let step = rows.len() / sample_size;
    let sample_total: usize =
        rows.iter().step_by(step.max(1)).take(sample_size).map(|r| r.estimated_size_bytes()).sum();

    let avg_row_size = sample_total / sample_size.min(rows.len());
    avg_row_size * rows.len()
}

/// Apply DISTINCT to remove duplicate rows
///
/// Uses an IndexSet to track unique rows while preserving insertion order.
/// This ensures deterministic results that match SQLite's behavior.
/// This requires SqlValue to implement Hash and Eq, which we've implemented
/// with SQL semantics:
/// - NULL == NULL for grouping
/// - NaN == NaN for grouping
pub(super) fn apply_distinct(rows: Vec<vibesql_storage::Row>) -> Vec<vibesql_storage::Row> {
    let mut seen = IndexSet::new();
    let mut result = Vec::new();

    for row in rows {
        // Try to insert the row's values into the set
        // If insertion succeeds (wasn't already present), keep the row
        if seen.insert(row.values.clone()) {
            result.push(row);
        }
    }

    result
}

/// Apply LIMIT and OFFSET to a result set
pub(super) fn apply_limit_offset(
    rows: Vec<vibesql_storage::Row>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<vibesql_storage::Row> {
    let start = offset.unwrap_or(0);
    if start >= rows.len() {
        return Vec::new();
    }

    let max_take = rows.len() - start;
    let take = limit.unwrap_or(max_take).min(max_take);

    rows.into_iter().skip(start).take(take).collect()
}
