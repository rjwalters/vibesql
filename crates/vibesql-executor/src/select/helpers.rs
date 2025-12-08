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
///
/// # Optimization
///
/// Checks containment BEFORE cloning to avoid unnecessary allocations.
/// Only clones row values when the row is actually unique and needs to be stored.
/// This reduces cloning from O(n) to O(unique rows), which is significant
/// when there are many duplicate values (common in DISTINCT queries).
pub(super) fn apply_distinct(rows: Vec<vibesql_storage::Row>) -> Vec<vibesql_storage::Row> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Track seen values - IndexSet maintains insertion order for deterministic results
    // Pre-allocate assuming ~50% unique rows as a reasonable default for DISTINCT queries
    let mut seen: IndexSet<vibesql_storage::RowValues> = IndexSet::with_capacity(rows.len() / 2);
    let mut result = Vec::with_capacity(rows.len() / 2);

    for row in rows {
        // Check containment first (no clone needed for lookup)
        // Only clone if the row is actually new and needs to be stored
        if !seen.contains(&row.values) {
            seen.insert(row.values.clone());
            result.push(row);
        }
        // Duplicates are skipped without any cloning
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

/// Apply LIMIT and OFFSET to an iterator without forcing full materialization
///
/// This is more efficient than `apply_limit_offset` when the input is a lazy iterator,
/// as it only materializes the rows that will actually be returned.
///
/// # Performance (Issue #4060)
///
/// For `SELECT * FROM t LIMIT 10` on a 10,000 row table:
/// - `apply_limit_offset(iter.collect(), Some(10), None)`: clones 10,000 rows, keeps 10
/// - `apply_limit_offset_iter(iter, Some(10), None)`: clones only 10 rows
///
/// # Arguments
///
/// * `iter` - Iterator over rows (can be lazy or materialized)
/// * `limit` - Maximum number of rows to return (None = unlimited)
/// * `offset` - Number of rows to skip from the start (None = 0)
#[allow(dead_code)]
pub(super) fn apply_limit_offset_iter<I>(
    iter: I,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<vibesql_storage::Row>
where
    I: Iterator<Item = vibesql_storage::Row>,
{
    let start = offset.unwrap_or(0);

    // Skip offset rows
    let iter = iter.skip(start);

    // Apply limit if specified
    match limit {
        Some(n) => iter.take(n).collect(),
        None => iter.collect(),
    }
}
