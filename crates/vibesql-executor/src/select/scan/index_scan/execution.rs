//! Index scan execution
//!
//! Executes index scans to retrieve rows from tables using indexes.

use std::collections::HashMap;

use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};

use super::predicate::{
    build_residual_where_clause, coerce_index_predicate_for_temporal_keys,
    extract_composite_predicates_with_in, extract_index_predicate_for_indexed_column,
    extract_prefix_equality_predicates, extract_prefix_with_trailing_range,
    generate_composite_keys, where_clause_fully_satisfied_by_composite_key,
    where_clause_fully_satisfied_by_indexed_column, CompositePredicateType, IndexPredicate,
    PrefixPredicateResult, PrefixWithRangeResult,
};
use crate::{
    errors::ExecutorError, optimizer::PredicatePlan, schema::CombinedSchema, select::cte::CteResult,
};

/// Issue #5823: an index probe looks up raw (BINARY-ordered) key bytes, but a
/// column declared with a non-BINARY collation (e.g. NOCASE) must match per
/// that collation (`'XYZ'` must find stored `'xyz'`). Probing raw literals
/// against such a column silently loses rows, and the WHERE post-filter can
/// only remove rows, never restore missed ones. Return true when `col_name`
/// resolves to a column with a declared non-BINARY collation, so callers can
/// decline the probe and fall back to the full-index-scan + collation-aware
/// WHERE-filter path (correct, just slower). BINARY/undeclared collations keep
/// the fast probe.
fn column_has_nonbinary_collation(schema: &vibesql_catalog::TableSchema, col_name: &str) -> bool {
    schema
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(col_name))
        .and_then(|c| c.collation.as_deref())
        .is_some_and(|coll| !coll.eq_ignore_ascii_case("binary"))
}

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy with optimized range scans.
///
/// If sorted_columns is provided, the function preserves index order and returns results
/// marked as pre-sorted, allowing the caller to skip ORDER BY sorting.
///
/// If limit is provided AND sorted_columns indicates the index satisfies ORDER BY,
/// the scan will stop early after fetching enough rows, avoiding the cost of
/// fetching all matching rows just to apply LIMIT later.
///
/// # Performance Optimization
/// When the WHERE clause can be fully satisfied by the index predicate (e.g., simple
/// predicates like `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`), we skip redundant
/// WHERE clause re-evaluation, significantly improving performance for large result sets.
///
/// For ORDER BY with LIMIT queries:
/// - Without pushdown: Fetch 30 rows, reverse, take 1 = O(30)
/// - With pushdown: Scan from end, stop after 1 = O(1)
///
/// # Arguments
/// * `cte_results` - CTE context for IN subqueries that may reference CTEs (Issue #3562)
#[allow(private_interfaces)]
pub(crate) fn execute_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&Expression>,
    sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    limit: Option<usize>,
    database: &Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<super::super::FromResult, ExecutorError> {
    // Get table and index
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let index_metadata = database
        .get_index(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // Determine if this is a multi-column index
    let is_multi_column_index = index_metadata.columns.len() > 1;

    // Get the first indexed column (for single-column predicate extraction fallback)
    let first_indexed_column = index_metadata.columns.first();

    // Check if this is an expression index (first column is an expression, not a column name)
    let is_expression_index = first_indexed_column.map(|col| col.is_expression()).unwrap_or(false);

    // Get column names for the index (in order) - only for column-based indexes
    // For expression indexes, we cannot use composite key lookups (requires column names)
    let index_column_names: Vec<&str> = if is_expression_index {
        // Expression indexes don't have simple column names for composite lookups
        vec![]
    } else {
        index_metadata.columns.iter().filter_map(|col| col.column_name()).collect()
    };

    // Try composite key lookup first (for multi-column indexes with full predicates)
    // This handles queries like:
    //   WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 42 (all equality)
    //   WHERE c_w_id IN (1, 2) AND c_d_id = 5 (mixed equality + IN)
    let composite_predicates = if is_multi_column_index {
        where_clause
            .and_then(|expr| extract_composite_predicates_with_in(expr, &index_column_names))
    } else {
        None
    };

    // Issue #5823: a composite-key probe compares raw (BINARY-ordered) key
    // bytes, so decline it when ANY covered index column has a non-BINARY
    // collation — fall back to the collation-aware WHERE path below.
    // `extract_composite_predicates_with_in` only succeeds when EVERY index
    // column is covered, so checking all index columns is exact here.
    let composite_predicates = composite_predicates.filter(|_| {
        !index_column_names.iter().any(|col| column_has_nonbinary_collation(&table.schema, col))
    });

    // Generate composite keys (handles both single key and multiple keys for IN predicates)
    let composite_keys: Option<Vec<Vec<vibesql_types::SqlValue>>> =
        composite_predicates.as_ref().map(|preds| generate_composite_keys(preds));

    // Check if we have any IN predicates (affects lookup strategy)
    let has_in_predicate = composite_predicates
        .as_ref()
        .map(|preds| preds.iter().any(|p| matches!(p, CompositePredicateType::In(_))))
        .unwrap_or(false);

    // Determine if we can use composite key point lookup
    let use_composite_lookup = composite_keys.as_ref().map(|k| !k.is_empty()).unwrap_or(false);

    // Try prefix + trailing range lookup first (for queries like WHERE s_w_id = 1 AND s_quantity <
    // 10) This is more efficient than prefix-only lookup because it bounds the scan
    let prefix_with_range_result: Option<PrefixWithRangeResult> = if !use_composite_lookup
        && is_multi_column_index
    {
        where_clause.and_then(|expr| extract_prefix_with_trailing_range(expr, &index_column_names))
    } else {
        None
    };

    // Issue #5823: decline the prefix+range probe when any covered column has a
    // non-BINARY collation (a raw-key probe would lose collated rows). The
    // `covered_columns` set is uppercase; `column_has_nonbinary_collation`
    // matches case-insensitively.
    let prefix_with_range_result = prefix_with_range_result.filter(|r| {
        !r.covered_columns.iter().any(|col| column_has_nonbinary_collation(&table.schema, col))
    });

    let use_prefix_bounded_lookup = prefix_with_range_result.is_some();

    // Try prefix lookup if full composite key not available (for partial prefix matches)
    // This handles queries like: WHERE c_w_id = 1 AND c_d_id = 2 AND c_balance > 100
    // where only c_w_id and c_d_id are in the index
    let prefix_result: Option<PrefixPredicateResult> =
        if !use_composite_lookup && !use_prefix_bounded_lookup && is_multi_column_index {
            where_clause
                .and_then(|expr| extract_prefix_equality_predicates(expr, &index_column_names))
        } else {
            None
        };

    // Issue #5823: decline the prefix-equality probe when any covered column
    // has a non-BINARY collation (raw-key probe would lose collated rows).
    let prefix_result = prefix_result.filter(|r| {
        !r.covered_columns.iter().any(|col| column_has_nonbinary_collation(&table.schema, col))
    });

    // Check if we're using prefix lookup (partial composite key match)
    let use_prefix_lookup =
        prefix_result.is_some() && !use_composite_lookup && !use_prefix_bounded_lookup;

    // Fall back to single-column predicate extraction if neither composite nor prefix available
    let index_predicate = if use_composite_lookup || use_prefix_lookup || use_prefix_bounded_lookup
    {
        None // Don't need single-column predicate - using composite/prefix key
    } else {
        // Use the unified function that handles both column and expression indexes
        first_indexed_column.and_then(|idx_col| {
            where_clause.and_then(|expr| extract_index_predicate_for_indexed_column(expr, idx_col))
        })
    };

    // Issue #5806 / #5823: an equality/range/IN-list probe looks up the raw
    // literal values in the index, but the index stores raw (BINARY-ordered)
    // keys while a predicate on a non-BINARY-collated column (e.g. NOCASE) must
    // match per the column collation ('XYZ' must find stored 'xyz'). Such a
    // probe silently loses rows, and the WHERE post-filter can only remove
    // rows, never restore missed ones. Decline the probe so we fall back to
    // the full-index-scan + collation-aware WHERE-filter path below (correct,
    // just slower). #5806 first added this gate for `IndexPredicate::In`;
    // #5823 extends it to `IndexPredicate::Range` (which backs `=`, `<`, `<=`,
    // `>`, `>=`, BETWEEN). BINARY/undeclared collations keep the fast probe.
    let index_predicate = if matches!(
        index_predicate,
        Some(IndexPredicate::In(_)) | Some(IndexPredicate::Range(_))
    ) && first_indexed_column
        .and_then(|idx_col| idx_col.column_name())
        .is_some_and(|col_name| column_has_nonbinary_collation(&table.schema, col_name))
    {
        None
    } else {
        index_predicate
    };

    // Issue #5333: when the index stores temporal keys (e.g. an expression
    // index on date()/datetime(), or a plain TIMESTAMP column index) but the
    // WHERE clause supplies string bounds, coerce the bounds to the stored
    // key type so the probe matches executor comparison semantics. Without
    // this, type-tag ordering makes equality/upper-bounded probes silently
    // lose rows and lower-bounded probes over-return. If no faithful
    // coercion exists the predicate is dropped, falling back to the
    // full-index-scan + WHERE-filter path below (correct, just slower).
    let index_predicate = coerce_index_predicate_for_temporal_keys(index_predicate, index_data);

    // Build residual WHERE clause for prefix lookups
    // This contains only the predicates NOT covered by the index prefix
    let residual_where = if let Some(ref prefix_range) = prefix_with_range_result {
        // Prefix + range lookup - use covered_columns from the prefix+range result
        if let Some(where_expr) = where_clause {
            build_residual_where_clause(where_expr, &prefix_range.covered_columns)
        } else {
            None
        }
    } else if let Some(ref prefix) = prefix_result {
        if let Some(where_expr) = where_clause {
            build_residual_where_clause(where_expr, &prefix.covered_columns)
        } else {
            None
        }
    } else {
        None
    };

    // Performance optimization: Determine if WHERE filtering can be skipped
    // Check if the index predicate fully satisfies the WHERE clause
    let (need_where_filter, effective_where) = if use_composite_lookup {
        // Composite key lookup (with or without IN) - check if WHERE is fully satisfied
        match where_clause {
            Some(where_expr) => {
                let satisfied =
                    where_clause_fully_satisfied_by_composite_key(where_expr, &index_column_names);
                if satisfied {
                    (false, None)
                } else {
                    (true, Some((*where_expr).clone()))
                }
            }
            None => (false, None),
        }
    } else if use_prefix_bounded_lookup {
        // Prefix + range lookup - apply only residual WHERE clause
        match &residual_where {
            Some(residual) => (true, Some(residual.clone())), // Apply residual only
            None => (false, None), // All predicates covered by prefix+range - skip filtering
        }
    } else if use_prefix_lookup {
        // Prefix lookup - apply only residual WHERE clause
        match &residual_where {
            Some(residual) => (true, Some(residual.clone())), // Apply residual only
            None => (false, None),                            /* All predicates covered by
                                                                * prefix - skip filtering */
        }
    } else {
        match (&where_clause, &index_predicate, first_indexed_column) {
            (Some(where_expr), Some(_), Some(idx_col)) => {
                // Only skip WHERE filtering if we're certain the index handles everything
                // Use the unified function that handles both column and expression indexes
                let need_filter = !where_clause_fully_satisfied_by_indexed_column(
                    where_expr,
                    idx_col,
                    &index_predicate,
                );
                (need_filter, if need_filter { Some((*where_expr).clone()) } else { None })
            }
            (Some(where_expr), None, _) => (true, Some((*where_expr).clone())), /* WHERE present but no index predicate extracted */
            (Some(where_expr), Some(_), None) => (true, Some((*where_expr).clone())), /* No indexed column found */
            (None, _, _) => (false, None), // No WHERE clause
        }
    };

    // ==========================================================================
    // Streaming Fast Path for Simple Range Scans (#3781)
    // ==========================================================================
    //
    // For simple range queries without ORDER BY, LIMIT, or post-filtering,
    // we can use streaming to avoid materializing all row indices into a Vec.
    // This is critical for queries like:
    //   SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?
    //
    // Conditions for streaming:
    // - Single-column index with range predicate (not composite key, prefix, or IN)
    // - No WHERE post-filtering needed (index fully satisfies predicate)
    // - No ORDER BY (sorted_columns is None)
    // - No LIMIT (limit is None) - streaming doesn't help much with LIMIT
    //
    // Performance: Avoids O(k) Vec allocation and sorting, processes rows on-demand.
    let can_use_streaming = !use_composite_lookup
        && !use_prefix_lookup
        && !use_prefix_bounded_lookup
        && !need_where_filter
        && sorted_columns.is_none()
        && limit.is_none()
        && matches!(&index_predicate, Some(IndexPredicate::Range(_)));

    // Phase 1d follow-up of #5136 (issue #5204): MVCC snapshot for visibility
    // filtering on index-scan paths. With the `mvcc_enabled` feature OFF this
    // is the same is-not-bitmap-deleted check used today.
    let snapshot = crate::mvcc::read_snapshot(database);

    if can_use_streaming {
        if let Some(IndexPredicate::Range(range)) = &index_predicate {
            // Try streaming range scan
            if let Some(streaming_iter) = index_data.range_scan_streaming(
                range.start.as_ref(),
                range.end.as_ref(),
                range.inclusive_start,
                range.inclusive_end,
            ) {
                // Stream directly: iterate indices → lookup rows → clone
                // This avoids:
                // - Allocating Vec<usize> for all matching indices
                // - Sorting the indices (not needed without ORDER BY)

                // Get column index for NULL filtering if needed
                // SQL semantics: NULL < X returns NULL (not true), so NULLs shouldn't match
                // Note: For expression indexes, we can't easily determine the column to filter
                let null_filter_col_idx = if range.exclude_nulls {
                    first_indexed_column.and_then(|idx_col| idx_col.column_name()).and_then(
                        |col_name| {
                            table
                                .schema
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(col_name))
                        },
                    )
                } else {
                    None
                };

                // Profiling: Measure time spent in each phase when RANGE_SCAN_PROFILE=1
                let profile = std::env::var("RANGE_SCAN_PROFILE").is_ok();

                let rows: Vec<Row> = if profile {
                    use std::time::Instant;
                    let mut index_time = std::time::Duration::ZERO;
                    let mut lookup_time = std::time::Duration::ZERO;
                    let mut clone_time = std::time::Duration::ZERO;
                    let mut rows = Vec::new();
                    let mut row_count = 0usize;
                    let mut streaming_iter = streaming_iter;

                    loop {
                        let t0 = Instant::now();
                        let idx = streaming_iter.next();
                        index_time += t0.elapsed();

                        let Some(idx) = idx else { break };

                        let t1 = Instant::now();
                        // Issue #5204: MVCC visibility — skip rows invisible to
                        // the current snapshot (or deleted via the bitmap).
                        if !table.is_row_visible(idx, &snapshot) {
                            lookup_time += t1.elapsed();
                            continue;
                        }
                        if let Some(row_ref) = table.get_row(idx) {
                            lookup_time += t1.elapsed();

                            // Skip rows with NULL in indexed column (SQL semantics)
                            if let Some(col_idx) = null_filter_col_idx {
                                if matches!(
                                    row_ref.values.get(col_idx),
                                    Some(vibesql_types::SqlValue::Null)
                                ) {
                                    continue;
                                }
                            }

                            let t2 = Instant::now();
                            // Issue #4954: Set row_id when cloning for rowid support
                            // Issue #5517: an explicitly relocated rowid
                            // (`UPDATE ... SET rowid=`) is stored in row.row_id;
                            // only synthesize physical-index+1 when absent so an
                            // index scan reports the relocated rowid, not the slot.
                            let mut cloned = row_ref.clone();
                            if cloned.row_id.is_none() {
                                cloned.set_row_id((idx + 1) as u64);
                            }
                            rows.push(cloned);
                            clone_time += t2.elapsed();

                            row_count += 1;
                        } else {
                            lookup_time += t1.elapsed();
                        }
                    }

                    // Only print summary at end to avoid per-row overhead
                    static PROFILE_COUNT: std::sync::atomic::AtomicUsize =
                        std::sync::atomic::AtomicUsize::new(0);
                    let count = PROFILE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count.is_multiple_of(1000) {
                        eprintln!(
                            "[RangeScan Profile] rows={}, index={:?}, lookup={:?}, clone={:?}",
                            row_count, index_time, lookup_time, clone_time
                        );
                    }

                    rows
                } else {
                    // Issue #4954: Set row_id when cloning for rowid support
                    // Issue #5204: filter rows invisible to the MVCC snapshot
                    // (off-state: this is the existing not-bitmap-deleted check).
                    streaming_iter
                        .filter(|idx| table.is_row_visible(*idx, &snapshot))
                        .filter_map(|idx| table.get_row(idx).map(|row| (idx, row)))
                        .filter(|(_, row)| {
                            // Skip rows with NULL in indexed column (SQL semantics)
                            if let Some(col_idx) = null_filter_col_idx {
                                !matches!(
                                    row.values.get(col_idx),
                                    Some(vibesql_types::SqlValue::Null)
                                )
                            } else {
                                true
                            }
                        })
                        .map(|(idx, row)| {
                            // Issue #5517: honor an explicitly relocated rowid.
                            let mut cloned = row.clone();
                            if cloned.row_id.is_none() {
                                cloned.set_row_id((idx + 1) as u64);
                            }
                            cloned
                        })
                        .collect()
                };
                // sqlite_search_count: Track rows examined during streaming index scan
                database.increment_search_count(rows.len() as u64);

                // Build schema and return result
                let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
                let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

                // Mark as WHERE-filtered since index fully satisfied the predicate
                return Ok(super::super::FromResult::from_rows_where_filtered(schema, rows, None));
            }
        }
    }
    // ==========================================================================
    // End Streaming Fast Path
    // ==========================================================================

    // Track if we used reverse iteration (to skip manual reversal later)
    let mut used_reverse_iteration = false;

    // Get row indices using the appropriate index operation
    let matching_row_indices: Vec<usize> = if let Some(ref keys) = composite_keys {
        if keys.is_empty() {
            vec![]
        } else if keys.len() == 1 && !has_in_predicate {
            // Single composite key - O(log n) exact match
            // This is the fast path for multi-column equality predicates
            index_data.get(&keys[0]).unwrap_or_default()
        } else {
            // Multiple composite keys (from IN predicates) - do multiple lookups
            let mut all_indices = Vec::new();
            for key in keys {
                let indices = index_data.get(key).unwrap_or_default();
                all_indices.extend(indices);
            }
            // Deduplicate (in case the same row matches multiple keys)
            all_indices.sort_unstable();
            all_indices.dedup();
            all_indices
        }
    } else if let Some(ref prefix_range) = prefix_with_range_result {
        // Prefix + range lookup - O(log n + k) where k is matching rows
        // This is the most efficient path for queries like:
        // - WHERE s_w_id = 1 AND s_quantity < 10
        // - WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id >= 2981 AND ol_o_id < 3001
        // It uses both lower and upper bounds to minimize rows scanned
        index_data.prefix_range_scan(
            &prefix_range.prefix_key,
            prefix_range.lower_bound.as_ref(),
            prefix_range.inclusive_lower,
            prefix_range.upper_bound.as_ref(),
            prefix_range.inclusive_upper,
        )
    } else if let Some(ref prefix) = prefix_result {
        // Prefix key lookup - O(log n + k) where k is matching rows
        // This handles partial composite key matches
        // Check if DESC order is requested - if so, use reverse iteration for efficiency
        let needs_desc_order = sorted_columns
            .as_ref()
            .and_then(|cols| cols.first())
            .map(|(_, dir)| *dir == vibesql_ast::OrderDirection::Desc)
            .unwrap_or(false);

        if needs_desc_order {
            // Use reverse iteration - rows come in descending key order
            // This is more efficient than fetching all and reversing
            used_reverse_iteration = true;

            // Optimization: Use prefix_scan_reverse_limit for true early termination (#3285)
            // When we have DESC order + LIMIT + no WHERE filtering needed,
            // stop scanning at the index level instead of fetching all rows
            if let (Some(limit_val), false) = (limit, need_where_filter) {
                // O(log n + limit) instead of O(log n + k) where k = all matching rows
                // Critical for TPC-C Order Status: customer may have 30+ orders, we only need 1
                index_data.prefix_scan_reverse_limit(&prefix.prefix_key, limit_val)
            } else {
                index_data.prefix_scan_reverse(&prefix.prefix_key)
            }
        } else {
            index_data.prefix_scan(&prefix.prefix_key)
        }
    } else {
        match index_predicate {
            Some(IndexPredicate::Range(ref range)) => {
                // Use storage layer's optimized range_scan for >, <, >=, <=, BETWEEN
                // The storage layer handles empty/inverted range validation efficiently
                //
                // Optimization: Use range_scan_limit when LIMIT is provided and no post-filter
                // needed This enables early termination at the index level for
                // simple LIMIT queries (#3638) Example: SELECT c FROM t WHERE id
                // BETWEEN 1 AND 100 LIMIT 10
                //   - Without: Fetch all 100 rows, then take first 10
                //   - With: Stop scanning after 10 rows
                let use_limit_optimization =
                    limit.is_some() && !need_where_filter && sorted_columns.is_none();

                if use_limit_optimization {
                    index_data.range_scan_limit(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                        limit,
                    )
                } else {
                    index_data.range_scan(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                    )
                }
            }
            Some(IndexPredicate::In(ref values)) => {
                // For multi-column indexes, use prefix matching to find all rows
                // where the first column matches any of the IN values
                if is_multi_column_index {
                    // Use prefix_multi_lookup which performs range scans to match
                    // partial keys (e.g., [10] matches [10, 20], [10, 30], etc.)
                    index_data.prefix_multi_lookup(values)
                } else {
                    // For single-column indexes, use regular exact match lookup
                    index_data.multi_lookup(values)
                }
            }
            None => {
                // Full index scan - collect all row indices from the index in index key order
                // (Will be sorted by row index later if needed, see lines 425-427)
                // Note: values() now returns owned Vec<usize>, so no need for .copied()
                index_data.values().flatten().collect()
            }
        }
    };

    // If we're not returning sorted results, ensure rows are in table order (by row index)
    // This is important when the index doesn't satisfy the ORDER BY clause.
    // Without this, rows would be returned in index key order, which would cause
    // incorrect results when ORDER BY specifies a different column.
    let mut matching_row_indices = matching_row_indices;
    if sorted_columns.is_none() {
        matching_row_indices.sort_unstable();
    }

    // LIMIT pushdown optimization for ORDER BY queries (#3253)
    //
    // When ORDER BY is satisfied by the index AND no post-filtering is needed,
    // we can apply LIMIT early by:
    // 1. For DESC: reverse indices and take first N
    // 2. For ASC: just take first N
    //
    // This transforms ORDER BY ... LIMIT N from O(all_matching_rows) to O(N).
    // Critical for TPC-C Order-Status where a customer may have 30+ orders but
    // we only need the most recent one.
    //
    // Example: SELECT o_id FROM orders WHERE o_w_id=1 AND o_d_id=2 AND o_c_id=3
    //          ORDER BY o_id DESC LIMIT 1
    // - Before: Fetch all 30 orders, reverse, take 1
    // - After: Reverse indices, take 1, fetch just 1 row
    let limit_already_applied =
        if let (Some(sorted_cols), Some(limit_val)) = (&sorted_columns, limit) {
            if need_where_filter {
                false
            } else {
                let is_desc = sorted_cols
                    .first()
                    .is_some_and(|(_, dir)| *dir == vibesql_ast::OrderDirection::Desc);

                if is_desc {
                    // For DESC: reverse and take first N
                    matching_row_indices.reverse();
                    matching_row_indices.truncate(limit_val);
                    true // We already handled the reverse
                } else {
                    // For ASC: just take first N
                    matching_row_indices.truncate(limit_val);
                    false // ASC doesn't need reverse tracking
                }
            }
        } else {
            false
        };

    // Build schema early (needed for WHERE filtering)
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Get column index for NULL filtering if we have a range predicate with exclude_nulls
    // SQL semantics: NULL < X returns NULL (not true), so NULLs shouldn't match range predicates
    // Note: For expression indexes, we can't easily determine the column to filter
    let null_filter_col_idx = if let Some(IndexPredicate::Range(ref range)) = index_predicate {
        if range.exclude_nulls {
            first_indexed_column.and_then(|idx_col| idx_col.column_name()).and_then(|col_name| {
                table.schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
            })
        } else {
            None
        }
    } else {
        None
    };

    // Zero-copy optimization: Work with row references until the final step
    // This avoids cloning rows that will be filtered out by the WHERE clause
    // Issue #3790: Use get_row() which returns None for deleted rows
    // Issue #5204: also apply MVCC visibility to index-scan output rows.
    //   With `mvcc_enabled` OFF, `is_row_visible` reduces to the same
    //   is-not-bitmap-deleted check the existing `get_row` filter already
    //   performs, so this is behavior-preserving in the off-state.
    //
    // Issue #4954: Track row indices alongside row references so we can set row_id
    // when cloning. Row indices (0-based) become rowids (1-based) for SQLite compatibility.
    // We create a mapping from row pointer to index, then look up when cloning.
    let indexed_row_refs: Vec<(usize, &Row)> = matching_row_indices
        .iter()
        .filter(|idx| table.is_row_visible(**idx, &snapshot))
        .filter_map(|idx| table.get_row(*idx).map(|row| (*idx, row)))
        .filter(|(_, row)| {
            // Skip rows with NULL in indexed column (SQL semantics for range predicates)
            if let Some(col_idx) = null_filter_col_idx {
                !matches!(row.values.get(col_idx), Some(vibesql_types::SqlValue::Null))
            } else {
                true
            }
        })
        .collect();

    // Create mapping from row pointer address to row index for rowid preservation
    // This allows us to recover the row index after WHERE filtering
    let row_ptr_to_idx: std::collections::HashMap<usize, usize> =
        indexed_row_refs.iter().map(|(idx, row)| (*row as *const Row as usize, *idx)).collect();

    // Extract just the row references for filtering (preserving indices via the mapping)
    let row_refs: Vec<&Row> = indexed_row_refs.into_iter().map(|(_, row)| row).collect();
    // sqlite_search_count: Track rows examined during index scan
    database.increment_search_count(row_refs.len() as u64);

    // Apply WHERE clause predicates if needed (zero-copy filtering)
    // Performance optimization: Skip WHERE clause evaluation if the index already
    // guarantees all rows satisfy the predicate (e.g., simple predicates like
    // `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`).
    //
    // For prefix lookups, we only apply the residual WHERE clause (uncovered predicates).
    //
    // We still need to filter when:
    // - Predicates involve non-indexed columns
    // - Complex predicates that couldn't be fully pushed to index
    // - OR predicates (not yet optimized for index pushdown)
    // - Multi-column predicates where only first column was indexed
    let filtered_row_refs: Vec<&Row> = if need_where_filter && effective_where.is_some() {
        // Build predicate plan from effective WHERE (original or residual)
        let predicate_plan = PredicatePlan::from_where_clause(effective_where.as_ref(), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Filter with zero-copy references
        // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
        apply_where_filter_zerocopy(
            row_refs,
            &schema,
            &predicate_plan,
            table_name,
            database,
            cte_results,
        )?
    } else {
        row_refs
    };

    // Reverse row refs if needed for DESC ORDER BY
    // BTreeMap iteration is always ascending, but for DESC ORDER BY we need descending order
    // Check if we're using this index for ORDER BY and if the first ORDER BY column is DESC
    //
    // NOTE: Skip this if we already:
    // - Applied limit pushdown with DESC order (reversed indices for early termination)
    // - Used reverse iteration (prefix_scan_reverse already returns descending order)
    let mut filtered_row_refs = filtered_row_refs;
    if !limit_already_applied && !used_reverse_iteration {
        if let Some(ref sorted_cols) = sorted_columns {
            if let Some((_, first_order_direction)) = sorted_cols.first() {
                if *first_order_direction == vibesql_ast::OrderDirection::Desc {
                    filtered_row_refs.reverse();
                }
            }
        }
    }

    // Final step: Clone only the filtered rows, preserving row indices as rowids
    // This is the only place where cloning happens, and only for rows that survived filtering
    // Issue #4954: Set row_id when cloning so MIN/MAX(rowid) queries work with indexes
    let rows: Vec<Row> = filtered_row_refs
        .into_iter()
        .map(|row| {
            let mut cloned = row.clone();
            // Look up the original row index from our mapping and convert to 1-based rowid.
            // Issue #5517: a stored (relocated) row_id wins over physical-index+1.
            if cloned.row_id.is_none() {
                if let Some(&idx) = row_ptr_to_idx.get(&(row as *const Row as usize)) {
                    cloned.set_row_id((idx + 1) as u64);
                }
            }
            cloned
        })
        .collect();

    // Return results with sorting metadata if available
    // If WHERE clause was fully handled by index (!need_where_filter), indicate this
    // so the executor doesn't redundantly re-apply WHERE filtering
    if !need_where_filter {
        Ok(super::super::FromResult::from_rows_where_filtered(schema, rows, sorted_columns))
    } else {
        match sorted_columns {
            Some(sorted) => Ok(super::super::FromResult::from_rows_sorted(schema, rows, sorted)),
            None => Ok(super::super::FromResult::from_rows(schema, rows)),
        }
    }
}

/// Execute a MULTI-INDEX OR scan (epic #5668, PR 2).
///
/// Executes the union of per-branch index lookups for a top-level OR whose every
/// branch is independently indexable, then applies the residual non-OR
/// AND-conjuncts as a post-filter.
///
/// # Correctness (the dominant risk — see #5668 §2b/§3)
///
/// - **Exactly-once semantics.** A row satisfying multiple branches (e.g.
///   `c = 31031 OR d IS NULL` where both hold) must appear **exactly once**.
///   SQLite deduplicates by **rowid**: it unions the rowid sets from each
///   per-branch lookup, deduplicating, then fetches each surviving row once.
///   This function accumulates rows into an **insertion-ordered** dedup set
///   keyed by rowid (first-encounter order: branch 1's matches, then branch 2's
///   not-already-seen, ...), matching SQLite's union emission order. Any explicit
///   `ORDER BY` sorts downstream as today.
/// - **`IS NULL` vs `=`.** Each branch is executed by running its original
///   branch predicate through [`execute_index_scan`]. An `IS NULL` branch is
///   therefore evaluated as `col IS NULL` (a NULL-key match), distinct from the
///   `=` equality seek a `col = ?` branch performs — never conflated.
/// - **Residual.** The non-OR AND-conjuncts (e.g. `b > 1000`) are applied as a
///   filter **around** the deduped union, so they constrain rows regardless of
///   which branch matched them.
///
/// # Rowid source
///
/// [`execute_index_scan`] sets each cloned row's `row_id` (1-based; an explicitly
/// relocated rowid wins over physical-index+1). MULTI-INDEX OR is only selected
/// for tables that have a rowid (WITHOUT ROWID tables fall back to single-scan +
/// residual at selection time), so every branch row carries a stable rowid.
#[allow(private_interfaces)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_multi_index_or(
    table_name: &str,
    alias: Option<&String>,
    branches: &[super::selection::OrBranch],
    residual: Option<&Expression>,
    database: &Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<super::super::FromResult, ExecutorError> {
    use std::collections::HashSet;

    // Build the result schema once (independent of the per-branch scans, which
    // each build their own equivalent schema internally).
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Insertion-ordered rowid dedup: `seen` rejects duplicates, `deduped` holds
    // the surviving rows in first-encounter order.
    let mut seen: HashSet<u64> = HashSet::new();
    let mut deduped: Vec<Row> = Vec::new();

    for branch in branches {
        // Execute this branch's index lookup with the branch predicate as the
        // WHERE clause. We pass no ORDER BY / LIMIT: the union's ordering is the
        // first-encounter order, and any LIMIT must apply to the whole union
        // downstream (never per-branch). `execute_index_scan` re-applies the
        // branch predicate as a filter on the fetched rows, so both `=` and
        // `IS NULL` branches return exactly their matching rows.
        let branch_result = execute_index_scan(
            table_name,
            &branch.index_name,
            alias,
            Some(&branch.branch_predicate),
            None, // sorted_columns: union order, not index order
            None, // limit: applied downstream over the whole union
            database,
            cte_results,
        )?;

        for row in branch_result.into_rows() {
            // Every branch row carries a rowid (see #4954 / #5517 handling in
            // execute_index_scan). If one is somehow absent, fall back to a
            // synthetic key that still dedups identical rows within this union.
            let key = match row.row_id {
                Some(id) => id,
                None => {
                    // Defensive: should not happen for rowid tables (selection
                    // guards out WITHOUT ROWID). Skip the row rather than risk a
                    // mis-dedup; the debug_assert flags it in test builds.
                    debug_assert!(
                        false,
                        "MULTI-INDEX OR branch row missing row_id on a rowid table"
                    );
                    continue;
                }
            };

            if seen.insert(key) {
                deduped.push(row);
            }
        }
    }

    // Apply the residual non-OR AND-conjuncts (e.g. `b > 1000`) around the union.
    let rows = if let Some(residual_expr) = residual {
        let predicate_plan = PredicatePlan::from_where_clause(Some(residual_expr), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;
        let row_refs: Vec<&Row> = deduped.iter().collect();
        let filtered = apply_where_filter_zerocopy(
            row_refs,
            &schema,
            &predicate_plan,
            table_name,
            database,
            cte_results,
        )?;
        filtered.into_iter().cloned().collect()
    } else {
        deduped
    };

    // The union has no inherent sort order (first-encounter), so report it as a
    // plain WHERE-filtered result: the original WHERE clause is fully satisfied
    // by the branch predicates + residual, so downstream WHERE re-filtering is
    // unnecessary.
    Ok(super::super::FromResult::from_rows_where_filtered(schema, rows, None))
}

/// Apply WHERE filter using zero-copy row references
///
/// This function filters rows by reference, avoiding clones for rows that don't pass the filter.
/// Only the final filtered result needs to be cloned (done by the caller).
///
/// # Performance
/// For queries with selective WHERE clauses (e.g., filtering 1000 rows down to 100),
/// this saves ~90% of row cloning overhead compared to clone-then-filter approach.
///
/// For simple predicates (col = literal, col > literal, etc.), uses a compiled fast path
/// that bypasses CSE overhead entirely, providing 10-50x improvement for OLTP workloads.
///
/// # Arguments
/// * `cte_results` - CTE context for IN subqueries that may reference CTEs (Issue #3562)
fn apply_where_filter_zerocopy<'a>(
    row_refs: Vec<&'a Row>,
    schema: &CombinedSchema,
    predicate_plan: &PredicatePlan,
    table_name: &str,
    database: &vibesql_storage::Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<Vec<&'a Row>, ExecutorError> {
    use crate::{
        evaluator::{compiled::CompiledPredicate, CombinedExpressionEvaluator},
        select::scan::predicates::combine_predicates_with_and,
    };

    // Get table statistics for selectivity-based ordering
    // If no statistics available, create fallback estimates based on schema
    let table_stats_owned = database.get_table(table_name).map(|table| {
        table.get_statistics().cloned().unwrap_or_else(|| {
            // Fallback: create estimated statistics from table schema
            // This enables cost-based optimization even without ANALYZE
            vibesql_storage::statistics::TableStatistics::estimate_from_schema(
                table.row_count(),
                &table.schema,
            )
        })
    });

    // Get predicates ordered by selectivity (most selective first)
    let ordered_preds =
        predicate_plan.get_table_filters_ordered(table_name, table_stats_owned.as_ref());

    // If no table-local predicates, return all rows
    if ordered_preds.is_empty() {
        return Ok(row_refs);
    }

    // Combine ordered predicates with AND
    let combined_where = combine_predicates_with_and(ordered_preds);

    // Try to compile the predicate for fast path evaluation
    // This avoids CSE cache creation/clearing and expression traversal overhead
    let compiled = CompiledPredicate::compile(&combined_where, schema);

    // Use fast path if predicate is fully compiled (no Complex fallback)
    if compiled.is_fully_compiled() {
        return apply_where_filter_compiled(row_refs, &compiled);
    }

    // Fallback: Create evaluator for filtering complex predicates
    // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
    let evaluator = if cte_results.is_empty() {
        CombinedExpressionEvaluator::with_database(schema, database)
    } else {
        CombinedExpressionEvaluator::with_database_and_cte(schema, database, cte_results)
    };

    // Check if we should use parallel filtering
    #[cfg(feature = "parallel")]
    {
        let config = crate::select::parallel::ParallelConfig::global();
        if config.should_parallelize_scan(row_refs.len()) {
            return apply_where_filter_zerocopy_parallel(
                row_refs,
                schema,
                combined_where,
                evaluator,
            );
        }
    }

    // Sequential path for small datasets - filter rows using references (no cloning)
    let mut filtered = Vec::new();
    for row_ref in row_refs {
        evaluator.clear_cse_cache();

        let include_row = match evaluator.eval(&combined_where, row_ref)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            vibesql_types::SqlValue::Smallint(0) => false,
            vibesql_types::SqlValue::Smallint(_) => true,
            vibesql_types::SqlValue::Bigint(0) => false,
            vibesql_types::SqlValue::Bigint(_) => true,
            vibesql_types::SqlValue::Float(0.0) => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(0.0) => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(0.0) => false,
            vibesql_types::SqlValue::Double(_) => true,
            vibesql_types::SqlValue::Numeric(n) if n == 0.0 => false,
            vibesql_types::SqlValue::Numeric(_) => true,
            // String types (SQLite coerces strings to numeric for boolean context)
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                string_to_truthy(&s)
            }
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE clause must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            filtered.push(row_ref);
        }
    }

    Ok(filtered)
}

/// Fast path for compiled predicates
///
/// This function uses pre-compiled predicates to filter rows without any expression
/// evaluation overhead. No CSE caches, no expression tree traversal, no depth tracking.
///
/// # Performance
/// For simple predicates like `col = 5` or `col > 10 AND col < 100`, this provides
/// 10-50x faster evaluation compared to the full expression evaluator.
#[inline]
fn apply_where_filter_compiled<'a>(
    row_refs: Vec<&'a Row>,
    compiled: &crate::evaluator::compiled::CompiledPredicate,
) -> Result<Vec<&'a Row>, ExecutorError> {
    let mut filtered = Vec::with_capacity(row_refs.len() / 2); // Estimate 50% selectivity

    for row_ref in row_refs {
        // Evaluate compiled predicate - returns Option<bool>
        // None means NULL (unknown), which we treat as false for filtering
        let include_row = compiled.evaluate(row_ref).unwrap_or(false);

        if include_row {
            filtered.push(row_ref);
        }
    }

    Ok(filtered)
}

/// Apply WHERE filter using zero-copy row references with parallel execution
///
/// This function filters rows using Rayon's parallel iterators while maintaining zero-copy
/// semantics. Only used for large datasets where parallelization provides performance benefits.
///
/// # Performance
/// Parallelization is beneficial for datasets where `ParallelConfig::should_parallelize_scan()`
/// returns true, typically for 10,000+ rows. The overhead of thread spawning is amortized across
/// many rows.
#[cfg(feature = "parallel")]
fn apply_where_filter_zerocopy_parallel<'a>(
    row_refs: Vec<&'a Row>,
    _schema: &CombinedSchema,
    combined_where: vibesql_ast::Expression,
    evaluator: crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<&'a Row>, ExecutorError> {
    use std::sync::Arc;

    use rayon::prelude::*;

    // Clone expression for thread-safe sharing
    let where_expr_arc = Arc::new(combined_where);

    // Extract evaluator components for parallel execution (including CTE context)
    // Issue #3562: Now includes cte_context for IN subqueries referencing CTEs
    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        evaluator.get_parallel_components();

    // Use rayon's parallel iterator for filtering
    let result: Result<Vec<_>, ExecutorError> = row_refs
        .into_par_iter()
        .map(|row_ref| {
            // Create thread-local evaluator with independent caches
            let thread_evaluator =
                crate::evaluator::CombinedExpressionEvaluator::from_parallel_components(
                    schema,
                    database,
                    outer_row,
                    outer_schema,
                    window_mapping,
                    cte_context,
                    enable_cse,
                );

            // Evaluate predicate for this row reference (no cloning)
            let include_row = match thread_evaluator.eval(&where_expr_arc, row_ref)? {
                vibesql_types::SqlValue::Boolean(true) => true,
                vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
                // SQLLogicTest compatibility: treat integers as truthy/falsy
                vibesql_types::SqlValue::Integer(0) => false,
                vibesql_types::SqlValue::Integer(_) => true,
                vibesql_types::SqlValue::Smallint(0) => false,
                vibesql_types::SqlValue::Smallint(_) => true,
                vibesql_types::SqlValue::Bigint(0) => false,
                vibesql_types::SqlValue::Bigint(_) => true,
                vibesql_types::SqlValue::Float(0.0) => false,
                vibesql_types::SqlValue::Float(_) => true,
                vibesql_types::SqlValue::Real(0.0) => false,
                vibesql_types::SqlValue::Real(_) => true,
                vibesql_types::SqlValue::Double(0.0) => false,
                vibesql_types::SqlValue::Double(_) => true,
                vibesql_types::SqlValue::Numeric(n) if n == 0.0 => false,
                vibesql_types::SqlValue::Numeric(_) => true,
                // String types (SQLite coerces strings to numeric for boolean context)
                vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                    string_to_truthy(&s)
                }
                other => {
                    return Err(ExecutorError::InvalidWhereClause(format!(
                        "WHERE clause must evaluate to boolean, got: {:?}",
                        other
                    )))
                }
            };

            if include_row {
                Ok(Some(row_ref))
            } else {
                Ok(None)
            }
        })
        .collect();

    // Filter out None values and extract Ok row references
    result.map(|v| v.into_iter().flatten().collect())
}

/// Convert string to boolean using SQLite semantics
#[inline(always)]
fn string_to_truthy(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Parse leading numeric portion
    let mut end = 0;
    let mut has_dot = false;
    let mut has_digit = false;
    let chars: Vec<char> = trimmed.chars().collect();
    if !chars.is_empty() && (chars[0] == '-' || chars[0] == '+') {
        end = 1;
    }
    while end < chars.len() {
        let c = chars[end];
        if c.is_ascii_digit() {
            has_digit = true;
            end += 1;
        } else if c == '.' && !has_dot {
            has_dot = true;
            end += 1;
        } else {
            break;
        }
    }
    if !has_digit {
        return false;
    }
    let num_str: String = chars[..end].iter().collect();
    num_str.parse::<f64>().map(|n| n != 0.0).unwrap_or(false)
}

/// Execute a skip-scan index operation
///
/// Skip-scan enables using a composite index when the WHERE clause filters on
/// non-prefix columns. It works by:
/// 1. Getting distinct values of the prefix column(s)
/// 2. For each prefix value, seeking to (prefix, filter_value) in the index
/// 3. Returning matching rows
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `index_name` - Name of the index to use
/// * `alias` - Optional table alias
/// * `where_clause` - WHERE clause predicate (required for skip-scan)
/// * `skip_scan_info` - Skip-scan configuration from planning phase
/// * `database` - Database reference
/// * `cte_results` - CTE context for subqueries
///
/// # Performance
/// Cost = O(prefix_cardinality × log n + k) vs O(n) for table scan
/// Beneficial when prefix columns have low cardinality and filter is selective.
pub(in crate::select::scan) fn execute_skip_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: &Expression,
    skip_scan_info: &crate::optimizer::index_planner::SkipScanInfo,
    database: &Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<super::super::FromResult, ExecutorError> {
    // Get table and index
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let _index_metadata = database
        .get_index(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // Extract the filter value from the WHERE clause for the filter column
    let filter_column = &skip_scan_info.filter_column;
    let filter_column_idx = skip_scan_info.skip_columns; // The column index in the composite key

    // Extract predicate from WHERE clause for the filter column
    let skip_scan_predicate = extract_skip_scan_predicate(where_clause, filter_column);

    // Execute skip-scan based on predicate type
    let matching_row_indices: Vec<usize> = match skip_scan_predicate {
        SkipScanPredicate::Equality(value) => {
            if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                eprintln!(
                    "[SKIP_SCAN] Executing equality skip-scan: index={}, filter_col={}, filter_col_idx={}, value={:?}",
                    index_name, filter_column, filter_column_idx, value
                );
            }
            index_data.skip_scan_equality(filter_column_idx, &value)
        }
        SkipScanPredicate::Range { lower, inclusive_lower, upper, inclusive_upper } => {
            if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                eprintln!(
                    "[SKIP_SCAN] Executing range skip-scan: index={}, filter_col={}, filter_col_idx={}, lower={:?}, upper={:?}",
                    index_name, filter_column, filter_column_idx, lower, upper
                );
            }
            index_data.skip_scan_range(
                filter_column_idx,
                lower.as_ref(),
                inclusive_lower,
                upper.as_ref(),
                inclusive_upper,
            )
        }
        SkipScanPredicate::None => {
            // No suitable predicate found - fall back to empty result
            // This shouldn't happen if planning was done correctly
            if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                eprintln!(
                    "[SKIP_SCAN] Warning: No predicate extracted for filter column '{}', returning empty",
                    filter_column
                );
            }
            vec![]
        }
    };

    if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
        eprintln!("[SKIP_SCAN] Found {} matching rows from skip-scan", matching_row_indices.len());
    }

    // Build schema
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name.clone(), table.schema.clone());

    // Fetch matching rows
    // Issue #3790: Use get_row() which returns None for deleted rows
    // Issue #5204: also apply MVCC visibility filtering. With `mvcc_enabled`
    // OFF, `is_row_visible` is equivalent to the existing not-bitmap-deleted
    // check, so off-state behavior is preserved bit-for-bit.
    let snapshot = crate::mvcc::read_snapshot(database);
    let row_refs: Vec<&Row> = matching_row_indices
        .iter()
        .filter(|idx| table.is_row_visible(**idx, &snapshot))
        .filter_map(|idx| table.get_row(*idx))
        .collect();

    // Skip-scan doesn't fully satisfy WHERE clause, so we need to apply post-filtering
    // This handles any additional predicates not covered by the skip-scan
    let predicate_plan = PredicatePlan::from_where_clause(Some(where_clause), &schema)
        .map_err(ExecutorError::InvalidWhereClause)?;

    // Apply WHERE filtering
    let filtered_row_refs = apply_where_filter_for_skip_scan(
        row_refs,
        &schema,
        &predicate_plan,
        table_name,
        database,
        cte_results,
    )?;

    // Clone only the filtered rows
    let rows: Vec<Row> = filtered_row_refs.into_iter().cloned().collect();

    // Skip-scan doesn't provide sorted output
    Ok(super::super::FromResult::from_rows(schema, rows))
}

/// Predicate type extracted for skip-scan execution
#[derive(Debug)]
enum SkipScanPredicate {
    /// Equality: filter_col = value
    Equality(vibesql_types::SqlValue),
    /// Range: filter_col > lower AND/OR filter_col < upper
    Range {
        lower: Option<vibesql_types::SqlValue>,
        inclusive_lower: bool,
        upper: Option<vibesql_types::SqlValue>,
        inclusive_upper: bool,
    },
    /// No suitable predicate found
    None,
}

/// Extract predicate for skip-scan filter column from WHERE clause
fn extract_skip_scan_predicate(
    where_clause: &Expression,
    filter_column: &str,
) -> SkipScanPredicate {
    use vibesql_ast::BinaryOperator;

    use super::selection::is_column_reference;

    match where_clause {
        Expression::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::Equal => {
                    // col = value or value = col
                    if is_column_reference(left, filter_column) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return SkipScanPredicate::Equality(value.clone());
                        }
                    }
                    if is_column_reference(right, filter_column) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return SkipScanPredicate::Equality(value.clone());
                        }
                    }
                }
                BinaryOperator::GreaterThan => {
                    if is_column_reference(left, filter_column) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: Some(value.clone()),
                                inclusive_lower: false,
                                upper: None,
                                inclusive_upper: false,
                            };
                        }
                    }
                    if is_column_reference(right, filter_column) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: None,
                                inclusive_lower: false,
                                upper: Some(value.clone()),
                                inclusive_upper: false,
                            };
                        }
                    }
                }
                BinaryOperator::GreaterThanOrEqual => {
                    if is_column_reference(left, filter_column) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: Some(value.clone()),
                                inclusive_lower: true,
                                upper: None,
                                inclusive_upper: false,
                            };
                        }
                    }
                    if is_column_reference(right, filter_column) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: None,
                                inclusive_lower: false,
                                upper: Some(value.clone()),
                                inclusive_upper: true,
                            };
                        }
                    }
                }
                BinaryOperator::LessThan => {
                    if is_column_reference(left, filter_column) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: None,
                                inclusive_lower: false,
                                upper: Some(value.clone()),
                                inclusive_upper: false,
                            };
                        }
                    }
                    if is_column_reference(right, filter_column) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: Some(value.clone()),
                                inclusive_lower: false,
                                upper: None,
                                inclusive_upper: false,
                            };
                        }
                    }
                }
                BinaryOperator::LessThanOrEqual => {
                    if is_column_reference(left, filter_column) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: None,
                                inclusive_lower: false,
                                upper: Some(value.clone()),
                                inclusive_upper: true,
                            };
                        }
                    }
                    if is_column_reference(right, filter_column) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return SkipScanPredicate::Range {
                                lower: Some(value.clone()),
                                inclusive_lower: true,
                                upper: None,
                                inclusive_upper: false,
                            };
                        }
                    }
                }
                BinaryOperator::And => {
                    // Try to find predicates on the filter column in both sides
                    let left_pred = extract_skip_scan_predicate(left, filter_column);
                    let right_pred = extract_skip_scan_predicate(right, filter_column);

                    // Merge range predicates if both are ranges
                    match (left_pred, right_pred) {
                        (SkipScanPredicate::Equality(v), SkipScanPredicate::None) => {
                            return SkipScanPredicate::Equality(v);
                        }
                        (SkipScanPredicate::None, SkipScanPredicate::Equality(v)) => {
                            return SkipScanPredicate::Equality(v);
                        }
                        (
                            SkipScanPredicate::Range {
                                lower: l1,
                                inclusive_lower: il1,
                                upper: u1,
                                inclusive_upper: iu1,
                            },
                            SkipScanPredicate::Range {
                                lower: l2,
                                inclusive_lower: il2,
                                upper: u2,
                                inclusive_upper: iu2,
                            },
                        ) => {
                            // Merge ranges: take the more restrictive bounds
                            let (lower, inclusive_lower) = match (l1, l2) {
                                (Some(v1), Some(v2)) => {
                                    // Take the larger lower bound (more restrictive)
                                    if v1 >= v2 {
                                        (Some(v1), il1)
                                    } else {
                                        (Some(v2), il2)
                                    }
                                }
                                (Some(v), None) | (None, Some(v)) => (Some(v), il1 || il2),
                                (None, None) => (None, false),
                            };
                            let (upper, inclusive_upper) = match (u1, u2) {
                                (Some(v1), Some(v2)) => {
                                    // Take the smaller upper bound (more restrictive)
                                    if v1 <= v2 {
                                        (Some(v1), iu1)
                                    } else {
                                        (Some(v2), iu2)
                                    }
                                }
                                (Some(v), None) | (None, Some(v)) => (Some(v), iu1 || iu2),
                                (None, None) => (None, false),
                            };
                            return SkipScanPredicate::Range {
                                lower,
                                inclusive_lower,
                                upper,
                                inclusive_upper,
                            };
                        }
                        (r @ SkipScanPredicate::Range { .. }, SkipScanPredicate::None) => return r,
                        (SkipScanPredicate::None, r @ SkipScanPredicate::Range { .. }) => return r,
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        Expression::Between { expr, low, high, negated: false, .. } => {
            if is_column_reference(expr, filter_column) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    return SkipScanPredicate::Range {
                        lower: Some(low_val.clone()),
                        inclusive_lower: true,
                        upper: Some(high_val.clone()),
                        inclusive_upper: true,
                    };
                }
            }
        }
        _ => {}
    }

    SkipScanPredicate::None
}

/// Apply WHERE filter for skip-scan results
///
/// Similar to apply_where_filter_zerocopy but simplified for skip-scan use case.
fn apply_where_filter_for_skip_scan<'a>(
    row_refs: Vec<&'a Row>,
    schema: &CombinedSchema,
    predicate_plan: &PredicatePlan,
    table_name: &str,
    database: &Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<Vec<&'a Row>, ExecutorError> {
    use crate::{
        evaluator::{compiled::CompiledPredicate, CombinedExpressionEvaluator},
        select::scan::predicates::combine_predicates_with_and,
    };

    // Get table statistics for selectivity-based ordering
    let table_stats_owned = database.get_table(table_name).map(|table| {
        table.get_statistics().cloned().unwrap_or_else(|| {
            vibesql_storage::statistics::TableStatistics::estimate_from_schema(
                table.row_count(),
                &table.schema,
            )
        })
    });

    // Get predicates ordered by selectivity
    let ordered_preds =
        predicate_plan.get_table_filters_ordered(table_name, table_stats_owned.as_ref());

    // If no table-local predicates, return all rows
    if ordered_preds.is_empty() {
        return Ok(row_refs);
    }

    // Combine ordered predicates with AND
    let combined_where = combine_predicates_with_and(ordered_preds);

    // Try compiled predicate path
    let compiled = CompiledPredicate::compile(&combined_where, schema);
    if compiled.is_fully_compiled() {
        let mut filtered = Vec::with_capacity(row_refs.len() / 2);
        for row_ref in row_refs {
            if compiled.evaluate(row_ref).unwrap_or(false) {
                filtered.push(row_ref);
            }
        }
        return Ok(filtered);
    }

    // Fallback: Use full expression evaluator
    let evaluator = if cte_results.is_empty() {
        CombinedExpressionEvaluator::with_database(schema, database)
    } else {
        CombinedExpressionEvaluator::with_database_and_cte(schema, database, cte_results)
    };

    let mut filtered = Vec::new();
    for row_ref in row_refs {
        evaluator.clear_cse_cache();
        let include_row = match evaluator.eval(&combined_where, row_ref)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            _ => false,
        };
        if include_row {
            filtered.push(row_ref);
        }
    }

    Ok(filtered)
}
