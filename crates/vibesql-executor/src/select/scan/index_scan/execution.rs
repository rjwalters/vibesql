//! Index scan execution
//!
//! Executes index scans to retrieve rows from tables using indexes.

use std::collections::HashMap;

use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};

use crate::{
    errors::ExecutorError, optimizer::PredicatePlan, schema::CombinedSchema, select::cte::CteResult,
};

use super::predicate::{
    build_residual_where_clause, extract_composite_predicates_with_in, extract_index_predicate,
    extract_prefix_equality_predicates, extract_prefix_with_trailing_range,
    generate_composite_keys, where_clause_fully_satisfied_by_composite_key, CompositePredicateType,
    IndexPredicate, PrefixPredicateResult, PrefixWithRangeResult,
};

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

    // Get column names for the index (in order)
    let index_column_names: Vec<&str> =
        index_metadata.columns.iter().map(|col| col.column_name.as_str()).collect();

    // Get the first indexed column (for single-column predicate extraction fallback)
    let first_indexed_column = index_column_names.first().copied().unwrap_or("");

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

    // Try prefix + trailing range lookup first (for queries like WHERE s_w_id = 1 AND s_quantity < 10)
    // This is more efficient than prefix-only lookup because it bounds the scan
    let prefix_with_range_result: Option<PrefixWithRangeResult> = if !use_composite_lookup
        && is_multi_column_index
    {
        where_clause.and_then(|expr| extract_prefix_with_trailing_range(expr, &index_column_names))
    } else {
        None
    };

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

    // Check if we're using prefix lookup (partial composite key match)
    let use_prefix_lookup =
        prefix_result.is_some() && !use_composite_lookup && !use_prefix_bounded_lookup;

    // Fall back to single-column predicate extraction if neither composite nor prefix available
    let index_predicate = if use_composite_lookup || use_prefix_lookup || use_prefix_bounded_lookup
    {
        None // Don't need single-column predicate - using composite/prefix key
    } else {
        where_clause.and_then(|expr| extract_index_predicate(expr, first_indexed_column))
    };

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
            None => (false, None), // All predicates covered by prefix - skip filtering
        }
    } else {
        match (&where_clause, &index_predicate) {
            (Some(where_expr), Some(_)) => {
                // Only skip WHERE filtering if we're certain the index handles everything
                let need_filter = !where_clause_fully_satisfied_by_index(
                    where_expr,
                    first_indexed_column,
                    &index_predicate,
                );
                (need_filter, if need_filter { Some((*where_expr).clone()) } else { None })
            }
            (Some(where_expr), None) => (true, Some((*where_expr).clone())), // WHERE present but no index predicate extracted
            (None, _) => (false, None),                                      // No WHERE clause
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
                        if let Some(row_ref) = table.get_row(idx) {
                            lookup_time += t1.elapsed();

                            let t2 = Instant::now();
                            rows.push(row_ref.clone());
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
                    streaming_iter
                        .filter_map(|idx| table.get_row(idx))
                        .cloned()
                        .collect()
                };

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
            Some(IndexPredicate::Range(range)) => {
                // Use storage layer's optimized range_scan for >, <, >=, <=, BETWEEN
                // The storage layer handles empty/inverted range validation efficiently
                //
                // Optimization: Use range_scan_limit when LIMIT is provided and no post-filter needed
                // This enables early termination at the index level for simple LIMIT queries (#3638)
                // Example: SELECT c FROM t WHERE id BETWEEN 1 AND 100 LIMIT 10
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
            Some(IndexPredicate::In(values)) => {
                // For multi-column indexes, use prefix matching to find all rows
                // where the first column matches any of the IN values
                if is_multi_column_index {
                    // Use prefix_multi_lookup which performs range scans to match
                    // partial keys (e.g., [10] matches [10, 20], [10, 30], etc.)
                    index_data.prefix_multi_lookup(&values)
                } else {
                    // For single-column indexes, use regular exact match lookup
                    index_data.multi_lookup(&values)
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

    // Zero-copy optimization: Work with row references until the final step
    // This avoids cloning rows that will be filtered out by the WHERE clause
    // Issue #3790: Use get_row() which returns None for deleted rows
    let row_refs: Vec<&Row> =
        matching_row_indices.iter().filter_map(|idx| table.get_row(*idx)).collect();

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

    // Final step: Clone only the filtered rows
    // This is the only place where cloning happens, and only for rows that survived filtering
    let rows: Vec<Row> = filtered_row_refs.into_iter().cloned().collect();

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
    use crate::evaluator::compiled::CompiledPredicate;
    use crate::evaluator::CombinedExpressionEvaluator;
    use crate::select::scan::predicates::combine_predicates_with_and;

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
    let ordered_preds = predicate_plan.get_table_filters_ordered(table_name, table_stats_owned.as_ref());

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
/// This function filters rows using Rayon's parallel iterators while maintaining zero-copy semantics.
/// Only used for large datasets where parallelization provides performance benefits.
///
/// # Performance
/// Parallelization is beneficial for datasets where `ParallelConfig::should_parallelize_scan()` returns true,
/// typically for 10,000+ rows. The overhead of thread spawning is amortized across many rows.
#[cfg(feature = "parallel")]
fn apply_where_filter_zerocopy_parallel<'a>(
    row_refs: Vec<&'a Row>,
    _schema: &CombinedSchema,
    combined_where: vibesql_ast::Expression,
    evaluator: crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<&'a Row>, ExecutorError> {
    use rayon::prelude::*;
    use std::sync::Arc;

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

/// Determines if the WHERE clause is fully satisfied by the index predicate
///
/// Returns true only when we're 100% certain that the index has already filtered
/// rows exactly according to the WHERE clause, making WHERE re-evaluation redundant.
///
/// # Conservative Approach
/// This function is intentionally conservative - it only returns true for simple cases
/// where we can prove the index predicate exactly matches the WHERE semantics.
/// When in doubt, we return false to ensure correctness.
///
/// # Safe Cases (returns true)
/// - `WHERE col = value` with extracted equality predicate
/// - `WHERE col BETWEEN a AND b` with extracted BETWEEN predicate
/// - `WHERE col >= a AND col <= b` with extracted range predicate
/// - `WHERE col > a` / `WHERE col < b` with extracted range predicate
/// - `WHERE col IN (...)` with extracted IN predicate
///
/// # Unsafe Cases (returns false)
/// - OR predicates: `WHERE col1 = 5 OR col2 = 10`
/// - AND with multiple columns: `WHERE col1 = 5 AND col2 = 10` (only first column indexed)
/// - Complex predicates: `WHERE col = 5 AND func(col2) = 1`
/// - Negations: `NOT IN`, `NOT BETWEEN`, `!=`
/// - Any case where the WHERE clause structure doesn't exactly match the extracted predicate
fn where_clause_fully_satisfied_by_index(
    where_expr: &Expression,
    indexed_column: &str,
    index_predicate: &Option<IndexPredicate>,
) -> bool {
    use super::super::super::scan::index_scan::selection::is_column_reference;
    use vibesql_ast::BinaryOperator;

    let Some(pred) = index_predicate else {
        return false; // No index predicate, can't be satisfied
    };

    match where_expr {
        // Simple equality: WHERE col = value
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check if this is exactly "indexed_column = literal"
            let is_indexed_col_equals_literal = (is_column_reference(left, indexed_column)
                && matches!(right.as_ref(), Expression::Literal(_)))
                || (is_column_reference(right, indexed_column)
                    && matches!(left.as_ref(), Expression::Literal(_)));

            if !is_indexed_col_equals_literal {
                return false;
            }

            // Verify the index predicate is a matching equality range
            matches!(pred, IndexPredicate::Range(range)
                if range.start.is_some() && range.end.is_some()
                && range.start == range.end
                && range.inclusive_start && range.inclusive_end)
        }

        // BETWEEN: WHERE col BETWEEN low AND high
        Expression::Between { expr, negated: false, .. } => {
            // Must be our indexed column
            if !is_column_reference(expr, indexed_column) {
                return false;
            }

            // Verify the index predicate is a BETWEEN-compatible range
            matches!(pred, IndexPredicate::Range(range)
                if range.start.is_some() && range.end.is_some()
                && range.inclusive_start && range.inclusive_end)
        }

        // Simple range: WHERE col > value, WHERE col >= value, etc.
        Expression::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual => {
                    // Check if this is "indexed_column <op> literal" or "literal <op> indexed_column"
                    let is_simple_range = (is_column_reference(left, indexed_column)
                        && matches!(right.as_ref(), Expression::Literal(_)))
                        || (is_column_reference(right, indexed_column)
                            && matches!(left.as_ref(), Expression::Literal(_)));

                    if !is_simple_range {
                        return false;
                    }

                    // Verify the index predicate is a range (any range is fine for simple comparisons)
                    matches!(pred, IndexPredicate::Range(_))
                }

                // AND: Only safe if it's "col >= a AND col <= b" forming a complete BETWEEN
                BinaryOperator::And => {
                    // This is only safe if both sides reference the same indexed column
                    // and together form a complete range that matches our index predicate
                    // For now, be conservative and reject AND unless it's obviously safe
                    // The predicate extraction already handles simple "col >= a AND col <= b" cases

                    // Check if this is exactly the pattern: indexed_col >= val AND indexed_col <= val
                    match (left.as_ref(), right.as_ref()) {
                        (
                            Expression::BinaryOp { left: l_left, op: l_op, right: l_right },
                            Expression::BinaryOp { left: r_left, op: r_op, right: r_right },
                        ) => {
                            // Both sides must reference our indexed column
                            let left_has_col = is_column_reference(l_left, indexed_column)
                                || is_column_reference(l_right, indexed_column);
                            let right_has_col = is_column_reference(r_left, indexed_column)
                                || is_column_reference(r_right, indexed_column);

                            if !left_has_col || !right_has_col {
                                return false; // Not both sides on our column
                            }

                            // Both sides must be range operators
                            let is_range_op = |op: &BinaryOperator| {
                                matches!(
                                    op,
                                    BinaryOperator::GreaterThan
                                        | BinaryOperator::GreaterThanOrEqual
                                        | BinaryOperator::LessThan
                                        | BinaryOperator::LessThanOrEqual
                                )
                            };

                            if !is_range_op(l_op) || !is_range_op(r_op) {
                                return false;
                            }

                            // Must have extracted a range with both bounds
                            matches!(pred, IndexPredicate::Range(range)
                                if range.start.is_some() && range.end.is_some())
                        }
                        _ => false, // Not the right structure
                    }
                }

                _ => false, // Other binary operators not handled
            }
        }

        // IN: WHERE col IN (value1, value2, ...)
        Expression::InList { expr, negated: false, .. } => {
            // Must be our indexed column
            if !is_column_reference(expr, indexed_column) {
                return false;
            }

            // Verify the index predicate is an IN predicate
            matches!(pred, IndexPredicate::In(_))
        }

        // Anything else is unsafe
        _ => false,
    }
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
        eprintln!(
            "[SKIP_SCAN] Found {} matching rows from skip-scan",
            matching_row_indices.len()
        );
    }

    // Build schema
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name.clone(), table.schema.clone());

    // Fetch matching rows
    // Issue #3790: Use get_row() which returns None for deleted rows
    let row_refs: Vec<&Row> =
        matching_row_indices.iter().filter_map(|idx| table.get_row(*idx)).collect();

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
fn extract_skip_scan_predicate(where_clause: &Expression, filter_column: &str) -> SkipScanPredicate {
    use super::selection::is_column_reference;
    use vibesql_ast::BinaryOperator;

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
                        (SkipScanPredicate::Range { lower: l1, inclusive_lower: il1, upper: u1, inclusive_upper: iu1 },
                         SkipScanPredicate::Range { lower: l2, inclusive_lower: il2, upper: u2, inclusive_upper: iu2 }) => {
                            // Merge ranges: take the more restrictive bounds
                            let (lower, inclusive_lower) = match (l1, l2) {
                                (Some(v1), Some(v2)) => {
                                    // Take the larger lower bound (more restrictive)
                                    if v1 >= v2 { (Some(v1), il1) } else { (Some(v2), il2) }
                                }
                                (Some(v), None) | (None, Some(v)) => (Some(v), il1 || il2),
                                (None, None) => (None, false),
                            };
                            let (upper, inclusive_upper) = match (u1, u2) {
                                (Some(v1), Some(v2)) => {
                                    // Take the smaller upper bound (more restrictive)
                                    if v1 <= v2 { (Some(v1), iu1) } else { (Some(v2), iu2) }
                                }
                                (Some(v), None) | (None, Some(v)) => (Some(v), iu1 || iu2),
                                (None, None) => (None, false),
                            };
                            return SkipScanPredicate::Range { lower, inclusive_lower, upper, inclusive_upper };
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
    use crate::evaluator::compiled::CompiledPredicate;
    use crate::evaluator::CombinedExpressionEvaluator;
    use crate::select::scan::predicates::combine_predicates_with_and;

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
    let ordered_preds = predicate_plan.get_table_filters_ordered(table_name, table_stats_owned.as_ref());

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
