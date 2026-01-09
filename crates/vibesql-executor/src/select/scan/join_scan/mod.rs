//! JOIN execution logic for FROM clause scanning
//!
//! Handles execution of JOIN operations within FROM clauses by:
//! - Recursively executing left and right sides
//! - Extracting equijoin predicates from WHERE clause
//! - Delegating to nested loop join implementation
//!
//! ## SEMI/ANTI Join Optimization
//!
//! For SEMI and ANTI joins (from IN/EXISTS subquery transformations), this module
//! extracts right-table-only predicates from the join condition and passes them
//! to the right-side scan for index selection. This enables index usage for
//! predicates like `s_quantity < ?` in TPC-C Stock-Level queries.
//!
//! ## Index Nested Loop Semi-Join (#3392)
//!
//! When the left side of a semi-join is small (< INL_THRESHOLD rows), we use
//! Index Nested Loop (INL) instead of hash join. For each distinct join key
//! from the left side, we do a point lookup on the right table using an index.
//! This avoids scanning potentially thousands of rows from the right table.
//!
//! Example: TPC-C Stock-Level query
//!   SELECT COUNT(DISTINCT ol_i_id) FROM order_line
//!   WHERE ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_quantity < 10)
//!
//! With hash join: Scan ~5000 STOCK rows to build hash table, probe ~200 ORDER_LINE rows
//! With INL: For each of ~170 distinct ol_i_id values, do one point lookup on STOCK

use std::collections::{HashMap, HashSet};

use crate::{
    errors::ExecutorError, optimizer::PredicatePlan, schema::CombinedSchema,
    select::cte::CteResult, timeout::TimeoutContext,
};

// Submodules
mod inl;
mod predicates;
mod pushdown;
mod semi_anti;

/// Base threshold for using Index Nested Loop semi-join instead of hash join.
/// If left side has fewer rows than this, INL is always preferred.
const INL_BASE_THRESHOLD: usize = 1000;

/// Maximum threshold for INL semi-join when cost-based analysis is used.
/// Even with favorable cost estimates, we cap INL at this threshold to avoid
/// excessive random I/O for very large left sides.
const INL_MAX_THRESHOLD: usize = 100_000;

/// Minimum ratio of right table size to left size required for cost-based INL.
/// When right_rows / left_rows > this ratio, INL is likely faster even for
/// larger left sides because hash join would scan the entire right table.
const INL_SIZE_RATIO_THRESHOLD: usize = 10;

/// Execute a JOIN operation
///
/// The `join_alias` parameter is the alias for a parenthesized join expression (e.g., `(A JOIN B) AS X`).
/// This alias must be passed so that ON clause validation can recognize references like `X.column`.
/// Issue #4786: ON clause validation was failing because the alias wasn't available.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_join<F>(
    left: &vibesql_ast::FromClause,
    right: &vibesql_ast::FromClause,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
    using_columns: &Option<Vec<String>>,
    natural: bool,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&crate::schema::CombinedSchema>,
    join_alias: Option<&str>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<crate::select::SelectResult, ExecutorError> + Copy,
{
    // Execute left and right sides with WHERE clause for predicate pushdown
    // Note: ORDER BY and LIMIT are not optimized at JOIN level, so we pass None
    //
    // Bug fix for #3773: For RIGHT/FULL OUTER joins, predicates that reference ONLY
    // the left table must NOT be pushed down to the left-side scan. These predicates
    // test for NULL values produced by the join, not stored NULLs.
    let left_where_clause = match join_type {
        vibesql_ast::JoinType::RightOuter | vibesql_ast::JoinType::FullOuter => {
            // For RIGHT/FULL OUTER JOIN: Remove predicates that reference ONLY the left table
            if let Some(where_expr) = where_clause {
                pushdown::filter_out_nullable_side_predicates(left, where_expr, database)
            } else {
                None
            }
        }
        _ => where_clause.cloned(),
    };
    let left_result = super::execute_from_clause(
        left,
        cte_results,
        database,
        left_where_clause.as_ref(),
        None,
        None,
        outer_row,
        outer_schema,
        execute_subquery,
    )?;

    // Index Nested Loop (INL) optimization for SEMI joins (#3392, #4354)
    // When left side is small OR right table is much larger, use index lookups
    // on right table instead of scanning all rows for hash join.
    //
    // Cost model:
    // - Hash join: O(right_rows) to build hash table + O(left_rows) to probe
    // - INL: O(left_distinct_keys * avg_matches_per_key) with index seeks
    //
    // INL wins when: left_rows * avg_matches << right_rows
    // This is especially true for semi-joins where we stop at first match.
    if matches!(join_type, vibesql_ast::JoinType::Semi) {
        let left_row_count = left_result.as_slice().len();
        if left_row_count > 0 {
            // Determine if INL should be used based on cost estimation
            let should_use_inl =
                semi_anti::should_use_inl_for_semi_join(left_row_count, right, condition, database);

            if should_use_inl {
                // Try exact-match INL first (requires full PK coverage)
                if let Some(result) =
                    inl::try_index_nested_loop_semi_join(&left_result, right, condition, database)?
                {
                    return Ok(result);
                }

                // Try prefix-scan INL (works with partial index key match)
                // This handles cases like TPC-H Q4 where we join on L_ORDERKEY but the
                // index is on (L_ORDERKEY, L_LINENUMBER) - we can still use a prefix scan
                if let Some(result) =
                    inl::try_prefix_scan_semi_join(&left_result, right, condition, database)?
                {
                    return Ok(result);
                }
            }
        }
    }

    // For SEMI and ANTI joins (from IN/EXISTS subquery transformations), we must NOT pass
    // the outer WHERE clause to the right side. The right side represents the subquery
    // table, and the outer query's WHERE conditions should not be pushed down to it.
    //
    // Bug fix for #2599: Passing outer WHERE clause to the right side caused incorrect
    // index scans that filtered out rows that should have been in the subquery result.
    //
    // HOWEVER, we DO want to extract right-table-only predicates from the JOIN condition
    // (which contains the subquery's original WHERE clause) and pass them to the right-side
    // scan for index selection. This enables efficient index usage for predicates like
    // `s_quantity < ?` in TPC-C Stock-Level queries.
    //
    // Performance fix for #3130: Extract right-only predicates from JOIN condition.
    //
    // Bug fix for #3773: For LEFT/RIGHT/FULL OUTER joins, predicates that reference ONLY
    // the nullable side must NOT be pushed down to that side's scan. These predicates
    // test for NULL values that are PRODUCED BY THE JOIN (unmatched rows), not stored
    // in the underlying table. Example: `LEFT JOIN ... WHERE right.col IS NULL` should
    // find rows with no match, but if pushed to right-side scan, it filters on stored NULLs.
    let right_where_clause = match join_type {
        vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti => {
            // Extract right-table-only predicates from the join condition
            if let Some(cond) = condition {
                pushdown::extract_right_only_predicates(right, cond, database)
            } else {
                None
            }
        }
        vibesql_ast::JoinType::LeftOuter | vibesql_ast::JoinType::FullOuter => {
            // For LEFT/FULL OUTER JOIN: Remove predicates that reference ONLY the right table
            // These must be evaluated post-join when NULL values from unmatched rows are present
            if let Some(where_expr) = where_clause {
                pushdown::filter_out_nullable_side_predicates(right, where_expr, database)
            } else {
                None
            }
        }
        _ => where_clause.cloned(),
    };
    let mut right_result = super::execute_from_clause(
        right,
        cte_results,
        database,
        right_where_clause.as_ref(),
        None,
        None,
        outer_row,
        outer_schema,
        execute_subquery,
    )?;

    // Issue #4786: If the join has an alias (parenthesized join expression), add it to the
    // right side's schema BEFORE combining with the left side. This ensures:
    // 1. ON clause validation recognizes `j1.column` references
    // 2. Only the right side's tables are shadowed by the alias, not the left side's tables
    // 3. SELECT * outputs both left side columns AND alias columns correctly
    if let Some(alias) = join_alias {
        right_result.schema = right_result.schema.add_join_alias(alias);
    }

    // Validate ON clause doesn't reference tables not yet introduced (to the right)
    // This is a SQLite-specific semantic check: ON clause can only reference tables
    // in the current join, not tables that appear later in the FROM clause.
    //
    // IMPORTANT: This check only applies to OUTER joins (LEFT, RIGHT, FULL).
    // For INNER joins, SQLite allows referencing tables to the right because
    // INNER joins are commutative and SQLite can reorder them during optimization.
    let is_outer_join = matches!(
        join_type,
        vibesql_ast::JoinType::LeftOuter
            | vibesql_ast::JoinType::RightOuter
            | vibesql_ast::JoinType::FullOuter
    );
    if is_outer_join {
        if let Some(on_condition) = condition {
            // Pass join_alias so validation recognizes references like `j1.column` as valid.
            // Issue #4786: ON clause validation was failing for parenthesized join aliases.
            validate_on_clause_table_references(
                on_condition,
                &left_result.schema,
                &right_result.schema,
                database,
                join_alias,
            )?;
        }
    }

    // For NATURAL JOIN, generate the implicit join condition based on common column names
    let natural_join_condition = if natural {
        generate_natural_join_condition(&left_result.schema, &right_result.schema)?
    } else {
        None
    };

    // For USING clause, generate the join condition based on specified columns
    let using_join_condition = if let Some(cols) = using_columns {
        generate_using_join_condition(cols, &left_result.schema, &right_result.schema)?
    } else {
        None
    };

    // Use the natural join condition, USING condition, or explicit condition
    let effective_condition =
        natural_join_condition.or(using_join_condition).or_else(|| condition.clone());

    // If we have a WHERE clause, use predicate plan to extract equijoin conditions (Phase 1)
    let equijoin_predicates = if let Some(where_expr) = where_clause {
        // Build combined schema for WHERE clause analysis using SchemaBuilder for O(n) performance
        let mut schema_builder =
            crate::schema::SchemaBuilder::from_schema(left_result.schema.clone());
        for (table_id, (_start_idx, table_schema)) in &right_result.schema.table_schemas {
            // If adding table fails due to duplicate, the combined_schema might be incorrect
            // but this is an edge case and we'll let subsequent operations handle it
            let _ = schema_builder.add_table(table_id.display().to_string(), table_schema.clone());
        }
        let combined_schema = schema_builder.build();

        // Build predicate plan once for this join (Phase 1 optimization)
        let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &combined_schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Extract equijoin conditions that apply to this join
        let left_schema_tables: std::collections::HashSet<String> =
            left_result.schema.table_names().into_iter().collect();
        let right_schema_tables: std::collections::HashSet<String> =
            right_result.schema.table_names().into_iter().collect();

        predicate_plan
            .get_equijoin_conditions()
            .iter()
            .filter_map(|(left_table, _left_col, right_table, _right_col, expr)| {
                // Check if this equijoin connects tables from left and right
                let left_in_left = left_schema_tables.contains(left_table);
                let right_in_right = right_schema_tables.contains(right_table);
                let right_in_left = left_schema_tables.contains(right_table);
                let left_in_right = right_schema_tables.contains(left_table);

                if (left_in_left && right_in_right) || (right_in_left && left_in_right) {
                    Some(expr.clone())
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    // Perform nested loop join with equijoin predicates from WHERE clause
    // Save schemas for USING clause deduplication before join consumes them
    let (left_schema_for_using, right_schema_for_using) = if using_columns.is_some() {
        (Some(left_result.schema.clone()), Some(right_result.schema.clone()))
    } else {
        (None, None)
    };

    use crate::select::join::nested_loop_join;
    // Note: Using default timeout context - proper timeout propagation from SelectExecutor
    // is a future improvement (see issue #2631 for context)
    let timeout_ctx = TimeoutContext::new_default();
    // Issue #3562: Pass CTE context so post-join filters with IN subqueries can resolve CTEs
    // Note: We pass None for using_columns here because USING clause deduplication is handled
    // as post-processing below. The join condition was already built from the USING columns.
    let mut result = nested_loop_join(
        left_result,
        right_result,
        join_type,
        &effective_condition,
        natural,
        None, // USING clause deduplication handled below as post-processing
        database,
        &equijoin_predicates,
        &timeout_ctx,
        cte_results,
    )?;

    // For USING clause joins, remove duplicate columns from the result
    // (USING clause should only include join columns once as the "primary" column)
    //
    // Issue #4781: For RIGHT/FULL OUTER JOINs, the USING column should use COALESCE semantics:
    // - RIGHT OUTER JOIN: hide left-side USING column (use right as primary, since left may be
    //   NULL)
    // - FULL OUTER JOIN: coalesce left with right value, then hide right (standard behavior)
    // - LEFT OUTER / INNER: hide right-side USING column (standard behavior)
    if let (Some(using_cols), Some(ref left_schema), Some(ref right_schema)) =
        (using_columns.as_ref(), &left_schema_for_using, &right_schema_for_using)
    {
        result = remove_duplicate_columns_for_using_join(
            result,
            left_schema,
            right_schema,
            using_cols,
            join_type,
        )?;
    }

    Ok(result)
}

/// Mark USING columns as hidden for USING clause join result
///
/// USING clause should only include join columns once (from the left side) in `SELECT *`.
/// However, qualified wildcards like `SELECT t1.*` should still return all columns from t1.
///
/// This function marks USING columns from the right side as hidden in the schema,
/// rather than removing them. This allows:
/// - `SELECT *` to correctly deduplicate columns (skips hidden columns)
/// - `SELECT t1.*` to return all columns from t1 (includes hidden columns)
///
/// Issue #4466: table.* should return all columns in NATURAL JOIN context
fn remove_duplicate_columns_for_using_join(
    mut result: super::FromResult,
    left_schema: &CombinedSchema,
    right_schema: &CombinedSchema,
    using_columns: &[String],
    join_type: &vibesql_ast::JoinType,
) -> Result<super::FromResult, crate::errors::ExecutorError> {
    // Create a set of USING column names (lowercase for case-insensitive comparison)
    let using_cols_lower: HashSet<String> =
        using_columns.iter().map(|c| c.to_lowercase()).collect();

    // Mark all USING columns as "joined columns" so they won't be
    // considered ambiguous when referenced without table qualification (issue #4517)
    for col in using_columns {
        result.schema.add_joined_column(col);
    }

    // Count columns in left schema
    let left_col_count = left_schema.total_columns;

    // For RIGHT OUTER and FULL OUTER joins, we need COALESCE semantics:
    // The USING column value should be COALESCE(left.col, right.col)
    // - RIGHT OUTER: left can be NULL for unmatched rows from right
    // - FULL OUTER: either side can be NULL for unmatched rows
    // Issue #4783: USING column semantics differ from SQLite in OUTER JOINs
    let needs_coalesce =
        matches!(join_type, vibesql_ast::JoinType::RightOuter | vibesql_ast::JoinType::FullOuter);

    // Build a map of column name -> best left column index for coalesce pairs
    // For chained joins (e.g., RIGHT JOIN followed by FULL JOIN), prefer non-hidden
    // columns because hidden columns may be NULL from a previous outer join.
    // Issue #4798: Hidden columns from previous RIGHT/FULL JOINs should not be used.
    let mut leftmost_left_indices: std::collections::HashMap<String, (usize, bool)> =
        std::collections::HashMap::new();
    if needs_coalesce {
        for (table_start_idx, table_schema) in left_schema.table_schemas.values() {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let lowercase = col.name.to_lowercase();
                if using_cols_lower.contains(&lowercase) {
                    let absolute_idx = table_start_idx + col_idx;
                    let is_hidden = left_schema.hidden_columns.contains(&absolute_idx);

                    // Prefer non-hidden over hidden, then prefer leftmost
                    let should_update = match leftmost_left_indices.get(&lowercase) {
                        None => true,
                        Some((best_idx, best_is_hidden)) => {
                            if *best_is_hidden && !is_hidden {
                                true // prefer non-hidden
                            } else if !*best_is_hidden && is_hidden {
                                false // keep non-hidden
                            } else {
                                absolute_idx < *best_idx // same hidden status: prefer leftmost
                            }
                        }
                    };

                    if should_update {
                        leftmost_left_indices.insert(lowercase, (absolute_idx, is_hidden));
                    }
                }
            }
        }
    }

    // Issue #4798 (continued): For RIGHT JOIN USING, hide LEFT-side USING columns,
    // not right-side. This ensures subsequent JOINs on the same column use the
    // non-hidden column (which contains the actual value, not NULL).
    //
    // For LEFT/INNER JOIN: hide right-side columns (right can be NULL)
    // For RIGHT JOIN: hide left-side columns (left can be NULL for unmatched right rows)
    // For FULL JOIN: hide left-side columns (use right value; both could be NULL but
    //                the coalesce pair handles this for SELECT *)
    let is_right_or_full =
        matches!(join_type, vibesql_ast::JoinType::RightOuter | vibesql_ast::JoinType::FullOuter);

    if is_right_or_full {
        // For RIGHT/FULL JOIN: hide left-side USING columns
        // This ensures subsequent JOINs use the right-side column (which has the actual value)
        for (table_start_idx, table_schema) in left_schema.table_schemas.values() {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let lowercase = col.name.to_lowercase();
                if using_cols_lower.contains(&lowercase) {
                    let left_idx = table_start_idx + col_idx;
                    result.schema.hide_column(left_idx);
                }
            }
        }

        // Also register coalesce pairs for SELECT * expansion
        // The coalesce pair maps column name to (left_idx, right_idx) for COALESCE semantics
        // For the right side, prefer non-hidden columns (same logic as left side)
        let mut best_right_indices: std::collections::HashMap<String, (usize, bool)> =
            std::collections::HashMap::new();
        for (table_start_idx, table_schema) in right_schema.table_schemas.values() {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let lowercase = col.name.to_lowercase();
                if using_cols_lower.contains(&lowercase) {
                    let right_rel_idx = table_start_idx + col_idx;
                    let is_hidden = right_schema.hidden_columns.contains(&right_rel_idx);

                    // Prefer non-hidden over hidden, then prefer leftmost
                    let should_update = match best_right_indices.get(&lowercase) {
                        None => true,
                        Some((best_idx, best_is_hidden)) => {
                            if *best_is_hidden && !is_hidden {
                                true // prefer non-hidden
                            } else if !*best_is_hidden && is_hidden {
                                false // keep non-hidden
                            } else {
                                right_rel_idx < *best_idx // same hidden status: prefer leftmost
                            }
                        }
                    };

                    if should_update {
                        best_right_indices.insert(lowercase, (right_rel_idx, is_hidden));
                    }
                }
            }
        }

        // Now register coalesce pairs using the best left and right indices
        for (col_name, (right_rel_idx, _)) in &best_right_indices {
            let right_idx = left_col_count + right_rel_idx;
            if let Some(&(left_idx, _)) = leftmost_left_indices.get(col_name) {
                // Find original column name from using_columns for case preservation
                let original_name = using_columns
                    .iter()
                    .find(|c| c.to_lowercase() == *col_name)
                    .map(|s| s.as_str())
                    .unwrap_or(col_name);
                result.schema.add_using_coalesce_pair(original_name, left_idx, right_idx);
            }
        }
    } else {
        // For LEFT/INNER JOIN: hide right-side USING columns (original behavior)
        //
        // Issue #4786: Build map of leftmost left indices for coalesce pairs.
        // Even though LEFT/INNER joins don't need COALESCE semantics for output,
        // we need to add the left column to any existing coalesce chain from
        // nested USING joins. This ensures is_using_coalesce_right_side() correctly
        // filters out the nested join's visible column.
        let mut leftmost_left_indices: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (table_start_idx, table_schema) in left_schema.table_schemas.values() {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let lowercase = col.name.to_lowercase();
                if using_cols_lower.contains(&lowercase) {
                    let absolute_idx = table_start_idx + col_idx;
                    leftmost_left_indices
                        .entry(lowercase)
                        .and_modify(|e| {
                            if absolute_idx < *e {
                                *e = absolute_idx
                            }
                        })
                        .or_insert(absolute_idx);
                }
            }
        }

        for (table_start_idx, table_schema) in right_schema.table_schemas.values() {
            for (col_idx, col) in table_schema.columns.iter().enumerate() {
                let lowercase = col.name.to_lowercase();
                if using_cols_lower.contains(&lowercase) {
                    // This is a USING column, mark it as hidden for SELECT * expansion
                    // Position in result = left_col_count + position_in_right_schema
                    let right_idx = left_col_count + table_start_idx + col_idx;
                    result.schema.hide_column(right_idx);

                    // Issue #4786: Add coalesce pair so is_using_coalesce_right_side()
                    // correctly filters the nested join's visible column.
                    if let Some(&left_idx) = leftmost_left_indices.get(&lowercase) {
                        let original_name = using_columns
                            .iter()
                            .find(|c| c.to_lowercase() == lowercase)
                            .map(|s| s.as_str())
                            .unwrap_or(&col.name);
                        result.schema.add_using_coalesce_pair(original_name, left_idx, right_idx);
                    }
                }
            }
        }
    }

    Ok(result)
}

/// Generate the implicit join condition for a NATURAL JOIN
///
/// Finds all common column names between the left and right schemas (case-insensitive)
/// and creates an AND chain of equality conditions.
///
/// Returns None if there are no common columns (which means NATURAL JOIN should behave like CROSS
/// JOIN)
///
/// For self-joins (where left and right have the same table name), we use synthetic table
/// identifiers to ensure the condition correctly distinguishes between the left and right
/// instances.
fn generate_natural_join_condition(
    left_schema: &crate::schema::CombinedSchema,
    right_schema: &crate::schema::CombinedSchema,
) -> Result<Option<vibesql_ast::Expression>, ExecutorError> {
    use std::collections::HashMap;

    // Check if this is a self-join case (same table names on both sides)
    let left_table_names: std::collections::HashSet<_> =
        left_schema.table_schemas.keys().map(|k| k.canonical().to_lowercase()).collect();
    let right_table_names: std::collections::HashSet<_> =
        right_schema.table_schemas.keys().map(|k| k.canonical().to_lowercase()).collect();
    let is_self_join = !left_table_names.is_disjoint(&right_table_names);

    // Get all column names from left schema (normalized to lowercase for case-insensitive
    // comparison)
    // Store: lowercase_name -> (table, actual_name, absolute_index, collation)
    // IMPORTANT: For chained NATURAL JOINs (e.g., t1 NATURAL LEFT JOIN t2 NATURAL LEFT JOIN t3),
    // the left_schema may contain multiple tables with the same column name.
    // We must pick the LEFTMOST table (lowest absolute_index) to ensure we use the correct
    // value for the join condition. This matches SQLite's COALESCE semantics where
    // the leftmost non-NULL value is used for NATURAL JOIN columns.
    // Issue #4753: HashMap iteration order is non-deterministic, so we need to track
    // the absolute index and pick the leftmost occurrence.
    //
    // Issue #4798: For NATURAL RIGHT/FULL JOIN, the left-side columns are hidden and the
    // right-side columns contain the correct COALESCED values. We must prefer NON-HIDDEN
    // columns when there are multiple matches. This ensures the join condition uses
    // the correct column value.
    //
    // Issue #4845: For chained NATURAL RIGHT JOINs (e.g., t4 NATURAL RIGHT JOIN t5 NATURAL RIGHT
    // JOIN t6), when all matching columns are hidden, we must prefer REPLACEMENT TARGETS over
    // replacement sources. The replacement target contains the actual value that should be used
    // in the join condition. Example: After `t4 NATURAL RIGHT JOIN t5`, column_replacement_map
    // has {0 -> 2}, meaning t5.id (idx 2) holds the value. When the second join is processed,
    // we must use idx 2 (the target) rather than idx 0 (the source which is NULL for unmatched
    // rows).
    let replacement_targets: std::collections::HashSet<usize> =
        left_schema.column_replacement_map.values().copied().collect();

    let mut left_columns: HashMap<String, (String, String, usize, Option<String>, bool)> =
        HashMap::new();
    for (table_name, (table_start_idx, table_schema)) in &left_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let lowercase_name = col.name.to_lowercase();
            let absolute_idx = table_start_idx + col_offset;
            let is_hidden = left_schema.hidden_columns.contains(&absolute_idx);
            let is_replacement_target = replacement_targets.contains(&absolute_idx);

            // Prefer non-hidden columns over hidden ones (issue #4798)
            // Among hidden columns, prefer replacement targets (issue #4845)
            // Among columns with the same status, prefer leftmost (lowest index)
            let should_update = match left_columns.get(&lowercase_name) {
                None => true,
                Some((_, _, best_idx, _, best_is_hidden)) => {
                    let best_is_replacement_target = replacement_targets.contains(best_idx);

                    // Prefer non-hidden over hidden
                    if *best_is_hidden && !is_hidden {
                        true
                    } else if !*best_is_hidden && is_hidden {
                        false
                    } else if *best_is_hidden && is_hidden {
                        // Both hidden: prefer replacement target (issue #4845)
                        if is_replacement_target && !best_is_replacement_target {
                            true
                        } else if !is_replacement_target && best_is_replacement_target {
                            false
                        } else {
                            // Same replacement target status: prefer leftmost
                            absolute_idx < *best_idx
                        }
                    } else {
                        // Both non-hidden: prefer leftmost
                        absolute_idx < *best_idx
                    }
                }
            };

            if should_update {
                left_columns.insert(
                    lowercase_name,
                    (
                        table_name.to_string(),
                        col.name.clone(),
                        absolute_idx,
                        col.collation.clone(),
                        is_hidden,
                    ),
                );
            }
        }
    }

    // Find common column names from right schema
    // Store: (left_table, left_col, right_table, right_col, right_table_start_idx, collation)
    // Collation is taken from the left column (left takes precedence per SQLite semantics),
    // falling back to the right column's collation if the left has none.
    //
    // Issue #4909: We must deduplicate by column name. When the right side is a join result
    // (e.g., `c NATURAL FULL JOIN d`), both c.id and d.id exist in table_schemas but they
    // represent the same logical "id" column. We should only add ONE entry per column name.
    let mut common_columns: Vec<(String, String, String, String, usize, Option<String>)> =
        Vec::new();
    let mut matched_column_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for (table_name, (table_idx, table_schema)) in &right_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            // Skip if we've already matched this column name
            if matched_column_names.contains(&lowercase_name) {
                continue;
            }
            if let Some((left_table, left_col, _absolute_idx, left_collation, _is_hidden)) =
                left_columns.get(&lowercase_name)
            {
                // Found a common column - only use the leftmost occurrence from left schema
                let collation = left_collation.clone().or_else(|| col.collation.clone());
                common_columns.push((
                    left_table.clone(),
                    left_col.clone(),
                    table_name.to_string(),
                    col.name.clone(),
                    *table_idx,
                    collation,
                ));
                // Mark this column name as matched so we don't match it again
                matched_column_names.insert(lowercase_name);
            }
        }
    }

    // If no common columns, return None (NATURAL JOIN behaves like CROSS JOIN)
    if common_columns.is_empty() {
        return Ok(None);
    }

    // Issue #4903: Build index-to-column mapping for generating COALESCE expressions
    // in chained NATURAL FULL JOINs. When the left side already has a coalesce chain
    // for a column (e.g., from `t4 NATURAL FULL JOIN t6`), we need to use
    // COALESCE(t4.id, t6.id) in the join condition instead of just t4.id.
    let mut left_idx_to_col: HashMap<usize, (String, String)> = HashMap::new();
    for (table_name, (start_idx, table_schema)) in &left_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let absolute_idx = start_idx + col_offset;
            left_idx_to_col.insert(absolute_idx, (table_name.to_string(), col.name.clone()));
        }
    }

    // Issue #4909: Build index-to-column mapping for the right side as well
    // When the right side is a join result (e.g., `c NATURAL FULL JOIN d`), we need
    // to use COALESCE(c.id, d.id) in the join condition.
    let mut right_idx_to_col: HashMap<usize, (String, String)> = HashMap::new();
    for (table_name, (start_idx, table_schema)) in &right_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let absolute_idx = start_idx + col_offset;
            right_idx_to_col.insert(absolute_idx, (table_name.to_string(), col.name.clone()));
        }
    }

    // Build the join condition as an AND chain of equalities
    let mut condition: Option<vibesql_ast::Expression> = None;
    for (left_table, left_col, right_table, right_col, right_table_start, collation) in
        common_columns
    {
        // For self-joins, use synthetic table name for right side that matches
        // what CombinedSchema::merge will create
        let right_table_ref = if is_self_join {
            let adjusted_start = left_schema.total_columns + right_table_start;
            format!("__selfjoin_right_{}_{}", right_table.to_lowercase(), adjusted_start)
        } else {
            right_table
        };

        // Issue #4909: Build right expression with COALESCE if this column has a coalesce chain
        // For joins like `(a NATURAL FULL JOIN b) NATURAL FULL JOIN (c NATURAL FULL JOIN d)`,
        // the join condition must use COALESCE on BOTH sides:
        //   COALESCE(a.id, b.id) = COALESCE(c.id, d.id)
        // This ensures rows are properly matched when some columns in either chain are NULL.
        let right_col_lower = right_col.to_lowercase();
        let right_base_expr: vibesql_ast::Expression = if let Some(coalesce_indices) =
            right_schema.using_coalesce_indices.get(&right_col_lower)
        {
            if coalesce_indices.len() > 1 {
                // Build COALESCE function with all indexed columns from the chain
                let coalesce_args: Vec<vibesql_ast::Expression> = coalesce_indices
                    .iter()
                    .filter_map(|&idx| {
                        right_idx_to_col.get(&idx).map(|(table, col)| {
                            let table_ref = if is_self_join {
                                let adjusted_start = left_schema.total_columns + idx;
                                format!("__selfjoin_right_{}_{}", table.to_lowercase(), adjusted_start)
                            } else {
                                table.clone()
                            };
                            vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::qualified(&table_ref, false, col, false),
                            )
                        })
                    })
                    .collect();
                if coalesce_args.len() > 1 {
                    vibesql_ast::Expression::Function {
                        name: "COALESCE".into(),
                        args: coalesce_args,
                        character_unit: None,
                    }
                } else {
                    // Fallback to simple reference if we couldn't build COALESCE args
                    vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                        &right_table_ref,
                        false,
                        &right_col,
                        false,
                    ))
                }
            } else {
                // Single index, no COALESCE needed
                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    &right_table_ref,
                    false,
                    &right_col,
                    false,
                ))
            }
        } else {
            // No coalesce chain, use simple reference
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                &right_table_ref,
                false,
                &right_col,
                false,
            ))
        };

        // Apply COLLATE if the column has a collation defined
        let right_col_with_collate = if let Some(ref coll) = collation {
            vibesql_ast::Expression::Collate {
                expr: Box::new(right_base_expr),
                collation: coll.clone(),
            }
        } else {
            right_base_expr
        };

        // Issue #4903: Build left expression with COALESCE if this column has a coalesce chain
        // For chained NATURAL FULL JOINs like `t4 NATURAL FULL JOIN t6 NATURAL FULL JOIN t5`,
        // the join condition for the second join must be:
        //   COALESCE(t4.id, t6.id) = t5.id
        // instead of just:
        //   t4.id = t5.id
        // This ensures rows are properly matched even when some columns in the chain are NULL.
        let left_col_lower = left_col.to_lowercase();
        let left_expr: vibesql_ast::Expression = if let Some(coalesce_indices) =
            left_schema.using_coalesce_indices.get(&left_col_lower)
        {
            if coalesce_indices.len() > 1 {
                // Build COALESCE function with all indexed columns from the chain
                let coalesce_args: Vec<vibesql_ast::Expression> = coalesce_indices
                    .iter()
                    .filter_map(|&idx| {
                        left_idx_to_col.get(&idx).map(|(table, col)| {
                            vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::qualified(table, false, col, false),
                            )
                        })
                    })
                    .collect();
                if coalesce_args.len() > 1 {
                    vibesql_ast::Expression::Function {
                        name: "COALESCE".into(),
                        args: coalesce_args,
                        character_unit: None,
                    }
                } else {
                    // Fallback to simple reference if we couldn't build COALESCE args
                    vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                        &left_table,
                        false,
                        &left_col,
                        false,
                    ))
                }
            } else {
                // Single index, no COALESCE needed
                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    &left_table,
                    false,
                    &left_col,
                    false,
                ))
            }
        } else {
            // No coalesce chain, use simple reference
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                &left_table,
                false,
                &left_col,
                false,
            ))
        };

        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(left_expr),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(right_col_with_collate),
        };

        condition = Some(match condition {
            None => equality,
            Some(existing) => vibesql_ast::Expression::BinaryOp {
                left: Box::new(existing),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }

    Ok(condition)
}

/// Generate a join condition from USING columns with proper table qualification.
///
/// USING (col1, col2) is equivalent to:
/// `left_table.col1 = right_table.col1 AND left_table.col2 = right_table.col2`
///
/// This function finds the specified columns in both schemas and creates
/// properly qualified column references for the join condition.
///
/// For self-joins (where left and right have the same table name), we use
/// synthetic table identifiers that match what CombinedSchema::merge creates,
/// allowing the condition to correctly distinguish between left and right instances.
fn generate_using_join_condition(
    columns: &[String],
    left_schema: &crate::schema::CombinedSchema,
    right_schema: &crate::schema::CombinedSchema,
) -> Result<Option<vibesql_ast::Expression>, ExecutorError> {
    let mut condition: Option<vibesql_ast::Expression> = None;

    // Check if this is a self-join case (same table names on both sides)
    // In this case, we need to use column indices instead of table-qualified names
    // because both would resolve to the same column position in the combined schema.
    let left_table_names: std::collections::HashSet<_> =
        left_schema.table_schemas.keys().map(|k| k.canonical().to_lowercase()).collect();
    let right_table_names: std::collections::HashSet<_> =
        right_schema.table_schemas.keys().map(|k| k.canonical().to_lowercase()).collect();
    let is_self_join = !left_table_names.is_disjoint(&right_table_names);

    // Issue #4903: Build index-to-column mapping for generating COALESCE expressions
    // in chained USING JOINs. When the left side already has a coalesce chain
    // for a column, we need to use COALESCE in the join condition.
    let mut left_idx_to_col: HashMap<usize, (String, String)> = HashMap::new();
    for (table_name, (start_idx, table_schema)) in &left_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let absolute_idx = start_idx + col_offset;
            left_idx_to_col.insert(absolute_idx, (table_name.to_string(), col.name.clone()));
        }
    }

    // Issue #4909: Build index-to-column mapping for the right side as well
    // When the right side is a join result (e.g., `(t4 FULL JOIN t5 USING(id))`),
    // we need to use COALESCE in the join condition.
    let mut right_idx_to_col: HashMap<usize, (String, String)> = HashMap::new();
    for (table_name, (start_idx, table_schema)) in &right_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let absolute_idx = start_idx + col_offset;
            right_idx_to_col.insert(absolute_idx, (table_name.to_string(), col.name.clone()));
        }
    }

    for col_name in columns {
        let col_lower = col_name.to_lowercase();

        // Find column in left schema (case-insensitive) - also get absolute index and collation
        // IMPORTANT: For chained USING joins (e.g., t1 LEFT JOIN t2 USING(a) LEFT JOIN t3
        // USING(a)), the left_schema may contain multiple tables with the same column name.
        // We must pick the LEFTMOST table (lowest start_idx) to ensure we use the correct
        // value for the join condition. This matches SQLite's COALESCE semantics where
        // the leftmost non-NULL value is used for USING columns.
        // Issue #4753: HashMap iteration order is non-deterministic, so using find_map()
        // could pick t2.a (NULL from LEFT JOIN) instead of t1.a (actual value).
        //
        // Issue #4798: For RIGHT/FULL JOIN USING, the left-side columns are hidden and the
        // right-side columns contain the correct COALESCED values. We must prefer NON-HIDDEN
        // columns when there are multiple matches. This ensures the join condition uses
        // the correct column value (e.g., t4.a = 19 instead of t1.a = NULL for the a=19 row).
        let left_col = {
            let mut best_match: Option<(
                vibesql_catalog::TableIdentifier,
                String,
                usize,
                Option<String>,
                bool, // is_hidden
            )> = None;
            for (table_name, (start_idx, table_schema)) in &left_schema.table_schemas {
                for (col_offset, col) in table_schema.columns.iter().enumerate() {
                    if col.name.to_lowercase() == col_lower {
                        let absolute_idx = start_idx + col_offset;
                        let is_hidden = left_schema.hidden_columns.contains(&absolute_idx);

                        // Prefer non-hidden columns over hidden ones (issue #4798)
                        // Among columns with the same hidden status, prefer leftmost (lowest index)
                        let should_update = match &best_match {
                            None => true,
                            Some((_, _, _, _, best_is_hidden)) => {
                                // Prefer non-hidden over hidden
                                if *best_is_hidden && !is_hidden {
                                    true
                                } else if !*best_is_hidden && is_hidden {
                                    false
                                } else {
                                    // Same hidden status: prefer leftmost
                                    absolute_idx < best_match.as_ref().unwrap().2
                                }
                            }
                        };

                        if should_update {
                            best_match = Some((
                                table_name.clone(),
                                col.name.clone(),
                                absolute_idx,
                                col.collation.clone(),
                                is_hidden,
                            ));
                        }
                    }
                }
            }
            // Convert to the expected type (drop the is_hidden field)
            best_match.map(|(table, col, idx, collation, _)| (table, col, idx, collation))
        }
        .ok_or_else(|| ExecutorError::JoinUsingColumnNotPresent {
            column_name: col_name.to_string(),
        })?;

        // Find column in right schema (case-insensitive) - get absolute index, table start,
        // collation IMPORTANT: For nested USING joins (e.g., t2 INNER JOIN (t3 LEFT JOIN t4
        // USING(a)) USING(a)), the right_schema may contain multiple tables with the same
        // column name (t3.a, t4.a). We must prefer NON-HIDDEN columns and the LEFTMOST
        // (lowest index) column to ensure we use the correct value for the join condition.
        // This matches the coalesce semantics where the leftmost non-NULL value is used for
        // USING columns. Issue #4843: Using find_map() with HashMap has non-deterministic
        // iteration order, which could pick a NULL column (e.g., t4.a from LEFT JOIN)
        // instead of the correct coalesced value (e.g., t3.a).
        let right_col = {
            let mut best_match: Option<(
                vibesql_catalog::TableIdentifier,
                String,
                usize,
                usize,
                Option<String>,
                bool, // is_hidden
            )> = None;
            for (table_name, (start_idx, table_schema)) in &right_schema.table_schemas {
                for (col_offset, col) in table_schema.columns.iter().enumerate() {
                    if col.name.to_lowercase() == col_lower {
                        let absolute_idx = start_idx + col_offset;
                        let is_hidden = right_schema.hidden_columns.contains(&absolute_idx);

                        // Prefer non-hidden columns over hidden ones
                        // Among columns with the same hidden status, prefer leftmost (lowest index)
                        let should_update = match &best_match {
                            None => true,
                            Some((_, _, best_idx, _, _, best_is_hidden)) => {
                                // Prefer non-hidden over hidden
                                if *best_is_hidden && !is_hidden {
                                    true
                                } else if !*best_is_hidden && is_hidden {
                                    false
                                } else {
                                    // Same hidden status: prefer leftmost
                                    absolute_idx < *best_idx
                                }
                            }
                        };

                        if should_update {
                            best_match = Some((
                                table_name.clone(),
                                col.name.clone(),
                                absolute_idx,
                                *start_idx,
                                col.collation.clone(),
                                is_hidden,
                            ));
                        }
                    }
                }
            }
            // Convert to the expected type (drop the is_hidden field)
            best_match
                .map(|(table, col, idx, start, collation, _)| (table, col, idx, start, collation))
        }
        .ok_or_else(|| ExecutorError::JoinUsingColumnNotPresent {
            column_name: col_name.to_string(),
        })?;

        // Use left column's collation if present, otherwise use right column's
        let collation = left_col.3.clone().or(right_col.4.clone());

        // Create equality condition with table-qualified column references
        // For self-joins, we need to use a synthetic table name for the right side
        // that matches what CombinedSchema::merge will create.
        let right_table_name = if is_self_join {
            // Use the same synthetic key format as CombinedSchema::merge
            // Format: __selfjoin_right_{original_name}_{adjusted_start}
            // adjusted_start = left_schema.total_columns + table_start_index (not column index!)
            let adjusted_start = left_schema.total_columns + right_col.3;
            format!("__selfjoin_right_{}_{}", right_col.0.canonical(), adjusted_start)
        } else {
            right_col.0.to_string()
        };

        // Issue #4909: Build right expression with COALESCE if this column has a coalesce chain
        // For joins like `t3 FULL JOIN (t4 FULL JOIN t5 USING(id)) USING(id)`,
        // the join condition must use COALESCE on BOTH sides:
        //   COALESCE(t3.id, ...) = COALESCE(t4.id, t5.id)
        let right_base_expr: vibesql_ast::Expression = if let Some(coalesce_indices) =
            right_schema.using_coalesce_indices.get(&col_lower)
        {
            if coalesce_indices.len() > 1 {
                // Build COALESCE function with all indexed columns from the chain
                let coalesce_args: Vec<vibesql_ast::Expression> = coalesce_indices
                    .iter()
                    .filter_map(|&idx| {
                        right_idx_to_col.get(&idx).map(|(table, col)| {
                            let table_ref = if is_self_join {
                                let adjusted_start = left_schema.total_columns + idx;
                                format!("__selfjoin_right_{}_{}", table.to_lowercase(), adjusted_start)
                            } else {
                                table.clone()
                            };
                            vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::qualified(&table_ref, false, col, false),
                            )
                        })
                    })
                    .collect();
                if coalesce_args.len() > 1 {
                    vibesql_ast::Expression::Function {
                        name: "COALESCE".into(),
                        args: coalesce_args,
                        character_unit: None,
                    }
                } else {
                    // Fallback to simple reference
                    vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                        &right_table_name,
                        false,
                        &right_col.1,
                        false,
                    ))
                }
            } else {
                // Single index, no COALESCE needed
                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    &right_table_name,
                    false,
                    &right_col.1,
                    false,
                ))
            }
        } else {
            // No coalesce chain, use simple reference
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                &right_table_name,
                false,
                &right_col.1,
                false,
            ))
        };

        // Apply COLLATE if the column has a collation defined
        let right_col_with_collate = if let Some(ref coll) = collation {
            vibesql_ast::Expression::Collate {
                expr: Box::new(right_base_expr),
                collation: coll.clone(),
            }
        } else {
            right_base_expr
        };

        // Issue #4903: Build left expression with COALESCE if this column has a coalesce chain
        // For chained USING JOINs like `t4 FULL JOIN t6 USING(id) FULL JOIN t5 USING(id)`,
        // the join condition for the second join must be:
        //   COALESCE(t4.id, t6.id) = t5.id
        // instead of just:
        //   t4.id = t5.id
        let left_expr: vibesql_ast::Expression = if let Some(coalesce_indices) =
            left_schema.using_coalesce_indices.get(&col_lower)
        {
            if coalesce_indices.len() > 1 {
                // Build COALESCE function with all indexed columns from the chain
                let coalesce_args: Vec<vibesql_ast::Expression> = coalesce_indices
                    .iter()
                    .filter_map(|&idx| {
                        left_idx_to_col.get(&idx).map(|(table, col)| {
                            vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::qualified(table, false, col, false),
                            )
                        })
                    })
                    .collect();
                if coalesce_args.len() > 1 {
                    vibesql_ast::Expression::Function {
                        name: "COALESCE".into(),
                        args: coalesce_args,
                        character_unit: None,
                    }
                } else {
                    // Fallback to simple reference
                    vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                        &left_col.0.to_string(),
                        false,
                        &left_col.1,
                        false,
                    ))
                }
            } else {
                // Single index, no COALESCE needed
                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    &left_col.0.to_string(),
                    false,
                    &left_col.1,
                    false,
                ))
            }
        } else {
            // No coalesce chain, use simple reference
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                &left_col.0.to_string(),
                false,
                &left_col.1,
                false,
            ))
        };

        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(left_expr),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(right_col_with_collate),
        };

        condition = Some(match condition {
            None => equality,
            Some(existing) => vibesql_ast::Expression::BinaryOp {
                left: Box::new(existing),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(equality),
            },
        });
    }

    Ok(condition)
}

/// Validate that an ON clause doesn't reference tables that aren't in the current join.
///
/// SQLite requires that ON clauses only reference tables from the current join (left and right
/// sides). If the ON clause references a table that appears later in the FROM clause (to the
/// right), SQLite returns the error: "ON clause references tables to its right"
///
/// This validation checks two cases:
/// 1. Table-qualified references (e.g., `t3.x`) - the table must exist in left or right schema
/// 2. Unqualified references (e.g., `b`) - if the column doesn't exist in left/right schemas but
///    DOES exist in some table in the database, it's referencing a table to the right
///
/// Example that should fail:
/// ```sql
/// SELECT * FROM t1 INNER JOIN t2 ON t2.a = t3.x INNER JOIN t3
/// ```
/// The ON clause for the t1/t2 join references t3, which hasn't been introduced yet.
///
/// The `join_alias` parameter is the alias for the parenthesized join expression on the right
/// side (e.g., for `t1 JOIN (...) AS j1 ON j1.id = t1.id`, the alias is "j1").
/// Issue #4786: This alias must be recognized as a valid table reference.
fn validate_on_clause_table_references(
    on_condition: &vibesql_ast::Expression,
    left_schema: &CombinedSchema,
    right_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    join_alias: Option<&str>,
) -> Result<(), ExecutorError> {
    // Collect all table names from both schemas (lowercase for case-insensitive comparison)
    let mut valid_tables: HashSet<String> = HashSet::new();
    for table_id in left_schema.table_schemas.keys() {
        valid_tables.insert(table_id.canonical().to_lowercase());
    }
    for table_id in right_schema.table_schemas.keys() {
        valid_tables.insert(table_id.canonical().to_lowercase());
    }
    // Issue #4786: Add the join alias (if any) to valid tables so that references like
    // `j1.column` are recognized when the right side is a parenthesized join with alias.
    if let Some(alias) = join_alias {
        valid_tables.insert(alias.to_lowercase());
    }

    // Collect all column names from both schemas (lowercase for case-insensitive comparison)
    let mut valid_columns: HashSet<String> = HashSet::new();
    for (_, (_, table_schema)) in &left_schema.table_schemas {
        for col in &table_schema.columns {
            valid_columns.insert(col.name.to_lowercase());
        }
    }
    for (_, (_, table_schema)) in &right_schema.table_schemas {
        for col in &table_schema.columns {
            valid_columns.insert(col.name.to_lowercase());
        }
    }

    // Collect all column names from all tables in the database (for detecting right-table
    // references)
    let mut all_db_columns: HashSet<String> = HashSet::new();
    for table_name in database.list_tables() {
        if let Some(table) = database.get_table(&table_name) {
            for col in &table.schema.columns {
                all_db_columns.insert(col.name.to_lowercase());
            }
        }
    }

    // Check if the ON condition references any tables/columns not in valid sets
    if on_clause_references_unknown_table_or_column(
        on_condition,
        &valid_tables,
        &valid_columns,
        &all_db_columns,
    ) {
        return Err(ExecutorError::OnClauseReferencesRightTable);
    }

    Ok(())
}

/// Recursively check if an expression references a table not in the valid set.
///
/// For table-qualified references (e.g., `t3.x`): checks if the table is in valid_tables.
/// For unqualified references (e.g., `b`): checks if the column exists in valid_columns.
///   - If it exists in valid_columns, it's OK (column is in current join)
///   - If it doesn't exist in valid_columns BUT exists in all_db_columns, it's referencing a table
///     to the right (error)
///   - If it doesn't exist in either, it might be a SELECT alias (allow, will fail later if
///     invalid)
fn on_clause_references_unknown_table_or_column(
    expr: &vibesql_ast::Expression,
    valid_tables: &HashSet<String>,
    valid_columns: &HashSet<String>,
    all_db_columns: &HashSet<String>,
) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => {
            if let Some(table_name) = col_id.table_canonical() {
                // Table-qualified: check if the table is valid
                let table_lower = table_name.to_lowercase();
                !valid_tables.contains(&table_lower)
            } else {
                // Unqualified column: check if it references a table to the right
                let col_lower = col_id.column_canonical().to_lowercase();
                if valid_columns.contains(&col_lower) {
                    // Column exists in current join schemas - OK
                    false
                } else if all_db_columns.contains(&col_lower) {
                    // Column doesn't exist in current schemas but exists in some DB table
                    // This means it's referencing a table to the right
                    true
                } else {
                    // Column doesn't exist anywhere - might be a SELECT alias
                    // Allow it through, will fail later if truly invalid
                    false
                }
            }
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            on_clause_references_unknown_table_or_column(
                left,
                valid_tables,
                valid_columns,
                all_db_columns,
            ) || on_clause_references_unknown_table_or_column(
                right,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            ) || on_clause_references_unknown_table_or_column(
                low,
                valid_tables,
                valid_columns,
                all_db_columns,
            ) || on_clause_references_unknown_table_or_column(
                high,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            ) || values.iter().any(|e| {
                on_clause_references_unknown_table_or_column(
                    e,
                    valid_tables,
                    valid_columns,
                    all_db_columns,
                )
            })
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand
                .as_ref()
                .map(|e| {
                    on_clause_references_unknown_table_or_column(
                        e,
                        valid_tables,
                        valid_columns,
                        all_db_columns,
                    )
                })
                .unwrap_or(false)
                || when_clauses.iter().any(|case_when| {
                    case_when.conditions.iter().any(|c| {
                        on_clause_references_unknown_table_or_column(
                            c,
                            valid_tables,
                            valid_columns,
                            all_db_columns,
                        )
                    }) || on_clause_references_unknown_table_or_column(
                        &case_when.result,
                        valid_tables,
                        valid_columns,
                        all_db_columns,
                    )
                })
                || else_result
                    .as_ref()
                    .map(|e| {
                        on_clause_references_unknown_table_or_column(
                            e,
                            valid_tables,
                            valid_columns,
                            all_db_columns,
                        )
                    })
                    .unwrap_or(false)
        }
        vibesql_ast::Expression::Function { args, .. } => args.iter().any(|e| {
            on_clause_references_unknown_table_or_column(
                e,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }),
        vibesql_ast::Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(|e| {
                on_clause_references_unknown_table_or_column(
                    e,
                    valid_tables,
                    valid_columns,
                    all_db_columns,
                )
            }) || filter
                .as_ref()
                .map(|e| {
                    on_clause_references_unknown_table_or_column(
                        e,
                        valid_tables,
                        valid_columns,
                        all_db_columns,
                    )
                })
                .unwrap_or(false)
        }
        vibesql_ast::Expression::WindowFunction { function, over } => {
            let func_refs = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, filter, .. } => {
                    args.iter().any(|e| {
                        on_clause_references_unknown_table_or_column(
                            e,
                            valid_tables,
                            valid_columns,
                            all_db_columns,
                        )
                    }) || filter
                        .as_ref()
                        .map(|e| {
                            on_clause_references_unknown_table_or_column(
                                e,
                                valid_tables,
                                valid_columns,
                                all_db_columns,
                            )
                        })
                        .unwrap_or(false)
                }
                vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args.iter().any(|e| {
                    on_clause_references_unknown_table_or_column(
                        e,
                        valid_tables,
                        valid_columns,
                        all_db_columns,
                    )
                }),
            };
            func_refs
                || over
                    .partition_by
                    .as_ref()
                    .map(|exprs| {
                        exprs.iter().any(|e| {
                            on_clause_references_unknown_table_or_column(
                                e,
                                valid_tables,
                                valid_columns,
                                all_db_columns,
                            )
                        })
                    })
                    .unwrap_or(false)
                || over
                    .order_by
                    .as_ref()
                    .map(|items| {
                        items.iter().any(|item| {
                            on_clause_references_unknown_table_or_column(
                                &item.expr,
                                valid_tables,
                                valid_columns,
                                all_db_columns,
                            )
                        })
                    })
                    .unwrap_or(false)
        }
        vibesql_ast::Expression::Collate { expr, .. } => {
            on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::Cast { expr, .. } => on_clause_references_unknown_table_or_column(
            expr,
            valid_tables,
            valid_columns,
            all_db_columns,
        ),
        vibesql_ast::Expression::ScalarSubquery(select_stmt) => {
            // Pass only outer context - subquery_references_unknown_table will build up
            // the subquery's own tables as it checks nested joins
            subquery_references_unknown_table(
                select_stmt,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::Expression::In { expr, subquery, .. } => {
            if on_clause_references_unknown_table_or_column(
                expr,
                valid_tables,
                valid_columns,
                all_db_columns,
            ) {
                return true;
            }
            // Pass only outer context
            subquery_references_unknown_table(subquery, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::Exists { subquery, .. } => {
            // Pass only outer context
            subquery_references_unknown_table(subquery, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::Conjunction(exprs)
        | vibesql_ast::Expression::Disjunction(exprs) => exprs.iter().any(|e| {
            on_clause_references_unknown_table_or_column(
                e,
                valid_tables,
                valid_columns,
                all_db_columns,
            )
        }),
        _ => false,
    }
}

/// Collect all table names from a FromClause (recursively for joins)
fn collect_tables_from_from_clause(
    from: &Option<vibesql_ast::FromClause>,
    tables: &mut HashSet<String>,
) {
    if let Some(from_clause) = from {
        collect_tables_from_from_clause_inner(from_clause, tables);
    }
}

fn collect_tables_from_from_clause_inner(
    from: &vibesql_ast::FromClause,
    tables: &mut HashSet<String>,
) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            let table_name = alias.as_ref().unwrap_or(name);
            tables.insert(table_name.to_lowercase());
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            collect_tables_from_from_clause_inner(left, tables);
            collect_tables_from_from_clause_inner(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.insert(alias.to_lowercase());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            tables.insert(alias.to_lowercase());
        }
    }
}

/// Check if a subquery's column references point to tables not in valid_tables
fn subquery_references_unknown_table(
    select_stmt: &vibesql_ast::SelectStmt,
    valid_tables: &HashSet<String>,
    valid_columns: &HashSet<String>,
    all_db_columns: &HashSet<String>,
) -> bool {
    // For checking SELECT/WHERE/HAVING, use all tables from this subquery
    // But for FROM clause, pass only outer context so nested joins are checked properly
    let mut subquery_all_tables = valid_tables.clone();
    collect_tables_from_from_clause(&select_stmt.from, &mut subquery_all_tables);

    for item in &select_stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if on_clause_references_unknown_table_or_column(
                expr,
                &subquery_all_tables,
                valid_columns,
                all_db_columns,
            ) {
                return true;
            }
        }
    }

    if let Some(where_expr) = &select_stmt.where_clause {
        if on_clause_references_unknown_table_or_column(
            where_expr,
            &subquery_all_tables,
            valid_columns,
            all_db_columns,
        ) {
            return true;
        }
    }

    if let Some(having_expr) = &select_stmt.having {
        if on_clause_references_unknown_table_or_column(
            having_expr,
            &subquery_all_tables,
            valid_columns,
            all_db_columns,
        ) {
            return true;
        }
    }

    // For FROM clause, pass only outer context's valid tables
    if let Some(from) = &select_stmt.from {
        if from_clause_references_unknown_table(from, valid_tables, valid_columns, all_db_columns) {
            return true;
        }
    }

    false
}

/// Check if a FROM clause (including nested joins) has ON conditions referencing unknown tables
fn from_clause_references_unknown_table(
    from: &vibesql_ast::FromClause,
    valid_tables: &HashSet<String>,
    valid_columns: &HashSet<String>,
    all_db_columns: &HashSet<String>,
) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } => false,
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            // Build valid tables for this join's ON clause: outer context + left + right of THIS
            // join only
            let mut join_valid_tables = valid_tables.clone();
            collect_tables_from_from_clause_inner(left, &mut join_valid_tables);
            collect_tables_from_from_clause_inner(right, &mut join_valid_tables);

            if let Some(cond) = condition {
                if on_clause_references_unknown_table_or_column(
                    cond,
                    &join_valid_tables,
                    valid_columns,
                    all_db_columns,
                ) {
                    return true;
                }
            }
            // Recursively check left side (with only outer context)
            // Then check right side (with outer context + left tables)
            let mut left_context = valid_tables.clone();
            if from_clause_references_unknown_table(
                left,
                &left_context,
                valid_columns,
                all_db_columns,
            ) {
                return true;
            }
            collect_tables_from_from_clause_inner(left, &mut left_context);
            from_clause_references_unknown_table(
                right,
                &left_context,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::FromClause::Subquery { query, .. } => {
            let mut subquery_valid_tables = valid_tables.clone();
            collect_tables_from_from_clause(&query.from, &mut subquery_valid_tables);
            subquery_references_unknown_table(
                query,
                &subquery_valid_tables,
                valid_columns,
                all_db_columns,
            )
        }
        vibesql_ast::FromClause::Values { .. } => false,
    }
}
