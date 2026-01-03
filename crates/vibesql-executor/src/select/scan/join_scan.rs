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

use ahash::AHashSet;

use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    optimizer::{
        combine_with_and,
        where_pushdown::{extract_referenced_tables_branch, flatten_conjuncts},
        PredicatePlan,
    },
    schema::CombinedSchema,
    select::cte::CteResult,
    timeout::TimeoutContext,
};

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
                filter_out_nullable_side_predicates(left, where_expr, database)
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
                should_use_inl_for_semi_join(left_row_count, right, condition, database);

            if should_use_inl {
                // Try exact-match INL first (requires full PK coverage)
                if let Some(result) =
                    try_index_nested_loop_semi_join(&left_result, right, condition, database)?
                {
                    return Ok(result);
                }

                // Try prefix-scan INL (works with partial index key match)
                // This handles cases like TPC-H Q4 where we join on L_ORDERKEY but the
                // index is on (L_ORDERKEY, L_LINENUMBER) - we can still use a prefix scan
                if let Some(result) =
                    try_prefix_scan_semi_join(&left_result, right, condition, database)?
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
                extract_right_only_predicates(right, cond, database)
            } else {
                None
            }
        }
        vibesql_ast::JoinType::LeftOuter | vibesql_ast::JoinType::FullOuter => {
            // For LEFT/FULL OUTER JOIN: Remove predicates that reference ONLY the right table
            // These must be evaluated post-join when NULL values from unmatched rows are present
            if let Some(where_expr) = where_clause {
                filter_out_nullable_side_predicates(right, where_expr, database)
            } else {
                None
            }
        }
        _ => where_clause.cloned(),
    };
    let right_result = super::execute_from_clause(
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
            validate_on_clause_table_references(
                on_condition,
                &left_result.schema,
                &right_result.schema,
                database,
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
    // - RIGHT OUTER JOIN: hide left-side USING column (use right as primary, since left may be NULL)
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
    let needs_coalesce = matches!(
        join_type,
        vibesql_ast::JoinType::RightOuter | vibesql_ast::JoinType::FullOuter
    );

    // Build a map of column name -> leftmost left column index
    // This is the index that unqualified references will resolve to
    let mut leftmost_left_indices: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    if needs_coalesce {
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
    }

    // Identify which columns from the right side are USING columns to hide
    // Use table_start_idx from right_schema to compute correct absolute positions
    for (table_start_idx, table_schema) in right_schema.table_schemas.values() {
        for (col_idx, col) in table_schema.columns.iter().enumerate() {
            let lowercase = col.name.to_lowercase();
            if using_cols_lower.contains(&lowercase) {
                // This is a USING column, mark it as hidden for SELECT * expansion
                // Position in result = left_col_count + position_in_right_schema
                let right_idx = left_col_count + table_start_idx + col_idx;
                result.schema.hide_column(right_idx);

                // If coalescing is needed, register the coalesce pair in the schema
                // This maps the column name to (left_idx, right_idx) so the evaluator
                // can apply COALESCE semantics for unqualified column references
                if needs_coalesce {
                    if let Some(&left_idx) = leftmost_left_indices.get(&lowercase) {
                        result
                            .schema
                            .add_using_coalesce_pair(&col.name, left_idx, right_idx);
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
/// identifiers to ensure the condition correctly distinguishes between the left and right instances.
fn generate_natural_join_condition(
    left_schema: &crate::schema::CombinedSchema,
    right_schema: &crate::schema::CombinedSchema,
) -> Result<Option<vibesql_ast::Expression>, ExecutorError> {
    use std::collections::HashMap;

    // Check if this is a self-join case (same table names on both sides)
    let left_table_names: std::collections::HashSet<_> = left_schema
        .table_schemas
        .keys()
        .map(|k| k.canonical().to_lowercase())
        .collect();
    let right_table_names: std::collections::HashSet<_> = right_schema
        .table_schemas
        .keys()
        .map(|k| k.canonical().to_lowercase())
        .collect();
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
    let mut left_columns: HashMap<String, (String, String, usize, Option<String>)> = HashMap::new();
    for (table_name, (table_start_idx, table_schema)) in &left_schema.table_schemas {
        for (col_offset, col) in table_schema.columns.iter().enumerate() {
            let lowercase_name = col.name.to_lowercase();
            let absolute_idx = table_start_idx + col_offset;
            // Only keep the leftmost occurrence (lowest absolute_idx)
            if !left_columns.contains_key(&lowercase_name)
                || absolute_idx < left_columns.get(&lowercase_name).unwrap().2
            {
                left_columns.insert(
                    lowercase_name,
                    (
                        table_name.to_string(),
                        col.name.clone(),
                        absolute_idx,
                        col.collation.clone(),
                    ),
                );
            }
        }
    }

    // Find common column names from right schema
    // Store: (left_table, left_col, right_table, right_col, right_table_start_idx, collation)
    // Collation is taken from the left column (left takes precedence per SQLite semantics),
    // falling back to the right column's collation if the left has none.
    let mut common_columns: Vec<(String, String, String, String, usize, Option<String>)> =
        Vec::new();
    for (table_name, (table_idx, table_schema)) in &right_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            if let Some((left_table, left_col, _absolute_idx, left_collation)) =
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
            }
        }
    }

    // If no common columns, return None (NATURAL JOIN behaves like CROSS JOIN)
    if common_columns.is_empty() {
        return Ok(None);
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
            format!(
                "__selfjoin_right_{}_{}",
                right_table.to_lowercase(),
                adjusted_start
            )
        } else {
            right_table
        };

        // Build right column expression, applying COLLATE if the column has a collation
        // defined. This ensures NATURAL JOIN respects column-level COLLATE declarations.
        let right_col_expr = vibesql_ast::Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::qualified(&right_table_ref, false, &right_col, false),
        );
        let right_col_with_collate = if let Some(ref coll) = collation {
            vibesql_ast::Expression::Collate {
                expr: Box::new(right_col_expr),
                collation: coll.clone(),
            }
        } else {
            right_col_expr
        };

        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified(&left_table, false, &left_col, false),
            )),
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

/// Extract right-table-only predicates from a join condition for SEMI/ANTI join pushdown
///
/// For IN subquery to SEMI JOIN conversions, the subquery's WHERE clause predicates are
/// combined into the JOIN condition. This function extracts predicates that only reference
/// the right-side table(s) so they can be pushed down to the right-side scan for index selection.
///
/// Example: For `WHERE ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)`
/// The JOIN condition is: `ol_i_id = s_i_id AND s_w_id = ? AND s_quantity < ?`
/// This function extracts: `s_w_id = ? AND s_quantity < ?` (right-only predicates)
///
/// Returns None if no right-only predicates can be extracted.
fn extract_right_only_predicates(
    right_from: &vibesql_ast::FromClause,
    condition: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Option<vibesql_ast::Expression> {
    // Get the table name(s) from the right-side FROM clause
    let right_tables = extract_table_names_from_from_clause(right_from);
    if right_tables.is_empty() {
        return None;
    }

    // We need to build a minimal schema to analyze predicates
    // For each table, get its schema from the database
    let mut right_table_set: HashSet<String> = HashSet::new();
    let mut schema_builder = crate::schema::SchemaBuilder::new();

    for table_name in &right_tables {
        // Normalize table name for lookup
        let normalized = table_name.to_lowercase();
        if let Some(table) = database.get_table(&normalized) {
            // Add table to schema builder (duplicates are tracked, not rejected)
            schema_builder.add_table(table_name.clone(), table.schema.clone());
            right_table_set.insert(table_name.clone());
            // Also add normalized version
            right_table_set.insert(normalized);
        }
    }

    if right_table_set.is_empty() {
        return None;
    }

    let right_schema = schema_builder.build();

    // Flatten the condition into conjuncts (AND-separated predicates)
    let conjuncts = flatten_conjuncts(condition);

    // Filter to keep only predicates that reference only right-side tables
    let right_only_predicates: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // extract_referenced_tables_branch returns Option<HashSet<String>>
            // None means the expression couldn't be analyzed (skip it)
            // Some(empty set) means no tables referenced (skip it)
            // Some(non-empty set) - check if all tables are in right_table_set
            match extract_referenced_tables_branch(pred, &right_schema) {
                Some(ref tables) if !tables.is_empty() => tables.iter().all(|t| {
                    let t_lower = t.to_lowercase();
                    right_table_set.contains(t) || right_table_set.contains(&t_lower)
                }),
                _ => false,
            }
        })
        .collect();

    if right_only_predicates.is_empty() {
        return None;
    }

    // Combine predicates with AND
    combine_with_and(right_only_predicates)
}

/// Filter out predicates that reference ONLY the nullable side of an outer join.
///
/// For LEFT/FULL OUTER JOIN, the right side is "nullable" - it can produce NULL values
/// for unmatched rows. Predicates like `right.col IS NULL` test for these join-produced NULLs,
/// not for NULL values stored in the table. If we push such predicates to the right-side scan,
/// they filter on stored NULLs instead of join-produced NULLs, causing incorrect results.
///
/// This function takes a WHERE clause and returns a modified version with nullable-side-only
/// predicates removed. These predicates will be evaluated post-join instead.
///
/// Example: For `LEFT JOIN t2 ON t1.id = t2.tid WHERE t2.tid IS NULL`
/// - Input: `t2.tid IS NULL`
/// - Output: None (the entire predicate references only the nullable side)
///
/// Example: For `LEFT JOIN t2 ON t1.id = t2.tid WHERE t1.id > 5 AND t2.tid IS NULL`
/// - Input: `t1.id > 5 AND t2.tid IS NULL`
/// - Output: `t1.id > 5` (keep left-side predicate, remove right-side predicate)
fn filter_out_nullable_side_predicates(
    nullable_side_from: &vibesql_ast::FromClause,
    where_expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Option<vibesql_ast::Expression> {
    // Get the table name(s) from the nullable-side FROM clause
    let nullable_tables = extract_table_names_from_from_clause(nullable_side_from);
    if nullable_tables.is_empty() {
        // No tables to filter, return original expression
        return Some(where_expr.clone());
    }

    // Build a schema and table set for the nullable side
    let mut nullable_table_set: HashSet<String> = HashSet::new();
    let mut schema_builder = crate::schema::SchemaBuilder::new();

    for table_name in &nullable_tables {
        // Normalize table name for lookup
        let normalized = table_name.to_lowercase();
        if let Some(table) = database.get_table(&normalized) {
            // Add table to schema builder (duplicates are tracked, not rejected)
            schema_builder.add_table(table_name.clone(), table.schema.clone());
            nullable_table_set.insert(table_name.clone());
            // Also add normalized version for case-insensitive matching
            nullable_table_set.insert(normalized);
        } else {
            // Table not found in database - might be a subquery alias
            // Still add to the set so we can check column references
            nullable_table_set.insert(table_name.clone());
            nullable_table_set.insert(normalized);
        }
    }

    let nullable_schema = schema_builder.build();

    // Flatten the WHERE clause into conjuncts (AND-separated predicates)
    let conjuncts = flatten_conjuncts(where_expr);

    // Filter to KEEP predicates that do NOT reference only nullable-side tables
    // (i.e., remove predicates that reference ONLY the nullable side)
    let kept_predicates: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // extract_referenced_tables_branch returns Option<HashSet<String>>
            // None means the expression couldn't be analyzed (keep it, evaluate post-join)
            // Some(empty set) means no tables referenced (keep it - e.g., constant expression)
            // Some(non-empty set) - check if ALL tables are in nullable_table_set
            //   If ALL are nullable-side-only -> REMOVE (don't keep)
            //   If ANY are from other tables -> KEEP
            match extract_referenced_tables_branch(pred, &nullable_schema) {
                Some(ref tables) if !tables.is_empty() => {
                    // Check if ALL referenced tables are in the nullable set
                    let all_nullable = tables.iter().all(|t| {
                        let t_lower = t.to_lowercase();
                        nullable_table_set.contains(t) || nullable_table_set.contains(&t_lower)
                    });
                    // KEEP if NOT all-nullable (i.e., at least one non-nullable table referenced)
                    !all_nullable
                }
                Some(_) => true, // Empty set - keep (no table refs, e.g., literals)
                None => {
                    // Couldn't analyze - check if predicate mentions nullable table columns
                    // For safety, keep predicates we can't analyze and evaluate post-join
                    // But first, do a simple check for column references
                    !predicate_references_only_tables(pred, &nullable_table_set)
                }
            }
        })
        .collect();

    // Combine remaining predicates with AND (returns None if empty)
    combine_with_and(kept_predicates)
}

/// Simple check if a predicate references only tables from a given set.
/// This is a fallback for when extract_referenced_tables_branch returns None.
fn predicate_references_only_tables(
    expr: &vibesql_ast::Expression,
    table_set: &HashSet<String>,
) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() =>
        {
            let t = col_id.table_canonical().unwrap();
            let t_lower = t.to_lowercase();
            table_set.contains(t) || table_set.contains(&t_lower)
        }
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            // Unqualified column - can't determine which table, assume it might be from nullable
            // side Return false to be conservative (keep the predicate)
            false
        }
        vibesql_ast::Expression::ColumnRef(_) => {
            // Schema-qualified or other cases, be conservative
            false
        }
        vibesql_ast::Expression::IsNull { expr: inner, .. } => {
            predicate_references_only_tables(inner, table_set)
        }
        vibesql_ast::Expression::UnaryOp { expr: inner, .. } => {
            predicate_references_only_tables(inner, table_set)
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            // For binary ops, check if BOTH sides reference only the table set
            let left_only = predicate_references_only_tables(left, table_set);
            let right_only = predicate_references_only_tables(right, table_set);
            // If either side references other tables, this is not "only" nullable-side
            left_only && right_only
        }
        vibesql_ast::Expression::Literal(_) => {
            // Literals don't reference any table
            true
        }
        // For other expression types, be conservative
        _ => false,
    }
}

/// Extract table names from a FROM clause
fn extract_table_names_from_from_clause(from: &vibesql_ast::FromClause) -> Vec<String> {
    let mut tables = Vec::new();
    collect_table_names(from, &mut tables);
    tables
}

fn collect_table_names(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            // Use alias if present, otherwise use table name
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            collect_table_names(left, tables);
            collect_table_names(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            // Subquery alias is required (String, not Option<String>)
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            // VALUES alias is required (String, not Option<String>)
            tables.push(alias.clone());
        }
    }
}

/// Determine whether to use Index Nested Loop (INL) for a semi-join.
///
/// This implements a cost-based decision between INL and hash semi-join:
///
/// 1. **Always use INL** when left_rows < INL_BASE_THRESHOLD (1000)
///    - Small left side means few index lookups, definitely faster than hash
///
/// 2. **Use INL when right table is much larger** (right_rows / left_rows > ratio threshold)
///    - Hash join scans the entire right table to build hash table
///    - INL does left_rows index lookups, each returning ~(right_rows/distinct_keys) rows
///    - For semi-join, we stop at first match, so avg cost is even lower
///
/// 3. **Cap at INL_MAX_THRESHOLD** to prevent excessive random I/O
///
/// Example (TPC-H Q4):
/// - left_rows = 5,406 (orders after date filter)
/// - right_rows = 600,000 (lineitem)
/// - ratio = 600,000 / 5,406 ≈ 111 >> 10
/// - Decision: Use INL (index lookups) instead of hash (full lineitem scan)
fn should_use_inl_for_semi_join(
    left_row_count: usize,
    right_from: &vibesql_ast::FromClause,
    _condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> bool {
    let debug = std::env::var("INL_DECISION_DEBUG").is_ok();

    // Rule 1: Always use INL for small left sides
    if left_row_count < INL_BASE_THRESHOLD {
        if debug {
            eprintln!(
                "[INL_DECISION] left_rows={} < base_threshold={}, using INL",
                left_row_count, INL_BASE_THRESHOLD
            );
        }
        return true;
    }

    // Rule 3: Never use INL for very large left sides (too much random I/O)
    if left_row_count > INL_MAX_THRESHOLD {
        if debug {
            eprintln!(
                "[INL_DECISION] left_rows={} > max_threshold={}, using hash join",
                left_row_count, INL_MAX_THRESHOLD
            );
        }
        return false;
    }

    // Rule 2: Use cost-based decision for medium-sized left sides
    // Get right table cardinality
    let right_row_count = match right_from {
        vibesql_ast::FromClause::Table { name, .. } => {
            database.get_table(name).map(|t| t.row_count()).unwrap_or(0)
        }
        _ => {
            // Complex right side (subquery/join), can't estimate, use hash join
            if debug {
                eprintln!("[INL_DECISION] Complex right side, using hash join");
            }
            return false;
        }
    };

    // Calculate size ratio
    let ratio = if left_row_count > 0 { right_row_count / left_row_count } else { 0 };

    // Decision: Use INL if right table is significantly larger than left
    let use_inl = ratio >= INL_SIZE_RATIO_THRESHOLD;

    if debug {
        eprintln!(
            "[INL_DECISION] left_rows={}, right_rows={}, ratio={}, threshold={}, decision={}",
            left_row_count,
            right_row_count,
            ratio,
            INL_SIZE_RATIO_THRESHOLD,
            if use_inl { "INL" } else { "hash" }
        );
    }

    use_inl
}

/// Try to execute a SEMI join using Index Nested Loop (INL) strategy.
///
/// This optimization is used when:
/// 1. The left side is small (< INL_BASE_THRESHOLD rows)
/// 2. The right side is a simple table (not a subquery or join)
/// 3. There's an equi-join condition on a column with an index
///
/// For each distinct join key from the left side, we do point lookups on the
/// right table instead of scanning all matching rows.
///
/// Returns Some(result) if INL was used successfully, None to fall back to hash join.
fn try_index_nested_loop_semi_join(
    left_result: &super::FromResult,
    right_from: &vibesql_ast::FromClause,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<super::FromResult>, ExecutorError> {
    // Must have a join condition
    let cond = match condition {
        Some(c) => c,
        None => return Ok(None),
    };

    // Right side must be a simple table (not a join or subquery)
    let (right_table_name, _right_alias) = match right_from {
        vibesql_ast::FromClause::Table { name, alias, .. } => (name.clone(), alias.clone()),
        _ => return Ok(None), // Complex right side, can't use INL
    };

    // Get the right table
    let right_table = match database.get_table(&right_table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Parse the join condition to extract:
    // 1. The equi-join columns (left_col = right_col)
    // 2. Additional filter predicates on the right table
    let (equi_join, right_filters) =
        match parse_semi_join_condition(cond, left_result, &right_table_name) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };

    // Check if the right table has a usable index for point lookups
    // We need an index that starts with the join key column
    let pk_columns = match &right_table.schema.primary_key {
        Some(cols) => cols.clone(),
        None => return Ok(None), // No primary key, can't use INL
    };

    // For TPC-C Stock-Level: STOCK has PK (s_w_id, s_i_id)
    // The join is on s_i_id, but we also have s_w_id = ? in the filter
    // So we can build a composite key for point lookups

    // Try to extract constant predicates from right_filters that can be combined with join key
    let constant_prefix =
        extract_constant_prefix_for_pk(&right_filters, &pk_columns, &equi_join.right_col);

    // Build the index lookup key template
    // For Stock-Level: key = [s_w_id (from filter), s_i_id (from join)]
    let lookup_key_template =
        match build_lookup_key_template(&pk_columns, &equi_join.right_col, &constant_prefix) {
            Some(template) => template,
            None => return Ok(None),
        };

    // Get the primary key index
    let pk_index = match right_table.primary_key_index() {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Get left column index for extracting join keys
    let left_col_idx = match find_column_index(&left_result.schema, &equi_join.left_col) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Build evaluator for right-side filter (residual predicates after PK lookup)
    let right_schema =
        CombinedSchema::from_table(right_table_name.clone(), right_table.schema.clone());
    let residual_filter = build_residual_filter(&right_filters, &pk_columns);
    let evaluator = residual_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right_schema, database));

    // Collect distinct join keys from left side
    let left_slice = left_result.as_slice();
    let mut seen_keys: AHashSet<vibesql_types::SqlValue> =
        AHashSet::with_capacity(left_slice.len());
    let mut matching_keys: AHashSet<vibesql_types::SqlValue> = AHashSet::new();

    for left_row in left_slice {
        let join_key = &left_row.values[left_col_idx];

        // Skip NULL join keys
        if *join_key == vibesql_types::SqlValue::Null {
            continue;
        }

        // Skip duplicate keys (we only need one match for SEMI join)
        if seen_keys.contains(join_key) {
            continue;
        }
        seen_keys.insert(join_key.clone());

        // Build the full lookup key
        let lookup_key: Vec<vibesql_types::SqlValue> = lookup_key_template
            .iter()
            .map(|slot| match slot {
                KeySlot::Constant(v) => v.clone(),
                KeySlot::JoinKey => join_key.clone(),
            })
            .collect();

        // Do point lookup
        // Issue #3790: Use get_row() which returns None for deleted rows
        if let Some(&row_idx) = pk_index.get(&lookup_key) {
            let right_row = match right_table.get_row(row_idx) {
                Some(row) => row,
                None => continue, // Row deleted or invalid
            };

            // Apply residual filter if any
            let passes = if let (Some(filter), Some(eval)) = (&residual_filter, &evaluator) {
                eval.clear_cse_cache();
                matches!(eval.eval(filter, right_row), Ok(vibesql_types::SqlValue::Boolean(true)))
            } else {
                true // No residual filter
            };

            if passes {
                matching_keys.insert(join_key.clone());
            }
        }
    }

    // Build result: all left rows whose join key is in matching_keys
    let result_rows: Vec<vibesql_storage::Row> = left_slice
        .iter()
        .filter(|row| {
            let key = &row.values[left_col_idx];
            matching_keys.contains(key)
        })
        .cloned()
        .collect();

    Ok(Some(super::FromResult::from_rows(left_result.schema.clone(), result_rows)))
}

/// Parsed equi-join condition
struct EquiJoinInfo {
    left_col: String,
    right_col: String,
}

/// Parse a semi-join condition to extract equi-join and right-side filters.
fn parse_semi_join_condition(
    cond: &vibesql_ast::Expression,
    left_result: &super::FromResult,
    right_table_name: &str,
) -> Option<(EquiJoinInfo, Option<vibesql_ast::Expression>)> {
    let conjuncts = flatten_conjuncts(cond);

    let left_tables: HashSet<String> =
        left_result.schema.table_schemas.keys().map(|s| s.canonical().to_string()).collect();

    let right_table_lower = right_table_name.to_lowercase();

    let mut equi_join: Option<EquiJoinInfo> = None;
    let mut right_only_preds: Vec<vibesql_ast::Expression> = Vec::new();

    for pred in conjuncts {
        // Check if this is an equi-join predicate (col1 = col2)
        if let vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Equal,
            right,
        } = &pred
        {
            if let (
                vibesql_ast::Expression::ColumnRef(left_col_id),
                vibesql_ast::Expression::ColumnRef(right_col_id),
            ) = (left.as_ref(), right.as_ref())
            {
                if left_col_id.schema_canonical().is_some()
                    || right_col_id.schema_canonical().is_some()
                {
                    // Schema-qualified, skip this predicate
                    right_only_preds.push(pred);
                    continue;
                }
                let left_tbl = left_col_id.table_canonical();
                let right_tbl = right_col_id.table_canonical();
                let left_col = left_col_id.column_canonical();
                let right_col = right_col_id.column_canonical();

                // Determine which column is from left and which from right
                let left_tbl_lower = left_tbl.map(|s| s.to_lowercase());
                let right_tbl_lower = right_tbl.map(|s| s.to_lowercase());

                let left_col_lower = left_col.to_lowercase();

                // Check if left_col is from left tables and right_col is from right table
                // When table qualifier is None, check if column exists in any left table's schema
                let left_is_left =
                    left_tbl_lower.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false)
                        || left_result.schema.table_schemas.values().any(|(_, schema)| {
                            schema.columns.iter().any(|c| c.name.to_lowercase() == left_col_lower)
                        });
                let right_is_right =
                    right_tbl_lower.as_ref().map(|t| t == &right_table_lower).unwrap_or(true);

                if left_is_left && right_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: left_col.to_string(),
                        right_col: right_col.to_string(),
                    });
                    continue;
                }

                // Check the reverse: right_col from left, left_col from right
                let right_is_left =
                    right_tbl_lower.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false);
                let left_is_right =
                    left_tbl_lower.as_ref().map(|t| t == &right_table_lower).unwrap_or(true);

                if right_is_left && left_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: right_col.to_string(),
                        right_col: left_col.to_string(),
                    });
                    continue;
                }
            }
        }

        // Check if this predicate references only the right table
        // (We'll add it to right_only_preds)
        right_only_preds.push(pred);
    }

    equi_join.map(|ej| {
        let filter = combine_with_and(right_only_preds);
        (ej, filter)
    })
}

/// Slot in a lookup key template
#[derive(Debug, Clone)]
enum KeySlot {
    Constant(vibesql_types::SqlValue),
    JoinKey,
}

/// Extract constant values from right filters that match PK columns.
/// Returns a map from column name to constant value.
fn extract_constant_prefix_for_pk(
    right_filters: &Option<vibesql_ast::Expression>,
    pk_columns: &[String],
    join_key_col: &str,
) -> HashMap<String, vibesql_types::SqlValue> {
    let mut constants: HashMap<String, vibesql_types::SqlValue> = HashMap::new();

    let filter = match right_filters {
        Some(f) => f,
        None => return constants,
    };

    // Flatten and look for equality predicates with constants
    let conjuncts = flatten_conjuncts(filter);
    let join_key_upper = join_key_col.to_lowercase();

    for pred in conjuncts {
        if let vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Equal,
            right,
        } = &pred
        {
            // Check if left is column and right is literal
            if let (
                vibesql_ast::Expression::ColumnRef(col_id),
                vibesql_ast::Expression::Literal(value),
            ) = (left.as_ref(), right.as_ref())
            {
                let column = col_id.column_canonical();
                let col_upper = column.to_lowercase();
                // Only add if it's a PK column and not the join key
                if pk_columns.iter().any(|pk| pk.to_lowercase() == col_upper)
                    && col_upper != join_key_upper
                {
                    constants.insert(col_upper, value.clone());
                }
            }

            // Check reverse: right is column and left is literal
            if let (
                vibesql_ast::Expression::Literal(value),
                vibesql_ast::Expression::ColumnRef(col_id),
            ) = (left.as_ref(), right.as_ref())
            {
                let column = col_id.column_canonical();
                let col_upper = column.to_lowercase();
                if pk_columns.iter().any(|pk| pk.to_lowercase() == col_upper)
                    && col_upper != join_key_upper
                {
                    constants.insert(col_upper, value.clone());
                }
            }
        }
    }

    constants
}

/// Build a lookup key template based on PK columns.
fn build_lookup_key_template(
    pk_columns: &[String],
    join_key_col: &str,
    constant_prefix: &HashMap<String, vibesql_types::SqlValue>,
) -> Option<Vec<KeySlot>> {
    let join_key_upper = join_key_col.to_lowercase();
    let mut template = Vec::with_capacity(pk_columns.len());

    for pk_col in pk_columns {
        let pk_upper = pk_col.to_lowercase();

        if pk_upper == join_key_upper {
            template.push(KeySlot::JoinKey);
        } else if let Some(value) = constant_prefix.get(&pk_upper) {
            template.push(KeySlot::Constant(value.clone()));
        } else {
            // Missing a PK component, can't use INL
            return None;
        }
    }

    Some(template)
}

/// Build residual filter (predicates not covered by PK lookup)
fn build_residual_filter(
    right_filters: &Option<vibesql_ast::Expression>,
    pk_columns: &[String],
) -> Option<vibesql_ast::Expression> {
    let filter = match right_filters {
        Some(f) => f,
        None => return None,
    };

    let pk_upper: HashSet<String> = pk_columns.iter().map(|s| s.to_lowercase()).collect();
    let conjuncts = flatten_conjuncts(filter);

    // Keep predicates that are NOT equality on PK columns
    let residual: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // Check if this is an equality predicate on a PK column
            if let vibesql_ast::Expression::BinaryOp {
                left,
                op: vibesql_ast::BinaryOperator::Equal,
                right,
            } = pred
            {
                // Check if it's col = literal or literal = col
                if let vibesql_ast::Expression::ColumnRef(col_id) = left.as_ref() {
                    if pk_upper.contains(&col_id.column_canonical().to_lowercase())
                        && matches!(right.as_ref(), vibesql_ast::Expression::Literal(_))
                    {
                        return false; // Filter out, covered by PK lookup
                    }
                }
                if let vibesql_ast::Expression::ColumnRef(col_id) = right.as_ref() {
                    if pk_upper.contains(&col_id.column_canonical().to_lowercase())
                        && matches!(left.as_ref(), vibesql_ast::Expression::Literal(_))
                    {
                        return false; // Filter out, covered by PK lookup
                    }
                }
            }
            true // Keep this predicate
        })
        .collect();

    combine_with_and(residual)
}

/// Find the index of a column in the schema
fn find_column_index(schema: &CombinedSchema, col_name: &str) -> Option<usize> {
    let col_upper = col_name.to_lowercase();
    let mut offset = 0;

    for (_, table_schema) in schema.table_schemas.values() {
        for (idx, col) in table_schema.columns.iter().enumerate() {
            if col.name.to_lowercase() == col_upper {
                return Some(offset + idx);
            }
        }
        offset += table_schema.columns.len();
    }

    None
}

/// Prefix-scan based semi-join for cases where an index exists but not all key columns
/// are covered by the join condition.
///
/// This handles cases like TPC-H Q4:
/// - Join on `o_orderkey = l_orderkey`
/// - Index on `(l_orderkey, l_linenumber)`
/// - Additional filter: `l_commitdate < l_receiptdate`
///
/// Instead of building a hash table from 60K lineitem rows, we do ~500 prefix scans
/// (one per qualifying order), which is much faster when the left side is small.
fn try_prefix_scan_semi_join(
    left_result: &super::FromResult,
    right_from: &vibesql_ast::FromClause,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<super::FromResult>, ExecutorError> {
    let debug = std::env::var("PREFIX_SEMI_DEBUG").is_ok();

    // Must have a join condition
    let cond = match condition {
        Some(c) => c,
        None => return Ok(None),
    };

    // Right side must be a simple table (not a join or subquery)
    let (right_table_name, _right_alias) = match right_from {
        vibesql_ast::FromClause::Table { name, alias, .. } => (name.clone(), alias.clone()),
        _ => return Ok(None),
    };

    // Get the right table
    let right_table = match database.get_table(&right_table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Parse the join condition to extract equi-join columns and additional filters
    let (equi_join, right_filters) =
        match parse_semi_join_condition(cond, left_result, &right_table_name) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };

    if debug {
        eprintln!(
            "[PREFIX_SEMI] left_rows={}, join on {}={}, right_filters={:?}",
            left_result.as_slice().len(),
            equi_join.left_col,
            equi_join.right_col,
            right_filters
        );
    }

    // Find an index on the right table that starts with the join key column
    let right_col_upper = equi_join.right_col.to_lowercase();
    let index_names = database.list_indexes_for_table(&right_table_name);

    // Find an index where the first column is the join key
    let mut usable_index_name: Option<String> = None;
    for index_name in &index_names {
        if let Some(idx_metadata) = database.get_index(index_name) {
            if let Some(first_col) = idx_metadata.columns.first() {
                if first_col.expect_column_name().to_lowercase() == right_col_upper {
                    usable_index_name = Some(index_name.clone());
                    break;
                }
            }
        }
    }

    let index_name = match usable_index_name {
        Some(name) => name,
        None => {
            if debug {
                eprintln!(
                    "[PREFIX_SEMI] No index starting with {} on {}",
                    equi_join.right_col, right_table_name
                );
            }
            return Ok(None);
        }
    };

    let index = match database.get_index_data(&index_name) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    if debug {
        eprintln!(
            "[PREFIX_SEMI] Using index {} for prefix scan on {}",
            index_name, right_table_name
        );
    }

    // Get left column index for extracting join keys
    let left_col_idx = match find_column_index(&left_result.schema, &equi_join.left_col) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Build evaluator for right-side filter (e.g., l_commitdate < l_receiptdate)
    let right_schema =
        CombinedSchema::from_table(right_table_name.clone(), right_table.schema.clone());
    let evaluator = right_filters
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right_schema, database));

    // Collect distinct join keys from left side
    let left_slice = left_result.as_slice();
    let mut seen_keys: AHashSet<vibesql_types::SqlValue> =
        AHashSet::with_capacity(left_slice.len());
    let mut matching_keys: AHashSet<vibesql_types::SqlValue> = AHashSet::new();

    let start = std::time::Instant::now();

    for left_row in left_slice {
        let join_key = &left_row.values[left_col_idx];

        // Skip NULL join keys
        if *join_key == vibesql_types::SqlValue::Null {
            continue;
        }

        // Skip duplicate keys (we only need one match for SEMI join)
        if seen_keys.contains(join_key) {
            continue;
        }
        seen_keys.insert(join_key.clone());

        // Do prefix lookup: find all rows where the first index column = join_key
        let row_indices = index.prefix_multi_lookup(std::slice::from_ref(join_key));

        // Check if any of the matching rows pass the additional filter
        for row_idx in row_indices {
            // Get the row data
            let right_row = match right_table.get_row(row_idx) {
                Some(row) => row,
                None => continue, // Row deleted or invalid
            };

            // Apply filter if any
            let passes = if let (Some(filter), Some(eval)) = (&right_filters, &evaluator) {
                eval.clear_cse_cache();
                matches!(eval.eval(filter, right_row), Ok(vibesql_types::SqlValue::Boolean(true)))
            } else {
                true // No filter
            };

            if passes {
                matching_keys.insert(join_key.clone());
                break; // Semi-join: we only need one match per left row
            }
        }
    }

    if debug {
        let elapsed = start.elapsed();
        eprintln!(
            "[PREFIX_SEMI] {} distinct keys, {} matches, {:?} elapsed",
            seen_keys.len(),
            matching_keys.len(),
            elapsed
        );
    }

    // Build result: all left rows whose join key is in matching_keys
    let result_rows: Vec<vibesql_storage::Row> = left_slice
        .iter()
        .filter(|row| {
            let key = &row.values[left_col_idx];
            matching_keys.contains(key)
        })
        .cloned()
        .collect();

    Ok(Some(super::FromResult::from_rows(left_result.schema.clone(), result_rows)))
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
    let left_table_names: std::collections::HashSet<_> = left_schema
        .table_schemas
        .keys()
        .map(|k| k.canonical().to_lowercase())
        .collect();
    let right_table_names: std::collections::HashSet<_> = right_schema
        .table_schemas
        .keys()
        .map(|k| k.canonical().to_lowercase())
        .collect();
    let is_self_join = !left_table_names.is_disjoint(&right_table_names);

    for col_name in columns {
        let col_lower = col_name.to_lowercase();

        // Find column in left schema (case-insensitive) - also get absolute index and collation
        // IMPORTANT: For chained USING joins (e.g., t1 LEFT JOIN t2 USING(a) LEFT JOIN t3 USING(a)),
        // the left_schema may contain multiple tables with the same column name.
        // We must pick the LEFTMOST table (lowest start_idx) to ensure we use the correct
        // value for the join condition. This matches SQLite's COALESCE semantics where
        // the leftmost non-NULL value is used for USING columns.
        // Issue #4753: HashMap iteration order is non-deterministic, so using find_map()
        // could pick t2.a (NULL from LEFT JOIN) instead of t1.a (actual value).
        let left_col = {
            let mut best_match: Option<(
                vibesql_catalog::TableIdentifier,
                String,
                usize,
                Option<String>,
            )> = None;
            for (table_name, (start_idx, table_schema)) in &left_schema.table_schemas {
                for (col_offset, col) in table_schema.columns.iter().enumerate() {
                    if col.name.to_lowercase() == col_lower {
                        let absolute_idx = start_idx + col_offset;
                        // Pick the column with the lowest absolute index (leftmost table)
                        if best_match.is_none()
                            || absolute_idx < best_match.as_ref().unwrap().2
                        {
                            best_match = Some((
                                table_name.clone(),
                                col.name.clone(),
                                absolute_idx,
                                col.collation.clone(),
                            ));
                        }
                    }
                }
            }
            best_match
        }
        .ok_or_else(|| ExecutorError::JoinUsingColumnNotPresent {
            column_name: col_name.to_string(),
        })?;

        // Find column in right schema (case-insensitive) - get absolute index, table start, collation
        let right_col = right_schema
            .table_schemas
            .iter()
            .find_map(|(table_name, (start_idx, table_schema))| {
                table_schema
                    .columns
                    .iter()
                    .enumerate()
                    .find_map(|(col_offset, col)| {
                        if col.name.to_lowercase() == col_lower {
                            // Return: (table_name, col_name, absolute_col_idx, table_start_idx, collation)
                            Some((
                                table_name.clone(),
                                col.name.clone(),
                                start_idx + col_offset,
                                *start_idx,
                                col.collation.clone(),
                            ))
                        } else {
                            None
                        }
                    })
            })
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
            format!(
                "__selfjoin_right_{}_{}",
                right_col.0.canonical(),
                adjusted_start
            )
        } else {
            right_col.0.to_string()
        };

        // Build right column expression, applying COLLATE if the column has a collation
        // defined. This ensures USING clause joins respect column-level COLLATE declarations.
        let right_col_expr = vibesql_ast::Expression::ColumnRef(
            vibesql_ast::ColumnIdentifier::qualified(&right_table_name, false, &right_col.1, false),
        );
        let right_col_with_collate = if let Some(ref coll) = collation {
            vibesql_ast::Expression::Collate {
                expr: Box::new(right_col_expr),
                collation: coll.clone(),
            }
        } else {
            right_col_expr
        };

        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef(
                vibesql_ast::ColumnIdentifier::qualified(
                    &left_col.0.to_string(),
                    false,
                    &left_col.1,
                    false,
                ),
            )),
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
/// SQLite requires that ON clauses only reference tables from the current join (left and right sides).
/// If the ON clause references a table that appears later in the FROM clause (to the right),
/// SQLite returns the error: "ON clause references tables to its right"
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
fn validate_on_clause_table_references(
    on_condition: &vibesql_ast::Expression,
    left_schema: &CombinedSchema,
    right_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // Collect all table names from both schemas (lowercase for case-insensitive comparison)
    let mut valid_tables: HashSet<String> = HashSet::new();
    for table_id in left_schema.table_schemas.keys() {
        valid_tables.insert(table_id.canonical().to_lowercase());
    }
    for table_id in right_schema.table_schemas.keys() {
        valid_tables.insert(table_id.canonical().to_lowercase());
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

    // Collect all column names from all tables in the database (for detecting right-table references)
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
///   - If it doesn't exist in valid_columns BUT exists in all_db_columns, it's referencing
///     a table to the right (error)
///   - If it doesn't exist in either, it might be a SELECT alias (allow, will fail later if invalid)
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
            on_clause_references_unknown_table_or_column(left, valid_tables, valid_columns, all_db_columns)
                || on_clause_references_unknown_table_or_column(right, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
                || on_clause_references_unknown_table_or_column(low, valid_tables, valid_columns, all_db_columns)
                || on_clause_references_unknown_table_or_column(high, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
                || values
                    .iter()
                    .any(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand
                .as_ref()
                .map(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns))
                .unwrap_or(false)
                || when_clauses.iter().any(|case_when| {
                    case_when.conditions.iter().any(|c| {
                        on_clause_references_unknown_table_or_column(c, valid_tables, valid_columns, all_db_columns)
                    }) || on_clause_references_unknown_table_or_column(
                        &case_when.result,
                        valid_tables,
                        valid_columns,
                        all_db_columns,
                    )
                })
                || else_result
                    .as_ref()
                    .map(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns))
                    .unwrap_or(false)
        }
        vibesql_ast::Expression::Function { args, .. } => args
            .iter()
            .any(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns)),
        vibesql_ast::Expression::AggregateFunction { args, filter, .. } => {
            args.iter()
                .any(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns))
                || filter
                    .as_ref()
                    .map(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns))
                    .unwrap_or(false)
        }
        vibesql_ast::Expression::WindowFunction { function, over } => {
            let func_refs = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, filter, .. } => {
                    args.iter().any(|e| {
                        on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns)
                    }) || filter
                        .as_ref()
                        .map(|e| {
                            on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns)
                        })
                        .unwrap_or(false)
                }
                vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args
                    .iter()
                    .any(|e| on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns)),
            };
            func_refs
                || over
                    .partition_by
                    .as_ref()
                    .map(|exprs| {
                        exprs.iter().any(|e| {
                            on_clause_references_unknown_table_or_column(e, valid_tables, valid_columns, all_db_columns)
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
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
        }
        vibesql_ast::Expression::Cast { expr, .. } => {
            on_clause_references_unknown_table_or_column(expr, valid_tables, valid_columns, all_db_columns)
        }
        // Literals, Null, Subquery, etc. don't reference tables in the join
        _ => false,
    }
}
