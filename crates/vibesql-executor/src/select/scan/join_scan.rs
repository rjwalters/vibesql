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

use ahash::AHashSet;
use std::collections::{HashMap, HashSet};

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

/// Threshold for using Index Nested Loop semi-join instead of hash join.
/// If left side has fewer rows than this, INL is typically faster.
const INL_THRESHOLD: usize = 1000;

/// Execute a JOIN operation
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_join<F>(
    left: &vibesql_ast::FromClause,
    right: &vibesql_ast::FromClause,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
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

    // Index Nested Loop (INL) optimization for SEMI joins with small left side (#3392)
    // When left side is small, use index lookups on right table instead of scanning all rows
    if matches!(join_type, vibesql_ast::JoinType::Semi) {
        let left_row_count = left_result.as_slice().len();
        if left_row_count > 0 && left_row_count < INL_THRESHOLD {
            if let Some(result) =
                try_index_nested_loop_semi_join(&left_result, right, condition, database)?
            {
                return Ok(result);
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

    // For NATURAL JOIN, generate the implicit join condition based on common column names
    let natural_join_condition = if natural {
        generate_natural_join_condition(&left_result.schema, &right_result.schema)?
    } else {
        None
    };

    // Use the natural join condition if present, otherwise use the explicit condition
    let effective_condition = natural_join_condition.or_else(|| condition.clone());

    // If we have a WHERE clause, use predicate plan to extract equijoin conditions (Phase 1)
    let equijoin_predicates = if let Some(where_expr) = where_clause {
        // Build combined schema for WHERE clause analysis using SchemaBuilder for O(n) performance
        let mut schema_builder =
            crate::schema::SchemaBuilder::from_schema(left_result.schema.clone());
        for (table_name, (_start_idx, table_schema)) in &right_result.schema.table_schemas {
            schema_builder.add_table(table_name.clone(), table_schema.clone());
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
    use crate::select::join::nested_loop_join;
    // Note: Using default timeout context - proper timeout propagation from SelectExecutor
    // is a future improvement (see issue #2631 for context)
    let timeout_ctx = TimeoutContext::new_default();
    // Issue #3562: Pass CTE context so post-join filters with IN subqueries can resolve CTEs
    let result = nested_loop_join(
        left_result,
        right_result,
        join_type,
        &effective_condition,
        natural,
        database,
        &equijoin_predicates,
        &timeout_ctx,
        cte_results,
    )?;
    Ok(result)
}

/// Generate the implicit join condition for a NATURAL JOIN
///
/// Finds all common column names between the left and right schemas (case-insensitive)
/// and creates an AND chain of equality conditions.
///
/// Returns None if there are no common columns (which means NATURAL JOIN should behave like CROSS JOIN)
fn generate_natural_join_condition(
    left_schema: &crate::schema::CombinedSchema,
    right_schema: &crate::schema::CombinedSchema,
) -> Result<Option<vibesql_ast::Expression>, ExecutorError> {
    use std::collections::HashMap;

    // Get all column names from left schema (normalized to lowercase for case-insensitive comparison)
    let mut left_columns: HashMap<String, Vec<(String, String)>> = HashMap::new(); // lowercase_name -> [(table, actual_name)]
    for (table_name, (_table_idx, table_schema)) in &left_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            left_columns
                .entry(lowercase_name)
                .or_default()
                .push((table_name.to_string(), col.name.clone()));
        }
    }

    // Find common column names from right schema
    let mut common_columns: Vec<(String, String, String, String)> = Vec::new(); // (left_table, left_col, right_table, right_col)
    for (table_name, (_table_idx, table_schema)) in &right_schema.table_schemas {
        for col in &table_schema.columns {
            let lowercase_name = col.name.to_lowercase();
            if let Some(left_occurrences) = left_columns.get(&lowercase_name) {
                // Found a common column
                for (left_table, left_col) in left_occurrences {
                    common_columns.push((
                        left_table.clone(),
                        left_col.clone(),
                        table_name.to_string(),
                        col.name.clone(),
                    ));
                }
            }
        }
    }

    // If no common columns, return None (NATURAL JOIN behaves like CROSS JOIN)
    if common_columns.is_empty() {
        return Ok(None);
    }

    // Build the join condition as an AND chain of equalities
    let mut condition: Option<vibesql_ast::Expression> = None;
    for (left_table, left_col, right_table, right_col) in common_columns {
        let equality = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some(left_table),
                column: left_col,
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some(right_table),
                column: right_col,
            }),
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
        vibesql_ast::Expression::ColumnRef { table: Some(t), .. } => {
            let t_lower = t.to_lowercase();
            table_set.contains(t) || table_set.contains(&t_lower)
        }
        vibesql_ast::Expression::ColumnRef { table: None, .. } => {
            // Unqualified column - can't determine which table, assume it might be from nullable side
            // Return false to be conservative (keep the predicate)
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
    }
}

/// Try to execute a SEMI join using Index Nested Loop (INL) strategy.
///
/// This optimization is used when:
/// 1. The left side is small (< INL_THRESHOLD rows)
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
        left_result.schema.table_schemas.keys().map(|s| s.to_uppercase()).collect();

    let right_table_upper = right_table_name.to_uppercase();

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
                vibesql_ast::Expression::ColumnRef { table: left_tbl, column: left_col },
                vibesql_ast::Expression::ColumnRef { table: right_tbl, column: right_col },
            ) = (left.as_ref(), right.as_ref())
            {
                // Determine which column is from left and which from right
                let left_tbl_upper = left_tbl.as_ref().map(|s| s.to_uppercase());
                let right_tbl_upper = right_tbl.as_ref().map(|s| s.to_uppercase());

                let left_col_upper = left_col.to_uppercase();

                // Check if left_col is from left tables and right_col is from right table
                // When table qualifier is None, check if column exists in any left table's schema
                let left_is_left =
                    left_tbl_upper.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false)
                        || left_result.schema.table_schemas.values().any(|(_, schema)| {
                            schema.columns.iter().any(|c| c.name.to_uppercase() == left_col_upper)
                        });
                let right_is_right =
                    right_tbl_upper.as_ref().map(|t| t == &right_table_upper).unwrap_or(true);

                if left_is_left && right_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: left_col.clone(),
                        right_col: right_col.clone(),
                    });
                    continue;
                }

                // Check the reverse: right_col from left, left_col from right
                let right_is_left =
                    right_tbl_upper.as_ref().map(|t| left_tables.contains(t)).unwrap_or(false);
                let left_is_right =
                    left_tbl_upper.as_ref().map(|t| t == &right_table_upper).unwrap_or(true);

                if right_is_left && left_is_right && equi_join.is_none() {
                    equi_join = Some(EquiJoinInfo {
                        left_col: right_col.clone(),
                        right_col: left_col.clone(),
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
    let join_key_upper = join_key_col.to_uppercase();

    for pred in conjuncts {
        if let vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Equal,
            right,
        } = &pred
        {
            // Check if left is column and right is literal
            if let (
                vibesql_ast::Expression::ColumnRef { column, .. },
                vibesql_ast::Expression::Literal(value),
            ) = (left.as_ref(), right.as_ref())
            {
                let col_upper = column.to_uppercase();
                // Only add if it's a PK column and not the join key
                if pk_columns.iter().any(|pk| pk.to_uppercase() == col_upper)
                    && col_upper != join_key_upper
                {
                    constants.insert(col_upper, value.clone());
                }
            }

            // Check reverse: right is column and left is literal
            if let (
                vibesql_ast::Expression::Literal(value),
                vibesql_ast::Expression::ColumnRef { column, .. },
            ) = (left.as_ref(), right.as_ref())
            {
                let col_upper = column.to_uppercase();
                if pk_columns.iter().any(|pk| pk.to_uppercase() == col_upper)
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
    let join_key_upper = join_key_col.to_uppercase();
    let mut template = Vec::with_capacity(pk_columns.len());

    for pk_col in pk_columns {
        let pk_upper = pk_col.to_uppercase();

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

    let pk_upper: HashSet<String> = pk_columns.iter().map(|s| s.to_uppercase()).collect();
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
                if let vibesql_ast::Expression::ColumnRef { column, .. } = left.as_ref() {
                    if pk_upper.contains(&column.to_uppercase())
                        && matches!(right.as_ref(), vibesql_ast::Expression::Literal(_))
                    {
                        return false; // Filter out, covered by PK lookup
                    }
                }
                if let vibesql_ast::Expression::ColumnRef { column, .. } = right.as_ref() {
                    if pk_upper.contains(&column.to_uppercase())
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
    let col_upper = col_name.to_uppercase();
    let mut offset = 0;

    for (_, table_schema) in schema.table_schemas.values() {
        for (idx, col) in table_schema.columns.iter().enumerate() {
            if col.name.to_uppercase() == col_upper {
                return Some(offset + idx);
            }
        }
        offset += table_schema.columns.len();
    }

    None
}
