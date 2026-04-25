//! JOIN helper functions for columnar execution
//!
//! This module contains free functions that support the JOIN execution path,
//! including join tree flattening, condition extraction, and schema building.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType};

use crate::{errors::ExecutorError, schema::CombinedSchema, select::columnar};

/// Check if a FROM clause only contains join types supported by the columnar path
///
/// Supported join types:
/// - INNER JOIN (explicit `JOIN ... ON` syntax)
/// - CROSS JOIN (comma-separated tables `FROM a, b`)
/// - LEFT OUTER JOIN (preserves all left rows, NULLs for unmatched right)
/// - RIGHT OUTER JOIN (preserves all right rows, NULLs for unmatched left)
///
/// FULL OUTER, SEMI, and ANTI joins are not yet supported in the columnar path.
pub(super) fn is_columnar_supported_join(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => true,
        FromClause::Join { left, right, join_type, .. } => {
            matches!(
                join_type,
                JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter
            ) && is_columnar_supported_join(left)
                && is_columnar_supported_join(right)
        }
    }
}

/// Check if a FROM clause contains any outer join (LEFT, RIGHT, or FULL OUTER)
///
/// Used to detect when WHERE clause predicates like IS NULL / IS NOT NULL
/// need special handling, since the columnar SIMD filter doesn't support
/// null-testing predicates and would silently drop them.
pub(super) fn has_outer_join(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => false,
        FromClause::Join { left, right, join_type, .. } => {
            matches!(
                join_type,
                JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter
            ) || has_outer_join(left)
                || has_outer_join(right)
        }
    }
}

/// Check if an expression contains IS NULL or IS NOT NULL predicates
///
/// The columnar SIMD filter path does not support IS NULL / IS NOT NULL predicates,
/// so they are silently dropped. For outer joins this produces incorrect results
/// because post-join null-testing (e.g., anti-join pattern `LEFT JOIN ... WHERE right.col IS NULL`)
/// is essential for correctness.
pub(super) fn expression_has_is_null(expr: &Expression) -> bool {
    match expr {
        Expression::IsNull { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_has_is_null(left) || expression_has_is_null(right)
        }
        Expression::UnaryOp { expr, .. } => expression_has_is_null(expr),
        _ => false,
    }
}

/// Check if a FROM clause contains a CROSS JOIN with a join condition
///
/// CROSS JOIN with ON condition is semantically invalid SQL.
/// CROSS JOIN with USING clause or NATURAL CROSS JOIN should be treated as INNER JOIN
/// and require special handling that the columnar path doesn't support.
///
/// We detect these cases to fall back to regular execution which handles them correctly.
pub(super) fn has_cross_join_with_on_condition(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => false,
        FromClause::Join { left, right, join_type, condition, using_columns, natural, .. } => {
            // CROSS JOIN with any join condition (ON, USING, or NATURAL) should fall back
            // to regular execution path which handles filtering and column deduplication
            if matches!(join_type, JoinType::Cross)
                && (condition.is_some() || using_columns.is_some() || *natural)
            {
                return true;
            }
            // Recursively check children
            has_cross_join_with_on_condition(left) || has_cross_join_with_on_condition(right)
        }
    }
}

/// Simple table reference: (name, alias, is_subquery)
pub(super) type SimpleTableRef = (String, Option<String>, bool);

/// Flatten a join tree into a list of simple table references
pub(super) fn flatten_join_tree_simple(from: &FromClause, tables: &mut Vec<SimpleTableRef>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push((name.clone(), alias.clone(), false));
        }
        FromClause::Subquery { alias, .. } => {
            tables.push((alias.clone(), Some(alias.clone()), true));
        }
        FromClause::Values { alias, .. } => {
            tables.push((alias.clone(), Some(alias.clone()), true));
        }
        FromClause::Join { left, right, .. } => {
            flatten_join_tree_simple(left, tables);
            flatten_join_tree_simple(right, tables);
        }
    }
}

/// Flatten a join tree into a list of table references with their join types.
///
/// The first table in the list has no join type (it's the leftmost table in the tree).
/// Each subsequent table has the JoinType that connects it to the previously joined tables.
///
/// For a query like `FROM a INNER JOIN b ON ... LEFT JOIN c ON ...`, this produces:
/// - (a_info, None)
/// - (b_info, Some(Inner))
/// - (c_info, Some(LeftOuter))
pub(super) fn flatten_join_tree_with_types(
    from: &FromClause,
    tables: &mut Vec<(SimpleTableRef, Option<JoinType>)>,
) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push(((name.clone(), alias.clone(), false), None));
        }
        FromClause::Subquery { alias, .. } => {
            tables.push(((alias.clone(), Some(alias.clone()), true), None));
        }
        FromClause::Values { alias, .. } => {
            tables.push(((alias.clone(), Some(alias.clone()), true), None));
        }
        FromClause::Join { left, right, join_type, .. } => {
            flatten_join_tree_with_types(left, tables);
            // The right side of this join node gets the join type
            match right.as_ref() {
                FromClause::Table { name, alias, .. } => {
                    tables.push(((name.clone(), alias.clone(), false), Some(join_type.clone())));
                }
                FromClause::Subquery { alias, .. } => {
                    tables.push((
                        (alias.clone(), Some(alias.clone()), true),
                        Some(join_type.clone()),
                    ));
                }
                FromClause::Values { alias, .. } => {
                    tables.push((
                        (alias.clone(), Some(alias.clone()), true),
                        Some(join_type.clone()),
                    ));
                }
                FromClause::Join { .. } => {
                    // Nested join on the right side - flatten it but mark the first
                    // entry with this join's type
                    let start_idx = tables.len();
                    flatten_join_tree_with_types(right, tables);
                    // Override the join type of the first table from the nested join
                    if start_idx < tables.len() {
                        tables[start_idx].1 = Some(join_type.clone());
                    }
                }
            }
        }
    }
}

/// Equi-join condition: left_table.left_column = right_table.right_column
#[derive(Debug, Clone)]
pub(super) struct EquiJoinCondition {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_table: Option<String>,
    pub right_column: String,
}

/// Extract join conditions from a FROM clause (ON conditions)
pub(super) fn extract_join_conditions(from: &FromClause, conditions: &mut Vec<EquiJoinCondition>) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => {}
        FromClause::Join { left, right, condition, join_type, .. } => {
            // Handle INNER, CROSS, LEFT OUTER, and RIGHT OUTER joins in columnar path
            // FULL OUTER, SEMI, and ANTI joins are not supported
            if !matches!(
                join_type,
                JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter
            ) {
                return;
            }

            // Extract ON conditions (CROSS joins typically don't have ON conditions -
            // their join predicates are in the WHERE clause which is handled separately)
            if let Some(cond) = condition {
                extract_equijoin_conditions(cond, conditions);
            }

            extract_join_conditions(left, conditions);
            extract_join_conditions(right, conditions);
        }
    }
}

/// Extract equi-join conditions from an expression
pub(super) fn extract_equijoin_conditions(
    expr: &Expression,
    conditions: &mut Vec<EquiJoinCondition>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_equijoin_conditions(left, conditions);
            extract_equijoin_conditions(right, conditions);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check if this is col1 = col2 (equi-join)
            if let (Expression::ColumnRef(left_col_id), Expression::ColumnRef(right_col_id)) =
                (left.as_ref(), right.as_ref())
            {
                if left_col_id.schema_canonical().is_none()
                    && right_col_id.schema_canonical().is_none()
                {
                    conditions.push(EquiJoinCondition {
                        left_table: left_col_id.table_canonical().map(|t| t.to_string()),
                        left_column: left_col_id.column_canonical().to_string(),
                        right_table: right_col_id.table_canonical().map(|t| t.to_string()),
                        right_column: right_col_id.column_canonical().to_string(),
                    });
                }
            }
        }
        _ => {}
    }
}

/// Extract non-join predicates (conditions that aren't col1 = col2)
pub(super) fn extract_non_join_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<Vec<columnar::ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_non_join_predicates_recursive(expr, schema, case_sensitive_like, &mut predicates);
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

fn extract_non_join_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
    predicates: &mut Vec<columnar::ColumnPredicate>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_non_join_predicates_recursive(left, schema, case_sensitive_like, predicates);
            extract_non_join_predicates_recursive(right, schema, case_sensitive_like, predicates);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Skip column = column (join conditions)
            if matches!(left.as_ref(), Expression::ColumnRef(_))
                && matches!(right.as_ref(), Expression::ColumnRef(_))
            {
                return;
            }
            // Try to extract as column predicate
            if let Some(pred) = columnar::extract_column_predicates(expr, schema, case_sensitive_like) {
                predicates.extend(pred);
            }
        }
        _ => {
            // Try to extract other predicates
            if let Some(pred) = columnar::extract_column_predicates(expr, schema, case_sensitive_like) {
                predicates.extend(pred);
            }
        }
    }
}

/// Build a combined schema from multiple table batches
pub(super) fn build_combined_schema(
    batches: &[(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)],
) -> CombinedSchema {
    let mut combined = CombinedSchema {
        table_schemas: HashMap::new(),
        total_columns: 0,
        hidden_columns: HashSet::new(),
        outer_schema: None,
        duplicate_aliases: HashSet::new(),
        joined_columns: HashSet::new(),
        using_coalesce_indices: HashMap::new(),
        column_replacement_map: HashMap::new(),
        alias_tables: HashSet::new(),
        shadowed_tables: HashMap::new(),
    };

    for (table_name, alias, _batch, schema) in batches {
        let name = alias.as_ref().unwrap_or(table_name);
        combined.insert_table(name.clone(), combined.total_columns, schema.clone());
        combined.total_columns += schema.columns.len();
    }

    combined
}

/// Check if a column exists in any of the given tables
pub(super) fn is_column_in_tables(column: &str, tables: &[&str], schema: &CombinedSchema) -> bool {
    tables.iter().any(|t| is_column_in_table(column, t, schema))
}

/// Check if a column exists in a specific table
pub(super) fn is_column_in_table(column: &str, table: &str, schema: &CombinedSchema) -> bool {
    // TableKey lookup is case-insensitive
    if let Some((_, table_schema)) = schema.get_table(table) {
        table_schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(column))
    } else {
        false
    }
}

/// Resolve join column indices for the current join operation
pub(super) fn resolve_join_column_indices(
    cond: &EquiJoinCondition,
    joined_tables: &[&str],
    new_table: &str,
    new_table_schema: &vibesql_catalog::TableSchema,
    combined_schema: &CombinedSchema,
) -> Result<(usize, usize), ExecutorError> {
    // Determine which side refers to joined tables vs new table
    let left_in_joined = cond.left_table.as_deref().map_or_else(
        || is_column_in_tables(&cond.left_column, joined_tables, combined_schema),
        |t| joined_tables.contains(&t),
    );

    let (left_col, right_col) = if left_in_joined {
        (&cond.left_column, &cond.right_column)
    } else {
        (&cond.right_column, &cond.left_column)
    };

    // Find left column index in the current (joined) batch
    let left_idx = combined_schema.get_column_index(None, left_col).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: left_col.clone(),
            table_name: String::new(),
            searched_tables: joined_tables.iter().map(|s| s.to_string()).collect(),
            available_columns: vec![],
        }
    })?;

    // Find right column index in the new table
    let right_idx = new_table_schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(right_col))
        .ok_or_else(|| ExecutorError::ColumnNotFound {
            column_name: right_col.clone(),
            table_name: new_table.to_string(),
            searched_tables: vec![new_table.to_string()],
            available_columns: new_table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        })?;

    Ok((left_idx, right_idx))
}

/// Extract a single table name from a FROM clause if it's a simple table reference
///
/// Returns None if the FROM clause contains JOINs, subqueries, or other complex constructs.
pub(super) fn extract_single_table_name(from_clause: &FromClause) -> Option<String> {
    match from_clause {
        FromClause::Table { name, .. } => Some(name.clone()),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
        FromClause::Values { .. } => None, // VALUES not supported
    }
}

/// Extract table name and optional alias from a FROM clause if it's a simple table reference
///
/// Returns (table_name, alias) where alias is the alias if specified, otherwise None.
/// Returns None if the FROM clause contains JOINs, subqueries, or other complex constructs.
///
/// # Issue #4111
/// The alias (if present) must be used as the schema key, since queries reference
/// columns using the alias (e.g., `J.I_CURRENT_PRICE` in `FROM item J`).
pub(super) fn extract_table_name_and_alias(
    from_clause: &FromClause,
) -> Option<(String, Option<String>)> {
    match from_clause {
        FromClause::Table { name, alias, .. } => Some((name.clone(), alias.clone())),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
        FromClause::Values { .. } => None, // VALUES not supported
    }
}
