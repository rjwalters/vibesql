//! JOIN helper functions for columnar execution
//!
//! This module contains free functions that support the JOIN execution path,
//! including join tree flattening, condition extraction, and schema building.

use std::collections::HashMap;

use crate::{errors::ExecutorError, schema::CombinedSchema, select::columnar};
use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType};

/// Check if a FROM clause only contains INNER or CROSS joins
///
/// CROSS joins are included because:
/// - Comma-separated tables (`FROM a, b`) are parsed as CROSS joins
/// - CROSS JOIN with equijoin conditions in WHERE is semantically equivalent to INNER JOIN
/// - This enables columnar execution for implicit join syntax (e.g., TPC-H Q19)
pub(super) fn is_inner_or_cross_join_only(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => true,
        FromClause::Join { left, right, join_type, .. } => {
            matches!(join_type, JoinType::Inner | JoinType::Cross)
                && is_inner_or_cross_join_only(left)
                && is_inner_or_cross_join_only(right)
        }
    }
}

/// Check if a FROM clause contains a CROSS JOIN with an ON condition
///
/// This is semantically invalid SQL (CROSS JOIN should not have ON clause).
/// We detect this case to fall back to regular execution which produces a proper error.
pub(super) fn has_cross_join_with_on_condition(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => false,
        FromClause::Join { left, right, join_type, condition, .. } => {
            // CROSS JOIN with ON condition is invalid
            if matches!(join_type, JoinType::Cross) && condition.is_some() {
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
        FromClause::Join { left, right, .. } => {
            flatten_join_tree_simple(left, tables);
            flatten_join_tree_simple(right, tables);
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
        FromClause::Table { .. } | FromClause::Subquery { .. } => {}
        FromClause::Join { left, right, condition, join_type, .. } => {
            // Only handle INNER and CROSS joins in columnar path
            // OUTER joins need special handling and are not supported
            if !matches!(join_type, JoinType::Inner | JoinType::Cross) {
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
            if let (
                Expression::ColumnRef { table: lt, column: lc },
                Expression::ColumnRef { table: rt, column: rc },
            ) = (left.as_ref(), right.as_ref())
            {
                conditions.push(EquiJoinCondition {
                    left_table: lt.clone(),
                    left_column: lc.clone(),
                    right_table: rt.clone(),
                    right_column: rc.clone(),
                });
            }
        }
        _ => {}
    }
}

/// Extract non-join predicates (conditions that aren't col1 = col2)
pub(super) fn extract_non_join_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<columnar::ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_non_join_predicates_recursive(expr, schema, &mut predicates);
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

fn extract_non_join_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    predicates: &mut Vec<columnar::ColumnPredicate>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_non_join_predicates_recursive(left, schema, predicates);
            extract_non_join_predicates_recursive(right, schema, predicates);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Skip column = column (join conditions)
            if matches!(left.as_ref(), Expression::ColumnRef { .. })
                && matches!(right.as_ref(), Expression::ColumnRef { .. })
            {
                return;
            }
            // Try to extract as column predicate
            if let Some(pred) = columnar::extract_column_predicates(expr, schema) {
                predicates.extend(pred);
            }
        }
        _ => {
            // Try to extract other predicates
            if let Some(pred) = columnar::extract_column_predicates(expr, schema) {
                predicates.extend(pred);
            }
        }
    }
}

/// Build a combined schema from multiple table batches
pub(super) fn build_combined_schema(
    batches: &[(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)],
) -> CombinedSchema {
    let mut combined = CombinedSchema { table_schemas: HashMap::new(), total_columns: 0 };

    for (table_name, alias, _batch, schema) in batches {
        let name = alias.as_ref().unwrap_or(table_name);
        combined.insert_table(name, combined.total_columns, schema.clone());
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
pub(super) fn extract_table_name_and_alias(from_clause: &FromClause) -> Option<(String, Option<String>)> {
    match from_clause {
        FromClause::Table { name, alias, .. } => Some((name.clone(), alias.clone())),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
    }
}
