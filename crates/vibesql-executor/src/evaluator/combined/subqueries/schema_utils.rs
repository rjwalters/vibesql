//! Schema validation utilities for subqueries
//!
//! This module provides utilities to compute and validate the schema of
//! subquery results, particularly for handling wildcards and column counts.

use std::collections::HashMap;

use crate::{errors::ExecutorError, select::cte::CteResult};

/// Build a linked outer schema chain for nested subquery column resolution
///
/// **NEW APPROACH (Issue #4493)**: Instead of flattening all outer scopes into a single
/// HashMap (which causes collisions when the same table exists at multiple levels),
/// we now preserve the outer schema chain using `outer_schema` links, similar to
/// SQLite's NameContext.pNext approach.
///
/// This enables proper column resolution in deeply nested subqueries where the same
/// table appears at multiple nesting levels.
///
/// # Example: Issue #4493 Test Case
///
/// ```sql
/// SELECT x FROM t2, t1 WHERE x IN (           -- Level 0: t2, t1
///     SELECT x FROM t2, t1 WHERE x IN (       -- Level 1: t2, t1
///         SELECT x FROM t1 WHERE x = c        -- Level 2: t1 only, but can see outer t2.x
///     )
/// )
/// ```
///
/// With chaining:
/// - Level 2: schema=[t1], outer_schema -> Level 1's schema
/// - Level 1: schema=[t2, t1], outer_schema -> Level 0's schema
/// - Level 0: schema=[t2, t1], outer_schema -> None
///
/// When Level 2 resolves `x`, it searches:
/// 1. Current level [t1] - not found
/// 2. Follow outer_schema chain to Level 1 [t2, t1] - FOUND in t2!
///
/// No HashMap collision because each level keeps its own tables!
pub(super) fn build_merged_outer_schema<'a>(
    current_schema: &'a crate::schema::CombinedSchema,
    outer_schema: Option<&'a crate::schema::CombinedSchema>,
) -> std::borrow::Cow<'a, crate::schema::CombinedSchema> {
    if let Some(outer) = outer_schema {
        // Create a new schema with current_schema's tables but linked to outer_schema
        // This preserves the chain instead of flattening
        let mut new_schema = current_schema.clone();
        new_schema.outer_schema = Some(Box::new(outer.clone()));
        std::borrow::Cow::Owned(new_schema)
    } else {
        // No outer schema to link to
        std::borrow::Cow::Borrowed(current_schema)
    }
}

/// Build a merged outer row that matches the merged outer schema
///
/// When we merge schemas from multiple levels, we must also merge the corresponding
/// rows so that column indices align correctly.
///
/// # Arguments
/// * `current_row` - Row from the current level
/// * `outer_row` - Optional row from outer level(s)
///
/// # Returns
/// A merged row with values from both rows, or just the current row if no outer row exists
pub(super) fn build_merged_outer_row<'a>(
    current_row: &'a vibesql_storage::Row,
    outer_row: Option<&'a vibesql_storage::Row>,
) -> std::borrow::Cow<'a, vibesql_storage::Row> {
    if let Some(outer) = outer_row {
        // Merge: outer row values + current row values
        let mut merged_values = outer.values.clone();
        merged_values.extend(current_row.values.iter().cloned());

        std::borrow::Cow::Owned(vibesql_storage::Row {
            values: merged_values,
            row_id: None,
            row_ids: None,
        })
    } else {
        // No outer row to merge, just use current row
        std::borrow::Cow::Borrowed(current_row)
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
///
/// Issue #3562: Added CTE context so wildcards can be expanded for CTE references
pub(super) fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database, cte_results)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
                // Issue #3562: Check CTEs first before database tables
                if let Some(cte_ctx) = cte_results {
                    if let Some((schema, _)) = cte_ctx.get(qualifier).or_else(|| {
                        cte_ctx
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(qualifier))
                            .map(|(_, v)| v)
                    }) {
                        count += schema.columns.len();
                        continue;
                    }
                }
                let tbl = database
                    .get_table(qualifier)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualifier.clone()))?;
                count += tbl.schema.columns.len();
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                // Each expression contributes one column
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Count total columns in a FROM clause (handles joins and multiple tables)
///
/// Issue #3562: Added CTE context so CTEs can be resolved in FROM clause
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
    cte_results: Option<&HashMap<String, CteResult>>,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            // Issue #3562: Check CTEs first before database tables
            if let Some(cte_ctx) = cte_results {
                if let Some((schema, _)) = cte_ctx.get(name).or_else(|| {
                    cte_ctx.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v)
                }) {
                    return Ok(schema.columns.len());
                }
            }
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database, cte_results)?;
            let right_count = count_columns_in_from_clause(right, database, cte_results)?;
            Ok(left_count + right_count)
        }
        vibesql_ast::FromClause::Subquery { .. } => {
            // For subqueries in FROM, we'd need to execute them to know column count
            // This is complex, so for now we'll return an error
            // In practice, this case is rare in IN subqueries
            Err(ExecutorError::UnsupportedFeature(
                "Subqueries in FROM clause within IN predicates are not yet supported for schema validation".to_string(),
            ))
        }
        vibesql_ast::FromClause::Values { rows, column_aliases, .. } => {
            // VALUES clause column count is determined by either:
            // 1. The column_aliases if provided, or
            // 2. The number of expressions in the first row
            if let Some(aliases) = column_aliases {
                Ok(aliases.len())
            } else if let Some(first_row) = rows.first() {
                Ok(first_row.len())
            } else {
                Ok(0) // Empty VALUES clause
            }
        }
    }
}
