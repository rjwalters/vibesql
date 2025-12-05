//! Schema validation utilities for subqueries
//!
//! This module provides utilities to compute and validate the schema of
//! subquery results, particularly for handling wildcards and column counts.

use crate::errors::ExecutorError;
use crate::select::cte::CteResult;
use std::collections::HashMap;

/// Build a merged outer schema that includes all parent scopes
///
/// For nested correlated subqueries, we need to pass a schema that includes:
/// 1. The current level's schema (self.schema)
/// 2. Any outer schemas from parent levels (self.outer_schema)
///
/// This ensures that deeply nested subqueries can resolve columns from
/// any ancestor scope, not just the immediate parent.
///
/// # Example: TPC-H Q20
///
/// ```sql
/// SELECT ... FROM supplier WHERE s_suppkey IN (
///     SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (...) AND ps_availqty > (
///         SELECT 0.5 * SUM(l_quantity) FROM lineitem
///         WHERE l_partkey = ps_partkey  -- Needs to resolve ps_partkey from partsupp (2 levels up)
///     )
/// )
/// ```
///
/// Without merging, the lineitem subquery only sees its immediate parent scope,
/// causing ps_partkey to not be found.
pub(super) fn build_merged_outer_schema<'a>(
    current_schema: &'a crate::schema::CombinedSchema,
    outer_schema: Option<&'a crate::schema::CombinedSchema>,
) -> std::borrow::Cow<'a, crate::schema::CombinedSchema> {
    if let Some(outer) = outer_schema {
        // Build merged schema: outer schema + current schema
        // Use SchemaBuilder for efficient O(n) construction
        let mut builder = crate::schema::SchemaBuilder::from_schema(outer.clone());

        // Add all tables from current schema
        for (table_name, (_offset, table_schema)) in &current_schema.table_schemas {
            builder.add_table(table_name.clone(), table_schema.clone());
        }

        std::borrow::Cow::Owned(builder.build())
    } else {
        // No outer schema to merge, just use current schema
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

        std::borrow::Cow::Owned(vibesql_storage::Row { values: merged_values })
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
    }
}
