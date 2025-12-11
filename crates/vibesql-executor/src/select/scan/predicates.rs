//! Predicate pushdown and filtering logic
//!
//! Handles optimization of WHERE clause predicates by:
//! - Applying pre-decomposed table-local predicates early during table scans
//! - Reducing intermediate result sizes before joins
//!
//! **Phase 1 Optimization**: This module now uses `PredicatePlan` to avoid redundant
//! WHERE clause decomposition. The plan is computed once at query start and passed through.

use std::collections::HashMap;
#[cfg(feature = "parallel")]
use std::sync::Arc;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, optimizer::PredicatePlan,
    schema::CombinedSchema, select::cte::CteResult,
};

/// Apply table-local predicates from a pre-computed predicate plan
///
/// This function implements predicate pushdown by filtering rows early,
/// before they contribute to larger Cartesian products in JOINs.
///
/// Uses parallel filtering when beneficial based on row count and hardware.
///
/// **Phase 1**: Now accepts `PredicatePlan` instead of decomposing WHERE clause internally.
/// **Phase 2**: Attempts columnar execution for complex predicates with OR logic.
/// **Phase 4**: Uses cost-based predicate ordering via selectivity estimation.
/// **Issue #3562**: Added CTE context for IN subqueries referencing CTEs.
pub(crate) fn apply_table_local_predicates(
    rows: Vec<vibesql_storage::Row>,
    schema: CombinedSchema,
    predicate_plan: &PredicatePlan,
    table_name: &str,
    database: &vibesql_storage::Database,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
    cte_context: Option<&HashMap<String, CteResult>>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
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
    // Falls back to parse order if statistics unavailable
    let ordered_preds =
        predicate_plan.get_table_filters_ordered(table_name, table_stats_owned.as_ref());

    // If there are table-local predicates, apply them
    if !ordered_preds.is_empty() {
        // Combine ordered predicates with AND
        let combined_where = combine_predicates_with_and(ordered_preds);

        // Fast path: Try columnar execution for non-correlated queries
        // Threshold lowered to >=100 rows for better performance on SQLLogicTest queries
        if outer_row.is_none() && outer_schema.is_none() && rows.len() >= 100 {
            use crate::select::columnar::{create_filter_bitmap_tree, extract_predicate_tree};

            if let Some(predicate_tree) = extract_predicate_tree(&combined_where, &schema) {
                // Use columnar filtering
                let bitmap =
                    create_filter_bitmap_tree(rows.len(), &predicate_tree, |row_idx, col_idx| {
                        rows.get(row_idx).and_then(|row| row.get(col_idx))
                    })?;

                // Keep only rows that pass the filter
                let filtered_rows: Vec<_> = rows
                    .into_iter()
                    .zip(&bitmap)
                    .filter_map(|(row, &passes)| if passes { Some(row) } else { None })
                    .collect();

                return Ok(filtered_rows);
            }
        }

        // Create evaluator for filtering with outer context for correlated subqueries
        // Issue #3562: Add CTE context so IN subqueries can reference CTEs
        let evaluator = match (outer_row, outer_schema, cte_context) {
            (Some(outer_row), Some(outer_schema), Some(cte_ctx)) => {
                CombinedExpressionEvaluator::with_database_and_outer_context_and_cte(
                    &schema,
                    database,
                    outer_row,
                    outer_schema,
                    cte_ctx,
                )
            }
            (Some(outer_row), Some(outer_schema), None) => {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &schema,
                    database,
                    outer_row,
                    outer_schema,
                )
            }
            (None, None, Some(cte_ctx)) => {
                CombinedExpressionEvaluator::with_database_and_cte(&schema, database, cte_ctx)
            }
            _ => CombinedExpressionEvaluator::with_database(&schema, database),
        };

        // Check if we should use parallel filtering
        #[cfg(feature = "parallel")]
        {
            let config = ParallelConfig::global();
            if config.should_parallelize_scan(rows.len()) {
                return apply_predicates_parallel(rows, combined_where, evaluator);
            }
        }

        // Sequential path for small datasets
        let mut filtered_rows = Vec::new();
        for row in rows {
            evaluator.clear_cse_cache();

            let include_row = match evaluator.eval(&combined_where, &row)? {
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
                filtered_rows.push(row);
            }
        }
        return Ok(filtered_rows);
    }

    // No table-local predicates - return rows as-is
    Ok(rows)
}

/// Apply predicates using parallel execution
///
/// Issue #3562: Now properly propagates CTE context to parallel threads,
/// enabling IN subqueries that reference CTEs during parallel filtering.
#[cfg(feature = "parallel")]
fn apply_predicates_parallel(
    rows: Vec<vibesql_storage::Row>,
    combined_where: vibesql_ast::Expression,
    evaluator: CombinedExpressionEvaluator,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Clone expression for thread-safe sharing
    let where_expr_arc = Arc::new(combined_where);

    // Extract evaluator components for parallel execution (including CTE context)
    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        evaluator.get_parallel_components();

    // Use rayon's parallel iterator for filtering
    let result: Result<Vec<_>, ExecutorError> = rows
        .into_par_iter()
        .map(|row| {
            // Create thread-local evaluator with independent caches
            let thread_evaluator = CombinedExpressionEvaluator::from_parallel_components(
                schema,
                database,
                outer_row,
                outer_schema,
                window_mapping,
                cte_context,
                enable_cse,
            );

            // Evaluate predicate for this row
            let include_row = match thread_evaluator.eval(&where_expr_arc, &row)? {
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
                Ok(Some(row))
            } else {
                Ok(None)
            }
        })
        .collect();

    // Filter out None values and extract Ok rows
    result.map(|v| v.into_iter().flatten().collect())
}

/// Helper function to combine predicates with AND operator
pub(crate) fn combine_predicates_with_and(
    mut predicates: Vec<vibesql_ast::Expression>,
) -> vibesql_ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}

/// Filter-while-copy optimization for CTE scans
///
/// Unlike `apply_table_local_predicates` which takes ownership of rows,
/// this function takes a borrowed slice and only clones rows that pass
/// the filter. This is critical for CTE performance when a CTE is scanned
/// multiple times with different filters (e.g., TPC-DS Q4 with 6-way self-join).
///
/// **Issue #4199**: Avoids cloning all CTE rows (O(n)) when only a small
/// fraction pass the filter. For Q4, this reduces row clones from ~54,000
/// to ~3,000.
pub(crate) fn filter_and_clone_rows(
    rows: &[vibesql_storage::Row],
    schema: CombinedSchema,
    predicate_plan: &PredicatePlan,
    table_name: &str,
    database: &vibesql_storage::Database,
    cte_context: Option<&HashMap<String, CteResult>>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Get predicates ordered by selectivity (most selective first)
    let ordered_preds = predicate_plan.get_table_filters_ordered(table_name, None);

    // If there are no table-local predicates, clone all rows
    if ordered_preds.is_empty() {
        return Ok(rows.to_vec());
    }

    // Combine ordered predicates with AND
    let combined_where = combine_predicates_with_and(ordered_preds);

    // Fast path: Try columnar execution for larger datasets
    // Columnar still requires building a bitmap, but avoids per-row evaluator overhead
    if rows.len() >= 100 {
        use crate::select::columnar::{create_filter_bitmap_tree, extract_predicate_tree};

        if let Some(predicate_tree) = extract_predicate_tree(&combined_where, &schema) {
            // Use columnar filtering with bitmap
            let bitmap =
                create_filter_bitmap_tree(rows.len(), &predicate_tree, |row_idx, col_idx| {
                    rows.get(row_idx).and_then(|row| row.get(col_idx))
                })?;

            // Only clone rows that pass the filter
            let filtered_rows: Vec<_> = rows
                .iter()
                .zip(&bitmap)
                .filter_map(|(row, &passes)| if passes { Some(row.clone()) } else { None })
                .collect();

            return Ok(filtered_rows);
        }
    }

    // Create evaluator for filtering
    let evaluator = match cte_context {
        Some(cte_ctx) => {
            CombinedExpressionEvaluator::with_database_and_cte(&schema, database, cte_ctx)
        }
        None => CombinedExpressionEvaluator::with_database(&schema, database),
    };

    // Sequential path: evaluate each row and only clone if it passes
    let mut filtered_rows = Vec::new();
    for row in rows {
        evaluator.clear_cse_cache();

        let include_row = match evaluator.eval(&combined_where, row)? {
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
            filtered_rows.push(row.clone());
        }
    }

    Ok(filtered_rows)
}
