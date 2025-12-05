//! GROUP BY execution for native columnar path
//!
//! This module implements the GROUP BY path for native columnar execution,
//! using hash-based grouping with SIMD-accelerated aggregation.

use super::cse::{expression_depth, find_cached_subexpression, hash_expression};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{columnar, executor::builder::SelectExecutor},
};
use vibesql_ast::Expression;

impl SelectExecutor<'_> {
    /// Execute a GROUP BY query using columnar hash aggregation
    ///
    /// This method implements the GROUP BY path for native columnar execution,
    /// using hash-based grouping with the existing `columnar_group_by` function.
    pub(in crate::select::executor) fn execute_columnar_group_by(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        batch: &columnar::ColumnarBatch,
        predicates: &[columnar::ColumnPredicate],
        aggregates: &[columnar::AggregateSpec],
        schema: &CombinedSchema,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        #[cfg(feature = "profile-q6")]
        let start = std::time::Instant::now();

        // Phase 1: Apply auto-vectorized filtering to get filtered batch
        let filtered_batch = if predicates.is_empty() {
            batch.clone()
        } else {
            columnar::simd_filter_batch(batch, predicates)?
        };

        #[cfg(feature = "profile-q6")]
        {
            let filter_time = start.elapsed();
            eprintln!(
                "[PROFILE-Q6]   GROUP BY Phase 1 - Filter: {:?} ({}/{} rows)",
                filter_time,
                filtered_batch.row_count(),
                batch.row_count()
            );
        }

        // Phase 2: Extract group column indices from GROUP BY clause
        let group_by_clause = stmt.group_by.as_ref().ok_or_else(|| {
            ExecutorError::Other("GROUP BY clause required for group_by execution".to_string())
        })?;

        // Only support simple GROUP BY in columnar path (not ROLLUP/CUBE/GROUPING SETS)
        let simple_exprs = group_by_clause.as_simple().ok_or_else(|| {
            ExecutorError::Other(
                "ROLLUP/CUBE/GROUPING SETS not supported in columnar execution path".to_string(),
            )
        })?;

        let group_cols: Vec<usize> = simple_exprs
            .iter()
            .filter_map(|expr| {
                match expr {
                    vibesql_ast::Expression::ColumnRef { table, column } => {
                        schema.get_column_index(table.as_deref(), column.as_str())
                    }
                    _ => None, // Only simple column references supported for now
                }
            })
            .collect();

        if group_cols.len() != simple_exprs.len() {
            log::debug!("GROUP BY contains non-column expressions, falling back to row-oriented");
            return Err(ExecutorError::Other(
                "GROUP BY with non-column expressions not supported in columnar path".to_string(),
            ));
        }

        // Phase 3: Handle expression aggregates by pre-computing them as new columns
        // This enables TPC-H Q1 style queries with SUM(col * expr) to use SIMD GROUP BY
        //
        // Optimization: Common Sub-Expression Elimination (CSE)
        // For Q1-style queries with expressions like:
        //   SUM(l_extendedprice * (1 - l_discount))         -- E1
        //   SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))  -- E2 = E1 * (1 + l_tax)
        // We detect that E1 is a sub-expression of E2 and compute E1 first,
        // then use the cached column to compute E2, avoiding redundant arithmetic.
        let has_expression_aggs = aggregates
            .iter()
            .any(|spec| matches!(&spec.source, columnar::AggregateSource::Expression(_)));

        #[cfg(feature = "profile-q6")]
        let expr_start = std::time::Instant::now();

        let (expanded_batch, agg_cols) = if has_expression_aggs {
            // Clone the batch so we can add computed expression columns
            let mut expanded = filtered_batch.clone();
            let mut expanded_agg_cols = Vec::with_capacity(aggregates.len());

            // Expression cache: maps expression hash to column index
            // This allows us to reuse previously computed expression columns
            let mut expr_cache: std::collections::HashMap<u64, usize> =
                std::collections::HashMap::new();

            // First pass: collect all expressions and sort by complexity (simpler first)
            // This ensures sub-expressions are evaluated before expressions that use them
            let mut expr_indices: Vec<(usize, &columnar::AggregateSpec)> = aggregates
                .iter()
                .enumerate()
                .filter(|(_, spec)| {
                    matches!(&spec.source, columnar::AggregateSource::Expression(_))
                })
                .collect();

            // Sort by expression depth (simpler expressions first for CSE)
            expr_indices.sort_by_key(|(_, spec)| {
                if let columnar::AggregateSource::Expression(expr) = &spec.source {
                    expression_depth(expr)
                } else {
                    0
                }
            });

            // Build a mapping of original expression index -> computed column index
            let mut computed_expr_cols: std::collections::HashMap<usize, usize> =
                std::collections::HashMap::new();

            // Second pass: evaluate expressions in order of complexity
            for (orig_idx, spec) in &expr_indices {
                if let columnar::AggregateSource::Expression(expr) = &spec.source {
                    let expr_hash = hash_expression(expr);

                    if let Some(&cached_col_idx) = expr_cache.get(&expr_hash) {
                        // Exact expression match - reuse cached column
                        log::debug!(
                            "GROUP BY CSE: reusing cached column {} for expression",
                            cached_col_idx
                        );
                        computed_expr_cols.insert(*orig_idx, cached_col_idx);
                    } else {
                        // Check if this expression can be computed using a cached sub-expression
                        // For TPC-H Q1: E2 = E1 * (1 + l_tax) where E1 is already computed
                        let expr_col = if let Some((sub_expr_col, remaining_expr)) =
                            find_cached_subexpression(expr, &expr_cache)
                        {
                            log::debug!(
                                "GROUP BY CSE: using cached sub-expression from column {} for compound expression",
                                sub_expr_col
                            );
                            // Evaluate only the remaining part using the cached column
                            columnar::evaluate_expression_with_cached_column(
                                &expanded,
                                &remaining_expr,
                                sub_expr_col,
                                schema,
                            )?
                        } else {
                            // No sub-expression found - evaluate full expression
                            columnar::evaluate_expression_to_column(&expanded, expr, schema)?
                        };

                        expanded.add_column(expr_col)?;
                        let col_idx = expanded.column_count() - 1;
                        expr_cache.insert(expr_hash, col_idx);
                        computed_expr_cols.insert(*orig_idx, col_idx);
                    }
                }
            }

            // Third pass: build final aggregate column indices in original order
            for (orig_idx, spec) in aggregates.iter().enumerate() {
                match &spec.source {
                    columnar::AggregateSource::Column(idx) => {
                        expanded_agg_cols.push((*idx, spec.op));
                    }
                    columnar::AggregateSource::CountStar => {
                        expanded_agg_cols.push((0, columnar::AggregateOp::Count));
                    }
                    columnar::AggregateSource::Expression(_) => {
                        let col_idx = computed_expr_cols
                            .get(&orig_idx)
                            .copied()
                            .expect("Expression should have been computed in second pass");
                        expanded_agg_cols.push((col_idx, spec.op));
                    }
                }
            }

            log::debug!(
                "GROUP BY: expanded batch with {} expression columns ({} -> {} columns), CSE cache hits: {}",
                aggregates.iter().filter(|s| matches!(&s.source, columnar::AggregateSource::Expression(_))).count(),
                filtered_batch.column_count(),
                expanded.column_count(),
                aggregates.iter().filter(|s| matches!(&s.source, columnar::AggregateSource::Expression(_))).count()
                    - (expanded.column_count() - filtered_batch.column_count())
            );

            (expanded, expanded_agg_cols)
        } else {
            // No expression aggregates - convert directly (no batch clone needed)
            let agg_cols: Vec<(usize, columnar::AggregateOp)> = aggregates
                .iter()
                .map(|spec| match &spec.source {
                    columnar::AggregateSource::Column(idx) => (*idx, spec.op),
                    columnar::AggregateSource::CountStar => (0, columnar::AggregateOp::Count),
                    columnar::AggregateSource::Expression(_) => unreachable!(),
                })
                .collect();
            (filtered_batch, agg_cols)
        };

        #[cfg(feature = "profile-q6")]
        {
            let expr_time = expr_start.elapsed();
            if has_expression_aggs {
                eprintln!(
                    "[PROFILE-Q6]   GROUP BY Phase 2 - Expression pre-compute: {:?}",
                    expr_time
                );
            }
        }

        #[cfg(feature = "profile-q6")]
        let group_start = std::time::Instant::now();

        // Phase 4: Execute SIMD-accelerated GROUP BY aggregation directly on batch
        // Uses columnar_group_by_batch for 3-5x improvement over scalar path
        let result = columnar::columnar_group_by_batch(&expanded_batch, &group_cols, &agg_cols)?;

        #[cfg(feature = "profile-q6")]
        {
            let group_time = group_start.elapsed();
            eprintln!(
                "[PROFILE-Q6]   GROUP BY Phase 2 - SIMD hash aggregation: {:?} ({} groups)",
                group_time,
                result.len()
            );
        }

        Ok(result)
    }

    /// Execute GROUP BY aggregation on a joined columnar batch
    pub(in crate::select::executor) fn execute_columnar_join_group_by(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        batch: &columnar::ColumnarBatch,
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        let group_by_clause = match stmt.group_by.as_ref() {
            Some(g) => g,
            None => return Ok(Some(batch.to_rows()?)),
        };

        // Only support simple GROUP BY
        let simple_exprs = match group_by_clause.as_simple() {
            Some(exprs) => exprs,
            None => {
                log::debug!("Columnar join: ROLLUP/CUBE/GROUPING SETS not supported");
                return Ok(None);
            }
        };

        // Resolve group column indices
        let group_cols: Vec<usize> = simple_exprs
            .iter()
            .filter_map(|expr| match expr {
                Expression::ColumnRef { table, column } => {
                    schema.get_column_index(table.as_deref(), column.as_str())
                }
                _ => None,
            })
            .collect();

        if group_cols.len() != simple_exprs.len() {
            log::debug!("Columnar join: some GROUP BY columns couldn't be resolved");
            return Ok(None);
        }

        // Extract select expressions
        let select_exprs: Vec<_> = stmt
            .select_list
            .iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => Some(expr.clone()),
                _ => None,
            })
            .collect();

        // Extract aggregates
        let aggregates = match columnar::extract_aggregates(&select_exprs, schema) {
            Some(aggs) => aggs,
            None => {
                log::debug!("Columnar join: failed to extract aggregates");
                return Ok(None);
            }
        };

        // Convert aggregates to (column_idx, op) format
        let agg_cols: Vec<(usize, columnar::AggregateOp)> = aggregates
            .iter()
            .filter_map(|spec| {
                match &spec.source {
                    columnar::AggregateSource::Column(idx) => Some((*idx, spec.op)),
                    columnar::AggregateSource::CountStar => Some((0, columnar::AggregateOp::Count)),
                    columnar::AggregateSource::Expression(_) => {
                        // Expression aggregates require row-based evaluation
                        log::debug!("Columnar join: expression aggregate not supported");
                        None
                    }
                }
            })
            .collect();

        if agg_cols.len() != aggregates.len() {
            return Ok(None);
        }

        // Convert to rows and run GROUP BY
        let rows = batch.to_rows()?;
        let result = columnar::columnar_group_by(&rows, &group_cols, &agg_cols, None)?;

        log::info!("Columnar join: GROUP BY produced {} groups", result.len());
        Ok(Some(result))
    }
}
