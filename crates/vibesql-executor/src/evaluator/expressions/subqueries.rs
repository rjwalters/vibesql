//! Subquery evaluation methods

use super::super::{caching::compute_subquery_hash, core::ExpressionEvaluator};
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate IN subquery predicate
    ///
    /// **Optimization**: Caches non-correlated subquery results to avoid redundant execution.
    pub(super) fn eval_in_subquery(
        &self,
        expr: &vibesql_ast::Expression,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "IN with subquery requires database reference".to_string(),
        ))?;

        let expr_val = self.eval(expr, row)?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer = self
            .table_alias
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated = crate::correlation::is_correlated(subquery, &outer_combined);

        // Execute or retrieve from cache
        let rows = if !is_correlated {
            // Non-correlated subquery - try cache first
            let cache_key = compute_subquery_hash(subquery);

            // Check cache (explicitly scope the borrow to avoid holding it during execution)
            // Use peek() for readonly access (get() requires &mut for LRU tracking)
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            if let Some(cached_rows) = cached_result {
                // Cache hit - use cached result
                cached_rows
            } else {
                // Cache miss - execute and cache
                // IMPORTANT: Propagate depth to prevent bypassing MAX_EXPRESSION_DEPTH
                // Use CTE context if available for WITH clause support in UPDATE/DELETE
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let rows = select_executor.execute(subquery)?;

                // Cache the result
                self.subquery_cache.borrow_mut().put(cache_key, rows.clone());
                rows
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
            // TODO: Add CTE context support for correlated IN subqueries
            let select_executor = if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else {
                crate::select::SelectExecutor::new_with_outer_context_and_depth(
                    database,
                    row,
                    &outer_combined,
                    self.depth,
                )
            };
            select_executor.execute(subquery)?
        };

        // SQL standard (R-35033-20570): The subquery must be a scalar subquery
        // (single column) when the left expression is not a row value expression.
        // We must validate this AFTER execution because wildcards like SELECT *
        // expand to multiple columns at runtime.
        //
        // Issue: Must also validate when rows are empty (e.g., empty table).
        // Without this, `5 IN (SELECT a,b FROM empty_table)` incorrectly returns 0
        // instead of erroring with "sub-select returns 2 columns - expected 1".
        let column_count = if !rows.is_empty() {
            rows[0].values.len()
        } else {
            // For empty result sets, compute column count from select list
            crate::evaluator::combined::subqueries::schema_utils::compute_select_list_column_count(
                subquery,
                database,
                self.cte_context,
            )?
        };
        if column_count != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: column_count,
            });
        }

        // SQL standard behavior for NULL IN (subquery):
        // - NULL IN (empty set) → FALSE (special case per R-52275-55503)
        // - NULL IN (non-empty set without NULL) → NULL (three-valued logic)
        // - NULL IN (set containing NULL) → NULL
        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            // Special case: empty set always returns FALSE for IN, TRUE for NOT IN
            // This overrides the usual NULL behavior (R-52275-55503)
            if rows.is_empty() {
                return Ok(vibesql_types::SqlValue::Boolean(negated));
            }

            // For non-empty sets, check if subquery contains NULL
            for subquery_row in &rows {
                let subquery_val = subquery_row
                    .get(0)
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                    // NULL IN (set with NULL) → NULL
                    return Ok(vibesql_types::SqlValue::Null);
                }
            }

            // NULL IN (non-empty set without NULL) → NULL (not FALSE!)
            // This follows three-valued logic: NULL compared to any value is NULL
            return Ok(vibesql_types::SqlValue::Null);
        }

        // Get affinities for coercion
        // Left expression affinity (e.g., TEXT column from outer table)
        let left_affinity = self.get_expression_affinity(expr);
        // Subquery result affinity (e.g., INTEGER column from inner table)
        let subquery_affinity = crate::evaluator::combined::subqueries::schema_utils::get_subquery_first_column_affinity(subquery, database);

        let mut found_null = false;
        for subquery_row in &rows {
            let subquery_val =
                subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

            if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            // Apply affinity coercion before comparison
            // SQLite IN subquery coercion rules:
            // - If subquery has INTEGER/REAL/NUMERIC affinity and expr is TEXT, convert TEXT to numeric
            // - If subquery has TEXT affinity and expr is numeric, convert numeric to TEXT
            let (coerced_expr, coerced_subquery) = apply_in_subquery_affinity_coercion(
                expr_val.clone(),
                subquery_val.clone(),
                left_affinity,
                subquery_affinity,
            );

            if coerced_expr == coerced_subquery {
                return Ok(vibesql_types::SqlValue::Boolean(!negated));
            }
        }

        if found_null {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(vibesql_types::SqlValue::Boolean(negated))
        }
    }

    /// Evaluate scalar subquery
    ///
    /// **Optimization**: Caches non-correlated subquery results to avoid redundant execution.
    pub(super) fn eval_scalar_subquery(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer = self
            .table_alias
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated = crate::correlation::is_correlated(subquery, &outer_combined);

        // Execute or retrieve from cache
        let rows = if !is_correlated {
            // Non-correlated subquery - try cache first
            let cache_key = compute_subquery_hash(subquery);

            // Check cache (use peek() for readonly access)
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            if let Some(cached_rows) = cached_result {
                // Cache hit - use cached result
                cached_rows
            } else {
                // Cache miss - execute and cache
                // Use CTE context if available for WITH clause support in UPDATE/DELETE
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let executed_rows = select_executor.execute(subquery)?;

                // Cache the result
                self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());
                executed_rows
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
            let select_executor = if !outer_combined.table_schemas.is_empty() {
                crate::select::SelectExecutor::new_with_outer_context_and_depth(
                    database,
                    row,
                    &outer_combined,
                    self.depth,
                )
            } else if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else {
                crate::select::SelectExecutor::new(database)
            };
            select_executor.execute(subquery)?
        };

        // Delegate to shared logic
        super::super::subqueries_shared::eval_scalar_subquery_core(&rows)
    }

    /// Evaluate EXISTS predicate
    ///
    /// **Optimization**: Caches non-correlated subquery results to avoid redundant execution.
    pub(super) fn eval_exists(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "EXISTS requires database reference".to_string(),
        ))?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer = self
            .table_alias
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated = crate::correlation::is_correlated(subquery, &outer_combined);

        // Execute or retrieve from cache
        let rows = if !is_correlated {
            // Non-correlated subquery - try cache first
            let cache_key = compute_subquery_hash(subquery);

            // Check cache (use peek() for readonly access)
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            if let Some(cached_rows) = cached_result {
                // Cache hit - use cached result
                cached_rows
            } else {
                // Cache miss - execute and cache
                // Use CTE context if available for WITH clause support in UPDATE/DELETE
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let executed_rows = select_executor.execute(subquery)?;

                // Cache the result
                self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());
                executed_rows
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
            let select_executor = if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else if !outer_combined.table_schemas.is_empty() {
                crate::select::SelectExecutor::new_with_outer_context_and_depth(
                    database,
                    row,
                    &outer_combined,
                    self.depth,
                )
            } else {
                crate::select::SelectExecutor::new(database)
            };
            select_executor.execute(subquery)?
        };

        // Delegate to shared logic
        Ok(super::super::subqueries_shared::eval_exists_core(!rows.is_empty(), negated))
    }

    /// Evaluate quantified comparison (ALL/ANY/SOME)
    ///
    /// **Optimization**: Caches non-correlated subquery results to avoid redundant execution.
    pub(super) fn eval_quantified(
        &self,
        expr: &vibesql_ast::Expression,
        op: &vibesql_ast::BinaryOperator,
        quantifier: &vibesql_ast::Quantifier,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Quantified comparison requires database reference".to_string(),
        ))?;

        // Evaluate the left-hand expression
        let left_val = self.eval(expr, row)?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer = self
            .table_alias
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated = crate::correlation::is_correlated(subquery, &outer_combined);

        // Execute or retrieve from cache
        let rows = if !is_correlated {
            // Non-correlated subquery - try cache first
            let cache_key = compute_subquery_hash(subquery);

            // Check cache (use peek() for readonly access)
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            if let Some(cached_rows) = cached_result {
                // Cache hit - use cached result
                cached_rows
            } else {
                // Cache miss - execute and cache
                // Use CTE context if available for WITH clause support in UPDATE/DELETE
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let executed_rows = select_executor.execute(subquery)?;

                // Cache the result
                self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());
                executed_rows
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
            let select_executor = if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else if !outer_combined.table_schemas.is_empty() {
                crate::select::SelectExecutor::new_with_outer_context_and_depth(
                    database,
                    row,
                    &outer_combined,
                    self.depth,
                )
            } else {
                crate::select::SelectExecutor::new(database)
            };
            select_executor.execute(subquery)?
        };

        // Delegate to shared logic
        super::super::subqueries_shared::eval_quantified_core(
            &left_val,
            &rows,
            op,
            quantifier,
            |left, op, right| {
                Self::eval_binary_op_static(left, op, right, vibesql_types::SqlMode::default())
            },
        )
    }
}

/// Apply SQLite affinity coercion rules for IN subquery comparisons
///
/// SQLite uses the affinity of the subquery's first column to determine comparison rules:
/// - If subquery has INTEGER/REAL/NUMERIC affinity and left is TEXT value, convert TEXT to numeric
/// - If subquery has TEXT affinity and left is numeric, convert numeric to TEXT
/// - If either has no affinity, use storage class ordering (no conversion)
///
/// This differs from regular `=` comparison which uses storage class ordering by default.
fn apply_in_subquery_affinity_coercion(
    left_val: vibesql_types::SqlValue,
    right_val: vibesql_types::SqlValue,
    left_affinity: Option<vibesql_types::TypeAffinity>,
    right_affinity: Option<vibesql_types::TypeAffinity>,
) -> (vibesql_types::SqlValue, vibesql_types::SqlValue) {
    use vibesql_types::{SqlValue, TypeAffinity};

    // If right (subquery) has numeric affinity and left is a TEXT value, convert TEXT to numeric
    let right_is_numeric_affinity = matches!(
        right_affinity,
        Some(TypeAffinity::Integer) | Some(TypeAffinity::Real) | Some(TypeAffinity::Numeric)
    );

    if right_is_numeric_affinity {
        if let SqlValue::Varchar(s) | SqlValue::Character(s) = &left_val {
            // Try to convert left TEXT to numeric
            if let Some(coerced) = try_coerce_string_to_numeric(s) {
                return (coerced, right_val);
            }
        }
    }

    // If left has numeric affinity and right is a TEXT value, convert TEXT to numeric
    let left_is_numeric_affinity = matches!(
        left_affinity,
        Some(TypeAffinity::Integer) | Some(TypeAffinity::Real) | Some(TypeAffinity::Numeric)
    );

    if left_is_numeric_affinity {
        if let SqlValue::Varchar(s) | SqlValue::Character(s) = &right_val {
            // Try to convert right TEXT to numeric
            if let Some(coerced) = try_coerce_string_to_numeric(s) {
                return (left_val, coerced);
            }
        }
    }

    // If right has TEXT affinity and left is numeric, convert numeric to TEXT
    if matches!(right_affinity, Some(TypeAffinity::Text)) {
        if let SqlValue::Integer(n) = &left_val {
            return (
                SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                right_val,
            );
        }
        if let SqlValue::Real(n) | SqlValue::Double(n) = &left_val {
            return (
                SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text(*n))),
                right_val,
            );
        }
    }

    // If left has TEXT affinity and right is numeric, convert numeric to TEXT
    if matches!(left_affinity, Some(TypeAffinity::Text)) {
        if let SqlValue::Integer(n) = &right_val {
            return (
                left_val,
                SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
            );
        }
        if let SqlValue::Real(n) | SqlValue::Double(n) = &right_val {
            return (
                left_val,
                SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text(*n))),
            );
        }
    }

    // No coercion needed - use original values
    (left_val, right_val)
}

/// Try to coerce a string to a numeric value
fn try_coerce_string_to_numeric(s: &str) -> Option<vibesql_types::SqlValue> {
    // Try integer first
    if let Ok(n) = s.parse::<i64>() {
        return Some(vibesql_types::SqlValue::Integer(n));
    }

    // Try float
    if let Ok(n) = s.parse::<f64>() {
        // For values like "10.0", use Real to preserve the decimal
        return Some(vibesql_types::SqlValue::Real(n));
    }

    None
}

/// Format a float for TEXT comparison
fn format_float_for_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        format!("{}.0", n as i64)
    } else {
        n.to_string()
    }
}
