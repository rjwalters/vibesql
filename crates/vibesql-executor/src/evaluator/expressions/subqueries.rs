//! Subquery evaluation methods

use super::super::caching::compute_subquery_hash;
use super::super::core::ExpressionEvaluator;
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
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

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
                let select_executor =
                    crate::select::SelectExecutor::new_with_depth(database, self.depth);
                let rows = select_executor.execute(subquery)?;

                // Cache the result
                self.subquery_cache.borrow_mut().put(cache_key, rows.clone());
                rows
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
            let select_executor = crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                &outer_combined,
                self.depth,
            );
            select_executor.execute(subquery)?
        };

        // SQL standard (R-35033-20570): The subquery must be a scalar subquery
        // (single column) when the left expression is not a row value expression.
        // We must validate this AFTER execution because wildcards like SELECT *
        // expand to multiple columns at runtime.
        if !rows.is_empty() && rows[0].values.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: rows[0].values.len(),
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

        let mut found_null = false;
        for subquery_row in &rows {
            let subquery_val =
                subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

            if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            if expr_val == *subquery_val {
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
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

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
                let select_executor =
                    crate::select::SelectExecutor::new_with_depth(database, self.depth);
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
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

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
                let select_executor =
                    crate::select::SelectExecutor::new_with_depth(database, self.depth);
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
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

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
                let select_executor =
                    crate::select::SelectExecutor::new_with_depth(database, self.depth);
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
