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

        // Trigger context (#5585): substitute NEW/OLD pseudo-variables with their
        // literal values so a correlated IN subquery in a trigger WHEN clause
        // resolves against the firing row. See `substitute_trigger_pseudo_vars`.
        let substituted_subquery = match self.trigger_context {
            Some(trigger_ctx) => Some(substitute_trigger_pseudo_vars(subquery, trigger_ctx)?),
            None => None,
        };
        let subquery = substituted_subquery.as_ref().unwrap_or(subquery);

        // Handle row value IN subquery: (a, b) IN (SELECT x, y FROM t)
        if let vibesql_ast::Expression::RowValueConstructor(expr_elements) = expr {
            return self.eval_row_value_in_subquery(
                expr_elements,
                subquery,
                negated,
                row,
                database,
            );
        }

        // Multi-column scalar subquery on the LHS of IN:
        // `(SELECT a, b) IN (SELECT x, y FROM t)` — evaluate the LHS subquery
        // to a row and reuse the row-value IN path.
        if let vibesql_ast::Expression::ScalarSubquery(left_sub) = expr {
            if !crate::evaluator::row_value::subquery_is_obviously_single_column(left_sub) {
                let arity = crate::evaluator::combined::subqueries::schema_utils::compute_select_list_column_count(
                    left_sub,
                    database,
                    self.cte_context,
                )?;
                if arity > 1 {
                    let left_values = self.eval_scalar_subquery_as_row(left_sub, row, arity)?;
                    let elem_exprs: Vec<vibesql_ast::Expression> =
                        left_values.into_iter().map(vibesql_ast::Expression::Literal).collect();
                    return self.eval_row_value_in_subquery(
                        &elem_exprs,
                        subquery,
                        negated,
                        row,
                        database,
                    );
                }
            }
        }

        let expr_val = self.eval(expr, row)?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer =
            self.table_alias.as_ref().cloned().unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated =
            crate::correlation::is_correlated(subquery, &outer_combined, self.database);

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
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
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
        let rows = self.execute_scalar_subquery_rows(subquery, row)?;
        super::super::subqueries_shared::eval_scalar_subquery_core(&rows)
    }

    /// Evaluate a scalar subquery as a row value of `expected` columns, for use
    /// in row-value comparisons like `(a, b, c) > (SELECT a, b, c FROM ...)`.
    ///
    /// Returns the first result row's values (SQLite uses the first row when a
    /// scalar subquery yields several). A subquery returning no rows behaves as a
    /// row of NULLs. The column count is validated against `expected`, producing
    /// a SQLite-compatible "sub-select returns N values" arity error on mismatch.
    pub(super) fn eval_scalar_subquery_as_row(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
        expected: usize,
    ) -> Result<Vec<vibesql_types::SqlValue>, ExecutorError> {
        let rows = self.execute_scalar_subquery_rows(subquery, row)?;
        if rows.is_empty() {
            // No rows: behaves as a row of NULLs, but still validate arity so a
            // width mismatch reports the same error SQLite raises at prepare time.
            let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                "Subquery execution requires database reference".to_string(),
            ))?;
            let column_count =
                crate::evaluator::combined::subqueries::schema_utils::compute_select_list_column_count(
                    subquery,
                    database,
                    self.cte_context,
                )?;
            if column_count != expected {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected,
                    actual: column_count,
                });
            }
            return Ok(vec![vibesql_types::SqlValue::Null; expected]);
        }

        let first = &rows[0];
        if first.values.len() != expected {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected,
                actual: first.values.len(),
            });
        }
        Ok(first.values.to_vec())
    }

    /// Execute a scalar subquery and return its result rows (without the
    /// single-row / single-column reduction). Shared by `eval_scalar_subquery`
    /// (which reduces to one value) and `eval_scalar_subquery_as_row` (which
    /// keeps the full row for row-value comparisons).
    pub(super) fn execute_scalar_subquery_rows(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
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
        let table_name_for_outer =
            self.table_alias.as_ref().cloned().unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Trigger context: when this scalar subquery is evaluated from within a
        // trigger's WHEN clause (or body), its inner SELECT may reference the
        // firing row's NEW/OLD pseudo-columns, e.g.
        // `WHEN (SELECT count(*) FROM seen WHERE a = NEW.a) = 0` (#5585).
        //
        // We resolve NEW/OLD against the trigger context and substitute the
        // resulting literal values into a cloned subquery AST *before*
        // execution. This makes the subquery fully self-contained (no
        // pseudo-variables, not correlated to NEW/OLD), so every downstream
        // path — table scan with predicate pushdown, the iterator/materialized
        // WHERE filter, aggregation, nested subqueries — works unchanged
        // without threading the trigger context through the whole scan
        // machinery. The substituted result is row-specific, so it is executed
        // fresh (not cached). Non-trigger callers (trigger_context == None) fall
        // through to the unchanged cached/correlated paths below.
        if let Some(trigger_ctx) = self.trigger_context {
            let substituted = substitute_trigger_pseudo_vars(subquery, trigger_ctx)?;
            let select_executor =
                crate::select::SelectExecutor::new_with_depth(database, self.depth);
            let rows = select_executor.execute(&substituted)?;
            return Ok(rows);
        }

        let rows = {
            // Check if this is a non-correlated subquery that can be cached
            let is_correlated =
                crate::correlation::is_correlated(subquery, &outer_combined, self.database);

            // Execute or retrieve from cache
            if !is_correlated {
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
                        crate::select::SelectExecutor::new_with_cte_and_depth(
                            database, cte_ctx, self.depth,
                        )
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
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new(database)
                };
                select_executor.execute(subquery)?
            }
        };

        Ok(rows)
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

        // Trigger context (#5585): substitute NEW/OLD pseudo-variables with their
        // literal values so a correlated EXISTS subquery in a trigger WHEN clause
        // (e.g. `WHEN EXISTS (SELECT 1 FROM other WHERE other.id = NEW.id)`)
        // resolves against the firing row. See `substitute_trigger_pseudo_vars`.
        let substituted_subquery = match self.trigger_context {
            Some(trigger_ctx) => Some(substitute_trigger_pseudo_vars(subquery, trigger_ctx)?),
            None => None,
        };
        let subquery = substituted_subquery.as_ref().unwrap_or(subquery);

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer =
            self.table_alias.as_ref().cloned().unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated =
            crate::correlation::is_correlated(subquery, &outer_combined, self.database);

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
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
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

        // Trigger context (#5585): substitute NEW/OLD pseudo-variables with their
        // literal values so a correlated ALL/ANY/SOME subquery in a trigger WHEN
        // clause resolves against the firing row. See
        // `substitute_trigger_pseudo_vars`.
        let substituted_subquery = match self.trigger_context {
            Some(trigger_ctx) => Some(substitute_trigger_pseudo_vars(subquery, trigger_ctx)?),
            None => None,
        };
        let subquery = substituted_subquery.as_ref().unwrap_or(subquery);

        // Evaluate the left-hand expression
        let left_val = self.eval(expr, row)?;

        // Convert TableSchema to CombinedSchema for outer context
        // Use table_alias if set (for UPDATE t1 AS xyz), otherwise use schema name
        let table_name_for_outer =
            self.table_alias.as_ref().cloned().unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated =
            crate::correlation::is_correlated(subquery, &outer_combined, self.database);

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
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
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

    /// Evaluate row value IN subquery: (a, b) IN (SELECT x, y FROM t)
    ///
    /// SQL:1999 Section 8.4: Row value IN predicate
    /// A row value matches if it equals any row from the subquery.
    fn eval_row_value_in_subquery(
        &self,
        expr_elements: &[vibesql_ast::Expression],
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
        database: &vibesql_storage::Database,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        use vibesql_types::SqlValue;

        let expected_columns = expr_elements.len();

        // Evaluate all elements of the left row value
        let mut expr_values = Vec::with_capacity(expected_columns);
        for elem_expr in expr_elements {
            let val = self.eval(elem_expr, row)?;
            expr_values.push(val);
        }

        // Convert TableSchema to CombinedSchema for outer context
        let table_name_for_outer =
            self.table_alias.as_ref().cloned().unwrap_or_else(|| self.schema.name.clone());
        let outer_combined =
            crate::schema::CombinedSchema::from_table(table_name_for_outer, self.schema.clone());

        // Check if this is a correlated subquery
        let is_correlated =
            crate::correlation::is_correlated(subquery, &outer_combined, self.database);

        // Execute the subquery
        let rows = if !is_correlated {
            let cache_key = compute_subquery_hash(subquery);
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            if let Some(cached_rows) = cached_result {
                cached_rows
            } else {
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let executed_rows = select_executor.execute(subquery)?;
                self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());
                executed_rows
            }
        } else {
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

        // Validate column count
        let column_count = if !rows.is_empty() {
            rows[0].values.len()
        } else {
            crate::evaluator::combined::subqueries::schema_utils::compute_select_list_column_count(
                subquery,
                database,
                self.cte_context,
            )?
        };

        if column_count != expected_columns {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: expected_columns,
                actual: column_count,
            });
        }

        // Empty subquery result
        if rows.is_empty() {
            return Ok(SqlValue::Boolean(negated));
        }

        // Whether any row compared as UNKNOWN (all non-NULL elements equal,
        // but at least one element comparison involved NULL). Rows that are
        // definitively FALSE (some element definitively unequal) do NOT make
        // the overall result UNKNOWN — SQL three-valued AND semantics:
        // (0,0) IN (SELECT NULL, 1) is FALSE (0=1 is false), while
        // (0,0) IN (SELECT NULL, 0) is NULL (0=NULL is unknown). (#5268,
        // mirrors the combined-evaluator fix from #5265)
        let mut found_unknown_row = false;

        // Compare with each row from subquery
        for subquery_row in &rows {
            // Check if this row matches using row value equality
            let mut all_equal = true;
            let mut has_null_comparison = false;

            for (i, expr_val) in expr_values.iter().enumerate() {
                let subquery_val = subquery_row
                    .get(i)
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: i })?;

                // NULL on either side propagates as UNKNOWN for this element
                if matches!(expr_val, SqlValue::Null) || matches!(subquery_val, SqlValue::Null) {
                    has_null_comparison = true;
                    // Can't determine equality with NULL - continue to check other elements
                    continue;
                }

                // Compare values
                let eq_result = self.eval_binary_op(
                    expr_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    subquery_val,
                )?;

                match eq_result {
                    SqlValue::Boolean(true) => {
                        // Elements are equal, continue checking
                    }
                    SqlValue::Boolean(false) => {
                        // Elements differ, this row doesn't match
                        all_equal = false;
                        break;
                    }
                    SqlValue::Null => {
                        // Comparison returned NULL
                        has_null_comparison = true;
                    }
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "Comparison returned non-boolean: {:?}",
                            eq_result
                        )));
                    }
                }
            }

            if all_equal {
                if !has_null_comparison {
                    // Found an exact match
                    return Ok(SqlValue::Boolean(!negated));
                }
                // No element definitively unequal, but some comparison was
                // UNKNOWN — this row's match is UNKNOWN.
                found_unknown_row = true;
            }
        }

        // No exact match found
        if found_unknown_row {
            Ok(SqlValue::Null)
        } else {
            Ok(SqlValue::Boolean(negated))
        }
    }
}

/// Substitute trigger NEW/OLD pseudo-variables in a subquery with their literal
/// values resolved from the trigger context (#5585).
///
/// A trigger's WHEN clause (or body) may contain a subquery whose inner SELECT
/// references the firing row, e.g. `(SELECT count(*) FROM seen WHERE a = NEW.a)`.
/// The subquery is executed by a fresh `SelectExecutor` that does not carry the
/// trigger context, so a `NEW.a` reference inside it would otherwise fail with
/// "Pseudo-variable NEW.a is only valid within trigger bodies". By replacing each
/// `OLD.col` / `NEW.col` with the concrete value from the firing row, the
/// returned subquery is self-contained and resolves through all the normal
/// execution paths (scan pushdown, WHERE filter, aggregation, nesting).
///
/// Resolution errors (e.g. `NEW` not available for a DELETE trigger, or an
/// unknown column) are propagated.
fn substitute_trigger_pseudo_vars(
    subquery: &vibesql_ast::SelectStmt,
    trigger_ctx: &crate::trigger_execution::TriggerContext<'_>,
) -> Result<vibesql_ast::SelectStmt, ExecutorError> {
    struct PseudoVarSubstitutor<'a, 'b> {
        trigger_ctx: &'a crate::trigger_execution::TriggerContext<'b>,
        error: Option<ExecutorError>,
    }

    impl vibesql_ast::visitor::ExpressionMutVisitor for PseudoVarSubstitutor<'_, '_> {
        fn post_visit_expression(
            &mut self,
            expr: vibesql_ast::Expression,
        ) -> vibesql_ast::Expression {
            if self.error.is_some() {
                return expr;
            }
            if let vibesql_ast::Expression::PseudoVariable { pseudo_table, column } = &expr {
                match self.trigger_ctx.resolve_pseudo_var(*pseudo_table, column) {
                    Ok(value) => return vibesql_ast::Expression::Literal(value),
                    Err(e) => {
                        self.error = Some(e);
                        return expr;
                    }
                }
            }
            expr
        }
    }

    let mut substitutor = PseudoVarSubstitutor { trigger_ctx, error: None };
    let rewritten = vibesql_ast::visitor::transform_select(&mut substitutor, subquery.clone());
    if let Some(e) = substitutor.error {
        return Err(e);
    }
    Ok(rewritten)
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
            return (SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())), right_val);
        }
        if let SqlValue::Real(n) | SqlValue::Double(n) = &left_val {
            return (SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text(*n))), right_val);
        }
    }

    // If left has TEXT affinity and right is numeric, convert numeric to TEXT
    if matches!(left_affinity, Some(TypeAffinity::Text)) {
        if let SqlValue::Integer(n) = &right_val {
            return (left_val, SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())));
        }
        if let SqlValue::Real(n) | SqlValue::Double(n) = &right_val {
            return (left_val, SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text(*n))));
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
