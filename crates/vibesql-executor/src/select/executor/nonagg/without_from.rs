//! SELECT without FROM clause execution
//!
//! This module handles the special case of SELECT statements without a FROM clause,
//! which evaluates expressions without any table context.

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
    select::cte::CteResult,
    select::helpers::{apply_limit_offset, evaluate_limit, evaluate_offset},
    select::window::{evaluate_window_functions, has_window_functions},
};

impl SelectExecutor<'_> {
    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    ///
    /// Example: `SELECT 1 + 1, 'hello', CURRENT_DATE`
    ///
    /// Fix for select1-18.1: When outer context is available (outer_row, outer_schema),
    /// column references can be resolved from the outer scope. This enables queries like:
    /// `SELECT * FROM t2 WHERE x IN (SELECT x)` where the inner `SELECT x` has no FROM
    /// but `x` is resolved from the outer `t2`.
    ///
    /// Fix for #5350: `cte_results` carries the enclosing statement's CTE bindings
    /// (already merged with any outer CTE context by `execute()`), so subqueries in
    /// expression position (scalar, EXISTS, IN) resolve CTE names before falling
    /// back to same-named catalog tables/views, matching SQLite precedence.
    pub(in crate::select::executor) fn execute_select_without_from(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Validate ORDER BY column positions early (SQLite compatibility)
        if let Some(order_by) = &stmt.order_by {
            for (term_index, order_item) in order_by.iter().enumerate() {
                // Check for numeric column positions
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) =
                    &order_item.expr
                {
                    if *pos <= 0 || (*pos as usize) > stmt.select_list.len() {
                        return Err(ExecutorError::OrderByOutOfRange {
                            term_position: term_index + 1,
                            column_number: *pos,
                            select_list_len: stmt.select_list.len(),
                        });
                    }
                }
                // Check for negative column positions (parsed as UnaryOp { Minus, Integer })
                if let vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr,
                } = &order_item.expr
                {
                    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) =
                        expr.as_ref()
                    {
                        return Err(ExecutorError::OrderByOutOfRange {
                            term_position: term_index + 1,
                            column_number: -*pos,
                            select_list_len: stmt.select_list.len(),
                        });
                    }
                }
            }
        }

        // CTE context for expression-position subqueries (#5350).
        // `cte_results` already merges local CTEs over the executor's outer CTE
        // context (see execute()); fall back to the executor's context when the
        // merged map is empty (same pattern as the materialized/iterator paths).
        let cte_ctx = if !cte_results.is_empty() { Some(cte_results) } else { self.cte_context };

        // Check if we have outer context (for correlated subqueries)
        let has_outer_context = self.outer_row.is_some() && self.outer_schema.is_some();

        // SQLite allows the WHERE clause of a FROM-less SELECT to reference
        // select-list aliases, e.g. `SELECT (SELECT '') x WHERE x+x` (window1.test
        // 15.2). Without outer context, a column reference in WHERE can only
        // resolve to a select-list alias, so evaluate the select list first and
        // bind the aliases for the WHERE evaluation.
        if let Some(where_clause) = &stmt.where_clause {
            if !has_outer_context
                && !has_window_functions(&stmt.select_list)
                && self.expression_references_column(where_clause)
            {
                return self.execute_select_without_from_alias_where(stmt, where_clause, cte_ctx);
            }
        }

        // Evaluate WHERE clause first - if it's false, return empty result (SQLite compatibility)
        // This handles cases like `SELECT 99 WHERE 0` which should return no rows
        if let Some(where_clause) = &stmt.where_clause {
            let where_result = if has_outer_context {
                let outer_row = self.outer_row.unwrap();
                let outer_schema = self.outer_schema.unwrap();
                let empty_combined_schema = crate::schema::CombinedSchema::empty();
                let mut evaluator = CombinedExpressionEvaluator::with_database_and_outer_context(
                    &empty_combined_schema,
                    self.database,
                    outer_row,
                    outer_schema,
                );
                if let Some(cte) = cte_ctx {
                    evaluator = evaluator.with_cte_context(cte);
                }
                evaluator.eval(where_clause, outer_row)?
            } else {
                let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
                let mut evaluator =
                    ExpressionEvaluator::with_database(&empty_schema, self.database);
                if let Some(cte) = cte_ctx {
                    evaluator = evaluator.with_cte_context(cte);
                }
                let empty_row = vibesql_storage::Row::new(vec![]);
                evaluator.eval(where_clause, &empty_row)?
            };

            // Check if WHERE condition is truthy (SQLite boolean semantics)
            let is_truthy = match &where_result {
                vibesql_types::SqlValue::Boolean(b) => *b,
                vibesql_types::SqlValue::Null => false,
                vibesql_types::SqlValue::Integer(n) => *n != 0,
                vibesql_types::SqlValue::Smallint(n) => *n != 0,
                vibesql_types::SqlValue::Bigint(n) => *n != 0,
                vibesql_types::SqlValue::Float(f) => *f != 0.0,
                vibesql_types::SqlValue::Real(f) => *f != 0.0,
                vibesql_types::SqlValue::Double(f) => *f != 0.0,
                vibesql_types::SqlValue::Numeric(n) => *n != 0.0,
                _ => true, // Non-numeric values are truthy
            };

            if !is_truthy {
                // WHERE clause is false - return empty result
                return Ok(vec![]);
            }
        }

        // Check if SELECT list contains window functions
        // Window functions without FROM clause operate on a single implicit row (Issue #4747)
        // Example: SELECT min(1) OVER() returns 1 (the min of a single implicit row)
        if has_window_functions(&stmt.select_list) {
            return self.execute_select_without_from_with_windows(stmt, has_outer_context, cte_ctx);
        }

        // Evaluate each item in the SELECT list
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    // Check if expression references a column
                    // FIX for select1-18.1: Only error if we don't have outer context
                    // When outer context exists, columns can be resolved from outer scope
                    if !has_outer_context && self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }

                    // Evaluate the expression
                    let value = if has_outer_context {
                        // Use outer context for column resolution
                        let outer_row = self.outer_row.unwrap();
                        let outer_schema = self.outer_schema.unwrap();
                        // Create an empty CombinedSchema for current level (no FROM clause)
                        let empty_combined_schema = crate::schema::CombinedSchema::empty();
                        // Create evaluator that can resolve columns from outer schema
                        let mut evaluator =
                            CombinedExpressionEvaluator::with_database_and_outer_context(
                                &empty_combined_schema,
                                self.database,
                                outer_row,
                                outer_schema,
                            );
                        if let Some(cte) = cte_ctx {
                            evaluator = evaluator.with_cte_context(cte);
                        }
                        evaluator.eval(expr, outer_row)?
                    } else {
                        // No outer context - use simple evaluation
                        let empty_schema =
                            vibesql_catalog::TableSchema::new("".to_string(), vec![]);
                        let mut evaluator =
                            ExpressionEvaluator::with_database(&empty_schema, self.database);
                        if let Some(cte) = cte_ctx {
                            evaluator = evaluator.with_cte_context(cte);
                        }
                        let empty_row = vibesql_storage::Row::new(vec![]);
                        evaluator.eval(expr, &empty_row)?
                    };
                    values.push(value);
                }
            }
        }

        // Build result row
        let result = vec![vibesql_storage::Row::new(values)];

        // Apply LIMIT and OFFSET (important for queries like SELECT 1 LIMIT 0)
        let limit = evaluate_limit(&stmt.limit, self.database)?;
        let offset = evaluate_offset(&stmt.offset, self.database)?;
        Ok(apply_limit_offset(result, limit, offset))
    }

    /// Execute a FROM-less SELECT whose WHERE clause references select-list
    /// aliases (SQLite compatibility, e.g. `SELECT (SELECT '') x WHERE x+x`).
    ///
    /// Evaluates the select list first, binds each item's alias as a column in
    /// a synthetic single-row schema, then evaluates the WHERE clause against
    /// that row. Column references that don't match an alias produce the same
    /// "column not found" error as before.
    fn execute_select_without_from_alias_where(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        where_clause: &vibesql_ast::Expression,
        cte_ctx: Option<&HashMap<String, CteResult>>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
        let mut evaluator = ExpressionEvaluator::with_database(&empty_schema, self.database);
        if let Some(cte) = cte_ctx {
            evaluator = evaluator.with_cte_context(cte);
        }
        let empty_row = vibesql_storage::Row::new(vec![]);

        // Evaluate the select list and collect alias bindings
        let mut values = Vec::new();
        let mut alias_columns = Vec::new();
        for item in &stmt.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    if self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }
                    let value = evaluator.eval(expr, &empty_row)?;
                    let column_name = alias.clone().unwrap_or_default();
                    let data_type = crate::select::cte::infer_type_from_value(&value);
                    alias_columns.push(vibesql_catalog::ColumnSchema::new(
                        column_name,
                        data_type,
                        true,
                    ));
                    values.push(value);
                }
            }
        }

        // Evaluate WHERE with the aliases bound as columns of a single row
        let alias_schema = vibesql_catalog::TableSchema::new("".to_string(), alias_columns);
        let mut where_evaluator = ExpressionEvaluator::with_database(&alias_schema, self.database);
        if let Some(cte) = cte_ctx {
            where_evaluator = where_evaluator.with_cte_context(cte);
        }
        let result_row = vibesql_storage::Row::new(values);
        let where_result = where_evaluator.eval(where_clause, &result_row)?;

        let is_truthy = match &where_result {
            vibesql_types::SqlValue::Boolean(b) => *b,
            vibesql_types::SqlValue::Null => false,
            vibesql_types::SqlValue::Integer(n) => *n != 0,
            vibesql_types::SqlValue::Smallint(n) => *n != 0,
            vibesql_types::SqlValue::Bigint(n) => *n != 0,
            vibesql_types::SqlValue::Float(f) => *f != 0.0,
            vibesql_types::SqlValue::Real(f) => *f != 0.0,
            vibesql_types::SqlValue::Double(f) => *f != 0.0,
            vibesql_types::SqlValue::Numeric(n) => *n != 0.0,
            // Non-numeric, non-NULL values follow SQLite numeric coercion:
            // strings coerce to their numeric prefix (0 when non-numeric)
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                crate::evaluator::casting::string_to_number(s).1 != 0.0
            }
            _ => true,
        };

        if !is_truthy {
            return Ok(vec![]);
        }

        let result = vec![result_row];
        let limit = evaluate_limit(&stmt.limit, self.database)?;
        let offset = evaluate_offset(&stmt.offset, self.database)?;
        Ok(apply_limit_offset(result, limit, offset))
    }

    /// Execute SELECT without FROM clause that contains window functions
    ///
    /// Window functions without FROM clause operate on a single implicit row.
    /// For example: `SELECT min(1) OVER()` returns 1 because the window frame
    /// contains exactly one row (the implicit single row for queries without FROM).
    ///
    /// This is SQLite-compatible behavior where window functions can appear in
    /// scalar subqueries without FROM.
    fn execute_select_without_from_with_windows(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        has_outer_context: bool,
        cte_ctx: Option<&HashMap<String, CteResult>>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Create an implicit single empty row for window function processing
        // This represents the single implicit row that exists for SELECT without FROM
        let implicit_row = vibesql_storage::Row::new(vec![]);
        let mut rows = vec![implicit_row];

        // Create evaluator for window function processing
        let empty_combined_schema = crate::schema::CombinedSchema::empty();
        let mut evaluator = if has_outer_context {
            let outer_row = self.outer_row.unwrap();
            let outer_schema = self.outer_schema.unwrap();
            CombinedExpressionEvaluator::with_database_and_outer_context(
                &empty_combined_schema,
                self.database,
                outer_row,
                outer_schema,
            )
        } else {
            CombinedExpressionEvaluator::with_database(&empty_combined_schema, self.database)
        };
        if let Some(cte) = cte_ctx {
            evaluator = evaluator.with_cte_context(cte);
        }

        // Process window functions - this adds computed values to each row
        // and returns a mapping from WindowFunctionKey to column index
        let (rows_with_windows, window_mapping) = evaluate_window_functions(
            rows,
            &stmt.select_list,
            stmt.window_definitions.as_ref(),
            &evaluator,
        )?;
        rows = rows_with_windows;

        // Now evaluate the SELECT list with the window mapping
        // The evaluator needs access to the window mapping to resolve WindowFunction expressions
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    // Create evaluator with window mapping for this expression
                    let mut eval_with_windows = if has_outer_context {
                        let outer_row = self.outer_row.unwrap();
                        let outer_schema = self.outer_schema.unwrap();
                        CombinedExpressionEvaluator::with_database_outer_and_windows(
                            &empty_combined_schema,
                            self.database,
                            outer_row,
                            outer_schema,
                            &window_mapping,
                        )
                    } else {
                        CombinedExpressionEvaluator::with_database_and_windows(
                            &empty_combined_schema,
                            self.database,
                            &window_mapping,
                        )
                    };
                    if let Some(cte) = cte_ctx {
                        eval_with_windows = eval_with_windows.with_cte_context(cte);
                    }
                    // Use the row with window values appended
                    let row = &rows[0];
                    let value = eval_with_windows.eval(expr, row)?;
                    values.push(value);
                }
            }
        }

        // Build result row
        let result = vec![vibesql_storage::Row::new(values)];

        // Apply LIMIT and OFFSET
        let limit = evaluate_limit(&stmt.limit, self.database)?;
        let offset = evaluate_offset(&stmt.offset, self.database)?;
        Ok(apply_limit_offset(result, limit, offset))
    }
}
