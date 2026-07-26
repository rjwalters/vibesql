//! Expression evaluation with aggregates for SelectExecutor
//!
//! This module coordinates expression evaluation in the context of aggregation,
//! delegating to specialized handlers for different expression types.

mod aggregate_function;
mod binary_op;
mod case;
mod function;
mod simple;
mod subquery;

use super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    select::grouping::GroupingContext,
};

impl SelectExecutor<'_> {
    /// Evaluate an expression in the context of aggregation
    ///
    /// This is the main entry point for expression evaluation during aggregation.
    /// It delegates to specialized handlers based on expression type.
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates(
        &self,
        expr: &vibesql_ast::Expression,
        group_rows: &[vibesql_storage::Row],
        group_key: &[vibesql_types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate functions (AggregateFunction variant only)
            vibesql_ast::Expression::AggregateFunction { .. } => {
                aggregate_function::evaluate(self, expr, group_rows, evaluator)
            }

            // Regular functions (e.g., NULLIF wrapping an aggregate) - may contain aggregates in
            // arguments
            vibesql_ast::Expression::Function { .. } => {
                function::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Binary operation - recursively evaluate both sides
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                binary_op::evaluate_binary(self, left, op, right, group_rows, group_key, evaluator)
            }

            // Unary operations - recursively evaluate inner expression with aggregates
            vibesql_ast::Expression::UnaryOp { op, expr: inner_expr } => {
                binary_op::evaluate_unary(self, op, inner_expr, group_rows, group_key, evaluator)
            }

            // Scalar subquery - delegate to evaluator
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                subquery::evaluate_scalar(self, expr, group_rows, evaluator)
            }

            // IN with subquery - special handling for aggregate left-hand side
            vibesql_ast::Expression::In { .. } => {
                subquery::evaluate_in(self, expr, group_rows, group_key, evaluator)
            }

            // Quantified comparison - special handling for aggregate left-hand side
            vibesql_ast::Expression::QuantifiedComparison { .. } => {
                subquery::evaluate_quantified(self, expr, group_rows, group_key, evaluator)
            }

            // CASE expression - may contain aggregates in operand, conditions, or results
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                case::evaluate(self, operand, when_clauses, else_result, group_rows, group_key, evaluator)
            }

            // Simple expressions that can potentially contain aggregates
            vibesql_ast::Expression::Cast { .. }
            | vibesql_ast::Expression::Between { .. }
            | vibesql_ast::Expression::InList { .. }
            | vibesql_ast::Expression::Like { .. }
            | vibesql_ast::Expression::Glob { .. }
            | vibesql_ast::Expression::IsNull { .. }
            | vibesql_ast::Expression::IsDistinctFrom { .. }
            | vibesql_ast::Expression::IsTruthValue { .. }
            | vibesql_ast::Expression::Position { .. }
            | vibesql_ast::Expression::Trim { .. }
            | vibesql_ast::Expression::Extract { .. }
            | vibesql_ast::Expression::Interval { .. } => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Truly simple expressions: Literal, ColumnRef (cannot contain aggregates)
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::CollatedLiteral { .. }
            | vibesql_ast::Expression::ColumnRef(_)
            | vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. }
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::NextValue { .. }
            | vibesql_ast::Expression::DuplicateKeyValue { .. }
            | vibesql_ast::Expression::PseudoVariable { .. }
            | vibesql_ast::Expression::SessionVariable { .. } => {
                simple::evaluate_no_aggregates(self, expr, group_rows, evaluator)
            }

            // Window functions in aggregate context: evaluate the inner aggregate argument
            // The outer window function will be applied after aggregation completes
            // Example: SUM(SUM(x)) OVER (PARTITION BY y) - we evaluate SUM(x) here
            vibesql_ast::Expression::WindowFunction { function, .. } => {
                // Extract the aggregate argument from the window function
                let args = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Ranking { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
                };

                // If the window function has an argument, evaluate it (may contain aggregates)
                // This handles cases like SUM(SUM(x)) OVER() where the inner SUM needs evaluation
                if !args.is_empty() {
                    self.evaluate_with_aggregates(&args[0], group_rows, group_key, evaluator)
                } else {
                    // Window functions like ROW_NUMBER() have no arguments
                    // Return a placeholder - actual value will be computed in window phase
                    Ok(vibesql_types::SqlValue::Null)
                }
            }
            vibesql_ast::Expression::MatchAgainst { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "MATCH...AGAINST not supported in aggregate context".to_string(),
                ))
            }

            // Placeholders should have been bound before evaluation
            vibesql_ast::Expression::Placeholder(_)
            | vibesql_ast::Expression::NumberedPlaceholder(_)
            | vibesql_ast::Expression::NamedPlaceholder(_) => {
                Err(ExecutorError::UnsupportedExpression(
                    "Unbound placeholder in aggregate context - placeholders must be bound before execution".to_string(),
                ))
            }

            // Conjunction, Disjunction, and RowValueConstructor - evaluate children recursively
            vibesql_ast::Expression::Conjunction(_)
            | vibesql_ast::Expression::Disjunction(_)
            | vibesql_ast::Expression::RowValueConstructor(_) => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            vibesql_ast::Expression::Collate { .. } => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // RAISE() has no aggregate semantics; evaluating it surfaces the
            // trigger abort/error via the normal evaluator.
            vibesql_ast::Expression::Raise { .. } => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }
        }
    }

    /// Apply ORDER BY to aggregated results
    ///
    /// The `group_by_exprs` parameter contains the GROUP BY expressions, which are appended
    /// as hidden columns after the SELECT list columns. This allows ORDER BY to reference
    /// GROUP BY columns that are not in the SELECT list.
    ///
    /// The `order_by_aggregates` parameter contains aggregate expressions extracted from
    /// ORDER BY items, which are also appended as hidden columns. This allows ORDER BY
    /// expressions like "ORDER BY max(n)+0" to work.
    pub(in crate::select::executor) fn apply_order_by_to_aggregates(
        &self,
        rows: Vec<vibesql_storage::Row>,
        _stmt: &vibesql_ast::SelectStmt,
        order_by: &[vibesql_ast::OrderByItem],
        expanded_select_list: &[vibesql_ast::SelectItem],
        group_by_exprs: &[vibesql_ast::Expression],
        order_by_aggregates: &[vibesql_ast::Expression],
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Build a schema from the expanded SELECT list to enable ORDER BY column resolution
        let mut result_columns = Vec::new();
        for (idx, item) in expanded_select_list.iter().enumerate() {
            match item {
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    let column_name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        // Try to extract column name from expression
                        match expr {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
                                col_id.column_canonical().to_string()
                            }
                            vibesql_ast::Expression::AggregateFunction { name, .. } => {
                                name.to_lowercase()
                            }
                            _ => format!("col{}", idx + 1),
                        }
                    };
                    result_columns.push(vibesql_catalog::ColumnSchema::new(
                        column_name,
                        vibesql_types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                        true,
                    ));
                }
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    // This should not happen after expansion, but keep for safety
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards not supported with aggregates"
                            .to_string(),
                    ));
                }
            }
        }

        // Add GROUP BY expressions as hidden columns for ORDER BY resolution
        // These come after the SELECT list columns
        for expr in group_by_exprs {
            let column_name = match expr {
                vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
                _ => format!("__group_by_{}", result_columns.len()),
            };
            result_columns.push(vibesql_catalog::ColumnSchema::new(
                column_name,
                vibesql_types::DataType::Varchar { max_length: Some(255) },
                true,
            ));
        }

        // Add ORDER BY aggregate expressions as hidden columns
        // These come after GROUP BY columns
        let order_by_agg_start_idx = result_columns.len();
        for (i, _) in order_by_aggregates.iter().enumerate() {
            result_columns.push(vibesql_catalog::ColumnSchema::new(
                format!("__order_by_agg_{}", i),
                vibesql_types::DataType::Varchar { max_length: Some(255) },
                true,
            ));
        }

        let result_table_schema =
            vibesql_catalog::TableSchema::new("result".to_string(), result_columns);

        // Create a CombinedSchema for the result set
        let result_schema = crate::schema::CombinedSchema::from_table(
            "result".to_string(),
            result_table_schema.clone(),
        );

        let result_evaluator =
            CombinedExpressionEvaluator::with_database(&result_schema, self.database);

        // Evaluate ORDER BY expressions and attach sort keys to rows.
        //
        // SortKey tuple: (value, direction, nulls_order, collation).
        //
        // The collation is captured from the *original* ORDER BY expression
        // (before alias / aggregate-column resolution rewrites it into a
        // ColumnRef into the result schema), so that explicit `COLLATE` clauses
        // and propagated collations from compound expressions like
        // `max(b COLLATE nocase) || ''` are honored. See
        // `CombinedExpressionEvaluator::get_expression_collation` for the
        // propagation rules.
        type SortKey = (
            vibesql_types::SqlValue,
            vibesql_ast::OrderDirection,
            Option<vibesql_ast::NullsOrder>,
            Option<String>,
        );
        let mut rows_with_keys: Vec<(vibesql_storage::Row, Vec<SortKey>)> = Vec::new();
        for row in rows {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            result_evaluator.clear_cse_cache();

            let mut sort_keys = Vec::new();
            for (term_index, order_item) in order_by.iter().enumerate() {
                // Capture the effective collation from the ORIGINAL order-by
                // expression first, before resolution rewrites it into a
                // ColumnRef (which loses any inner COLLATE clauses).
                //
                // We use `result_evaluator` here for convenience — explicit
                // `COLLATE` clauses are matched by AST shape, not by column
                // lookup, so an evaluator over the result schema is sufficient
                // for the collation-propagation rules implemented in
                // `get_expression_collation`. (Column-declared collations on
                // raw source columns referenced inside the ORDER BY would not
                // be visible here, but those are unaffected by this fix.)
                let order_collation = result_evaluator.get_expression_collation(&order_item.expr);

                // Resolve ORDER BY expression to result schema column names
                // For aggregates, we need ColumnRef expressions to look up computed values
                // in the result schema, not re-evaluate aggregate expressions
                // Note: Pass None for schema since aggregate queries don't have wildcards (#4413)
                let resolved_expr = crate::select::order::resolve_order_by_for_aggregates(
                    &order_item.expr,
                    expanded_select_list,
                    term_index,
                    None,
                )?;

                // Replace aggregate expressions with references to their hidden columns
                let resolved_expr = replace_aggregates_with_columns(
                    &resolved_expr,
                    order_by_aggregates,
                    order_by_agg_start_idx,
                );

                let key_value = result_evaluator.eval(&resolved_expr, &row)?;
                sort_keys.push((
                    key_value,
                    order_item.direction.clone(),
                    order_item.nulls_order,
                    order_collation,
                ));
            }
            rows_with_keys.push((row, sort_keys));
        }

        // Sort using the shared ORDER BY comparator so NULL placement, collation, and
        // direction handling stay consistent with the non-aggregate and join paths.
        // The default (no explicit NULLS clause) rule is SQLite's: NULL is the smallest
        // value, so it sorts first for ASC and last for DESC (#6014).
        rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
            crate::select::order::compare_rows_by_sort_keys(keys_a, keys_b)
        });

        // Extract rows without sort keys
        Ok(rows_with_keys.into_iter().map(|(row, _)| row).collect())
    }
}

/// Replace aggregate expressions with column references to their hidden columns.
///
/// When ORDER BY contains aggregates like `max(n)+0`, we pre-compute the aggregate values
/// and store them in hidden columns. This function replaces the aggregate expressions
/// with ColumnRef expressions pointing to those hidden columns.
#[allow(clippy::only_used_in_recursion)] // start_idx is pre-existing unused parameter
fn replace_aggregates_with_columns(
    expr: &vibesql_ast::Expression,
    order_by_aggregates: &[vibesql_ast::Expression],
    start_idx: usize,
) -> vibesql_ast::Expression {
    use vibesql_ast::Expression;

    // Check if this expression is one of the pre-computed aggregates
    if let Expression::AggregateFunction { .. } = expr {
        for (i, agg) in order_by_aggregates.iter().enumerate() {
            if format!("{:?}", expr) == format!("{:?}", agg) {
                // Replace with column reference to the hidden column
                return Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    &format!("__order_by_agg_{}", i),
                    false,
                ));
            }
        }
    }

    // Recursively process compound expressions
    match expr {
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(replace_aggregates_with_columns(left, order_by_aggregates, start_idx)),
            op: *op,
            right: Box::new(replace_aggregates_with_columns(right, order_by_aggregates, start_idx)),
        },
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(replace_aggregates_with_columns(inner, order_by_aggregates, start_idx)),
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| replace_aggregates_with_columns(a, order_by_aggregates, start_idx))
                .collect(),
            character_unit: character_unit.clone(),
        },
        Expression::Cast { expr: inner, data_type, .. } => Expression::Cast {
            expr: Box::new(replace_aggregates_with_columns(inner, order_by_aggregates, start_idx)),
            data_type: data_type.clone(),
        },
        // For other expressions, return as-is
        _ => expr.clone(),
    }
}

impl SelectExecutor<'_> {
    /// Evaluate an expression in the context of aggregation with grouping context
    ///
    /// This is used for ROLLUP/CUBE/GROUPING SETS to support the GROUPING() function
    /// and to return NULL for columns that are rolled up in the current grouping set.
    ///
    /// This function handles all expression types recursively to ensure GROUPING() calls
    /// nested within binary operations, CASE expressions, etc. are properly evaluated.
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates_and_grouping(
        &self,
        expr: &vibesql_ast::Expression,
        group_rows: &[vibesql_storage::Row],
        group_key: &[vibesql_types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
        grouping_context: &GroupingContext,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Check for GROUPING() or GROUPING_ID() function call
            vibesql_ast::Expression::Function { name, args, .. } => {
                if name.eq_ignore_ascii_case("GROUPING") {
                    // GROUPING() function - returns 1 if the column is rolled up, 0 otherwise
                    if args.len() != 1 {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "GROUPING() requires exactly 1 argument, got {}",
                            args.len()
                        )));
                    }
                    let result = grouping_context.is_rolled_up(&args[0]);
                    return Ok(vibesql_types::SqlValue::Integer(result as i64));
                }

                if name.eq_ignore_ascii_case("GROUPING_ID") {
                    // GROUPING_ID() function - returns a bitmap for multiple columns
                    // Formula: GROUPING(c1) * 2^(n-1) + GROUPING(c2) * 2^(n-2) + ... + GROUPING(cn)
                    if args.is_empty() {
                        return Err(ExecutorError::UnsupportedExpression(
                            "GROUPING_ID() requires at least 1 argument".to_string(),
                        ));
                    }
                    let result = grouping_context.grouping_id(args);
                    return Ok(vibesql_types::SqlValue::Integer(result));
                }

                // For other functions (including aggregates like COUNT, SUM, etc.),
                // delegate to the function evaluator which handles Wildcard and other
                // special arguments appropriately
                function::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Binary operation - recursively evaluate both sides with grouping context
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_with_aggregates_and_grouping(
                    left,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                let right_val = self.evaluate_with_aggregates_and_grouping(
                    right,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                // Apply SQLite type affinity rules before comparison.
                // This ensures TEXT columns compared to INTEGER columns use
                // affinity-based coercion rather than strict type ordering.
                let (left_val, right_val) =
                    evaluator.apply_affinity_for_comparison(left, left_val, right, right_val);
                crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                    &left_val,
                    op,
                    &right_val,
                    self.database.sql_mode(),
                )
            }

            // Unary operations - recursively evaluate inner expression with grouping context
            vibesql_ast::Expression::UnaryOp { op, expr: inner_expr } => {
                let inner_val = self.evaluate_with_aggregates_and_grouping(
                    inner_expr,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                crate::evaluator::eval_unary_op(op, &inner_val)
            }

            // CASE expression - evaluate with grouping context
            // Need to handle both simple CASE (CASE x WHEN v THEN ...) and searched CASE (CASE WHEN
            // cond THEN ...)
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                match operand {
                    // Simple CASE: CASE operand WHEN value THEN result ...
                    Some(operand_expr) => {
                        let operand_value = self.evaluate_with_aggregates_and_grouping(
                            operand_expr,
                            group_rows,
                            group_key,
                            evaluator,
                            grouping_context,
                        )?;

                        for when_clause in when_clauses {
                            // Check if ANY condition matches (OR logic)
                            for condition_expr in &when_clause.conditions {
                                let when_value = self.evaluate_with_aggregates_and_grouping(
                                    condition_expr,
                                    group_rows,
                                    group_key,
                                    evaluator,
                                    grouping_context,
                                )?;

                                // Affinity-aware equality (not the permissive
                                // `values_are_equal` used for hash-join keys):
                                // a bare literal operand carries no affinity,
                                // so `CASE COUNT(*) WHEN '2' THEN ...` must not
                                // match INTEGER 2 against TEXT '2'
                                // (e_expr-23.1.6, same fix as the main
                                // evaluators). Route through the shared
                                // affinity-aware comparator on the combined
                                // evaluator; the operand/condition expressions
                                // are threaded through here.
                                if evaluator.affinity_aware_equal(
                                    operand_expr,
                                    operand_value.clone(),
                                    condition_expr,
                                    when_value,
                                )? {
                                    return self.evaluate_with_aggregates_and_grouping(
                                        &when_clause.result,
                                        group_rows,
                                        group_key,
                                        evaluator,
                                        grouping_context,
                                    );
                                }
                            }
                        }

                        if let Some(else_expr) = else_result {
                            self.evaluate_with_aggregates_and_grouping(
                                else_expr,
                                group_rows,
                                group_key,
                                evaluator,
                                grouping_context,
                            )
                        } else {
                            Ok(vibesql_types::SqlValue::Null)
                        }
                    }

                    // Searched CASE: CASE WHEN condition THEN result ...
                    None => {
                        for when_clause in when_clauses {
                            for condition_expr in &when_clause.conditions {
                                let condition_value = self.evaluate_with_aggregates_and_grouping(
                                    condition_expr,
                                    group_rows,
                                    group_key,
                                    evaluator,
                                    grouping_context,
                                )?;

                                // Delegate to the shared SQLite truthiness
                                // helper (numerics non-zero, strings/blobs
                                // via the leading-numeric parse, NULL
                                // falsy). (#5856)
                                let is_true =
                                    crate::evaluator::operators::is_truthy(&condition_value);

                                if is_true {
                                    return self.evaluate_with_aggregates_and_grouping(
                                        &when_clause.result,
                                        group_rows,
                                        group_key,
                                        evaluator,
                                        grouping_context,
                                    );
                                }
                            }
                        }

                        if let Some(else_expr) = else_result {
                            self.evaluate_with_aggregates_and_grouping(
                                else_expr,
                                group_rows,
                                group_key,
                                evaluator,
                                grouping_context,
                            )
                        } else {
                            Ok(vibesql_types::SqlValue::Null)
                        }
                    }
                }
            }

            // Column references - check if the column is rolled up
            vibesql_ast::Expression::ColumnRef(_) => {
                if grouping_context.is_rolled_up(expr) == 1 {
                    return Ok(vibesql_types::SqlValue::Null);
                }
                // Not rolled up, evaluate normally
                simple::evaluate_no_aggregates(self, expr, group_rows, evaluator)
            }

            // Aggregate functions - delegate to aggregate handler
            vibesql_ast::Expression::AggregateFunction { .. } => {
                aggregate_function::evaluate(self, expr, group_rows, evaluator)
            }

            // Cast - evaluate inner expression with grouping context
            vibesql_ast::Expression::Cast { expr: inner, data_type } => {
                let inner_val = self.evaluate_with_aggregates_and_grouping(
                    inner,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                crate::evaluator::casting::cast_value(
                    &inner_val,
                    data_type,
                    &self.database.sql_mode(),
                )
            }

            // IsNull - evaluate inner expression with grouping context
            vibesql_ast::Expression::IsNull { expr: inner, negated } => {
                let inner_val = self.evaluate_with_aggregates_and_grouping(
                    inner,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                let is_null = matches!(inner_val, vibesql_types::SqlValue::Null);
                Ok(vibesql_types::SqlValue::Boolean(if *negated { !is_null } else { is_null }))
            }

            // IsDistinctFrom - evaluate both expressions with grouping context
            vibesql_ast::Expression::IsDistinctFrom { left, right, negated } => {
                let left_val = self.evaluate_with_aggregates_and_grouping(
                    left,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                let right_val = self.evaluate_with_aggregates_and_grouping(
                    right,
                    group_rows,
                    group_key,
                    evaluator,
                    grouping_context,
                )?;
                // Affinity-aware, NULL-safe distinctness (not the permissive
                // `values_are_equal` used for hash-join keys), matching the
                // main evaluators' `IsDistinctFrom` fix: `COUNT(*) IS NOT '2'`
                // must treat INTEGER 2 as distinct from TEXT '2'
                // (e_expr-23.1.6). The operand expressions are threaded
                // through here, so route through the shared comparator.
                let is_distinct =
                    evaluator.affinity_aware_is_distinct(left, left_val, right, right_val)?;
                Ok(vibesql_types::SqlValue::Boolean(if *negated {
                    !is_distinct
                } else {
                    is_distinct
                }))
            }

            // For all other expressions, delegate to existing evaluation
            // These don't typically contain GROUPING() calls
            _ => self.evaluate_with_aggregates(expr, group_rows, group_key, evaluator),
        }
    }

    /// Check if a SQL value is truthy (for HAVING clause evaluation)
    ///
    /// Delegates to the shared SQLite truthiness helper
    /// (`crate::evaluator::operators::is_truthy`) so HAVING accepts any
    /// expression like SQLite does — strings and blobs coerce via the
    /// leading-numeric parse (`HAVING 'first'` → falsy, `HAVING '1'` →
    /// truthy, `HAVING X'31'` → truthy, `HAVING zeroblob(3)` → falsy)
    /// instead of erroring. This also unifies the row-based HAVING path
    /// with the columnar one (columnar_execution/having.rs). (#5803)
    pub(in crate::select::executor) fn is_truthy(
        &self,
        value: &vibesql_types::SqlValue,
    ) -> Result<bool, ExecutorError> {
        Ok(crate::evaluator::operators::is_truthy(value))
    }
}
