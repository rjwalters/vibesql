//! Utility methods for SelectExecutor

use super::builder::SelectExecutor;

/// Check if an expression references a column (which requires FROM clause).
///
/// `count_pseudo` controls whether NEW/OLD pseudo-variables (`NEW.x`, `OLD.x`)
/// count as column references. They do for a plain from-less SELECT (which has
/// no row to resolve them from), but inside a trigger body (#5445) the firing
/// row's NEW/OLD context resolves them, so callers there pass `false`.
fn expression_references_column_inner(expr: &vibesql_ast::Expression, count_pseudo: bool) -> bool {
    let expression_references_column =
        |e: &vibesql_ast::Expression| expression_references_column_inner(e, count_pseudo);
    match expr {
        vibesql_ast::Expression::ColumnRef(_) => true,
        vibesql_ast::Expression::PseudoVariable { .. } => count_pseudo, /* Pseudo-variables */
        // reference columns (OLD.x, NEW.x)
        vibesql_ast::Expression::Default => false, // DEFAULT doesn't reference columns
        vibesql_ast::Expression::DuplicateKeyValue { .. } => false, /* DuplicateKeyValue doesn't */
        // reference columns
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expression_references_column(left) || expression_references_column(right)
        }

        vibesql_ast::Expression::UnaryOp { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::AggregateFunction { args, .. } => {
            args.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::IsNull { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::IsDistinctFrom { left, right, .. } => {
            expression_references_column(left) || expression_references_column(right)
        }

        vibesql_ast::Expression::IsTruthValue { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::InList { expr, values, .. } => {
            expression_references_column(expr) || values.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expression_references_column(expr)
                || expression_references_column(low)
                || expression_references_column(high)
        }

        vibesql_ast::Expression::Cast { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Interval { value, .. } => expression_references_column(value),

        vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
            expression_references_column(substring) || expression_references_column(string)
        }

        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_references_column(e))
                || expression_references_column(string)
        }

        vibesql_ast::Expression::Extract { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Like { expr, pattern, .. }
        | vibesql_ast::Expression::Glob { expr, pattern, .. } => {
            expression_references_column(expr) || expression_references_column(pattern)
        }

        vibesql_ast::Expression::In { expr, .. } => {
            // Note: subquery could reference outer columns but that's a different case
            expression_references_column(expr)
        }

        vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
            expression_references_column(expr)
        }

        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_references_column(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(expression_references_column)
                        || expression_references_column(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_references_column(e))
        }

        vibesql_ast::Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let args_reference_column = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    args.iter().any(expression_references_column)
                }
            };

            // Check PARTITION BY and ORDER BY clauses
            let partition_references = over
                .partition_by
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(expression_references_column));

            let order_references = over.order_by.as_ref().is_some_and(|items| {
                items.iter().any(|item| expression_references_column(&item.expr))
            });

            args_reference_column || partition_references || order_references
        }

        // These don't contain column references:
        vibesql_ast::Expression::Literal(_) => false,
        vibesql_ast::Expression::Wildcard => false,
        vibesql_ast::Expression::ScalarSubquery(_) => false, // Subquery has its own scope
        vibesql_ast::Expression::Exists { .. } => false,     // Subquery has its own scope
        vibesql_ast::Expression::CurrentDate => false,
        vibesql_ast::Expression::CurrentTime { .. } => false,
        vibesql_ast::Expression::CurrentTimestamp { .. } => false,
        vibesql_ast::Expression::NextValue { .. } => false, // Sequence reference, not column
        vibesql_ast::Expression::SessionVariable { .. } => false, // Session variable, not column
        vibesql_ast::Expression::MatchAgainst { columns, search_modifier, .. } => {
            // MATCH AGAINST references columns and the search term
            !columns.is_empty() || expression_references_column(search_modifier)
        }

        // Placeholders don't reference columns (they're parameter markers)
        vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_) => false,

        // Conjunction and Disjunction - check all children
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => {
            children.iter().any(expression_references_column)
        }

        // Row value constructor - check all values
        vibesql_ast::Expression::RowValueConstructor(values) => {
            values.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::Collate { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Raise { error_message, .. } => {
            error_message.as_ref().is_some_and(|msg| expression_references_column(msg))
        }
    }
}

impl SelectExecutor<'_> {
    /// Check if an expression references a column (which requires FROM clause).
    ///
    /// NEW/OLD pseudo-variables count as column references here.
    pub(super) fn expression_references_column(&self, expr: &vibesql_ast::Expression) -> bool {
        expression_references_column_inner(expr, true)
    }

    /// Check if an expression references a *non-pseudo* column (i.e. a real
    /// `ColumnRef` that requires a FROM clause), ignoring NEW/OLD pseudo-variables.
    ///
    /// Used by the from-less SELECT path inside a trigger body (#5445): there the
    /// firing row's NEW/OLD context resolves pseudo-variables, so only a real
    /// column reference (which has nothing to bind to) still requires a FROM clause.
    pub(super) fn expression_references_non_pseudo_column(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> bool {
        expression_references_column_inner(expr, false)
    }

    /// Evaluate a LIMIT or OFFSET expression and convert to usize
    ///
    /// LIMIT and OFFSET accept arbitrary expressions (e.g., `5+3`, `(SELECT 10)`)
    /// that must evaluate to a non-negative integer at runtime.
    ///
    /// SQLite compatibility:
    /// - Any negative LIMIT means unlimited (returns usize::MAX)
    /// - Any negative OFFSET is treated as 0
    ///
    /// # Errors
    ///
    /// - Expression evaluation fails
    /// - Result is not an integer
    pub(super) fn eval_limit_offset_expr(
        &self,
        expr: &vibesql_ast::Expression,
        clause_name: &str,
    ) -> Result<usize, crate::errors::ExecutorError> {
        use crate::evaluator::ExpressionEvaluator;

        // Pre-validate column references in LIMIT/OFFSET expressions (#5092).
        // LIMIT/OFFSET expressions are evaluated against an empty row/schema,
        // so any column reference is unresolvable. SQLite reports this as
        // "no such column: X". This pre-check must run BEFORE the evaluator,
        // otherwise the evaluator's WindowFunction / AggregateFunction arms
        // fire first and produce a misleading "misuse of window function ..."
        // error for queries like:
        //   SELECT count(*) FROM t1 LIMIT nth_value(x, 1) OVER ();
        let mut col_refs = Vec::new();
        crate::select::executor::validation::extract_column_refs(expr, &mut col_refs);
        if let Some(first) = col_refs.into_iter().next() {
            let column_ref = match first.table {
                Some(t) => format!("{}.{}", t, first.column),
                None => first.column,
            };
            return Err(crate::errors::ExecutorError::NoSuchColumn { column_ref });
        }

        // Create empty schema and row for expression evaluation
        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::with_database(&empty_schema, self.database);
        let empty_row = vibesql_storage::Row::new(vec![]);

        // Evaluate the expression
        let value = evaluator.eval(expr, &empty_row)?;

        // Convert to integer
        // SQLite compatibility: negative values have special meanings
        match value {
            vibesql_types::SqlValue::Integer(n) => {
                if n < 0 {
                    if clause_name == "OFFSET" {
                        // Negative offset is treated as 0
                        Ok(0)
                    } else {
                        // Negative limit means unlimited - return MAX
                        Ok(usize::MAX)
                    }
                } else {
                    Ok(n as usize)
                }
            }
            vibesql_types::SqlValue::Null => {
                Err(crate::errors::ExecutorError::InvalidLimitOffset {
                    clause: clause_name.to_string(),
                    value: "NULL".to_string(),
                    reason: "must be an integer".to_string(),
                })
            }
            other => Err(crate::errors::ExecutorError::InvalidLimitOffset {
                clause: clause_name.to_string(),
                value: format!("{:?}", other),
                reason: "must be an integer".to_string(),
            }),
        }
    }
}
