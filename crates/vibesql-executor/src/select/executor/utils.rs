//! Utility methods for SelectExecutor

use super::builder::SelectExecutor;

/// Find the first column reference in an expression that would require a FROM
/// clause to resolve, returning its display text (source case preserved) for
/// use in SQLite-compatible `no such column: X` errors (#5804).
///
/// `count_pseudo` controls whether NEW/OLD pseudo-variables (`NEW.x`, `OLD.x`)
/// count as column references. They do for a plain from-less SELECT (which has
/// no row to resolve them from), but inside a trigger body (#5445) the firing
/// row's NEW/OLD context resolves them, so callers there pass `false`.
///
/// Returns `None` when the expression contains no such reference. Subqueries
/// (scalar subqueries, EXISTS) have their own scope and are not descended into.
fn find_from_less_column_ref(expr: &vibesql_ast::Expression, count_pseudo: bool) -> Option<String> {
    let find = |e: &vibesql_ast::Expression| find_from_less_column_ref(e, count_pseudo);
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => Some(col_id.display().to_string()),
        vibesql_ast::Expression::PseudoVariable { pseudo_table, column } => {
            // Pseudo-variables reference columns (OLD.x, NEW.x)
            if count_pseudo {
                use vibesql_ast::pretty_print::ToSql;
                Some(format!("{}.{}", pseudo_table.to_sql(), column))
            } else {
                None
            }
        }
        vibesql_ast::Expression::Default => None, // DEFAULT doesn't reference columns
        vibesql_ast::Expression::DuplicateKeyValue { .. } => None, /* DuplicateKeyValue doesn't */
        // reference columns
        vibesql_ast::Expression::BinaryOp { left, right, .. } => find(left).or_else(|| find(right)),

        vibesql_ast::Expression::UnaryOp { expr, .. } => find(expr),

        vibesql_ast::Expression::Function { args, .. } => args.iter().find_map(find),

        vibesql_ast::Expression::AggregateFunction { args, .. } => args.iter().find_map(find),

        vibesql_ast::Expression::IsNull { expr, .. } => find(expr),

        vibesql_ast::Expression::IsDistinctFrom { left, right, .. } => {
            find(left).or_else(|| find(right))
        }

        vibesql_ast::Expression::IsTruthValue { expr, .. } => find(expr),

        vibesql_ast::Expression::InList { expr, values, .. } => {
            find(expr).or_else(|| values.iter().find_map(find))
        }

        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            find(expr).or_else(|| find(low)).or_else(|| find(high))
        }

        vibesql_ast::Expression::Cast { expr, .. } => find(expr),

        vibesql_ast::Expression::Interval { value, .. } => find(value),

        vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
            find(substring).or_else(|| find(string))
        }

        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().and_then(|e| find(e)).or_else(|| find(string))
        }

        vibesql_ast::Expression::Extract { expr, .. } => find(expr),

        vibesql_ast::Expression::Like { expr, pattern, .. }
        | vibesql_ast::Expression::Glob { expr, pattern, .. } => {
            find(expr).or_else(|| find(pattern))
        }

        vibesql_ast::Expression::In { expr, .. } => {
            // Note: subquery could reference outer columns but that's a different case
            find(expr)
        }

        vibesql_ast::Expression::QuantifiedComparison { expr, .. } => find(expr),

        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => operand
            .as_ref()
            .and_then(|e| find(e))
            .or_else(|| {
                when_clauses.iter().find_map(|when_clause| {
                    when_clause
                        .conditions
                        .iter()
                        .find_map(find)
                        .or_else(|| find(&when_clause.result))
                })
            })
            .or_else(|| else_result.as_ref().and_then(|e| find(e))),

        vibesql_ast::Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let args_reference = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args.iter().find_map(find),
            };

            // Check PARTITION BY and ORDER BY clauses
            args_reference
                .or_else(|| {
                    over.partition_by.as_ref().and_then(|exprs| exprs.iter().find_map(find))
                })
                .or_else(|| {
                    over.order_by
                        .as_ref()
                        .and_then(|items| items.iter().find_map(|item| find(&item.expr)))
                })
        }

        // These don't contain column references:
        vibesql_ast::Expression::Literal(_) => None,
        vibesql_ast::Expression::CollatedLiteral { .. } => None,
        vibesql_ast::Expression::Wildcard => None,
        vibesql_ast::Expression::ScalarSubquery(_) => None, // Subquery has its own scope
        vibesql_ast::Expression::Exists { .. } => None,     // Subquery has its own scope
        vibesql_ast::Expression::CurrentDate => None,
        vibesql_ast::Expression::CurrentTime { .. } => None,
        vibesql_ast::Expression::CurrentTimestamp { .. } => None,
        vibesql_ast::Expression::NextValue { .. } => None, // Sequence reference, not column
        vibesql_ast::Expression::SessionVariable { .. } => None, // Session variable, not column
        vibesql_ast::Expression::MatchAgainst { columns, search_modifier, .. } => {
            // MATCH AGAINST references columns and the search term
            columns.first().cloned().or_else(|| find(search_modifier))
        }

        // Placeholders don't reference columns (they're parameter markers)
        vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_) => None,

        // Conjunction and Disjunction - check all children
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => children.iter().find_map(find),

        // Row value constructor - check all values
        vibesql_ast::Expression::RowValueConstructor(values) => values.iter().find_map(find),

        vibesql_ast::Expression::Collate { expr, .. } => find(expr),

        vibesql_ast::Expression::Raise { error_message, .. } => {
            error_message.as_ref().and_then(|msg| find(msg))
        }
    }
}

/// Check whether an expression contains a subquery (scalar subquery, EXISTS,
/// IN-subquery, or quantified comparison) anywhere in its tree.
///
/// Used only for FROM-less SELECT WHERE-clause *routing* (issue #6306): a
/// column reference living inside a nested subquery — e.g. `WHERE (SELECT
/// c)` — is invisible to [`find_from_less_column_ref`] because that helper
/// deliberately treats subqueries as their own scope (needed for accurate
/// "no such column" error reporting on the SELECT list). But the outer
/// alias-binding WHERE path still needs to be engaged in that case, since
/// `c` can only resolve to a select-list alias. This helper is intentionally
/// coarse: it does not distinguish whether the subquery's *own* references
/// bind to an outer alias or to its own inner scope — over-routing to the
/// alias-binding path is benign because that path binds aliases as outer
/// context and subqueries still resolve their own inner scope first
/// (innermost-scope-first, #5880).
fn expression_contains_subquery(expr: &vibesql_ast::Expression) -> bool {
    let contains = expression_contains_subquery;
    match expr {
        vibesql_ast::Expression::ScalarSubquery(_)
        | vibesql_ast::Expression::Exists { .. }
        | vibesql_ast::Expression::In { .. }
        | vibesql_ast::Expression::QuantifiedComparison { .. } => true,

        vibesql_ast::Expression::Literal(_)
        | vibesql_ast::Expression::CollatedLiteral { .. }
        | vibesql_ast::Expression::Wildcard
        | vibesql_ast::Expression::CurrentDate
        | vibesql_ast::Expression::CurrentTime { .. }
        | vibesql_ast::Expression::CurrentTimestamp { .. }
        | vibesql_ast::Expression::NextValue { .. }
        | vibesql_ast::Expression::SessionVariable { .. }
        | vibesql_ast::Expression::PseudoVariable { .. }
        | vibesql_ast::Expression::Default
        | vibesql_ast::Expression::DuplicateKeyValue { .. }
        | vibesql_ast::Expression::ColumnRef(_)
        | vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_) => false,

        vibesql_ast::Expression::BinaryOp { left, right, .. } => contains(left) || contains(right),
        vibesql_ast::Expression::UnaryOp { expr, .. } => contains(expr),
        vibesql_ast::Expression::Function { args, .. }
        | vibesql_ast::Expression::AggregateFunction { args, .. } => args.iter().any(contains),
        vibesql_ast::Expression::IsNull { expr, .. } => contains(expr),
        vibesql_ast::Expression::IsDistinctFrom { left, right, .. } => {
            contains(left) || contains(right)
        }
        vibesql_ast::Expression::IsTruthValue { expr, .. } => contains(expr),
        vibesql_ast::Expression::InList { expr, values, .. } => {
            contains(expr) || values.iter().any(contains)
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            contains(expr) || contains(low) || contains(high)
        }
        vibesql_ast::Expression::Cast { expr, .. } => contains(expr),
        vibesql_ast::Expression::Interval { value, .. } => contains(value),
        vibesql_ast::Expression::Position { substring, string, .. } => {
            contains(substring) || contains(string)
        }
        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| contains(e)) || contains(string)
        }
        vibesql_ast::Expression::Extract { expr, .. } => contains(expr),
        vibesql_ast::Expression::Like { expr, pattern, .. }
        | vibesql_ast::Expression::Glob { expr, pattern, .. } => {
            contains(expr) || contains(pattern)
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| contains(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(contains) || contains(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains(e))
        }
        vibesql_ast::Expression::WindowFunction { function, over } => {
            let args_have_subquery = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, filter, .. } => {
                    args.iter().any(contains) || filter.as_ref().is_some_and(|f| contains(f))
                }
                vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args.iter().any(contains),
            };
            args_have_subquery
                || over.partition_by.as_ref().is_some_and(|exprs| exprs.iter().any(contains))
                || over
                    .order_by
                    .as_ref()
                    .is_some_and(|items| items.iter().any(|item| contains(&item.expr)))
        }
        vibesql_ast::Expression::MatchAgainst { search_modifier, .. } => contains(search_modifier),
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children)
        | vibesql_ast::Expression::RowValueConstructor(children) => children.iter().any(contains),
        vibesql_ast::Expression::Collate { expr, .. } => contains(expr),
        vibesql_ast::Expression::Raise { error_message, .. } => {
            error_message.as_ref().is_some_and(|msg| contains(msg))
        }
    }
}

impl SelectExecutor<'_> {
    /// Check if an expression references a column (which requires FROM clause).
    ///
    /// NEW/OLD pseudo-variables count as column references here.
    pub(super) fn expression_references_column(&self, expr: &vibesql_ast::Expression) -> bool {
        find_from_less_column_ref(expr, true).is_some()
    }

    /// Check if an expression contains a subquery (scalar subquery, EXISTS,
    /// IN-subquery, or quantified comparison) anywhere in its tree.
    ///
    /// Issue #6306: routing-only helper for the FROM-less SELECT WHERE-clause
    /// alias-binding decision — see [`expression_contains_subquery`] for why
    /// this is needed alongside [`Self::expression_references_column`].
    pub(super) fn expression_contains_subquery(&self, expr: &vibesql_ast::Expression) -> bool {
        expression_contains_subquery(expr)
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
        find_from_less_column_ref(expr, false).is_some()
    }

    /// Find the first column reference in an expression that a from-less
    /// SELECT cannot resolve, returning its display text (source case
    /// preserved) for SQLite's `no such column: X` error (#5804).
    ///
    /// NEW/OLD pseudo-variables count as unresolvable references here.
    pub(super) fn find_column_ref(&self, expr: &vibesql_ast::Expression) -> Option<String> {
        find_from_less_column_ref(expr, true)
    }

    /// Like [`Self::find_column_ref`], but ignores NEW/OLD pseudo-variables.
    ///
    /// Used inside a trigger body (#5445), where the firing row's NEW/OLD
    /// context resolves pseudo-variables.
    pub(super) fn find_non_pseudo_column_ref(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<String> {
        find_from_less_column_ref(expr, false)
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

        // Convert to integer using the shared SQLite LIMIT/OFFSET affinity
        // rules (integers, booleans as 0/1, integral reals, full-string
        // numeric text — see select::helpers::coerce_limit_offset_to_i64).
        let n = crate::select::helpers::coerce_limit_offset_to_i64(value)?;

        // SQLite compatibility: negative values have special meanings
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
}
