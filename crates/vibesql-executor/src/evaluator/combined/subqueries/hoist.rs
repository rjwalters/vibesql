//! Hoisting of uncorrelated scalar subqueries out of per-row predicates
//!
//! ## Problem (issue #5809)
//!
//! A query like:
//!
//! ```sql
//! SELECT ... FROM t WHERE run_id = (SELECT MAX(run_id) FROM t)
//! ```
//!
//! contains an *uncorrelated* scalar subquery: its value does not depend on the
//! outer row, so it must be evaluated exactly once per statement (this is also
//! what SQLite does, including for non-deterministic functions like
//! `random()`).
//!
//! The sequential evaluator already achieves "evaluate once" through the
//! per-evaluator LRU `subquery_cache`. However, the parallel filter paths
//! (morsel-driven WHERE filtering, parallel predicate pushdown) construct a
//! **fresh thread-local evaluator per row** via `from_parallel_components`,
//! each with an empty cache — so the subquery's full table scan re-executes
//! for every candidate row, turning an O(n) filter into an O(n²) blowup
//! (413k rows × 413k-row scan on the canonical CLAUDE.md pass-rate query).
//!
//! ## Fix
//!
//! Before per-row filtering begins, rewrite the predicate expression once,
//! replacing each provably-uncorrelated single-column scalar subquery with the
//! `Expression::Literal` value it evaluates to. Downstream filtering (parallel
//! or sequential) then sees a plain literal comparison, which additionally
//! unlocks the compiled-predicate fast paths.
//!
//! ## Safety conditions
//!
//! A `ScalarSubquery` node is hoisted only when ALL of the following hold:
//!
//! 1. The evaluator has no outer context of any kind (no outer row/schema,
//!    no chained outer evaluator, no trigger or procedural context). This is
//!    the top-level-query case; nested evaluation contexts fall back to the
//!    existing cached per-row path.
//! 2. The syntactic correlation check (`is_correlated_cached`) reports the
//!    subquery as uncorrelated.
//! 3. The correlation-reference extraction used by the caching layer
//!    (`extract_correlation_values`) finds **zero** outer column references.
//!    This catches unqualified column names that the syntactic check can miss
//!    (the aggnested-1.4 / select1-18.x hazard).
//! 4. The subquery's select list is statically known to produce exactly one
//!    column — multi-column `ScalarSubquery` nodes participate in row-value
//!    comparisons like `(a, b) = (SELECT x, y ...)` and must not be folded to
//!    a scalar literal.
//! 5. The one-shot evaluation succeeds. On error the node is left untouched so
//!    the per-row path preserves lazy error semantics (e.g. a subquery inside
//!    a CASE branch that is never taken).
//!
//! The traversal never descends into nested `SelectStmt` bodies (`IN`,
//! `EXISTS`, quantified comparisons, or the subquery's own AST): expressions
//! inside those bodies are scoped to the inner query and may be correlated to
//! intermediate scopes this evaluator cannot see.

use super::{
    super::super::core::CombinedExpressionEvaluator, correlation::extract_correlation_values,
};

impl CombinedExpressionEvaluator<'_> {
    /// Rewrite `expr`, replacing each provably-uncorrelated scalar subquery
    /// with the literal value it evaluates to (evaluated exactly once).
    ///
    /// Returns `Some(rewritten)` if at least one subquery was hoisted, or
    /// `None` if the expression is unchanged (caller keeps the original).
    /// Never returns an error: any subquery that cannot be safely or
    /// successfully evaluated is simply left in place for the per-row path.
    pub(crate) fn hoist_uncorrelated_scalar_subqueries(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<vibesql_ast::Expression> {
        // Only safe in a top-level evaluation context (see module docs).
        if self.database.is_none()
            || self.outer_row.is_some()
            || self.outer_schema.is_some()
            || self.outer_rows.is_some()
            || self.outer_context.is_some()
            || self.trigger_context.is_some()
            || self.procedural_context.is_some()
        {
            return None;
        }

        // Cheap pre-scan: the vast majority of WHERE clauses contain no scalar
        // subquery. Avoid cloning the expression tree for those.
        if !contains_hoistable_position(expr) {
            return None;
        }

        let mut rewritten = expr.clone();
        let mut changed = false;
        self.hoist_expr_mut(&mut rewritten, &mut changed);
        if changed {
            Some(rewritten)
        } else {
            None
        }
    }

    /// Attempt to evaluate a single scalar subquery once, returning its value
    /// if all safety conditions hold (see module docs).
    fn try_hoist_scalar_subquery(
        &self,
        subquery: &vibesql_ast::SelectStmt,
    ) -> Option<vibesql_types::SqlValue> {
        let database = self.database?;

        // Condition 2: syntactically uncorrelated.
        if self.is_correlated_cached(subquery) {
            return None;
        }

        // Condition 3: the caching layer's correlation-reference walk must
        // find zero outer references. `extract_correlation_values` returns
        // `Some(vec![])` exactly when no outer refs exist; a non-empty vec or
        // `None` (extraction failure against the empty probe row) means the
        // subquery might read the outer row, so we must not hoist.
        let probe_row = vibesql_storage::Row::new(Vec::<vibesql_types::SqlValue>::new());
        match extract_correlation_values(subquery, &probe_row, self.schema) {
            Some(refs) if refs.is_empty() => {}
            _ => return None,
        }

        // Condition 4: genuine single-column scalar subquery.
        match super::schema_utils::compute_select_list_column_count(
            subquery,
            database,
            self.cte_context,
        ) {
            Ok(1) => {}
            _ => return None,
        }

        // Condition 5: evaluate once; on error fall back to the per-row path.
        // The empty probe row is safe because conditions 2+3 guarantee the
        // subquery never reads outer row values.
        self.eval_scalar_subquery(subquery, &probe_row).ok()
    }

    /// Recursively rewrite hoistable scalar subqueries in place.
    ///
    /// Recurses only through scalar expression positions; never descends into
    /// nested `SelectStmt` bodies.
    fn hoist_expr_mut(&self, expr: &mut vibesql_ast::Expression, changed: &mut bool) {
        use vibesql_ast::Expression;

        match expr {
            Expression::ScalarSubquery(subquery) => {
                if let Some(value) = self.try_hoist_scalar_subquery(subquery) {
                    *expr = Expression::Literal(value);
                    *changed = true;
                }
                // If not hoistable, leave the node fully intact (do not
                // descend into the subquery body).
            }
            Expression::BinaryOp { left, right, .. } => {
                self.hoist_expr_mut(left, changed);
                self.hoist_expr_mut(right, changed);
            }
            Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
                for e in exprs {
                    self.hoist_expr_mut(e, changed);
                }
            }
            Expression::UnaryOp { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::Function { args, .. } => {
                for arg in args {
                    self.hoist_expr_mut(arg, changed);
                }
            }
            Expression::IsNull { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::IsDistinctFrom { left, right, .. } => {
                self.hoist_expr_mut(left, changed);
                self.hoist_expr_mut(right, changed);
            }
            Expression::IsTruthValue { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    self.hoist_expr_mut(op, changed);
                }
                for when in when_clauses {
                    for condition in &mut when.conditions {
                        self.hoist_expr_mut(condition, changed);
                    }
                    self.hoist_expr_mut(&mut when.result, changed);
                }
                if let Some(else_expr) = else_result {
                    self.hoist_expr_mut(else_expr, changed);
                }
            }
            // For IN / quantified comparisons, only the scalar LHS is in this
            // scope; the subquery body is not descended into.
            Expression::In { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::QuantifiedComparison { expr: inner, .. } => {
                self.hoist_expr_mut(inner, changed)
            }
            Expression::InList { expr: inner, values, .. } => {
                self.hoist_expr_mut(inner, changed);
                for v in values {
                    self.hoist_expr_mut(v, changed);
                }
            }
            Expression::Between { expr: inner, low, high, .. } => {
                self.hoist_expr_mut(inner, changed);
                self.hoist_expr_mut(low, changed);
                self.hoist_expr_mut(high, changed);
            }
            Expression::Cast { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::Position { substring, string, .. } => {
                self.hoist_expr_mut(substring, changed);
                self.hoist_expr_mut(string, changed);
            }
            Expression::Trim { removal_char, string, .. } => {
                if let Some(c) = removal_char {
                    self.hoist_expr_mut(c, changed);
                }
                self.hoist_expr_mut(string, changed);
            }
            Expression::Extract { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            Expression::Like { expr: inner, pattern, escape, .. }
            | Expression::Glob { expr: inner, pattern, escape, .. } => {
                self.hoist_expr_mut(inner, changed);
                self.hoist_expr_mut(pattern, changed);
                if let Some(esc) = escape {
                    self.hoist_expr_mut(esc, changed);
                }
            }
            Expression::Interval { value, .. } => self.hoist_expr_mut(value, changed),
            Expression::RowValueConstructor(elements) => {
                for e in elements {
                    self.hoist_expr_mut(e, changed);
                }
            }
            Expression::Collate { expr: inner, .. } => self.hoist_expr_mut(inner, changed),
            // Leaves and constructs whose bodies are out of scope
            // (EXISTS, aggregate functions, window functions, literals,
            // column refs, placeholders, ...): nothing to do.
            _ => {}
        }
    }
}

/// Cheap pre-scan: does the expression contain a `ScalarSubquery` node in a
/// position the rewriter would visit? Mirrors `hoist_expr_mut`'s traversal.
fn contains_hoistable_position(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ScalarSubquery(_) => true,
        Expression::BinaryOp { left, right, .. } => {
            contains_hoistable_position(left) || contains_hoistable_position(right)
        }
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(contains_hoistable_position)
        }
        Expression::UnaryOp { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::Function { args, .. } => args.iter().any(contains_hoistable_position),
        Expression::IsNull { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::IsDistinctFrom { left, right, .. } => {
            contains_hoistable_position(left) || contains_hoistable_position(right)
        }
        Expression::IsTruthValue { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_deref().is_some_and(contains_hoistable_position)
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(contains_hoistable_position)
                        || contains_hoistable_position(&w.result)
                })
                || else_result.as_deref().is_some_and(contains_hoistable_position)
        }
        Expression::In { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::QuantifiedComparison { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::InList { expr: inner, values, .. } => {
            contains_hoistable_position(inner) || values.iter().any(contains_hoistable_position)
        }
        Expression::Between { expr: inner, low, high, .. } => {
            contains_hoistable_position(inner)
                || contains_hoistable_position(low)
                || contains_hoistable_position(high)
        }
        Expression::Cast { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::Position { substring, string, .. } => {
            contains_hoistable_position(substring) || contains_hoistable_position(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_deref().is_some_and(contains_hoistable_position)
                || contains_hoistable_position(string)
        }
        Expression::Extract { expr: inner, .. } => contains_hoistable_position(inner),
        Expression::Like { expr: inner, pattern, escape, .. }
        | Expression::Glob { expr: inner, pattern, escape, .. } => {
            contains_hoistable_position(inner)
                || contains_hoistable_position(pattern)
                || escape.as_deref().is_some_and(contains_hoistable_position)
        }
        Expression::Interval { value, .. } => contains_hoistable_position(value),
        Expression::RowValueConstructor(elements) => {
            elements.iter().any(contains_hoistable_position)
        }
        Expression::Collate { expr: inner, .. } => contains_hoistable_position(inner),
        _ => false,
    }
}
