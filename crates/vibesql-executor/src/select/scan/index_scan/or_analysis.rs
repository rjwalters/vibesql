//! MULTI-INDEX OR branch analysis.
//!
//! This module detects when a WHERE clause has a **top-level OR whose every
//! branch is independently indexable**, and produces a structured
//! [`MultiIndexOrPlan`] describing one index lookup per branch plus the residual
//! non-OR AND-conjuncts.
//!
//! # Scope (PR 1 of the MULTI-INDEX OR epic, #5668)
//!
//! This is **pure plan representation + analysis** — there is intentionally **no
//! behavior change**. Nothing in the selection or execution path calls
//! [`analyze_multi_index_or`] yet, so existing query results and EXPLAIN QUERY
//! PLAN output are byte-identical. Execution (PR 2), the OR-aware cost model
//! (PR 3), EQP rendering (PR 4), and correlated/join handling (PR 5) build on
//! top of this representation.
//!
//! # Conformance traps (called out in #5668)
//!
//! 1. **Original branch ordinals.** SQLite labels OR branches `INDEX <n>` by the branch's 1-based
//!    position in the *original* OR expression, NOT by a renumbering over the chosen branches.
//!    where9-3.1 expects `INDEX 1` / `INDEX 3`. [`OrBranch::ordinal`] therefore preserves the
//!    original term position.
//! 2. **`IS NULL` vs `=`.** `d IS NULL` is an index seek on the NULL key, distinct from `d = ?`.
//!    The two route to different lookups in later PRs, so they are classified distinctly here via
//!    [`OrBranchKind`].

// PR 1 lands plan representation + analysis only; nothing in the selection or
// execution path calls into this module yet (that is PR 2+). The items below
// are therefore dead-but-tested. Allow dead_code at the module level so the
// intent is explicit and the build stays clean.
#![allow(dead_code)]

use vibesql_ast::Expression;

use super::selection::{OrBranch, OrBranchKind};

/// A detected MULTI-INDEX OR plan: one indexable lookup per OR branch plus the
/// residual non-OR AND-conjuncts.
#[derive(Debug, Clone)]
pub(crate) struct MultiIndexOrPlan {
    /// One entry per OR branch, in original-branch ordinal order.
    pub branches: Vec<OrBranch>,
    /// The non-OR AND-conjuncts (e.g. `b > 1000`) applied around the union, or
    /// `None` when the WHERE clause is exactly the top-level OR.
    pub residual: Option<Expression>,
}

/// Resolve the index that a single OR branch predicate would use, if any.
///
/// Returns the index name when the branch is independently indexable, else
/// `None`. Abstracted as a trait-object-free closure parameter so the
/// structural analysis (ordinals, residual splitting, IS-NULL classification)
/// is unit-testable without constructing a full `Database`.
pub(crate) trait BranchIndexResolver {
    fn resolve(&self, branch: &Expression) -> Option<String>;
}

impl<F> BranchIndexResolver for F
where
    F: Fn(&Expression) -> Option<String>,
{
    fn resolve(&self, branch: &Expression) -> Option<String> {
        (self)(branch)
    }
}

/// Classify an OR branch predicate as an `IS NULL` seek or an ordinary lookup.
///
/// Only `col IS NULL` (not `col IS NOT NULL`) is the NULL-key seek; `IS NOT
/// NULL` is treated as an ordinary lookup shape.
fn classify_branch(branch: &Expression) -> OrBranchKind {
    match branch {
        Expression::IsNull { negated: false, .. } => OrBranchKind::IsNull,
        _ => OrBranchKind::Lookup,
    }
}

/// Split a top-level WHERE clause into its top-level OR (if any) and the
/// surrounding residual AND-conjuncts.
///
/// Two accepted shapes:
/// - The WHERE clause is itself a `Disjunction` (or 2-arg `OR` `BinaryOp`): the OR branches are
///   returned with no residual.
/// - The WHERE clause is a `Conjunction` (or `AND` `BinaryOp`) containing **exactly one** top-level
///   OR term: that OR's branches are returned, with the remaining conjuncts as the residual.
///
/// Returns `(or_branches, residual)` where `or_branches` is a borrowed slice of
/// the original branch expressions and `residual` is the rebuilt non-OR part.
/// Returns `None` when there is no usable top-level OR (e.g. zero or multiple
/// top-level ORs, or none at all).
fn split_top_level_or(where_clause: &Expression) -> Option<(Vec<&Expression>, Option<Expression>)> {
    // Collect the top-level AND-conjuncts (flattened). A non-AND expression is
    // a single-element conjunct list.
    let conjuncts: Vec<&Expression> = match where_clause {
        Expression::Conjunction(exprs) => exprs.iter().collect(),
        Expression::BinaryOp { op: vibesql_ast::BinaryOperator::And, left, right } => {
            vec![left.as_ref(), right.as_ref()]
        }
        other => vec![other],
    };

    // Find the OR conjuncts among the top-level AND terms. We require exactly
    // one top-level OR: multiple top-level ORs are not a single union plan.
    let mut or_term: Option<&Expression> = None;
    let mut residual_terms: Vec<&Expression> = Vec::new();
    for conjunct in &conjuncts {
        if is_or(conjunct) {
            if or_term.is_some() {
                // More than one top-level OR — not a single MULTI-INDEX OR.
                return None;
            }
            or_term = Some(conjunct);
        } else {
            residual_terms.push(conjunct);
        }
    }

    let or_term = or_term?;
    let or_branches = or_branches(or_term);

    let residual = rebuild_residual(&residual_terms);

    Some((or_branches, residual))
}

/// Whether an expression is a top-level OR (flattened `Disjunction` or a 2-arg
/// `OR` `BinaryOp`).
fn is_or(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Disjunction(_)
            | Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, .. }
    )
}

/// Return the OR branches of a top-level OR expression as borrowed slices.
fn or_branches(or_expr: &Expression) -> Vec<&Expression> {
    match or_expr {
        Expression::Disjunction(exprs) => exprs.iter().collect(),
        Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, left, right } => {
            vec![left.as_ref(), right.as_ref()]
        }
        // Caller guarantees `is_or` held.
        _ => Vec::new(),
    }
}

/// Rebuild the residual predicate from the non-OR top-level AND-conjuncts.
fn rebuild_residual(residual_terms: &[&Expression]) -> Option<Expression> {
    match residual_terms.len() {
        0 => None,
        1 => Some(residual_terms[0].clone()),
        _ => Some(Expression::Conjunction(residual_terms.iter().map(|e| (*e).clone()).collect())),
    }
}

/// Analyze a WHERE clause for a MULTI-INDEX OR opportunity.
///
/// Detects a top-level OR whose **every** branch is independently indexable
/// (per `resolver`), preserving original 1-based branch ordinals and splitting
/// the residual non-OR AND-conjuncts. Returns `None` (leaving the existing
/// single-scan path unchanged) when:
/// - there is no usable single top-level OR, or
/// - any branch fails to resolve to an index.
///
/// This function performs **no** side effects and never mutates the plan — it
/// is consumed by later PRs.
pub(crate) fn analyze_multi_index_or<R: BranchIndexResolver>(
    where_clause: &Expression,
    resolver: &R,
) -> Option<MultiIndexOrPlan> {
    let (or_branches, residual) = split_top_level_or(where_clause)?;

    // Need at least two branches for a union to be meaningful.
    if or_branches.len() < 2 {
        return None;
    }

    let mut branches = Vec::with_capacity(or_branches.len());
    for (idx, branch) in or_branches.iter().enumerate() {
        // CONFORMANCE: ordinal is the 1-based original term position, NOT a
        // renumbering over chosen branches.
        let ordinal = idx + 1;

        // Every branch must be independently indexable. If any branch is not,
        // SQLite (and VibeSQL today) falls back to a single scan + residual.
        let index_name = resolver.resolve(branch)?;

        branches.push(OrBranch {
            ordinal,
            index_name,
            branch_predicate: (*branch).clone(),
            kind: classify_branch(branch),
        });
    }

    Some(MultiIndexOrPlan { branches, residual })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};
    use vibesql_types::SqlValue;

    use super::*;

    // ---- expression builders -------------------------------------------------

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::simple(name, false))
    }

    fn eq(column: &str, value: i64) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(col(column)),
            right: Box::new(Expression::Literal(SqlValue::Integer(value))),
        }
    }

    fn gt(column: &str, value: i64) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(col(column)),
            right: Box::new(Expression::Literal(SqlValue::Integer(value))),
        }
    }

    fn is_null(column: &str) -> Expression {
        Expression::IsNull { expr: Box::new(col(column)), negated: false }
    }

    fn or(branches: Vec<Expression>) -> Expression {
        Expression::Disjunction(branches)
    }

    fn and(terms: Vec<Expression>) -> Expression {
        Expression::Conjunction(terms)
    }

    /// Resolver that maps a single-column predicate to a per-column index name.
    /// Recognizes `col = ?`, ranges, and `col IS NULL` on the known columns.
    struct ColumnIndexResolver {
        indexes: HashMap<String, String>, // column -> index name
    }

    impl ColumnIndexResolver {
        fn new(pairs: &[(&str, &str)]) -> Self {
            ColumnIndexResolver {
                indexes: pairs.iter().map(|(c, i)| (c.to_string(), i.to_string())).collect(),
            }
        }

        fn column_of(branch: &Expression) -> Option<String> {
            match branch {
                Expression::BinaryOp { left, right, .. } => {
                    if let Expression::ColumnRef(c) = left.as_ref() {
                        return Some(c.column_canonical().to_string());
                    }
                    if let Expression::ColumnRef(c) = right.as_ref() {
                        return Some(c.column_canonical().to_string());
                    }
                    None
                }
                Expression::IsNull { expr, .. } => {
                    if let Expression::ColumnRef(c) = expr.as_ref() {
                        Some(c.column_canonical().to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
    }

    impl BranchIndexResolver for ColumnIndexResolver {
        fn resolve(&self, branch: &Expression) -> Option<String> {
            let column = Self::column_of(branch)?;
            self.indexes.get(&column).cloned()
        }
    }

    // ---- tests ---------------------------------------------------------------

    #[test]
    fn detects_fully_indexable_top_level_or() {
        // WHERE c = 1 OR d = 2  — both branches indexable.
        let where_clause = or(vec![eq("c", 1), eq("d", 2)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches.len(), 2);
        assert!(plan.residual.is_none());
        assert_eq!(plan.branches[0].index_name, "t1c");
        assert_eq!(plan.branches[1].index_name, "t1d");
        assert_eq!(plan.branches[0].kind, OrBranchKind::Lookup);
        assert_eq!(plan.branches[1].kind, OrBranchKind::Lookup);
    }

    #[test]
    fn rejects_partially_indexable_or_returns_none() {
        // WHERE c = 1 OR d = 2 — only `c` is indexed → not a MULTI-INDEX OR.
        let where_clause = or(vec![eq("c", 1), eq("d", 2)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c")]); // no index on d

        assert!(analyze_multi_index_or(&where_clause, &resolver).is_none());
    }

    #[test]
    fn preserves_original_branch_ordinals() {
        // Three-term OR; only branches 1 and 3 are indexable individually, but
        // because branch 2 is NOT indexable the whole thing is rejected. To
        // exercise ordinal preservation with a fold-away, we model the SQLite
        // where9-3.1 shape where the middle term collapses: here all three are
        // indexable and we assert ordinals are 1, 2, 3 in original order — and
        // separately that when a plan is built the ordinals equal original
        // positions, never a 1..=N renumber over a filtered subset.
        let where_clause = or(vec![eq("c", 1), eq("e", 2), eq("f", 3)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("e", "t1e"), ("f", "t1f")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        let ordinals: Vec<usize> = plan.branches.iter().map(|b| b.ordinal).collect();
        assert_eq!(ordinals, vec![1, 2, 3]);
        // index names line up with the original branch positions
        assert_eq!(plan.branches[0].index_name, "t1c");
        assert_eq!(plan.branches[2].index_name, "t1f");
    }

    #[test]
    fn ordinal_is_original_position_not_chosen_renumber() {
        // A custom resolver that resolves branches by *position* would expose a
        // renumbering bug. We assert the ordinal equals the original term index
        // even when the branch predicates are heterogeneous.
        let where_clause = or(vec![is_null("d"), eq("c", 1)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches[0].ordinal, 1);
        assert_eq!(plan.branches[1].ordinal, 2);
    }

    #[test]
    fn splits_residual_and_conjuncts() {
        // WHERE (c = 1 OR d = 2) AND b > 1000
        let where_clause = and(vec![or(vec![eq("c", 1), eq("d", 2)]), gt("b", 1000)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches.len(), 2);
        // Residual is exactly `b > 1000`.
        assert_eq!(plan.residual, Some(gt("b", 1000)));
    }

    #[test]
    fn splits_multiple_residual_conjuncts_into_conjunction() {
        // WHERE (c = 1 OR d = 2) AND b > 1000 AND a = 5
        let where_clause = and(vec![or(vec![eq("c", 1), eq("d", 2)]), gt("b", 1000), eq("a", 5)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        match plan.residual {
            Some(Expression::Conjunction(terms)) => {
                assert_eq!(terms.len(), 2);
                assert_eq!(terms[0], gt("b", 1000));
                assert_eq!(terms[1], eq("a", 5));
            }
            other => panic!("expected a 2-term residual conjunction, got {other:?}"),
        }
    }

    #[test]
    fn is_null_branch_is_distinct_from_equality() {
        // WHERE c = 31031 OR d IS NULL  (where9-5.1 shape)
        let where_clause = or(vec![eq("c", 31031), is_null("d")]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches[0].kind, OrBranchKind::Lookup);
        assert_eq!(plan.branches[1].kind, OrBranchKind::IsNull);
        // The branch predicate preserves the original IS NULL expression.
        assert_eq!(plan.branches[1].branch_predicate, is_null("d"));
    }

    #[test]
    fn is_not_null_branch_is_lookup_kind() {
        // `IS NOT NULL` is not the NULL-key seek; classify as a lookup shape.
        let is_not_null = Expression::IsNull { expr: Box::new(col("d")), negated: true };
        let where_clause = or(vec![eq("c", 1), is_not_null]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches[1].kind, OrBranchKind::Lookup);
    }

    #[test]
    fn no_top_level_or_returns_none() {
        // WHERE c = 1 AND d = 2 — no OR at all.
        let where_clause = and(vec![eq("c", 1), eq("d", 2)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        assert!(analyze_multi_index_or(&where_clause, &resolver).is_none());
    }

    #[test]
    fn multiple_top_level_ors_returns_none() {
        // WHERE (c = 1 OR d = 2) AND (e = 3 OR f = 4) — two top-level ORs is
        // not a single union plan.
        let where_clause =
            and(vec![or(vec![eq("c", 1), eq("d", 2)]), or(vec![eq("e", 3), eq("f", 4)])]);
        let resolver =
            ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d"), ("e", "t1e"), ("f", "t1f")]);

        assert!(analyze_multi_index_or(&where_clause, &resolver).is_none());
    }

    #[test]
    fn handles_binaryop_or_shape() {
        // Some predicates may arrive as a 2-arg BinaryOp OR rather than a
        // flattened Disjunction. Both shapes must be detected.
        let where_clause = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(eq("c", 1)),
            right: Box::new(eq("d", 2)),
        };
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.branches[0].ordinal, 1);
        assert_eq!(plan.branches[1].ordinal, 2);
    }

    #[test]
    fn handles_binaryop_and_residual_shape() {
        // WHERE (c = 1 OR d = 2) AND b > 1000 expressed with BinaryOp::And.
        let where_clause = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(or(vec![eq("c", 1), eq("d", 2)])),
            right: Box::new(gt("b", 1000)),
        };
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches.len(), 2);
        assert_eq!(plan.residual, Some(gt("b", 1000)));
    }

    #[test]
    fn three_way_or_all_indexable() {
        // WHERE c = 1 OR d IS NULL OR e = 3  — mixed shapes, all indexable.
        let where_clause = or(vec![eq("c", 1), is_null("d"), eq("e", 3)]);
        let resolver = ColumnIndexResolver::new(&[("c", "t1c"), ("d", "t1d"), ("e", "t1e")]);

        let plan = analyze_multi_index_or(&where_clause, &resolver).expect("plan");
        assert_eq!(plan.branches.len(), 3);
        assert_eq!(plan.branches.iter().map(|b| b.ordinal).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(plan.branches[1].kind, OrBranchKind::IsNull);
    }
}
