//! View-expansion reference guard (SQLite ticket `[d58ccbb3f1b]`).
//!
//! VibeSQL resolves a view in the FROM clause by *executing* the view's query
//! (see `select/scan/table.rs`). With deeply self-referential view nests such as
//!
//! ```sql
//! CREATE VIEW v2  AS SELECT * FROM v1  UNION SELECT * FROM v1;
//! CREATE VIEW v4  AS SELECT * FROM v2  UNION SELECT * FROM v2;
//! -- ... doubling each level ...
//! CREATE VIEW v32768 AS SELECT * FROM v16384 UNION SELECT * FROM v16384;
//! SELECT * FROM v32768 UNION SELECT * FROM v32768;
//! ```
//!
//! each level doubles the number of times the innermost view (`v1`) is
//! materialized, so a 16-deep nest expands to `2^16 = 65536` materializations.
//! Executing that lazily is exponential and effectively hangs the query
//! (this is the `view3.test` TIMEOUT tracked in #5394).
//!
//! SQLite catches the same pathology *statically* at prepare time by tracking a
//! per-table reference count (`Table.nRef`, a `u16`) while expanding views, and
//! raising `too many references to "<view>": max 65535` once any table/view is
//! referenced more than 65535 times after full expansion.
//!
//! This module reproduces that guard cheaply: before a top-level SELECT runs, we
//! walk the query's view-reference graph and, with per-view memoization, compute
//! the total number of times each view would be referenced after the whole tree
//! is expanded. If any view exceeds [`MAX_VIEW_REFERENCES`] we return the same
//! error SQLite does instead of attempting the exponential materialization.
//!
//! The walk is linear in the size of the (distinct) view definitions thanks to
//! memoization, so it adds negligible overhead to ordinary queries.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{Expression, FromClause, SelectStmt};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Maximum number of references to any single table/view after full view
/// expansion. Matches SQLite's `Table.nRef` `u16` ceiling.
pub const MAX_VIEW_REFERENCES: u64 = 65535;

/// Saturating ceiling used while accumulating reference counts. Keeps the
/// arithmetic cheap and bounded even for pathological nests far beyond the
/// 65535 limit (we only care whether the limit is exceeded).
const SATURATE_AT: u64 = MAX_VIEW_REFERENCES + 1;

/// Check whether executing `stmt` would reference any view more than
/// [`MAX_VIEW_REFERENCES`] times after full view expansion.
///
/// Returns `Err(ExecutorError::Other(...))` with SQLite's message
/// (`too many references to "<view>": max 65535`) for the most-referenced view
/// when the limit is exceeded. Returns `Ok(())` otherwise.
///
/// This is intended to run once per top-level query.
pub(crate) fn check_view_reference_limit(
    stmt: &SelectStmt,
    database: &Database,
) -> Result<(), ExecutorError> {
    let totals = view_reference_totals(stmt, database);

    // Report the most-referenced offending view. SQLite reports the innermost,
    // most-referenced table; for the canonical doubling nest the leaf view
    // dominates, so picking the maximum matches its behaviour.
    if let Some((name, _)) =
        totals.iter().filter(|(_, c)| **c > MAX_VIEW_REFERENCES).max_by_key(|(_, c)| **c)
    {
        return Err(ExecutorError::Other(format!(
            "too many references to \"{name}\": max {MAX_VIEW_REFERENCES}"
        )));
    }

    Ok(())
}

/// Returns the highest number of times any single view would be referenced if
/// `stmt` were fully view-expanded (saturated at [`SATURATE_AT`]). Used to
/// decide whether executing the query to derive columns at CREATE VIEW time
/// would be prohibitively expensive (#5394).
pub(crate) fn max_view_reference_count(stmt: &SelectStmt, database: &Database) -> u64 {
    view_reference_totals(stmt, database).values().copied().max().unwrap_or(0)
}

/// Compute, per view, the total number of references produced by fully
/// expanding `stmt`'s view tree. Linear in the number of distinct views thanks
/// to per-view subtree memoization. Returns an empty map when no views are
/// touched (the common case — a fast exit).
fn view_reference_totals(stmt: &SelectStmt, database: &Database) -> HashMap<String, u64> {
    // Collect the view references named directly in the top-level query. Each is
    // a single reference whose subtree we then expand.
    let mut top_level_views: Vec<String> = Vec::new();
    collect_view_refs(stmt, database, &mut top_level_views);
    if top_level_views.is_empty() {
        return HashMap::new();
    }

    // Memoizes, per view, how many times each view is referenced when ONE
    // reference to that view is fully expanded (the view itself counts once).
    // This makes the whole walk linear in the number of distinct views even for
    // doubling nests, where a naive recursion would be exponential.
    let mut subtree_memo: HashMap<String, HashMap<String, u64>> = HashMap::new();

    let mut totals: HashMap<String, u64> = HashMap::new();
    for view_name in &top_level_views {
        let subtree = expand_view(view_name, database, &mut subtree_memo, &mut Vec::new());
        for (name, count) in subtree {
            let entry = totals.entry(name).or_insert(0);
            *entry = entry.saturating_add(count).min(SATURATE_AT);
        }
    }
    totals
}

/// Compute, with memoization, the per-view reference counts produced by fully
/// expanding ONE reference to `view_name`. The returned map includes the view
/// itself (count 1) plus the expansion of every view its definition references.
///
/// `stack` guards against cyclic view definitions: if `view_name` is already on
/// the current expansion path we stop (cycles are reported as an error
/// elsewhere; here we simply avoid infinite recursion). Results computed while a
/// cycle is on the stack are not memoized, since they would be incomplete.
fn expand_view(
    view_name: &str,
    database: &Database,
    memo: &mut HashMap<String, HashMap<String, u64>>,
    stack: &mut Vec<String>,
) -> HashMap<String, u64> {
    let Some(view) = database.catalog.get_view(view_name) else {
        // Not a view (base table, CTE shadowing, etc.) — contributes nothing.
        return HashMap::new();
    };
    let canonical = view.name.clone();

    if let Some(cached) = memo.get(&canonical) {
        return cached.clone();
    }

    // Cycle guard.
    if stack.iter().any(|n| n.eq_ignore_ascii_case(&canonical)) {
        let mut single = HashMap::new();
        single.insert(canonical, 1);
        return single;
    }

    // Start with one reference to this view itself.
    let mut counts: HashMap<String, u64> = HashMap::new();
    counts.insert(canonical.clone(), 1);

    stack.push(canonical.clone());

    let mut child_views: Vec<String> = Vec::new();
    collect_view_refs(&view.query, database, &mut child_views);
    let mut hit_cycle = false;
    for child in &child_views {
        let child_counts = expand_view(child, database, memo, stack);
        for (name, count) in child_counts {
            // If a child's expansion references this view back, we are inside a
            // cycle; the count is incomplete and must not be memoized.
            if name.eq_ignore_ascii_case(&canonical) {
                hit_cycle = true;
            }
            let entry = counts.entry(name).or_insert(0);
            *entry = entry.saturating_add(count).min(SATURATE_AT);
        }
    }

    stack.pop();

    // Only memoize when this expansion did not depend on a cycle through itself,
    // so cached results are always complete and reusable.
    if !hit_cycle {
        memo.insert(canonical, counts.clone());
    }

    counts
}

/// Collect the names of all relations referenced in `stmt`'s FROM clauses
/// (including UNION/INTERSECT/EXCEPT arms, derived-table subqueries, CTEs, and
/// scalar/IN subqueries in expressions) that resolve to catalog views.
fn collect_view_refs(stmt: &SelectStmt, database: &Database, out: &mut Vec<String>) {
    // CTE names declared in this query shadow catalog views of the same name
    // (CTEs take precedence during FROM resolution). Collect them so we do not
    // count `FROM cte_named_like_a_view` against the view.
    let mut cte_names: HashSet<String> = HashSet::new();

    // WITH-clause CTE bodies can reference views.
    if let Some(ctes) = &stmt.with_clause {
        for cte in ctes {
            cte_names.insert(cte.name.to_ascii_lowercase());
            collect_view_refs(&cte.query, database, out);
        }
    }

    if let Some(from) = &stmt.from {
        collect_from_view_refs(from, database, &cte_names, out);
    }

    // Set-operation right arm (recursively chains for A UNION B UNION C ...).
    if let Some(set_op) = &stmt.set_operation {
        collect_view_refs(&set_op.right, database, out);
    }

    // Subqueries in WHERE / HAVING / SELECT list / projections can reference
    // views too. We scan all expressions for nested SELECTs.
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            collect_expr_view_refs(expr, database, out);
        }
    }
    if let Some(where_clause) = &stmt.where_clause {
        collect_expr_view_refs(where_clause, database, out);
    }
    if let Some(having) = &stmt.having {
        collect_expr_view_refs(having, database, out);
    }
}

fn collect_from_view_refs(
    from: &FromClause,
    database: &Database,
    cte_names: &HashSet<String>,
    out: &mut Vec<String>,
) {
    match from {
        FromClause::Table { name, .. } => {
            // A CTE of the same name shadows the catalog view.
            if cte_names.contains(&name.to_ascii_lowercase()) {
                return;
            }
            if database.catalog.get_view(name).is_some() {
                out.push(name.clone());
            }
        }
        FromClause::Subquery { query, .. } => {
            collect_view_refs(query, database, out);
        }
        FromClause::Join { left, right, condition, .. } => {
            collect_from_view_refs(left, database, cte_names, out);
            collect_from_view_refs(right, database, cte_names, out);
            if let Some(cond) = condition {
                collect_expr_view_refs(cond, database, out);
            }
        }
        FromClause::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_expr_view_refs(expr, database, out);
                }
            }
        }
        FromClause::TableFunction { args, .. } => {
            for expr in args {
                collect_expr_view_refs(expr, database, out);
            }
        }
    }
}

/// Walk an expression tree collecting view references inside nested subqueries
/// (scalar subqueries, IN (SELECT ...), EXISTS (...), quantified comparisons).
///
/// We reuse the shared expression visitor purely to find subquery-bearing
/// nodes; the nested `SelectStmt`s are then handed to [`collect_view_refs`]
/// (which understands FROM clauses), and `Skip` is returned so the generic
/// walker does not also descend into them.
fn collect_expr_view_refs(expr: &Expression, database: &Database, out: &mut Vec<String>) {
    use vibesql_ast::visitor::{walk_expression, ExpressionVisitor, VisitResult};

    struct SubqueryCollector<'a> {
        database: &'a Database,
        out: &'a mut Vec<String>,
    }

    impl ExpressionVisitor for SubqueryCollector<'_> {
        fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
            match expr {
                Expression::ScalarSubquery(query) | Expression::Exists { subquery: query, .. } => {
                    collect_view_refs(query, self.database, self.out);
                    VisitResult::Skip
                }
                Expression::In { subquery, .. }
                | Expression::QuantifiedComparison { subquery, .. } => {
                    collect_view_refs(subquery, self.database, self.out);
                    // The left-hand operand is a plain expression; let the
                    // walker continue so any subqueries there are captured too.
                    VisitResult::Continue
                }
                _ => VisitResult::Continue,
            }
        }
    }

    let mut collector = SubqueryCollector { database, out };
    walk_expression(&mut collector, expr);
}
