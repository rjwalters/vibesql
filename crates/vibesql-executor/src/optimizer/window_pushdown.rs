//! WHERE push-down into window-function subqueries and views (#5292)
//!
//! SQLite pushes WHERE constraints down into views/subqueries that contain
//! window functions when the predicate is constant within every window
//! partition (`pushDownWhereTerms()` in src/select.c). Filtering whole
//! partitions before the window functions run is semantics-preserving, and
//! lets the inner table scan use an index instead of materializing the full
//! window result and filtering afterwards.
//!
//! ## Safety rule
//!
//! A WHERE conjunct may be pushed below the window functions ONLY if every
//! column it references maps (through the subquery SELECT list) to a bare
//! inner column that appears in the PARTITION BY list of EVERY window
//! function in the subquery. If any window function lacks a PARTITION BY
//! covering the predicate's columns (e.g. `row_number() OVER ()`), the
//! predicate is NOT constant within that window's partitions and pushing it
//! would change results (windowpushd.test v2). Predicates containing
//! subqueries, aggregate/window functions, placeholders, or volatile
//! functions are never pushed.
//!
//! ## Scope (conservative)
//!
//! - Only fires when the outer FROM clause is a single derived table or a
//!   single view reference (no joins). With multiple FROM sources an
//!   unqualified column reference might bind to a sibling table, so pushing
//!   by name alone would be unsound. Multi-source push-down is follow-on
//!   work.
//! - The pushed conjuncts are *copied* into the inner WHERE clause; the
//!   outer WHERE clause is left untouched. The outer evaluation is redundant
//!   but harmless for the deterministic predicates this pass accepts, and it
//!   keeps the transform trivially value-preserving.
//! - The inner query must be a plain SELECT: no set operation, VALUES,
//!   GROUP BY, HAVING, DISTINCT, LIMIT or OFFSET.
//!
//! Note: EXPLAIN QUERY PLAN does not yet model this rewrite (views are
//! rendered opaquely by `explain.rs`), so windowpushd.test EQP patterns
//! remain shim warnings until plan rendering learns to expand
//! views/subqueries. See the follow-on issue referenced in #5292.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{CommonTableExpr, Expression, FromClause, SelectItem, SelectStmt};
use vibesql_storage::Database;

use super::where_pushdown::flatten_conjuncts;

/// Apply WHERE push-down into window subqueries/views at the top level of
/// `stmt`. Returns the (possibly) rewritten statement; when the pass does
/// not fire the input is returned unchanged (no AST clone).
///
/// `outer_cte_names` holds the lowercased names of CTEs already in scope
/// from enclosing queries (the executor's `cte_context`). At execution time
/// CTEs take precedence over catalog objects (`select/scan/table.rs` checks
/// `cte_results` first), so the view-expansion branch must NOT fire for a
/// name that is bound as a CTE — either by this statement's own WITH clause
/// or by an enclosing query — or the rewrite would silently redirect the
/// CTE reference to a same-named view.
///
/// Nested subqueries are handled when they are themselves executed: the
/// SELECT executor invokes this pass for every statement it runs, so a
/// derived table's own FROM subquery is rewritten during the derived table's
/// execution.
pub fn push_where_into_window_subqueries(
    mut stmt: SelectStmt,
    database: &Database,
    outer_cte_names: &HashSet<String>,
) -> SelectStmt {
    let Some(where_clause) = stmt.where_clause.as_ref() else {
        return stmt;
    };

    let new_from = match &stmt.from {
        // FROM (SELECT ... window fns ...) AS alias [(col, ...)]
        Some(FromClause::Subquery { query, alias, column_aliases }) => {
            try_push_into_subquery(where_clause, query, alias, column_aliases.as_deref()).map(
                |new_query| FromClause::Subquery {
                    query: Box::new(new_query),
                    alias: alias.clone(),
                    column_aliases: column_aliases.clone(),
                },
            )
        }

        // FROM view_name [AS alias] — expand the view into a derived table
        // carrying the pushed predicate. Only done when at least one
        // conjunct is pushable, so plain view scans keep their existing
        // execution path (including the SELECT privilege check, which the
        // scan performs; if the check would fail here we skip the rewrite
        // and let the scan raise the error).
        Some(FromClause::Table { name, alias, column_aliases, .. }) => {
            // CTE shadowing gate: a CTE bound to this name (in this
            // statement's WITH clause or in an enclosing query's scope)
            // takes precedence over a catalog view at execution time, so
            // expanding the view here would change which object the query
            // reads. SQLite identifiers compare case-insensitively (ASCII),
            // matching the executor's `cte_results` lookup.
            if is_shadowed_by_cte(name, stmt.with_clause.as_deref(), outer_cte_names) {
                None
            } else {
                database.catalog.get_view(name).and_then(|view| {
                    if crate::privilege_checker::PrivilegeChecker::check_select(database, name)
                        .is_err()
                    {
                        return None;
                    }
                    // Effective correlation name: explicit alias wins, else the
                    // view name as written in the query.
                    let source = alias.as_deref().unwrap_or(name.as_str());
                    // Effective output column names: FROM-clause column aliases
                    // override the view's explicit column list.
                    let effective_aliases: Option<Vec<String>> =
                        column_aliases.clone().or_else(|| view.columns.clone());

                    try_push_into_subquery(
                        where_clause,
                        &view.query,
                        source,
                        effective_aliases.as_deref(),
                    )
                    .map(|new_query| FromClause::Subquery {
                        query: Box::new(new_query),
                        alias: source.to_string(),
                        column_aliases: effective_aliases,
                    })
                })
            }
        }

        _ => None,
    };

    if let Some(from) = new_from {
        stmt.from = Some(from);
    }
    stmt
}

/// True when `name` is bound as a CTE in scope — by the statement's own
/// WITH clause or by an enclosing query (`outer_cte_names`, lowercased).
/// Comparison is ASCII case-insensitive, matching the executor's CTE lookup
/// in `select/scan/table.rs`.
fn is_shadowed_by_cte(
    name: &str,
    with_clause: Option<&[CommonTableExpr]>,
    outer_cte_names: &HashSet<String>,
) -> bool {
    outer_cte_names.contains(&name.to_ascii_lowercase())
        || with_clause
            .is_some_and(|ctes| ctes.iter().any(|cte| cte.name.eq_ignore_ascii_case(name)))
}

/// Attempt to push conjuncts of `where_clause` into `subquery`.
///
/// Returns `Some(rewritten_subquery)` when at least one conjunct was pushed,
/// `None` when nothing is pushable (callers leave the statement unchanged).
fn try_push_into_subquery(
    where_clause: &Expression,
    subquery: &SelectStmt,
    source_name: &str,
    column_aliases: Option<&[String]>,
) -> Option<SelectStmt> {
    // UNION ALL compound subquery/view: push the outer WHERE into each branch
    // so each branch's table scan can use an index (#5723). This is a distinct
    // rewrite from the window-function push-down handled below; it is purely
    // additive (the outer WHERE still applies post-union) so it never changes
    // results, only enables index use.
    if subquery.set_operation.is_some() {
        return try_push_into_union_all_chain(where_clause, subquery, source_name, column_aliases);
    }

    // Inner-query gate: plain SELECT only.
    if subquery.values.is_some()
        || subquery.set_operation.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.distinct
        || subquery.into_table.is_some()
        || subquery.into_variables.is_some()
    {
        return None;
    }

    // Window functions in the subquery's ORDER BY are not visible to
    // collect_resolved_window_specs (it scans the SELECT list); bail out so
    // the coverage check below cannot miss a window.
    if let Some(order_by) = &subquery.order_by {
        if order_by.iter().any(|item| contains_window_function(&item.expr)) {
            return None;
        }
    }

    let specs = crate::select::window::collect_resolved_window_specs(
        &subquery.select_list,
        subquery.window_definitions.as_ref(),
    )
    .ok()?;

    // Scope gate: this pass only targets subqueries containing window
    // functions. (Plain derived tables produce correct results today;
    // generalized predicate push-down is separate work.)
    if specs.is_empty() {
        return None;
    }

    // Every window must have a non-empty PARTITION BY; `OVER ()` makes the
    // whole result one partition, so no non-constant predicate is pushable.
    let partition_lists: Vec<&[Expression]> = specs
        .iter()
        .map(|spec| spec.partition_by.as_deref().filter(|p| !p.is_empty()))
        .collect::<Option<Vec<_>>>()?;

    let output_map = build_output_map(&subquery.select_list, column_aliases)?;

    let ctx =
        PushContext { source_name, output_map: &output_map, partition_lists: &partition_lists };

    let mut pushed: Vec<Expression> = Vec::new();
    for conjunct in flatten_conjuncts(where_clause) {
        if let Some(mapped) = map_conjunct(&conjunct, &ctx) {
            pushed.push(mapped);
        }
    }
    if pushed.is_empty() {
        return None;
    }

    let mut new_subquery = subquery.clone();
    let mut all = Vec::new();
    if let Some(existing) = new_subquery.where_clause.take() {
        all.push(existing);
    }
    all.extend(pushed);
    new_subquery.where_clause = super::combine_with_and(all);
    Some(new_subquery)
}

/// Push the outer WHERE into every branch of a UNION ALL compound
/// subquery/view (#5723).
///
/// The compound `subquery` is the left-most SELECT whose `set_operation`
/// chains the remaining branches via `set_op.right`. The output columns of
/// the whole compound come positionally from the FIRST branch's SELECT list
/// (SQL semantics), optionally renamed by `column_aliases` (the view's
/// declared column list or the FROM-clause column aliases). An outer column
/// reference therefore names the i-th output column; in each branch that
/// column is produced by that branch's i-th SELECT item.
///
/// For each branch we build a per-branch output map (output-column-name →
/// that branch's i-th expression) keyed by the SAME positional output names,
/// then map each outer conjunct through it and AND-append the successfully
/// mapped conjuncts to that branch's WHERE.
///
/// Safety:
/// - UNION ALL only. UNION/INTERSECT/EXCEPT dedup against the full branch contents; filtering a
///   branch first can change which rows survive dedup, so we bail if any operator is not `UNION
///   ALL`.
/// - A trailing LIMIT/OFFSET on the compound (parsed onto the LEFT-most stmt) applies to the whole
///   union; filtering ANY branch changes which rows the LIMIT sees, so we bail the entire chain. A
///   LIMIT/OFFSET on a non-first branch (should not occur with the current parser, but is handled
///   defensively) skips only that branch.
/// - A conjunct is pushed into a branch only when every column it references maps to a bare
///   ColumnRef in that branch's i-th output position; complex expressions / literals are not
///   addressable and are skipped for that branch.
///
/// The transform is additive: the outer WHERE is left in place, so any
/// branch (or conjunct) that could not be pushed is still filtered correctly
/// after the union.
///
/// Returns `Some(rewritten)` if at least one conjunct was pushed into at
/// least one branch, else `None` (caller leaves the statement unchanged).
fn try_push_into_union_all_chain(
    where_clause: &Expression,
    subquery: &SelectStmt,
    source_name: &str,
    column_aliases: Option<&[String]>,
) -> Option<SelectStmt> {
    // Collect references to every branch SELECT (left stmt + each right) and
    // verify every operator is UNION ALL.
    //
    // We work on a clone so we can mutate the per-branch WHERE clauses; the
    // chain is right-nested (`set_op.right` carries the next op), so we walk
    // it after cloning.
    // A trailing LIMIT/OFFSET binds to the whole compound (the parser stores
    // it on the left-most stmt; right branches always have None). Pushing a
    // filter into any branch then changes which rows the global LIMIT sees,
    // so bail the entire chain.
    if subquery.limit.is_some() || subquery.offset.is_some() {
        return None;
    }

    let mut new_subquery = subquery.clone();

    // Positional output names come from the first branch's SELECT list,
    // overridden positionally by the effective column aliases. If we cannot
    // derive a stable name for an output position it is left absent, and any
    // conjunct touching that position simply won't be pushed.
    let output_names = output_column_names(&subquery.select_list, column_aliases)?;

    let conjuncts: Vec<Expression> = flatten_conjuncts(where_clause);

    let mut any_pushed = false;

    // Branch 0: the left statement's own SELECT-level fields.
    if push_into_branch(&mut new_subquery, &output_names, source_name, &conjuncts) {
        any_pushed = true;
    }

    // Remaining branches: walk the set-operation chain. Each `set_op.right`
    // is the next branch; its own `set_operation` (if any) chains further.
    let mut current = new_subquery.set_operation.as_mut();
    while let Some(set_op) = current {
        // UNION ALL only. A non-ALL operator (or a non-UNION op) deduplicates;
        // pushing into a branch can change post-dedup results.
        if !(matches!(set_op.op, vibesql_ast::SetOperator::Union) && set_op.all) {
            return None;
        }
        if push_into_branch(set_op.right.as_mut(), &output_names, source_name, &conjuncts) {
            any_pushed = true;
        }
        current = set_op.right.set_operation.as_mut();
    }

    if any_pushed {
        Some(new_subquery)
    } else {
        None
    }
}

/// AND-append every pushable conjunct to `branch`'s WHERE clause, mapping
/// outer column references (named by `output_names`) through this branch's
/// own SELECT list. Returns true if at least one conjunct was pushed.
///
/// A branch carrying its own LIMIT/OFFSET is skipped entirely (the filter
/// would change which rows the LIMIT observes).
fn push_into_branch(
    branch: &mut SelectStmt,
    output_names: &[Option<String>],
    source_name: &str,
    conjuncts: &[Expression],
) -> bool {
    if branch.limit.is_some() || branch.offset.is_some() {
        return false;
    }

    // Per-branch output map: output-column-name → this branch's i-th SELECT
    // expression. Only bare ColumnRef expressions are addressable (the push
    // must resolve to a real inner column to enable an index scan).
    let mut branch_map: HashMap<String, Expression> = HashMap::new();
    let mut poisoned: Vec<String> = Vec::new();
    for (i, name) in output_names.iter().enumerate() {
        let Some(name) = name else { continue };
        let Some(SelectItem::Expression { expr, .. }) = branch.select_list.get(i) else {
            // Wildcard or missing position: not addressable.
            continue;
        };
        if matches!(expr, Expression::ColumnRef(_)) {
            if branch_map.insert(name.clone(), expr.clone()).is_some() {
                poisoned.push(name.clone());
            }
        }
    }
    for name in poisoned {
        branch_map.remove(&name);
    }

    let ctx = BranchPushContext { source_name, output_map: &branch_map };

    let mut pushed: Vec<Expression> = Vec::new();
    for conjunct in conjuncts {
        if let Some(mapped) = map_branch_conjunct(conjunct, &ctx) {
            pushed.push(mapped);
        }
    }
    if pushed.is_empty() {
        return false;
    }

    let mut all = Vec::new();
    if let Some(existing) = branch.where_clause.take() {
        all.push(existing);
    }
    all.extend(pushed);
    branch.where_clause = super::combine_with_and(all);
    true
}

/// Build the positional output-column names of a compound query: the first
/// branch's SELECT-list names, overridden positionally by `column_aliases`.
/// Returns one entry per output column (absent when no stable name exists,
/// e.g. an unnamed complex expression). Wildcards make positional naming
/// unreliable without the inner schema, so we bail (`None`) entirely.
fn output_column_names(
    select_list: &[SelectItem],
    column_aliases: Option<&[String]>,
) -> Option<Vec<Option<String>>> {
    let mut names: Vec<Option<String>> = Vec::with_capacity(select_list.len());
    for (i, item) in select_list.iter().enumerate() {
        let SelectItem::Expression { expr, alias, .. } = item else {
            return None;
        };
        let name: Option<String> = if let Some(aliases) = column_aliases {
            Some(aliases.get(i)?.to_ascii_lowercase())
        } else if let Some(a) = alias {
            Some(a.to_ascii_lowercase())
        } else if let Expression::ColumnRef(ci) = expr {
            Some(ci.column_canonical().to_ascii_lowercase())
        } else {
            None
        };
        names.push(name);
    }
    Some(names)
}

/// Context for mapping an outer conjunct into one UNION ALL branch. Unlike
/// [`PushContext`] there is no PARTITION BY coverage check — any bare-column
/// mapping is valid for a plain predicate push.
struct BranchPushContext<'a> {
    source_name: &'a str,
    output_map: &'a HashMap<String, Expression>,
}

impl BranchPushContext<'_> {
    fn resolve_column(&self, ci: &vibesql_ast::ColumnIdentifier) -> Option<Expression> {
        if ci.schema_canonical().is_some() {
            return None;
        }
        if let Some(table) = ci.table_canonical() {
            if !table.eq_ignore_ascii_case(self.source_name) {
                return None;
            }
        }
        let inner = self.output_map.get(&ci.column_canonical().to_ascii_lowercase())?;
        Some(inner.clone())
    }
}

/// Map an outer conjunct for pushing into a single UNION ALL branch.
/// Mirrors [`map_conjunct`] but resolves columns via the branch's own output
/// map (no window-partition coverage requirement).
fn map_branch_conjunct(expr: &Expression, ctx: &BranchPushContext) -> Option<Expression> {
    use Expression as E;

    let map_box = |e: &Expression| map_branch_conjunct(e, ctx).map(Box::new);
    let map_vec = |es: &[Expression]| {
        es.iter().map(|e| map_branch_conjunct(e, ctx)).collect::<Option<Vec<_>>>()
    };

    match expr {
        E::Literal(_) | E::CurrentDate | E::CurrentTime { .. } | E::CurrentTimestamp { .. } => {
            if matches!(expr, E::Literal(_)) {
                Some(expr.clone())
            } else {
                None
            }
        }

        E::ColumnRef(ci) => ctx.resolve_column(ci),

        E::BinaryOp { op, left, right } => {
            Some(E::BinaryOp { op: op.clone(), left: map_box(left)?, right: map_box(right)? })
        }

        E::Conjunction(es) => Some(E::Conjunction(map_vec(es)?)),
        E::Disjunction(es) => Some(E::Disjunction(map_vec(es)?)),

        E::UnaryOp { op, expr } => Some(E::UnaryOp { op: op.clone(), expr: map_box(expr)? }),

        E::IsNull { expr, negated } => Some(E::IsNull { expr: map_box(expr)?, negated: *negated }),

        E::IsDistinctFrom { left, right, negated } => Some(E::IsDistinctFrom {
            left: map_box(left)?,
            right: map_box(right)?,
            negated: *negated,
        }),

        E::IsTruthValue { expr, truth_value, negated } => Some(E::IsTruthValue {
            expr: map_box(expr)?,
            truth_value: truth_value.clone(),
            negated: *negated,
        }),

        E::Case { operand, when_clauses, else_result } => {
            let operand = match operand {
                Some(op) => Some(map_box(op)?),
                None => None,
            };
            let when_clauses = when_clauses
                .iter()
                .map(|wc| {
                    Some(vibesql_ast::CaseWhen {
                        conditions: map_vec(&wc.conditions)?,
                        result: map_branch_conjunct(&wc.result, ctx)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            let else_result = match else_result {
                Some(er) => Some(map_box(er)?),
                None => None,
            };
            Some(E::Case { operand, when_clauses, else_result })
        }

        E::InList { expr, values, negated } => {
            Some(E::InList { expr: map_box(expr)?, values: map_vec(values)?, negated: *negated })
        }

        E::Between { expr, low, high, negated, symmetric } => Some(E::Between {
            expr: map_box(expr)?,
            low: map_box(low)?,
            high: map_box(high)?,
            negated: *negated,
            symmetric: *symmetric,
        }),

        E::Cast { expr, data_type } => {
            Some(E::Cast { expr: map_box(expr)?, data_type: data_type.clone() })
        }

        E::Like { expr, pattern, negated, escape } => Some(E::Like {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Glob { expr, pattern, negated, escape } => Some(E::Glob {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Collate { expr, collation } => {
            Some(E::Collate { expr: map_box(expr)?, collation: collation.clone() })
        }

        E::Function { name, args, character_unit } => {
            if is_volatile_function(name.canonical()) {
                return None;
            }
            Some(E::Function {
                name: name.clone(),
                args: map_vec(args)?,
                character_unit: character_unit.clone(),
            })
        }

        _ => None,
    }
}

/// Map from subquery output column name (case-folded) to the inner
/// expression that produces it. Names that are duplicated (ambiguous) or
/// produced by expressions we cannot address are absent.
fn build_output_map(
    select_list: &[SelectItem],
    column_aliases: Option<&[String]>,
) -> Option<HashMap<String, Expression>> {
    let mut map: HashMap<String, Expression> = HashMap::new();
    let mut poisoned: Vec<String> = Vec::new();

    for (i, item) in select_list.iter().enumerate() {
        let SelectItem::Expression { expr, alias, .. } = item else {
            // Wildcards make positional/name mapping unreliable without the
            // inner schema; bail out entirely.
            return None;
        };

        let name: Option<String> = if let Some(aliases) = column_aliases {
            // Explicit column list renames positionally; a mismatch in
            // length means we cannot trust the mapping.
            Some(aliases.get(i)?.to_ascii_lowercase())
        } else if let Some(a) = alias {
            Some(a.to_ascii_lowercase())
        } else if let Expression::ColumnRef(ci) = expr {
            Some(ci.column_canonical().to_ascii_lowercase())
        } else {
            // Unnamed complex expression (e.g. the window function itself):
            // not addressable by a simple outer column reference.
            None
        };

        if let Some(name) = name {
            if map.insert(name.clone(), expr.clone()).is_some() {
                poisoned.push(name);
            }
        }
    }

    for name in poisoned {
        map.remove(&name);
    }
    Some(map)
}

struct PushContext<'a> {
    /// The correlation name of the subquery/view in the outer FROM clause.
    source_name: &'a str,
    /// Output column name → inner expression.
    output_map: &'a HashMap<String, Expression>,
    /// PARTITION BY expressions of every window function in the subquery.
    partition_lists: &'a [&'a [Expression]],
}

impl PushContext<'_> {
    /// Resolve an outer column reference to a pushable inner expression.
    ///
    /// Requirements:
    /// - the table qualifier (if any) names the subquery source
    /// - the column maps to a *bare column* of the inner query
    /// - that inner column appears in the PARTITION BY of every window
    fn resolve_column(&self, ci: &vibesql_ast::ColumnIdentifier) -> Option<Expression> {
        if ci.schema_canonical().is_some() {
            return None;
        }
        if let Some(table) = ci.table_canonical() {
            if !table.eq_ignore_ascii_case(self.source_name) {
                return None;
            }
        }

        let inner = self.output_map.get(&ci.column_canonical().to_ascii_lowercase())?;
        let Expression::ColumnRef(inner_ci) = inner else {
            return None;
        };

        let covered = self.partition_lists.iter().all(|list| {
            list.iter().any(|p| match p {
                Expression::ColumnRef(pci) => {
                    pci.column_canonical().eq_ignore_ascii_case(inner_ci.column_canonical())
                        && match (pci.table_canonical(), inner_ci.table_canonical()) {
                            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                            _ => true,
                        }
                }
                _ => false,
            })
        });
        if !covered {
            return None;
        }

        Some(inner.clone())
    }
}

/// Reject functions that are (or may be) non-deterministic. The
/// classification is centralized in [`vibesql_ast::volatility`] (also
/// used by the replication freeze pass in `vibesql-consensus`); push-down
/// uses the coarse union — any possibly-volatile function blocks the
/// rewrite.
fn is_volatile_function(canonical_name: &str) -> bool {
    vibesql_ast::volatility::is_volatile_function(canonical_name)
}

/// Recursively rewrite a conjunct for pushing: outer column references are
/// replaced by the inner expressions they map to. Returns `None` if the
/// conjunct contains anything unsafe to push (subqueries, window/aggregate
/// functions, placeholders, volatile functions, unmappable columns, ...).
///
/// Only a whitelist of expression forms is traversed; everything else is
/// conservatively rejected.
fn map_conjunct(expr: &Expression, ctx: &PushContext) -> Option<Expression> {
    use Expression as E;

    let map_box = |e: &Expression| map_conjunct(e, ctx).map(Box::new);
    let map_vec =
        |es: &[Expression]| es.iter().map(|e| map_conjunct(e, ctx)).collect::<Option<Vec<_>>>();

    match expr {
        E::Literal(_) | E::CurrentDate | E::CurrentTime { .. } | E::CurrentTimestamp { .. } => {
            // CURRENT_* are non-deterministic; reject them. Literals pass.
            if matches!(expr, E::Literal(_)) {
                Some(expr.clone())
            } else {
                None
            }
        }

        E::ColumnRef(ci) => ctx.resolve_column(ci),

        E::BinaryOp { op, left, right } => {
            Some(E::BinaryOp { op: op.clone(), left: map_box(left)?, right: map_box(right)? })
        }

        E::Conjunction(es) => Some(E::Conjunction(map_vec(es)?)),
        E::Disjunction(es) => Some(E::Disjunction(map_vec(es)?)),

        E::UnaryOp { op, expr } => Some(E::UnaryOp { op: op.clone(), expr: map_box(expr)? }),

        E::IsNull { expr, negated } => Some(E::IsNull { expr: map_box(expr)?, negated: *negated }),

        E::IsDistinctFrom { left, right, negated } => Some(E::IsDistinctFrom {
            left: map_box(left)?,
            right: map_box(right)?,
            negated: *negated,
        }),

        E::IsTruthValue { expr, truth_value, negated } => Some(E::IsTruthValue {
            expr: map_box(expr)?,
            truth_value: truth_value.clone(),
            negated: *negated,
        }),

        E::Case { operand, when_clauses, else_result } => {
            let operand = match operand {
                Some(op) => Some(map_box(op)?),
                None => None,
            };
            let when_clauses = when_clauses
                .iter()
                .map(|wc| {
                    Some(vibesql_ast::CaseWhen {
                        conditions: map_vec(&wc.conditions)?,
                        result: map_conjunct(&wc.result, ctx)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            let else_result = match else_result {
                Some(er) => Some(map_box(er)?),
                None => None,
            };
            Some(E::Case { operand, when_clauses, else_result })
        }

        E::InList { expr, values, negated } => {
            Some(E::InList { expr: map_box(expr)?, values: map_vec(values)?, negated: *negated })
        }

        E::Between { expr, low, high, negated, symmetric } => Some(E::Between {
            expr: map_box(expr)?,
            low: map_box(low)?,
            high: map_box(high)?,
            negated: *negated,
            symmetric: *symmetric,
        }),

        E::Cast { expr, data_type } => {
            Some(E::Cast { expr: map_box(expr)?, data_type: data_type.clone() })
        }

        E::Like { expr, pattern, negated, escape } => Some(E::Like {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Glob { expr, pattern, negated, escape } => Some(E::Glob {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Collate { expr, collation } => {
            Some(E::Collate { expr: map_box(expr)?, collation: collation.clone() })
        }

        E::Function { name, args, character_unit } => {
            if is_volatile_function(name.canonical()) {
                return None;
            }
            Some(E::Function {
                name: name.clone(),
                args: map_vec(args)?,
                character_unit: character_unit.clone(),
            })
        }

        // Everything else — subqueries, aggregate/window functions,
        // placeholders, sequence/session/pseudo variables, wildcards,
        // MATCH ... AGAINST, etc. — is unsafe or pointless to push.
        _ => None,
    }
}

/// Detect window functions anywhere inside an expression.
fn contains_window_function(expr: &Expression) -> bool {
    struct Finder {
        found: bool,
    }
    impl vibesql_ast::visitor::ExpressionVisitor for Finder {
        fn pre_visit_expression(&mut self, expr: &Expression) -> vibesql_ast::visitor::VisitResult {
            if matches!(expr, Expression::WindowFunction { .. }) {
                self.found = true;
                return vibesql_ast::visitor::VisitResult::Stop;
            }
            vibesql_ast::visitor::VisitResult::Continue
        }
    }
    let mut finder = Finder { found: false };
    vibesql_ast::visitor::walk_expression(&mut finder, expr);
    finder.found
}

#[cfg(test)]
mod tests {
    use vibesql_ast::Statement;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use super::*;

    fn run_ddl(db: &mut Database, sql: &str) {
        let stmt = Parser::parse_sql(sql).expect("parse failed");
        match stmt {
            Statement::CreateTable(s) => {
                crate::CreateTableExecutor::execute(&s, db).unwrap();
            }
            Statement::CreateIndex(s) => {
                crate::CreateIndexExecutor::execute(&s, db).unwrap();
            }
            Statement::CreateView(s) => {
                crate::advanced_objects::execute_create_view(&s, db).unwrap();
            }
            Statement::Insert(s) => {
                crate::InsertExecutor::execute(db, &s).unwrap();
            }
            other => panic!("unsupported DDL in test: {:?}", other),
        }
    }

    fn parse_select(sql: &str) -> SelectStmt {
        match Parser::parse_sql(sql).expect("parse failed") {
            Statement::Select(s) => *s,
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    /// Database with the windowpushd.test section-1 schema.
    fn setup_db() -> Database {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER)");
        run_ddl(&mut db, "CREATE INDEX i1 ON t1(grp_id)");
        run_ddl(
            &mut db,
            "CREATE VIEW lll AS SELECT row_number() OVER (PARTITION BY grp_id), grp_id, id FROM t1",
        );
        db
    }

    /// Extract the inner subquery's WHERE clause after the rewrite (None if
    /// the FROM clause is not a subquery or has no inner WHERE).
    fn inner_where(stmt: &SelectStmt) -> Option<Expression> {
        match &stmt.from {
            Some(FromClause::Subquery { query, .. }) => query.where_clause.clone(),
            _ => None,
        }
    }

    fn no_outer_ctes() -> HashSet<String> {
        HashSet::new()
    }

    fn rewrite(db: &Database, sql: &str) -> SelectStmt {
        push_where_into_window_subqueries(parse_select(sql), db, &no_outer_ctes())
    }

    // ----------------------------------------------------------------
    // Positive cases
    // ----------------------------------------------------------------

    #[test]
    fn pushes_equality_on_partition_column_into_derived_table() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT grp_id, id, row_number() OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE grp_id = 2",
        );
        let pushed = inner_where(&out).expect("predicate should be pushed");
        assert!(format!("{:?}", pushed).contains("grp_id"), "pushed: {:?}", pushed);
        // Outer WHERE is preserved (push copies, never removes).
        assert!(out.where_clause.is_some());
    }

    #[test]
    fn pushes_into_view_reference() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = 2");
        // View is expanded into a derived table carrying the predicate.
        match &out.from {
            Some(FromClause::Subquery { query, alias, .. }) => {
                assert_eq!(alias, "lll");
                assert!(query.where_clause.is_some(), "inner WHERE missing");
            }
            other => panic!("expected Subquery FROM, got {:?}", other),
        }
    }

    #[test]
    fn pushes_in_list_on_partition_column() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id IN (1, 2)");
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_collate_wrapped_predicate() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = '2' COLLATE nocase");
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_through_select_alias() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT g FROM (SELECT grp_id AS g, row_number() OVER (PARTITION BY grp_id) AS rn FROM t1) AS v \
             WHERE g = 1",
        );
        let pushed = inner_where(&out).expect("aliased predicate should be pushed");
        // The pushed predicate must reference the INNER column name.
        assert!(format!("{:?}", pushed).contains("grp_id"), "pushed: {:?}", pushed);
    }

    #[test]
    fn pushes_when_covered_by_all_windows() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                row_number() OVER (PARTITION BY grp_id), \
                rank() OVER (PARTITION BY grp_id, id ORDER BY id) \
             FROM t1) AS v WHERE grp_id = 2",
        );
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_only_eligible_conjunct() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = 2 AND id > 3");
        let pushed = inner_where(&out).expect("grp_id conjunct should be pushed");
        let s = format!("{:?}", pushed);
        assert!(s.contains("grp_id"));
        assert!(!s.contains("\"id\""), "id predicate must NOT be pushed: {}", s);
    }

    // ----------------------------------------------------------------
    // Negative cases — the rewrite must NOT fire
    // ----------------------------------------------------------------

    fn assert_unchanged(db: &Database, sql: &str) {
        let stmt = parse_select(sql);
        let out = push_where_into_window_subqueries(stmt.clone(), db, &no_outer_ctes());
        assert_eq!(stmt, out, "statement should be unchanged");
    }

    #[test]
    fn does_not_push_non_partition_column() {
        let db = setup_db();
        // `id` is not in the PARTITION BY list.
        assert_unchanged(&db, "SELECT * FROM lll WHERE id = 5");
    }

    #[test]
    fn does_not_push_when_any_window_lacks_partition() {
        let db = setup_db();
        // Second window is OVER () — one big partition; nothing is pushable
        // (windowpushd.test v2).
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                max(id) OVER (PARTITION BY grp_id), \
                row_number() OVER () \
             FROM t1) AS v WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_when_window_partitions_differ_and_predicate_uncovered() {
        let db = setup_db();
        // Predicate column is in the first window's PARTITION BY only.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                max(id) OVER (PARTITION BY grp_id), \
                row_number() OVER (PARTITION BY id) \
             FROM t1) AS v WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_volatile_predicate() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll WHERE grp_id = random()");
    }

    #[test]
    fn does_not_push_subquery_predicate() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll WHERE grp_id IN (SELECT id FROM t1)");
    }

    #[test]
    fn does_not_push_into_subquery_with_limit() {
        let db = setup_db();
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, row_number() OVER (PARTITION BY grp_id) FROM t1 LIMIT 5) AS v \
             WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_into_plain_subquery_without_windows() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM (SELECT grp_id, id FROM t1) AS v WHERE grp_id = 2");
    }

    #[test]
    fn does_not_fire_on_join_from_clause() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll, t1 WHERE grp_id = 2");
    }

    #[test]
    fn does_not_push_predicate_qualified_with_other_table() {
        let db = setup_db();
        // Qualifier names something other than the derived table.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, row_number() OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE t1.grp_id = 2",
        );
    }

    #[test]
    fn does_not_expand_view_shadowed_by_with_clause_cte() {
        let db = setup_db();
        // The statement's own WITH clause binds `lll`, which shadows the
        // view of the same name; expanding the view would redirect the
        // query (judge regression case 1 on PR #5349).
        assert_unchanged(
            &db,
            "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
             SELECT * FROM lll WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_expand_view_shadowed_by_outer_cte() {
        let db = setup_db();
        // An enclosing query's CTE context binds `lll` (e.g. the view name
        // referenced inside a derived table whose outer statement declares
        // the CTE — judge regression case 2 on PR #5349). Names are
        // compared ASCII case-insensitively, matching the executor's CTE
        // lookup.
        let outer: HashSet<String> = ["lll".to_string()].into_iter().collect();
        for sql in ["SELECT * FROM lll WHERE grp_id = 2", "SELECT * FROM LLL WHERE grp_id = 2"] {
            let stmt = parse_select(sql);
            let out = push_where_into_window_subqueries(stmt.clone(), &db, &outer);
            assert_eq!(stmt, out, "statement should be unchanged for: {}", sql);
        }
    }

    // ----------------------------------------------------------------
    // Correctness parity — results identical with and without the rewrite
    // ----------------------------------------------------------------

    fn populate(db: &mut Database) {
        run_ddl(
            db,
            "INSERT INTO t1 VALUES \
              (1, 2), (2, 3), (3, 3), (4, 1), (5, 1), \
              (6, 1), (7, 1), (8, 1), (9, 3), (10, 3), \
              (11, 2), (12, 3), (13, 3), (14, 2), (15, 1), \
              (16, 2), (17, 1), (18, 2), (19, 3), (20, 2)",
        );
    }

    fn execute_rows(db: &Database, stmt: &SelectStmt) -> Vec<Vec<vibesql_types::SqlValue>> {
        crate::select::SelectExecutor::new(db)
            .execute(stmt)
            .unwrap()
            .into_iter()
            .map(|r| r.values.to_vec())
            .collect()
    }

    #[test]
    fn parity_view_equality_predicate() {
        let mut db = setup_db();
        populate(&mut db);
        let stmt = parse_select("SELECT * FROM lll WHERE grp_id = 2");

        // Executor output (rewrite enabled inside execute()).
        let executed = execute_rows(&db, &stmt);

        // windowpushd.test 1.3 expected rows: row_number, grp_id, id.
        let expected: Vec<Vec<i64>> = vec![
            vec![1, 2, 1],
            vec![2, 2, 11],
            vec![3, 2, 14],
            vec![4, 2, 16],
            vec![5, 2, 18],
            vec![6, 2, 20],
        ];
        assert_eq!(executed.len(), expected.len(), "row count: {:?}", executed);
        for (row, exp) in executed.iter().zip(&expected) {
            let got: Vec<i64> = row
                .iter()
                .map(|v| match v {
                    vibesql_types::SqlValue::Integer(i) => *i,
                    vibesql_types::SqlValue::Bigint(i) => *i,
                    other => panic!("unexpected value {:?}", other),
                })
                .collect();
            assert_eq!(&got, exp);
        }
    }

    /// Convert executed rows to i64 matrices for compact assertions.
    fn rows_as_i64(rows: &[Vec<vibesql_types::SqlValue>]) -> Vec<Vec<i64>> {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        vibesql_types::SqlValue::Integer(i) => *i,
                        vibesql_types::SqlValue::Bigint(i) => *i,
                        other => panic!("unexpected value {:?}", other),
                    })
                    .collect()
            })
            .collect()
    }

    /// Judge regression case 1 (PR #5349): a same-statement CTE shadowing a
    /// window view must win over the view. sqlite3 returns the CTE row.
    #[test]
    fn execute_cte_shadowing_view_returns_cte_rows() {
        let mut db = setup_db();
        run_ddl(&mut db, "INSERT INTO t1 VALUES (1, 2), (2, 3), (3, 2)");

        let stmt = parse_select(
            "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
             SELECT * FROM lll WHERE grp_id = 2",
        );
        let executed = execute_rows(&db, &stmt);
        assert_eq!(rows_as_i64(&executed), vec![vec![99, 2, 100]], "CTE row expected, not view");
    }

    /// Judge regression case 2 (PR #5349): the shadowing CTE referenced via
    /// a derived table. The inner SELECT is executed recursively with the
    /// outer CTE context set, so the pass must also see outer CTE names.
    #[test]
    fn execute_outer_cte_shadowing_view_in_derived_table_returns_cte_rows() {
        let mut db = setup_db();
        run_ddl(&mut db, "INSERT INTO t1 VALUES (1, 2), (2, 3), (3, 2)");

        let stmt = parse_select(
            "WITH lll AS (SELECT 99 AS rn, 2 AS grp_id, 100 AS id) \
             SELECT * FROM (SELECT * FROM lll WHERE grp_id = 2) v",
        );
        let executed = execute_rows(&db, &stmt);
        assert_eq!(rows_as_i64(&executed), vec![vec![99, 2, 100]], "CTE row expected, not view");
    }

    #[test]
    fn parity_rewritten_vs_unrewritten_ast() {
        let mut db = setup_db();
        populate(&mut db);

        for sql in [
            "SELECT * FROM lll WHERE grp_id = 2",
            "SELECT * FROM lll WHERE grp_id IN (1, 3)",
            "SELECT * FROM (SELECT grp_id, id, sum(id) OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE grp_id > 1",
        ] {
            let stmt = parse_select(sql);
            let rewritten = push_where_into_window_subqueries(stmt.clone(), &db, &no_outer_ctes());
            assert_ne!(stmt, rewritten, "rewrite should fire for: {}", sql);

            // Execute the REWRITTEN statement (executor will not rewrite the
            // outer FROM again since the predicate is already pushed — but
            // even if it did, the transform is idempotent in effect) and the
            // original; row sets must be identical and in the same order.
            let base = execute_rows(&db, &stmt);
            let opt = execute_rows(&db, &rewritten);
            assert_eq!(base, opt, "results differ for: {}", sql);
        }
    }

    // ----------------------------------------------------------------
    // UNION ALL compound-branch push-down (#5723)
    // ----------------------------------------------------------------

    /// Database with a 2-branch UNION ALL view over indexed text columns,
    /// mirroring select9 section 5 (t51/t52 with indexes t51x/t52x).
    fn setup_union_db() -> Database {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE t51(x TEXT, y TEXT)");
        run_ddl(&mut db, "CREATE TABLE t52(x TEXT, y TEXT)");
        run_ddl(&mut db, "CREATE INDEX t51x ON t51(x)");
        run_ddl(&mut db, "CREATE INDEX t52x ON t52(x)");
        run_ddl(&mut db, "INSERT INTO t51 VALUES('12345','a'),('99','b')");
        run_ddl(&mut db, "INSERT INTO t52 VALUES('12345','c'),('77','d')");
        run_ddl(&mut db, "CREATE VIEW v5 AS SELECT x, y FROM t51 UNION ALL SELECT x, y FROM t52");
        db
    }

    /// Collect each branch's WHERE clause (debug-formatted) from a rewritten
    /// compound subquery FROM clause: index 0 is the left-most branch, then
    /// each `set_op.right` in chain order.
    fn branch_wheres(stmt: &SelectStmt) -> Vec<Option<String>> {
        let Some(FromClause::Subquery { query, .. }) = &stmt.from else {
            return Vec::new();
        };
        let mut out = Vec::new();
        out.push(query.where_clause.as_ref().map(|w| format!("{:?}", w)));
        let mut cur = query.set_operation.as_ref();
        while let Some(set_op) = cur {
            out.push(set_op.right.where_clause.as_ref().map(|w| format!("{:?}", w)));
            cur = set_op.right.set_operation.as_ref();
        }
        out
    }

    #[test]
    fn union_all_pushes_predicate_into_every_branch() {
        let db = setup_union_db();
        let out = rewrite(&db, "SELECT * FROM v5 WHERE x = '12345'");
        let wheres = branch_wheres(&out);
        assert_eq!(wheres.len(), 2, "expected 2 branches, got {:?}", wheres);
        for (i, w) in wheres.iter().enumerate() {
            let w = w.as_ref().unwrap_or_else(|| panic!("branch {i} missing WHERE: {wheres:?}"));
            assert!(w.contains("\"x\"") || w.contains("x"), "branch {i} WHERE: {w}");
            assert!(w.contains("12345"), "branch {i} WHERE: {w}");
        }
        // Outer WHERE is preserved (push is additive).
        assert!(out.where_clause.is_some());
    }

    #[test]
    fn union_all_three_branch_chain_pushes_all() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE c(x INTEGER)");
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c) v \
             WHERE x = 5",
        );
        let wheres = branch_wheres(&out);
        assert_eq!(wheres.len(), 3, "expected 3 branches, got {:?}", wheres);
        assert!(wheres.iter().all(|w| w.is_some()), "all branches pushed: {:?}", wheres);
    }

    #[test]
    fn dedup_union_does_not_push() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(x INTEGER)");
        // UNION (dedup) — pushing could change which rows survive dedup.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT x FROM a UNION SELECT x FROM b) v WHERE x = 5",
        );
    }

    #[test]
    fn intersect_and_except_do_not_push() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(x INTEGER)");
        for sql in [
            "SELECT * FROM (SELECT x FROM a INTERSECT SELECT x FROM b) v WHERE x = 5",
            "SELECT * FROM (SELECT x FROM a EXCEPT SELECT x FROM b) v WHERE x = 5",
        ] {
            assert_unchanged(&db, sql);
        }
    }

    #[test]
    fn union_all_with_trailing_limit_does_not_push() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(x INTEGER)");
        // Trailing LIMIT binds to the whole compound; filtering any branch
        // changes which rows the LIMIT observes.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT x FROM a UNION ALL SELECT x FROM b LIMIT 1) v WHERE x = 5",
        );
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT x FROM a UNION ALL SELECT x FROM b LIMIT 1 OFFSET 2) v \
             WHERE x = 5",
        );
    }

    #[test]
    fn union_all_skips_expression_column_branch_only() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(z INTEGER)");
        // Branch 2 produces a literal (999) in the predicate's output column,
        // which is not addressable: only branch 1 receives the pushed filter.
        let out =
            rewrite(&db, "SELECT * FROM (SELECT x FROM a UNION ALL SELECT 999) v WHERE x = 5");
        let wheres = branch_wheres(&out);
        assert_eq!(wheres.len(), 2, "got {:?}", wheres);
        assert!(wheres[0].is_some(), "branch 0 (bare column) should be pushed: {:?}", wheres);
        assert!(wheres[1].is_none(), "branch 1 (literal column) must NOT be pushed: {:?}", wheres);
    }

    #[test]
    fn union_all_and_appends_to_existing_branch_where() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(x INTEGER, k INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(x INTEGER, k INTEGER)");
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT x, k FROM a WHERE k > 0 UNION ALL SELECT x, k FROM b WHERE k < 9) v \
             WHERE x = 5",
        );
        let wheres = branch_wheres(&out);
        // Each branch keeps its original predicate AND the pushed x=5.
        let b0 = wheres[0].as_ref().unwrap();
        assert!(b0.contains("\"k\"") && b0.contains("\"x\""), "branch 0: {b0}");
        let b1 = wheres[1].as_ref().unwrap();
        assert!(b1.contains("\"k\"") && b1.contains("\"x\""), "branch 1: {b1}");
    }

    #[test]
    fn union_all_maps_through_view_column_aliases() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE a(p INTEGER)");
        run_ddl(&mut db, "CREATE TABLE b(q INTEGER)");
        run_ddl(&mut db, "CREATE VIEW va(col) AS SELECT p FROM a UNION ALL SELECT q FROM b");
        // Outer `col` must map through the view's column list to p / q.
        let out = rewrite(&db, "SELECT * FROM va WHERE col = 7");
        let wheres = branch_wheres(&out);
        assert_eq!(wheres.len(), 2, "got {:?}", wheres);
        let b0 = wheres[0].as_ref().expect("branch 0");
        assert!(b0.contains("\"p\""), "branch 0 should reference p: {b0}");
        let b1 = wheres[1].as_ref().expect("branch 1");
        assert!(b1.contains("\"q\""), "branch 1 should reference q: {b1}");
    }

    // --- result-correctness parity (incl. the #5749 affinity case) ---

    fn rows_as_strings(rows: &[Vec<vibesql_types::SqlValue>]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        vibesql_types::SqlValue::Character(s)
                        | vibesql_types::SqlValue::Varchar(s) => s.as_str().to_string(),
                        vibesql_types::SqlValue::Integer(i) => i.to_string(),
                        vibesql_types::SqlValue::Bigint(i) => i.to_string(),
                        vibesql_types::SqlValue::Null => "NULL".to_string(),
                        other => format!("{:?}", other),
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn union_all_results_unchanged_text_predicate() {
        let db = setup_union_db();
        let stmt = parse_select("SELECT * FROM v5 WHERE x = '12345' ORDER BY y");
        let executed = rows_as_strings(&execute_rows(&db, &stmt));
        assert_eq!(
            executed,
            vec![
                vec!["12345".to_string(), "a".to_string()],
                vec!["12345".to_string(), "c".to_string()]
            ],
            "got {executed:?}"
        );
    }

    /// The #5749 affinity case: a TEXT-literal predicate over a UNION ALL
    /// derived table whose branch column has numeric affinity must still
    /// return the matching row. Before #5749 the pushed branch WHERE lost
    /// affinity and returned zero rows.
    #[test]
    fn union_all_text_literal_vs_numeric_branch_affinity() {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE t1(a INTEGER)");
        run_ddl(&mut db, "CREATE TABLE t2(d INTEGER)");
        run_ddl(&mut db, "INSERT INTO t1 VALUES(14),(15),(16)");
        run_ddl(&mut db, "INSERT INTO t2 VALUES(20),(21)");

        // Derived table executes the rewritten branch WHERE directly.
        let stmt = parse_select(
            "SELECT * FROM (SELECT a FROM t1 UNION ALL SELECT d FROM t2) AS v WHERE a = '14'",
        );
        let executed = rows_as_i64(&execute_rows(&db, &stmt));
        assert_eq!(
            executed,
            vec![vec![14]],
            "affinity-preserving result expected, got {executed:?}"
        );

        // Curator's canonical mixed-literal compound branch case.
        let stmt2 = parse_select(
            "SELECT * FROM (SELECT a FROM t1 UNION ALL SELECT 999) AS v WHERE a = '14'",
        );
        let executed2 = rows_as_i64(&execute_rows(&db, &stmt2));
        assert_eq!(executed2, vec![vec![14]], "got {executed2:?}");
    }

    /// Executing the AST-rewritten compound and the original must produce the
    /// same rows (push-down is value-preserving).
    #[test]
    fn union_all_parity_rewritten_vs_original() {
        let db = setup_union_db();
        for sql in [
            "SELECT * FROM v5 WHERE x = '12345' ORDER BY y",
            "SELECT * FROM v5 WHERE x = '99' OR x = '77' ORDER BY y",
            "SELECT * FROM (SELECT x, y FROM t51 UNION ALL SELECT x, y FROM t52) v \
             WHERE x = '12345' ORDER BY y",
        ] {
            let stmt = parse_select(sql);
            let rewritten = push_where_into_window_subqueries(stmt.clone(), &db, &no_outer_ctes());
            assert_ne!(stmt, rewritten, "rewrite should fire for: {sql}");
            let base = rows_as_strings(&execute_rows(&db, &stmt));
            let opt = rows_as_strings(&execute_rows(&db, &rewritten));
            assert_eq!(base, opt, "results differ for: {sql}");
        }
    }
}
