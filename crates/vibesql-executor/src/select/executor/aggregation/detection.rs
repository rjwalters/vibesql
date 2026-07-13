//! Aggregate detection helpers for SelectExecutor

use std::collections::HashSet;

use super::super::builder::SelectExecutor;

/// Check whether an expression contains a *bare* (FROM-less) scalar subquery
/// whose body has an aggregate referencing an outer column, OR a FROM-bearing
/// scalar subquery whose body contains a nested aggregate that references a
/// column outside of the inner FROM scope chain. Either case triggers
/// SQLite's implicit-outer-aggregate-collapse semantics (#5104, #5135).
///
/// "Outer column" inside a bare subquery means *any* column reference (since
/// the subquery has no tables of its own). For FROM-bearing subqueries we use
/// scope-aware analysis (see `scope_chain_contains_outer_correlated_aggregate`)
/// so that a column reference inside an aggregate is identified as outer iff
/// no enclosing inner FROM scope defines it.
fn expression_contains_outer_aggregate_collapse(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ScalarSubquery(stmt) => subquery_stmt_triggers_collapse(stmt, database),
        // Recurse into compound expressions.
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_outer_aggregate_collapse(left, database)
                || expression_contains_outer_aggregate_collapse(right, database)
        }
        Expression::UnaryOp { expr, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
        }
        Expression::Cast { expr, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
        }
        Expression::IsNull { expr, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
                || expression_contains_outer_aggregate_collapse(pattern, database)
        }
        Expression::Between { expr, low, high, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
                || expression_contains_outer_aggregate_collapse(low, database)
                || expression_contains_outer_aggregate_collapse(high, database)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
                || values.iter().any(|v| expression_contains_outer_aggregate_collapse(v, database))
        }
        // IN with subquery: the collapse trigger can hide in the LHS *or* in
        // the IN-subquery itself (window1.test 44.x:
        // `SELECT (0,0) IN (SELECT MIN(c0), NTILE(1) OVER()) FROM t0`).
        // The FROM-less IN subquery's MIN(c0) references an outer column, so
        // the outer query collapses to a single-row implicit aggregate.
        // (#5232)
        Expression::In { expr, subquery, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
                || subquery_stmt_triggers_collapse(subquery, database)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand
                .as_ref()
                .is_some_and(|e| expression_contains_outer_aggregate_collapse(e, database))
                || when_clauses.iter().any(|w| {
                    w.conditions
                        .iter()
                        .any(|c| expression_contains_outer_aggregate_collapse(c, database))
                        || expression_contains_outer_aggregate_collapse(&w.result, database)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expression_contains_outer_aggregate_collapse(e, database))
        }
        Expression::Function { args, .. } => {
            args.iter().any(|a| expression_contains_outer_aggregate_collapse(a, database))
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            // The collapse pattern only triggers from a bare scalar subquery,
            // so an aggregate at this level is the *outer* aggregate (already
            // handled by `has_aggregates`). We only recurse to find subqueries
            // that themselves trigger the pattern (e.g. `min(...((SELECT avg(a))))`).
            args.iter().any(|a| expression_contains_outer_aggregate_collapse(a, database))
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_contains_outer_aggregate_collapse(f, database))
                || order_by.as_ref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|i| expression_contains_outer_aggregate_collapse(&i.expr, database))
                })
        }
        Expression::WindowFunction { function, over, .. } => {
            // Window function: check the function's args AND the OVER clause's
            // PARTITION BY / ORDER BY / frame, since the example in #5104
            // (window1.test 57.3) places the collapse-trigger inside the
            // ORDER BY of a window function.
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            if args.iter().any(|a| expression_contains_outer_aggregate_collapse(a, database)) {
                return true;
            }
            if let Some(partition_by) = &over.partition_by {
                if partition_by
                    .iter()
                    .any(|p| expression_contains_outer_aggregate_collapse(p, database))
                {
                    return true;
                }
            }
            if let Some(order_by) = &over.order_by {
                if order_by
                    .iter()
                    .any(|item| expression_contains_outer_aggregate_collapse(&item.expr, database))
                {
                    return true;
                }
            }
            false
        }
        Expression::QuantifiedComparison { expr, .. } => {
            expression_contains_outer_aggregate_collapse(expr, database)
        }
        // EXISTS has its own scope — don't recurse into it.
        // Other leaf expressions can't contain a scalar subquery.
        _ => false,
    }
}

/// Check whether a subquery (scalar or IN) triggers SQLite's
/// implicit-outer-aggregate-collapse semantics.
///
/// Bare (FROM-less) subquery: an aggregate in the projection that references
/// a column is necessarily outer-correlated (no inner tables) → triggers
/// collapse.
///
/// FROM-bearing subquery: walk the SELECT list, FROM, WHERE, HAVING, and
/// ORDER BY of the inner query, tracking the chain of inner FROM scopes. If
/// we find an aggregate whose column reference does not resolve to any inner
/// scope along the chain, it is outer-correlated to the surrounding query →
/// triggers collapse. (#5135 / window1.test 57.3.)
fn subquery_stmt_triggers_collapse(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> bool {
    if stmt.from.is_none() {
        for item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr: inner, .. } = item {
                if expr_has_column_referencing_aggregate(inner) {
                    return true;
                }
                // Recurse: nested scalar subqueries.
                if expression_contains_outer_aggregate_collapse(inner, database) {
                    return true;
                }
            }
        }
        return false;
    }

    // The scope chain is initialized with this subquery's own FROM scope;
    // nested subqueries extend the chain with their own FROM scopes when
    // descending.
    scope_chain_contains_outer_correlated_aggregate(stmt, &[], database)
}

// ------------------------------------------------------------------------
// Scope-aware walker for FROM-bearing subqueries (#5135)
// ------------------------------------------------------------------------

/// Compute the set of (lowercased) column names introduced by a FROM clause.
/// Used to determine which columns are "inner" vs "outer" relative to a
/// surrounding scalar-subquery context. Returns None if the FROM clause shape
/// is too dynamic to resolve statically (e.g. CTE references) — in that case
/// callers conservatively treat the inner scope as "everything", suppressing
/// false positives at the cost of missing some true positives.
fn columns_from_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Option<HashSet<String>> {
    use vibesql_ast::FromClause;

    let mut out = HashSet::new();
    match from {
        FromClause::Table { name, .. } => {
            // Catalog lookup: get column names from the table schema.
            // If the table doesn't exist, treat as unresolved (None) so we
            // don't generate spurious false positives for a query that will
            // error out anyway.
            let table = database.get_table(name)?;
            for col in &table.schema.columns {
                out.insert(col.name.to_ascii_lowercase());
            }
            Some(out)
        }
        FromClause::Join { left, right, .. } => {
            let l = columns_from_from_clause(left, database)?;
            let r = columns_from_from_clause(right, database)?;
            out.extend(l);
            out.extend(r);
            Some(out)
        }
        FromClause::Subquery { query, column_aliases, .. } => {
            // A derived table's columns come from explicit column_aliases
            // (if provided) or from the SELECT list of the inner query
            // (each item's alias, or the underlying column name).
            if let Some(aliases) = column_aliases {
                for a in aliases {
                    out.insert(a.to_ascii_lowercase());
                }
                return Some(out);
            }
            for item in &query.select_list {
                if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = item {
                    if let Some(a) = alias {
                        out.insert(a.to_ascii_lowercase());
                    } else if let vibesql_ast::Expression::ColumnRef(col) = expr {
                        out.insert(col.column_canonical().to_ascii_lowercase());
                    } else {
                        // Anonymous expression — best-effort: skip. Falling
                        // through means we may miss inclusion, but the only
                        // consequence is potentially identifying a column as
                        // outer when it could resolve here. To stay
                        // conservative (no false positives), bail out.
                        return None;
                    }
                } else {
                    // Wildcards in derived tables — bail out as we can't
                    // resolve all columns without executing.
                    return None;
                }
            }
            Some(out)
        }
        FromClause::Values { rows, column_aliases, .. } => {
            if let Some(aliases) = column_aliases {
                for a in aliases {
                    out.insert(a.to_ascii_lowercase());
                }
                Some(out)
            } else {
                // Unaliased VALUES columns are typically named columnN; we
                // can't resolve them by name without execution. Conservative.
                let _ = rows;
                None
            }
        }
        FromClause::TableFunction { column_aliases, .. } => {
            if let Some(aliases) = column_aliases {
                for a in aliases {
                    out.insert(a.to_ascii_lowercase());
                }
                Some(out)
            } else {
                // Table-function columns can't be resolved by name without
                // execution. Conservative: unresolved.
                None
            }
        }
    }
}

/// Walk a SelectStmt looking for an aggregate whose column reference does not
/// resolve to any inner scope along the chain. Returns true on first match.
///
/// `outer_inner_scopes` is the chain of inner-FROM scopes from outer-most
/// (oldest) to inner-most (newest), NOT including this stmt's own FROM scope.
/// We compute this stmt's FROM scope and append it to extend the chain.
fn scope_chain_contains_outer_correlated_aggregate(
    stmt: &vibesql_ast::SelectStmt,
    outer_inner_scopes: &[HashSet<String>],
    database: &vibesql_storage::Database,
) -> bool {
    // Compute the scope this stmt's FROM contributes. None means we can't
    // resolve statically — conservatively act as if everything is in-scope
    // here so we don't false-positive. We do this by building an "all columns"
    // sentinel: an Option<HashSet<String>> where None means "matches anything".
    let this_scope: Option<HashSet<String>> = match stmt.from.as_ref() {
        Some(from) => columns_from_from_clause(from, database),
        None => Some(HashSet::new()),
    };

    // Decision: if we couldn't resolve this stmt's scope, bail out without
    // triggering. This preserves the false-positive guard for unresolvable
    // queries.
    let this_scope = match this_scope {
        Some(s) => s,
        None => return false,
    };

    // Build the extended scope chain that nested scalar subqueries will use.
    let mut extended: Vec<HashSet<String>> = outer_inner_scopes.to_vec();
    extended.push(this_scope);

    // Walk SELECT list.
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if expression_in_scope_chain_has_outer_correlated_aggregate(expr, &extended, database) {
                return true;
            }
        }
    }

    // Walk WHERE.
    if let Some(w) = &stmt.where_clause {
        if expression_in_scope_chain_has_outer_correlated_aggregate(w, &extended, database) {
            return true;
        }
    }

    // Walk HAVING.
    if let Some(h) = &stmt.having {
        if expression_in_scope_chain_has_outer_correlated_aggregate(h, &extended, database) {
            return true;
        }
    }

    // Walk ORDER BY.
    if let Some(items) = &stmt.order_by {
        for item in items {
            if expression_in_scope_chain_has_outer_correlated_aggregate(
                &item.expr, &extended, database,
            ) {
                return true;
            }
        }
    }

    // Walk GROUP BY.
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            if expression_in_scope_chain_has_outer_correlated_aggregate(&expr, &extended, database)
            {
                return true;
            }
        }
    }

    // Walk FROM clause for nested derived-table subqueries.
    if let Some(from) = &stmt.from {
        if from_clause_has_outer_correlated_aggregate(from, &extended, database) {
            return true;
        }
    }

    false
}

/// Check whether a FROM clause (looking at any derived-table subqueries it
/// contains) hides an outer-correlated aggregate.
fn from_clause_has_outer_correlated_aggregate(
    from: &vibesql_ast::FromClause,
    inner_scopes: &[HashSet<String>],
    database: &vibesql_storage::Database,
) -> bool {
    use vibesql_ast::FromClause;

    match from {
        FromClause::Table { .. } | FromClause::Values { .. } | FromClause::TableFunction { .. } => {
            false
        }
        FromClause::Join { left, right, condition, .. } => {
            from_clause_has_outer_correlated_aggregate(left, inner_scopes, database)
                || from_clause_has_outer_correlated_aggregate(right, inner_scopes, database)
                || condition.as_ref().is_some_and(|c| {
                    expression_in_scope_chain_has_outer_correlated_aggregate(
                        c,
                        inner_scopes,
                        database,
                    )
                })
        }
        FromClause::Subquery { query, .. } => {
            scope_chain_contains_outer_correlated_aggregate(query, inner_scopes, database)
        }
    }
}

/// Walk an expression looking for an aggregate function whose argument /
/// FILTER / ORDER BY references a column that is NOT in any inner scope.
/// Such a column must resolve to the surrounding outer query → triggers
/// implicit-outer-aggregate-collapse semantics.
///
/// Important: the aggregate must reference *only* outer columns (i.e.
/// no inner-scope column references) for the collapse to fire. If an
/// aggregate references both inner and outer columns, it's a normal
/// correlated aggregate that produces one value per outer row (no collapse).
/// E.g., `(SELECT count(a+c) FROM t2) FROM t1` — `a` is outer, `c` is inner;
/// expected: 2 rows (one per t1 row), NOT collapsed (window1.test 74.2).
fn expression_in_scope_chain_has_outer_correlated_aggregate(
    expr: &vibesql_ast::Expression,
    inner_scopes: &[HashSet<String>],
    database: &vibesql_storage::Database,
) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            // Found an aggregate. Check if its args / filter / order_by
            // reference an outer column AND no inner column. Both
            // conditions must hold to qualify as a collapse trigger.
            let has_outer =
                args.iter().any(|a| expression_has_column_outside_scopes(a, inner_scopes))
                    || filter
                        .as_ref()
                        .is_some_and(|f| expression_has_column_outside_scopes(f, inner_scopes))
                    || order_by.as_ref().is_some_and(|items| {
                        items
                            .iter()
                            .any(|i| expression_has_column_outside_scopes(&i.expr, inner_scopes))
                    });
            let has_inner = args
                .iter()
                .any(|a| expression_has_column_inside_scopes(a, inner_scopes))
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_has_column_inside_scopes(f, inner_scopes))
                || order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| expression_has_column_inside_scopes(&i.expr, inner_scopes))
                });
            if has_outer && !has_inner {
                return true;
            }
            // Aggregate didn't itself trigger; recurse into its args for
            // nested subquery triggers.
            args.iter().any(|a| {
                expression_in_scope_chain_has_outer_correlated_aggregate(a, inner_scopes, database)
            })
        }
        Expression::Function { name, args, .. }
            if matches!(
                name.to_uppercase().as_str(),
                "COUNT"
                    | "SUM"
                    | "AVG"
                    | "TOTAL"
                    | "MIN"
                    | "MAX"
                    | "GROUP_CONCAT"
                    | "STRING_AGG"
                    | "MEDIAN"
                    | "PERCENTILE"
                    | "PERCENTILE_CONT"
                    | "PERCENTILE_DISC"
                    | "STDDEV"
                    | "STDDEV_SAMP"
                    | "STDDEV_POP"
                    | "VARIANCE"
                    | "VAR_SAMP"
                    | "VAR_POP"
            ) =>
        {
            let upper = name.to_uppercase();
            let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
            if !is_scalar_minmax {
                let has_outer =
                    args.iter().any(|a| expression_has_column_outside_scopes(a, inner_scopes));
                let has_inner =
                    args.iter().any(|a| expression_has_column_inside_scopes(a, inner_scopes));
                if has_outer && !has_inner {
                    return true;
                }
            }
            args.iter().any(|a| {
                expression_in_scope_chain_has_outer_correlated_aggregate(a, inner_scopes, database)
            })
        }
        Expression::ScalarSubquery(stmt) => {
            // Descend into the nested scalar subquery, extending the scope
            // chain with this stmt's own FROM scope (handled by callee).
            scope_chain_contains_outer_correlated_aggregate(stmt, inner_scopes, database)
        }
        // Recurse into structural expressions.
        Expression::BinaryOp { left, right, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(left, inner_scopes, database)
                || expression_in_scope_chain_has_outer_correlated_aggregate(
                    right,
                    inner_scopes,
                    database,
                )
        }
        Expression::UnaryOp { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::IsNull { expr, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
                || expression_in_scope_chain_has_outer_correlated_aggregate(
                    pattern,
                    inner_scopes,
                    database,
                )
        }
        Expression::Between { expr, low, high, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
                || expression_in_scope_chain_has_outer_correlated_aggregate(
                    low,
                    inner_scopes,
                    database,
                )
                || expression_in_scope_chain_has_outer_correlated_aggregate(
                    high,
                    inner_scopes,
                    database,
                )
        }
        Expression::InList { expr, values, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
                || values.iter().any(|v| {
                    expression_in_scope_chain_has_outer_correlated_aggregate(
                        v,
                        inner_scopes,
                        database,
                    )
                })
        }
        Expression::In { expr, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| {
                expression_in_scope_chain_has_outer_correlated_aggregate(e, inner_scopes, database)
            }) || when_clauses.iter().any(|w| {
                w.conditions.iter().any(|c| {
                    expression_in_scope_chain_has_outer_correlated_aggregate(
                        c,
                        inner_scopes,
                        database,
                    )
                }) || expression_in_scope_chain_has_outer_correlated_aggregate(
                    &w.result,
                    inner_scopes,
                    database,
                )
            }) || else_result.as_ref().is_some_and(|e| {
                expression_in_scope_chain_has_outer_correlated_aggregate(e, inner_scopes, database)
            })
        }
        Expression::Function { args, .. } => args.iter().any(|a| {
            expression_in_scope_chain_has_outer_correlated_aggregate(a, inner_scopes, database)
        }),
        Expression::WindowFunction { function, over } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            if args.iter().any(|a| {
                expression_in_scope_chain_has_outer_correlated_aggregate(a, inner_scopes, database)
            }) {
                return true;
            }
            if let Some(p) = &over.partition_by {
                if p.iter().any(|e| {
                    expression_in_scope_chain_has_outer_correlated_aggregate(
                        e,
                        inner_scopes,
                        database,
                    )
                }) {
                    return true;
                }
            }
            if let Some(o) = &over.order_by {
                if o.iter().any(|i| {
                    expression_in_scope_chain_has_outer_correlated_aggregate(
                        &i.expr,
                        inner_scopes,
                        database,
                    )
                }) {
                    return true;
                }
            }
            false
        }
        Expression::QuantifiedComparison { expr, .. } => {
            expression_in_scope_chain_has_outer_correlated_aggregate(expr, inner_scopes, database)
        }
        _ => false,
    }
}

/// Check whether an expression contains any column reference whose name is
/// NOT defined in any of the supplied inner scopes. Used to detect
/// "out-of-scope" column references inside an aggregate's args / FILTER /
/// ORDER BY.
///
/// This stops descending at scalar subqueries and window functions —
/// those have their own scope analysis.
fn expression_has_column_outside_scopes(
    expr: &vibesql_ast::Expression,
    inner_scopes: &[HashSet<String>],
) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col) => {
            // The legacy parser represents COUNT(*) as ColumnRef { column: "*" }
            // (the arena parser uses Expression::Wildcard). The "*" wildcard
            // references no specific column — it counts rows of its own query
            // scope — so it can never be an outer column reference. Without
            // this guard, "*" is absent from every inner scope and would be
            // misclassified as outer, incorrectly triggering collapse (#5288).
            if col.schema_canonical().is_none()
                && col.table_canonical().is_none()
                && col.column_canonical() == "*"
            {
                return false;
            }
            let name = col.column_canonical().to_ascii_lowercase();
            // If the column is qualified by a table name, we still check by
            // column name only — qualifying doesn't change whether the
            // column is in an inner FROM scope (it just disambiguates which
            // table within that scope it comes from). A more sophisticated
            // check could verify the qualifier matches an inner-scope table,
            // but column-name absence is sufficient to flag outer.
            !inner_scopes.iter().any(|s| s.contains(&name))
        }
        Expression::BinaryOp { left, right, .. } => {
            expression_has_column_outside_scopes(left, inner_scopes)
                || expression_has_column_outside_scopes(right, inner_scopes)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::IsNull { expr, .. } => {
            expression_has_column_outside_scopes(expr, inner_scopes)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_has_column_outside_scopes(expr, inner_scopes)
                || expression_has_column_outside_scopes(pattern, inner_scopes)
        }
        Expression::Between { expr, low, high, .. } => {
            expression_has_column_outside_scopes(expr, inner_scopes)
                || expression_has_column_outside_scopes(low, inner_scopes)
                || expression_has_column_outside_scopes(high, inner_scopes)
        }
        Expression::InList { expr, values, .. } => {
            expression_has_column_outside_scopes(expr, inner_scopes)
                || values.iter().any(|v| expression_has_column_outside_scopes(v, inner_scopes))
        }
        Expression::Function { args, .. } => {
            args.iter().any(|a| expression_has_column_outside_scopes(a, inner_scopes))
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            args.iter().any(|a| expression_has_column_outside_scopes(a, inner_scopes))
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_has_column_outside_scopes(f, inner_scopes))
                || order_by.as_ref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|i| expression_has_column_outside_scopes(&i.expr, inner_scopes))
                })
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_column_outside_scopes(e, inner_scopes))
                || when_clauses.iter().any(|w| {
                    w.conditions
                        .iter()
                        .any(|c| expression_has_column_outside_scopes(c, inner_scopes))
                        || expression_has_column_outside_scopes(&w.result, inner_scopes)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expression_has_column_outside_scopes(e, inner_scopes))
        }
        // Don't cross subquery / window scope boundaries here — the parent
        // walker already handles those by extending the scope chain.
        _ => false,
    }
}

/// Mirror of `expression_has_column_outside_scopes` checking for column refs
/// that ARE in some inner scope. Used to detect aggregates that reference
/// both outer and inner columns (in which case collapse must NOT fire).
fn expression_has_column_inside_scopes(
    expr: &vibesql_ast::Expression,
    inner_scopes: &[HashSet<String>],
) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col) => {
            // Legacy-parser COUNT(*) wildcard (ColumnRef { column: "*" }) is
            // not a column reference. "*" can never appear in a scope set, so
            // this guard is a no-op behaviorally, but it documents intent and
            // mirrors expression_has_column_outside_scopes (#5288).
            if col.schema_canonical().is_none()
                && col.table_canonical().is_none()
                && col.column_canonical() == "*"
            {
                return false;
            }
            let name = col.column_canonical().to_ascii_lowercase();
            inner_scopes.iter().any(|s| s.contains(&name))
        }
        Expression::BinaryOp { left, right, .. } => {
            expression_has_column_inside_scopes(left, inner_scopes)
                || expression_has_column_inside_scopes(right, inner_scopes)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::IsNull { expr, .. } => {
            expression_has_column_inside_scopes(expr, inner_scopes)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_has_column_inside_scopes(expr, inner_scopes)
                || expression_has_column_inside_scopes(pattern, inner_scopes)
        }
        Expression::Between { expr, low, high, .. } => {
            expression_has_column_inside_scopes(expr, inner_scopes)
                || expression_has_column_inside_scopes(low, inner_scopes)
                || expression_has_column_inside_scopes(high, inner_scopes)
        }
        Expression::InList { expr, values, .. } => {
            expression_has_column_inside_scopes(expr, inner_scopes)
                || values.iter().any(|v| expression_has_column_inside_scopes(v, inner_scopes))
        }
        Expression::Function { args, .. } => {
            args.iter().any(|a| expression_has_column_inside_scopes(a, inner_scopes))
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            args.iter().any(|a| expression_has_column_inside_scopes(a, inner_scopes))
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_has_column_inside_scopes(f, inner_scopes))
                || order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| expression_has_column_inside_scopes(&i.expr, inner_scopes))
                })
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_column_inside_scopes(e, inner_scopes))
                || when_clauses.iter().any(|w| {
                    w.conditions
                        .iter()
                        .any(|c| expression_has_column_inside_scopes(c, inner_scopes))
                        || expression_has_column_inside_scopes(&w.result, inner_scopes)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expression_has_column_inside_scopes(e, inner_scopes))
        }
        // Don't cross subquery / window scope boundaries here.
        _ => false,
    }
}

/// Check whether an expression contains an aggregate function whose arguments,
/// FILTER, or ORDER BY items reference a column. Inside a bare (FROM-less)
/// scalar subquery, *any* column reference is necessarily an outer reference.
fn expr_has_column_referencing_aggregate(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            if args.iter().any(expression_contains_column_ref_simple) {
                return true;
            }
            if filter.as_ref().is_some_and(|f| expression_contains_column_ref_simple(f)) {
                return true;
            }
            if order_by.as_ref().is_some_and(|items| {
                items.iter().any(|i| expression_contains_column_ref_simple(&i.expr))
            }) {
                return true;
            }
            // An outer aggregate doesn't itself match, but it might have a
            // nested aggregate inside its args.
            args.iter().any(expr_has_column_referencing_aggregate)
        }
        Expression::Function { name, args, .. }
            if matches!(
                name.to_uppercase().as_str(),
                "COUNT"
                    | "SUM"
                    | "AVG"
                    | "TOTAL"
                    | "MIN"
                    | "MAX"
                    | "GROUP_CONCAT"
                    | "STRING_AGG"
                    | "MEDIAN"
                    | "PERCENTILE"
                    | "PERCENTILE_CONT"
                    | "PERCENTILE_DISC"
                    | "STDDEV"
                    | "STDDEV_SAMP"
                    | "STDDEV_POP"
                    | "VARIANCE"
                    | "VAR_SAMP"
                    | "VAR_POP"
            ) =>
        {
            // Old Function variant aggregate. min/max with >1 args are scalar.
            let upper = name.to_uppercase();
            let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
            if !is_scalar_minmax && args.iter().any(expression_contains_column_ref_simple) {
                return true;
            }
            args.iter().any(expr_has_column_referencing_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            expr_has_column_referencing_aggregate(left)
                || expr_has_column_referencing_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => expr_has_column_referencing_aggregate(expr),
        Expression::Cast { expr, .. } => expr_has_column_referencing_aggregate(expr),
        Expression::IsNull { expr, .. } => expr_has_column_referencing_aggregate(expr),
        Expression::Function { args, .. } => args.iter().any(expr_has_column_referencing_aggregate),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expr_has_column_referencing_aggregate(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expr_has_column_referencing_aggregate)
                        || expr_has_column_referencing_aggregate(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expr_has_column_referencing_aggregate(e))
        }
        // Window function: the collapse trigger can hide inside the window's
        // args or its OVER (PARTITION BY / ORDER BY / frame) expressions,
        // e.g. `SELECT (SELECT count(a) OVER (ORDER BY sum(a))) FROM t1`
        // (window1.test 61.4.2). Only *plain* AggregateFunction nodes found
        // by the recursion trigger collapse — the window function itself
        // (e.g. `total(a) OVER()`, a `WindowFunctionSpec::Aggregate`) must
        // NOT trigger (window1.test 61.3.1). (#5232)
        Expression::WindowFunction { function, over } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            if args.iter().any(expr_has_column_referencing_aggregate) {
                return true;
            }
            if over
                .partition_by
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(expr_has_column_referencing_aggregate))
            {
                return true;
            }
            if over.order_by.as_ref().is_some_and(|items| {
                items.iter().any(|i| expr_has_column_referencing_aggregate(&i.expr))
            }) {
                return true;
            }
            if let Some(frame) = &over.frame {
                if let vibesql_ast::FrameBound::Preceding(e)
                | vibesql_ast::FrameBound::Following(e) = &frame.start
                {
                    if expr_has_column_referencing_aggregate(e) {
                        return true;
                    }
                }
                if let Some(vibesql_ast::FrameBound::Preceding(e))
                | Some(vibesql_ast::FrameBound::Following(e)) = &frame.end
                {
                    if expr_has_column_referencing_aggregate(e) {
                        return true;
                    }
                }
            }
            false
        }
        // Don't descend into inner subqueries — they have their own scope.
        // Stop here.
        _ => false,
    }
}

/// Simple recursive column-reference check (no scope analysis — we're already
/// inside a bare subquery so any column ref is necessarily an outer one).
fn expression_contains_column_ref_simple(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(col) => {
            // Exclude the legacy-parser COUNT(*) representation
            // (ColumnRef { column: "*" }) — "*" references no specific column,
            // so it is never an outer column reference (#5288).
            !(col.schema_canonical().is_none()
                && col.table_canonical().is_none()
                && col.column_canonical() == "*")
        }
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_column_ref_simple(left)
                || expression_contains_column_ref_simple(right)
        }
        Expression::UnaryOp { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::Cast { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::IsNull { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::Function { args, .. } => args.iter().any(expression_contains_column_ref_simple),
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            args.iter().any(expression_contains_column_ref_simple)
                || filter.as_ref().is_some_and(|f| expression_contains_column_ref_simple(f))
                || order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| expression_contains_column_ref_simple(&i.expr))
                })
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_contains_column_ref_simple(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_contains_column_ref_simple)
                        || expression_contains_column_ref_simple(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_contains_column_ref_simple(e))
        }
        Expression::Like { expr, pattern, .. } => {
            expression_contains_column_ref_simple(expr)
                || expression_contains_column_ref_simple(pattern)
        }
        Expression::Between { expr, low, high, .. } => {
            expression_contains_column_ref_simple(expr)
                || expression_contains_column_ref_simple(low)
                || expression_contains_column_ref_simple(high)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_column_ref_simple(expr)
                || values.iter().any(expression_contains_column_ref_simple)
        }
        // Don't descend into nested subqueries (separate scope).
        _ => false,
    }
}

impl SelectExecutor<'_> {
    /// Check if SELECT list contains aggregate functions
    pub(in crate::select::executor) fn has_aggregates(
        &self,
        select_list: &[vibesql_ast::SelectItem],
    ) -> bool {
        select_list.iter().any(|item| match item {
            vibesql_ast::SelectItem::Expression { expr, .. } => self.expression_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn expression_has_aggregate(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> bool {
        match expr {
            // New AggregateFunction variant
            vibesql_ast::Expression::AggregateFunction { .. } => true,
            // Old Function variant (backwards compatibility for aggregates)
            // Note: MIN/MAX with >1 argument are scalar functions (SQLite compatibility)
            vibesql_ast::Expression::Function { name, args, .. } => {
                let name_upper = name.to_uppercase();
                // Check if this is an aggregate function name
                let is_aggregate = match name_upper.as_str() {
                    "COUNT" | "SUM" | "AVG" | "TOTAL" | "GROUP_CONCAT" | "STRING_AGG"
                    | "JSON_GROUP_ARRAY" | "JSON_GROUP_OBJECT" | "MD5SUM" | "MEDIAN"
                    | "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" | "STDDEV"
                    | "STDDEV_SAMP" | "STDDEV_POP" | "VARIANCE" | "VAR_SAMP" | "VAR_POP" => true,
                    "MIN" | "MAX" => args.len() <= 1, // multi-arg min/max are scalar functions
                    _ => false,
                };
                if is_aggregate {
                    return true;
                }
                // Otherwise, check if any arguments contain aggregates
                args.iter().any(|arg| self.expression_has_aggregate(arg))
            }
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_has_aggregate(left) || self.expression_has_aggregate(right)
            }
            // Unary operations - check if inner expression contains aggregate
            vibesql_ast::Expression::UnaryOp { expr, .. } => self.expression_has_aggregate(expr),
            vibesql_ast::Expression::Cast { expr, .. } => self.expression_has_aggregate(expr),
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
                    || when_clauses.iter().any(|when_clause| {
                        when_clause.conditions.iter().any(|c| self.expression_has_aggregate(c))
                            || self.expression_has_aggregate(&when_clause.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
            }
            // BETWEEN: check all three sub-expressions
            vibesql_ast::Expression::Between { expr, low, high, .. } => {
                self.expression_has_aggregate(expr)
                    || self.expression_has_aggregate(low)
                    || self.expression_has_aggregate(high)
            }
            // IN list: check test expression and all values
            vibesql_ast::Expression::InList { expr, values, .. } => {
                self.expression_has_aggregate(expr)
                    || values.iter().any(|v| self.expression_has_aggregate(v))
            }
            // IN subquery: check test expression
            vibesql_ast::Expression::In { expr, .. } => self.expression_has_aggregate(expr),
            // LIKE: check both expression and pattern
            vibesql_ast::Expression::Like { expr, pattern, .. } => {
                self.expression_has_aggregate(expr) || self.expression_has_aggregate(pattern)
            }
            // IS NULL: check inner expression
            vibesql_ast::Expression::IsNull { expr, .. } => self.expression_has_aggregate(expr),
            // Position: check both substring and string
            vibesql_ast::Expression::Position { substring, string, .. } => {
                self.expression_has_aggregate(substring) || self.expression_has_aggregate(string)
            }
            // Trim: check removal char and string
            vibesql_ast::Expression::Trim { removal_char, string, .. } => {
                removal_char.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
                    || self.expression_has_aggregate(string)
            }
            // Interval: check the value expression
            vibesql_ast::Expression::Interval { value, .. } => self.expression_has_aggregate(value),
            // Quantified comparison: check left-hand expression
            vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
                self.expression_has_aggregate(expr)
            }
            // Scalar subquery and EXISTS: subqueries can contain aggregates, but we don't check
            // inside them The aggregates inside subqueries are in their own scope
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                false
            }
            // Window functions may contain nested aggregates in their arguments
            // e.g., min(sum(a)) OVER () — the sum(a) is an aggregate that must be
            // evaluated in the aggregation path before the window function is applied.
            // Aggregates can also hide in the OVER clause's PARTITION BY / ORDER BY,
            // e.g. `count(a) OVER (ORDER BY sum(a))` — per SQLite this makes the
            // query an aggregate query (one row per group). (#5232 / window1.test
            // 61.4.2.) Note: ScalarSubquery is not descended into anywhere in this
            // function, so `OVER (ORDER BY (SELECT sum(y) FROM t2))` does NOT make
            // the outer query an aggregate (window1.test 61.3.1).
            vibesql_ast::Expression::WindowFunction { function, over } => {
                let args = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Ranking { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
                };
                args.iter().any(|arg| self.expression_has_aggregate(arg))
                    || over
                        .partition_by
                        .as_ref()
                        .is_some_and(|exprs| exprs.iter().any(|e| self.expression_has_aggregate(e)))
                    || over.order_by.as_ref().is_some_and(|items| {
                        items.iter().any(|i| self.expression_has_aggregate(&i.expr))
                    })
            }
            // DuplicateKeyValue references a column from INSERT VALUES
            vibesql_ast::Expression::DuplicateKeyValue { .. } => false,
            // Literals, column refs, wildcards, current date/time, defaults, sequences, etc. don't
            // contain aggregates
            _ => false,
        }
    }

    /// Detect SQLite's implicit-outer-aggregate-collapse pattern (#5104).
    ///
    /// Returns true when the SELECT list contains a *bare* (FROM-less) scalar
    /// subquery whose body has an aggregate function referencing an outer
    /// column. SQLite collapses the outer query into a single-row aggregate
    /// in this case, with the inner aggregate computed over all outer rows.
    ///
    /// Examples that match:
    /// - `SELECT (SELECT avg(a)) FROM t2`               — bare aggregate, outer-correlated
    /// - `SELECT (SELECT sum(y) FILTER(WHERE x>0)) FROM t` — outer-correlated via FILTER
    ///
    /// Examples that do NOT match (return false):
    /// - `SELECT (SELECT 1) FROM t`                   — no aggregate
    /// - `SELECT (SELECT min(a) OVER ()) FROM t`      — window, not bare aggregate
    /// - `SELECT (SELECT avg(x) FROM other) FROM t`   — has FROM, not bare
    /// - `SELECT avg(a) FROM t`                       — top-level aggregate (already handled)
    pub(in crate::select::executor) fn select_list_has_outer_aggregate_collapse(
        &self,
        select_list: &[vibesql_ast::SelectItem],
    ) -> bool {
        select_list.iter().any(|item| match item {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                expression_contains_outer_aggregate_collapse(expr, self.database)
            }
            _ => false,
        })
    }

    /// Check if statement is a simple COUNT(*) query that can use fast path
    ///
    /// Fast path conditions:
    /// - Single SELECT item: COUNT(*)
    /// - No WHERE clause
    /// - No GROUP BY clause
    /// - No HAVING clause
    /// - No DISTINCT
    /// - No JOIN (single table reference)
    /// - No set operations (UNION, INTERSECT, EXCEPT)
    /// - FROM clause contains single table (not subquery/CTE)
    pub(in crate::select::executor) fn is_simple_count_star(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Option<String> {
        // Must have exactly one select item
        if stmt.select_list.len() != 1 {
            return None;
        }

        // Check if it's COUNT(*)
        let is_count_star = match &stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
                        // Must be COUNT, not DISTINCT, with single wildcard argument
                        if name.to_uppercase() != "COUNT" || *distinct || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], vibesql_ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                            )
                    }
                    vibesql_ast::Expression::Function { name, args, .. } => {
                        // Old Function variant (backwards compatibility)
                        if name.to_uppercase() != "COUNT" || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], vibesql_ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                vibesql_ast::Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                            )
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        if !is_count_star {
            return None;
        }

        // Must not have WHERE, GROUP BY, HAVING, DISTINCT, or set operations
        if stmt.where_clause.is_some()
            || stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.set_operation.is_some()
        {
            return None;
        }

        // Must have a FROM clause with a single table
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
            Some(vibesql_ast::FromClause::Join { .. }) => return None, // JOIN not allowed
            Some(vibesql_ast::FromClause::Subquery { .. }) => return None, // Subquery not allowed
            Some(vibesql_ast::FromClause::Values { .. }) => return None, // VALUES not allowed
            Some(vibesql_ast::FromClause::TableFunction { .. }) => return None, // Table function not allowed
            None => return None,                                                // No FROM clause
        };

        Some(table_name)
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for #5135 — scope-aware outer-aggregate-collapse detection.
    //!
    //! These tests pin down the false-positive guard: deeply nested
    //! FROM-bearing subqueries that aggregate over their own inner tables
    //! must NOT trigger collapse, while aggregates that genuinely reference
    //! the outermost outer scope must trigger.

    use vibesql_ast::{
        ColumnIdentifier, Expression, FromClause, FunctionIdentifier, SelectItem, SelectStmt,
    };
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Database;
    use vibesql_types::DataType;

    use super::*;

    /// Build an empty SelectStmt scaffold.
    fn empty_select_stmt() -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: Vec::new(),
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    /// Build a one-table FROM clause referring to the named table.
    fn from_table(name: &str) -> FromClause {
        FromClause::Table {
            index_hint: None,
            name: name.to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        }
    }

    /// Build an aggregate-function expression `agg(arg)`.
    fn agg(name: &str, arg: Expression) -> Expression {
        Expression::AggregateFunction {
            name: FunctionIdentifier::new(name),
            distinct: false,
            args: vec![arg],
            order_by: None,
            filter: None,
        }
    }

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::simple(name, false))
    }

    /// Build a Database with two tables: `t1(a)` and `t2(x)`. Used by
    /// the false-positive guard test below.
    fn setup_db_t1_t2() -> Database {
        let mut db = Database::new();
        db.create_table(TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        ))
        .expect("create t1");
        db.create_table(TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
        ))
        .expect("create t2");
        db
    }

    /// Build a Database with two tables: `t1(a)` and `t3(y)`. Used by the
    /// 57.3 positive case below.
    fn setup_db_t1_t3() -> Database {
        let mut db = Database::new();
        db.create_table(TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        ))
        .expect("create t1");
        db.create_table(TableSchema::new(
            "t3".to_string(),
            vec![ColumnSchema::new("y".to_string(), DataType::Integer, true)],
        ))
        .expect("create t3");
        db
    }

    /// False-positive guard (acceptance criterion for #5135):
    /// `SELECT (SELECT sum(x) FROM t2) FROM t1` must NOT trigger
    /// outer-aggregate-collapse, because `x` resolves to `t2.x` (an inner
    /// scope column), not to t1.
    #[test]
    fn no_collapse_when_aggregate_resolves_to_inner_from_table() {
        let db = setup_db_t1_t2();

        // Inner: SELECT sum(x) FROM t2
        let mut inner = empty_select_stmt();
        inner.select_list.push(SelectItem::Expression {
            expr: agg("sum", col("x")),
            alias: None,
            source_text: None,
        });
        inner.from = Some(from_table("t2"));

        // Outer SELECT list expression: (SELECT sum(x) FROM t2)
        let outer_subq = Expression::ScalarSubquery(Box::new(inner));

        assert!(
            !expression_contains_outer_aggregate_collapse(&outer_subq, &db),
            "SELECT (SELECT sum(x) FROM t2) FROM t1 must NOT trigger collapse — \
             `x` resolves to inner scope t2.x, not outer scope t1"
        );
    }

    /// Positive case for #5135 (window1.test 57.3 mini-fixture):
    /// the inner-most subquery `(SELECT sum(y) AS x FROM t1)` aggregates
    /// over `y`, which is NOT in t1's column set, so `y` must resolve to
    /// the outermost outer scope (t3) → triggers collapse.
    #[test]
    fn collapse_when_aggregate_references_outer_through_nested_from_subqueries() {
        let db = setup_db_t1_t3();

        // Innermost: SELECT sum(y) AS x FROM t1
        let mut innermost = empty_select_stmt();
        innermost.select_list.push(SelectItem::Expression {
            expr: agg("sum", col("y")),
            alias: Some("x".to_string()),
            source_text: None,
        });
        innermost.from = Some(from_table("t1"));

        // Middle: SELECT x FROM (innermost)
        let mut middle = empty_select_stmt();
        middle.select_list.push(SelectItem::Expression {
            expr: col("x"),
            alias: None,
            source_text: None,
        });
        middle.from = Some(FromClause::Subquery {
            query: Box::new(innermost),
            alias: "_anon".to_string(),
            column_aliases: None,
        });

        // Wrap in a scalar subquery (the outer SELECT-list expression in a
        // hypothetical `SELECT (SELECT x FROM (...)) FROM t3` query).
        let outer_subq = Expression::ScalarSubquery(Box::new(middle));

        assert!(
            expression_contains_outer_aggregate_collapse(&outer_subq, &db),
            "57.3 pattern: nested FROM subqueries with sum(y) aggregating over \
             outer t3.y must trigger collapse"
        );
    }

    /// Mixed-references case (analogous to window1.test 74.2):
    /// `SELECT (SELECT count(a+x) FROM t2) FROM t1` aggregates over both
    /// outer (`a`) and inner (`x`) columns — must NOT collapse.
    #[test]
    fn no_collapse_when_aggregate_references_both_inner_and_outer() {
        let db = setup_db_t1_t2();

        // Inner: SELECT count(a+x) FROM t2
        let plus = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Plus,
            left: Box::new(col("a")),
            right: Box::new(col("x")),
        };

        let mut inner = empty_select_stmt();
        inner.select_list.push(SelectItem::Expression {
            expr: agg("count", plus),
            alias: None,
            source_text: None,
        });
        inner.from = Some(from_table("t2"));

        let outer_subq = Expression::ScalarSubquery(Box::new(inner));

        assert!(
            !expression_contains_outer_aggregate_collapse(&outer_subq, &db),
            "An aggregate referencing both an outer column (a from t1) and an \
             inner column (x from t2) must NOT trigger collapse — it's a normal \
             correlated aggregate evaluated per outer row"
        );
    }

    /// Build a window-function expression `name(args) OVER (ORDER BY order_by_expr)`.
    fn window_fn(
        function: vibesql_ast::WindowFunctionSpec,
        order_by_expr: Option<Expression>,
    ) -> Expression {
        Expression::WindowFunction {
            function,
            over: vibesql_ast::WindowSpec {
                base_window_name: None,
                partition_by: None,
                order_by: order_by_expr.map(|e| {
                    vec![vibesql_ast::OrderByItem {
                        expr: e,
                        direction: vibesql_ast::OrderDirection::Asc,
                        nulls_order: None,
                    }]
                }),
                frame: None,
            },
        }
    }

    /// Gap 2 (#5232, window1.test 44.x): the collapse trigger must be
    /// detected through an IN subquery:
    /// `SELECT (0, 0) IN (SELECT MIN(c0), NTILE(1) OVER()) FROM t0`.
    #[test]
    fn collapse_detected_through_in_subquery() {
        let db = setup_db_t1_t2();

        // Inner (FROM-less): SELECT MIN(a), NTILE(1) OVER()
        let mut inner = empty_select_stmt();
        inner.select_list.push(SelectItem::Expression {
            expr: agg("min", col("a")),
            alias: None,
            source_text: None,
        });
        inner.select_list.push(SelectItem::Expression {
            expr: window_fn(
                vibesql_ast::WindowFunctionSpec::Ranking {
                    name: FunctionIdentifier::new("ntile"),
                    args: vec![Expression::Literal(vibesql_types::SqlValue::Integer(1))],
                },
                None,
            ),
            alias: None,
            source_text: None,
        });

        let in_expr = Expression::In {
            expr: Box::new(Expression::RowValueConstructor(vec![
                Expression::Literal(vibesql_types::SqlValue::Integer(0)),
                Expression::Literal(vibesql_types::SqlValue::Integer(0)),
            ])),
            subquery: Box::new(inner),
            negated: false,
        };

        assert!(
            expression_contains_outer_aggregate_collapse(&in_expr, &db),
            "(0,0) IN (SELECT MIN(a), NTILE(1) OVER()) must trigger collapse — \
             the FROM-less IN subquery aggregates over an outer column"
        );
    }

    /// Gap 3 (#5232, window1.test 61.4.2): the collapse trigger must be
    /// detected when the aggregate hides inside a window function's
    /// OVER (ORDER BY ...): `SELECT (SELECT count(a) OVER (ORDER BY sum(a))) FROM t1`.
    #[test]
    fn collapse_detected_for_aggregate_inside_over_order_by() {
        let db = setup_db_t1_t2();

        let mut inner = empty_select_stmt();
        inner.select_list.push(SelectItem::Expression {
            expr: window_fn(
                vibesql_ast::WindowFunctionSpec::Aggregate {
                    name: FunctionIdentifier::new("count"),
                    args: vec![col("a")],
                    filter: None,
                },
                Some(agg("sum", col("a"))),
            ),
            alias: None,
            source_text: None,
        });

        let outer_subq = Expression::ScalarSubquery(Box::new(inner));

        assert!(
            expression_contains_outer_aggregate_collapse(&outer_subq, &db),
            "(SELECT count(a) OVER (ORDER BY sum(a))) must trigger collapse — \
             sum(a) inside the OVER ORDER BY is an outer-correlated aggregate"
        );
    }

    /// Guard (#5232, window1.test 61.3.1): a window-aggregate like
    /// `total(a) OVER()` is a `WindowFunctionSpec::Aggregate`, NOT a plain
    /// `AggregateFunction`, and must NOT trigger collapse —
    /// `SELECT (SELECT total(a) OVER()) FROM t1` returns 0 rows on empty t1.
    #[test]
    fn no_collapse_for_window_aggregate_spec() {
        let db = setup_db_t1_t2();

        let mut inner = empty_select_stmt();
        inner.select_list.push(SelectItem::Expression {
            expr: window_fn(
                vibesql_ast::WindowFunctionSpec::Aggregate {
                    name: FunctionIdentifier::new("total"),
                    args: vec![col("a")],
                    filter: None,
                },
                None,
            ),
            alias: None,
            source_text: None,
        });

        let outer_subq = Expression::ScalarSubquery(Box::new(inner));

        assert!(
            !expression_contains_outer_aggregate_collapse(&outer_subq, &db),
            "(SELECT total(a) OVER()) must NOT trigger collapse — the window \
             aggregate has its own scope and is not a plain aggregate"
        );
    }
}
