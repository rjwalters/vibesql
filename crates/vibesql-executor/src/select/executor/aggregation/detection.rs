//! Aggregate detection helpers for SelectExecutor

use super::super::builder::SelectExecutor;

/// Check whether an expression contains a *bare* (FROM-less) scalar subquery
/// whose body has an aggregate referencing an outer column. This triggers
/// SQLite's implicit-outer-aggregate-collapse semantics (#5104).
///
/// "Outer column" inside a bare subquery means *any* column reference (since
/// the subquery has no tables of its own). We check the aggregate's args, its
/// FILTER clause, and its ORDER BY items.
fn expression_contains_outer_aggregate_collapse(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ScalarSubquery(stmt) => {
            // Bare (FROM-less) subquery: an aggregate in the projection that
            // references a column is necessarily outer-correlated (no inner
            // tables) → triggers collapse.
            if stmt.from.is_none() {
                for item in &stmt.select_list {
                    if let vibesql_ast::SelectItem::Expression { expr: inner, .. } = item {
                        if expr_has_column_referencing_aggregate(inner) {
                            return true;
                        }
                        // Recurse: nested scalar subqueries.
                        if expression_contains_outer_aggregate_collapse(inner) {
                            return true;
                        }
                    }
                }
                return false;
            }

            // FROM-bearing subquery: only recurse into the SELECT list to
            // find further bare-subquery collapse triggers. We do NOT walk
            // into FROM/WHERE/HAVING/ORDER-BY of FROM-bearing subqueries
            // here because we lack schema information to disambiguate
            // which columns resolve to inner vs outer scope. Without that
            // disambiguation, recursing too deeply produces false positives
            // (e.g. `SELECT (SELECT sum(x) FROM t2) FROM t1` would falsely
            // match if we treated any aggregate-with-column-ref as a
            // trigger). Issue #5104's window1.test 57.3 case (which
            // requires deep cross-scope analysis) is handled by a follow-up.
            stmt.select_list.iter().any(|item| match item {
                vibesql_ast::SelectItem::Expression { expr: inner, .. } => {
                    expression_contains_outer_aggregate_collapse(inner)
                }
                _ => false,
            })
        }
        // Recurse into compound expressions.
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_outer_aggregate_collapse(left)
                || expression_contains_outer_aggregate_collapse(right)
        }
        Expression::UnaryOp { expr, .. } => expression_contains_outer_aggregate_collapse(expr),
        Expression::Cast { expr, .. } => expression_contains_outer_aggregate_collapse(expr),
        Expression::IsNull { expr, .. } => expression_contains_outer_aggregate_collapse(expr),
        Expression::Like { expr, pattern, .. } => {
            expression_contains_outer_aggregate_collapse(expr)
                || expression_contains_outer_aggregate_collapse(pattern)
        }
        Expression::Between { expr, low, high, .. } => {
            expression_contains_outer_aggregate_collapse(expr)
                || expression_contains_outer_aggregate_collapse(low)
                || expression_contains_outer_aggregate_collapse(high)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_outer_aggregate_collapse(expr)
                || values.iter().any(expression_contains_outer_aggregate_collapse)
        }
        Expression::In { expr, .. } => expression_contains_outer_aggregate_collapse(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand
                .as_ref()
                .is_some_and(|e| expression_contains_outer_aggregate_collapse(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_contains_outer_aggregate_collapse)
                        || expression_contains_outer_aggregate_collapse(&w.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expression_contains_outer_aggregate_collapse(e))
        }
        Expression::Function { args, .. } => {
            args.iter().any(expression_contains_outer_aggregate_collapse)
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            // The collapse pattern only triggers from a bare scalar subquery,
            // so an aggregate at this level is the *outer* aggregate (already
            // handled by `has_aggregates`). We only recurse to find subqueries
            // that themselves trigger the pattern (e.g. `min(...((SELECT avg(a))))`).
            args.iter().any(expression_contains_outer_aggregate_collapse)
                || filter
                    .as_ref()
                    .is_some_and(|f| expression_contains_outer_aggregate_collapse(f))
                || order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| expression_contains_outer_aggregate_collapse(&i.expr))
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
            if args.iter().any(expression_contains_outer_aggregate_collapse) {
                return true;
            }
            if let Some(partition_by) = &over.partition_by {
                if partition_by.iter().any(expression_contains_outer_aggregate_collapse) {
                    return true;
                }
            }
            if let Some(order_by) = &over.order_by {
                if order_by
                    .iter()
                    .any(|item| expression_contains_outer_aggregate_collapse(&item.expr))
                {
                    return true;
                }
            }
            false
        }
        Expression::QuantifiedComparison { expr, .. } => {
            expression_contains_outer_aggregate_collapse(expr)
        }
        // EXISTS has its own scope — don't recurse into it.
        // Other leaf expressions can't contain a scalar subquery.
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
                "COUNT" | "SUM" | "AVG" | "TOTAL" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG"
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
        Expression::Function { args, .. } => {
            args.iter().any(expr_has_column_referencing_aggregate)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expr_has_column_referencing_aggregate(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expr_has_column_referencing_aggregate)
                        || expr_has_column_referencing_aggregate(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expr_has_column_referencing_aggregate(e))
        }
        // Don't descend into inner subqueries / window functions — they have
        // their own scope. Stop here.
        _ => false,
    }
}

/// Simple recursive column-reference check (no scope analysis — we're already
/// inside a bare subquery so any column ref is necessarily an outer one).
fn expression_contains_column_ref_simple(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(_) => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_column_ref_simple(left)
                || expression_contains_column_ref_simple(right)
        }
        Expression::UnaryOp { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::Cast { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::IsNull { expr, .. } => expression_contains_column_ref_simple(expr),
        Expression::Function { args, .. } => {
            args.iter().any(expression_contains_column_ref_simple)
        }
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
                    | "JSON_GROUP_ARRAY" | "MD5SUM" => true,
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
            // evaluated in the aggregation path before the window function is applied
            vibesql_ast::Expression::WindowFunction { function, .. } => {
                let args = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Ranking { args, .. } => args,
                    vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
                };
                args.iter().any(|arg| self.expression_has_aggregate(arg))
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
                expression_contains_outer_aggregate_collapse(expr)
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
            None => return None,                                       // No FROM clause
        };

        Some(table_name)
    }
}
