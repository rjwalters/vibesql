//! Aggregate function validation
//!
//! Validates aggregate function usage including:
//! - Argument count validation
//! - Nested aggregate detection
//! - Aliased aggregate misuse in HAVING clause

use std::collections::HashSet;

use vibesql_ast::{Expression, SelectItem};

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Check if a function name is an aggregate function
pub fn is_aggregate_function(name: &str) -> bool {
    let upper = name.to_uppercase();
    matches!(
        upper.as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "TOTAL"
            | "GROUP_CONCAT"
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
    )
}

/// Action taken at each node during a generic expression walk.
///
/// Used by [`walk_first`] to let callers control short-circuiting and
/// recursion on a per-node basis.
enum WalkAction<T> {
    /// Stop the walk and return this value wrapped in `Some`.
    Stop(T),
    /// Skip this node entirely — do not descend into its children.
    /// The walk continues with the next sibling.
    Skip,
    /// Descend into this node's children using the standard recursion.
    Descend,
}

/// Walk an `Expression` tree and return the first `Some(T)` produced by the
/// visitor, or `None` if no node matched.
///
/// The visitor `f` is called for every node before its children are visited.
/// It returns a [`WalkAction`] that controls the walk:
/// - [`WalkAction::Stop`] short-circuits with the wrapped value.
/// - [`WalkAction::Skip`] skips this subtree (children are NOT visited).
/// - [`WalkAction::Descend`] visits all of the node's children using the
///   standard recursion defined here.
///
/// **Subtree skipping** is the mechanism callers use to opt out of certain
/// scopes (e.g. `ScalarSubquery` / `Exists`, the OVER-clause of a
/// `WindowFunction`, etc.). Any node that should be treated as a barrier
/// should return [`WalkAction::Skip`] from the visitor.
///
/// The recursion descends into every child of every compound `Expression`
/// variant. Variants with no child expressions (literals, column refs, etc.)
/// terminate the walk on their branch.
fn walk_first<T>(expr: &Expression, f: &impl Fn(&Expression) -> WalkAction<T>) -> Option<T> {
    match f(expr) {
        WalkAction::Stop(t) => return Some(t),
        WalkAction::Skip => return None,
        WalkAction::Descend => {}
    }

    match expr {
        // Compound expressions — recurse into children.
        Expression::BinaryOp { left, right, .. } => {
            walk_first(left, f).or_else(|| walk_first(right, f))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().find_map(|c| walk_first(c, f))
        }
        Expression::UnaryOp { expr, .. } => walk_first(expr, f),
        Expression::Function { args, .. } => args.iter().find_map(|a| walk_first(a, f)),
        Expression::AggregateFunction { args, order_by, filter, .. } => args
            .iter()
            .find_map(|a| walk_first(a, f))
            .or_else(|| {
                order_by
                    .as_ref()
                    .and_then(|items| items.iter().find_map(|item| walk_first(&item.expr, f)))
            })
            .or_else(|| filter.as_deref().and_then(|fexpr| walk_first(fexpr, f))),
        Expression::IsNull { expr, .. } => walk_first(expr, f),
        Expression::IsDistinctFrom { left, right, .. } => {
            walk_first(left, f).or_else(|| walk_first(right, f))
        }
        Expression::IsTruthValue { expr, .. } => walk_first(expr, f),
        Expression::Case { operand, when_clauses, else_result } => operand
            .as_deref()
            .and_then(|op| walk_first(op, f))
            .or_else(|| {
                when_clauses.iter().find_map(|w| {
                    w.conditions
                        .iter()
                        .find_map(|c| walk_first(c, f))
                        .or_else(|| walk_first(&w.result, f))
                })
            })
            .or_else(|| else_result.as_deref().and_then(|e| walk_first(e, f))),
        Expression::In { expr, .. } => walk_first(expr, f),
        Expression::InList { expr, values, .. } => {
            walk_first(expr, f).or_else(|| values.iter().find_map(|v| walk_first(v, f)))
        }
        Expression::Between { expr, low, high, .. } => {
            walk_first(expr, f).or_else(|| walk_first(low, f)).or_else(|| walk_first(high, f))
        }
        Expression::Cast { expr, .. } => walk_first(expr, f),
        Expression::Position { substring, string, .. } => {
            walk_first(substring, f).or_else(|| walk_first(string, f))
        }
        Expression::Trim { removal_char, string, .. } => removal_char
            .as_deref()
            .and_then(|rc| walk_first(rc, f))
            .or_else(|| walk_first(string, f)),
        Expression::Extract { expr, .. } => walk_first(expr, f),
        Expression::Like { expr, pattern, escape, .. }
        | Expression::Glob { expr, pattern, escape, .. } => walk_first(expr, f)
            .or_else(|| walk_first(pattern, f))
            .or_else(|| escape.as_deref().and_then(|e| walk_first(e, f))),
        Expression::QuantifiedComparison { expr, .. } => walk_first(expr, f),
        Expression::Interval { value, .. } => walk_first(value, f),
        Expression::WindowFunction { function, .. } => {
            // By default, recurse into the function's arguments AND its
            // FILTER clause (for Aggregate variants). The OVER clause is
            // intentionally NOT visited here — the `WindowFunction` arm of
            // every existing walker either ignores it or treats it as a
            // separate scope. Callers that need OVER-clause inspection
            // should match `WindowFunction` themselves and return
            // `WalkAction::Skip` from the visitor.
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, filter, .. } => args
                    .iter()
                    .find_map(|a| walk_first(a, f))
                    .or_else(|| filter.as_deref().and_then(|fx| walk_first(fx, f))),
                vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    args.iter().find_map(|a| walk_first(a, f))
                }
            }
        }
        Expression::MatchAgainst { search_modifier, .. } => walk_first(search_modifier, f),
        Expression::RowValueConstructor(children) => children.iter().find_map(|c| walk_first(c, f)),
        Expression::Collate { expr, .. } => walk_first(expr, f),
        Expression::Raise { error_message, .. } => {
            error_message.as_deref().and_then(|msg| walk_first(msg, f))
        }
        // Leaf-like or scope-closing expressions. ScalarSubquery / Exists
        // intentionally terminate the walk — most callers want subqueries
        // treated as their own scope. Callers that need to recurse must
        // handle these in the visitor.
        Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => None,
    }
}

/// Check if an aggregate function has wrong number of arguments
/// Returns Some((function_name, arg_count)) if there's an error, None otherwise
pub fn check_aggregate_arg_count(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, distinct, .. } => {
            let upper = name.to_uppercase();
            let arg_count = args.len();

            // Check for wildcard in non-COUNT aggregates
            let has_wildcard = args.iter().any(|arg| {
                let is_wildcard = matches!(arg, Expression::Wildcard);
                let is_star_ref = matches!(
                    arg,
                    Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                );
                is_wildcard || is_star_ref
            });

            match upper.as_str() {
                "COUNT" => {
                    // Multi-arg COUNT without DISTINCT is an error
                    // SQLite: "wrong number of arguments to function count()"
                    if arg_count > 1 && !*distinct {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                "MIN" | "MAX" => {
                    if has_wildcard || arg_count == 0 {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                "SUM" | "AVG" | "TOTAL" => {
                    if has_wildcard || arg_count == 0 || arg_count > 1 {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                "GROUP_CONCAT" => {
                    if arg_count == 0 || arg_count > 2 {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                "MEDIAN" => {
                    // median(Y) takes exactly 1 argument (percentile.c)
                    if has_wildcard || arg_count != 1 {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                    // percentile family takes exactly 2 arguments (percentile.c)
                    if has_wildcard || arg_count != 2 {
                        Some(name.display().to_string())
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        Expression::Function { name, args, .. } => {
            // Check if this is an aggregate function with wrong args
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                let arg_count = args.len();

                // Check for wildcard
                let has_wildcard = args.iter().any(|arg| {
                    matches!(arg, Expression::Wildcard)
                        || matches!(
                            arg,
                            Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                        )
                });

                match upper.as_str() {
                    "COUNT" => {
                        // count(a, b) without DISTINCT is wrong
                        // Regular count without DISTINCT can only have 0-1 args
                        if arg_count > 1 {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    "MIN" | "MAX" => {
                        // Multi-arg min/max are scalar, so only check single arg case
                        if arg_count <= 1 && (has_wildcard || arg_count == 0) {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    "SUM" | "AVG" | "TOTAL" => {
                        if has_wildcard || arg_count == 0 || arg_count > 1 {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    "GROUP_CONCAT" => {
                        if arg_count == 0 || arg_count > 2 {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    "MEDIAN" => {
                        if has_wildcard || arg_count != 1 {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                        if has_wildcard || arg_count != 2 {
                            Some(name.display().to_string())
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = check_aggregate_arg_count(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            check_aggregate_arg_count(left).or_else(|| check_aggregate_arg_count(right))
        }
        Expression::UnaryOp { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = check_aggregate_arg_count(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = check_aggregate_arg_count(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = check_aggregate_arg_count(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                check_aggregate_arg_count(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Cast { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = check_aggregate_arg_count(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the first aggregate function in an expression
/// Returns the function name (original case preserved) if found, None otherwise
pub fn find_aggregate_in_expression(expr: &Expression) -> Option<String> {
    walk_first(expr, &|e| match e {
        Expression::AggregateFunction { name, .. } => WalkAction::Stop(name.to_string()),
        Expression::Function { name, args, .. } if is_aggregate_function(name.as_str()) => {
            // Multi-arg MIN/MAX are scalar functions in SQLite, not aggregates.
            let upper = name.to_uppercase();
            if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                WalkAction::Descend
            } else {
                WalkAction::Stop(name.to_string())
            }
        }
        // Window functions are not regular aggregates — skip them entirely.
        Expression::WindowFunction { .. } => WalkAction::Skip,
        // The original walker treated these variants as leaves; preserve
        // that behavior here.
        Expression::Glob { .. }
        | Expression::RowValueConstructor(_)
        | Expression::Collate { .. } => WalkAction::Skip,
        _ => WalkAction::Descend,
    })
}

/// Find nested aggregate function in an expression
///
/// A nested aggregate is when one aggregate's arguments contain another aggregate,
/// e.g., `SUM(MIN(x))`. This is invalid in SQL.
///
/// Returns Some(inner_aggregate_name) if found, None otherwise.
pub fn find_nested_aggregate(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { args, order_by, .. } => {
            // Check if any argument contains an aggregate function
            for arg in args {
                if let Some(inner_name) = find_aggregate_in_expression(arg) {
                    return Some(inner_name);
                }
            }
            // Also check ORDER BY expressions for aggregate functions
            // e.g., group_concat(a ORDER BY max(d)) is invalid
            if let Some(order_items) = order_by {
                for item in order_items {
                    if let Some(inner_name) = find_aggregate_in_expression(&item.expr) {
                        return Some(inner_name);
                    }
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate with nested aggregate args
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions, not aggregates
                let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
                if !is_scalar_minmax {
                    // This is an aggregate - check for nested aggregates in args
                    for arg in args {
                        if let Some(inner_name) = find_aggregate_in_expression(arg) {
                            return Some(inner_name);
                        }
                    }
                }
            }
            // Check arguments recursively (for non-aggregate functions)
            for arg in args {
                if let Some(found) = find_nested_aggregate(arg) {
                    return Some(found);
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_nested_aggregate(left).or_else(|| find_nested_aggregate(right))
        }
        Expression::UnaryOp { expr, .. } => find_nested_aggregate(expr),
        Expression::Cast { expr, .. } => find_nested_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_nested_aggregate(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = find_nested_aggregate(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_nested_aggregate(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_nested_aggregate(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => find_nested_aggregate(expr),
        Expression::Between { expr, low, high, .. } => find_nested_aggregate(expr)
            .or_else(|| find_nested_aggregate(low))
            .or_else(|| find_nested_aggregate(high)),
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_nested_aggregate(expr) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_nested_aggregate(val) {
                    return Some(found);
                }
            }
            None
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_nested_aggregate(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Validate aggregate function argument counts in SELECT list
///
/// This validates that aggregate functions have the correct number of arguments:
/// - MIN, MAX, SUM, AVG, TOTAL require exactly 1 argument (no wildcard)
/// - COUNT allows 0-1 arguments (supports *)
/// - GROUP_CONCAT requires 1-2 arguments
///
/// Returns an error with SQLite-compatible message if validation fails.
pub fn validate_aggregate_arguments(select_list: &[SelectItem]) -> Result<(), ExecutorError> {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(agg_name) = check_aggregate_arg_count(expr) {
                return Err(ExecutorError::WrongNumberOfArguments { function_name: agg_name });
            }
        }
    }
    Ok(())
}

/// Validate that there are no nested aggregate functions in the SELECT list
///
/// Nested aggregates like `SUM(MIN(x))` are invalid in SQL.
/// Returns an error with SQLite-compatible message if nested aggregates are found.
///
/// Note: This uses the "misuse of aggregate function X()" format (with "function")
/// as SQLite detects this during name resolution, not during execution.
pub fn validate_no_nested_aggregates(select_list: &[SelectItem]) -> Result<(), ExecutorError> {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(inner_agg_name) = find_nested_aggregate(expr) {
                return Err(ExecutorError::MisuseOfAggregate { function_name: inner_agg_name });
            }
        }
    }
    Ok(())
}

/// Build a set of aggregate alias names from the SELECT list
///
/// An "aggregate alias" is an alias that refers to an expression containing
/// an aggregate function, e.g., `min(f1) AS m` makes `m` an aggregate alias.
pub fn build_aggregate_aliases(select_list: &[SelectItem]) -> HashSet<String> {
    let mut aliases = HashSet::new();

    for item in select_list {
        if let SelectItem::Expression { expr, alias: Some(alias_name), .. } = item {
            // Check if this expression contains an aggregate
            if expression_contains_aggregate(expr) {
                // Store alias in lowercase for case-insensitive matching
                aliases.insert(alias_name.to_lowercase());
            }
        }
    }

    aliases
}

/// Check if an expression contains an aggregate function
pub fn expression_contains_aggregate(expr: &Expression) -> bool {
    walk_first(expr, &|e| match e {
        Expression::AggregateFunction { .. } => WalkAction::Stop(()),
        Expression::Function { name, args, .. } if is_aggregate_function(name.as_str()) => {
            // Multi-arg MIN/MAX are scalar functions — keep walking children
            // (they may still contain a nested aggregate).
            let upper = name.to_uppercase();
            if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                WalkAction::Descend
            } else {
                WalkAction::Stop(())
            }
        }
        // Window functions are not aggregates in this context — skip entirely.
        Expression::WindowFunction { .. } => WalkAction::Skip,
        // The original walker treated these variants as leaves
        // (`_ => false`); preserve that by skipping them here.
        Expression::Glob { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::IsTruthValue { .. }
        | Expression::QuantifiedComparison { .. }
        | Expression::RowValueConstructor(_)
        | Expression::MatchAgainst { .. }
        | Expression::Collate { .. } => WalkAction::Skip,
        _ => WalkAction::Descend,
    })
    .is_some()
}

/// Find the first window function in an expression
///
/// Window functions can only appear in SELECT list and ORDER BY clauses.
/// This function finds window functions in expressions where they are not allowed
/// (e.g., WHERE, HAVING, GROUP BY, aggregate function arguments).
///
/// Returns the function name if found, None otherwise.
pub fn find_window_function_in_expression(expr: &Expression) -> Option<String> {
    walk_first(expr, &|e| match e {
        Expression::WindowFunction { function, .. } => WalkAction::Stop(function.name()),
        // The original walker treated these variants as leaves; preserve
        // that behavior here.
        Expression::Glob { .. }
        | Expression::RowValueConstructor(_)
        | Expression::Collate { .. } => WalkAction::Skip,
        _ => WalkAction::Descend,
    })
}

/// Check for misuse of aliased aggregates in HAVING clause
///
/// SQLite error: When an aggregate alias (e.g., `m` from `min(f1) AS m`) is used
/// inside another aggregate function in the HAVING clause (e.g., `HAVING max(m) < 10`),
/// it's an error. This function detects such misuse.
///
/// The `schema_columns` set contains column names from the actual table schema.
/// If a column reference matches an actual table column, it's NOT a reference to an alias,
/// even if an alias with the same name exists.
///
/// Returns Some(alias_name) if misuse is found, None otherwise.
fn find_aliased_aggregate_misuse_in_expression(
    expr: &Expression,
    aggregate_aliases: &HashSet<String>,
    schema_columns: &HashSet<String>,
    inside_aggregate: bool,
) -> Option<String> {
    match expr {
        // Check if this is an aggregate function - if so, mark that we're inside one
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    arg,
                    aggregate_aliases,
                    schema_columns,
                    true,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            let is_agg = is_aggregate_function(name.as_str());
            let upper = name.to_uppercase();
            // Multi-arg MIN/MAX are scalar functions
            let effectively_aggregate =
                is_agg && !(matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1);

            let new_inside_aggregate = inside_aggregate || effectively_aggregate;

            for arg in args {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    arg,
                    aggregate_aliases,
                    schema_columns,
                    new_inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        // Column reference - check if it's an aggregate alias used inside an aggregate
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // If this column exists in the actual table schema, it's a real column reference,
            // not a reference to a SELECT alias (even if an alias with the same name exists).
            // Table columns take precedence over aliases in HAVING clause.
            if schema_columns.contains(&column.to_lowercase()) {
                return None; // Real column, not an alias reference
            }

            if inside_aggregate && aggregate_aliases.contains(&column.to_lowercase()) {
                // Found misuse: aggregate alias used inside another aggregate
                Some(column.to_string())
            } else {
                None
            }
        }
        Expression::ColumnRef(_) => None, // Qualified refs can't be aliases
        // Recursively check composite expressions
        Expression::BinaryOp { left, right, .. } => find_aliased_aggregate_misuse_in_expression(
            left,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                right,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::UnaryOp { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Cast { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    op,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                        cond,
                        aggregate_aliases,
                        schema_columns,
                        inside_aggregate,
                    ) {
                        return Some(alias);
                    }
                }
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    &when_clause.result,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            if let Some(else_expr) = else_result {
                return find_aliased_aggregate_misuse_in_expression(
                    else_expr,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                );
            }
            None
        }
        Expression::IsNull { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Between { expr, low, high, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                low,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        })
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                high,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::InList { expr, values, .. } => {
            if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                expr,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            ) {
                return Some(alias);
            }
            for val in values {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    val,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        Expression::In { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Like { expr, pattern, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                pattern,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::Position { substring, string, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                substring,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
            .or_else(|| {
                find_aliased_aggregate_misuse_in_expression(
                    string,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                )
            })
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    rc,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            find_aliased_aggregate_misuse_in_expression(
                string,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }
        Expression::Extract { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Interval { value, .. } => find_aliased_aggregate_misuse_in_expression(
            value,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    child,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => None,
        Expression::QuantifiedComparison { expr, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                expr,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                left,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
            .or_else(|| {
                find_aliased_aggregate_misuse_in_expression(
                    right,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                )
            })
        }
        Expression::IsTruthValue { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        // Other expressions don't contain column refs that could be aggregate aliases
        _ => None,
    }
}

/// Validate HAVING clause for misuse of aliased aggregates
///
/// This should be called after building the aggregate aliases from the SELECT list.
/// Returns an error if an aggregate alias is used inside another aggregate in HAVING.
///
/// The `schema` parameter provides the actual table columns. If a column reference
/// matches an actual table column, it's not considered an alias reference, even if
/// an alias with the same name exists in the SELECT list.
pub fn validate_having_aliased_aggregates(
    having_clause: Option<&Expression>,
    select_list: &[SelectItem],
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    let Some(having_expr) = having_clause else {
        return Ok(());
    };

    // Build the set of aggregate aliases
    let aggregate_aliases = build_aggregate_aliases(select_list);

    if aggregate_aliases.is_empty() {
        return Ok(()); // No aggregate aliases to check
    }

    // Build the set of actual table column names (lowercase for case-insensitive matching)
    let schema_columns: HashSet<String> = schema
        .table_schemas
        .values()
        .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.to_lowercase()))
        .collect();

    // Check for misuse in HAVING clause
    if let Some(alias_name) = find_aliased_aggregate_misuse_in_expression(
        having_expr,
        &aggregate_aliases,
        &schema_columns,
        false,
    ) {
        return Err(ExecutorError::MisuseOfAliasedAggregate { alias_name });
    }

    Ok(())
}

/// Build a set of window function alias names from the SELECT list
///
/// A "window alias" is an alias that refers to an expression containing
/// a window function, e.g., `count(*) OVER() AS m` makes `m` a window alias.
pub fn build_window_aliases(select_list: &[SelectItem]) -> HashSet<String> {
    let mut aliases = HashSet::new();

    for item in select_list {
        if let SelectItem::Expression { expr, alias: Some(alias_name), .. } = item {
            if expression_contains_window_function(expr) {
                // Store alias in lowercase for case-insensitive matching
                aliases.insert(alias_name.to_lowercase());
            }
        }
    }

    aliases
}

/// Check if an expression contains a window function.
///
/// This walker has a deliberately narrow shape: it descends only through
/// `BinaryOp`, `UnaryOp`, `Cast`, `Function` and `Case`. All other
/// compound variants (e.g. `IsNull`, `Between`, `InList`) are treated as
/// leaves, matching the historical behavior. If you need to detect a
/// window function inside such variants, use a different walker.
fn expression_contains_window_function(expr: &Expression) -> bool {
    walk_first(expr, &|e| match e {
        Expression::WindowFunction { .. } => WalkAction::Stop(()),
        Expression::BinaryOp { .. }
        | Expression::UnaryOp { .. }
        | Expression::Cast { .. }
        | Expression::Function { .. }
        | Expression::Case { .. } => WalkAction::Descend,
        // Every other variant (including AggregateFunction, IsNull,
        // Between, etc.) is treated as a leaf to preserve original
        // behavior.
        _ => WalkAction::Skip,
    })
    .is_some()
}

/// Validate ORDER BY clause for misuse of aliased window functions
///
/// In SQLite, `ORDER BY (SELECT alias)` where alias refers to a window function is an error:
/// "misuse of aliased window function <alias>"
///
/// This happens when ORDER BY contains a scalar subquery like `(SELECT m)` and `m` is
/// an alias for a window function expression in the SELECT list.
pub fn validate_order_by_aliased_window_functions(
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    select_list: &[SelectItem],
) -> Result<(), ExecutorError> {
    let Some(order_items) = order_by else {
        return Ok(());
    };

    let window_aliases = build_window_aliases(select_list);
    if window_aliases.is_empty() {
        return Ok(());
    }

    for item in order_items {
        if let Some(alias_name) =
            find_aliased_window_misuse_in_order_by(&item.expr, &window_aliases)
        {
            return Err(ExecutorError::MisuseOfAliasedWindowFunction { alias_name });
        }
    }

    Ok(())
}

/// Find misuse of aliased window functions in ORDER BY expressions
///
/// Detects scalar subqueries like `(SELECT m)` where `m` matches a window function alias.
fn find_aliased_window_misuse_in_order_by(
    expr: &Expression,
    window_aliases: &HashSet<String>,
) -> Option<String> {
    match expr {
        Expression::ScalarSubquery(select_stmt) => {
            // Check if the scalar subquery is a simple `(SELECT alias)` pattern
            if select_stmt.select_list.len() == 1
                && select_stmt.from.is_none()
                && select_stmt.where_clause.is_none()
            {
                if let SelectItem::Expression { expr: Expression::ColumnRef(col_id), .. } =
                    &select_stmt.select_list[0]
                {
                    if col_id.table_canonical().is_none() {
                        let col_name = col_id.column_canonical().to_lowercase();
                        if window_aliases.contains(&col_name) {
                            return Some(col_name);
                        }
                    }
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_aliased_window_misuse_in_order_by(left, window_aliases)
                .or_else(|| find_aliased_window_misuse_in_order_by(right, window_aliases))
        }
        Expression::UnaryOp { expr, .. } => {
            find_aliased_window_misuse_in_order_by(expr, window_aliases)
        }
        _ => None,
    }
}

/// Validate ORDER BY for misuse of aggregates that are correlated to the outer
/// scope when the outer query is a window-aggregate query.
///
/// **Mirrors SQLite's rule** at `src/window.c::disallowAggregatesInOrderByCb`
/// (invoked from `sqlite3WindowRewrite` when the SELECT is not a regular
/// aggregate query — i.e. has windows but no GROUP BY/HAVING/aggregate
/// SELECT items). SQLite walks the ORDER BY (recursing through subqueries)
/// looking for `TK_AGG_FUNCTION` nodes whose `pAggInfo` is null — that is,
/// aggregate references that resolved to the *outer* scope rather than the
/// nested subquery's own scope. Such an aggregate is reported as
/// `"misuse of aggregate: <name>()"`.
///
/// We approximate the resolver's behaviour: an aggregate inside a scalar
/// subquery is "correlated to the outer window query" iff it has at least
/// one column-argument that
///   - is unqualified or refers to an outer table, AND
///   - does not resolve against the nested subquery's own FROM clause, AND
///   - resolves against the outer schema.
///
/// Test case 61.4.3 (must error):
/// ```sql
/// SELECT sum(a) OVER (ORDER BY a) FROM t1
/// ORDER BY (SELECT sum(a) FROM t2)   -- t2 has no column 'a'; 'a' is t1.a (outer)
/// ```
///
/// Test case 61.4.4 (must NOT error):
/// ```sql
/// SELECT sum(a) OVER (ORDER BY a) FROM t1
/// ORDER BY (SELECT sum(y) FROM t2)   -- 'y' is t2.y, fully local to subquery
/// ```
///
/// The walker only fires when the outer SELECT contains a window function;
/// non-window queries already validate ORDER BY through other paths.
pub fn validate_window_query_order_by_aggregates(
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    select_list: &[SelectItem],
    outer_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    let Some(order_items) = order_by else {
        return Ok(());
    };

    // Rule only applies to window-aggregate queries (has window function in
    // SELECT list). Plain aggregate or scalar queries use other validators.
    let has_window = select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_contains_window_function(expr),
        _ => false,
    });
    if !has_window {
        return Ok(());
    }

    for item in order_items {
        if let Some(name) =
            find_outer_correlated_agg_in_order_by_subquery(&item.expr, outer_schema, database)
        {
            return Err(ExecutorError::MisuseOfAggregateContext { function_name: name });
        }
    }

    Ok(())
}

/// Walk an ORDER BY expression looking for scalar subqueries whose body
/// contains an aggregate function correlated to the outer (window) query.
fn find_outer_correlated_agg_in_order_by_subquery(
    expr: &Expression,
    outer_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Option<String> {
    match expr {
        Expression::ScalarSubquery(stmt) => {
            // Build the set of column names visible inside the subquery's own
            // FROM clause. If we cannot determine it (e.g. nested subquery in
            // FROM), fall through to None — be conservative and don't flag
            // anything we can't prove correlated.
            let inner_columns = collect_from_clause_column_names(stmt.from.as_ref(), database)?;

            // Walk the subquery's SELECT list looking for aggregate functions
            // whose column args resolve to the outer scope.
            for item in &stmt.select_list {
                if let SelectItem::Expression { expr: inner, .. } = item {
                    if let Some(name) =
                        find_outer_correlated_aggregate(inner, &inner_columns, outer_schema)
                    {
                        return Some(name);
                    }
                }
            }
            None
        }
        // Recurse through composite expressions in ORDER BY (an ORDER BY
        // expression may e.g. add a constant to a subquery: `(SELECT sum(a)
        // FROM t2) + 1`).
        Expression::BinaryOp { left, right, .. } => {
            find_outer_correlated_agg_in_order_by_subquery(left, outer_schema, database).or_else(
                || find_outer_correlated_agg_in_order_by_subquery(right, outer_schema, database),
            )
        }
        Expression::UnaryOp { expr, .. } => {
            find_outer_correlated_agg_in_order_by_subquery(expr, outer_schema, database)
        }
        Expression::Cast { expr, .. } => {
            find_outer_correlated_agg_in_order_by_subquery(expr, outer_schema, database)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                if let Some(name) =
                    find_outer_correlated_agg_in_order_by_subquery(arg, outer_schema, database)
                {
                    return Some(name);
                }
            }
            None
        }
        _ => None,
    }
}

/// Collect column names visible inside a FROM clause by looking up each
/// referenced table in the catalog. Returns `None` if the FROM contains a
/// subquery / VALUES / unknown table — in those cases we conservatively skip
/// the check rather than risk a false positive.
fn collect_from_clause_column_names(
    from: Option<&vibesql_ast::FromClause>,
    database: &vibesql_storage::Database,
) -> Option<HashSet<String>> {
    let mut columns = HashSet::new();
    let from = from?;
    if collect_from_columns_recursive(from, database, &mut columns) {
        Some(columns)
    } else {
        None
    }
}

fn collect_from_columns_recursive(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
    columns: &mut HashSet<String>,
) -> bool {
    use vibesql_ast::FromClause;
    match from {
        FromClause::Table { name, .. } => {
            // Strip any schema qualifier ("main.t" → "t").
            let table_name = name.rsplit_once('.').map(|(_, t)| t).unwrap_or(name.as_str());
            if let Some(table) = database.get_table(table_name) {
                for col in &table.schema.columns {
                    columns.insert(col.name.to_lowercase());
                }
                true
            } else {
                // Unknown table — bail out and skip the check.
                false
            }
        }
        FromClause::Join { left, right, .. } => {
            collect_from_columns_recursive(left, database, columns)
                && collect_from_columns_recursive(right, database, columns)
        }
        // Subqueries / VALUES / table functions in FROM: too complex to
        // resolve here. Bail.
        FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => false,
    }
}

/// Recursively search an expression for an aggregate function whose column
/// arguments resolve to the outer scope rather than the inner subquery's FROM.
///
/// Returns the function name of the first such aggregate found.
fn find_outer_correlated_aggregate(
    expr: &Expression,
    inner_columns: &HashSet<String>,
    outer_schema: &CombinedSchema,
) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, order_by, filter, .. } => {
            if aggregate_args_correlate_outer(
                args,
                order_by.as_deref(),
                filter.as_deref(),
                inner_columns,
                outer_schema,
            ) {
                return Some(name.to_string());
            }
            // Recurse into args in case there's a deeper aggregate (which
            // would itself be a separate validation issue).
            for arg in args {
                if let Some(found) =
                    find_outer_correlated_aggregate(arg, inner_columns, outer_schema)
                {
                    return Some(found);
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
                if !is_scalar_minmax {
                    if args
                        .iter()
                        .any(|a| column_ref_resolves_to_outer(a, inner_columns, outer_schema))
                    {
                        return Some(name.to_string());
                    }
                }
            }
            for arg in args {
                if let Some(found) =
                    find_outer_correlated_aggregate(arg, inner_columns, outer_schema)
                {
                    return Some(found);
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_outer_correlated_aggregate(left, inner_columns, outer_schema)
                .or_else(|| find_outer_correlated_aggregate(right, inner_columns, outer_schema))
        }
        Expression::UnaryOp { expr, .. } => {
            find_outer_correlated_aggregate(expr, inner_columns, outer_schema)
        }
        Expression::Cast { expr, .. } => {
            find_outer_correlated_aggregate(expr, inner_columns, outer_schema)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) =
                    find_outer_correlated_aggregate(op, inner_columns, outer_schema)
                {
                    return Some(found);
                }
            }
            for w in when_clauses {
                for cond in &w.conditions {
                    if let Some(found) =
                        find_outer_correlated_aggregate(cond, inner_columns, outer_schema)
                    {
                        return Some(found);
                    }
                }
                if let Some(found) =
                    find_outer_correlated_aggregate(&w.result, inner_columns, outer_schema)
                {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_outer_correlated_aggregate(else_expr, inner_columns, outer_schema)
            } else {
                None
            }
        }
        // Don't recurse into nested scalar subqueries — those have their own
        // scope and are validated separately.
        _ => None,
    }
}

/// True if the column references inside an aggregate's args/order_by/filter
/// resolve to the outer schema rather than the subquery's own FROM columns.
fn aggregate_args_correlate_outer(
    args: &[Expression],
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    filter: Option<&Expression>,
    inner_columns: &HashSet<String>,
    outer_schema: &CombinedSchema,
) -> bool {
    let arg_correlated =
        args.iter().any(|a| column_ref_resolves_to_outer(a, inner_columns, outer_schema));
    let order_correlated = order_by.is_some_and(|items| {
        items.iter().any(|i| column_ref_resolves_to_outer(&i.expr, inner_columns, outer_schema))
    });
    let filter_correlated =
        filter.is_some_and(|f| column_ref_resolves_to_outer(f, inner_columns, outer_schema));
    arg_correlated || order_correlated || filter_correlated
}

/// True if the expression contains at least one ColumnRef that resolves to
/// the outer scope but not the inner subquery's own FROM columns.
///
/// "Resolves to outer" means: the column name is present in `outer_schema`
/// AND not present in `inner_columns`. A qualified reference whose table
/// name is not visible in the inner scope but whose name is in `outer_schema`
/// also counts as outer-correlated.
fn column_ref_resolves_to_outer(
    expr: &Expression,
    inner_columns: &HashSet<String>,
    outer_schema: &CombinedSchema,
) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => {
            let col_name = col_id.column_canonical().to_lowercase();
            // If the inner scope has this column, the reference is local.
            if inner_columns.contains(&col_name) {
                return false;
            }
            // Otherwise it must resolve to the outer scope to be a "misuse"
            // (an unresolvable reference would be reported elsewhere).
            outer_schema.has_column(&col_name)
        }
        Expression::BinaryOp { left, right, .. } => {
            column_ref_resolves_to_outer(left, inner_columns, outer_schema)
                || column_ref_resolves_to_outer(right, inner_columns, outer_schema)
        }
        Expression::UnaryOp { expr, .. } => {
            column_ref_resolves_to_outer(expr, inner_columns, outer_schema)
        }
        Expression::Cast { expr, .. } => {
            column_ref_resolves_to_outer(expr, inner_columns, outer_schema)
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(|a| column_ref_resolves_to_outer(a, inner_columns, outer_schema))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand
                .as_ref()
                .is_some_and(|e| column_ref_resolves_to_outer(e, inner_columns, outer_schema))
                || when_clauses.iter().any(|w| {
                    w.conditions
                        .iter()
                        .any(|c| column_ref_resolves_to_outer(c, inner_columns, outer_schema))
                        || column_ref_resolves_to_outer(&w.result, inner_columns, outer_schema)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| column_ref_resolves_to_outer(e, inner_columns, outer_schema))
        }
        Expression::IsNull { expr, .. } => {
            column_ref_resolves_to_outer(expr, inner_columns, outer_schema)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().any(|c| column_ref_resolves_to_outer(c, inner_columns, outer_schema))
        }
        // Don't recurse into nested subqueries — their scope is independent.
        _ => false,
    }
}

/// Check if an expression contains any column reference (recursively).
///
/// Used by `validate_subquery_context_misuse` to determine whether an
/// aggregate inside a bare (FROM-less) scalar subquery references columns
/// from the outer scope — those references are what make the aggregate a
/// "misuse" of the outer aggregation context.
///
/// Window functions are inspected only through their argument list, not
/// the OVER clause or FILTER (which have their own scope).
fn expression_contains_column_ref(expr: &Expression) -> bool {
    walk_first(expr, &|e| match e {
        Expression::ColumnRef(_) => WalkAction::Stop(()),
        // For AggregateFunction we only check `args` (matching the
        // original walker, which excluded ORDER BY and FILTER).
        Expression::AggregateFunction { args, .. } => {
            if args.iter().any(expression_contains_column_ref) {
                WalkAction::Stop(())
            } else {
                WalkAction::Skip
            }
        }
        // Window function: only the argument list is considered to
        // reference the outer scope. The OVER clause and FILTER are
        // evaluated against the window's own scope.
        Expression::WindowFunction { function, .. } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            if args.iter().any(expression_contains_column_ref) {
                WalkAction::Stop(())
            } else {
                WalkAction::Skip
            }
        }
        // The original walker treated these variants as leaves
        // (`_ => false`), so preserve that behavior here.
        Expression::Glob { .. } | Expression::Collate { .. } | Expression::MatchAgainst { .. } => {
            WalkAction::Skip
        }
        _ => WalkAction::Descend,
    })
    .is_some()
}

/// Find the first aggregate function whose args/order_by/filter reference a
/// column. Returns the function name if found, None otherwise.
///
/// This is used inside bare (FROM-less) scalar subqueries: any column there
/// is necessarily an outer reference, so an aggregate referencing one is a
/// misuse of the outer aggregation context.
fn find_outer_referencing_aggregate(expr: &Expression) -> Option<String> {
    walk_first(expr, &|e| match e {
        Expression::AggregateFunction { name, args, order_by, filter, .. } => {
            let arg_refs = args.iter().any(expression_contains_column_ref);
            let order_refs = order_by.as_ref().is_some_and(|items| {
                items.iter().any(|item| expression_contains_column_ref(&item.expr))
            });
            let filter_refs = filter.as_ref().is_some_and(|f| expression_contains_column_ref(f));
            if arg_refs || order_refs || filter_refs {
                WalkAction::Stop(name.to_string())
            } else {
                // The aggregate itself does not reference an outer column,
                // but a deeper aggregate (in args) might. Descend into
                // args only — order_by / filter are already handled above.
                args.iter()
                    .find_map(find_outer_referencing_aggregate)
                    .map_or(WalkAction::Skip, WalkAction::Stop)
            }
        }
        Expression::Function { name, args, .. } if is_aggregate_function(name.as_str()) => {
            let upper = name.to_uppercase();
            let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
            if !is_scalar_minmax && args.iter().any(expression_contains_column_ref) {
                WalkAction::Stop(name.to_string())
            } else {
                // Descend into args looking for nested aggregates.
                WalkAction::Descend
            }
        }
        // The original walker only recursed into a subset of compound
        // expressions; everything else fell into `_ => None`. Preserve
        // that behavior by skipping the variants the original ignored.
        Expression::IsDistinctFrom { .. }
        | Expression::IsTruthValue { .. }
        | Expression::Position { .. }
        | Expression::Trim { .. }
        | Expression::Extract { .. }
        | Expression::Interval { .. }
        | Expression::QuantifiedComparison { .. }
        | Expression::RowValueConstructor(_)
        | Expression::MatchAgainst { .. }
        | Expression::Collate { .. }
        | Expression::Glob { .. }
        | Expression::WindowFunction { .. } => WalkAction::Skip,
        _ => WalkAction::Descend,
    })
}

/// Context in which a subquery appears, controlling how
/// `validate_subquery_context_misuse` treats outer-correlated aggregates inside
/// bare scalar subqueries.
///
/// SQLite has two distinct behaviors for a bare scalar subquery whose body
/// contains an aggregate referencing an *outer* column:
///
/// 1. **WHERE / HAVING / ORDER BY** (and arguments to other outer functions):
///    SQLite raises `"misuse of aggregate: X()"`. The outer aggregation context
///    is not implicitly borrowed in these positions.
///
/// 2. **SELECT list** (issue #5104): SQLite implicitly collapses the outer
///    query into a single-row aggregate, with the inner aggregate computed
///    over all outer rows. The aggregate is *not* a misuse here — it's
///    well-defined and produces a single output row.
///
/// We therefore parameterize the validator on context: SELECT-list calls pass
/// [`SubqueryContext::SelectList`] to skip the outer-correlated-aggregate
/// rejection (it will be handled by the implicit-collapse path). All other
/// callers pass [`SubqueryContext::WhereOrEqual`] to preserve the SQLite
/// rejection.
///
/// Row-value misuse is checked in *both* contexts — `(SELECT (a, b))` is
/// always an error regardless of where the subquery appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryContext {
    /// Bare scalar subquery in WHERE, HAVING, ORDER BY, or as an argument to
    /// an outer function. Outer-correlated aggregates → "misuse of aggregate".
    WhereOrEqual,
    /// Bare scalar subquery in the SELECT list. Outer-correlated aggregates
    /// trigger SQLite's implicit-outer-aggregate-collapse instead of a misuse
    /// error (#5104).
    SelectList,
}

/// Detect aggregate / row-value misuse inside *bare* scalar subqueries.
///
/// SQLite has a special rule: when a scalar subquery has no FROM clause and
/// its body contains an aggregate function whose argument or FILTER references
/// an *outer* column, the aggregate implicitly borrows the outer aggregation
/// context. SQLite reports this as `"misuse of aggregate: X()"` *unless* the
/// subquery appears in the SELECT list, where it instead triggers
/// implicit-outer-aggregate-collapse (#5104).
///
/// Examples that trigger this rule (in WHERE/HAVING/ORDER BY context):
/// - `WHERE (SELECT AVG(0) FILTER(WHERE outer.c))`  → "misuse of aggregate: AVG()"
/// - `ntile((SELECT sum(x))) OVER (...)`             → "misuse of aggregate: sum()"
///
/// In SELECT-list context (#5104), the same patterns are *allowed* and trigger
/// outer-aggregate collapse:
/// - `SELECT (SELECT avg(a)) FROM t2` → outer collapses to single row
///
/// Additionally, when a bare scalar subquery's sole projection is a
/// `RowValueConstructor` with multiple elements, that's `RowValueMisused`
/// (a row value where a scalar is required) — checked in both contexts.
///
/// Note: pure *window* functions inside bare scalar subqueries are NOT flagged
/// here — SQLite allows them (they are evaluated row-by-row against the outer
/// scope). Misuse for window functions is detected through other paths
/// (WHERE-clause walker, GROUP BY positional refs, etc.).
///
/// This walker is meant to be called on expressions appearing in the outer
/// scope — typically WHERE, HAVING, ORDER BY, or arguments to outer functions
/// (with `WhereOrEqual`), or the SELECT list (with `SelectList`).
/// It deliberately does NOT recurse into scalar subqueries that have a FROM
/// clause (those have their own aggregation context and are validated
/// independently).
///
/// To preserve SQLite's exact error text (which uses the function name as
/// originally written by the user), we walk the inner expression and report
/// the first such aggregate's name.
pub fn validate_subquery_context_misuse(
    expr: &Expression,
    context: SubqueryContext,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::ScalarSubquery(stmt) => {
            // Walk the full compound chain: the head SELECT plus every
            // set-operation arm (UNION/INTERSECT/EXCEPT). Each arm carries
            // its own FROM clause, and SQLite flags misuse in *any* FROM-less
            // arm — e.g. `(SELECT 2,2 UNION SELECT sum(b),... )` errors even
            // though the head arm is clean (window1.test 71.0).
            let mut arm: &vibesql_ast::SelectStmt = stmt;
            loop {
                // Only "bare" arms (no FROM) re-borrow the outer context.
                if arm.from.is_none() {
                    // Check for row value misuse (sole projection is a row value
                    // constructor with multiple elements). This is an error in
                    // any context.
                    if arm.select_list.len() == 1 {
                        if let SelectItem::Expression {
                            expr: Expression::RowValueConstructor(items),
                            ..
                        } = &arm.select_list[0]
                        {
                            if items.len() > 1 {
                                return Err(ExecutorError::RowValueMisused);
                            }
                        }
                    }

                    // Check each projection for an aggregate function. We only
                    // flag aggregates whose body references something outside the
                    // bare subquery's own scope (i.e., references a column — the
                    // bare subquery has no tables, so any column is an outer ref).
                    //
                    // In SELECT-list context, this is *not* a misuse; it triggers
                    // implicit-outer-aggregate-collapse instead (#5104). Skip the
                    // rejection so the executor's collapse path can handle it.
                    if context == SubqueryContext::WhereOrEqual {
                        for item in &arm.select_list {
                            if let SelectItem::Expression { expr: inner, .. } = item {
                                if let Some(name) = find_outer_referencing_aggregate(inner) {
                                    return Err(ExecutorError::MisuseOfAggregateContext {
                                        function_name: name,
                                    });
                                }
                            }
                        }
                    }

                    // Recurse into nested expressions inside the bare arm's
                    // SELECT list as well — e.g. (SELECT (SELECT sum(x))).
                    // Inner subqueries inherit the same context: nested bare
                    // subqueries in a SELECT-list context still treat their own
                    // SELECT-list position as SELECT-list.
                    for item in &arm.select_list {
                        if let SelectItem::Expression { expr: inner, .. } = item {
                            validate_subquery_context_misuse(inner, context)?;
                        }
                    }
                }

                match arm.set_operation.as_ref() {
                    Some(set_op) => arm = &set_op.right,
                    None => break,
                }
            }
            Ok(())
        }
        // For non-subquery expressions, recurse into their children. We do
        // NOT recurse into AggregateFunction / WindowFunction children here —
        // those are checked by their own dedicated validators which understand
        // the surrounding context (FILTER, ORDER BY, etc.).
        Expression::BinaryOp { left, right, .. } => {
            validate_subquery_context_misuse(left, context)?;
            validate_subquery_context_misuse(right, context)
        }
        Expression::UnaryOp { expr, .. } => validate_subquery_context_misuse(expr, context),
        Expression::IsNull { expr, .. } => validate_subquery_context_misuse(expr, context),
        Expression::IsDistinctFrom { left, right, .. } => {
            validate_subquery_context_misuse(left, context)?;
            validate_subquery_context_misuse(right, context)
        }
        Expression::IsTruthValue { expr, .. } => validate_subquery_context_misuse(expr, context),
        Expression::Cast { expr, .. } => validate_subquery_context_misuse(expr, context),
        Expression::Like { expr, pattern, .. } => {
            validate_subquery_context_misuse(expr, context)?;
            validate_subquery_context_misuse(pattern, context)
        }
        Expression::Between { expr, low, high, .. } => {
            validate_subquery_context_misuse(expr, context)?;
            validate_subquery_context_misuse(low, context)?;
            validate_subquery_context_misuse(high, context)
        }
        Expression::InList { expr, values, .. } => {
            validate_subquery_context_misuse(expr, context)?;
            for v in values {
                validate_subquery_context_misuse(v, context)?;
            }
            Ok(())
        }
        Expression::In { expr, .. } => validate_subquery_context_misuse(expr, context),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_subquery_context_misuse(op, context)?;
            }
            for w in when_clauses {
                for cond in &w.conditions {
                    validate_subquery_context_misuse(cond, context)?;
                }
                validate_subquery_context_misuse(&w.result, context)?;
            }
            if let Some(else_expr) = else_result {
                validate_subquery_context_misuse(else_expr, context)?;
            }
            Ok(())
        }
        Expression::Function { args, .. } => {
            for arg in args {
                validate_subquery_context_misuse(arg, context)?;
            }
            Ok(())
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            // Aggregate function arguments are evaluated in WHERE/HAVING-like
            // context (their value must be a scalar). Subqueries inside their
            // args are NOT in SELECT-list position, so reset context.
            for arg in args {
                validate_subquery_context_misuse(arg, SubqueryContext::WhereOrEqual)?;
            }
            if let Some(f) = filter {
                validate_subquery_context_misuse(f, SubqueryContext::WhereOrEqual)?;
            }
            if let Some(items) = order_by {
                for item in items {
                    validate_subquery_context_misuse(&item.expr, SubqueryContext::WhereOrEqual)?;
                }
            }
            Ok(())
        }
        Expression::WindowFunction { function, .. } => {
            // Only recurse into the function's arguments — the OVER clause has
            // its own context and may legally contain aggregates / nested
            // subqueries (window functions are evaluated AFTER aggregation).
            // Window function arguments are like aggregate-function arguments
            // (must be scalar) — reset context to WhereOrEqual.
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for a in args {
                validate_subquery_context_misuse(a, SubqueryContext::WhereOrEqual)?;
            }
            Ok(())
        }
        Expression::QuantifiedComparison { expr, .. } => {
            validate_subquery_context_misuse(expr, context)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                validate_subquery_context_misuse(child, context)?;
            }
            Ok(())
        }
        Expression::RowValueConstructor(children) => {
            for child in children {
                validate_subquery_context_misuse(child, context)?;
            }
            Ok(())
        }
        // Leaf expressions and EXISTS (which has its own scope) — nothing to check
        _ => Ok(()),
    }
}

/// Recursively validate that no SELECT statement (the given one or any nested
/// subquery) has a GROUP BY whose expression — directly or after positional
/// resolution against its own SELECT list — is a window function.
///
/// SQLite reports this as `"misuse of window function X()"` at *prepare* time,
/// before any rows are fetched, so it must be detected during query
/// preparation rather than only at the execution path of the inner SELECT.
///
/// The check fires both on the GROUP BY expression as written and on its
/// alias-resolved form so that `GROUP BY 1` referring to a window-function
/// SELECT item (window1.test 47.2) is also caught.
pub fn validate_group_by_window_misuse(
    stmt: &vibesql_ast::SelectStmt,
) -> Result<(), ExecutorError> {
    // 1. Check this statement's own GROUP BY for window misuse, with
    //    positional/alias resolution against its SELECT list.
    if let Some(ref group_by_clause) = stmt.group_by {
        for (term_index, group_expr) in group_by_clause.all_expressions().iter().enumerate() {
            if let Some(window_name) = find_window_function_in_expression(group_expr) {
                return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
            }
            let resolved = crate::select::grouping::resolve_group_by_alias(
                group_expr,
                &stmt.select_list,
                term_index,
            )?;
            if let Some(window_name) = find_window_function_in_expression(&resolved) {
                return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
            }
        }
    }

    // 2. Recurse into all expressions of this statement to find nested SELECTs.
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            walk_expr_for_subquery_group_by(expr)?;
        }
    }
    if let Some(where_expr) = stmt.where_clause.as_ref() {
        walk_expr_for_subquery_group_by(where_expr)?;
    }
    if let Some(having_expr) = stmt.having.as_ref() {
        walk_expr_for_subquery_group_by(having_expr)?;
    }
    if let Some(group_by) = stmt.group_by.as_ref() {
        for expr in group_by.all_expressions() {
            walk_expr_for_subquery_group_by(expr)?;
        }
    }
    if let Some(order_by) = stmt.order_by.as_ref() {
        for item in order_by {
            walk_expr_for_subquery_group_by(&item.expr)?;
        }
    }

    // 3. Recurse into FROM clause subqueries (derived tables, JOINs).
    if let Some(from) = stmt.from.as_ref() {
        walk_from_for_subquery_group_by(from)?;
    }

    // 4. Recurse into CTEs.
    if let Some(ctes) = stmt.with_clause.as_ref() {
        for cte in ctes {
            validate_group_by_window_misuse(&cte.query)?;
        }
    }

    Ok(())
}

/// Walk an expression tree, finding nested ScalarSubquery / Exists subqueries
/// and validating their GROUP BY clauses for window-function misuse.
fn walk_expr_for_subquery_group_by(expr: &Expression) -> Result<(), ExecutorError> {
    match expr {
        Expression::ScalarSubquery(stmt) => validate_group_by_window_misuse(stmt),
        Expression::Exists { subquery, .. } => validate_group_by_window_misuse(subquery),
        Expression::BinaryOp { left, right, .. } => {
            walk_expr_for_subquery_group_by(left)?;
            walk_expr_for_subquery_group_by(right)
        }
        Expression::UnaryOp { expr, .. } => walk_expr_for_subquery_group_by(expr),
        Expression::IsNull { expr, .. } => walk_expr_for_subquery_group_by(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            walk_expr_for_subquery_group_by(left)?;
            walk_expr_for_subquery_group_by(right)
        }
        Expression::IsTruthValue { expr, .. } => walk_expr_for_subquery_group_by(expr),
        Expression::Cast { expr, .. } => walk_expr_for_subquery_group_by(expr),
        Expression::Like { expr, pattern, .. } => {
            walk_expr_for_subquery_group_by(expr)?;
            walk_expr_for_subquery_group_by(pattern)
        }
        Expression::Between { expr, low, high, .. } => {
            walk_expr_for_subquery_group_by(expr)?;
            walk_expr_for_subquery_group_by(low)?;
            walk_expr_for_subquery_group_by(high)
        }
        Expression::InList { expr, values, .. } => {
            walk_expr_for_subquery_group_by(expr)?;
            for v in values {
                walk_expr_for_subquery_group_by(v)?;
            }
            Ok(())
        }
        Expression::In { expr, subquery, .. } => {
            walk_expr_for_subquery_group_by(expr)?;
            validate_group_by_window_misuse(subquery)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                walk_expr_for_subquery_group_by(op)?;
            }
            for w in when_clauses {
                for cond in &w.conditions {
                    walk_expr_for_subquery_group_by(cond)?;
                }
                walk_expr_for_subquery_group_by(&w.result)?;
            }
            if let Some(else_expr) = else_result {
                walk_expr_for_subquery_group_by(else_expr)?;
            }
            Ok(())
        }
        Expression::Function { args, .. } => {
            for arg in args {
                walk_expr_for_subquery_group_by(arg)?;
            }
            Ok(())
        }
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            for arg in args {
                walk_expr_for_subquery_group_by(arg)?;
            }
            if let Some(f) = filter {
                walk_expr_for_subquery_group_by(f)?;
            }
            if let Some(items) = order_by {
                for item in items {
                    walk_expr_for_subquery_group_by(&item.expr)?;
                }
            }
            Ok(())
        }
        Expression::WindowFunction { function, .. } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for a in args {
                walk_expr_for_subquery_group_by(a)?;
            }
            Ok(())
        }
        Expression::QuantifiedComparison { expr, subquery, .. } => {
            walk_expr_for_subquery_group_by(expr)?;
            validate_group_by_window_misuse(subquery)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                walk_expr_for_subquery_group_by(child)?;
            }
            Ok(())
        }
        Expression::RowValueConstructor(children) => {
            for child in children {
                walk_expr_for_subquery_group_by(child)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Walk a FROM clause to find nested SELECT statements (derived tables, JOINs).
fn walk_from_for_subquery_group_by(from: &vibesql_ast::FromClause) -> Result<(), ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { .. } => Ok(()),
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            walk_from_for_subquery_group_by(left)?;
            walk_from_for_subquery_group_by(right)?;
            if let Some(expr) = condition {
                walk_expr_for_subquery_group_by(expr)?;
            }
            Ok(())
        }
        vibesql_ast::FromClause::Subquery { query, .. } => validate_group_by_window_misuse(query),
        vibesql_ast::FromClause::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    walk_expr_for_subquery_group_by(expr)?;
                }
            }
            Ok(())
        }
        vibesql_ast::FromClause::TableFunction { args, .. } => {
            for expr in args {
                walk_expr_for_subquery_group_by(expr)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{BinaryOperator, ColumnIdentifier, FunctionIdentifier, UnaryOperator};
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    use super::*;

    /// Create a schema with f1 and f2 columns (for aliased aggregate tests)
    fn make_f1_f2_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "f1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "f2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("test1".to_string(), columns);
        CombinedSchema::from_table("test1".to_string(), table_schema)
    }

    #[test]
    fn test_min_star_invalid() {
        // MIN(*) should be invalid - returns error with function name (preserving original case)
        let expr = Expression::AggregateFunction {
            name: FunctionIdentifier::new("MIN"),
            distinct: false,
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("*", false))],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MIN(*) should be invalid");
        assert_eq!(result.unwrap(), "MIN"); // Preserves original case
    }

    #[test]
    fn test_max_star_invalid() {
        // MAX(*) should be invalid
        let expr = Expression::AggregateFunction {
            name: FunctionIdentifier::new("MAX"),
            distinct: false,
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("*", false))],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MAX(*) should be invalid");
        assert_eq!(result.unwrap(), "MAX"); // Preserves original case
    }

    #[test]
    fn test_min_no_args_invalid() {
        // MIN() with no arguments should be invalid
        let expr = Expression::AggregateFunction {
            name: FunctionIdentifier::new("MIN"),
            distinct: false,
            args: vec![],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MIN() should be invalid");
        assert_eq!(result.unwrap(), "MIN"); // Preserves original case
    }

    #[test]
    fn test_validate_aggregate_arguments() {
        // Test the public function
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: FunctionIdentifier::new("MIN"),
                distinct: false,
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("*", false))],
                order_by: None,
                filter: None,
            },
            alias: None,
            source_text: None,
        }];
        let result = validate_aggregate_arguments(&select_list);
        assert!(result.is_err());
    }

    #[test]
    fn test_having_with_aliased_aggregate_inside_aggregate() {
        // SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING max(m+5)<10
        // The alias 'm' refers to an aggregate and is used inside max() - should error
        // Note: 'm' is NOT a column in the table, so it's treated as an alias reference
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: FunctionIdentifier::new("min"),
                distinct: false,
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("f1", false))],
                order_by: None,
                filter: None,
            },
            alias: Some("m".to_string()),
            source_text: None,
        }];

        // HAVING max(m+5)<10
        let having_expr = Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::AggregateFunction {
                name: FunctionIdentifier::new("max"),
                distinct: false,
                args: vec![Expression::BinaryOp {
                    op: BinaryOperator::Plus,
                    left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("m", false))),
                    right: Box::new(Expression::Literal(SqlValue::Integer(5))),
                }],
                order_by: None,
                filter: None,
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        // Use schema with f1, f2 - 'm' is not a column, so it's an alias reference
        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::MisuseOfAliasedAggregate { alias_name }) => {
                assert_eq!(alias_name, "m");
            }
            _ => panic!("Expected MisuseOfAliasedAggregate error"),
        }
    }

    #[test]
    fn test_having_with_aggregate_alias_not_inside_aggregate() {
        // SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING m>0
        // The alias 'm' refers to an aggregate but is NOT used inside another aggregate
        // This is a gray area - SQLite actually treats this as an error too,
        // because the alias cannot be resolved in HAVING context at all.
        // For now, we only detect the case where it's inside an aggregate.
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: FunctionIdentifier::new("min"),
                distinct: false,
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("f1", false))],
                order_by: None,
                filter: None,
            },
            alias: Some("m".to_string()),
            source_text: None,
        }];

        // HAVING m>0 - alias used directly, not inside an aggregate
        let having_expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("m", false))),
            right: Box::new(Expression::Literal(SqlValue::Integer(0))),
        };

        // This should pass our current validation (alias not inside aggregate)
        // SQLite would error on this too, but we'll catch it later during evaluation
        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_without_aggregate_alias() {
        // SELECT count(*) FROM test1 GROUP BY f1 HAVING f1>0
        // No aliased aggregate, should pass
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: FunctionIdentifier::new("count"),
                distinct: false,
                args: vec![Expression::Wildcard],
                order_by: None,
                filter: None,
            },
            alias: None, // No alias
            source_text: None,
        }];

        let having_expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("f1", false))),
            right: Box::new(Expression::Literal(SqlValue::Integer(0))),
        };

        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_with_non_aggregate_alias() {
        // SELECT f1 AS x, count(*) FROM test1 GROUP BY f1 HAVING max(x)<10
        // 'x' is an alias for f1, not an aggregate - should pass
        let select_list = vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("f1", false)),
                alias: Some("x".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: FunctionIdentifier::new("count"),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                    order_by: None,
                    filter: None,
                },
                alias: None,
                source_text: None,
            },
        ];

        // HAVING max(x)<10 - 'x' is not an aggregate alias
        let having_expr = Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::AggregateFunction {
                name: FunctionIdentifier::new("max"),
                distinct: false,
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("x", false))],
                order_by: None,
                filter: None,
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_alias_shadows_column_uses_column() {
        // SELECT - col2 * - AVG(-col2) AS col0 FROM tab0 GROUP BY col2 HAVING AVG(col0) IS NULL
        // The alias 'col0' happens to match the aggregate expression, but 'col0' is also
        // a real column in the table. In this case, the HAVING clause refers to the
        // actual column col0, NOT the alias. This should NOT be an error.
        let select_list = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                op: BinaryOperator::Multiply,
                left: Box::new(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("col2", false))),
                }),
                right: Box::new(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expression::AggregateFunction {
                        name: FunctionIdentifier::new("AVG"),
                        distinct: false,
                        args: vec![Expression::UnaryOp {
                            op: UnaryOperator::Minus,
                            expr: Box::new(Expression::ColumnRef(ColumnIdentifier::simple(
                                "col2", false,
                            ))),
                        }],
                        order_by: None,
                        filter: None,
                    }),
                }),
            },
            alias: Some("col0".to_string()), // Alias matches a column name!
            source_text: None,
        }];

        // HAVING AVG(col0) IS NULL - col0 is a real column, not the alias
        let having_expr = Expression::IsNull {
            expr: Box::new(Expression::AggregateFunction {
                name: FunctionIdentifier::new("AVG"),
                distinct: false,
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("col0", false))],
                order_by: None,
                filter: None,
            }),
            negated: false,
        };

        // Schema with col0, col1, col2 - col0 exists as an actual column
        let columns = vec![
            ColumnSchema {
                name: "col0".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "col1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "col2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("tab0".to_string(), columns);
        let schema = CombinedSchema::from_table("tab0".to_string(), table_schema);

        // This should pass - col0 refers to the real column, not the alias
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok(), "Expected Ok but got {:?}", result);
    }

    #[test]
    fn test_build_aggregate_aliases() {
        // Test the helper function
        let select_list = vec![
            SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: FunctionIdentifier::new("min"),
                    distinct: false,
                    args: vec![Expression::ColumnRef(ColumnIdentifier::simple("f1", false))],
                    order_by: None,
                    filter: None,
                },
                alias: Some("m".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("f2", false)),
                alias: Some("col2".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                // coalesce(min(f1)+5, 11) AS m2
                expr: Expression::Function {
                    name: FunctionIdentifier::new("coalesce"),
                    args: vec![
                        Expression::BinaryOp {
                            op: BinaryOperator::Plus,
                            left: Box::new(Expression::AggregateFunction {
                                name: FunctionIdentifier::new("min"),
                                distinct: false,
                                args: vec![Expression::ColumnRef(ColumnIdentifier::simple(
                                    "f1", false,
                                ))],
                                order_by: None,
                                filter: None,
                            }),
                            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
                        },
                        Expression::Literal(SqlValue::Integer(11)),
                    ],
                    character_unit: None,
                },
                alias: Some("m2".to_string()),
                source_text: None,
            },
        ];

        let aliases = build_aggregate_aliases(&select_list);
        assert!(aliases.contains("m")); // min(f1) AS m is an aggregate alias
        assert!(!aliases.contains("col2")); // f2 AS col2 is NOT an aggregate alias
        assert!(aliases.contains("m2")); // coalesce(min(f1)+5, 11) contains an aggregate
    }

    // Helper to build a bare scalar subquery with a given SELECT list.
    fn make_bare_subquery(select_list: Vec<SelectItem>) -> Expression {
        Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
            with_clause: None,
            distinct: false,
            select_list,
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
        }))
    }

    #[test]
    fn test_subquery_misuse_aggregate_with_outer_filter_ref() {
        // (SELECT AVG(0) FILTER(WHERE outer_col)) — bare subquery, FILTER ref outer col
        // Expected: misuse of aggregate: AVG()
        let inner_agg = Expression::AggregateFunction {
            name: FunctionIdentifier::new("AVG"),
            distinct: false,
            args: vec![Expression::Literal(SqlValue::Integer(0))],
            order_by: None,
            filter: Some(Box::new(Expression::ColumnRef(ColumnIdentifier::simple(
                "outer_col",
                false,
            )))),
        };
        let subquery = make_bare_subquery(vec![SelectItem::Expression {
            expr: inner_agg,
            alias: None,
            source_text: None,
        }]);

        // WhereOrEqual context: SQLite reports "misuse of aggregate" here.
        let result = validate_subquery_context_misuse(&subquery, SubqueryContext::WhereOrEqual);
        match result {
            Err(ExecutorError::MisuseOfAggregateContext { function_name }) => {
                assert_eq!(function_name, "AVG");
            }
            other => panic!("expected MisuseOfAggregateContext(AVG), got {:?}", other),
        }

        // SelectList context (#5104): allowed — triggers implicit-collapse
        // instead of erroring.
        let result_select_list =
            validate_subquery_context_misuse(&subquery, SubqueryContext::SelectList);
        assert!(
            result_select_list.is_ok(),
            "SelectList context should allow outer-correlated aggregate, got {:?}",
            result_select_list
        );
    }

    #[test]
    fn test_subquery_misuse_aggregate_with_outer_arg_ref() {
        // (SELECT sum(x)) — bare subquery, sum's arg is an outer column ref
        // Expected: misuse of aggregate: sum() in WhereOrEqual context;
        // allowed in SelectList context (#5104).
        let inner_agg = Expression::AggregateFunction {
            name: FunctionIdentifier::new("sum"),
            distinct: false,
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("x", false))],
            order_by: None,
            filter: None,
        };
        let subquery = make_bare_subquery(vec![SelectItem::Expression {
            expr: inner_agg,
            alias: None,
            source_text: None,
        }]);

        let result = validate_subquery_context_misuse(&subquery, SubqueryContext::WhereOrEqual);
        match result {
            Err(ExecutorError::MisuseOfAggregateContext { function_name }) => {
                assert_eq!(function_name, "sum");
            }
            other => panic!("expected MisuseOfAggregateContext(sum), got {:?}", other),
        }

        let result_select_list =
            validate_subquery_context_misuse(&subquery, SubqueryContext::SelectList);
        assert!(
            result_select_list.is_ok(),
            "SelectList context should allow outer-correlated aggregate, got {:?}",
            result_select_list
        );
    }

    #[test]
    fn test_subquery_misuse_aggregate_with_constant_args_does_not_fire() {
        // (SELECT sum(0)) — bare subquery whose aggregate has no column refs.
        // Should NOT error in either context (no outer reference).
        let inner_agg = Expression::AggregateFunction {
            name: FunctionIdentifier::new("sum"),
            distinct: false,
            args: vec![Expression::Literal(SqlValue::Integer(0))],
            order_by: None,
            filter: None,
        };
        let subquery = make_bare_subquery(vec![SelectItem::Expression {
            expr: inner_agg,
            alias: None,
            source_text: None,
        }]);

        let result = validate_subquery_context_misuse(&subquery, SubqueryContext::WhereOrEqual);
        assert!(result.is_ok(), "constant aggregate in bare subquery should be OK");
        let result_select =
            validate_subquery_context_misuse(&subquery, SubqueryContext::SelectList);
        assert!(result_select.is_ok(), "constant aggregate in SelectList should be OK");
    }

    #[test]
    fn test_subquery_row_value_misused() {
        // (SELECT (a, b)) — bare subquery returning a row value where a scalar
        // is expected. Expected: row value misused — in BOTH contexts.
        let row_value = Expression::RowValueConstructor(vec![
            Expression::ColumnRef(ColumnIdentifier::simple("a", false)),
            Expression::ColumnRef(ColumnIdentifier::simple("b", false)),
        ]);
        let subquery = make_bare_subquery(vec![SelectItem::Expression {
            expr: row_value,
            alias: None,
            source_text: None,
        }]);

        let result = validate_subquery_context_misuse(&subquery, SubqueryContext::WhereOrEqual);
        assert!(matches!(result, Err(ExecutorError::RowValueMisused)));

        let result_select =
            validate_subquery_context_misuse(&subquery, SubqueryContext::SelectList);
        assert!(
            matches!(result_select, Err(ExecutorError::RowValueMisused)),
            "row value misuse should still error in SelectList, got {:?}",
            result_select
        );
    }

    #[test]
    fn test_subquery_window_function_does_not_fire() {
        // (SELECT count(a) OVER ()) — bare subquery with a window function
        // referencing outer column. SQLite allows this; we should NOT flag it.
        let inner_window = Expression::WindowFunction {
            function: vibesql_ast::WindowFunctionSpec::Aggregate {
                name: FunctionIdentifier::new("count"),
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("a", false))],
                filter: None,
            },
            over: vibesql_ast::WindowSpec {
                base_window_name: None,
                partition_by: None,
                order_by: None,
                frame: None,
            },
        };
        let subquery = make_bare_subquery(vec![SelectItem::Expression {
            expr: inner_window,
            alias: None,
            source_text: None,
        }]);

        let result = validate_subquery_context_misuse(&subquery, SubqueryContext::WhereOrEqual);
        assert!(
            result.is_ok(),
            "window function in bare subquery should not be flagged, got {:?}",
            result
        );
    }

    // ----- validate_group_by_window_misuse (#5093) -----

    /// Build a SELECT statement with a single FROM table reference and a given
    /// SELECT list / WHERE / GROUP BY for testing.
    fn make_select(
        select_list: Vec<SelectItem>,
        from_table: Option<&str>,
        where_clause: Option<Expression>,
        group_by: Option<vibesql_ast::GroupByClause>,
    ) -> vibesql_ast::SelectStmt {
        vibesql_ast::SelectStmt {
            with_clause: None,
            distinct: false,
            select_list,
            into_table: None,
            into_variables: None,
            from: from_table.map(|name| vibesql_ast::FromClause::Table {
                index_hint: None,
                name: name.to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause,
            group_by,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    fn make_window_sum_a() -> Expression {
        Expression::WindowFunction {
            function: vibesql_ast::WindowFunctionSpec::Aggregate {
                name: FunctionIdentifier::new("sum"),
                args: vec![Expression::ColumnRef(ColumnIdentifier::simple("a", false))],
                filter: None,
            },
            over: vibesql_ast::WindowSpec {
                base_window_name: None,
                partition_by: None,
                order_by: None,
                frame: None,
            },
        }
    }

    #[test]
    fn test_group_by_positional_resolves_to_window_function() {
        // SELECT sum(a) OVER() FROM t1 GROUP BY 1
        // Position 1 in select list is a window function — must error.
        let stmt = make_select(
            vec![SelectItem::Expression {
                expr: make_window_sum_a(),
                alias: None,
                source_text: None,
            }],
            Some("t1"),
            None,
            Some(vibesql_ast::GroupByClause::Simple(vec![Expression::Literal(SqlValue::Integer(
                1,
            ))])),
        );

        let result = validate_group_by_window_misuse(&stmt);
        match result {
            Err(ExecutorError::MisuseOfWindowFunction { function_name }) => {
                assert_eq!(function_name, "sum");
            }
            other => panic!("expected MisuseOfWindowFunction(sum), got {:?}", other),
        }
    }

    #[test]
    fn test_group_by_positional_non_window_does_not_fire() {
        // SELECT a FROM t1 GROUP BY 1
        // Position 1 is a plain column — must NOT error.
        let stmt = make_select(
            vec![SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("a", false)),
                alias: None,
                source_text: None,
            }],
            Some("t1"),
            None,
            Some(vibesql_ast::GroupByClause::Simple(vec![Expression::Literal(SqlValue::Integer(
                1,
            ))])),
        );

        assert!(validate_group_by_window_misuse(&stmt).is_ok());
    }

    #[test]
    fn test_group_by_alias_resolves_to_window_function() {
        // SELECT sum(a) OVER() AS w FROM t1 GROUP BY w
        // Alias 'w' refers to a window function — must error.
        let stmt = make_select(
            vec![SelectItem::Expression {
                expr: make_window_sum_a(),
                alias: Some("w".to_string()),
                source_text: None,
            }],
            Some("t1"),
            None,
            Some(vibesql_ast::GroupByClause::Simple(vec![Expression::ColumnRef(
                ColumnIdentifier::simple("w", false),
            )])),
        );

        let result = validate_group_by_window_misuse(&stmt);
        match result {
            Err(ExecutorError::MisuseOfWindowFunction { function_name }) => {
                assert_eq!(function_name, "sum");
            }
            other => panic!("expected MisuseOfWindowFunction(sum), got {:?}", other),
        }
    }

    #[test]
    fn test_group_by_window_in_nested_subquery() {
        // window1.test 47.2 shape:
        // SELECT 234 FROM t2 WHERE k=1 OR (SELECT k FROM t2 WHERE
        //     (SELECT sum(a) OVER() FROM t1 GROUP BY 1));
        // The deepest subquery's GROUP BY 1 resolves to a window function.
        let inner_subquery = Expression::ScalarSubquery(Box::new(make_select(
            vec![SelectItem::Expression {
                expr: make_window_sum_a(),
                alias: None,
                source_text: None,
            }],
            Some("t1"),
            None,
            Some(vibesql_ast::GroupByClause::Simple(vec![Expression::Literal(SqlValue::Integer(
                1,
            ))])),
        )));

        let middle_subquery = Expression::ScalarSubquery(Box::new(make_select(
            vec![SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("k", false)),
                alias: None,
                source_text: None,
            }],
            Some("t2"),
            Some(inner_subquery),
            None,
        )));

        let outer = make_select(
            vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(234)),
                alias: None,
                source_text: None,
            }],
            Some("t2"),
            Some(Expression::Disjunction(vec![
                Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("k", false))),
                    right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                },
                middle_subquery,
            ])),
            None,
        );

        let result = validate_group_by_window_misuse(&outer);
        match result {
            Err(ExecutorError::MisuseOfWindowFunction { function_name }) => {
                assert_eq!(function_name, "sum");
            }
            other => {
                panic!("expected MisuseOfWindowFunction(sum) from nested subquery, got {:?}", other)
            }
        }
    }
}
