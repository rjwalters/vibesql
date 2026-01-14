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
    matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" | "GROUP_CONCAT")
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
    match expr {
        Expression::AggregateFunction { name, .. } => Some(name.to_string()), /* Preserve original case */
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            // Note: MIN/MAX with multiple args are scalar functions in SQLite
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Multi-arg min/max are scalar, not aggregate
                    None
                } else {
                    Some(name.to_string()) // Preserve original case
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = find_aggregate_in_expression(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            find_aggregate_in_expression(left).or_else(|| find_aggregate_in_expression(right))
        }
        Expression::UnaryOp { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_aggregate_in_expression(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = find_aggregate_in_expression(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_aggregate_in_expression(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_aggregate_in_expression(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => find_aggregate_in_expression(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            find_aggregate_in_expression(left).or_else(|| find_aggregate_in_expression(right))
        }
        Expression::IsTruthValue { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Between { expr, low, high, .. } => find_aggregate_in_expression(expr)
            .or_else(|| find_aggregate_in_expression(low))
            .or_else(|| find_aggregate_in_expression(high)),
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_aggregate_in_expression(expr) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_aggregate_in_expression(val) {
                    return Some(found);
                }
            }
            None
        }
        Expression::In { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Exists { .. } => None, // EXISTS subqueries have their own scope
        Expression::Cast { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Like { expr, pattern, .. } => {
            find_aggregate_in_expression(expr).or_else(|| find_aggregate_in_expression(pattern))
        }
        Expression::Position { substring, string, .. } => {
            find_aggregate_in_expression(substring).or_else(|| find_aggregate_in_expression(string))
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                if let Some(found) = find_aggregate_in_expression(char_expr) {
                    return Some(found);
                }
            }
            find_aggregate_in_expression(string)
        }
        Expression::Extract { expr, .. } => find_aggregate_in_expression(expr),
        Expression::ScalarSubquery(_) => None, // Subqueries have their own scope
        Expression::QuantifiedComparison { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Interval { value, .. } => find_aggregate_in_expression(value),
        Expression::WindowFunction { .. } => None, // Window functions are not regular aggregates
        Expression::MatchAgainst { search_modifier, .. } => {
            find_aggregate_in_expression(search_modifier)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_aggregate_in_expression(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
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
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Still check arguments for nested aggregates
                    args.iter().any(expression_contains_aggregate)
                } else {
                    true
                }
            } else {
                // Check function arguments
                args.iter().any(expression_contains_aggregate)
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => expression_contains_aggregate(expr),
        Expression::Cast { expr, .. } => expression_contains_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_contains_aggregate(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_contains_aggregate)
                        || expression_contains_aggregate(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_contains_aggregate(e))
        }
        Expression::IsNull { expr, .. } => expression_contains_aggregate(expr),
        Expression::Between { expr, low, high, .. } => {
            expression_contains_aggregate(expr)
                || expression_contains_aggregate(low)
                || expression_contains_aggregate(high)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_aggregate(expr) || values.iter().any(expression_contains_aggregate)
        }
        Expression::In { expr, .. } => expression_contains_aggregate(expr),
        Expression::Like { expr, pattern, .. } => {
            expression_contains_aggregate(expr) || expression_contains_aggregate(pattern)
        }
        Expression::Position { substring, string, .. } => {
            expression_contains_aggregate(substring) || expression_contains_aggregate(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_contains_aggregate(e))
                || expression_contains_aggregate(string)
        }
        Expression::Extract { expr, .. } => expression_contains_aggregate(expr),
        Expression::Interval { value, .. } => expression_contains_aggregate(value),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().any(expression_contains_aggregate)
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => false,
        // Window functions are not aggregates in this context
        Expression::WindowFunction { .. } => false,
        // Other expressions don't contain aggregates
        _ => false,
    }
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
}
