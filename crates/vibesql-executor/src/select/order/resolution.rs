//! Alias and expression resolution for ORDER BY and WHERE clauses.
//!
//! This module handles:
//! - Resolving ORDER BY column positions and aliases
//! - Resolving ORDER BY expressions for aggregate queries
//! - Resolving SELECT aliases in WHERE clauses (SQLite extension)
//! - Extracting aggregate expressions from ORDER BY for pre-computation

use std::borrow::Cow;

use super::position::{
    count_select_columns, extract_column_position, resolve_position_to_column_name,
    resolve_position_with_wildcards, validate_column_position, ColumnPositionResult,
    ResolvedPosition,
};
use crate::{errors::ExecutorError, schema::CombinedSchema};

// =============================================================================
// ORDER BY Alias Resolution for Aggregate Queries
// =============================================================================

/// Resolve ORDER BY expression for aggregate results to result schema column names
///
/// For aggregate queries, the result schema has columns named by their aliases.
/// This function maps ORDER BY expressions to ColumnRef expressions that can be
/// evaluated against the result schema.
///
/// Handles cases:
/// 1. Numeric position (ORDER BY 1) - returns ColumnRef to the alias/column at that position
/// 2. Alias name (ORDER BY alias) - returns ColumnRef to that alias
/// 3. Original column name (ORDER BY col where col is aliased to alias) - returns ColumnRef to
///    alias
/// 4. Complex expressions containing GROUPING() - recursively resolves sub-expressions
/// 5. Otherwise - returns the original expression (for expressions not matching aliases)
///
/// Returns an error if a numeric column position is out of range (0 or > column count)
///
/// # Arguments
/// * `order_expr` - The ORDER BY expression to resolve
/// * `select_list` - The SELECT list items
/// * `term_index` - 0-indexed position of this ORDER BY term (for error messages)
/// * `schema` - Optional schema for proper wildcard expansion when counting columns (#4413)
pub(crate) fn resolve_order_by_for_aggregates(
    order_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
    term_index: usize, // 0-indexed position of this ORDER BY term
    schema: Option<&CombinedSchema>,
) -> Result<vibesql_ast::Expression, ExecutorError> {
    // Count actual columns after wildcard expansion (#4413)
    // Note: For aggregate queries, wildcards are typically not present, but we
    // handle them for consistency
    let column_count = count_select_columns(select_list, schema);

    // Validate numeric column positions at the TOP LEVEL ONLY
    // (nested integer literals like `WHERE x = 0` are not column positions)

    // Check for numeric column position (ORDER BY N, ORDER BY +N, ORDER BY -N)
    match extract_column_position(order_expr) {
        ColumnPositionResult::Position(pos) => {
            let idx = validate_column_position(pos, column_count, term_index)?;
            if let Some(col_name) = resolve_position_to_column_name(idx, select_list) {
                return Ok(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::simple(&col_name, false),
                ));
            }
        }
        ColumnPositionResult::Negative(pos) => {
            // Negative column positions are always invalid
            return Err(ExecutorError::OrderByOutOfRange {
                term_position: term_index + 1,
                column_number: -pos,
                select_list_len: column_count,
            });
        }
        ColumnPositionResult::NotAPosition => {
            // Not a column position, continue to other resolution logic
        }
    }

    // Delegate to helper that doesn't validate column numbers (for recursive calls)
    Ok(resolve_order_by_for_aggregates_inner(order_expr, select_list))
}

/// Internal helper for recursively resolving ORDER BY expressions.
/// Does NOT validate numeric column positions (validation is done at top level only).
fn resolve_order_by_for_aggregates_inner(
    order_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> vibesql_ast::Expression {
    // Check for numeric column position (ORDER BY 1, 2, 3, etc.)
    // NOTE: At this point we're in a nested expression, so integer literals are NOT column positions
    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = order_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = &select_list[idx] {
                // Return a ColumnRef to the alias name (or derive from expression)
                let col_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else if let vibesql_ast::Expression::ColumnRef(col_id) = expr {
                    col_id.column_canonical().to_string()
                } else if let vibesql_ast::Expression::AggregateFunction { name, .. } = expr {
                    // For aggregate functions, use the function name (lowercase)
                    // This matches the schema column name generated in apply_order_by_to_aggregates
                    name.to_lowercase()
                } else {
                    format!("col{}", idx + 1)
                };
                return vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    &col_name, false,
                ));
            }
        }
        // If not a valid column position, just return the literal as-is
        return order_expr.clone();
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef(col_id) = order_expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            let column = col_id.column_canonical();
            // First, check if column matches an alias name - return ColumnRef to that alias
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression { alias: Some(alias_name), .. } = item {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // ORDER BY uses alias name, return ColumnRef to that alias
                        return vibesql_ast::Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple(alias_name, false),
                        );
                    }
                }
            }

            // Second, check if column matches an original column name that has an alias
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: vibesql_ast::Expression::ColumnRef(select_col_id),
                    alias: Some(alias_name),
                    ..
                } = item
                {
                    if select_col_id.schema_canonical().is_none() {
                        let select_col = select_col_id.column_canonical();
                        if select_col.eq_ignore_ascii_case(column) {
                            // ORDER BY uses original column name, return ColumnRef to the alias
                            return vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::simple(alias_name, false),
                            );
                        }
                    }
                }
            }

            // Third, check if column matches a SELECT list column without alias
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: vibesql_ast::Expression::ColumnRef(select_col_id),
                    alias: None,
                    ..
                } = item
                {
                    if select_col_id.schema_canonical().is_none() {
                        let select_col = select_col_id.column_canonical();
                        if select_col.eq_ignore_ascii_case(column) {
                            // Found matching non-aliased column, return as-is
                            return order_expr.clone();
                        }
                    }
                }
            }
        }
    }

    // IMPORTANT: Check if the ENTIRE expression matches any SELECT list expression FIRST
    // This handles cases like GROUPING(a) + GROUPING(b) matching an alias like "lochierarchy"
    // before we try to recursively decompose the expression
    if let Some(alias) = find_matching_select_expression(order_expr, select_list) {
        return vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
            &alias, false,
        ));
    }

    // Handle CASE expressions by recursively resolving sub-expressions
    // Use inner helper for recursion (doesn't validate column numbers)
    if let vibesql_ast::Expression::Case { operand, when_clauses, else_result } = order_expr {
        let resolved_operand = operand
            .as_ref()
            .map(|op| Box::new(resolve_order_by_for_aggregates_inner(op, select_list)));

        let mut resolved_when_clauses: Vec<vibesql_ast::CaseWhen> = Vec::new();
        for clause in when_clauses {
            let mut resolved_conditions = Vec::new();
            for cond in &clause.conditions {
                resolved_conditions.push(resolve_order_by_for_aggregates_inner(cond, select_list));
            }
            resolved_when_clauses.push(vibesql_ast::CaseWhen {
                conditions: resolved_conditions,
                result: resolve_order_by_for_aggregates_inner(&clause.result, select_list),
            });
        }

        let resolved_else = else_result
            .as_ref()
            .map(|e| Box::new(resolve_order_by_for_aggregates_inner(e, select_list)));

        return vibesql_ast::Expression::Case {
            operand: resolved_operand,
            when_clauses: resolved_when_clauses,
            else_result: resolved_else,
        };
    }

    // Handle BinaryOp expressions by recursively resolving sub-expressions
    // Try to match the entire binary expression first (already done above with
    // find_matching_select_expression) If no match, try matching each side separately
    if let vibesql_ast::Expression::BinaryOp { left, op, right } = order_expr {
        let resolved_left = resolve_order_by_for_aggregates_inner(left, select_list);
        let resolved_right = resolve_order_by_for_aggregates_inner(right, select_list);

        return vibesql_ast::Expression::BinaryOp {
            left: Box::new(resolved_left),
            op: *op,
            right: Box::new(resolved_right),
        };
    }

    // Handle Function calls (including GROUPING) by checking if they match a SELECT expression
    // This is already handled by find_matching_select_expression above, but keep as fallback
    if let vibesql_ast::Expression::Function { name, .. } = order_expr {
        if name.eq_ignore_ascii_case("GROUPING") || name.eq_ignore_ascii_case("GROUPING_ID") {
            // Try to find a matching GROUPING expression in the SELECT list
            if let Some(alias) = find_matching_select_expression(order_expr, select_list) {
                return vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    &alias, false,
                ));
            }
        }
    }

    // Not an alias or column position, return the original expression
    order_expr.clone()
}

/// Find a matching expression in the SELECT list and return its alias or generated column name
fn find_matching_select_expression(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<String> {
    for (idx, item) in select_list.iter().enumerate() {
        if let vibesql_ast::SelectItem::Expression { expr: select_expr, alias, .. } = item {
            if expressions_equal(expr, select_expr) {
                // Found matching expression
                // Return alias if present, otherwise derive column name from expression
                return Some(if let Some(alias_name) = alias {
                    alias_name.clone()
                } else if let vibesql_ast::Expression::ColumnRef(col_id) = select_expr {
                    col_id.column_canonical().to_string()
                } else if let vibesql_ast::Expression::AggregateFunction { name, .. } = select_expr
                {
                    // For aggregate functions, use the function name (lowercase)
                    // This matches the schema column name generated in apply_order_by_to_aggregates
                    name.to_lowercase()
                } else {
                    format!("col{}", idx + 1)
                });
            }
        }
    }
    None
}

/// Check if two expressions are structurally equal (for matching ORDER BY expressions to SELECT
/// list)
fn expressions_equal(a: &vibesql_ast::Expression, b: &vibesql_ast::Expression) -> bool {
    match (a, b) {
        (
            vibesql_ast::Expression::ColumnRef(col_id1),
            vibesql_ast::Expression::ColumnRef(col_id2),
        ) => {
            col_id1.schema_canonical().is_none()
                && col_id2.schema_canonical().is_none()
                && col_id1.table_canonical() == col_id2.table_canonical()
                && col_id1.column_canonical() == col_id2.column_canonical()
        }

        (vibesql_ast::Expression::Literal(v1), vibesql_ast::Expression::Literal(v2)) => v1 == v2,

        (
            vibesql_ast::Expression::BinaryOp { left: l1, op: o1, right: r1 },
            vibesql_ast::Expression::BinaryOp { left: l2, op: o2, right: r2 },
        ) => o1 == o2 && expressions_equal(l1, l2) && expressions_equal(r1, r2),

        (
            vibesql_ast::Expression::Function { name: n1, args: a1, .. },
            vibesql_ast::Expression::Function { name: n2, args: a2, .. },
        ) => {
            n1 == n2 // FunctionIdentifier comparison is case-insensitive via canonical
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| expressions_equal(x, y))
        }

        // For other expression types, use debug representation comparison as fallback
        // This is not perfect but handles most common cases
        _ => format!("{:?}", a) == format!("{:?}", b),
    }
}

/// For a single ORDER BY term, determine whether it references a SELECT output
/// column whose projected expression is **non-deterministic** (e.g.
/// `abs(random())%5`). When it does, the term must be sorted on the already
/// projected output value (by column position) rather than by re-evaluating the
/// expression — otherwise a volatile function like `random()` is called a second
/// time during sort-key evaluation and the sort key no longer matches the row's
/// output value (issue #5712, distinct2-5020).
///
/// Returns `Some(output_column_index)` when the term maps to a non-deterministic
/// projected column and the mapping is unambiguous; otherwise `None` (the caller
/// falls back to normal expression-based resolution).
///
/// Only the wildcard-free case is handled: when the SELECT list contains a
/// wildcard the 1:1 mapping from select-item index to output-column index no
/// longer holds, so we conservatively return `None`.
pub(crate) fn order_by_volatile_output_index(
    order_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<usize> {
    use crate::evaluator::expression_hash::ExpressionHasher;

    // Wildcards break the simple item-index == output-index mapping.
    let has_wildcard = select_list.iter().any(|item| {
        matches!(
            item,
            vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. }
        )
    });
    if has_wildcard {
        return None;
    }

    // Helper: a projected select item is "volatile" if it is an expression that
    // is not deterministic (RANDOM(), CURRENT_TIMESTAMP, etc.).
    let item_is_volatile = |item: &vibesql_ast::SelectItem| -> bool {
        matches!(
            item,
            vibesql_ast::SelectItem::Expression { expr, .. }
            if !ExpressionHasher::is_deterministic(expr)
        )
    };

    // Case 1: ORDER BY N (positional reference into the SELECT list).
    if let ColumnPositionResult::Position(pos) = extract_column_position(order_expr) {
        if pos >= 1 && (pos as usize) <= select_list.len() {
            let idx = (pos as usize) - 1;
            if item_is_volatile(&select_list[idx]) {
                return Some(idx);
            }
        }
        return None;
    }

    // Case 2: ORDER BY <alias>, where <alias> is a bare (unqualified) name that
    // matches a SELECT-list alias whose expression is non-deterministic.
    if let vibesql_ast::Expression::ColumnRef(col_id) = order_expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            let column = col_id.column_canonical();
            for (idx, item) in select_list.iter().enumerate() {
                if let vibesql_ast::SelectItem::Expression {
                    expr, alias: Some(alias_name), ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column)
                        && !ExpressionHasher::is_deterministic(expr)
                    {
                        return Some(idx);
                    }
                }
            }
        }
    }

    // Case 3: ORDER BY <expr> where <expr> is structurally identical to a
    // non-deterministic SELECT-list expression (e.g.
    // `SELECT random() AS y FROM t1 ORDER BY random()`). The ORDER BY term is
    // neither a positional reference nor a bare alias, so Cases 1 and 2 miss it,
    // yet SQLite evaluates the volatile expression once per row and reuses that
    // value for both the projected output and the sort key. Re-evaluating
    // `random()` independently during sorting would produce a sort key that no
    // longer matches the projected value, leaving the output unsorted
    // (orderby9-1.1).
    for (idx, item) in select_list.iter().enumerate() {
        if let vibesql_ast::SelectItem::Expression { expr: select_expr, .. } = item {
            if !ExpressionHasher::is_deterministic(select_expr)
                && expressions_equal(order_expr, select_expr)
            {
                return Some(idx);
            }
        }
    }

    None
}

// =============================================================================
// ORDER BY Alias Resolution for Regular Queries
// =============================================================================

/// Resolve ORDER BY expression that might be a SELECT list alias or column position
///
/// Handles these cases:
/// 1. Numeric literal (e.g., ORDER BY 1, 2, 3) - returns the expression from that position in
///    SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Simple column reference that matches an aliased column's original name - returns a ColumnRef
///    to the alias
/// 4. Complex expressions containing alias references (e.g., ORDER BY -x, ORDER BY abs(x)) -
///    recursively resolves alias references within the expression (#4436)
/// 5. Otherwise - returns the original ORDER BY expression
///
/// Returns an error if a numeric column position is out of range (0 or > column count)
///
/// # Arguments
/// * `order_expr` - The ORDER BY expression to resolve
/// * `select_list` - The SELECT list items
/// * `term_index` - 0-indexed position of this ORDER BY term (for error messages)
/// * `schema` - Optional schema for proper wildcard expansion when counting columns (#4413)
pub(crate) fn resolve_order_by_alias<'a>(
    order_expr: &'a vibesql_ast::Expression,
    select_list: &'a [vibesql_ast::SelectItem],
    term_index: usize, // 0-indexed position of this ORDER BY term
    schema: Option<&CombinedSchema>,
) -> Result<Cow<'a, vibesql_ast::Expression>, ExecutorError> {
    // Count actual columns after wildcard expansion (#4413)
    let column_count = count_select_columns(select_list, schema);

    // Check for numeric column position (ORDER BY N, ORDER BY +N, ORDER BY -N)
    match extract_column_position(order_expr) {
        ColumnPositionResult::Position(pos) => {
            let idx = validate_column_position(pos, column_count, term_index)?;

            // Resolve the position, handling wildcard expansion
            match resolve_position_with_wildcards(idx, select_list, schema) {
                ResolvedPosition::Expression(expr) => {
                    return Ok(Cow::Borrowed(expr));
                }
                ResolvedPosition::ColumnName { table, column } => {
                    // Build a table-qualified reference when the source table is
                    // known so the ambiguity check is not tripped when multiple
                    // tables expose the same column name (issue #5231).
                    // quoted=true preserves the canonical table/column names
                    // exactly as stored in the schema.
                    let col_id = match table {
                        Some(table_name) => vibesql_ast::ColumnIdentifier::qualified(
                            &table_name,
                            true,
                            &column,
                            true,
                        ),
                        None => vibesql_ast::ColumnIdentifier::simple(&column, false),
                    };
                    return Ok(Cow::Owned(vibesql_ast::Expression::ColumnRef(col_id)));
                }
                ResolvedPosition::OwnedExpression(expr) => {
                    // USING/NATURAL OUTER-JOIN output column: sort by the merged
                    // COALESCE value rather than a single base-table column
                    // (issue #5657).
                    return Ok(Cow::Owned(expr));
                }
                ResolvedPosition::NotFound => {
                    // Fallback: shouldn't reach here if validation passed
                }
            }
        }
        ColumnPositionResult::Negative(pos) => {
            // Negative column positions are always invalid
            return Err(ExecutorError::OrderByOutOfRange {
                term_position: term_index + 1,
                column_number: -pos,
                select_list_len: column_count,
            });
        }
        ColumnPositionResult::NotAPosition => {
            // Not a column position, continue to other resolution logic
        }
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef(col_id) = order_expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            let column = col_id.column_canonical();
            // First, search for matching alias in SELECT list (ORDER BY using alias name)
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr, alias: Some(alias_name), ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // Found matching alias, use the SELECT list expression
                        return Ok(Cow::Borrowed(expr));
                    }
                }
            }

            // Second, check if column matches a SELECT list expression that has an alias
            // This handles: SELECT col AS alias ... ORDER BY col
            // HOWEVER: If the column exists in the FROM schema, we should NOT transform it
            // to the alias. The ORDER BY should reference the actual column from the schema.
            //
            // Example: SELECT cnt as tests_affected FROM (SELECT COUNT(*) as cnt ...) sub ORDER BY cnt
            // Here, 'cnt' exists in the subquery schema, so ORDER BY cnt should evaluate 'cnt' directly,
            // not be transformed to 'tests_affected' (which doesn't exist in the schema).
            //
            // Only transform to alias when the column does NOT exist in the FROM schema (rare case).
            let column_exists_in_schema = schema.is_some_and(|s| {
                // Check if column exists in any table in the schema
                s.table_schemas.values().any(|(_start_idx, table_schema)| {
                    table_schema.get_column_index(column).is_some()
                })
            });

            if !column_exists_in_schema {
                for item in select_list {
                    // Check if the SELECT expression is a column reference to the same column
                    if let vibesql_ast::SelectItem::Expression {
                        expr: vibesql_ast::Expression::ColumnRef(select_col_id),
                        alias: Some(alias_name),
                        ..
                    } = item
                    {
                        if select_col_id.schema_canonical().is_none() {
                            let select_col = select_col_id.column_canonical();
                            if select_col.eq_ignore_ascii_case(column) {
                                // The ORDER BY column matches the original column, but it's aliased
                                // Return a new ColumnRef using the alias name
                                return Ok(Cow::Owned(vibesql_ast::Expression::ColumnRef(
                                    vibesql_ast::ColumnIdentifier::simple(alias_name, false),
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    // For complex expressions (UnaryOp, BinaryOp, Function calls, etc.), recursively resolve
    // alias references within the expression (#4436)
    // This handles cases like: ORDER BY -x, ORDER BY abs(x), ORDER BY 10-x
    if let Some(resolved) = resolve_aliases_in_expression(order_expr, select_list) {
        return Ok(Cow::Owned(resolved));
    }

    // Not an alias or column position, use the original expression
    Ok(Cow::Borrowed(order_expr))
}

/// Recursively resolve alias references within an ORDER BY expression.
///
/// This handles complex expressions like:
/// - `ORDER BY -x` where x is an alias
/// - `ORDER BY abs(x)` where x is an alias
/// - `ORDER BY 10-x` where x is an alias
///
/// Returns Some(resolved_expression) if any aliases were resolved, None otherwise.
fn resolve_aliases_in_expression(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<vibesql_ast::Expression> {
    match expr {
        // Handle UnaryOp (e.g., -x, +x, NOT x)
        vibesql_ast::Expression::UnaryOp { op, expr: inner } => {
            // Try to resolve the inner expression
            resolve_alias_or_clone(inner, select_list).map(|resolved_inner| {
                vibesql_ast::Expression::UnaryOp { op: *op, expr: Box::new(resolved_inner) }
            })
        }

        // Handle BinaryOp (e.g., 10-x, x+y)
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            let resolved_left = resolve_alias_or_clone(left, select_list);
            let resolved_right = resolve_alias_or_clone(right, select_list);

            // Only return Some if at least one side was resolved
            if resolved_left.is_some() || resolved_right.is_some() {
                Some(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(resolved_left.unwrap_or_else(|| left.as_ref().clone())),
                    op: *op,
                    right: Box::new(resolved_right.unwrap_or_else(|| right.as_ref().clone())),
                })
            } else {
                None
            }
        }

        // Handle Function calls (e.g., abs(x), coalesce(x, 0))
        vibesql_ast::Expression::Function { name, args, character_unit } => {
            let mut any_resolved = false;
            let resolved_args: Vec<_> = args
                .iter()
                .map(|arg| {
                    if let Some(resolved) = resolve_alias_or_clone(arg, select_list) {
                        any_resolved = true;
                        resolved
                    } else {
                        arg.clone()
                    }
                })
                .collect();

            if any_resolved {
                Some(vibesql_ast::Expression::Function {
                    name: name.clone(),
                    args: resolved_args,
                    character_unit: character_unit.clone(),
                })
            } else {
                None
            }
        }

        // Handle CASE expressions
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            let mut any_resolved = false;

            let resolved_operand = operand.as_ref().map(|op| {
                if let Some(resolved) = resolve_alias_or_clone(op, select_list) {
                    any_resolved = true;
                    Box::new(resolved)
                } else {
                    op.clone()
                }
            });

            let resolved_when_clauses: Vec<_> = when_clauses
                .iter()
                .map(|clause| {
                    let resolved_conditions: Vec<_> = clause
                        .conditions
                        .iter()
                        .map(|cond| {
                            if let Some(resolved) = resolve_alias_or_clone(cond, select_list) {
                                any_resolved = true;
                                resolved
                            } else {
                                cond.clone()
                            }
                        })
                        .collect();

                    let resolved_result = if let Some(resolved) =
                        resolve_alias_or_clone(&clause.result, select_list)
                    {
                        any_resolved = true;
                        resolved
                    } else {
                        clause.result.clone()
                    };

                    vibesql_ast::CaseWhen {
                        conditions: resolved_conditions,
                        result: resolved_result,
                    }
                })
                .collect();

            let resolved_else = else_result.as_ref().map(|e| {
                if let Some(resolved) = resolve_alias_or_clone(e, select_list) {
                    any_resolved = true;
                    Box::new(resolved)
                } else {
                    e.clone()
                }
            });

            if any_resolved {
                Some(vibesql_ast::Expression::Case {
                    operand: resolved_operand,
                    when_clauses: resolved_when_clauses,
                    else_result: resolved_else,
                })
            } else {
                None
            }
        }

        // Handle IS / IS NOT (parsed as IsDistinctFrom), e.g. ORDER BY (z IS y)
        // where z is a SELECT alias (window9.test 7.2/7.3)
        vibesql_ast::Expression::IsDistinctFrom { left, right, negated } => {
            let resolved_left = resolve_alias_or_clone(left, select_list);
            let resolved_right = resolve_alias_or_clone(right, select_list);

            if resolved_left.is_some() || resolved_right.is_some() {
                Some(vibesql_ast::Expression::IsDistinctFrom {
                    left: Box::new(resolved_left.unwrap_or_else(|| left.as_ref().clone())),
                    right: Box::new(resolved_right.unwrap_or_else(|| right.as_ref().clone())),
                    negated: *negated,
                })
            } else {
                None
            }
        }

        // Handle IS NULL / IS NOT NULL (e.g. ORDER BY x IS NULL where x is an alias)
        vibesql_ast::Expression::IsNull { expr: inner, negated } => {
            resolve_alias_or_clone(inner, select_list).map(|resolved_inner| {
                vibesql_ast::Expression::IsNull {
                    expr: Box::new(resolved_inner),
                    negated: *negated,
                }
            })
        }

        // Handle IS TRUE / IS FALSE / IS UNKNOWN
        vibesql_ast::Expression::IsTruthValue { expr: inner, truth_value, negated } => {
            resolve_alias_or_clone(inner, select_list).map(|resolved_inner| {
                vibesql_ast::Expression::IsTruthValue {
                    expr: Box::new(resolved_inner),
                    truth_value: *truth_value,
                    negated: *negated,
                }
            })
        }

        // Handle CAST(x AS type)
        vibesql_ast::Expression::Cast { expr: inner, data_type } => {
            resolve_alias_or_clone(inner, select_list).map(|resolved_inner| {
                vibesql_ast::Expression::Cast {
                    expr: Box::new(resolved_inner),
                    data_type: data_type.clone(),
                }
            })
        }

        // Handle x COLLATE name
        vibesql_ast::Expression::Collate { expr: inner, collation } => {
            resolve_alias_or_clone(inner, select_list).map(|resolved_inner| {
                vibesql_ast::Expression::Collate {
                    expr: Box::new(resolved_inner),
                    collation: collation.clone(),
                }
            })
        }

        // Handle x BETWEEN low AND high
        vibesql_ast::Expression::Between { expr: inner, low, high, negated, symmetric } => {
            let resolved_inner = resolve_alias_or_clone(inner, select_list);
            let resolved_low = resolve_alias_or_clone(low, select_list);
            let resolved_high = resolve_alias_or_clone(high, select_list);

            if resolved_inner.is_some() || resolved_low.is_some() || resolved_high.is_some() {
                Some(vibesql_ast::Expression::Between {
                    expr: Box::new(resolved_inner.unwrap_or_else(|| inner.as_ref().clone())),
                    low: Box::new(resolved_low.unwrap_or_else(|| low.as_ref().clone())),
                    high: Box::new(resolved_high.unwrap_or_else(|| high.as_ref().clone())),
                    negated: *negated,
                    symmetric: *symmetric,
                })
            } else {
                None
            }
        }

        // Handle x IN (a, b, c)
        vibesql_ast::Expression::InList { expr: inner, values, negated } => {
            let resolved_inner = resolve_alias_or_clone(inner, select_list);
            let mut any_resolved = resolved_inner.is_some();
            let resolved_values: Vec<_> = values
                .iter()
                .map(|v| {
                    if let Some(resolved) = resolve_alias_or_clone(v, select_list) {
                        any_resolved = true;
                        resolved
                    } else {
                        v.clone()
                    }
                })
                .collect();

            if any_resolved {
                Some(vibesql_ast::Expression::InList {
                    expr: Box::new(resolved_inner.unwrap_or_else(|| inner.as_ref().clone())),
                    values: resolved_values,
                    negated: *negated,
                })
            } else {
                None
            }
        }

        // For other expression types, no resolution needed
        _ => None,
    }
}

/// Helper: resolve an alias in an expression, or return None if no alias was found.
/// Returns Some(resolved_expression) if the expression contains an alias that was resolved.
fn resolve_alias_or_clone(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<vibesql_ast::Expression> {
    // Check if this is a simple column reference that matches an alias
    if let vibesql_ast::Expression::ColumnRef(col_id) = expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            let column = col_id.column_canonical();
            // Search for matching alias in SELECT list
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: select_expr,
                    alias: Some(alias_name),
                    ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // Found matching alias, return the SELECT list expression
                        return Some(select_expr.clone());
                    }
                }
            }
        }
    }

    // Try recursive resolution for complex expressions
    resolve_aliases_in_expression(expr, select_list)
}

// =============================================================================
// WHERE Clause Alias Resolution (SQLite Extension)
// =============================================================================

/// Resolve SELECT aliases in WHERE clause expression (SQLite extension).
///
/// SQLite allows referencing SELECT column aliases in WHERE clause, even though
/// standard SQL does not. This function resolves alias references in a WHERE
/// clause expression by replacing them with the actual SELECT expressions.
///
/// Example:
/// ```sql
/// SELECT f1-22 AS x FROM t1 WHERE x > 0
/// -- becomes: SELECT f1-22 AS x FROM t1 WHERE (f1-22) > 0
/// ```
///
/// Returns the resolved expression (cloned with aliases replaced).
pub(crate) fn resolve_where_aliases(
    where_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> vibesql_ast::Expression {
    // Fast path: if no SELECT items have aliases, nothing to resolve
    if !select_list_has_aliases(select_list) {
        return where_expr.clone();
    }

    // Use empty schema - no table columns to protect from alias resolution
    resolve_where_expression_with_schema(where_expr, select_list, &std::collections::HashSet::new())
}

/// Check if any SELECT item has an alias.
///
/// This is used as a fast path check to skip alias resolution when no aliases exist.
/// Call sites should use this BEFORE building schemas to avoid unnecessary work.
#[inline]
pub(crate) fn select_list_has_aliases(select_list: &[vibesql_ast::SelectItem]) -> bool {
    select_list
        .iter()
        .any(|item| matches!(item, vibesql_ast::SelectItem::Expression { alias: Some(_), .. }))
}

/// Resolve SELECT aliases in WHERE clause with schema column awareness.
///
/// Same as `resolve_where_aliases`, but also takes a schema to extract table column names.
/// Column names that exist in the table schema will NOT be resolved as aliases.
///
/// **IMPORTANT**: Table column names take precedence over SELECT aliases.
/// If a column name exists in the table schema, it will NOT be resolved as an alias,
/// even if a SELECT alias with the same name exists.
///
/// Example:
/// ```sql
/// SELECT COUNT(*) AS col1 FROM tab0 WHERE col1 > 0
/// -- col1 in WHERE refers to the TABLE COLUMN (tab0.col1), not the alias
/// ```
pub(crate) fn resolve_where_aliases_with_schema(
    where_expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
    schema: &CombinedSchema,
) -> vibesql_ast::Expression {
    // Fast path: if no SELECT items have aliases, nothing to resolve
    // This is a common case in TPC-C and other OLTP workloads
    if !select_list_has_aliases(select_list) {
        return where_expr.clone();
    }

    // Use schema's has_column method directly instead of building a HashSet
    // This is a performance optimization for TPC-C and other high-throughput workloads
    resolve_where_expression_with_schema_ref(where_expr, select_list, schema)
}

/// Optimized version that uses CombinedSchema.has_column() directly.
/// Avoids building a HashSet of column names on every query.
fn resolve_where_expression_with_schema_ref(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
    schema: &CombinedSchema,
) -> vibesql_ast::Expression {
    use vibesql_ast::Expression;

    match expr {
        // Column reference: check if it's an alias
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // SQLite behavior: table column names ALWAYS take precedence over aliases
            // Use schema.has_column() directly instead of HashSet lookup
            if schema.has_column(column) {
                return expr.clone();
            }

            // Fallback: check if any SELECT item is a direct column reference with this name
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: Expression::ColumnRef(sel_col_id),
                    ..
                } = item
                {
                    if sel_col_id.schema_canonical().is_none()
                        && sel_col_id.table_canonical().is_none()
                    {
                        let col_name = sel_col_id.column_canonical();
                        if col_name.eq_ignore_ascii_case(column) {
                            return expr.clone();
                        }
                    }
                    if sel_col_id.schema_canonical().is_none()
                        && sel_col_id.table_canonical().is_some()
                    {
                        let col_name = sel_col_id.column_canonical();
                        if col_name.eq_ignore_ascii_case(column) {
                            return expr.clone();
                        }
                    }
                }
            }

            // Now search for matching alias in SELECT list
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: select_expr,
                    alias: Some(alias_name),
                    ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        return select_expr.clone();
                    }
                }
            }
            expr.clone()
        }

        // Qualified column references: not aliases
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() =>
        {
            expr.clone()
        }
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_some() => expr.clone(),
        Expression::ColumnRef(_) => expr.clone(),

        // BinaryOp: resolve both sides
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(resolve_where_expression_with_schema_ref(left, select_list, schema)),
            op: *op,
            right: Box::new(resolve_where_expression_with_schema_ref(right, select_list, schema)),
        },

        // UnaryOp: resolve inner expression
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
        },

        // Function call: resolve all arguments
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_where_expression_with_schema_ref(arg, select_list, schema))
                .collect(),
            character_unit: character_unit.clone(),
        },

        // CASE expression
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|op| {
                Box::new(resolve_where_expression_with_schema_ref(op, select_list, schema))
            }),
            when_clauses: when_clauses
                .iter()
                .map(|clause| vibesql_ast::CaseWhen {
                    conditions: clause
                        .conditions
                        .iter()
                        .map(|cond| {
                            resolve_where_expression_with_schema_ref(cond, select_list, schema)
                        })
                        .collect(),
                    result: resolve_where_expression_with_schema_ref(
                        &clause.result,
                        select_list,
                        schema,
                    ),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema_ref(e, select_list, schema))
            }),
        },

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            negated: *negated,
        },

        // IS DISTINCT FROM
        Expression::IsDistinctFrom { left, right, negated } => Expression::IsDistinctFrom {
            left: Box::new(resolve_where_expression_with_schema_ref(left, select_list, schema)),
            right: Box::new(resolve_where_expression_with_schema_ref(right, select_list, schema)),
            negated: *negated,
        },

        // IS TRUE / IS FALSE / IS UNKNOWN
        Expression::IsTruthValue { expr: inner, truth_value, negated } => {
            Expression::IsTruthValue {
                expr: Box::new(resolve_where_expression_with_schema_ref(
                    inner,
                    select_list,
                    schema,
                )),
                truth_value: *truth_value,
                negated: *negated,
            }
        }

        // IN list
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            values: values
                .iter()
                .map(|v| resolve_where_expression_with_schema_ref(v, select_list, schema))
                .collect(),
            negated: *negated,
        },

        // IN subquery
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            subquery: subquery.clone(),
            negated: *negated,
        },

        // BETWEEN
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            low: Box::new(resolve_where_expression_with_schema_ref(low, select_list, schema)),
            high: Box::new(resolve_where_expression_with_schema_ref(high, select_list, schema)),
            negated: *negated,
            symmetric: *symmetric,
        },

        // LIKE
        Expression::Like { expr: inner, pattern, negated, escape } => Expression::Like {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            pattern: Box::new(resolve_where_expression_with_schema_ref(
                pattern,
                select_list,
                schema,
            )),
            negated: *negated,
            escape: escape.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema_ref(e, select_list, schema))
            }),
        },

        // GLOB
        Expression::Glob { expr: inner, pattern, negated, escape } => Expression::Glob {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            pattern: Box::new(resolve_where_expression_with_schema_ref(
                pattern,
                select_list,
                schema,
            )),
            negated: *negated,
            escape: escape.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema_ref(e, select_list, schema))
            }),
        },

        // CAST
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(resolve_where_expression_with_schema_ref(inner, select_list, schema)),
            data_type: data_type.clone(),
        },

        // Conjunction / Disjunction
        Expression::Conjunction(children) => Expression::Conjunction(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema_ref(c, select_list, schema))
                .collect(),
        ),
        Expression::Disjunction(children) => Expression::Disjunction(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema_ref(c, select_list, schema))
                .collect(),
        ),

        // Aggregate functions
        Expression::AggregateFunction { name, args, distinct, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| resolve_where_expression_with_schema_ref(arg, select_list, schema))
                    .collect(),
                distinct: *distinct,
                order_by: order_by.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|item| vibesql_ast::OrderByItem {
                            expr: resolve_where_expression_with_schema_ref(
                                &item.expr,
                                select_list,
                                schema,
                            ),
                            direction: item.direction.clone(),
                            nulls_order: item.nulls_order,
                        })
                        .collect()
                }),
                filter: filter.as_ref().map(|f| {
                    Box::new(resolve_where_expression_with_schema_ref(f, select_list, schema))
                }),
            }
        }

        // RowValueConstructor
        Expression::RowValueConstructor(children) => Expression::RowValueConstructor(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema_ref(c, select_list, schema))
                .collect(),
        ),

        // Collate
        Expression::Collate { expr, collation } => Expression::Collate {
            expr: Box::new(resolve_where_expression_with_schema_ref(expr, select_list, schema)),
            collation: collation.clone(),
        },

        // For all other expressions that don't need alias resolution, just clone
        _ => expr.clone(),
    }
}

/// Recursively resolve alias references in a WHERE clause expression (legacy version).
/// Uses a pre-built HashSet of column names. Kept for backward compatibility with
/// resolve_where_aliases() which doesn't have a schema reference.
fn resolve_where_expression_with_schema(
    expr: &vibesql_ast::Expression,
    select_list: &[vibesql_ast::SelectItem],
    table_columns: &std::collections::HashSet<String>,
) -> vibesql_ast::Expression {
    use vibesql_ast::Expression;

    match expr {
        // Column reference: check if it's an alias
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // SQLite behavior: table column names ALWAYS take precedence over aliases
            // If the column name exists in the table schema, it refers to the table column,
            // not to any SELECT alias with the same name.
            //
            // Example: SELECT COUNT(*) AS col1 FROM tab0 WHERE col1 > 0
            // Here 'col1' in WHERE refers to the TABLE COLUMN (tab0.col1), not the alias
            //
            // Check if this column exists in the table schema
            if table_columns.contains(&column.to_lowercase()) {
                // Column exists in table - don't resolve to alias
                return expr.clone();
            }

            // Fallback: check if any SELECT item is a direct column reference with this name
            // (this is for backward compatibility when no schema is provided)
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: Expression::ColumnRef(sel_col_id),
                    ..
                } = item
                {
                    if sel_col_id.schema_canonical().is_none()
                        && sel_col_id.table_canonical().is_none()
                    {
                        let col_name = sel_col_id.column_canonical();
                        if col_name.eq_ignore_ascii_case(column) {
                            // The name matches a column reference in SELECT - don't resolve to alias
                            return expr.clone();
                        }
                    }
                    // Also check qualified column references
                    if sel_col_id.schema_canonical().is_none()
                        && sel_col_id.table_canonical().is_some()
                    {
                        let col_name = sel_col_id.column_canonical();
                        if col_name.eq_ignore_ascii_case(column) {
                            // The name matches a column reference in SELECT - don't resolve to alias
                            return expr.clone();
                        }
                    }
                }
            }

            // Now search for matching alias in SELECT list (case-insensitive)
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr: select_expr,
                    alias: Some(alias_name),
                    ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // Found matching alias, return the SELECT expression
                        return select_expr.clone();
                    }
                }
            }
            // Not an alias, return as-is
            expr.clone()
        }

        // Qualified column reference: not an alias, return as-is
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_some() =>
        {
            expr.clone()
        }

        // Schema-qualified column reference: not an alias, return as-is
        Expression::ColumnRef(col_id) if col_id.schema_canonical().is_some() => expr.clone(),

        // Any other ColumnRef case (shouldn't happen, but handle for completeness)
        Expression::ColumnRef(_) => expr.clone(),

        // BinaryOp: resolve both sides
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(resolve_where_expression_with_schema(left, select_list, table_columns)),
            op: *op,
            right: Box::new(resolve_where_expression_with_schema(
                right,
                select_list,
                table_columns,
            )),
        },

        // UnaryOp: resolve inner expression
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
        },

        // Function call: resolve all arguments
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_where_expression_with_schema(arg, select_list, table_columns))
                .collect(),
            character_unit: character_unit.clone(),
        },

        // CASE expression: resolve all parts
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|op| {
                Box::new(resolve_where_expression_with_schema(op, select_list, table_columns))
            }),
            when_clauses: when_clauses
                .iter()
                .map(|clause| vibesql_ast::CaseWhen {
                    conditions: clause
                        .conditions
                        .iter()
                        .map(|cond| {
                            resolve_where_expression_with_schema(cond, select_list, table_columns)
                        })
                        .collect(),
                    result: resolve_where_expression_with_schema(
                        &clause.result,
                        select_list,
                        table_columns,
                    ),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema(e, select_list, table_columns))
            }),
        },

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            negated: *negated,
        },

        // IS DISTINCT FROM / IS NOT DISTINCT FROM
        Expression::IsDistinctFrom { left, right, negated } => Expression::IsDistinctFrom {
            left: Box::new(resolve_where_expression_with_schema(left, select_list, table_columns)),
            right: Box::new(resolve_where_expression_with_schema(
                right,
                select_list,
                table_columns,
            )),
            negated: *negated,
        },

        // IS TRUE / IS FALSE / IS UNKNOWN
        Expression::IsTruthValue { expr: inner, truth_value, negated } => {
            Expression::IsTruthValue {
                expr: Box::new(resolve_where_expression_with_schema(
                    inner,
                    select_list,
                    table_columns,
                )),
                truth_value: *truth_value,
                negated: *negated,
            }
        }

        // IN list
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            values: values
                .iter()
                .map(|v| resolve_where_expression_with_schema(v, select_list, table_columns))
                .collect(),
            negated: *negated,
        },

        // IN subquery: resolve the left expression, leave subquery unchanged
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            subquery: subquery.clone(),
            negated: *negated,
        },

        // BETWEEN
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            low: Box::new(resolve_where_expression_with_schema(low, select_list, table_columns)),
            high: Box::new(resolve_where_expression_with_schema(high, select_list, table_columns)),
            negated: *negated,
            symmetric: *symmetric,
        },

        // LIKE
        Expression::Like { expr: inner, pattern, negated, escape } => Expression::Like {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            pattern: Box::new(resolve_where_expression_with_schema(
                pattern,
                select_list,
                table_columns,
            )),
            negated: *negated,
            escape: escape.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema(e, select_list, table_columns))
            }),
        },

        // GLOB
        Expression::Glob { expr: inner, pattern, negated, escape } => Expression::Glob {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            pattern: Box::new(resolve_where_expression_with_schema(
                pattern,
                select_list,
                table_columns,
            )),
            negated: *negated,
            escape: escape.as_ref().map(|e| {
                Box::new(resolve_where_expression_with_schema(e, select_list, table_columns))
            }),
        },

        // CAST
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
            data_type: data_type.clone(),
        },

        // Conjunction / Disjunction
        Expression::Conjunction(children) => Expression::Conjunction(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema(c, select_list, table_columns))
                .collect(),
        ),
        Expression::Disjunction(children) => Expression::Disjunction(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema(c, select_list, table_columns))
                .collect(),
        ),

        // Aggregate functions (resolve arguments)
        Expression::AggregateFunction { name, args, distinct, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        resolve_where_expression_with_schema(arg, select_list, table_columns)
                    })
                    .collect(),
                distinct: *distinct,
                order_by: order_by.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|item| vibesql_ast::OrderByItem {
                            expr: resolve_where_expression_with_schema(
                                &item.expr,
                                select_list,
                                table_columns,
                            ),
                            direction: item.direction.clone(),
                            nulls_order: item.nulls_order,
                        })
                        .collect()
                }),
                filter: filter.as_ref().map(|f| {
                    Box::new(resolve_where_expression_with_schema(f, select_list, table_columns))
                }),
            }
        }

        // RowValueConstructor
        Expression::RowValueConstructor(children) => Expression::RowValueConstructor(
            children
                .iter()
                .map(|c| resolve_where_expression_with_schema(c, select_list, table_columns))
                .collect(),
        ),

        // Collate
        Expression::Collate { expr, collation } => Expression::Collate {
            expr: Box::new(resolve_where_expression_with_schema(expr, select_list, table_columns)),
            collation: collation.clone(),
        },

        // TRIM
        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position: position.clone(),
            removal_char: removal_char.as_ref().map(|c| {
                Box::new(resolve_where_expression_with_schema(c, select_list, table_columns))
            }),
            string: Box::new(resolve_where_expression_with_schema(
                string,
                select_list,
                table_columns,
            )),
        },

        // POSITION
        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(resolve_where_expression_with_schema(
                substring,
                select_list,
                table_columns,
            )),
            string: Box::new(resolve_where_expression_with_schema(
                string,
                select_list,
                table_columns,
            )),
            character_unit: character_unit.clone(),
        },

        // EXTRACT
        Expression::Extract { field, expr: inner } => Expression::Extract {
            field: field.clone(),
            expr: Box::new(resolve_where_expression_with_schema(inner, select_list, table_columns)),
        },

        // INTERVAL
        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(resolve_where_expression_with_schema(
                    value,
                    select_list,
                    table_columns,
                )),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            }
        }

        // Quantified comparison
        Expression::QuantifiedComparison { expr: inner, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(resolve_where_expression_with_schema(
                    inner,
                    select_list,
                    table_columns,
                )),
                op: *op,
                quantifier: quantifier.clone(),
                subquery: subquery.clone(),
            }
        }

        // RAISE: resolve aliases inside the error-message expression.
        Expression::Raise { action, error_message } => Expression::Raise {
            action: *action,
            error_message: error_message.as_ref().map(|msg| {
                Box::new(resolve_where_expression_with_schema(msg, select_list, table_columns))
            }),
        },

        // Expressions that don't need alias resolution (pass through)
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::PseudoVariable { .. }
        | Expression::DuplicateKeyValue { .. }
        | Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::WindowFunction { .. }
        | Expression::MatchAgainst { .. } => expr.clone(),
    }
}

// =============================================================================
// ORDER BY Aggregate Extraction
// =============================================================================

/// Extract aggregate expressions from ORDER BY items for pre-computation during GROUP BY.
///
/// When ORDER BY contains aggregate functions (e.g., `ORDER BY max(n)+0`), these aggregates
/// must be computed during the GROUP BY phase, not during ORDER BY evaluation.
/// This function collects all aggregate expressions from ORDER BY so they can be pre-computed.
///
/// Returns a vector of unique aggregate expressions found in ORDER BY.
pub(crate) fn extract_order_by_aggregates(
    order_by: &[vibesql_ast::OrderByItem],
) -> Vec<vibesql_ast::Expression> {
    let mut aggregates = Vec::new();
    for item in order_by {
        collect_aggregates_from_expr(&item.expr, &mut aggregates);
    }
    aggregates
}

/// Extract aggregate expressions from window function PARTITION BY/ORDER BY clauses
/// in the SELECT list. These need to be pre-computed during GROUP BY so the window
/// evaluator (which runs after aggregation) can reference them.
///
/// Example:
/// ```sql
/// SELECT max(b) OVER (ORDER BY max(c)) FROM t GROUP BY b;
/// ```
/// Here `max(c)` inside the OVER clause must be pre-computed per group, then the window
/// evaluator can sort by that pre-computed value.
///
/// Returns a vector of unique aggregate expressions used in any window's PARTITION BY,
/// ORDER BY, or frame offset.
pub(crate) fn extract_window_aggregates(
    select_list: &[vibesql_ast::SelectItem],
) -> Vec<vibesql_ast::Expression> {
    use vibesql_ast::SelectItem;

    let mut aggregates = Vec::new();

    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            // Window functions can be top-level SELECT items or embedded inside
            // larger expressions (e.g. `count(a) OVER (ORDER BY sum(a)) + total(a)
            // OVER()`, #5232) — walk the expression to find all of them.
            collect_window_over_aggregates_from_expr(expr, &mut aggregates);
        }
    }

    aggregates
}

/// Walk an expression tree (without descending into subqueries) and, for every
/// window function found, collect the aggregate expressions appearing in its
/// OVER clause (PARTITION BY, ORDER BY, frame offsets).
fn collect_window_over_aggregates_from_expr(
    expr: &vibesql_ast::Expression,
    aggregates: &mut Vec<vibesql_ast::Expression>,
) {
    use vibesql_ast::Expression;

    match expr {
        Expression::WindowFunction { function, over } => {
            // Window function arguments (e.g., `lead(sum(c)) OVER (...)`):
            // aggregates appearing as args need hidden per-group columns so the
            // post-aggregation window pass can resolve them (#5267).
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for arg in args {
                collect_aggregates_from_expr(arg, aggregates);
            }
            // PARTITION BY expressions
            if let Some(partition_by) = &over.partition_by {
                for p_expr in partition_by {
                    collect_aggregates_from_expr(p_expr, aggregates);
                }
            }
            // ORDER BY expressions
            if let Some(order_by) = &over.order_by {
                for ob_item in order_by {
                    collect_aggregates_from_expr(&ob_item.expr, aggregates);
                }
            }
            // Frame offset expressions (e.g., `ROWS BETWEEN sum(x) PRECEDING ...`)
            if let Some(frame) = &over.frame {
                if let vibesql_ast::FrameBound::Preceding(e)
                | vibesql_ast::FrameBound::Following(e) = &frame.start
                {
                    collect_aggregates_from_expr(e, aggregates);
                }
                if let Some(end) = &frame.end {
                    if let vibesql_ast::FrameBound::Preceding(e)
                    | vibesql_ast::FrameBound::Following(e) = end
                    {
                        collect_aggregates_from_expr(e, aggregates);
                    }
                }
            }
        }
        // Recurse into compound expressions to find embedded window functions.
        Expression::BinaryOp { left, right, .. } => {
            collect_window_over_aggregates_from_expr(left, aggregates);
            collect_window_over_aggregates_from_expr(right, aggregates);
        }
        Expression::UnaryOp { expr: inner, .. }
        | Expression::Cast { expr: inner, .. }
        | Expression::IsNull { expr: inner, .. }
        | Expression::IsTruthValue { expr: inner, .. }
        | Expression::Collate { expr: inner, .. } => {
            collect_window_over_aggregates_from_expr(inner, aggregates);
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            collect_window_over_aggregates_from_expr(left, aggregates);
            collect_window_over_aggregates_from_expr(right, aggregates);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_window_over_aggregates_from_expr(arg, aggregates);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_window_over_aggregates_from_expr(op, aggregates);
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_window_over_aggregates_from_expr(cond, aggregates);
                }
                collect_window_over_aggregates_from_expr(&when_clause.result, aggregates);
            }
            if let Some(else_expr) = else_result {
                collect_window_over_aggregates_from_expr(else_expr, aggregates);
            }
        }
        Expression::Between { expr, low, high, .. } => {
            collect_window_over_aggregates_from_expr(expr, aggregates);
            collect_window_over_aggregates_from_expr(low, aggregates);
            collect_window_over_aggregates_from_expr(high, aggregates);
        }
        Expression::InList { expr, values, .. } => {
            collect_window_over_aggregates_from_expr(expr, aggregates);
            for item in values {
                collect_window_over_aggregates_from_expr(item, aggregates);
            }
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            collect_window_over_aggregates_from_expr(expr, aggregates);
            collect_window_over_aggregates_from_expr(pattern, aggregates);
        }
        Expression::Conjunction(exprs)
        | Expression::Disjunction(exprs)
        | Expression::RowValueConstructor(exprs) => {
            for e in exprs {
                collect_window_over_aggregates_from_expr(e, aggregates);
            }
        }
        // IN / quantified comparison: only the LHS shares this scope.
        Expression::In { expr, .. } | Expression::QuantifiedComparison { expr, .. } => {
            collect_window_over_aggregates_from_expr(expr, aggregates);
        }
        // Subqueries have their own scope; other leaves can't contain windows.
        _ => {}
    }
}

/// Recursively collect aggregate function expressions from an expression tree.
fn collect_aggregates_from_expr(
    expr: &vibesql_ast::Expression,
    aggregates: &mut Vec<vibesql_ast::Expression>,
) {
    use vibesql_ast::Expression;

    match expr {
        Expression::AggregateFunction { .. } => {
            // Check if this aggregate is already collected (avoid duplicates)
            let already_exists = aggregates
                .iter()
                .any(|existing| format!("{:?}", existing) == format!("{:?}", expr));
            if !already_exists {
                aggregates.push(expr.clone());
            }
        }

        // Recursively search in compound expressions
        Expression::BinaryOp { left, right, .. } => {
            collect_aggregates_from_expr(left, aggregates);
            collect_aggregates_from_expr(right, aggregates);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            collect_aggregates_from_expr(inner, aggregates);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_aggregates_from_expr(arg, aggregates);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_aggregates_from_expr(op, aggregates);
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_aggregates_from_expr(cond, aggregates);
                }
                collect_aggregates_from_expr(&when_clause.result, aggregates);
            }
            if let Some(else_expr) = else_result {
                collect_aggregates_from_expr(else_expr, aggregates);
            }
        }
        Expression::Cast { expr: inner, .. } => {
            collect_aggregates_from_expr(inner, aggregates);
        }
        Expression::Between { expr, low, high, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
            collect_aggregates_from_expr(low, aggregates);
            collect_aggregates_from_expr(high, aggregates);
        }
        Expression::InList { expr, values, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
            for item in values {
                collect_aggregates_from_expr(item, aggregates);
            }
        }

        // Leaf expressions - nothing to extract
        Expression::Literal(_)
        | Expression::ColumnRef(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::NextValue { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::PseudoVariable { .. }
        | Expression::DuplicateKeyValue { .. }
        | Expression::SessionVariable { .. } => {}

        // Skip subqueries and window functions
        Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::In { .. }
        | Expression::QuantifiedComparison { .. }
        | Expression::WindowFunction { .. } => {}

        // Other expressions we might need to handle
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
            collect_aggregates_from_expr(pattern, aggregates);
        }
        Expression::IsNull { expr, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            collect_aggregates_from_expr(left, aggregates);
            collect_aggregates_from_expr(right, aggregates);
        }
        Expression::IsTruthValue { expr, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
        }
        Expression::Conjunction(terms) | Expression::Disjunction(terms) => {
            for term in terms {
                collect_aggregates_from_expr(term, aggregates);
            }
        }
        Expression::RowValueConstructor(items) => {
            for item in items {
                collect_aggregates_from_expr(item, aggregates);
            }
        }
        Expression::Position { substring, string, .. } => {
            collect_aggregates_from_expr(substring, aggregates);
            collect_aggregates_from_expr(string, aggregates);
        }
        Expression::Trim { string, removal_char, .. } => {
            collect_aggregates_from_expr(string, aggregates);
            if let Some(c) = removal_char {
                collect_aggregates_from_expr(c, aggregates);
            }
        }
        Expression::Extract { expr, .. } => {
            collect_aggregates_from_expr(expr, aggregates);
        }
        Expression::Interval { value, .. } => {
            collect_aggregates_from_expr(value, aggregates);
        }
        Expression::Collate { expr: inner, .. } => {
            collect_aggregates_from_expr(inner, aggregates);
        }
        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                collect_aggregates_from_expr(msg, aggregates);
            }
        }
        Expression::MatchAgainst { .. } => {}
    }
}
