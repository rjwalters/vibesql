//! GROUP BY alias resolution
//!
//! Handles resolution of SELECT list aliases and column positions
//! in GROUP BY clauses, enabling standard SQL alias syntax.

use vibesql_ast::Expression;

use super::ResolvedGroupingSet;
use crate::errors::ExecutorError;

/// Resolve GROUP BY expression that might be a SELECT list alias or column position
///
/// Similar to ORDER BY alias resolution, handles three cases:
/// 1. Numeric literal (e.g., GROUP BY 1, 2, 3) - returns the expression from that position in
///    SELECT list (returns error if out of range)
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Otherwise - returns the original GROUP BY expression
///
/// This enables standard SQL behavior where GROUP BY can reference SELECT aliases:
/// ```sql
/// SELECT n_name as nation, COUNT(*) FROM ... GROUP BY nation
/// ```
///
/// # Errors
/// Returns `GroupByOutOfRange` error when a numeric position is 0 or exceeds select_list length.
pub fn resolve_group_by_alias(
    group_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    term_index: usize,
) -> Result<Expression, ExecutorError> {
    // Check for numeric column position (GROUP BY 1, 2, 3, etc.)
    if let Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = group_expr {
        // Validate column position range
        if *pos <= 0 || (*pos as usize) > select_list.len() {
            return Err(ExecutorError::GroupByOutOfRange {
                term_position: term_index + 1, // 1-indexed for error message
                column_number: *pos,
                select_list_len: select_list.len(),
            });
        }
        // Valid column position, return the expression at that position
        let idx = (*pos as usize) - 1;
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
            return Ok(expr.clone());
        }
    }

    // Check if GROUP BY expression is a simple column reference (no table qualifier)
    if let Expression::ColumnRef(col_id) = group_expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            let column = col_id.column_canonical();
            // Search for matching alias in SELECT list (case-insensitive)
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr, alias: Some(alias_name), ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // Found matching alias, use the SELECT list expression
                        return Ok(expr.clone());
                    }
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    Ok(group_expr.clone())
}

/// Resolve all aliases in a ResolvedGroupingSet against SELECT list
///
/// Processes each GROUP BY expression, resolving any aliases to their
/// underlying expressions from the SELECT list.
///
/// # Errors
/// Returns `GroupByOutOfRange` error when a numeric position is out of range.
pub fn resolve_grouping_set_aliases(
    set: &ResolvedGroupingSet,
    select_list: &[vibesql_ast::SelectItem],
) -> Result<ResolvedGroupingSet, ExecutorError> {
    let mut resolved_exprs = Vec::with_capacity(set.group_by_exprs.len());
    for (i, expr) in set.group_by_exprs.iter().enumerate() {
        resolved_exprs.push(resolve_group_by_alias(expr, select_list, i)?);
    }
    Ok(ResolvedGroupingSet { group_by_exprs: resolved_exprs, rolled_up: set.rolled_up.clone() })
}

/// Resolve aliases in base expressions for GroupingContext
///
/// Used to ensure GROUPING() function can properly match aliased columns
///
/// # Errors
/// Returns `GroupByOutOfRange` error when a numeric position is out of range.
pub fn resolve_base_expressions_aliases(
    base_exprs: &[Expression],
    select_list: &[vibesql_ast::SelectItem],
) -> Result<Vec<Expression>, ExecutorError> {
    let mut resolved = Vec::with_capacity(base_exprs.len());
    for (i, expr) in base_exprs.iter().enumerate() {
        resolved.push(resolve_group_by_alias(expr, select_list, i)?);
    }
    Ok(resolved)
}

/// Resolve aliases in HAVING expression against SELECT list
///
/// Recursively replaces column references that match SELECT list aliases
/// with the underlying expression. This allows:
/// ```sql
/// SELECT log, count(*) AS y FROM t1 GROUP BY log HAVING y >= 4
/// ```
///
/// The alias `y` will be resolved to `count(*)`.
///
/// IMPORTANT: Column references that match GROUP BY column names should NOT
/// be resolved to SELECT aliases, because HAVING should evaluate against the
/// GROUP BY columns, not SELECT aliases that happen to have the same name.
/// For example:
/// ```sql
/// SELECT -col1 AS col1 FROM t GROUP BY col1 HAVING col1 > 0
/// ```
/// Here `col1` in HAVING refers to the GROUP BY column, not `-col1`.
///
/// Similarly, column references that match actual table columns should NOT
/// be resolved to SELECT aliases. Table columns take precedence over aliases.
/// For example:
/// ```sql
/// SELECT AVG(col2) AS col0 FROM tab0 GROUP BY col2 HAVING AVG(col0) IS NOT NULL
/// ```
/// Here `col0` in HAVING refers to the table column col0, not the AVG(col2) alias.
#[allow(dead_code)]
pub fn resolve_having_aliases(
    having_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    group_by_exprs: &[Expression],
) -> Expression {
    // No schema provided - use legacy behavior (for backward compatibility)
    resolve_having_aliases_inner(having_expr, select_list, group_by_exprs, None)
}

/// Resolve aliases in HAVING expression with schema awareness
///
/// Like `resolve_having_aliases`, but also accepts a set of actual table column names.
/// Column references that match table columns are NOT resolved to SELECT aliases,
/// because table columns take precedence over aliases in SQL.
///
/// The `schema_columns` parameter should contain lowercase column names from the table schema.
///
/// Note: For HAVING clauses with correlated subqueries, use `resolve_having_aliases_with_values`
/// instead, as it also resolves outer alias references within subqueries.
#[allow(dead_code)]
fn resolve_having_aliases_with_schema(
    having_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    group_by_exprs: &[Expression],
    schema_columns: &std::collections::HashSet<String>,
) -> Expression {
    resolve_having_aliases_inner(having_expr, select_list, group_by_exprs, Some(schema_columns))
}

/// Resolve aliases in HAVING expression with schema awareness and computed alias values.
///
/// This variant also traverses into subqueries (EXISTS, IN, etc.) and replaces
/// references to outer SELECT aliases with their computed literal values.
/// This is necessary for correlated subqueries that reference outer aliases
/// in their HAVING clauses (issue #4676).
///
/// Example:
/// ```sql
/// SELECT a.x, avg(a.y) AS avg1 FROM t AS a GROUP BY a.x
/// HAVING NOT EXISTS(
///   SELECT b.x, avg(b.y) AS avg2 FROM t AS b GROUP BY b.x
///   HAVING avg1 > avg2  -- avg1 from outer query
/// )
/// ```
/// Here `avg1` in the inner HAVING is resolved to its computed value (e.g., 2.5).
pub fn resolve_having_aliases_with_values(
    having_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    group_by_exprs: &[Expression],
    schema_columns: &std::collections::HashSet<String>,
    alias_values: &std::collections::HashMap<String, vibesql_types::SqlValue>,
) -> Expression {
    resolve_having_aliases_with_values_inner(
        having_expr,
        select_list,
        group_by_exprs,
        Some(schema_columns),
        alias_values,
    )
}

/// Check if a column name matches any GROUP BY column expression
fn is_group_by_column(column: &str, group_by_exprs: &[Expression]) -> bool {
    for expr in group_by_exprs {
        if let Expression::ColumnRef(col_id) = expr {
            if col_id.schema_canonical().is_none() {
                let group_col = col_id.column_canonical();
                if group_col.eq_ignore_ascii_case(column) {
                    return true;
                }
            }
        }
    }
    false
}

fn resolve_having_aliases_inner(
    having_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    group_by_exprs: &[Expression],
    schema_columns: Option<&std::collections::HashSet<String>>,
) -> Expression {
    match having_expr {
        // Check if this column reference matches a SELECT list alias
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // Skip alias resolution if this column is in GROUP BY
            // GROUP BY columns take precedence over SELECT aliases in HAVING
            if is_group_by_column(column, group_by_exprs) {
                return having_expr.clone();
            }

            // Skip alias resolution if this column exists in the table schema
            // Actual table columns take precedence over SELECT aliases (#4531)
            if let Some(schema_cols) = schema_columns {
                if schema_cols.contains(&column.to_lowercase()) {
                    return having_expr.clone();
                }
            }

            // Search for matching alias in SELECT list (case-insensitive)
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr, alias: Some(alias_name), ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        // Found matching alias, use the SELECT list expression
                        return expr.clone();
                    }
                }
            }
            // No match, return original expression
            having_expr.clone()
        }

        // Recursively resolve in binary operations
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(resolve_having_aliases_inner(
                left,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
            op: *op,
            right: Box::new(resolve_having_aliases_inner(
                right,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
        },

        // Recursively resolve in unary operations
        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(resolve_having_aliases_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
        },

        // Recursively resolve in function arguments
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| {
                    resolve_having_aliases_inner(a, select_list, group_by_exprs, schema_columns)
                })
                .collect(),
            character_unit: character_unit.clone(),
        },

        // Recursively resolve in aggregate function arguments
        Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                distinct: *distinct,
                args: args
                    .iter()
                    .map(|a| {
                        resolve_having_aliases_inner(a, select_list, group_by_exprs, schema_columns)
                    })
                    .collect(),
                order_by: order_by.clone(),
                filter: filter.as_ref().map(|f| {
                    Box::new(resolve_having_aliases_inner(
                        f,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                    ))
                }),
            }
        }

        // Recursively resolve in CASE expressions
        Expression::Case { operand, when_clauses, else_result } => {
            let resolved_operand = operand.as_ref().map(|o| {
                Box::new(resolve_having_aliases_inner(
                    o,
                    select_list,
                    group_by_exprs,
                    schema_columns,
                ))
            });
            let resolved_when_clauses = when_clauses
                .iter()
                .map(|wc| vibesql_ast::CaseWhen {
                    conditions: wc
                        .conditions
                        .iter()
                        .map(|c| {
                            resolve_having_aliases_inner(
                                c,
                                select_list,
                                group_by_exprs,
                                schema_columns,
                            )
                        })
                        .collect(),
                    result: resolve_having_aliases_inner(
                        &wc.result,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                    ),
                })
                .collect();
            let resolved_else = else_result.as_ref().map(|e| {
                Box::new(resolve_having_aliases_inner(
                    e,
                    select_list,
                    group_by_exprs,
                    schema_columns,
                ))
            });
            Expression::Case {
                operand: resolved_operand,
                when_clauses: resolved_when_clauses,
                else_result: resolved_else,
            }
        }

        // Recursively resolve in boolean expressions
        Expression::Conjunction(terms) => Expression::Conjunction(
            terms
                .iter()
                .map(|t| {
                    resolve_having_aliases_inner(t, select_list, group_by_exprs, schema_columns)
                })
                .collect(),
        ),
        Expression::Disjunction(terms) => Expression::Disjunction(
            terms
                .iter()
                .map(|t| {
                    resolve_having_aliases_inner(t, select_list, group_by_exprs, schema_columns)
                })
                .collect(),
        ),

        // Recursively resolve in comparison expressions
        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(resolve_having_aliases_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
            low: Box::new(resolve_having_aliases_inner(
                low,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
            high: Box::new(resolve_having_aliases_inner(
                high,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(resolve_having_aliases_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
            )),
            negated: *negated,
        },

        // For other expression types, return as-is
        _ => having_expr.clone(),
    }
}

/// Inner implementation for resolving HAVING aliases with computed values.
/// Traverses into subqueries and replaces outer alias references with literal values.
fn resolve_having_aliases_with_values_inner(
    having_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
    group_by_exprs: &[Expression],
    schema_columns: Option<&std::collections::HashSet<String>>,
    alias_values: &std::collections::HashMap<String, vibesql_types::SqlValue>,
) -> Expression {
    match having_expr {
        // Check if this column reference matches a SELECT list alias
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();

            // Skip alias resolution if this column is in GROUP BY
            if is_group_by_column(column, group_by_exprs) {
                return having_expr.clone();
            }

            // Skip alias resolution if this column exists in the table schema
            if let Some(schema_cols) = schema_columns {
                if schema_cols.contains(&column.to_lowercase()) {
                    return having_expr.clone();
                }
            }

            // Search for matching alias in SELECT list and replace with expression
            for item in select_list {
                if let vibesql_ast::SelectItem::Expression {
                    expr, alias: Some(alias_name), ..
                } = item
                {
                    if alias_name.eq_ignore_ascii_case(column) {
                        return expr.clone();
                    }
                }
            }
            having_expr.clone()
        }

        // Recursively resolve in binary operations
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(resolve_having_aliases_with_values_inner(
                left,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
            op: *op,
            right: Box::new(resolve_having_aliases_with_values_inner(
                right,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
        },

        // Recursively resolve in unary operations
        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(resolve_having_aliases_with_values_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
        },

        // Recursively resolve in function arguments
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| {
                    resolve_having_aliases_with_values_inner(
                        a,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                        alias_values,
                    )
                })
                .collect(),
            character_unit: character_unit.clone(),
        },

        // Recursively resolve in aggregate function arguments
        Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                distinct: *distinct,
                args: args
                    .iter()
                    .map(|a| {
                        resolve_having_aliases_with_values_inner(
                            a,
                            select_list,
                            group_by_exprs,
                            schema_columns,
                            alias_values,
                        )
                    })
                    .collect(),
                order_by: order_by.clone(),
                filter: filter.as_ref().map(|f| {
                    Box::new(resolve_having_aliases_with_values_inner(
                        f,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                        alias_values,
                    ))
                }),
            }
        }

        // Recursively resolve in CASE expressions
        Expression::Case { operand, when_clauses, else_result } => {
            let resolved_operand = operand.as_ref().map(|o| {
                Box::new(resolve_having_aliases_with_values_inner(
                    o,
                    select_list,
                    group_by_exprs,
                    schema_columns,
                    alias_values,
                ))
            });
            let resolved_when_clauses = when_clauses
                .iter()
                .map(|wc| vibesql_ast::CaseWhen {
                    conditions: wc
                        .conditions
                        .iter()
                        .map(|c| {
                            resolve_having_aliases_with_values_inner(
                                c,
                                select_list,
                                group_by_exprs,
                                schema_columns,
                                alias_values,
                            )
                        })
                        .collect(),
                    result: resolve_having_aliases_with_values_inner(
                        &wc.result,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                        alias_values,
                    ),
                })
                .collect();
            let resolved_else = else_result.as_ref().map(|e| {
                Box::new(resolve_having_aliases_with_values_inner(
                    e,
                    select_list,
                    group_by_exprs,
                    schema_columns,
                    alias_values,
                ))
            });
            Expression::Case {
                operand: resolved_operand,
                when_clauses: resolved_when_clauses,
                else_result: resolved_else,
            }
        }

        // Recursively resolve in boolean expressions
        Expression::Conjunction(terms) => Expression::Conjunction(
            terms
                .iter()
                .map(|t| {
                    resolve_having_aliases_with_values_inner(
                        t,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                        alias_values,
                    )
                })
                .collect(),
        ),
        Expression::Disjunction(terms) => Expression::Disjunction(
            terms
                .iter()
                .map(|t| {
                    resolve_having_aliases_with_values_inner(
                        t,
                        select_list,
                        group_by_exprs,
                        schema_columns,
                        alias_values,
                    )
                })
                .collect(),
        ),

        // Recursively resolve in comparison expressions
        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(resolve_having_aliases_with_values_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
            low: Box::new(resolve_having_aliases_with_values_inner(
                low,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
            high: Box::new(resolve_having_aliases_with_values_inner(
                high,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(resolve_having_aliases_with_values_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            )),
            negated: *negated,
        },

        // Handle EXISTS subquery - traverse into the subquery and resolve outer aliases
        Expression::Exists { subquery, negated } => {
            let resolved_subquery = resolve_outer_aliases_in_subquery(subquery, alias_values);
            Expression::Exists { subquery: Box::new(resolved_subquery), negated: *negated }
        }

        // Handle IN with subquery - resolve outer aliases in subquery
        Expression::In { expr, subquery, negated } => {
            let resolved_expr = resolve_having_aliases_with_values_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            );
            let resolved_subquery = resolve_outer_aliases_in_subquery(subquery, alias_values);
            Expression::In {
                expr: Box::new(resolved_expr),
                subquery: Box::new(resolved_subquery),
                negated: *negated,
            }
        }

        // Handle scalar subquery - resolve outer aliases in subquery
        Expression::ScalarSubquery(subquery) => {
            let resolved_subquery = resolve_outer_aliases_in_subquery(subquery, alias_values);
            Expression::ScalarSubquery(Box::new(resolved_subquery))
        }

        // Handle quantified comparison (ALL/ANY/SOME) - resolve outer aliases in subquery
        Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
            let resolved_expr = resolve_having_aliases_with_values_inner(
                expr,
                select_list,
                group_by_exprs,
                schema_columns,
                alias_values,
            );
            let resolved_subquery = resolve_outer_aliases_in_subquery(subquery, alias_values);
            Expression::QuantifiedComparison {
                expr: Box::new(resolved_expr),
                op: *op,
                quantifier: quantifier.clone(),
                subquery: Box::new(resolved_subquery),
            }
        }

        // For other expression types, return as-is
        _ => having_expr.clone(),
    }
}

/// Resolve outer alias references in a subquery by replacing them with literal values.
/// This enables correlated subqueries to access outer SELECT aliases.
fn resolve_outer_aliases_in_subquery(
    subquery: &vibesql_ast::SelectStmt,
    alias_values: &std::collections::HashMap<String, vibesql_types::SqlValue>,
) -> vibesql_ast::SelectStmt {
    // Create a clone and resolve aliases in the HAVING clause
    let resolved_having = subquery
        .having
        .as_ref()
        .map(|having_expr| resolve_outer_alias_refs_to_literals(having_expr, alias_values));

    // Also resolve in WHERE clause (for correlated subqueries that reference outer aliases there)
    let resolved_where = subquery
        .where_clause
        .as_ref()
        .map(|where_expr| resolve_outer_alias_refs_to_literals(where_expr, alias_values));

    // Build the resolved subquery
    vibesql_ast::SelectStmt {
        with_clause: subquery.with_clause.clone(),
        distinct: subquery.distinct,
        select_list: subquery.select_list.clone(),
        into_table: subquery.into_table.clone(),
        into_variables: subquery.into_variables.clone(),
        from: subquery.from.clone(),
        where_clause: resolved_where,
        group_by: subquery.group_by.clone(),
        having: resolved_having,
        order_by: subquery.order_by.clone(),
        limit: subquery.limit.clone(),
        offset: subquery.offset.clone(),
        set_operation: subquery.set_operation.clone(),
        values: subquery.values.clone(),
    }
}

/// Replace unqualified column references that match outer aliases with literal values.
/// This is the core transformation that makes outer SELECT aliases accessible in subqueries.
fn resolve_outer_alias_refs_to_literals(
    expr: &Expression,
    alias_values: &std::collections::HashMap<String, vibesql_types::SqlValue>,
) -> Expression {
    match expr {
        // Check if this unqualified column reference matches an outer alias
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // Look up the column name in alias_values (case-insensitive)
            for (alias_name, value) in alias_values {
                if alias_name.eq_ignore_ascii_case(column) {
                    // Found a match - replace with literal value
                    return Expression::Literal(value.clone());
                }
            }
            // Not an outer alias, keep as-is
            expr.clone()
        }

        // Recursively process compound expressions
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(resolve_outer_alias_refs_to_literals(left, alias_values)),
            op: *op,
            right: Box::new(resolve_outer_alias_refs_to_literals(right, alias_values)),
        },

        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(resolve_outer_alias_refs_to_literals(inner, alias_values)),
        },

        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_outer_alias_refs_to_literals(a, alias_values))
                .collect(),
            character_unit: character_unit.clone(),
        },

        Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                distinct: *distinct,
                args: args
                    .iter()
                    .map(|a| resolve_outer_alias_refs_to_literals(a, alias_values))
                    .collect(),
                order_by: order_by.clone(),
                filter: filter
                    .as_ref()
                    .map(|f| Box::new(resolve_outer_alias_refs_to_literals(f, alias_values))),
            }
        }

        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(resolve_outer_alias_refs_to_literals(o, alias_values))),
            when_clauses: when_clauses
                .iter()
                .map(|wc| vibesql_ast::CaseWhen {
                    conditions: wc
                        .conditions
                        .iter()
                        .map(|c| resolve_outer_alias_refs_to_literals(c, alias_values))
                        .collect(),
                    result: resolve_outer_alias_refs_to_literals(&wc.result, alias_values),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(resolve_outer_alias_refs_to_literals(e, alias_values))),
        },

        Expression::Conjunction(terms) => Expression::Conjunction(
            terms.iter().map(|t| resolve_outer_alias_refs_to_literals(t, alias_values)).collect(),
        ),

        Expression::Disjunction(terms) => Expression::Disjunction(
            terms.iter().map(|t| resolve_outer_alias_refs_to_literals(t, alias_values)).collect(),
        ),

        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            low: Box::new(resolve_outer_alias_refs_to_literals(low, alias_values)),
            high: Box::new(resolve_outer_alias_refs_to_literals(high, alias_values)),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            negated: *negated,
        },

        Expression::InList { expr, values, negated } => Expression::InList {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            values: values
                .iter()
                .map(|v| resolve_outer_alias_refs_to_literals(v, alias_values))
                .collect(),
            negated: *negated,
        },

        Expression::Like { expr, pattern, negated } => Expression::Like {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            pattern: Box::new(resolve_outer_alias_refs_to_literals(pattern, alias_values)),
            negated: *negated,
        },

        Expression::Glob { expr, pattern, negated } => Expression::Glob {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            pattern: Box::new(resolve_outer_alias_refs_to_literals(pattern, alias_values)),
            negated: *negated,
        },

        Expression::Cast { expr, data_type } => Expression::Cast {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            data_type: data_type.clone(),
        },

        // Nested subqueries - recursively resolve in their clauses
        Expression::Exists { subquery, negated } => {
            let resolved_subquery = resolve_outer_aliases_in_subquery(subquery, alias_values);
            Expression::Exists { subquery: Box::new(resolved_subquery), negated: *negated }
        }

        Expression::In { expr, subquery, negated } => Expression::In {
            expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
            subquery: Box::new(resolve_outer_aliases_in_subquery(subquery, alias_values)),
            negated: *negated,
        },

        Expression::ScalarSubquery(subquery) => Expression::ScalarSubquery(Box::new(
            resolve_outer_aliases_in_subquery(subquery, alias_values),
        )),

        Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(resolve_outer_alias_refs_to_literals(expr, alias_values)),
                op: *op,
                quantifier: quantifier.clone(),
                subquery: Box::new(resolve_outer_aliases_in_subquery(subquery, alias_values)),
            }
        }

        // For other expressions, return as-is
        _ => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::SelectItem;

    use super::*;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(name, false))
    }

    fn select_item(expr: Expression, alias: Option<&str>) -> SelectItem {
        SelectItem::Expression { expr, alias: alias.map(|s| s.to_string()), source_text: None }
    }

    #[test]
    fn test_resolve_group_by_alias_simple() {
        // SELECT n_name AS nation, COUNT(*) FROM ... GROUP BY nation
        let select_list = vec![
            select_item(col("n_name"), Some("nation")),
            select_item(
                Expression::AggregateFunction {
                    name: vibesql_ast::FunctionIdentifier::new("COUNT"),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                    order_by: None,
                    filter: None,
                },
                None,
            ),
        ];

        // GROUP BY nation should resolve to n_name
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        assert!(
            matches!(&resolved, Expression::ColumnRef(col_id) if col_id.column_canonical() == "n_name")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_case_insensitive() {
        // Alias matching should be case-insensitive
        let select_list = vec![select_item(col("n_name"), Some("NATION"))];

        // GROUP BY nation (lowercase) should match NATION (uppercase)
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        assert!(
            matches!(&resolved, Expression::ColumnRef(col_id) if col_id.column_canonical() == "n_name")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_numeric_position() {
        // GROUP BY 1 should return the first SELECT item
        let select_list =
            vec![select_item(col("n_name"), Some("nation")), select_item(col("amount"), None)];

        let group_expr = Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        assert!(
            matches!(&resolved, Expression::ColumnRef(col_id) if col_id.column_canonical() == "n_name")
        );

        // GROUP BY 2 should return the second SELECT item
        let group_expr2 = Expression::Literal(vibesql_types::SqlValue::Integer(2));
        let resolved2 = resolve_group_by_alias(&group_expr2, &select_list, 1).unwrap();

        assert!(
            matches!(&resolved2, Expression::ColumnRef(col_id) if col_id.column_canonical() == "amount")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_out_of_range() {
        // GROUP BY 0 should return an error
        let select_list =
            vec![select_item(col("n_name"), Some("nation")), select_item(col("amount"), None)];

        let group_expr_zero = Expression::Literal(vibesql_types::SqlValue::Integer(0));
        let result = resolve_group_by_alias(&group_expr_zero, &select_list, 0);
        assert!(matches!(result, Err(ExecutorError::GroupByOutOfRange { .. })));

        // GROUP BY 3 (out of range for 2 items) should return an error
        let group_expr_three = Expression::Literal(vibesql_types::SqlValue::Integer(3));
        let result = resolve_group_by_alias(&group_expr_three, &select_list, 0);
        assert!(matches!(result, Err(ExecutorError::GroupByOutOfRange { .. })));
    }

    #[test]
    fn test_resolve_group_by_alias_no_match() {
        // GROUP BY expression that doesn't match any alias should remain unchanged
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY something_else should not resolve to anything
        let group_expr = col("something_else");
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        assert!(
            matches!(&resolved, Expression::ColumnRef(col_id) if col_id.column_canonical() == "something_else")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_qualified_column() {
        // Qualified column references (e.g., t.col) should NOT resolve aliases
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY t.nation should NOT resolve to n_name (it's table-qualified)
        let group_expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
            "t", false, "nation", false,
        ));
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        // Should remain unchanged
        assert!(
            matches!(&resolved, Expression::ColumnRef(col_id) if col_id.table_canonical() == Some("t") && col_id.column_canonical() == "nation")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_expression_alias() {
        // SELECT SUBSTR(o_orderdate, 1, 4) AS o_year ... GROUP BY o_year
        let substr_expr = Expression::Function {
            name: vibesql_ast::FunctionIdentifier::new("SUBSTR"),
            args: vec![
                col("o_orderdate"),
                Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                Expression::Literal(vibesql_types::SqlValue::Integer(4)),
            ],
            character_unit: None,
        };
        let select_list = vec![select_item(substr_expr.clone(), Some("o_year"))];

        // GROUP BY o_year should resolve to SUBSTR(o_orderdate, 1, 4)
        let group_expr = col("o_year");
        let resolved = resolve_group_by_alias(&group_expr, &select_list, 0).unwrap();

        // Should be the SUBSTR expression
        assert!(matches!(resolved, Expression::Function { name, .. } if name == "SUBSTR"));
    }

    #[test]
    fn test_resolve_grouping_set_aliases() {
        // Test resolving aliases in a full grouping set
        let select_list = vec![
            select_item(col("n1_n_name"), Some("supp_nation")),
            select_item(col("n2_n_name"), Some("cust_nation")),
        ];

        let original_set = ResolvedGroupingSet {
            group_by_exprs: vec![col("supp_nation"), col("cust_nation")],
            rolled_up: vec![false, false],
        };

        let resolved_set = resolve_grouping_set_aliases(&original_set, &select_list).unwrap();

        // Both should resolve to the actual column names
        assert_eq!(resolved_set.group_by_exprs.len(), 2);
        assert!(
            matches!(&resolved_set.group_by_exprs[0], Expression::ColumnRef(col_id) if col_id.column_canonical() == "n1_n_name")
        );
        assert!(
            matches!(&resolved_set.group_by_exprs[1], Expression::ColumnRef(col_id) if col_id.column_canonical() == "n2_n_name")
        );
        // rolled_up should be preserved
        assert_eq!(resolved_set.rolled_up, vec![false, false]);
    }

    #[test]
    fn test_resolve_base_expressions_aliases() {
        let select_list =
            vec![select_item(col("a_col"), Some("a")), select_item(col("b_col"), Some("b"))];

        let base_exprs = vec![col("a"), col("b")];
        let resolved = resolve_base_expressions_aliases(&base_exprs, &select_list).unwrap();

        assert_eq!(resolved.len(), 2);
        assert!(
            matches!(&resolved[0], Expression::ColumnRef(col_id) if col_id.column_canonical() == "a_col")
        );
        assert!(
            matches!(&resolved[1], Expression::ColumnRef(col_id) if col_id.column_canonical() == "b_col")
        );
    }
}
