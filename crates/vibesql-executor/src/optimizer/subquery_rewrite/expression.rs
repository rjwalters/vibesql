//! Expression rewriting and traversal
//!
//! This module provides recursive expression rewriting to apply IN subquery
//! optimizations throughout the entire query AST.

use vibesql_ast::{Expression, SelectItem, SelectStmt};

use super::correlation::is_correlated;
use super::transformations::{
    add_distinct_to_in_subquery, rewrite_exists_to_in, rewrite_in_to_exists,
};

/// Rewrite an expression to optimize IN subqueries
///
/// This function recursively traverses the expression tree and applies
/// the appropriate optimization for each IN subquery encountered.
pub(super) fn rewrite_expression(
    expr: &Expression,
    rewrite_subquery_fn: &impl Fn(&SelectStmt) -> SelectStmt,
) -> Expression {
    rewrite_expression_with_context(expr, rewrite_subquery_fn, &[])
}

/// Rewrite expression with outer table context for EXISTS decorrelation
pub(super) fn rewrite_expression_with_context(
    expr: &Expression,
    rewrite_subquery_fn: &impl Fn(&SelectStmt) -> SelectStmt,
    outer_tables: &[String],
) -> Expression {
    match expr {
        // Optimize IN subquery
        Expression::In { expr: in_expr, subquery, negated } => {
            // Validate that this is a single-column IN subquery
            // Multi-column IN requires tuple comparison which we don't optimize
            if subquery.select_list.len() != 1 {
                // Multi-column IN: skip optimization
                return Expression::In {
                    expr: Box::new(rewrite_expression_with_context(
                        in_expr,
                        rewrite_subquery_fn,
                        outer_tables,
                    )),
                    subquery: Box::new(rewrite_subquery_fn(subquery)),
                    negated: *negated,
                };
            }

            // Check if subquery SELECT expression is a simple column reference
            // Complex expressions (e.g., UPPER(col)) can't be safely used in correlation predicates
            let is_simple_column = matches!(
                subquery.select_list.first(),
                Some(SelectItem::Expression { expr: Expression::ColumnRef { .. }, .. })
            );

            // Check if subquery is correlated
            if is_correlated(subquery) && is_simple_column {
                // Correlated subquery with simple column: Rewrite IN → EXISTS
                // This allows database to stop after first match and better leverage indexes
                rewrite_in_to_exists(in_expr, subquery, *negated)
            } else if is_correlated(subquery) && !is_simple_column {
                // Correlated subquery with complex expression: skip IN → EXISTS
                // Complex expressions can't be safely used in correlation predicates
                // Fall back to DISTINCT optimization only
                let optimized_subquery = add_distinct_to_in_subquery(subquery);
                let optimized_subquery = rewrite_subquery_fn(&optimized_subquery);
                Expression::In {
                    expr: Box::new(rewrite_expression_with_context(
                        in_expr,
                        rewrite_subquery_fn,
                        outer_tables,
                    )),
                    subquery: Box::new(optimized_subquery),
                    negated: *negated,
                }
            } else {
                // Uncorrelated subquery: Add DISTINCT to reduce duplicate processing
                let optimized_subquery = add_distinct_to_in_subquery(subquery);
                let optimized_subquery = rewrite_subquery_fn(&optimized_subquery);
                Expression::In {
                    expr: Box::new(rewrite_expression_with_context(
                        in_expr,
                        rewrite_subquery_fn,
                        outer_tables,
                    )),
                    subquery: Box::new(optimized_subquery),
                    negated: *negated,
                }
            }
        }

        // Recursively rewrite nested expressions (preserve outer_tables context)
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(rewrite_expression_with_context(
                left,
                rewrite_subquery_fn,
                outer_tables,
            )),
            right: Box::new(rewrite_expression_with_context(
                right,
                rewrite_subquery_fn,
                outer_tables,
            )),
        },

        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
        },

        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
            negated: *negated,
        },

        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|e| {
                Box::new(rewrite_expression_with_context(e, rewrite_subquery_fn, outer_tables))
            }),
            when_clauses: when_clauses
                .iter()
                .map(|clause| vibesql_ast::CaseWhen {
                    conditions: clause
                        .conditions
                        .iter()
                        .map(|c| {
                            rewrite_expression_with_context(c, rewrite_subquery_fn, outer_tables)
                        })
                        .collect(),
                    result: rewrite_expression_with_context(
                        &clause.result,
                        rewrite_subquery_fn,
                        outer_tables,
                    ),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| {
                Box::new(rewrite_expression_with_context(e, rewrite_subquery_fn, outer_tables))
            }),
        },

        Expression::ScalarSubquery(subquery) => {
            Expression::ScalarSubquery(Box::new(rewrite_subquery_fn(subquery)))
        }

        Expression::Exists { subquery, negated } => {
            // Try to decorrelate EXISTS to IN for better performance
            if is_correlated(subquery) && !outer_tables.is_empty() {
                if let Some((outer_expr, decorrelated_subquery, neg)) =
                    rewrite_exists_to_in(subquery, *negated, outer_tables)
                {
                    // Successfully decorrelated! Return as IN expression
                    return Expression::In {
                        expr: Box::new(outer_expr),
                        subquery: Box::new(rewrite_subquery_fn(&decorrelated_subquery)),
                        negated: neg,
                    };
                }
            }
            // Fallback: keep EXISTS but recursively optimize inner subquery
            Expression::Exists {
                subquery: Box::new(rewrite_subquery_fn(subquery)),
                negated: *negated,
            }
        }

        Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(rewrite_expression_with_context(
                    expr,
                    rewrite_subquery_fn,
                    outer_tables,
                )),
                op: *op,
                quantifier: quantifier.clone(),
                subquery: Box::new(rewrite_subquery_fn(subquery)),
            }
        }

        Expression::InList { expr, values, negated } => Expression::InList {
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
            values: values
                .iter()
                .map(|v| rewrite_expression_with_context(v, rewrite_subquery_fn, outer_tables))
                .collect(),
            negated: *negated,
        },

        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
            low: Box::new(rewrite_expression_with_context(low, rewrite_subquery_fn, outer_tables)),
            high: Box::new(rewrite_expression_with_context(
                high,
                rewrite_subquery_fn,
                outer_tables,
            )),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::Cast { expr, data_type } => Expression::Cast {
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
            data_type: data_type.clone(),
        },

        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_expression_with_context(a, rewrite_subquery_fn, outer_tables))
                .collect(),
            character_unit: character_unit.clone(),
        },

        Expression::AggregateFunction { name, distinct, args } => Expression::AggregateFunction {
            name: name.clone(),
            distinct: *distinct,
            args: args
                .iter()
                .map(|a| rewrite_expression_with_context(a, rewrite_subquery_fn, outer_tables))
                .collect(),
        },

        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(rewrite_expression_with_context(
                substring,
                rewrite_subquery_fn,
                outer_tables,
            )),
            string: Box::new(rewrite_expression_with_context(
                string,
                rewrite_subquery_fn,
                outer_tables,
            )),
            character_unit: character_unit.clone(),
        },

        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position: position.clone(),
            removal_char: removal_char.as_ref().map(|e| {
                Box::new(rewrite_expression_with_context(e, rewrite_subquery_fn, outer_tables))
            }),
            string: Box::new(rewrite_expression_with_context(
                string,
                rewrite_subquery_fn,
                outer_tables,
            )),
        },

        Expression::Extract { field, expr } => Expression::Extract {
            field: field.clone(),
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
        },

        Expression::Like { expr, pattern, negated } => Expression::Like {
            expr: Box::new(rewrite_expression_with_context(
                expr,
                rewrite_subquery_fn,
                outer_tables,
            )),
            pattern: Box::new(rewrite_expression_with_context(
                pattern,
                rewrite_subquery_fn,
                outer_tables,
            )),
            negated: *negated,
        },

        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(rewrite_expression_with_context(
                    value,
                    rewrite_subquery_fn,
                    outer_tables,
                )),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            }
        }

        Expression::Conjunction(children) => Expression::Conjunction(
            children
                .iter()
                .map(|child| {
                    rewrite_expression_with_context(child, rewrite_subquery_fn, outer_tables)
                })
                .collect(),
        ),

        Expression::Disjunction(children) => Expression::Disjunction(
            children
                .iter()
                .map(|child| {
                    rewrite_expression_with_context(child, rewrite_subquery_fn, outer_tables)
                })
                .collect(),
        ),

        // Literals, column refs, and special expressions don't need rewriting
        Expression::Literal(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::WindowFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => expr.clone(),
    }
}

/// Recursively rewrite FROM clause subqueries
pub(super) fn rewrite_from_clause(
    from: &vibesql_ast::FromClause,
    rewrite_subquery_fn: &impl Fn(&SelectStmt) -> SelectStmt,
) -> vibesql_ast::FromClause {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => vibesql_ast::FromClause::Table {
            name: name.clone(),
            alias: alias.clone(),
            column_aliases: None,
        },
        vibesql_ast::FromClause::Join { left, right, join_type, condition, natural } => {
            vibesql_ast::FromClause::Join {
                left: Box::new(rewrite_from_clause(left, rewrite_subquery_fn)),
                right: Box::new(rewrite_from_clause(right, rewrite_subquery_fn)),
                join_type: join_type.clone(),
                condition: condition.as_ref().map(|c| rewrite_expression(c, rewrite_subquery_fn)),
                natural: *natural,
            }
        }
        vibesql_ast::FromClause::Subquery { query, alias, .. } => {
            vibesql_ast::FromClause::Subquery {
                query: Box::new(rewrite_subquery_fn(query)),
                alias: alias.clone(),
                column_aliases: None,
            }
        }
    }
}
