//! Helper functions for subquery-to-join transformations
//!
//! This module provides utility functions for:
//! - Collecting and detecting table names (for self-join detection)
//! - Qualifying and rewriting column references

use vibesql_ast::{Expression, FromClause};

/// Extract all table names from a FROM clause (for self-join detection)
pub(super) fn collect_table_names(from: &FromClause, names: &mut Vec<String>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            // Use alias if present, otherwise table name
            names.push(alias.clone().unwrap_or_else(|| name.clone()));
            names.push(name.clone()); // Also add original name for matching
        }
        FromClause::Join { left, right, .. } => {
            collect_table_names(left, names);
            collect_table_names(right, names);
        }
        FromClause::Subquery { alias, .. } => {
            names.push(alias.clone());
        }
    }
}

/// Check if a table name conflicts with existing tables in the FROM clause
pub(super) fn is_self_join(
    from: &FromClause,
    table_name: &str,
    table_alias: &Option<String>,
) -> bool {
    let mut existing_names = Vec::new();
    collect_table_names(from, &mut existing_names);

    // The effective name is alias if present, otherwise table_name
    let effective_name = table_alias.as_deref().unwrap_or(table_name);

    // Check if this name conflicts with any existing table
    existing_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case(effective_name) || n.eq_ignore_ascii_case(table_name))
}

/// Qualify an unqualified column reference with a table name
/// Only rewrites unqualified columns, leaves qualified ones unchanged
pub(super) fn qualify_outer_column_refs(expr: &Expression, outer_table: &str) -> Expression {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Unqualified column - add outer table qualifier
            Expression::ColumnRef { table: Some(outer_table.to_string()), column: column.clone() }
        }
        Expression::ColumnRef { table: Some(_), .. } => {
            // Already qualified - leave unchanged
            expr.clone()
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(qualify_outer_column_refs(left, outer_table)),
            op: op.clone(),
            right: Box::new(qualify_outer_column_refs(right, outer_table)),
        },
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| qualify_outer_column_refs(a, outer_table)).collect(),
            character_unit: character_unit.clone(),
        },
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            low: Box::new(qualify_outer_column_refs(low, outer_table)),
            high: Box::new(qualify_outer_column_refs(high, outer_table)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            values: values.iter().map(|v| qualify_outer_column_refs(v, outer_table)).collect(),
            negated: *negated,
        },
        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            pattern: Box::new(qualify_outer_column_refs(pattern, outer_table)),
            negated: *negated,
        },
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            data_type: data_type.clone(),
        },
        // IN subquery: qualify the outer expr but NOT the subquery (it has its own scope)
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(qualify_outer_column_refs(inner, outer_table)),
            subquery: subquery.clone(), // Don't qualify inside subquery - separate scope
            negated: *negated,
        },
        // EXISTS and scalar subqueries have their own scope, don't qualify inside
        Expression::Exists { .. } | Expression::ScalarSubquery(_) => expr.clone(),
        // For other expression types (Literal, Wildcard, etc.), just clone
        _ => expr.clone(),
    }
}

/// Rewrite column references in an expression to use a new table qualifier
pub(super) fn rewrite_column_refs_with_alias(
    expr: &Expression,
    old_table: &str,
    new_alias: &str,
) -> Expression {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Rewrite if:
            // 1. No table qualifier (unqualified column from the subquery table)
            // 2. Table qualifier matches the old table name
            let should_rewrite = match table {
                None => true, // Unqualified columns from subquery should be rewritten
                Some(t) => t.eq_ignore_ascii_case(old_table),
            };

            if should_rewrite {
                Expression::ColumnRef { table: Some(new_alias.to_string()), column: column.clone() }
            } else {
                expr.clone()
            }
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(rewrite_column_refs_with_alias(left, old_table, new_alias)),
            op: op.clone(),
            right: Box::new(rewrite_column_refs_with_alias(right, old_table, new_alias)),
        },
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
        },
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            low: Box::new(rewrite_column_refs_with_alias(low, old_table, new_alias)),
            high: Box::new(rewrite_column_refs_with_alias(high, old_table, new_alias)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            values: values
                .iter()
                .map(|v| rewrite_column_refs_with_alias(v, old_table, new_alias))
                .collect(),
            negated: *negated,
        },
        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            pattern: Box::new(rewrite_column_refs_with_alias(pattern, old_table, new_alias)),
            negated: *negated,
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_column_refs_with_alias(a, old_table, new_alias))
                .collect(),
            character_unit: character_unit.clone(),
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(rewrite_column_refs_with_alias(o, old_table, new_alias))),
            when_clauses: when_clauses
                .iter()
                .map(|case_when| vibesql_ast::CaseWhen {
                    conditions: case_when
                        .conditions
                        .iter()
                        .map(|c| rewrite_column_refs_with_alias(c, old_table, new_alias))
                        .collect(),
                    result: rewrite_column_refs_with_alias(&case_when.result, old_table, new_alias),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(rewrite_column_refs_with_alias(e, old_table, new_alias))),
        },
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            data_type: data_type.clone(),
        },
        // Handle nested IN subqueries: rewrite the outer expression but NOT the subquery
        // (the subquery has its own scope and column references shouldn't be changed)
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            subquery: subquery.clone(), // Don't rewrite inside subquery - different scope
            negated: *negated,
        },
        // Handle EXISTS subqueries similarly - don't rewrite inside the subquery
        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: subquery.clone(), // Don't rewrite inside subquery - different scope
            negated: *negated,
        },
        // Scalar subquery: same as EXISTS, separate scope
        Expression::ScalarSubquery(subquery) => Expression::ScalarSubquery(subquery.clone()),
        // For other expression types, just clone (they don't contain column refs that need rewriting)
        _ => expr.clone(),
    }
}
