//! Transform IN/EXISTS subqueries to semi/anti-joins
//!
//! Note: Some clone_on_copy lints are suppressed because the code is clearer
//! with explicit clones for operators that may not be Copy in future.

#![allow(clippy::clone_on_copy)]

//!
//! This module transforms decorrelated IN/NOT IN/EXISTS/NOT EXISTS subqueries
//! in the WHERE clause into SEMI/ANTI joins in the FROM clause, enabling
//! efficient hash-based join execution instead of row-by-row subquery evaluation.
//!
//! ## Transformation Examples
//!
//! ### IN → SEMI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### NOT IN → ANTI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders ANTI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### EXISTS → SEMI JOIN
//! ```sql
//! -- Before (after decorrelation):
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT DISTINCT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};
#[cfg(test)]
use vibesql_ast::GroupByClause;

/// Transform a SELECT statement by converting IN/NOT IN subqueries to semi/anti-joins
///
/// This transformation only applies to simple IN subqueries with single-column SELECT lists
/// and simple table references. Complex subqueries (joins, aggregations, etc.) are left unchanged.
///
/// The transformation is applied iteratively to handle queries with multiple subqueries
/// (e.g., Q21 with both EXISTS and NOT EXISTS clauses).
pub fn transform_subqueries_to_joins(stmt: &SelectStmt) -> SelectStmt {
    let mut result = stmt.clone();

    // Only transform if we have a FROM clause and a WHERE clause
    if result.from.is_none() || result.where_clause.is_none() {
        return result;
    }

    // Apply transformation iteratively until no more changes
    // This handles queries with multiple IN/EXISTS subqueries
    let max_iterations = 10; // Safety limit to prevent infinite loops
    for iteration in 0..max_iterations {
        let mut made_progress = false;

        // Try to extract IN/NOT IN/EXISTS subqueries from WHERE clause and convert to joins
        if let Some(where_clause) = &result.where_clause {
            if let Some((new_from, new_where)) = try_extract_subqueries_to_joins(
                result.from.as_ref().unwrap(),
                where_clause,
            ) {
                // Debug output for subquery transformation
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!("[SUBQUERY_TRANSFORM] Iteration {}: Converted subquery to join", iteration + 1);
                }
                result.from = Some(new_from);
                result.where_clause = new_where;
                made_progress = true;
            }
        }

        // If no transformation was applied this iteration, we're done
        if !made_progress {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() && iteration > 0 {
                eprintln!("[SUBQUERY_TRANSFORM] Completed after {} iterations", iteration);
            }
            break;
        }
    }

    result
}

/// Try to extract IN/NOT IN subqueries from WHERE clause and convert to semi/anti-joins
fn try_extract_subqueries_to_joins(
    from: &FromClause,
    where_clause: &Expression,
) -> Option<(FromClause, Option<Expression>)> {
    // Look for IN subquery at the top level or in AND branches
    match where_clause {
        // Single IN subquery
        Expression::In { expr, subquery, negated } => {
            if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                return Some((result.from, None)); // Removed WHERE clause entirely
            }
            None
        }

        // AND with potential IN/EXISTS subqueries
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left side first - check both IN and EXISTS
            match left.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                        // Successfully converted left IN side, keep right side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join table).
                        // The subquery's columns have already been rewritten to use the new alias,
                        // so column resolution will work correctly during execution.
                        return Some((result.from, Some((**right).clone())));
                    }
                }
                Expression::Exists { subquery, negated } => {
                    if let Some((join, _)) = try_convert_exists_to_join(from, subquery, *negated) {
                        // Successfully converted left EXISTS side, keep right side as WHERE clause
                        return Some((join, Some((**right).clone())));
                    }
                }
                _ => {}
            }

            // Try right side - check both IN and EXISTS
            match right.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                        // Successfully converted right IN side, keep left side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join table).
                        return Some((result.from, Some((**left).clone())));
                    }
                }
                Expression::Exists { subquery, negated } => {
                    if let Some((join, _)) = try_convert_exists_to_join(from, subquery, *negated) {
                        // Successfully converted right EXISTS side, keep left side as WHERE clause
                        return Some((join, Some((**left).clone())));
                    }
                }
                _ => {}
            }

            // Try recursively on left side
            if let Some((new_left_from, new_left_where)) = try_extract_subqueries_to_joins(from, left) {
                let combined_where = match new_left_where {
                    Some(new_left) => Some(Expression::BinaryOp {
                        op: BinaryOperator::And,
                        left: Box::new(new_left),
                        right: right.clone(),
                    }),
                    None => Some((**right).clone()),
                };
                return Some((new_left_from, combined_where));
            }

            // Try recursively on right side
            if let Some((new_right_from, new_right_where)) = try_extract_subqueries_to_joins(from, right) {
                let combined_where = match new_right_where {
                    Some(new_right) => Some(Expression::BinaryOp {
                        op: BinaryOperator::And,
                        left: left.clone(),
                        right: Box::new(new_right),
                    }),
                    None => Some((**left).clone()),
                };
                return Some((new_right_from, combined_where));
            }

            None
        }

        // EXISTS can also be converted (after decorrelation it becomes IN, but handle it directly too)
        Expression::Exists { subquery, negated } => {
            // Try to convert EXISTS to a semi-join by extracting correlation
            try_convert_exists_to_join(from, subquery, *negated)
        }

        _ => None,
    }
}

/// Extract all table names from a FROM clause (for self-join detection)
fn collect_table_names(from: &FromClause, names: &mut Vec<String>) {
    match from {
        FromClause::Table { name, alias } => {
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
fn is_self_join(from: &FromClause, table_name: &str, table_alias: &Option<String>) -> bool {
    let mut existing_names = Vec::new();
    collect_table_names(from, &mut existing_names);

    // The effective name is alias if present, otherwise table_name
    let effective_name = table_alias.as_deref().unwrap_or(table_name);

    // Check if this name conflicts with any existing table
    existing_names.iter().any(|n| n.eq_ignore_ascii_case(effective_name) || n.eq_ignore_ascii_case(table_name))
}

/// Get the primary (leftmost) table name from a FROM clause
/// Returns the effective name (alias if present, otherwise table name)
fn get_primary_table_name(from: &FromClause) -> Option<String> {
    match from {
        FromClause::Table { name, alias } => {
            Some(alias.clone().unwrap_or_else(|| name.clone()))
        }
        FromClause::Join { left, .. } => {
            get_primary_table_name(left)
        }
        FromClause::Subquery { alias, .. } => {
            Some(alias.clone())
        }
    }
}

/// Qualify an unqualified column reference with a table name
/// Only rewrites unqualified columns, leaves qualified ones unchanged
fn qualify_outer_column_refs(expr: &Expression, outer_table: &str) -> Expression {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Unqualified column - add outer table qualifier
            Expression::ColumnRef {
                table: Some(outer_table.to_string()),
                column: column.clone(),
            }
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
fn rewrite_column_refs_with_alias(expr: &Expression, old_table: &str, new_alias: &str) -> Expression {
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
                Expression::ColumnRef {
                    table: Some(new_alias.to_string()),
                    column: column.clone(),
                }
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
            values: values.iter().map(|v| rewrite_column_refs_with_alias(v, old_table, new_alias)).collect(),
            negated: *negated,
        },
        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            pattern: Box::new(rewrite_column_refs_with_alias(pattern, old_table, new_alias)),
            negated: *negated,
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| rewrite_column_refs_with_alias(a, old_table, new_alias)).collect(),
            character_unit: character_unit.clone(),
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|o| Box::new(rewrite_column_refs_with_alias(o, old_table, new_alias))),
            when_clauses: when_clauses.iter().map(|case_when| {
                vibesql_ast::CaseWhen {
                    conditions: case_when.conditions.iter()
                        .map(|c| rewrite_column_refs_with_alias(c, old_table, new_alias))
                        .collect(),
                    result: rewrite_column_refs_with_alias(&case_when.result, old_table, new_alias),
                }
            }).collect(),
            else_result: else_result.as_ref().map(|e| Box::new(rewrite_column_refs_with_alias(e, old_table, new_alias))),
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

/// Result of converting an IN subquery to a join
/// Contains the new FROM clause
struct InToJoinResult {
    from: FromClause,
}

/// Try to convert an IN subquery to a SEMI or ANTI join
fn try_convert_in_to_join(
    from: &FromClause,
    expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<InToJoinResult> {
    // Only handle simple subqueries:
    // - Single table in FROM clause
    // - Single column in SELECT list
    // - No GROUP BY, HAVING, LIMIT, etc.

    // Check for simple single-table subquery
    let (table_name, table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause, skip
    };

    // Must have exactly one column in SELECT list
    if subquery.select_list.len() != 1 {
        return None;
    }

    let subquery_column = match &subquery.select_list[0] {
        SelectItem::Expression { expr, .. } => expr.clone(),
        _ => return None,
    };

    // Skip if subquery has complex features
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.set_operation.is_some()
    {
        return None;
    }

    // Detect self-join: check if subquery table name conflicts with outer query tables
    let needs_alias = is_self_join(from, &table_name, &table_alias);

    // Generate a unique alias for self-joins to avoid schema conflicts
    let (effective_alias, outer_expr_qualified, subquery_column_rewritten, subquery_where_rewritten) = if needs_alias {
        // Create a unique alias for the right side of the self-join
        let new_alias = format!("__subquery_{}", table_name);

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] Self-join detected: table={}, new_alias={}", table_name, new_alias);
        }

        // Get the outer table name to qualify outer column references
        let outer_table_name = get_primary_table_name(from)
            .unwrap_or_else(|| table_name.clone()); // Fallback to table_name if not found

        // Qualify outer expression columns with the outer table name
        // This prevents ambiguity when both tables have the same column names
        let qualified_expr = qualify_outer_column_refs(expr, &outer_table_name);

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the subquery column to use the new alias
        let rewritten_col = rewrite_column_refs_with_alias(&subquery_column, old_table_ref, &new_alias);

        // Rewrite column references in the subquery WHERE clause
        let rewritten_where = subquery.where_clause.as_ref().map(|w| {
            rewrite_column_refs_with_alias(w, old_table_ref, &new_alias)
        });

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] outer_table_name={}", outer_table_name);
            eprintln!("[SUBQUERY_TRANSFORM] qualified_expr={:?}", qualified_expr);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_col={:?}", rewritten_col);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_where={:?}", rewritten_where);
        }

        (Some(new_alias), qualified_expr, rewritten_col, rewritten_where)
    } else {
        (table_alias.clone(), expr.clone(), subquery_column.clone(), subquery.where_clause.clone())
    };

    // Create the join condition: expr = subquery_column
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_expr_qualified),
        right: Box::new(subquery_column_rewritten),
    };

    // Combine join condition with subquery's WHERE clause if it exists
    let final_condition = if let Some(subquery_where) = subquery_where_rewritten {
        Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(join_condition),
            right: Box::new(subquery_where),
        })
    } else {
        Some(join_condition)
    };

    // Create the right side of the join
    let right_from = FromClause::Table {
        name: table_name,
        alias: effective_alias,
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: final_condition.clone(),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", final_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}

/// Try to convert an EXISTS subquery to a SEMI or ANTI join
fn try_convert_exists_to_join(
    from: &FromClause,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<(FromClause, Option<Expression>)> {
    // For EXISTS, we need to extract the correlation predicate from the WHERE clause
    // and use it as the join condition

    // Check for simple single-table subquery
    let (table_name, table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause, skip
    };

    // EXISTS subqueries should have a WHERE clause with correlation
    let where_clause = subquery.where_clause.as_ref()?;

    // CRITICAL: Only transform correlated EXISTS subqueries to joins.
    // Uncorrelated EXISTS (e.g., EXISTS (SELECT 1 FROM t WHERE t.col = 5))
    // should NOT be converted to a join because the WHERE clause doesn't
    // correlate with the outer query - it's just a filter on the subquery's table.
    // Converting it would incorrectly use the filter as a join condition.
    if !super::subquery_rewrite::correlation::is_correlated(subquery) {
        return None;
    }

    // Skip if subquery has complex features
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.set_operation.is_some()
    {
        return None;
    }

    // Detect self-join: check if subquery table name conflicts with outer query tables
    let needs_alias = is_self_join(from, &table_name, &table_alias);

    // Handle self-join case: generate unique alias and rewrite column references
    let (effective_alias, rewritten_where) = if needs_alias {
        // Create a unique alias for the right side of the self-join
        let new_alias = format!("__subquery_{}", table_alias.as_ref().unwrap_or(&table_name));

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] EXISTS self-join detected: table={}, alias={:?}, new_alias={}",
                     table_name, table_alias, new_alias);
        }

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the WHERE clause to use the new alias
        let rewritten = rewrite_column_refs_with_alias(where_clause, old_table_ref, &new_alias);

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] EXISTS rewritten_where={:?}", rewritten);
        }

        (Some(new_alias), rewritten)
    } else {
        (table_alias.clone(), where_clause.clone())
    };

    // Create the right side of the join
    let right_from = FromClause::Table {
        name: table_name,
        alias: effective_alias,
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: Some(rewritten_where),
        natural: false,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] EXISTS new_from={:?}", new_from);
    }

    // EXISTS doesn't leave any residual WHERE clause
    Some((new_from, None))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_table_from(name: &str) -> FromClause {
        FromClause::Table {
            name: name.to_string(),
            alias: None,
        }
    }

    fn column_ref(column: &str) -> Expression {
        Expression::ColumnRef {
            table: None,
            column: column.to_string(),
        }
    }

    fn simple_select(table: &str, column: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: column_ref(column),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(simple_table_from(table)),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    #[test]
    fn test_in_subquery_to_semi_join() {
        let mut stmt = simple_select("orders", "o_orderkey");
        let subquery = simple_select("lineitem", "l_orderkey");

        stmt.where_clause = Some(Expression::In {
            expr: Box::new(column_ref("o_orderkey")),
            subquery: Box::new(subquery),
            negated: false,
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have created a SEMI JOIN
        assert!(transformed.where_clause.is_none(), "WHERE clause should be removed");
        match transformed.from {
            Some(FromClause::Join { join_type, .. }) => {
                assert!(matches!(join_type, JoinType::Semi), "Should be SEMI join");
            }
            _ => panic!("Expected JOIN in FROM clause"),
        }
    }

    #[test]
    fn test_not_in_subquery_to_anti_join() {
        let mut stmt = simple_select("orders", "o_orderkey");
        let subquery = simple_select("lineitem", "l_orderkey");

        stmt.where_clause = Some(Expression::In {
            expr: Box::new(column_ref("o_orderkey")),
            subquery: Box::new(subquery),
            negated: true,
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have created an ANTI JOIN
        assert!(transformed.where_clause.is_none(), "WHERE clause should be removed");
        match transformed.from {
            Some(FromClause::Join { join_type, .. }) => {
                assert!(matches!(join_type, JoinType::Anti), "Should be ANTI join");
            }
            _ => panic!("Expected JOIN in FROM clause"),
        }
    }

    #[test]
    fn test_complex_subquery_unchanged() {
        let mut stmt = simple_select("orders", "o_orderkey");
        let mut subquery = simple_select("lineitem", "l_orderkey");
        // Add GROUP BY to make it complex
        subquery.group_by = Some(GroupByClause::Simple(vec![column_ref("l_orderkey")]));

        stmt.where_clause = Some(Expression::In {
            expr: Box::new(column_ref("o_orderkey")),
            subquery: Box::new(subquery),
            negated: false,
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should be unchanged because subquery is complex
        assert!(transformed.where_clause.is_some(), "Complex subquery should remain in WHERE");
        match transformed.from {
            Some(FromClause::Table { .. }) => {} // Good, no join created
            _ => panic!("Complex subquery should not create JOIN"),
        }
    }

    #[test]
    fn test_multiple_subqueries_to_joins() {
        // Test Q21-like pattern with multiple IN subqueries in a deep AND chain
        // WHERE a = b AND c = d AND x IN (...) AND y NOT IN (...)
        let mut stmt = simple_select("orders", "o_orderkey");
        let subquery1 = simple_select("lineitem", "l_orderkey");
        let subquery2 = simple_select("supplier", "s_suppkey");

        // Build WHERE: o_custkey = 1 AND o_orderkey IN (subquery1) AND o_custkey NOT IN (subquery2)
        let predicate1 = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(column_ref("o_custkey")),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        };

        let in_subquery = Expression::In {
            expr: Box::new(column_ref("o_orderkey")),
            subquery: Box::new(subquery1),
            negated: false,
        };

        let not_in_subquery = Expression::In {
            expr: Box::new(column_ref("o_custkey")),
            subquery: Box::new(subquery2),
            negated: true,
        };

        // Build: predicate1 AND in_subquery AND not_in_subquery
        let combined_where = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(predicate1),
                right: Box::new(in_subquery),
            }),
            right: Box::new(not_in_subquery),
        };

        stmt.where_clause = Some(combined_where);

        // Transform should extract BOTH subqueries iteratively
        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have two joins (SEMI and ANTI)
        match transformed.from {
            Some(FromClause::Join {
                left: inner_join_box,
                join_type: outer_join_type,
                ..
            }) => {
                // Outer join should be either SEMI or ANTI
                assert!(
                    matches!(outer_join_type, JoinType::Semi | JoinType::Anti),
                    "Outer join should be SEMI or ANTI, got: {:?}",
                    outer_join_type
                );

                // Inner join should also be a join (not just a table)
                match *inner_join_box {
                    FromClause::Join { join_type: inner_join_type, .. } => {
                        assert!(
                            matches!(inner_join_type, JoinType::Semi | JoinType::Anti),
                            "Inner join should be SEMI or ANTI, got: {:?}",
                            inner_join_type
                        );
                    }
                    _ => panic!("Expected nested JOIN, got table"),
                }
            }
            _ => panic!("Expected JOIN in FROM clause"),
        }

        // WHERE should only have the simple predicate left
        assert!(transformed.where_clause.is_some(), "Simple predicate should remain in WHERE");
    }

    #[test]
    fn test_nested_in_subquery_self_join_column_qualification() {
        // Test that nested IN subqueries in self-joins properly qualify the outer expression
        // This is the bug from issue #2630:
        // SELECT pk FROM tab0 WHERE col3 IN (SELECT col0 FROM tab0 WHERE col0 IN (...) AND col4 >= 7680.91)
        //
        // When the outer IN is transformed to a SEMI JOIN, the nested IN's outer column (col0)
        // should be qualified with the subquery alias (__subquery_TAB0), not left unqualified.

        // Create a nested IN subquery pattern
        let innermost_subquery = simple_select("tab0", "col3"); // SELECT col3 FROM tab0

        // Middle subquery: SELECT col0 FROM tab0 WHERE col0 IN (innermost) AND col4 >= 7680
        let mut middle_subquery = simple_select("tab0", "col0");
        middle_subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::In {
                expr: Box::new(column_ref("col0")), // This should get qualified!
                subquery: Box::new(innermost_subquery),
                negated: false,
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThanOrEqual,
                left: Box::new(column_ref("col4")),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(7680))),
            }),
        });

        // Outer query: SELECT pk FROM tab0 WHERE col3 IN (middle_subquery)
        let mut stmt = simple_select("tab0", "pk");
        stmt.where_clause = Some(Expression::In {
            expr: Box::new(column_ref("col3")),
            subquery: Box::new(middle_subquery),
            negated: false,
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have transformed to a SEMI JOIN
        match &transformed.from {
            Some(FromClause::Join {
                join_type,
                condition,
                right,
                ..
            }) => {
                assert!(matches!(join_type, JoinType::Semi), "Should be SEMI join");

                // Check that the right side has the alias
                match right.as_ref() {
                    FromClause::Table { alias: Some(alias), .. } => {
                        assert!(alias.starts_with("__subquery_"), "Should have subquery alias");
                    }
                    _ => panic!("Expected aliased table on right side"),
                }

                // Check the join condition - nested IN's outer column should be qualified
                if let Some(cond) = condition {
                    // The condition should contain a nested IN expression
                    // with a qualified column reference for col0
                    fn check_nested_in_qualification(expr: &Expression) -> bool {
                        match expr {
                            Expression::In { expr: inner_expr, .. } => {
                                // The outer expression of the nested IN should be qualified
                                match inner_expr.as_ref() {
                                    Expression::ColumnRef { table: Some(t), column: c } => {
                                        t.starts_with("__subquery_") && c.eq_ignore_ascii_case("col0")
                                    }
                                    _ => false,
                                }
                            }
                            Expression::BinaryOp { left, right, .. } => {
                                check_nested_in_qualification(left) || check_nested_in_qualification(right)
                            }
                            _ => false,
                        }
                    }

                    assert!(
                        check_nested_in_qualification(cond),
                        "Nested IN subquery's outer column should be qualified with subquery alias. Condition: {:?}",
                        cond
                    );
                }
            }
            _ => panic!("Expected SEMI JOIN in FROM clause"),
        }
    }

    fn table_from_with_alias(name: &str, alias: &str) -> FromClause {
        FromClause::Table {
            name: name.to_string(),
            alias: Some(alias.to_string()),
        }
    }

    fn qualified_column_ref(table: &str, column: &str) -> Expression {
        Expression::ColumnRef {
            table: Some(table.to_string()),
            column: column.to_string(),
        }
    }

    #[test]
    fn test_exists_self_join_column_qualification() {
        // Test EXISTS with self-join aliasing, similar to TPC-H Q21 pattern:
        // SELECT * FROM lineitem l1 WHERE EXISTS (
        //   SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
        // )
        //
        // The EXISTS subquery references the same table with a different alias.
        // After transformation to a SEMI join, the join condition should properly
        // reference both the outer alias (l1) and the subquery's alias.

        // Create outer query: SELECT * FROM lineitem l1
        let outer_from = table_from_with_alias("lineitem", "l1");
        let mut stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(outer_from.clone()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Create correlated EXISTS subquery:
        // EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        let exists_subquery = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(table_from_with_alias("lineitem", "l2")),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(qualified_column_ref("l2", "l_orderkey")),
                    right: Box::new(qualified_column_ref("l1", "l_orderkey")),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::NotEqual,
                    left: Box::new(qualified_column_ref("l2", "l_suppkey")),
                    right: Box::new(qualified_column_ref("l1", "l_suppkey")),
                }),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        stmt.where_clause = Some(Expression::Exists {
            subquery: Box::new(exists_subquery),
            negated: false,
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have created a SEMI JOIN
        assert!(
            transformed.where_clause.is_none(),
            "EXISTS should be fully transformed, no WHERE clause should remain"
        );

        match &transformed.from {
            Some(FromClause::Join {
                join_type,
                condition,
                right,
                ..
            }) => {
                assert!(
                    matches!(join_type, JoinType::Semi),
                    "EXISTS should transform to SEMI join, got: {:?}",
                    join_type
                );

                // Check that the right side has the unique subquery alias
                // Self-joins use __subquery_<original_alias> to avoid conflicts
                match right.as_ref() {
                    FromClause::Table { name, alias } => {
                        assert_eq!(name, "lineitem", "Table name should be lineitem");
                        assert_eq!(
                            alias.as_deref(),
                            Some("__subquery_l2"),
                            "Alias should be __subquery_l2 for self-join"
                        );
                    }
                    _ => panic!("Expected Table on right side of join"),
                }

                // Verify the join condition includes the correlation predicate
                assert!(condition.is_some(), "Join should have a condition");
            }
            _ => panic!("Expected SEMI JOIN in FROM clause"),
        }
    }

    #[test]
    fn test_not_exists_self_join_column_qualification() {
        // Test NOT EXISTS with self-join aliasing, similar to TPC-H Q21 pattern:
        // SELECT * FROM lineitem l1 WHERE NOT EXISTS (
        //   SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_receiptdate > l3.l_commitdate
        // )
        //
        // NOT EXISTS should transform to an ANTI join with proper alias handling.

        // Create outer query: SELECT * FROM lineitem l1
        let outer_from = table_from_with_alias("lineitem", "l1");
        let mut stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(outer_from.clone()),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        // Create correlated NOT EXISTS subquery:
        // NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_receiptdate > l3.l_commitdate)
        let not_exists_subquery = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(table_from_with_alias("lineitem", "l3")),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(qualified_column_ref("l3", "l_orderkey")),
                    right: Box::new(qualified_column_ref("l1", "l_orderkey")),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(qualified_column_ref("l3", "l_receiptdate")),
                    right: Box::new(qualified_column_ref("l3", "l_commitdate")),
                }),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        stmt.where_clause = Some(Expression::Exists {
            subquery: Box::new(not_exists_subquery),
            negated: true, // NOT EXISTS
        });

        let transformed = transform_subqueries_to_joins(&stmt);

        // Should have created an ANTI JOIN
        assert!(
            transformed.where_clause.is_none(),
            "NOT EXISTS should be fully transformed, no WHERE clause should remain"
        );

        match &transformed.from {
            Some(FromClause::Join {
                join_type,
                condition,
                right,
                ..
            }) => {
                assert!(
                    matches!(join_type, JoinType::Anti),
                    "NOT EXISTS should transform to ANTI join, got: {:?}",
                    join_type
                );

                // Check that the right side has the unique subquery alias
                // Self-joins use __subquery_<original_alias> to avoid conflicts
                match right.as_ref() {
                    FromClause::Table { name, alias } => {
                        assert_eq!(name, "lineitem", "Table name should be lineitem");
                        assert_eq!(
                            alias.as_deref(),
                            Some("__subquery_l3"),
                            "Alias should be __subquery_l3 for self-join"
                        );
                    }
                    _ => panic!("Expected Table on right side of join"),
                }

                // Verify the join condition includes the correlation predicate
                assert!(condition.is_some(), "Join should have a condition");

                // Verify the condition contains both correlation and filter predicates
                // Column references should use the rewritten __subquery_l3 alias
                if let Some(cond) = condition {
                    fn contains_subquery_l3_ref(expr: &Expression) -> bool {
                        match expr {
                            Expression::ColumnRef { table: Some(t), .. } => {
                                t.eq_ignore_ascii_case("__subquery_l3")
                            }
                            Expression::BinaryOp { left, right, .. } => {
                                contains_subquery_l3_ref(left) || contains_subquery_l3_ref(right)
                            }
                            _ => false,
                        }
                    }

                    assert!(
                        contains_subquery_l3_ref(cond),
                        "Join condition should contain references to __subquery_l3 alias. Condition: {:?}",
                        cond
                    );
                }
            }
            _ => panic!("Expected ANTI JOIN in FROM clause"),
        }
    }
}
