//! Transform IN/EXISTS subqueries to semi/anti-joins
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

/// Transform a SELECT statement by converting IN/NOT IN subqueries to semi/anti-joins
///
/// This transformation only applies to simple IN subqueries with single-column SELECT lists
/// and simple table references. Complex subqueries (joins, aggregations, etc.) are left unchanged.
pub fn transform_subqueries_to_joins(stmt: &SelectStmt) -> SelectStmt {
    let mut result = stmt.clone();

    // Only transform if we have a FROM clause and a WHERE clause
    if result.from.is_none() || result.where_clause.is_none() {
        return result;
    }

    // Try to extract IN/NOT IN subqueries from WHERE clause and convert to joins
    if let Some(where_clause) = &result.where_clause {
        if let Some((new_from, new_where)) = try_extract_subqueries_to_joins(
            result.from.as_ref().unwrap(),
            where_clause,
        ) {
            result.from = Some(new_from);
            result.where_clause = new_where;
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
            if let Some(join) = try_convert_in_to_join(from, expr, subquery, *negated) {
                return Some((join, None)); // Removed WHERE clause entirely
            }
            None
        }

        // AND with potential IN subqueries
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left side first
            if let Expression::In { expr, subquery, negated } = left.as_ref() {
                if let Some(join) = try_convert_in_to_join(from, expr, subquery, *negated) {
                    // Successfully converted left side, keep right side as WHERE clause
                    return Some((join, Some((**right).clone())));
                }
            }

            // Try right side
            if let Expression::In { expr, subquery, negated } = right.as_ref() {
                if let Some(join) = try_convert_in_to_join(from, expr, subquery, *negated) {
                    // Successfully converted right side, keep left side as WHERE clause
                    return Some((join, Some((**left).clone())));
                }
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

/// Try to convert an IN subquery to a SEMI or ANTI join
fn try_convert_in_to_join(
    from: &FromClause,
    expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
) -> Option<FromClause> {
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

    // Create the join condition: expr = subquery_column
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(expr.clone()),
        right: Box::new(subquery_column),
    };

    // Combine join condition with subquery's WHERE clause if it exists
    let final_condition = if let Some(subquery_where) = &subquery.where_clause {
        Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(join_condition),
            right: Box::new(subquery_where.clone()),
        })
    } else {
        Some(join_condition)
    };

    // Create the right side of the join
    let right_from = FromClause::Table {
        name: table_name,
        alias: table_alias,
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
        condition: final_condition,
        natural: false,
    };

    Some(new_from)
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

    // Try to extract a simple equi-join predicate from WHERE
    // For now, we'll just use the entire WHERE clause as the join condition
    // A more sophisticated implementation would separate correlation from filters

    // Skip if subquery has complex features
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.set_operation.is_some()
    {
        return None;
    }

    // Create the right side of the join
    let right_from = FromClause::Table {
        name: table_name,
        alias: table_alias,
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
        condition: Some(where_clause.clone()),
        natural: false,
    };

    // EXISTS doesn't leave any residual WHERE clause
    Some((new_from, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::DataType;

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
        subquery.group_by = Some(vec![column_ref("l_orderkey")]);

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
}
