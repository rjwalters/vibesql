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

mod exists;
mod helpers;
mod in_clause;
mod scalar_comparison;

use exists::try_convert_exists_to_join;
use in_clause::try_convert_in_to_join;
use scalar_comparison::try_convert_scalar_comparison_to_join;
use vibesql_ast::{BinaryOperator, CommonTableExpr, Expression, FromClause, SelectStmt};
use vibesql_storage::Database;

/// Transform a SELECT statement by converting IN/NOT IN subqueries to semi/anti-joins
///
/// This transformation only applies to simple IN subqueries with single-column SELECT lists
/// and simple table references. Complex subqueries (joins, aggregations, etc.) are left unchanged.
///
/// The transformation is applied iteratively to handle queries with multiple subqueries
/// (e.g., Q21 with both EXISTS and NOT EXISTS clauses).
pub fn transform_subqueries_to_joins(stmt: &SelectStmt, database: &Database) -> SelectStmt {
    let mut result = stmt.clone();

    // First, recursively transform any subqueries in the WHERE clause
    if let Some(where_clause) = &result.where_clause {
        result.where_clause = Some(transform_subqueries_in_expression(where_clause, database));
    }

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
                database,
                stmt.with_clause.as_deref(),
            ) {
                // Debug output for subquery transformation
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!(
                        "[SUBQUERY_TRANSFORM] Iteration {}: Converted subquery to join",
                        iteration + 1
                    );
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

/// Transform scalar comparison subqueries within a SELECT statement (for nested subqueries)
///
/// This is a lighter-weight version of `transform_subqueries_to_joins` that ONLY handles
/// scalar comparison subqueries like `col > (SELECT SUM(...) ...)`. It does NOT convert
/// IN/EXISTS to semi/anti-joins.
///
/// This is used when recursing into IN/EXISTS subqueries to decorrelate scalar comparisons
/// without affecting the structure of nested IN/EXISTS (which would prevent the parent
/// IN/EXISTS from being transformed to a join).
fn transform_scalar_subqueries_in_stmt(stmt: &SelectStmt) -> SelectStmt {
    let mut result = stmt.clone();

    // Only transform if we have a FROM clause and a WHERE clause
    if result.from.is_none() || result.where_clause.is_none() {
        return result;
    }

    // Apply scalar comparison transformation iteratively
    let max_iterations = 5;
    for _iteration in 0..max_iterations {
        let mut made_progress = false;

        if let Some(where_clause) = &result.where_clause {
            if let Some((new_from, new_where)) =
                try_extract_scalar_comparisons_only(result.from.as_ref().unwrap(), where_clause)
            {
                result.from = Some(new_from);
                result.where_clause = new_where;
                made_progress = true;
            }
        }

        if !made_progress {
            break;
        }
    }

    result
}

/// Try to extract scalar comparison subqueries from WHERE clause and convert to LEFT JOINs
/// This does NOT handle IN/EXISTS - only scalar comparisons like `col > (SELECT ...)`
fn try_extract_scalar_comparisons_only(
    from: &FromClause,
    where_clause: &Expression,
) -> Option<(FromClause, Option<Expression>)> {
    match where_clause {
        // Scalar comparison with subquery at top level
        Expression::BinaryOp { op, left, right }
            if matches!(right.as_ref(), Expression::ScalarSubquery(_)) =>
        {
            if let Some(result) = try_convert_scalar_comparison_to_join(from, op, left, right) {
                return Some((result.from, Some(result.replacement_expr)));
            }
            None
        }

        // AND with potential scalar comparison
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left side
            if let Expression::BinaryOp { op, left: inner_left, right: inner_right } = left.as_ref()
            {
                if matches!(inner_right.as_ref(), Expression::ScalarSubquery(_)) {
                    if let Some(result) =
                        try_convert_scalar_comparison_to_join(from, op, inner_left, inner_right)
                    {
                        let new_where = Expression::BinaryOp {
                            op: BinaryOperator::And,
                            left: Box::new(result.replacement_expr),
                            right: right.clone(),
                        };
                        return Some((result.from, Some(new_where)));
                    }
                }
            }

            // Try right side
            if let Expression::BinaryOp { op, left: inner_left, right: inner_right } =
                right.as_ref()
            {
                if matches!(inner_right.as_ref(), Expression::ScalarSubquery(_)) {
                    if let Some(result) =
                        try_convert_scalar_comparison_to_join(from, op, inner_left, inner_right)
                    {
                        let new_where = Expression::BinaryOp {
                            op: BinaryOperator::And,
                            left: left.clone(),
                            right: Box::new(result.replacement_expr),
                        };
                        return Some((result.from, Some(new_where)));
                    }
                }
            }

            None
        }

        // Conjunction (flattened AND chain)
        Expression::Conjunction(children) => {
            for (i, child) in children.iter().enumerate() {
                if let Expression::BinaryOp { op, left, right } = child {
                    if matches!(right.as_ref(), Expression::ScalarSubquery(_)) {
                        if let Some(result) =
                            try_convert_scalar_comparison_to_join(from, op, left, right)
                        {
                            let mut remaining: Vec<_> = children
                                .iter()
                                .enumerate()
                                .filter(|(j, _)| *j != i)
                                .map(|(_, c)| c.clone())
                                .collect();
                            remaining.push(result.replacement_expr);
                            let new_where = match remaining.len() {
                                0 => None,
                                1 => Some(remaining.into_iter().next().unwrap()),
                                _ => Some(Expression::Conjunction(remaining)),
                            };
                            return Some((result.from, new_where));
                        }
                    }
                }
            }
            None
        }

        _ => None,
    }
}

/// Recursively transform subqueries within an expression
///
/// This applies scalar comparison decorrelation to SELECT statements nested within
/// IN and EXISTS subquery expressions (like Q20's pattern: `ps_availqty > (SELECT SUM(...) ...)`).
///
/// IMPORTANT: We only decorrelate scalar comparisons, NOT IN/EXISTS to semi/anti-joins.
/// Converting nested IN/EXISTS would change their FROM clause structure, which would then
/// prevent the PARENT IN/EXISTS from being converted to a join (because the parent
/// transformation expects a simple table in the subquery's FROM clause).
fn transform_subqueries_in_expression(expr: &Expression, database: &Database) -> Expression {
    match expr {
        Expression::In { expr: inner_expr, subquery, negated } => {
            // Transform the inner expression
            let transformed_inner = transform_subqueries_in_expression(inner_expr, database);
            // Only apply scalar comparison decorrelation, NOT IN/EXISTS transformation
            // This preserves the subquery's FROM clause structure for parent transformation
            let transformed_subquery = transform_scalar_subqueries_in_stmt(subquery);
            Expression::In {
                expr: Box::new(transformed_inner),
                subquery: Box::new(transformed_subquery),
                negated: *negated,
            }
        }
        Expression::Exists { subquery, negated } => {
            // Only apply scalar comparison decorrelation, NOT IN/EXISTS transformation
            let transformed_subquery = transform_scalar_subqueries_in_stmt(subquery);
            Expression::Exists { subquery: Box::new(transformed_subquery), negated: *negated }
        }
        Expression::ScalarSubquery(subquery) => {
            let transformed_subquery = transform_subqueries_to_joins(subquery, database);
            Expression::ScalarSubquery(Box::new(transformed_subquery))
        }
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: op.clone(),
            left: Box::new(transform_subqueries_in_expression(left, database)),
            right: Box::new(transform_subqueries_in_expression(right, database)),
        },
        Expression::Conjunction(children) => Expression::Conjunction(
            children.iter().map(|c| transform_subqueries_in_expression(c, database)).collect(),
        ),
        Expression::Disjunction(children) => Expression::Disjunction(
            children.iter().map(|c| transform_subqueries_in_expression(c, database)).collect(),
        ),
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(transform_subqueries_in_expression(inner, database)),
        },
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(transform_subqueries_in_expression(inner, database)),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(transform_subqueries_in_expression(inner, database)),
            low: Box::new(transform_subqueries_in_expression(low, database)),
            high: Box::new(transform_subqueries_in_expression(high, database)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(transform_subqueries_in_expression(o, database))),
            when_clauses: when_clauses
                .iter()
                .map(|w| vibesql_ast::CaseWhen {
                    conditions: w
                        .conditions
                        .iter()
                        .map(|c| transform_subqueries_in_expression(c, database))
                        .collect(),
                    result: transform_subqueries_in_expression(&w.result, database),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(transform_subqueries_in_expression(e, database))),
        },
        // For other expression types, just return as-is
        _ => expr.clone(),
    }
}

/// Try to extract IN/NOT IN subqueries from WHERE clause and convert to semi/anti-joins
fn try_extract_subqueries_to_joins(
    from: &FromClause,
    where_clause: &Expression,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
) -> Option<(FromClause, Option<Expression>)> {
    // Look for IN subquery at the top level or in AND branches
    match where_clause {
        // Single IN subquery
        Expression::In { expr, subquery, negated } => {
            if let Some(result) =
                try_convert_in_to_join(from, expr, subquery, *negated, database, with_clause)
            {
                return Some((result.from, None)); // Removed WHERE clause entirely
            }
            None
        }

        // AND with potential IN/EXISTS/scalar comparison subqueries
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left side first - check IN, EXISTS, and scalar comparison
            match left.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(
                        from,
                        expr,
                        subquery,
                        *negated,
                        database,
                        with_clause,
                    ) {
                        // Successfully converted left IN side, keep right side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join
                        // table). The subquery's columns have already been
                        // rewritten to use the new alias,
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
                Expression::BinaryOp { op, left: inner_left, right: inner_right }
                    if matches!(inner_right.as_ref(), Expression::ScalarSubquery(_)) =>
                {
                    if let Some(result) =
                        try_convert_scalar_comparison_to_join(from, op, inner_left, inner_right)
                    {
                        // Replace the scalar comparison with the new expression
                        let new_where = Expression::BinaryOp {
                            op: BinaryOperator::And,
                            left: Box::new(result.replacement_expr),
                            right: right.clone(),
                        };
                        return Some((result.from, Some(new_where)));
                    }
                }
                _ => {}
            }

            // Try right side - check IN, EXISTS, and scalar comparison
            match right.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(
                        from,
                        expr,
                        subquery,
                        *negated,
                        database,
                        with_clause,
                    ) {
                        // Successfully converted right IN side, keep left side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join
                        // table).
                        return Some((result.from, Some((**left).clone())));
                    }
                }
                Expression::Exists { subquery, negated } => {
                    if let Some((join, _)) = try_convert_exists_to_join(from, subquery, *negated) {
                        // Successfully converted right EXISTS side, keep left side as WHERE clause
                        return Some((join, Some((**left).clone())));
                    }
                }
                Expression::BinaryOp { op, left: inner_left, right: inner_right }
                    if matches!(inner_right.as_ref(), Expression::ScalarSubquery(_)) =>
                {
                    if let Some(result) =
                        try_convert_scalar_comparison_to_join(from, op, inner_left, inner_right)
                    {
                        // Replace the scalar comparison with the new expression
                        let new_where = Expression::BinaryOp {
                            op: BinaryOperator::And,
                            left: left.clone(),
                            right: Box::new(result.replacement_expr),
                        };
                        return Some((result.from, Some(new_where)));
                    }
                }
                _ => {}
            }

            // Try recursively on left side
            if let Some((new_left_from, new_left_where)) =
                try_extract_subqueries_to_joins(from, left, database, with_clause)
            {
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
            if let Some((new_right_from, new_right_where)) =
                try_extract_subqueries_to_joins(from, right, database, with_clause)
            {
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

        // EXISTS can also be converted (after decorrelation it becomes IN, but handle it directly
        // too)
        Expression::Exists { subquery, negated } => {
            // Try to convert EXISTS to a semi-join by extracting correlation
            try_convert_exists_to_join(from, subquery, *negated)
        }

        // Scalar comparison with subquery (e.g., ps_availqty > (SELECT SUM(...) ...))
        Expression::BinaryOp { op, left, right }
            if matches!(right.as_ref(), Expression::ScalarSubquery(_)) =>
        {
            if let Some(result) = try_convert_scalar_comparison_to_join(from, op, left, right) {
                return Some((result.from, Some(result.replacement_expr)));
            }
            None
        }

        // Conjunction (flattened AND chain) - produced by arena parser
        // Handle IN/EXISTS/scalar comparison subqueries within the conjunction children
        Expression::Conjunction(children) => {
            // Look for IN, EXISTS, or scalar comparison subqueries among children
            for (i, child) in children.iter().enumerate() {
                match child {
                    Expression::In { expr, subquery, negated } => {
                        if let Some(result) = try_convert_in_to_join(
                            from,
                            expr,
                            subquery,
                            *negated,
                            database,
                            with_clause,
                        ) {
                            // Build remaining WHERE from other children
                            let remaining: Vec<_> = children
                                .iter()
                                .enumerate()
                                .filter(|(j, _)| *j != i)
                                .map(|(_, c)| c.clone())
                                .collect();
                            let new_where = match remaining.len() {
                                0 => None,
                                1 => Some(remaining.into_iter().next().unwrap()),
                                _ => Some(Expression::Conjunction(remaining)),
                            };
                            return Some((result.from, new_where));
                        }
                    }
                    Expression::Exists { subquery, negated } => {
                        if let Some((join, _)) =
                            try_convert_exists_to_join(from, subquery, *negated)
                        {
                            // Build remaining WHERE from other children
                            let remaining: Vec<_> = children
                                .iter()
                                .enumerate()
                                .filter(|(j, _)| *j != i)
                                .map(|(_, c)| c.clone())
                                .collect();
                            let new_where = match remaining.len() {
                                0 => None,
                                1 => Some(remaining.into_iter().next().unwrap()),
                                _ => Some(Expression::Conjunction(remaining)),
                            };
                            return Some((join, new_where));
                        }
                    }
                    Expression::BinaryOp { op, left, right }
                        if matches!(right.as_ref(), Expression::ScalarSubquery(_)) =>
                    {
                        if let Some(result) =
                            try_convert_scalar_comparison_to_join(from, op, left, right)
                        {
                            // Build remaining WHERE from other children, replacing this child
                            // with the replacement expression
                            let mut remaining: Vec<_> = children
                                .iter()
                                .enumerate()
                                .filter(|(j, _)| *j != i)
                                .map(|(_, c)| c.clone())
                                .collect();
                            remaining.push(result.replacement_expr);
                            let new_where = match remaining.len() {
                                0 => None,
                                1 => Some(remaining.into_iter().next().unwrap()),
                                _ => Some(Expression::Conjunction(remaining)),
                            };
                            return Some((result.from, new_where));
                        }
                    }
                    _ => {}
                }
            }

            // Try recursively on each child (for nested expressions)
            for (i, child) in children.iter().enumerate() {
                if let Some((new_from, new_child_where)) =
                    try_extract_subqueries_to_joins(from, child, database, with_clause)
                {
                    let mut new_children: Vec<_> = children
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .map(|(_, c)| c.clone())
                        .collect();
                    if let Some(residual) = new_child_where {
                        new_children.push(residual);
                    }
                    let combined_where = match new_children.len() {
                        0 => None,
                        1 => Some(new_children.into_iter().next().unwrap()),
                        _ => Some(Expression::Conjunction(new_children)),
                    };
                    return Some((new_from, combined_where));
                }
            }

            None
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests;
