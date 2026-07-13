//! Static validation of row-value (tuple) usage.
//!
//! SQLite rejects a row value used in a scalar context at prepare time with
//! "row value misused" — even when the table is empty and no row would ever be
//! evaluated (e.g. `SELECT * FROM empty WHERE (a,a) <= 1`). This walker mirrors
//! that behavior: it flags row values that appear anywhere other than the
//! positions where SQLite accepts them:
//!
//! - both sides of a comparison / `IS` (`(a,b) = (c,d)`, arity must match;
//!   nested row values pair up recursively);
//! - one side of a comparison / `IS` when the other side is a scalar subquery
//!   (`(a,b) = (SELECT x,y)`);
//! - all three operands of BETWEEN (row values or scalar subqueries);
//! - the left-hand side of `IN (SELECT ...)` and `IN (list)` (where every list
//!   element must be a row value of the same arity);
//! - the operand / WHEN values of a simple CASE.
//!
//! Runtime checks in the evaluators cover the same shapes for non-SELECT
//! statements; this static pass exists so empty-table queries still error.

use vibesql_ast::{BinaryOperator, Expression};

use crate::errors::ExecutorError;

/// True for a row value constructor with more than one element (a
/// single-element parenthesized expression is just a scalar).
fn is_multi_row_value(expr: &Expression) -> bool {
    matches!(expr, Expression::RowValueConstructor(elems) if elems.len() > 1)
}

/// Acceptable opposite operand for a row value in a comparison context.
fn is_row_value_or_subquery(expr: &Expression) -> bool {
    is_multi_row_value(expr) || matches!(expr, Expression::ScalarSubquery(_))
}

fn is_comparison_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
    )
}

/// Validate row-value usage in an expression appearing in a scalar-producing
/// context (SELECT list item, WHERE clause, ORDER BY / GROUP BY / HAVING
/// expression).
pub fn validate_row_value_usage(expr: &Expression) -> Result<(), ExecutorError> {
    walk(expr)
}

/// Validate a row-value vs row-value comparison pair: arities must match and
/// nested row values must pair up structurally.
fn validate_pair(left: &[Expression], right: &[Expression]) -> Result<(), ExecutorError> {
    if left.len() != right.len() {
        return Err(ExecutorError::RowValueMisused);
    }
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Expression::RowValueConstructor(le), Expression::RowValueConstructor(re)) => {
                validate_pair(le, re)?;
            }
            (Expression::RowValueConstructor(_), _) | (_, Expression::RowValueConstructor(_)) => {
                return Err(ExecutorError::RowValueMisused);
            }
            _ => {
                walk(l)?;
                walk(r)?;
            }
        }
    }
    Ok(())
}

/// Validate the elements of a row value whose opposite operand is a scalar
/// subquery (each element must itself be scalar).
fn validate_elements_scalar(elems: &[Expression]) -> Result<(), ExecutorError> {
    for e in elems {
        walk(e)?;
    }
    Ok(())
}

fn walk(expr: &Expression) -> Result<(), ExecutorError> {
    match expr {
        // A row value reached outside one of the accepted positions below is a
        // misuse (bare in a SELECT list, function argument, arithmetic
        // operand, ORDER BY / GROUP BY expression, ...).
        Expression::RowValueConstructor(elems) => {
            if elems.len() > 1 {
                return Err(ExecutorError::RowValueMisused);
            }
            validate_elements_scalar(elems)
        }

        Expression::BinaryOp { left, op, right } if is_comparison_op(op) => {
            match (left.as_ref(), right.as_ref()) {
                (Expression::RowValueConstructor(le), Expression::RowValueConstructor(re))
                    if le.len() > 1 || re.len() > 1 =>
                {
                    validate_pair(le, re)
                }
                (Expression::RowValueConstructor(le), other) if le.len() > 1 => {
                    if matches!(other, Expression::ScalarSubquery(_)) {
                        validate_elements_scalar(le)
                    } else {
                        Err(ExecutorError::RowValueMisused)
                    }
                }
                (other, Expression::RowValueConstructor(re)) if re.len() > 1 => {
                    if matches!(other, Expression::ScalarSubquery(_)) {
                        validate_elements_scalar(re)
                    } else {
                        Err(ExecutorError::RowValueMisused)
                    }
                }
                _ => {
                    walk(left)?;
                    walk(right)
                }
            }
        }

        Expression::BinaryOp { left, right, .. } => {
            walk(left)?;
            walk(right)
        }

        Expression::IsDistinctFrom { left, right, .. } => match (left.as_ref(), right.as_ref()) {
            (Expression::RowValueConstructor(le), Expression::RowValueConstructor(re))
                if le.len() > 1 || re.len() > 1 =>
            {
                validate_pair(le, re)
            }
            (Expression::RowValueConstructor(le), other) if le.len() > 1 => {
                if matches!(other, Expression::ScalarSubquery(_)) {
                    validate_elements_scalar(le)
                } else {
                    Err(ExecutorError::RowValueMisused)
                }
            }
            (other, Expression::RowValueConstructor(re)) if re.len() > 1 => {
                if matches!(other, Expression::ScalarSubquery(_)) {
                    validate_elements_scalar(re)
                } else {
                    Err(ExecutorError::RowValueMisused)
                }
            }
            _ => {
                walk(left)?;
                walk(right)
            }
        },

        Expression::Between { expr, low, high, .. } => {
            let operands = [expr.as_ref(), low.as_ref(), high.as_ref()];
            if operands.iter().any(|e| is_multi_row_value(e)) {
                // All three operands must be row values or scalar subqueries,
                // and the row-value operands must agree on arity.
                if !operands.iter().all(|e| is_row_value_or_subquery(e)) {
                    return Err(ExecutorError::RowValueMisused);
                }
                let mut arity: Option<usize> = None;
                for operand in operands {
                    if let Expression::RowValueConstructor(elems) = operand {
                        if let Some(a) = arity {
                            if a != elems.len() {
                                return Err(ExecutorError::RowValueMisused);
                            }
                        } else {
                            arity = Some(elems.len());
                        }
                        validate_elements_scalar(elems)?;
                    }
                }
                Ok(())
            } else {
                walk(expr)?;
                walk(low)?;
                walk(high)
            }
        }

        Expression::InList { expr, values, .. } => {
            if let Expression::RowValueConstructor(elems) = expr.as_ref() {
                if elems.len() > 1 {
                    validate_elements_scalar(elems)?;
                    for value in values {
                        // A candidate row value must have the same arity as the
                        // LHS tuple; a mismatch (including a bare scalar, which
                        // has arity 1) is SQLite's
                        // "IN(...) element has N term(s) - expected M".
                        let cand_arity = match value {
                            Expression::RowValueConstructor(cand) => {
                                validate_elements_scalar(cand)?;
                                cand.len()
                            }
                            _ => 1,
                        };
                        if cand_arity != elems.len() {
                            return Err(ExecutorError::InElementArity {
                                expected: elems.len(),
                                actual: cand_arity,
                            });
                        }
                    }
                    return Ok(());
                }
            }
            walk(expr)?;
            for value in values {
                walk(value)?;
            }
            Ok(())
        }

        // Row value on the left of `IN (SELECT ...)` is legal; arity is
        // validated at execution time against the subquery's column count.
        Expression::In { expr, .. } => {
            if let Expression::RowValueConstructor(elems) = expr.as_ref() {
                validate_elements_scalar(elems)
            } else {
                walk(expr)
            }
        }

        // Simple CASE with a row-value operand or row-value WHEN values is
        // legal (compared with row-value equality). Only the results and ELSE
        // are scalar contexts.
        Expression::Case { operand, when_clauses, else_result } => {
            let row_value_case = operand.as_deref().is_some_and(is_row_value_or_subquery)
                || when_clauses.iter().any(|wc| wc.conditions.iter().any(is_multi_row_value));
            if !row_value_case {
                if let Some(op_expr) = operand {
                    walk(op_expr)?;
                }
                for wc in when_clauses {
                    for cond in &wc.conditions {
                        walk(cond)?;
                    }
                }
            }
            for wc in when_clauses {
                walk(&wc.result)?;
            }
            if let Some(else_expr) = else_result {
                walk(else_expr)?;
            }
            Ok(())
        }

        // Subqueries are validated independently when they execute.
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => Ok(()),

        Expression::QuantifiedComparison { expr, .. } => walk(expr),

        Expression::UnaryOp { expr, .. } => walk(expr),
        Expression::Collate { expr, .. } => walk(expr),
        Expression::Cast { expr, .. } => walk(expr),
        Expression::IsNull { expr, .. } => walk(expr),
        Expression::IsTruthValue { expr, .. } => walk(expr),
        Expression::Like { expr, pattern, escape, .. } => {
            walk(expr)?;
            walk(pattern)?;
            if let Some(esc) = escape {
                walk(esc)?;
            }
            Ok(())
        }
        Expression::Glob { expr, pattern, .. } => {
            walk(expr)?;
            walk(pattern)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                walk(arg)?;
            }
            Ok(())
        }
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                walk(arg)?;
            }
            Ok(())
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                walk(child)?;
            }
            Ok(())
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                walk(rc)?;
            }
            walk(string)
        }
        Expression::Position { substring, string, .. } => {
            walk(substring)?;
            walk(string)
        }
        Expression::Extract { expr, .. } => walk(expr),

        // Everything else (literals, column refs, placeholders, ...) contains
        // no nested expressions we need to inspect.
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_parser::Parser;

    use super::*;

    fn where_expr_of(sql: &str) -> Expression {
        match Parser::parse_sql(sql) {
            Ok(vibesql_ast::Statement::Select(select)) => {
                select.where_clause.expect("query must have a WHERE clause")
            }
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    fn select_item_of(sql: &str) -> Expression {
        match Parser::parse_sql(sql) {
            Ok(vibesql_ast::Statement::Select(select)) => match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => expr.clone(),
                other => panic!("expected expression item, got {:?}", other),
            },
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    #[test]
    fn row_value_vs_scalar_comparison_is_misused() {
        for op in ["=", "==", "!=", "<>", "<", "<=", ">", ">="] {
            let expr = where_expr_of(&format!("SELECT * FROM t WHERE (a,a) {} 1", op));
            assert!(
                matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)),
                "expected misuse for operator {}",
                op
            );
        }
    }

    #[test]
    fn row_value_is_scalar_is_misused() {
        let expr = where_expr_of("SELECT * FROM t WHERE (a,a) IS 1");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
        let expr = where_expr_of("SELECT * FROM t WHERE (a,a) IS NOT 1");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
    }

    #[test]
    fn row_value_vs_row_value_is_legal() {
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) = (1,2)");
        assert!(validate_row_value_usage(&expr).is_ok());
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) < (c,d)");
        assert!(validate_row_value_usage(&expr).is_ok());
    }

    #[test]
    fn row_value_vs_subquery_is_legal() {
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) = (SELECT x, y FROM u)");
        assert!(validate_row_value_usage(&expr).is_ok());
    }

    #[test]
    fn row_value_arity_mismatch_is_misused() {
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) = (1,2,3)");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
    }

    #[test]
    fn nested_row_values_pair_up() {
        let expr = select_item_of("SELECT (2,(2,0)) IS (2,(2,0))");
        assert!(validate_row_value_usage(&expr).is_ok());
        let expr = select_item_of("SELECT (2,(2,0)) IS (2,(2,0,1))");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
    }

    #[test]
    fn bare_row_value_in_select_list_is_misused() {
        let expr = select_item_of("SELECT (1,2)");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
    }

    #[test]
    fn between_mixed_scalar_is_misused() {
        let expr = select_item_of("SELECT (1,2) BETWEEN 1 AND 2");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
        let expr = select_item_of("SELECT 1 BETWEEN (1,2) AND 2");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
        let expr = select_item_of("SELECT 2 BETWEEN 1 AND (1,2)");
        assert!(matches!(validate_row_value_usage(&expr), Err(ExecutorError::RowValueMisused)));
    }

    #[test]
    fn between_all_row_values_is_legal() {
        let expr = select_item_of("SELECT (2,2) BETWEEN (1,1) AND (3,3)");
        assert!(validate_row_value_usage(&expr).is_ok());
        let expr = where_expr_of("SELECT 1 WHERE (SELECT 2,2) BETWEEN (1,1) AND (3,3)");
        assert!(validate_row_value_usage(&expr).is_ok());
    }

    #[test]
    fn row_value_in_list_shapes() {
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) IN ((1,2),(3,4))");
        assert!(validate_row_value_usage(&expr).is_ok());
        // Mismatched candidate arity: SQLite reports the element-arity error
        // ("IN(...) element has N term(s) - expected M"), not a generic misuse.
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) IN ((1,2,3))");
        assert!(matches!(
            validate_row_value_usage(&expr),
            Err(ExecutorError::InElementArity { expected: 2, actual: 3 })
        ));
        // A bare-scalar candidate has arity 1.
        let expr = where_expr_of("SELECT * FROM t WHERE (a,b) IN ((1,2),4)");
        assert!(matches!(
            validate_row_value_usage(&expr),
            Err(ExecutorError::InElementArity { expected: 2, actual: 1 })
        ));
    }

    #[test]
    fn case_with_row_values_is_legal() {
        let expr = select_item_of("SELECT CASE (2,2) WHEN (1,1) THEN 2 ELSE 1 END");
        assert!(validate_row_value_usage(&expr).is_ok());
    }
}
