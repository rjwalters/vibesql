//! Shared row-value (tuple) comparison logic
//!
//! Row values are tuples used in comparisons and IN, e.g. `(a, b) = (1, 2)`,
//! `(a, b) < (c, d)`, `(a, b, c) > (SELECT a, b, c FROM t WHERE ...)`.
//!
//! The functions here operate on already-evaluated value slices so they can be
//! shared by both the single-table `ExpressionEvaluator` and the multi-table
//! `CombinedExpressionEvaluator`, and reused whether the right-hand side is a
//! literal tuple or a row produced by a scalar subquery. Each evaluator supplies
//! its own scalar comparison via the `eval_op` closure (one uses an instance
//! method, the other a static method that also threads `SqlMode`).
//!
//! ## SQL semantics (SQL:1999 §7.1, matching SQLite)
//!
//! Comparison is lexicographic with three-valued NULL logic:
//! - `=`  : TRUE iff every element compares equal; FALSE as soon as one element
//!   is definitively unequal; otherwise (some element comparison UNKNOWN and no
//!   definitive inequality) UNKNOWN (NULL).
//! - `<>` : the negation of `=` (TRUE as soon as one element is definitively
//!   unequal; FALSE if all equal; otherwise UNKNOWN).
//! - `<`, `<=`, `>`, `>=` : compare element by element; a NULL at the first
//!   differing position makes the ordering UNKNOWN (NULL) immediately, while a
//!   definite strict inequality settles the result even if later elements are
//!   NULL.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Apply a collation transformation to both sides of a single element
/// comparison inside a row-value, mirroring the scalar comparison path:
/// NOCASE uppercases string values, RTRIM trims trailing whitespace, any other
/// collation (BINARY, ...) leaves values unchanged.
pub(crate) fn apply_collation_to_pair(
    left: SqlValue,
    right: SqlValue,
    collation: Option<&str>,
) -> (SqlValue, SqlValue) {
    let Some(collation_name) = collation else {
        return (left, right);
    };

    fn transform(val: SqlValue, collation_name: &str) -> SqlValue {
        if collation_name.eq_ignore_ascii_case("nocase") {
            match val {
                SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase())),
                SqlValue::Character(s) => {
                    SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase()))
                }
                other => other,
            }
        } else if collation_name.eq_ignore_ascii_case("rtrim") {
            match val {
                SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end())),
                SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.trim_end())),
                other => other,
            }
        } else {
            val
        }
    }

    (transform(left, collation_name), transform(right, collation_name))
}

/// Compare two already-evaluated row values with a comparison operator.
///
/// `eval_op` performs the scalar comparison for a single element pair and must
/// return `Boolean(_)` or `Null`. The operator must be one of the six SQL
/// comparison operators; anything else yields an `UnsupportedExpression` error.
pub(crate) fn compare_row_values<F>(
    left_values: &[SqlValue],
    op: &vibesql_ast::BinaryOperator,
    right_values: &[SqlValue],
    eval_op: F,
) -> Result<SqlValue, ExecutorError>
where
    F: Fn(&SqlValue, &vibesql_ast::BinaryOperator, &SqlValue) -> Result<SqlValue, ExecutorError>,
{
    use vibesql_ast::BinaryOperator;

    match op {
        BinaryOperator::Equal => {
            // (a, b) = (c, d) → a = c AND b = d
            let mut has_null = false;
            for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
                match eval_op(left_val, op, right_val)? {
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                    SqlValue::Null => has_null = true,
                    SqlValue::Boolean(true) => {}
                    other => {
                        return Err(ExecutorError::TypeError(format!(
                            "Comparison returned non-boolean: {:?}",
                            other
                        )))
                    }
                }
            }
            Ok(if has_null { SqlValue::Null } else { SqlValue::Boolean(true) })
        }

        BinaryOperator::NotEqual => {
            // (a, b) <> (c, d) → a <> c OR b <> d
            let mut has_null = false;
            for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
                match eval_op(left_val, &BinaryOperator::Equal, right_val)? {
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(true)),
                    SqlValue::Null => has_null = true,
                    SqlValue::Boolean(true) => {}
                    other => {
                        return Err(ExecutorError::TypeError(format!(
                            "Comparison returned non-boolean: {:?}",
                            other
                        )))
                    }
                }
            }
            Ok(if has_null { SqlValue::Null } else { SqlValue::Boolean(false) })
        }

        BinaryOperator::LessThan
        | BinaryOperator::LessThanOrEqual
        | BinaryOperator::GreaterThan
        | BinaryOperator::GreaterThanOrEqual => {
            compare_row_values_ordering(left_values, op, right_values, eval_op)
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unsupported operator for row value comparison: {:?}",
            op
        ))),
    }
}

/// Lexicographic ordering comparison (`<`, `<=`, `>`, `>=`) on evaluated values.
fn compare_row_values_ordering<F>(
    left_values: &[SqlValue],
    op: &vibesql_ast::BinaryOperator,
    right_values: &[SqlValue],
    eval_op: F,
) -> Result<SqlValue, ExecutorError>
where
    F: Fn(&SqlValue, &vibesql_ast::BinaryOperator, &SqlValue) -> Result<SqlValue, ExecutorError>,
{
    use vibesql_ast::BinaryOperator;

    let is_less = matches!(op, BinaryOperator::LessThan | BinaryOperator::LessThanOrEqual);
    let is_or_equal =
        matches!(op, BinaryOperator::LessThanOrEqual | BinaryOperator::GreaterThanOrEqual);

    // The strict comparison operator (without the equality part).
    let strict_op = if is_less { BinaryOperator::LessThan } else { BinaryOperator::GreaterThan };
    let eq_op = BinaryOperator::Equal;

    for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
        // We only reach this position because every prior element compared
        // definitively equal, so this element decides the lexicographic order:
        //  - a definite strict inequality settles the result (TRUE),
        //  - a definite equality moves on to the next element,
        //  - a definite non-equality (with strict FALSE) settles it (FALSE),
        //  - a NULL on either side makes this first differing position
        //    undetermined, so the whole comparison is UNKNOWN. It must return
        //    NULL *immediately* — a later element must not be allowed to force a
        //    definite result once the ordering is already undetermined here.
        match eval_op(left_val, &strict_op, right_val)? {
            SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Boolean(false) => {
                // Not strictly less/greater — only continue if elements are equal.
                match eval_op(left_val, &eq_op, right_val)? {
                    SqlValue::Boolean(true) => {} // equal here, look at next element
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                    SqlValue::Null => return Ok(SqlValue::Null),
                    other => {
                        return Err(ExecutorError::TypeError(format!(
                            "Comparison returned non-boolean: {:?}",
                            other
                        )))
                    }
                }
            }
            other => {
                return Err(ExecutorError::TypeError(format!(
                    "Comparison returned non-boolean: {:?}",
                    other
                )))
            }
        }
    }

    // All elements compared definitively equal: <= / >= are TRUE, < / > FALSE.
    Ok(SqlValue::Boolean(is_or_equal))
}

/// Cheap static check: is a subquery *obviously* single-column (so a comparison
/// with it is an ordinary scalar comparison, not a row-value comparison)? Returns
/// true only for a single non-wildcard projected expression with no set
/// operation; anything with a wildcard or set operation needs a schema-aware
/// column-count computation to decide.
pub(crate) fn subquery_is_obviously_single_column(sub: &vibesql_ast::SelectStmt) -> bool {
    sub.set_operation.is_none()
        && sub.select_list.len() == 1
        && matches!(sub.select_list[0], vibesql_ast::SelectItem::Expression { .. })
}

/// Flip a comparison operator so the operands can be swapped while preserving
/// the comparison's meaning (`a < b` ⇔ `b > a`). Equality operators are
/// symmetric and returned unchanged.
pub(crate) fn flip_comparison_operator(
    op: &vibesql_ast::BinaryOperator,
) -> vibesql_ast::BinaryOperator {
    use vibesql_ast::BinaryOperator;
    match op {
        BinaryOperator::LessThan => BinaryOperator::GreaterThan,
        BinaryOperator::LessThanOrEqual => BinaryOperator::GreaterThanOrEqual,
        BinaryOperator::GreaterThan => BinaryOperator::LessThan,
        BinaryOperator::GreaterThanOrEqual => BinaryOperator::LessThanOrEqual,
        other => *other,
    }
}

/// Row-value `IS [NOT] DISTINCT FROM` on already-evaluated values.
///
/// `negated == true` corresponds to `IS` / `IS NOT DISTINCT FROM` (NULL-safe
/// equality: NULL IS NULL is TRUE), `negated == false` to `IS NOT` /
/// `IS DISTINCT FROM`. The result is never NULL.
pub(crate) fn row_values_is_distinct(
    left_values: &[SqlValue],
    right_values: &[SqlValue],
    negated: bool,
) -> SqlValue {
    for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
        if crate::evaluator::core::values_are_distinct(left_val, right_val) {
            // For IS NOT DISTINCT FROM (negated): a distinct element → FALSE.
            // For IS DISTINCT FROM: a distinct element → TRUE.
            return SqlValue::Boolean(!negated);
        }
    }
    // No element was distinct.
    SqlValue::Boolean(negated)
}
