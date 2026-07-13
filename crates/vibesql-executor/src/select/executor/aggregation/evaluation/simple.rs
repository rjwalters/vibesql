//! Simple expression evaluation in aggregate context (literals, column refs, etc.)

/// Re-import like_match for convenience
use pattern::like_match;

use super::super::super::builder::SelectExecutor;
/// Import pattern matching function for LIKE evaluation
use crate::evaluator::pattern;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Evaluate expressions that may contain nested aggregates
///
/// Handles: Cast, Between, InList, Like, IsNull
///
/// These expressions need recursive evaluation because their sub-expressions
/// might contain aggregate functions.
pub(super) fn evaluate(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        // CAST needs special handling to support nested aggregates
        // Example: CAST(MIN(74) AS SIGNED) or CAST(-MIN(74) AS SIGNED)
        vibesql_ast::Expression::Cast { expr: inner_expr, data_type } => {
            // Recursively evaluate the inner expression with aggregate support
            let inner_value =
                executor.evaluate_with_aggregates(inner_expr, group_rows, group_key, evaluator)?;

            // Cast the result to the target type using the casting module
            let sql_mode = executor.database.sql_mode();
            crate::evaluator::casting::cast_value(&inner_value, data_type, &sql_mode)
        }

        // BETWEEN: expr BETWEEN low AND high
        // All three sub-expressions may contain aggregates
        vibesql_ast::Expression::Between { expr: test_expr, low, high, negated, symmetric } => {
            let test_val =
                executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let mut low_val =
                executor.evaluate_with_aggregates(low, group_rows, group_key, evaluator)?;
            let mut high_val =
                executor.evaluate_with_aggregates(high, group_rows, group_key, evaluator)?;

            // For SYMMETRIC: swap bounds if low > high
            if *symmetric {
                let gt_result = ExpressionEvaluator::eval_binary_op_static(
                    &low_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    &high_val,
                    vibesql_types::SqlMode::default(),
                )?;

                if let vibesql_types::SqlValue::Boolean(true) = gt_result {
                    std::mem::swap(&mut low_val, &mut high_val);
                }
            }

            // Check if test_val >= low
            let ge_low = ExpressionEvaluator::eval_binary_op_static(
                &test_val,
                &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                &low_val,
                vibesql_types::SqlMode::default(),
            )?;

            // Check if test_val <= high
            let le_high = ExpressionEvaluator::eval_binary_op_static(
                &test_val,
                &vibesql_ast::BinaryOperator::LessThanOrEqual,
                &high_val,
                vibesql_types::SqlMode::default(),
            )?;

            // Combine with AND/OR depending on negated
            if *negated {
                // NOT BETWEEN: test_val < low OR test_val > high
                let lt_low = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::LessThan,
                    &low_val,
                    vibesql_types::SqlMode::default(),
                )?;
                let gt_high = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    &high_val,
                    vibesql_types::SqlMode::default(),
                )?;
                ExpressionEvaluator::eval_binary_op_static(
                    &lt_low,
                    &vibesql_ast::BinaryOperator::Or,
                    &gt_high,
                    vibesql_types::SqlMode::default(),
                )
            } else {
                // BETWEEN: test_val >= low AND test_val <= high
                ExpressionEvaluator::eval_binary_op_static(
                    &ge_low,
                    &vibesql_ast::BinaryOperator::And,
                    &le_high,
                    vibesql_types::SqlMode::default(),
                )
            }
        }

        // IN list: expr IN (val1, val2, ...)
        vibesql_ast::Expression::InList { expr: test_expr, values, negated } => {
            // Handle empty IN list: returns false for IN, true for NOT IN
            // This is per SQLite behavior (SQL:1999 extension, not standard SQL)
            if values.is_empty() {
                return Ok(vibesql_types::SqlValue::Boolean(*negated));
            }

            let test_val =
                executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;

            // SQL standard behavior for NULL IN (list):
            // - NULL IN (empty list) → FALSE (already handled above)
            // - NULL IN (non-empty list) → NULL (three-valued logic)
            // The IN operator returns NULL when comparing NULL to any value
            // Fix for issue #1863: CASE expressions with aggregates must return NULL correctly
            if matches!(test_val, vibesql_types::SqlValue::Null) {
                return Ok(vibesql_types::SqlValue::Null);
            }

            // Evaluate all values in the list
            let mut list_values = Vec::new();
            for value_expr in values {
                list_values.push(
                    executor
                        .evaluate_with_aggregates(value_expr, group_rows, group_key, evaluator)?,
                );
            }

            // Check if test_val is in the list
            let mut found = false;
            let mut found_null = false;
            for list_val in &list_values {
                // Track if we encounter NULL in the list
                if matches!(list_val, vibesql_types::SqlValue::Null) {
                    found_null = true;
                    continue;
                }

                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    list_val,
                    vibesql_types::SqlMode::default(),
                )?;

                if let vibesql_types::SqlValue::Boolean(true) = eq_result {
                    found = true;
                    break;
                }
            }

            // SQL three-valued logic:
            // - If found a match: return TRUE (or FALSE if negated)
            // - If not found but list contains NULL: return NULL
            // - If not found and no NULL: return FALSE (or TRUE if negated)
            if found {
                Ok(vibesql_types::SqlValue::Boolean(!negated))
            } else if found_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(*negated))
            }
        }

        // LIKE: expr LIKE pattern
        vibesql_ast::Expression::Like { expr: test_expr, pattern, negated, escape } => {
            let test_val =
                executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let pattern_val =
                executor.evaluate_with_aggregates(pattern, group_rows, group_key, evaluator)?;

            // Extract string values
            // Extract string values, mirroring the coercion rules of the
            // scalar evaluators (evaluator/expressions/predicates.rs):
            // numerics render as text, booleans as 0/1, blob bytes as text.
            let text = match test_val {
                vibesql_types::SqlValue::Varchar(ref s)
                | vibesql_types::SqlValue::Character(ref s) => s.clone(),
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
                // SQLite has no boolean type: EXISTS/IN results behave as integers 0/1
                vibesql_types::SqlValue::Boolean(b) => {
                    arcstr::ArcStr::from(if b { "1" } else { "0" })
                }
                // SQLite treats blob bytes as raw text for LIKE comparison
                vibesql_types::SqlValue::Blob(ref b) => {
                    arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned())
                }
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "LIKE".to_string(),
                        right: pattern_val,
                    })
                }
            };

            let pattern_str = match pattern_val {
                vibesql_types::SqlValue::Varchar(ref s)
                | vibesql_types::SqlValue::Character(ref s) => s.clone(),
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
                // SQLite has no boolean type: EXISTS/IN results behave as integers 0/1
                vibesql_types::SqlValue::Boolean(b) => {
                    arcstr::ArcStr::from(if b { "1" } else { "0" })
                }
                // SQLite treats blob bytes as raw text for the LIKE pattern too
                vibesql_types::SqlValue::Blob(ref b) => {
                    arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned())
                }
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "LIKE".to_string(),
                        right: pattern_val,
                    })
                }
            };

            // Evaluate the escape character if provided
            let escape_char = if let Some(escape_expr) = escape {
                let escape_val = executor.evaluate_with_aggregates(
                    escape_expr,
                    group_rows,
                    group_key,
                    evaluator,
                )?;
                match escape_val {
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        let mut chars = s.chars();
                        match (chars.next(), chars.next()) {
                            (Some(c), None) => Some(c), // Exactly one character
                            _ => {
                                // Empty string or multi-character string: error per SQLite
                                return Err(ExecutorError::SqliteCompatError(
                                    "ESCAPE expression must be a single character".to_string(),
                                ));
                            }
                        }
                    }
                    vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                    vibesql_types::SqlValue::Integer(n) => {
                        let s = n.to_string();
                        let mut chars = s.chars();
                        match (chars.next(), chars.next()) {
                            (Some(c), None) => Some(c), // Exactly one character
                            _ => {
                                return Err(ExecutorError::SqliteCompatError(
                                    "ESCAPE expression must be a single character".to_string(),
                                ))
                            }
                        }
                    }
                    _ => {
                        return Err(ExecutorError::TypeMismatch {
                            left: escape_val,
                            op: "ESCAPE".to_string(),
                            right: vibesql_types::SqlValue::Null,
                        })
                    }
                }
            } else {
                None
            };

            // Get case_sensitive_like setting from database (default: false = case-insensitive)
            let case_sensitive = executor.database.case_sensitive_like();

            // Perform pattern matching
            let matches = like_match(&text, &pattern_str, case_sensitive, escape_char);

            // Apply negation if needed
            let result = if *negated { !matches } else { matches };

            Ok(vibesql_types::SqlValue::Boolean(result))
        }

        // GLOB: expr GLOB pattern
        //
        // Mirrors the scalar-evaluator path (evaluator/expressions/predicates.rs
        // eval_glob) but evaluates both operands with aggregate support so that
        // non-literal operands (columns, subqueries, IN, IsNull, Cast, function
        // calls, NULL/numeric literals) work in the simple/scalar evaluator
        // instead of raising "Unexpected expression in simple evaluator". (#6070)
        //
        // GLOB is always case-sensitive and does not take an ESCAPE clause,
        // which is why it uses glob_match rather than like_match.
        vibesql_ast::Expression::Glob { expr: test_expr, pattern, negated, .. } => {
            let test_val =
                executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let pattern_val =
                executor.evaluate_with_aggregates(pattern, group_rows, group_key, evaluator)?;

            // Coerce operands to text, mirroring eval_glob's rules: SQLite
            // renders numerics as text, booleans (EXISTS/IN results) as 0/1,
            // and blob bytes as raw text; NULL on either side yields NULL.
            let text = match &test_val {
                vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                    s.clone()
                }
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Boolean(b) => {
                    arcstr::ArcStr::from(if *b { "1" } else { "0" })
                }
                vibesql_types::SqlValue::Blob(b) => {
                    arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned())
                }
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "GLOB".to_string(),
                        right: pattern_val,
                    })
                }
            };

            let pattern_str = match &pattern_val {
                vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                    s.clone()
                }
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
                vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
                vibesql_types::SqlValue::Boolean(b) => {
                    arcstr::ArcStr::from(if *b { "1" } else { "0" })
                }
                vibesql_types::SqlValue::Blob(b) => {
                    arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned())
                }
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "GLOB".to_string(),
                        right: pattern_val,
                    })
                }
            };

            let matches = pattern::glob_match(&text, &pattern_str);
            let result = if *negated { !matches } else { matches };

            Ok(vibesql_types::SqlValue::Boolean(result))
        }

        // IS NULL / IS NOT NULL
        vibesql_ast::Expression::IsNull { expr: test_expr, negated } => {
            let value =
                executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let is_null = matches!(value, vibesql_types::SqlValue::Null);
            let result = if *negated { !is_null } else { is_null };
            Ok(vibesql_types::SqlValue::Boolean(result))
        }

        // POSITION: find position of substring in string
        vibesql_ast::Expression::Position { substring, string, .. } => {
            let substring_val =
                executor.evaluate_with_aggregates(substring, group_rows, group_key, evaluator)?;
            let string_val =
                executor.evaluate_with_aggregates(string, group_rows, group_key, evaluator)?;

            // Evaluate position (1-indexed, 0 if not found)
            match (&substring_val, &string_val) {
                (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
                    Ok(vibesql_types::SqlValue::Null)
                }
                (
                    vibesql_types::SqlValue::Varchar(sub) | vibesql_types::SqlValue::Character(sub),
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s),
                ) => {
                    // Find position (1-indexed, 0 if not found)
                    let pos = s.find(&**sub).map(|p| p + 1).unwrap_or(0);
                    Ok(vibesql_types::SqlValue::Integer(pos as i64))
                }
                _ => Err(ExecutorError::TypeMismatch {
                    left: substring_val,
                    op: "POSITION".to_string(),
                    right: string_val,
                }),
            }
        }

        // TRIM: remove characters from string
        vibesql_ast::Expression::Trim { position, removal_char, string } => {
            let string_val =
                executor.evaluate_with_aggregates(string, group_rows, group_key, evaluator)?;
            let removal_val = if let Some(rc) = removal_char {
                executor.evaluate_with_aggregates(rc, group_rows, group_key, evaluator)?
            } else {
                vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(" "))
            };

            // Delegate to standard evaluator logic
            match (&string_val, &removal_val) {
                (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
                    Ok(vibesql_types::SqlValue::Null)
                }
                (
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s),
                    vibesql_types::SqlValue::Varchar(rem) | vibesql_types::SqlValue::Character(rem),
                ) => {
                    use vibesql_ast::TrimPosition;
                    let trimmed = match position {
                        Some(TrimPosition::Leading) => s.trim_start_matches(&**rem),
                        Some(TrimPosition::Trailing) => s.trim_end_matches(&**rem),
                        Some(TrimPosition::Both) | None => {
                            s.trim_start_matches(&**rem).trim_end_matches(&**rem)
                        }
                    };
                    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(trimmed)))
                }
                _ => Err(ExecutorError::TypeMismatch {
                    left: string_val,
                    op: "TRIM".to_string(),
                    right: removal_val,
                }),
            }
        }

        // INTERVAL: evaluate the value expression and delegate to evaluator
        vibesql_ast::Expression::Interval { value, .. } => {
            let value_result =
                executor.evaluate_with_aggregates(value, group_rows, group_key, evaluator)?;

            // For now, delegate full interval evaluation to the standard evaluator
            // This requires creating a new Interval expression with the evaluated value as a
            // literal
            let evaluated_expr = vibesql_ast::Expression::Interval {
                value: Box::new(vibesql_ast::Expression::Literal(value_result)),
                unit: match expr {
                    vibesql_ast::Expression::Interval { unit, .. } => unit.clone(),
                    _ => unreachable!(),
                },
                leading_precision: match expr {
                    vibesql_ast::Expression::Interval { leading_precision, .. } => {
                        *leading_precision
                    }
                    _ => unreachable!(),
                },
                fractional_precision: match expr {
                    vibesql_ast::Expression::Interval { fractional_precision, .. } => {
                        *fractional_precision
                    }
                    _ => unreachable!(),
                },
            };

            // Use the standard evaluator to process the interval
            if let Some(first_row) = group_rows.first() {
                evaluator.eval(&evaluated_expr, first_row)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }

        // EXTRACT: evaluate the expression and delegate to evaluator
        vibesql_ast::Expression::Extract { field, expr: inner_expr } => {
            let inner_value =
                executor.evaluate_with_aggregates(inner_expr, group_rows, group_key, evaluator)?;

            // Create an Extract expression with the evaluated value as a literal
            let evaluated_expr = vibesql_ast::Expression::Extract {
                field: field.clone(),
                expr: Box::new(vibesql_ast::Expression::Literal(inner_value)),
            };

            // Use the standard evaluator to process the extract
            if let Some(first_row) = group_rows.first() {
                evaluator.eval(&evaluated_expr, first_row)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }

        // Conjunction (AND) - evaluate all children with short-circuit
        vibesql_ast::Expression::Conjunction(children) => {
            let mut result = vibesql_types::SqlValue::Boolean(true);
            for child in children {
                let val =
                    executor.evaluate_with_aggregates(child, group_rows, group_key, evaluator)?;
                match val {
                    vibesql_types::SqlValue::Boolean(false) => {
                        return Ok(vibesql_types::SqlValue::Boolean(false))
                    }
                    vibesql_types::SqlValue::Null => result = vibesql_types::SqlValue::Null,
                    vibesql_types::SqlValue::Boolean(true) => {}
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "Conjunction requires boolean operands, got {:?}",
                            val
                        )))
                    }
                }
            }
            Ok(result)
        }

        // Disjunction (OR) - evaluate all children with short-circuit
        vibesql_ast::Expression::Disjunction(children) => {
            let mut result = vibesql_types::SqlValue::Boolean(false);
            for child in children {
                let val =
                    executor.evaluate_with_aggregates(child, group_rows, group_key, evaluator)?;
                match val {
                    vibesql_types::SqlValue::Boolean(true) => {
                        return Ok(vibesql_types::SqlValue::Boolean(true))
                    }
                    vibesql_types::SqlValue::Null => result = vibesql_types::SqlValue::Null,
                    vibesql_types::SqlValue::Boolean(false) => {}
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "Disjunction requires boolean operands, got {:?}",
                            val
                        )))
                    }
                }
            }
            Ok(result)
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unexpected expression in simple evaluator: {:?}",
            expr
        ))),
    }
}

/// Evaluate expressions that CANNOT contain nested aggregates
///
/// Handles: Literal, ColumnRef, Wildcard, CurrentDate, etc.
///
/// These are truly simple expressions that can be evaluated directly using the standard evaluator.
/// Uses the representative row (from MAX/MIN aggregate) when available for SQLite compatibility.
pub(super) fn evaluate_no_aggregates(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        // Literals can be evaluated without row context
        vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

        // All other simple expressions: use representative row from MAX/MIN aggregate when
        // available, otherwise fall back to first row. This matches SQLite's behavior where
        // bare column references in aggregate queries return values from the row that
        // determined the MAX/MIN result.
        _ => {
            let row = if let Some(rep_idx) = executor.get_aggregate_representative_row() {
                group_rows.get(rep_idx)
            } else {
                group_rows.first()
            };

            if let Some(row) = row {
                evaluator.eval(expr, row)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }
    }
}
