//! Shared subquery evaluation logic
//!
//! This module contains core subquery evaluation functions that are shared between
//! `ExpressionEvaluator` and `CombinedExpressionEvaluator` to eliminate code duplication.
//!
//! ## Architecture
//!
//! The functions in this module implement the pure logic for subquery evaluation,
//! operating on already-executed query results (`Vec<Row>`). The evaluator-specific
//! code handles schema conversion and query execution, then delegates to these
//! shared functions for result processing.
//!
//! ## Refactoring Pattern
//!
//! This follows the DRY pattern established in #1427 and architectural plan from #1437:
//! - Extract common logic into static/standalone functions
//! - Both evaluators delegate to shared implementation
//! - Maintain identical SQL semantics across both evaluators

use crate::errors::ExecutorError;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Evaluate scalar subquery result - must return exactly one row and one column
///
/// ## SQL Semantics (SQL:1999 Section 7.9)
/// - Must return exactly 1 row (error if multiple rows)
/// - Must return exactly 1 column (error if multiple columns)
/// - Returns NULL if subquery returns zero rows
///
/// ## Parameters
/// - `rows`: The result set from executing the subquery
///
/// ## Returns
/// - `Ok(SqlValue::Null)` if no rows returned
/// - `Ok(value)` the single value if exactly one row returned
/// - `Err(SubqueryReturnedMultipleRows)` if more than one row
/// - `Err(SubqueryColumnCountMismatch)` if not exactly one column
/// - `Err(ColumnIndexOutOfBounds)` if row doesn't have a value at index 0
pub fn eval_scalar_subquery_core(rows: &[Row]) -> Result<SqlValue, ExecutorError> {
    // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 row
    if rows.len() > 1 {
        return Err(ExecutorError::SubqueryReturnedMultipleRows {
            expected: 1,
            actual: rows.len(),
        });
    }

    // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 column
    // SQL standard (R-35033-20570): We must validate the actual column count AFTER
    // execution because wildcards like SELECT * expand to multiple columns at runtime.
    if !rows.is_empty() && rows[0].values.len() != 1 {
        return Err(ExecutorError::SubqueryColumnCountMismatch {
            expected: 1,
            actual: rows[0].values.len(),
        });
    }

    // Return the single value, or NULL if no rows
    if rows.is_empty() {
        Ok(SqlValue::Null)
    } else {
        rows[0].get(0).cloned().ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })
    }
}

/// Evaluate EXISTS predicate result
///
/// ## SQL Semantics (SQL:1999 Section 8.7)
/// - Returns TRUE if subquery returns at least one row
/// - Returns FALSE if subquery returns zero rows
/// - Never returns NULL (unlike most predicates)
/// - Negation applied after evaluation (NOT EXISTS)
///
/// ## Parameters
/// - `has_rows`: Whether the subquery returned any rows
/// - `negated`: Whether this is a NOT EXISTS predicate
///
/// ## Returns
/// - `SqlValue::Boolean(true)` if has_rows and not negated, or !has_rows and negated
/// - `SqlValue::Boolean(false)` otherwise
pub fn eval_exists_core(has_rows: bool, negated: bool) -> SqlValue {
    let result = if negated { !has_rows } else { has_rows };
    SqlValue::Boolean(result)
}

/// Evaluate quantified comparison result (ALL/ANY/SOME)
///
/// ## SQL Semantics (SQL:1999 Section 8.8)
///
/// ### ALL quantifier:
/// - Empty subquery: returns TRUE (vacuously true)
/// - Left value NULL: returns NULL
/// - Any comparison FALSE: returns FALSE immediately
/// - Any comparison NULL (and none FALSE): returns NULL
/// - All comparisons TRUE: returns TRUE
///
/// ### ANY/SOME quantifier:
/// - Empty subquery: returns FALSE (no rows to satisfy)
/// - Left value NULL: returns NULL
/// - Any comparison TRUE: returns TRUE immediately
/// - All comparisons FALSE: returns FALSE
/// - Any comparison NULL (and none TRUE): returns NULL
///
/// ## Parameters
/// - `left_val`: The left-hand value from the comparison expression
/// - `rows`: The result set from executing the subquery
/// - `op`: The comparison operator (=, <, >, etc.)
/// - `quantifier`: ALL, ANY, or SOME
/// - `eval_op`: Function to evaluate the binary operation (comparison)
///
/// ## Returns
/// - `Ok(SqlValue::Boolean)` with the quantified comparison result
/// - `Ok(SqlValue::Null)` if result is NULL per SQL three-valued logic
/// - `Err(SubqueryColumnCountMismatch)` if subquery doesn't return exactly one column
/// - `Err(...)` any error from evaluating the comparison operation
pub fn eval_quantified_core<F>(
    left_val: &SqlValue,
    rows: &[Row],
    op: &vibesql_ast::BinaryOperator,
    quantifier: &vibesql_ast::Quantifier,
    eval_op: F,
) -> Result<SqlValue, ExecutorError>
where
    F: Fn(&SqlValue, &vibesql_ast::BinaryOperator, &SqlValue) -> Result<SqlValue, ExecutorError>,
{
    // Empty subquery special cases:
    // - ALL: returns TRUE (vacuously true - all zero rows satisfy the condition)
    // - ANY/SOME: returns FALSE (no rows to satisfy the condition)
    if rows.is_empty() {
        return Ok(SqlValue::Boolean(matches!(quantifier, vibesql_ast::Quantifier::All)));
    }

    // If left value is NULL, result is NULL
    if matches!(left_val, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match quantifier {
        vibesql_ast::Quantifier::All => {
            // ALL: comparison must be TRUE for all rows
            // If any comparison is FALSE, return FALSE
            // If any comparison is NULL (and none FALSE), return NULL
            let mut has_null = false;

            for subquery_row in rows {
                if subquery_row.values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery_row.values.len(),
                    });
                }

                let right_val = &subquery_row.values[0];

                // Handle NULL in subquery result
                if matches!(right_val, SqlValue::Null) {
                    has_null = true;
                    continue;
                }

                // Evaluate comparison
                let cmp_result = eval_op(left_val, op, right_val)?;

                match cmp_result {
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                    SqlValue::Null => has_null = true,
                    _ => {} // TRUE, continue checking
                }
            }

            // If we saw any NULLs (and no FALSEs), return NULL
            // Otherwise return TRUE (all comparisons were TRUE)
            if has_null {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(true))
            }
        }

        vibesql_ast::Quantifier::Any | vibesql_ast::Quantifier::Some => {
            // ANY/SOME: comparison must be TRUE for at least one row
            // If any comparison is TRUE, return TRUE
            // If all comparisons are FALSE, return FALSE
            // If any comparison is NULL (and none TRUE), return NULL
            let mut has_null = false;

            for subquery_row in rows {
                if subquery_row.values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery_row.values.len(),
                    });
                }

                let right_val = &subquery_row.values[0];

                // Handle NULL in subquery result
                if matches!(right_val, SqlValue::Null) {
                    has_null = true;
                    continue;
                }

                // Evaluate comparison
                let cmp_result = eval_op(left_val, op, right_val)?;

                match cmp_result {
                    SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                    SqlValue::Null => has_null = true,
                    _ => {} // FALSE, continue checking
                }
            }

            // If we saw any NULLs (and no TRUEs), return NULL
            // Otherwise return FALSE (no comparisons were TRUE)
            if has_null {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(false))
            }
        }
    }
}
