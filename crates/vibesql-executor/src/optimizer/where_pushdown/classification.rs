//! Predicate classification logic
//!
//! This module handles classifying predicates into:
//! - Table-local predicates (single table)
//! - Equijoin conditions (simple equality between two tables)
//! - Complex predicates (everything else)
//! - Always-false predicates (constant FALSE, which short-circuits the entire WHERE)

use super::{
    table_refs::{extract_referenced_tables_branch, try_extract_equijoin_branch},
    PredicateDecomposition,
};
use crate::schema::CombinedSchema;

/// Try to evaluate a constant expression (no column references) to a boolean result.
/// Returns Some(true) if the expression evaluates to TRUE,
/// Some(false) if it evaluates to FALSE or NULL (which is falsy in WHERE),
/// None if it cannot be evaluated (contains column refs or errors).
fn try_evaluate_constant_predicate(expr: &vibesql_ast::Expression) -> Option<bool> {
    use crate::optimizer::expressions::is_constant_false_where;

    // Use the existing constant expression evaluator
    match is_constant_false_where(expr) {
        Some(true) => Some(false), // Expression is always FALSE
        Some(false) => Some(true), // Expression is always TRUE
        None => None,              // Not a constant or couldn't evaluate
    }
}

/// Classify a single predicate (one of the AND-separated clauses) - branch version
pub(crate) fn classify_predicate_branch(
    expr: &vibesql_ast::Expression,
    from_schema: &CombinedSchema,
    decomposition: &mut PredicateDecomposition,
) -> Result<(), String> {
    // Extract tables referenced by this predicate
    let referenced_tables_opt = extract_referenced_tables_branch(expr, from_schema);

    match referenced_tables_opt {
        None => {
            // Predicate references tables not in schema - treat as complex
            decomposition.complex_predicates.push(expr.clone());
            Ok(())
        }
        Some(referenced_tables) => {
            match referenced_tables.len() {
                0 => {
                    // Predicate references no tables (e.g., constant expression like `1 = 2`)
                    // Try to evaluate it as a constant:
                    // - If FALSE: set always_false flag (entire WHERE is false)
                    // - If TRUE: ignore it (TRUE AND X = X)
                    // - If unknown: add to complex predicates
                    match try_evaluate_constant_predicate(expr) {
                        Some(false) => {
                            // Constant FALSE found - entire WHERE clause is always false
                            decomposition.always_false = true;
                        }
                        Some(true) => {
                            // Constant TRUE - ignore it (doesn't filter anything)
                        }
                        None => {
                            // Couldn't evaluate - defer to complex predicates
                            decomposition.complex_predicates.push(expr.clone());
                        }
                    }
                    Ok(())
                }
                1 => {
                    // Single table - can be pushed to table scan
                    // But first check for empty IN list which is always FALSE
                    if let vibesql_ast::Expression::InList { values, negated, .. } = expr {
                        if values.is_empty() && !negated {
                            // Empty IN list (not NOT IN) is always FALSE
                            // This short-circuits the entire WHERE clause
                            decomposition.always_false = true;
                            return Ok(());
                        }
                    }

                    let table_name = referenced_tables.iter().next().unwrap().clone();
                    decomposition
                        .table_local_predicates
                        .entry(table_name)
                        .or_default()
                        .push(expr.clone());
                    Ok(())
                }
                2 => {
                    // Two tables - check if it's a simple equijoin
                    if let Some((left_table, left_col, right_table, right_col)) =
                        try_extract_equijoin_branch(expr, from_schema)
                    {
                        decomposition.equijoin_conditions.push((
                            left_table,
                            left_col,
                            right_table,
                            right_col,
                            expr.clone(),
                        ));
                        Ok(())
                    } else {
                        // Complex predicate involving two tables
                        decomposition.complex_predicates.push(expr.clone());
                        Ok(())
                    }
                }
                _ => {
                    // Multiple tables - always complex
                    decomposition.complex_predicates.push(expr.clone());
                    Ok(())
                }
            }
        }
    }
}
