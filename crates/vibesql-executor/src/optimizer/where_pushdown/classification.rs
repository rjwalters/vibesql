//! Predicate classification logic
//!
//! This module handles classifying predicates into:
//! - Table-local predicates (single table)
//! - Equijoin conditions (simple equality between two tables)
//! - Complex predicates (everything else)

use super::{
    table_refs::{extract_referenced_tables_branch, try_extract_equijoin_branch},
    PredicateDecomposition,
};
use crate::schema::CombinedSchema;

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
                    // Predicate references no tables (e.g., constant expression)
                    // This is complex but we can still defer it
                    decomposition.complex_predicates.push(expr.clone());
                    Ok(())
                }
                1 => {
                    // Single table - can be pushed to table scan
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
