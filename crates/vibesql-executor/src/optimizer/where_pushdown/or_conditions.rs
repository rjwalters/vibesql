//! OR condition handling and filter extraction
//!
//! This module provides logic for:
//! - Extracting implied single-table filters from OR predicates
//! - Combining predicates with AND/OR operators

use std::collections::HashMap;

use vibesql_ast::Expression;

use crate::schema::CombinedSchema;

use super::table_refs::{extract_referenced_tables_branch, flatten_conjuncts};
use super::PredicateDecomposition;

/// Extract implied single-table filters from complex OR predicates
///
/// For predicates like:
///   (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
///
/// We can extract implied filters:
///   n1.n_name IN ('FRANCE', 'GERMANY')
///   n2.n_name IN ('FRANCE', 'GERMANY')
///
/// This enables much better join ordering by recognizing that nation tables will be filtered.
pub(crate) fn extract_implied_filters_from_or_predicates(
    decomposition: &mut PredicateDecomposition,
    schema: &CombinedSchema,
) {
    // Process each complex predicate to see if it's an OR we can extract from
    for complex_pred in &decomposition.complex_predicates {
        if let Some(implied_filters) = extract_table_filters_from_or(complex_pred, schema) {
            // Add extracted filters to table_local_predicates
            for (table_name, filter_expr) in implied_filters {
                decomposition
                    .table_local_predicates
                    .entry(table_name)
                    .or_default()
                    .push(filter_expr);
            }
        }
    }
}

/// Extract single-table filters from an OR predicate
///
/// For (A1 AND B1) OR (A2 AND B2) where:
/// - A1, A2 reference only table t1
/// - B1, B2 reference only table t2
///
/// Returns filters: [(t1, A1 OR A2), (t2, B1 OR B2)]
pub(super) fn extract_table_filters_from_or(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<(String, Expression)>> {
    // Check if this is an OR expression
    let (left_branch, right_branch) = match expr {
        Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, left, right } => {
            (left.as_ref(), right.as_ref())
        }
        _ => return None,
    };

    // Extract table->predicates for each OR branch
    let left_filters = extract_table_predicates_from_branch(left_branch, schema);
    let right_filters = extract_table_predicates_from_branch(right_branch, schema);

    // Find tables that are filtered in BOTH branches
    let mut result = Vec::new();
    for (table_name, left_preds) in &left_filters {
        if let Some(right_preds) = right_filters.get(table_name) {
            // Combine left and right predicates for this table with OR
            let left_combined = combine_predicates_with_and(left_preds.clone());
            let right_combined = combine_predicates_with_and(right_preds.clone());

            let combined_filter = Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Or,
                left: Box::new(left_combined),
                right: Box::new(right_combined),
            };

            result.push((table_name.clone(), combined_filter));
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Extract table-local predicates from a branch of an OR
///
/// For an AND chain like (t1.a = 'X' AND t2.b = 'Y' AND t1.c = 'Z')
/// Returns: {"t1": [t1.a = 'X', t1.c = 'Z'], "t2": [t2.b = 'Y']}
fn extract_table_predicates_from_branch(
    expr: &Expression,
    schema: &CombinedSchema,
) -> HashMap<String, Vec<Expression>> {
    let mut table_predicates: HashMap<String, Vec<Expression>> = HashMap::new();

    // Flatten ANDs in this branch
    let conjuncts = flatten_conjuncts(expr);

    for conjunct in conjuncts {
        // Get tables referenced by this conjunct
        if let Some(tables) = extract_referenced_tables_branch(&conjunct, schema) {
            if tables.len() == 1 {
                // Single-table predicate - add to that table's list
                let table_name = tables.iter().next().unwrap().clone();
                table_predicates.entry(table_name).or_default().push(conjunct);
            }
            // Multi-table predicates are ignored for filter extraction
        }
    }

    table_predicates
}

/// Combine a list of predicates into a single expression using AND
pub(crate) fn combine_predicates_with_and(
    mut predicates: Vec<vibesql_ast::Expression>,
) -> vibesql_ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}
