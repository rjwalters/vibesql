//! Local predicate extraction for table elimination
//!
//! Functions for identifying predicates that reference only a single table,
//! which can be used as filters when converting tables to EXISTS checks.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression};

use super::select_analysis::{
    collect_unqualified_columns_from_expr, extract_tables_from_expr, has_unqualified_column_ref,
};

/// Extract local predicates per table
///
/// Uses both qualified column references and prefix matching for unqualified columns
pub(super) fn extract_local_predicates(
    expr: &Expression,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) -> HashMap<String, Expression> {
    let mut predicates: HashMap<String, Vec<Expression>> = HashMap::new();
    collect_local_predicates(expr, &mut predicates, table_names, table_prefixes);

    // Combine predicates for each table
    predicates.into_iter().map(|(table, preds)| (table, combine_predicates(preds))).collect()
}

fn collect_local_predicates(
    expr: &Expression,
    predicates: &mut HashMap<String, Vec<Expression>>,
    _table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            collect_local_predicates(left, predicates, _table_names, table_prefixes);
            collect_local_predicates(right, predicates, _table_names, table_prefixes);
        }
        _ => {
            // Get tables referenced by this predicate (qualified refs only)
            let mut qualified_refs = HashSet::new();
            extract_tables_from_expr(expr, &mut qualified_refs);

            // Get unqualified columns in this predicate
            let mut unqualified_cols = HashSet::new();
            collect_unqualified_columns_from_expr(expr, &mut unqualified_cols);

            // Determine which tables are referenced
            let mut all_refs = qualified_refs.clone();

            // For unqualified columns, try to attribute them to tables via prefix matching
            for col in &unqualified_cols {
                let col_lower = col.to_lowercase();
                for (table, prefix) in table_prefixes {
                    if col_lower.starts_with(prefix) {
                        all_refs.insert(table.clone());
                    }
                }
            }

            // Skip if this looks like a join condition (qualified + unqualified on different
            // tables)
            if is_potential_join_condition(expr) {
                return;
            }

            // A predicate is local to a table if it references exactly one table
            // (via qualified refs OR via prefix-matched unqualified refs)
            if all_refs.len() == 1 {
                let table = all_refs.into_iter().next().unwrap();
                predicates.entry(table).or_default().push(expr.clone());
            }
        }
    }
}

/// Check if a predicate might be a join condition
///
/// A predicate is a potential join if it's an equality with one side
/// having qualified column refs and the other having unqualified refs
fn is_potential_join_condition(expr: &Expression) -> bool {
    if let Expression::BinaryOp { op: BinaryOperator::Equal, left, right } = expr {
        let mut left_tables = HashSet::new();
        let mut right_tables = HashSet::new();
        extract_tables_from_expr(left, &mut left_tables);
        extract_tables_from_expr(right, &mut right_tables);

        let left_unqualified = has_unqualified_column_ref(left);
        let right_unqualified = has_unqualified_column_ref(right);

        // It's a potential join if one side has qualified ref and other has unqualified
        if !left_tables.is_empty() && right_unqualified && right_tables.is_empty() {
            return true;
        }
        if !right_tables.is_empty() && left_unqualified && left_tables.is_empty() {
            return true;
        }
    }
    false
}

/// Combine predicates with AND
pub(super) fn combine_predicates(predicates: Vec<Expression>) -> Expression {
    if predicates.is_empty() {
        return Expression::Literal(vibesql_types::SqlValue::Boolean(true));
    }

    let mut iter = predicates.into_iter();
    let mut result = iter.next().unwrap();

    for pred in iter {
        result = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(result),
            right: Box::new(pred),
        };
    }

    result
}

/// Flatten AND chain into individual predicates
pub(super) fn flatten_and_chain(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            let mut result = flatten_and_chain(left);
            result.extend(flatten_and_chain(right));
            result
        }
        _ => vec![expr.clone()],
    }
}
