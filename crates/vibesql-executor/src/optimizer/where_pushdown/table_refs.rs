//! Table reference extraction utilities
//!
//! This module provides utilities for:
//! - Extracting table references from expressions
//! - Flattening conjunctive (AND) expressions
//! - Detecting equijoin conditions

use std::collections::HashSet;

use crate::schema::CombinedSchema;

/// Flatten a WHERE clause into conjunction (AND-separated) clauses
/// Only splits on top-level AND, not on OR or nested AND
pub(crate) fn flatten_conjuncts(expr: &vibesql_ast::Expression) -> Vec<vibesql_ast::Expression> {
    match expr {
        vibesql_ast::Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            let mut conjuncts = flatten_conjuncts(left);
            conjuncts.extend(flatten_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

/// Extract all table names referenced in a predicate (branch version with schema)
/// Returns None if the expression references tables not in the schema (should be treated as complex)
pub(crate) fn extract_referenced_tables_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<HashSet<String>> {
    let mut tables = HashSet::new();
    let success = extract_tables_recursive_branch(expr, schema, &mut tables);
    if success {
        Some(tables)
    } else {
        None
    }
}

/// Recursive helper to extract table references (branch version)
/// Returns false if the expression references tables not in the schema
fn extract_tables_recursive_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    tables: &mut HashSet<String>,
) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef { table: Some(table_name), .. } => {
            let normalized = table_name.to_lowercase();
            if schema.contains_table(&normalized) {
                tables.insert(normalized);
                true
            } else {
                // Table qualification not in schema - treat as complex predicate
                false
            }
        }
        vibesql_ast::Expression::ColumnRef { table: None, column } => {
            // Unqualified column reference - need to resolve it to table(s)
            // Search all tables in the schema to find which contain this column
            let column_lower = column.to_lowercase();
            let mut found = false;
            for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
                if table_schema.columns.iter().any(|col| col.name.to_lowercase() == column_lower) {
                    tables.insert(table_name.to_string());
                    found = true;
                }
            }
            found // Return true if column found in at least one table
        }
        vibesql_ast::Expression::BinaryOp { left, op: _, right } => {
            extract_tables_recursive_branch(left, schema, tables)
                && extract_tables_recursive_branch(right, schema, tables)
        }
        vibesql_ast::Expression::UnaryOp { expr: inner, .. } => {
            extract_tables_recursive_branch(inner, schema, tables)
        }
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().all(|arg| extract_tables_recursive_branch(arg, schema, tables))
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            extract_tables_recursive_branch(expr, schema, tables)
                && extract_tables_recursive_branch(low, schema, tables)
                && extract_tables_recursive_branch(high, schema, tables)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            extract_tables_recursive_branch(expr, schema, tables)
                && values.iter().all(|val| extract_tables_recursive_branch(val, schema, tables))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            let op_ok = operand
                .as_ref()
                .is_none_or(|op| extract_tables_recursive_branch(op, schema, tables));
            let when_ok = when_clauses.iter().all(|when_clause| {
                when_clause
                    .conditions
                    .iter()
                    .all(|condition| extract_tables_recursive_branch(condition, schema, tables))
                    && extract_tables_recursive_branch(&when_clause.result, schema, tables)
            });
            let else_ok = else_result
                .as_ref()
                .is_none_or(|else_res| extract_tables_recursive_branch(else_res, schema, tables));
            op_ok && when_ok && else_ok
        }
        vibesql_ast::Expression::In { expr, subquery, .. } => {
            // IN with subquery: check if non-correlated
            // Non-correlated subqueries can be pushed down as table-local predicates
            // because they can be evaluated independently (and cached)
            //
            // For example, in Q18:
            //   WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
            // The subquery is non-correlated, so this can be pushed down to filter the orders table
            // during the scan phase, significantly reducing the join input size.
            if !crate::correlation::is_correlated(subquery, schema) {
                // Non-correlated subquery: the predicate can be pushed down
                // Extract table references from the left-hand expression only
                // (the subquery is evaluated independently, so it doesn't add table references)
                extract_tables_recursive_branch(expr, schema, tables)
            } else {
                // Correlated subquery: treat as complex (must be evaluated after joins)
                false
            }
        }
        vibesql_ast::Expression::ScalarSubquery(_) => {
            // Scalar subqueries may reference columns from outer scope (correlated subqueries)
            // Treat as complex predicates to prevent incorrect pushdown to individual tables
            // This ensures the subquery has access to the full combined schema including outer context
            false
        }
        vibesql_ast::Expression::Exists { .. } => {
            // EXISTS subqueries may be correlated, treat as complex
            false
        }
        vibesql_ast::Expression::QuantifiedComparison { .. } => {
            // Quantified comparisons (ANY/ALL/SOME) may be correlated, treat as complex
            false
        }
        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            // LIKE predicate: extract table references from both the expression and pattern
            // Example: p_name LIKE '%green%' references the table containing p_name
            extract_tables_recursive_branch(expr, schema, tables)
                && extract_tables_recursive_branch(pattern, schema, tables)
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            // IS NULL / IS NOT NULL: extract table references from the expression
            extract_tables_recursive_branch(expr, schema, tables)
        }
        _ => {
            // Other expression types: Literal, Wildcard, etc.
            true
        }
    }
}

/// Try to extract an equijoin condition from an expression (branch version)
/// Returns (left_table, left_col, right_table, right_col) if successful
pub(crate) fn try_extract_equijoin_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String, String, String)> {
    if let vibesql_ast::Expression::BinaryOp {
        left,
        op: vibesql_ast::BinaryOperator::Equal,
        right,
    } = expr
    {
        // Try to extract column references from both sides
        let (left_table, left_col) = extract_column_reference_branch(left, schema)?;
        let (right_table, right_col) = extract_column_reference_branch(right, schema)?;

        // Ensure they reference different tables
        if left_table != right_table {
            return Some((left_table, left_col, right_table, right_col));
        }
    }
    None
}

/// Extract (table_name, column_name) from a column reference expression (branch version)
fn extract_column_reference_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String)> {
    match expr {
        vibesql_ast::Expression::ColumnRef { table, column } => {
            if let Some(table_name) = table {
                let normalized_table = table_name.to_lowercase();
                if schema.contains_table(&normalized_table) {
                    return Some((normalized_table, column.to_lowercase()));
                }
            }
            None
        }
        _ => None,
    }
}
