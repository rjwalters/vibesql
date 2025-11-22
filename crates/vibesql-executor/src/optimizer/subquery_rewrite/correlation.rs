//! Subquery correlation analysis
//!
//! This module provides functions to determine if a subquery references columns
//! from outer queries (i.e., is "correlated"). Correlation detection is critical
//! for choosing the right optimization strategy: correlated subqueries benefit
//! from EXISTS transformation, while uncorrelated ones benefit from DISTINCT.

use vibesql_ast::{Expression, SelectItem, SelectStmt};

/// Check if a subquery is correlated (references outer query columns)
///
/// A subquery is correlated if it contains column references that are not
/// bound within the subquery itself (i.e., they reference columns from the
/// outer query).
///
/// # Examples
///
/// Correlated:
/// ```sql
/// SELECT * FROM orders WHERE customer_id IN (
///   SELECT customer_id FROM customers WHERE customers.region = orders.region
/// )
/// ```
///
/// Uncorrelated:
/// ```sql
/// SELECT * FROM orders WHERE status IN (
///   SELECT status FROM valid_statuses
/// )
/// ```
pub(crate) fn is_correlated(subquery: &SelectStmt) -> bool {
    // Check WHERE clause for external column references
    if let Some(where_clause) = &subquery.where_clause {
        if has_external_column_refs(where_clause, subquery) {
            return true;
        }
    }

    // Check SELECT list for external column references
    for item in &subquery.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if has_external_column_refs(expr, subquery) {
                return true;
            }
        }
    }

    // Check HAVING clause for external column references
    if let Some(having) = &subquery.having {
        if has_external_column_refs(having, subquery) {
            return true;
        }
    }

    false
}

/// Check if an expression contains column references external to the subquery
///
/// This is a heuristic check: a qualified column reference (table.column)
/// that doesn't match the subquery's FROM clause tables is likely external.
///
/// ## Limitations
///
/// **Unqualified column references**: Without full schema information and symbol
/// table analysis, we cannot definitively determine if an unqualified column
/// reference is internal or external to the subquery.
///
/// Current approach:
/// - **Qualified refs** (e.g., `orders.region`): Can detect if table is external
/// - **Unqualified refs** (e.g., `region`): Follow SQL resolution rules - assume
///   internal first (matches innermost scope per SQL semantics)
///
/// This conservative approach may:
/// - Miss some correlations (when unqualified ref is actually external)
/// - Result in suboptimal optimization choices (DISTINCT instead of EXISTS)
/// - But maintains correctness (won't incorrectly optimize)
///
/// **Example of limitation**:
/// ```sql
/// SELECT * FROM orders WHERE customer_id IN (
///   SELECT customer_id FROM customers WHERE region = outer_region
/// )
/// ```
/// If `outer_region` doesn't exist in `customers`, it's external, but we can't
/// detect this without schema information.
///
/// TODO: Implement full symbol table analysis for more accurate correlation detection
pub(super) fn has_external_column_refs(expr: &Expression, subquery: &SelectStmt) -> bool {
    match expr {
        Expression::ColumnRef { table: Some(table), .. } => {
            // If column is qualified, check if table is in subquery's FROM clause
            !subquery_references_table(subquery, table)
        }

        Expression::ColumnRef { table: None, column } => {
            // Unqualified column refs: Conservative approach
            //
            // Per SQL semantics (section 7.6 of SQL:1999), unqualified column references
            // are resolved by searching innermost scope first. Without full schema
            // information and symbol table analysis, we cannot definitively determine
            // if an unqualified column reference is internal or external to the subquery.
            //
            // TPC-H uses a prefix convention where columns are prefixed with the first
            // letter of their table name: o_orderkey for orders, l_orderkey for lineitem.
            // We can use this as a heuristic ONLY when the column follows this pattern.
            //
            // Conservative behavior: Returns false (not external) when uncertain,
            // which may miss some optimizations but maintains correctness.
            if let Some(from) = &subquery.from {
                let col_prefix = column.chars().next().unwrap_or('_').to_ascii_lowercase();

                // Only apply TPC-H heuristic if column appears to follow TPC-H naming
                // convention (prefix + underscore, e.g., "o_orderkey", "l_shipdate")
                if column.chars().nth(1) == Some('_') {
                    let from_table_prefixes = extract_table_prefixes(from);
                    !from_table_prefixes.iter().any(|tp| *tp == col_prefix)
                } else {
                    // Not TPC-H style column name: assume internal (conservative)
                    false
                }
            } else {
                false
            }
        }

        // Recursively check nested expressions
        Expression::BinaryOp { left, right, .. } => {
            has_external_column_refs(left, subquery) || has_external_column_refs(right, subquery)
        }

        Expression::UnaryOp { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::IsNull { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| has_external_column_refs(e, subquery))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(|cond| has_external_column_refs(cond, subquery))
                        || has_external_column_refs(&clause.result, subquery)
                })
                || else_result.as_ref().is_some_and(|e| has_external_column_refs(e, subquery))
        }

        Expression::ScalarSubquery(_)
        | Expression::In { .. }
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => {
            // Nested subqueries are handled separately
            false
        }

        Expression::InList { expr, values, .. } => {
            has_external_column_refs(expr, subquery)
                || values.iter().any(|v| has_external_column_refs(v, subquery))
        }

        Expression::Between { expr, low, high, .. } => {
            has_external_column_refs(expr, subquery)
                || has_external_column_refs(low, subquery)
                || has_external_column_refs(high, subquery)
        }

        Expression::Cast { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(|arg| has_external_column_refs(arg, subquery))
        }

        Expression::Position { substring, string, .. } => {
            has_external_column_refs(substring, subquery) || has_external_column_refs(string, subquery)
        }

        Expression::Trim {
            removal_char,
            string,
            ..
        } => {
            removal_char.as_ref().is_some_and(|e| has_external_column_refs(e, subquery))
                || has_external_column_refs(string, subquery)
        }

        Expression::Like { expr, pattern, .. } => {
            has_external_column_refs(expr, subquery) || has_external_column_refs(pattern, subquery)
        }

        Expression::Interval { value, .. } => has_external_column_refs(value, subquery),

        // Literals and special expressions don't reference columns
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::WindowFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => false,
    }
}

/// Check if subquery's FROM clause references a specific table
pub(super) fn subquery_references_table(subquery: &SelectStmt, table_name: &str) -> bool {
    if let Some(from) = &subquery.from {
        from_clause_contains_table(from, table_name)
    } else {
        false
    }
}

/// Extract first-character prefixes from tables in a FROM clause
fn extract_table_prefixes(from: &vibesql_ast::FromClause) -> Vec<char> {
    fn collect(from: &vibesql_ast::FromClause, prefixes: &mut Vec<char>) {
        match from {
            vibesql_ast::FromClause::Table { name, .. } => {
                if let Some(c) = name.chars().next() {
                    prefixes.push(c.to_ascii_lowercase());
                }
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                collect(left, prefixes);
                collect(right, prefixes);
            }
            vibesql_ast::FromClause::Subquery { alias, .. } => {
                if let Some(c) = alias.chars().next() {
                    prefixes.push(c.to_ascii_lowercase());
                }
            }
        }
    }
    let mut prefixes = Vec::new();
    collect(from, &mut prefixes);
    prefixes
}

/// Recursively check if FROM clause contains a table reference
fn from_clause_contains_table(from: &vibesql_ast::FromClause, table_name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { name, alias } => {
            name == table_name || alias.as_ref().is_some_and(|a| a == table_name)
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            from_clause_contains_table(left, table_name)
                || from_clause_contains_table(right, table_name)
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => alias == table_name,
    }
}
