//! Join graph construction and table reference analysis

use std::collections::HashSet;
use vibesql_ast::{Expression, FromClause, JoinType};

/// Information about a table extracted from a FROM clause
#[derive(Debug, Clone)]
pub(super) struct TableRef {
    pub(super) name: String,
    pub(super) alias: Option<String>,
    #[allow(dead_code)]
    pub(super) is_cte: bool,
    pub(super) is_subquery: bool,
    pub(super) subquery: Option<Box<vibesql_ast::SelectStmt>>,
}

/// Join condition with its associated join type
#[derive(Debug, Clone)]
pub(super) struct JoinConditionWithType {
    pub(super) condition: Expression,
    pub(super) join_type: JoinType,
}

/// Flatten a nested join tree into a list of table references
pub(super) fn flatten_join_tree(from: &FromClause, tables: &mut Vec<TableRef>) {
    match from {
        FromClause::Table { name, alias } => {
            tables.push(TableRef {
                name: name.clone(),
                alias: alias.clone(),
                is_cte: false,
                is_subquery: false,
                subquery: None,
            });
        }
        FromClause::Subquery { query, alias } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: true,
                subquery: Some(query.clone()),
            });
        }
        FromClause::Join { left, right, .. } => {
            flatten_join_tree(left, tables);
            flatten_join_tree(right, tables);
        }
    }
}

/// Extract all join conditions and WHERE predicates from a FROM clause
pub(super) fn extract_all_conditions(from: &FromClause, conditions: &mut Vec<Expression>) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        FromClause::Join { left, right, condition, .. } => {
            // Add this join's condition
            if let Some(cond) = condition {
                conditions.push(cond.clone());
            }
            // Recurse into nested joins
            extract_all_conditions(left, conditions);
            extract_all_conditions(right, conditions);
        }
    }
}

/// Extract all join conditions with their associated join types from a FROM clause
pub(super) fn extract_conditions_with_types(from: &FromClause, conditions: &mut Vec<JoinConditionWithType>) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        FromClause::Join { left, right, join_type, condition, .. } => {
            // Add this join's condition with its type
            if let Some(cond) = condition {
                conditions.push(JoinConditionWithType {
                    condition: cond.clone(),
                    join_type: join_type.clone(),
                });
            }
            // Recurse into nested joins
            extract_conditions_with_types(left, conditions);
            extract_conditions_with_types(right, conditions);
        }
    }
}

/// Extract all table names referenced in an expression
///
/// This function recursively walks an expression tree and collects all table names
/// mentioned in column references. Used to determine which tables a join condition connects.
/// Handles both explicit table qualifiers and infers tables from column name prefixes.
///
/// # Parameters
/// - `expr`: The expression to analyze
/// - `output`: HashSet to populate with referenced table names
/// - `available_tables`: Set of FROM clause tables (for inferring unqualified columns)
pub(super) fn extract_referenced_tables(
    expr: &Expression,
    output: &mut HashSet<String>,
    available_tables: &HashSet<String>,
) {
    match expr {
        Expression::ColumnRef { table: Some(table), .. } => {
            output.insert(table.to_lowercase());
        }
        Expression::ColumnRef { table: None, column } => {
            // Infer table from column name prefix by matching against FROM clause tables
            // This handles naming conventions where columns are prefixed with table name/initials
            // Example: C_CUSTKEY matches CUSTOMER, PS_PARTKEY matches PARTSUPP, emp_id matches employees

            // Extract prefix: everything before the first underscore
            let prefix = column
                .split('_')
                .next()
                .unwrap_or("");

            if !prefix.is_empty() {
                // Try to find a FROM clause table that starts with this prefix (case-insensitive)
                let prefix_upper = prefix.to_uppercase();

                // Try three matching strategies in order of specificity:
                // 1. Exact match (e.g., "P" == "P")
                // 2. Prefix match (e.g., "P" matches "PART")
                // 3. Abbreviation match (e.g., "PS" matches "partsupp")
                let mut exact_match: Option<String> = None;
                let mut prefix_matches = Vec::new();
                let mut abbrev_matches = Vec::new();

                for table in available_tables {
                    let table_upper = table.to_uppercase();
                    if table_upper == prefix_upper {
                        exact_match = Some(table.clone());
                        break;  // Exact match takes priority
                    } else if table_upper.starts_with(&prefix_upper) {
                        prefix_matches.push(table.clone());
                    } else {
                        // Check abbreviation match (e.g., "ps" → "partsupp")
                        let abbrev = super::utils::get_table_abbreviation(table);
                        if abbrev.to_uppercase() == prefix_upper {
                            abbrev_matches.push(table.clone());
                        }
                    }
                }

                if let Some(table) = exact_match {
                    output.insert(table);
                } else if !prefix_matches.is_empty() {
                    // Sort by length ascending to prefer shorter, more specific matches
                    // This ensures "PART" matches before "PARTSUPP" for prefix "P"
                    prefix_matches.sort_by_key(|t| t.len());
                    if let Some(table) = prefix_matches.into_iter().next() {
                        output.insert(table);
                    }
                } else if !abbrev_matches.is_empty() {
                    // Use abbreviation match as last resort
                    // Sort by length to prefer shorter names if multiple match
                    abbrev_matches.sort_by_key(|t| t.len());
                    if let Some(table) = abbrev_matches.into_iter().next() {
                        output.insert(table);
                    }
                }
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_referenced_tables(left, output, available_tables);
            extract_referenced_tables(right, output, available_tables);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_referenced_tables(arg, output, available_tables);
            }
        }
        Expression::InList { expr, values, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            for item in values {
                extract_referenced_tables(item, output, available_tables);
            }
        }
        Expression::Between { expr, low, high, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            extract_referenced_tables(low, output, available_tables);
            extract_referenced_tables(high, output, available_tables);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_referenced_tables(op, output, available_tables);
            }
            for clause in when_clauses {
                for condition in &clause.conditions {
                    extract_referenced_tables(condition, output, available_tables);
                }
                extract_referenced_tables(&clause.result, output, available_tables);
            }
            if let Some(else_res) = else_result {
                extract_referenced_tables(else_res, output, available_tables);
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        Expression::Cast { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        Expression::In { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            // Note: We don't traverse into subqueries as they reference different tables
        }
        Expression::Position { substring, string, .. } => {
            extract_referenced_tables(substring, output, available_tables);
            extract_referenced_tables(string, output, available_tables);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                extract_referenced_tables(char_expr, output, available_tables);
            }
            extract_referenced_tables(string, output, available_tables);
        }
        Expression::Like { expr, pattern, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            extract_referenced_tables(pattern, output, available_tables);
        }
        // For other expressions (literals, wildcards, subqueries, etc.), no direct column refs to extract
        _ => {}
    }
}
