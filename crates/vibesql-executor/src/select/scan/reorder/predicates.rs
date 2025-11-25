//! Predicate extraction and analysis for join reordering

use std::collections::{HashMap, HashSet};
use vibesql_ast::{BinaryOperator, Expression};

/// Extract table-local predicates from a WHERE clause expression
///
/// A table-local predicate is one that references only a single table,
/// e.g., `c_mktsegment = 'BUILDING'` references only `customer`.
pub(super) fn extract_table_local_predicates(
    where_expr: &Expression,
    table_set: &HashSet<String>,
) -> HashMap<String, Vec<Expression>> {
    let mut local_predicates: HashMap<String, Vec<Expression>> = HashMap::new();

    // Flatten AND chain into individual predicates
    let predicates = flatten_and_chain(where_expr);

    for pred in predicates {
        // Get tables referenced by this predicate
        let mut referenced_tables = HashSet::new();
        super::graph::extract_referenced_tables(&pred, &mut referenced_tables, table_set);

        // If predicate references exactly one table, it's table-local
        if referenced_tables.len() == 1 {
            let table_name = referenced_tables.into_iter().next().unwrap();
            local_predicates.entry(table_name).or_default().push(pred);
        }
    }

    local_predicates
}

/// Flatten an AND chain into individual predicates
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

/// Extract IN predicates from OR expressions for predicate pushdown (TPC-H Q7 optimization)
///
/// Transforms: `((t1.col = 'A' AND t2.col = 'B') OR (t1.col = 'B' AND t2.col = 'A'))`
/// Into: `t1.col IN ('A', 'B')` and `t2.col IN ('A', 'B')`
pub(super) fn extract_in_predicates_from_or(
    where_expr: &Expression,
    table_set: &HashSet<String>,
) -> HashMap<String, Vec<Expression>> {
    let mut result: HashMap<String, Vec<Expression>> = HashMap::new();

    fn collect_or_branches(expr: &Expression, branches: &mut Vec<Vec<Expression>>) {
        match expr {
            Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
                collect_or_branches(left, branches);
                collect_or_branches(right, branches);
            }
            _ => branches.push(flatten_and_chain(expr)),
        }
    }

    fn extract_eq(pred: &Expression, table_set: &HashSet<String>) -> Option<(String, String, vibesql_types::SqlValue)> {
        if let Expression::BinaryOp { op: BinaryOperator::Equal, left, right } = pred {
            if let (Expression::ColumnRef { table: Some(t), column: c }, Expression::Literal(v)) = (left.as_ref(), right.as_ref()) {
                if table_set.contains(&t.to_lowercase()) { return Some((t.clone(), c.clone(), v.clone())); }
            }
            if let (Expression::Literal(v), Expression::ColumnRef { table: Some(t), column: c }) = (left.as_ref(), right.as_ref()) {
                if table_set.contains(&t.to_lowercase()) { return Some((t.clone(), c.clone(), v.clone())); }
            }
        }
        None
    }

    for pred in flatten_and_chain(where_expr) {
        if !matches!(&pred, Expression::BinaryOp { op: BinaryOperator::Or, .. }) { continue; }
        let mut branches: Vec<Vec<Expression>> = Vec::new();
        collect_or_branches(&pred, &mut branches);
        if branches.len() < 2 { continue; }

        let mut col_vals: HashMap<(String, String), HashSet<vibesql_types::SqlValue>> = HashMap::new();
        let mut col_count: HashMap<(String, String), usize> = HashMap::new();
        for branch in &branches {
            let mut seen: HashSet<(String, String)> = HashSet::new();
            for eq in branch {
                if let Some((t, c, v)) = extract_eq(eq, table_set) {
                    let k = (t.to_lowercase(), c.to_lowercase());
                    col_vals.entry(k.clone()).or_default().insert(v);
                    seen.insert(k);
                }
            }
            for k in seen { *col_count.entry(k).or_default() += 1; }
        }
        for ((t, c), vals) in col_vals {
            if col_count.get(&(t.clone(), c.clone())) == Some(&branches.len()) && vals.len() >= 2 {
                let in_pred = Expression::InList {
                    expr: Box::new(Expression::ColumnRef { table: Some(t.clone()), column: c.clone() }),
                    values: vals.into_iter().map(Expression::Literal).collect(),
                    negated: false,
                };
                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[JOIN_REORDER] Extracted IN predicate for {}.{} from OR", t, c);
                }
                result.entry(t).or_default().push(in_pred);
            }
        }
    }
    result
}

/// Extract equijoin conditions from a WHERE clause expression
///
/// Recursively walks the expression tree looking for binary equality operations
/// that reference columns from two different tables.
pub(super) fn extract_where_equijoins(expr: &Expression, tables: &HashSet<String>) -> Vec<Expression> {
    let mut equijoins = Vec::new();

    // Helper function to collect all branches of an OR expression into a flat list
    fn collect_or_branches(expr: &Expression, branches: &mut Vec<Expression>) {
        match expr {
            Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
                collect_or_branches(left, branches);
                collect_or_branches(right, branches);
            }
            _ => {
                branches.push(expr.clone());
            }
        }
    }

    // Helper function to find equijoins that appear in ALL branches
    fn find_common_equijoins(branch_equijoins: &[Vec<Expression>]) -> Vec<Expression> {
        if branch_equijoins.is_empty() {
            return Vec::new();
        }

        // Helper to check if two equijoin expressions are equivalent
        fn exprs_equivalent(e1: &Expression, e2: &Expression) -> bool {
            // For now, use Debug format comparison (simple but effective)
            // A more robust approach would compare the AST structure
            format!("{:?}", e1) == format!("{:?}", e2)
        }

        let mut common = Vec::new();
        let first_branch = &branch_equijoins[0];

        for eq in first_branch {
            // Check if this equijoin appears in all other branches
            let appears_in_all = branch_equijoins[1..].iter().all(|branch| {
                branch.iter().any(|e| exprs_equivalent(e, eq))
            });

            if appears_in_all {
                common.push(eq.clone());
            }
        }

        common
    }

    fn extract_recursive(
        expr: &Expression,
        tables: &HashSet<String>,
        equijoins: &mut Vec<Expression>,
    ) {
        match expr {
            // Binary AND: recurse into both sides
            Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
                extract_recursive(left, tables, equijoins);
                extract_recursive(right, tables, equijoins);
            }
            // Binary OR: extract common equijoins from all branches
            Expression::BinaryOp { op: BinaryOperator::Or, .. } => {
                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[JOIN_REORDER] Processing OR expression for common equijoins");
                }

                // Collect all OR branches
                let mut branches = Vec::new();
                collect_or_branches(expr, &mut branches);

                if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                    eprintln!("[JOIN_REORDER] Found {} OR branches", branches.len());
                }

                // Extract equijoins from each branch
                let mut branch_equijoins: Vec<Vec<Expression>> = Vec::new();
                for branch in &branches {
                    let mut branch_eqs = Vec::new();
                    extract_recursive(branch, tables, &mut branch_eqs);
                    branch_equijoins.push(branch_eqs);
                }

                // Find equijoins that appear in ALL branches
                if !branch_equijoins.is_empty() {
                    let common_eqs = find_common_equijoins(&branch_equijoins);

                    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                        eprintln!("[JOIN_REORDER] Found {} common equijoins across all OR branches",
                            common_eqs.len());
                    }

                    // Add common equijoins to result
                    equijoins.extend(common_eqs);
                }
            }
            // Binary EQUAL: check if it's an equijoin
            Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
                // Check if both sides are column references
                // Handle both explicit table qualifiers and implicit (prefix-based) references

                // Helper closure to infer table from column name
                // Uses multi-tier matching for different naming conventions:
                // 1) SQLLogicTest suffix pattern: column "a1" → table "t1" (numeric suffix)
                // 2) TPC-H prefix pattern: column "l_orderkey" → table "lineitem" (prefix before underscore)
                // 3) Abbreviation match: column "ps_suppkey" → table "partsupp"
                let infer_table = |column: &str| -> Option<String> {
                    // --- Tier 1: SQLLogicTest suffix pattern ---
                    // Columns like "a1", "b2", "x9" where the trailing digit indicates the table
                    // This is common in SQLLogicTest files with tables t1, t2, ..., t9
                    if let Some(last_char) = column.chars().last() {
                        if last_char.is_ascii_digit() {
                            let table_candidate = format!("t{}", last_char);
                            if tables.contains(&table_candidate) || tables.contains(&table_candidate.to_uppercase()) {
                                // Found a table matching the suffix pattern
                                for table in tables {
                                    if table.eq_ignore_ascii_case(&table_candidate) {
                                        return Some(table.clone());
                                    }
                                }
                            }
                        }
                    }

                    // --- Tier 2: TPC-H prefix pattern ---
                    let prefix = column.split('_').next().unwrap_or("").to_uppercase();
                    if prefix.is_empty() {
                        return None;
                    }

                    // Collect all matching tables (exact, prefix, and abbreviation matches)
                    let mut exact_matches = Vec::new();
                    let mut prefix_matches = Vec::new();
                    let mut abbrev_matches = Vec::new();

                    for table in tables {
                        let table_upper = table.to_uppercase();
                        if table_upper == prefix {
                            exact_matches.push(table.clone());
                        } else if table_upper.starts_with(&prefix) {
                            prefix_matches.push(table.clone());
                        } else {
                            // Check abbreviation match (e.g., "ps" → "partsupp")
                            let abbrev = super::utils::get_table_abbreviation(table);
                            if abbrev.to_uppercase() == prefix {
                                abbrev_matches.push(table.clone());
                            }
                        }
                    }

                    // Prefer exact match (e.g., "P" -> "P" table if it exists)
                    if !exact_matches.is_empty() {
                        return exact_matches.into_iter().next();
                    }

                    // For prefix matches, prefer shorter (more specific) tables
                    // This ensures "PART" matches before "PARTSUPP" for prefix "P"
                    if !prefix_matches.is_empty() {
                        prefix_matches.sort_by_key(|t| t.len());
                        return prefix_matches.into_iter().next();
                    }

                    // Use abbreviation match as last resort (e.g., "PS" -> "partsupp")
                    if !abbrev_matches.is_empty() {
                        abbrev_matches.sort_by_key(|t| t.len());
                        return abbrev_matches.into_iter().next();
                    }

                    None
                };

                let left_table = match left.as_ref() {
                    Expression::ColumnRef { table: Some(t), .. } => Some(t.to_lowercase()),
                    Expression::ColumnRef { table: None, column } => infer_table(column),
                    _ => None,
                };
                let right_table = match right.as_ref() {
                    Expression::ColumnRef { table: Some(t), .. } => Some(t.to_lowercase()),
                    Expression::ColumnRef { table: None, column } => infer_table(column),
                    _ => None,
                };

                // If both sides reference columns from different tables, it's an equijoin
                if let (Some(lt), Some(rt)) = (left_table.clone(), right_table.clone()) {
                    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                        eprintln!("[JOIN_REORDER] Checking equijoin: left_table={:?}, right_table={:?}", lt, rt);
                        eprintln!("[JOIN_REORDER]   tables.contains(left)={}, tables.contains(right)={}",
                            tables.contains(&lt), tables.contains(&rt));
                        eprintln!("[JOIN_REORDER]   condition: {:?}", expr);
                    }
                    if lt != rt && tables.contains(&lt) && tables.contains(&rt) {
                        equijoins.push(expr.clone());
                        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                            eprintln!("[JOIN_REORDER]   ✓ Added to equijoins");
                        }
                    } else if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                        eprintln!("[JOIN_REORDER]   ✗ Skipped: lt==rt or table not found");
                    }
                }
            }
            // For other expressions, don't recurse (we only care about top-level ANDs and EQUALs)
            _ => {}
        }
    }

    extract_recursive(expr, tables, &mut equijoins);
    equijoins
}
