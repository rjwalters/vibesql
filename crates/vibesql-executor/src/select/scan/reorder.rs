//! Join reordering optimization
//!
//! Provides cost-based join reordering for multi-table queries:
//! - Analyzes join conditions and WHERE predicates
//! - Uses exhaustive search with pruning to find optimal join order
//! - Minimizes intermediate result sizes
//!
//! This optimization is enabled by default for 3-8 table INNER/CROSS joins.
//! Disabled for 9+ tables to prevent excessive search time (9! = 362,880).
//! Can be disabled via JOIN_REORDER_DISABLED environment variable.

use std::collections::{HashMap, HashSet};

use super::{derived::execute_derived_table, table::execute_table_scan, FromResult};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{
        cte::CteResult,
        join::{nested_loop_join, JoinOrderAnalyzer, JoinOrderSearch},
        SelectResult,
    },
};

/// Extract table-local predicates from a WHERE clause expression
///
/// A table-local predicate is one that references only a single table,
/// e.g., `c_mktsegment = 'BUILDING'` references only `customer`.
fn extract_table_local_predicates(
    where_expr: &vibesql_ast::Expression,
    table_set: &HashSet<String>,
) -> HashMap<String, Vec<vibesql_ast::Expression>> {
    let mut local_predicates: HashMap<String, Vec<vibesql_ast::Expression>> = HashMap::new();

    // Flatten AND chain into individual predicates
    let predicates = flatten_and_chain(where_expr);

    for pred in predicates {
        // Get tables referenced by this predicate
        let mut referenced_tables = HashSet::new();
        extract_referenced_tables(&pred, &mut referenced_tables, table_set);

        // If predicate references exactly one table, it's table-local
        if referenced_tables.len() == 1 {
            let table_name = referenced_tables.into_iter().next().unwrap();
            local_predicates.entry(table_name).or_default().push(pred);
        }
    }

    local_predicates
}

/// Flatten an AND chain into individual predicates
fn flatten_and_chain(expr: &vibesql_ast::Expression) -> Vec<vibesql_ast::Expression> {
    use vibesql_ast::{BinaryOperator, Expression};

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
fn extract_in_predicates_from_or(
    where_expr: &vibesql_ast::Expression,
    table_set: &HashSet<String>,
) -> HashMap<String, Vec<vibesql_ast::Expression>> {
    use vibesql_ast::{BinaryOperator, Expression};
    let mut result: HashMap<String, Vec<vibesql_ast::Expression>> = HashMap::new();

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

/// Check if join reordering optimization should be applied
///
/// Enabled by default for 2-8 table joins. Can be disabled via JOIN_REORDER_DISABLED env var.
///
/// Table count limits:
/// - < 2 tables: Not applicable (no join)
/// - 2-8 tables: Enabled (branch-and-bound pruning keeps search manageable)
/// - > 8 tables: Disabled (excessive search time: 9! = 362,880, 10! = 3,628,800)
///
/// The branch-and-bound search with cost-based pruning efficiently handles up to 8 tables
/// by eliminating suboptimal paths early. Even with 8! = 40,320 theoretical orderings,
/// pruning reduces the search space by orders of magnitude in practice.
///
/// 2-table joins benefit from choosing optimal build/probe sides, especially when one
/// table has highly selective predicates (e.g., TPC-H Q19's complex OR conditions).
pub(crate) fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 2 tables for reordering to be beneficial
    if table_count < 2 {
        return false;
    }

    // Limit to 8 tables maximum to prevent excessive search time
    // With 9+ tables, the search space becomes impractical (9! = 362,880, 10! = 3,628,800)
    // Even with aggressive pruning, the overhead becomes prohibitive
    if table_count > 8 {
        return false;
    }

    // Allow opt-out via environment variable if needed
    std::env::var("JOIN_REORDER_DISABLED").is_err()
}

/// Count the number of tables in a FROM clause (including nested joins)
pub(crate) fn count_tables_in_from(from: &vibesql_ast::FromClause) -> usize {
    match from {
        vibesql_ast::FromClause::Table { .. } => 1,
        vibesql_ast::FromClause::Subquery { .. } => 1,
        vibesql_ast::FromClause::Join { left, right, .. } => {
            count_tables_in_from(left) + count_tables_in_from(right)
        }
    }
}

/// Check if all joins in the tree are CROSS joins (comma-list syntax)
///
/// Join reordering changes column ordering, so we only apply it to implicit CROSS joins
/// from comma-list syntax (FROM t1, t2, t3). Explicit INNER/LEFT/RIGHT joins must
/// preserve their declared ordering.
pub(crate) fn all_joins_are_cross(from: &vibesql_ast::FromClause) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } | vibesql_ast::FromClause::Subquery { .. } => true,
        vibesql_ast::FromClause::Join { left, right, join_type, .. } => {
            matches!(join_type, vibesql_ast::JoinType::Cross)
                && all_joins_are_cross(left)
                && all_joins_are_cross(right)
        }
    }
}

/// Information about a table extracted from a FROM clause
#[derive(Debug, Clone)]
struct TableRef {
    name: String,
    alias: Option<String>,
    #[allow(dead_code)]
    is_cte: bool,
    is_subquery: bool,
    subquery: Option<Box<vibesql_ast::SelectStmt>>,
}

/// Flatten a nested join tree into a list of table references
fn flatten_join_tree(from: &vibesql_ast::FromClause, tables: &mut Vec<TableRef>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias } => {
            tables.push(TableRef {
                name: name.clone(),
                alias: alias.clone(),
                is_cte: false,
                is_subquery: false,
                subquery: None,
            });
        }
        vibesql_ast::FromClause::Subquery { query, alias } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: true,
                subquery: Some(query.clone()),
            });
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            flatten_join_tree(left, tables);
            flatten_join_tree(right, tables);
        }
    }
}

/// Extract all join conditions and WHERE predicates from a FROM clause
fn extract_all_conditions(from: &vibesql_ast::FromClause, conditions: &mut Vec<vibesql_ast::Expression>) {
    match from {
        vibesql_ast::FromClause::Table { .. } | vibesql_ast::FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
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
fn extract_referenced_tables(
    expr: &vibesql_ast::Expression,
    output: &mut HashSet<String>,
    available_tables: &HashSet<String>,
) {
    match expr {
        vibesql_ast::Expression::ColumnRef { table: Some(table), .. } => {
            output.insert(table.to_lowercase());
        }
        vibesql_ast::Expression::ColumnRef { table: None, column } => {
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

                // Collect exact and prefix matches
                let mut exact_match: Option<String> = None;
                let mut prefix_matches = Vec::new();

                for table in available_tables {
                    let table_upper = table.to_uppercase();
                    if table_upper == prefix_upper {
                        exact_match = Some(table.clone());
                        break;  // Exact match takes priority
                    } else if table_upper.starts_with(&prefix_upper) {
                        prefix_matches.push(table.clone());
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
                }
            }
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            extract_referenced_tables(left, output, available_tables);
            extract_referenced_tables(right, output, available_tables);
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        vibesql_ast::Expression::Function { args, .. } | vibesql_ast::Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_referenced_tables(arg, output, available_tables);
            }
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            for item in values {
                extract_referenced_tables(item, output, available_tables);
            }
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            extract_referenced_tables(low, output, available_tables);
            extract_referenced_tables(high, output, available_tables);
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
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
        vibesql_ast::Expression::IsNull { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        vibesql_ast::Expression::Cast { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
        }
        vibesql_ast::Expression::In { expr, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            // Note: We don't traverse into subqueries as they reference different tables
        }
        vibesql_ast::Expression::Position { substring, string, .. } => {
            extract_referenced_tables(substring, output, available_tables);
            extract_referenced_tables(string, output, available_tables);
        }
        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                extract_referenced_tables(char_expr, output, available_tables);
            }
            extract_referenced_tables(string, output, available_tables);
        }
        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            extract_referenced_tables(expr, output, available_tables);
            extract_referenced_tables(pattern, output, available_tables);
        }
        // For other expressions (literals, wildcards, subqueries, etc.), no direct column refs to extract
        _ => {}
    }
}

/// Extract equijoin conditions from a WHERE clause expression
///
/// Recursively walks the expression tree looking for binary equality operations
/// that reference columns from two different tables.
fn extract_where_equijoins(expr: &vibesql_ast::Expression, tables: &HashSet<String>) -> Vec<vibesql_ast::Expression> {
    use vibesql_ast::{BinaryOperator, Expression};

    let mut equijoins = Vec::new();

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
            // Binary EQUAL: check if it's an equijoin
            Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
                // Check if both sides are column references
                // Handle both explicit table qualifiers and implicit (prefix-based) references

                // Helper closure to infer table from column prefix
                let infer_table = |column: &str| -> Option<String> {
                    let prefix = column.split('_').next().unwrap_or("").to_uppercase();
                    if prefix.is_empty() {
                        return None;
                    }

                    // Collect all matching tables (both exact and prefix matches)
                    let mut exact_matches = Vec::new();
                    let mut prefix_matches = Vec::new();

                    for table in tables {
                        let table_upper = table.to_uppercase();
                        if table_upper == prefix {
                            exact_matches.push(table.clone());
                        } else if table_upper.starts_with(&prefix) {
                            prefix_matches.push(table.clone());
                        }
                    }

                    // Prefer exact match (e.g., "P" -> "PART" even if "PARTSUPP" exists)
                    if !exact_matches.is_empty() {
                        return exact_matches.into_iter().next();
                    }

                    // For prefix matches, prefer shorter (more specific) tables
                    // This ensures "PART" matches before "PARTSUPP" for prefix "P"
                    if !prefix_matches.is_empty() {
                        prefix_matches.sort_by_key(|t| t.len());
                        return prefix_matches.into_iter().next();
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
                if let (Some(lt), Some(rt)) = (left_table, right_table) {
                    if lt != rt && tables.contains(&lt) && tables.contains(&rt) {
                        equijoins.push(expr.clone());
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

/// Apply join reordering optimization to a multi-table join
///
/// This function:
/// 1. Flattens the join tree to extract all tables
/// 2. Analyzes join conditions and WHERE predicates
/// 3. Uses cost-based search to find optimal join order
/// 4. Builds and executes joins in the optimal order
/// 5. Restores original column ordering to preserve query semantics
pub(crate) fn execute_with_join_reordering<F>(
    from: &vibesql_ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError> + Copy,
{
    // Step 1: Flatten join tree to extract all tables
    let mut table_refs = Vec::new();
    flatten_join_tree(from, &mut table_refs);

    // Step 2: Extract all join conditions
    let mut join_conditions = Vec::new();
    extract_all_conditions(from, &mut join_conditions);

    // Step 3: Build analyzer with table names (preserving original order)
    let table_names: Vec<String> =
        table_refs.iter().map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone())).collect();

    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(table_names.clone());

    // Combine table names into a set for predicate analysis (normalize to lowercase)
    let table_set: HashSet<String> = table_names.iter().map(|t| t.to_lowercase()).collect();

    // Step 4: Analyze join conditions to extract edges
    for condition in &join_conditions {
        analyzer.analyze_predicate(condition, &table_set);
    }

    // Step 5: Analyze WHERE clause predicates if available
    // Also extract WHERE clause equijoins for join execution
    let where_equijoins = if let Some(where_expr) = where_clause {
        analyzer.analyze_predicate(where_expr, &table_set);

        // Debug logging
        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] WHERE clause present: {:?}", where_expr);
            eprintln!("[JOIN_REORDER] Table set: {:?}", table_set);
        }

        // Extract equijoin conditions from WHERE clause manually
        // This is simpler and more reliable than using decompose_where_clause,
        // which requires a full schema with column information
        let equijoins = extract_where_equijoins(where_expr, &table_set);

        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] Extracted {} WHERE equijoins", equijoins.len());
        }

        equijoins
    } else {
        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] No WHERE clause");
        }
        Vec::new()
    };

    // Step 6: Add WHERE equijoins to join_conditions for execution
    // This ensures WHERE clause equijoins are used during join execution, not just for optimization
    join_conditions.extend(where_equijoins);

    // Step 6.5: Extract table-local predicates for cardinality estimation
    let mut table_local_predicates = if let Some(where_expr) = where_clause {
        extract_table_local_predicates(where_expr, &table_set)
    } else {
        HashMap::new()
    };

    // Also extract IN predicates from OR expressions (TPC-H Q7 optimization)
    if let Some(where_expr) = where_clause {
        for (table, preds) in extract_in_predicates_from_or(where_expr, &table_set) {
            table_local_predicates.entry(table).or_default().extend(preds);
        }
    }

    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && !table_local_predicates.is_empty() {
        eprintln!("[JOIN_REORDER] Table-local predicates: {:?}",
            table_local_predicates.keys().collect::<Vec<_>>());
    }

    // Step 7: Use search to find optimal join order (with real statistics + selectivity)
    let search = JoinOrderSearch::from_analyzer_with_predicates(&analyzer, database, &table_local_predicates);
    let optimal_order = search.find_optimal_order();

    // Log the reordering decision (optional, for debugging)
    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!("[JOIN_REORDER] Original order: {:?}", table_names);
        eprintln!("[JOIN_REORDER] Optimal order:  {:?}", optimal_order);
        eprintln!("[JOIN_REORDER] Join conditions (including WHERE equijoins): {}", join_conditions.len());
    }

    // Step 8: Build a map from table name to TableRef for easy lookup
    // IMPORTANT: Normalize keys to lowercase to match analyzer's normalization
    let table_map: HashMap<String, TableRef> = table_refs
        .into_iter()
        .map(|t| {
            let key = t.alias.clone().unwrap_or_else(|| t.name.clone()).to_lowercase();
            (key, t)
        })
        .collect();

    // Step 9: Track column count per table for later column reordering
    let mut table_column_counts: HashMap<String, usize> = HashMap::new();

    // Step 10: Execute tables in optimal order, joining them sequentially
    let mut result: Option<super::FromResult> = None;
    let mut joined_tables: HashSet<String> = HashSet::new();
    let mut applied_conditions: HashSet<usize> = HashSet::new();

    for table_name in &optimal_order {
        let table_ref = table_map.get(table_name).ok_or_else(|| {
            ExecutorError::UnsupportedFeature(format!("Table not found in map: {}", table_name))
        })?;

        // Execute this table
        let table_result = if table_ref.is_subquery {
            if let Some(subquery) = &table_ref.subquery {
                execute_derived_table(subquery, table_name, execute_subquery)?
            } else {
                return Err(ExecutorError::UnsupportedFeature(
                    "Subquery reference missing query".to_string(),
                ));
            }
        } else {
            execute_table_scan(&table_ref.name, table_ref.alias.as_ref(), cte_results, database, where_clause, None, outer_row, outer_schema)?
        };

        // Record the column count for this table (using table_schemas to get column info)
        let col_count = if let Some((_, schema)) = table_result.schema.table_schemas.get(table_name) {
            schema.columns.len()
        } else {
            table_result.schema.total_columns
        };
        table_column_counts.insert(table_name.clone(), col_count);

        // Join with previous result (if any)
        if let Some(prev_result) = result {
            // Extract join conditions that connect this table to already-joined tables
            let mut applicable_conditions: Vec<vibesql_ast::Expression> = Vec::new();

            for (idx, condition) in join_conditions.iter().enumerate() {
                // Skip conditions we've already applied
                if applied_conditions.contains(&idx) {
                    continue;
                }

                // Extract tables referenced in this condition
                let mut referenced_tables = HashSet::new();
                extract_referenced_tables(condition, &mut referenced_tables, &table_set);

                // Check if condition connects the new table with any already-joined table
                // Condition is applicable if it references the new table AND at least one joined table
                let references_new_table = referenced_tables.contains(&table_name.to_lowercase());
                let references_joined_table = referenced_tables.iter().any(|t| joined_tables.contains(t));

                if references_new_table && references_joined_table {
                    applicable_conditions.push(condition.clone());
                    applied_conditions.insert(idx);
                }
            }

            // Debug logging for applicable conditions
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!("[JOIN_REORDER] Joining {} to {:?}, found {} applicable conditions",
                    table_name, joined_tables, applicable_conditions.len());
            }

            // Always use INNER join for comma-list joins, even when applicable_conditions is empty.
            // This allows nested_loop_join to find equijoins from WHERE clause and use hash join.
            // Using CROSS join would trigger memory limit checks for large Cartesian products.
            let join_type = &vibesql_ast::JoinType::Inner;

            result = Some(nested_loop_join(
                prev_result,
                table_result,
                join_type,
                &None, // No ON condition (using additional_equijoins instead)
                false, // Not a NATURAL JOIN
                database,
                &applicable_conditions, // Pass only the applicable conditions for this join
            )?);
        } else {
            result = Some(table_result);
        }

        // Mark this table as joined
        joined_tables.insert(table_name.to_lowercase());
    }

    let result = result.ok_or_else(|| ExecutorError::UnsupportedFeature("No tables in join".to_string()))?;

    // Step 11: Restore original column ordering if needed
    // Build column permutation: map from current position to target position
    let column_permutation = build_column_permutation(&table_names, &optimal_order, &table_column_counts);

    // Reorder rows according to the permutation
    let rows = result.data.into_rows();
    let reordered_rows: Vec<vibesql_storage::Row> = rows
        .into_iter()
        .map(|row| {
            let mut new_values = Vec::with_capacity(row.values.len());
            for &idx in &column_permutation {
                new_values.push(row.values[idx].clone());
            }
            vibesql_storage::Row::new(new_values)
        })
        .collect();

    // Build a new combined schema with tables in original order
    let new_schema = build_reordered_schema(&result.schema, &table_names, &optimal_order);

    // Return result with reordered data and schema
    Ok(FromResult::from_rows(new_schema, reordered_rows))
}

/// Build a reordered combined schema with tables in original order
///
/// Takes the current schema (with tables in optimal order) and reconstructs it
/// with tables in the original FROM clause order.
fn build_reordered_schema(
    current_schema: &CombinedSchema,
    original_order: &[String],
    _optimal_order: &[String],
) -> CombinedSchema {
    let mut new_table_schemas = HashMap::new();
    let mut current_position = 0;

    // Walk through original order and rebuild schema with correct positions
    for table_name in original_order {
        let table_lower = table_name.to_lowercase();

        // Find this table's schema in the current (optimally ordered) schema
        // Try exact match first, then case-insensitive
        let table_schema = current_schema
            .table_schemas
            .get(table_name)
            .or_else(|| {
                current_schema.table_schemas.iter().find_map(|(k, v): (&String, &(usize, vibesql_catalog::TableSchema))| {
                    if k.to_lowercase() == table_lower {
                        Some(v)
                    } else {
                        None
                    }
                })
            })
            .map(|(_, schema): &(usize, vibesql_catalog::TableSchema)| schema.clone());

        if let Some(schema) = table_schema {
            let col_count = schema.columns.len();
            new_table_schemas.insert(table_name.clone(), (current_position, schema));
            current_position += col_count;
        }
    }

    CombinedSchema { table_schemas: new_table_schemas, total_columns: current_position }
}

/// Build a column permutation to restore original table ordering
///
/// Given:
/// - Original table order: [tab0, tab2, tab1]
/// - Optimal execution order: [tab1, tab0, tab2]
/// - Column counts: {tab0: 3, tab1: 3, tab2: 3}
///
/// Returns permutation mapping current positions to original positions:
/// - Current: [tab1.col0, tab1.col1, tab1.col2, tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2]
/// - Target:  [tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2, tab1.col0, tab1.col1, tab1.col2]
/// - Permutation: [3, 4, 5, 6, 7, 8, 0, 1, 2]
fn build_column_permutation(
    original_order: &[String],
    optimal_order: &[String],
    column_counts: &HashMap<String, usize>,
) -> Vec<usize> {
    // Build position map: table name -> starting column index in optimal order
    let mut optimal_positions: HashMap<String, usize> = HashMap::new();
    let mut current_position = 0;
    for table in optimal_order {
        optimal_positions.insert(table.clone(), current_position);
        current_position += column_counts.get(table).unwrap_or(&0);
    }

    // Build permutation by walking through original order
    let mut permutation = Vec::new();
    for table in original_order {
        let table_lower = table.to_lowercase();
        let start_pos = optimal_positions.get(&table_lower).unwrap_or(&0);
        let col_count = column_counts.get(&table_lower).unwrap_or(&0);

        // Add all column indices for this table
        for i in 0..*col_count {
            permutation.push(start_pos + i);
        }
    }

    permutation
}
