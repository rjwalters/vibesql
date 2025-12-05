//! Unused table elimination optimizer pass
//!
//! Detects and eliminates tables from FROM clauses that:
//! 1. Have no columns in the SELECT list
//! 2. Have no equijoin predicates with other tables
//! 3. Only have self-filters in WHERE
//!
//! These tables create expensive cross joins that multiply result rows
//! without providing useful data. They are converted to EXISTS checks.
//!
//! This pass runs BEFORE semi-join transformation to avoid complex
//! interactions with derived tables from EXISTS/IN transformations.

use std::collections::{HashMap, HashSet};
use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType, SelectItem, SelectStmt};

/// Apply table elimination optimization to a SELECT statement
///
/// Returns a new statement with eliminable tables removed from FROM
/// and converted to EXISTS checks in WHERE
///
/// Can be disabled with TABLE_ELIM_DISABLED environment variable
pub fn eliminate_unused_tables(stmt: &SelectStmt) -> SelectStmt {
    // Check if optimization is disabled
    if std::env::var("TABLE_ELIM_DISABLED").is_ok() {
        return stmt.clone();
    }

    let verbose = std::env::var("TABLE_ELIM_VERBOSE").is_ok();

    // Must have FROM clause
    let from = match &stmt.from {
        Some(f) => f,
        None => return stmt.clone(),
    };

    // Extract tables from FROM clause
    let mut tables = Vec::new();
    flatten_from_clause(from, &mut tables);

    if verbose {
        eprintln!(
            "[TABLE_ELIM_OPT] Analyzing {} tables: {:?}",
            tables.len(),
            tables.iter().map(|t| &t.name).collect::<Vec<_>>()
        );
    }

    // Need at least 2 tables (keep at least 1, eliminate at least 1)
    if tables.len() < 2 {
        return stmt.clone();
    }

    // Don't apply to subqueries (SELECT 1 FROM ...) - these are intentional
    let is_select_literal = stmt.select_list.len() == 1
        && matches!(
            &stmt.select_list[0],
            SelectItem::Expression { expr: Expression::Literal(_), .. }
        );
    if is_select_literal {
        if verbose {
            eprintln!("[TABLE_ELIM_OPT] Skipping: SELECT literal subquery");
        }
        return stmt.clone();
    }

    // Build table name set
    let table_names: HashSet<String> =
        tables.iter().map(|t| t.alias.as_ref().unwrap_or(&t.name).to_lowercase()).collect();

    // Don't apply when query has global aggregates (like COUNT(*)) without GROUP BY.
    // Such aggregates operate over the entire Cartesian product, so eliminating tables
    // would change the result (e.g., COUNT(*) on cross join should count all product rows).
    if stmt.group_by.is_none() && has_global_aggregates(&stmt.select_list, &table_names) {
        if verbose {
            eprintln!("[TABLE_ELIM_OPT] Skipping: query has global aggregates without GROUP BY");
        }
        return stmt.clone();
    }

    // Collect unqualified column names from SELECT for later checking
    let unqualified_columns = collect_unqualified_columns(&stmt.select_list);

    if verbose && !unqualified_columns.is_empty() {
        eprintln!(
            "[TABLE_ELIM_OPT] Found {} unqualified columns in SELECT: {:?}",
            unqualified_columns.len(),
            unqualified_columns.iter().take(5).collect::<Vec<_>>()
        );
    }

    // Build column prefix mapping from qualified refs in the entire query
    // E.g., if we see `date_dim.d_year`, we know `date_dim` columns start with `d_`
    let table_column_prefixes = build_column_prefix_map(stmt, &table_names);

    if verbose && !table_column_prefixes.is_empty() {
        eprintln!("[TABLE_ELIM_OPT] Column prefixes: {:?}", table_column_prefixes);
    }

    // Find tables referenced in SELECT list
    let select_tables = extract_tables_from_select(&stmt.select_list, &table_names);

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Tables in SELECT: {:?}", select_tables);
    }

    // Safety check: If there are unqualified columns that don't match ANY known table prefix,
    // we can't reliably determine which table they belong to. Skip optimization to be safe.
    // This handles CTEs and other cases where derived prefixes don't match actual column names.
    if !unqualified_columns.is_empty() {
        let known_prefixes: Vec<_> = table_column_prefixes.values().collect();
        let has_unknown_columns = unqualified_columns.iter().any(|col| {
            let col_lower = col.to_lowercase();
            !known_prefixes.iter().any(|prefix| col_lower.starts_with(*prefix))
        });
        if has_unknown_columns {
            if verbose {
                eprintln!(
                    "[TABLE_ELIM_OPT] Skipping: unqualified columns don't match any known prefix"
                );
            }
            return stmt.clone();
        }
    }

    // Find tables in equijoins (WHERE clause predicates AND JOIN ON conditions)
    // Uses prefix matching to detect joins with unqualified column refs
    let mut equijoin_tables = if let Some(where_expr) = &stmt.where_clause {
        extract_equijoin_tables(where_expr, &table_names, &table_column_prefixes)
    } else {
        HashSet::new()
    };

    // Also extract tables from JOIN ON conditions in the FROM clause (#3572)
    // This ensures we don't eliminate tables that are part of explicit JOIN conditions
    if let Some(from_clause) = &stmt.from {
        let on_condition_tables =
            extract_equijoin_tables_from_joins(from_clause, &table_names, &table_column_prefixes);
        equijoin_tables.extend(on_condition_tables);
    }

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Tables in equijoins: {:?}", equijoin_tables);
    }

    // Find local predicates per table (using prefix matching for unqualified columns)
    let local_predicates = if let Some(where_expr) = &stmt.where_clause {
        extract_local_predicates(where_expr, &table_names, &table_column_prefixes)
    } else {
        HashMap::new()
    };

    // Classify tables
    let mut eliminated = Vec::new();
    let mut kept_tables = Vec::new();

    for table in tables {
        let table_key = table.alias.as_ref().unwrap_or(&table.name).to_lowercase();

        // Check if table is referenced by qualified columns in SELECT
        let in_select_qualified = select_tables.contains(&table_key);

        // Check if table might be referenced by unqualified columns in SELECT
        // Using prefix matching: if table has known prefix (e.g., "d_" for date_dim)
        // check if any unqualified column matches
        let in_select_unqualified = if !unqualified_columns.is_empty() {
            if let Some(prefix) = table_column_prefixes.get(&table_key) {
                // We know this table's column prefix - check for matches
                let matches_prefix =
                    unqualified_columns.iter().any(|col| col.to_lowercase().starts_with(prefix));
                if verbose && matches_prefix {
                    eprintln!(
                        "[TABLE_ELIM_OPT] Table '{}' might be referenced by unqualified cols (prefix '{}')",
                        table_key, prefix
                    );
                }
                matches_prefix
            } else {
                // No prefix known for this table - conservatively assume it might be used
                // unless it's a common pattern (table has qualified refs but no matching unqualified)
                if verbose {
                    eprintln!(
                        "[TABLE_ELIM_OPT] Table '{}' has no known prefix, conservatively keeping",
                        table_key
                    );
                }
                true
            }
        } else {
            false
        };

        let in_select = in_select_qualified || in_select_unqualified;
        let in_equijoin = equijoin_tables.contains(&table_key);

        if verbose {
            eprintln!(
                "[TABLE_ELIM_OPT] Table '{}': in_select={} (qualified={}, unqualified={}), in_equijoin={}",
                table_key, in_select, in_select_qualified, in_select_unqualified, in_equijoin
            );
        }

        // Table can be eliminated if:
        // 1. Not in SELECT list
        // 2. Not in any equijoin condition
        // 3. HAS a local predicate/filter (otherwise it's an intentional cross join
        //    that multiplies rows, and we must preserve that row count)
        let filter = local_predicates.get(&table_key).cloned();
        if !in_select && !in_equijoin && filter.is_some() {
            if verbose {
                eprintln!(
                    "[TABLE_ELIM_OPT] ✓ Eliminating table '{}' with filter: {:?}",
                    table_key, filter
                );
            }
            eliminated.push(EliminatedTable {
                name: table.name.clone(),
                alias: table.alias.clone(),
                filter,
            });
        } else {
            if verbose && !in_select && !in_equijoin && filter.is_none() {
                eprintln!(
                    "[TABLE_ELIM_OPT] ✗ Keeping table '{}': no filter (cross join multiplies rows)",
                    table_key
                );
            }
            kept_tables.push(table);
        }
    }

    // If no tables eliminated, return unchanged
    if eliminated.is_empty() {
        return stmt.clone();
    }

    // If ALL tables would be eliminated, return unchanged.
    // Eliminating all tables would leave no FROM clause, causing incorrect
    // semantics for WHERE clauses that evaluate to FALSE (like NULL IS NOT NULL).
    if kept_tables.is_empty() {
        if verbose {
            eprintln!(
                "[TABLE_ELIM_OPT] Skipping: would eliminate all tables, leaving no FROM clause"
            );
        }
        return stmt.clone();
    }

    // Build new FROM clause without eliminated tables
    let new_from = rebuild_from_clause(&kept_tables);

    // Build EXISTS checks for eliminated tables
    let exists_checks = build_exists_checks(&eliminated);

    // Build eliminated table names set
    let eliminated_names: HashSet<String> =
        eliminated.iter().map(|t| t.alias.as_ref().unwrap_or(&t.name).to_lowercase()).collect();

    // Build prefixes for eliminated tables
    let eliminated_prefixes: HashSet<String> =
        eliminated_names.iter().filter_map(|t| table_column_prefixes.get(t).cloned()).collect();

    // Remove eliminated table predicates from WHERE
    let filtered_where = if let Some(where_expr) = &stmt.where_clause {
        remove_eliminated_predicates(where_expr, &eliminated_names, &eliminated_prefixes)
    } else {
        None
    };

    // Add EXISTS checks to WHERE
    let new_where = add_exists_to_where(filtered_where.as_ref(), exists_checks);

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Applied elimination: {} tables removed", eliminated.len());
    }

    // Return modified statement
    SelectStmt {
        with_clause: stmt.with_clause.clone(),
        distinct: stmt.distinct,
        select_list: stmt.select_list.clone(),
        into_table: stmt.into_table.clone(),
        into_variables: stmt.into_variables.clone(),
        from: new_from,
        where_clause: new_where,
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
        order_by: stmt.order_by.clone(),
        limit: stmt.limit,
        offset: stmt.offset,
        set_operation: stmt.set_operation.clone(),
    }
}

/// Info about a table in FROM clause
#[derive(Debug, Clone)]
struct TableInfo {
    name: String,
    alias: Option<String>,
}

/// Info about an eliminated table
#[derive(Debug)]
struct EliminatedTable {
    name: String,
    alias: Option<String>,
    filter: Option<Expression>,
}

/// Flatten FROM clause into list of tables (simple tables only, not subqueries)
fn flatten_from_clause(from: &FromClause, tables: &mut Vec<TableInfo>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push(TableInfo { name: name.clone(), alias: alias.clone() });
        }
        FromClause::Join { left, right, .. } => {
            flatten_from_clause(left, tables);
            flatten_from_clause(right, tables);
        }
        // Skip subqueries - they can't be eliminated
        FromClause::Subquery { .. } => {}
    }
}

/// Extract tables referenced in SELECT list (only qualified references)
///
/// Only extracts tables that are explicitly qualified in column references.
/// Unqualified columns are handled separately using prefix matching.
fn extract_tables_from_select(
    select_list: &[SelectItem],
    table_names: &HashSet<String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();

    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } => {
                // SELECT * references ALL tables
                tables.extend(table_names.iter().cloned());
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                tables.insert(qualifier.to_lowercase());
            }
            SelectItem::Expression { expr, .. } => {
                // Only extract qualified column references
                extract_tables_from_expr(expr, &mut tables);
            }
        }
    }

    tables
}

/// Check if the SELECT list contains "global" aggregate functions.
///
/// A global aggregate is one that:
/// 1. Is an aggregate function (COUNT, SUM, MIN, MAX, AVG, etc.)
/// 2. Does NOT reference any specific table columns (e.g., COUNT(*), MIN(42))
///
/// When such aggregates exist without GROUP BY, they operate over the entire
/// result set (including Cartesian products from cross joins). Eliminating tables
/// would incorrectly reduce the number of rows being aggregated.
fn has_global_aggregates(select_list: &[SelectItem], table_names: &HashSet<String>) -> bool {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if expr_has_global_aggregate(expr, table_names) {
                return true;
            }
        }
    }
    false
}

/// Recursively check if an expression contains a global aggregate
fn expr_has_global_aggregate(expr: &Expression, _table_names: &HashSet<String>) -> bool {
    match expr {
        Expression::AggregateFunction { args, .. } => {
            // Check if this aggregate references any table columns
            let mut referenced_tables = HashSet::new();
            for arg in args {
                extract_tables_from_expr(arg, &mut referenced_tables);
            }
            // Also check for unqualified column references
            let mut has_column_ref = false;
            for arg in args {
                if has_any_column_ref(arg) {
                    has_column_ref = true;
                    break;
                }
            }
            // Global if no table refs AND no column refs (e.g., COUNT(*) or MIN(42))
            referenced_tables.is_empty() && !has_column_ref
        }
        Expression::BinaryOp { left, right, .. } => {
            expr_has_global_aggregate(left, _table_names)
                || expr_has_global_aggregate(right, _table_names)
        }
        Expression::UnaryOp { expr, .. } => expr_has_global_aggregate(expr, _table_names),
        Expression::Function { args, .. } => {
            args.iter().any(|a| expr_has_global_aggregate(a, _table_names))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| expr_has_global_aggregate(o, _table_names))
                || when_clauses.iter().any(|c| {
                    c.conditions.iter().any(|cond| expr_has_global_aggregate(cond, _table_names))
                        || expr_has_global_aggregate(&c.result, _table_names)
                })
                || else_result.as_ref().is_some_and(|e| expr_has_global_aggregate(e, _table_names))
        }
        _ => false,
    }
}

/// Check if expression contains any column reference (qualified or unqualified)
/// Note: The special "*" wildcard (used in COUNT(*)) is NOT considered a real column reference
fn has_any_column_ref(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => {
            // The special "*" wildcard in COUNT(*) is not a real column reference
            column != "*"
        }
        Expression::BinaryOp { left, right, .. } => {
            has_any_column_ref(left) || has_any_column_ref(right)
        }
        Expression::UnaryOp { expr, .. } => has_any_column_ref(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(has_any_column_ref)
        }
        Expression::Cast { expr, .. } => has_any_column_ref(expr),
        _ => false,
    }
}

/// Check if expression contains any unqualified column references
fn has_unqualified_column_ref(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { table: None, .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            has_unqualified_column_ref(left) || has_unqualified_column_ref(right)
        }
        Expression::UnaryOp { expr, .. } => has_unqualified_column_ref(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(has_unqualified_column_ref)
        }
        Expression::InList { expr, values, .. } => {
            has_unqualified_column_ref(expr) || values.iter().any(has_unqualified_column_ref)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| has_unqualified_column_ref(o))
                || when_clauses.iter().any(|c| {
                    c.conditions.iter().any(has_unqualified_column_ref)
                        || has_unqualified_column_ref(&c.result)
                })
                || else_result.as_ref().is_some_and(|e| has_unqualified_column_ref(e))
        }
        Expression::IsNull { expr, .. } => has_unqualified_column_ref(expr),
        Expression::Cast { expr, .. } => has_unqualified_column_ref(expr),
        _ => false,
    }
}

/// Extract tables referenced in an expression (only qualified column refs)
fn extract_tables_from_expr(expr: &Expression, tables: &mut HashSet<String>) {
    match expr {
        Expression::ColumnRef { table: Some(t), .. } => {
            tables.insert(t.to_lowercase());
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_tables_from_expr(left, tables);
            extract_tables_from_expr(right, tables);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_tables_from_expr(arg, tables);
            }
        }
        Expression::InList { expr, values, .. } => {
            extract_tables_from_expr(expr, tables);
            for v in values {
                extract_tables_from_expr(v, tables);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_tables_from_expr(op, tables);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    extract_tables_from_expr(cond, tables);
                }
                extract_tables_from_expr(&clause.result, tables);
            }
            if let Some(else_res) = else_result {
                extract_tables_from_expr(else_res, tables);
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        Expression::Cast { expr, .. } => {
            extract_tables_from_expr(expr, tables);
        }
        _ => {}
    }
}

/// Extract tables that participate in equijoin conditions from JOIN ON clauses (#3572)
///
/// Recursively walks the FROM clause tree and extracts tables referenced in ON conditions.
/// This ensures tables joined via explicit ON conditions are not incorrectly eliminated.
fn extract_equijoin_tables_from_joins(
    from: &FromClause,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    extract_join_on_tables(from, &mut tables, table_names, table_prefixes);
    tables
}

fn extract_join_on_tables(
    from: &FromClause,
    tables: &mut HashSet<String>,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) {
    match from {
        FromClause::Table { .. } => {
            // Leaf node - no ON conditions
        }
        FromClause::Join { left, right, condition, .. } => {
            // Recursively process left and right subtrees
            extract_join_on_tables(left, tables, table_names, table_prefixes);
            extract_join_on_tables(right, tables, table_names, table_prefixes);

            // Extract tables from this join's ON condition
            if let Some(cond) = condition {
                let on_tables = extract_equijoin_tables(cond, table_names, table_prefixes);
                tables.extend(on_tables);
            }
        }
        FromClause::Subquery { .. } => {
            // Subqueries are opaque - don't examine their internals
        }
    }
}

/// Extract tables that participate in equijoin conditions
fn extract_equijoin_tables(
    expr: &Expression,
    table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    find_equijoin_tables(expr, &mut tables, table_names, table_prefixes);
    tables
}

fn find_equijoin_tables(
    expr: &Expression,
    tables: &mut HashSet<String>,
    _table_names: &HashSet<String>,
    table_prefixes: &HashMap<String, String>,
) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            find_equijoin_tables(left, tables, _table_names, table_prefixes);
            find_equijoin_tables(right, tables, _table_names, table_prefixes);
        }
        // Also recurse into OR branches to find equijoins
        // This is critical for queries like TPC-H Q19 where the join condition
        // (p_partkey = l_partkey) appears inside multiple OR branches
        Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
            find_equijoin_tables(left, tables, _table_names, table_prefixes);
            find_equijoin_tables(right, tables, _table_names, table_prefixes);
        }
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Check if this is a join between two tables
            let mut left_tables = HashSet::new();
            let mut right_tables = HashSet::new();
            extract_tables_from_expr(left, &mut left_tables);
            extract_tables_from_expr(right, &mut right_tables);

            // Check if either side has unqualified column references
            let left_has_unqualified = has_unqualified_column_ref(left);
            let right_has_unqualified = has_unqualified_column_ref(right);

            // For unqualified columns, try to determine their tables via prefix matching
            if left_has_unqualified && left_tables.is_empty() {
                let left_cols = collect_unqualified_columns_from_expr_single(left);
                for col in left_cols {
                    let col_lower = col.to_lowercase();
                    for (table, prefix) in table_prefixes {
                        if col_lower.starts_with(prefix) {
                            left_tables.insert(table.clone());
                        }
                    }
                }
            }
            if right_has_unqualified && right_tables.is_empty() {
                let right_cols = collect_unqualified_columns_from_expr_single(right);
                for col in right_cols {
                    let col_lower = col.to_lowercase();
                    for (table, prefix) in table_prefixes {
                        if col_lower.starts_with(prefix) {
                            right_tables.insert(table.clone());
                        }
                    }
                }
            }

            // It's an equijoin if both sides reference different tables
            // (via qualified refs OR prefix-matched unqualified refs)
            if !left_tables.is_empty()
                && !right_tables.is_empty()
                && left_tables.is_disjoint(&right_tables)
            {
                tables.extend(left_tables);
                tables.extend(right_tables);
            }
        }
        _ => {}
    }
}

/// Collect unqualified columns from a single expression (helper)
fn collect_unqualified_columns_from_expr_single(expr: &Expression) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_unqualified_columns_from_expr(expr, &mut cols);
    cols
}

/// Extract local predicates per table
///
/// Uses both qualified column references and prefix matching for unqualified columns
fn extract_local_predicates(
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

            // Skip if this looks like a join condition (qualified + unqualified on different tables)
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
fn combine_predicates(predicates: Vec<Expression>) -> Expression {
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

/// Rebuild FROM clause from kept tables
fn rebuild_from_clause(tables: &[TableInfo]) -> Option<FromClause> {
    if tables.is_empty() {
        return None;
    }

    let mut iter = tables.iter();
    let first = iter.next()?;
    let mut result = FromClause::Table {
        name: first.name.clone(),
        alias: first.alias.clone(),
        column_aliases: None,
    };

    for table in iter {
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(FromClause::Table {
                name: table.name.clone(),
                alias: table.alias.clone(),
                column_aliases: None,
            }),
            join_type: JoinType::Cross,
            condition: None,
            natural: false,
        };
    }

    Some(result)
}

/// Build EXISTS checks for eliminated tables
fn build_exists_checks(eliminated: &[EliminatedTable]) -> Vec<Expression> {
    eliminated
        .iter()
        .map(|table| {
            let subquery = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(FromClause::Table {
                    name: table.name.clone(),
                    alias: table.alias.clone(),
                    column_aliases: None,
                }),
                where_clause: table.filter.clone(),
                group_by: None,
                having: None,
                order_by: None,
                limit: Some(1),
                offset: None,
                set_operation: None,
            };

            Expression::Exists { subquery: Box::new(subquery), negated: false }
        })
        .collect()
}

/// Add EXISTS checks to WHERE clause
fn add_exists_to_where(
    where_clause: Option<&Expression>,
    exists_checks: Vec<Expression>,
) -> Option<Expression> {
    if exists_checks.is_empty() {
        return where_clause.cloned();
    }

    let combined_exists = combine_predicates(exists_checks);

    match where_clause {
        Some(existing) => Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(existing.clone()),
            right: Box::new(combined_exists),
        }),
        None => Some(combined_exists),
    }
}

/// Remove predicates that only reference eliminated tables
///
/// Uses both qualified table references and prefix matching for unqualified columns
fn remove_eliminated_predicates(
    expr: &Expression,
    eliminated_tables: &HashSet<String>,
    eliminated_prefixes: &HashSet<String>,
) -> Option<Expression> {
    let predicates = flatten_and_chain(expr);
    let mut kept = Vec::new();

    for pred in predicates {
        // Get qualified table refs
        let mut qualified_refs = HashSet::new();
        extract_tables_from_expr(&pred, &mut qualified_refs);

        // Get unqualified columns
        let mut unqualified_cols = HashSet::new();
        collect_unqualified_columns_from_expr(&pred, &mut unqualified_cols);

        // Check if all qualified refs are to eliminated tables
        let qualified_all_eliminated = qualified_refs.is_empty()
            || qualified_refs.iter().all(|t| eliminated_tables.contains(t));

        // Check if all unqualified columns match eliminated table prefixes
        let unqualified_all_eliminated = unqualified_cols.is_empty()
            || unqualified_cols.iter().all(|col| {
                let col_lower = col.to_lowercase();
                eliminated_prefixes.iter().any(|prefix| col_lower.starts_with(prefix))
            });

        // A predicate should be removed if ALL its column references
        // (both qualified and unqualified) belong to eliminated tables
        let should_remove = qualified_all_eliminated
            && unqualified_all_eliminated
            && (!qualified_refs.is_empty() || !unqualified_cols.is_empty());

        if !should_remove {
            kept.push(pred);
        }
    }

    if kept.is_empty() {
        None
    } else {
        Some(combine_predicates(kept))
    }
}

/// Flatten AND chain
fn flatten_and_chain(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            let mut result = flatten_and_chain(left);
            result.extend(flatten_and_chain(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Collect all unqualified column names from SELECT list
fn collect_unqualified_columns(select_list: &[SelectItem]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_unqualified_columns_from_expr(expr, &mut columns);
        }
    }
    columns
}

fn collect_unqualified_columns_from_expr(expr: &Expression, columns: &mut HashSet<String>) {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Skip the special "*" wildcard (used in COUNT(*))
            if column != "*" {
                columns.insert(column.to_lowercase());
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_unqualified_columns_from_expr(left, columns);
            collect_unqualified_columns_from_expr(right, columns);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_unqualified_columns_from_expr(expr, columns);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_unqualified_columns_from_expr(arg, columns);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_unqualified_columns_from_expr(op, columns);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_unqualified_columns_from_expr(cond, columns);
                }
                collect_unqualified_columns_from_expr(&clause.result, columns);
            }
            if let Some(else_res) = else_result {
                collect_unqualified_columns_from_expr(else_res, columns);
            }
        }
        Expression::IsNull { expr, .. } | Expression::Cast { expr, .. } => {
            collect_unqualified_columns_from_expr(expr, columns);
        }
        _ => {}
    }
}

/// Build a map from table name to column prefix based on qualified column references
///
/// For example, if we see `date_dim.d_year`, we extract prefix `d_` for table `date_dim`.
/// This allows us to determine if unqualified columns could belong to a table.
///
/// For tables with no qualified refs, we derive a potential prefix from the table name
/// (e.g., `date_dim` → `d_`, `customer` → `c_`) for TPC-DS style naming conventions.
fn build_column_prefix_map(
    stmt: &SelectStmt,
    table_names: &HashSet<String>,
) -> HashMap<String, String> {
    let mut table_columns: HashMap<String, Vec<String>> = HashMap::new();

    // Collect qualified columns from entire statement
    collect_qualified_columns_from_select(&stmt.select_list, &mut table_columns);
    if let Some(from) = &stmt.from {
        collect_qualified_columns_from_from(from, &mut table_columns);
    }
    if let Some(where_expr) = &stmt.where_clause {
        collect_qualified_columns_from_expr(where_expr, &mut table_columns);
    }

    // Derive prefix for each table from its columns
    let mut prefixes = HashMap::new();
    for (table, columns) in &table_columns {
        let table_lower = table.to_lowercase();
        if !table_names.contains(&table_lower) {
            continue; // Skip tables not in FROM clause
        }
        if let Some(prefix) = find_common_prefix(columns) {
            prefixes.insert(table_lower, prefix);
        }
    }

    // For tables with no qualified refs, try to derive prefix from table name
    // This handles cases like `date_dim` with unqualified `d_year` filter
    for table_name in table_names {
        if !prefixes.contains_key(table_name) {
            if let Some(prefix) = derive_prefix_from_table_name(table_name) {
                prefixes.insert(table_name.clone(), prefix);
            }
        }
    }

    prefixes
}

/// Derive a column prefix from a table name using naming conventions
///
/// For TPC-DS style naming:
/// - Short names (2-3 chars, likely aliases): use as-is + underscore (ss → ss_, ws → ws_)
/// - Dimension tables (`_dim` suffix): first letter (date_dim → d_, time_dim → t_)
/// - Multi-word tables: acronym (customer_address → ca_, store_sales → ss_)
/// - Single word tables: first letter (customer → c_, item → i_)
fn derive_prefix_from_table_name(table_name: &str) -> Option<String> {
    let name = table_name.to_lowercase();

    // Short names (2-3 chars) are likely aliases for tables
    // Use the full alias + underscore (ss → ss_, ws → ws_, cs → cs_)
    // This handles common TPC-DS alias patterns
    if name.len() <= 3 && !name.contains('_') {
        return Some(format!("{}_", name));
    }

    // Handle dimension tables: `*_dim` uses first letter only
    if name.ends_with("_dim") {
        let first_char = name.chars().next()?;
        return Some(format!("{}_", first_char));
    }

    // Handle multi-word table names (e.g., customer_address → ca_, store_sales → ss_)
    if let Some(underscore_pos) = name.find('_') {
        let first_word = &name[..underscore_pos];
        let rest = &name[underscore_pos + 1..];

        // Acronym style: first letter of each word
        if !rest.is_empty() {
            let mut prefix = String::new();
            prefix.push(first_word.chars().next()?);
            // Take first letter of second word
            if let Some(second_first) = rest.chars().next() {
                prefix.push(second_first);
            }
            prefix.push('_');
            return Some(prefix);
        }
    }

    // Single word: first letter + underscore (customer → c_, item → i_)
    let first_char = name.chars().next()?;
    Some(format!("{}_", first_char))
}

fn collect_qualified_columns_from_select(
    select_list: &[SelectItem],
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
    }
}

fn collect_qualified_columns_from_from(
    from: &FromClause,
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    match from {
        FromClause::Table { .. } => {}
        FromClause::Subquery { .. } => {}
        FromClause::Join { left, right, condition, .. } => {
            collect_qualified_columns_from_from(left, table_columns);
            collect_qualified_columns_from_from(right, table_columns);
            if let Some(cond) = condition {
                collect_qualified_columns_from_expr(cond, table_columns);
            }
        }
    }
}

fn collect_qualified_columns_from_expr(
    expr: &Expression,
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        Expression::ColumnRef { table: Some(t), column } => {
            table_columns.entry(t.to_lowercase()).or_default().push(column.to_lowercase());
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_qualified_columns_from_expr(left, table_columns);
            collect_qualified_columns_from_expr(right, table_columns);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_qualified_columns_from_expr(arg, table_columns);
            }
        }
        Expression::InList { expr, values, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
            for v in values {
                collect_qualified_columns_from_expr(v, table_columns);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_qualified_columns_from_expr(op, table_columns);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_qualified_columns_from_expr(cond, table_columns);
                }
                collect_qualified_columns_from_expr(&clause.result, table_columns);
            }
            if let Some(else_res) = else_result {
                collect_qualified_columns_from_expr(else_res, table_columns);
            }
        }
        Expression::IsNull { expr, .. } | Expression::Cast { expr, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
        _ => {}
    }
}

/// Find common prefix for a set of column names
///
/// For TPC-DS style naming (d_year, d_date_sk), finds the underscore-delimited prefix.
fn find_common_prefix(columns: &[String]) -> Option<String> {
    if columns.is_empty() {
        return None;
    }

    // Try to find prefix ending with underscore
    let first = &columns[0];
    if let Some(underscore_pos) = first.find('_') {
        let prefix = &first[..=underscore_pos]; // Include the underscore

        // Check if all columns have this prefix
        if columns.iter().all(|c| c.starts_with(prefix)) {
            return Some(prefix.to_string());
        }
    }

    // Fallback: use first 2 characters if all columns share them
    if first.len() >= 2 {
        let prefix = &first[..2];
        if columns.iter().all(|c| c.starts_with(prefix)) {
            return Some(prefix.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    mod derive_prefix_from_table_name_tests {
        use super::*;

        #[test]
        fn short_aliases_two_chars() {
            // TPC-DS style 2-char aliases
            assert_eq!(derive_prefix_from_table_name("ss"), Some("ss_".to_string()));
            assert_eq!(derive_prefix_from_table_name("ws"), Some("ws_".to_string()));
            assert_eq!(derive_prefix_from_table_name("cs"), Some("cs_".to_string()));
        }

        #[test]
        fn short_aliases_three_chars() {
            // 3-char aliases
            assert_eq!(derive_prefix_from_table_name("inv"), Some("inv_".to_string()));
        }

        #[test]
        fn dimension_tables() {
            // Tables ending with _dim use first letter
            assert_eq!(derive_prefix_from_table_name("date_dim"), Some("d_".to_string()));
            assert_eq!(derive_prefix_from_table_name("time_dim"), Some("t_".to_string()));
            assert_eq!(derive_prefix_from_table_name("item_dim"), Some("i_".to_string()));
        }

        #[test]
        fn multi_word_tables() {
            // Multi-word tables use acronym (first letter of each word)
            assert_eq!(derive_prefix_from_table_name("customer_address"), Some("ca_".to_string()));
            assert_eq!(derive_prefix_from_table_name("store_sales"), Some("ss_".to_string()));
            assert_eq!(derive_prefix_from_table_name("web_returns"), Some("wr_".to_string()));
            assert_eq!(derive_prefix_from_table_name("catalog_page"), Some("cp_".to_string()));
        }

        #[test]
        fn single_word_tables() {
            // Single word tables use first letter
            assert_eq!(derive_prefix_from_table_name("customer"), Some("c_".to_string()));
            assert_eq!(derive_prefix_from_table_name("item"), Some("i_".to_string()));
            assert_eq!(derive_prefix_from_table_name("store"), Some("s_".to_string()));
            assert_eq!(derive_prefix_from_table_name("warehouse"), Some("w_".to_string()));
        }

        #[test]
        fn case_insensitive() {
            // Should handle mixed case
            assert_eq!(derive_prefix_from_table_name("DATE_DIM"), Some("d_".to_string()));
            assert_eq!(derive_prefix_from_table_name("Customer"), Some("c_".to_string()));
            assert_eq!(derive_prefix_from_table_name("Store_Sales"), Some("ss_".to_string()));
        }

        #[test]
        fn empty_string() {
            // Empty string falls through to short alias logic (len <= 3)
            // and returns "_" (empty + underscore)
            assert_eq!(derive_prefix_from_table_name(""), Some("_".to_string()));
        }
    }

    mod find_common_prefix_tests {
        use super::*;

        #[test]
        fn common_underscore_prefix() {
            let cols = vec!["d_year".to_string(), "d_date_sk".to_string(), "d_month".to_string()];
            assert_eq!(find_common_prefix(&cols), Some("d_".to_string()));
        }

        #[test]
        fn two_char_prefix() {
            let cols = vec!["ca_state".to_string(), "ca_city".to_string(), "ca_zip".to_string()];
            assert_eq!(find_common_prefix(&cols), Some("ca_".to_string()));
        }

        #[test]
        fn no_common_prefix() {
            let cols = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
            assert_eq!(find_common_prefix(&cols), None);
        }

        #[test]
        fn empty_columns() {
            let cols: Vec<String> = vec![];
            assert_eq!(find_common_prefix(&cols), None);
        }

        #[test]
        fn single_column() {
            let cols = vec!["d_year".to_string()];
            assert_eq!(find_common_prefix(&cols), Some("d_".to_string()));
        }

        #[test]
        fn partial_match_not_all_columns() {
            // First two columns share prefix, but third doesn't
            let cols = vec!["d_year".to_string(), "d_month".to_string(), "t_hour".to_string()];
            assert_eq!(find_common_prefix(&cols), None);
        }

        #[test]
        fn fallback_to_two_char_prefix() {
            // No underscore, but shares first 2 chars
            let cols = vec!["item1".to_string(), "item2".to_string()];
            assert_eq!(find_common_prefix(&cols), Some("it".to_string()));
        }
    }

    mod eliminate_unused_tables_tests {
        use super::*;
        use vibesql_ast::{BinaryOperator, Expression, FromClause, SelectItem, SelectStmt};
        use vibesql_types::SqlValue;

        fn make_column_ref(table: Option<&str>, column: &str) -> Expression {
            Expression::ColumnRef {
                table: table.map(|t| t.to_string()),
                column: column.to_string(),
            }
        }

        fn make_table(name: &str, alias: Option<&str>) -> FromClause {
            FromClause::Table {
                name: name.to_string(),
                alias: alias.map(|a| a.to_string()),
                column_aliases: None,
            }
        }

        fn make_cross_join(left: FromClause, right: FromClause) -> FromClause {
            FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Cross,
                condition: None,
                natural: false,
            }
        }

        #[test]
        fn single_table_unchanged() {
            // Single table should not be eliminated
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: make_column_ref(Some("t1"), "col1"),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(make_table("table1", Some("t1"))),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            assert!(matches!(result.from, Some(FromClause::Table { .. })));
        }

        #[test]
        fn table_in_select_not_eliminated() {
            // Table referenced in SELECT should not be eliminated
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![
                    SelectItem::Expression {
                        expr: make_column_ref(Some("t1"), "col1"),
                        alias: None,
                    },
                    SelectItem::Expression {
                        expr: make_column_ref(Some("t2"), "col2"),
                        alias: None,
                    },
                ],
                into_table: None,
                into_variables: None,
                from: Some(make_cross_join(
                    make_table("table1", Some("t1")),
                    make_table("table2", Some("t2")),
                )),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Both tables should be kept since both are in SELECT
            assert!(matches!(result.from, Some(FromClause::Join { .. })));
        }

        #[test]
        fn table_in_equijoin_not_eliminated() {
            // Table in equijoin should not be eliminated
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: make_column_ref(Some("t1"), "col1"),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(make_cross_join(
                    make_table("table1", Some("t1")),
                    make_table("table2", Some("t2")),
                )),
                where_clause: Some(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(make_column_ref(Some("t1"), "id")),
                    right: Box::new(make_column_ref(Some("t2"), "id")),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Both tables should be kept since they're in equijoin
            assert!(matches!(result.from, Some(FromClause::Join { .. })));
        }

        #[test]
        fn unused_table_with_filter_eliminated() {
            // Table not in SELECT and not in equijoin should be eliminated
            // with its filter converted to EXISTS
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: make_column_ref(Some("t1"), "col1"),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(make_cross_join(
                    make_table("table1", Some("t1")),
                    make_table("date_dim", Some("d")),
                )),
                where_clause: Some(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(make_column_ref(Some("d"), "d_year")),
                    right: Box::new(Expression::Literal(SqlValue::Integer(2000))),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);

            // FROM should now only have table1
            match &result.from {
                Some(FromClause::Table { name, .. }) => {
                    assert_eq!(name, "table1");
                }
                _ => panic!("Expected single table, got: {:?}", result.from),
            }

            // WHERE should contain EXISTS check
            match &result.where_clause {
                Some(Expression::Exists { negated, .. }) => {
                    assert!(!negated);
                }
                _ => panic!("Expected EXISTS clause, got: {:?}", result.where_clause),
            }
        }

        #[test]
        fn select_literal_subquery_unchanged() {
            // SELECT 1 FROM ... subqueries should not be optimized
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::Literal(SqlValue::Integer(1)),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: Some(make_cross_join(
                    make_table("table1", Some("t1")),
                    make_table("table2", Some("t2")),
                )),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Should be unchanged (both tables kept)
            assert!(matches!(result.from, Some(FromClause::Join { .. })));
        }

        #[test]
        fn no_from_clause_unchanged() {
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::Literal(SqlValue::Integer(42)),
                    alias: None,
                }],
                into_table: None,
                into_variables: None,
                from: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            assert!(result.from.is_none());
        }

        #[test]
        fn select_star_references_all_tables() {
            // SELECT * should reference all tables, preventing elimination
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Wildcard { alias: None }],
                into_table: None,
                into_variables: None,
                from: Some(make_cross_join(
                    make_table("table1", Some("t1")),
                    make_table("table2", Some("t2")),
                )),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Both tables should be kept due to SELECT *
            assert!(matches!(result.from, Some(FromClause::Join { .. })));
        }

        #[test]
        fn cross_join_without_filter_preserved() {
            // Regression test: tables in cross joins without filters should NOT be eliminated
            // because cross joins multiply rows intentionally.
            // Example: SELECT 86 * - cor0.col2 FROM tab1, tab2 AS cor0
            // This should return 9 rows (3x3), not 3 rows.
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                // SELECT only references cor0.col2
                select_list: vec![SelectItem::Expression {
                    expr: Expression::BinaryOp {
                        op: BinaryOperator::Multiply,
                        left: Box::new(Expression::Literal(SqlValue::Integer(86))),
                        right: Box::new(Expression::UnaryOp {
                            op: vibesql_ast::UnaryOperator::Minus,
                            expr: Box::new(make_column_ref(Some("cor0"), "col2")),
                        }),
                    },
                    alias: Some("col0".to_string()),
                }],
                into_table: None,
                into_variables: None,
                // Cross join - tab1 is NOT referenced but has no filter
                from: Some(make_cross_join(
                    make_table("tab1", None),
                    make_table("tab2", Some("cor0")),
                )),
                // No WHERE clause
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Both tables should be kept - tab1 has no filter, so cross join
            // must be preserved to maintain correct row count
            assert!(
                matches!(result.from, Some(FromClause::Join { .. })),
                "Expected cross join to be preserved, got {:?}",
                result.from
            );
        }

        #[test]
        fn all_tables_eliminable_keeps_unchanged() {
            // Regression test: when ALL tables could be eliminated,
            // we should keep them all to preserve FROM clause.
            // This ensures WHERE clauses like NULL IS NOT NULL work correctly.
            // Example: SELECT - 0 FROM tab0, tab0 cor0 WHERE NULL IS NOT NULL
            let stmt = SelectStmt {
                with_clause: None,
                distinct: false,
                // SELECT with literal (no column refs)
                select_list: vec![SelectItem::Expression {
                    expr: Expression::UnaryOp {
                        op: vibesql_ast::UnaryOperator::Minus,
                        expr: Box::new(Expression::Literal(SqlValue::Integer(0))),
                    },
                    alias: Some("col3".to_string()),
                }],
                into_table: None,
                into_variables: None,
                // Cross join of same table
                from: Some(make_cross_join(
                    make_table("tab0", None),
                    make_table("tab0", Some("cor0")),
                )),
                // WHERE clause with no column refs (NULL IS NOT NULL)
                where_clause: Some(Expression::IsNull {
                    expr: Box::new(Expression::Literal(SqlValue::Null)),
                    negated: true,
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            };

            let result = eliminate_unused_tables(&stmt);
            // Both tables should be kept (not eliminated) because eliminating
            // both would leave no FROM clause
            assert!(
                matches!(result.from, Some(FromClause::Join { .. })),
                "Expected FROM clause to be preserved, got {:?}",
                result.from
            );
        }
    }

    mod helper_function_tests {
        use super::*;
        use vibesql_ast::{BinaryOperator, Expression, SelectItem};
        use vibesql_types::SqlValue;

        #[test]
        fn flatten_and_chain_single() {
            let expr = Expression::Literal(SqlValue::Integer(1));
            let result = flatten_and_chain(&expr);
            assert_eq!(result.len(), 1);
        }

        #[test]
        fn flatten_and_chain_multiple() {
            let expr = Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::Literal(SqlValue::Integer(1))),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::And,
                    left: Box::new(Expression::Literal(SqlValue::Integer(2))),
                    right: Box::new(Expression::Literal(SqlValue::Integer(3))),
                }),
            };
            let result = flatten_and_chain(&expr);
            assert_eq!(result.len(), 3);
        }

        #[test]
        fn combine_predicates_empty() {
            let preds: Vec<Expression> = vec![];
            let result = combine_predicates(preds);
            assert!(matches!(result, Expression::Literal(SqlValue::Boolean(true))));
        }

        #[test]
        fn combine_predicates_single() {
            let preds = vec![Expression::Literal(SqlValue::Integer(42))];
            let result = combine_predicates(preds);
            assert!(matches!(result, Expression::Literal(SqlValue::Integer(42))));
        }

        #[test]
        fn combine_predicates_multiple() {
            let preds = vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
            ];
            let result = combine_predicates(preds);
            assert!(matches!(result, Expression::BinaryOp { op: BinaryOperator::And, .. }));
        }

        #[test]
        fn collect_unqualified_columns_finds_refs() {
            let select_list = vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "col1".to_string() },
                    alias: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "col2".to_string(),
                    },
                    alias: None,
                },
            ];
            let result = collect_unqualified_columns(&select_list);
            assert!(result.contains("col1"));
            assert!(!result.contains("col2")); // Qualified column should not be included
        }

        #[test]
        fn has_unqualified_column_ref_detects_unqualified() {
            let qualified =
                Expression::ColumnRef { table: Some("t1".to_string()), column: "col1".to_string() };
            assert!(!has_unqualified_column_ref(&qualified));

            let unqualified = Expression::ColumnRef { table: None, column: "col1".to_string() };
            assert!(has_unqualified_column_ref(&unqualified));
        }
    }
}
