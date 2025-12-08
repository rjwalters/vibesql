//! Index selection logic
//!
//! Determines when and which index to use for query optimization.
//! Supports both rule-based (simple) and cost-based (statistics-aware) selection.
//! Also includes skip-scan optimization for queries filtering on non-prefix columns.

use vibesql_ast::Expression;
use vibesql_catalog::TableSchema;
use vibesql_storage::{
    statistics::{AccessMethod, CostEstimator},
    Database,
};

use crate::optimizer::index_planner::{IndexPlanner, SkipScanInfo};

/// Result of index selection, distinguishing between regular and skip-scan
#[derive(Debug, Clone)]
pub(crate) enum IndexScanChoice {
    /// Regular index scan using prefix columns
    Regular {
        index_name: String,
        sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    },
    /// Skip-scan using non-prefix column filter
    SkipScan {
        index_name: String,
        skip_scan_info: SkipScanInfo,
    },
}


/// Check if any ORDER BY column is nullable
///
/// BTreeMap orders NULLs first (NULL < everything), but SQL default is:
/// - NULLS LAST for ASC
/// - NULLS FIRST for DESC
///
/// When a nullable column is used for ORDER BY without explicit NULLS FIRST/LAST,
/// using an index would produce incorrect NULL ordering. This function helps
/// detect such cases to avoid using the index for ORDER BY.
fn any_order_by_column_nullable(
    order_items: &[vibesql_ast::OrderByItem],
    table_schema: &TableSchema,
) -> bool {
    for item in order_items {
        if let Expression::ColumnRef { column, .. } = &item.expr {
            // Look up column in schema (case-insensitive)
            if let Some(idx) = table_schema.get_column_index(column) {
                if table_schema.columns[idx].nullable {
                    return true;
                }
            }
        }
    }
    false
}

/// Determines if an index scan is beneficial for the given query
///
/// Returns Some((index_name, sorted_columns)) if an index should be used, None otherwise.
/// The sorted_columns vector indicates which columns are pre-sorted by the index scan.
#[allow(clippy::type_complexity)]
pub(crate) fn should_use_index_scan(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // We use indexes in three scenarios:
    // 1. WHERE clause references an indexed column (with or without ORDER BY)
    // 2. ORDER BY references an indexed column (even without WHERE)
    // 3. Both WHERE and ORDER BY use the same index
    //
    // Note: Index scans can provide partial optimization even for complex
    // predicates (including OR expressions). The full WHERE clause is always
    // applied as a post-filter in execute_index_scan() to ensure correctness.

    // Get all indexes for this table
    let table = database.get_table(table_name)?;
    let indexes = database.list_indexes_for_table(table_name);

    if indexes.is_empty() {
        return None;
    }

    // Find the best index (most pinned columns = better filtering)
    // We evaluate all applicable indexes and pick the one that covers the most WHERE columns
    let mut best_index: Option<(
        String,
        usize,
        bool,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, pinned_count, can_use_for_order, sorted_columns)

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE clause
            let can_use_for_where = where_clause
                .map(|expr| expression_filters_column(expr, &first_indexed_column.column_name))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // Check if this index can be used for ORDER BY clause
            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    pinned_columns,
                );

                // Don't use index for ORDER BY if any column is nullable
                // BTreeMap orders NULLs first, but SQL default is NULLS LAST for ASC
                // This would produce incorrect results for nullable columns
                if columns_match && any_order_by_column_nullable(order_items, &table.schema) {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip if this index can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                continue;
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            let col_name = match &item.expr {
                                Expression::ColumnRef { column, .. } => column.clone(),
                                _ => unreachable!(
                                    "can_use_index_for_order_by ensures simple column refs"
                                ),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Compare with best index so far
            // Prefer: more pinned columns > can satisfy ORDER BY > first found
            let is_better = match &best_index {
                None => true,
                Some((_, best_pinned, best_can_order, _)) => {
                    // More pinned columns = better (narrows down rows more)
                    if pinned_columns > *best_pinned {
                        true
                    } else if pinned_columns == *best_pinned {
                        // Same pinned count: prefer one that can satisfy ORDER BY
                        can_use_for_order && !*best_can_order
                    } else {
                        false
                    }
                }
            };

            if is_better {
                best_index =
                    Some((index_name.clone(), pinned_columns, can_use_for_order, sorted_columns));
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, sorted_columns)) = best_index {
        return Some((index_name, sorted_columns));
    }

    None
}

/// Check if an expression filters a specific column
///
/// Returns true if the expression contains a predicate on the given column
/// For example: "WHERE age = 25" filters column "age"
pub(crate) fn expression_filters_column(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check for comparison operators
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Index can only filter when comparing column to a LITERAL value
                    // NOT when comparing column to another column (equijoin conditions)
                    // e.g., `l_shipdate > '1995-03-15'` CAN use index
                    // e.g., `l_orderkey = o_orderkey` CANNOT use index
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_literal = is_literal(left);
                    let right_is_literal = is_literal(right);

                    // column op literal OR literal op column
                    if (left_is_col && right_is_literal) || (left_is_literal && right_is_col) {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    // Recursively check sub-expressions for AND/OR
                    return expression_filters_column(left, column_name)
                        || expression_filters_column(right, column_name);
                }
                _ => {}
            }
            false
        }
        // IN with value list: col IN (1, 2, 3)
        Expression::InList { expr, .. } => is_column_reference(expr, column_name),
        // IN with subquery: col IN (SELECT ...)
        Expression::In { expr, .. } => is_column_reference(expr, column_name),
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr, .. } => is_column_reference(expr, column_name),
        _ => false,
    }
}

/// Check if an expression is a reference to a specific column
///
/// Uses case-insensitive comparison because:
/// - SQL parser normalizes unquoted identifiers to uppercase (e.g., `i_id` → `I_ID`)
/// - Index column names may be lowercase or uppercase depending on how they're created
/// - Table column statistics are stored with the original column name case
pub(super) fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => column.eq_ignore_ascii_case(column_name),
        _ => false,
    }
}

/// Check if an expression is a literal value or parameter placeholder
///
/// Index scans can filter on columns compared to literal values or bound parameters,
/// not columns compared to other columns (which are equijoin conditions).
/// Parameter placeholders (?, $1, :name, etc.) are treated as literals for index selection
/// because they are resolved to concrete values at execution time.
fn is_literal(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Literal(_)
            | Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_)
    )
}

/// Case-insensitive lookup of column statistics
///
/// Returns the ColumnStatistics for a column name, ignoring case differences.
/// This is necessary because schema column names may use different casing than
/// index column names or query column references.
fn get_column_stats_ignore_case<'a>(
    columns: &'a std::collections::HashMap<String, vibesql_storage::statistics::ColumnStatistics>,
    column_name: &str,
) -> Option<&'a vibesql_storage::statistics::ColumnStatistics> {
    // First try exact match for efficiency
    if let Some(stats) = columns.get(column_name) {
        return Some(stats);
    }
    // Fall back to case-insensitive search
    columns.iter().find(|(key, _)| key.eq_ignore_ascii_case(column_name)).map(|(_, stats)| stats)
}

/// Check if an index can be used to satisfy an ORDER BY clause
///
/// Returns true if the ORDER BY columns match the index columns (after skipping
/// any prefix columns pinned by equality predicates) and the sort directions
/// are compatible (either all matching or all reversed).
///
/// Examples:
/// - ORDER BY col0 ASC can use index (col0 ASC)
/// - ORDER BY col0 DESC can use index (col0 DESC) via reversal
/// - ORDER BY col0, col1 can use index (col0, col1)
/// - WHERE col0 = 1 ORDER BY col1 can use index (col0, col1) - col0 is pinned
pub(crate) fn can_use_index_for_order_by(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    can_use_index_for_order_by_with_pinned(order_items, index_columns, 0)
}

/// Check if an index can be used for ORDER BY, accounting for pinned prefix columns
///
/// When a query has equality predicates on leading index columns (e.g., WHERE a = 1 AND b = 2),
/// those columns are "pinned" and the index is effectively sorted by the remaining columns.
/// This function skips over the pinned columns and checks if the ORDER BY matches.
///
/// For example, with index (a, b, c):
/// - WHERE a = 1 ORDER BY b, c → can use index (skip 1 pinned column)
/// - WHERE a = 1 AND b = 2 ORDER BY c → can use index (skip 2 pinned columns)
/// - WHERE a = 1 ORDER BY c → cannot use index (b must come before c)
pub(crate) fn can_use_index_for_order_by_with_pinned(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
    pinned_columns: usize,
) -> bool {
    // Skip pinned columns
    let remaining_index_columns = &index_columns[pinned_columns..];

    // ORDER BY must not have more columns than remaining index columns
    if order_items.len() > remaining_index_columns.len() {
        return false;
    }

    // If no ORDER BY columns, nothing to match
    if order_items.is_empty() {
        return false;
    }

    // Check if ORDER BY matches index direction or is completely reversed
    // (allowing reverse scan to satisfy DESC ordering)
    let mut all_match = true;
    let mut all_reversed = true;

    // Check each ORDER BY column against corresponding index column
    for (order_item, index_col) in order_items.iter().zip(remaining_index_columns.iter()) {
        // ORDER BY expression must be a simple column reference
        let order_col_name = match &order_item.expr {
            Expression::ColumnRef { table: None, column } => column,
            _ => return false, // Complex expressions not supported
        };

        // Column names must match (case-insensitive due to SQL identifier normalization)
        if !order_col_name.eq_ignore_ascii_case(&index_col.column_name) {
            return false;
        }

        // Check sort directions
        let directions_match = order_item.direction == index_col.direction;
        let directions_opposite = matches!(
            (&order_item.direction, &index_col.direction),
            (vibesql_ast::OrderDirection::Asc, vibesql_ast::OrderDirection::Desc)
                | (vibesql_ast::OrderDirection::Desc, vibesql_ast::OrderDirection::Asc)
        );

        if !directions_match {
            all_match = false;
        }
        if !directions_opposite {
            all_reversed = false;
        }
    }

    // Accept if all directions match OR all are reversed (reverse scan)
    all_match || all_reversed
}

/// Count how many leading index columns are pinned by equality predicates
///
/// A column is "pinned" if there's an equality predicate (col = value) in the WHERE clause.
/// Returns the number of consecutive leading index columns that are pinned.
pub(crate) fn count_pinned_index_columns(
    where_clause: Option<&Expression>,
    index_columns: &[vibesql_ast::IndexColumn],
) -> usize {
    let where_clause = match where_clause {
        Some(expr) => expr,
        None => return 0,
    };

    // Collect all columns that have equality predicates
    let mut pinned_columns = std::collections::HashSet::new();
    collect_equality_columns(where_clause, &mut pinned_columns);

    // Count consecutive leading index columns that are pinned
    let mut count = 0;
    for index_col in index_columns {
        // Check if this index column is pinned (case-insensitive match)
        let is_pinned =
            pinned_columns.iter().any(|c| c.eq_ignore_ascii_case(&index_col.column_name));
        if is_pinned {
            count += 1;
        } else {
            break; // Stop at first non-pinned column
        }
    }
    count
}

/// Collect all column names that have equality predicates (col = literal)
fn collect_equality_columns(expr: &Expression, columns: &mut std::collections::HashSet<String>) {
    if let Expression::BinaryOp { left, op, right } = expr {
        match op {
            vibesql_ast::BinaryOperator::Equal => {
                // Check for column = literal pattern
                if let Expression::ColumnRef { column, .. } = &**left {
                    if is_literal(right) {
                        columns.insert(column.to_uppercase());
                    }
                }
                if let Expression::ColumnRef { column, .. } = &**right {
                    if is_literal(left) {
                        columns.insert(column.to_uppercase());
                    }
                }
            }
            vibesql_ast::BinaryOperator::And => {
                // Recurse into both sides of AND
                collect_equality_columns(left, columns);
                collect_equality_columns(right, columns);
            }
            _ => {}
        }
    }
}

/// Cost-based index selection using statistics
///
/// This function uses table and column statistics to make intelligent decisions
/// about whether to use an index scan or a table scan. It estimates the cost of
/// both access methods and chooses the cheaper one.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some((index_name, sorted_columns))` if cost-based analysis suggests using an index
/// - `None` if table scan is cheaper or no statistics are available
///
/// # Fallback Behavior
/// If statistics are not available or stale, falls back to rule-based selection
/// using `should_use_index_scan()`.
#[allow(clippy::type_complexity)]
pub(crate) fn cost_based_index_selection(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // Get table statistics
    let table = database.get_table(table_name)?;
    let table_stats = table.get_statistics();

    // If no statistics or stale, fall back to rule-based selection
    if table_stats.is_none() || table_stats.as_ref().map(|s| s.needs_refresh()).unwrap_or(false) {
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    let table_stats = table_stats.unwrap();
    let cost_estimator = CostEstimator::default();

    // Get all indexes for this table
    let indexes = database.list_indexes_for_table(table_name);
    if indexes.is_empty() {
        return None; // No indexes available
    }

    // Try each index and find the one with best score (pinned columns + cost)
    #[allow(clippy::type_complexity)]
    let mut best_index: Option<(
        String,
        AccessMethod,
        usize,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, access_method, pinned_count, sorted_columns)
    let mut has_applicable_index_without_stats = false;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;
            let column_name = &first_indexed_column.column_name;

            // Check if this index can be used for WHERE or ORDER BY
            let can_use_for_where = where_clause
                .map(|expr| expression_filters_column(expr, column_name))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // Debug: trace index selection
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] table={}, index={}, first_col={}, can_use_for_where={}",
                    table_name, index_name, column_name, can_use_for_where
                );
            }

            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    pinned_columns,
                );

                // Don't use index for ORDER BY if any column is nullable
                // BTreeMap orders NULLs first, but SQL default is NULLS LAST for ASC
                // This would produce incorrect results for nullable columns
                if columns_match && any_order_by_column_nullable(order_items, &table.schema) {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip this index if it can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] skipping {} - can't use for where or order",
                        index_name
                    );
                }
                continue;
            }

            // Debug: continue trace
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} passed where/order check, checking stats...",
                    index_name
                );
            }

            // Get column statistics for the indexed column (case-insensitive lookup)
            let col_stats = get_column_stats_ignore_case(&table_stats.columns, column_name);
            if col_stats.is_none() {
                // Track that we found an applicable index without column stats
                // We'll fall back to rule-based selection if cost-based fails
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} no column stats for {}, will fallback",
                        index_name, column_name
                    );
                }
                has_applicable_index_without_stats = true;
                continue; // No stats for this column, try next index
            }
            let col_stats = col_stats.unwrap();

            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!("[INDEX_SELECT] {} has column stats for {}", index_name, column_name);
            }

            // Estimate selectivity based on WHERE clause
            let selectivity = if let Some(where_expr) = where_clause {
                estimate_selectivity(where_expr, column_name, col_stats)
            } else {
                1.0 // No WHERE clause means all rows
            };

            // Use cost estimator to decide
            let access_method =
                cost_estimator.choose_access_method(table_stats, Some(col_stats), selectivity);

            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} selectivity={:.4}, access_method={:?}, is_index_scan={}",
                    index_name,
                    selectivity,
                    access_method,
                    access_method.is_index_scan()
                );
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            let col_name = match &item.expr {
                                Expression::ColumnRef { column, .. } => column.clone(),
                                _ => unreachable!(
                                    "can_use_index_for_order_by ensures simple column refs"
                                ),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Track the best index
            // Priority: more pinned columns (better filtering) > lower cost
            if access_method.is_index_scan() {
                let is_better = match &best_index {
                    None => true,
                    Some((_, best_method, best_pinned, _)) => {
                        // More pinned columns is always better (filters more rows)
                        if pinned_columns > *best_pinned {
                            true
                        } else if pinned_columns == *best_pinned {
                            // Same pinned count: prefer lower cost
                            access_method.cost() < best_method.cost()
                        } else {
                            false
                        }
                    }
                };

                if is_better {
                    best_index =
                        Some((index_name.clone(), access_method, pinned_columns, sorted_columns));
                }
            } else if selectivity < 0.40 && can_use_for_where {
                // Cost-based chose table scan, but selectivity is good enough for index
                // The cost model may be too conservative for in-memory/prefix scans
                // Common fallback values: 0.33 (single predicate), 0.1089 (two predicates)
                // Fall back to rule-based selection for selective queries
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} selectivity={:.4} good, falling back to rule-based",
                        index_name, selectivity
                    );
                }
                return should_use_index_scan(table_name, where_clause, order_by, database);
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, sorted_columns)) = best_index {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] selected best_index={} for table={}", index_name, table_name);
        }
        return Some((index_name, sorted_columns));
    }

    // If we have applicable indexes but no column stats, fall back to rule-based selection
    // This ensures we use indexes even when statistics are incomplete
    if has_applicable_index_without_stats {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] falling back to rule-based for table={}", table_name);
        }
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    // No applicable indexes found
    if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
        eprintln!("[INDEX_SELECT] no index selected for table={}", table_name);
    }
    None
}

/// Estimate selectivity of a predicate on a specific column
///
/// Uses column statistics to estimate what fraction of rows will match the predicate.
/// Returns a value between 0.0 (no rows) and 1.0 (all rows).
pub(crate) fn estimate_selectivity(
    expr: &Expression,
    column_name: &str,
    col_stats: &vibesql_storage::statistics::ColumnStatistics,
) -> f64 {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check if this is a predicate on our column (case-insensitive)
                    // For literal values, use actual statistics
                    if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if column.eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                        (&**left, &**right)
                    {
                        if column.eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    // For placeholder parameters, estimate using 1/n_distinct
                    // This is more accurate than the generic 0.33 fallback for equality predicates
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_lit = is_literal(left);
                    let right_is_lit = is_literal(right);

                    if (left_is_col && right_is_lit) || (left_is_lit && right_is_col) {
                        // Use 1/n_distinct as selectivity estimate for equality with parameter
                        if col_stats.n_distinct > 0 {
                            return 1.0 / col_stats.n_distinct as f64;
                        }
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Range predicates (case-insensitive column comparison)
                    if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if column.eq_ignore_ascii_case(column_name) {
                            let op_str = match op {
                                vibesql_ast::BinaryOperator::GreaterThan => ">",
                                vibesql_ast::BinaryOperator::GreaterThanOrEqual => ">=",
                                vibesql_ast::BinaryOperator::LessThan => "<",
                                vibesql_ast::BinaryOperator::LessThanOrEqual => "<=",
                                _ => unreachable!(),
                            };
                            return col_stats.estimate_range_selectivity(value, op_str);
                        }
                    }
                    // For placeholder parameters, use a conservative 0.25 (assume filtering ~75% of rows)
                    if (is_column_reference(left, column_name) && is_literal(right))
                        || (is_literal(left) && is_column_reference(right, column_name))
                    {
                        return 0.25;
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::And => {
                    // For AND, multiply selectivities (assuming independence)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel * right_sel
                }
                vibesql_ast::BinaryOperator::Or => {
                    // For OR, use formula: P(A OR B) = P(A) + P(B) - P(A AND B)
                    // Assuming independence: P(A OR B) = P(A) + P(B) - P(A)*P(B)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel + right_sel - (left_sel * right_sel)
                }
                _ => 0.33, // Default fallback for other operators
            }
        }
        Expression::Between { expr, low, high, negated: _, symmetric: _ } => {
            if let Expression::ColumnRef { column, .. } = &**expr {
                if column.eq_ignore_ascii_case(column_name) {
                    // Estimate BETWEEN as: P(col >= low AND col <= high)
                    if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                        (&**low, &**high)
                    {
                        let low_sel = col_stats.estimate_range_selectivity(low_val, ">=");
                        let high_sel = col_stats.estimate_range_selectivity(high_val, "<=");
                        return low_sel * high_sel; // Assuming independence
                    }
                }
            }
            0.33 // Default fallback
        }
        _ => 0.33, // Default fallback for unsupported expressions
    }
}

/// Unified index selection that returns IndexScanChoice
///
/// This function first tries regular index selection, and if no suitable index is found,
/// it attempts skip-scan optimization as a fallback. Skip-scan enables using composite
/// indexes when the WHERE clause filters on non-prefix columns.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some(IndexScanChoice::Regular {...})` if a regular index scan should be used
/// - `Some(IndexScanChoice::SkipScan {...})` if skip-scan is beneficial
/// - `None` if table scan is more appropriate
///
/// # Example
/// ```text
/// // Query: SELECT * FROM sales WHERE date = '2024-01-01'
/// // Index: (region, date) - a composite index
/// //
/// // Regular index selection fails (no filter on 'region' prefix column)
/// // Skip-scan is considered: iterate through distinct 'region' values,
/// // for each region, seek to entries with date = '2024-01-01'
/// //
/// // If skip-scan cost < table scan cost, returns IndexScanChoice::SkipScan
/// ```
pub(crate) fn select_index_scan_method(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<IndexScanChoice> {
    // First, try regular cost-based index selection
    if let Some((index_name, sorted_columns)) =
        cost_based_index_selection(table_name, where_clause, order_by, database)
    {
        return Some(IndexScanChoice::Regular { index_name, sorted_columns });
    }

    // If regular index selection failed and we have a WHERE clause,
    // try skip-scan optimization
    if let Some(where_expr) = where_clause {
        let planner = IndexPlanner::new(database);
        if let Some(plan) = planner.plan_skip_scan(table_name, where_expr) {
            if plan.is_skip_scan {
                if let Some(skip_info) = plan.skip_scan_info {
                    if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                        eprintln!(
                            "[SKIP_SCAN] Selected skip-scan for table={}, index={}, filter_col={}, prefix_cardinality={}",
                            table_name,
                            plan.index_name,
                            skip_info.filter_column,
                            skip_info.prefix_cardinality
                        );
                    }
                    return Some(IndexScanChoice::SkipScan {
                        index_name: plan.index_name,
                        skip_scan_info: skip_info,
                    });
                }
            }
        }
    }

    // No index or skip-scan option found
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    #[test]
    fn test_expression_filters_column_simple() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_expression_filters_column_and() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "city".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Boston")))),
            }),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(expression_filters_column(&expr, "city"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference() {
        let expr = Expression::ColumnRef { table: None, column: "age".to_string() };

        assert!(is_column_reference(&expr, "age"));
        assert!(!is_column_reference(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference_case_insensitive() {
        // SQL parser normalizes unquoted identifiers to uppercase
        // but index columns might be lowercase
        let expr = Expression::ColumnRef {
            table: None,
            column: "I_ID".to_string(), // Uppercase from parser
        };

        // Should match regardless of case
        assert!(is_column_reference(&expr, "i_id")); // lowercase
        assert!(is_column_reference(&expr, "I_ID")); // exact match
        assert!(is_column_reference(&expr, "I_id")); // mixed case
        assert!(!is_column_reference(&expr, "other_column"));
    }

    #[test]
    fn test_expression_filters_column_case_insensitive() {
        // WHERE I_ID = 42 (uppercase from parser)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "I_ID".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        // Should match lowercase index column name
        assert!(expression_filters_column(&expr, "i_id"));
        assert!(expression_filters_column(&expr, "I_ID"));
        assert!(!expression_filters_column(&expr, "other"));
    }
}
