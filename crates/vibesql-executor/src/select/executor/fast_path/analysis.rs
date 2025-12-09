//! Query analysis functions for fast path detection
//!
//! This module provides functions to analyze queries and determine if they qualify
//! for various fast execution paths. These functions are pure analysis with no
//! execution side effects.

use vibesql_ast::{Expression, OrderByItem, OrderDirection, SelectItem, SelectStmt};

/// Check if a query is a simple point-lookup that can use the fast path
///
/// Returns true for queries that:
/// 1. Query a single table (no joins, no subqueries in FROM)
/// 2. Have no WITH clause (CTEs)
/// 3. Have no aggregates or window functions
/// 4. Have no GROUP BY, HAVING, DISTINCT, or set operations
/// 5. Have no ORDER BY with complex expressions
/// 6. Have a simple WHERE clause (only AND-connected equality predicates)
pub fn is_simple_point_query(stmt: &SelectStmt) -> bool {
    // No CTEs
    if stmt.with_clause.is_some() {
        return false;
    }

    // No set operations (UNION, INTERSECT, EXCEPT)
    if stmt.set_operation.is_some() {
        return false;
    }

    // No SELECT INTO (DDL or procedural variable assignment)
    // These require special handling that the fast path doesn't support
    if stmt.into_table.is_some() || stmt.into_variables.is_some() {
        return false;
    }

    // No GROUP BY, HAVING, or DISTINCT
    if stmt.group_by.is_some() || stmt.having.is_some() || stmt.distinct {
        return false;
    }

    // Must have a FROM clause
    let Some(from) = &stmt.from else {
        return false;
    };

    // FROM must be a simple table (no joins, no subqueries)
    if !matches!(from, vibesql_ast::FromClause::Table { .. }) {
        return false;
    }

    // SELECT list must be simple columns or * (no aggregates, no subqueries)
    if !has_simple_select_list(&stmt.select_list) {
        return false;
    }

    // WHERE clause must be simple equality predicates (if present)
    if let Some(where_clause) = &stmt.where_clause {
        if !is_simple_where_clause(where_clause) {
            return false;
        }
    }

    // ORDER BY is allowed if it's simple (column references only, no complex expressions)
    // and doesn't use SELECT list aliases (which require post-projection sorting)
    // The index scan logic will automatically use index ordering when possible
    if let Some(order_by) = &stmt.order_by {
        if !is_simple_order_by(order_by) {
            return false;
        }
        // Check that ORDER BY doesn't use SELECT list aliases
        // Fast path sorts before projection, so aliases can't be resolved
        if uses_select_alias(order_by, &stmt.select_list) {
            return false;
        }
    }

    true
}

/// Check if a query is a streaming aggregate that can use the ultra-fast path
///
/// Returns true for queries that:
/// 1. Query a single table (no joins)
/// 2. Have no CTEs, set operations, GROUP BY, HAVING, DISTINCT, ORDER BY
/// 3. Have a simple WHERE clause with PK range predicate (BETWEEN)
/// 4. SELECT list contains only simple aggregates (SUM, COUNT, AVG, MIN, MAX)
///    on single column references (not DISTINCT aggregates)
///
/// # Example queries that use this path:
/// ```sql
/// SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100
/// SELECT COUNT(c), SUM(k) FROM sbtest1 WHERE id BETWEEN 50 AND 150
/// ```
pub fn is_streaming_aggregate_query(stmt: &SelectStmt) -> bool {
    // No CTEs
    if stmt.with_clause.is_some() {
        return false;
    }

    // No set operations (UNION, INTERSECT, EXCEPT)
    if stmt.set_operation.is_some() {
        return false;
    }

    // No GROUP BY, HAVING, DISTINCT, ORDER BY
    if stmt.group_by.is_some() || stmt.having.is_some() || stmt.distinct {
        return false;
    }

    if stmt.order_by.is_some() {
        return false;
    }

    // Must have a FROM clause (simple table)
    let Some(from) = &stmt.from else {
        return false;
    };

    // FROM must be a simple table (no joins)
    if !matches!(from, vibesql_ast::FromClause::Table { .. }) {
        return false;
    }

    // Must have a WHERE clause
    if stmt.where_clause.is_none() {
        return false;
    }

    // SELECT list must contain only simple aggregates
    if !has_simple_aggregate_select_list(&stmt.select_list) {
        return false;
    }

    true
}

/// Check if a SELECT list contains only simple aggregates (SUM, COUNT, AVG, MIN, MAX)
/// on single column references without DISTINCT.
pub(crate) fn has_simple_aggregate_select_list(select_list: &[SelectItem]) -> bool {
    if select_list.is_empty() {
        return false;
    }

    for item in select_list {
        match item {
            SelectItem::Expression { expr, .. } => {
                if !is_simple_aggregate_expression(expr) {
                    return false;
                }
            }
            // Wildcards are not aggregates
            _ => return false,
        }
    }
    true
}

/// Check if an expression is a simple aggregate function call
pub(crate) fn is_simple_aggregate_expression(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { name, args, distinct } => {
            // Only support non-DISTINCT aggregates for streaming
            if *distinct {
                return false;
            }

            // Only support standard aggregate functions
            let upper_name = name.to_uppercase();
            if !matches!(upper_name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX") {
                return false;
            }

            // Must have exactly one argument (column reference)
            // COUNT(*) is excluded until properly supported in extract_simple_aggregate
            if args.len() != 1 {
                return false;
            }

            // Argument must be a simple column reference
            matches!(&args[0], Expression::ColumnRef { .. })
        }
        _ => false,
    }
}

/// Extract the aggregate function name and column index from a SELECT item
pub(crate) fn extract_simple_aggregate(
    expr: &Expression,
    table_schema: &vibesql_catalog::TableSchema,
) -> Option<(String, usize)> {
    match expr {
        Expression::AggregateFunction { name, args, distinct } => {
            if *distinct {
                return None;
            }

            let upper_name = name.to_uppercase();
            if !matches!(upper_name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX") {
                return None;
            }

            // Get the column reference
            if args.len() != 1 {
                return None;
            }

            if let Expression::ColumnRef { column, .. } = &args[0] {
                // Find column index
                let col_idx = table_schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column))?;
                return Some((upper_name, col_idx));
            }

            None
        }
        _ => None,
    }
}

/// Check if an ORDER BY clause is simple enough for the fast path
///
/// Returns true if all ORDER BY items are simple column references.
/// Complex expressions (functions, arithmetic, subqueries) are not supported.
///
/// Examples:
/// - `ORDER BY col ASC` -> true
/// - `ORDER BY col1, col2 DESC` -> true
/// - `ORDER BY col LIMIT 1` -> true (LIMIT doesn't affect ORDER BY simplicity)
/// - `ORDER BY UPPER(col)` -> false (function call)
/// - `ORDER BY col + 1` -> false (arithmetic expression)
pub(crate) fn is_simple_order_by(order_by: &[OrderByItem]) -> bool {
    for item in order_by {
        // ORDER BY expression must be a simple column reference
        if !matches!(item.expr, Expression::ColumnRef { .. }) {
            return false;
        }
    }
    true
}

/// Check if ORDER BY uses any SELECT list aliases
///
/// Returns true if any ORDER BY column matches a SELECT list alias.
/// This is used to exclude such queries from the fast path, since
/// the fast path sorts before projection and can't resolve aliases.
pub(crate) fn uses_select_alias(order_by: &[OrderByItem], select_list: &[SelectItem]) -> bool {
    // Collect all aliases from the SELECT list
    let aliases: Vec<&str> = select_list
        .iter()
        .filter_map(|item| {
            if let SelectItem::Expression { alias: Some(alias), .. } = item {
                Some(alias.as_str())
            } else {
                None
            }
        })
        .collect();

    // If no aliases, no conflict possible
    if aliases.is_empty() {
        return false;
    }

    // Check if any ORDER BY column matches an alias
    for item in order_by {
        if let Expression::ColumnRef { table: None, column } = &item.expr {
            // Case-insensitive comparison for SQL identifiers
            if aliases.iter().any(|alias| alias.eq_ignore_ascii_case(column)) {
                return true;
            }
        }
    }

    false
}

/// Check if we need to apply explicit sorting (index didn't provide the order)
///
/// Returns true if ORDER BY columns don't match the sorted_by metadata from index scan.
pub fn needs_sorting(
    order_by: &[OrderByItem],
    sorted_by: &Option<Vec<(String, OrderDirection)>>,
) -> bool {
    let Some(sorted_cols) = sorted_by else {
        return true; // No sorting metadata, need to sort
    };

    // Check if ORDER BY is a prefix of sorted_by with matching directions
    if order_by.len() > sorted_cols.len() {
        return true; // ORDER BY has more columns than sorted
    }

    for (order_item, (col_name, col_dir)) in order_by.iter().zip(sorted_cols.iter()) {
        // Extract column name from ORDER BY expression
        let order_col = match &order_item.expr {
            Expression::ColumnRef { column, .. } => column,
            _ => return true, // Non-column expression, need to sort
        };

        // Check column name matches (case-insensitive)
        if !order_col.eq_ignore_ascii_case(col_name) {
            return true;
        }

        // Check direction matches
        if &order_item.direction != col_dir {
            return true;
        }
    }

    false // Sorting is already satisfied
}

/// Check if a SELECT list contains only simple columns or *
pub(crate) fn has_simple_select_list(select_list: &[SelectItem]) -> bool {
    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => continue,
            SelectItem::Expression { expr, .. } => {
                if !is_simple_expression(expr) {
                    return false;
                }
            }
        }
    }
    true
}

/// Check if an expression is simple (column ref, literal, or basic arithmetic)
pub(crate) fn is_simple_expression(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { .. } | Expression::Literal(_) => true,
        Expression::BinaryOp { left, right, op } => {
            // Allow simple arithmetic on columns/literals
            matches!(
                op,
                vibesql_ast::BinaryOperator::Plus
                    | vibesql_ast::BinaryOperator::Minus
                    | vibesql_ast::BinaryOperator::Multiply
                    | vibesql_ast::BinaryOperator::Divide
                    | vibesql_ast::BinaryOperator::Concat
            ) && is_simple_expression(left)
                && is_simple_expression(right)
        }
        Expression::UnaryOp { expr, .. } => is_simple_expression(expr),
        Expression::Cast { expr, .. } => is_simple_expression(expr),
        // Functions are not simple (could be aggregates or expensive)
        _ => false,
    }
}

/// Check if a WHERE clause is simple (only AND-connected equality/comparison predicates)
pub(crate) fn is_simple_where_clause(expr: &Expression) -> bool {
    match expr {
        // Simple comparison: col = val, col > val, etc.
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::NotEqual
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Must be column vs literal (not column vs column for join conditions)
                    is_column_or_literal(left) && is_column_or_literal(right)
                }
                vibesql_ast::BinaryOperator::And => {
                    // AND is fine - recurse
                    is_simple_where_clause(left) && is_simple_where_clause(right)
                }
                // OR could be optimized but is more complex
                vibesql_ast::BinaryOperator::Or => false,
                _ => false,
            }
        }
        // BETWEEN is simple
        Expression::Between { expr, low, high, .. } => {
            is_column_or_literal(expr) && is_column_or_literal(low) && is_column_or_literal(high)
        }
        // IN list is simple (not IN subquery)
        Expression::InList { expr, values, .. } => {
            is_column_or_literal(expr) && values.iter().all(is_column_or_literal)
        }
        // IS NULL is simple
        Expression::IsNull { expr, .. } => is_column_or_literal(expr),
        // LIKE is simple
        Expression::Like { expr, pattern, .. } => {
            is_column_or_literal(expr) && is_column_or_literal(pattern)
        }
        _ => false,
    }
}

/// Check if an expression is a column reference or literal
pub(crate) fn is_column_or_literal(expr: &Expression) -> bool {
    matches!(expr, Expression::ColumnRef { .. } | Expression::Literal(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::Statement;
    use vibesql_parser::Parser;

    fn parse_select(sql: &str) -> SelectStmt {
        match Parser::parse_sql(sql).unwrap() {
            Statement::Select(stmt) => *stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_simple_point_query_detection() {
        // Simple point queries should be detected
        assert!(is_simple_point_query(&parse_select("SELECT w_tax FROM warehouse WHERE w_id = 1")));
        assert!(is_simple_point_query(&parse_select("SELECT * FROM users WHERE id = 123")));
        assert!(is_simple_point_query(&parse_select("SELECT a, b FROM t WHERE x = 1 AND y = 2")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x > 10")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x BETWEEN 1 AND 10")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x IN (1, 2, 3)")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x IS NULL")));
    }

    #[test]
    fn test_non_simple_query_detection() {
        // Complex queries should not be detected as simple
        assert!(!is_simple_point_query(&parse_select("SELECT COUNT(*) FROM t WHERE id = 1")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t1, t2 WHERE t1.id = t2.id")));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE id IN (SELECT id FROM t2)"
        )));
        assert!(!is_simple_point_query(&parse_select("SELECT DISTINCT a FROM t")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t GROUP BY a")));
        assert!(!is_simple_point_query(&parse_select("WITH cte AS (SELECT 1) SELECT * FROM cte")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t UNION SELECT b FROM t2")));
    }

    #[test]
    fn test_or_not_simple() {
        // OR predicates are not simple (could be optimized later)
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t WHERE x = 1 OR y = 2")));
    }

    #[test]
    fn test_order_by_simple_queries() {
        // Simple ORDER BY with column references should be detected as simple
        assert!(is_simple_point_query(&parse_select(
            "SELECT no_o_id FROM new_order WHERE no_w_id = 1 ORDER BY no_o_id"
        )));
        assert!(is_simple_point_query(&parse_select(
            "SELECT * FROM t WHERE id = 1 ORDER BY col ASC"
        )));
        assert!(is_simple_point_query(&parse_select(
            "SELECT a, b FROM t WHERE x = 1 ORDER BY a DESC"
        )));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x = 1 ORDER BY a, b")));
        assert!(is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a DESC, b ASC"
        )));
        // ORDER BY with LIMIT
        assert!(is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a LIMIT 1"
        )));
    }

    #[test]
    fn test_order_by_complex_not_simple() {
        // Complex ORDER BY expressions should not be detected as simple
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY UPPER(a)"
        )));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a + 1"
        )));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY COALESCE(a, b)"
        )));
    }

    #[test]
    fn test_needs_sorting() {
        // No sorted_by means we need to sort
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &None
        ));

        // Matching sorted_by means no sorting needed
        assert!(!needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // Different column means sorting needed
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "b".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // Different direction means sorting needed
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Desc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // ORDER BY prefix of sorted_by is OK
        assert!(!needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![
                ("a".to_string(), vibesql_ast::OrderDirection::Asc),
                ("b".to_string(), vibesql_ast::OrderDirection::Asc),
            ])
        ));

        // ORDER BY with more columns than sorted_by needs sorting
        assert!(needs_sorting(
            &[
                vibesql_ast::OrderByItem {
                    expr: vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "a".to_string()
                    },
                    direction: vibesql_ast::OrderDirection::Asc,
                },
                vibesql_ast::OrderByItem {
                    expr: vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "b".to_string()
                    },
                    direction: vibesql_ast::OrderDirection::Asc,
                },
            ],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));
    }

    #[test]
    fn test_streaming_aggregate_detection() {
        // Simple streaming aggregate queries should be detected
        assert!(is_streaming_aggregate_query(&parse_select(
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));
        assert!(is_streaming_aggregate_query(&parse_select(
            "SELECT COUNT(c) FROM sbtest1 WHERE id BETWEEN 50 AND 150"
        )));
        assert!(is_streaming_aggregate_query(&parse_select(
            "SELECT AVG(k), SUM(k), COUNT(k) FROM sbtest1 WHERE id BETWEEN 1 AND 10"
        )));
        assert!(is_streaming_aggregate_query(&parse_select(
            "SELECT MIN(k), MAX(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));
    }

    #[test]
    fn test_streaming_aggregate_negative_cases() {
        // Non-streaming aggregate queries should not be detected

        // Regular column selection (not aggregate)
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT c FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));

        // WITH clause not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "WITH cte AS (SELECT 1) SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));

        // GROUP BY not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100 GROUP BY c"
        )));

        // DISTINCT not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT DISTINCT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));

        // ORDER BY not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100 ORDER BY 1"
        )));

        // HAVING not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100 HAVING SUM(k) > 10"
        )));

        // No WHERE clause
        assert!(!is_streaming_aggregate_query(&parse_select("SELECT SUM(k) FROM sbtest1")));

        // DISTINCT aggregate not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT COUNT(DISTINCT k) FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));

        // Wildcard not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT * FROM sbtest1 WHERE id BETWEEN 1 AND 100"
        )));

        // Join not supported
        assert!(!is_streaming_aggregate_query(&parse_select(
            "SELECT SUM(a.k) FROM sbtest1 a, sbtest1 b WHERE a.id BETWEEN 1 AND 100"
        )));
    }
}
