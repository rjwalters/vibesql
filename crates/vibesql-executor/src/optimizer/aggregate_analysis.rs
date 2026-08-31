//! Aggregate detection helper
//!
//! Checks whether a SELECT statement contains aggregates (a GROUP BY clause
//! or an aggregate function call). Used by the columnar join path to decide
//! whether it must fall back to row-oriented execution, since aggregates
//! without GROUP BY aren't supported there.

#![allow(clippy::redundant_closure, clippy::unnecessary_map_or)]

use vibesql_ast::{Expression, SelectStmt};

/// Returns true if the statement has a GROUP BY clause or any aggregate function
/// in the SELECT list or HAVING clause.
pub fn has_aggregates(stmt: &SelectStmt) -> bool {
    stmt.group_by.is_some() || has_aggregate_functions(stmt)
}

/// Check if a SELECT statement contains any aggregate functions
fn has_aggregate_functions(stmt: &SelectStmt) -> bool {
    // Check SELECT list
    for select_item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = select_item {
            if contains_aggregate(expr) {
                return true;
            }
        }
    }

    // Check HAVING clause
    if let Some(ref having) = stmt.having {
        if contains_aggregate(having) {
            return true;
        }
    }

    false
}

/// Check if an expression contains an aggregate function
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if contains_aggregate(op) {
                    return true;
                }
            }
            for when_clause in when_clauses {
                for condition in &when_clause.conditions {
                    if contains_aggregate(condition) {
                        return true;
                    }
                }
                if contains_aggregate(&when_clause.result) {
                    return true;
                }
            }
            if let Some(else_expr) = else_result {
                if contains_aggregate(else_expr) {
                    return true;
                }
            }
            false
        }
        Expression::Function { args, .. } => args.iter().any(|arg| contains_aggregate(arg)),
        Expression::IsNull { expr, .. } => contains_aggregate(expr),
        Expression::In { expr, .. } => contains_aggregate(expr),
        Expression::InList { expr, values, .. } => {
            contains_aggregate(expr) || values.iter().any(|v| contains_aggregate(v))
        }
        Expression::Between { expr, low, high, .. } => {
            contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high)
        }
        Expression::Cast { expr, .. } => contains_aggregate(expr),
        Expression::Position { substring, string, .. } => {
            contains_aggregate(substring) || contains_aggregate(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().map_or(false, |rc| contains_aggregate(rc))
                || contains_aggregate(string)
        }
        Expression::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{Expression, GroupByClause, SelectItem, SelectStmt};

    use super::*;

    fn make_column_ref(table: &str, column: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(table, false, column, false))
    }

    fn make_aggregate(name: &str, arg: Expression) -> Expression {
        Expression::AggregateFunction {
            name: vibesql_ast::FunctionIdentifier::new(name),
            distinct: false,
            args: vec![arg],
            order_by: None,
            filter: None,
        }
    }

    fn base_stmt() -> SelectStmt {
        SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: Vec::new(),
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_no_aggregates() {
        let mut stmt = base_stmt();
        stmt.select_list = vec![SelectItem::Expression {
            expr: make_column_ref("users", "name"),
            alias: None,
            source_text: None,
        }];

        assert!(!has_aggregates(&stmt));
    }

    #[test]
    fn test_group_by_alone_counts_as_aggregate() {
        let mut stmt = base_stmt();
        stmt.select_list = vec![SelectItem::Expression {
            expr: make_column_ref("orders", "customer_id"),
            alias: None,
            source_text: None,
        }];
        stmt.group_by = Some(GroupByClause::Simple(vec![make_column_ref("orders", "customer_id")]));

        assert!(has_aggregates(&stmt));
    }

    #[test]
    fn test_aggregate_function_in_select_list() {
        let mut stmt = base_stmt();
        stmt.select_list = vec![SelectItem::Expression {
            expr: make_aggregate("COUNT", Expression::Wildcard),
            alias: Some("order_count".to_string()),
            source_text: None,
        }];

        assert!(has_aggregates(&stmt));
    }

    #[test]
    fn test_aggregate_function_in_having() {
        let mut stmt = base_stmt();
        stmt.select_list = vec![SelectItem::Expression {
            expr: make_column_ref("lineitem", "l_orderkey"),
            alias: None,
            source_text: None,
        }];
        stmt.having = Some(Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(make_aggregate("SUM", make_column_ref("lineitem", "l_quantity"))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(300))),
        });

        assert!(has_aggregates(&stmt));
    }
}
