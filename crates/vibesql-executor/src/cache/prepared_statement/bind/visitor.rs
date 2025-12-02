//! Visitor pattern for traversing AST and counting placeholders
//!
//! This module provides functions to traverse SQL statements and visit all expressions,
//! used for counting and collecting placeholder parameters.

use vibesql_ast::{
    DeleteStmt, Expression, FromClause, InsertSource, InsertStmt, SelectItem, SelectStmt,
    Statement, UpdateStmt, WhereClause,
};

/// Visit all expressions in a statement (for counting placeholders)
pub fn visit_statement<F>(stmt: &Statement, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    match stmt {
        Statement::Select(select) => visit_select(select, visitor),
        Statement::Insert(insert) => visit_insert(insert, visitor),
        Statement::Update(update) => visit_update(update, visitor),
        Statement::Delete(delete) => visit_delete(delete, visitor),
        _ => {}
    }
}

fn visit_select<F>(stmt: &SelectStmt, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    // Visit CTEs
    if let Some(ctes) = &stmt.with_clause {
        for cte in ctes {
            visit_select(&cte.query, visitor);
        }
    }

    // Visit select items
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            visit_expression(expr, visitor);
        }
    }

    // Visit FROM clause
    if let Some(from) = &stmt.from {
        visit_from_clause(from, visitor);
    }

    // Visit WHERE
    if let Some(where_clause) = &stmt.where_clause {
        visit_expression(where_clause, visitor);
    }

    // Visit GROUP BY
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            visit_expression(expr, visitor);
        }
    }

    // Visit HAVING
    if let Some(having) = &stmt.having {
        visit_expression(having, visitor);
    }

    // Visit ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            visit_expression(&item.expr, visitor);
        }
    }

    // Visit set operation
    if let Some(set_op) = &stmt.set_operation {
        visit_select(&set_op.right, visitor);
    }
}

fn visit_from_clause<F>(from: &FromClause, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    match from {
        FromClause::Table { .. } => {}
        FromClause::Subquery { query, .. } => visit_select(query, visitor),
        FromClause::Join { left, right, condition, .. } => {
            visit_from_clause(left, visitor);
            visit_from_clause(right, visitor);
            if let Some(cond) = condition {
                visit_expression(cond, visitor);
            }
        }
    }
}

fn visit_insert<F>(stmt: &InsertStmt, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    match &stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    visit_expression(expr, visitor);
                }
            }
        }
        InsertSource::Select(select) => visit_select(select, visitor),
    }

    if let Some(updates) = &stmt.on_duplicate_key_update {
        for assignment in updates {
            visit_expression(&assignment.value, visitor);
        }
    }
}

fn visit_update<F>(stmt: &UpdateStmt, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    for assignment in &stmt.assignments {
        visit_expression(&assignment.value, visitor);
    }
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        visit_expression(expr, visitor);
    }
}

fn visit_delete<F>(stmt: &DeleteStmt, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        visit_expression(expr, visitor);
    }
}

fn visit_expression<F>(expr: &Expression, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    visitor(expr);

    match expr {
        Expression::BinaryOp { left, right, .. } => {
            visit_expression(left, visitor);
            visit_expression(right, visitor);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            visit_expression(inner, visitor);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                visit_expression(arg, visitor);
            }
        }
        Expression::IsNull { expr: inner, .. } => {
            visit_expression(inner, visitor);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                visit_expression(op, visitor);
            }
            for w in when_clauses {
                for c in &w.conditions {
                    visit_expression(c, visitor);
                }
                visit_expression(&w.result, visitor);
            }
            if let Some(e) = else_result {
                visit_expression(e, visitor);
            }
        }
        Expression::ScalarSubquery(select) => visit_select(select, visitor),
        Expression::In { expr: inner, subquery, .. } => {
            visit_expression(inner, visitor);
            visit_select(subquery, visitor);
        }
        Expression::InList { expr: inner, values, .. } => {
            visit_expression(inner, visitor);
            for v in values {
                visit_expression(v, visitor);
            }
        }
        Expression::Between { expr: inner, low, high, .. } => {
            visit_expression(inner, visitor);
            visit_expression(low, visitor);
            visit_expression(high, visitor);
        }
        Expression::Cast { expr: inner, .. } => {
            visit_expression(inner, visitor);
        }
        Expression::Position { substring, string, .. } => {
            visit_expression(substring, visitor);
            visit_expression(string, visitor);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(c) = removal_char {
                visit_expression(c, visitor);
            }
            visit_expression(string, visitor);
        }
        Expression::Extract { expr: inner, .. } => {
            visit_expression(inner, visitor);
        }
        Expression::Like { expr: inner, pattern, .. } => {
            visit_expression(inner, visitor);
            visit_expression(pattern, visitor);
        }
        Expression::Exists { subquery, .. } => {
            visit_select(subquery, visitor);
        }
        Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
            visit_expression(inner, visitor);
            visit_select(subquery, visitor);
        }
        Expression::Interval { value, .. } => {
            visit_expression(value, visitor);
        }
        Expression::WindowFunction { function, over } => {
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    for arg in args {
                        visit_expression(arg, visitor);
                    }
                }
            }
            if let Some(partition_by) = &over.partition_by {
                for expr in partition_by {
                    visit_expression(expr, visitor);
                }
            }
            if let Some(order_by) = &over.order_by {
                for item in order_by {
                    visit_expression(&item.expr, visitor);
                }
            }
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            visit_expression(search_modifier, visitor);
        }
        // Leaf nodes
        Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => {}
    }
}
