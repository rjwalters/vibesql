//! AST-level parameter binding for prepared statements
//!
//! This module provides functions to bind parameters to prepared statements
//! by replacing Placeholder expressions in the AST with Literal values,
//! avoiding the overhead of re-parsing SQL strings.

use vibesql_ast::{
    Assignment, CommonTableExpr, DeleteStmt, Expression, FromClause, GroupByClause,
    GroupingElement, GroupingSet, InsertSource, InsertStmt, MixedGroupingItem, OrderByItem,
    SelectItem, SelectStmt, SetOperation, Statement, UpdateStmt, WhereClause,
};
use vibesql_types::SqlValue;

/// Count the number of placeholder parameters in a statement
pub fn count_placeholders(stmt: &Statement) -> usize {
    let mut count = 0;
    visit_statement(stmt, &mut |expr| {
        if matches!(expr, Expression::Placeholder(_)) {
            count += 1;
        }
    });
    count
}

/// Bind parameters to a statement by replacing Placeholder expressions with Literal values
///
/// Returns a new statement with all placeholders replaced. The params slice must have
/// exactly the right number of parameters (one for each placeholder index).
pub fn bind_parameters(stmt: &Statement, params: &[SqlValue]) -> Statement {
    match stmt {
        Statement::Select(select) => Statement::Select(Box::new(bind_select(select, params))),
        Statement::Insert(insert) => Statement::Insert(bind_insert(insert, params)),
        Statement::Update(update) => Statement::Update(bind_update(update, params)),
        Statement::Delete(delete) => Statement::Delete(bind_delete(delete, params)),
        // Other statement types don't typically have placeholders
        other => other.clone(),
    }
}

/// Bind parameters in a SELECT statement
fn bind_select(stmt: &SelectStmt, params: &[SqlValue]) -> SelectStmt {
    SelectStmt {
        with_clause: stmt.with_clause.as_ref().map(|ctes| {
            ctes.iter()
                .map(|cte| CommonTableExpr {
                    name: cte.name.clone(),
                    columns: cte.columns.clone(),
                    query: Box::new(bind_select(&cte.query, params)),
                })
                .collect()
        }),
        distinct: stmt.distinct,
        select_list: stmt
            .select_list
            .iter()
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => SelectItem::Expression {
                    expr: bind_expression(expr, params),
                    alias: alias.clone(),
                },
                SelectItem::Wildcard { alias } => SelectItem::Wildcard { alias: alias.clone() },
                SelectItem::QualifiedWildcard { qualifier, alias } => {
                    SelectItem::QualifiedWildcard {
                        qualifier: qualifier.clone(),
                        alias: alias.clone(),
                    }
                }
            })
            .collect(),
        into_table: stmt.into_table.clone(),
        into_variables: stmt.into_variables.clone(),
        from: stmt.from.as_ref().map(|f| bind_from_clause(f, params)),
        where_clause: stmt.where_clause.as_ref().map(|e| bind_expression(e, params)),
        group_by: stmt.group_by.as_ref().map(|g| bind_group_by(g, params)),
        having: stmt.having.as_ref().map(|e| bind_expression(e, params)),
        order_by: stmt
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|o| bind_order_by(o, params)).collect()),
        limit: stmt.limit,
        offset: stmt.offset,
        set_operation: stmt.set_operation.as_ref().map(|op| SetOperation {
            op: op.op.clone(),
            all: op.all,
            right: Box::new(bind_select(&op.right, params)),
        }),
    }
}

/// Bind parameters in GROUP BY clause
fn bind_group_by(clause: &GroupByClause, params: &[SqlValue]) -> GroupByClause {
    match clause {
        GroupByClause::Simple(exprs) => {
            GroupByClause::Simple(exprs.iter().map(|e| bind_expression(e, params)).collect())
        }
        GroupByClause::Rollup(elements) => GroupByClause::Rollup(bind_grouping_elements(elements, params)),
        GroupByClause::Cube(elements) => GroupByClause::Cube(bind_grouping_elements(elements, params)),
        GroupByClause::GroupingSets(sets) => {
            GroupByClause::GroupingSets(sets.iter().map(|s| bind_grouping_set(s, params)).collect())
        }
        GroupByClause::Mixed(items) => GroupByClause::Mixed(
            items
                .iter()
                .map(|item| match item {
                    MixedGroupingItem::Simple(expr) => {
                        MixedGroupingItem::Simple(bind_expression(expr, params))
                    }
                    MixedGroupingItem::Rollup(elements) => {
                        MixedGroupingItem::Rollup(bind_grouping_elements(elements, params))
                    }
                    MixedGroupingItem::Cube(elements) => {
                        MixedGroupingItem::Cube(bind_grouping_elements(elements, params))
                    }
                    MixedGroupingItem::GroupingSets(sets) => MixedGroupingItem::GroupingSets(
                        sets.iter().map(|s| bind_grouping_set(s, params)).collect(),
                    ),
                })
                .collect(),
        ),
    }
}

fn bind_grouping_elements(elements: &[GroupingElement], params: &[SqlValue]) -> Vec<GroupingElement> {
    elements
        .iter()
        .map(|e| match e {
            GroupingElement::Single(expr) => GroupingElement::Single(bind_expression(expr, params)),
            GroupingElement::Composite(exprs) => {
                GroupingElement::Composite(exprs.iter().map(|e| bind_expression(e, params)).collect())
            }
        })
        .collect()
}

fn bind_grouping_set(set: &GroupingSet, params: &[SqlValue]) -> GroupingSet {
    GroupingSet {
        columns: set.columns.iter().map(|e| bind_expression(e, params)).collect(),
    }
}

/// Bind parameters in an INSERT statement
fn bind_insert(stmt: &InsertStmt, params: &[SqlValue]) -> InsertStmt {
    InsertStmt {
        table_name: stmt.table_name.clone(),
        columns: stmt.columns.clone(),
        source: match &stmt.source {
            InsertSource::Values(rows) => InsertSource::Values(
                rows.iter()
                    .map(|row| row.iter().map(|e| bind_expression(e, params)).collect())
                    .collect(),
            ),
            InsertSource::Select(select) => InsertSource::Select(Box::new(bind_select(select, params))),
        },
        conflict_clause: stmt.conflict_clause.clone(),
        on_duplicate_key_update: stmt.on_duplicate_key_update.as_ref().map(|updates| {
            updates
                .iter()
                .map(|a| Assignment {
                    column: a.column.clone(),
                    value: bind_expression(&a.value, params),
                })
                .collect()
        }),
    }
}

/// Bind parameters in an UPDATE statement
fn bind_update(stmt: &UpdateStmt, params: &[SqlValue]) -> UpdateStmt {
    UpdateStmt {
        table_name: stmt.table_name.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|a| Assignment {
                column: a.column.clone(),
                value: bind_expression(&a.value, params),
            })
            .collect(),
        where_clause: stmt.where_clause.as_ref().map(|w| match w {
            WhereClause::Condition(expr) => WhereClause::Condition(bind_expression(expr, params)),
            WhereClause::CurrentOf(cursor) => WhereClause::CurrentOf(cursor.clone()),
        }),
    }
}

/// Bind parameters in a DELETE statement
fn bind_delete(stmt: &DeleteStmt, params: &[SqlValue]) -> DeleteStmt {
    DeleteStmt {
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        where_clause: stmt.where_clause.as_ref().map(|w| match w {
            WhereClause::Condition(expr) => WhereClause::Condition(bind_expression(expr, params)),
            WhereClause::CurrentOf(cursor) => WhereClause::CurrentOf(cursor.clone()),
        }),
    }
}

/// Bind parameters in an expression
fn bind_expression(expr: &Expression, params: &[SqlValue]) -> Expression {
    match expr {
        // The key case: replace placeholders with literal values
        Expression::Placeholder(idx) => {
            if *idx < params.len() {
                Expression::Literal(params[*idx].clone())
            } else {
                // Should not happen if param count was validated
                expr.clone()
            }
        }

        // Literals and other leaf nodes: return as-is
        Expression::Literal(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::Default
        | Expression::SessionVariable { .. } => expr.clone(),

        // Recursively bind in compound expressions
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: op.clone(),
            left: Box::new(bind_expression(left, params)),
            right: Box::new(bind_expression(right, params)),
        },

        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(bind_expression(inner, params)),
        },

        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| bind_expression(a, params)).collect(),
            character_unit: character_unit.clone(),
        },

        Expression::AggregateFunction { name, distinct, args } => Expression::AggregateFunction {
            name: name.clone(),
            distinct: *distinct,
            args: args.iter().map(|a| bind_expression(a, params)).collect(),
        },

        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(bind_expression(inner, params)),
            negated: *negated,
        },

        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|o| Box::new(bind_expression(o, params))),
            when_clauses: when_clauses
                .iter()
                .map(|w| vibesql_ast::CaseWhen {
                    conditions: w.conditions.iter().map(|c| bind_expression(c, params)).collect(),
                    result: bind_expression(&w.result, params),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| Box::new(bind_expression(e, params))),
        },

        Expression::ScalarSubquery(select) => {
            Expression::ScalarSubquery(Box::new(bind_select(select, params)))
        }

        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(bind_expression(inner, params)),
            subquery: Box::new(bind_select(subquery, params)),
            negated: *negated,
        },

        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(bind_expression(inner, params)),
            values: values.iter().map(|v| bind_expression(v, params)).collect(),
            negated: *negated,
        },

        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(bind_expression(inner, params)),
            low: Box::new(bind_expression(low, params)),
            high: Box::new(bind_expression(high, params)),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(bind_expression(inner, params)),
            data_type: data_type.clone(),
        },

        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(bind_expression(substring, params)),
            string: Box::new(bind_expression(string, params)),
            character_unit: character_unit.clone(),
        },

        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position: position.clone(),
            removal_char: removal_char.as_ref().map(|c| Box::new(bind_expression(c, params))),
            string: Box::new(bind_expression(string, params)),
        },

        Expression::Extract { field, expr: inner } => Expression::Extract {
            field: field.clone(),
            expr: Box::new(bind_expression(inner, params)),
        },

        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(bind_expression(inner, params)),
            pattern: Box::new(bind_expression(pattern, params)),
            negated: *negated,
        },

        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: Box::new(bind_select(subquery, params)),
            negated: *negated,
        },

        Expression::QuantifiedComparison { expr: inner, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(bind_expression(inner, params)),
                op: op.clone(),
                quantifier: quantifier.clone(),
                subquery: Box::new(bind_select(subquery, params)),
            }
        }

        Expression::CurrentTime { precision } => Expression::CurrentTime { precision: *precision },

        Expression::CurrentTimestamp { precision } => {
            Expression::CurrentTimestamp { precision: *precision }
        }

        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(bind_expression(value, params)),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            }
        }

        Expression::DuplicateKeyValue { column } => {
            Expression::DuplicateKeyValue { column: column.clone() }
        }

        Expression::WindowFunction { function, over } => Expression::WindowFunction {
            function: bind_window_function_spec(function, params),
            over: bind_window_spec(over, params),
        },

        Expression::NextValue { sequence_name } => {
            Expression::NextValue { sequence_name: sequence_name.clone() }
        }

        Expression::MatchAgainst { columns, search_modifier, mode } => Expression::MatchAgainst {
            columns: columns.clone(),
            search_modifier: Box::new(bind_expression(search_modifier, params)),
            mode: mode.clone(),
        },

        Expression::PseudoVariable { pseudo_table, column } => Expression::PseudoVariable {
            pseudo_table: *pseudo_table,
            column: column.clone(),
        },
    }
}

/// Bind parameters in a FROM clause
fn bind_from_clause(from: &FromClause, params: &[SqlValue]) -> FromClause {
    match from {
        FromClause::Table { name, alias } => FromClause::Table {
            name: name.clone(),
            alias: alias.clone(),
        },
        FromClause::Join { left, right, join_type, condition, natural } => FromClause::Join {
            left: Box::new(bind_from_clause(left, params)),
            right: Box::new(bind_from_clause(right, params)),
            join_type: join_type.clone(),
            condition: condition.as_ref().map(|c| bind_expression(c, params)),
            natural: *natural,
        },
        FromClause::Subquery { query, alias } => FromClause::Subquery {
            query: Box::new(bind_select(query, params)),
            alias: alias.clone(),
        },
    }
}

/// Bind parameters in an ORDER BY item
fn bind_order_by(item: &OrderByItem, params: &[SqlValue]) -> OrderByItem {
    OrderByItem {
        expr: bind_expression(&item.expr, params),
        direction: item.direction.clone(),
    }
}

/// Bind parameters in a window function specification
fn bind_window_function_spec(
    spec: &vibesql_ast::WindowFunctionSpec,
    params: &[SqlValue],
) -> vibesql_ast::WindowFunctionSpec {
    match spec {
        vibesql_ast::WindowFunctionSpec::Aggregate { name, args } => {
            vibesql_ast::WindowFunctionSpec::Aggregate {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression(a, params)).collect(),
            }
        }
        vibesql_ast::WindowFunctionSpec::Ranking { name, args } => {
            vibesql_ast::WindowFunctionSpec::Ranking {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression(a, params)).collect(),
            }
        }
        vibesql_ast::WindowFunctionSpec::Value { name, args } => {
            vibesql_ast::WindowFunctionSpec::Value {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression(a, params)).collect(),
            }
        }
    }
}

/// Bind parameters in a window specification
fn bind_window_spec(spec: &vibesql_ast::WindowSpec, params: &[SqlValue]) -> vibesql_ast::WindowSpec {
    vibesql_ast::WindowSpec {
        partition_by: spec
            .partition_by
            .as_ref()
            .map(|exprs| exprs.iter().map(|e| bind_expression(e, params)).collect()),
        order_by: spec
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|o| bind_order_by(o, params)).collect()),
        frame: spec.frame.as_ref().map(|f| bind_window_frame(f, params)),
    }
}

/// Bind parameters in a window frame
fn bind_window_frame(
    frame: &vibesql_ast::WindowFrame,
    params: &[SqlValue],
) -> vibesql_ast::WindowFrame {
    vibesql_ast::WindowFrame {
        unit: frame.unit.clone(),
        start: bind_frame_bound(&frame.start, params),
        end: frame.end.as_ref().map(|b| bind_frame_bound(b, params)),
    }
}

/// Bind parameters in a frame bound
fn bind_frame_bound(bound: &vibesql_ast::FrameBound, params: &[SqlValue]) -> vibesql_ast::FrameBound {
    match bound {
        vibesql_ast::FrameBound::UnboundedPreceding => vibesql_ast::FrameBound::UnboundedPreceding,
        vibesql_ast::FrameBound::Preceding(expr) => {
            vibesql_ast::FrameBound::Preceding(Box::new(bind_expression(expr, params)))
        }
        vibesql_ast::FrameBound::CurrentRow => vibesql_ast::FrameBound::CurrentRow,
        vibesql_ast::FrameBound::Following(expr) => {
            vibesql_ast::FrameBound::Following(Box::new(bind_expression(expr, params)))
        }
        vibesql_ast::FrameBound::UnboundedFollowing => vibesql_ast::FrameBound::UnboundedFollowing,
    }
}

/// Visit all expressions in a statement (for counting placeholders)
fn visit_statement<F>(stmt: &Statement, visitor: &mut F)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_placeholders_simple() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 1);
    }

    #[test]
    fn test_count_placeholders_multiple() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 3);
    }

    #[test]
    fn test_count_placeholders_none() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 0);
    }

    #[test]
    fn test_bind_parameters_select() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let bound = bind_parameters(&stmt, &[SqlValue::Integer(42)]);

        // Verify the placeholder was replaced with literal
        if let Statement::Select(select) = bound {
            if let Some(Expression::BinaryOp { right, .. }) = &select.where_clause {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_parameters_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (?, ?)";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let params = vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())];
        let bound = bind_parameters(&stmt, &params);

        if let Statement::Insert(insert) = bound {
            if let InsertSource::Values(rows) = &insert.source {
                assert_eq!(rows[0][0], Expression::Literal(SqlValue::Integer(1)));
                assert_eq!(
                    rows[0][1],
                    Expression::Literal(SqlValue::Varchar("Alice".to_string()))
                );
            } else {
                panic!("Expected VALUES insert source");
            }
        } else {
            panic!("Expected INSERT statement");
        }
    }
}
