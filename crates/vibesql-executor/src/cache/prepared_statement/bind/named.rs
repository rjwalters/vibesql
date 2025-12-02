//! Named parameter binding (using `:name` placeholders)
//!
//! This module provides functions to bind named parameters to prepared statements
//! by replacing NamedPlaceholder expressions with Literal values using a HashMap lookup.

use std::collections::HashMap;
use vibesql_ast::{
    Assignment, CommonTableExpr, DeleteStmt, Expression, FromClause, GroupByClause,
    GroupingElement, GroupingSet, InsertSource, InsertStmt, MixedGroupingItem, OrderByItem,
    SelectItem, SelectStmt, SetOperation, UpdateStmt, WhereClause,
};
use vibesql_types::SqlValue;

/// Bind named parameters in a SELECT statement
pub fn bind_select_named(stmt: &SelectStmt, params: &HashMap<String, SqlValue>) -> SelectStmt {
    SelectStmt {
        with_clause: stmt.with_clause.as_ref().map(|ctes| {
            ctes.iter()
                .map(|cte| CommonTableExpr {
                    name: cte.name.clone(),
                    columns: cte.columns.clone(),
                    query: Box::new(bind_select_named(&cte.query, params)),
                })
                .collect()
        }),
        distinct: stmt.distinct,
        select_list: stmt
            .select_list
            .iter()
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => SelectItem::Expression {
                    expr: bind_expression_named(expr, params),
                    alias: alias.clone(),
                },
                SelectItem::Wildcard { alias } => SelectItem::Wildcard { alias: alias.clone() },
                SelectItem::QualifiedWildcard { qualifier, alias } => {
                    SelectItem::QualifiedWildcard { qualifier: qualifier.clone(), alias: alias.clone() }
                }
            })
            .collect(),
        into_table: stmt.into_table.clone(),
        into_variables: stmt.into_variables.clone(),
        from: stmt.from.as_ref().map(|f| bind_from_clause_named(f, params)),
        where_clause: stmt.where_clause.as_ref().map(|e| bind_expression_named(e, params)),
        group_by: stmt.group_by.as_ref().map(|g| bind_group_by_named(g, params)),
        having: stmt.having.as_ref().map(|e| bind_expression_named(e, params)),
        order_by: stmt
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|o| bind_order_by_named(o, params)).collect()),
        limit: stmt.limit,
        offset: stmt.offset,
        set_operation: stmt.set_operation.as_ref().map(|op| SetOperation {
            op: op.op.clone(),
            all: op.all,
            right: Box::new(bind_select_named(&op.right, params)),
        }),
    }
}

/// Bind named parameters in GROUP BY clause
fn bind_group_by_named(clause: &GroupByClause, params: &HashMap<String, SqlValue>) -> GroupByClause {
    match clause {
        GroupByClause::Simple(exprs) => {
            GroupByClause::Simple(exprs.iter().map(|e| bind_expression_named(e, params)).collect())
        }
        GroupByClause::Rollup(elements) => {
            GroupByClause::Rollup(bind_grouping_elements_named(elements, params))
        }
        GroupByClause::Cube(elements) => {
            GroupByClause::Cube(bind_grouping_elements_named(elements, params))
        }
        GroupByClause::GroupingSets(sets) => GroupByClause::GroupingSets(
            sets.iter().map(|s| bind_grouping_set_named(s, params)).collect(),
        ),
        GroupByClause::Mixed(items) => GroupByClause::Mixed(
            items
                .iter()
                .map(|item| match item {
                    MixedGroupingItem::Simple(expr) => {
                        MixedGroupingItem::Simple(bind_expression_named(expr, params))
                    }
                    MixedGroupingItem::Rollup(elements) => {
                        MixedGroupingItem::Rollup(bind_grouping_elements_named(elements, params))
                    }
                    MixedGroupingItem::Cube(elements) => {
                        MixedGroupingItem::Cube(bind_grouping_elements_named(elements, params))
                    }
                    MixedGroupingItem::GroupingSets(sets) => MixedGroupingItem::GroupingSets(
                        sets.iter().map(|s| bind_grouping_set_named(s, params)).collect(),
                    ),
                })
                .collect(),
        ),
    }
}

fn bind_grouping_elements_named(
    elements: &[GroupingElement],
    params: &HashMap<String, SqlValue>,
) -> Vec<GroupingElement> {
    elements
        .iter()
        .map(|e| match e {
            GroupingElement::Single(expr) => GroupingElement::Single(bind_expression_named(expr, params)),
            GroupingElement::Composite(exprs) => GroupingElement::Composite(
                exprs.iter().map(|e| bind_expression_named(e, params)).collect(),
            ),
        })
        .collect()
}

fn bind_grouping_set_named(set: &GroupingSet, params: &HashMap<String, SqlValue>) -> GroupingSet {
    GroupingSet {
        columns: set.columns.iter().map(|e| bind_expression_named(e, params)).collect(),
    }
}

/// Bind named parameters in an INSERT statement
pub fn bind_insert_named(stmt: &InsertStmt, params: &HashMap<String, SqlValue>) -> InsertStmt {
    InsertStmt {
        table_name: stmt.table_name.clone(),
        columns: stmt.columns.clone(),
        source: match &stmt.source {
            InsertSource::Values(rows) => InsertSource::Values(
                rows.iter()
                    .map(|row| row.iter().map(|e| bind_expression_named(e, params)).collect())
                    .collect(),
            ),
            InsertSource::Select(select) => {
                InsertSource::Select(Box::new(bind_select_named(select, params)))
            }
        },
        conflict_clause: stmt.conflict_clause.clone(),
        on_duplicate_key_update: stmt.on_duplicate_key_update.as_ref().map(|updates| {
            updates
                .iter()
                .map(|a| Assignment {
                    column: a.column.clone(),
                    value: bind_expression_named(&a.value, params),
                })
                .collect()
        }),
    }
}

/// Bind named parameters in an UPDATE statement
pub fn bind_update_named(stmt: &UpdateStmt, params: &HashMap<String, SqlValue>) -> UpdateStmt {
    UpdateStmt {
        table_name: stmt.table_name.clone(),
        assignments: stmt
            .assignments
            .iter()
            .map(|a| Assignment {
                column: a.column.clone(),
                value: bind_expression_named(&a.value, params),
            })
            .collect(),
        where_clause: stmt.where_clause.as_ref().map(|w| match w {
            WhereClause::Condition(expr) => {
                WhereClause::Condition(bind_expression_named(expr, params))
            }
            WhereClause::CurrentOf(cursor) => WhereClause::CurrentOf(cursor.clone()),
        }),
    }
}

/// Bind named parameters in a DELETE statement
pub fn bind_delete_named(stmt: &DeleteStmt, params: &HashMap<String, SqlValue>) -> DeleteStmt {
    DeleteStmt {
        only: stmt.only,
        table_name: stmt.table_name.clone(),
        where_clause: stmt.where_clause.as_ref().map(|w| match w {
            WhereClause::Condition(expr) => {
                WhereClause::Condition(bind_expression_named(expr, params))
            }
            WhereClause::CurrentOf(cursor) => WhereClause::CurrentOf(cursor.clone()),
        }),
    }
}

/// Bind named parameters in a FROM clause
fn bind_from_clause_named(from: &FromClause, params: &HashMap<String, SqlValue>) -> FromClause {
    match from {
        FromClause::Table { name, alias } => FromClause::Table { name: name.clone(), alias: alias.clone() },
        FromClause::Join { left, right, join_type, condition, natural } => FromClause::Join {
            left: Box::new(bind_from_clause_named(left, params)),
            right: Box::new(bind_from_clause_named(right, params)),
            join_type: join_type.clone(),
            condition: condition.as_ref().map(|c| bind_expression_named(c, params)),
            natural: *natural,
        },
        FromClause::Subquery { query, alias } => FromClause::Subquery {
            query: Box::new(bind_select_named(query, params)),
            alias: alias.clone(),
        },
    }
}

/// Bind named parameters in an ORDER BY item
fn bind_order_by_named(item: &OrderByItem, params: &HashMap<String, SqlValue>) -> OrderByItem {
    OrderByItem {
        expr: bind_expression_named(&item.expr, params),
        direction: item.direction.clone(),
    }
}

/// Bind named parameters in an expression
pub fn bind_expression_named(expr: &Expression, params: &HashMap<String, SqlValue>) -> Expression {
    match expr {
        // Named placeholders - look up in the HashMap
        Expression::NamedPlaceholder(name) => {
            if let Some(value) = params.get(name) {
                Expression::Literal(value.clone())
            } else {
                // Parameter not found - return as-is (or could return an error)
                expr.clone()
            }
        }

        // Other placeholders and leaf nodes: return as-is
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::Literal(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::Default
        | Expression::SessionVariable { .. } => expr.clone(),

        // Recursively bind in compound expressions
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(bind_expression_named(left, params)),
            right: Box::new(bind_expression_named(right, params)),
        },

        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(bind_expression_named(inner, params)),
        },

        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| bind_expression_named(a, params)).collect(),
            character_unit: character_unit.clone(),
        },

        Expression::AggregateFunction { name, distinct, args } => Expression::AggregateFunction {
            name: name.clone(),
            distinct: *distinct,
            args: args.iter().map(|a| bind_expression_named(a, params)).collect(),
        },

        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(bind_expression_named(inner, params)),
            negated: *negated,
        },

        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|o| Box::new(bind_expression_named(o, params))),
            when_clauses: when_clauses
                .iter()
                .map(|w| vibesql_ast::CaseWhen {
                    conditions: w.conditions.iter().map(|c| bind_expression_named(c, params)).collect(),
                    result: bind_expression_named(&w.result, params),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| Box::new(bind_expression_named(e, params))),
        },

        Expression::ScalarSubquery(select) => {
            Expression::ScalarSubquery(Box::new(bind_select_named(select, params)))
        }

        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(bind_expression_named(inner, params)),
            subquery: Box::new(bind_select_named(subquery, params)),
            negated: *negated,
        },

        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(bind_expression_named(inner, params)),
            values: values.iter().map(|v| bind_expression_named(v, params)).collect(),
            negated: *negated,
        },

        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(bind_expression_named(inner, params)),
            low: Box::new(bind_expression_named(low, params)),
            high: Box::new(bind_expression_named(high, params)),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(bind_expression_named(inner, params)),
            data_type: data_type.clone(),
        },

        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(bind_expression_named(substring, params)),
            string: Box::new(bind_expression_named(string, params)),
            character_unit: character_unit.clone(),
        },

        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position: position.clone(),
            removal_char: removal_char.as_ref().map(|c| Box::new(bind_expression_named(c, params))),
            string: Box::new(bind_expression_named(string, params)),
        },

        Expression::Extract { field, expr: inner } => Expression::Extract {
            field: field.clone(),
            expr: Box::new(bind_expression_named(inner, params)),
        },

        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(bind_expression_named(inner, params)),
            pattern: Box::new(bind_expression_named(pattern, params)),
            negated: *negated,
        },

        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: Box::new(bind_select_named(subquery, params)),
            negated: *negated,
        },

        Expression::QuantifiedComparison { expr: inner, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(bind_expression_named(inner, params)),
                op: *op,
                quantifier: quantifier.clone(),
                subquery: Box::new(bind_select_named(subquery, params)),
            }
        }

        Expression::CurrentTime { precision } => Expression::CurrentTime { precision: *precision },

        Expression::CurrentTimestamp { precision } => {
            Expression::CurrentTimestamp { precision: *precision }
        }

        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(bind_expression_named(value, params)),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            }
        }

        Expression::DuplicateKeyValue { column } => {
            Expression::DuplicateKeyValue { column: column.clone() }
        }

        Expression::WindowFunction { function, over } => Expression::WindowFunction {
            function: bind_window_function_spec_named(function, params),
            over: bind_window_spec_named(over, params),
        },

        Expression::NextValue { sequence_name } => {
            Expression::NextValue { sequence_name: sequence_name.clone() }
        }

        Expression::MatchAgainst { columns, search_modifier, mode } => Expression::MatchAgainst {
            columns: columns.clone(),
            search_modifier: Box::new(bind_expression_named(search_modifier, params)),
            mode: mode.clone(),
        },

        Expression::PseudoVariable { pseudo_table, column } => Expression::PseudoVariable {
            pseudo_table: *pseudo_table,
            column: column.clone(),
        },
    }
}

/// Bind named parameters in a window function specification
fn bind_window_function_spec_named(
    spec: &vibesql_ast::WindowFunctionSpec,
    params: &HashMap<String, SqlValue>,
) -> vibesql_ast::WindowFunctionSpec {
    match spec {
        vibesql_ast::WindowFunctionSpec::Aggregate { name, args } => {
            vibesql_ast::WindowFunctionSpec::Aggregate {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression_named(a, params)).collect(),
            }
        }
        vibesql_ast::WindowFunctionSpec::Ranking { name, args } => {
            vibesql_ast::WindowFunctionSpec::Ranking {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression_named(a, params)).collect(),
            }
        }
        vibesql_ast::WindowFunctionSpec::Value { name, args } => {
            vibesql_ast::WindowFunctionSpec::Value {
                name: name.clone(),
                args: args.iter().map(|a| bind_expression_named(a, params)).collect(),
            }
        }
    }
}

/// Bind named parameters in a window specification
fn bind_window_spec_named(
    spec: &vibesql_ast::WindowSpec,
    params: &HashMap<String, SqlValue>,
) -> vibesql_ast::WindowSpec {
    vibesql_ast::WindowSpec {
        partition_by: spec
            .partition_by
            .as_ref()
            .map(|exprs| exprs.iter().map(|e| bind_expression_named(e, params)).collect()),
        order_by: spec
            .order_by
            .as_ref()
            .map(|items| items.iter().map(|o| bind_order_by_named(o, params)).collect()),
        frame: spec.frame.as_ref().map(|f| bind_window_frame_named(f, params)),
    }
}

/// Bind named parameters in a window frame
fn bind_window_frame_named(
    frame: &vibesql_ast::WindowFrame,
    params: &HashMap<String, SqlValue>,
) -> vibesql_ast::WindowFrame {
    vibesql_ast::WindowFrame {
        unit: frame.unit.clone(),
        start: bind_frame_bound_named(&frame.start, params),
        end: frame.end.as_ref().map(|b| bind_frame_bound_named(b, params)),
    }
}

/// Bind named parameters in a frame bound
fn bind_frame_bound_named(
    bound: &vibesql_ast::FrameBound,
    params: &HashMap<String, SqlValue>,
) -> vibesql_ast::FrameBound {
    match bound {
        vibesql_ast::FrameBound::UnboundedPreceding => vibesql_ast::FrameBound::UnboundedPreceding,
        vibesql_ast::FrameBound::Preceding(expr) => {
            vibesql_ast::FrameBound::Preceding(Box::new(bind_expression_named(expr, params)))
        }
        vibesql_ast::FrameBound::CurrentRow => vibesql_ast::FrameBound::CurrentRow,
        vibesql_ast::FrameBound::Following(expr) => {
            vibesql_ast::FrameBound::Following(Box::new(bind_expression_named(expr, params)))
        }
        vibesql_ast::FrameBound::UnboundedFollowing => vibesql_ast::FrameBound::UnboundedFollowing,
    }
}
