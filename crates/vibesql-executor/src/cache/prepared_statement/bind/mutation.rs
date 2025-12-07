//! Mutation-based parameter binding (in-place modification)
//!
//! This module provides functions to bind parameters to prepared statements
//! by mutating Placeholder expressions in-place, avoiding the O(n) AST cloning
//! overhead of the clone-based approach.
//!
//! ## Performance
//!
//! For a query with 100 AST nodes and 5 placeholders:
//! - Clone-based: Clone 100 nodes, modify 5 = O(100) allocations
//! - Mutation-based: Modify 5 nodes in-place = O(5) operations
//!
//! ## Usage
//!
//! ```text
//! // Clone once, then mutate in-place
//! let mut stmt = prepared_stmt.statement.clone();
//! bind_statement_mut(&mut stmt, &params);
//! // stmt now has placeholders replaced with literals
//! ```

#[cfg(test)]
use std::collections::HashMap;
use vibesql_ast::{
    DeleteStmt, Expression, FromClause, GroupByClause, GroupingElement, GroupingSet, InsertSource,
    InsertStmt, MixedGroupingItem, SelectItem, SelectStmt, Statement, UpdateStmt, WhereClause,
};
use vibesql_types::SqlValue;

/// Bind positional parameters to a statement by mutating placeholders in-place
///
/// This replaces all `Placeholder(idx)` and `NumberedPlaceholder(n)` expressions
/// with `Literal(params[idx])` values directly in the AST.
pub fn bind_statement_mut(stmt: &mut Statement, params: &[SqlValue]) {
    match stmt {
        Statement::Select(select) => bind_select_mut(select, params),
        Statement::Insert(insert) => bind_insert_mut(insert, params),
        Statement::Update(update) => bind_update_mut(update, params),
        Statement::Delete(delete) => bind_delete_mut(delete, params),
        // Other statement types don't typically have placeholders
        _ => {}
    }
}

/// Bind named parameters to a statement by mutating placeholders in-place
#[cfg(test)]
pub fn bind_statement_named_mut(stmt: &mut Statement, params: &HashMap<String, SqlValue>) {
    match stmt {
        Statement::Select(select) => bind_select_named_mut(select, params),
        Statement::Insert(insert) => bind_insert_named_mut(insert, params),
        Statement::Update(update) => bind_update_named_mut(update, params),
        Statement::Delete(delete) => bind_delete_named_mut(delete, params),
        _ => {}
    }
}

// =============================================================================
// Positional parameter binding (?, $N)
// =============================================================================

/// Bind parameters in a SELECT statement (in-place)
fn bind_select_mut(stmt: &mut SelectStmt, params: &[SqlValue]) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_mut(&mut cte.query, params);
        }
    }

    // Select list
    for item in &mut stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            bind_expression_mut(expr, params);
        }
    }

    // FROM clause
    if let Some(from) = &mut stmt.from {
        bind_from_clause_mut(from, params);
    }

    // WHERE clause
    if let Some(where_clause) = &mut stmt.where_clause {
        bind_expression_mut(where_clause, params);
    }

    // GROUP BY
    if let Some(group_by) = &mut stmt.group_by {
        bind_group_by_mut(group_by, params);
    }

    // HAVING
    if let Some(having) = &mut stmt.having {
        bind_expression_mut(having, params);
    }

    // ORDER BY
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_mut(&mut item.expr, params);
        }
    }

    // Set operation (UNION, INTERSECT, EXCEPT)
    if let Some(set_op) = &mut stmt.set_operation {
        bind_select_mut(&mut set_op.right, params);
    }
}

fn bind_group_by_mut(clause: &mut GroupByClause, params: &[SqlValue]) {
    match clause {
        GroupByClause::Simple(exprs) => {
            for expr in exprs {
                bind_expression_mut(expr, params);
            }
        }
        GroupByClause::Rollup(elements) => bind_grouping_elements_mut(elements, params),
        GroupByClause::Cube(elements) => bind_grouping_elements_mut(elements, params),
        GroupByClause::GroupingSets(sets) => {
            for set in sets {
                bind_grouping_set_mut(set, params);
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items {
                match item {
                    MixedGroupingItem::Simple(expr) => bind_expression_mut(expr, params),
                    MixedGroupingItem::Rollup(elements) => {
                        bind_grouping_elements_mut(elements, params)
                    }
                    MixedGroupingItem::Cube(elements) => {
                        bind_grouping_elements_mut(elements, params)
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        for set in sets {
                            bind_grouping_set_mut(set, params);
                        }
                    }
                }
            }
        }
    }
}

fn bind_grouping_elements_mut(elements: &mut [GroupingElement], params: &[SqlValue]) {
    for element in elements {
        match element {
            GroupingElement::Single(expr) => bind_expression_mut(expr, params),
            GroupingElement::Composite(exprs) => {
                for expr in exprs {
                    bind_expression_mut(expr, params);
                }
            }
        }
    }
}

fn bind_grouping_set_mut(set: &mut GroupingSet, params: &[SqlValue]) {
    for expr in &mut set.columns {
        bind_expression_mut(expr, params);
    }
}

/// Bind parameters in an INSERT statement (in-place)
fn bind_insert_mut(stmt: &mut InsertStmt, params: &[SqlValue]) {
    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_expression_mut(expr, params);
                }
            }
        }
        InsertSource::Select(select) => bind_select_mut(select, params),
    }

    if let Some(updates) = &mut stmt.on_duplicate_key_update {
        for assignment in updates {
            bind_expression_mut(&mut assignment.value, params);
        }
    }
}

/// Bind parameters in an UPDATE statement (in-place)
fn bind_update_mut(stmt: &mut UpdateStmt, params: &[SqlValue]) {
    for assignment in &mut stmt.assignments {
        bind_expression_mut(&mut assignment.value, params);
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_mut(expr, params);
    }
}

/// Bind parameters in a DELETE statement (in-place)
fn bind_delete_mut(stmt: &mut DeleteStmt, params: &[SqlValue]) {
    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_mut(expr, params);
    }
}

fn bind_from_clause_mut(from: &mut FromClause, params: &[SqlValue]) {
    match from {
        FromClause::Table { .. } => {}
        FromClause::Join { left, right, condition, .. } => {
            bind_from_clause_mut(left, params);
            bind_from_clause_mut(right, params);
            if let Some(cond) = condition {
                bind_expression_mut(cond, params);
            }
        }
        FromClause::Subquery { query, .. } => bind_select_mut(query, params),
    }
}

/// Bind parameters in an expression (in-place)
///
/// This is the core function that replaces placeholder expressions with literal values.
fn bind_expression_mut(expr: &mut Expression, params: &[SqlValue]) {
    match expr {
        // The key cases: replace placeholders with literal values
        Expression::Placeholder(idx) => {
            if *idx < params.len() {
                *expr = Expression::Literal(params[*idx].clone());
            }
        }

        Expression::NumberedPlaceholder(n) => {
            // $1 = params[0], $2 = params[1], etc.
            let array_idx = n.saturating_sub(1);
            if array_idx < params.len() {
                *expr = Expression::Literal(params[array_idx].clone());
            }
        }

        // Named placeholders are not bound by this function
        Expression::NamedPlaceholder(_) => {}

        // Leaf nodes: nothing to do
        Expression::Literal(_)
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

        // Recurse into compound expressions
        Expression::BinaryOp { left, right, .. } => {
            bind_expression_mut(left, params);
            bind_expression_mut(right, params);
        }

        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                bind_expression_mut(child, params);
            }
        }

        Expression::UnaryOp { expr: inner, .. } => {
            bind_expression_mut(inner, params);
        }

        Expression::Function { args, .. } => {
            for arg in args {
                bind_expression_mut(arg, params);
            }
        }

        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                bind_expression_mut(arg, params);
            }
        }

        Expression::IsNull { expr: inner, .. } => {
            bind_expression_mut(inner, params);
        }

        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                bind_expression_mut(op, params);
            }
            for w in when_clauses {
                for c in &mut w.conditions {
                    bind_expression_mut(c, params);
                }
                bind_expression_mut(&mut w.result, params);
            }
            if let Some(e) = else_result {
                bind_expression_mut(e, params);
            }
        }

        Expression::ScalarSubquery(select) => {
            bind_select_mut(select, params);
        }

        Expression::In { expr: inner, subquery, .. } => {
            bind_expression_mut(inner, params);
            bind_select_mut(subquery, params);
        }

        Expression::InList { expr: inner, values, .. } => {
            bind_expression_mut(inner, params);
            for v in values {
                bind_expression_mut(v, params);
            }
        }

        Expression::Between { expr: inner, low, high, .. } => {
            bind_expression_mut(inner, params);
            bind_expression_mut(low, params);
            bind_expression_mut(high, params);
        }

        Expression::Cast { expr: inner, .. } => {
            bind_expression_mut(inner, params);
        }

        Expression::Position { substring, string, .. } => {
            bind_expression_mut(substring, params);
            bind_expression_mut(string, params);
        }

        Expression::Trim { removal_char, string, .. } => {
            if let Some(c) = removal_char {
                bind_expression_mut(c, params);
            }
            bind_expression_mut(string, params);
        }

        Expression::Extract { expr: inner, .. } => {
            bind_expression_mut(inner, params);
        }

        Expression::Like { expr: inner, pattern, .. } => {
            bind_expression_mut(inner, params);
            bind_expression_mut(pattern, params);
        }

        Expression::Exists { subquery, .. } => {
            bind_select_mut(subquery, params);
        }

        Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
            bind_expression_mut(inner, params);
            bind_select_mut(subquery, params);
        }

        Expression::Interval { value, .. } => {
            bind_expression_mut(value, params);
        }

        Expression::WindowFunction { function, over } => {
            bind_window_function_spec_mut(function, params);
            bind_window_spec_mut(over, params);
        }

        Expression::MatchAgainst { search_modifier, .. } => {
            bind_expression_mut(search_modifier, params);
        }
    }
}

fn bind_window_function_spec_mut(spec: &mut vibesql_ast::WindowFunctionSpec, params: &[SqlValue]) {
    match spec {
        vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
        | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
        | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
            for arg in args {
                bind_expression_mut(arg, params);
            }
        }
    }
}

fn bind_window_spec_mut(spec: &mut vibesql_ast::WindowSpec, params: &[SqlValue]) {
    if let Some(partition_by) = &mut spec.partition_by {
        for expr in partition_by {
            bind_expression_mut(expr, params);
        }
    }
    if let Some(order_by) = &mut spec.order_by {
        for item in order_by {
            bind_expression_mut(&mut item.expr, params);
        }
    }
    if let Some(frame) = &mut spec.frame {
        bind_window_frame_mut(frame, params);
    }
}

fn bind_window_frame_mut(frame: &mut vibesql_ast::WindowFrame, params: &[SqlValue]) {
    bind_frame_bound_mut(&mut frame.start, params);
    if let Some(end) = &mut frame.end {
        bind_frame_bound_mut(end, params);
    }
}

fn bind_frame_bound_mut(bound: &mut vibesql_ast::FrameBound, params: &[SqlValue]) {
    match bound {
        vibesql_ast::FrameBound::Preceding(expr) | vibesql_ast::FrameBound::Following(expr) => {
            bind_expression_mut(expr, params);
        }
        vibesql_ast::FrameBound::UnboundedPreceding
        | vibesql_ast::FrameBound::CurrentRow
        | vibesql_ast::FrameBound::UnboundedFollowing => {}
    }
}

// =============================================================================
// Named parameter binding (:name) - only used for tests
// =============================================================================

#[cfg(test)]
fn bind_select_named_mut(stmt: &mut SelectStmt, params: &HashMap<String, SqlValue>) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_named_mut(&mut cte.query, params);
        }
    }

    // Select list
    for item in &mut stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            bind_expression_named_mut(expr, params);
        }
    }

    // FROM clause
    if let Some(from) = &mut stmt.from {
        bind_from_clause_named_mut(from, params);
    }

    // WHERE clause
    if let Some(where_clause) = &mut stmt.where_clause {
        bind_expression_named_mut(where_clause, params);
    }

    // GROUP BY
    if let Some(group_by) = &mut stmt.group_by {
        bind_group_by_named_mut(group_by, params);
    }

    // HAVING
    if let Some(having) = &mut stmt.having {
        bind_expression_named_mut(having, params);
    }

    // ORDER BY
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_named_mut(&mut item.expr, params);
        }
    }

    // Set operation
    if let Some(set_op) = &mut stmt.set_operation {
        bind_select_named_mut(&mut set_op.right, params);
    }
}

#[cfg(test)]
fn bind_group_by_named_mut(clause: &mut GroupByClause, params: &HashMap<String, SqlValue>) {
    match clause {
        GroupByClause::Simple(exprs) => {
            for expr in exprs {
                bind_expression_named_mut(expr, params);
            }
        }
        GroupByClause::Rollup(elements) => bind_grouping_elements_named_mut(elements, params),
        GroupByClause::Cube(elements) => bind_grouping_elements_named_mut(elements, params),
        GroupByClause::GroupingSets(sets) => {
            for set in sets {
                bind_grouping_set_named_mut(set, params);
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items {
                match item {
                    MixedGroupingItem::Simple(expr) => bind_expression_named_mut(expr, params),
                    MixedGroupingItem::Rollup(elements) => {
                        bind_grouping_elements_named_mut(elements, params)
                    }
                    MixedGroupingItem::Cube(elements) => {
                        bind_grouping_elements_named_mut(elements, params)
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        for set in sets {
                            bind_grouping_set_named_mut(set, params);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
fn bind_grouping_elements_named_mut(
    elements: &mut [GroupingElement],
    params: &HashMap<String, SqlValue>,
) {
    for element in elements {
        match element {
            GroupingElement::Single(expr) => bind_expression_named_mut(expr, params),
            GroupingElement::Composite(exprs) => {
                for expr in exprs {
                    bind_expression_named_mut(expr, params);
                }
            }
        }
    }
}

#[cfg(test)]
fn bind_grouping_set_named_mut(set: &mut GroupingSet, params: &HashMap<String, SqlValue>) {
    for expr in &mut set.columns {
        bind_expression_named_mut(expr, params);
    }
}

#[cfg(test)]
fn bind_insert_named_mut(stmt: &mut InsertStmt, params: &HashMap<String, SqlValue>) {
    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_expression_named_mut(expr, params);
                }
            }
        }
        InsertSource::Select(select) => bind_select_named_mut(select, params),
    }

    if let Some(updates) = &mut stmt.on_duplicate_key_update {
        for assignment in updates {
            bind_expression_named_mut(&mut assignment.value, params);
        }
    }
}

#[cfg(test)]
fn bind_update_named_mut(stmt: &mut UpdateStmt, params: &HashMap<String, SqlValue>) {
    for assignment in &mut stmt.assignments {
        bind_expression_named_mut(&mut assignment.value, params);
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_named_mut(expr, params);
    }
}

#[cfg(test)]
fn bind_delete_named_mut(stmt: &mut DeleteStmt, params: &HashMap<String, SqlValue>) {
    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_named_mut(expr, params);
    }
}

#[cfg(test)]
fn bind_from_clause_named_mut(from: &mut FromClause, params: &HashMap<String, SqlValue>) {
    match from {
        FromClause::Table { .. } => {}
        FromClause::Join { left, right, condition, .. } => {
            bind_from_clause_named_mut(left, params);
            bind_from_clause_named_mut(right, params);
            if let Some(cond) = condition {
                bind_expression_named_mut(cond, params);
            }
        }
        FromClause::Subquery { query, .. } => bind_select_named_mut(query, params),
    }
}

/// Bind named parameters in an expression (in-place)
#[cfg(test)]
fn bind_expression_named_mut(expr: &mut Expression, params: &HashMap<String, SqlValue>) {
    match expr {
        // The key case: replace named placeholders with literal values
        Expression::NamedPlaceholder(name) => {
            if let Some(value) = params.get(name) {
                *expr = Expression::Literal(value.clone());
            }
        }

        // Positional placeholders are not bound by this function
        Expression::Placeholder(_) | Expression::NumberedPlaceholder(_) => {}

        // Leaf nodes: nothing to do
        Expression::Literal(_)
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

        // Recurse into compound expressions
        Expression::BinaryOp { left, right, .. } => {
            bind_expression_named_mut(left, params);
            bind_expression_named_mut(right, params);
        }

        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                bind_expression_named_mut(child, params);
            }
        }

        Expression::UnaryOp { expr: inner, .. } => {
            bind_expression_named_mut(inner, params);
        }

        Expression::Function { args, .. } => {
            for arg in args {
                bind_expression_named_mut(arg, params);
            }
        }

        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                bind_expression_named_mut(arg, params);
            }
        }

        Expression::IsNull { expr: inner, .. } => {
            bind_expression_named_mut(inner, params);
        }

        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                bind_expression_named_mut(op, params);
            }
            for w in when_clauses {
                for c in &mut w.conditions {
                    bind_expression_named_mut(c, params);
                }
                bind_expression_named_mut(&mut w.result, params);
            }
            if let Some(e) = else_result {
                bind_expression_named_mut(e, params);
            }
        }

        Expression::ScalarSubquery(select) => {
            bind_select_named_mut(select, params);
        }

        Expression::In { expr: inner, subquery, .. } => {
            bind_expression_named_mut(inner, params);
            bind_select_named_mut(subquery, params);
        }

        Expression::InList { expr: inner, values, .. } => {
            bind_expression_named_mut(inner, params);
            for v in values {
                bind_expression_named_mut(v, params);
            }
        }

        Expression::Between { expr: inner, low, high, .. } => {
            bind_expression_named_mut(inner, params);
            bind_expression_named_mut(low, params);
            bind_expression_named_mut(high, params);
        }

        Expression::Cast { expr: inner, .. } => {
            bind_expression_named_mut(inner, params);
        }

        Expression::Position { substring, string, .. } => {
            bind_expression_named_mut(substring, params);
            bind_expression_named_mut(string, params);
        }

        Expression::Trim { removal_char, string, .. } => {
            if let Some(c) = removal_char {
                bind_expression_named_mut(c, params);
            }
            bind_expression_named_mut(string, params);
        }

        Expression::Extract { expr: inner, .. } => {
            bind_expression_named_mut(inner, params);
        }

        Expression::Like { expr: inner, pattern, .. } => {
            bind_expression_named_mut(inner, params);
            bind_expression_named_mut(pattern, params);
        }

        Expression::Exists { subquery, .. } => {
            bind_select_named_mut(subquery, params);
        }

        Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
            bind_expression_named_mut(inner, params);
            bind_select_named_mut(subquery, params);
        }

        Expression::Interval { value, .. } => {
            bind_expression_named_mut(value, params);
        }

        Expression::WindowFunction { function, over } => {
            bind_window_function_spec_named_mut(function, params);
            bind_window_spec_named_mut(over, params);
        }

        Expression::MatchAgainst { search_modifier, .. } => {
            bind_expression_named_mut(search_modifier, params);
        }
    }
}

#[cfg(test)]
fn bind_window_function_spec_named_mut(
    spec: &mut vibesql_ast::WindowFunctionSpec,
    params: &HashMap<String, SqlValue>,
) {
    match spec {
        vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
        | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
        | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
            for arg in args {
                bind_expression_named_mut(arg, params);
            }
        }
    }
}

#[cfg(test)]
fn bind_window_spec_named_mut(
    spec: &mut vibesql_ast::WindowSpec,
    params: &HashMap<String, SqlValue>,
) {
    if let Some(partition_by) = &mut spec.partition_by {
        for expr in partition_by {
            bind_expression_named_mut(expr, params);
        }
    }
    if let Some(order_by) = &mut spec.order_by {
        for item in order_by {
            bind_expression_named_mut(&mut item.expr, params);
        }
    }
    if let Some(frame) = &mut spec.frame {
        bind_window_frame_named_mut(frame, params);
    }
}

#[cfg(test)]
fn bind_window_frame_named_mut(
    frame: &mut vibesql_ast::WindowFrame,
    params: &HashMap<String, SqlValue>,
) {
    bind_frame_bound_named_mut(&mut frame.start, params);
    if let Some(end) = &mut frame.end {
        bind_frame_bound_named_mut(end, params);
    }
}

#[cfg(test)]
fn bind_frame_bound_named_mut(
    bound: &mut vibesql_ast::FrameBound,
    params: &HashMap<String, SqlValue>,
) {
    match bound {
        vibesql_ast::FrameBound::Preceding(expr) | vibesql_ast::FrameBound::Following(expr) => {
            bind_expression_named_mut(expr, params);
        }
        vibesql_ast::FrameBound::UnboundedPreceding
        | vibesql_ast::FrameBound::CurrentRow
        | vibesql_ast::FrameBound::UnboundedFollowing => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::InsertSource;

    #[test]
    fn test_bind_expression_mut_placeholder() {
        let mut expr = Expression::Placeholder(0);
        bind_expression_mut(&mut expr, &[SqlValue::Integer(42)]);
        assert_eq!(expr, Expression::Literal(SqlValue::Integer(42)));
    }

    #[test]
    fn test_bind_expression_mut_numbered_placeholder() {
        let mut expr = Expression::NumberedPlaceholder(1);
        bind_expression_mut(&mut expr, &[SqlValue::Integer(42)]);
        assert_eq!(expr, Expression::Literal(SqlValue::Integer(42)));
    }

    #[test]
    fn test_bind_expression_mut_binary_op() {
        use vibesql_ast::BinaryOperator;
        let mut expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            right: Box::new(Expression::Placeholder(0)),
        };
        bind_expression_mut(&mut expr, &[SqlValue::Integer(42)]);

        if let Expression::BinaryOp { right, .. } = &expr {
            assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
        } else {
            panic!("Expected BinaryOp");
        }
    }

    #[test]
    fn test_bind_select_mut() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        bind_statement_mut(&mut stmt, &[SqlValue::Integer(42)]);

        if let Statement::Select(select) = stmt {
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
    fn test_bind_insert_mut() {
        let sql = "INSERT INTO users (id, name) VALUES (?, ?)";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        let params = vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))];
        bind_statement_mut(&mut stmt, &params);

        if let Statement::Insert(insert) = stmt {
            if let InsertSource::Values(rows) = &insert.source {
                assert_eq!(rows[0][0], Expression::Literal(SqlValue::Integer(1)));
                assert_eq!(rows[0][1], Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice"))));
            } else {
                panic!("Expected VALUES insert source");
            }
        } else {
            panic!("Expected INSERT statement");
        }
    }

    #[test]
    fn test_bind_update_mut() {
        let sql = "UPDATE users SET name = ? WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        let params = vec![SqlValue::Varchar(arcstr::ArcStr::from("Bob")), SqlValue::Integer(42)];
        bind_statement_mut(&mut stmt, &params);

        if let Statement::Update(update) = stmt {
            // Check SET clause
            assert_eq!(
                update.assignments[0].value,
                Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob")))
            );
            // Check WHERE clause
            if let Some(WhereClause::Condition(Expression::BinaryOp { right, .. })) =
                &update.where_clause
            {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected UPDATE statement");
        }
    }

    #[test]
    fn test_bind_delete_mut() {
        let sql = "DELETE FROM users WHERE id = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        bind_statement_mut(&mut stmt, &[SqlValue::Integer(42)]);

        if let Statement::Delete(delete) = stmt {
            if let Some(WhereClause::Condition(Expression::BinaryOp { right, .. })) =
                &delete.where_clause
            {
                assert_eq!(**right, Expression::Literal(SqlValue::Integer(42)));
            } else {
                panic!("Expected BinaryOp in WHERE clause");
            }
        } else {
            panic!("Expected DELETE statement");
        }
    }

    #[test]
    fn test_bind_numbered_placeholders_out_of_order() {
        let sql = "SELECT * FROM users WHERE name = $2 AND id = $1";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        let params = vec![
            SqlValue::Integer(42),                // $1
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")), // $2
        ];
        bind_statement_mut(&mut stmt, &params);

        if let Statement::Select(select) = stmt {
            if let Some(Expression::BinaryOp { left, right, .. }) = &select.where_clause {
                // left is: name = $2 (should be "Bob")
                if let Expression::BinaryOp { right: left_right, .. } = left.as_ref() {
                    assert_eq!(
                        **left_right,
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Bob")))
                    );
                }
                // right is: id = $1 (should be 42)
                if let Expression::BinaryOp { right: right_right, .. } = right.as_ref() {
                    assert_eq!(**right_right, Expression::Literal(SqlValue::Integer(42)));
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_bind_named_placeholder_mut() {
        let sql = "SELECT * FROM users WHERE id = :user_id";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        let mut stmt = stmt.clone();
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), SqlValue::Integer(42));

        bind_statement_named_mut(&mut stmt, &params);

        if let Statement::Select(select) = stmt {
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
    fn test_mutation_equivalence_to_clone() {
        // Verify that mutation produces the same result as the clone-based approach
        use crate::cache::prepared_statement::bind::bind_parameters;

        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        let params = vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))];

        // Clone-based binding
        let cloned = bind_parameters(&stmt, &params);

        // Mutation-based binding
        let mut mutated = stmt.clone();
        bind_statement_mut(&mut mutated, &params);

        // Both should produce the same result
        assert_eq!(format!("{:?}", cloned), format!("{:?}", mutated));
    }
}
