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
//!
//! ## Invariant: binding and counting must stay in lockstep
//!
//! Every expression position visited here MUST also be visited by the
//! corresponding `walk_*` function in `vibesql_ast::visitor` (which backs
//! [`super::count_placeholders`], the source of `PreparedStatement::param_count`),
//! and vice versa. An asymmetry is a user-visible bug in one of two directions:
//!
//! - counted but not bound → the placeholder survives binding and the executor rejects it with
//!   "Unbound placeholder ?N";
//! - bound but not counted → `param_count` is too low and `bind()` rejects the call with a spurious
//!   "Parameter count mismatch".
//!
//! See #6359 / PR #6411, where `SELECT ... LIMIT ?` hit the second case.

#[cfg(test)]
use std::collections::HashMap;

use vibesql_ast::{
    ConflictTargetItem, DeleteStmt, Expression, FromClause, GroupByClause, GroupingElement,
    GroupingSet, InsertSource, InsertStmt, MixedGroupingItem, OnConflictAction, SelectItem,
    SelectStmt, Statement, UpdateStmt, WhereClause,
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

    // Named window definitions (WINDOW w AS (...))
    if let Some(defs) = &mut stmt.window_definitions {
        for def in defs {
            bind_window_spec_mut(&mut def.spec, params);
        }
    }

    // ORDER BY
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_mut(&mut item.expr, params);
        }
    }

    // LIMIT / OFFSET (`LIMIT ? OFFSET ?`)
    if let Some(limit) = &mut stmt.limit {
        bind_expression_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_mut(offset, params);
    }

    // Set operation (UNION, INTERSECT, EXCEPT)
    if let Some(set_op) = &mut stmt.set_operation {
        bind_select_mut(&mut set_op.right, params);
    }

    // Standalone VALUES rows (`VALUES (?), (?)`)
    if let Some(rows) = &mut stmt.values {
        for row in rows {
            for expr in row {
                bind_expression_mut(expr, params);
            }
        }
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
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_mut(&mut cte.query, params);
        }
    }

    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_expression_mut(expr, params);
                }
            }
        }
        InsertSource::Select(select) => bind_select_mut(select, params),
        InsertSource::DefaultValues => {
            // No parameters to bind for DEFAULT VALUES
        }
    }

    // ON CONFLICT clauses (target expressions, target WHERE, DO UPDATE SET)
    for clause in &mut stmt.on_conflict {
        if let Some(items) = &mut clause.conflict_target {
            for item in items {
                if let ConflictTargetItem::Expression(expr) = item {
                    bind_expression_mut(expr, params);
                }
            }
        }
        if let Some(expr) = &mut clause.target_where {
            bind_expression_mut(expr, params);
        }
        if let OnConflictAction::DoUpdate { assignments, where_clause } = &mut clause.action {
            for assignment in assignments {
                bind_expression_mut(&mut assignment.value, params);
            }
            if let Some(expr) = where_clause {
                bind_expression_mut(expr, params);
            }
        }
    }

    if let Some(updates) = &mut stmt.on_duplicate_key_update {
        for assignment in updates {
            bind_expression_mut(&mut assignment.value, params);
        }
    }

    // RETURNING
    bind_returning_mut(stmt.returning.as_mut(), params);
}

/// Bind parameters in an UPDATE statement (in-place)
fn bind_update_mut(stmt: &mut UpdateStmt, params: &[SqlValue]) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_mut(&mut cte.query, params);
        }
    }

    for assignment in &mut stmt.assignments {
        bind_expression_mut(&mut assignment.value, params);
    }

    // FROM clause (UPDATE ... FROM syntax)
    if let Some(froms) = &mut stmt.from_clause {
        for from in froms {
            bind_from_clause_mut(from, params);
        }
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_mut(expr, params);
    }

    // ORDER BY / LIMIT / OFFSET
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_mut(&mut item.expr, params);
        }
    }
    if let Some(limit) = &mut stmt.limit {
        bind_expression_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_mut(offset, params);
    }

    // RETURNING
    bind_returning_mut(stmt.returning.as_mut(), params);
}

/// Bind parameters in a DELETE statement (in-place)
fn bind_delete_mut(stmt: &mut DeleteStmt, params: &[SqlValue]) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_mut(&mut cte.query, params);
        }
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_mut(expr, params);
    }

    // ORDER BY / LIMIT / OFFSET (SQLite extension: DELETE ... ORDER BY ... LIMIT ?)
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_mut(&mut item.expr, params);
        }
    }
    if let Some(limit) = &mut stmt.limit {
        bind_expression_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_mut(offset, params);
    }

    // RETURNING
    bind_returning_mut(stmt.returning.as_mut(), params);
}

/// Bind parameters in a RETURNING clause (in-place)
fn bind_returning_mut(items: Option<&mut Vec<SelectItem>>, params: &[SqlValue]) {
    if let Some(items) = items {
        for item in items {
            if let SelectItem::Expression { expr, .. } = item {
                bind_expression_mut(expr, params);
            }
        }
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
        FromClause::Values { rows, .. } => {
            // Bind parameters in VALUES expressions
            for row in rows {
                for expr in row {
                    bind_expression_mut(expr, params);
                }
            }
        }
        FromClause::TableFunction { args, .. } => {
            // Bind parameters in table function arguments
            for expr in args {
                bind_expression_mut(expr, params);
            }
        }
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
        | Expression::CollatedLiteral { .. }
        | Expression::ColumnRef(_)
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

        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                bind_expression_mut(child, params);
            }
        }

        Expression::Collate { expr, .. } => {
            bind_expression_mut(expr, params);
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                bind_expression_mut(msg, params);
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

        Expression::IsDistinctFrom { left, right, .. } => {
            bind_expression_mut(left, params);
            bind_expression_mut(right, params);
        }

        Expression::IsTruthValue { expr: inner, .. } => {
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

        Expression::Like { expr: inner, pattern, .. }
        | Expression::Glob { expr: inner, pattern, .. } => {
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

    // Named window definitions (WINDOW w AS (...))
    if let Some(defs) = &mut stmt.window_definitions {
        for def in defs {
            bind_window_spec_named_mut(&mut def.spec, params);
        }
    }

    // ORDER BY
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_named_mut(&mut item.expr, params);
        }
    }

    // LIMIT / OFFSET (`LIMIT :n OFFSET :m`)
    if let Some(limit) = &mut stmt.limit {
        bind_expression_named_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_named_mut(offset, params);
    }

    // Set operation
    if let Some(set_op) = &mut stmt.set_operation {
        bind_select_named_mut(&mut set_op.right, params);
    }

    // Standalone VALUES rows
    if let Some(rows) = &mut stmt.values {
        for row in rows {
            for expr in row {
                bind_expression_named_mut(expr, params);
            }
        }
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
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_named_mut(&mut cte.query, params);
        }
    }

    match &mut stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    bind_expression_named_mut(expr, params);
                }
            }
        }
        InsertSource::Select(select) => bind_select_named_mut(select, params),
        InsertSource::DefaultValues => {
            // No parameters to bind for DEFAULT VALUES
        }
    }

    // ON CONFLICT clauses
    for clause in &mut stmt.on_conflict {
        if let Some(items) = &mut clause.conflict_target {
            for item in items {
                if let ConflictTargetItem::Expression(expr) = item {
                    bind_expression_named_mut(expr, params);
                }
            }
        }
        if let Some(expr) = &mut clause.target_where {
            bind_expression_named_mut(expr, params);
        }
        if let OnConflictAction::DoUpdate { assignments, where_clause } = &mut clause.action {
            for assignment in assignments {
                bind_expression_named_mut(&mut assignment.value, params);
            }
            if let Some(expr) = where_clause {
                bind_expression_named_mut(expr, params);
            }
        }
    }

    if let Some(updates) = &mut stmt.on_duplicate_key_update {
        for assignment in updates {
            bind_expression_named_mut(&mut assignment.value, params);
        }
    }

    bind_returning_named_mut(stmt.returning.as_mut(), params);
}

#[cfg(test)]
fn bind_update_named_mut(stmt: &mut UpdateStmt, params: &HashMap<String, SqlValue>) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_named_mut(&mut cte.query, params);
        }
    }

    for assignment in &mut stmt.assignments {
        bind_expression_named_mut(&mut assignment.value, params);
    }

    // FROM clause (UPDATE ... FROM syntax)
    if let Some(froms) = &mut stmt.from_clause {
        for from in froms {
            bind_from_clause_named_mut(from, params);
        }
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_named_mut(expr, params);
    }

    // ORDER BY / LIMIT / OFFSET
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_named_mut(&mut item.expr, params);
        }
    }
    if let Some(limit) = &mut stmt.limit {
        bind_expression_named_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_named_mut(offset, params);
    }

    bind_returning_named_mut(stmt.returning.as_mut(), params);
}

#[cfg(test)]
fn bind_delete_named_mut(stmt: &mut DeleteStmt, params: &HashMap<String, SqlValue>) {
    // CTEs
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes {
            bind_select_named_mut(&mut cte.query, params);
        }
    }

    if let Some(WhereClause::Condition(expr)) = &mut stmt.where_clause {
        bind_expression_named_mut(expr, params);
    }

    // ORDER BY / LIMIT / OFFSET
    if let Some(order_by) = &mut stmt.order_by {
        for item in order_by {
            bind_expression_named_mut(&mut item.expr, params);
        }
    }
    if let Some(limit) = &mut stmt.limit {
        bind_expression_named_mut(limit, params);
    }
    if let Some(offset) = &mut stmt.offset {
        bind_expression_named_mut(offset, params);
    }

    bind_returning_named_mut(stmt.returning.as_mut(), params);
}

/// Bind named parameters in a RETURNING clause (in-place)
#[cfg(test)]
fn bind_returning_named_mut(
    items: Option<&mut Vec<SelectItem>>,
    params: &HashMap<String, SqlValue>,
) {
    if let Some(items) = items {
        for item in items {
            if let SelectItem::Expression { expr, .. } = item {
                bind_expression_named_mut(expr, params);
            }
        }
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
        FromClause::Values { rows, .. } => {
            // Bind parameters in VALUES expressions
            for row in rows {
                for expr in row {
                    bind_expression_named_mut(expr, params);
                }
            }
        }
        FromClause::TableFunction { args, .. } => {
            // Bind parameters in table-valued function arguments
            for expr in args {
                bind_expression_named_mut(expr, params);
            }
        }
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
        | Expression::CollatedLiteral { .. }
        | Expression::ColumnRef(_)
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

        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                bind_expression_named_mut(child, params);
            }
        }

        Expression::Collate { expr, .. } => {
            bind_expression_named_mut(expr, params);
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                bind_expression_named_mut(msg, params);
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

        Expression::IsDistinctFrom { left, right, .. } => {
            bind_expression_named_mut(left, params);
            bind_expression_named_mut(right, params);
        }

        Expression::IsTruthValue { expr: inner, .. } => {
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

        Expression::Like { expr: inner, pattern, .. }
        | Expression::Glob { expr: inner, pattern, .. } => {
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
    use vibesql_ast::InsertSource;

    use super::*;

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
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "id", false,
            ))),
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
                assert_eq!(
                    rows[0][1],
                    Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice")))
                );
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
            SqlValue::Integer(42),                          // $1
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

    /// Regression test for the `LIMIT ?` / `OFFSET ?` gap (#6359, PR #6411):
    /// LIMIT and OFFSET placeholders must be both counted and bound.
    #[test]
    fn test_bind_select_limit_offset_mut() {
        use crate::cache::prepared_statement::bind::count_placeholders;

        let sql = "SELECT * FROM t ORDER BY a LIMIT ? OFFSET ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();

        // Counting side: both placeholders are visible to count_placeholders.
        assert_eq!(count_placeholders(&stmt), 2);

        // Binding side: both placeholders are replaced with literals.
        let mut bound = stmt.clone();
        bind_statement_mut(&mut bound, &[SqlValue::Integer(2), SqlValue::Integer(1)]);

        if let Statement::Select(select) = &bound {
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(2))));
            assert_eq!(select.offset, Some(Expression::Literal(SqlValue::Integer(1))));
        } else {
            panic!("Expected SELECT statement");
        }
        assert_eq!(count_placeholders(&bound), 0);
    }

    /// DELETE ORDER BY / LIMIT / OFFSET placeholders were counted but never
    /// bound, so they reached the executor as "Unbound placeholder" errors.
    #[test]
    fn test_bind_delete_limit_mut() {
        use crate::cache::prepared_statement::bind::count_placeholders;

        let sql = "DELETE FROM t WHERE a = ? ORDER BY b LIMIT ?";
        let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
        assert_eq!(count_placeholders(&stmt), 2);

        let mut bound = stmt.clone();
        bind_statement_mut(&mut bound, &[SqlValue::Integer(7), SqlValue::Integer(3)]);

        if let Statement::Delete(delete) = &bound {
            assert_eq!(delete.limit, Some(Expression::Literal(SqlValue::Integer(3))));
        } else {
            panic!("Expected DELETE statement");
        }
        assert_eq!(count_placeholders(&bound), 0);
    }

    /// Counting (`walk_*`) and binding (`bind_*_mut`) must cover exactly the
    /// same placeholder positions. For each statement below: the count must
    /// match the number of parameters supplied, and after binding no
    /// placeholder may survive.
    #[test]
    fn test_count_and_bind_stay_in_lockstep() {
        use crate::cache::prepared_statement::bind::count_placeholders;

        let cases: &[(&str, usize)] = &[
            ("SELECT * FROM t WHERE a = ?", 1),
            ("SELECT * FROM t ORDER BY a LIMIT ?", 1),
            ("SELECT * FROM t ORDER BY a LIMIT ? OFFSET ?", 2),
            ("SELECT ? FROM t WHERE a = ? LIMIT ?", 3),
            ("VALUES (?, ?)", 2),
            ("INSERT INTO t (a, b) VALUES (?, ?) RETURNING a + ?", 3),
            ("UPDATE t SET a = ? WHERE b = ? RETURNING a + ?", 3),
            ("DELETE FROM t WHERE a = ? ORDER BY b LIMIT ? OFFSET ?", 3),
            ("DELETE FROM t WHERE a = ? RETURNING a + ?", 2),
        ];

        for (sql, expected_params) in cases {
            let stmt = vibesql_parser::Parser::parse_sql(sql)
                .unwrap_or_else(|e| panic!("failed to parse {sql}: {e:?}"));
            assert_eq!(
                count_placeholders(&stmt),
                *expected_params,
                "placeholder count mismatch for: {sql}"
            );

            let params: Vec<SqlValue> =
                (0..*expected_params).map(|i| SqlValue::Integer(i as i64 + 1)).collect();
            let mut bound = stmt.clone();
            bind_statement_mut(&mut bound, &params);
            assert_eq!(
                count_placeholders(&bound),
                0,
                "placeholder survived binding (counted but not bound) for: {sql}"
            );
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
