//! Visitor pattern for AST traversal
//!
//! This module provides traits and functions for traversing SQL AST nodes
//! without duplicating traversal logic across the codebase.
//!
//! # Overview
//!
//! The visitor pattern allows separating traversal logic from processing logic.
//! Instead of writing custom recursive `match` statements for each use case,
//! implement the appropriate visitor trait and let the `walk_*` functions handle traversal.
//!
//! # Visitor Traits
//!
//! - [`ExpressionVisitor`]: Visit expression nodes (read-only traversal)
//! - [`ExpressionMutVisitor`]: Visit and transform expression nodes (mutable traversal)
//! - [`StatementVisitor`]: Visit statement nodes (read-only traversal)
//!
//! # Control Flow
//!
//! Use [`VisitResult`] to control traversal:
//! - `Continue`: Visit children normally
//! - `Skip`: Skip children, continue with siblings
//! - `Stop`: Stop traversal entirely
//!
//! # Examples
//!
//! ## Collecting column references
//!
//! ```text
//! struct ColumnCollector {
//!     columns: Vec<(Option<String>, String)>,
//! }
//!
//! impl ExpressionVisitor for ColumnCollector {
//!     fn visit_column_ref(&mut self, table: Option<&str>, column: &str) -> VisitResult {
//!         self.columns.push((table.map(String::from), column.to_string()));
//!         VisitResult::Continue
//!     }
//! }
//!
//! let mut collector = ColumnCollector { columns: vec![] };
//! walk_expression(&mut collector, &expr);
//! ```
//!
//! ## Constant folding transformer
//!
//! ```text
//! struct ConstantFolder;
//!
//! impl ExpressionMutVisitor for ConstantFolder {
//!     fn post_visit_expression(&mut self, expr: Expression) -> Expression {
//!         match &expr {
//!             Expression::BinaryOp { op: BinaryOperator::Add, left, right } => {
//!                 if let (Expression::Literal(SqlValue::Integer(a)),
//!                         Expression::Literal(SqlValue::Integer(b))) =
//!                     (left.as_ref(), right.as_ref())
//!                 {
//!                     return Expression::Literal(SqlValue::Integer(a + b));
//!                 }
//!                 expr
//!             }
//!             _ => expr,
//!         }
//!     }
//! }
//! ```

use crate::{
    Assignment, DeleteStmt, Expression, FromClause, GroupByClause, GroupingElement, GroupingSet,
    InsertSource, InsertStmt, MixedGroupingItem, SelectItem, SelectStmt, Statement, UpdateStmt,
    WhereClause, WindowFunctionSpec, WindowSpec,
};

// ============================================================================
// Visit Result
// ============================================================================

/// Result of visiting a node, controlling traversal behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisitResult {
    /// Continue visiting children normally
    Continue,
    /// Skip visiting children, continue with siblings
    Skip,
    /// Stop traversal entirely
    Stop,
}

impl VisitResult {
    /// Returns true if traversal should stop
    #[inline]
    pub fn should_stop(&self) -> bool {
        matches!(self, VisitResult::Stop)
    }

    /// Returns true if children should be skipped
    #[inline]
    pub fn should_skip(&self) -> bool {
        matches!(self, VisitResult::Skip | VisitResult::Stop)
    }
}

// ============================================================================
// Expression Visitor (Read-only)
// ============================================================================

/// Visitor trait for traversing expressions (read-only)
///
/// Implement this trait to process expressions without modifying them.
/// The `walk_expression` function handles the recursive traversal.
///
/// Default implementations return `VisitResult::Continue` for all methods.
pub trait ExpressionVisitor {
    /// Called before visiting an expression's children (pre-order)
    fn pre_visit_expression(&mut self, _expr: &Expression) -> VisitResult {
        VisitResult::Continue
    }

    /// Called after visiting an expression's children (post-order)
    fn post_visit_expression(&mut self, _expr: &Expression) -> VisitResult {
        VisitResult::Continue
    }

    // Leaf node visitors - override for specific handling

    /// Visit a literal value
    fn visit_literal(&mut self, _value: &vibesql_types::SqlValue) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit a placeholder (`?`)
    fn visit_placeholder(&mut self, _index: usize) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit a numbered placeholder (`$1`, `$2`, etc.)
    fn visit_numbered_placeholder(&mut self, _number: usize) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit a named placeholder (`:name`)
    fn visit_named_placeholder(&mut self, _name: &str) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit a column reference
    fn visit_column_ref(&mut self, _table: Option<&str>, _column: &str) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit a wildcard (`*`)
    fn visit_wildcard(&mut self) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit CURRENT_DATE
    fn visit_current_date(&mut self) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit CURRENT_TIME
    fn visit_current_time(&mut self, _precision: Option<u32>) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit CURRENT_TIMESTAMP
    fn visit_current_timestamp(&mut self, _precision: Option<u32>) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit DEFAULT keyword
    fn visit_default(&mut self) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit NEXT VALUE FOR sequence
    fn visit_next_value(&mut self, _sequence_name: &str) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit session variable (`@@name`)
    fn visit_session_variable(&mut self, _name: &str) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit pseudo-variable (OLD.col, NEW.col)
    fn visit_pseudo_variable(
        &mut self,
        _pseudo_table: &crate::PseudoTable,
        _column: &str,
    ) -> VisitResult {
        VisitResult::Continue
    }

    /// Visit VALUES(column) for duplicate key handling
    fn visit_duplicate_key_value(&mut self, _column: &str) -> VisitResult {
        VisitResult::Continue
    }
}

/// Walk an expression tree, calling visitor methods in pre/post order
pub fn walk_expression<V: ExpressionVisitor>(visitor: &mut V, expr: &Expression) -> VisitResult {
    // Pre-order visit
    let result = visitor.pre_visit_expression(expr);
    if result.should_stop() {
        return VisitResult::Stop;
    }
    if result.should_skip() {
        return VisitResult::Continue;
    }

    // Visit node-specific and recurse into children
    let result = match expr {
        Expression::Literal(value) => visitor.visit_literal(value),

        Expression::Placeholder(index) => visitor.visit_placeholder(*index),

        Expression::NumberedPlaceholder(number) => visitor.visit_numbered_placeholder(*number),

        Expression::NamedPlaceholder(name) => visitor.visit_named_placeholder(name),

        Expression::ColumnRef { table, column } => {
            visitor.visit_column_ref(table.as_deref(), column)
        }

        Expression::BinaryOp { left, right, .. } => {
            let result = walk_expression(visitor, left);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_expression(visitor, right)
        }

        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                let result = walk_expression(visitor, child);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            VisitResult::Continue
        }

        Expression::UnaryOp { expr: inner, .. } => walk_expression(visitor, inner),

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                let result = walk_expression(visitor, arg);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            VisitResult::Continue
        }

        Expression::IsNull { expr: inner, .. } => walk_expression(visitor, inner),

        Expression::Wildcard => visitor.visit_wildcard(),

        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                let result = walk_expression(visitor, op);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    let result = walk_expression(visitor, cond);
                    if result.should_stop() {
                        return VisitResult::Stop;
                    }
                }
                let result = walk_expression(visitor, &clause.result);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            if let Some(else_expr) = else_result {
                walk_expression(visitor, else_expr)
            } else {
                VisitResult::Continue
            }
        }

        Expression::ScalarSubquery(select) => {
            walk_select(visitor, select);
            VisitResult::Continue
        }

        Expression::In { expr: inner, subquery, .. } => {
            let result = walk_expression(visitor, inner);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_select(visitor, subquery);
            VisitResult::Continue
        }

        Expression::InList { expr: inner, values, .. } => {
            let result = walk_expression(visitor, inner);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            for value in values {
                let result = walk_expression(visitor, value);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            VisitResult::Continue
        }

        Expression::Between { expr: inner, low, high, .. } => {
            let result = walk_expression(visitor, inner);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            let result = walk_expression(visitor, low);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_expression(visitor, high)
        }

        Expression::Cast { expr: inner, .. } => walk_expression(visitor, inner),

        Expression::Position { substring, string, .. } => {
            let result = walk_expression(visitor, substring);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_expression(visitor, string)
        }

        Expression::Trim { removal_char, string, .. } => {
            if let Some(removal) = removal_char {
                let result = walk_expression(visitor, removal);
                if result.should_stop() {
                    return VisitResult::Stop;
                }
            }
            walk_expression(visitor, string)
        }

        Expression::Extract { expr: inner, .. } => walk_expression(visitor, inner),

        Expression::Like { expr: inner, pattern, .. } => {
            let result = walk_expression(visitor, inner);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_expression(visitor, pattern)
        }

        Expression::Exists { subquery, .. } => {
            walk_select(visitor, subquery);
            VisitResult::Continue
        }

        Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
            let result = walk_expression(visitor, inner);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_select(visitor, subquery);
            VisitResult::Continue
        }

        Expression::CurrentDate => visitor.visit_current_date(),

        Expression::CurrentTime { precision } => visitor.visit_current_time(*precision),

        Expression::CurrentTimestamp { precision } => visitor.visit_current_timestamp(*precision),

        Expression::Interval { value, .. } => walk_expression(visitor, value),

        Expression::Default => visitor.visit_default(),

        Expression::DuplicateKeyValue { column } => visitor.visit_duplicate_key_value(column),

        Expression::WindowFunction { function, over } => {
            let result = walk_window_function(visitor, function);
            if result.should_stop() {
                return VisitResult::Stop;
            }
            walk_window_spec(visitor, over)
        }

        Expression::NextValue { sequence_name } => visitor.visit_next_value(sequence_name),

        Expression::MatchAgainst { search_modifier, .. } => {
            walk_expression(visitor, search_modifier)
        }

        Expression::PseudoVariable { pseudo_table, column } => {
            visitor.visit_pseudo_variable(pseudo_table, column)
        }

        Expression::SessionVariable { name } => visitor.visit_session_variable(name),
    };

    if result.should_stop() {
        return VisitResult::Stop;
    }

    // Post-order visit
    visitor.post_visit_expression(expr)
}

/// Walk a window function specification
fn walk_window_function<V: ExpressionVisitor>(
    visitor: &mut V,
    spec: &WindowFunctionSpec,
) -> VisitResult {
    let args = match spec {
        WindowFunctionSpec::Aggregate { args, .. }
        | WindowFunctionSpec::Ranking { args, .. }
        | WindowFunctionSpec::Value { args, .. } => args,
    };
    for arg in args {
        let result = walk_expression(visitor, arg);
        if result.should_stop() {
            return VisitResult::Stop;
        }
    }
    VisitResult::Continue
}

/// Walk a window specification
fn walk_window_spec<V: ExpressionVisitor>(visitor: &mut V, spec: &WindowSpec) -> VisitResult {
    if let Some(partition_by) = &spec.partition_by {
        for expr in partition_by {
            let result = walk_expression(visitor, expr);
            if result.should_stop() {
                return VisitResult::Stop;
            }
        }
    }
    if let Some(order_by) = &spec.order_by {
        for item in order_by {
            let result = walk_expression(visitor, &item.expr);
            if result.should_stop() {
                return VisitResult::Stop;
            }
        }
    }
    // Frame bounds can contain expressions
    if let Some(frame) = &spec.frame {
        if let crate::FrameBound::Preceding(expr) | crate::FrameBound::Following(expr) =
            &frame.start
        {
            let result = walk_expression(visitor, expr);
            if result.should_stop() {
                return VisitResult::Stop;
            }
        }
        if let Some(crate::FrameBound::Preceding(expr) | crate::FrameBound::Following(expr)) =
            &frame.end
        {
            let result = walk_expression(visitor, expr);
            if result.should_stop() {
                return VisitResult::Stop;
            }
        }
    }
    VisitResult::Continue
}

// ============================================================================
// Expression Mutable Visitor (Transformations)
// ============================================================================

/// Visitor trait for transforming expressions (mutable traversal)
///
/// Implement this trait to transform expressions during traversal.
/// The `transform_expression` function handles the recursive traversal.
///
/// The transformation happens in post-order (children are transformed first).
pub trait ExpressionMutVisitor {
    /// Called before transforming children, can skip the node
    fn pre_visit_expression(&mut self, _expr: &Expression) -> VisitResult {
        VisitResult::Continue
    }

    /// Transform an expression after its children have been transformed
    ///
    /// Return the expression unchanged if no transformation is needed.
    fn post_visit_expression(&mut self, expr: Expression) -> Expression {
        expr
    }
}

/// Transform an expression tree, calling visitor methods in pre/post order
///
/// Returns the transformed expression. Children are transformed before parents (post-order).
pub fn transform_expression<V: ExpressionMutVisitor>(
    visitor: &mut V,
    expr: Expression,
) -> Expression {
    // Pre-order check
    let result = visitor.pre_visit_expression(&expr);
    if result.should_skip() {
        return expr;
    }

    // Transform children first, then post-visit
    let transformed = match expr {
        // Leaf nodes - no children to transform
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
        | Expression::NextValue { .. }
        | Expression::DuplicateKeyValue { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => expr,

        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op,
            left: Box::new(transform_expression(visitor, *left)),
            right: Box::new(transform_expression(visitor, *right)),
        },

        Expression::Conjunction(children) => Expression::Conjunction(
            children.into_iter().map(|c| transform_expression(visitor, c)).collect(),
        ),

        Expression::Disjunction(children) => Expression::Disjunction(
            children.into_iter().map(|c| transform_expression(visitor, c)).collect(),
        ),

        Expression::UnaryOp { op, expr: inner } => {
            Expression::UnaryOp { op, expr: Box::new(transform_expression(visitor, *inner)) }
        }

        Expression::Function { name, args, character_unit } => Expression::Function {
            name,
            args: args.into_iter().map(|a| transform_expression(visitor, a)).collect(),
            character_unit,
        },

        Expression::AggregateFunction { name, distinct, args } => Expression::AggregateFunction {
            name,
            distinct,
            args: args.into_iter().map(|a| transform_expression(visitor, a)).collect(),
        },

        Expression::IsNull { expr: inner, negated } => {
            Expression::IsNull { expr: Box::new(transform_expression(visitor, *inner)), negated }
        }

        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.map(|op| Box::new(transform_expression(visitor, *op))),
            when_clauses: when_clauses
                .into_iter()
                .map(|wc| crate::CaseWhen {
                    conditions: wc
                        .conditions
                        .into_iter()
                        .map(|c| transform_expression(visitor, c))
                        .collect(),
                    result: transform_expression(visitor, wc.result),
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(transform_expression(visitor, *e))),
        },

        Expression::ScalarSubquery(select) => {
            Expression::ScalarSubquery(Box::new(transform_select(visitor, *select)))
        }

        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(transform_expression(visitor, *inner)),
            subquery: Box::new(transform_select(visitor, *subquery)),
            negated,
        },

        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(transform_expression(visitor, *inner)),
            values: values.into_iter().map(|v| transform_expression(visitor, v)).collect(),
            negated,
        },

        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(transform_expression(visitor, *inner)),
            low: Box::new(transform_expression(visitor, *low)),
            high: Box::new(transform_expression(visitor, *high)),
            negated,
            symmetric,
        },

        Expression::Cast { expr: inner, data_type } => {
            Expression::Cast { expr: Box::new(transform_expression(visitor, *inner)), data_type }
        }

        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(transform_expression(visitor, *substring)),
            string: Box::new(transform_expression(visitor, *string)),
            character_unit,
        },

        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position,
            removal_char: removal_char.map(|r| Box::new(transform_expression(visitor, *r))),
            string: Box::new(transform_expression(visitor, *string)),
        },

        Expression::Extract { field, expr: inner } => {
            Expression::Extract { field, expr: Box::new(transform_expression(visitor, *inner)) }
        }

        Expression::Like { expr: inner, pattern, negated } => Expression::Like {
            expr: Box::new(transform_expression(visitor, *inner)),
            pattern: Box::new(transform_expression(visitor, *pattern)),
            negated,
        },

        Expression::Exists { subquery, negated } => {
            Expression::Exists { subquery: Box::new(transform_select(visitor, *subquery)), negated }
        }

        Expression::QuantifiedComparison { expr: inner, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(transform_expression(visitor, *inner)),
                op,
                quantifier,
                subquery: Box::new(transform_select(visitor, *subquery)),
            }
        }

        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(transform_expression(visitor, *value)),
                unit,
                leading_precision,
                fractional_precision,
            }
        }

        Expression::WindowFunction { function, over } => Expression::WindowFunction {
            function: transform_window_function(visitor, function),
            over: transform_window_spec(visitor, over),
        },

        Expression::MatchAgainst { columns, search_modifier, mode } => Expression::MatchAgainst {
            columns,
            search_modifier: Box::new(transform_expression(visitor, *search_modifier)),
            mode,
        },
    };

    // Post-order transform
    visitor.post_visit_expression(transformed)
}

/// Transform a window function specification
fn transform_window_function<V: ExpressionMutVisitor>(
    visitor: &mut V,
    spec: WindowFunctionSpec,
) -> WindowFunctionSpec {
    match spec {
        WindowFunctionSpec::Aggregate { name, args } => WindowFunctionSpec::Aggregate {
            name,
            args: args.into_iter().map(|a| transform_expression(visitor, a)).collect(),
        },
        WindowFunctionSpec::Ranking { name, args } => WindowFunctionSpec::Ranking {
            name,
            args: args.into_iter().map(|a| transform_expression(visitor, a)).collect(),
        },
        WindowFunctionSpec::Value { name, args } => WindowFunctionSpec::Value {
            name,
            args: args.into_iter().map(|a| transform_expression(visitor, a)).collect(),
        },
    }
}

/// Transform a window specification
fn transform_window_spec<V: ExpressionMutVisitor>(visitor: &mut V, spec: WindowSpec) -> WindowSpec {
    WindowSpec {
        partition_by: spec
            .partition_by
            .map(|exprs| exprs.into_iter().map(|e| transform_expression(visitor, e)).collect()),
        order_by: spec.order_by.map(|items| {
            items
                .into_iter()
                .map(|item| crate::OrderByItem {
                    expr: transform_expression(visitor, item.expr),
                    direction: item.direction,
                })
                .collect()
        }),
        frame: spec.frame.map(|f| crate::WindowFrame {
            unit: f.unit,
            start: transform_frame_bound(visitor, f.start),
            end: f.end.map(|e| transform_frame_bound(visitor, e)),
        }),
    }
}

/// Transform a frame bound
fn transform_frame_bound<V: ExpressionMutVisitor>(
    visitor: &mut V,
    bound: crate::FrameBound,
) -> crate::FrameBound {
    match bound {
        crate::FrameBound::Preceding(expr) => {
            crate::FrameBound::Preceding(Box::new(transform_expression(visitor, *expr)))
        }
        crate::FrameBound::Following(expr) => {
            crate::FrameBound::Following(Box::new(transform_expression(visitor, *expr)))
        }
        other => other,
    }
}

// ============================================================================
// Statement Visitor
// ============================================================================

/// Visitor trait for traversing statements
///
/// Provides methods for visiting DML statements that contain expressions.
/// The `walk_statement` function handles the recursive traversal.
pub trait StatementVisitor: ExpressionVisitor {
    /// Called when entering a SELECT statement
    fn enter_select(&mut self, _stmt: &SelectStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when exiting a SELECT statement
    fn exit_select(&mut self, _stmt: &SelectStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when entering an INSERT statement
    fn enter_insert(&mut self, _stmt: &InsertStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when exiting an INSERT statement
    fn exit_insert(&mut self, _stmt: &InsertStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when entering an UPDATE statement
    fn enter_update(&mut self, _stmt: &UpdateStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when exiting an UPDATE statement
    fn exit_update(&mut self, _stmt: &UpdateStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when entering a DELETE statement
    fn enter_delete(&mut self, _stmt: &DeleteStmt) -> VisitResult {
        VisitResult::Continue
    }

    /// Called when exiting a DELETE statement
    fn exit_delete(&mut self, _stmt: &DeleteStmt) -> VisitResult {
        VisitResult::Continue
    }
}

/// Walk a statement, visiting all contained expressions
pub fn walk_statement<V: StatementVisitor>(visitor: &mut V, stmt: &Statement) {
    match stmt {
        Statement::Select(select) => walk_select(visitor, select),
        Statement::Insert(insert) => walk_insert(visitor, insert),
        Statement::Update(update) => walk_update(visitor, update),
        Statement::Delete(delete) => walk_delete(visitor, delete),
        _ => {} // Other statement types don't contain expressions we need to visit
    }
}

/// Walk a SELECT statement
pub fn walk_select<V: ExpressionVisitor>(visitor: &mut V, stmt: &SelectStmt) {
    // Visit CTEs
    if let Some(ctes) = &stmt.with_clause {
        for cte in ctes {
            walk_select(visitor, &cte.query);
        }
    }

    // Visit select items
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            let result = walk_expression(visitor, expr);
            if result.should_stop() {
                return;
            }
        }
    }

    // Visit FROM clause
    if let Some(from) = &stmt.from {
        walk_from_clause(visitor, from);
    }

    // Visit WHERE
    if let Some(where_clause) = &stmt.where_clause {
        let result = walk_expression(visitor, where_clause);
        if result.should_stop() {
            return;
        }
    }

    // Visit GROUP BY
    if let Some(group_by) = &stmt.group_by {
        walk_group_by(visitor, group_by);
    }

    // Visit HAVING
    if let Some(having) = &stmt.having {
        let result = walk_expression(visitor, having);
        if result.should_stop() {
            return;
        }
    }

    // Visit ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            let result = walk_expression(visitor, &item.expr);
            if result.should_stop() {
                return;
            }
        }
    }

    // Visit set operation
    if let Some(set_op) = &stmt.set_operation {
        walk_select(visitor, &set_op.right);
    }
}

/// Walk a FROM clause
fn walk_from_clause<V: ExpressionVisitor>(visitor: &mut V, from: &FromClause) {
    match from {
        FromClause::Table { .. } => {}
        FromClause::Subquery { query, .. } => walk_select(visitor, query),
        FromClause::Join { left, right, condition, .. } => {
            walk_from_clause(visitor, left);
            walk_from_clause(visitor, right);
            if let Some(cond) = condition {
                walk_expression(visitor, cond);
            }
        }
    }
}

/// Walk a GROUP BY clause
fn walk_group_by<V: ExpressionVisitor>(visitor: &mut V, group_by: &GroupByClause) {
    match group_by {
        GroupByClause::Simple(exprs) => {
            for expr in exprs {
                walk_expression(visitor, expr);
            }
        }
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            walk_grouping_elements(visitor, elements);
        }
        GroupByClause::GroupingSets(sets) => {
            for set in sets {
                for expr in &set.columns {
                    walk_expression(visitor, expr);
                }
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items {
                walk_mixed_grouping_item(visitor, item);
            }
        }
    }
}

fn walk_grouping_elements<V: ExpressionVisitor>(visitor: &mut V, elements: &[GroupingElement]) {
    for element in elements {
        match element {
            GroupingElement::Single(expr) => {
                walk_expression(visitor, expr);
            }
            GroupingElement::Composite(exprs) => {
                for expr in exprs {
                    walk_expression(visitor, expr);
                }
            }
        }
    }
}

fn walk_mixed_grouping_item<V: ExpressionVisitor>(visitor: &mut V, item: &MixedGroupingItem) {
    match item {
        MixedGroupingItem::Simple(expr) => {
            walk_expression(visitor, expr);
        }
        MixedGroupingItem::Rollup(elements) | MixedGroupingItem::Cube(elements) => {
            walk_grouping_elements(visitor, elements);
        }
        MixedGroupingItem::GroupingSets(sets) => {
            for set in sets {
                for expr in &set.columns {
                    walk_expression(visitor, expr);
                }
            }
        }
    }
}

/// Walk an INSERT statement
fn walk_insert<V: ExpressionVisitor>(visitor: &mut V, stmt: &InsertStmt) {
    match &stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    let result = walk_expression(visitor, expr);
                    if result.should_stop() {
                        return;
                    }
                }
            }
        }
        InsertSource::Select(select) => walk_select(visitor, select),
    }

    if let Some(updates) = &stmt.on_duplicate_key_update {
        for assignment in updates {
            let result = walk_expression(visitor, &assignment.value);
            if result.should_stop() {
                return;
            }
        }
    }
}

/// Walk an UPDATE statement
fn walk_update<V: ExpressionVisitor>(visitor: &mut V, stmt: &UpdateStmt) {
    for assignment in &stmt.assignments {
        let result = walk_expression(visitor, &assignment.value);
        if result.should_stop() {
            return;
        }
    }
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        walk_expression(visitor, expr);
    }
}

/// Walk a DELETE statement
fn walk_delete<V: ExpressionVisitor>(visitor: &mut V, stmt: &DeleteStmt) {
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        walk_expression(visitor, expr);
    }
}

// ============================================================================
// Statement Transformer
// ============================================================================

/// Transform a SELECT statement using a mutable visitor
pub fn transform_select<V: ExpressionMutVisitor>(visitor: &mut V, stmt: SelectStmt) -> SelectStmt {
    SelectStmt {
        with_clause: stmt.with_clause.map(|ctes| {
            ctes.into_iter()
                .map(|cte| crate::CommonTableExpr {
                    name: cte.name,
                    columns: cte.columns,
                    query: Box::new(transform_select(visitor, *cte.query)),
                })
                .collect()
        }),
        distinct: stmt.distinct,
        select_list: stmt
            .select_list
            .into_iter()
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => {
                    SelectItem::Expression { expr: transform_expression(visitor, expr), alias }
                }
                other => other,
            })
            .collect(),
        into_table: stmt.into_table,
        into_variables: stmt.into_variables,
        from: stmt.from.map(|f| transform_from_clause(visitor, f)),
        where_clause: stmt.where_clause.map(|w| transform_expression(visitor, w)),
        group_by: stmt.group_by.map(|g| transform_group_by(visitor, g)),
        having: stmt.having.map(|h| transform_expression(visitor, h)),
        order_by: stmt.order_by.map(|items| {
            items
                .into_iter()
                .map(|item| crate::OrderByItem {
                    expr: transform_expression(visitor, item.expr),
                    direction: item.direction,
                })
                .collect()
        }),
        limit: stmt.limit,
        offset: stmt.offset,
        set_operation: stmt.set_operation.map(|op| crate::SetOperation {
            op: op.op,
            all: op.all,
            right: Box::new(transform_select(visitor, *op.right)),
        }),
    }
}

/// Transform a FROM clause
fn transform_from_clause<V: ExpressionMutVisitor>(visitor: &mut V, from: FromClause) -> FromClause {
    match from {
        FromClause::Table { name, alias, column_aliases } => {
            FromClause::Table { name, alias, column_aliases }
        }
        FromClause::Subquery { query, alias, column_aliases } => FromClause::Subquery {
            query: Box::new(transform_select(visitor, *query)),
            alias,
            column_aliases,
        },
        FromClause::Join { left, right, join_type, condition, natural } => FromClause::Join {
            left: Box::new(transform_from_clause(visitor, *left)),
            right: Box::new(transform_from_clause(visitor, *right)),
            join_type,
            condition: condition.map(|c| transform_expression(visitor, c)),
            natural,
        },
    }
}

/// Transform a GROUP BY clause
fn transform_group_by<V: ExpressionMutVisitor>(
    visitor: &mut V,
    group_by: GroupByClause,
) -> GroupByClause {
    match group_by {
        GroupByClause::Simple(exprs) => GroupByClause::Simple(
            exprs.into_iter().map(|e| transform_expression(visitor, e)).collect(),
        ),
        GroupByClause::Rollup(elements) => {
            GroupByClause::Rollup(transform_grouping_elements(visitor, elements))
        }
        GroupByClause::Cube(elements) => {
            GroupByClause::Cube(transform_grouping_elements(visitor, elements))
        }
        GroupByClause::GroupingSets(sets) => GroupByClause::GroupingSets(
            sets.into_iter()
                .map(|set| GroupingSet {
                    columns: set
                        .columns
                        .into_iter()
                        .map(|e| transform_expression(visitor, e))
                        .collect(),
                })
                .collect(),
        ),
        GroupByClause::Mixed(items) => GroupByClause::Mixed(
            items.into_iter().map(|item| transform_mixed_grouping_item(visitor, item)).collect(),
        ),
    }
}

fn transform_grouping_elements<V: ExpressionMutVisitor>(
    visitor: &mut V,
    elements: Vec<GroupingElement>,
) -> Vec<GroupingElement> {
    elements
        .into_iter()
        .map(|element| match element {
            GroupingElement::Single(expr) => {
                GroupingElement::Single(transform_expression(visitor, expr))
            }
            GroupingElement::Composite(exprs) => GroupingElement::Composite(
                exprs.into_iter().map(|e| transform_expression(visitor, e)).collect(),
            ),
        })
        .collect()
}

fn transform_mixed_grouping_item<V: ExpressionMutVisitor>(
    visitor: &mut V,
    item: MixedGroupingItem,
) -> MixedGroupingItem {
    match item {
        MixedGroupingItem::Simple(expr) => {
            MixedGroupingItem::Simple(transform_expression(visitor, expr))
        }
        MixedGroupingItem::Rollup(elements) => {
            MixedGroupingItem::Rollup(transform_grouping_elements(visitor, elements))
        }
        MixedGroupingItem::Cube(elements) => {
            MixedGroupingItem::Cube(transform_grouping_elements(visitor, elements))
        }
        MixedGroupingItem::GroupingSets(sets) => MixedGroupingItem::GroupingSets(
            sets.into_iter()
                .map(|set| GroupingSet {
                    columns: set
                        .columns
                        .into_iter()
                        .map(|e| transform_expression(visitor, e))
                        .collect(),
                })
                .collect(),
        ),
    }
}

/// Transform an INSERT statement
pub fn transform_insert<V: ExpressionMutVisitor>(visitor: &mut V, stmt: InsertStmt) -> InsertStmt {
    InsertStmt {
        table_name: stmt.table_name,
        columns: stmt.columns,
        source: match stmt.source {
            InsertSource::Values(rows) => InsertSource::Values(
                rows.into_iter()
                    .map(|row| row.into_iter().map(|e| transform_expression(visitor, e)).collect())
                    .collect(),
            ),
            InsertSource::Select(select) => {
                InsertSource::Select(Box::new(transform_select(visitor, *select)))
            }
        },
        conflict_clause: stmt.conflict_clause,
        on_duplicate_key_update: stmt.on_duplicate_key_update.map(|updates| {
            updates
                .into_iter()
                .map(|a| Assignment {
                    column: a.column,
                    value: transform_expression(visitor, a.value),
                })
                .collect()
        }),
    }
}

/// Transform an UPDATE statement
pub fn transform_update<V: ExpressionMutVisitor>(visitor: &mut V, stmt: UpdateStmt) -> UpdateStmt {
    UpdateStmt {
        table_name: stmt.table_name,
        assignments: stmt
            .assignments
            .into_iter()
            .map(|a| Assignment { column: a.column, value: transform_expression(visitor, a.value) })
            .collect(),
        where_clause: stmt.where_clause.map(|w| match w {
            WhereClause::Condition(expr) => {
                WhereClause::Condition(transform_expression(visitor, expr))
            }
            other => other,
        }),
    }
}

/// Transform a DELETE statement
pub fn transform_delete<V: ExpressionMutVisitor>(visitor: &mut V, stmt: DeleteStmt) -> DeleteStmt {
    DeleteStmt {
        only: stmt.only,
        table_name: stmt.table_name,
        where_clause: stmt.where_clause.map(|w| match w {
            WhereClause::Condition(expr) => {
                WhereClause::Condition(transform_expression(visitor, expr))
            }
            other => other,
        }),
    }
}

/// Transform a statement
pub fn transform_statement<V: ExpressionMutVisitor>(visitor: &mut V, stmt: Statement) -> Statement {
    match stmt {
        Statement::Select(select) => {
            Statement::Select(Box::new(transform_select(visitor, *select)))
        }
        Statement::Insert(insert) => Statement::Insert(transform_insert(visitor, insert)),
        Statement::Update(update) => Statement::Update(transform_update(visitor, update)),
        Statement::Delete(delete) => Statement::Delete(transform_delete(visitor, delete)),
        other => other,
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Visit all expressions in a statement using a simple closure
///
/// This is a convenience function for cases where you just want to visit
/// all expressions without implementing a full visitor trait.
///
/// # Example
///
/// ```text
/// use vibesql_ast::visitor::visit_expressions;
///
/// let mut count = 0;
/// visit_expressions(&stmt, |expr| {
///     if matches!(expr, Expression::Placeholder(_)) {
///         count += 1;
///     }
/// });
/// ```
pub fn visit_expressions<F>(stmt: &Statement, visitor: F)
where
    F: FnMut(&Expression),
{
    struct ClosureVisitor<F> {
        closure: F,
    }

    impl<F: FnMut(&Expression)> ExpressionVisitor for ClosureVisitor<F> {
        fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
            (self.closure)(expr);
            VisitResult::Continue
        }
    }

    impl<F: FnMut(&Expression)> StatementVisitor for ClosureVisitor<F> {}

    let mut v = ClosureVisitor { closure: visitor };
    walk_statement(&mut v, stmt);
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    // Helper to create a simple expression for testing
    fn make_binary_expr() -> Expression {
        Expression::BinaryOp {
            op: crate::BinaryOperator::Plus,
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }
    }

    #[test]
    fn test_visit_result() {
        assert!(!VisitResult::Continue.should_stop());
        assert!(!VisitResult::Continue.should_skip());

        assert!(!VisitResult::Skip.should_stop());
        assert!(VisitResult::Skip.should_skip());

        assert!(VisitResult::Stop.should_stop());
        assert!(VisitResult::Stop.should_skip());
    }

    #[test]
    fn test_expression_visitor_defaults() {
        struct EmptyVisitor;
        impl ExpressionVisitor for EmptyVisitor {}

        let mut visitor = EmptyVisitor;
        let expr = make_binary_expr();
        let result = walk_expression(&mut visitor, &expr);
        assert_eq!(result, VisitResult::Continue);
    }

    #[test]
    fn test_literal_visitor() {
        struct LiteralCounter {
            count: usize,
        }
        impl ExpressionVisitor for LiteralCounter {
            fn visit_literal(&mut self, _value: &SqlValue) -> VisitResult {
                self.count += 1;
                VisitResult::Continue
            }
        }

        let mut visitor = LiteralCounter { count: 0 };
        let expr = make_binary_expr();
        walk_expression(&mut visitor, &expr);
        assert_eq!(visitor.count, 2);
    }

    #[test]
    fn test_placeholder_visitor() {
        struct PlaceholderCollector {
            indices: Vec<usize>,
        }
        impl ExpressionVisitor for PlaceholderCollector {
            fn visit_placeholder(&mut self, index: usize) -> VisitResult {
                self.indices.push(index);
                VisitResult::Continue
            }
        }

        let expr = Expression::BinaryOp {
            op: crate::BinaryOperator::And,
            left: Box::new(Expression::Placeholder(0)),
            right: Box::new(Expression::Placeholder(1)),
        };

        let mut visitor = PlaceholderCollector { indices: vec![] };
        walk_expression(&mut visitor, &expr);
        assert_eq!(visitor.indices, vec![0, 1]);
    }

    #[test]
    fn test_column_ref_visitor() {
        struct ColumnCollector {
            columns: Vec<(Option<String>, String)>,
        }
        impl ExpressionVisitor for ColumnCollector {
            fn visit_column_ref(&mut self, table: Option<&str>, column: &str) -> VisitResult {
                self.columns.push((table.map(String::from), column.to_string()));
                VisitResult::Continue
            }
        }

        let expr = Expression::BinaryOp {
            op: crate::BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
        };

        let mut visitor = ColumnCollector { columns: vec![] };
        walk_expression(&mut visitor, &expr);
        assert_eq!(
            visitor.columns,
            vec![(Some("users".to_string()), "id".to_string()), (None, "value".to_string())]
        );
    }

    #[test]
    fn test_stop_traversal() {
        struct StopAfterFirst {
            count: usize,
        }
        impl ExpressionVisitor for StopAfterFirst {
            fn visit_literal(&mut self, _value: &SqlValue) -> VisitResult {
                self.count += 1;
                VisitResult::Stop
            }
        }

        let mut visitor = StopAfterFirst { count: 0 };
        let expr = make_binary_expr();
        walk_expression(&mut visitor, &expr);
        assert_eq!(visitor.count, 1);
    }

    #[test]
    fn test_skip_children() {
        struct SkipBinaryOps {
            literals_seen: usize,
        }
        impl ExpressionVisitor for SkipBinaryOps {
            fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
                if matches!(expr, Expression::BinaryOp { .. }) {
                    VisitResult::Skip
                } else {
                    VisitResult::Continue
                }
            }
            fn visit_literal(&mut self, _value: &SqlValue) -> VisitResult {
                self.literals_seen += 1;
                VisitResult::Continue
            }
        }

        let mut visitor = SkipBinaryOps { literals_seen: 0 };
        let expr = make_binary_expr();
        walk_expression(&mut visitor, &expr);
        // Literals should not be visited since we skipped the binary op's children
        assert_eq!(visitor.literals_seen, 0);
    }

    #[test]
    fn test_transform_expression() {
        struct DoubleIntegers;
        impl ExpressionMutVisitor for DoubleIntegers {
            fn post_visit_expression(&mut self, expr: Expression) -> Expression {
                match expr {
                    Expression::Literal(SqlValue::Integer(n)) => {
                        Expression::Literal(SqlValue::Integer(n * 2))
                    }
                    other => other,
                }
            }
        }

        let mut visitor = DoubleIntegers;
        let expr = make_binary_expr();
        let transformed = transform_expression(&mut visitor, expr);

        match transformed {
            Expression::BinaryOp { left, right, .. } => {
                assert_eq!(*left, Expression::Literal(SqlValue::Integer(2)));
                assert_eq!(*right, Expression::Literal(SqlValue::Integer(4)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_transform_case_expression() {
        struct IncrementIntegers;
        impl ExpressionMutVisitor for IncrementIntegers {
            fn post_visit_expression(&mut self, expr: Expression) -> Expression {
                match expr {
                    Expression::Literal(SqlValue::Integer(n)) => {
                        Expression::Literal(SqlValue::Integer(n + 1))
                    }
                    other => other,
                }
            }
        }

        let expr = Expression::Case {
            operand: Some(Box::new(Expression::Literal(SqlValue::Integer(1)))),
            when_clauses: vec![crate::CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Integer(2))],
                result: Expression::Literal(SqlValue::Integer(3)),
            }],
            else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(4)))),
        };

        let mut visitor = IncrementIntegers;
        let transformed = transform_expression(&mut visitor, expr);

        match transformed {
            Expression::Case { operand, when_clauses, else_result } => {
                assert_eq!(*operand.unwrap(), Expression::Literal(SqlValue::Integer(2)));
                assert_eq!(
                    when_clauses[0].conditions[0],
                    Expression::Literal(SqlValue::Integer(3))
                );
                assert_eq!(when_clauses[0].result, Expression::Literal(SqlValue::Integer(4)));
                assert_eq!(*else_result.unwrap(), Expression::Literal(SqlValue::Integer(5)));
            }
            _ => panic!("Expected Case"),
        }
    }

    #[test]
    fn test_visit_expressions_closure() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(SqlValue::Integer(1)),
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: Some(Expression::BinaryOp {
                op: crate::BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                right: Box::new(Expression::Placeholder(0)),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        };

        let stmt = Statement::Select(Box::new(select));

        let mut placeholder_count = 0;
        visit_expressions(&stmt, |expr| {
            if matches!(expr, Expression::Placeholder(_)) {
                placeholder_count += 1;
            }
        });
        assert_eq!(placeholder_count, 1);
    }
}
