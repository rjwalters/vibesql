//! Parse-time folding of empty `IN ()` lists for the arena parser (issue
//! #6733). Mirrors `parser/expressions/empty_in.rs`: both parsers must produce
//! equivalent ASTs, so `<expr> [NOT] IN ()` becomes a boolean literal here too
//! whenever the operand holds no function call outside a subquery.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::{
    arena::{Expression, ExtendedExpr},
    BinaryOperator,
};
use vibesql_types::SqlValue;

use super::ArenaParser;

impl<'arena> ArenaParser<'arena> {
    /// Build the node for `<left> [NOT] IN (<values>)`, folding an empty list
    /// to `negated` (FALSE for `IN ()`, TRUE for `NOT IN ()`) like SQLite.
    pub(super) fn build_in_list_expression(
        &mut self,
        left: Expression<'arena>,
        values: BumpVec<'arena, Expression<'arena>>,
        negated: bool,
    ) -> Expression<'arena> {
        if values.is_empty() && !arena_expression_has_function(&left) {
            return Expression::Literal(SqlValue::Boolean(negated));
        }
        let left_ref = self.arena.alloc(left);
        Expression::Extended(self.arena.alloc(ExtendedExpr::InList {
            expr: left_ref,
            values,
            negated,
        }))
    }
}

/// SQLite's `EP_HasFunc` for an arena expression: a function call anywhere in
/// `expr` except inside a subquery. See `vibesql_ast::has_function::has_function_call`
/// for the rules.
fn arena_expression_has_function(expr: &Expression<'_>) -> bool {
    let any = |exprs: &[Expression<'_>]| exprs.iter().any(arena_expression_has_function);
    match expr {
        Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::Default => false,
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => true,
        Expression::BinaryOp { op, left, right } => {
            matches!(op, BinaryOperator::JsonExtract | BinaryOperator::JsonExtractText)
                || arena_expression_has_function(left)
                || arena_expression_has_function(right)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => any(children),
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. } => arena_expression_has_function(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            arena_expression_has_function(left) || arena_expression_has_function(right)
        }
        Expression::Extended(ext) => match ext {
            ExtendedExpr::Function { .. }
            | ExtendedExpr::AggregateFunction { .. }
            | ExtendedExpr::WindowFunction { .. }
            | ExtendedExpr::Like { .. }
            | ExtendedExpr::Glob { .. }
            | ExtendedExpr::MatchAgainst { .. }
            | ExtendedExpr::Position { .. }
            | ExtendedExpr::Trim { .. }
            | ExtendedExpr::Extract { .. }
            | ExtendedExpr::NextValue { .. } => true,
            ExtendedExpr::ScalarSubquery(_)
            | ExtendedExpr::Exists { .. }
            | ExtendedExpr::DuplicateKeyValue { .. }
            | ExtendedExpr::PseudoVariable { .. }
            | ExtendedExpr::SessionVariable { .. } => false,
            ExtendedExpr::In { expr, .. }
            | ExtendedExpr::QuantifiedComparison { expr, .. }
            | ExtendedExpr::Cast { expr, .. }
            | ExtendedExpr::Interval { value: expr, .. } => arena_expression_has_function(expr),
            ExtendedExpr::InList { expr, values, .. } => {
                arena_expression_has_function(expr) || any(values)
            }
            ExtendedExpr::Between { expr, low, high, .. } => {
                arena_expression_has_function(expr)
                    || arena_expression_has_function(low)
                    || arena_expression_has_function(high)
            }
            ExtendedExpr::Case { operand, when_clauses, else_result } => {
                operand.is_some_and(|op| arena_expression_has_function(op))
                    || when_clauses
                        .iter()
                        .any(|w| any(&w.conditions) || arena_expression_has_function(&w.result))
                    || else_result.is_some_and(|e| arena_expression_has_function(e))
            }
            ExtendedExpr::RowValueConstructor(items) => any(items),
        },
    }
}
