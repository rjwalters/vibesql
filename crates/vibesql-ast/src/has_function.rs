//! SQLite's `EP_HasFunc` expression property (issue #6733).
//!
//! SQLite folds `<expr> [NOT] IN ()` to a constant at parse time and discards
//! `<expr>` without resolving it, *unless* `<expr>` carries `EP_HasFunc` (it
//! contains a function call). [`has_function_call`] answers that question for
//! a VibeSQL expression; the parser uses it to decide whether to fold, and
//! executor passes that special-case an empty `InList` use it to tell a kept
//! operand from a discarded one.

use crate::{
    visitor::{walk_expression, ExpressionVisitor, VisitResult},
    BinaryOperator, Expression,
};

/// Does `expr` contain a function call outside any subquery?
///
/// This is SQLite's `EP_HasFunc`: the flag is set on function nodes and
/// propagates up through operands, CASE arms, and function arguments, but not
/// out of a subquery. `LIKE`, `GLOB`, `MATCH`, `REGEXP`, `->`, `->>`, and the
/// `CURRENT_*` keywords are function calls in SQLite, so they count too.
pub fn has_function_call(expr: &Expression) -> bool {
    let mut finder = HasFunctionFinder { found: false };
    walk_expression(&mut finder, expr);
    finder.found
}

struct HasFunctionFinder {
    found: bool,
}

impl ExpressionVisitor for HasFunctionFinder {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        match expr {
            Expression::Function { .. }
            | Expression::AggregateFunction { .. }
            | Expression::WindowFunction { .. }
            | Expression::Like { .. }
            | Expression::Glob { .. }
            | Expression::MatchAgainst { .. }
            | Expression::Position { .. }
            | Expression::Trim { .. }
            | Expression::Extract { .. }
            | Expression::NextValue { .. }
            | Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. }
            | Expression::BinaryOp {
                op: BinaryOperator::JsonExtract | BinaryOperator::JsonExtractText,
                ..
            } => {
                self.found = true;
                VisitResult::Stop
            }
            // A function inside a subquery does not mark the outer operand.
            Expression::ScalarSubquery(_) | Expression::Exists { .. } => VisitResult::Skip,
            // Only the left operand of an IN-subquery / quantified comparison
            // is outside the subquery.
            Expression::In { expr: inner, .. }
            | Expression::QuantifiedComparison { expr: inner, .. } => {
                if has_function_call(inner) {
                    self.found = true;
                    VisitResult::Stop
                } else {
                    VisitResult::Skip
                }
            }
            _ => VisitResult::Continue,
        }
    }
}
