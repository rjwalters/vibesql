//! Parse-time folding of empty `IN ()` lists (issue #6733).
//!
//! SQLite rewrites `<expr> IN ()` / `<expr> NOT IN ()` to the constant
//! `false` / `true` while parsing (`parse.y`, the `expr in_op LP exprlist RP`
//! rule). The left operand is deleted with `sqlite3ExprUnmapAndDelete` unless
//! it carries `EP_HasFunc`, so SQLite never resolves the names inside it:
//!
//! - `SELECT nosuch IN () FROM t1` returns `0` instead of reporting a missing column, while `SELECT
//!   abs(nosuch) IN () FROM t1` still fails (the operand holds a function call, so it is kept).
//! - `ALTER TABLE ... RENAME` leaves the names in a discarded operand alone (altertab3.test 3.2 /
//!   10.2), and schema re-validation never looks at them.
//!
//! VibeSQL mirrors this: the empty list is folded to a boolean literal and the
//! byte range of the discarded operand is recorded, so the token-level RENAME
//! rewriters in `vibesql-executor` can skip it (see
//! [`Parser::empty_in_discarded_operand_spans`]).

use vibesql_ast::{has_function::has_function_call, Expression};

use super::*;

impl Parser {
    /// Build the node for `<left> [NOT] IN (<values>)`, folding an empty list
    /// the way SQLite does.
    ///
    /// `operand_start` is the token index where `left` begins and `operand_end`
    /// the (exclusive) token index where it ends, i.e. the position of the
    /// `IN` keyword (or the `NOT` of `NOT IN`). They are only used to record
    /// the discarded operand's source range.
    pub(super) fn build_in_list_expression(
        &mut self,
        left: Expression,
        values: Vec<Expression>,
        negated: bool,
        operand_start: usize,
        operand_end: usize,
    ) -> Expression {
        if values.is_empty() && self.fold_empty_in_lists && !has_function_call(&left) {
            self.record_discarded_empty_in_operand(operand_start, operand_end);
            // `x IN ()` is always false and `x NOT IN ()` always true, even
            // when `x` is NULL.
            return Expression::Literal(vibesql_types::SqlValue::Boolean(negated));
        }
        Expression::InList { expr: Box::new(left), values, negated }
    }

    /// Record the byte range covering tokens `[start, end)` as a discarded
    /// empty-`IN ()` operand. A no-op when span info is unavailable.
    fn record_discarded_empty_in_operand(&mut self, start: usize, end: usize) {
        if self.source.is_empty() || end <= start {
            return;
        }
        let (Some(first), Some(last)) = (self.spans.get(start), self.spans.get(end - 1)) else {
            return;
        };
        let span = Span::new(first.start, last.end);
        if !self.discarded_empty_in_operands.contains(&span) {
            self.discarded_empty_in_operands.push(span);
        }
    }

    /// Byte ranges of the operands discarded by the empty-`IN ()` fold so far.
    pub(crate) fn discarded_empty_in_operands(&self) -> &[Span] {
        &self.discarded_empty_in_operands
    }

    /// Run `f` with the empty-`IN ()` fold disabled, restoring the previous
    /// setting afterwards (even when `f` fails).
    pub(crate) fn with_empty_in_fold_disabled<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        let prev = std::mem::replace(&mut self.fold_empty_in_lists, false);
        let result = f(self);
        self.fold_empty_in_lists = prev;
        result
    }
}
