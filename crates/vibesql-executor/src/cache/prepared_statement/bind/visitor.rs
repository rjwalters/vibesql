//! Visitor pattern for traversing AST and counting placeholders
//!
//! This module re-exports the visitor functionality from `vibesql_ast::visitor`
//! and provides a convenient function for visiting all expressions in a statement.

use vibesql_ast::{
    visitor::{ExpressionVisitor, StatementVisitor, VisitResult},
    Expression, Statement,
};

/// Visit all expressions in a statement (for counting placeholders)
///
/// This is a convenience wrapper around `vibesql_ast::visitor::walk_statement`
/// that maintains backward compatibility with the existing API.
pub fn visit_statement<F>(stmt: &Statement, visitor: &mut F)
where
    F: FnMut(&Expression),
{
    // Wrap the closure in a struct that implements ExpressionVisitor
    struct ClosureVisitor<'a, F> {
        closure: &'a mut F,
    }

    impl<F: FnMut(&Expression)> ExpressionVisitor for ClosureVisitor<'_, F> {
        fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
            (self.closure)(expr);
            VisitResult::Continue
        }
    }

    impl<F: FnMut(&Expression)> StatementVisitor for ClosureVisitor<'_, F> {}

    let mut v = ClosureVisitor { closure: visitor };
    vibesql_ast::visitor::walk_statement(&mut v, stmt);
}
