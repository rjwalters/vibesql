//! Utility methods for SelectExecutor

use super::builder::SelectExecutor;

/// Check if an expression references a column (which requires FROM clause)
fn expression_references_column(expr: &vibesql_ast::Expression) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef { .. } => true,
        vibesql_ast::Expression::PseudoVariable { .. } => true, // Pseudo-variables reference columns (OLD.x, NEW.x)
        vibesql_ast::Expression::Default => false,              // DEFAULT doesn't reference columns
        vibesql_ast::Expression::DuplicateKeyValue { .. } => false, // DuplicateKeyValue doesn't reference columns

        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expression_references_column(left) || expression_references_column(right)
        }

        vibesql_ast::Expression::UnaryOp { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::AggregateFunction { args, .. } => {
            args.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::IsNull { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::InList { expr, values, .. } => {
            expression_references_column(expr) || values.iter().any(expression_references_column)
        }

        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expression_references_column(expr)
                || expression_references_column(low)
                || expression_references_column(high)
        }

        vibesql_ast::Expression::Cast { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Interval { value, .. } => expression_references_column(value),

        vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
            expression_references_column(substring) || expression_references_column(string)
        }

        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_references_column(e))
                || expression_references_column(string)
        }

        vibesql_ast::Expression::Extract { expr, .. } => expression_references_column(expr),

        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            expression_references_column(expr) || expression_references_column(pattern)
        }

        vibesql_ast::Expression::In { expr, .. } => {
            // Note: subquery could reference outer columns but that's a different case
            expression_references_column(expr)
        }

        vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
            expression_references_column(expr)
        }

        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_references_column(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(expression_references_column)
                        || expression_references_column(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_references_column(e))
        }

        vibesql_ast::Expression::WindowFunction { function, over } => {
            // Check window function arguments
            let args_reference_column = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    args.iter().any(expression_references_column)
                }
            };

            // Check PARTITION BY and ORDER BY clauses
            let partition_references = over
                .partition_by
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(expression_references_column));

            let order_references = over.order_by.as_ref().is_some_and(|items| {
                items.iter().any(|item| expression_references_column(&item.expr))
            });

            args_reference_column || partition_references || order_references
        }

        // These don't contain column references:
        vibesql_ast::Expression::Literal(_) => false,
        vibesql_ast::Expression::Wildcard => false,
        vibesql_ast::Expression::ScalarSubquery(_) => false, // Subquery has its own scope
        vibesql_ast::Expression::Exists { .. } => false,     // Subquery has its own scope
        vibesql_ast::Expression::CurrentDate => false,
        vibesql_ast::Expression::CurrentTime { .. } => false,
        vibesql_ast::Expression::CurrentTimestamp { .. } => false,
        vibesql_ast::Expression::NextValue { .. } => false, // Sequence reference, not column
        vibesql_ast::Expression::SessionVariable { .. } => false, // Session variable, not column
        vibesql_ast::Expression::MatchAgainst { columns, search_modifier, .. } => {
            // MATCH AGAINST references columns and the search term
            !columns.is_empty() || expression_references_column(search_modifier)
        }

        // Placeholders don't reference columns (they're parameter markers)
        vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_) => false,

        // Conjunction and Disjunction - check all children
        vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => {
            children.iter().any(expression_references_column)
        }
    }
}

impl SelectExecutor<'_> {
    /// Check if an expression references a column (which requires FROM clause)
    pub(super) fn expression_references_column(&self, expr: &vibesql_ast::Expression) -> bool {
        expression_references_column(expr)
    }
}
