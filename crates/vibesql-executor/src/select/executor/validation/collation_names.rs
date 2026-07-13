//! COLLATE-name validation (issue #6089).
//!
//! SQLite raises `no such collation sequence: <name>` at prepare time when a
//! query names a collating sequence that is not registered, and it does so even
//! when the target table is empty. VibeSQL only implements the three built-in
//! collations (`BINARY`, `NOCASE`, `RTRIM`); any other name is invalid.
//!
//! Prior to this validation an unknown COLLATE name was silently ignored — the
//! collation transform fell through to a no-op — so `SELECT a COLLATE nose FROM t`
//! returned a row instead of erroring (rowvalue4 §7.3/§7.4, rowvalue §23.100
//! adjacent COLLATE cases).
//!
//! This walks the SELECT list and WHERE clause expressions and raises on the
//! first unknown COLLATE name it finds. It intentionally recurses only through
//! the *directly evaluated* expression subtree; nested subqueries carry their
//! own COLLATE nodes but are validated when that subquery is itself prepared, so
//! recursing into them here would either duplicate work or, worse, evaluate a
//! COLLATE against a schema it does not belong to.

use vibesql_ast::Expression;

use crate::errors::ExecutorError;

/// Return `true` if `name` is a collating sequence VibeSQL implements.
///
/// The three SQLite built-ins are the only registered collations; matching is
/// case-insensitive (`COLLATE nocase` and `COLLATE NOCASE` are identical).
pub fn is_known_collation(name: &str) -> bool {
    name.eq_ignore_ascii_case("binary")
        || name.eq_ignore_ascii_case("nocase")
        || name.eq_ignore_ascii_case("rtrim")
}

/// Validate every COLLATE name reachable within `expr`'s directly-evaluated
/// subtree, returning `no such collation sequence: <name>` on the first
/// unknown one.
pub fn validate_collation_names(expr: &Expression) -> Result<(), ExecutorError> {
    match expr {
        Expression::Collate { expr: inner, collation } => {
            if !is_known_collation(collation) {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "no such collation sequence: {collation}"
                )));
            }
            validate_collation_names(inner)
        }
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            validate_collation_names(left)?;
            validate_collation_names(right)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Extract { expr, .. }
        | Expression::QuantifiedComparison { expr, .. }
        | Expression::Interval { value: expr, .. } => validate_collation_names(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                validate_collation_names(arg)?;
            }
            Ok(())
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_collation_names(op)?;
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    validate_collation_names(cond)?;
                }
                validate_collation_names(&case_when.result)?;
            }
            if let Some(else_expr) = else_result {
                validate_collation_names(else_expr)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            validate_collation_names(expr)?;
            validate_collation_names(low)?;
            validate_collation_names(high)
        }
        Expression::InList { expr, values, .. } => {
            validate_collation_names(expr)?;
            for val in values {
                validate_collation_names(val)?;
            }
            Ok(())
        }
        // For IN / EXISTS / scalar subqueries, only the directly-evaluated
        // left-hand expression is validated here; the subquery body is
        // validated when that subquery is prepared.
        Expression::In { expr, .. } => validate_collation_names(expr),
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            validate_collation_names(expr)?;
            validate_collation_names(pattern)
        }
        Expression::Position { substring, string, .. } => {
            validate_collation_names(substring)?;
            validate_collation_names(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                validate_collation_names(char_expr)?;
            }
            validate_collation_names(string)
        }
        Expression::WindowFunction { function, over } => {
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    for arg in args {
                        validate_collation_names(arg)?;
                    }
                }
            }
            if let Some(partition) = &over.partition_by {
                for expr in partition {
                    validate_collation_names(expr)?;
                }
            }
            if let Some(order) = &over.order_by {
                for item in order {
                    validate_collation_names(&item.expr)?;
                }
            }
            Ok(())
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            validate_collation_names(search_modifier)
        }
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                validate_collation_names(child)?;
            }
            Ok(())
        }
        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                validate_collation_names(msg)?;
            }
            Ok(())
        }
        // Terminals and subquery-bearing nodes (validated on their own prepare):
        // no directly-evaluated COLLATE to check here.
        Expression::ColumnRef(_)
        | Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Wildcard
        | Expression::Exists { .. }
        | Expression::ScalarSubquery(_)
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. }
        | Expression::PseudoVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnIdentifier, Expression};

    use super::*;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::unquoted(name))
    }

    fn collate(inner: Expression, name: &str) -> Expression {
        Expression::Collate { expr: Box::new(inner), collation: name.to_string() }
    }

    #[test]
    fn known_collations_pass() {
        for name in ["binary", "BINARY", "nocase", "NOCASE", "rtrim", "RTRIM"] {
            assert!(is_known_collation(name), "{name} should be known");
            assert!(validate_collation_names(&collate(col("a"), name)).is_ok());
        }
    }

    #[test]
    fn unknown_collation_errors() {
        let expr = collate(col("a"), "nose");
        let err = validate_collation_names(&expr).unwrap_err();
        assert_eq!(
            err,
            ExecutorError::SqliteCompatError("no such collation sequence: nose".to_string())
        );
    }

    #[test]
    fn unknown_collation_nested_in_row_value_errors() {
        // (a COLLATE nose, b) — mirrors rowvalue4 §7.3.
        let expr = Expression::RowValueConstructor(vec![collate(col("a"), "nose"), col("b")]);
        let err = validate_collation_names(&expr).unwrap_err();
        assert_eq!(
            err,
            ExecutorError::SqliteCompatError("no such collation sequence: nose".to_string())
        );
    }

    #[test]
    fn bare_column_without_collate_passes() {
        assert!(validate_collation_names(&col("a")).is_ok());
    }
}
