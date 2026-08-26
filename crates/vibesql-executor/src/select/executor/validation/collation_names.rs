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
//! This walks an expression subtree and raises on the first unknown COLLATE
//! name it finds. It is invoked for every clause of a top-level `SELECT` that
//! can carry a COLLATE node: the SELECT list and WHERE clause (from
//! `validate_select_columns_with_context`), and — added in #6110 — the
//! ORDER BY, GROUP BY, HAVING, and JOIN ... ON clauses (from
//! `SelectExecutor::execute`). Before #6110 the latter four were silently
//! accepted, so `SELECT a FROM t ORDER BY a COLLATE nose` returned rows instead
//! of erroring; SQLite reports `no such collation sequence: nose` at prepare
//! time in every one of these positions.
//!
//! It recurses through the directly evaluated expression subtree and, for
//! EXISTS / IN / scalar-subquery / quantified-comparison expressions, also
//! walks the referenced subquery's own SELECT list, WHERE, GROUP BY, HAVING,
//! ORDER BY, and JOIN ... ON clauses (`validate_collation_names_in_subquery`
//! below) rather than deferring to that subquery's own `execute()`. A
//! subquery is normally validated when it is itself prepared, but a
//! correlated or uncorrelated EXISTS/IN/scalar subquery is only actually
//! *executed* (and thus only actually prepared) while evaluating the outer
//! per-row predicate -- which never happens if the outer FROM table has zero
//! rows. SQLite still raises `no such collation sequence: <name>` at prepare
//! time in that case (existsexpr-6.1: `SELECT a FROM t1 WHERE EXISTS (SELECT
//! 1 FROM t2 WHERE c COLLATE f = a)` over an empty `t1`), so this validation
//! must not depend on the subquery ever actually running. This walk is purely
//! syntactic (no schema needed), so it is safe to run eagerly regardless of
//! row counts.

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

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
        // IN / EXISTS / scalar / quantified-comparison subqueries: validate
        // both the directly-evaluated left-hand expression (if any) and the
        // subquery body itself -- see the module doc comment for why the
        // subquery body cannot simply wait for its own `execute()`.
        Expression::In { expr, subquery, .. } => {
            validate_collation_names(expr)?;
            validate_collation_names_in_subquery(subquery)
        }
        Expression::Exists { subquery, .. } => validate_collation_names_in_subquery(subquery),
        Expression::ScalarSubquery(subquery) => validate_collation_names_in_subquery(subquery),
        Expression::QuantifiedComparison { expr, subquery, .. } => {
            validate_collation_names(expr)?;
            validate_collation_names_in_subquery(subquery)
        }
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
        // Terminals: no directly-evaluated COLLATE to check here.
        Expression::ColumnRef(_)
        | Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Wildcard
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

/// Recursively walk `from`'s JOIN ... ON conditions, validating COLLATE names
/// in each. Mirrors `SelectExecutor::for_each_join_condition`'s traversal
/// shape but is duplicated here (rather than shared) because this module is
/// schema-free and must stay callable before any table/subquery is prepared.
fn validate_collation_names_in_from(from: &FromClause) -> Result<(), ExecutorError> {
    if let FromClause::Join { left, right, condition, .. } = from {
        validate_collation_names_in_from(left)?;
        validate_collation_names_in_from(right)?;
        if let Some(cond) = condition {
            validate_collation_names(cond)?;
        }
    }
    Ok(())
}

/// Validate every COLLATE name in `stmt`'s own directly-evaluated clauses:
/// SELECT list, WHERE, GROUP BY, HAVING, ORDER BY, and JOIN ... ON. Used to
/// eagerly validate a subquery reached through EXISTS / IN / scalar-subquery
/// / quantified-comparison, since (per the module doc comment) that subquery
/// is not guaranteed to ever actually execute during outer-query evaluation.
fn validate_collation_names_in_subquery(stmt: &SelectStmt) -> Result<(), ExecutorError> {
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            validate_collation_names(expr)?;
        }
    }
    if let Some(where_expr) = &stmt.where_clause {
        validate_collation_names(where_expr)?;
    }
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            validate_collation_names(expr)?;
        }
    }
    if let Some(having_expr) = &stmt.having {
        validate_collation_names(having_expr)?;
    }
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            validate_collation_names(&item.expr)?;
        }
    }
    if let Some(from) = &stmt.from {
        validate_collation_names_in_from(from)?;
    }
    // Compound queries (UNION/INTERSECT/EXCEPT) chain further SelectStmts off
    // `set_operation.right`; walk the whole chain so a COLLATE name in a
    // later branch is validated too.
    if let Some(set_op) = &stmt.set_operation {
        validate_collation_names_in_subquery(&set_op.right)?;
    }
    Ok(())
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

    /// A bare `SelectStmt` with just a WHERE clause, otherwise minimal — for
    /// building EXISTS/IN/scalar-subquery bodies in the tests below.
    fn subquery_with_where(where_clause: Expression) -> SelectStmt {
        SelectStmt {
            hints: Vec::new(),
            into_table: None,
            into_variables: None,
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: None,
                source_text: None,
            }],
            from: Some(FromClause::Table {
                index_hint: None,
                name: "t2".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(where_clause),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    /// Regression test for existsexpr-6.1 (#6172): `EXISTS (SELECT ... WHERE
    /// c COLLATE f = a)` must raise `no such collation sequence: f` at
    /// prepare time even when the outer table driving the EXISTS predicate
    /// has zero rows (so the subquery is never actually executed/prepared on
    /// its own to trigger the check).
    #[test]
    fn unknown_collation_inside_exists_subquery_errors() {
        let where_clause = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(collate(col("c"), "f")),
            right: Box::new(col("a")),
        };
        let expr = Expression::Exists {
            subquery: Box::new(subquery_with_where(where_clause)),
            negated: false,
        };
        let err = validate_collation_names(&expr).unwrap_err();
        assert_eq!(
            err,
            ExecutorError::SqliteCompatError("no such collation sequence: f".to_string())
        );
    }

    #[test]
    fn known_collation_inside_exists_subquery_passes() {
        let where_clause = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(collate(col("c"), "nocase")),
            right: Box::new(col("a")),
        };
        let expr = Expression::Exists {
            subquery: Box::new(subquery_with_where(where_clause)),
            negated: false,
        };
        assert!(validate_collation_names(&expr).is_ok());
    }

    #[test]
    fn unknown_collation_inside_in_subquery_errors() {
        let where_clause = collate(col("c"), "nose");
        let expr = Expression::In {
            expr: Box::new(col("a")),
            subquery: Box::new(subquery_with_where(where_clause)),
            negated: false,
        };
        let err = validate_collation_names(&expr).unwrap_err();
        assert_eq!(
            err,
            ExecutorError::SqliteCompatError("no such collation sequence: nose".to_string())
        );
    }

    #[test]
    fn unknown_collation_inside_scalar_subquery_errors() {
        let where_clause = collate(col("c"), "nose");
        let expr = Expression::ScalarSubquery(Box::new(subquery_with_where(where_clause)));
        let err = validate_collation_names(&expr).unwrap_err();
        assert_eq!(
            err,
            ExecutorError::SqliteCompatError("no such collation sequence: nose".to_string())
        );
    }
}
