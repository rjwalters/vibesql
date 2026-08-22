//! Shared collation-resolution rules for expressions and comparisons.
//!
//! Implements SQLite's collating-sequence rules
//! (https://sqlite.org/datatype3.html#collation, section 7.1):
//!
//! For a binary comparison `X <op> Y` the collating sequence is chosen by:
//! 1. An explicit `COLLATE` operator anywhere inside either operand's subtree, with precedence to
//!    the left operand.
//! 2. Otherwise, if either operand *is a column* — optionally wrapped in unary `+` and/or `CAST` —
//!    that column's collation is used, with precedence to the left operand. Note that a column with
//!    no declared collation still counts: it contributes the default BINARY and blocks fallback to
//!    the right operand (`a = b` compares BINARY even when `b` is NOCASE, per in4-4.1).
//! 3. Otherwise, BINARY.
//!
//! Crucially, a column's *implicit* collation does **not** propagate through
//! `||` or any other operator or function: `(b || '')` has no collating
//! sequence even when `b` is declared `COLLATE NOCASE` (issue #5792,
//! `in4-4.4`). Only an explicit `COLLATE` operator carries through compound
//! expressions.
//!
//! This module is shared by `ExpressionEvaluator` (single-table) and
//! `CombinedExpressionEvaluator` (multi-table); the two differ only in how a
//! column reference is resolved to its declared collation, which is supplied
//! by the `column_collation` callback. The callback returns the column's
//! *declared* collation, or `None` when the column has none (default
//! BINARY).

use vibesql_ast::{ColumnIdentifier, Expression, SelectItem, UnaryOperator};

/// Find an *explicit* COLLATE operator anywhere within an expression's
/// subtree (SQLite's `EP_Collate` propagation).
///
/// An explicit `COLLATE` inside any operand of a binary operator beats the
/// column-derived (implicit) collation of the other operand. We walk through
/// compound expressions (BinaryOp, UnaryOp, CAST, single-arg
/// Function/Aggregate) looking specifically for a `Collate` node —
/// column-level collations are *not* considered explicit and are
/// deliberately ignored here.
pub(crate) fn explicit_collation_of(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Collate { collation, .. } => Some(collation.clone()),
        Expression::BinaryOp { left, right, .. } => {
            explicit_collation_of(left).or_else(|| explicit_collation_of(right))
        }
        Expression::UnaryOp { expr, .. } => explicit_collation_of(expr),
        Expression::Cast { expr, .. } => explicit_collation_of(expr),
        Expression::AggregateFunction { args, .. } if args.len() == 1 => {
            explicit_collation_of(&args[0])
        }
        Expression::Function { args, .. } if args.len() == 1 => explicit_collation_of(&args[0]),
        _ => None,
    }
}

/// If the expression *is a column operand* in the sense of datatype3 §7.1
/// rule 2 — a column reference optionally wrapped in unary `+`, `CAST`, or
/// an explicit `COLLATE` — return `Some(collation)` where the inner value is
/// the column's declared collation (`None` = default BINARY).
///
/// Returns `None` when the expression is not a column operand at all
/// (literal, `||`, function call, ...), in which case a comparison falls
/// through to the other operand.
pub(crate) fn operand_column_collation(
    expr: &Expression,
    column_collation: &dyn Fn(&ColumnIdentifier) -> Option<String>,
) -> Option<Option<String>> {
    match expr {
        // An explicit COLLATE makes this a collating operand carrying that name.
        Expression::Collate { collation, .. } => Some(Some(collation.clone())),
        Expression::ColumnRef(col_id) => Some(column_collation(col_id)),
        // A substituted outer column (issue #6105) is a collating operand
        // carrying the column's *implicit* declared collation — it behaves like
        // a `ColumnRef`, participating in rule 2 (left-operand precedence),
        // NOT like an explicit COLLATE (rule 1). A BINARY collation is surfaced
        // as `Some(None)` so a left-operand's default BINARY still blocks the
        // right operand, mirroring a real column.
        Expression::CollatedLiteral { collation, .. } => {
            Some((!collation.eq_ignore_ascii_case("binary")).then(|| collation.clone()))
        }
        Expression::UnaryOp { op: UnaryOperator::Plus, expr } => {
            operand_column_collation(expr, column_collation)
        }
        Expression::Cast { expr, .. } => operand_column_collation(expr, column_collation),
        _ => None,
    }
}

/// Derive the per-column collation of a view (or subquery) body's select list,
/// positionally aligned with the result columns.
///
/// SQLite propagates a view column's collation to the outer query in two ways
/// (ticket a7debbe0ad1, issue #5864):
///  1. An explicit `COLLATE` operator anywhere in the defining expression's subtree — including
///     through `||`, `CAST`, and unary `+` — wins (`explicit_collation_of`).
///  2. Otherwise a bare column reference (optionally wrapped in unary `+` or `CAST`) to an
///     underlying column with a *declared* collation propagates that collation, resolved via the
///     `column_collation` callback.
///
/// Any other expression (bare literal, function result, a `||` of operands
/// without an explicit `COLLATE`, ...) has no collating sequence — `None`,
/// i.e. default BINARY. Non-expression select items (wildcards) yield a `None`
/// placeholder; because a wildcard expands to a variable number of columns,
/// callers must guard by comparing the returned length against the actual
/// column count and fall back to all-`None` when they differ.
pub(crate) fn view_select_list_collations(
    select_list: &[SelectItem],
    column_collation: &dyn Fn(&ColumnIdentifier) -> Option<String>,
) -> Vec<Option<String>> {
    select_list
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, .. } => explicit_collation_of(expr)
                .or_else(|| operand_column_collation(expr, column_collation).flatten()),
            _ => None,
        })
        .collect()
}

/// Resolve the effective collating sequence of a single expression.
///
/// Mirrors SQLite's `sqlite3ExprCollSeq`:
/// - An explicit `COLLATE` operator assigns its collation.
/// - A column reference carries the column's declared (implicit) collation.
/// - Unary `+` and `CAST` are transparent: a column wrapped in them still counts as that column.
/// - Any other operator or function does **not** carry its operands' implicit collation; only an
///   explicit `COLLATE` somewhere in the subtree propagates out.
/// - Everything else has no collating sequence (callers default to BINARY).
///
/// `column_collation` resolves a column reference to its declared collation
/// (schema lookup differs between the single-table and combined evaluators).
pub(crate) fn resolve_expression_collation(
    expr: &Expression,
    column_collation: &dyn Fn(&ColumnIdentifier) -> Option<String>,
) -> Option<String> {
    match expr {
        // Explicit COLLATE has highest priority.
        Expression::Collate { collation, .. } => Some(collation.clone()),
        // Column reference - the column's declared collation (implicit).
        Expression::ColumnRef(col_id) => column_collation(col_id),
        // A substituted outer column (issue #6105) carries the column's
        // implicit declared collation (BINARY reported as `None`).
        Expression::CollatedLiteral { collation, .. } => {
            (!collation.eq_ignore_ascii_case("binary")).then(|| collation.clone())
        }
        // Unary `+` and CAST are transparent: `+a` and `CAST(a AS TEXT)`
        // are still "a column" for collation purposes (datatype3 §7.1 rule 2).
        Expression::UnaryOp { op: UnaryOperator::Plus, expr } => {
            resolve_expression_collation(expr, column_collation)
        }
        Expression::Cast { expr, .. } => resolve_expression_collation(expr, column_collation),
        // Any other operator (including `||`), unary op, or function: the
        // operands' implicit column collation does NOT propagate through.
        // Only an explicit COLLATE anywhere in the subtree carries out.
        Expression::BinaryOp { left, right, .. } => {
            explicit_collation_of(left).or_else(|| explicit_collation_of(right))
        }
        Expression::UnaryOp { expr, .. } => explicit_collation_of(expr),
        Expression::AggregateFunction { args, .. } if args.len() == 1 => {
            explicit_collation_of(&args[0])
        }
        Expression::Function { args, .. } if args.len() == 1 => explicit_collation_of(&args[0]),
        // Other expressions have no intrinsic collation.
        _ => None,
    }
}

/// Resolve the collating sequence for a binary comparison `left <op> right`
/// per SQLite datatype3 §7.1:
///
/// 1. explicit COLLATE in left subtree, else explicit in right subtree;
/// 2. else, if the left operand is a column (through unary `+`/CAST), that column's collation —
///    *including* the default BINARY, which blocks the right operand's collation;
/// 3. else the same for the right operand;
/// 4. else `None` (BINARY).
pub(crate) fn comparison_collation(
    left: &Expression,
    right: &Expression,
    column_collation: &dyn Fn(&ColumnIdentifier) -> Option<String>,
) -> Option<String> {
    if let Some(c) = explicit_collation_of(left).or_else(|| explicit_collation_of(right)) {
        return Some(c);
    }
    if let Some(coll) = operand_column_collation(left, column_collation) {
        return coll;
    }
    if let Some(coll) = operand_column_collation(right, column_collation) {
        return coll;
    }
    None
}

/// Resolve the datatype3 §7.1 comparison collation for a single element pair from
/// the operands' collating contributions (as returned by
/// [`operand_column_collation`]): `Some(Some(coll))` = column operand with
/// declared collation, `Some(None)` = column operand with default BINARY (which
/// still blocks the other side), `None` = not a column operand.
///
/// Precedence: the left operand wins if it is a column operand (rule 2 — its
/// default BINARY blocks fallback to the right); otherwise the right operand's
/// collation; otherwise `None` (BINARY). An explicit COLLATE is surfaced as
/// `Some(Some(coll))` by `operand_column_collation`, so rule-1 precedence falls
/// out of the left-first ordering for the common single-explicit case.
///
/// This is used where one side of the comparison is an already-evaluated value
/// (a scalar-subquery row), so its collating contribution is supplied directly
/// rather than derived from a live `Expression`.
pub(crate) fn comparison_collation_of_operands(
    left_operand: Option<Option<String>>,
    right_operand: Option<Option<String>>,
) -> Option<String> {
    match left_operand {
        Some(coll) => coll,
        None => right_operand.flatten(),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression, UnaryOperator};
    use vibesql_types::SqlValue;

    use super::*;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::unquoted(name))
    }

    fn lit(s: &str) -> Expression {
        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(s)))
    }

    fn concat(left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Concat,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Lookup where column `b` is declared COLLATE NOCASE and all other
    /// columns have no declared collation (default BINARY).
    fn lookup(col_id: &ColumnIdentifier) -> Option<String> {
        if col_id.column_canonical() == "b" {
            Some("NOCASE".to_string())
        } else {
            None
        }
    }

    #[test]
    fn column_carries_implicit_collation() {
        assert_eq!(resolve_expression_collation(&col("b"), &lookup), Some("NOCASE".to_string()));
        assert_eq!(resolve_expression_collation(&col("a"), &lookup), None);
    }

    #[test]
    fn implicit_collation_does_not_propagate_through_concat() {
        // (b || '') has NO collating sequence (issue #5792, in4-4.4)
        let expr = concat(col("b"), lit(""));
        assert_eq!(resolve_expression_collation(&expr, &lookup), None);
    }

    #[test]
    fn unary_plus_and_cast_are_transparent() {
        let plus = Expression::UnaryOp { op: UnaryOperator::Plus, expr: Box::new(col("b")) };
        assert_eq!(resolve_expression_collation(&plus, &lookup), Some("NOCASE".to_string()));

        let cast = Expression::Cast {
            expr: Box::new(col("b")),
            data_type: vibesql_types::DataType::Varchar { max_length: None },
        };
        assert_eq!(resolve_expression_collation(&cast, &lookup), Some("NOCASE".to_string()));

        // But unary minus is not transparent for implicit collation.
        let minus = Expression::UnaryOp { op: UnaryOperator::Minus, expr: Box::new(col("b")) };
        assert_eq!(resolve_expression_collation(&minus, &lookup), None);
    }

    #[test]
    fn explicit_collate_propagates_through_concat() {
        // (a COLLATE RTRIM || '') keeps the explicit collation
        let collated =
            Expression::Collate { expr: Box::new(col("a")), collation: "RTRIM".to_string() };
        let expr = concat(collated, lit(""));
        assert_eq!(resolve_expression_collation(&expr, &lookup), Some("RTRIM".to_string()));
    }

    #[test]
    fn comparison_left_column_binary_blocks_right_nocase() {
        // a = b -> a is a column with default BINARY collation; left
        // precedence means BINARY, not b's NOCASE (in4-4.1 expects only
        // the exact-match row).
        assert_eq!(comparison_collation(&col("a"), &col("b"), &lookup), None);
    }

    #[test]
    fn comparison_left_nocase_wins() {
        // b = a -> NOCASE (left column's collation wins) (in4-4.2)
        assert_eq!(comparison_collation(&col("b"), &col("a"), &lookup), Some("NOCASE".to_string()));
    }

    #[test]
    fn comparison_concat_wrapped_operands_are_binary() {
        // (a||'') = (b||'') -> no collation on either side -> BINARY (in4-4.4)
        let left = concat(col("a"), lit(""));
        let right = concat(col("b"), lit(""));
        assert_eq!(comparison_collation(&left, &right, &lookup), None);
    }

    #[test]
    fn comparison_concat_left_falls_back_to_right_column() {
        // (a||'') = b -> LHS is not a column, RHS column's NOCASE applies (in4-4.3)
        let left = concat(col("a"), lit(""));
        assert_eq!(comparison_collation(&left, &col("b"), &lookup), Some("NOCASE".to_string()));
    }

    #[test]
    fn comparison_literal_vs_nocase_column() {
        // 'XYZ' = b -> literal has no collation, b's NOCASE applies.
        assert_eq!(
            comparison_collation(&lit("XYZ"), &col("b"), &lookup),
            Some("NOCASE".to_string())
        );
        // b = 'XYZ' likewise.
        assert_eq!(
            comparison_collation(&col("b"), &lit("XYZ"), &lookup),
            Some("NOCASE".to_string())
        );
    }

    #[test]
    fn explicit_right_beats_implicit_left() {
        // b = (a COLLATE RTRIM) -> explicit RTRIM on the right wins over
        // implicit NOCASE on the left (datatype3 §7.1 rule 1).
        let collated =
            Expression::Collate { expr: Box::new(col("a")), collation: "RTRIM".to_string() };
        assert_eq!(comparison_collation(&col("b"), &collated, &lookup), Some("RTRIM".to_string()));
    }

    #[test]
    fn explicit_left_beats_explicit_right() {
        let l = Expression::Collate { expr: Box::new(col("a")), collation: "RTRIM".to_string() };
        let r = Expression::Collate { expr: Box::new(col("a")), collation: "NOCASE".to_string() };
        assert_eq!(comparison_collation(&l, &r, &lookup), Some("RTRIM".to_string()));
    }
}
