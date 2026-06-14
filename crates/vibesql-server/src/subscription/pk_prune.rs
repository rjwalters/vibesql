//! Conservative primary-key predicate pruning for replicated subscriptions (#5472).
//!
//! When a replicated write changes a row, the subscription manager normally
//! re-queries every subscription on that table to recompute its result set.
//! This is wasteful when the changed row's **primary key** provably cannot
//! satisfy a subscription's `WHERE` predicate — e.g. the subscription is
//! `... WHERE id = 5` and the changed PK is `id = 9`.
//!
//! This module analyzes a subscription's query and decides, for a candidate PK
//! value, whether that value *could* satisfy the filter. It is deliberately
//! **conservative**: it only ever returns "cannot match" (`false`) when it can
//! *prove* it; for anything it cannot analyze it returns "might match" (`true`)
//! and the caller re-queries. A false "cannot match" would drop a real change
//! (a correctness bug), so the analyzer is allowed exactly one direction of
//! imprecision — over-reporting possible matches.
//!
//! # What is analyzable
//!
//! A subscription is analyzable only if its query is a single-table `SELECT`
//! over the changed table, with no joins, subqueries, `VALUES`, CTEs, set
//! operations, `GROUP BY`, or `HAVING`, and whose `WHERE` predicate references
//! **only the primary-key column** using supported comparison/range operators
//! against constant literals. Anything else (a non-PK column reference, a
//! function call, `LIKE`, a subquery, a placeholder, etc.) makes the predicate
//! `Unanalyzable`, and the caller re-queries unconditionally.
//!
//! # Correctness for INSERT / UPDATE / DELETE
//!
//! The caller (see [`crate::subscription::manager`]) consults
//! [`PkPruner::pk_might_match`] per changed row:
//! - **Insert**: re-query unless the new PK cannot match.
//! - **Delete**: re-query unless the old PK cannot match (a removed in-set row
//!   is a real change the subscriber must observe).
//! - **Update**: re-query unless **both** the old and new PK cannot match — a
//!   row moving *into* or *out of* the set is a real change.

use vibesql_ast::{BinaryOperator, Expression, FromClause, SelectStmt, Statement};
use vibesql_types::SqlValue;

/// A parsed, conservative pruning analysis of one subscription's PK filter.
///
/// Construct via [`PkPruner::analyze`]. The result is cached per subscription
/// so the query is parsed once.
#[derive(Debug, Clone)]
pub enum PkPruner {
    /// The query could not be reduced to a pure single-PK-column predicate.
    /// Every change must be treated as possibly relevant (re-query).
    Unanalyzable,
    /// The `WHERE` predicate references only the primary-key column and is built
    /// from supported comparisons/ranges. `predicate` can be evaluated against a
    /// candidate PK value; `pk_column` is the canonical (lower-cased) PK column
    /// name the predicate is written in terms of. The predicate is boxed to keep
    /// the enum small (the common `Unanalyzable` variant is zero-sized).
    PkOnly { pk_column: String, predicate: Box<Expression> },
}

impl PkPruner {
    /// Analyze a subscription `query` against the table's single-column primary
    /// key `pk_column` (canonical / lower-cased).
    ///
    /// Returns [`PkPruner::Unanalyzable`] (the safe default) whenever the query
    /// cannot be proven to filter purely on the PK column. Never errors: any
    /// parse failure or unsupported shape degrades to `Unanalyzable`.
    pub fn analyze(query: &str, pk_column: &str) -> Self {
        let pk_column = pk_column.to_lowercase();

        let select = match vibesql_parser::Parser::parse_sql(query) {
            Ok(Statement::Select(select)) => select,
            _ => return PkPruner::Unanalyzable,
        };

        if !Self::is_simple_single_table(&select) {
            return PkPruner::Unanalyzable;
        }

        let where_clause = match &select.where_clause {
            // No WHERE at all: every row matches, so no change can ever be
            // pruned. Treat as unanalyzable (always re-query).
            None => return PkPruner::Unanalyzable,
            Some(expr) => expr,
        };

        if !Self::references_only_pk(where_clause, &pk_column) {
            return PkPruner::Unanalyzable;
        }

        PkPruner::PkOnly { pk_column, predicate: Box::new(where_clause.clone()) }
    }

    /// Whether a change to a row with primary key `value` could affect this
    /// subscription's result set.
    ///
    /// Returns `true` (caller must re-query) unless it can *prove* the value
    /// cannot satisfy the filter, in which case it returns `false` (safe to
    /// skip). For [`PkPruner::Unanalyzable`] always returns `true`.
    pub fn pk_might_match(&self, value: &SqlValue) -> bool {
        match self {
            PkPruner::Unanalyzable => true,
            PkPruner::PkOnly { predicate, .. } => {
                // Evaluate the predicate with the PK column bound to `value`.
                // A definite FALSE means the value cannot be in the set; any
                // other outcome (TRUE, NULL/unknown, or evaluation error) is
                // treated conservatively as "might match".
                !matches!(Self::eval(predicate, value), Some(false))
            }
        }
    }

    /// Reject anything that isn't a plain `SELECT ... FROM <one table> WHERE ...`.
    fn is_simple_single_table(select: &SelectStmt) -> bool {
        if select.with_clause.is_some()
            || select.set_operation.is_some()
            || select.group_by.is_some()
            || select.having.is_some()
            || select.values.is_some()
        {
            return false;
        }
        matches!(&select.from, Some(FromClause::Table { .. }))
    }

    /// Verify every column reference in `expr` resolves to `pk_column` and that
    /// every node is a shape this analyzer can soundly evaluate against a single
    /// bound PK value. Anything else → not PK-only (caller re-queries).
    fn references_only_pk(expr: &Expression, pk_column: &str) -> bool {
        match expr {
            Expression::Literal(_) => true,

            Expression::ColumnRef(col) => col.column_canonical().to_lowercase() == pk_column,

            Expression::BinaryOp { op, left, right } => {
                Self::is_supported_binop(op)
                    && Self::references_only_pk(left, pk_column)
                    && Self::references_only_pk(right, pk_column)
            }

            Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
                exprs.iter().all(|e| Self::references_only_pk(e, pk_column))
            }

            Expression::UnaryOp { op, expr } => {
                matches!(
                    op,
                    vibesql_ast::UnaryOperator::Not
                        | vibesql_ast::UnaryOperator::Minus
                        | vibesql_ast::UnaryOperator::Plus
                ) && Self::references_only_pk(expr, pk_column)
            }

            Expression::Between { expr, low, high, .. } => {
                Self::references_only_pk(expr, pk_column)
                    && Self::references_only_pk(low, pk_column)
                    && Self::references_only_pk(high, pk_column)
            }

            Expression::InList { expr, values, .. } => {
                Self::references_only_pk(expr, pk_column)
                    && values.iter().all(|e| Self::references_only_pk(e, pk_column))
            }

            // Anything else (functions, LIKE, IS NULL, subqueries, placeholders,
            // CASE, aggregates, ...) is not soundly evaluable here.
            _ => false,
        }
    }

    fn is_supported_binop(op: &BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::And
                | BinaryOperator::Or
        )
    }

    /// Evaluate a PK-only predicate with the single PK column bound to `pk`.
    ///
    /// Returns:
    /// - `Some(true)`  — the value definitely satisfies the predicate,
    /// - `Some(false)` — the value definitely does NOT satisfy the predicate,
    /// - `None`        — unknown (NULL / not comparable / unsupported); the
    ///   caller treats this conservatively as "might match".
    fn eval(expr: &Expression, pk: &SqlValue) -> Option<bool> {
        match expr {
            Expression::Conjunction(exprs) => {
                // AND: FALSE if any operand is definitely FALSE; TRUE only if
                // all are definitely TRUE; otherwise unknown.
                let mut all_true = true;
                for e in exprs {
                    match Self::eval(e, pk) {
                        Some(false) => return Some(false),
                        Some(true) => {}
                        None => all_true = false,
                    }
                }
                if all_true {
                    Some(true)
                } else {
                    None
                }
            }

            Expression::Disjunction(exprs) => {
                // OR: TRUE if any operand is definitely TRUE; FALSE only if all
                // are definitely FALSE; otherwise unknown.
                let mut all_false = true;
                for e in exprs {
                    match Self::eval(e, pk) {
                        Some(true) => return Some(true),
                        Some(false) => {}
                        None => all_false = false,
                    }
                }
                if all_false {
                    Some(false)
                } else {
                    None
                }
            }

            Expression::BinaryOp { op, left, right } => match op {
                BinaryOperator::And => {
                    Self::eval(&Expression::Conjunction(vec![(**left).clone(), (**right).clone()]), pk)
                }
                BinaryOperator::Or => {
                    Self::eval(&Expression::Disjunction(vec![(**left).clone(), (**right).clone()]), pk)
                }
                _ => {
                    let l = Self::resolve_value(left, pk)?;
                    let r = Self::resolve_value(right, pk)?;
                    Self::compare_op(op, &l, &r)
                }
            },

            Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Not, expr } => {
                Self::eval(expr, pk).map(|b| !b)
            }

            Expression::Between { expr, low, high, negated, .. } => {
                let v = Self::resolve_value(expr, pk)?;
                let lo = Self::resolve_value(low, pk)?;
                let hi = Self::resolve_value(high, pk)?;
                let ge_lo = Self::cmp(&v, &lo)? != std::cmp::Ordering::Less;
                let le_hi = Self::cmp(&v, &hi)? != std::cmp::Ordering::Greater;
                let in_range = ge_lo && le_hi;
                Some(if *negated { !in_range } else { in_range })
            }

            Expression::InList { expr, values, negated } => {
                let v = Self::resolve_value(expr, pk)?;
                let mut any_match = false;
                let mut any_unknown = false;
                for item in values {
                    match Self::resolve_value(item, pk) {
                        Some(iv) => match Self::cmp(&v, &iv) {
                            Some(std::cmp::Ordering::Equal) => any_match = true,
                            Some(_) => {}
                            None => any_unknown = true,
                        },
                        None => any_unknown = true,
                    }
                }
                if any_match {
                    Some(!negated)
                } else if any_unknown {
                    // Could still match an unknown element — don't claim FALSE.
                    None
                } else {
                    Some(*negated)
                }
            }

            _ => None,
        }
    }

    /// Resolve a scalar sub-expression to a concrete value, binding the PK
    /// column reference to `pk`. Returns `None` for anything non-constant /
    /// non-PK / NULL / unsupported (forcing the conservative path).
    fn resolve_value(expr: &Expression, pk: &SqlValue) -> Option<SqlValue> {
        match expr {
            Expression::Literal(SqlValue::Null) => None,
            Expression::Literal(v) => Some(v.clone()),
            Expression::ColumnRef(_) => {
                // `references_only_pk` already guaranteed this is the PK column.
                if matches!(pk, SqlValue::Null) {
                    None
                } else {
                    Some(pk.clone())
                }
            }
            Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Minus, expr } => {
                match Self::resolve_value(expr, pk)? {
                    SqlValue::Integer(i) => Some(SqlValue::Integer(i.checked_neg()?)),
                    SqlValue::Bigint(i) => Some(SqlValue::Bigint(i.checked_neg()?)),
                    SqlValue::Smallint(i) => Some(SqlValue::Smallint(i.checked_neg()?)),
                    SqlValue::Float(f) => Some(SqlValue::Float(-f)),
                    SqlValue::Double(f) => Some(SqlValue::Double(-f)),
                    SqlValue::Numeric(f) => Some(SqlValue::Numeric(-f)),
                    _ => None,
                }
            }
            Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Plus, expr } => {
                Self::resolve_value(expr, pk)
            }
            _ => None,
        }
    }

    fn compare_op(op: &BinaryOperator, l: &SqlValue, r: &SqlValue) -> Option<bool> {
        let ord = Self::cmp(l, r)?;
        use std::cmp::Ordering::*;
        Some(match op {
            BinaryOperator::Equal => ord == Equal,
            BinaryOperator::NotEqual => ord != Equal,
            BinaryOperator::LessThan => ord == Less,
            BinaryOperator::LessThanOrEqual => ord != Greater,
            BinaryOperator::GreaterThan => ord == Greater,
            BinaryOperator::GreaterThanOrEqual => ord != Less,
            _ => return None,
        })
    }

    /// Total-ish comparison for the value kinds a PK can take. Returns `None`
    /// for incomparable / NULL operands so the caller stays conservative.
    fn cmp(l: &SqlValue, r: &SqlValue) -> Option<std::cmp::Ordering> {
        use SqlValue::*;
        match (l, r) {
            (Null, _) | (_, Null) => None,
            (Integer(a), Integer(b)) => Some(a.cmp(b)),
            (Bigint(a), Bigint(b)) => Some(a.cmp(b)),
            (Smallint(a), Smallint(b)) => Some(a.cmp(b)),
            // Cross-width integer comparisons (PK literals may parse as Integer
            // while the column value is Bigint, etc.). Integer and Bigint are
            // both i64; Smallint is i16 and is widened to i64.
            (Integer(a), Bigint(b)) => Some(a.cmp(b)),
            (Bigint(a), Integer(b)) => Some(a.cmp(b)),
            (Integer(a), Smallint(b)) => Some(a.cmp(&(*b as i64))),
            (Smallint(a), Integer(b)) => Some((*a as i64).cmp(b)),
            (Bigint(a), Smallint(b)) => Some(a.cmp(&(*b as i64))),
            (Smallint(a), Bigint(b)) => Some((*a as i64).cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),
            (Character(a), Character(b))
            | (Varchar(a), Varchar(b))
            | (Character(a), Varchar(b))
            | (Varchar(a), Character(b)) => Some(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Some(a.cmp(b)),
            // Mixed numeric/float or other cross-type combos: stay conservative.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(v: i64) -> SqlValue {
        SqlValue::Integer(v)
    }

    #[test]
    fn eq_filter_prunes_non_matching_pk() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id = 5", "id");
        assert!(matches!(p, PkPruner::PkOnly { .. }));
        assert!(p.pk_might_match(&pk(5)), "matching PK must re-query");
        assert!(!p.pk_might_match(&pk(9)), "non-matching PK must be prunable");
    }

    #[test]
    fn eq_filter_is_case_insensitive_on_column() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE ID = 5", "id");
        assert!(matches!(p, PkPruner::PkOnly { .. }));
        assert!(!p.pk_might_match(&pk(9)));
    }

    #[test]
    fn range_filter_prunes_outside() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id >= 10", "id");
        assert!(!p.pk_might_match(&pk(9)));
        assert!(p.pk_might_match(&pk(10)));
        assert!(p.pk_might_match(&pk(100)));
    }

    #[test]
    fn between_filter() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id BETWEEN 10 AND 20", "id");
        assert!(!p.pk_might_match(&pk(9)));
        assert!(p.pk_might_match(&pk(10)));
        assert!(p.pk_might_match(&pk(15)));
        assert!(p.pk_might_match(&pk(20)));
        assert!(!p.pk_might_match(&pk(21)));
    }

    #[test]
    fn in_list_filter() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id IN (1, 2, 3)", "id");
        assert!(p.pk_might_match(&pk(2)));
        assert!(!p.pk_might_match(&pk(4)));
    }

    #[test]
    fn and_range_window() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id >= 10 AND id <= 20", "id");
        assert!(!p.pk_might_match(&pk(9)));
        assert!(p.pk_might_match(&pk(15)));
        assert!(!p.pk_might_match(&pk(21)));
    }

    #[test]
    fn or_of_equalities() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id = 1 OR id = 100", "id");
        assert!(p.pk_might_match(&pk(1)));
        assert!(p.pk_might_match(&pk(100)));
        assert!(!p.pk_might_match(&pk(50)));
    }

    #[test]
    fn non_pk_column_is_unanalyzable() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE status = 'active'", "id");
        assert!(matches!(p, PkPruner::Unanalyzable));
        // Unanalyzable always re-queries.
        assert!(p.pk_might_match(&pk(9)));
    }

    #[test]
    fn mixed_pk_and_non_pk_is_unanalyzable() {
        // The non-PK conjunct means a row outside `id = 5` could still be
        // affected via the other column; we must not prune.
        let p = PkPruner::analyze("SELECT * FROM t WHERE id = 5 AND status = 'x'", "id");
        assert!(matches!(p, PkPruner::Unanalyzable));
        assert!(p.pk_might_match(&pk(9)));
    }

    #[test]
    fn function_on_pk_is_unanalyzable() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE ABS(id) = 5", "id");
        assert!(matches!(p, PkPruner::Unanalyzable));
        assert!(p.pk_might_match(&pk(9)));
    }

    #[test]
    fn no_where_is_unanalyzable() {
        let p = PkPruner::analyze("SELECT * FROM t", "id");
        assert!(matches!(p, PkPruner::Unanalyzable));
        assert!(p.pk_might_match(&pk(9)));
    }

    #[test]
    fn join_is_unanalyzable() {
        let p =
            PkPruner::analyze("SELECT * FROM t JOIN u ON t.id = u.id WHERE t.id = 5", "id");
        assert!(matches!(p, PkPruner::Unanalyzable));
    }

    #[test]
    fn subquery_predicate_is_unanalyzable() {
        let p = PkPruner::analyze(
            "SELECT * FROM t WHERE id IN (SELECT id FROM u)",
            "id",
        );
        assert!(matches!(p, PkPruner::Unanalyzable));
        assert!(p.pk_might_match(&pk(9)));
    }

    #[test]
    fn null_candidate_pk_never_pruned() {
        // A NULL PK can't really happen, but if it did the analyzer must stay
        // conservative rather than prune.
        let p = PkPruner::analyze("SELECT * FROM t WHERE id = 5", "id");
        assert!(p.pk_might_match(&SqlValue::Null));
    }

    #[test]
    fn not_equal_filter() {
        let p = PkPruner::analyze("SELECT * FROM t WHERE id <> 5", "id");
        // id != 5 matches everything except 5; only 5 is prunable.
        assert!(!p.pk_might_match(&pk(5)));
        assert!(p.pk_might_match(&pk(6)));
    }
}
