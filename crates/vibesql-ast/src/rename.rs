//! Column-rename rewriting for expression trees.
//!
//! `ALTER TABLE ... RENAME COLUMN old TO new` must rewrite every dependent
//! object that references the renamed column (SQLite rewrites indexes, views,
//! triggers, and foreign keys). Index metadata stores the column list and the
//! expression / partial-index WHERE predicates as ASTs, so the rename has to
//! walk those ASTs and rewrite matching [`Expression::ColumnRef`] nodes in
//! place. Without this, the stale metadata is persisted to the next checkpoint
//! and the database refuses to reopen ("Column 'old' not found in table ...",
//! issue #5877).
//!
//! The walker deliberately does **not** descend into subqueries
//! (`ScalarSubquery`, `EXISTS`, `IN (SELECT ...)`, quantified comparisons) or
//! window specifications: those forms are not allowed in index expressions or
//! partial-index predicates, which are the only ASTs this rewriter is applied
//! to. The immediate left-hand operand of `IN`/quantified comparisons is still
//! rewritten.

use crate::{expression::Expression, identifier::ColumnIdentifier};

/// Rewrite every reference to column `old` in `expr` to `new`, in place.
///
/// Matching is case-insensitive on the column part of the identifier (SQLite
/// folds identifiers regardless of quoting); any schema/table qualifiers are
/// preserved unchanged. Callers apply this only to expressions that belong to
/// the table whose column is being renamed (an index expression or
/// partial-index predicate can only reference its own table's columns), so no
/// table-qualifier check is needed.
///
/// Returns `true` if at least one reference was rewritten.
pub fn rename_column_in_expression(expr: &mut Expression, old: &str, new: &str) -> bool {
    let mut changed = false;
    rename_walk(expr, old, new, &mut changed);
    changed
}

/// Build a copy of `col_id` with the column part replaced by `new`, keeping
/// any schema/table qualifiers (and their quoting) intact.
fn renamed_column_identifier(col_id: &ColumnIdentifier, new: &str) -> ColumnIdentifier {
    // Preserve the original quoting flag: quoting does not affect canonical
    // case-folding (SQLite folds regardless), and display text stores the raw
    // name either way.
    let quoted = col_id.is_column_quoted();
    match (col_id.schema_display(), col_id.table_display()) {
        (Some(schema), Some(table)) => ColumnIdentifier::fully_qualified(
            schema,
            col_id.is_schema_quoted(),
            table,
            col_id.is_table_quoted(),
            new,
            quoted,
        ),
        (None, Some(table)) => {
            ColumnIdentifier::qualified(table, col_id.is_table_quoted(), new, quoted)
        }
        _ => ColumnIdentifier::simple(new, quoted),
    }
}

fn rename_walk(expr: &mut Expression, old: &str, new: &str, changed: &mut bool) {
    match expr {
        Expression::ColumnRef(col_id) => {
            if col_id.column_canonical().eq_ignore_ascii_case(old) {
                *col_id = renamed_column_identifier(col_id, new);
                *changed = true;
            }
        }
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            rename_walk(left, old, new, changed);
            rename_walk(right, old, new, changed);
        }
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                rename_walk(child, old, new, changed);
            }
        }
        Expression::UnaryOp { expr: inner, .. }
        | Expression::IsNull { expr: inner, .. }
        | Expression::IsTruthValue { expr: inner, .. }
        | Expression::Cast { expr: inner, .. }
        | Expression::Extract { expr: inner, .. }
        | Expression::Collate { expr: inner, .. }
        | Expression::Interval { value: inner, .. } => {
            rename_walk(inner, old, new, changed);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                rename_walk(arg, old, new, changed);
            }
        }
        Expression::AggregateFunction { args, order_by, filter, .. } => {
            for arg in args {
                rename_walk(arg, old, new, changed);
            }
            if let Some(items) = order_by {
                for item in items {
                    rename_walk(&mut item.expr, old, new, changed);
                }
            }
            if let Some(f) = filter {
                rename_walk(f, old, new, changed);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                rename_walk(op, old, new, changed);
            }
            for clause in when_clauses {
                for cond in &mut clause.conditions {
                    rename_walk(cond, old, new, changed);
                }
                rename_walk(&mut clause.result, old, new, changed);
            }
            if let Some(e) = else_result {
                rename_walk(e, old, new, changed);
            }
        }
        Expression::InList { expr: inner, values, .. } => {
            rename_walk(inner, old, new, changed);
            for value in values {
                rename_walk(value, old, new, changed);
            }
        }
        Expression::Between { expr: inner, low, high, .. } => {
            rename_walk(inner, old, new, changed);
            rename_walk(low, old, new, changed);
            rename_walk(high, old, new, changed);
        }
        Expression::Position { substring, string, .. } => {
            rename_walk(substring, old, new, changed);
            rename_walk(string, old, new, changed);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                rename_walk(rc, old, new, changed);
            }
            rename_walk(string, old, new, changed);
        }
        Expression::Like { expr: inner, pattern, escape, .. }
        | Expression::Glob { expr: inner, pattern, escape, .. } => {
            rename_walk(inner, old, new, changed);
            rename_walk(pattern, old, new, changed);
            if let Some(esc) = escape {
                rename_walk(esc, old, new, changed);
            }
        }
        // Subquery-bearing forms: only the immediate scalar operand is
        // rewritten; the subquery body is out of scope (not legal in index
        // expressions / partial-index predicates).
        Expression::In { expr: inner, .. }
        | Expression::QuantifiedComparison { expr: inner, .. } => {
            rename_walk(inner, old, new, changed);
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            rename_walk(search_modifier, old, new, changed);
        }
        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                rename_walk(msg, old, new, changed);
            }
        }
        // A `NEW.<col>` / `OLD.<col>` pseudo-variable inside a trigger's WHEN
        // condition references the trigger's subject table. When that table's
        // column is renamed, the pseudo-variable's column must be rewritten too
        // (index expressions never contain pseudo-variables, so this arm is
        // inert for the index-metadata caller).
        Expression::PseudoVariable { column, .. } => {
            if column.eq_ignore_ascii_case(old) {
                *column = new.to_string();
                *changed = true;
            }
        }
        // Leaf / out-of-scope forms: literals, placeholders, wildcard,
        // current date/time, DEFAULT, sequence refs, session variables,
        // subqueries, EXISTS, and window functions carry no rewritable
        // column reference here.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;
    use crate::{pretty_print::ToSql, BinaryOperator};

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::simple(name, false))
    }

    #[test]
    fn renames_simple_column_ref() {
        let mut expr = col("b");
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        assert_eq!(expr.to_sql(), "d");
    }

    #[test]
    fn rename_is_case_insensitive() {
        let mut expr = col("B");
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        assert_eq!(expr.to_sql(), "d");
    }

    #[test]
    fn leaves_other_columns_untouched() {
        let mut expr = col("c");
        assert!(!rename_column_in_expression(&mut expr, "b", "d"));
        assert_eq!(expr.to_sql(), "c");
    }

    #[test]
    fn renames_inside_binary_chain() {
        // b + b + b + b (the altercol.test 1.12 expression-index shape)
        let mut expr = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Plus,
                left: Box::new(col("b")),
                right: Box::new(col("b")),
            }),
            right: Box::new(col("b")),
        };
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        assert!(!expr.to_sql().contains('b'), "sql: {}", expr.to_sql());
        assert!(expr.to_sql().contains('d'));
    }

    #[test]
    fn renames_inside_function_args_and_where_shape() {
        // coalesce(b, c) AND b > 0
        let mut expr = Expression::Conjunction(vec![
            Expression::Function {
                name: crate::identifier::FunctionIdentifier::new("coalesce"),
                args: vec![col("b"), col("c")],
                character_unit: None,
            },
            Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(col("b")),
                right: Box::new(Expression::Literal(SqlValue::Integer(0))),
            },
        ]);
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        let sql = expr.to_sql();
        assert!(sql.contains("d, c") && sql.contains("d>0"), "sql: {}", sql);
    }

    #[test]
    fn preserves_table_qualifier() {
        let mut expr = Expression::ColumnRef(ColumnIdentifier::qualified("t1", false, "b", false));
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        match &expr {
            Expression::ColumnRef(c) => {
                assert_eq!(c.table_canonical(), Some("t1"));
                assert_eq!(c.column_canonical(), "d");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn renames_new_old_pseudo_variable_column() {
        // `NEW.b < 0` — the pseudo-variable's column must be rewritten so a
        // trigger WHEN condition tracks a RENAME COLUMN of its subject table.
        let mut expr = Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::PseudoVariable {
                pseudo_table: crate::expression::PseudoTable::New,
                column: "B".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(0))),
        };
        assert!(rename_column_in_expression(&mut expr, "b", "d"));
        match &expr {
            Expression::BinaryOp { left, .. } => match left.as_ref() {
                Expression::PseudoVariable { column, .. } => assert_eq!(column, "d"),
                other => panic!("expected pseudo-variable, got {other:?}"),
            },
            _ => unreachable!(),
        }
    }

    #[test]
    fn leaves_unrelated_pseudo_variable_column_untouched() {
        let mut expr = Expression::PseudoVariable {
            pseudo_table: crate::expression::PseudoTable::Old,
            column: "c".to_string(),
        };
        assert!(!rename_column_in_expression(&mut expr, "b", "d"));
    }
}
