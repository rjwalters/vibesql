//! Expression equality utilities for grouping operations
//!
//! Provides semantic comparison of SQL expressions with case-insensitive
//! identifier matching, used for GROUPING() function argument resolution.

use vibesql_ast::{Expression, WindowFunctionSpec, WindowSpec};

/// Check if two expressions are semantically equal (for matching GROUPING() arguments)
/// Uses case-insensitive comparison for column names, structural equality for others.
pub fn expressions_equal(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        // ColumnRef: case-insensitive comparison for identifiers
        (
            Expression::ColumnRef { table: t1, column: c1 },
            Expression::ColumnRef { table: t2, column: c2 },
        ) => {
            let columns_equal = c1.eq_ignore_ascii_case(c2);
            let tables_equal = match (t1, t2) {
                (Some(tb1), Some(tb2)) => tb1.eq_ignore_ascii_case(tb2),
                (None, None) => true,
                // If one has a qualifier and the other doesn't, they could still be equal
                _ => true,
            };
            columns_equal && tables_equal
        }

        // Literal: use derived PartialEq
        (Expression::Literal(v1), Expression::Literal(v2)) => v1 == v2,

        // BinaryOp: recurse into operands
        (
            Expression::BinaryOp { op: op1, left: l1, right: r1 },
            Expression::BinaryOp { op: op2, left: l2, right: r2 },
        ) => op1 == op2 && expressions_equal(l1, l2) && expressions_equal(r1, r2),

        // UnaryOp: recurse into operand
        (Expression::UnaryOp { op: op1, expr: e1 }, Expression::UnaryOp { op: op2, expr: e2 }) => {
            op1 == op2 && expressions_equal(e1, e2)
        }

        // Function: case-insensitive name, recurse into args
        (
            Expression::Function { name: n1, args: a1, character_unit: cu1 },
            Expression::Function { name: n2, args: a2, character_unit: cu2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && cu1 == cu2
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }

        // AggregateFunction: case-insensitive name, check distinct, recurse into args
        (
            Expression::AggregateFunction { name: n1, distinct: d1, args: a1 },
            Expression::AggregateFunction { name: n2, distinct: d2, args: a2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && d1 == d2
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }

        // IsNull: recurse into expression
        (
            Expression::IsNull { expr: e1, negated: n1 },
            Expression::IsNull { expr: e2, negated: n2 },
        ) => n1 == n2 && expressions_equal(e1, e2),

        // Wildcard
        (Expression::Wildcard, Expression::Wildcard) => true,

        // Case: recurse into operand, when_clauses, else_result
        (
            Expression::Case { operand: op1, when_clauses: w1, else_result: e1 },
            Expression::Case { operand: op2, when_clauses: w2, else_result: e2 },
        ) => {
            let operands_equal = match (op1, op2) {
                (Some(o1), Some(o2)) => expressions_equal(o1, o2),
                (None, None) => true,
                _ => false,
            };
            let whens_equal = w1.len() == w2.len()
                && w1.iter().zip(w2).all(|(wc1, wc2)| {
                    wc1.conditions.len() == wc2.conditions.len()
                        && wc1
                            .conditions
                            .iter()
                            .zip(&wc2.conditions)
                            .all(|(c1, c2)| expressions_equal(c1, c2))
                        && expressions_equal(&wc1.result, &wc2.result)
                });
            let else_equal = match (e1, e2) {
                (Some(el1), Some(el2)) => expressions_equal(el1, el2),
                (None, None) => true,
                _ => false,
            };
            operands_equal && whens_equal && else_equal
        }

        // ScalarSubquery: use reference equality (subqueries are unlikely to be semantically equal)
        (Expression::ScalarSubquery(_), Expression::ScalarSubquery(_)) => false,

        // In subquery: subqueries are not comparable for GROUPING() purposes
        (Expression::In { .. }, Expression::In { .. }) => false,

        // InList: recurse into expression and values
        (
            Expression::InList { expr: e1, values: v1, negated: n1 },
            Expression::InList { expr: e2, values: v2, negated: n2 },
        ) => {
            n1 == n2
                && expressions_equal(e1, e2)
                && v1.len() == v2.len()
                && v1.iter().zip(v2).all(|(x, y)| expressions_equal(x, y))
        }

        // Between: recurse into expressions
        (
            Expression::Between { expr: e1, low: l1, high: h1, negated: n1, symmetric: s1 },
            Expression::Between { expr: e2, low: l2, high: h2, negated: n2, symmetric: s2 },
        ) => {
            n1 == n2
                && s1 == s2
                && expressions_equal(e1, e2)
                && expressions_equal(l1, l2)
                && expressions_equal(h1, h2)
        }

        // Cast: recurse into expression, check data type
        (
            Expression::Cast { expr: e1, data_type: dt1 },
            Expression::Cast { expr: e2, data_type: dt2 },
        ) => dt1 == dt2 && expressions_equal(e1, e2),

        // Position: recurse into expressions
        (
            Expression::Position { substring: s1, string: st1, character_unit: cu1 },
            Expression::Position { substring: s2, string: st2, character_unit: cu2 },
        ) => cu1 == cu2 && expressions_equal(s1, s2) && expressions_equal(st1, st2),

        // Trim: check position, recurse into expressions
        (
            Expression::Trim { position: p1, removal_char: r1, string: s1 },
            Expression::Trim { position: p2, removal_char: r2, string: s2 },
        ) => {
            let removal_equal = match (r1, r2) {
                (Some(rc1), Some(rc2)) => expressions_equal(rc1, rc2),
                (None, None) => true,
                _ => false,
            };
            p1 == p2 && removal_equal && expressions_equal(s1, s2)
        }

        // Like: recurse into expressions
        (
            Expression::Like { expr: e1, pattern: p1, negated: n1 },
            Expression::Like { expr: e2, pattern: p2, negated: n2 },
        ) => n1 == n2 && expressions_equal(e1, e2) && expressions_equal(p1, p2),

        // Exists: subqueries are not comparable for GROUPING() purposes
        (Expression::Exists { .. }, Expression::Exists { .. }) => false,

        // QuantifiedComparison: subqueries are not comparable for GROUPING() purposes
        (Expression::QuantifiedComparison { .. }, Expression::QuantifiedComparison { .. }) => false,

        // Current date/time functions
        (Expression::CurrentDate, Expression::CurrentDate) => true,
        (Expression::CurrentTime { precision: p1 }, Expression::CurrentTime { precision: p2 }) => {
            p1 == p2
        }
        (
            Expression::CurrentTimestamp { precision: p1 },
            Expression::CurrentTimestamp { precision: p2 },
        ) => p1 == p2,

        // Interval: recurse into value expression
        (
            Expression::Interval {
                value: v1,
                unit: u1,
                leading_precision: lp1,
                fractional_precision: fp1,
            },
            Expression::Interval {
                value: v2,
                unit: u2,
                leading_precision: lp2,
                fractional_precision: fp2,
            },
        ) => u1 == u2 && lp1 == lp2 && fp1 == fp2 && expressions_equal(v1, v2),

        // Default
        (Expression::Default, Expression::Default) => true,

        // DuplicateKeyValue: case-insensitive column comparison
        (
            Expression::DuplicateKeyValue { column: c1 },
            Expression::DuplicateKeyValue { column: c2 },
        ) => c1.eq_ignore_ascii_case(c2),

        // WindowFunction: recurse into function spec and over clause
        (
            Expression::WindowFunction { function: f1, over: o1 },
            Expression::WindowFunction { function: f2, over: o2 },
        ) => window_function_equal(f1, f2) && window_spec_equal(o1, o2),

        // NextValue: case-insensitive sequence name
        (
            Expression::NextValue { sequence_name: s1 },
            Expression::NextValue { sequence_name: s2 },
        ) => s1.eq_ignore_ascii_case(s2),

        // MatchAgainst: case-insensitive column names, recurse into search modifier
        (
            Expression::MatchAgainst { columns: c1, search_modifier: s1, mode: m1 },
            Expression::MatchAgainst { columns: c2, search_modifier: s2, mode: m2 },
        ) => {
            m1 == m2
                && c1.len() == c2.len()
                && c1.iter().zip(c2).all(|(a, b)| a.eq_ignore_ascii_case(b))
                && expressions_equal(s1, s2)
        }

        // PseudoVariable: check pseudo table and case-insensitive column
        (
            Expression::PseudoVariable { pseudo_table: pt1, column: c1 },
            Expression::PseudoVariable { pseudo_table: pt2, column: c2 },
        ) => pt1 == pt2 && c1.eq_ignore_ascii_case(c2),

        // SessionVariable: case-insensitive name
        (Expression::SessionVariable { name: n1 }, Expression::SessionVariable { name: n2 }) => {
            n1.eq_ignore_ascii_case(n2)
        }

        // Different variants are not equal
        _ => false,
    }
}

/// Check if two window function specs are equal
fn window_function_equal(a: &WindowFunctionSpec, b: &WindowFunctionSpec) -> bool {
    match (a, b) {
        (
            WindowFunctionSpec::Aggregate { name: n1, args: a1 },
            WindowFunctionSpec::Aggregate { name: n2, args: a2 },
        )
        | (
            WindowFunctionSpec::Ranking { name: n1, args: a1 },
            WindowFunctionSpec::Ranking { name: n2, args: a2 },
        )
        | (
            WindowFunctionSpec::Value { name: n1, args: a1 },
            WindowFunctionSpec::Value { name: n2, args: a2 },
        ) => {
            n1.eq_ignore_ascii_case(n2)
                && a1.len() == a2.len()
                && a1.iter().zip(a2).all(|(x, y)| expressions_equal(x, y))
        }
        _ => false,
    }
}

/// Check if two window specs are equal
fn window_spec_equal(a: &WindowSpec, b: &WindowSpec) -> bool {
    let partition_equal = match (&a.partition_by, &b.partition_by) {
        (Some(p1), Some(p2)) => {
            p1.len() == p2.len() && p1.iter().zip(p2).all(|(x, y)| expressions_equal(x, y))
        }
        (None, None) => true,
        _ => false,
    };

    // For order_by and frame, use derived PartialEq (they don't contain identifiers
    // that need case-insensitive comparison at the top level)
    let order_equal = a.order_by == b.order_by;
    let frame_equal = a.frame == b.frame;

    partition_equal && order_equal && frame_equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, SelectStmt, UnaryOperator};
    use vibesql_types::DataType;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef { table: None, column: name.to_string() }
    }

    fn qualified_col(table: &str, column: &str) -> Expression {
        Expression::ColumnRef { table: Some(table.to_string()), column: column.to_string() }
    }

    fn lit_int(n: i64) -> Expression {
        Expression::Literal(vibesql_types::SqlValue::Integer(n))
    }

    fn lit_str(s: &str) -> Expression {
        Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(s)))
    }

    fn binary_op(op: BinaryOperator, left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) }
    }

    fn unary_op(op: UnaryOperator, expr: Expression) -> Expression {
        Expression::UnaryOp { op, expr: Box::new(expr) }
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::Function { name: name.to_string(), args, character_unit: None }
    }

    fn agg(name: &str, args: Vec<Expression>, distinct: bool) -> Expression {
        Expression::AggregateFunction { name: name.to_string(), distinct, args }
    }

    // --- ColumnRef tests ---

    #[test]
    fn test_expressions_equal_column_ref_same() {
        assert!(expressions_equal(&col("name"), &col("name")));
    }

    #[test]
    fn test_expressions_equal_column_ref_case_insensitive() {
        assert!(expressions_equal(&col("Name"), &col("name")));
        assert!(expressions_equal(&col("NAME"), &col("name")));
        assert!(expressions_equal(&col("NaMe"), &col("nAmE")));
    }

    #[test]
    fn test_expressions_equal_column_ref_different() {
        assert!(!expressions_equal(&col("name"), &col("id")));
    }

    #[test]
    fn test_expressions_equal_column_ref_qualified() {
        // Same table and column
        assert!(expressions_equal(
            &qualified_col("users", "name"),
            &qualified_col("users", "name")
        ));
        // Case-insensitive table
        assert!(expressions_equal(
            &qualified_col("USERS", "name"),
            &qualified_col("users", "name")
        ));
        // Different table
        assert!(!expressions_equal(
            &qualified_col("users", "name"),
            &qualified_col("orders", "name")
        ));
    }

    #[test]
    fn test_expressions_equal_column_ref_mixed_qualification() {
        // One qualified, one unqualified - allowed to be equal (conservative)
        assert!(expressions_equal(&col("name"), &qualified_col("users", "name")));
        assert!(expressions_equal(&qualified_col("users", "name"), &col("name")));
    }

    // --- Literal tests ---

    #[test]
    fn test_expressions_equal_literal_same() {
        assert!(expressions_equal(&lit_int(42), &lit_int(42)));
        assert!(expressions_equal(&lit_str("hello"), &lit_str("hello")));
    }

    #[test]
    fn test_expressions_equal_literal_different() {
        assert!(!expressions_equal(&lit_int(42), &lit_int(43)));
        assert!(!expressions_equal(&lit_str("hello"), &lit_str("world")));
        assert!(!expressions_equal(&lit_int(42), &lit_str("42")));
    }

    // --- BinaryOp tests ---

    #[test]
    fn test_expressions_equal_binary_op() {
        // Same operation
        assert!(expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1))
        ));

        // Case-insensitive column in binary op
        assert!(expressions_equal(
            &binary_op(BinaryOperator::Plus, col("A"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1))
        ));

        // Different operator
        assert!(!expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Minus, col("a"), lit_int(1))
        ));

        // Different operands
        assert!(!expressions_equal(
            &binary_op(BinaryOperator::Plus, col("a"), lit_int(1)),
            &binary_op(BinaryOperator::Plus, col("b"), lit_int(1))
        ));
    }

    // --- UnaryOp tests ---

    #[test]
    fn test_expressions_equal_unary_op() {
        assert!(expressions_equal(
            &unary_op(UnaryOperator::Minus, col("x")),
            &unary_op(UnaryOperator::Minus, col("x"))
        ));

        assert!(expressions_equal(
            &unary_op(UnaryOperator::Minus, col("X")),
            &unary_op(UnaryOperator::Minus, col("x"))
        ));

        assert!(!expressions_equal(
            &unary_op(UnaryOperator::Minus, col("x")),
            &unary_op(UnaryOperator::Not, col("x"))
        ));
    }

    // --- Function tests ---

    #[test]
    fn test_expressions_equal_function() {
        // Same function
        assert!(expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("UPPER", vec![col("name")])
        ));

        // Case-insensitive function name
        assert!(expressions_equal(
            &func("upper", vec![col("name")]),
            &func("UPPER", vec![col("name")])
        ));

        // Case-insensitive arg column
        assert!(expressions_equal(
            &func("UPPER", vec![col("NAME")]),
            &func("UPPER", vec![col("name")])
        ));

        // Different function
        assert!(!expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("LOWER", vec![col("name")])
        ));

        // Different args
        assert!(!expressions_equal(
            &func("UPPER", vec![col("name")]),
            &func("UPPER", vec![col("id")])
        ));

        // Different arg count
        assert!(!expressions_equal(
            &func("COALESCE", vec![col("a"), lit_int(0)]),
            &func("COALESCE", vec![col("a")])
        ));
    }

    // --- AggregateFunction tests ---

    #[test]
    fn test_expressions_equal_aggregate() {
        // Same aggregate
        assert!(expressions_equal(
            &agg("SUM", vec![col("amount")], false),
            &agg("SUM", vec![col("amount")], false)
        ));

        // Case-insensitive
        assert!(expressions_equal(
            &agg("sum", vec![col("amount")], false),
            &agg("SUM", vec![col("amount")], false)
        ));

        // Different distinct flag
        assert!(!expressions_equal(
            &agg("COUNT", vec![col("id")], true),
            &agg("COUNT", vec![col("id")], false)
        ));
    }

    // --- IsNull tests ---

    #[test]
    fn test_expressions_equal_is_null() {
        let is_null =
            |e: Expression, negated: bool| Expression::IsNull { expr: Box::new(e), negated };

        assert!(expressions_equal(&is_null(col("a"), false), &is_null(col("a"), false)));
        assert!(expressions_equal(&is_null(col("A"), false), &is_null(col("a"), false)));
        assert!(!expressions_equal(&is_null(col("a"), false), &is_null(col("a"), true)));
        assert!(!expressions_equal(&is_null(col("a"), false), &is_null(col("b"), false)));
    }

    // --- Cast tests ---

    #[test]
    fn test_expressions_equal_cast() {
        let cast =
            |e: Expression, dt: DataType| Expression::Cast { expr: Box::new(e), data_type: dt };

        assert!(expressions_equal(
            &cast(col("a"), DataType::Integer),
            &cast(col("a"), DataType::Integer)
        ));

        assert!(expressions_equal(
            &cast(col("A"), DataType::Integer),
            &cast(col("a"), DataType::Integer)
        ));

        assert!(!expressions_equal(
            &cast(col("a"), DataType::Integer),
            &cast(col("a"), DataType::Varchar { max_length: None })
        ));
    }

    // --- InList tests ---

    #[test]
    fn test_expressions_equal_in_list() {
        let in_list = |e: Expression, vals: Vec<Expression>, negated: bool| Expression::InList {
            expr: Box::new(e),
            values: vals,
            negated,
        };

        assert!(expressions_equal(
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false)
        ));

        // Case-insensitive column
        assert!(expressions_equal(
            &in_list(col("STATUS"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false)
        ));

        // Different values
        assert!(!expressions_equal(
            &in_list(col("status"), vec![lit_str("a"), lit_str("b")], false),
            &in_list(col("status"), vec![lit_str("a"), lit_str("c")], false)
        ));

        // Different negation
        assert!(!expressions_equal(
            &in_list(col("status"), vec![lit_str("a")], false),
            &in_list(col("status"), vec![lit_str("a")], true)
        ));
    }

    // --- Between tests ---

    #[test]
    fn test_expressions_equal_between() {
        let between =
            |e: Expression, lo: Expression, hi: Expression, negated: bool| Expression::Between {
                expr: Box::new(e),
                low: Box::new(lo),
                high: Box::new(hi),
                negated,
                symmetric: false,
            };

        assert!(expressions_equal(
            &between(col("age"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), false)
        ));

        assert!(expressions_equal(
            &between(col("AGE"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), false)
        ));

        assert!(!expressions_equal(
            &between(col("age"), lit_int(18), lit_int(65), false),
            &between(col("age"), lit_int(18), lit_int(65), true)
        ));
    }

    // --- Like tests ---

    #[test]
    fn test_expressions_equal_like() {
        let like = |e: Expression, p: Expression, negated: bool| Expression::Like {
            expr: Box::new(e),
            pattern: Box::new(p),
            negated,
        };

        assert!(expressions_equal(
            &like(col("name"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%john%"), false)
        ));

        assert!(expressions_equal(
            &like(col("NAME"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%john%"), false)
        ));

        assert!(!expressions_equal(
            &like(col("name"), lit_str("%john%"), false),
            &like(col("name"), lit_str("%jane%"), false)
        ));
    }

    // --- Subquery tests (always return false) ---

    #[test]
    fn test_expressions_equal_scalar_subquery() {
        // Create minimal SelectStmt instances
        let subq1 = Expression::ScalarSubquery(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));
        let subq2 = Expression::ScalarSubquery(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        // Subqueries always compare as false (reference inequality)
        assert!(!expressions_equal(&subq1, &subq2));
    }

    // --- Different variants ---

    #[test]
    fn test_expressions_equal_different_variants() {
        assert!(!expressions_equal(&col("a"), &lit_int(1)));
        assert!(!expressions_equal(&lit_int(1), &Expression::Wildcard));
        assert!(!expressions_equal(&col("a"), &func("UPPER", vec![col("a")])));
    }

    // --- Wildcard ---

    #[test]
    fn test_expressions_equal_wildcard() {
        assert!(expressions_equal(&Expression::Wildcard, &Expression::Wildcard));
    }

    // --- Current date/time ---

    #[test]
    fn test_expressions_equal_current_date() {
        assert!(expressions_equal(&Expression::CurrentDate, &Expression::CurrentDate));
        assert!(expressions_equal(
            &Expression::CurrentTime { precision: Some(3) },
            &Expression::CurrentTime { precision: Some(3) }
        ));
        assert!(!expressions_equal(
            &Expression::CurrentTime { precision: Some(3) },
            &Expression::CurrentTime { precision: Some(6) }
        ));
        assert!(!expressions_equal(
            &Expression::CurrentDate,
            &Expression::CurrentTime { precision: None }
        ));
    }

    // --- Nested expressions ---

    #[test]
    fn test_expressions_equal_nested() {
        // (a + b) * c == (A + B) * C
        let expr1 = binary_op(
            BinaryOperator::Multiply,
            binary_op(BinaryOperator::Plus, col("a"), col("b")),
            col("c"),
        );
        let expr2 = binary_op(
            BinaryOperator::Multiply,
            binary_op(BinaryOperator::Plus, col("A"), col("B")),
            col("C"),
        );
        assert!(expressions_equal(&expr1, &expr2));

        // UPPER(LOWER(name)) == UPPER(LOWER(NAME))
        let expr3 = func("UPPER", vec![func("LOWER", vec![col("name")])]);
        let expr4 = func("UPPER", vec![func("LOWER", vec![col("NAME")])]);
        assert!(expressions_equal(&expr3, &expr4));
    }
}
