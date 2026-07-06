use super::*;

// ========================================================================
// ISNULL / NOTNULL / NOT NULL postfix composability tests (issue #5857)
//
// Per SQLite, ISNULL, NOTNULL, and postfix NOT NULL are left-associative
// comparison-tier postfix operators whose result composes as an operand.
// They are syntactically closed (no right operand), so — exactly like IN
// (issue #5801 / PR #5811) — a tighter-binding operator (+, -, *, /, %,
// ||, <<, >>) that follows takes the whole null-test node as its left
// operand: `1 ISNULL + 1` parses as `(1 ISNULL) + 1` = 1.
//
// Contrast with `IS NULL`: there SQLite treats NULL as an ordinary
// expression operand of IS, so `1 IS NULL + 1` parses as `1 IS (NULL + 1)`
// (that IS-operand shape is out of scope here).
//
// All expected values in the comments below were verified against sqlite3.
// ========================================================================

/// Parse a `SELECT <expr>;` statement and return the first select-list expression.
fn parse_select_expr(sql: &str) -> vibesql_ast::Expression {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("should parse: {}: {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            return expr.clone();
        }
    }
    panic!("expected SELECT with expression select list: {}", sql);
}

/// Assert the expression is `IsNull` with the given polarity and return its operand.
fn unwrap_is_null(
    expr: vibesql_ast::Expression,
    expected_negated: bool,
    context: &str,
) -> vibesql_ast::Expression {
    if let vibesql_ast::Expression::IsNull { expr: inner, negated } = expr {
        assert_eq!(negated, expected_negated, "{}: wrong IsNull polarity", context);
        *inner
    } else {
        panic!("{}: expected IsNull node", context);
    }
}

// ------------------------------------------------------------------------
// Chaining within the comparison tier
// ------------------------------------------------------------------------

#[test]
fn test_isnull_chained_notnull() {
    // sqlite3: SELECT 1 ISNULL NOTNULL; -- 1, i.e. ((1 ISNULL) NOTNULL)
    let expr = parse_select_expr("SELECT 1 ISNULL NOTNULL;");
    let inner = unwrap_is_null(expr, true, "outer NOTNULL");
    unwrap_is_null(inner, false, "inner ISNULL");
}

#[test]
fn test_not_null_chained_not_null() {
    // sqlite3: SELECT 1 NOT NULL NOT NULL; -- 1
    let expr = parse_select_expr("SELECT 1 NOT NULL NOT NULL;");
    let inner = unwrap_is_null(expr, true, "outer NOT NULL");
    unwrap_is_null(inner, true, "inner NOT NULL");
}

#[test]
fn test_isnull_chained_not_null() {
    // sqlite3: SELECT 1 ISNULL NOT NULL; -- 1
    let expr = parse_select_expr("SELECT 1 ISNULL NOT NULL;");
    let inner = unwrap_is_null(expr, true, "outer NOT NULL");
    unwrap_is_null(inner, false, "inner ISNULL");
}

#[test]
fn test_not_null_chained_isnull() {
    // sqlite3: SELECT 1 NOT NULL ISNULL; -- 0
    let expr = parse_select_expr("SELECT 1 NOT NULL ISNULL;");
    let inner = unwrap_is_null(expr, false, "outer ISNULL");
    unwrap_is_null(inner, true, "inner NOT NULL");
}

// ------------------------------------------------------------------------
// Tighter-binding operators after the closed postfix node
// ------------------------------------------------------------------------

#[test]
fn test_isnull_followed_by_plus() {
    // sqlite3: SELECT 1 ISNULL + 1; -- 1, i.e. ((1 ISNULL) + 1)
    let expr = parse_select_expr("SELECT 1 ISNULL + 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        unwrap_is_null(*left, false, "left operand of +");
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

#[test]
fn test_isnull_followed_by_minus() {
    // sqlite3: SELECT 1 ISNULL - 1; -- -1
    let expr = parse_select_expr("SELECT 1 ISNULL - 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Minus);
        unwrap_is_null(*left, false, "left operand of -");
    } else {
        panic!("expected BinaryOp Minus at top level");
    }
}

#[test]
fn test_isnull_followed_by_multiply() {
    // sqlite3: SELECT NULL ISNULL * 3; -- 3
    let expr = parse_select_expr("SELECT NULL ISNULL * 3;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
        unwrap_is_null(*left, false, "left operand of *");
    } else {
        panic!("expected BinaryOp Multiply at top level");
    }
}

#[test]
fn test_isnull_followed_by_divide() {
    // sqlite3: SELECT 1 ISNULL / 1; -- 0
    let expr = parse_select_expr("SELECT 1 ISNULL / 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Divide);
        unwrap_is_null(*left, false, "left operand of /");
    } else {
        panic!("expected BinaryOp Divide at top level");
    }
}

#[test]
fn test_notnull_followed_by_modulo() {
    // sqlite3: SELECT 3 NOTNULL % 2; -- 1
    let expr = parse_select_expr("SELECT 3 NOTNULL % 2;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Modulo);
        unwrap_is_null(*left, true, "left operand of %");
    } else {
        panic!("expected BinaryOp Modulo at top level");
    }
}

#[test]
fn test_notnull_followed_by_concat() {
    // sqlite3: SELECT 4 NOTNULL || 'x'; -- '1x'
    let expr = parse_select_expr("SELECT 4 NOTNULL || 'x';");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Concat);
        unwrap_is_null(*left, true, "left operand of ||");
    } else {
        panic!("expected BinaryOp Concat at top level");
    }
}

#[test]
fn test_isnull_followed_by_shift() {
    // sqlite3: SELECT 1 ISNULL << 2; -- 0
    let expr = parse_select_expr("SELECT 1 ISNULL << 2;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::LeftShift);
        unwrap_is_null(*left, false, "left operand of <<");
    } else {
        panic!("expected BinaryOp LeftShift at top level");
    }
}

#[test]
fn test_not_null_followed_by_plus() {
    // sqlite3: SELECT 1 NOT NULL + 1; -- 2, i.e. ((1 NOT NULL) + 1)
    let expr = parse_select_expr("SELECT 1 NOT NULL + 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        unwrap_is_null(*left, true, "left operand of +");
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

#[test]
fn test_not_null_followed_by_shift() {
    // sqlite3: SELECT 1 NOT NULL << 3; -- 8
    let expr = parse_select_expr("SELECT 1 NOT NULL << 3;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::LeftShift);
        unwrap_is_null(*left, true, "left operand of <<");
    } else {
        panic!("expected BinaryOp LeftShift at top level");
    }
}

#[test]
fn test_not_null_followed_by_concat() {
    // sqlite3: SELECT NULL NOT NULL || 'x'; -- '0x'
    let expr = parse_select_expr("SELECT NULL NOT NULL || 'x';");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Concat);
        unwrap_is_null(*left, true, "left operand of ||");
    } else {
        panic!("expected BinaryOp Concat at top level");
    }
}

#[test]
fn test_isnull_inside_function_call() {
    // sqlite3: SELECT lower(1 NOTNULL << 2); -- '4'
    let result = Parser::parse_sql("SELECT lower(1 NOTNULL << 2);");
    assert!(
        result.is_ok(),
        "NOTNULL followed by << inside function args should parse: {:?}",
        result
    );
}

// ------------------------------------------------------------------------
// Relative precedence among the continued tighter tiers is preserved
// ------------------------------------------------------------------------

#[test]
fn test_notnull_plus_then_multiply() {
    // sqlite3: SELECT 1 NOTNULL + 2 * 3; -- 7, i.e. (1 NOTNULL) + (2 * 3)
    let expr = parse_select_expr("SELECT 1 NOTNULL + 2 * 3;");
    if let vibesql_ast::Expression::BinaryOp { op, left, right } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        unwrap_is_null(*left, true, "left operand of +");
        assert!(
            matches!(
                *right,
                vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Multiply, .. }
            ),
            "right operand of + should be (2 * 3): {:?}",
            right
        );
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

#[test]
fn test_notnull_multiply_then_plus() {
    // sqlite3: SELECT 1 NOTNULL * 2 + 3; -- 5, i.e. ((1 NOTNULL) * 2) + 3
    let expr = parse_select_expr("SELECT 1 NOTNULL * 2 + 3;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        if let vibesql_ast::Expression::BinaryOp { op: inner_op, left: inner_left, .. } = *left {
            assert_eq!(inner_op, vibesql_ast::BinaryOperator::Multiply);
            unwrap_is_null(*inner_left, true, "innermost left");
        } else {
            panic!("left operand of + should be ((1 NOTNULL) * 2): {:?}", left);
        }
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

#[test]
fn test_notnull_plus_minus_chain() {
    // sqlite3: SELECT 5 NOTNULL + 10 - 3; -- 8, i.e. ((5 NOTNULL) + 10) - 3
    let expr = parse_select_expr("SELECT 5 NOTNULL + 10 - 3;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Minus);
        if let vibesql_ast::Expression::BinaryOp { op: inner_op, left: inner_left, .. } = *left {
            assert_eq!(inner_op, vibesql_ast::BinaryOperator::Plus);
            unwrap_is_null(*inner_left, true, "innermost left");
        } else {
            panic!("left operand of - should be ((5 NOTNULL) + 10): {:?}", left);
        }
    } else {
        panic!("expected BinaryOp Minus at top level");
    }
}

#[test]
fn test_isnull_isnull_then_plus() {
    // sqlite3: SELECT 1 ISNULL ISNULL + 1; -- 1, i.e. ((1 ISNULL) ISNULL) + 1
    let expr = parse_select_expr("SELECT 1 ISNULL ISNULL + 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        let inner = unwrap_is_null(*left, false, "left operand of +");
        unwrap_is_null(inner, false, "inner ISNULL");
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

// ------------------------------------------------------------------------
// Comparison-tier operators after the postfix node (loop chaining)
// ------------------------------------------------------------------------

#[test]
fn test_isnull_followed_by_less_than() {
    // sqlite3: SELECT 1 ISNULL < 2; -- 1
    let expr = parse_select_expr("SELECT 1 ISNULL < 2;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::LessThan);
        unwrap_is_null(*left, false, "left operand of <");
    } else {
        panic!("expected BinaryOp LessThan at top level");
    }
}

#[test]
fn test_isnull_followed_by_equal() {
    // sqlite3: SELECT NULL ISNULL = 1; -- 1
    let expr = parse_select_expr("SELECT NULL ISNULL = 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Equal);
        unwrap_is_null(*left, false, "left operand of =");
    } else {
        panic!("expected BinaryOp Equal at top level");
    }
}

#[test]
fn test_equal_followed_by_isnull() {
    // sqlite3: SELECT 5 = 5 ISNULL; -- 0, i.e. ((5 = 5) ISNULL)
    let expr = parse_select_expr("SELECT 5 = 5 ISNULL;");
    let inner = unwrap_is_null(expr, false, "outer ISNULL");
    assert!(
        matches!(
            inner,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Equal, .. }
        ),
        "ISNULL operand should be (5 = 5): {:?}",
        inner
    );
}

#[test]
fn test_isnull_followed_by_between() {
    // sqlite3: SELECT 1 ISNULL BETWEEN -1 AND 1; -- 1
    let expr = parse_select_expr("SELECT 1 ISNULL BETWEEN -1 AND 1;");
    if let vibesql_ast::Expression::Between { expr: inner, negated, .. } = expr {
        assert!(!negated);
        unwrap_is_null(*inner, false, "BETWEEN subject");
    } else {
        panic!("expected Between at top level");
    }
}

#[test]
fn test_in_followed_by_isnull() {
    // sqlite3: SELECT 2 IN (0,1) ISNULL; -- 0
    let expr = parse_select_expr("SELECT 2 IN (0,1) ISNULL;");
    let inner = unwrap_is_null(expr, false, "outer ISNULL");
    assert!(
        matches!(inner, vibesql_ast::Expression::InList { .. }),
        "ISNULL operand should be the IN list node: {:?}",
        inner
    );
}

#[test]
fn test_isnull_followed_by_in() {
    // sqlite3: SELECT 1 ISNULL IN (0,1); -- 1
    let expr = parse_select_expr("SELECT 1 ISNULL IN (0,1);");
    if let vibesql_ast::Expression::InList { expr: inner, negated, .. } = expr {
        assert!(!negated);
        unwrap_is_null(*inner, false, "IN subject");
    } else {
        panic!("expected InList at top level");
    }
}

#[test]
fn test_isnull_followed_by_like() {
    // sqlite3: SELECT NULL ISNULL LIKE '1'; -- 1
    let expr = parse_select_expr("SELECT NULL ISNULL LIKE '1';");
    if let vibesql_ast::Expression::Like { expr: inner, negated, .. } = expr {
        assert!(!negated);
        unwrap_is_null(*inner, false, "LIKE subject");
    } else {
        panic!("expected Like at top level");
    }
}

// ------------------------------------------------------------------------
// Boolean tier and surrounding constructs still behave
// ------------------------------------------------------------------------

#[test]
fn test_isnull_followed_by_and() {
    // sqlite3: SELECT 1 ISNULL AND 1; -- 0, i.e. ((1 ISNULL) AND 1)
    let expr = parse_select_expr("SELECT 1 ISNULL AND 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::And);
        unwrap_is_null(*left, false, "left operand of AND");
    } else {
        panic!("expected BinaryOp And at top level");
    }
}

#[test]
fn test_parenthesized_isnull_plus_still_works() {
    // sqlite3: SELECT (1 ISNULL) + 1; -- 1 (worked before the fix)
    let expr = parse_select_expr("SELECT (1 ISNULL) + 1;");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(op, vibesql_ast::BinaryOperator::Plus);
        unwrap_is_null(*left, false, "left operand of +");
    } else {
        panic!("expected BinaryOp Plus at top level");
    }
}

#[test]
fn test_lhs_precedence_preserved() {
    // sqlite3: SELECT 2 + 1 ISNULL; -- 0, i.e. ((2 + 1) ISNULL)
    // + binds tighter than ISNULL on the LEFT side.
    let expr = parse_select_expr("SELECT 2 + 1 ISNULL;");
    let inner = unwrap_is_null(expr, false, "outer ISNULL");
    assert!(
        matches!(
            inner,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Plus, .. }
        ),
        "ISNULL operand should be (2 + 1): {:?}",
        inner
    );
}

#[test]
fn test_continuation_stops_at_select_list_comma() {
    // sqlite3: SELECT 1 NOTNULL * 2, 2 ISNULL + 5; -- 2|5
    let stmt = Parser::parse_sql("SELECT 1 NOTNULL * 2, 2 ISNULL + 5;").expect("should parse");
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 2, "continuation must not consume the comma");
    } else {
        panic!("expected SELECT statement");
    }
}

#[test]
fn test_default_not_null_column_constraint_unaffected() {
    // The column-definition heuristic (DEFAULT 'v' NOT NULL is a constraint,
    // not the postfix operator) must be unaffected by the continuation.
    let result = Parser::parse_sql("CREATE TABLE t (a TEXT DEFAULT 'v' NOT NULL, b INT);");
    assert!(result.is_ok(), "DEFAULT <literal> NOT NULL constraint should parse: {:?}", result);
}

// ========================================================================
// `IS` operand composability (issue #5889)
//
// Unlike the closed postfix operators ISNULL / NOTNULL / NOT NULL, SQLite's
// `IS` takes an ordinary *expression* right operand, and NULL/TRUE/FALSE are
// themselves expressions. So a tighter-binding operator that follows binds
// to the right operand, not to the IS result:
//
//   SELECT 1 IS NULL + 1;      -- 0   i.e. 1 IS (NULL + 1)
//   SELECT NULL IS NULL + 1;   -- 1   i.e. NULL IS (NULL + 1)
//   SELECT 1 IS NOT NULL + 1;  -- 1   i.e. 1 IS NOT (NULL + 1)
//   SELECT 1 IS TRUE + 1;      -- 0   i.e. 1 IS (TRUE + 1) = 1 IS 2
//
// When the right operand is a bare NULL/TRUE/FALSE literal we keep emitting
// the specialized IsNull / IsTruthValue AST shape optimizers rely on; when it
// is any other expression we emit IsDistinctFrom (NULL-safe equality), whose
// `negated` field is the inverse of the surface IS/IS-NOT polarity.
// ========================================================================

/// Assert the expression is `IsDistinctFrom` with the given polarity and
/// return `(left, right)` operands.
fn unwrap_is_distinct_from(
    expr: vibesql_ast::Expression,
    expected_negated: bool,
    context: &str,
) -> (vibesql_ast::Expression, vibesql_ast::Expression) {
    if let vibesql_ast::Expression::IsDistinctFrom { left, right, negated } = expr {
        assert_eq!(negated, expected_negated, "{}: wrong IsDistinctFrom polarity", context);
        (*left, *right)
    } else {
        panic!("{}: expected IsDistinctFrom node, got {:?}", context, expr);
    }
}

/// Assert the expression is a `NULL + 1` additive BinaryOp.
fn assert_null_plus_one(expr: &vibesql_ast::Expression, context: &str) {
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = expr {
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus, "{}: expected + operator", context);
        assert!(
            matches!(**left, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
            "{}: expected NULL as left operand of +: {:?}",
            context,
            left
        );
    } else {
        panic!("{}: expected BinaryOp Plus, got {:?}", context, expr);
    }
}

#[test]
fn test_is_null_plus_one_binds_to_operand() {
    // sqlite3: SELECT 1 IS NULL + 1; -- 0, i.e. 1 IS (NULL + 1)
    let expr = parse_select_expr("SELECT 1 IS NULL + 1;");
    // IS (not negated) → IsDistinctFrom { negated: true } (NULL-safe equals).
    let (_left, right) = unwrap_is_distinct_from(expr, true, "1 IS NULL + 1");
    assert_null_plus_one(&right, "1 IS NULL + 1 right operand");
}

#[test]
fn test_null_is_null_plus_one_binds_to_operand() {
    // sqlite3: SELECT NULL IS NULL + 1; -- 1, i.e. NULL IS (NULL + 1)
    let expr = parse_select_expr("SELECT NULL IS NULL + 1;");
    let (left, right) = unwrap_is_distinct_from(expr, true, "NULL IS NULL + 1");
    assert!(
        matches!(left, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        "left operand should be NULL: {:?}",
        left
    );
    assert_null_plus_one(&right, "NULL IS NULL + 1 right operand");
}

#[test]
fn test_is_not_null_plus_one_binds_to_operand() {
    // sqlite3: SELECT 1 IS NOT NULL + 1; -- 1, i.e. 1 IS NOT (NULL + 1)
    let expr = parse_select_expr("SELECT 1 IS NOT NULL + 1;");
    // IS NOT (negated) → IsDistinctFrom { negated: false } (NULL-safe not-equals).
    let (_left, right) = unwrap_is_distinct_from(expr, false, "1 IS NOT NULL + 1");
    assert_null_plus_one(&right, "1 IS NOT NULL + 1 right operand");
}

#[test]
fn test_is_true_plus_one_binds_to_operand() {
    // sqlite3: SELECT 1 IS TRUE + 1; -- 0, i.e. 1 IS (TRUE + 1) = 1 IS 2
    let expr = parse_select_expr("SELECT 1 IS TRUE + 1;");
    // TRUE + 1 is a BinaryOp, not a bare literal → IsDistinctFrom, not IsTruthValue.
    let (_left, right) = unwrap_is_distinct_from(expr, true, "1 IS TRUE + 1");
    if let vibesql_ast::Expression::BinaryOp { op, left, .. } = &right {
        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus, "expected + operator");
        assert!(
            matches!(**left, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))),
            "expected TRUE as left operand of +: {:?}",
            left
        );
    } else {
        panic!("expected BinaryOp Plus, got {:?}", right);
    }
}

// ------------------------------------------------------------------------
// Non-regression: plain literal right operands keep their specialized shape
// ------------------------------------------------------------------------

#[test]
fn test_plain_is_null_still_is_null_node() {
    // No trailing tighter operator → keep the IsNull AST optimizers rely on.
    let expr = parse_select_expr("SELECT x IS NULL;");
    unwrap_is_null(expr, false, "x IS NULL");
}

#[test]
fn test_plain_is_not_null_still_is_null_node() {
    let expr = parse_select_expr("SELECT x IS NOT NULL;");
    unwrap_is_null(expr, true, "x IS NOT NULL");
}

#[test]
fn test_plain_is_true_still_truth_value_node() {
    let expr = parse_select_expr("SELECT x IS TRUE;");
    assert!(
        matches!(
            expr,
            vibesql_ast::Expression::IsTruthValue {
                truth_value: vibesql_ast::TruthValue::True,
                negated: false,
                ..
            }
        ),
        "x IS TRUE should be IsTruthValue(True): {:?}",
        expr
    );
}

#[test]
fn test_plain_is_false_still_truth_value_node() {
    let expr = parse_select_expr("SELECT x IS FALSE;");
    assert!(
        matches!(
            expr,
            vibesql_ast::Expression::IsTruthValue {
                truth_value: vibesql_ast::TruthValue::False,
                negated: false,
                ..
            }
        ),
        "x IS FALSE should be IsTruthValue(False): {:?}",
        expr
    );
}

#[test]
fn test_plain_is_not_true_still_truth_value_node() {
    let expr = parse_select_expr("SELECT x IS NOT TRUE;");
    assert!(
        matches!(
            expr,
            vibesql_ast::Expression::IsTruthValue {
                truth_value: vibesql_ast::TruthValue::True,
                negated: true,
                ..
            }
        ),
        "x IS NOT TRUE should be IsTruthValue(True, negated): {:?}",
        expr
    );
}

#[test]
fn test_plain_is_unknown_still_truth_value_node() {
    // UNKNOWN is not a literal in the expression grammar; the fixed IS-form
    // must keep mapping to IsTruthValue(Unknown).
    let expr = parse_select_expr("SELECT x IS UNKNOWN;");
    assert!(
        matches!(
            expr,
            vibesql_ast::Expression::IsTruthValue {
                truth_value: vibesql_ast::TruthValue::Unknown,
                negated: false,
                ..
            }
        ),
        "x IS UNKNOWN should be IsTruthValue(Unknown): {:?}",
        expr
    );
}

#[test]
fn test_plain_is_literal_int_still_distinct_from() {
    // sqlite3: SELECT 1 IS 1; -- 1. `IS <int-literal>` was already IsDistinctFrom.
    let expr = parse_select_expr("SELECT 1 IS 1;");
    let (_left, right) = unwrap_is_distinct_from(expr, true, "1 IS 1");
    assert!(
        matches!(right, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        "right operand should be integer 1: {:?}",
        right
    );
}
