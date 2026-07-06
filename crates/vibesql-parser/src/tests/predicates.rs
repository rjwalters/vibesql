//! Tests for SQL predicates (BETWEEN, LIKE, etc.)

use super::*;

// ============================================================================
// SQLite compatibility: == operator
// ============================================================================

#[test]
fn test_double_equals_sqlite_compat() {
    // SQLite uses == as a synonym for = (equality)
    let sql = "SELECT * FROM users WHERE id == 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's an equality comparison
        match where_expr {
            vibesql_ast::Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, vibesql_ast::BinaryOperator::Equal, "== should parse as Equal");

                // Check left is 'id'
                match *left {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "id");
                    }
                    _ => panic!("Expected ColumnRef for left"),
                }

                // Check right is 1
                match *right {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 1);
                    }
                    _ => panic!("Expected Integer literal for right"),
                }
            }
            _ => panic!("Expected BinaryOp expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_double_equals_in_expression() {
    // Test == in more complex expressions
    let sql = "SELECT a, b FROM t WHERE a == b AND c == 'test'";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        // If it parses without error, the == operator is working
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_integer() {
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        // Check WHERE clause contains BETWEEN
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's a BETWEEN expression
        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'age'
                match *expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let table = col_id.table_canonical();
                        let column = col_id.column_canonical();
                        assert_eq!(table, None);
                        assert_eq!(column, "age");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 18
                match *low {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 18);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 65
                match *high {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 65);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression, got {:?}", where_expr),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between() {
    let sql = "SELECT * FROM products WHERE price NOT BETWEEN 10.0 AND 20.0";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low: _, high: _, negated, symmetric } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'price'
                match *expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let table = col_id.table_canonical();
                        let column = col_id.column_canonical();
                        assert_eq!(table, None);
                        assert_eq!(column, "price");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_with_expressions() {
    let sql = "SELECT * FROM orders WHERE total BETWEEN price * 0.9 AND price * 1.1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated);
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Verify expr is 'total'
                match *expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "total");
                    }
                    _ => panic!("Expected ColumnRef"),
                }

                // Verify low and high are multiplication expressions
                match *low {
                    vibesql_ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for low"),
                }

                match *high {
                    vibesql_ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, vibesql_ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_with_column_references() {
    let sql = "SELECT * FROM data WHERE value BETWEEN min_val AND max_val";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { low, high, .. } => {
                // Verify low is min_val
                match *low {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "min_val");
                    }
                    _ => panic!("Expected ColumnRef for low"),
                }

                // Verify high is max_val
                match *high {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "max_val");
                    }
                    _ => panic!("Expected ColumnRef for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_asymmetric_explicit() {
    let sql = "SELECT * FROM t WHERE x BETWEEN ASYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x BETWEEN SYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");

                // Check expr is 'x'
                match *expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "x");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 1
                match *low {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 1);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 5
                match *high {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 5);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN SYMMETRIC 10 AND 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_asymmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN ASYMMETRIC 1 AND 10";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let vibesql_ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            vibesql_ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

// ============================================================================
// BETWEEN/LIKE/GLOB operands parse at the bitwise tier (issues #5813, #5848)
//
// Per SQLite, everything tighter than the comparison tier is allowed in
// BETWEEN bounds and LIKE/GLOB patterns; only the boolean AND separating
// BETWEEN's low/high must not be consumed. Since #5848 merged the shift and
// bitwise tiers, `<<`/`>>` (below) and `&`/`|` (see the bitwise section) are
// all admitted; the negated and non-negated forms must match. All expected
// results below were verified against sqlite3.
// ============================================================================

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

fn assert_left_shift(expr: &vibesql_ast::Expression, context: &str) {
    assert!(
        matches!(
            expr,
            vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::LeftShift, .. }
        ),
        "{} should be a << BinaryOp, got {:?}",
        context,
        expr
    );
}

#[test]
fn test_between_shift_in_high_bound() {
    // sqlite3: SELECT 1 BETWEEN 0 AND 1<<2; -- 1  (high bound is 1<<2 = 4)
    let expr = parse_select_expr("SELECT 1 BETWEEN 0 AND 1<<2;");
    match expr {
        vibesql_ast::Expression::Between { low, high, negated, .. } => {
            assert!(!negated);
            assert!(matches!(
                *low,
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))
            ));
            assert_left_shift(&high, "BETWEEN high bound");
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_between_shift_in_low_bound() {
    // sqlite3: SELECT 8 BETWEEN 1<<1 AND 10; -- 1  (low bound is 1<<1 = 2)
    let expr = parse_select_expr("SELECT 8 BETWEEN 1<<1 AND 10;");
    match expr {
        vibesql_ast::Expression::Between { low, high, negated, .. } => {
            assert!(!negated);
            assert_left_shift(&low, "BETWEEN low bound");
            assert!(matches!(
                *high,
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))
            ));
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_between_right_shift_in_low_bound() {
    // sqlite3: SELECT 5 BETWEEN 16>>2 AND 6; -- 1  (low bound is 16>>2 = 4)
    let expr = parse_select_expr("SELECT 5 BETWEEN 16>>2 AND 6;");
    match expr {
        vibesql_ast::Expression::Between { low, negated, .. } => {
            assert!(!negated);
            assert!(
                matches!(
                    *low,
                    vibesql_ast::Expression::BinaryOp {
                        op: vibesql_ast::BinaryOperator::RightShift,
                        ..
                    }
                ),
                "BETWEEN low bound should be a >> BinaryOp, got {:?}",
                low
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_like_shift_pattern() {
    // sqlite3: SELECT 2 LIKE 1<<1; -- 1  (pattern is 1<<1 = 2)
    let expr = parse_select_expr("SELECT 2 LIKE 1<<1;");
    match expr {
        vibesql_ast::Expression::Like { pattern, negated, escape, .. } => {
            assert!(!negated);
            assert!(escape.is_none());
            assert_left_shift(&pattern, "LIKE pattern");
        }
        other => panic!("expected Like expression, got {:?}", other),
    }
}

#[test]
fn test_like_escape_shift_parses() {
    // Parse-level only: sqlite3 accepts this syntactically (it fails later at
    // runtime with "ESCAPE expression must be a single character").
    let expr = parse_select_expr("SELECT 'a' LIKE 'b' ESCAPE 1<<1;");
    match expr {
        vibesql_ast::Expression::Like { escape, negated, .. } => {
            assert!(!negated);
            let escape = escape.expect("ESCAPE clause should be present");
            assert_left_shift(&escape, "LIKE ESCAPE expression");
        }
        other => panic!("expected Like expression, got {:?}", other),
    }
}

#[test]
fn test_glob_shift_pattern() {
    // sqlite3: SELECT 2 GLOB 1<<1; -- 1  (pattern is 1<<1 = 2)
    let expr = parse_select_expr("SELECT 2 GLOB 1<<1;");
    match expr {
        vibesql_ast::Expression::Glob { pattern, negated, escape, .. } => {
            assert!(!negated);
            assert!(escape.is_none());
            assert_left_shift(&pattern, "GLOB pattern");
        }
        other => panic!("expected Glob expression, got {:?}", other),
    }
}

#[test]
fn test_glob_escape_shift_parses() {
    // Parse-level only, mirroring the LIKE ESCAPE case.
    let expr = parse_select_expr("SELECT 'a' GLOB 'b' ESCAPE 1<<1;");
    match expr {
        vibesql_ast::Expression::Glob { escape, negated, .. } => {
            assert!(!negated);
            let escape = escape.expect("ESCAPE clause should be present");
            assert_left_shift(&escape, "GLOB ESCAPE expression");
        }
        other => panic!("expected Glob expression, got {:?}", other),
    }
}

// ----------------------------------------------------------------------------
// Symmetry: the negated and non-negated forms must produce identically shaped
// ASTs, differing only in the `negated` flag.
// ----------------------------------------------------------------------------

#[test]
fn test_between_negated_symmetry_with_shift_bounds() {
    // sqlite3: SELECT 1 BETWEEN 0 AND 1<<2;     -- 1
    // sqlite3: SELECT 1 NOT BETWEEN 0 AND 1<<2; -- 0
    let plain = parse_select_expr("SELECT 1 BETWEEN 0 AND 1<<2;");
    let negated = parse_select_expr("SELECT 1 NOT BETWEEN 0 AND 1<<2;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Between {
                expr: e1,
                low: l1,
                high: h1,
                negated: n1,
                symmetric: s1,
            },
            vibesql_ast::Expression::Between {
                expr: e2,
                low: l2,
                high: h2,
                negated: n2,
                symmetric: s2,
            },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(e1, e2, "BETWEEN subject must parse identically in both forms");
            assert_eq!(l1, l2, "BETWEEN low bound must parse identically in both forms");
            assert_eq!(h1, h2, "BETWEEN high bound must parse identically in both forms");
            assert_eq!(s1, s2);
        }
        other => panic!("expected two Between expressions, got {:?}", other),
    }
}

#[test]
fn test_like_negated_symmetry_with_shift_pattern() {
    // sqlite3: SELECT 2 LIKE 1<<1;     -- 1
    // sqlite3: SELECT 2 NOT LIKE 1<<1; -- 0
    let plain = parse_select_expr("SELECT 2 LIKE 1<<1;");
    let negated = parse_select_expr("SELECT 2 NOT LIKE 1<<1;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Like { expr: e1, pattern: p1, negated: n1, escape: esc1 },
            vibesql_ast::Expression::Like { expr: e2, pattern: p2, negated: n2, escape: esc2 },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(e1, e2, "LIKE subject must parse identically in both forms");
            assert_eq!(p1, p2, "LIKE pattern must parse identically in both forms");
            assert_eq!(esc1, esc2);
        }
        other => panic!("expected two Like expressions, got {:?}", other),
    }
}

#[test]
fn test_glob_negated_symmetry_with_shift_pattern() {
    // sqlite3: SELECT 2 GLOB 1<<1;     -- 1
    // sqlite3: SELECT 2 NOT GLOB 1<<1; -- 0
    let plain = parse_select_expr("SELECT 2 GLOB 1<<1;");
    let negated = parse_select_expr("SELECT 2 NOT GLOB 1<<1;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Glob { expr: e1, pattern: p1, negated: n1, escape: esc1 },
            vibesql_ast::Expression::Glob { expr: e2, pattern: p2, negated: n2, escape: esc2 },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(e1, e2, "GLOB subject must parse identically in both forms");
            assert_eq!(p1, p2, "GLOB pattern must parse identically in both forms");
            assert_eq!(esc1, esc2);
        }
        other => panic!("expected two Glob expressions, got {:?}", other),
    }
}

#[test]
fn test_between_shift_bound_does_not_consume_boolean_and() {
    // The AND separating low/high must still terminate the low bound, and a
    // following boolean AND must still terminate the whole BETWEEN.
    // sqlite3: SELECT 1 BETWEEN 0 AND 1<<2 AND 2 BETWEEN 1<<0 AND 3; -- 1
    let expr = parse_select_expr("SELECT 1 BETWEEN 0 AND 1<<2 AND 2 BETWEEN 1<<0 AND 3;");
    match expr {
        vibesql_ast::Expression::BinaryOp { op, left, right } => {
            assert_eq!(op, vibesql_ast::BinaryOperator::And);
            assert!(
                matches!(*left, vibesql_ast::Expression::Between { negated: false, .. }),
                "left of AND should be a Between, got {:?}",
                left
            );
            assert!(
                matches!(*right, vibesql_ast::Expression::Between { negated: false, .. }),
                "right of AND should be a Between, got {:?}",
                right
            );
        }
        other => panic!("expected boolean AND of two Between expressions, got {:?}", other),
    }
}

// ============================================================================
// Bitwise operators bind tighter than comparison (issues #5848, #5851 Part A)
//
// Per SQLite (https://sqlite.org/lang_expr.html#operators), `|`, `&`, `<<`,
// `>>` form a single flat left-to-right tier that binds TIGHTER than the
// comparison operators (`= < > <= >= != IS IN LIKE GLOB BETWEEN ...`). There
// is no C-style sub-ordering: `5 | 3 & 1` parses as `((5|3)&1)`, not
// `(5|(3&1))`. Every expected result below was verified against sqlite3 3.51.0.
// ============================================================================

fn bin_op(expr: &vibesql_ast::Expression) -> vibesql_ast::BinaryOperator {
    match expr {
        vibesql_ast::Expression::BinaryOp { op, .. } => *op,
        other => panic!("expected BinaryOp, got {:?}", other),
    }
}

/// Assert `expr` is `BinaryOp { op }` and return its (left, right) children.
fn expect_bin<'a>(
    expr: &'a vibesql_ast::Expression,
    op: vibesql_ast::BinaryOperator,
    ctx: &str,
) -> (&'a vibesql_ast::Expression, &'a vibesql_ast::Expression) {
    match expr {
        vibesql_ast::Expression::BinaryOp { op: got, left, right } => {
            assert_eq!(*got, op, "{}: expected {:?}, got {:?}", ctx, op, got);
            (left, right)
        }
        other => panic!("{}: expected BinaryOp {:?}, got {:?}", ctx, op, other),
    }
}

#[test]
fn test_equal_binds_looser_than_bitwise_and_rhs() {
    // sqlite3: SELECT 2 = 2 & 2; -- 1, i.e. 2 = (2 & 2)
    let expr = parse_select_expr("SELECT 2 = 2 & 2;");
    let (_, right) = expect_bin(&expr, vibesql_ast::BinaryOperator::Equal, "top");
    assert_eq!(
        bin_op(right),
        vibesql_ast::BinaryOperator::BitwiseAnd,
        "RHS of = should be (2 & 2), got {:?}",
        right
    );
}

#[test]
fn test_equal_binds_looser_than_bitwise_or_lhs() {
    // sqlite3: SELECT 2 | 3 = 3; -- 1, i.e. (2 | 3) = 3
    let expr = parse_select_expr("SELECT 2 | 3 = 3;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::Equal, "top");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::BitwiseOr,
        "LHS of = should be (2 | 3), got {:?}",
        left
    );
}

#[test]
fn test_less_than_binds_looser_than_bitwise_and() {
    // sqlite3: SELECT 5 & 3 > 1; -- 0, i.e. (5 & 3) > 1
    let expr = parse_select_expr("SELECT 5 & 3 > 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::GreaterThan, "top");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::BitwiseAnd,
        "LHS of > should be (5 & 3), got {:?}",
        left
    );
}

#[test]
fn test_bitwise_or_and_are_flat_left_to_right() {
    // sqlite3: SELECT 5 | 3 & 1; -- 1, i.e. ((5 | 3) & 1), NOT (5 | (3 & 1)).
    let expr = parse_select_expr("SELECT 5 | 3 & 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::BitwiseAnd, "top");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::BitwiseOr,
        "flat L-to-R: left of & should be (5 | 3), got {:?}",
        left
    );
}

#[test]
fn test_shift_is_same_tier_as_bitwise_and() {
    // sqlite3: SELECT 5 & 3 << 1; -- 2, i.e. ((5 & 3) << 1) — shift NOT tighter than &.
    let expr = parse_select_expr("SELECT 5 & 3 << 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::LeftShift, "top");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::BitwiseAnd,
        "left of << should be (5 & 3), got {:?}",
        left
    );
}

#[test]
fn test_shift_is_same_tier_as_bitwise_or() {
    // sqlite3: SELECT 5 | 3 << 1; -- 14, i.e. ((5 | 3) << 1).
    let expr = parse_select_expr("SELECT 5 | 3 << 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::LeftShift, "top");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::BitwiseOr,
        "left of << should be (5 | 3), got {:?}",
        left
    );
}

#[test]
fn test_multi_op_bitwise_chain_flat() {
    // sqlite3: SELECT 1 | 6 & 4 << 1; -- 8, i.e. (((1 | 6) & 4) << 1).
    let expr = parse_select_expr("SELECT 1 | 6 & 4 << 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::LeftShift, "top <<");
    let (inner_left, _) = expect_bin(left, vibesql_ast::BinaryOperator::BitwiseAnd, "inner &");
    assert_eq!(
        bin_op(inner_left),
        vibesql_ast::BinaryOperator::BitwiseOr,
        "innermost should be (1 | 6), got {:?}",
        inner_left
    );
}

#[test]
fn test_bitwise_binds_looser_than_additive() {
    // sqlite3: SELECT 4 & 6 + 1; -- 4, i.e. (4 & (6 + 1)) — `+` tighter than `&`.
    let expr = parse_select_expr("SELECT 4 & 6 + 1;");
    let (_, right) = expect_bin(&expr, vibesql_ast::BinaryOperator::BitwiseAnd, "top &");
    assert_eq!(
        bin_op(right),
        vibesql_ast::BinaryOperator::Plus,
        "RHS of & should be (6 + 1), got {:?}",
        right
    );
}

#[test]
fn test_bitwise_binds_looser_than_concat() {
    // sqlite3: SELECT 1 | 2 || 3; -- '23', i.e. (1 | (2 || 3)) — `||` binds
    // TIGHTER than `|` (concat tier is tighter than the bitwise tier), so the
    // RHS of `|` is the concatenation `2 || 3` = '23', and `1 | 23` = 23.
    let expr = parse_select_expr("SELECT 1 | 2 || 3;");
    let (_, right) = expect_bin(&expr, vibesql_ast::BinaryOperator::BitwiseOr, "top |");
    assert_eq!(
        bin_op(right),
        vibesql_ast::BinaryOperator::Concat,
        "RHS of | should be (2 || 3), got {:?}",
        right
    );
}

#[test]
fn test_in_list_followed_by_bitwise_and() {
    // sqlite3: SELECT 1 IN (1) & 3; -- 1, i.e. (1 IN (1)) & 3.
    // continue_higher_precedence_ops must climb the new bitwise tier.
    let expr = parse_select_expr("SELECT 1 IN (1) & 3;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::BitwiseAnd, "top &");
    assert!(
        matches!(left, vibesql_ast::Expression::InList { .. }),
        "LHS of & should be an IN list, got {:?}",
        left
    );
}

#[test]
fn test_isnull_postfix_followed_by_bitwise_and() {
    // sqlite3: SELECT 1 ISNULL & 3; -- 0, i.e. (1 ISNULL) & 3.
    let expr = parse_select_expr("SELECT 1 ISNULL & 3;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::BitwiseAnd, "top &");
    assert!(
        matches!(left, vibesql_ast::Expression::IsNull { negated: false, .. }),
        "LHS of & should be (1 ISNULL), got {:?}",
        left
    );
}

// ----------------------------------------------------------------------------
// Part A of #5851: `& | << >>` now admitted in BETWEEN bounds / LIKE-GLOB
// patterns (bounds parse at the bitwise tier). Values verified against sqlite3.
// ----------------------------------------------------------------------------

#[test]
fn test_between_bitwise_and_in_high_bound() {
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 & 1; -- 0  (high bound is 3 & 1 = 1)
    let expr = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 & 1;");
    match expr {
        vibesql_ast::Expression::Between { high, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&high),
                vibesql_ast::BinaryOperator::BitwiseAnd,
                "high bound should be (3 & 1), got {:?}",
                high
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_between_bitwise_or_in_low_bound() {
    // sqlite3: SELECT 7 BETWEEN 5 | 2 AND 10; -- 1  (low bound is 5 | 2 = 7)
    let expr = parse_select_expr("SELECT 7 BETWEEN 5 | 2 AND 10;");
    match expr {
        vibesql_ast::Expression::Between { low, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&low),
                vibesql_ast::BinaryOperator::BitwiseOr,
                "low bound should be (5 | 2), got {:?}",
                low
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_between_bitwise_bound_negated_symmetry() {
    // The negated and non-negated forms must produce identically shaped bounds.
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 & 1;     -- 0
    // sqlite3: SELECT 2 NOT BETWEEN 0 AND 3 & 1; -- 1
    let plain = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 & 1;");
    let negated = parse_select_expr("SELECT 2 NOT BETWEEN 0 AND 3 & 1;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Between { high: h1, negated: n1, .. },
            vibesql_ast::Expression::Between { high: h2, negated: n2, .. },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(h1, h2, "BETWEEN high bound must parse identically in both forms");
        }
        other => panic!("expected two Between expressions, got {:?}", other),
    }
}

#[test]
fn test_like_bitwise_and_pattern() {
    // sqlite3: SELECT 2 LIKE 1 & 3; -- 0  (pattern is 1 & 3 = 1)
    let expr = parse_select_expr("SELECT 2 LIKE 1 & 3;");
    match expr {
        vibesql_ast::Expression::Like { pattern, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&pattern),
                vibesql_ast::BinaryOperator::BitwiseAnd,
                "LIKE pattern should be (1 & 3), got {:?}",
                pattern
            );
        }
        other => panic!("expected Like expression, got {:?}", other),
    }
}

// ============================================================================
// Part B of #5851: relational operators (`< <= > >=`) form a tier that binds
// TIGHTER than the equality/predicate tier (`= == != <> IS IN LIKE GLOB
// BETWEEN`) but LOOSER than the bitwise tier. So BETWEEN bounds and LIKE/GLOB
// patterns parse at the relational tier, while `=`/`==`/IS reduce left. Every
// expected value below was verified against sqlite3 3.51.0.
// ============================================================================

#[test]
fn test_relational_lt_in_between_high_bound() {
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 < 3; -- 0  (high bound is 3 < 3 = 0)
    let expr = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 < 3;");
    match expr {
        vibesql_ast::Expression::Between { high, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&high),
                vibesql_ast::BinaryOperator::LessThan,
                "high bound should be (3 < 3), got {:?}",
                high
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_relational_le_in_between_high_bound() {
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 <= 3; -- 0  (high bound is 3 <= 3 = 1)
    let expr = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 <= 3;");
    match expr {
        vibesql_ast::Expression::Between { high, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&high),
                vibesql_ast::BinaryOperator::LessThanOrEqual,
                "high bound should be (3 <= 3), got {:?}",
                high
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_relational_gt_in_between_low_bound() {
    // sqlite3: SELECT 2 BETWEEN 1 > 0 AND 3; -- 1  (low bound is 1 > 0 = 1)
    let expr = parse_select_expr("SELECT 2 BETWEEN 1 > 0 AND 3;");
    match expr {
        vibesql_ast::Expression::Between { low, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&low),
                vibesql_ast::BinaryOperator::GreaterThan,
                "low bound should be (1 > 0), got {:?}",
                low
            );
        }
        other => panic!("expected Between expression, got {:?}", other),
    }
}

#[test]
fn test_relational_lt_in_like_pattern() {
    // sqlite3: SELECT 2 LIKE 1 < 3; -- 0  (pattern is 1 < 3 = 1)
    let expr = parse_select_expr("SELECT 2 LIKE 1 < 3;");
    match expr {
        vibesql_ast::Expression::Like { pattern, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&pattern),
                vibesql_ast::BinaryOperator::LessThan,
                "LIKE pattern should be (1 < 3), got {:?}",
                pattern
            );
        }
        other => panic!("expected Like expression, got {:?}", other),
    }
}

#[test]
fn test_relational_gt_in_like_pattern() {
    // sqlite3: SELECT 2 LIKE 1 > 3; -- 0  (pattern is 1 > 3 = 0)
    let expr = parse_select_expr("SELECT 2 LIKE 1 > 3;");
    match expr {
        vibesql_ast::Expression::Like { pattern, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&pattern),
                vibesql_ast::BinaryOperator::GreaterThan,
                "LIKE pattern should be (1 > 3), got {:?}",
                pattern
            );
        }
        other => panic!("expected Like expression, got {:?}", other),
    }
}

#[test]
fn test_relational_in_glob_pattern() {
    // sqlite3: SELECT 2 GLOB 1 < 3; -- 0  (pattern is 1 < 3 = 1)
    let expr = parse_select_expr("SELECT 2 GLOB 1 < 3;");
    match expr {
        vibesql_ast::Expression::Glob { pattern, negated, .. } => {
            assert!(!negated);
            assert_eq!(
                bin_op(&pattern),
                vibesql_ast::BinaryOperator::LessThan,
                "GLOB pattern should be (1 < 3), got {:?}",
                pattern
            );
        }
        other => panic!("expected Glob expression, got {:?}", other),
    }
}

#[test]
fn test_equality_reduces_left_over_between() {
    // Equality binds LOOSER than the relational tier and reduces left, so the
    // trailing `= 1` takes the whole BETWEEN as its left operand (unchanged).
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 = 1; -- 1, i.e. (2 BETWEEN 0 AND 3) = 1
    let expr = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 = 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::Equal, "top =");
    assert!(
        matches!(left, vibesql_ast::Expression::Between { negated: false, .. }),
        "LHS of = should be the BETWEEN node, got {:?}",
        left
    );
}

#[test]
fn test_relational_binds_tighter_than_equality_lhs() {
    // sqlite3: SELECT 1 < 2 = 1; -- 1, i.e. (1 < 2) = 1 (relational reduces first)
    let expr = parse_select_expr("SELECT 1 < 2 = 1;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::Equal, "top =");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::LessThan,
        "LHS of = should be (1 < 2), got {:?}",
        left
    );
}

#[test]
fn test_relational_binds_tighter_than_equality_rhs() {
    // sqlite3: SELECT 1 = 2 < 3; -- 1, i.e. 1 = (2 < 3) (relational reduces first)
    let expr = parse_select_expr("SELECT 1 = 2 < 3;");
    let (_, right) = expect_bin(&expr, vibesql_ast::BinaryOperator::Equal, "top =");
    assert_eq!(
        bin_op(right),
        vibesql_ast::BinaryOperator::LessThan,
        "RHS of = should be (2 < 3), got {:?}",
        right
    );
}

#[test]
fn test_chained_relational_is_left_associative() {
    // sqlite3: SELECT 1 < 2 < 3; -- 1, i.e. ((1 < 2) < 3), NOT (1 < (2 < 3)).
    let expr = parse_select_expr("SELECT 1 < 2 < 3;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::LessThan, "top <");
    assert_eq!(
        bin_op(left),
        vibesql_ast::BinaryOperator::LessThan,
        "left of < should be (1 < 2), got {:?}",
        left
    );
}

#[test]
fn test_in_list_followed_by_relational() {
    // sqlite3: SELECT 1 IN (1) < 3; -- 1, i.e. (1 IN (1)) < 3.
    // continue_higher_precedence_ops must climb the relational tier.
    let expr = parse_select_expr("SELECT 1 IN (1) < 3;");
    let (left, _) = expect_bin(&expr, vibesql_ast::BinaryOperator::LessThan, "top <");
    assert!(
        matches!(left, vibesql_ast::Expression::InList { .. }),
        "LHS of < should be an IN list, got {:?}",
        left
    );
}

// ----------------------------------------------------------------------------
// Symmetry: negated and non-negated forms must produce identically shaped
// ASTs with relational bounds/patterns, differing only in the `negated` flag.
// ----------------------------------------------------------------------------

#[test]
fn test_between_relational_bound_negated_symmetry() {
    // sqlite3: SELECT 2 BETWEEN 0 AND 3 < 3;     -- 0
    // sqlite3: SELECT 2 NOT BETWEEN 0 AND 3 < 3; -- 1
    let plain = parse_select_expr("SELECT 2 BETWEEN 0 AND 3 < 3;");
    let negated = parse_select_expr("SELECT 2 NOT BETWEEN 0 AND 3 < 3;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Between { low: l1, high: h1, negated: n1, .. },
            vibesql_ast::Expression::Between { low: l2, high: h2, negated: n2, .. },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(l1, l2, "BETWEEN low bound must parse identically in both forms");
            assert_eq!(h1, h2, "BETWEEN high bound must parse identically in both forms");
        }
        other => panic!("expected two Between expressions, got {:?}", other),
    }
}

#[test]
fn test_like_relational_pattern_negated_symmetry() {
    // sqlite3: SELECT 2 LIKE 1 < 3;     -- 0
    // sqlite3: SELECT 2 NOT LIKE 1 < 3; -- 1
    let plain = parse_select_expr("SELECT 2 LIKE 1 < 3;");
    let negated = parse_select_expr("SELECT 2 NOT LIKE 1 < 3;");
    match (plain, negated) {
        (
            vibesql_ast::Expression::Like { pattern: p1, negated: n1, .. },
            vibesql_ast::Expression::Like { pattern: p2, negated: n2, .. },
        ) => {
            assert!(!n1);
            assert!(n2);
            assert_eq!(p1, p2, "LIKE pattern must parse identically in both forms");
        }
        other => panic!("expected two Like expressions, got {:?}", other),
    }
}
