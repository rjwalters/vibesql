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
// BETWEEN/LIKE/GLOB operands parse at the shift tier (issue #5813)
//
// Per SQLite, everything tighter than the comparison tier is allowed in
// BETWEEN bounds and LIKE/GLOB patterns; only the boolean AND separating
// BETWEEN's low/high must not be consumed. The negated forms (NOT BETWEEN,
// NOT LIKE, NOT GLOB) already parsed at the shift tier, so the non-negated
// forms must match. All expected results below were verified against sqlite3.
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
