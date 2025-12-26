//! Test for NOT ... IS NULL operator precedence
//! Related to issue #1710
//!
//! This test verifies that `NOT col IS NULL` is parsed as `NOT (col IS NULL)`
//! and not as `(NOT col) IS NULL`.

use vibesql_ast::{Expression, UnaryOperator};

use crate::Parser;

#[test]
fn test_not_is_null_precedence() {
    // Parse: SELECT * FROM t WHERE NOT col0 IS NULL
    let sql = "SELECT * FROM t WHERE NOT col0 IS NULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // The correct parse should be: NOT (col0 IS NULL)
    // Which is: UnaryOp(Not, IsNull(col0, negated: false))
    //
    // The incorrect parse would be: (NOT col0) IS NULL
    // Which is: IsNull(UnaryOp(Not, col0), negated: false)

    match &where_clause {
        Expression::UnaryOp { op, expr } => {
            assert_eq!(*op, UnaryOperator::Not, "Outer operator should be NOT");

            // Inner expression should be IS NULL
            match &**expr {
                Expression::IsNull { expr: inner_expr, negated } => {
                    assert!(!*negated, "IS NULL should not be negated");

                    // Innermost expression should be column reference
                    match &**inner_expr {
                        Expression::ColumnRef(col_id) => {
                            let table = col_id.table_canonical();
                            let column = col_id.column_canonical();
                            assert!(table.is_none());
                            assert_eq!(column, "col0");
                        }
                        _ => panic!("Expected column reference, got {:?}", inner_expr),
                    }
                }
                _ => panic!("Expected IS NULL expression, got {:?}", expr),
            }
        }
        Expression::IsNull { expr, negated } => {
            // This is the INCORRECT parse: (NOT col0) IS NULL
            panic!(
                "Incorrect parse! Got IS NULL {{ expr: {:?}, negated: {} }}\n\
                 This means it was parsed as (NOT col0) IS NULL instead of NOT (col0 IS NULL)",
                expr, negated
            );
        }
        _ => panic!("Expected UnaryOp or IsNull, got {:?}", where_clause),
    }
}

#[test]
fn test_is_not_null_parsing() {
    // Parse: SELECT * FROM t WHERE col0 IS NOT NULL
    let sql = "SELECT * FROM t WHERE col0 IS NOT NULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: IsNull(col0, negated: true)
    match &where_clause {
        Expression::IsNull { expr, negated } => {
            assert!(*negated, "IS NOT NULL should have negated=true");

            match &**expr {
                Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();
                    assert!(table.is_none());
                    assert_eq!(column, "col0");
                }
                _ => panic!("Expected column reference, got {:?}", expr),
            }
        }
        _ => panic!("Expected IsNull, got {:?}", where_clause),
    }
}

#[test]
fn test_not_null_is_null_parsing() {
    // Parse: SELECT * FROM t WHERE NOT (NULL) IS NULL
    let sql = "SELECT * FROM t WHERE NOT (NULL) IS NULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: NOT (NULL IS NULL)
    // Which is: UnaryOp(Not, IsNull(Literal(Null), negated: false))
    match &where_clause {
        Expression::UnaryOp { op, expr } => {
            assert_eq!(*op, UnaryOperator::Not);

            match &**expr {
                Expression::IsNull { expr: inner_expr, negated } => {
                    assert!(!*negated);

                    match &**inner_expr {
                        Expression::Literal(vibesql_types::SqlValue::Null) => {
                            // Correct!
                        }
                        _ => panic!("Expected NULL literal, got {:?}", inner_expr),
                    }
                }
                _ => panic!("Expected IS NULL, got {:?}", expr),
            }
        }
        _ => panic!("Expected UnaryOp(NOT), got {:?}", where_clause),
    }
}

/// Tests for SQLite ISNULL postfix operator
/// ISNULL is equivalent to IS NULL
#[test]
fn test_isnull_operator() {
    // Parse: SELECT * FROM t WHERE col0 ISNULL
    let sql = "SELECT * FROM t WHERE col0 ISNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: IsNull(col0, negated: false)
    match &where_clause {
        Expression::IsNull { expr, negated } => {
            assert!(!*negated, "ISNULL should have negated=false (equivalent to IS NULL)");

            match &**expr {
                Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();
                    assert!(table.is_none());
                    assert_eq!(column, "col0");
                }
                _ => panic!("Expected column reference, got {:?}", expr),
            }
        }
        _ => panic!("Expected IsNull, got {:?}", where_clause),
    }
}

/// Tests for SQLite NOTNULL postfix operator
/// NOTNULL is equivalent to IS NOT NULL
#[test]
fn test_notnull_operator() {
    // Parse: SELECT * FROM t WHERE col0 NOTNULL
    let sql = "SELECT * FROM t WHERE col0 NOTNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: IsNull(col0, negated: true)
    match &where_clause {
        Expression::IsNull { expr, negated } => {
            assert!(*negated, "NOTNULL should have negated=true (equivalent to IS NOT NULL)");

            match &**expr {
                Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();
                    assert!(table.is_none());
                    assert_eq!(column, "col0");
                }
                _ => panic!("Expected column reference, got {:?}", expr),
            }
        }
        _ => panic!("Expected IsNull, got {:?}", where_clause),
    }
}

/// Test ISNULL with complex expressions
#[test]
fn test_isnull_complex_expression() {
    // Parse: SELECT * FROM t WHERE (a + b) ISNULL
    let sql = "SELECT * FROM t WHERE (a + b) ISNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: IsNull((a + b), negated: false)
    match &where_clause {
        Expression::IsNull { expr, negated } => {
            assert!(!*negated, "ISNULL should have negated=false");

            // Inner should be a binary op (a + b)
            match &**expr {
                Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
                }
                _ => panic!("Expected BinaryOp, got {:?}", expr),
            }
        }
        _ => panic!("Expected IsNull, got {:?}", where_clause),
    }
}

/// Test NOT col ISNULL precedence
#[test]
fn test_not_isnull_precedence() {
    // Parse: SELECT * FROM t WHERE NOT col0 ISNULL
    // This should parse as: NOT (col0 ISNULL) = NOT (col0 IS NULL)
    let sql = "SELECT * FROM t WHERE NOT col0 ISNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // Should be: UnaryOp(Not, IsNull(col0, negated: false))
    match &where_clause {
        Expression::UnaryOp { op, expr } => {
            assert_eq!(*op, UnaryOperator::Not, "Outer operator should be NOT");

            match &**expr {
                Expression::IsNull { expr: inner_expr, negated } => {
                    assert!(!*negated, "ISNULL should not be negated");

                    match &**inner_expr {
                        Expression::ColumnRef(col_id) => {
                            let column = col_id.column_canonical();
                            assert_eq!(column, "col0");
                        }
                        _ => panic!("Expected column reference, got {:?}", inner_expr),
                    }
                }
                _ => panic!("Expected IS NULL expression, got {:?}", expr),
            }
        }
        _ => panic!("Expected UnaryOp(NOT), got {:?}", where_clause),
    }
}

/// Tests for SQLite "expr NOT NULL" postfix operator (without IS)
/// "expr NOT NULL" is equivalent to "expr IS NOT NULL"
#[test]
fn test_not_null_without_is() {
    // Parse: SELECT * FROM t WHERE col0 NOT NULL
    let sql = "SELECT * FROM t WHERE col0 NOT NULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: IsNull(col0, negated: true)
    match &where_clause {
        Expression::IsNull { expr, negated } => {
            assert!(*negated, "NOT NULL should have negated=true (equivalent to IS NOT NULL)");

            match &**expr {
                Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();
                    assert!(table.is_none());
                    assert_eq!(column, "col0");
                }
                _ => panic!("Expected column reference, got {:?}", expr),
            }
        }
        _ => panic!("Expected IsNull, got {:?}", where_clause),
    }
}

/// Test chained NOTNULL operators (issue #4713)
/// SQLite allows: `x NOTNULL NOTNULL` → `(x IS NOT NULL) IS NOT NULL`
#[test]
fn test_chained_notnull_operators() {
    // Parse: SELECT 1 NOTNULL NOTNULL
    let sql = "SELECT 1 NOTNULL NOTNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let expr = &select.select_list[0];
    match expr {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            // Should be: IsNull(IsNull(1, negated: true), negated: true)
            // i.e., (1 IS NOT NULL) IS NOT NULL
            match expr {
                Expression::IsNull { expr: inner, negated: outer_negated } => {
                    assert!(*outer_negated, "Outer NOTNULL should have negated=true");

                    match inner.as_ref() {
                        Expression::IsNull { expr: innermost, negated: inner_negated } => {
                            assert!(*inner_negated, "Inner NOTNULL should have negated=true");

                            match innermost.as_ref() {
                                Expression::Literal(vibesql_types::SqlValue::Integer(1)) => {
                                    // Correct!
                                }
                                _ => panic!("Expected literal 1, got {:?}", innermost),
                            }
                        }
                        _ => panic!("Expected inner IsNull, got {:?}", inner),
                    }
                }
                _ => panic!("Expected IsNull, got {:?}", expr),
            }
        }
        _ => panic!("Expected Expression column"),
    }
}

/// Test chained ISNULL operators
#[test]
fn test_chained_isnull_operators() {
    // Parse: SELECT 1 ISNULL ISNULL
    let sql = "SELECT 1 ISNULL ISNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let expr = &select.select_list[0];
    match expr {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            // Should be: IsNull(IsNull(1, negated: false), negated: false)
            match expr {
                Expression::IsNull { expr: inner, negated: outer_negated } => {
                    assert!(!*outer_negated, "Outer ISNULL should have negated=false");

                    match inner.as_ref() {
                        Expression::IsNull { expr: innermost, negated: inner_negated } => {
                            assert!(!*inner_negated, "Inner ISNULL should have negated=false");

                            match innermost.as_ref() {
                                Expression::Literal(vibesql_types::SqlValue::Integer(1)) => {
                                    // Correct!
                                }
                                _ => panic!("Expected literal 1, got {:?}", innermost),
                            }
                        }
                        _ => panic!("Expected inner IsNull, got {:?}", inner),
                    }
                }
                _ => panic!("Expected IsNull, got {:?}", expr),
            }
        }
        _ => panic!("Expected Expression column"),
    }
}

/// Test mixed NOTNULL and ISNULL chaining
#[test]
fn test_mixed_notnull_isnull_chaining() {
    // Parse: SELECT 1 NOTNULL ISNULL
    let sql = "SELECT 1 NOTNULL ISNULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let expr = &select.select_list[0];
    match expr {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            // Should be: IsNull(IsNull(1, negated: true), negated: false)
            // i.e., (1 IS NOT NULL) IS NULL
            match expr {
                Expression::IsNull { expr: inner, negated: outer_negated } => {
                    assert!(!*outer_negated, "Outer ISNULL should have negated=false");

                    match inner.as_ref() {
                        Expression::IsNull { expr: innermost, negated: inner_negated } => {
                            assert!(*inner_negated, "Inner NOTNULL should have negated=true");

                            match innermost.as_ref() {
                                Expression::Literal(vibesql_types::SqlValue::Integer(1)) => {
                                    // Correct!
                                }
                                _ => panic!("Expected literal 1, got {:?}", innermost),
                            }
                        }
                        _ => panic!("Expected inner IsNull, got {:?}", inner),
                    }
                }
                _ => panic!("Expected IsNull, got {:?}", expr),
            }
        }
        _ => panic!("Expected Expression column"),
    }
}

/// Test NOT NULL in complex WHERE clause
#[test]
fn test_not_null_in_complex_where() {
    // Parse: SELECT * FROM t WHERE a NOT NULL AND b NOT NULL
    let sql = "SELECT * FROM t WHERE a NOT NULL AND b NOT NULL";
    let stmt = Parser::parse_sql(sql).expect("Should parse");

    let select = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        _ => panic!("Expected SELECT statement"),
    };

    let where_clause = select.where_clause.expect("Should have WHERE clause");

    // This should parse as: (a IS NOT NULL) AND (b IS NOT NULL)
    match &where_clause {
        Expression::BinaryOp { op, left, right } => {
            assert_eq!(*op, vibesql_ast::BinaryOperator::And);

            // Left should be IsNull(a, negated: true)
            match &**left {
                Expression::IsNull { expr, negated } => {
                    assert!(*negated);
                    match &**expr {
                        Expression::ColumnRef(col_id) => {
                            assert_eq!(col_id.column_canonical(), "a");
                        }
                        _ => panic!("Expected column reference"),
                    }
                }
                _ => panic!("Expected IsNull on left"),
            }

            // Right should be IsNull(b, negated: true)
            match &**right {
                Expression::IsNull { expr, negated } => {
                    assert!(*negated);
                    match &**expr {
                        Expression::ColumnRef(col_id) => {
                            assert_eq!(col_id.column_canonical(), "b");
                        }
                        _ => panic!("Expected column reference"),
                    }
                }
                _ => panic!("Expected IsNull on right"),
            }
        }
        _ => panic!("Expected BinaryOp(AND), got {:?}", where_clause),
    }
}
