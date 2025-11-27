//! Test for NOT ... IS NULL operator precedence
//! Related to issue #1710
//!
//! This test verifies that `NOT col IS NULL` is parsed as `NOT (col IS NULL)`
//! and not as `(NOT col) IS NULL`.

use crate::Parser;
use vibesql_ast::{Expression, UnaryOperator};

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
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(*table, None);
                            assert_eq!(column.to_uppercase(), "COL0");
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
                Expression::ColumnRef { table, column } => {
                    assert_eq!(*table, None);
                    assert_eq!(column.to_uppercase(), "COL0");
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
