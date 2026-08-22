use vibesql_ast::Expression;
use vibesql_types::SqlValue;

use super::*;

// ========================================================================
// LIMIT and OFFSET Tests
// ========================================================================

#[test]
fn test_parse_limit() {
    let result = Parser::parse_sql("SELECT * FROM users LIMIT 10;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(10))));
            assert_eq!(select.offset, None);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_offset() {
    let result = Parser::parse_sql("SELECT * FROM users OFFSET 5;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.limit, None);
            assert_eq!(select.offset, Some(Expression::Literal(SqlValue::Integer(5))));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_limit_and_offset() {
    let result = Parser::parse_sql("SELECT * FROM users LIMIT 10 OFFSET 5;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(10))));
            assert_eq!(select.offset, Some(Expression::Literal(SqlValue::Integer(5))));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_limit_with_where() {
    let result = Parser::parse_sql("SELECT name FROM users WHERE age > 18 LIMIT 5;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(5))));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_limit_with_order_by() {
    let result = Parser::parse_sql("SELECT * FROM users ORDER BY name ASC LIMIT 10;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.order_by.is_some());
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(10))));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_full_query_with_limit_offset() {
    let result = Parser::parse_sql(
        "SELECT name, age FROM users WHERE age >= 18 ORDER BY age DESC LIMIT 10 OFFSET 5;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            assert!(select.order_by.is_some());
            assert_eq!(select.limit, Some(Expression::Literal(SqlValue::Integer(10))));
            assert_eq!(select.offset, Some(Expression::Literal(SqlValue::Integer(5))));
        }
        _ => panic!("Expected SELECT"),
    }
}
