//! Tests for string concatenation operator (||)

use crate::token::MultiCharOperator;
use crate::*;

#[test]
fn test_parse_concat_basic() {
    let sql = "SELECT first_name || ' ' || last_name FROM users";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => {
                    // Should be: (first_name || ' ') || last_name
                    match expr {
                        vibesql_ast::Expression::BinaryOp { op, .. } => {
                            assert_eq!(*op, vibesql_ast::BinaryOperator::Concat);
                        }
                        _ => panic!("Expected BinaryOp expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_concat_with_literals() {
    let sql = "SELECT 'Hello' || ' ' || 'World'";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_concat_precedence() {
    // || should have same precedence as + and -
    let sql = "SELECT a + b || c FROM t";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should parse as (a + b) || c
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    match expr {
                        vibesql_ast::Expression::BinaryOp { left, op, .. } => {
                            assert_eq!(*op, vibesql_ast::BinaryOperator::Concat);
                            // Left should be (a + b)
                            match &**left {
                                vibesql_ast::Expression::BinaryOp { op: inner_op, .. } => {
                                    assert_eq!(*inner_op, vibesql_ast::BinaryOperator::Plus);
                                }
                                _ => panic!("Expected BinaryOp for left side"),
                            }
                        }
                        _ => panic!("Expected BinaryOp expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_lex_concat_operator() {
    let mut lexer = Lexer::new("'a' || 'b'");
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(tokens.len(), 4); // 'a', ||, 'b', EOF
    assert_eq!(tokens[0], Token::String("a".to_string()));
    assert_eq!(tokens[1], Token::Operator(MultiCharOperator::Concat));
    assert_eq!(tokens[2], Token::String("b".to_string()));
    assert_eq!(tokens[3], Token::Eof);
}

#[test]
fn test_lex_single_pipe_error() {
    let mut lexer = Lexer::new("'a' | 'b'");
    let result = lexer.tokenize();

    assert!(result.is_err());
    match result {
        Err(LexerError { message, .. }) => {
            assert!(message.contains("did you mean '||'?"));
        }
        _ => panic!("Expected lexer error"),
    }
}
