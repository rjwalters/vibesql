use super::super::*;
use crate::token::MultiCharOperator;

// ============================================================================

#[test]
fn test_tokenize_semicolon() {
    let mut lexer = Lexer::new(";");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Semicolon);
}

#[test]
fn test_tokenize_comma() {
    let mut lexer = Lexer::new(",");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Comma);
}

#[test]
fn test_tokenize_parentheses() {
    let mut lexer = Lexer::new("()");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::LParen);
    assert_eq!(tokens[1], Token::RParen);
}

#[test]
fn test_tokenize_arithmetic_symbols() {
    let mut lexer = Lexer::new("+ - * /");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('+'));
    assert_eq!(tokens[1], Token::Symbol('-'));
    assert_eq!(tokens[2], Token::Symbol('*'));
    assert_eq!(tokens[3], Token::Symbol('/'));
}

#[test]
fn test_tokenize_comparison_symbols() {
    let mut lexer = Lexer::new("= < >");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('='));
    assert_eq!(tokens[1], Token::Symbol('<'));
    assert_eq!(tokens[2], Token::Symbol('>'));
}

#[test]
fn test_tokenize_multi_char_operators() {
    let mut lexer = Lexer::new("<= >= != <>");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Operator(MultiCharOperator::LessEqual));
    assert_eq!(tokens[1], Token::Operator(MultiCharOperator::GreaterEqual));
    assert_eq!(tokens[2], Token::Operator(MultiCharOperator::NotEqual));
    assert_eq!(tokens[3], Token::Operator(MultiCharOperator::NotEqualAlt));
}

#[test]
fn test_tokenize_operators_without_spaces() {
    // Test that >= is tokenized as one operator, not two
    let mut lexer = Lexer::new("age>=18");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("AGE".to_string()));
    assert_eq!(tokens[1], Token::Operator(MultiCharOperator::GreaterEqual));
    assert_eq!(tokens[2], Token::Number("18".to_string()));
}

#[test]
fn test_tokenize_single_vs_multi_char() {
    // Test that > and = are separate when not adjacent
    let mut lexer = Lexer::new("> =");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('>'));
    assert_eq!(tokens[1], Token::Symbol('='));

    // But >= is one token when adjacent
    let mut lexer2 = Lexer::new(">=");
    let tokens2 = lexer2.tokenize().unwrap();
    assert_eq!(tokens2[0], Token::Operator(MultiCharOperator::GreaterEqual));
}

#[test]
fn test_tokenize_session_variable() {
    let mut lexer = Lexer::new("@@sql_mode");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::SessionVariable("sql_mode".to_string()));
}

#[test]
fn test_tokenize_session_variable_with_scope() {
    let mut lexer = Lexer::new("@@global.variable");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::SessionVariable("global.variable".to_string()));
}

#[test]
fn test_tokenize_session_variable_explicit_scope() {
    let mut lexer = Lexer::new("@@session.sql_mode");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::SessionVariable("session.sql_mode".to_string()));
}

#[test]
fn test_tokenize_user_variable() {
    let mut lexer = Lexer::new("@user_var");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::UserVariable("user_var".to_string()));
}

#[test]
fn test_tokenize_session_variable_in_expression() {
    let mut lexer =
        Lexer::new("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))");
    let tokens = lexer.tokenize().unwrap();
    // Find the SessionVariable token
    let found = tokens.iter().any(|t| matches!(t, Token::SessionVariable(s) if s == "sql_mode"));
    assert!(found, "Session variable @@sql_mode not found in tokens");
}

// ============================================================================
