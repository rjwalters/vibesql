use crate::{keywords::Keyword, lexer::Lexer, token::Token};

#[test]
fn test_line_comment_simple() {
    let input = "-- This is a comment\nSELECT 1";
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 3);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "1"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Eof);
}

#[test]
fn test_line_comment_at_end() {
    let input = "SELECT 1 -- comment at end";
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 3);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "1"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Eof);
}

#[test]
fn test_multiple_line_comments() {
    let input = r#"-- First comment
-- Second comment
SELECT 1 -- inline comment
-- Final comment"#;
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 3);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "1"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Eof);
}

#[test]
fn test_comment_with_sql_keywords() {
    let input = "-- SELECT FROM WHERE\nSELECT * FROM users";
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 5);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(stream.tokens[1], Token::Symbol('*'));
    assert_eq!(stream.tokens[2], Token::Keyword(Keyword::From));
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USERS"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Eof);
}

#[test]
fn test_dash_vs_comment() {
    // Single dash should be tokenized as minus operator
    let input = "SELECT 5 - 3";
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 5);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "5"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Symbol('-'));
    match stream.tokens[3] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "3"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[4], Token::Eof);
}

#[test]
fn test_default_demo_sql() {
    let input = "-- Welcome to NIST MemSQL\n-- Use Ctrl/Cmd + Enter to execute the current query\nSELECT * FROM employees;";
    let lexer = Lexer::new(input);
    let stream = lexer.tokenize().unwrap();

    assert_eq!(stream.tokens.len(), 6);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(stream.tokens[1], Token::Symbol('*'));
    assert_eq!(stream.tokens[2], Token::Keyword(Keyword::From));
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "EMPLOYEES"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Semicolon);
    assert_eq!(stream.tokens[5], Token::Eof);
}
