use crate::{keywords::Keyword, lexer::Lexer, token::Token};

#[test]
fn test_line_comment_simple() {
    let input = "-- This is a comment\nSELECT 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![Token::Keyword(Keyword::Select), Token::Number("1".to_string()), Token::Eof,]
    );
}

#[test]
fn test_line_comment_at_end() {
    let input = "SELECT 1 -- comment at end";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![Token::Keyword(Keyword::Select), Token::Number("1".to_string()), Token::Eof,]
    );
}

#[test]
fn test_multiple_line_comments() {
    let input = r#"-- First comment
-- Second comment
SELECT 1 -- inline comment
-- Final comment"#;
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![Token::Keyword(Keyword::Select), Token::Number("1".to_string()), Token::Eof,]
    );
}

#[test]
fn test_comment_with_sql_keywords() {
    let input = "-- SELECT FROM WHERE\nSELECT * FROM users";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Symbol('*'),
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_dash_vs_comment() {
    // Single dash should be tokenized as minus operator
    let input = "SELECT 5 - 3";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Number("5".to_string()),
            Token::Symbol('-'),
            Token::Number("3".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_default_demo_sql() {
    let input = "-- Welcome to NIST MemSQL\n-- Use Ctrl/Cmd + Enter to execute the current query\nSELECT * FROM employees;";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Symbol('*'),
            Token::Keyword(Keyword::From),
            Token::Identifier("employees".to_string()),
            Token::Semicolon,
            Token::Eof,
        ]
    );
}
