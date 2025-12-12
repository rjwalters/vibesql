use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_select_42() {
    let mut lexer = Lexer::new("SELECT 42;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 4); // SELECT, 42, ;, EOF
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Number("42".to_string()));
    assert_eq!(tokens[2], Token::Semicolon);
    assert_eq!(tokens[3], Token::Eof);
}

#[test]
fn test_tokenize_select_string() {
    let mut lexer = Lexer::new("SELECT 'hello';");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 4);
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::String("hello".to_string()));
    assert_eq!(tokens[2], Token::Semicolon);
}

#[test]
fn test_tokenize_select_with_arithmetic() {
    let mut lexer = Lexer::new("SELECT 1 + 2;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 6); // SELECT, 1, +, 2, ;, EOF
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Number("1".to_string()));
    assert_eq!(tokens[2], Token::Symbol('+'));
    assert_eq!(tokens[3], Token::Number("2".to_string()));
    assert_eq!(tokens[4], Token::Semicolon);
}

#[test]
fn test_tokenize_select_from_table() {
    let mut lexer = Lexer::new("SELECT * FROM users;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 6); // SELECT, *, FROM, users, ;, EOF
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Symbol('*'));
    assert_eq!(tokens[2], Token::Keyword(Keyword::From));
    assert_eq!(tokens[3], Token::Identifier("users".to_string()));
    assert_eq!(tokens[4], Token::Semicolon);
}

#[test]
fn test_tokenize_select_columns() {
    let mut lexer = Lexer::new("SELECT id, name, age FROM users;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Identifier("id".to_string()));
    assert_eq!(tokens[2], Token::Comma);
    assert_eq!(tokens[3], Token::Identifier("name".to_string()));
    assert_eq!(tokens[4], Token::Comma);
    assert_eq!(tokens[5], Token::Identifier("age".to_string()));
    assert_eq!(tokens[6], Token::Keyword(Keyword::From));
    assert_eq!(tokens[7], Token::Identifier("users".to_string()));
}

#[test]
fn test_tokenize_select_with_where() {
    let mut lexer = Lexer::new("SELECT name FROM users WHERE id = 1;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Identifier("name".to_string()));
    assert_eq!(tokens[2], Token::Keyword(Keyword::From));
    assert_eq!(tokens[3], Token::Identifier("users".to_string()));
    assert_eq!(tokens[4], Token::Keyword(Keyword::Where));
    assert_eq!(tokens[5], Token::Identifier("id".to_string()));
    assert_eq!(tokens[6], Token::Symbol('='));
    assert_eq!(tokens[7], Token::Number("1".to_string()));
    assert_eq!(tokens[8], Token::Semicolon);
}

// ============================================================================
