use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_select_42() {
    let lexer = Lexer::new("SELECT 42;");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 4); // SELECT, 42, ;, EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "42"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Semicolon);
    assert_eq!(stream.tokens[3], Token::Eof);
}

#[test]
fn test_tokenize_select_string() {
    let lexer = Lexer::new("SELECT 'hello';");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 4);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "hello"),
        _ => panic!("Expected String token"),
    }
    assert_eq!(stream.tokens[2], Token::Semicolon);
}

#[test]
fn test_tokenize_select_with_arithmetic() {
    let lexer = Lexer::new("SELECT 1 + 2;");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 6); // SELECT, 1, +, 2, ;, EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "1"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[2], Token::Symbol('+'));
    match stream.tokens[3] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "2"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[4], Token::Semicolon);
}

#[test]
fn test_tokenize_select_from_table() {
    let lexer = Lexer::new("SELECT * FROM users;");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 6); // SELECT, *, FROM, users, ;, EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(stream.tokens[1], Token::Symbol('*'));
    assert_eq!(stream.tokens[2], Token::Keyword(Keyword::From));
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USERS"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Semicolon);
}

#[test]
fn test_tokenize_select_columns() {
    let lexer = Lexer::new("SELECT id, name, age FROM users;");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "ID"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[2], Token::Comma);
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "NAME"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Comma);
    match stream.tokens[5] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "AGE"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[6], Token::Keyword(Keyword::From));
    match stream.tokens[7] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USERS"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_tokenize_select_with_where() {
    let lexer = Lexer::new("SELECT name FROM users WHERE id = 1;");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "NAME"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[2], Token::Keyword(Keyword::From));
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USERS"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Keyword(Keyword::Where));
    match stream.tokens[5] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "ID"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[6], Token::Symbol('='));
    match stream.tokens[7] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "1"),
        _ => panic!("Expected Number token"),
    }
    assert_eq!(stream.tokens[8], Token::Semicolon);
}

// ============================================================================
