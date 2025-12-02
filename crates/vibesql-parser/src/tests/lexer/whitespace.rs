use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_with_multiple_spaces() {
    let lexer = Lexer::new("SELECT    42");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 3); // SELECT, 42, EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "42"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_with_tabs() {
    let lexer = Lexer::new("SELECT\t42");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 3);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "42"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_with_newlines() {
    let lexer = Lexer::new("SELECT\n42");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 3);
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "42"),
        _ => panic!("Expected Number token"),
    }
}

// ============================================================================
