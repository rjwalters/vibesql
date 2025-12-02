use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_single_quoted_string() {
    let lexer = Lexer::new("'hello'");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "hello"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_double_quoted_string() {
    // Double quotes now create delimited identifiers, not strings (SQL:1999 compliance)
    let lexer = Lexer::new("\"world\"");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "world"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_empty_string() {
    let lexer = Lexer::new("''");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), ""),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_string_with_spaces() {
    let lexer = Lexer::new("'hello world'");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "hello world"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_unterminated_string() {
    let lexer = Lexer::new("'hello");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.message, "Unterminated string literal");
}

#[test]
fn test_tokenize_string_with_escaped_quote() {
    let lexer = Lexer::new("'O''Reilly'");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "O'Reilly"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_string_with_multiple_escaped_quotes() {
    let lexer = Lexer::new("'Chef Anton''s Cajun Seasoning'");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "Chef Anton's Cajun Seasoning"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_string_with_double_escaped_quote() {
    let lexer = Lexer::new("'It''s ''great'''");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), "It's 'great'"),
        _ => panic!("Expected String token"),
    }
}

#[test]
fn test_tokenize_empty_string_not_confused_with_escaped_quote() {
    let lexer = Lexer::new("''");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::String(sym) => assert_eq!(stream.resolve(sym), ""),
        _ => panic!("Expected String token"),
    }
}

// ============================================================================
