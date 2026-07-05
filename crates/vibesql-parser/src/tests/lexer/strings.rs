use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_single_quoted_string() {
    let mut lexer = Lexer::new("'hello'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("hello".to_string()));
}

#[test]
fn test_tokenize_double_quoted_string() {
    // Double quotes now create delimited identifiers, not strings (SQL:1999 compliance)
    let mut lexer = Lexer::new("\"world\"");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("world".to_string()));
}

#[test]
fn test_tokenize_empty_string() {
    let mut lexer = Lexer::new("''");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("".to_string()));
}

#[test]
fn test_tokenize_string_with_spaces() {
    let mut lexer = Lexer::new("'hello world'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("hello world".to_string()));
}

#[test]
fn test_tokenize_unterminated_string() {
    let mut lexer = Lexer::new("'hello");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.message, "Unterminated string literal");
}

#[test]
fn test_tokenize_string_with_escaped_quote() {
    let mut lexer = Lexer::new("'O''Reilly'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("O'Reilly".to_string()));
}

#[test]
fn test_tokenize_string_with_multiple_escaped_quotes() {
    let mut lexer = Lexer::new("'Chef Anton''s Cajun Seasoning'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("Chef Anton's Cajun Seasoning".to_string()));
}

#[test]
fn test_tokenize_string_with_double_escaped_quote() {
    let mut lexer = Lexer::new("'It''s ''great'''");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("It's 'great'".to_string()));
}

#[test]
fn test_tokenize_empty_string_not_confused_with_escaped_quote() {
    let mut lexer = Lexer::new("''");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("".to_string()));
}

// ============================================================================

#[test]
fn test_tokenize_blob_literal() {
    let mut lexer = Lexer::new("x'4869'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::BlobLiteral(vec![0x48, 0x69]));
}

#[test]
fn test_unterminated_blob_literal_errors() {
    // Missing closing quote (EOF reached first) must be an error, not a valid
    // partial blob (SQLite: `unrecognized token: "x'4869"`).
    for input in ["x'4869", "X'ABCD", "x'"] {
        let mut lexer = Lexer::new(input);
        assert!(lexer.tokenize().is_err(), "Expected error for input: {}", input);
    }
}
