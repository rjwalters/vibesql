use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_invalid_character() {
    let mut lexer = Lexer::new("SELECT @");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.message.contains("Expected variable name after @"));
}

#[test]
fn test_error_token_with_multibyte_char_does_not_panic() {
    // Historically (issue #5236) `SELECT $Ց` hit the extract_error_token path
    // and could panic on a non-char-boundary slice. Since issue #5240,
    // non-ASCII chars after `$` start a named variable (SQLite IdChar), so
    // this input must lex cleanly as a single placeholder — and must still
    // never panic.
    let mut lexer = Lexer::new("SELECT $Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("Ց".to_string()));
}
