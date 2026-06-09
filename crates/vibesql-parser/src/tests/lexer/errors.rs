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
    // `$` followed by a non-placeholder char takes the extract_error_token
    // path; a multi-byte char right after the `$` must not cause a
    // non-char-boundary slice panic (issue #5236)
    let mut lexer = Lexer::new("SELECT $Ց");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.near_token.as_deref(), Some("$"));
}
