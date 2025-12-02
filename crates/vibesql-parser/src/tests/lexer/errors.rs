use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_invalid_character() {
    let lexer = Lexer::new("SELECT @");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.message.contains("Expected variable name after @"));
}
