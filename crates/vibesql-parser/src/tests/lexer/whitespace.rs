use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_with_multiple_spaces() {
    let mut lexer = Lexer::new("SELECT    42");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 3); // SELECT, 42, EOF
    assert_eq!(tokens[0], Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() });
    assert_eq!(tokens[1], Token::Number("42".to_string()));
}

#[test]
fn test_tokenize_with_tabs() {
    let mut lexer = Lexer::new("SELECT\t42");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() });
    assert_eq!(tokens[1], Token::Number("42".to_string()));
}

#[test]
fn test_tokenize_with_newlines() {
    let mut lexer = Lexer::new("SELECT\n42");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 3);
    assert_eq!(tokens[0], Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() });
    assert_eq!(tokens[1], Token::Number("42".to_string()));
}

// ============================================================================
