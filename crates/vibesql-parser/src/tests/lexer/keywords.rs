use super::super::*;

// ============================================================================
// ============================================================================

#[test]
fn test_tokenize_select_keyword() {
    let mut lexer = Lexer::new("select");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 2); // SELECT + EOF
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_select_lowercase() {
    let mut lexer = Lexer::new("select");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
}

#[test]
fn test_tokenize_select_mixed_case() {
    let mut lexer = Lexer::new("SeLeCt");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
}

#[test]
fn test_tokenize_from_keyword() {
    let mut lexer = Lexer::new("from");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::From));
}

#[test]
fn test_tokenize_where_keyword() {
    let mut lexer = Lexer::new("where");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Where));
}

#[test]
fn test_tokenize_multiple_keywords() {
    let mut lexer = Lexer::new("SELECT FROM WHERE");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens.len(), 4); // 3 keywords + EOF
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::Keyword(Keyword::From));
    assert_eq!(tokens[2], Token::Keyword(Keyword::Where));
    assert_eq!(tokens[3], Token::Eof);
}

// ============================================================================
