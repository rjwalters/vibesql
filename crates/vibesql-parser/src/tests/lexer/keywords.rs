use super::super::*;

// ============================================================================
// ============================================================================

#[test]
fn test_tokenize_select_keyword() {
    let lexer = Lexer::new("SELECT");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 2); // SELECT + EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(stream.tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_select_lowercase() {
    let lexer = Lexer::new("select");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
}

#[test]
fn test_tokenize_select_mixed_case() {
    let lexer = Lexer::new("SeLeCt");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
}

#[test]
fn test_tokenize_from_keyword() {
    let lexer = Lexer::new("FROM");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::From));
}

#[test]
fn test_tokenize_where_keyword() {
    let lexer = Lexer::new("WHERE");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Where));
}

#[test]
fn test_tokenize_multiple_keywords() {
    let lexer = Lexer::new("SELECT FROM WHERE");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens.len(), 4); // 3 keywords + EOF
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(stream.tokens[1], Token::Keyword(Keyword::From));
    assert_eq!(stream.tokens[2], Token::Keyword(Keyword::Where));
    assert_eq!(stream.tokens[3], Token::Eof);
}

// ============================================================================
