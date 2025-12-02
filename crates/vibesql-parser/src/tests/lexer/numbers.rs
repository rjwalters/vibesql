use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_integer() {
    let lexer = Lexer::new("42");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "42"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_decimal() {
    let lexer = Lexer::new("3.14");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "3.14"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_zero() {
    let lexer = Lexer::new("0");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "0"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_large_number() {
    let lexer = Lexer::new("999999");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "999999"),
        _ => panic!("Expected Number token"),
    }
}

// ============================================================================
