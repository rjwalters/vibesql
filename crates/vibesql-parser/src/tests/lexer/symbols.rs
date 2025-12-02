use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_semicolon() {
    let lexer = Lexer::new(";");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Semicolon);
}

#[test]
fn test_tokenize_comma() {
    let lexer = Lexer::new(",");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Comma);
}

#[test]
fn test_tokenize_parentheses() {
    let lexer = Lexer::new("()");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::LParen);
    assert_eq!(stream.tokens[1], Token::RParen);
}

#[test]
fn test_tokenize_arithmetic_symbols() {
    let lexer = Lexer::new("+ - * /");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Symbol('+'));
    assert_eq!(stream.tokens[1], Token::Symbol('-'));
    assert_eq!(stream.tokens[2], Token::Symbol('*'));
    assert_eq!(stream.tokens[3], Token::Symbol('/'));
}

#[test]
fn test_tokenize_comparison_symbols() {
    let lexer = Lexer::new("= < >");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Symbol('='));
    assert_eq!(stream.tokens[1], Token::Symbol('<'));
    assert_eq!(stream.tokens[2], Token::Symbol('>'));
}

#[test]
fn test_tokenize_multi_char_operators() {
    let lexer = Lexer::new("<= >= != <>");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Operator(sym) => assert_eq!(stream.resolve(sym), "<="),
        _ => panic!("Expected Operator token"),
    }
    match stream.tokens[1] {
        Token::Operator(sym) => assert_eq!(stream.resolve(sym), ">="),
        _ => panic!("Expected Operator token"),
    }
    match stream.tokens[2] {
        Token::Operator(sym) => assert_eq!(stream.resolve(sym), "!="),
        _ => panic!("Expected Operator token"),
    }
    match stream.tokens[3] {
        Token::Operator(sym) => assert_eq!(stream.resolve(sym), "<>"),
        _ => panic!("Expected Operator token"),
    }
}

#[test]
fn test_tokenize_operators_without_spaces() {
    // Test that >= is tokenized as one operator, not two
    let lexer = Lexer::new("age>=18");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "AGE"),
        _ => panic!("Expected Identifier token"),
    }
    match stream.tokens[1] {
        Token::Operator(sym) => assert_eq!(stream.resolve(sym), ">="),
        _ => panic!("Expected Operator token"),
    }
    match stream.tokens[2] {
        Token::Number(sym) => assert_eq!(stream.resolve(sym), "18"),
        _ => panic!("Expected Number token"),
    }
}

#[test]
fn test_tokenize_single_vs_multi_char() {
    // Test that > and = are separate when not adjacent
    let lexer = Lexer::new("> =");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Symbol('>'));
    assert_eq!(stream.tokens[1], Token::Symbol('='));

    // But >= is one token when adjacent
    let lexer2 = Lexer::new(">=");
    let stream2 = lexer2.tokenize().unwrap();
    match stream2.tokens[0] {
        Token::Operator(sym) => assert_eq!(stream2.resolve(sym), ">="),
        _ => panic!("Expected Operator token"),
    }
}

#[test]
fn test_tokenize_session_variable() {
    let lexer = Lexer::new("@@sql_mode");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::SessionVariable(sym) => assert_eq!(stream.resolve(sym), "sql_mode"),
        _ => panic!("Expected SessionVariable token"),
    }
}

#[test]
fn test_tokenize_session_variable_with_scope() {
    let lexer = Lexer::new("@@global.variable");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::SessionVariable(sym) => assert_eq!(stream.resolve(sym), "global.variable"),
        _ => panic!("Expected SessionVariable token"),
    }
}

#[test]
fn test_tokenize_session_variable_explicit_scope() {
    let lexer = Lexer::new("@@session.sql_mode");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::SessionVariable(sym) => assert_eq!(stream.resolve(sym), "session.sql_mode"),
        _ => panic!("Expected SessionVariable token"),
    }
}

#[test]
fn test_tokenize_user_variable() {
    let lexer = Lexer::new("@user_var");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::UserVariable(sym) => assert_eq!(stream.resolve(sym), "user_var"),
        _ => panic!("Expected UserVariable token"),
    }
}

#[test]
fn test_tokenize_session_variable_in_expression() {
    let lexer = Lexer::new("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))");
    let stream = lexer.tokenize().unwrap();
    // Find the SessionVariable token
    let found = stream.tokens.iter().any(|t| {
        matches!(t, Token::SessionVariable(sym) if stream.resolve(*sym) == "sql_mode")
    });
    assert!(found, "Session variable @@sql_mode not found in tokens");
}

// ============================================================================
