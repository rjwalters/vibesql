use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_simple_identifier() {
    let lexer = Lexer::new("users");
    let stream = lexer.tokenize().unwrap();
    // Regular identifiers are normalized to uppercase
    match stream.tokens[0] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USERS"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_tokenize_identifier_with_underscore() {
    let lexer = Lexer::new("user_id");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "USER_ID"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_tokenize_identifier_with_numbers() {
    let lexer = Lexer::new("table123");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "TABLE123"),
        _ => panic!("Expected Identifier token"),
    }
}

#[test]
fn test_tokenize_identifier_starting_with_underscore() {
    let lexer = Lexer::new("_internal");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "_INTERNAL"),
        _ => panic!("Expected Identifier token"),
    }
}

// ============================================================================
// Delimited Identifier Tests
// ============================================================================

#[test]
fn test_tokenize_delimited_identifier_simple() {
    let lexer = Lexer::new(r#""columnName""#);
    let stream = lexer.tokenize().unwrap();
    // Delimited identifiers preserve case
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "columnName"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_delimited_identifier_uppercase() {
    let lexer = Lexer::new(r#""A""#);
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "A"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_delimited_identifier_lowercase() {
    let lexer = Lexer::new(r#""a""#);
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "a"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_delimited_identifier_with_spaces() {
    let lexer = Lexer::new(r#""First Name""#);
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "First Name"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_delimited_identifier_reserved_word() {
    let lexer = Lexer::new(r#""SELECT""#);
    let stream = lexer.tokenize().unwrap();
    // Reserved words can be used as delimited identifiers
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "SELECT"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_delimited_identifier_with_escaped_quotes() {
    let lexer = Lexer::new(r#""O""Reilly""#);
    let stream = lexer.tokenize().unwrap();
    // Doubled quotes become single quote in the identifier
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), r#"O"Reilly"#),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_empty_delimited_identifier_error() {
    let lexer = Lexer::new(r#""""#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Empty delimited identifier"));
}

#[test]
fn test_tokenize_unterminated_delimited_identifier_error() {
    let lexer = Lexer::new(r#""unterminated"#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Unterminated delimited identifier"));
}

#[test]
fn test_tokenize_mixed_identifiers() {
    let lexer = Lexer::new(r#"SELECT "columnName", regularColumn FROM table"#);
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "columnName"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
    assert_eq!(stream.tokens[2], Token::Comma);
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "REGULARCOLUMN"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Keyword(Keyword::From));
    assert_eq!(stream.tokens[5], Token::Keyword(Keyword::Table)); // "table" is a reserved keyword
}

// ============================================================================
// Backtick Identifier Tests (MySQL-style)
// ============================================================================

#[test]
fn test_tokenize_backtick_identifier_simple() {
    let lexer = Lexer::new("`columnName`");
    let stream = lexer.tokenize().unwrap();
    // Backtick identifiers preserve case
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "columnName"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_uppercase() {
    let lexer = Lexer::new("`A`");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "A"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_lowercase() {
    let lexer = Lexer::new("`a`");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "a"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_with_spaces() {
    let lexer = Lexer::new("`First Name`");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "First Name"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_with_special_chars() {
    let lexer = Lexer::new("`my-table`");
    let stream = lexer.tokenize().unwrap();
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "my-table"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_reserved_word() {
    let lexer = Lexer::new("`SELECT`");
    let stream = lexer.tokenize().unwrap();
    // Reserved words can be used as backtick identifiers
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "SELECT"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_identifier_with_escaped_backticks() {
    let lexer = Lexer::new("`O``Reilly`");
    let stream = lexer.tokenize().unwrap();
    // Doubled backticks become single backtick in the identifier
    match stream.tokens[0] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "O`Reilly"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_empty_backtick_identifier_error() {
    let lexer = Lexer::new("``");
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Empty delimited identifier"));
}

#[test]
fn test_tokenize_unterminated_backtick_identifier_error() {
    let lexer = Lexer::new("`unterminated");
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Unterminated delimited identifier"));
}

#[test]
fn test_tokenize_mixed_backtick_and_regular_identifiers() {
    let lexer = Lexer::new("SELECT `columnName`, regularColumn FROM `table_name`");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "columnName"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
    assert_eq!(stream.tokens[2], Token::Comma);
    match stream.tokens[3] {
        Token::Identifier(sym) => assert_eq!(stream.resolve(sym), "REGULARCOLUMN"),
        _ => panic!("Expected Identifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Keyword(Keyword::From));
    match stream.tokens[5] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "table_name"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
}

#[test]
fn test_tokenize_backtick_vs_doublequote_identifiers() {
    let lexer = Lexer::new("SELECT `backtick`, \"doublequote\" FROM table");
    let stream = lexer.tokenize().unwrap();
    assert_eq!(stream.tokens[0], Token::Keyword(Keyword::Select));
    match stream.tokens[1] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "backtick"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
    assert_eq!(stream.tokens[2], Token::Comma);
    match stream.tokens[3] {
        Token::DelimitedIdentifier(sym) => assert_eq!(stream.resolve(sym), "doublequote"),
        _ => panic!("Expected DelimitedIdentifier token"),
    }
    assert_eq!(stream.tokens[4], Token::Keyword(Keyword::From));
    assert_eq!(stream.tokens[5], Token::Keyword(Keyword::Table));
}

// ============================================================================
