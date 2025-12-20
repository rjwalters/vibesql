use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_simple_identifier() {
    let mut lexer = Lexer::new("users");
    let tokens = lexer.tokenize().unwrap();
    // Regular identifiers preserve original case
    assert_eq!(tokens[0], Token::Identifier("users".to_string()));
}

#[test]
fn test_tokenize_identifier_with_underscore() {
    let mut lexer = Lexer::new("user_id");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("user_id".to_string()));
}

#[test]
fn test_tokenize_identifier_with_numbers() {
    let mut lexer = Lexer::new("table123");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("table123".to_string()));
}

#[test]
fn test_tokenize_identifier_starting_with_underscore() {
    let mut lexer = Lexer::new("_internal");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("_internal".to_string()));
}

// ============================================================================
// Delimited Identifier Tests
// ============================================================================

#[test]
fn test_tokenize_delimited_identifier_simple() {
    let mut lexer = Lexer::new(r#""columnName""#);
    let tokens = lexer.tokenize().unwrap();
    // Delimited identifiers preserve case
    assert_eq!(tokens[0], Token::DelimitedIdentifier("columnName".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_uppercase() {
    let mut lexer = Lexer::new(r#""A""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("A".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_lowercase() {
    let mut lexer = Lexer::new(r#""a""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("a".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_with_spaces() {
    let mut lexer = Lexer::new(r#""First Name""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("First Name".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_reserved_word() {
    let mut lexer = Lexer::new(r#""SELECT""#);
    let tokens = lexer.tokenize().unwrap();
    // Reserved words can be used as delimited identifiers
    assert_eq!(tokens[0], Token::DelimitedIdentifier("SELECT".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_with_escaped_quotes() {
    let mut lexer = Lexer::new(r#""O""Reilly""#);
    let tokens = lexer.tokenize().unwrap();
    // Doubled quotes become single quote in the identifier
    assert_eq!(tokens[0], Token::DelimitedIdentifier(r#"O"Reilly"#.to_string()));
}

#[test]
fn test_tokenize_empty_delimited_identifier_error() {
    let mut lexer = Lexer::new(r#""""#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Empty delimited identifier"));
}

#[test]
fn test_tokenize_unterminated_delimited_identifier_error() {
    let mut lexer = Lexer::new(r#""unterminated"#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Unterminated delimited identifier"));
}

#[test]
fn test_tokenize_mixed_identifiers() {
    let mut lexer = Lexer::new(r#"SELECT "columnName", regularColumn FROM table"#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens[0],
        Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() }
    );
    assert_eq!(tokens[1], Token::DelimitedIdentifier("columnName".to_string()));
    assert_eq!(tokens[2], Token::Comma);
    // Regular identifiers preserve original case
    assert_eq!(tokens[3], Token::Identifier("regularColumn".to_string()));
    assert_eq!(tokens[4], Token::Keyword { keyword: Keyword::From, original: "FROM".to_string() });
    assert_eq!(
        tokens[5],
        Token::Keyword { keyword: Keyword::Table, original: "table".to_string() }
    ); // "table" is a reserved keyword
}

// ============================================================================
// Backtick Identifier Tests (MySQL-style)
// ============================================================================

#[test]
fn test_tokenize_backtick_identifier_simple() {
    let mut lexer = Lexer::new("`columnName`");
    let tokens = lexer.tokenize().unwrap();
    // Backtick identifiers preserve case
    assert_eq!(tokens[0], Token::DelimitedIdentifier("columnName".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_uppercase() {
    let mut lexer = Lexer::new("`A`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("A".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_lowercase() {
    let mut lexer = Lexer::new("`a`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("a".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_with_spaces() {
    let mut lexer = Lexer::new("`First Name`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("First Name".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_with_special_chars() {
    let mut lexer = Lexer::new("`my-table`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("my-table".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_reserved_word() {
    let mut lexer = Lexer::new("`SELECT`");
    let tokens = lexer.tokenize().unwrap();
    // Reserved words can be used as backtick identifiers
    assert_eq!(tokens[0], Token::DelimitedIdentifier("SELECT".to_string()));
}

#[test]
fn test_tokenize_backtick_identifier_with_escaped_backticks() {
    let mut lexer = Lexer::new("`O``Reilly`");
    let tokens = lexer.tokenize().unwrap();
    // Doubled backticks become single backtick in the identifier
    assert_eq!(tokens[0], Token::DelimitedIdentifier("O`Reilly".to_string()));
}

#[test]
fn test_tokenize_empty_backtick_identifier_error() {
    let mut lexer = Lexer::new("``");
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Empty delimited identifier"));
}

#[test]
fn test_tokenize_unterminated_backtick_identifier_error() {
    let mut lexer = Lexer::new("`unterminated");
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Unterminated delimited identifier"));
}

#[test]
fn test_tokenize_mixed_backtick_and_regular_identifiers() {
    let mut lexer = Lexer::new("SELECT `columnName`, regularColumn FROM `table_name`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens[0],
        Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() }
    );
    assert_eq!(tokens[1], Token::DelimitedIdentifier("columnName".to_string()));
    assert_eq!(tokens[2], Token::Comma);
    // Regular identifiers preserve original case
    assert_eq!(tokens[3], Token::Identifier("regularColumn".to_string()));
    assert_eq!(tokens[4], Token::Keyword { keyword: Keyword::From, original: "FROM".to_string() });
    assert_eq!(tokens[5], Token::DelimitedIdentifier("table_name".to_string()));
}

#[test]
fn test_tokenize_backtick_vs_doublequote_identifiers() {
    let mut lexer = Lexer::new("SELECT `backtick`, \"doublequote\" FROM table");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens[0],
        Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() }
    );
    assert_eq!(tokens[1], Token::DelimitedIdentifier("backtick".to_string()));
    assert_eq!(tokens[2], Token::Comma);
    assert_eq!(tokens[3], Token::DelimitedIdentifier("doublequote".to_string()));
    assert_eq!(tokens[4], Token::Keyword { keyword: Keyword::From, original: "FROM".to_string() });
    assert_eq!(
        tokens[5],
        Token::Keyword { keyword: Keyword::Table, original: "table".to_string() }
    );
}

// ============================================================================
