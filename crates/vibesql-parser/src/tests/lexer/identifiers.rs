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
// Multi-byte UTF-8 Identifier Tests (issue #5236)
//
// SQLite treats any byte >= 0x80 as an identifier character (its IdChar
// macro), so identifiers may contain arbitrary non-ASCII characters.
// ============================================================================

#[test]
fn test_tokenize_multibyte_identifier_middle() {
    // Fuzzer reproducer: `t1Ցam` used to panic on a non-char-boundary slice.
    // It must lex as ONE identifier (matching SQLite), not split at `Ց`.
    let mut lexer = Lexer::new("t1Ցam");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("t1Ցam".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_multibyte_identifier_leading() {
    // A leading multi-byte char starts an identifier
    let mut lexer = Lexer::new("Ցam");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("Ցam".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_multibyte_identifier_at_end_of_input() {
    // Multi-byte char as the final bytes of input, no trailing whitespace
    let mut lexer = Lexer::new("t1Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("t1Ց".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_identifier_with_4byte_utf8() {
    // 4-byte UTF-8 sequence (U+1F600) inside an identifier
    let mut lexer = Lexer::new("table😀x");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("table😀x".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_mixed_ascii_multibyte_identifier() {
    let mut lexer = Lexer::new("tableՑ_2");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("tableՑ_2".to_string()));
    assert_eq!(tokens[1], Token::Eof);
}

#[test]
fn test_tokenize_multibyte_identifier_stack_buffer_path() {
    // Exactly 32 bytes (16 x 2-byte chars): exercises the stack-buffer
    // keyword-lookup path with non-ASCII bytes (must not corrupt UTF-8)
    let ident = "Ց".repeat(16);
    assert_eq!(ident.len(), 32);
    let mut lexer = Lexer::new(&ident);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier(ident.clone()));
}

#[test]
fn test_tokenize_long_multibyte_identifier_heap_fallback() {
    // > 32 bytes: exercises the heap-allocation keyword-lookup fallback
    let ident = format!("col_{}", "Ց".repeat(20)); // 4 + 40 = 44 bytes
    let mut lexer = Lexer::new(&ident);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier(ident.clone()));
}

#[test]
fn test_tokenize_select_with_multibyte_identifier() {
    // Full fuzzer reproducer statement: must tokenize without panicking,
    // with `t1Ցam` as a single identifier (SQLite reports
    // "no such column: t1Ցam" — a semantic error, not a parse error)
    let mut lexer = Lexer::new("SELECT t1Ցam;");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens[0],
        Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() }
    );
    assert_eq!(tokens[1], Token::Identifier("t1Ցam".to_string()));
    assert_eq!(tokens[2], Token::Semicolon);
}

#[test]
fn test_tokenize_create_table_with_multibyte_identifier() {
    // `CREATE TABLE tՑ(x INT)` is valid in SQLite
    let mut lexer = Lexer::new("CREATE TABLE tՑ(x INT)");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens[0],
        Token::Keyword { keyword: Keyword::Create, original: "CREATE".to_string() }
    );
    assert_eq!(
        tokens[1],
        Token::Keyword { keyword: Keyword::Table, original: "TABLE".to_string() }
    );
    assert_eq!(tokens[2], Token::Identifier("tՑ".to_string()));
    assert_eq!(tokens[3], Token::LParen);
}

#[test]
fn test_keyword_matching_unchanged_for_ascii() {
    // ASCII keyword recognition must be unaffected by non-ASCII support
    let mut lexer = Lexer::new("select Select SELECT");
    let tokens = lexer.tokenize().unwrap();
    for (i, original) in ["select", "Select", "SELECT"].iter().enumerate() {
        assert_eq!(
            tokens[i],
            Token::Keyword { keyword: Keyword::Select, original: original.to_string() }
        );
    }
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
