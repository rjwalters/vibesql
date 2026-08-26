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
// Multi-byte UTF-8 Placeholder/Variable Tests (issue #5240)
//
// SQLite applies its IdChar rule (any byte >= 0x80 is an identifier char) to
// variable/placeholder names too: `$tՑ`, `:tՑ`, `@tՑ`, `$Ց`, `:Ց`, `@Ց`,
// and `$::Ց` are each a single variable token.
// ============================================================================

#[test]
fn test_tokenize_dollar_placeholder_multibyte_middle() {
    // SQLite: one variable `$tՑ` — must not split into `$t` + identifier `Ց`
    let mut lexer = Lexer::new("SELECT $tՑ");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("tՑ".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_dollar_placeholder_multibyte_leading() {
    // SQLite: one variable `$Ց` — previously a lexer error in VibeSQL
    let mut lexer = Lexer::new("SELECT $Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("Ց".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_dollar_placeholder_4byte_utf8() {
    // 4-byte UTF-8 sequence (U+1F600) inside a placeholder name
    let mut lexer = Lexer::new("SELECT $t😀");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("t😀".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_colon_placeholder_multibyte_middle() {
    // SQLite: one variable `:tՑ`; also covers non-ASCII at end of input
    // with no trailing whitespace/semicolon
    let mut lexer = Lexer::new("SELECT :tՑ");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("tՑ".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_colon_placeholder_multibyte_leading() {
    // SQLite: one variable `:Ց` — previously Symbol(':') + identifier
    let mut lexer = Lexer::new("SELECT :Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("Ց".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_user_variable_multibyte_middle() {
    // SQLite: one variable `@tՑ` — must not split into `@t` + identifier `Ց`
    let mut lexer = Lexer::new("SELECT @tՑ");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::UserVariable("tՑ".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_user_variable_multibyte_leading() {
    // SQLite: one variable `@Ց` — previously "empty variable name" error
    let mut lexer = Lexer::new("SELECT @Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::UserVariable("Ց".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_tcl_global_placeholder_multibyte() {
    // SQLite: one variable `$::Ց` (TCL global namespace syntax)
    let mut lexer = Lexer::new("SELECT $::Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("::Ց".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_tcl_global_placeholder_multibyte_namespaced() {
    // Mixed namespace path with a non-ASCII trailing component
    let mut lexer = Lexer::new("SELECT $::ns::Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("::ns::Ց".to_string()));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_question_placeholder_not_extended_by_multibyte() {
    // SQLite: `?Ց` is the anonymous `?` placeholder followed by identifier
    // `Ց` (implicit alias) — `?` names are numeric-only, so no change here
    let mut lexer = Lexer::new("SELECT ?Ց");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::Placeholder);
    assert_eq!(tokens[2], Token::Identifier("Ց".to_string()));
    assert_eq!(tokens[3], Token::Eof);
}

#[test]
fn test_tokenize_ascii_placeholders_unchanged() {
    // ASCII placeholder lexing must be unaffected
    let mut lexer = Lexer::new("SELECT $name, :name, @name, $1, ?");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NamedPlaceholder("name".to_string()));
    assert_eq!(tokens[3], Token::NamedPlaceholder("name".to_string()));
    assert_eq!(tokens[5], Token::UserVariable("name".to_string()));
    assert_eq!(tokens[7], Token::NumberedPlaceholder(1));
    assert_eq!(tokens[9], Token::Placeholder);
}

// ============================================================================
// SQLite ?NNN Numbered Placeholder Tests (issue #5283)
// ============================================================================

#[test]
fn test_tokenize_question_numbered_placeholder_single_digit() {
    // SQLite ?NNN syntax: `?1` is a numbered placeholder, not `?` followed by 1
    let mut lexer = Lexer::new("SELECT ?1");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NumberedPlaceholder(1));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_question_numbered_placeholder_multi_digit() {
    let mut lexer = Lexer::new("SELECT ?23");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NumberedPlaceholder(23));
    assert_eq!(tokens[2], Token::Eof);
}

#[test]
fn test_tokenize_question_numbered_placeholder_in_expression() {
    // upsert1-1210: `b+?1` must lex as Identifier, '+', NumberedPlaceholder(1)
    let mut lexer = Lexer::new("b+?1");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("b".to_string()));
    assert_eq!(tokens[1], Token::Symbol('+'));
    assert_eq!(tokens[2], Token::NumberedPlaceholder(1));
    assert_eq!(tokens[3], Token::Eof);
}

#[test]
fn test_tokenize_question_zero_rejected() {
    // SQLite rejects ?0 with the verbatim diagnostic (not a `near "…": syntax
    // error` wrapping): "variable number must be between ?1 and ?999".
    let mut lexer = Lexer::new("SELECT ?0");
    let err = lexer.tokenize().unwrap_err();
    assert_eq!(err.message, "variable number must be between ?1 and ?999");
    // near_token must be unset so Display emits the message verbatim.
    assert_eq!(err.to_string(), "variable number must be between ?1 and ?999");
}

#[test]
fn test_tokenize_question_over_limit_rejected() {
    // ?1000 exceeds SQLITE_MAX_VARIABLE_NUMBER (999) and is rejected with the
    // same range diagnostic (SQLite e_expr-11.1.3).
    let mut lexer = Lexer::new("SELECT ?1000");
    let err = lexer.tokenize().unwrap_err();
    assert_eq!(err.to_string(), "variable number must be between ?1 and ?999");
}

#[test]
fn test_tokenize_question_max_allowed() {
    // ?999 is exactly at the limit and must lex successfully.
    let mut lexer = Lexer::new("SELECT ?999");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::NumberedPlaceholder(999));
}

#[test]
fn test_tokenize_question_overflow_rejected() {
    // A parameter number too large to represent yields the same range error
    // rather than panicking or wrapping (SQLite e_expr-11.1.5..13).
    let mut lexer = Lexer::new("SELECT ?12345678903456789034567890234567890");
    let err = lexer.tokenize().unwrap_err();
    assert_eq!(err.to_string(), "variable number must be between ?1 and ?999");
}

#[test]
fn test_tokenize_bare_question_placeholder_unchanged() {
    // Bare `?` (no trailing digits) still lexes as the anonymous placeholder
    let mut lexer = Lexer::new("SELECT ?, ? + 1");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::Placeholder);
    assert_eq!(tokens[3], Token::Placeholder);
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
fn test_tokenize_empty_double_quoted_identifier_allowed() {
    // SQLite's tokenizer does not reject a zero-length double-quoted
    // identifier — it lexes to a TK_ID token naming the empty string, which
    // ordinary column resolution then fails ("no such column"), but the
    // token itself is valid (quote.test 2.2/3.4: `t1("w"||"")`).
    let mut lexer = Lexer::new(r#""""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier(String::new()));
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

// ============================================================================
// to_sql round-trip for delimited identifiers with embedded quotes
// (triggerC-15.2, issue #6176)
// ============================================================================

/// `"""x2"""` lexes to the identifier `"x2"`, and `to_sql()` must re-emit it
/// with the embedded quotes doubled (`"""x2"""`), not the unlexable `""x2""`.
/// Trigger bodies are stored as reconstructed token text and re-parsed at fire
/// time, so a non-round-tripping to_sql broke any trigger touching such a
/// table (triggerC-15.2.1).
#[test]
fn test_delimited_identifier_with_embedded_quotes_roundtrips_through_to_sql() {
    let mut lexer = Lexer::new("SELECT * FROM \"\"\"x2\"\"\"");
    let tokens = lexer.tokenize().unwrap();
    let ident = &tokens[3];
    assert_eq!(*ident, Token::DelimitedIdentifier("\"x2\"".to_string()));

    // Re-emit as SQL and lex again: must produce the same identifier.
    let sql = ident.to_sql();
    assert_eq!(sql, "\"\"\"x2\"\"\"");
    let mut relexer = Lexer::new(&sql);
    let retokens = relexer.tokenize().unwrap();
    assert_eq!(retokens[0], Token::DelimitedIdentifier("\"x2\"".to_string()));
}
