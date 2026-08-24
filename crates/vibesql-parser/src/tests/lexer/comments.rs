use crate::{keywords::Keyword, lexer::Lexer, parser::Parser, token::Token};

#[test]
fn test_line_comment_simple() {
    let input = "-- This is a comment\nSELECT 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_line_comment_at_end() {
    let input = "SELECT 1 -- comment at end";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_multiple_line_comments() {
    let input = r#"-- First comment
-- Second comment
SELECT 1 -- inline comment
-- Final comment"#;
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_comment_with_sql_keywords() {
    let input = "-- SELECT FROM WHERE\nSELECT * FROM users";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Symbol('*'),
            Token::Keyword { keyword: Keyword::From, original: "FROM".to_string() },
            Token::Identifier("users".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_dash_vs_comment() {
    // Single dash should be tokenized as minus operator
    let input = "SELECT 5 - 3";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("5".to_string()),
            Token::Symbol('-'),
            Token::Number("3".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_leading() {
    // A block comment before a statement should be skipped entirely.
    let input = "/* leading comment */ SELECT 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_mid_statement() {
    // A block comment embedded mid-statement (e.g. a query hint) must not
    // affect tokenization of the surrounding SQL.
    let input = "SELECT /* COLUMNAR */ 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_multiline() {
    let input = "SELECT /* this comment\nspans multiple\nlines */ 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_does_not_nest() {
    // SQL standard block comments do not nest: the first `*/` closes the
    // comment, even though a `/*` appears inside it. So the trailing `*/`
    // after "still leftover" is unmatched and becomes its own token
    // (multiply followed by divide), which is a syntax error at the
    // parser level but must not be a *lexer* error.
    let input = "/* outer /* inner */ still leftover */";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    // The comment closes at the first `*/` (after "inner"), leaving
    // `still leftover */` as real SQL text to tokenize.
    assert_eq!(
        tokens,
        vec![
            Token::Identifier("still".to_string()),
            Token::Identifier("leftover".to_string()),
            Token::Symbol('*'),
            Token::Symbol('/'),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_adjacent_to_line_comment() {
    let input = "-- line comment\n/* block comment */\nSELECT 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Number("1".to_string()),
            Token::Eof,
        ]
    );
}

#[test]
fn test_block_comment_unterminated_is_lexer_error() {
    let input = "SELECT 1 /* unterminated comment";
    let mut lexer = Lexer::new(input);
    let result = lexer.tokenize();

    assert!(result.is_err(), "Unterminated block comment must be a lexer error");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("unterminated"),
        "Expected an 'unterminated' diagnostic, got: {}",
        err.message
    );
}

#[test]
fn test_block_comment_unterminated_empty_after_open() {
    // `/*` with nothing else at all (not even EOF-adjacent content) must
    // still error rather than silently succeeding.
    let input = "SELECT 1 /*";
    let mut lexer = Lexer::new(input);
    let result = lexer.tokenize();

    assert!(result.is_err(), "Bare unterminated '/*' must be a lexer error");
}

#[test]
fn test_parser_rejects_unterminated_block_comment_directly() {
    // Same as `test_block_comment_unterminated_is_lexer_error`, but through
    // the full `Parser::parse_sql` entry point (which uses
    // `tokenize_with_spans`, a separate call site from `tokenize`).
    let result = Parser::parse_sql("SELECT 1 /* oops");
    assert!(result.is_err(), "Unterminated block comment must fail to parse");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("unterminated"),
        "Expected an 'unterminated' diagnostic, got: {}",
        err.message
    );
}

#[test]
fn test_parser_accepts_block_comment_hint_directly() {
    // Regression test for #6544: feeding SQL with a block comment directly
    // to `Parser::parse_sql` (not through the CLI's separate comment-
    // stripping layer in `crates/vibesql-cli/src/script.rs`) must succeed.
    let result = Parser::parse_sql("SELECT /* COLUMNAR */ 1");
    assert!(result.is_ok(), "Expected block comment to parse directly: {:?}", result.err());
}

#[test]
fn test_default_demo_sql() {
    let input = "-- Welcome to NIST MemSQL\n-- Use Ctrl/Cmd + Enter to execute the current query\nSELECT * FROM employees;";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();

    assert_eq!(
        tokens,
        vec![
            Token::Keyword { keyword: Keyword::Select, original: "SELECT".to_string() },
            Token::Symbol('*'),
            Token::Keyword { keyword: Keyword::From, original: "FROM".to_string() },
            Token::Identifier("employees".to_string()),
            Token::Semicolon,
            Token::Eof,
        ]
    );
}
