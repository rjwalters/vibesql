use vibesql_ast::{QueryHint, Statement};

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

// ============================================================================
// Query-comment hint capture (#6547)
// ============================================================================

#[test]
fn test_hint_comment_captured_with_text_and_span() {
    let input = "SELECT /* COLUMNAR */ 1";
    let mut lexer = Lexer::new(input);
    lexer.tokenize().unwrap();
    let hints = lexer.take_hints();

    assert_eq!(hints.len(), 1);
    let (hint, span) = hints[0];
    assert_eq!(hint, QueryHint::Columnar);
    // The span covers the full `/* COLUMNAR */` comment, delimiters included.
    assert_eq!(&input[span.start..span.end], "/* COLUMNAR */");
}

#[test]
fn test_row_oriented_hint_comment_captured() {
    let input = "SELECT /* ROW_ORIENTED */ 1";
    let mut lexer = Lexer::new(input);
    lexer.tokenize().unwrap();
    let hints = lexer.take_hints();

    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0].0, QueryHint::RowOriented);
}

#[test]
fn test_hint_matching_is_case_insensitive() {
    let input = "SELECT /* columnar */ 1";
    let mut lexer = Lexer::new(input);
    lexer.tokenize().unwrap();
    assert_eq!(lexer.take_hints()[0].0, QueryHint::Columnar);
}

#[test]
fn test_ordinary_comment_not_captured_as_hint() {
    // A comment that merely mentions a hint keyword as a substring is not
    // an exact match and must not be captured — it is still discarded from
    // the token stream exactly like any other comment (#6544 behavior
    // unchanged).
    let input = "SELECT /* uses COLUMNAR storage internally */ 1";
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    assert!(lexer.take_hints().is_empty());
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
fn test_plain_comment_still_discarded_unchanged() {
    // Non-hint comments (the overwhelmingly common case) are completely
    // unaffected by hint capture — no hints recorded, token stream
    // unchanged.
    let input = "/* just a note about this query */ SELECT 1";
    let mut lexer = Lexer::new(input);
    lexer.tokenize().unwrap();
    assert!(lexer.take_hints().is_empty());
}

#[test]
fn test_multiple_hint_comments_captured_in_source_order() {
    let input = "SELECT /* COLUMNAR */ /* ROW_ORIENTED */ 1";
    let mut lexer = Lexer::new(input);
    lexer.tokenize().unwrap();
    let hints = lexer.take_hints();

    assert_eq!(hints.len(), 2);
    assert_eq!(hints[0].0, QueryHint::Columnar);
    assert_eq!(hints[1].0, QueryHint::RowOriented);
    assert!(hints[0].1.start < hints[1].1.start, "hints must be in source order");
}

#[test]
fn test_unterminated_hint_like_comment_still_errors() {
    // Even a comment whose visible prefix looks like a hint must still hit
    // the existing "unterminated block comment" error path when unclosed —
    // hint recognition only runs after the comment successfully closes.
    let input = "SELECT /* COLUMNAR";
    let mut lexer = Lexer::new(input);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("unterminated"));
}

#[test]
fn test_parser_attaches_leading_hint_to_select_stmt() {
    let stmt = Parser::parse_sql("SELECT /* COLUMNAR */ * FROM t").unwrap();
    match stmt {
        Statement::Select(select) => assert_eq!(select.hints, vec![QueryHint::Columnar]),
        other => panic!("expected Statement::Select, got {other:?}"),
    }
}

#[test]
fn test_parser_does_not_attach_hint_in_non_leading_position() {
    // A recognized hint keyword that appears anywhere other than
    // immediately after the leading `SELECT` keyword is not attached — it
    // is treated exactly like an ordinary comment (still lexes fine, just
    // doesn't reach `SelectStmt::hints`).
    let stmt = Parser::parse_sql("SELECT * FROM t /* COLUMNAR */").unwrap();
    match stmt {
        Statement::Select(select) => assert!(select.hints.is_empty()),
        other => panic!("expected Statement::Select, got {other:?}"),
    }
}

#[test]
fn test_parser_does_not_attach_hint_on_with_cte() {
    // Out of scope for v1 (see `QueryHint` module docs): a hint after the
    // outer SELECT of a WITH...SELECT statement is not attached, since the
    // leading-token check requires SELECT to be the statement's very first
    // token.
    let stmt =
        Parser::parse_sql("WITH cte AS (SELECT 1) SELECT /* COLUMNAR */ * FROM cte").unwrap();
    match stmt {
        Statement::Select(select) => assert!(select.hints.is_empty()),
        other => panic!("expected Statement::Select, got {other:?}"),
    }
}

#[test]
fn test_parser_captures_multiple_leading_hints_last_wins_documented() {
    let stmt = Parser::parse_sql("SELECT /* COLUMNAR */ /* ROW_ORIENTED */ * FROM t").unwrap();
    match stmt {
        Statement::Select(select) => {
            assert_eq!(select.hints, vec![QueryHint::Columnar, QueryHint::RowOriented]);
            // Precedence (last-one-wins) is a read-side convention exercised
            // by the executor-level tests in
            // `vibesql-executor::optimizer::adaptive` — this test only
            // verifies both hints reach the AST in source order.
        }
        other => panic!("expected Statement::Select, got {other:?}"),
    }
}

#[test]
fn test_arena_fallback_path_also_attaches_leading_hint() {
    // `parse_with_arena_fallback` re-lexes independently of `Parser::parse_sql`
    // for SELECT statements (it tries arena parsing first) — verify that
    // path also attaches the leading hint.
    let stmt = crate::parse_with_arena_fallback("SELECT /* COLUMNAR */ * FROM t").unwrap();
    match stmt {
        Statement::Select(select) => assert_eq!(select.hints, vec![QueryHint::Columnar]),
        other => panic!("expected Statement::Select, got {other:?}"),
    }
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
