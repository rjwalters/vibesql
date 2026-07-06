//! Tests for `Parser::parse_expression_sql` trailing-token handling.
//!
//! `parse_expression_sql` re-parses catalog-persisted expression text
//! (expression indexes, partial-index WHERE clauses). It must accept exactly
//! one expression, tolerating a single trailing `;` terminator but rejecting
//! anything smuggled after it — a fail-closed guard against corrupt or
//! hand-crafted catalog blobs like `x; DROP TABLE t` (issue #5866).

use crate::parser::Parser;

#[test]
fn test_bare_expression_ok() {
    assert!(Parser::parse_expression_sql("x").is_ok());
    assert!(Parser::parse_expression_sql("x = 1").is_ok());
}

#[test]
fn test_single_trailing_semicolon_ok() {
    assert!(Parser::parse_expression_sql("x;").is_ok());
    assert!(Parser::parse_expression_sql("x = 1;").is_ok());
}

#[test]
fn test_tokens_after_semicolon_error() {
    assert!(Parser::parse_expression_sql("x; DROP TABLE t").is_err());
    assert!(Parser::parse_expression_sql("x = 1; DELETE FROM t").is_err());
}

#[test]
fn test_double_semicolon_error() {
    // An empty statement after the terminator is still a trailing token.
    assert!(Parser::parse_expression_sql("x;;").is_err());
}

#[test]
fn test_semicolon_inside_string_literal_ok() {
    // The `;` is lexed into the string token and never seen as a top-level
    // `Token::Semicolon`, so the expression round-trips.
    assert!(Parser::parse_expression_sql("x = 'a;b'").is_ok());
}

#[test]
fn test_injection_style_trailing_paren_error() {
    // Regression: pre-existing rejection of a trailing `)` stays intact.
    assert!(Parser::parse_expression_sql("x); DROP TABLE t;--").is_err());
}
