//! SQL:1999 Parser crate.
//!
//! Provides tokenization and parsing of SQL statements into the shared AST.
//!
//! # Arena-allocated Parser
//!
//! For performance-critical code paths, the [`arena_parser`] module provides
//! an arena-based parser that allocates AST nodes from a bump allocator.
//!
//! # Arena Fallback Parsing
//!
//! The [`parse_with_arena_fallback`] function provides optimized parsing by
//! using arena allocation for supported statement types (SELECT) while falling
//! back to standard heap allocation for other statements. This provides the
//! best of both worlds: arena performance where it helps most (complex queries)
//! and full feature support for all SQL statements.
//!
//! ```
//! use vibesql_parser::parse_with_arena_fallback;
//!
//! let stmt = parse_with_arena_fallback("SELECT * FROM users").unwrap();
//! // Uses arena parsing internally, converts to standard Statement
//! ```

pub mod arena_parser;

mod keywords;
mod lexer;
mod parser;
#[cfg(test)]
mod tests;
mod token;
mod trigger_body;

pub use keywords::Keyword;
pub use lexer::{Lexer, LexerError, Span};
pub use parser::{ParseError, Parser};
pub use token::Token;
pub use trigger_body::split_trigger_body_statements;
use vibesql_ast::Statement;

/// Returns `true` if `name` is a SQL keyword (case-insensitive).
///
/// Used by callers that emit SQL text (e.g. the persistence dump) to decide
/// whether a bare identifier must be double-quoted to survive a round-trip. A
/// keyword-named identifier such as `create` must be written as `"create"`;
/// otherwise the reload re-lexes it as the keyword token and loses the original
/// spelling/case (issue #5618).
pub fn is_keyword(name: &str) -> bool {
    // Re-lex the bare name: if it tokenizes to a single keyword token, the
    // word is reserved/keyword and must be quoted to round-trip as an
    // identifier. (A genuine identifier lexes to `Token::Identifier`.)
    let mut lexer = Lexer::new(name);
    match lexer.tokenize() {
        Ok(tokens) => {
            // tokenize() appends a trailing Eof token.
            matches!(tokens.first(), Some(Token::Keyword { .. }))
                && matches!(tokens.get(1), Some(Token::Eof) | None)
        }
        Err(_) => false,
    }
}

/// Parse SQL using arena allocation where supported, falling back to standard parsing.
///
/// This function provides optimized parsing by:
/// 1. Detecting the statement type from the first token
/// 2. Using arena-allocated parsing for SELECT statements
/// 3. Converting arena AST to standard heap-allocated AST
/// 4. Falling back to standard parsing for unsupported statement types
///
/// # Performance
///
/// Arena parsing can provide 10-15% improvement for complex SELECT statements
/// due to reduced allocation overhead and better cache locality. The conversion
/// to standard AST types adds minimal overhead.
///
/// # Supported Statement Types
///
/// Currently uses arena parsing for:
/// - SELECT statements (including CTEs, subqueries, joins)
///
/// Falls back to standard parsing for:
/// - INSERT, UPDATE, DELETE (arena support planned for future)
/// - DDL statements (CREATE, ALTER, DROP)
/// - Transaction statements (BEGIN, COMMIT, ROLLBACK)
/// - Other SQL statements
///
/// # Example
///
/// ```
/// use vibesql_parser::parse_with_arena_fallback;
///
/// // Uses arena parsing
/// let select = parse_with_arena_fallback("SELECT * FROM users WHERE id = 1").unwrap();
///
/// // Falls back to standard parsing
/// let insert = parse_with_arena_fallback("INSERT INTO users VALUES (1, 'Alice')").unwrap();
/// ```
pub fn parse_with_arena_fallback(sql: &str) -> Result<Statement, ParseError> {
    // Tokenize to detect statement type
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize().map_err(|e| ParseError { message: e.to_string() })?;

    // Check first token to determine statement type
    if let Some(first_token) = tokens.first() {
        if matches!(
            first_token,
            Token::Keyword { keyword: Keyword::Select, .. }
                | Token::Keyword { keyword: Keyword::With, .. }
        ) {
            // Use arena parsing for SELECT statements (including WITH CTEs)
            match arena_parser::parse_select_to_owned(sql) {
                Ok(select_stmt) => {
                    return Ok(Statement::Select(Box::new(select_stmt)));
                }
                Err(_) => {
                    // Arena parsing failed, fall back to standard parser
                    // This can happen with edge cases the arena parser doesn't support yet
                }
            }
        }
    }

    // Fall back to standard parser for all other statements
    // or if arena parsing failed
    Parser::parse_sql(sql)
}

#[cfg(test)]
mod arena_fallback_tests {
    use super::*;

    #[test]
    fn test_arena_fallback_simple_select() {
        let result = parse_with_arena_fallback("SELECT * FROM users");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Select(_)));
    }

    #[test]
    fn test_arena_fallback_select_with_where() {
        let result = parse_with_arena_fallback("SELECT id, name FROM users WHERE active = TRUE");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Select(_)));
    }

    #[test]
    fn test_arena_fallback_select_with_cte() {
        let result = parse_with_arena_fallback(
            "WITH active_users AS (SELECT * FROM users WHERE active = TRUE) \
             SELECT * FROM active_users",
        );
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Select(_)));
    }

    #[test]
    fn test_arena_fallback_insert() {
        let result = parse_with_arena_fallback("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Insert(_)));
    }

    #[test]
    fn test_arena_fallback_update() {
        let result = parse_with_arena_fallback("UPDATE users SET name = 'Bob' WHERE id = 1");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Update(_)));
    }

    #[test]
    fn test_arena_fallback_delete() {
        let result = parse_with_arena_fallback("DELETE FROM users WHERE id = 1");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::Delete(_)));
    }

    #[test]
    fn test_arena_fallback_create_table() {
        let result = parse_with_arena_fallback("CREATE TABLE users (id INT PRIMARY KEY)");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Statement::CreateTable(_)));
    }

    #[test]
    fn test_arena_fallback_qualified_wildcard() {
        let result = parse_with_arena_fallback("SELECT t1.* FROM t1");
        assert!(result.is_ok());
        if let Statement::Select(select) = result.unwrap() {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "t1");
                }
                other => panic!("Expected QualifiedWildcard, got {:?}", other),
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_window_clause_parsing() {
        // Test 1: Just OVER win (without WINDOW clause) - window name reference
        let sql = "SELECT sum(x) OVER win FROM t1";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "OVER win reference failed: {:?}", result.err());

        // Test 2: WINDOW clause definition
        let sql = "SELECT 1 FROM t1 WINDOW win AS (ORDER BY y)";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "WINDOW clause failed: {:?}", result.err());

        // Test 3: Full window clause with reference
        let sql = "SELECT sum(x) OVER win FROM t1 WINDOW win AS (ORDER BY y)";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Full WINDOW clause failed: {:?}", result.err());

        // Test 4: Window inheritance (WINDOW win2 AS (win ORDER BY ...))
        let sql = "SELECT row_number() OVER win2 FROM t1 WINDOW win AS (PARTITION BY z), win2 AS (win ORDER BY x)";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Window inheritance failed: {:?}", result.err());
    }

    #[test]
    fn test_is_keyword() {
        // Reserved/keyword words (case-insensitive) — these must be quoted to be
        // used as identifiers, and a SQL emitter must quote them (issue #5618).
        assert!(is_keyword("create"));
        assert!(is_keyword("CREATE"));
        assert!(is_keyword("Select"));
        assert!(is_keyword("from"));
        assert!(is_keyword("table"));

        // Ordinary identifiers are not keywords.
        assert!(!is_keyword("mixedcase"));
        assert!(!is_keyword("MixedCase"));
        assert!(!is_keyword("f1"));
        assert!(!is_keyword("users"));
        assert!(!is_keyword("col_a"));

        // A multi-token / punctuated string is not a single keyword.
        assert!(!is_keyword("create table"));
        assert!(!is_keyword("a.b"));
        assert!(!is_keyword(""));
    }
}
