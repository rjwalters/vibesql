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

pub use keywords::Keyword;
pub use lexer::{Lexer, LexerError};
pub use parser::{ParseError, Parser};
pub use token::Token;

use vibesql_ast::Statement;

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
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    // Check first token to determine statement type
    if let Some(first_token) = tokens.first() {
        if matches!(first_token, Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With)) {
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
}
