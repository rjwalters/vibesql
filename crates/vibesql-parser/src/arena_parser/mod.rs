//! Arena-allocated SQL parser.
//!
//! This module provides a parser that allocates AST nodes from a bump arena
//! for improved performance. All allocations are contiguous in memory and
//! freed in a single operation when the arena is dropped.
//!
//! # Usage
//!
//! For arena-allocated AST (fastest, but requires arena lifetime management):
//! ```ignore
//! use bumpalo::Bump;
//! use vibesql_parser::arena_parser::ArenaParser;
//!
//! let arena = Bump::new();
//! let result = ArenaParser::parse_sql("SELECT * FROM users", &arena);
//! ```
//!
//! For standard heap-allocated AST (convenient, with arena parsing benefits):
//! ```ignore
//! use vibesql_parser::arena_parser::parse_select_to_owned;
//!
//! // Parse with arena internally, convert to owned SelectStmt
//! let stmt = parse_select_to_owned("SELECT * FROM users")?;
//! ```

mod expression;
mod select;

use bumpalo::Bump;
use vibesql_ast::arena::{Expression, SelectStmt};

use crate::{Lexer, ParseError, Token};
use crate::keywords::Keyword;

/// Arena-based SQL parser.
///
/// Unlike the standard [`Parser`](crate::Parser), this parser allocates all
/// AST nodes from a bump arena, resulting in:
/// - O(1) allocation time (vs heap allocation overhead)
/// - Better cache locality (contiguous memory)
/// - Single deallocation when arena is dropped
pub struct ArenaParser<'arena> {
    tokens: Vec<Token>,
    position: usize,
    placeholder_count: usize,
    arena: &'arena Bump,
}

impl<'arena> ArenaParser<'arena> {
    /// Create a new arena parser from tokens.
    pub fn new(tokens: Vec<Token>, arena: &'arena Bump) -> Self {
        ArenaParser {
            tokens,
            position: 0,
            placeholder_count: 0,
            arena,
        }
    }

    /// Parse SQL input string into an arena-allocated SelectStmt.
    ///
    /// Currently only supports SELECT statements for Phase 1 prototype.
    pub fn parse_sql(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer
            .tokenize()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_select_statement()
    }

    /// Parse an expression and return an arena-allocated reference.
    pub fn parse_expression_sql(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena Expression<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens = lexer
            .tokenize()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let expr = parser.parse_expression()?;
        Ok(arena.alloc(expr))
    }

    /// Allocate a string in the arena.
    #[inline]
    pub(crate) fn alloc_str(&self, s: &str) -> &'arena str {
        self.arena.alloc_str(s)
    }

    /// Get a reference to the arena.
    #[inline]
    pub(crate) fn arena(&self) -> &'arena Bump {
        self.arena
    }

    // ========================================================================
    // Token manipulation helpers (same as standard parser)
    // ========================================================================

    /// Peek at the current token without consuming it.
    pub(crate) fn peek(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }

    /// Peek at the next token (position + 1) without consuming.
    #[allow(dead_code)]
    pub(crate) fn peek_next(&self) -> &Token {
        self.tokens.get(self.position + 1).unwrap_or(&Token::Eof)
    }

    /// Peek at token at specific offset from current position.
    #[allow(dead_code)]
    pub(crate) fn peek_at_offset(&self, offset: usize) -> &Token {
        self.tokens.get(self.position + offset).unwrap_or(&Token::Eof)
    }

    /// Advance to the next token.
    pub(crate) fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    /// Check if current token is a specific keyword.
    pub(crate) fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek(), Token::Keyword(kw) if *kw == keyword)
    }

    /// Check if next token (position + 1) is a specific keyword.
    #[allow(dead_code)]
    pub(crate) fn peek_next_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_next(), Token::Keyword(kw) if *kw == keyword)
    }

    /// Consume a keyword, returning an error if it's not the expected keyword.
    pub(crate) fn consume_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        if self.peek_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected keyword {:?}, found {:?}", keyword, self.peek()),
            })
        }
    }

    /// Try to consume a keyword, returning true if successful.
    pub(crate) fn try_consume_keyword(&mut self, keyword: Keyword) -> bool {
        if self.peek_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Expect a specific keyword.
    pub(crate) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        self.consume_keyword(keyword)
    }

    /// Expect a specific token.
    pub(crate) fn expect_token(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.peek() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected {:?}, found {:?}", expected, self.peek()),
            })
        }
    }

    /// Try to consume a specific token, returning true if successful.
    pub(crate) fn try_consume(&mut self, token: &Token) -> bool {
        if self.peek() == token {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Get the next placeholder index.
    pub(crate) fn next_placeholder(&mut self) -> usize {
        let index = self.placeholder_count;
        self.placeholder_count += 1;
        index
    }
}

// ============================================================================
// Standalone parse-to-owned functions
// ============================================================================

/// Parse SQL and return a heap-allocated (owned) SelectStmt.
///
/// This function provides the performance benefits of arena parsing while
/// returning a standard `SelectStmt` that can be stored and used without
/// lifetime constraints.
///
/// # Performance
///
/// This is faster than the standard parser because:
/// - Arena parsing is 30-40% faster (fewer allocations during parse)
/// - Conversion allocates fewer, larger chunks (better cache locality)
/// - Many strings benefit from SSO (Small String Optimization)
///
/// # Example
///
/// ```ignore
/// use vibesql_parser::arena_parser::parse_select_to_owned;
///
/// let stmt = parse_select_to_owned("SELECT * FROM users")?;
/// // stmt is a standard SelectStmt, no lifetime constraints
/// ```
pub fn parse_select_to_owned(input: &str) -> Result<vibesql_ast::SelectStmt, ParseError> {
    let arena = Bump::new();
    let arena_stmt = ArenaParser::parse_sql(input, &arena)?;
    Ok(vibesql_ast::SelectStmt::from(arena_stmt))
}

/// Parse an expression and return a heap-allocated (owned) Expression.
///
/// Like [`parse_select_to_owned`], this provides arena parsing
/// benefits while returning an owned expression.
///
/// # Example
///
/// ```ignore
/// use vibesql_parser::arena_parser::parse_expression_to_owned;
///
/// let expr = parse_expression_to_owned("a + b * 2")?;
/// ```
pub fn parse_expression_to_owned(input: &str) -> Result<vibesql_ast::Expression, ParseError> {
    let arena = Bump::new();
    let arena_expr = ArenaParser::parse_expression_sql(input, &arena)?;
    Ok(vibesql_ast::Expression::from(arena_expr))
}
