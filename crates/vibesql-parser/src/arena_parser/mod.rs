//! Arena-allocated SQL parser.
//!
//! This module provides a parser that allocates AST nodes from a bump arena
//! for improved performance. All allocations are contiguous in memory and
//! freed in a single operation when the arena is dropped.
//!
//! # Usage
//!
//! For arena-allocated AST (fastest, but requires arena lifetime management):
//! ```text
//! use bumpalo::Bump;
//! use vibesql_parser::arena_parser::ArenaParser;
//!
//! let arena = Bump::new();
//! let result = ArenaParser::parse_sql("SELECT * FROM users", &arena);
//! ```
//!
//! For standard heap-allocated AST (convenient, with arena parsing benefits):
//! ```
//! use vibesql_parser::arena_parser::parse_select_to_owned;
//!
//! // Parse with arena internally, convert to owned SelectStmt
//! let stmt = parse_select_to_owned("SELECT * FROM users").unwrap();
//! ```

mod ddl;
mod delete;
mod expression;
mod insert;
mod select;
mod update;

use bumpalo::Bump;
use vibesql_ast::arena::{
    AlterTableStmt, ArenaInterner, Converter, DeleteStmt, Expression, InsertStmt, SelectStmt,
    Statement, Symbol, UpdateStmt,
};

use crate::{keywords::Keyword, lexer::Span, Lexer, ParseError, Token};

/// Arena-based SQL parser.
///
/// Unlike the standard [`Parser`](crate::Parser), this parser allocates all
/// AST nodes from a bump arena, resulting in:
/// - O(1) allocation time (vs heap allocation overhead)
/// - Better cache locality (contiguous memory)
/// - Single deallocation when arena is dropped
pub struct ArenaParser<'arena> {
    tokens: Vec<Token>,
    /// Byte spans for each token, used to extract original source text
    spans: Vec<Span>,
    /// Original SQL input string, used with spans to extract source text
    input: &'arena str,
    position: usize,
    placeholder_count: usize,
    arena: &'arena Bump,
    interner: ArenaInterner<'arena>,
}

impl<'arena> ArenaParser<'arena> {
    /// Create a new arena parser from tokens with spans and original input.
    pub fn new_with_spans(
        tokens: Vec<Token>,
        spans: Vec<Span>,
        input: &'arena str,
        arena: &'arena Bump,
    ) -> Self {
        ArenaParser {
            tokens,
            spans,
            input,
            position: 0,
            placeholder_count: 0,
            arena,
            interner: ArenaInterner::new(arena),
        }
    }

    /// Create a new arena parser from tokens (legacy constructor without spans).
    /// Source text reconstruction will fall back to token-based reconstruction.
    pub fn new(tokens: Vec<Token>, arena: &'arena Bump) -> Self {
        ArenaParser {
            tokens,
            spans: Vec::new(), // No spans available
            input: "",         // No original input
            position: 0,
            placeholder_count: 0,
            arena,
            interner: ArenaInterner::new(arena),
        }
    }

    /// Returns a reference to the interner for symbol resolution during conversion.
    pub fn interner(&self) -> &ArenaInterner<'arena> {
        &self.interner
    }

    /// Consumes the parser and returns the interner.
    pub fn into_interner(self) -> ArenaInterner<'arena> {
        self.interner
    }

    /// Parse SQL input string into an arena-allocated Statement.
    ///
    /// Supports the full range of SQL statements including DML (SELECT, INSERT,
    /// UPDATE, DELETE), DDL (CREATE, DROP, ALTER), and transaction statements.
    pub fn parse_sql(input: &str, arena: &'arena Bump) -> Result<Statement<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let (tokens, spans): (Vec<_>, Vec<_>) = tokens_with_spans.into_iter().unzip();
        let input_in_arena = arena.alloc_str(input);

        let mut parser = ArenaParser::new_with_spans(tokens, spans, input_in_arena, arena);
        parser.parse_statement()
    }

    /// Parse SQL input string into an arena-allocated SelectStmt.
    ///
    /// Convenience method for when you know you're parsing a SELECT.
    pub fn parse_select(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let (tokens, spans): (Vec<_>, Vec<_>) = tokens_with_spans.into_iter().unzip();
        let input_in_arena = arena.alloc_str(input);

        let mut parser = ArenaParser::new_with_spans(tokens, spans, input_in_arena, arena);
        parser.parse_select_statement()
    }

    /// Parse SQL input string into an arena-allocated SelectStmt, returning the interner too.
    ///
    /// Use this method when you need to resolve Symbol values to strings.
    pub fn parse_select_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena SelectStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let (tokens, spans): (Vec<_>, Vec<_>) = tokens_with_spans.into_iter().unzip();
        let input_in_arena = arena.alloc_str(input);

        let mut parser = ArenaParser::new_with_spans(tokens, spans, input_in_arena, arena);
        let stmt = parser.parse_select_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse a single statement.
    fn parse_statement(&mut self) -> Result<Statement<'arena>, ParseError> {
        // Skip leading semicolons
        while self.try_consume(&Token::Semicolon) {}

        match self.peek() {
            // DML statements
            Token::Keyword { keyword: Keyword::Select, .. }
            | Token::Keyword { keyword: Keyword::With, .. } => {
                let stmt = self.parse_select_statement()?;
                Ok(Statement::Select(stmt))
            }
            Token::Keyword { keyword: Keyword::Insert, .. } => {
                let stmt = self.parse_insert_statement()?;
                Ok(Statement::Insert(stmt.clone()))
            }
            Token::Keyword { keyword: Keyword::Replace, .. } => {
                let stmt = self.parse_replace_statement()?;
                Ok(Statement::Insert(stmt.clone()))
            }
            Token::Keyword { keyword: Keyword::Update, .. } => {
                let stmt = self.parse_update_statement()?;
                Ok(Statement::Update(stmt.clone()))
            }
            Token::Keyword { keyword: Keyword::Delete, .. } => {
                let stmt = self.parse_delete_statement()?;
                Ok(Statement::Delete(stmt.clone()))
            }

            // DDL statements
            Token::Keyword { keyword: Keyword::Create, .. } => self.parse_create_statement(),
            Token::Keyword { keyword: Keyword::Drop, .. } => self.parse_drop_statement(),
            Token::Keyword { keyword: Keyword::Alter, .. } => {
                let stmt = self.parse_alter_table_statement()?;
                Ok(Statement::AlterTable(stmt.clone()))
            }
            Token::Keyword { keyword: Keyword::Truncate, .. } => {
                let stmt = self.parse_truncate_table_statement()?;
                Ok(Statement::TruncateTable(stmt))
            }
            Token::Keyword { keyword: Keyword::Analyze, .. } => {
                let stmt = self.parse_analyze_statement()?;
                Ok(Statement::Analyze(stmt))
            }
            Token::Keyword { keyword: Keyword::Vacuum, .. } => {
                let stmt = self.parse_vacuum_statement()?;
                Ok(Statement::Vacuum(stmt))
            }
            Token::Keyword { keyword: Keyword::Pragma, .. } => {
                let stmt = self.parse_pragma_statement()?;
                Ok(Statement::Pragma(stmt))
            }

            // Transaction statements
            Token::Keyword { keyword: Keyword::Begin, .. }
            | Token::Keyword { keyword: Keyword::Start, .. } => {
                let stmt = self.parse_begin_statement()?;
                Ok(Statement::BeginTransaction(stmt))
            }
            Token::Keyword { keyword: Keyword::Commit, .. }
            | Token::Keyword { keyword: Keyword::End, .. } => {
                // END is a SQLite alias for COMMIT in transaction context
                let stmt = self.parse_commit_statement()?;
                Ok(Statement::Commit(stmt))
            }
            Token::Keyword { keyword: Keyword::Rollback, .. } => {
                // Check for ROLLBACK TO SAVEPOINT
                if self.peek_next_keyword(Keyword::To) {
                    let stmt = self.parse_rollback_to_savepoint_statement()?;
                    Ok(Statement::RollbackToSavepoint(stmt))
                } else {
                    let stmt = self.parse_rollback_statement()?;
                    Ok(Statement::Rollback(stmt))
                }
            }
            Token::Keyword { keyword: Keyword::Savepoint, .. } => {
                let stmt = self.parse_savepoint_statement()?;
                Ok(Statement::Savepoint(stmt))
            }
            Token::Keyword { keyword: Keyword::Release, .. } => {
                let stmt = self.parse_release_savepoint_statement()?;
                Ok(Statement::ReleaseSavepoint(stmt))
            }

            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse CREATE statement and dispatch to appropriate sub-parser.
    fn parse_create_statement(&mut self) -> Result<Statement<'arena>, ParseError> {
        // Peek ahead to determine what we're creating
        let mut offset = 1; // Skip CREATE

        // Skip optional OR REPLACE
        if matches!(self.peek_at_offset(offset), Token::Keyword { keyword: Keyword::Or, .. }) {
            offset += 2; // Skip OR REPLACE
        }

        // Skip optional UNIQUE, FULLTEXT, SPATIAL
        if matches!(
            self.peek_at_offset(offset),
            Token::Keyword { keyword: Keyword::Unique, .. }
                | Token::Keyword { keyword: Keyword::Fulltext, .. }
                | Token::Keyword { keyword: Keyword::Spatial, .. }
        ) {
            offset += 1;
        }

        // Skip optional TEMP/TEMPORARY
        if matches!(
            self.peek_at_offset(offset),
            Token::Keyword { keyword: Keyword::Temp, .. }
                | Token::Keyword { keyword: Keyword::Temporary, .. }
        ) {
            offset += 1;
        }

        match self.peek_at_offset(offset) {
            Token::Keyword { keyword: Keyword::Index, .. } => {
                let stmt = self.parse_create_index_statement()?;
                Ok(Statement::CreateIndex(stmt))
            }
            Token::Keyword { keyword: Keyword::View, .. } => {
                let stmt = self.parse_create_view_statement()?;
                Ok(Statement::CreateView(stmt))
            }
            _ => Err(ParseError {
                message: format!(
                    "Unsupported CREATE statement type: {:?}",
                    self.peek_at_offset(offset)
                ),
            }),
        }
    }

    /// Parse DROP statement and dispatch to appropriate sub-parser.
    fn parse_drop_statement(&mut self) -> Result<Statement<'arena>, ParseError> {
        // Peek ahead to determine what we're dropping
        match self.peek_at_offset(1) {
            Token::Keyword { keyword: Keyword::Table, .. } => {
                let stmt = self.parse_drop_table_statement()?;
                Ok(Statement::DropTable(stmt))
            }
            Token::Keyword { keyword: Keyword::Index, .. } => {
                let stmt = self.parse_drop_index_statement()?;
                Ok(Statement::DropIndex(stmt))
            }
            Token::Keyword { keyword: Keyword::View, .. } => {
                let stmt = self.parse_drop_view_statement()?;
                Ok(Statement::DropView(stmt))
            }
            _ => Err(ParseError {
                message: format!("Unsupported DROP statement type: {:?}", self.peek_at_offset(1)),
            }),
        }
    }

    /// Parse an expression and return an arena-allocated reference.
    pub fn parse_expression_sql(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena Expression<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let expr = parser.parse_expression()?;
        Ok(arena.alloc(expr))
    }

    /// Parse SQL input string into an arena-allocated AlterTableStmt.
    pub fn parse_alter_table_sql(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena AlterTableStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_alter_table_statement()
    }

    /// Parse an INSERT statement into an arena-allocated InsertStmt.
    pub fn parse_insert(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_insert_statement()
    }

    /// Parse an UPDATE statement into an arena-allocated UpdateStmt.
    pub fn parse_update(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena UpdateStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_update_statement()
    }

    /// Parse a DELETE statement into an arena-allocated DeleteStmt.
    pub fn parse_delete(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena DeleteStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_delete_statement()
    }

    /// Parse a REPLACE statement (alias for INSERT OR REPLACE) into an arena-allocated InsertStmt.
    pub fn parse_replace(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        parser.parse_replace_statement()
    }

    /// Parse an ALTER TABLE statement, returning the interner for symbol resolution.
    pub fn parse_alter_table_sql_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena AlterTableStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_alter_table_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse a DELETE statement, returning the interner for symbol resolution.
    pub fn parse_delete_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena DeleteStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_delete_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse an UPDATE statement, returning the interner for symbol resolution.
    pub fn parse_update_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena UpdateStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_update_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse an INSERT statement, returning the interner for symbol resolution.
    pub fn parse_insert_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena InsertStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_insert_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse a REPLACE statement, returning the interner for symbol resolution.
    pub fn parse_replace_with_interner(
        input: &str,
        arena: &'arena Bump,
    ) -> Result<(&'arena InsertStmt<'arena>, ArenaInterner<'arena>), ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_replace_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Intern a string and return a Symbol.
    #[inline]
    pub(crate) fn intern(&mut self, s: &str) -> Symbol {
        self.interner.intern(s)
    }

    /// Allocate a string in the arena (for non-identifier strings).
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn alloc_str(&self, s: &str) -> &'arena str {
        self.arena.alloc_str(s)
    }

    /// Get a reference to the arena.
    #[inline]
    #[allow(dead_code)]
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
        matches!(self.peek(), Token::Keyword { keyword: kw, .. } if *kw == keyword)
    }

    /// Check if next token (position + 1) is a specific keyword.
    #[allow(dead_code)]
    pub(crate) fn peek_next_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_next(), Token::Keyword { keyword: kw, .. } if *kw == keyword)
    }

    /// Check if there's a SELECT keyword after any number of opening parentheses.
    /// This is used to detect subqueries in contexts like `IN ((SELECT ...))` where
    /// extra parentheses around the SELECT should still be treated as a subquery.
    ///
    /// Returns (true, depth) if SELECT is found, where depth is the number of parens traversed.
    /// Returns (false, 0) otherwise.
    pub(crate) fn peek_select_through_parens(&self) -> (bool, usize) {
        let mut offset = 0;
        let mut paren_depth = 0;

        loop {
            let token = self.peek_at_offset(offset);
            match token {
                Token::LParen => {
                    paren_depth += 1;
                    offset += 1;
                }
                Token::Keyword { keyword: Keyword::Select, .. }
                | Token::Keyword { keyword: Keyword::Values, .. } => {
                    // Found SELECT or VALUES after parentheses - this is a subquery
                    return (true, paren_depth);
                }
                _ => {
                    // Found something else - not a subquery through parens
                    return (false, 0);
                }
            }
        }
    }

    /// Consume a keyword, returning an error if it's not the expected keyword.
    pub(crate) fn consume_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        if self.peek_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError { message: self.peek().syntax_error() })
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
            Err(ParseError { message: self.peek().syntax_error() })
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

    /// Require end of statement after the final clause of a statement.
    ///
    /// Consumes a trailing semicolon if present. Any token other than `;` or
    /// EOF is a syntax error matching SQLite's `near "X": syntax error`
    /// (issue #5261: trailing garbage after UPDATE/DELETE/INSERT was
    /// previously ignored silently).
    pub(crate) fn expect_statement_end(&mut self) -> Result<(), ParseError> {
        match self.peek() {
            Token::Semicolon => {
                self.advance();
                Ok(())
            }
            Token::Eof => Ok(()),
            token => Err(ParseError { message: token.syntax_error() }),
        }
    }

    /// Get the next placeholder index.
    pub(crate) fn next_placeholder(&mut self) -> usize {
        let index = self.placeholder_count;
        self.placeholder_count += 1;
        index
    }

    // ========================================================================
    // Common parsing helpers
    // ========================================================================

    /// Parse an identifier and intern it, returning a Symbol.
    pub(crate) fn parse_arena_identifier(&mut self) -> Result<Symbol, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(self.intern(&name))
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse a comma-separated list of identifiers.
    pub(crate) fn parse_identifier_list(
        &mut self,
    ) -> Result<bumpalo::collections::Vec<'arena, Symbol>, ParseError> {
        let mut list = bumpalo::collections::Vec::new_in(self.arena);
        loop {
            list.push(self.parse_arena_identifier()?);
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }
        Ok(list)
    }

    /// Parse an optional column alias list: (col1, col2, ...)
    ///
    /// SQL:1999 Feature E051-09: Derived column lists in table aliases
    /// Example: FROM t AS myalias (x, y) or FROM (SELECT a, b) AS mytemp (x, y)
    ///
    /// Returns None if no opening parenthesis is found, otherwise parses
    /// and returns the list of column aliases as Symbols.
    pub(crate) fn parse_column_alias_list(
        &mut self,
    ) -> Result<Option<bumpalo::collections::Vec<'arena, Symbol>>, ParseError> {
        // Check for opening parenthesis
        if !self.try_consume(&Token::LParen) {
            return Ok(None);
        }

        let mut aliases = bumpalo::collections::Vec::new_in(self.arena);

        // Handle empty list case: ()
        if self.try_consume(&Token::RParen) {
            return Ok(Some(aliases));
        }

        // Parse first alias (identifiers or keywords allowed)
        aliases.push(self.parse_alias_name_symbol()?);

        // Parse remaining aliases
        while self.try_consume(&Token::Comma) {
            aliases.push(self.parse_alias_name_symbol()?);
        }

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(Some(aliases))
    }

    /// Parse an identifier or keyword as an alias name, returning a Symbol.
    ///
    /// SQLite also allows single-quoted strings as aliases (e.g., `SELECT 1 AS 'a'`).
    /// In this context, the string literal is treated as an identifier name.
    fn parse_alias_name_symbol(&mut self) -> Result<Symbol, ParseError> {
        match self.peek() {
            Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(self.intern(&name))
            }
            Token::Keyword { keyword: kw, .. } => {
                // Allow keywords as alias names
                let name = kw.to_string();
                self.advance();
                Ok(self.intern(&name))
            }
            Token::String(s) => {
                // SQLite compatibility: single-quoted strings can be used as aliases
                let alias = s.clone();
                self.advance();
                Ok(self.intern(&alias))
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Reconstruct source text from tokens in a range.
    ///
    /// This reconstructs the original SQL text from the tokens consumed during
    /// expression parsing. Used for preserving original expression text as column
    /// names when no alias is provided (SQLite compatibility).
    ///
    /// When spans are available, extracts directly from the original input to
    /// preserve exact case and formatting. Falls back to token-based reconstruction
    /// (which uppercases identifiers) when spans are not available.
    pub(crate) fn reconstruct_source_text(
        &self,
        start_pos: usize,
        end_pos: usize,
    ) -> Option<&'arena str> {
        if start_pos >= end_pos || start_pos >= self.tokens.len() {
            return None;
        }

        // If we have spans, extract the original source text directly
        if !self.spans.is_empty() && start_pos < self.spans.len() && end_pos <= self.spans.len() {
            let start_byte = self.spans[start_pos].start;
            // Use end_pos - 1 because end_pos is exclusive (points past the last token)
            let end_byte = if end_pos > 0 && end_pos <= self.spans.len() {
                self.spans[end_pos - 1].end
            } else {
                self.spans[self.spans.len() - 1].end
            };

            if start_byte < end_byte && end_byte <= self.input.len() {
                let source_text = &self.input[start_byte..end_byte];
                return Some(self.arena.alloc_str(source_text));
            }
        }

        // Fall back to token-based reconstruction (won't preserve case)
        let mut result = String::new();
        let end = end_pos.min(self.tokens.len());

        for i in start_pos..end {
            let token = &self.tokens[i];
            if matches!(token, Token::Eof) {
                break;
            }
            result.push_str(&token.to_sql());
        }

        if result.is_empty() {
            None
        } else {
            // Allocate the string in the arena and return a reference
            Some(self.arena.alloc_str(&result))
        }
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
/// ```
/// use vibesql_parser::arena_parser::parse_select_to_owned;
///
/// let stmt = parse_select_to_owned("SELECT * FROM users").unwrap();
/// // stmt is a standard SelectStmt, no lifetime constraints
/// ```
pub fn parse_select_to_owned(input: &str) -> Result<vibesql_ast::SelectStmt, ParseError> {
    let arena = Bump::new();
    let mut lexer = Lexer::new(input);
    let tokens_with_spans = lexer
        .tokenize_with_spans()
        .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let (tokens, spans): (Vec<_>, Vec<_>) = tokens_with_spans.into_iter().unzip();
    let input_in_arena = arena.alloc_str(input);

    let mut parser = ArenaParser::new_with_spans(tokens, spans, input_in_arena, &arena);
    let arena_stmt = parser.parse_select_statement()?;
    let converter = Converter::new(parser.interner());
    Ok(converter.convert_select(arena_stmt))
}

/// Parse an expression and return a heap-allocated (owned) Expression.
///
/// Like [`parse_select_to_owned`], this provides arena parsing
/// benefits while returning an owned expression.
///
/// # Example
///
/// ```
/// use vibesql_parser::arena_parser::parse_expression_to_owned;
///
/// let expr = parse_expression_to_owned("a + b * 2").unwrap();
/// ```
pub fn parse_expression_to_owned(input: &str) -> Result<vibesql_ast::Expression, ParseError> {
    let arena = Bump::new();
    let mut lexer = Lexer::new(input);
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let mut parser = ArenaParser::new(tokens, &arena);
    let arena_expr = parser.parse_expression()?;
    let converter = Converter::new(parser.interner());
    Ok(converter.convert_expression(&arena_expr))
}

/// Parse INSERT SQL and return a heap-allocated (owned) InsertStmt.
///
/// Like [`parse_select_to_owned`], this provides arena parsing
/// benefits while returning a standard `InsertStmt`.
///
/// # Example
///
/// ```
/// use vibesql_parser::arena_parser::parse_insert_to_owned;
///
/// let stmt = parse_insert_to_owned("INSERT INTO users (name) VALUES ('Alice')").unwrap();
/// ```
pub fn parse_insert_to_owned(input: &str) -> Result<vibesql_ast::InsertStmt, ParseError> {
    let arena = Bump::new();
    let mut lexer = Lexer::new(input);
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let mut parser = ArenaParser::new(tokens, &arena);
    let arena_stmt = parser.parse_insert_statement()?;
    let converter = Converter::new(parser.interner());
    Ok(converter.convert_insert(arena_stmt))
}

/// Parse UPDATE SQL and return a heap-allocated (owned) UpdateStmt.
///
/// Like [`parse_select_to_owned`], this provides arena parsing
/// benefits while returning a standard `UpdateStmt`.
///
/// # Example
///
/// ```
/// use vibesql_parser::arena_parser::parse_update_to_owned;
///
/// let stmt = parse_update_to_owned("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
/// ```
pub fn parse_update_to_owned(input: &str) -> Result<vibesql_ast::UpdateStmt, ParseError> {
    let arena = Bump::new();
    let mut lexer = Lexer::new(input);
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let mut parser = ArenaParser::new(tokens, &arena);
    let arena_stmt = parser.parse_update_statement()?;
    let converter = Converter::new(parser.interner());
    Ok(converter.convert_update(arena_stmt))
}

/// Parse DELETE SQL and return a heap-allocated (owned) DeleteStmt.
///
/// Like [`parse_select_to_owned`], this provides arena parsing
/// benefits while returning a standard `DeleteStmt`.
///
/// # Example
///
/// ```
/// use vibesql_parser::arena_parser::parse_delete_to_owned;
///
/// let stmt = parse_delete_to_owned("DELETE FROM users WHERE id = 1").unwrap();
/// ```
pub fn parse_delete_to_owned(input: &str) -> Result<vibesql_ast::DeleteStmt, ParseError> {
    let arena = Bump::new();
    let mut lexer = Lexer::new(input);
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let mut parser = ArenaParser::new(tokens, &arena);
    let arena_stmt = parser.parse_delete_statement()?;
    let converter = Converter::new(parser.interner());
    Ok(converter.convert_delete(arena_stmt))
}

#[cfg(test)]
mod tests {
    use vibesql_ast::arena::Expression;
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_date_literal() {
        let arena = Bump::new();
        let expr = ArenaParser::parse_expression_sql("DATE '1998-12-01'", &arena).unwrap();
        match expr {
            Expression::Literal(SqlValue::Date(d)) => {
                assert_eq!(d.year, 1998);
                assert_eq!(d.month, 12);
                assert_eq!(d.day, 1);
            }
            _ => panic!("Expected Date literal, got {:?}", expr),
        }
    }

    #[test]
    fn test_time_literal() {
        let arena = Bump::new();
        let expr = ArenaParser::parse_expression_sql("TIME '12:30:45'", &arena).unwrap();
        match expr {
            Expression::Literal(SqlValue::Time(t)) => {
                assert_eq!(t.hour, 12);
                assert_eq!(t.minute, 30);
                assert_eq!(t.second, 45);
            }
            _ => panic!("Expected Time literal, got {:?}", expr),
        }
    }

    #[test]
    fn test_timestamp_literal() {
        let arena = Bump::new();
        let expr =
            ArenaParser::parse_expression_sql("TIMESTAMP '2024-01-15 10:30:00'", &arena).unwrap();
        match expr {
            Expression::Literal(SqlValue::Timestamp(ts)) => {
                assert_eq!(ts.date.year, 2024);
                assert_eq!(ts.date.month, 1);
                assert_eq!(ts.date.day, 15);
            }
            _ => panic!("Expected Timestamp literal, got {:?}", expr),
        }
    }

    #[test]
    fn test_interval_literal() {
        let arena = Bump::new();
        let expr = ArenaParser::parse_expression_sql("INTERVAL '90' DAY", &arena).unwrap();
        // Just verify it parses to an Interval type
        assert!(matches!(expr, Expression::Literal(SqlValue::Interval(_))));
    }

    #[test]
    fn test_date_minus_interval_expression() {
        let arena = Bump::new();
        let expr =
            ArenaParser::parse_expression_sql("DATE '1998-12-01' - INTERVAL '90' DAY", &arena)
                .unwrap();
        match expr {
            Expression::BinaryOp { op, left, right } => {
                assert_eq!(*op, vibesql_ast::BinaryOperator::Minus);
                assert!(matches!(left, Expression::Literal(SqlValue::Date(_))));
                assert!(matches!(right, Expression::Literal(SqlValue::Interval(_))));
            }
            _ => panic!("Expected BinaryOp, got {:?}", expr),
        }
    }

    #[test]
    fn test_tpch_q1_parses() {
        let arena = Bump::new();
        let sql = r#"SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM
            lineitem
        WHERE
            l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag,
            l_linestatus"#;

        // This should parse successfully now with typed literal support
        let result = ArenaParser::parse_sql(sql, &arena);
        assert!(result.is_ok(), "TPC-H Q1 should parse successfully: {:?}", result.err());
    }

    #[test]
    fn test_order_by_nulls_first_last() {
        use vibesql_ast::NullsOrder;

        // Test NULLS FIRST
        let stmt = parse_select_to_owned("SELECT x FROM t ORDER BY x ASC NULLS FIRST").unwrap();
        let order_by = stmt.order_by.unwrap();
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].nulls_order, Some(NullsOrder::First));

        // Test NULLS LAST
        let stmt = parse_select_to_owned("SELECT x FROM t ORDER BY x DESC NULLS LAST").unwrap();
        let order_by = stmt.order_by.unwrap();
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].nulls_order, Some(NullsOrder::Last));

        // Test default (None)
        let stmt = parse_select_to_owned("SELECT x FROM t ORDER BY x").unwrap();
        let order_by = stmt.order_by.unwrap();
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].nulls_order, None);

        // Test multiple columns
        let stmt =
            parse_select_to_owned("SELECT x, y FROM t ORDER BY x NULLS FIRST, y DESC NULLS LAST")
                .unwrap();
        let order_by = stmt.order_by.unwrap();
        assert_eq!(order_by.len(), 2);
        assert_eq!(order_by[0].nulls_order, Some(NullsOrder::First));
        assert_eq!(order_by[1].nulls_order, Some(NullsOrder::Last));
    }
}
