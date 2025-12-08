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

use crate::keywords::Keyword;
use crate::{Lexer, ParseError, Token};

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
    interner: ArenaInterner<'arena>,
}

impl<'arena> ArenaParser<'arena> {
    /// Create a new arena parser from tokens.
    pub fn new(tokens: Vec<Token>, arena: &'arena Bump) -> Self {
        ArenaParser {
            tokens,
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
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
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
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
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
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = ArenaParser::new(tokens, arena);
        let stmt = parser.parse_select_statement()?;
        Ok((stmt, parser.into_interner()))
    }

    /// Parse a single statement.
    fn parse_statement(&mut self) -> Result<Statement<'arena>, ParseError> {
        // Skip leading semicolons
        while self.try_consume(&Token::Semicolon) {}

        match self.peek() {
            // DML statements
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With) => {
                let stmt = self.parse_select_statement()?;
                Ok(Statement::Select(stmt))
            }
            Token::Keyword(Keyword::Insert) => {
                let stmt = self.parse_insert_statement()?;
                Ok(Statement::Insert(stmt.clone()))
            }
            Token::Keyword(Keyword::Replace) => {
                let stmt = self.parse_replace_statement()?;
                Ok(Statement::Insert(stmt.clone()))
            }
            Token::Keyword(Keyword::Update) => {
                let stmt = self.parse_update_statement()?;
                Ok(Statement::Update(stmt.clone()))
            }
            Token::Keyword(Keyword::Delete) => {
                let stmt = self.parse_delete_statement()?;
                Ok(Statement::Delete(stmt.clone()))
            }

            // DDL statements
            Token::Keyword(Keyword::Create) => self.parse_create_statement(),
            Token::Keyword(Keyword::Drop) => self.parse_drop_statement(),
            Token::Keyword(Keyword::Alter) => {
                let stmt = self.parse_alter_table_statement()?;
                Ok(Statement::AlterTable(stmt.clone()))
            }
            Token::Keyword(Keyword::Truncate) => {
                let stmt = self.parse_truncate_table_statement()?;
                Ok(Statement::TruncateTable(stmt))
            }
            Token::Keyword(Keyword::Analyze) => {
                let stmt = self.parse_analyze_statement()?;
                Ok(Statement::Analyze(stmt))
            }

            // Transaction statements
            Token::Keyword(Keyword::Begin) | Token::Keyword(Keyword::Start) => {
                let stmt = self.parse_begin_statement()?;
                Ok(Statement::BeginTransaction(stmt))
            }
            Token::Keyword(Keyword::Commit) => {
                let stmt = self.parse_commit_statement()?;
                Ok(Statement::Commit(stmt))
            }
            Token::Keyword(Keyword::Rollback) => {
                // Check for ROLLBACK TO SAVEPOINT
                if self.peek_next_keyword(Keyword::To) {
                    let stmt = self.parse_rollback_to_savepoint_statement()?;
                    Ok(Statement::RollbackToSavepoint(stmt))
                } else {
                    let stmt = self.parse_rollback_statement()?;
                    Ok(Statement::Rollback(stmt))
                }
            }
            Token::Keyword(Keyword::Savepoint) => {
                let stmt = self.parse_savepoint_statement()?;
                Ok(Statement::Savepoint(stmt))
            }
            Token::Keyword(Keyword::Release) => {
                let stmt = self.parse_release_savepoint_statement()?;
                Ok(Statement::ReleaseSavepoint(stmt))
            }

            _ => Err(ParseError { message: format!("Unexpected token: {:?}", self.peek()) }),
        }
    }

    /// Parse CREATE statement and dispatch to appropriate sub-parser.
    fn parse_create_statement(&mut self) -> Result<Statement<'arena>, ParseError> {
        // Peek ahead to determine what we're creating
        let mut offset = 1; // Skip CREATE

        // Skip optional OR REPLACE
        if matches!(self.peek_at_offset(offset), Token::Keyword(Keyword::Or)) {
            offset += 2; // Skip OR REPLACE
        }

        // Skip optional UNIQUE, FULLTEXT, SPATIAL
        if matches!(
            self.peek_at_offset(offset),
            Token::Keyword(Keyword::Unique)
                | Token::Keyword(Keyword::Fulltext)
                | Token::Keyword(Keyword::Spatial)
        ) {
            offset += 1;
        }

        // Skip optional TEMP/TEMPORARY
        if matches!(
            self.peek_at_offset(offset),
            Token::Keyword(Keyword::Temp) | Token::Keyword(Keyword::Temporary)
        ) {
            offset += 1;
        }

        match self.peek_at_offset(offset) {
            Token::Keyword(Keyword::Index) => {
                let stmt = self.parse_create_index_statement()?;
                Ok(Statement::CreateIndex(stmt))
            }
            Token::Keyword(Keyword::View) => {
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
            Token::Keyword(Keyword::Table) => {
                let stmt = self.parse_drop_table_statement()?;
                Ok(Statement::DropTable(stmt))
            }
            Token::Keyword(Keyword::Index) => {
                let stmt = self.parse_drop_index_statement()?;
                Ok(Statement::DropIndex(stmt))
            }
            Token::Keyword(Keyword::View) => {
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
            Err(ParseError { message: format!("Expected {:?}, found {:?}", expected, self.peek()) })
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
            _ => {
                Err(ParseError { message: format!("Expected identifier, found {:?}", self.peek()) })
            }
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
    fn parse_alias_name_symbol(&mut self) -> Result<Symbol, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(self.intern(&name))
            }
            Token::Keyword(kw) => {
                // Allow keywords as alias names
                let name = kw.to_string();
                self.advance();
                Ok(self.intern(&name))
            }
            _ => {
                Err(ParseError { message: format!("Expected alias name, found {:?}", self.peek()) })
            }
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
    let tokens =
        lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

    let mut parser = ArenaParser::new(tokens, &arena);
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
    use super::*;
    use vibesql_ast::arena::Expression;
    use vibesql_types::SqlValue;

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
}
