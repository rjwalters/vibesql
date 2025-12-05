//! Arena-allocated INSERT statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{ConflictClause, InsertSource, InsertStmt};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse an INSERT statement (including INSERT OR REPLACE).
    pub(crate) fn parse_insert_statement(
        &mut self,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE | INSERT OR IGNORE
        let conflict_clause = if self.try_consume_keyword(Keyword::Or) {
            if self.try_consume_keyword(Keyword::Replace) {
                Some(ConflictClause::Replace)
            } else if self.try_consume_keyword(Keyword::Ignore) {
                Some(ConflictClause::Ignore)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE or IGNORE after INSERT OR".to_string(),
                });
            }
        } else {
            None
        };

        self.consume_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError {
                message: "Expected table name after INSERT INTO".to_string(),
            });
        };

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES or SELECT
        let source = if self.try_consume_keyword(Keyword::Values) {
            // Parse VALUES clause
            let values = self.parse_values_clause()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            // Parse SELECT subquery
            let query = self.parse_select_statement()?;
            InsertSource::Select(query)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.try_consume_keyword(Keyword::On) {
            self.consume_keyword(Keyword::Duplicate)?;
            self.consume_keyword(Keyword::Key)?;
            self.consume_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        let stmt =
            InsertStmt { table_name, columns, source, conflict_clause, on_duplicate_key_update };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse REPLACE statement (alias for INSERT OR REPLACE).
    pub(crate) fn parse_replace_statement(
        &mut self,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Replace)?;
        self.consume_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError {
                message: "Expected table name after REPLACE INTO".to_string(),
            });
        };

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES or SELECT
        let source = if self.try_consume_keyword(Keyword::Values) {
            let values = self.parse_values_clause()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            let query = self.parse_select_statement()?;
            InsertSource::Select(query)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after REPLACE".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.try_consume_keyword(Keyword::On) {
            self.consume_keyword(Keyword::Duplicate)?;
            self.consume_keyword(Keyword::Key)?;
            self.consume_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        let stmt = InsertStmt {
            table_name,
            columns,
            source,
            conflict_clause: Some(ConflictClause::Replace),
            on_duplicate_key_update,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse VALUES clause: VALUES (row1), (row2), ...
    fn parse_values_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, BumpVec<'arena, vibesql_ast::arena::Expression<'arena>>>, ParseError>
    {
        let mut rows = BumpVec::new_in(self.arena);

        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_expression_list()?;
            self.expect_token(Token::RParen)?;
            rows.push(row);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(rows)
    }
}
