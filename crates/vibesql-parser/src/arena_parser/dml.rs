//! Arena-allocated DML statement parsing (INSERT, UPDATE, DELETE).

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    Assignment, ConflictClause, DeleteStmt, Expression, InsertSource, InsertStmt, UpdateStmt,
    WhereClause,
};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse INSERT statement (including INSERT OR REPLACE).
    pub(crate) fn parse_insert_statement(
        &mut self,
    ) -> Result<InsertStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE | INSERT OR IGNORE
        let conflict_clause = if self.peek_keyword(Keyword::Or) {
            self.advance();
            if self.peek_keyword(Keyword::Replace) {
                self.advance();
                Some(ConflictClause::Replace)
            } else if self.peek_keyword(Keyword::Ignore) {
                self.advance();
                Some(ConflictClause::Ignore)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE or IGNORE after INSERT OR".to_string(),
                });
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = self.parse_arena_identifier()?;

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES or SELECT
        let source = if self.peek_keyword(Keyword::Values) {
            self.consume_keyword(Keyword::Values)?;
            let values = self.parse_values_list()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            let select_stmt = self.parse_select_statement()?;
            InsertSource::Select(select_stmt)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.peek_keyword(Keyword::On) {
            self.advance();
            self.expect_keyword(Keyword::Duplicate)?;
            self.expect_keyword(Keyword::Key)?;
            self.expect_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        Ok(InsertStmt {
            table_name,
            columns,
            source,
            conflict_clause,
            on_duplicate_key_update,
        })
    }

    /// Parse REPLACE statement (alias for INSERT OR REPLACE).
    pub(crate) fn parse_replace_statement(
        &mut self,
    ) -> Result<InsertStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::Replace)?;
        self.expect_keyword(Keyword::Into)?;

        let table_name = self.parse_arena_identifier()?;

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES or SELECT
        let source = if self.peek_keyword(Keyword::Values) {
            self.consume_keyword(Keyword::Values)?;
            let values = self.parse_values_list()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            let select_stmt = self.parse_select_statement()?;
            InsertSource::Select(select_stmt)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after REPLACE".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.peek_keyword(Keyword::On) {
            self.advance();
            self.expect_keyword(Keyword::Duplicate)?;
            self.expect_keyword(Keyword::Key)?;
            self.expect_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        self.try_consume(&Token::Semicolon);

        Ok(InsertStmt {
            table_name,
            columns,
            source,
            conflict_clause: Some(ConflictClause::Replace),
            on_duplicate_key_update,
        })
    }

    /// Parse UPDATE statement.
    pub(crate) fn parse_update_statement(
        &mut self,
    ) -> Result<UpdateStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        let table_name = self.parse_arena_identifier()?;

        self.expect_keyword(Keyword::Set)?;

        let assignments = self.parse_assignments()?;

        // Parse optional WHERE clause
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_arena_identifier()?;
                Some(WhereClause::CurrentOf(cursor_name))
            } else {
                Some(WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        self.try_consume(&Token::Semicolon);

        Ok(UpdateStmt {
            table_name,
            assignments,
            where_clause,
        })
    }

    /// Parse DELETE statement.
    pub(crate) fn parse_delete_statement(
        &mut self,
    ) -> Result<DeleteStmt<'arena>, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis
        let has_paren = self.try_consume(&Token::LParen);

        let table_name = self.parse_arena_identifier()?;

        if has_paren {
            self.expect_token(Token::RParen)?;
        }

        // Parse optional WHERE clause
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_arena_identifier()?;
                Some(WhereClause::CurrentOf(cursor_name))
            } else {
                Some(WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        self.try_consume(&Token::Semicolon);

        Ok(DeleteStmt {
            only,
            table_name,
            where_clause,
        })
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    /// Parse VALUES list for INSERT.
    fn parse_values_list(
        &mut self,
    ) -> Result<BumpVec<'arena, BumpVec<'arena, Expression<'arena>>>, ParseError> {
        let mut values = BumpVec::new_in(self.arena);
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_expression_list()?;
            self.expect_token(Token::RParen)?;
            values.push(row);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    /// Parse assignment list (column = expr, column = expr, ...).
    fn parse_assignments(&mut self) -> Result<BumpVec<'arena, Assignment<'arena>>, ParseError> {
        let mut assignments = BumpVec::new_in(self.arena);
        loop {
            let column = self.parse_arena_identifier()?;
            self.expect_token(Token::Symbol('='))?;
            let value = self.parse_expression()?;
            assignments.push(Assignment { column, value });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }
        Ok(assignments)
    }
}
