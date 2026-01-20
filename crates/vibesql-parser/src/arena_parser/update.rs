//! Arena-allocated UPDATE statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{Assignment, ConflictClause, UpdateStmt, WhereClause};

use super::ArenaParser;
use crate::{keywords::Keyword, token::Token, ParseError};

impl<'arena> ArenaParser<'arena> {
    /// Parse an UPDATE statement.
    pub(crate) fn parse_update_statement(
        &mut self,
    ) -> Result<&'arena UpdateStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Update)?;

        // Check for conflict clause: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
        let conflict_clause = if self.try_consume_keyword(Keyword::Or) {
            if self.try_consume_keyword(Keyword::Replace) {
                Some(ConflictClause::Replace)
            } else if self.try_consume_keyword(Keyword::Ignore) {
                Some(ConflictClause::Ignore)
            } else if self.try_consume_keyword(Keyword::Abort) {
                Some(ConflictClause::Abort)
            } else if self.try_consume_keyword(Keyword::Rollback) {
                Some(ConflictClause::Rollback)
            } else if self.try_consume_keyword(Keyword::Fail) {
                Some(ConflictClause::Fail)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after UPDATE OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        // Parse table name and track if quoted (for SQL:1999 case-sensitive lookups)
        let (table_name, quoted) = match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), false)
            }
            Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), true)
            }
            _ => {
                return Err(ParseError { message: "Expected table name after UPDATE".to_string() });
            }
        };

        // Parse optional alias: UPDATE t1 AS alias SET ... or UPDATE t1 alias SET ...
        // SQLite extension for using table alias in UPDATE statements
        let alias = if self.try_consume_keyword(Keyword::As) {
            // AS keyword present, alias required
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(self.intern(&alias_name))
                }
                _ => return Err(ParseError { message: "Expected alias after AS".to_string() }),
            }
        } else if !self.peek_keyword(Keyword::Set) {
            // No AS keyword, but might have alias before SET
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(self.intern(&alias_name))
                }
                _ => None,
            }
        } else {
            None
        };

        // Parse SET keyword
        self.consume_keyword(Keyword::Set)?;

        // Parse assignments
        let assignments = self.parse_assignments()?;

        // Parse optional FROM clause (SQLite 3.33.0+ UPDATE FROM syntax)
        // Syntax: UPDATE t1 SET col = t2.val FROM t2 [, t3 ...] WHERE ...
        let from_clause = if self.try_consume_keyword(Keyword::From) {
            let mut froms = BumpVec::new_in(self.arena);
            loop {
                froms.push(self.parse_from_clause()?);
                if !self.try_consume(&Token::Comma) {
                    break;
                }
            }
            Some(froms)
        } else {
            None
        };

        // Parse optional WHERE clause
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            // Check for WHERE CURRENT OF cursor_name
            if self.try_consume_keyword(Keyword::Current) {
                self.consume_keyword(Keyword::Of)?;
                let cursor_name = if let Token::Identifier(name) = self.peek() {
                    let name = name.clone();
                    self.advance();
                    self.intern(&name)
                } else {
                    return Err(ParseError {
                        message: "Expected cursor name after WHERE CURRENT OF".to_string(),
                    });
                };
                Some(WhereClause::CurrentOf(cursor_name))
            } else {
                let expr = self.parse_expression()?;
                Some(WhereClause::Condition(expr))
            }
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        let stmt = UpdateStmt {
            with_clause: None,
            table_name,
            quoted,
            alias,
            assignments,
            from_clause,
            where_clause,
            conflict_clause,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse assignment list (column = value, ...)
    pub(crate) fn parse_assignments(
        &mut self,
    ) -> Result<BumpVec<'arena, Assignment<'arena>>, ParseError> {
        let mut assignments = BumpVec::new_in(self.arena);

        loop {
            // Parse column name (including rowid keyword for SQLite compatibility)
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let col = col.clone();
                    self.advance();
                    self.intern(&col)
                }
                // SQLite allows updating the virtual rowid column
                Token::Keyword { keyword: Keyword::Rowid, .. } => {
                    self.advance();
                    self.intern("rowid")
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    });
                }
            };

            // Expect =
            self.expect_token(Token::Symbol('='))?;

            // Parse value expression
            let value = self.parse_expression()?;

            assignments.push(Assignment { column, value });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(assignments)
    }
}
