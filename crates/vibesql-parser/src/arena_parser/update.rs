//! Arena-allocated UPDATE statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{Assignment, UpdateStmt, WhereClause};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse an UPDATE statement.
    pub(crate) fn parse_update_statement(
        &mut self,
    ) -> Result<&'arena UpdateStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Update)?;

        // Parse table name
        let table_name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError { message: "Expected table name after UPDATE".to_string() });
        };

        // Parse SET keyword
        self.consume_keyword(Keyword::Set)?;

        // Parse assignments
        let assignments = self.parse_assignments()?;

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

        let stmt = UpdateStmt { table_name, assignments, where_clause };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse assignment list (column = value, ...)
    pub(crate) fn parse_assignments(
        &mut self,
    ) -> Result<BumpVec<'arena, Assignment<'arena>>, ParseError> {
        let mut assignments = BumpVec::new_in(self.arena);

        loop {
            // Parse column name
            let column = if let Token::Identifier(col) = self.peek() {
                let col = col.clone();
                self.advance();
                self.intern(&col)
            } else {
                return Err(ParseError {
                    message: "Expected column name in SET clause".to_string(),
                });
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
