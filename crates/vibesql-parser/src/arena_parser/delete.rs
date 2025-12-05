//! Arena-allocated DELETE statement parsing.

use vibesql_ast::arena::{DeleteStmt, WhereClause};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    /// Parse a DELETE statement.
    pub(crate) fn parse_delete_statement(
        &mut self,
    ) -> Result<&'arena DeleteStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Delete)?;
        self.consume_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis
        let has_paren = self.try_consume(&Token::LParen);

        // Parse table name
        let table_name = if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            self.intern(&name)
        } else {
            return Err(ParseError {
                message: "Expected table name after DELETE FROM".to_string(),
            });
        };

        // If we had opening paren, expect closing paren
        if has_paren {
            self.expect_token(Token::RParen)?;
        }

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

        let stmt = DeleteStmt { only, table_name, where_clause };

        Ok(self.arena.alloc(stmt))
    }
}
