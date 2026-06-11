//! Arena-allocated DELETE statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{DeleteStmt, NullsOrder, OrderByItem, OrderDirection, WhereClause};

use super::ArenaParser;
use crate::{keywords::Keyword, token::Token, ParseError};

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
                return Err(ParseError {
                    message: "Expected table name after DELETE FROM".to_string(),
                });
            }
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

        // Parse optional ORDER BY clause (SQLite extension)
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_delete_order_by_clause()?)
        } else {
            None
        };

        // Parse optional LIMIT clause (SQLite extension, supports comma syntax)
        let (limit, offset_from_limit) = if self.try_consume_keyword(Keyword::Limit) {
            let first_expr = self.parse_expression()?;

            if self.try_consume(&Token::Comma) {
                let second_expr = self.parse_expression()?;
                // LIMIT offset,count syntax
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse optional OFFSET clause (only if not already set via comma syntax)
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.try_consume_keyword(Keyword::Offset) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: DELETE FROM ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.try_consume_keyword(Keyword::Returning) {
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        let stmt = DeleteStmt {
            with_clause: None,
            only,
            table_name,
            quoted,
            where_clause,
            order_by,
            limit,
            offset,
            returning,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse ORDER BY clause for DELETE statement
    fn parse_delete_order_by_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, OrderByItem<'arena>>, ParseError> {
        let mut items = BumpVec::new_in(self.arena);

        loop {
            let expr = self.parse_expression()?;
            let direction = if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                self.try_consume_keyword(Keyword::Asc);
                OrderDirection::Asc
            };

            // Parse optional NULLS FIRST/LAST
            let nulls_order = if self.try_consume_keyword(Keyword::Nulls) {
                if self.try_consume_keyword(Keyword::First) {
                    Some(NullsOrder::First)
                } else if self.try_consume_keyword(Keyword::Last) {
                    Some(NullsOrder::Last)
                } else {
                    return Err(ParseError {
                        message: format!(
                            "Expected FIRST or LAST after NULLS, found {}",
                            self.peek().syntax_error()
                        ),
                    });
                }
            } else {
                None
            };

            items.push(OrderByItem { expr, direction, nulls_order });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }
}
