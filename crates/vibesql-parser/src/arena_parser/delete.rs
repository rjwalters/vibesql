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

        // Parse optional alias: DELETE FROM t1 AS a WHERE ... or DELETE FROM t1 a WHERE ...
        // SQLite 3.24+ extension for using a table alias in DELETE statements.
        // Reserved words (WHERE, ORDER, LIMIT, OFFSET, RETURNING, etc.) tokenize as
        // keywords, so the bare-alias branch never swallows a following clause keyword.
        let alias = if self.try_consume_keyword(Keyword::As) {
            // AS keyword present, alias required.
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(self.intern(&alias_name))
                }
                _ => return Err(ParseError { message: "Expected alias after AS".to_string() }),
            }
        } else if self.peek_delete_index_hint() {
            // `INDEXED BY ...` / `NOT INDEXED` follows the table, not an alias.
            // INDEXED is lexed as an identifier, so guard against it here.
            None
        } else {
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(self.intern(&alias_name))
                }
                _ => None,
            }
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

        // Parse trailing ORDER BY / LIMIT / OFFSET in the first position.
        let (mut order_by, mut limit, mut offset) = self.parse_delete_order_limit_offset()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: DELETE FROM ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.try_consume_keyword(Keyword::Returning) {
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // SQLite also accepts ORDER BY / LIMIT / OFFSET *after* RETURNING
        // (issue #5747). Only consume them here if they were not already parsed.
        if returning.is_some()
            && order_by.is_none()
            && limit.is_none()
            && offset.is_none()
            && (self.peek_keyword(Keyword::Order)
                || self.peek_keyword(Keyword::Limit)
                || self.peek_keyword(Keyword::Offset))
        {
            let (ob, lim, off) = self.parse_delete_order_limit_offset()?;
            order_by = ob;
            limit = lim;
            offset = off;
        }

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        let stmt = DeleteStmt {
            with_clause: None,
            only,
            table_name,
            quoted,
            alias,
            where_clause,
            order_by,
            limit,
            offset,
            returning,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Whether the next tokens form an `INDEXED BY <name>` / `NOT INDEXED`
    /// hint rather than a bare alias. INDEXED is lexed as an identifier, so the
    /// bare-alias branch must skip it (mirrors `from_clause.rs::peek_index_hint`).
    fn peek_delete_index_hint(&self) -> bool {
        match self.peek() {
            Token::Identifier(id) if id.eq_ignore_ascii_case("indexed") => {
                self.peek_next_keyword(Keyword::By)
            }
            Token::Keyword { keyword: Keyword::Not, .. } => {
                matches!(self.peek_next(), Token::Identifier(id) if id.eq_ignore_ascii_case("indexed"))
            }
            _ => false,
        }
    }

    /// Parse the optional `ORDER BY ... LIMIT ... OFFSET ...` tail of a DELETE
    /// statement (SQLite extension), enforcing SQLite's grammar constraints:
    /// ORDER BY without LIMIT and OFFSET without LIMIT are syntax errors
    /// (issue #5747).
    #[allow(clippy::type_complexity)]
    fn parse_delete_order_limit_offset(
        &mut self,
    ) -> Result<
        (
            Option<BumpVec<'arena, OrderByItem<'arena>>>,
            Option<vibesql_ast::arena::Expression<'arena>>,
            Option<vibesql_ast::arena::Expression<'arena>>,
        ),
        ParseError,
    > {
        // ORDER BY.
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_delete_order_by_clause()?)
        } else {
            None
        };

        // LIMIT (supports `LIMIT offset, count` comma syntax).
        let (limit, offset_from_limit) = if self.try_consume_keyword(Keyword::Limit) {
            let first_expr = self.parse_expression()?;
            if self.try_consume(&Token::Comma) {
                let second_expr = self.parse_expression()?;
                // LIMIT offset, count syntax
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // SQLite rejects ORDER BY on DELETE without a LIMIT.
        if order_by.is_some() && limit.is_none() {
            return Err(ParseError { message: "ORDER BY without LIMIT on DELETE".to_string() });
        }

        // OFFSET (only if not already supplied via comma syntax).
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.peek_keyword(Keyword::Offset) {
            // SQLite rejects OFFSET without a preceding LIMIT.
            if limit.is_none() {
                return Err(ParseError { message: "near \"OFFSET\": syntax error".to_string() });
            }
            self.try_consume_keyword(Keyword::Offset);
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok((order_by, limit, offset))
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
