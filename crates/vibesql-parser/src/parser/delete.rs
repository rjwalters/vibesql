use super::*;

impl Parser {
    /// Parse DELETE statement
    pub(super) fn parse_delete_statement(&mut self) -> Result<vibesql_ast::DeleteStmt, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis (for DELETE FROM ONLY (table_name) syntax)
        let has_paren = matches!(self.peek(), Token::LParen);
        if has_paren {
            self.advance(); // consume '('
        }

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        // Use full_name() to get combined schema.table format for backward compatibility
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

        // If we had opening paren, expect closing paren
        if has_paren {
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError { message: "Expected ')' after table name".to_string() });
            }
            self.advance(); // consume ')'
        }

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            // Check for WHERE CURRENT OF cursor_name
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_identifier()?;
                Some(vibesql_ast::WhereClause::CurrentOf(cursor_name))
            } else {
                Some(vibesql_ast::WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        // Parse optional ORDER BY clause (SQLite extension)
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            let order_items = self.parse_comma_separated_list(|p| {
                let expr = p.parse_expression()?;
                let direction = if p.peek_keyword(Keyword::Asc) {
                    p.consume_keyword(Keyword::Asc)?;
                    vibesql_ast::OrderDirection::Asc
                } else if p.peek_keyword(Keyword::Desc) {
                    p.consume_keyword(Keyword::Desc)?;
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                let nulls_order = if p.peek_keyword(Keyword::Nulls) {
                    p.consume_keyword(Keyword::Nulls)?;
                    if p.peek_keyword(Keyword::First) {
                        p.consume_keyword(Keyword::First)?;
                        Some(vibesql_ast::NullsOrder::First)
                    } else if p.peek_keyword(Keyword::Last) {
                        p.consume_keyword(Keyword::Last)?;
                        Some(vibesql_ast::NullsOrder::Last)
                    } else {
                        return Err(ParseError {
                            message: format!(
                                "Expected FIRST or LAST after NULLS, found {}",
                                p.peek().syntax_error()
                            ),
                        });
                    }
                } else {
                    None
                };

                Ok(vibesql_ast::OrderByItem { expr, direction, nulls_order })
            })?;

            Some(order_items)
        } else {
            None
        };

        // Parse optional LIMIT clause (SQLite extension, supports comma syntax)
        let (limit, offset_from_limit) = if self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;

            if matches!(self.peek(), Token::Comma) {
                self.advance();
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
        } else if self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: DELETE FROM ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::DeleteStmt {
            with_clause: None,
            only,
            table_name,
            quoted,
            where_clause,
            order_by,
            limit,
            offset,
            returning,
        })
    }

    /// Parse DELETE statement with a pre-parsed WITH clause (CTEs)
    pub(super) fn parse_delete_statement_with_cte(
        &mut self,
        with_clause: Vec<vibesql_ast::CommonTableExpr>,
    ) -> Result<vibesql_ast::DeleteStmt, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis
        let has_paren = matches!(self.peek(), Token::LParen);
        if has_paren {
            self.advance();
        }

        // Parse table name with optional schema qualifier and quoted flag
        let table_ref = self.parse_table_ref()?;
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

        // If we had opening paren, expect closing paren
        if has_paren {
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError { message: "Expected ')' after table name".to_string() });
            }
            self.advance();
        }

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_identifier()?;
                Some(vibesql_ast::WhereClause::CurrentOf(cursor_name))
            } else {
                Some(vibesql_ast::WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        // Parse optional ORDER BY clause (SQLite extension)
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            let order_items = self.parse_comma_separated_list(|p| {
                let expr = p.parse_expression()?;
                let direction = if p.peek_keyword(Keyword::Asc) {
                    p.consume_keyword(Keyword::Asc)?;
                    vibesql_ast::OrderDirection::Asc
                } else if p.peek_keyword(Keyword::Desc) {
                    p.consume_keyword(Keyword::Desc)?;
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                let nulls_order = if p.peek_keyword(Keyword::Nulls) {
                    p.consume_keyword(Keyword::Nulls)?;
                    if p.peek_keyword(Keyword::First) {
                        p.consume_keyword(Keyword::First)?;
                        Some(vibesql_ast::NullsOrder::First)
                    } else if p.peek_keyword(Keyword::Last) {
                        p.consume_keyword(Keyword::Last)?;
                        Some(vibesql_ast::NullsOrder::Last)
                    } else {
                        return Err(ParseError {
                            message: format!(
                                "Expected FIRST or LAST after NULLS, found {}",
                                p.peek().syntax_error()
                            ),
                        });
                    }
                } else {
                    None
                };

                Ok(vibesql_ast::OrderByItem { expr, direction, nulls_order })
            })?;

            Some(order_items)
        } else {
            None
        };

        // Parse optional LIMIT clause (SQLite extension, supports comma syntax)
        let (limit, offset_from_limit) = if self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;

            if matches!(self.peek(), Token::Comma) {
                self.advance();
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
        } else if self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: DELETE FROM ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::DeleteStmt {
            with_clause: Some(with_clause),
            only,
            table_name,
            quoted,
            where_clause,
            order_by,
            limit,
            offset,
            returning,
        })
    }
}
