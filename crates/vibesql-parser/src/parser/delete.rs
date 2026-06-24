use super::*;

/// Trailing clauses of a DELETE/UPDATE statement: ORDER BY, LIMIT, OFFSET and
/// RETURNING. SQLite accepts these in either order — `ORDER BY ... LIMIT ...
/// RETURNING ...` or `RETURNING ... ORDER BY ... LIMIT ...` — so they are
/// parsed together (issue #5747).
pub(super) struct TrailingDmlClauses {
    pub order_by: Option<Vec<vibesql_ast::OrderByItem>>,
    pub limit: Option<vibesql_ast::Expression>,
    pub offset: Option<vibesql_ast::Expression>,
    pub returning: Option<Vec<vibesql_ast::SelectItem>>,
}

impl Parser {
    /// Parse an ORDER BY clause body (after the ORDER BY keywords).
    fn parse_order_by_items(&mut self) -> Result<Vec<vibesql_ast::OrderByItem>, ParseError> {
        self.parse_comma_separated_list(|p| {
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
        })
    }

    /// Parse the trailing ORDER BY / LIMIT / OFFSET / RETURNING clauses shared by
    /// DELETE and UPDATE. SQLite allows RETURNING to appear either after or before
    /// the ORDER BY / LIMIT / OFFSET trio (issue #5747), so this handles both:
    ///   `... ORDER BY x LIMIT n RETURNING ...`
    ///   `... RETURNING ... ORDER BY x LIMIT n`
    ///
    /// It also enforces SQLite's syntactic constraints:
    ///   - ORDER BY without a LIMIT is rejected.
    ///   - OFFSET without a LIMIT is rejected (`near "OFFSET": syntax error`).
    pub(super) fn parse_trailing_dml_clauses(
        &mut self,
        on_clause: &str,
    ) -> Result<TrailingDmlClauses, ParseError> {
        // First position: ORDER BY / LIMIT / OFFSET (may be absent).
        let (mut order_by, mut limit, mut offset) = self.parse_order_limit_offset(on_clause)?;

        // RETURNING.
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // If RETURNING was present, SQLite also accepts ORDER BY / LIMIT / OFFSET
        // *after* it. Only consume them here if they were not already parsed.
        if returning.is_some()
            && order_by.is_none()
            && limit.is_none()
            && offset.is_none()
            && (self.peek_keyword(Keyword::Order)
                || self.peek_keyword(Keyword::Limit)
                || self.peek_keyword(Keyword::Offset))
        {
            let (ob, lim, off) = self.parse_order_limit_offset(on_clause)?;
            order_by = ob;
            limit = lim;
            offset = off;
        }

        Ok(TrailingDmlClauses { order_by, limit, offset, returning })
    }

    /// Parse an optional ORDER BY clause followed by an optional LIMIT (with
    /// SQLite comma syntax) and optional OFFSET. Rejects ORDER-BY-without-LIMIT
    /// and OFFSET-without-LIMIT to match SQLite's DELETE/UPDATE grammar
    /// (issue #5747). `on_clause` is "DELETE" or "UPDATE" for the error message.
    #[allow(clippy::type_complexity)]
    fn parse_order_limit_offset(
        &mut self,
        on_clause: &str,
    ) -> Result<
        (
            Option<Vec<vibesql_ast::OrderByItem>>,
            Option<vibesql_ast::Expression>,
            Option<vibesql_ast::Expression>,
        ),
        ParseError,
    > {
        // ORDER BY.
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_order_by_items()?)
        } else {
            None
        };

        // LIMIT (supports `LIMIT offset, count` comma syntax).
        let (limit, offset_from_limit) = if self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;
            if matches!(self.peek(), Token::Comma) {
                self.advance();
                let second_expr = self.parse_expression()?;
                // LIMIT offset, count syntax
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // SQLite rejects ORDER BY on DELETE/UPDATE without a LIMIT.
        if order_by.is_some() && limit.is_none() {
            return Err(ParseError { message: format!("ORDER BY without LIMIT on {on_clause}") });
        }

        // OFFSET (only if not already supplied via comma syntax).
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.peek_keyword(Keyword::Offset) {
            // SQLite rejects OFFSET without a preceding LIMIT.
            if limit.is_none() {
                return Err(ParseError { message: "near \"OFFSET\": syntax error".to_string() });
            }
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok((order_by, limit, offset))
    }

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

        // Parse optional alias: DELETE FROM t1 AS a WHERE ... or DELETE FROM t1 a WHERE ...
        // SQLite 3.24+ extension for using a table alias in DELETE statements.
        let alias = self.parse_delete_alias()?;

        // Parse optional INDEXED BY / NOT INDEXED hint (SQLite extension).
        // Syntax: DELETE FROM t1 INDEXED BY idx WHERE ...
        // Advisory only: VibeSQL's planner chooses indexes independently.
        let index_hint = self.parse_index_hint()?;

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

        // Parse trailing ORDER BY / LIMIT / OFFSET / RETURNING (SQLite extension).
        // RETURNING may appear before or after the ORDER BY / LIMIT / OFFSET trio
        // (issue #5747).
        let TrailingDmlClauses { order_by, limit, offset, returning } =
            self.parse_trailing_dml_clauses("DELETE")?;

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::DeleteStmt {
            with_clause: None,
            only,
            table_name,
            quoted,
            alias,
            index_hint,
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

        // Parse optional alias: DELETE FROM t1 AS a WHERE ... or DELETE FROM t1 a WHERE ...
        // SQLite 3.24+ extension for using a table alias in DELETE statements.
        let alias = self.parse_delete_alias()?;

        // Parse optional INDEXED BY / NOT INDEXED hint (SQLite extension).
        // Advisory only: VibeSQL's planner chooses indexes independently.
        let index_hint = self.parse_index_hint()?;

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

        // Parse trailing ORDER BY / LIMIT / OFFSET / RETURNING (SQLite extension).
        // RETURNING may appear before or after the ORDER BY / LIMIT / OFFSET trio
        // (issue #5747).
        let TrailingDmlClauses { order_by, limit, offset, returning } =
            self.parse_trailing_dml_clauses("DELETE")?;

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::DeleteStmt {
            with_clause: Some(with_clause),
            only,
            table_name,
            quoted,
            alias,
            index_hint,
            where_clause,
            order_by,
            limit,
            offset,
            returning,
        })
    }

    /// Parse an optional table alias following the DELETE target table.
    ///
    /// Mirrors the UPDATE alias handling: `DELETE FROM t1 AS a WHERE ...` (AS
    /// keyword present, alias required) and the bare form `DELETE FROM t1 a
    /// WHERE ...`. SQLite 3.24+ allows an alias on the DELETE target.
    ///
    /// Most reserved words (WHERE, ORDER, LIMIT, OFFSET, RETURNING) tokenize as
    /// `Token::Keyword`, so the bare-alias branch never swallows a following
    /// clause keyword as an alias. INDEXED is lexed as an identifier, so the
    /// bare-alias branch additionally skips an `INDEXED BY` / `NOT INDEXED`
    /// hint (mirrors the UPDATE-alias precedent in `parser/update.rs`).
    fn parse_delete_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.try_consume_keyword(Keyword::As) {
            // AS keyword present, alias required.
            Ok(Some(self.parse_identifier()?))
        } else if self.peek_index_hint() {
            // `INDEXED BY ...` / `NOT INDEXED` follows the table, not an alias.
            Ok(None)
        } else {
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Ok(Some(alias_name))
                }
                _ => Ok(None),
            }
        }
    }
}
