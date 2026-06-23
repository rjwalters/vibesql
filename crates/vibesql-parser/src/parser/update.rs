use super::*;

impl Parser {
    /// Parse UPDATE statement
    pub(super) fn parse_update_statement(&mut self) -> Result<vibesql_ast::UpdateStmt, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        // Check for conflict clause: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
        let conflict_clause = if self.peek_keyword(Keyword::Or) {
            self.advance(); // consume OR
            if self.peek_keyword(Keyword::Replace) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Replace)
            } else if self.peek_keyword(Keyword::Ignore) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Ignore)
            } else if self.peek_keyword(Keyword::Abort) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Abort)
            } else if self.peek_keyword(Keyword::Rollback) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Rollback)
            } else if self.peek_keyword(Keyword::Fail) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Fail)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after UPDATE OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        // Use full_name() to get combined schema.table format for backward compatibility
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

        // Parse optional alias: UPDATE t1 AS alias SET ... or UPDATE t1 alias SET ...
        // SQLite extension for using table alias in UPDATE statements
        let alias = if self.try_consume_keyword(Keyword::As) {
            // AS keyword present, alias required
            Some(self.parse_identifier()?)
        } else if !self.peek_keyword(Keyword::Set) && !self.peek_index_hint() {
            // No AS keyword, but might have alias before SET / index hint.
            // INDEXED / NOT are keywords, so they are not consumed as an alias here.
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(alias_name)
                }
                _ => None,
            }
        } else {
            None
        };

        // Parse optional INDEXED BY / NOT INDEXED hint (SQLite extension).
        // Syntax: UPDATE t1 INDEXED BY idx SET ...
        // Advisory only: VibeSQL's planner chooses indexes independently.
        let index_hint = self.parse_index_hint()?;

        // Parse SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            // Parse column name (support regular, delimited identifiers, and rowid keyword)
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // SQLite allows updating the virtual rowid column
                // SQLite compatibility: the left-hand side of a SET assignment is an
                // unambiguous column-name position (it is followed by `=`), so any
                // keyword here is an unquoted column name. This covers the virtual ROWID
                // column and otherwise-reserved words like RELEASE used as column names
                // (see table.test table-7.3). Normalize to lowercase.
                Token::Keyword { keyword: kw, .. } => {
                    let col_name = format!("{}", kw).to_lowercase();
                    self.advance();
                    col_name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    })
                }
            };

            // Expect =
            self.expect_token(Token::Symbol('='))?;

            // Parse value expression
            let value = self.parse_expression()?;

            assignments.push(vibesql_ast::Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional FROM clause (SQLite 3.33.0+ UPDATE FROM syntax)
        // Syntax: UPDATE t1 SET col = t2.val FROM t2 [, t3 ...] WHERE ...
        let from_clause = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            let mut froms = Vec::new();
            loop {
                froms.push(self.parse_from_clause()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(froms)
        } else {
            None
        };

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

        // Parse optional ORDER BY / LIMIT / OFFSET (SQLite extension for UPDATE).
        // Mirrors the DELETE ... ORDER BY ... LIMIT ... OFFSET path so that
        // UPDATE ... LIMIT n [OFFSET m] restricts how many rows are updated.
        let (order_by, limit, offset) = self.parse_update_order_limit_offset()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: UPDATE ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::UpdateStmt {
            with_clause: None,
            table_name,
            quoted,
            alias,
            index_hint,
            assignments,
            from_clause,
            where_clause,
            order_by,
            limit,
            offset,
            conflict_clause,
            returning,
        })
    }

    /// Parse the optional `ORDER BY ... LIMIT ... OFFSET ...` tail of an UPDATE
    /// statement (SQLite extension). Returns `(order_by, limit, offset)`.
    ///
    /// This mirrors `parse_delete_statement`'s handling so UPDATE and DELETE
    /// accept the same LIMIT/OFFSET syntax, including the `LIMIT offset,count`
    /// comma form.
    #[allow(clippy::type_complexity)]
    fn parse_update_order_limit_offset(
        &mut self,
    ) -> Result<
        (
            Option<Vec<vibesql_ast::OrderByItem>>,
            Option<vibesql_ast::Expression>,
            Option<vibesql_ast::Expression>,
        ),
        ParseError,
    > {
        // Parse optional ORDER BY clause
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

        // Parse optional LIMIT clause (supports `LIMIT offset,count` comma syntax)
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

        Ok((order_by, limit, offset))
    }

    /// Parse UPDATE statement with a pre-parsed WITH clause (CTEs)
    pub(super) fn parse_update_statement_with_cte(
        &mut self,
        with_clause: Vec<vibesql_ast::CommonTableExpr>,
    ) -> Result<vibesql_ast::UpdateStmt, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        // Check for conflict clause: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
        let conflict_clause = if self.peek_keyword(Keyword::Or) {
            self.advance(); // consume OR
            if self.peek_keyword(Keyword::Replace) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Replace)
            } else if self.peek_keyword(Keyword::Ignore) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Ignore)
            } else if self.peek_keyword(Keyword::Abort) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Abort)
            } else if self.peek_keyword(Keyword::Rollback) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Rollback)
            } else if self.peek_keyword(Keyword::Fail) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Fail)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after UPDATE OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        // Parse table name with optional schema qualifier and quoted flag
        let table_ref = self.parse_table_ref()?;
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

        // Parse optional alias: UPDATE t1 AS alias SET ... or UPDATE t1 alias SET ...
        let alias = if self.try_consume_keyword(Keyword::As) {
            Some(self.parse_identifier()?)
        } else if !self.peek_keyword(Keyword::Set) && !self.peek_index_hint() {
            match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let alias_name = name.clone();
                    self.advance();
                    Some(alias_name)
                }
                _ => None,
            }
        } else {
            None
        };

        // Parse optional INDEXED BY / NOT INDEXED hint (SQLite extension).
        // Advisory only: VibeSQL's planner chooses indexes independently.
        let index_hint = self.parse_index_hint()?;

        // Parse SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // SQLite compatibility: the left-hand side of a SET assignment is an
                // unambiguous column-name position (it is followed by `=`), so any
                // keyword here is an unquoted column name. This covers the virtual ROWID
                // column and otherwise-reserved words like RELEASE used as column names
                // (see table.test table-7.3). Normalize to lowercase.
                Token::Keyword { keyword: kw, .. } => {
                    let col_name = format!("{}", kw).to_lowercase();
                    self.advance();
                    col_name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    })
                }
            };

            self.expect_token(Token::Symbol('='))?;
            let value = self.parse_expression()?;

            assignments.push(vibesql_ast::Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional FROM clause (SQLite 3.33.0+ UPDATE FROM syntax)
        // Syntax: UPDATE t1 SET col = t2.val FROM t2 [, t3 ...] WHERE ...
        let from_clause = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            let mut froms = Vec::new();
            loop {
                froms.push(self.parse_from_clause()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(froms)
        } else {
            None
        };

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

        // Parse optional ORDER BY / LIMIT / OFFSET (SQLite extension for UPDATE).
        let (order_by, limit, offset) = self.parse_update_order_limit_offset()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: UPDATE ... [WHERE ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::UpdateStmt {
            with_clause: Some(with_clause),
            table_name,
            quoted,
            alias,
            index_hint,
            assignments,
            from_clause,
            where_clause,
            order_by,
            limit,
            offset,
            conflict_clause,
            returning,
        })
    }
}
