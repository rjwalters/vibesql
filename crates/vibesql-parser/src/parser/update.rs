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
            // SQLite tuple assignment: `SET (a, b) = (row-value | subquery)`.
            if matches!(self.peek(), Token::LParen) {
                assignments.push(self.parse_tuple_assignment()?);
            } else {
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
                    // SQLite compatibility: a single-quoted string is accepted as
                    // a column name on the left of a SET assignment (it is
                    // followed by `=`, so there is no literal-vs-identifier
                    // ambiguity). quote.test quote-1.4: `UPDATE '@abc' SET '#xyz'=11`.
                    Token::String(col) => {
                        let c = col.clone();
                        self.advance();
                        c
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

                assignments.push(vibesql_ast::Assignment::single(column, value));
            }

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

        // Parse trailing ORDER BY / LIMIT / OFFSET / RETURNING (SQLite extension
        // for UPDATE). RETURNING may appear before or after the ORDER BY / LIMIT /
        // OFFSET trio (issue #5747).
        let super::delete::TrailingDmlClauses { order_by, limit, offset, returning } =
            self.parse_trailing_dml_clauses("UPDATE")?;

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
            // SQLite tuple assignment: `SET (a, b) = (row-value | subquery)`.
            if matches!(self.peek(), Token::LParen) {
                assignments.push(self.parse_tuple_assignment()?);
            } else {
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
                    // SQLite compatibility: single-quoted string as a SET-clause
                    // column name (quote.test quote-1.4).
                    Token::String(col) => {
                        let c = col.clone();
                        self.advance();
                        c
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected column name in SET clause".to_string(),
                        })
                    }
                };

                self.expect_token(Token::Symbol('='))?;
                let value = self.parse_expression()?;

                assignments.push(vibesql_ast::Assignment::single(column, value));
            }

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

        // Parse trailing ORDER BY / LIMIT / OFFSET / RETURNING (SQLite extension
        // for UPDATE). RETURNING may appear before or after the ORDER BY / LIMIT /
        // OFFSET trio (issue #5747).
        let super::delete::TrailingDmlClauses { order_by, limit, offset, returning } =
            self.parse_trailing_dml_clauses("UPDATE")?;

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
