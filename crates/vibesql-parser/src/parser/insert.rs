use super::*;

impl Parser {
    /// Parse INSERT statement (including INSERT OR REPLACE)
    pub(super) fn parse_insert_statement(&mut self) -> Result<vibesql_ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
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
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after INSERT OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::Into)?;

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        let schema_name = table_ref.schema_name;
        let schema_quoted = table_ref.schema_quoted;
        let table_name = table_ref.name;
        let table_quoted = table_ref.quoted;

        // Parse column list (optional in SQL, but we'll require it for now)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let cols = self.parse_comma_separated_list(|p| match p.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let name = col.clone();
                    p.advance();
                    Ok(name)
                }
                // SQLite compatibility: inside an INSERT column list `( ... )` any
                // keyword is an unquoted column name (this includes the virtual ROWID
                // column and otherwise-reserved words like RELEASE, BEGIN, END used as
                // column names — see table.test table-7.3). Normalize to lowercase.
                Token::Keyword { keyword: kw, .. } => {
                    let name = format!("{}", kw).to_lowercase();
                    p.advance();
                    Ok(name)
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new() // No columns specified
        };

        // Parse source: VALUES, SELECT, or DEFAULT VALUES
        // Use parse_values_statement_internal to handle compound VALUES like:
        // INSERT INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Default) {
            // INSERT ... DEFAULT VALUES
            self.advance(); // consume DEFAULT
            self.expect_keyword(Keyword::Values)?;
            vibesql_ast::InsertSource::DefaultValues
        } else if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            // allow_order_limit=false: INSERT VALUES doesn't support ORDER BY/LIMIT
            // validate_end_tokens=false: INSERT has additional tokens after VALUES
            let values_stmt = self.parse_values_statement_internal(false, false, true)?;

            // If there's a set operation (UNION/INTERSECT/EXCEPT), use Select source
            // Otherwise, extract the values directly for the Values source
            if values_stmt.set_operation.is_some() {
                vibesql_ast::InsertSource::Select(Box::new(values_stmt))
            } else {
                // Extract values from the SelectStmt
                // values_stmt.values should be Some for a pure VALUES statement
                match values_stmt.values {
                    Some(values) => vibesql_ast::InsertSource::Values(values),
                    None => {
                        return Err(ParseError {
                            message: "Internal error: VALUES statement missing values".to_string(),
                        })
                    }
                }
            }
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            // Parse SELECT
            let select_stmt = self.parse_dml_source_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON CONFLICT or ON DUPLICATE KEY UPDATE clause
        let (on_conflict, on_duplicate_key_update) = self.parse_on_clause_for_insert()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: INSERT INTO ... [ON CONFLICT ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::InsertStmt {
            with_clause: None,
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause,
            on_conflict,
            on_duplicate_key_update,
            returning,
        })
    }

    /// Parse INSERT statement with a pre-parsed WITH clause (CTEs)
    pub(super) fn parse_insert_statement_with_cte(
        &mut self,
        with_clause: Vec<vibesql_ast::CommonTableExpr>,
    ) -> Result<vibesql_ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
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
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after INSERT OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::Into)?;

        // Parse table name with optional schema qualifier and quoted flag
        let table_ref = self.parse_table_ref()?;
        let schema_name = table_ref.schema_name;
        let schema_quoted = table_ref.schema_quoted;
        let table_name = table_ref.name;
        let table_quoted = table_ref.quoted;

        // Parse column list (optional)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let cols = self.parse_comma_separated_list(|p| match p.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let name = col.clone();
                    p.advance();
                    Ok(name)
                }
                // SQLite compatibility: inside an INSERT column list `( ... )` any
                // keyword is an unquoted column name (this includes the virtual ROWID
                // column and otherwise-reserved words like RELEASE, BEGIN, END used as
                // column names — see table.test table-7.3). Normalize to lowercase.
                Token::Keyword { keyword: kw, .. } => {
                    let name = format!("{}", kw).to_lowercase();
                    p.advance();
                    Ok(name)
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // Parse source: VALUES, SELECT, or DEFAULT VALUES
        // Note: The CTE is already parsed, so the SELECT here should NOT start with WITH
        // Use parse_values_statement_internal to handle compound VALUES like:
        // WITH cte AS (...) INSERT INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Default) {
            // INSERT ... DEFAULT VALUES
            self.advance(); // consume DEFAULT
            self.expect_keyword(Keyword::Values)?;
            vibesql_ast::InsertSource::DefaultValues
        } else if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            let values_stmt = self.parse_values_statement_internal(false, false, true)?;

            // If there's a set operation (UNION/INTERSECT/EXCEPT), use Select source
            if values_stmt.set_operation.is_some() {
                vibesql_ast::InsertSource::Select(Box::new(values_stmt))
            } else {
                match values_stmt.values {
                    Some(values) => vibesql_ast::InsertSource::Values(values),
                    None => {
                        return Err(ParseError {
                            message: "Internal error: VALUES statement missing values".to_string(),
                        })
                    }
                }
            }
        } else if self.peek_keyword(Keyword::Select) {
            // Parse SELECT without consuming WITH (already parsed)
            let select_stmt = self.parse_dml_source_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON CONFLICT or ON DUPLICATE KEY UPDATE clause
        let (on_conflict, on_duplicate_key_update) = self.parse_on_clause_for_insert()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: INSERT INTO ... [ON CONFLICT ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::InsertStmt {
            with_clause: Some(with_clause),
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause,
            on_conflict,
            on_duplicate_key_update,
            returning,
        })
    }

    /// Parse REPLACE statement (alias for INSERT OR REPLACE)
    pub(super) fn parse_replace_statement(
        &mut self,
    ) -> Result<vibesql_ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Replace)?;
        self.expect_keyword(Keyword::Into)?;

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        let schema_name = table_ref.schema_name;
        let schema_quoted = table_ref.schema_quoted;
        let table_name = table_ref.name;
        let table_quoted = table_ref.quoted;

        // Parse column list (optional)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let cols = self.parse_comma_separated_list(|p| match p.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let name = col.clone();
                    p.advance();
                    Ok(name)
                }
                // SQLite compatibility: inside an INSERT column list `( ... )` any
                // keyword is an unquoted column name (this includes the virtual ROWID
                // column and otherwise-reserved words like RELEASE, BEGIN, END used as
                // column names — see table.test table-7.3). Normalize to lowercase.
                Token::Keyword { keyword: kw, .. } => {
                    let name = format!("{}", kw).to_lowercase();
                    p.advance();
                    Ok(name)
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // Parse source: VALUES, SELECT, or DEFAULT VALUES
        // Use parse_values_statement_internal to handle compound VALUES like:
        // REPLACE INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Default) {
            // REPLACE ... DEFAULT VALUES
            self.advance(); // consume DEFAULT
            self.expect_keyword(Keyword::Values)?;
            vibesql_ast::InsertSource::DefaultValues
        } else if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            let values_stmt = self.parse_values_statement_internal(false, false, true)?;

            // If there's a set operation (UNION/INTERSECT/EXCEPT), use Select source
            if values_stmt.set_operation.is_some() {
                vibesql_ast::InsertSource::Select(Box::new(values_stmt))
            } else {
                match values_stmt.values {
                    Some(values) => vibesql_ast::InsertSource::Values(values),
                    None => {
                        return Err(ParseError {
                            message: "Internal error: VALUES statement missing values".to_string(),
                        })
                    }
                }
            }
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            let select_stmt = self.parse_dml_source_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after REPLACE".to_string(),
            });
        };

        // Parse optional ON CONFLICT or ON DUPLICATE KEY UPDATE clause
        let (on_conflict, on_duplicate_key_update) = self.parse_on_clause_for_insert()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: INSERT INTO ... [ON CONFLICT ...] RETURNING expr [, expr ...]
        let returning = if self.peek_keyword(Keyword::Returning) {
            self.consume_keyword(Keyword::Returning)?;
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        Ok(vibesql_ast::InsertStmt {
            with_clause: None,
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause: Some(vibesql_ast::ConflictClause::Replace),
            on_conflict,
            on_duplicate_key_update,
            returning,
        })
    }

    /// Parse ON clause for INSERT statements (handles both ON CONFLICT and ON DUPLICATE KEY UPDATE)
    fn parse_on_clause_for_insert(
        &mut self,
    ) -> Result<
        (Option<vibesql_ast::OnConflictClause>, Option<Vec<vibesql_ast::Assignment>>),
        ParseError,
    > {
        if !self.peek_keyword(Keyword::On) {
            return Ok((None, None));
        }

        self.advance(); // consume ON

        if self.peek_keyword(Keyword::Conflict) {
            // SQLite/PostgreSQL: ON CONFLICT [(cols)] DO {NOTHING | UPDATE SET ...}
            self.advance(); // consume CONFLICT

            // Parse optional conflict target (indexed column list)
            //
            // SQLite's grammar for an upsert conflict target allows each
            // entry to be an arbitrary indexed expression with an optional
            // `COLLATE name` and an optional `ASC | DESC`, plus an optional
            // target-level `WHERE` predicate (partial-index upsert). It also
            // accepts `NULLS FIRST | LAST` syntactically and then rejects it
            // with the canonical "unsupported use of NULLS FIRST/LAST" error.
            // We mirror that behavior so nulls1.test 3.1.11 / 3.1.12 see the
            // expected error message.
            //
            // Plain column names (default BINARY collation) become
            // `ConflictTargetItem::Column`; everything else is retained as a
            // `ConflictTargetItem::Expression` for structural matching
            // against expression indexes (upsert1-200/210). An explicit
            // non-BINARY COLLATE still marks the target "inexact" so the
            // executor reports SQLite's canonical "ON CONFLICT clause does
            // not match any PRIMARY KEY or UNIQUE constraint" error
            // (upsert1-130; issue #5269).
            let mut target_inexact = false;
            let conflict_target = if matches!(self.peek(), Token::LParen) {
                self.advance(); // consume (
                let entries = self.parse_comma_separated_list(|p| {
                    // Parse the entry as a general expression; the expression
                    // parser consumes a trailing COLLATE into
                    // Expression::Collate.
                    let expr = p.parse_expression()?;

                    // Optional ASC | DESC
                    if p.peek_keyword(Keyword::Asc) || p.peek_keyword(Keyword::Desc) {
                        p.advance();
                    }

                    // SQLite accepts NULLS here only to emit a specific
                    // error message.
                    p.reject_nulls_in_index_position()?;

                    // Peel an optional COLLATE wrapper.
                    let (base, collation) = match expr {
                        vibesql_ast::Expression::Collate { expr: inner, collation } => {
                            (*inner, Some(collation))
                        }
                        other => (other, None),
                    };

                    // A non-default (non-BINARY) collation cannot be matched
                    // against an index in v1 (upsert1-130).
                    let inexact = collation
                        .as_deref()
                        .map(|c| !c.eq_ignore_ascii_case("binary"))
                        .unwrap_or(false);

                    let item = match &base {
                        vibesql_ast::Expression::ColumnRef(id)
                            if id.table_canonical().is_none() =>
                        {
                            vibesql_ast::ConflictTargetItem::Column(id.column_display().to_string())
                        }
                        _ => vibesql_ast::ConflictTargetItem::Expression(base),
                    };
                    Ok((item, inexact))
                })?;
                self.expect_token(Token::RParen)?;
                target_inexact = entries.iter().any(|(_, inexact)| *inexact);
                Some(entries.into_iter().map(|(item, _)| item).collect())
            } else {
                None
            };

            // Optional target-level WHERE predicate (partial-index upsert,
            // upsert1-310/320). Retained for structural matching against a
            // partial unique index's predicate.
            let target_where = if conflict_target.is_some() && self.peek_keyword(Keyword::Where) {
                self.advance(); // consume WHERE
                Some(self.parse_expression()?)
            } else {
                None
            };

            self.expect_keyword(Keyword::Do)?;

            let action = if self.peek_keyword(Keyword::Nothing) {
                self.advance(); // consume NOTHING
                vibesql_ast::OnConflictAction::DoNothing
            } else if self.peek_keyword(Keyword::Update) {
                self.advance(); // consume UPDATE
                self.expect_keyword(Keyword::Set)?;

                // Parse assignment list
                let assignments = self.parse_on_conflict_assignments()?;

                // Parse optional WHERE clause
                let where_clause = if self.peek_keyword(Keyword::Where) {
                    self.advance();
                    Some(self.parse_expression()?)
                } else {
                    None
                };

                vibesql_ast::OnConflictAction::DoUpdate { assignments, where_clause }
            } else {
                return Err(ParseError {
                    message: "Expected NOTHING or UPDATE after DO".to_string(),
                });
            };

            Ok((
                Some(vibesql_ast::OnConflictClause {
                    conflict_target,
                    target_where,
                    target_inexact,
                    action,
                }),
                None,
            ))
        } else if self.peek_keyword(Keyword::Duplicate) {
            // MySQL: ON DUPLICATE KEY UPDATE ...
            self.advance(); // consume DUPLICATE
            self.expect_keyword(Keyword::Key)?;
            self.expect_keyword(Keyword::Update)?;

            // Parse assignment list: column = expr, column = expr, ...
            let mut assignments = Vec::new();
            loop {
                let column = match self.peek() {
                    Token::Identifier(col) => {
                        let column_name = col.clone();
                        self.advance();
                        column_name
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected column name in ON DUPLICATE KEY UPDATE".to_string(),
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
            Ok((None, Some(assignments)))
        } else {
            Err(ParseError { message: "Expected CONFLICT or DUPLICATE after ON".to_string() })
        }
    }

    /// Parse assignments for ON CONFLICT ... DO UPDATE SET
    fn parse_on_conflict_assignments(
        &mut self,
    ) -> Result<Vec<vibesql_ast::Assignment>, ParseError> {
        let mut assignments = Vec::new();
        loop {
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let column_name = col.clone();
                    self.advance();
                    column_name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in ON CONFLICT UPDATE".to_string(),
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
        Ok(assignments)
    }
}
