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
                // SQLite allows inserting into the virtual rowid column
                Token::Keyword { keyword: Keyword::Rowid, .. } => {
                    p.advance();
                    Ok("rowid".to_string())
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new() // No columns specified
        };

        // Parse source: VALUES or SELECT
        // Use parse_values_statement_internal to handle compound VALUES like:
        // INSERT INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            // allow_order_limit=false: INSERT VALUES doesn't support ORDER BY/LIMIT
            // validate_end_tokens=false: INSERT has additional tokens after VALUES
            let values_stmt = self.parse_values_statement_internal(false, false)?;

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
            let select_stmt = self.parse_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            self.expect_keyword(Keyword::Duplicate)?;
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
            Some(assignments)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::InsertStmt {
            with_clause: None,
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause,
            on_duplicate_key_update,
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
                // SQLite allows inserting into the virtual rowid column
                Token::Keyword { keyword: Keyword::Rowid, .. } => {
                    p.advance();
                    Ok("rowid".to_string())
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // Parse source: VALUES or SELECT
        // Note: The CTE is already parsed, so the SELECT here should NOT start with WITH
        // Use parse_values_statement_internal to handle compound VALUES like:
        // WITH cte AS (...) INSERT INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            let values_stmt = self.parse_values_statement_internal(false, false)?;

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
            let select_stmt = self.parse_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            self.expect_keyword(Keyword::Duplicate)?;
            self.expect_keyword(Keyword::Key)?;
            self.expect_keyword(Keyword::Update)?;

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
            Some(assignments)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::InsertStmt {
            with_clause: Some(with_clause),
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause,
            on_duplicate_key_update,
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
                // SQLite allows inserting into the virtual rowid column
                Token::Keyword { keyword: Keyword::Rowid, .. } => {
                    p.advance();
                    Ok("rowid".to_string())
                }
                _ => Err(ParseError { message: "Expected column name".to_string() }),
            })?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // Parse source: VALUES or SELECT
        // Use parse_values_statement_internal to handle compound VALUES like:
        // REPLACE INTO t VALUES(1) UNION VALUES(2)
        let source = if self.peek_keyword(Keyword::Values) {
            // Parse VALUES using the full statement parser to handle compound operators
            let values_stmt = self.parse_values_statement_internal(false, false)?;

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
            let select_stmt = self.parse_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after REPLACE".to_string(),
            });
        };

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            self.expect_keyword(Keyword::Duplicate)?;
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
            Some(assignments)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::InsertStmt {
            with_clause: None,
            schema_name,
            schema_quoted,
            table_name,
            table_quoted,
            columns,
            source,
            conflict_clause: Some(vibesql_ast::ConflictClause::Replace),
            on_duplicate_key_update,
        })
    }
}
