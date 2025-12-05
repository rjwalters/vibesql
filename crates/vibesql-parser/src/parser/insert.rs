use super::*;

impl Parser {
    /// Parse INSERT statement (including INSERT OR REPLACE)
    pub(super) fn parse_insert_statement(&mut self) -> Result<vibesql_ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE | INSERT OR IGNORE
        let conflict_clause = if self.peek_keyword(Keyword::Or) {
            self.advance(); // consume OR
            if self.peek_keyword(Keyword::Replace) {
                self.advance(); // consume REPLACE
                Some(vibesql_ast::ConflictClause::Replace)
            } else if self.peek_keyword(Keyword::Ignore) {
                self.advance(); // consume IGNORE
                Some(vibesql_ast::ConflictClause::Ignore)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE or IGNORE after INSERT OR".to_string(),
                });
            }
        } else {
            None
        };

        self.expect_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after INSERT INTO".to_string(),
                })
            }
        };

        // Parse column list (optional in SQL, but we'll require it for now)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let cols = self.parse_comma_separated_list(|p| match p.peek() {
                Token::Identifier(col) => {
                    let name = col.clone();
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

        // Parse source: VALUES or SELECT
        let source = if self.peek_keyword(Keyword::Values) {
            // Parse VALUES
            self.expect_keyword(Keyword::Values)?;

            // Parse value lists
            let mut values = Vec::new();
            loop {
                self.expect_token(Token::LParen)?;
                let row = self.parse_comma_separated_list(|p| p.parse_expression())?;
                self.expect_token(Token::RParen)?;
                values.push(row);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            vibesql_ast::InsertSource::Values(values)
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
            table_name,
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

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after REPLACE INTO".to_string(),
                })
            }
        };

        // Parse column list (optional)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let mut cols = Vec::new();
            loop {
                match self.peek() {
                    Token::Identifier(col) => {
                        cols.push(col.clone());
                        self.advance();
                    }
                    _ => return Err(ParseError { message: "Expected column name".to_string() }),
                }

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new()
        };

        // Parse source: VALUES or SELECT
        let source = if self.peek_keyword(Keyword::Values) {
            self.expect_keyword(Keyword::Values)?;

            // Parse value lists
            let mut values = Vec::new();
            loop {
                self.expect_token(Token::LParen)?;
                let row = self.parse_comma_separated_list(|p| p.parse_expression())?;
                self.expect_token(Token::RParen)?;
                values.push(row);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            vibesql_ast::InsertSource::Values(values)
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
            table_name,
            columns,
            source,
            conflict_clause: Some(vibesql_ast::ConflictClause::Replace),
            on_duplicate_key_update,
        })
    }
}
