//! Constraint parsing for CREATE TABLE

use super::super::*;

impl Parser {
    /// Parse an optional ON CONFLICT clause for constraints
    /// Returns None if no ON CONFLICT clause is present
    /// Syntax: ON CONFLICT ROLLBACK|ABORT|FAIL|IGNORE|REPLACE
    pub(in crate::parser) fn parse_constraint_conflict_clause(
        &mut self,
    ) -> Result<Option<vibesql_ast::ConflictClause>, ParseError> {
        // Check for ON keyword
        if !self.peek_keyword(Keyword::On) {
            return Ok(None);
        }

        // Peek ahead to see if next is CONFLICT (vs ON DELETE/UPDATE for foreign keys)
        if self.position + 1 < self.tokens.len() {
            if let Token::Keyword { keyword: Keyword::Conflict, .. } =
                &self.tokens[self.position + 1]
            {
                self.advance(); // consume ON
                self.advance(); // consume CONFLICT

                // Parse the conflict resolution algorithm
                if self.peek_keyword(Keyword::Rollback) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Rollback));
                } else if self.peek_keyword(Keyword::Abort) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Abort));
                } else if self.peek_keyword(Keyword::Fail) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Fail));
                } else if self.peek_keyword(Keyword::Ignore) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Ignore));
                } else if self.peek_keyword(Keyword::Replace) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Replace));
                } else {
                    return Err(ParseError {
                        message:
                            "Expected ROLLBACK, ABORT, FAIL, IGNORE, or REPLACE after ON CONFLICT"
                                .to_string(),
                    });
                }
            }
        }

        Ok(None)
    }

    /// Parse an index column specification with optional prefix length or expression
    ///
    /// Supports both simple column references and expression indexes:
    /// - Column: `col1`, `col1(10)` (with prefix length)
    /// - Expression: `(lower(name))`, `(a + b)`
    ///
    /// Syntax: column_name [ ( integer ) ] | ( expression )
    fn parse_index_column_spec(&mut self) -> Result<vibesql_ast::IndexColumn, ParseError> {
        // Check if this is an expression index (starts with parenthesis)
        if self.peek() == &Token::LParen {
            self.advance(); // consume LParen

            // Parse the expression
            let expr = self.parse_expression()?;

            self.expect_token(Token::RParen)?;

            // Check for optional ASC/DESC
            let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                self.advance();
                vibesql_ast::OrderDirection::Asc
            } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                self.advance();
                vibesql_ast::OrderDirection::Desc
            } else {
                vibesql_ast::OrderDirection::Asc
            };

            return Ok(vibesql_ast::IndexColumn::new_expression(expr, direction));
        }

        let column_name = self.parse_identifier()?;

        // Check for optional prefix length: column_name(length)
        let prefix_length = if self.peek() == &Token::LParen {
            self.advance(); // consume LParen

            // Parse the integer length
            let length = match self.peek() {
                Token::Number(n) => {
                    let value = n.parse::<i64>().map_err(|_| ParseError {
                        message: "Invalid integer for column prefix length".to_string(),
                    })?;
                    self.advance();

                    // Validate prefix length range
                    if value < 1 {
                        return Err(ParseError {
                            message: "Prefix length must be at least 1".to_string(),
                        });
                    }
                    if value > 10000 {
                        return Err(ParseError {
                            message: "Prefix length must not exceed 10000".to_string(),
                        });
                    }

                    value
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected integer for column prefix length".to_string(),
                    })
                }
            };

            self.expect_token(Token::RParen)?;
            Some(length as u64)
        } else {
            None
        };

        // Check for optional ASC/DESC
        let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
            self.advance();
            vibesql_ast::OrderDirection::Asc
        } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
            self.advance();
            vibesql_ast::OrderDirection::Desc
        } else {
            vibesql_ast::OrderDirection::Asc // Default for constraints
        };

        Ok(vibesql_ast::IndexColumn::Column { column_name, direction, prefix_length })
    }
    /// Parse ON DELETE/UPDATE referential actions
    fn parse_referential_actions(
        &mut self,
    ) -> Result<
        (Option<vibesql_ast::ReferentialAction>, Option<vibesql_ast::ReferentialAction>),
        ParseError,
    > {
        let mut on_delete = None;
        let mut on_update = None;

        // Parse optional ON DELETE clause
        if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            if self.peek_keyword(Keyword::Delete) {
                self.advance(); // consume DELETE
                on_delete = Some(self.parse_referential_action()?);
            } else if self.peek_keyword(Keyword::Update) {
                self.advance(); // consume UPDATE
                on_update = Some(self.parse_referential_action()?);
            } else {
                return Err(ParseError {
                    message: "Expected DELETE or UPDATE after ON".to_string(),
                });
            }
        }

        // Parse optional second ON clause (if not already parsed above)
        if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            if self.peek_keyword(Keyword::Delete) {
                self.advance(); // consume DELETE
                on_delete = Some(self.parse_referential_action()?);
            } else if self.peek_keyword(Keyword::Update) {
                self.advance(); // consume UPDATE
                on_update = Some(self.parse_referential_action()?);
            } else {
                return Err(ParseError {
                    message: "Expected DELETE or UPDATE after ON".to_string(),
                });
            }
        }

        Ok((on_delete, on_update))
    }

    /// Parse a single referential action (NO ACTION, CASCADE, SET NULL, SET DEFAULT)
    fn parse_referential_action(&mut self) -> Result<vibesql_ast::ReferentialAction, ParseError> {
        if self.peek_keyword(Keyword::No) {
            self.advance(); // consume NO
            self.expect_keyword(Keyword::Action)?;
            Ok(vibesql_ast::ReferentialAction::NoAction)
        } else if self.peek_keyword(Keyword::Cascade) {
            self.advance(); // consume CASCADE
            Ok(vibesql_ast::ReferentialAction::Cascade)
        } else if self.peek_keyword(Keyword::Set) {
            self.advance(); // consume SET
            if self.peek_keyword(Keyword::Null) {
                self.advance(); // consume NULL
                Ok(vibesql_ast::ReferentialAction::SetNull)
            } else if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                Ok(vibesql_ast::ReferentialAction::SetDefault)
            } else {
                Err(ParseError { message: "Expected NULL or DEFAULT after SET".to_string() })
            }
        } else {
            Err(ParseError {
                message: "Expected NO ACTION, CASCADE, SET NULL, or SET DEFAULT".to_string(),
            })
        }
    }

    /// Parse column-level constraints (PRIMARY KEY, UNIQUE, CHECK, REFERENCES)
    pub(in crate::parser) fn parse_column_constraints(
        &mut self,
    ) -> Result<Vec<vibesql_ast::ColumnConstraint>, ParseError> {
        let mut constraints = Vec::new();

        loop {
            // Check for optional CONSTRAINT keyword
            let name = if self.peek_keyword(Keyword::Constraint) {
                self.advance(); // consume CONSTRAINT
                match self.peek() {
                    Token::Identifier(n) => {
                        let constraint_name = n.clone();
                        self.advance();
                        Some(constraint_name)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected constraint name after CONSTRAINT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            match self.peek() {
                Token::Keyword { keyword: Keyword::Null, .. } => {
                    // MySQL allows standalone NULL keyword to explicitly indicate nullable column
                    // (which is the default anyway), so we just consume it and skip it
                    self.advance(); // consume NULL
                                    // This is a no-op - nullable is the default
                }
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    self.advance(); // consume NOT
                    self.expect_keyword(Keyword::Null)?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::NotNull,
                    });
                }
                Token::Keyword { keyword: Keyword::Primary, .. } => {
                    self.advance(); // consume PRIMARY
                    self.expect_keyword(Keyword::Key)?;
                    // SQLite allows optional ASC or DESC after PRIMARY KEY
                    if self.peek_keyword(Keyword::Asc) || self.peek_keyword(Keyword::Desc) {
                        self.advance(); // consume ASC or DESC (ignored - SQLite ignores it too)
                    }
                    // Parse optional ON CONFLICT clause
                    let on_conflict = self.parse_constraint_conflict_clause()?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict },
                    });
                }
                Token::Keyword { keyword: Keyword::Unique, .. } => {
                    self.advance(); // consume UNIQUE
                                    // MySQL allows optional KEY keyword after UNIQUE
                    if self.peek_keyword(Keyword::Key) {
                        self.advance(); // consume KEY
                    }
                    // Parse optional ON CONFLICT clause
                    let on_conflict = self.parse_constraint_conflict_clause()?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Unique { on_conflict },
                    });
                }
                Token::Keyword { keyword: Keyword::Check, .. } => {
                    self.advance(); // consume CHECK
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Check(Box::new(expr)),
                    });
                }
                Token::Keyword { keyword: Keyword::References, .. } => {
                    self.advance(); // consume REFERENCES
                    let table = match self.peek() {
                        Token::Identifier(t) => {
                            let table_name = t.clone();
                            self.advance();
                            table_name
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected table name after REFERENCES".to_string(),
                            })
                        }
                    };

                    // Column specification is optional in SQLite - defaults to PK
                    let column = if self.peek() == &Token::LParen {
                        self.advance(); // consume LParen
                        let col_name = match self.peek() {
                            Token::Identifier(c) => {
                                let name = c.clone();
                                self.advance();
                                name
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected column name in REFERENCES".to_string(),
                                })
                            }
                        };
                        self.expect_token(Token::RParen)?;
                        Some(col_name)
                    } else {
                        None // Column not specified, defaults to PK
                    };

                    let (on_delete, on_update) = self.parse_referential_actions()?;

                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::References {
                            table,
                            column,
                            on_delete,
                            on_update,
                        },
                    });
                }
                Token::Keyword { keyword: Keyword::AutoIncrement, .. } => {
                    self.advance(); // consume AUTO_INCREMENT or AUTOINCREMENT
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::AutoIncrement,
                    });
                }
                Token::Keyword { keyword: Keyword::Key, .. } => {
                    self.advance(); // consume KEY
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Key,
                    });
                }
                Token::Keyword { keyword: Keyword::Collate, .. } => {
                    self.advance(); // consume COLLATE
                    let collation_name = match self.peek() {
                        Token::Identifier(n) | Token::DelimitedIdentifier(n) => {
                            let coll = n.clone();
                            self.advance();
                            coll
                        }
                        Token::Keyword { original, .. } => {
                            // Allow keyword-like collation names (e.g., NOCASE, BINARY)
                            let coll = original.clone();
                            self.advance();
                            coll
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected collation name after COLLATE".to_string(),
                            })
                        }
                    };
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Collate(collation_name),
                    });
                }
                _ => {
                    // If we parsed a CONSTRAINT name but no constraint type, error
                    if name.is_some() {
                        return Err(ParseError {
                            message: "Expected constraint type after CONSTRAINT name".to_string(),
                        });
                    }
                    break;
                }
            }
        }

        Ok(constraints)
    }

    /// Parse table-level constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
    pub(in crate::parser) fn parse_table_constraint(
        &mut self,
    ) -> Result<vibesql_ast::TableConstraint, ParseError> {
        // Check for optional CONSTRAINT keyword
        let name = if self.peek_keyword(Keyword::Constraint) {
            self.advance(); // consume CONSTRAINT
            match self.peek() {
                Token::Identifier(n) => {
                    let constraint_name = n.clone();
                    self.advance();
                    Some(constraint_name)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected constraint name after CONSTRAINT".to_string(),
                    })
                }
            }
        } else {
            None
        };

        let kind = match self.peek() {
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                self.advance(); // consume PRIMARY
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                // Parse optional ON CONFLICT clause
                let on_conflict = self.parse_constraint_conflict_clause()?;
                vibesql_ast::TableConstraintKind::PrimaryKey { columns, on_conflict }
            }
            Token::Keyword { keyword: Keyword::Foreign, .. } => {
                self.advance(); // consume FOREIGN
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in FOREIGN KEY".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                self.expect_keyword(Keyword::References)?;

                let references_table = match self.peek() {
                    Token::Identifier(t) => {
                        let table_name = t.clone();
                        self.advance();
                        table_name
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected table name after REFERENCES".to_string(),
                        })
                    }
                };

                self.expect_token(Token::LParen)?;

                let mut references_columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            references_columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in REFERENCES".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;

                let (on_delete, on_update) = self.parse_referential_actions()?;

                vibesql_ast::TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    on_delete,
                    on_update,
                }
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                self.advance(); // consume UNIQUE
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                // Parse optional ON CONFLICT clause
                let on_conflict = self.parse_constraint_conflict_clause()?;
                vibesql_ast::TableConstraintKind::Unique { columns, on_conflict }
            }
            Token::Keyword { keyword: Keyword::Check, .. } => {
                self.advance(); // consume CHECK
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                vibesql_ast::TableConstraintKind::Check { expr: Box::new(expr) }
            }
            Token::Keyword { keyword: Keyword::Fulltext, .. } => {
                self.advance(); // consume FULLTEXT

                // Optional INDEX keyword
                if self.peek_keyword(Keyword::Index) {
                    self.advance(); // consume INDEX
                }

                // Optional index name
                let index_name = if matches!(self.peek(), Token::Identifier(_)) {
                    // Look ahead to see if next token is LParen
                    if self.position + 1 < self.tokens.len()
                        && matches!(self.tokens[self.position + 1], Token::LParen)
                    {
                        let name = match self.peek() {
                            Token::Identifier(n) => Some(n.clone()),
                            _ => None,
                        };
                        if name.is_some() {
                            self.advance(); // consume index name
                        }
                        name
                    } else {
                        None
                    }
                } else {
                    None
                };

                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                vibesql_ast::TableConstraintKind::Fulltext { index_name, columns }
            }
            _ => return Err(ParseError {
                message:
                    "Expected table constraint keyword (PRIMARY, FOREIGN, UNIQUE, CHECK, FULLTEXT)"
                        .to_string(),
            }),
        };

        Ok(vibesql_ast::TableConstraint { name, kind })
    }
}
