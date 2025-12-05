//! Constraint parsing for CREATE TABLE

use super::super::*;

impl Parser {
    /// Parse a column name with optional prefix length for index/constraint definitions
    /// Syntax: column_name [ ( integer ) ]
    fn parse_index_column_spec(&mut self) -> Result<vibesql_ast::IndexColumn, ParseError> {
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

        Ok(vibesql_ast::IndexColumn {
            column_name,
            direction: vibesql_ast::OrderDirection::Asc, // Default for constraints
            prefix_length,
        })
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
                Token::Keyword(Keyword::Null) => {
                    // MySQL allows standalone NULL keyword to explicitly indicate nullable column
                    // (which is the default anyway), so we just consume it and skip it
                    self.advance(); // consume NULL
                                    // This is a no-op - nullable is the default
                }
                Token::Keyword(Keyword::Not) => {
                    self.advance(); // consume NOT
                    self.expect_keyword(Keyword::Null)?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::NotNull,
                    });
                }
                Token::Keyword(Keyword::Primary) => {
                    self.advance(); // consume PRIMARY
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance(); // consume UNIQUE
                                    // MySQL allows optional KEY keyword after UNIQUE
                    if self.peek_keyword(Keyword::Key) {
                        self.advance(); // consume KEY
                    }
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Unique,
                    });
                }
                Token::Keyword(Keyword::Check) => {
                    self.advance(); // consume CHECK
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Check(Box::new(expr)),
                    });
                }
                Token::Keyword(Keyword::References) => {
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

                    self.expect_token(Token::LParen)?;
                    let column = match self.peek() {
                        Token::Identifier(c) => {
                            let col_name = c.clone();
                            self.advance();
                            col_name
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in REFERENCES".to_string(),
                            })
                        }
                    };
                    self.expect_token(Token::RParen)?;

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
                Token::Keyword(Keyword::AutoIncrement) => {
                    self.advance(); // consume AUTO_INCREMENT or AUTOINCREMENT
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::AutoIncrement,
                    });
                }
                Token::Keyword(Keyword::Key) => {
                    self.advance(); // consume KEY
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Key,
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
            Token::Keyword(Keyword::Primary) => {
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
                vibesql_ast::TableConstraintKind::PrimaryKey { columns }
            }
            Token::Keyword(Keyword::Foreign) => {
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
            Token::Keyword(Keyword::Unique) => {
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
                vibesql_ast::TableConstraintKind::Unique { columns }
            }
            Token::Keyword(Keyword::Check) => {
                self.advance(); // consume CHECK
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                vibesql_ast::TableConstraintKind::Check { expr: Box::new(expr) }
            }
            Token::Keyword(Keyword::Fulltext) => {
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
