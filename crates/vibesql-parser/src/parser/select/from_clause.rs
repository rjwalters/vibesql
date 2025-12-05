use super::*;

impl Parser {
    /// Parse FROM clause
    pub(crate) fn parse_from_clause(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        // Parse the first table reference
        let mut left = self.parse_table_reference()?;

        // Check for JOINs or commas (left-associative)
        while self.is_join_keyword() || self.peek() == &Token::Comma {
            let (join_type, right, condition, natural) = if self.peek() == &Token::Comma {
                // Comma represents CROSS JOIN
                self.advance(); // Consume comma
                let right = self.parse_table_reference()?;
                (vibesql_ast::JoinType::Cross, right, None, false)
            } else {
                let (join_type, natural) = self.parse_join_type()?;

                // Parse right table reference
                let right = self.parse_table_reference()?;

                // Parse ON condition (comes after table reference)
                // NATURAL JOIN should not have an ON clause
                let condition = if self.peek_keyword(Keyword::On) {
                    if natural {
                        return Err(ParseError {
                            message: "NATURAL JOIN cannot have an ON clause".to_string(),
                        });
                    }
                    self.consume_keyword(Keyword::On)?;
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                (join_type, right, condition, natural)
            };

            // Build JOIN node
            left = vibesql_ast::FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
                natural,
            };
        }

        Ok(left)
    }

    /// Parse a single table reference (table name, subquery, or derived table with alias)
    pub(crate) fn parse_table_reference(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        match self.peek() {
            Token::LParen => {
                // Parenthesized expression: could be a subquery or a JOIN expression
                self.advance(); // Consume '('

                // Check if this is a subquery (starts with SELECT) or a table reference/JOIN
                let result = if self.peek_keyword(Keyword::Select) {
                    // Parse the SELECT statement (subquery)
                    let query = Box::new(self.parse_select_statement()?);

                    // Expect closing ')'
                    match self.peek() {
                        Token::RParen => {
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected ')' after subquery".to_string(),
                            })
                        }
                    }

                    // Support both SQL:1999 (AS required) and MySQL (AS optional) modes
                    // Parse optional AS keyword
                    if self.peek_keyword(Keyword::As) {
                        self.consume_keyword(Keyword::As)?;
                    }

                    // Parse alias (required for derived tables) - keywords allowed as aliases
                    let alias = match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            alias
                        }
                        Token::Keyword(kw) => {
                            // Allow keywords as alias names for derived tables
                            let alias = kw.to_string();
                            self.advance();
                            alias
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Derived table must have an alias".to_string(),
                            })
                        }
                    };

                    // Parse optional column aliases: AS alias (col1, col2, ...)
                    // SQL:1999 Feature E051-09
                    let column_aliases = self.parse_column_alias_list()?;

                    vibesql_ast::FromClause::Subquery { query, alias, column_aliases }
                } else {
                    // Parenthesized table reference or JOIN expression
                    // Parse as a FROM clause (which handles JOINs)
                    let from_clause = self.parse_from_clause()?;

                    // Expect closing ')'
                    match self.peek() {
                        Token::RParen => {
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected ')' after parenthesized table reference"
                                    .to_string(),
                            })
                        }
                    }

                    from_clause
                };

                Ok(result)
            }
            Token::Identifier(_) | Token::DelimitedIdentifier(_) => {
                let name = self.parse_qualified_identifier()?;

                // Check for optional alias
                // Parse optional table alias - keywords allowed after AS (e.g., FROM t AS year)
                let alias = if self.peek_keyword(Keyword::As) {
                    self.consume_keyword(Keyword::As)?;
                    Some(self.parse_alias_name()?)
                } else if matches!(
                    self.peek(),
                    Token::Identifier(_) | Token::DelimitedIdentifier(_)
                ) && !self.is_join_keyword()
                {
                    // Implicit alias (no AS keyword) - but not a JOIN keyword
                    match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                // Parse optional column aliases: AS alias (col1, col2, ...)
                // SQL:1999 Feature E051-09
                // Note: column_aliases requires an alias to be present
                let column_aliases =
                    if alias.is_some() { self.parse_column_alias_list()? } else { None };

                Ok(vibesql_ast::FromClause::Table { name, alias, column_aliases })
            }
            _ => Err(ParseError {
                message: "Expected table name or subquery in FROM clause".to_string(),
            }),
        }
    }

    /// Check if current token is a JOIN keyword
    pub(crate) fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Join)
                | Token::Keyword(Keyword::Inner)
                | Token::Keyword(Keyword::Left)
                | Token::Keyword(Keyword::Right)
                | Token::Keyword(Keyword::Cross)
                | Token::Keyword(Keyword::Full)
                | Token::Keyword(Keyword::Natural)
        )
    }

    /// Parse JOIN type (INNER JOIN, LEFT JOIN, NATURAL JOIN, etc.)
    /// Returns (JoinType, is_natural)
    pub(crate) fn parse_join_type(&mut self) -> Result<(vibesql_ast::JoinType, bool), ParseError> {
        // Check for optional NATURAL keyword first
        let is_natural = if self.peek_keyword(Keyword::Natural) {
            self.consume_keyword(Keyword::Natural)?;
            true
        } else {
            false
        };

        let join_type = match self.peek() {
            Token::Keyword(Keyword::Join) => {
                self.advance();
                vibesql_ast::JoinType::Inner // Default JOIN is INNER JOIN
            }
            Token::Keyword(Keyword::Inner) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Inner
            }
            Token::Keyword(Keyword::Left) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::LeftOuter
            }
            Token::Keyword(Keyword::Right) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::RightOuter
            }
            Token::Keyword(Keyword::Cross) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Cross
            }
            Token::Keyword(Keyword::Full) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::FullOuter
            }
            _ => return Err(ParseError { message: "Expected JOIN keyword".to_string() }),
        };

        // NATURAL CROSS JOIN is not valid in SQL
        if is_natural && join_type == vibesql_ast::JoinType::Cross {
            return Err(ParseError { message: "NATURAL CROSS JOIN is not valid SQL".to_string() });
        }

        Ok((join_type, is_natural))
    }
}
