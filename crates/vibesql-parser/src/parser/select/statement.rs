use super::*;

impl Parser {
    /// Parse SELECT statement (public entry point)
    pub(crate) fn parse_select_statement(&mut self) -> Result<vibesql_ast::SelectStmt, ParseError> {
        self.parse_select_statement_internal(true)
    }

    /// Internal SELECT parser with control over ORDER BY/LIMIT parsing
    ///
    /// The `allow_order_limit` parameter controls whether ORDER BY, LIMIT, and OFFSET
    /// are parsed. This is set to false when parsing the right-hand side of set operations
    /// to ensure these clauses only apply to the outermost query.
    fn parse_select_statement_internal(
        &mut self,
        allow_order_limit: bool,
    ) -> Result<vibesql_ast::SelectStmt, ParseError> {
        // Parse optional WITH clause (CTEs)
        let with_clause = if self.peek_keyword(Keyword::With) {
            self.consume_keyword(Keyword::With)?;
            Some(self.parse_cte_list()?)
        } else {
            None
        };

        self.expect_keyword(Keyword::Select)?;

        // Parse optional set quantifier (DISTINCT or ALL)
        // SQL:1999 syntax: SELECT [ALL | DISTINCT] select_list
        // ALL is the default (include duplicates), DISTINCT removes duplicates
        let distinct = if self.peek_keyword(Keyword::Distinct) {
            self.consume_keyword(Keyword::Distinct)?;
            true
        } else if self.peek_keyword(Keyword::All) {
            self.consume_keyword(Keyword::All)?;
            false // ALL means include duplicates (same as default)
        } else {
            false // Default is ALL (include duplicates)
        };

        // Parse SELECT list
        let select_list = self.parse_select_list()?;

        // Parse optional INTO clause
        // Two forms:
        // 1. DDL SELECT INTO: SELECT * INTO new_table FROM source (SQL:1999 Feature E111)
        // 2. Procedural SELECT INTO: SELECT col1, col2 INTO @var1, @var2 FROM table
        let (into_table, into_variables) = if self.peek_keyword(Keyword::Into) {
            self.consume_keyword(Keyword::Into)?;

            // Check if this is procedural SELECT INTO (variables) or DDL SELECT INTO (table)
            if matches!(self.peek(), Token::UserVariable(_)) {
                // Procedural SELECT INTO: parse comma-separated list of user variables
                let variables = self.parse_comma_separated_list(|p| match p.peek() {
                    Token::UserVariable(var_name) => {
                        let name = var_name.clone();
                        p.advance();
                        Ok(name)
                    }
                    _ => Err(ParseError {
                        message: "Expected user variable (@var) in procedural SELECT INTO"
                            .to_string(),
                    }),
                })?;
                (None, Some(variables))
            } else {
                // DDL SELECT INTO: parse table name
                (Some(self.parse_identifier()?), None)
            }
        } else {
            (None, None)
        };

        // Parse optional FROM clause
        let from = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional GROUP BY clause (with ROLLUP, CUBE, GROUPING SETS support)
        let group_by = if self.peek_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::Group)?;
            self.expect_keyword(Keyword::By)?;

            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse optional HAVING clause (only valid with GROUP BY)
        let having = if self.peek_keyword(Keyword::Having) {
            self.consume_keyword(Keyword::Having)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse set operations (UNION, INTERSECT, EXCEPT) before ORDER BY/LIMIT
        // This ensures ORDER BY/LIMIT apply to the entire set operation result
        let set_operation = if self.peek_keyword(Keyword::Union)
            || self.peek_keyword(Keyword::Intersect)
            || self.peek_keyword(Keyword::Except)
        {
            let op = if self.peek_keyword(Keyword::Union) {
                self.consume_keyword(Keyword::Union)?;
                vibesql_ast::SetOperator::Union
            } else if self.peek_keyword(Keyword::Intersect) {
                self.consume_keyword(Keyword::Intersect)?;
                vibesql_ast::SetOperator::Intersect
            } else {
                self.consume_keyword(Keyword::Except)?;
                vibesql_ast::SetOperator::Except
            };

            // Check for ALL or DISTINCT quantifier (default is DISTINCT if omitted)
            let all = if self.peek_keyword(Keyword::All) {
                self.consume_keyword(Keyword::All)?;
                true
            } else if self.peek_keyword(Keyword::Distinct) {
                self.consume_keyword(Keyword::Distinct)?;
                false // DISTINCT = remove duplicates (same as default)
            } else {
                false // Default behavior is DISTINCT
            };

            // Parse the right-hand side SELECT statement
            // Don't allow ORDER BY/LIMIT on the right side - they should only apply to the final
            // result
            //
            // Standard SQL allows the right-hand SELECT to be wrapped in parentheses:
            //   SELECT ... UNION ALL (SELECT ...)
            // This is commonly used in CTEs and TPC-DS queries.
            let right = if matches!(self.peek(), Token::LParen) {
                self.advance(); // consume '('
                let stmt = self.parse_select_statement_internal(false)?;
                if !matches!(self.peek(), Token::RParen) {
                    return Err(ParseError {
                        message: "Expected ')' after parenthesized SELECT in set operation"
                            .to_string(),
                    });
                }
                self.advance(); // consume ')'
                Box::new(stmt)
            } else {
                Box::new(self.parse_select_statement_internal(false)?)
            };

            Some(vibesql_ast::SetOperation { op, all, right })
        } else {
            None
        };

        // Parse ORDER BY, LIMIT, OFFSET after set operations (only if allowed)
        // These apply to the entire result (including set operations)

        // Parse optional ORDER BY clause
        let order_by = if allow_order_limit && self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            // Parse comma-separated list of order items
            let order_items = self.parse_comma_separated_list(|p| {
                let expr = p.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if p.peek_keyword(Keyword::Asc) {
                    p.consume_keyword(Keyword::Asc)?;
                    vibesql_ast::OrderDirection::Asc
                } else if p.peek_keyword(Keyword::Desc) {
                    p.consume_keyword(Keyword::Desc)?;
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc // Default
                };

                Ok(vibesql_ast::OrderByItem { expr, direction })
            })?;

            Some(order_items)
        } else {
            None
        };

        // Parse LIMIT clause
        let limit = if allow_order_limit && self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            match self.peek() {
                Token::Number(n) => {
                    let limit_value = n.parse::<usize>().map_err(|_| ParseError {
                        message: format!("Invalid LIMIT value: {}", n),
                    })?;

                    self.advance();
                    Some(limit_value)
                }
                _ => return Err(ParseError { message: "Expected number after LIMIT".to_string() }),
            }
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if allow_order_limit && self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            match self.peek() {
                Token::Number(n) => {
                    let offset_value = n.parse::<usize>().map_err(|_| ParseError {
                        message: format!("Invalid OFFSET value: {}", n),
                    })?;
                    self.advance();
                    Some(offset_value)
                }
                _ => {
                    return Err(ParseError { message: "Expected number after OFFSET".to_string() })
                }
            }
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::SelectStmt {
            with_clause,
            distinct,
            select_list,
            into_table,
            into_variables,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_operation,
        })
    }

    /// Parse a comma-separated list of CTEs
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...) [, ...]
    fn parse_cte_list(&mut self) -> Result<Vec<vibesql_ast::CommonTableExpr>, ParseError> {
        self.parse_comma_separated_list(|p| p.parse_cte())
    }

    /// Parse a single CTE definition
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...)
    fn parse_cte(&mut self) -> Result<vibesql_ast::CommonTableExpr, ParseError> {
        // Parse CTE name
        let name = match self.peek() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                name
            }
            _ => return Err(ParseError { message: "Expected CTE name (identifier)".to_string() }),
        };

        // Parse optional column list: (col1, col2, ...)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume '('

            // Check for empty column list
            if matches!(self.peek(), Token::RParen) {
                return Err(ParseError { message: "CTE column list cannot be empty".to_string() });
            }

            // Parse comma-separated list of column identifiers
            let cols = self.parse_comma_separated_list(|p| match p.peek() {
                Token::Identifier(col) => {
                    let name = col.clone();
                    p.advance();
                    Ok(name)
                }
                _ => Err(ParseError {
                    message: "Expected column name in CTE column list".to_string(),
                }),
            })?;

            // Expect closing paren
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError {
                    message: "Expected ')' after CTE column list".to_string(),
                });
            }
            self.advance(); // consume ')'

            Some(cols)
        } else {
            None
        };

        // Expect AS keyword
        self.expect_keyword(Keyword::As)?;

        // Expect opening paren for subquery
        if !matches!(self.peek(), Token::LParen) {
            return Err(ParseError {
                message: "Expected '(' after AS in CTE definition".to_string(),
            });
        }
        self.advance(); // consume '('

        // Parse the SELECT statement
        let query = Box::new(self.parse_select_statement()?);

        // Expect closing paren
        if !matches!(self.peek(), Token::RParen) {
            return Err(ParseError { message: "Expected ')' after CTE query".to_string() });
        }
        self.advance(); // consume ')'

        Ok(vibesql_ast::CommonTableExpr { name, columns, query })
    }
}
