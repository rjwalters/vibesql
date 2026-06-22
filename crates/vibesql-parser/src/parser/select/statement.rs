use super::*;

impl Parser {
    /// Parse SELECT statement (public entry point)
    pub(crate) fn parse_select_statement(&mut self) -> Result<vibesql_ast::SelectStmt, ParseError> {
        self.parse_select_statement_internal(true, true, false)
    }

    /// Parse a SELECT statement used as the source of a DML statement
    /// (e.g., INSERT INTO t SELECT ... RETURNING ...).
    ///
    /// Unlike `parse_select_statement`, a trailing RETURNING keyword is accepted
    /// as a valid end token because the clause belongs to the outer DML statement
    /// (issue #5263). Bare SELECT statements must NOT use this entry point, since
    /// SQLite rejects RETURNING outside DML (issue #5271).
    pub(crate) fn parse_dml_source_select_statement(
        &mut self,
    ) -> Result<vibesql_ast::SelectStmt, ParseError> {
        self.parse_select_statement_internal(true, true, true)
    }

    /// Parse SELECT or VALUES statement when embedded in another statement (cursor, view, etc.)
    /// This allows tokens after SELECT/VALUES that belong to the outer statement.
    ///
    /// Supports:
    /// - SELECT ... (standard select statement)
    /// - VALUES(expr, ...) [, ...] (table value constructor, e.g., CREATE VIEW dual AS VALUES('x'))
    pub(crate) fn parse_embedded_select_statement(
        &mut self,
    ) -> Result<vibesql_ast::SelectStmt, ParseError> {
        if self.peek_keyword(Keyword::Values) {
            // Handle VALUES clause as view source (e.g., CREATE VIEW dual(dummy) AS VALUES('x'))
            self.parse_values_statement_internal(true, false, false)
        } else {
            self.parse_select_statement_internal(true, false, false)
        }
    }

    /// Internal SELECT parser with control over ORDER BY/LIMIT parsing
    ///
    /// The `allow_order_limit` parameter controls whether ORDER BY, LIMIT, and OFFSET
    /// are parsed. This is set to false when parsing the right-hand side of set operations
    /// to ensure these clauses only apply to the outermost query.
    ///
    /// The `validate_end_tokens` parameter controls whether to validate that no
    /// unexpected tokens follow the SELECT statement. This is set to false when parsing
    /// SELECT as part of another statement (cursor, view, etc.) where additional tokens
    /// belong to the outer statement.
    ///
    /// The `allow_returning` parameter controls whether a trailing RETURNING keyword
    /// is accepted as a valid end token. This is true only when the SELECT/VALUES is
    /// the source of a DML statement (INSERT INTO t SELECT ... RETURNING ..., issue
    /// #5263). For bare SELECT statements it must be false so that SQLite-incompatible
    /// input like `SELECT 1 RETURNING a` is rejected (issue #5271).
    fn parse_select_statement_internal(
        &mut self,
        allow_order_limit: bool,
        validate_end_tokens: bool,
        allow_returning: bool,
    ) -> Result<vibesql_ast::SelectStmt, ParseError> {
        // Parse optional WITH clause (CTEs)
        let with_clause = if self.peek_keyword(Keyword::With) {
            self.consume_keyword(Keyword::With)?;
            // Check for optional RECURSIVE keyword (SQL:1999, SQLite)
            let recursive = if self.peek_keyword(Keyword::Recursive) {
                self.consume_keyword(Keyword::Recursive)?;
                true
            } else {
                false
            };
            Some(self.parse_cte_list(recursive)?)
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

        // Parse optional WINDOW clause (named window definitions)
        // Example: WINDOW win AS (PARTITION BY x ORDER BY y)
        let window_definitions = if self.peek_keyword(Keyword::Window) {
            self.consume_keyword(Keyword::Window)?;
            Some(self.parse_window_definitions()?)
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

            // Parse the right-hand side SELECT or VALUES statement
            // Don't allow ORDER BY/LIMIT on the right side - they should only apply to the final
            // result
            //
            // Standard SQL allows the right-hand side to be:
            //   - A SELECT statement: SELECT ... UNION ALL SELECT ...
            //   - A VALUES clause: SELECT ... UNION VALUES(1)
            //   - Parenthesized: SELECT ... UNION ALL (SELECT ...) or SELECT ... UNION (VALUES(1))
            let right = if matches!(self.peek(), Token::LParen) {
                self.advance(); // consume '('
                let stmt = if self.peek_keyword(Keyword::Values) {
                    self.parse_values_statement_internal(false, true, false)?
                } else {
                    self.parse_select_statement_internal(false, true, false)?
                };
                if !matches!(self.peek(), Token::RParen) {
                    return Err(ParseError {
                        message: "Expected ')' after parenthesized statement in set operation"
                            .to_string(),
                    });
                }
                self.advance(); // consume ')'
                Box::new(stmt)
            } else if self.peek_keyword(Keyword::Values) {
                Box::new(self.parse_values_statement_internal(false, true, allow_returning)?)
            } else {
                Box::new(self.parse_select_statement_internal(false, true, allow_returning)?)
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

                // Parse optional NULLS FIRST/LAST (SQL:2003 extension)
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

            // Check for too many ORDER BY terms (SQLite compatibility)
            if order_items.len() > super::super::MAX_ORDER_BY_TERMS {
                return Err(ParseError {
                    message: "too many terms in ORDER BY clause".to_string(),
                });
            }

            Some(order_items)
        } else {
            None
        };

        // Check for ORDER BY appearing before a set operation (SQLite-compatible error)
        // This catches: SELECT ... ORDER BY ... UNION SELECT ...
        if order_by.is_some() {
            let op_name = if self.peek_keyword(Keyword::Union) {
                // Check for UNION ALL vs UNION
                let saved = self.position;
                self.consume_keyword(Keyword::Union)?;
                let all = self.peek_keyword(Keyword::All);
                self.position = saved; // restore position
                if all {
                    Some("UNION ALL")
                } else {
                    Some("UNION")
                }
            } else if self.peek_keyword(Keyword::Intersect) {
                Some("INTERSECT")
            } else if self.peek_keyword(Keyword::Except) {
                Some("EXCEPT")
            } else {
                None
            };

            if let Some(op) = op_name {
                return Err(ParseError {
                    message: format!("ORDER BY clause should come after {} not before", op),
                });
            }
        }

        // Parse LIMIT clause
        // SQLite allows expressions in LIMIT (e.g., LIMIT 5+3)
        // SQLite also allows comma syntax: LIMIT offset,count (equivalent to LIMIT count OFFSET
        // offset)
        let (limit, offset_from_limit) = if allow_order_limit && self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;

            // Check for comma syntax: LIMIT offset,count
            if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume comma
                let second_expr = self.parse_expression()?;
                // In comma syntax, first is offset, second is count
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET clause (only if not already set via comma syntax)
        // SQLite allows expressions in OFFSET (e.g., OFFSET 10*2)
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if allow_order_limit && self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Check for LIMIT appearing before a set operation (SQLite-compatible error)
        // This catches: SELECT ... LIMIT ... UNION SELECT ...
        if limit.is_some() || offset.is_some() {
            let op_name = if self.peek_keyword(Keyword::Union) {
                // Check for UNION ALL vs UNION
                let saved = self.position;
                self.consume_keyword(Keyword::Union)?;
                let all = self.peek_keyword(Keyword::All);
                self.position = saved; // restore position
                if all {
                    Some("UNION ALL")
                } else {
                    Some("UNION")
                }
            } else if self.peek_keyword(Keyword::Intersect) {
                Some("INTERSECT")
            } else if self.peek_keyword(Keyword::Except) {
                Some("EXCEPT")
            } else {
                None
            };

            if let Some(op) = op_name {
                return Err(ParseError {
                    message: format!("LIMIT clause should come after {} not before", op),
                });
            }
        }

        // Issue #4448: Reject ORDER BY after LIMIT/OFFSET
        // SQLite rejects: SELECT f1 FROM test1 LIMIT 5 OFFSET 1 ORDER BY f2
        if (limit.is_some() || offset.is_some()) && self.peek_keyword(Keyword::Order) {
            return Err(ParseError { message: self.peek().syntax_error() });
        }

        // Issue #4448: Validate no unexpected tokens before semicolon/EOF
        // This catches incomplete input like: SELECT f1 FROM test1 AS 'hi', test2 AS
        // and unexpected keywords like: SELECT f1 FROM test1 ORDER BY f1 desc, f2 where
        //
        // We only validate when validate_end_tokens is true. When false, we skip
        // validation because the SELECT is embedded in another statement (cursor, view, etc.)
        // that may have additional tokens.
        //
        // When allow_order_limit is false, we're nested inside a set operation and
        // ORDER/LIMIT/OFFSET tokens belong to the outer statement.
        //
        // Even when validating, we allow RParen because SELECT can appear in:
        // - Subqueries in FROM clause
        // - CTEs (WITH ... AS (SELECT ...))
        // - Parenthesized subexpressions
        if validate_end_tokens {
            if allow_order_limit {
                // Top-level: only allow semicolon, EOF, or ) (for subqueries/CTEs).
                // RETURNING is allowed only when the SELECT is the source of
                // INSERT INTO t SELECT ... RETURNING ... (the clause belongs to
                // the outer INSERT, issue #5263). For a bare SELECT, a trailing
                // RETURNING is a syntax error, matching SQLite (issue #5271).
                // ON is likewise allowed only as a DML source so that
                // INSERT INTO t SELECT ... ON CONFLICT ... (SQLite upsert,
                // issue #5269) and ... ON DUPLICATE KEY UPDATE ... parse,
                // while a bare `SELECT 1 ON ...` remains a syntax error.
                let valid_end_token = match self.peek() {
                    Token::Semicolon | Token::Eof | Token::RParen => true,
                    Token::Keyword { keyword: Keyword::Returning, .. } if allow_returning => true,
                    Token::Keyword { keyword: Keyword::On, .. } if allow_returning => true,
                    _ => false,
                };
                if !valid_end_token {
                    return Err(ParseError { message: self.peek().syntax_error() });
                }
            } else {
                // Nested in set operation: also allow ORDER/LIMIT/OFFSET for outer statement
                match self.peek() {
                    Token::Semicolon | Token::Eof | Token::RParen => {}
                    Token::Keyword { keyword: Keyword::Order, .. }
                    | Token::Keyword { keyword: Keyword::Limit, .. }
                    | Token::Keyword { keyword: Keyword::Offset, .. } => {}
                    // RETURNING belongs to an outer DML statement when this SELECT
                    // is the right side of a set operation in its source, e.g.
                    // INSERT INTO t SELECT 1 UNION SELECT 2 RETURNING a (issue #5263)
                    Token::Keyword { keyword: Keyword::Returning, .. } if allow_returning => {}
                    _ => return Err(ParseError { message: self.peek().syntax_error() }),
                }
            }
        }

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
            window_definitions,
            order_by,
            limit,
            offset,
            set_operation,
            values: None,
        })
    }

    /// Parse a comma-separated list of CTEs
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...) [, ...]
    ///
    /// If `recursive` is true, all CTEs in this list are marked as recursive.
    /// In SQL:1999/SQLite, the RECURSIVE keyword applies to all CTEs in the WITH clause.
    pub(crate) fn parse_cte_list(
        &mut self,
        recursive: bool,
    ) -> Result<Vec<vibesql_ast::CommonTableExpr>, ParseError> {
        self.parse_comma_separated_list(|p| p.parse_cte(recursive))
    }

    /// Parse a single CTE definition
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...)
    ///
    /// The `recursive` parameter indicates whether this CTE was declared in a
    /// WITH RECURSIVE clause. In SQL:1999/SQLite, RECURSIVE applies to all CTEs
    /// in the WITH clause, even if they don't actually recurse.
    fn parse_cte(&mut self, recursive: bool) -> Result<vibesql_ast::CommonTableExpr, ParseError> {
        // Parse CTE name
        let name = match self.peek() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                name
            }
            // Allow unreserved keywords (like NULLS, TIMESTAMP, etc.) as CTE names
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                let name = format!("{}", kw).to_lowercase();
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
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let name = col.clone();
                    p.advance();
                    Ok(name)
                }
                // Allow unreserved keywords as column names
                Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                    let name = format!("{}", kw).to_lowercase();
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

        // Parse optional materialization hint: MATERIALIZED or NOT MATERIALIZED
        let materialization = if self.peek_keyword(Keyword::Not) {
            self.advance(); // consume NOT
            self.expect_keyword(Keyword::Materialized)?;
            vibesql_ast::CteMaterialization::NotMaterialized
        } else if self.peek_keyword(Keyword::Materialized) {
            self.advance(); // consume MATERIALIZED
            vibesql_ast::CteMaterialization::Materialized
        } else {
            vibesql_ast::CteMaterialization::Default
        };

        // Expect opening paren for subquery
        if !matches!(self.peek(), Token::LParen) {
            return Err(ParseError {
                message: "Expected '(' after AS in CTE definition".to_string(),
            });
        }
        self.advance(); // consume '('

        // Parse the SELECT or VALUES statement
        // CTE body can be either SELECT or VALUES (both return SelectStmt)
        let query = if self.peek_keyword(Keyword::Values) {
            Box::new(self.parse_values_statement()?)
        } else {
            Box::new(self.parse_select_statement()?)
        };

        // Expect closing paren
        if !matches!(self.peek(), Token::RParen) {
            return Err(ParseError { message: "Expected ')' after CTE query".to_string() });
        }
        self.advance(); // consume ')'

        Ok(vibesql_ast::CommonTableExpr { name, columns, query, recursive, materialization })
    }

    /// Parse a VALUES statement (standalone or in set operations)
    ///
    /// Syntax: VALUES(expr, ...) [, (expr, ...), ...] [set_operation] [ORDER BY] [LIMIT] [OFFSET]
    ///
    /// Examples:
    /// - VALUES(1);
    /// - VALUES(1,2,3);
    /// - VALUES(1),(2),(3);
    /// - VALUES(1) UNION VALUES(2);
    pub(crate) fn parse_values_statement(&mut self) -> Result<vibesql_ast::SelectStmt, ParseError> {
        self.parse_values_statement_internal(true, true, false)
    }

    /// Internal VALUES parser with control over ORDER BY/LIMIT parsing
    ///
    /// The `validate_end_tokens` parameter controls whether to validate that no
    /// unexpected tokens follow the statement.
    ///
    /// The `allow_returning` parameter controls whether a trailing RETURNING keyword
    /// is accepted as a valid end token (only when the VALUES is the source of a DML
    /// statement, issue #5263). For bare VALUES statements it must be false (issue
    /// #5271).
    ///
    /// This is used by INSERT parser to handle compound VALUES statements like:
    /// `INSERT INTO t VALUES(1) UNION VALUES(2)`
    pub(crate) fn parse_values_statement_internal(
        &mut self,
        allow_order_limit: bool,
        validate_end_tokens: bool,
        allow_returning: bool,
    ) -> Result<vibesql_ast::SelectStmt, ParseError> {
        // Parse the VALUES rows
        let rows = self.parse_values_rows()?;

        // Parse set operations (UNION, INTERSECT, EXCEPT)
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

            let all = if self.peek_keyword(Keyword::All) {
                self.consume_keyword(Keyword::All)?;
                true
            } else if self.peek_keyword(Keyword::Distinct) {
                self.consume_keyword(Keyword::Distinct)?;
                false
            } else {
                false
            };

            // Parse the right-hand side (can be SELECT or VALUES)
            let right = if matches!(self.peek(), Token::LParen) {
                self.advance(); // consume '('
                let stmt = if self.peek_keyword(Keyword::Values) {
                    self.parse_values_statement_internal(false, true, false)?
                } else {
                    self.parse_select_statement_internal(false, true, false)?
                };
                if !matches!(self.peek(), Token::RParen) {
                    return Err(ParseError {
                        message: "Expected ')' after parenthesized statement in set operation"
                            .to_string(),
                    });
                }
                self.advance(); // consume ')'
                Box::new(stmt)
            } else if self.peek_keyword(Keyword::Values) {
                Box::new(self.parse_values_statement_internal(false, true, allow_returning)?)
            } else {
                Box::new(self.parse_select_statement_internal(false, true, allow_returning)?)
            };

            Some(vibesql_ast::SetOperation { op, all, right })
        } else {
            None
        };

        // For a compound VALUES source of an INSERT (e.g.
        // `INSERT INTO t VALUES(2) UNION SELECT 3,4 ORDER BY 1`), the trailing
        // ORDER BY/LIMIT/OFFSET applies to the whole compound and must be
        // consumed here. This path is reached with `allow_order_limit=false,
        // validate_end_tokens=false` (set by the INSERT parser), so those
        // tokens would otherwise be left for the outer statement's
        // `expect_statement_end()`, masking the real column-count-mismatch
        // error with a bogus "near ORDER: syntax error" (issue #5714). The
        // nested set-operation right-side calls always pass
        // `validate_end_tokens=true`, so they are unaffected.
        let allow_order_limit = allow_order_limit
            || (!validate_end_tokens && set_operation.is_some());

        // Parse ORDER BY (only if allowed)
        let order_by = if allow_order_limit && self.peek_keyword(Keyword::Order) {
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

                Ok(vibesql_ast::OrderByItem { expr, direction, nulls_order: None })
            })?;

            // Check for too many ORDER BY terms (SQLite compatibility)
            if order_items.len() > super::super::MAX_ORDER_BY_TERMS {
                return Err(ParseError {
                    message: "too many terms in ORDER BY clause".to_string(),
                });
            }

            Some(order_items)
        } else {
            None
        };

        // Check for ORDER BY appearing before a set operation (SQLite-compatible error)
        // This catches: VALUES(...) ORDER BY ... UNION VALUES(...)
        if order_by.is_some() {
            let op_name = if self.peek_keyword(Keyword::Union) {
                // Check for UNION ALL vs UNION
                let saved = self.position;
                self.consume_keyword(Keyword::Union)?;
                let all = self.peek_keyword(Keyword::All);
                self.position = saved; // restore position
                if all {
                    Some("UNION ALL")
                } else {
                    Some("UNION")
                }
            } else if self.peek_keyword(Keyword::Intersect) {
                Some("INTERSECT")
            } else if self.peek_keyword(Keyword::Except) {
                Some("EXCEPT")
            } else {
                None
            };

            if let Some(op) = op_name {
                return Err(ParseError {
                    message: format!("ORDER BY clause should come after {} not before", op),
                });
            }
        }

        // Parse LIMIT (supports comma syntax: LIMIT offset,count)
        let (limit, offset_from_limit) = if allow_order_limit && self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;

            // Check for comma syntax: LIMIT offset,count
            if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume comma
                let second_expr = self.parse_expression()?;
                // In comma syntax, first is offset, second is count
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET (only if not already set via comma syntax)
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if allow_order_limit && self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Issue #4448: Reject ORDER BY after LIMIT/OFFSET
        if (limit.is_some() || offset.is_some()) && self.peek_keyword(Keyword::Order) {
            return Err(ParseError { message: self.peek().syntax_error() });
        }

        // Issue #4448: Validate no unexpected tokens before semicolon/EOF
        // Only validate when validate_end_tokens is true. When false, the VALUES statement
        // is embedded in another statement that may have additional tokens.
        if validate_end_tokens {
            // Note: RParen is valid because VALUES can appear in subqueries/CTEs
            // Note: When allow_order_limit is false (nested in set operation right side),
            //       ORDER/LIMIT/OFFSET may follow and belong to the outer statement
            let valid_end_token = match self.peek() {
                Token::Semicolon | Token::Eof | Token::RParen => true,
                // RETURNING belongs to an outer INSERT (INSERT INTO t VALUES
                // ... RETURNING ..., issue #5263). For a bare VALUES statement a
                // trailing RETURNING is a syntax error, matching SQLite (issue
                // #5271).
                Token::Keyword { keyword: Keyword::Returning, .. } if allow_returning => true,
                // When nested, allow ORDER BY/LIMIT/OFFSET for outer statement
                Token::Keyword { keyword: Keyword::Order, .. }
                | Token::Keyword { keyword: Keyword::Limit, .. }
                | Token::Keyword { keyword: Keyword::Offset, .. }
                    if !allow_order_limit =>
                {
                    true
                }
                _ => false,
            };
            if !valid_end_token {
                return Err(ParseError { message: self.peek().syntax_error() });
            }
        }

        // Consume optional semicolon
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by,
            limit,
            offset,
            set_operation,
            values: Some(rows),
        })
    }
}
