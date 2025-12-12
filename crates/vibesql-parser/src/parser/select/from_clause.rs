use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

/// Counter for generating unique derived table aliases when none is provided.
/// SQLite allows derived tables without aliases, unlike SQL:1999 which requires them.
static DERIVED_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Parser {
    /// Parse FROM clause
    pub(crate) fn parse_from_clause(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        // Parse the first table reference
        let mut left = self.parse_table_reference()?;

        // Check for JOINs or commas (left-associative)
        while self.is_join_keyword() || self.peek() == &Token::Comma {
            let (join_type, right, condition, using_columns, natural) =
                if self.peek() == &Token::Comma {
                    // Comma normally represents CROSS JOIN, but SQLite's legacy syntax
                    // allows "FROM t1, t2 ON condition" which behaves like INNER JOIN
                    self.advance(); // Consume comma
                    let right = self.parse_table_reference()?;

                    // Check for legacy ON clause after comma-join
                    // SQLite allows: FROM t1, t2 ON t1.a=t2.b (treated as INNER JOIN)
                    if self.peek_keyword(Keyword::On) {
                        self.consume_keyword(Keyword::On)?;
                        let condition = self.parse_expression()?;
                        (vibesql_ast::JoinType::Inner, right, Some(condition), None, false)
                    } else {
                        (vibesql_ast::JoinType::Cross, right, None, None, false)
                    }
                } else {
                    let (join_type, natural) = self.parse_join_type()?;

                    // Parse right table reference
                    let right = self.parse_table_reference()?;

                    // Parse ON condition or USING clause (comes after table reference)
                    // NATURAL JOIN should not have an ON or USING clause
                    let (condition, using_columns) = if self.peek_keyword(Keyword::On) {
                        if natural {
                            return Err(ParseError {
                                message: "NATURAL JOIN cannot have an ON clause".to_string(),
                            });
                        }
                        self.consume_keyword(Keyword::On)?;
                        (Some(self.parse_expression()?), None)
                    } else if self.peek_keyword(Keyword::Using) {
                        if natural {
                            return Err(ParseError {
                                message: "NATURAL JOIN cannot have a USING clause".to_string(),
                            });
                        }
                        self.consume_keyword(Keyword::Using)?;
                        self.expect_token(Token::LParen)?;
                        let columns = self.parse_comma_separated_list(|p| p.parse_identifier())?;
                        self.expect_token(Token::RParen)?;
                        (None, Some(columns))
                    } else {
                        (None, None)
                    };
                    (join_type, right, condition, using_columns, natural)
                };

            // Build JOIN node
            left = vibesql_ast::FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
                using_columns,
                natural,
            };
        }

        Ok(left)
    }

    /// Parse VALUES clause rows: VALUES(1,2), (3,4), ...
    /// Returns Vec<Vec<Expression>> where each inner vec is a row
    pub(crate) fn parse_values_rows(
        &mut self,
    ) -> Result<Vec<Vec<vibesql_ast::Expression>>, ParseError> {
        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_comma_separated_list(|p| p.parse_expression())?;
            self.expect_token(Token::RParen)?;
            rows.push(row);

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(rows)
    }

    /// Parse a single table reference (table name, subquery, or derived table with alias)
    pub(crate) fn parse_table_reference(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        match self.peek() {
            Token::LParen => {
                // Parenthesized expression: could be a subquery, VALUES, or a JOIN expression
                self.advance(); // Consume '('

                // Check if this is a subquery (starts with SELECT), VALUES, or a table
                // reference/JOIN
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

                    // Parse alias - keywords allowed as aliases
                    // SQLite allows derived tables without aliases; auto-generate if not provided
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
                            // Auto-generate unique alias for SQLite compatibility
                            format!(
                                "__derived_{}",
                                DERIVED_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
                            )
                        }
                    };

                    // Parse optional column aliases: AS alias (col1, col2, ...)
                    // SQL:1999 Feature E051-09
                    let column_aliases = self.parse_column_alias_list()?;

                    vibesql_ast::FromClause::Subquery { query, alias, column_aliases }
                } else if self.peek_keyword(Keyword::Values) {
                    // Parse VALUES clause as table constructor
                    // Example: (VALUES(1,'a'), (2,'b')) AS t(x, y)
                    let rows = self.parse_values_rows()?;

                    // Expect closing ')'
                    match self.peek() {
                        Token::RParen => {
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected ')' after VALUES clause".to_string(),
                            })
                        }
                    }

                    // Parse optional AS keyword
                    if self.peek_keyword(Keyword::As) {
                        self.consume_keyword(Keyword::As)?;
                    }

                    // Parse alias (required for VALUES tables) - keywords allowed as aliases
                    let alias = match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            alias
                        }
                        Token::Keyword(kw) => {
                            let alias = kw.to_string();
                            self.advance();
                            alias
                        }
                        _ => {
                            return Err(ParseError {
                                message: "VALUES table must have an alias".to_string(),
                            })
                        }
                    };

                    // Parse optional column aliases: AS alias (col1, col2, ...)
                    let column_aliases = self.parse_column_alias_list()?;

                    vibesql_ast::FromClause::Values { rows, alias, column_aliases }
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
                let table = self.parse_table_ref()?;

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
                } else if matches!(self.peek(), Token::Keyword(_))
                    && !self.is_join_keyword()
                    && !self.is_clause_keyword()
                {
                    // Allow non-reserved keywords as implicit aliases (e.g., FROM t m)
                    // Keywords like M, YEAR, etc. can be used as aliases
                    match self.peek() {
                        Token::Keyword(kw) => {
                            let alias = kw.to_string();
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

                Ok(vibesql_ast::FromClause::Table {
                    name: table.name,
                    alias,
                    column_aliases,
                    quoted: table.quoted,
                })
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

    /// Check if current token is a clause keyword that cannot be used as implicit alias
    /// These keywords start new clauses in SELECT statements
    pub(crate) fn is_clause_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::On)
                | Token::Keyword(Keyword::Where)
                | Token::Keyword(Keyword::Group)
                | Token::Keyword(Keyword::Having)
                | Token::Keyword(Keyword::Order)
                | Token::Keyword(Keyword::Limit)
                | Token::Keyword(Keyword::Offset)
                | Token::Keyword(Keyword::Union)
                | Token::Keyword(Keyword::Intersect)
                | Token::Keyword(Keyword::Except)
                | Token::Keyword(Keyword::Using)
                | Token::Keyword(Keyword::For)
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

        // SQLite allows NATURAL CROSS JOIN, treating it as a regular CROSS JOIN
        // (the NATURAL modifier is effectively ignored for CROSS JOINs).
        // We parse it successfully but set is_natural to false for CROSS JOINs.
        let is_natural = if join_type == vibesql_ast::JoinType::Cross { false } else { is_natural };

        Ok((join_type, is_natural))
    }
}
