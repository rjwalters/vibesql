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

        // Check for USING without a preceding JOIN - SQLite error compatibility
        if self.peek_keyword(Keyword::Using) {
            return Err(ParseError {
                message: "a JOIN clause is required before USING".to_string(),
            });
        }

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
                                message: "a NATURAL join may not have an ON or USING clause"
                                    .to_string(),
                            });
                        }
                        self.consume_keyword(Keyword::On)?;
                        (Some(self.parse_expression()?), None)
                    } else if self.peek_keyword(Keyword::Using) {
                        if natural {
                            return Err(ParseError {
                                message: "a NATURAL join may not have an ON or USING clause"
                                    .to_string(),
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

                    // Parse alias - keywords allowed as aliases (except clause keywords)
                    // SQLite allows derived tables without aliases; auto-generate if not provided
                    // SQLite also allows single-quoted strings as aliases (non-standard but common)
                    let alias = match self.peek() {
                        Token::Identifier(id)
                        | Token::DelimitedIdentifier(id)
                        | Token::String(id) => {
                            let alias = id.clone();
                            self.advance();
                            alias
                        }
                        Token::Keyword { keyword: kw, .. } => {
                            // Only consume keyword if it looks like it could be an alias
                            // (not a SQL keyword that starts a new clause)
                            if !matches!(
                                kw,
                                Keyword::Where
                                    | Keyword::Order
                                    | Keyword::Limit
                                    | Keyword::Offset
                                    | Keyword::Group
                                    | Keyword::Having
                                    | Keyword::Union
                                    | Keyword::Intersect
                                    | Keyword::Except
                                    | Keyword::Join
                                    | Keyword::Inner
                                    | Keyword::Left
                                    | Keyword::Right
                                    | Keyword::Full
                                    | Keyword::Cross
                                    | Keyword::Natural
                                    | Keyword::On
                                    | Keyword::Using
                            ) {
                                let alias = kw.to_string();
                                self.advance();
                                alias
                            } else {
                                // Generate default alias for clause keywords
                                format!(
                                    "(subquery-{})",
                                    DERIVED_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
                                )
                            }
                        }
                        _ => {
                            // Auto-generate unique alias for SQLite compatibility
                            format!(
                                "(subquery-{})",
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

                    // Parse alias (optional for VALUES tables) - keywords allowed as aliases
                    // If no alias is provided, generate a default one
                    // SQLite also allows single-quoted strings as aliases (non-standard but common)
                    let alias = match self.peek() {
                        Token::Identifier(id)
                        | Token::DelimitedIdentifier(id)
                        | Token::String(id) => {
                            let alias = id.clone();
                            self.advance();
                            alias
                        }
                        Token::Keyword { keyword: kw, .. } => {
                            // Only consume keyword if it looks like it could be an alias
                            // (not a SQL keyword that starts a new clause)
                            if !matches!(
                                kw,
                                Keyword::Where
                                    | Keyword::Order
                                    | Keyword::Limit
                                    | Keyword::Offset
                                    | Keyword::Group
                                    | Keyword::Having
                                    | Keyword::Union
                                    | Keyword::Intersect
                                    | Keyword::Except
                                    | Keyword::Join
                                    | Keyword::Inner
                                    | Keyword::Left
                                    | Keyword::Right
                                    | Keyword::Full
                                    | Keyword::Cross
                                    | Keyword::Natural
                                    | Keyword::On
                                    | Keyword::Using
                            ) {
                                let alias = kw.to_string();
                                self.advance();
                                alias
                            } else {
                                // Generate default alias when no explicit alias provided
                                "_values_".to_string()
                            }
                        }
                        _ => {
                            // Generate default alias when no explicit alias provided
                            "_values_".to_string()
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
            // SQLite compatibility: single-quoted strings can be used as table names
            Token::Identifier(_) | Token::DelimitedIdentifier(_) | Token::String(_) => {
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
                } else if matches!(self.peek(), Token::Keyword { keyword: _, .. })
                    && !self.is_join_keyword()
                    && !self.is_clause_keyword()
                {
                    // Allow non-reserved keywords as implicit aliases (e.g., FROM t m)
                    // Keywords like M, YEAR, etc. can be used as aliases
                    match self.peek() {
                        Token::Keyword { keyword: kw, .. } => {
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
                    name: table.full_name(),
                    alias,
                    column_aliases,
                    quoted: table.is_any_quoted(),
                })
            }
            // Allow unreserved keywords (like NULLS, TIMESTAMP, etc.) as table names
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                let table = self.parse_table_ref()?;

                // Check for optional alias
                let alias = if self.peek_keyword(Keyword::As) {
                    self.consume_keyword(Keyword::As)?;
                    Some(self.parse_alias_name()?)
                } else if matches!(
                    self.peek(),
                    Token::Identifier(_) | Token::DelimitedIdentifier(_)
                ) && !self.is_join_keyword()
                {
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

                let column_aliases =
                    if alias.is_some() { self.parse_column_alias_list()? } else { None };

                Ok(vibesql_ast::FromClause::Table {
                    name: table.full_name(),
                    alias,
                    column_aliases,
                    quoted: table.is_any_quoted(),
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
            Token::Keyword { keyword: Keyword::Join, .. }
                | Token::Keyword { keyword: Keyword::Inner, .. }
                | Token::Keyword { keyword: Keyword::Left, .. }
                | Token::Keyword { keyword: Keyword::Right, .. }
                | Token::Keyword { keyword: Keyword::Cross, .. }
                | Token::Keyword { keyword: Keyword::Full, .. }
                | Token::Keyword { keyword: Keyword::Natural, .. }
                | Token::Keyword { keyword: Keyword::Outer, .. }
        )
    }

    /// Check if current token is a clause keyword that cannot be used as implicit alias
    /// These keywords start new clauses in SELECT statements
    pub(crate) fn is_clause_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword { keyword: Keyword::On, .. }
                | Token::Keyword { keyword: Keyword::Where, .. }
                | Token::Keyword { keyword: Keyword::Group, .. }
                | Token::Keyword { keyword: Keyword::Having, .. }
                | Token::Keyword { keyword: Keyword::Order, .. }
                | Token::Keyword { keyword: Keyword::Limit, .. }
                | Token::Keyword { keyword: Keyword::Offset, .. }
                | Token::Keyword { keyword: Keyword::Union, .. }
                | Token::Keyword { keyword: Keyword::Intersect, .. }
                | Token::Keyword { keyword: Keyword::Except, .. }
                | Token::Keyword { keyword: Keyword::Using, .. }
                | Token::Keyword { keyword: Keyword::For, .. }
        )
    }

    /// Parse JOIN type (INNER JOIN, LEFT JOIN, NATURAL JOIN, etc.)
    /// Returns (JoinType, is_natural)
    ///
    /// SQLite supports NATURAL in various positions:
    /// - NATURAL JOIN, NATURAL LEFT JOIN, NATURAL LEFT OUTER JOIN
    /// - LEFT NATURAL JOIN, LEFT OUTER NATURAL JOIN
    /// - OUTER LEFT NATURAL JOIN
    ///
    /// Invalid combinations like "INNER OUTER" or "LEFT BOGUS" produce:
    /// "unknown join type: INNER OUTER" (SQLite-compatible error message)
    pub(crate) fn parse_join_type(&mut self) -> Result<(vibesql_ast::JoinType, bool), ParseError> {
        // Track consumed join type keywords for error messages
        let mut consumed_keywords: Vec<String> = Vec::new();

        // Check for optional NATURAL keyword first
        let mut is_natural = if self.peek_keyword(Keyword::Natural) {
            self.consume_keyword(Keyword::Natural)?;
            consumed_keywords.push("NATURAL".to_string());
            true
        } else {
            false
        };

        let join_type = match self.peek() {
            Token::Keyword { keyword: Keyword::Join, .. } => {
                self.advance();
                vibesql_ast::JoinType::Inner // Default JOIN is INNER JOIN
            }
            Token::Keyword { keyword: Keyword::Inner, .. } => {
                self.advance();
                consumed_keywords.push("INNER".to_string());
                // Check for NATURAL after INNER (INNER NATURAL JOIN)
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                // Check for invalid combinations like "INNER OUTER" or "INNER BOGUS"
                if let Token::Keyword { keyword: kw, .. } = self.peek() {
                    if *kw != Keyword::Join && *kw != Keyword::Natural {
                        // Consume remaining non-JOIN keywords to build error message
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        // If we ended on JOIN, that's part of an invalid combination
                        if self.peek_keyword(Keyword::Join) {
                            // We have something like "INNER OUTER JOIN" - invalid
                            return Err(ParseError {
                                message: format!(
                                    "unknown join type: {}",
                                    consumed_keywords.join(" ")
                                ),
                            });
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                } else if let Token::Identifier(id) = self.peek() {
                    // Handle "INNER BOGUS" where BOGUS is an identifier
                    consumed_keywords.push(id.to_uppercase());
                    self.advance();
                    // Consume any more keywords/identifiers before JOIN
                    loop {
                        match self.peek() {
                            Token::Keyword { keyword: next_kw, .. }
                                if *next_kw != Keyword::Join =>
                            {
                                consumed_keywords.push(next_kw.to_string().to_uppercase());
                                self.advance();
                            }
                            Token::Identifier(id2) => {
                                consumed_keywords.push(id2.to_uppercase());
                                self.advance();
                            }
                            _ => break,
                        }
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Inner
            }
            Token::Keyword { keyword: Keyword::Left, .. } => {
                self.advance();
                consumed_keywords.push("LEFT".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after LEFT [OUTER] (LEFT NATURAL JOIN, LEFT OUTER NATURAL JOIN)
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                // Check for invalid combinations like "LEFT BOGUS"
                if let Token::Keyword { keyword: kw, .. } = self.peek() {
                    if *kw != Keyword::Join && *kw != Keyword::Natural && *kw != Keyword::Outer {
                        // Consume remaining non-JOIN keywords to build error message
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                } else if let Token::Identifier(id) = self.peek() {
                    // Handle "LEFT BOGUS" where BOGUS is an identifier
                    consumed_keywords.push(id.to_uppercase());
                    self.advance();
                    // Consume any more keywords before JOIN
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::LeftOuter
            }
            Token::Keyword { keyword: Keyword::Right, .. } => {
                self.advance();
                consumed_keywords.push("RIGHT".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after RIGHT [OUTER]
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::RightOuter
            }
            Token::Keyword { keyword: Keyword::Cross, .. } => {
                self.advance();
                consumed_keywords.push("CROSS".to_string());
                // Check for NATURAL after CROSS
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Cross
            }
            Token::Keyword { keyword: Keyword::Full, .. } => {
                self.advance();
                consumed_keywords.push("FULL".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after FULL [OUTER]
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::FullOuter
            }
            // Support "OUTER LEFT/RIGHT/FULL JOIN" syntax (SQLite compatibility)
            Token::Keyword { keyword: Keyword::Outer, .. } => {
                self.advance(); // Consume OUTER
                consumed_keywords.push("OUTER".to_string());
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Left, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER LEFT
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::LeftOuter
                    }
                    Token::Keyword { keyword: Keyword::Right, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER RIGHT
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::RightOuter
                    }
                    Token::Keyword { keyword: Keyword::Full, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER FULL
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::FullOuter
                    }
                    Token::Keyword { keyword: Keyword::Join, .. } => {
                        // "OUTER JOIN" without LEFT/RIGHT/FULL defaults to LEFT OUTER JOIN
                        self.advance();
                        vibesql_ast::JoinType::LeftOuter
                    }
                    Token::Keyword { keyword: kw, .. } => {
                        // Invalid combination like "OUTER NATURAL INNER"
                        consumed_keywords.push(kw.to_string().to_uppercase());
                        self.advance();
                        // Consume any more keywords before JOIN
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                    _ => {
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        })
                    }
                }
            }
            _ => {
                // Handle "NATURAL AWK SED JOIN" and similar
                if !consumed_keywords.is_empty() {
                    // We already consumed NATURAL, now we see something unexpected
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    while let Token::Identifier(id) = self.peek() {
                        consumed_keywords.push(id.to_uppercase());
                        self.advance();
                    }
                    // Keep consuming keywords until JOIN
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                return Err(ParseError { message: "Expected JOIN keyword".to_string() });
            }
        };

        // NATURAL CROSS JOIN is valid SQL and should apply the natural join condition
        // (matching on common column names). The executor handles this by treating
        // NATURAL CROSS JOIN as an inner join with the natural join condition.
        Ok((join_type, is_natural))
    }
}
