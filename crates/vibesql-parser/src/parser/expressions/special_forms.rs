use super::*;

impl Parser {
    /// Parse special SQL forms (CASE, CAST, EXISTS, NOT EXISTS, CURRENT_DATE/TIME/TIMESTAMP)
    pub(super) fn parse_special_form(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (as identifiers)
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_DATE" => {
                let orig_name = id.clone();
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new(&orig_name),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIME" => {
                let orig_name = id.clone();
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new(&orig_name),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIMESTAMP" => {
                let orig_name = id.clone();
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new(&orig_name),
                    args: vec![],
                    character_unit: None,
                }))
            }
            // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (multi-token form)
            // The lexer tokenizes CURRENT_DATE as two tokens when CURRENT is a keyword:
            //   Token::Keyword { keyword: Current, .. } + Token::Identifier("_DATE")
            // This branch handles that tokenization pattern.
            Token::Keyword { keyword: Keyword::Current, .. } => {
                self.advance(); // consume CURRENT

                // Check for underscore followed by DATE/TIME/TIMESTAMP
                if let Token::Identifier(ref id) = self.peek() {
                    // Build the original name by combining CURRENT with the suffix
                    let suffix = id.clone();
                    match id.to_uppercase().as_str() {
                        "_DATE" => {
                            self.advance(); // consume _DATE
                            let function_name = format!("CURRENT{}", suffix);
                            Ok(Some(vibesql_ast::Expression::Function {
                                name: vibesql_ast::FunctionIdentifier::new(&function_name),
                                args: vec![],
                                character_unit: None,
                            }))
                        }
                        "_TIME" => {
                            self.advance(); // consume _TIME
                            let function_name = format!("CURRENT{}", suffix);
                            Ok(Some(vibesql_ast::Expression::Function {
                                name: vibesql_ast::FunctionIdentifier::new(&function_name),
                                args: vec![],
                                character_unit: None,
                            }))
                        }
                        "_TIMESTAMP" => {
                            self.advance(); // consume _TIMESTAMP
                            let function_name = format!("CURRENT{}", suffix);
                            Ok(Some(vibesql_ast::Expression::Function {
                                name: vibesql_ast::FunctionIdentifier::new(&function_name),
                                args: vec![],
                                character_unit: None,
                            }))
                        }
                        _ => {
                            // Not a CURRENT_DATE/TIME/TIMESTAMP — treat CURRENT as
                            // a regular identifier (column name). CURRENT is already
                            // listed in can_be_identifier().
                            Ok(Some(vibesql_ast::Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::simple("current", false),
                            )))
                        }
                    }
                } else {
                    // Next token is not an identifier (e.g., comma, RParen) —
                    // treat CURRENT as a column name reference.
                    Ok(Some(vibesql_ast::Expression::ColumnRef(
                        vibesql_ast::ColumnIdentifier::simple("current", false),
                    )))
                }
            }
            // CAST expression: CAST(expr AS data_type)
            // CASE expression: both simple and searched forms
            Token::Keyword { keyword: Keyword::Case, .. } => {
                self.advance(); // consume CASE

                // Try to parse operand for simple CASE
                // If next token is WHEN, it's a searched CASE (no operand)
                let operand = if !self.peek_keyword(Keyword::When) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Parse WHEN clauses
                let mut when_clauses = Vec::new();
                while self.peek_keyword(Keyword::When) {
                    self.advance(); // consume WHEN

                    // Parse first condition
                    let mut conditions = vec![self.parse_expression()?];

                    // Parse additional comma-separated conditions
                    while matches!(self.peek(), Token::Comma) {
                        self.advance(); // consume comma
                        conditions.push(self.parse_expression()?);
                    }

                    self.expect_keyword(Keyword::Then)?;
                    let result = self.parse_expression()?;

                    when_clauses.push(vibesql_ast::CaseWhen { conditions, result });
                }

                // Ensure at least one WHEN clause exists
                if when_clauses.is_empty() {
                    return Err(ParseError {
                        message: "CASE expression requires at least one WHEN clause".to_string(),
                    });
                }

                // Parse optional ELSE clause
                let else_result = if self.peek_keyword(Keyword::Else) {
                    self.advance(); // consume ELSE
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Expect END keyword
                self.expect_keyword(Keyword::End)?;

                Ok(Some(vibesql_ast::Expression::Case { operand, when_clauses, else_result }))
            }
            Token::Keyword { keyword: Keyword::Cast, .. } => {
                self.advance(); // consume CAST

                // Expect opening parenthesis
                self.expect_token(Token::LParen)?;

                // Parse the expression to cast
                let expr = self.parse_expression()?;

                // Expect AS keyword
                self.expect_keyword(Keyword::As)?;

                // Parse the target data type.
                // SQLite tolerates a missing type name: CAST(x AS ) gets
                // NUMERIC affinity, same as an unrecognized type name
                // (sqlite3AffinityType("") falls through to NUMERIC).
                // Verified against SQLite 3.51: CAST('seventeen' AS ) → 0
                // (typeof integer), CAST('5.5' AS ) → 5.5 (typeof real).
                let data_type = if matches!(self.peek(), Token::RParen) {
                    vibesql_types::DataType::Numeric { precision: 38, scale: 0 }
                } else {
                    self.parse_data_type()?
                };

                // Expect closing parenthesis
                self.expect_token(Token::RParen)?;

                Ok(Some(vibesql_ast::Expression::Cast { expr: Box::new(expr), data_type }))
            }
            // EXISTS expression: EXISTS (SELECT ...)
            Token::Keyword { keyword: Keyword::Exists, .. } => {
                self.advance(); // consume EXISTS

                // Expect opening parenthesis
                self.expect_token(Token::LParen)?;

                // Parse the subquery (parse_select_statement will consume SELECT keyword)
                let subquery = self.parse_select_statement()?;

                // Expect closing parenthesis
                self.expect_token(Token::RParen)?;

                Ok(Some(vibesql_ast::Expression::Exists {
                    subquery: Box::new(subquery),
                    negated: false,
                }))
            }
            // DEFAULT keyword: DEFAULT
            Token::Keyword { keyword: Keyword::Default, .. } => {
                self.advance(); // consume DEFAULT
                Ok(Some(vibesql_ast::Expression::Default))
            }
            // NOT keyword - could be NOT EXISTS or unary NOT
            Token::Keyword { keyword: Keyword::Not, .. } => {
                self.advance(); // consume NOT

                // Check if it's NOT EXISTS
                if self.peek_keyword(Keyword::Exists) {
                    self.advance(); // consume EXISTS

                    // Expect opening parenthesis
                    self.expect_token(Token::LParen)?;

                    // Parse the subquery
                    let subquery = self.parse_select_statement()?;

                    // Expect closing parenthesis
                    self.expect_token(Token::RParen)?;

                    Ok(Some(vibesql_ast::Expression::Exists {
                        subquery: Box::new(subquery),
                        negated: true,
                    }))
                } else {
                    // It's a unary NOT operator on another expression
                    // Parse the inner expression (including unary operators like +/-)
                    let expr = self.parse_unary_expression()?;

                    Ok(Some(vibesql_ast::Expression::UnaryOp {
                        op: vibesql_ast::UnaryOperator::Not,
                        expr: Box::new(expr),
                    }))
                }
            }
            // INTERVAL expression: INTERVAL '5' DAY, INTERVAL '1-6' YEAR TO MONTH
            Token::Keyword { keyword: Keyword::Interval, .. } => {
                self.advance(); // consume INTERVAL

                // Parse the value expression (typically a string literal)
                let value = self.parse_primary_expression()?;

                // Parse the interval unit
                let unit = self.parse_interval_unit()?;

                Ok(Some(vibesql_ast::Expression::Interval {
                    value: Box::new(value),
                    unit,
                    leading_precision: None,
                    fractional_precision: None,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Parse current date/time functions (CURRENT_DATE, CURRENT_TIME[(precision)],
    /// CURRENT_TIMESTAMP[(precision)])
    pub(super) fn parse_current_datetime_function(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            Token::Keyword { keyword: Keyword::CurrentDate, .. } => {
                self.advance(); // consume CURRENT_DATE
                Ok(Some(vibesql_ast::Expression::CurrentDate))
            }
            Token::Keyword { keyword: Keyword::CurrentTime, .. } => {
                self.advance(); // consume CURRENT_TIME
                let precision = if self.try_consume(&Token::LParen) {
                    let prec_str = match self.peek() {
                        Token::Number(n) => n.clone(),
                        _ => {
                            return Err(ParseError {
                                message: "Expected integer precision for CURRENT_TIME".to_string(),
                            })
                        }
                    };
                    let prec: u32 = prec_str.parse().map_err(|_| ParseError {
                        message: format!("Invalid precision value: {}", prec_str),
                    })?;
                    if prec > 9 {
                        return Err(ParseError {
                            message: format!(
                                "CURRENT_TIME precision must be between 0 and 9, got {}",
                                prec
                            ),
                        });
                    }
                    self.advance(); // consume the number
                    self.expect_token(Token::RParen)?;
                    Some(prec)
                } else {
                    None
                };
                Ok(Some(vibesql_ast::Expression::CurrentTime { precision }))
            }
            Token::Keyword { keyword: Keyword::CurrentTimestamp, .. } => {
                self.advance(); // consume CURRENT_TIMESTAMP
                let precision = if self.try_consume(&Token::LParen) {
                    let prec_str = match self.peek() {
                        Token::Number(n) => n.clone(),
                        _ => {
                            return Err(ParseError {
                                message: "Expected integer precision for CURRENT_TIMESTAMP"
                                    .to_string(),
                            })
                        }
                    };
                    let prec: u32 = prec_str.parse().map_err(|_| ParseError {
                        message: format!("Invalid precision value: {}", prec_str),
                    })?;
                    if prec > 9 {
                        return Err(ParseError {
                            message: format!(
                                "CURRENT_TIMESTAMP precision must be between 0 and 9, got {}",
                                prec
                            ),
                        });
                    }
                    self.advance(); // consume the number
                    self.expect_token(Token::RParen)?;
                    Some(prec)
                } else {
                    None
                };
                Ok(Some(vibesql_ast::Expression::CurrentTimestamp { precision }))
            }
            _ => Ok(None),
        }
    }

    /// Parse NEXT VALUE FOR expression
    /// Syntax: NEXT VALUE FOR sequence_name
    pub(super) fn parse_sequence_value_function(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Next, .. }) {
            self.advance(); // consume NEXT

            // Parse "VALUE" as identifier (not a reserved keyword)
            match self.peek() {
                Token::Identifier(s) if s.eq_ignore_ascii_case("VALUE") => {
                    self.advance();
                }
                _ => return Err(ParseError { message: "Expected VALUE after NEXT".to_string() }),
            }

            self.expect_keyword(Keyword::For)?;
            let sequence_name = self.parse_identifier()?;
            Ok(Some(vibesql_ast::Expression::NextValue { sequence_name }))
        } else if self.peek_keyword(Keyword::Match) {
            // MATCH...AGAINST full-text search
            // We need to look ahead to check if AGAINST follows the parenthesized expression.
            // If not, this is a regular function call like match(a, b), not FTS syntax.
            if !self.is_match_against_expression() {
                return Ok(None);
            }
            self.advance(); // consume MATCH
            self.expect_token(Token::LParen)?;

            // Parse column list
            let mut columns = Vec::new();
            loop {
                let col = self.parse_identifier()?;
                columns.push(col);
                if !matches!(self.peek(), Token::Comma) {
                    break;
                }
                self.advance(); // consume comma
            }

            self.expect_token(Token::RParen)?;

            // Expect AGAINST keyword
            self.expect_keyword(Keyword::Against)?;
            self.expect_token(Token::LParen)?;

            // Parse search string (primary expression, not full expression with operators)
            // This prevents IN keyword from being parsed as an IN operator
            let search_modifier = Box::new(self.parse_primary_expression()?);

            // Check for search mode modifier
            let mode = if self.peek_keyword(Keyword::In) {
                self.advance(); // consume IN
                if self.peek_keyword(Keyword::Boolean) {
                    self.advance(); // consume BOOLEAN
                                    // MODE is a required keyword after BOOLEAN in MySQL syntax
                                    // It might be a keyword or identifier depending on lexer
                    if matches!(self.peek(), Token::Identifier(s) | Token::DelimitedIdentifier(s) if s.eq_ignore_ascii_case("MODE"))
                    {
                        self.advance(); // consume MODE
                    } else if self.peek_keyword(Keyword::Mode) {
                        self.advance(); // consume MODE keyword if it exists
                    }
                    vibesql_ast::FulltextMode::Boolean
                } else {
                    return Err(ParseError { message: "Expected BOOLEAN after IN".to_string() });
                }
            } else if self.peek_keyword(Keyword::With) {
                self.advance(); // consume WITH
                self.expect_keyword(Keyword::Query)?;
                self.expect_keyword(Keyword::Expansion)?;
                vibesql_ast::FulltextMode::QueryExpansion
            } else {
                vibesql_ast::FulltextMode::NaturalLanguage
            };

            self.expect_token(Token::RParen)?;

            Ok(Some(vibesql_ast::Expression::MatchAgainst { columns, search_modifier, mode }))
        } else {
            Ok(None)
        }
    }

    /// Parse interval unit (DAY, MONTH, YEAR, etc.) or compound units (YEAR TO MONTH, DAY TO
    /// SECOND, etc.)
    pub(super) fn parse_interval_unit(&mut self) -> Result<vibesql_ast::IntervalUnit, ParseError> {
        use vibesql_ast::IntervalUnit;

        let first_unit = match self.peek() {
            Token::Keyword { keyword: Keyword::Microsecond, .. } => {
                self.advance();
                IntervalUnit::Microsecond
            }
            Token::Keyword { keyword: Keyword::Second, .. } => {
                self.advance();
                IntervalUnit::Second
            }
            Token::Keyword { keyword: Keyword::Minute, .. } => {
                self.advance();
                IntervalUnit::Minute
            }
            Token::Keyword { keyword: Keyword::Hour, .. } => {
                self.advance();
                IntervalUnit::Hour
            }
            Token::Keyword { keyword: Keyword::Day, .. } => {
                self.advance();
                IntervalUnit::Day
            }
            Token::Keyword { keyword: Keyword::Week, .. } => {
                self.advance();
                IntervalUnit::Week
            }
            Token::Keyword { keyword: Keyword::Month, .. } => {
                self.advance();
                IntervalUnit::Month
            }
            Token::Keyword { keyword: Keyword::Quarter, .. } => {
                self.advance();
                IntervalUnit::Quarter
            }
            Token::Keyword { keyword: Keyword::Year, .. } => {
                self.advance();
                IntervalUnit::Year
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected interval unit (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.), found {:?}",
                        self.peek()
                    ),
                })
            }
        };

        // Check for compound units (e.g., YEAR TO MONTH, DAY TO SECOND)
        // This supports MySQL's INTERVAL '1-6' YEAR TO MONTH syntax
        if self.peek_keyword(Keyword::To) {
            self.advance(); // consume TO

            let compound_unit = match (&first_unit, self.peek()) {
                (IntervalUnit::Year, Token::Keyword { keyword: Keyword::Month, .. }) => {
                    self.advance();
                    IntervalUnit::YearMonth
                }
                (IntervalUnit::Day, Token::Keyword { keyword: Keyword::Hour, .. }) => {
                    self.advance();
                    IntervalUnit::DayHour
                }
                (IntervalUnit::Day, Token::Keyword { keyword: Keyword::Minute, .. }) => {
                    self.advance();
                    IntervalUnit::DayMinute
                }
                (IntervalUnit::Day, Token::Keyword { keyword: Keyword::Second, .. }) => {
                    self.advance();
                    IntervalUnit::DaySecond
                }
                (IntervalUnit::Day, Token::Keyword { keyword: Keyword::Microsecond, .. }) => {
                    self.advance();
                    IntervalUnit::DayMicrosecond
                }
                (IntervalUnit::Hour, Token::Keyword { keyword: Keyword::Minute, .. }) => {
                    self.advance();
                    IntervalUnit::HourMinute
                }
                (IntervalUnit::Hour, Token::Keyword { keyword: Keyword::Second, .. }) => {
                    self.advance();
                    IntervalUnit::HourSecond
                }
                (IntervalUnit::Hour, Token::Keyword { keyword: Keyword::Microsecond, .. }) => {
                    self.advance();
                    IntervalUnit::HourMicrosecond
                }
                (IntervalUnit::Minute, Token::Keyword { keyword: Keyword::Second, .. }) => {
                    self.advance();
                    IntervalUnit::MinuteSecond
                }
                (IntervalUnit::Minute, Token::Keyword { keyword: Keyword::Microsecond, .. }) => {
                    self.advance();
                    IntervalUnit::MinuteMicrosecond
                }
                (IntervalUnit::Second, Token::Keyword { keyword: Keyword::Microsecond, .. }) => {
                    self.advance();
                    IntervalUnit::SecondMicrosecond
                }
                _ => {
                    return Err(ParseError {
                        message: format!(
                            "Invalid compound interval unit: {:?} TO {:?}",
                            first_unit,
                            self.peek()
                        ),
                    })
                }
            };

            Ok(compound_unit)
        } else {
            Ok(first_unit)
        }
    }

    /// Check if the current MATCH keyword is part of a MATCH...AGAINST expression.
    /// Returns true if AGAINST keyword follows after the parenthesized column list.
    /// This allows `match(a, b)` to be parsed as a regular function call.
    fn is_match_against_expression(&self) -> bool {
        // We're at MATCH keyword. Look ahead to find the closing paren
        // and check if AGAINST follows.
        let mut pos = self.position + 1; // skip MATCH

        // Expect LParen
        if pos >= self.tokens.len() || !matches!(self.tokens[pos], Token::LParen) {
            return false;
        }
        pos += 1;

        // Skip past the parenthesized content (handling nested parens)
        let mut depth = 1;
        while pos < self.tokens.len() && depth > 0 {
            match &self.tokens[pos] {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            pos += 1;
        }

        // Check if AGAINST follows
        pos < self.tokens.len()
            && matches!(&self.tokens[pos], Token::Keyword { keyword: Keyword::Against, .. })
    }
}
