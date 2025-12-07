use super::*;

impl Parser {
    /// Parse literal expressions (numbers, strings, booleans, NULL)
    pub(super) fn parse_literal(&mut self) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            Token::Number(n) => {
                let num_str = n.clone();
                self.advance();

                // Try to parse as integer first
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(i))))
                } else {
                    // Parse as f64 for Numeric type
                    match num_str.parse::<f64>() {
                        Ok(f) => Ok(Some(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Numeric(f),
                        ))),
                        Err(_) => Err(ParseError {
                            message: format!("Invalid numeric literal: {}", num_str),
                        }),
                    }
                }
            }
            Token::String(s) => {
                let string_val = arcstr::ArcStr::from(s.as_str());
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    string_val,
                ))))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false))))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)))
            }
            // Typed literals: DATE 'string', TIME 'string', TIMESTAMP 'string'
            // If not followed by a string literal, treat as column name (SQLite compatibility)
            Token::Keyword(Keyword::Date) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let date_str = s.clone();
                        self.advance();

                        // Parse the date string into a Date type
                        match date_str.parse::<vibesql_types::Date>() {
                            Ok(date) => Ok(Some(vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Date(date),
                            ))),
                            Err(e) => {
                                Err(ParseError { message: format!("Invalid DATE literal: {}", e) })
                            }
                        }
                    }
                    _ => {
                        // Treat DATE as column name when not followed by string literal
                        Ok(Some(vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "DATE".to_string(),
                        }))
                    }
                }
            }
            Token::Keyword(Keyword::Time) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let time_str = s.clone();
                        self.advance();

                        // Parse the time string into a Time type
                        match time_str.parse::<vibesql_types::Time>() {
                            Ok(time) => Ok(Some(vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Time(time),
                            ))),
                            Err(e) => {
                                Err(ParseError { message: format!("Invalid TIME literal: {}", e) })
                            }
                        }
                    }
                    _ => {
                        // Treat TIME as column name when not followed by string literal
                        Ok(Some(vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "TIME".to_string(),
                        }))
                    }
                }
            }
            Token::Keyword(Keyword::Timestamp) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let timestamp_str = s.clone();
                        self.advance();

                        // Parse the timestamp string into a Timestamp type
                        match timestamp_str.parse::<vibesql_types::Timestamp>() {
                            Ok(timestamp) => Ok(Some(vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Timestamp(timestamp),
                            ))),
                            Err(e) => Err(ParseError {
                                message: format!("Invalid TIMESTAMP literal: {}", e),
                            }),
                        }
                    }
                    _ => {
                        // Treat TIMESTAMP as column name when not followed by string literal
                        Ok(Some(vibesql_ast::Expression::ColumnRef {
                            table: None,
                            column: "TIMESTAMP".to_string(),
                        }))
                    }
                }
            }
            Token::Keyword(Keyword::Interval) => {
                self.advance();
                // Parse INTERVAL 'value' field [TO field]
                match self.peek() {
                    Token::String(interval_str) => {
                        let value_str = interval_str.clone();
                        self.advance();

                        // Parse interval field (YEAR, MONTH, DAY, etc.)
                        let start_field = match self.peek() {
                            Token::Identifier(field) => field.to_uppercase(),
                            Token::Keyword(Keyword::Year) => "YEAR".to_string(),
                            Token::Keyword(Keyword::Month) => "MONTH".to_string(),
                            Token::Keyword(Keyword::Day) => "DAY".to_string(),
                            Token::Keyword(Keyword::Hour) => "HOUR".to_string(),
                            Token::Keyword(Keyword::Minute) => "MINUTE".to_string(),
                            Token::Keyword(Keyword::Second) => "SECOND".to_string(),
                            _ => {
                                return Err(ParseError {
                                    message: "Expected interval field after INTERVAL value"
                                        .to_string(),
                                })
                            }
                        };
                        self.advance();

                        // Check for TO (multi-field interval)
                        let interval_spec = match self.peek() {
                            Token::Keyword(Keyword::To) => {
                                self.advance(); // consume TO keyword
                                let end_field = match self.peek() {
                                    Token::Identifier(field) => field.to_uppercase(),
                                    Token::Keyword(Keyword::Year) => "YEAR".to_string(),
                                    Token::Keyword(Keyword::Month) => "MONTH".to_string(),
                                    Token::Keyword(Keyword::Day) => "DAY".to_string(),
                                    Token::Keyword(Keyword::Hour) => "HOUR".to_string(),
                                    Token::Keyword(Keyword::Minute) => "MINUTE".to_string(),
                                    Token::Keyword(Keyword::Second) => "SECOND".to_string(),
                                    _ => {
                                        return Err(ParseError {
                                            message: "Expected interval field after TO".to_string(),
                                        })
                                    }
                                };
                                self.advance();
                                format!("{} {} TO {}", value_str, start_field, end_field)
                            }
                            Token::Identifier(word) if word.to_uppercase() == "TO" => {
                                self.advance(); // consume TO identifier (backward compat)
                                let end_field = match self.peek() {
                                    Token::Identifier(field) => field.to_uppercase(),
                                    Token::Keyword(Keyword::Year) => "YEAR".to_string(),
                                    Token::Keyword(Keyword::Month) => "MONTH".to_string(),
                                    Token::Keyword(Keyword::Day) => "DAY".to_string(),
                                    Token::Keyword(Keyword::Hour) => "HOUR".to_string(),
                                    Token::Keyword(Keyword::Minute) => "MINUTE".to_string(),
                                    Token::Keyword(Keyword::Second) => "SECOND".to_string(),
                                    _ => {
                                        return Err(ParseError {
                                            message: "Expected interval field after TO".to_string(),
                                        })
                                    }
                                };
                                self.advance();
                                format!("{} {} TO {}", value_str, start_field, end_field)
                            }
                            _ => format!("{} {}", value_str, start_field),
                        };

                        // Parse the interval string into an Interval type
                        match interval_spec.parse::<vibesql_types::Interval>() {
                            Ok(interval) => Ok(Some(vibesql_ast::Expression::Literal(
                                vibesql_types::SqlValue::Interval(interval),
                            ))),
                            Err(e) => Err(ParseError {
                                message: format!("Invalid INTERVAL literal: {}", e),
                            }),
                        }
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after INTERVAL keyword".to_string(),
                    }),
                }
            }
            _ => Ok(None),
        }
    }
}
