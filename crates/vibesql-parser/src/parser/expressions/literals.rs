use super::*;

impl Parser {
    /// Parse literal expressions (numbers, strings, booleans, NULL)
    pub(super) fn parse_literal(&mut self) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            Token::Number(sym) => {
                let num_str = self.resolve_str(*sym).to_string();
                self.advance();

                // Try to parse as integer first
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(i))))
                } else {
                    // Parse as f64 for Numeric type
                    match num_str.parse::<f64>() {
                        Ok(f) => Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Numeric(f)))),
                        Err(_) => Err(ParseError {
                            message: format!("Invalid numeric literal: {}", num_str),
                        }),
                    }
                }
            }
            Token::String(sym) => {
                let string_val = self.resolve(*sym);
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(string_val))))
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
            Token::Keyword(Keyword::Date) => {
                self.advance();
                match self.peek() {
                    Token::String(sym) => {
                        let date_str = self.resolve(*sym);
                        self.advance();

                        // Parse the date string into a Date type
                        match date_str.parse::<vibesql_types::Date>() {
                            Ok(date) => Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(date)))),
                            Err(e) => Err(ParseError {
                                message: format!("Invalid DATE literal: {}", e),
                            }),
                        }
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after DATE keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Time) => {
                self.advance();
                match self.peek() {
                    Token::String(sym) => {
                        let time_str = self.resolve(*sym);
                        self.advance();

                        // Parse the time string into a Time type
                        match time_str.parse::<vibesql_types::Time>() {
                            Ok(time) => Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Time(time)))),
                            Err(e) => Err(ParseError {
                                message: format!("Invalid TIME literal: {}", e),
                            }),
                        }
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after TIME keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Timestamp) => {
                self.advance();
                match self.peek() {
                    Token::String(sym) => {
                        let timestamp_str = self.resolve(*sym);
                        self.advance();

                        // Parse the timestamp string into a Timestamp type
                        match timestamp_str.parse::<vibesql_types::Timestamp>() {
                            Ok(timestamp) => Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(timestamp)))),
                            Err(e) => Err(ParseError {
                                message: format!("Invalid TIMESTAMP literal: {}", e),
                            }),
                        }
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after TIMESTAMP keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Interval) => {
                self.advance();
                // Parse INTERVAL 'value' field [TO field]
                match self.peek() {
                    Token::String(sym) => {
                        let value_str = self.resolve(*sym);
                        self.advance();

                        // Parse interval field (YEAR, MONTH, DAY, etc.)
                        let start_field = match self.peek() {
                            Token::Identifier(sym) => self.resolve_str(*sym).to_uppercase(),
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
                                    Token::Identifier(sym) => self.resolve_str(*sym).to_uppercase(),
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
                            Token::Identifier(sym) if self.resolve_str(*sym).to_uppercase() == "TO" => {
                                self.advance(); // consume TO identifier (backward compat)
                                let end_field = match self.peek() {
                                    Token::Identifier(sym) => self.resolve_str(*sym).to_uppercase(),
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
                            Ok(interval) => Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Interval(interval)))),
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
