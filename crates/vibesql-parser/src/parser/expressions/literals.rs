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
                // SQLite compatibility: a single-quoted string immediately
                // followed by `.` is a qualified name, not a string literal
                // (quote.test quote-1.3: `'@abc'.'!pqr'`). Defer to
                // `parse_identifier_expression`, which builds the ColumnRef.
                if matches!(self.peek_next(), Token::Symbol('.')) {
                    return Ok(None);
                }
                let string_val = arcstr::ArcStr::from(s.as_str());
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                    string_val,
                ))))
            }
            Token::BlobLiteral(bytes) => {
                let blob_val = bytes.clone();
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Blob(blob_val))))
            }
            Token::Keyword { keyword: Keyword::True, .. } => {
                // SQLite compatibility: TRUE/FALSE are not reserved words. When
                // immediately followed by `.` they are the qualifier of a dotted
                // name, not a boolean literal (istrue.test istrue-800/830/850:
                // `SELECT 9 IN (false.false)` must parse as a column reference and
                // fail with "no such column", not a syntax error). Defer to
                // `parse_identifier_expression`, which builds the ColumnRef.
                if matches!(self.peek_next(), Token::Symbol('.')) {
                    return Ok(None);
                }
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))))
            }
            Token::Keyword { keyword: Keyword::False, .. } => {
                if matches!(self.peek_next(), Token::Symbol('.')) {
                    return Ok(None);
                }
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false))))
            }
            Token::Keyword { keyword: Keyword::Null, .. } => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)))
            }
            // Typed literals: DATE 'string', TIME 'string', TIMESTAMP 'string'
            // If not followed by a string literal, treat as column name (SQLite compatibility)
            Token::Keyword { keyword: Keyword::Date, .. } => {
                // date(...) is a function call, not a typed literal - don't consume
                // the keyword; let parse_function_call handle it (issue #5307)
                if matches!(self.peek_next(), Token::LParen) {
                    return Ok(None);
                }
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
                        Ok(Some(vibesql_ast::Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple("DATE", false),
                        )))
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Time, .. } => {
                // time(...) is a function call, not a typed literal - don't consume
                // the keyword; let parse_function_call handle it (issue #5307)
                if matches!(self.peek_next(), Token::LParen) {
                    return Ok(None);
                }
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
                        Ok(Some(vibesql_ast::Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple("TIME", false),
                        )))
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Timestamp, .. } => {
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
                        Ok(Some(vibesql_ast::Expression::ColumnRef(
                            vibesql_ast::ColumnIdentifier::simple("TIMESTAMP", false),
                        )))
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Interval, .. } => {
                self.advance();
                // Parse INTERVAL 'value' field [TO field]
                match self.peek() {
                    Token::String(interval_str) => {
                        let value_str = interval_str.clone();
                        self.advance();

                        // Parse interval field (YEAR, MONTH, DAY, etc.)
                        let start_field = match self.peek() {
                            Token::Identifier(field) => field.to_uppercase(),
                            Token::Keyword { keyword: Keyword::Year, .. } => "YEAR".to_string(),
                            Token::Keyword { keyword: Keyword::Month, .. } => "MONTH".to_string(),
                            Token::Keyword { keyword: Keyword::Day, .. } => "DAY".to_string(),
                            Token::Keyword { keyword: Keyword::Hour, .. } => "HOUR".to_string(),
                            Token::Keyword { keyword: Keyword::Minute, .. } => "MINUTE".to_string(),
                            Token::Keyword { keyword: Keyword::Second, .. } => "SECOND".to_string(),
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
                            Token::Keyword { keyword: Keyword::To, .. } => {
                                self.advance(); // consume TO keyword
                                let end_field = match self.peek() {
                                    Token::Identifier(field) => field.to_uppercase(),
                                    Token::Keyword { keyword: Keyword::Year, .. } => {
                                        "YEAR".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Month, .. } => {
                                        "MONTH".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Day, .. } => {
                                        "DAY".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Hour, .. } => {
                                        "HOUR".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Minute, .. } => {
                                        "MINUTE".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Second, .. } => {
                                        "SECOND".to_string()
                                    }
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
                                    Token::Keyword { keyword: Keyword::Year, .. } => {
                                        "YEAR".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Month, .. } => {
                                        "MONTH".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Day, .. } => {
                                        "DAY".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Hour, .. } => {
                                        "HOUR".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Minute, .. } => {
                                        "MINUTE".to_string()
                                    }
                                    Token::Keyword { keyword: Keyword::Second, .. } => {
                                        "SECOND".to_string()
                                    }
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
