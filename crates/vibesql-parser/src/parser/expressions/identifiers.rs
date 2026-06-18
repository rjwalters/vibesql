use super::*;

impl Parser {
    /// Parse identifier-based expressions (column references, qualified names)
    pub(super) fn parse_identifier_expression(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        // Extract identifier string from token (handles regular identifiers and
        // keywords-as-identifiers)
        // Track whether identifier is quoted for proper SQL:1999 case handling
        let (first, first_quoted) = match self.peek() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                (name, false)
            }
            Token::DelimitedIdentifier(id) => {
                // Delimited identifiers (backtick/double-quote) preserve case
                let name = id.clone();
                self.advance();
                (name, true)
            }
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier_in_expression() => {
                // SQLite compatibility: in expression / primary position, a keyword that
                // is not a reserved operator, clause-structure word, or special primary
                // form is an unquoted column reference. This covers temporal words
                // (MONTH, YEAR, DAY, ...) as well as otherwise-reserved column names like
                // RELEASE, SAVEPOINT, ASC, DESC, KEY (see table.test table-7.3).
                //
                // Reserved operators (AND, OR, NOT, IN, IS, ...) and clause keywords
                // (FROM, WHERE, SELECT, ...) are excluded by
                // `can_be_identifier_in_expression()`, so they still terminate the
                // expression / are handled in operator position rather than being
                // swallowed here.
                //
                // SQL:1999 normalizes unquoted identifiers to lowercase.
                let name = format!("{}", kw).to_lowercase();
                self.advance();
                (name, false)
            }
            _ => return Ok(None),
        };

        // Check for hex/binary literal (x'...' or b'...')
        let first_upper = first.to_uppercase();
        if (first_upper == "X" || first_upper == "B") && matches!(self.peek(), Token::String(_)) {
            if let Token::String(s) = self.peek() {
                let string_val = s.clone();
                self.advance();

                if first_upper == "X" {
                    // Parse hex literal: x'303132' -> "\x30\x31\x32"
                    // Validate and decode hex string
                    if string_val.len() % 2 != 0 {
                        return Err(ParseError {
                            message: format!(
                                "Hex literal must have even number of digits: x'{}'",
                                string_val
                            ),
                        });
                    }

                    let mut bytes = Vec::new();
                    for i in (0..string_val.len()).step_by(2) {
                        let hex_byte = &string_val[i..i + 2];
                        match u8::from_str_radix(hex_byte, 16) {
                            Ok(byte) => bytes.push(byte),
                            Err(_) => {
                                return Err(ParseError {
                                    message: format!(
                                        "Invalid hex digit in literal: x'{}'",
                                        string_val
                                    ),
                                });
                            }
                        }
                    }

                    // Convert bytes to string (UTF-8 lossy conversion for compatibility)
                    let result = arcstr::ArcStr::from(String::from_utf8_lossy(&bytes).as_ref());
                    return Ok(Some(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Varchar(result),
                    )));
                } else {
                    // Parse binary literal: b'01010101' -> "U"
                    if !string_val.chars().all(|c| c == '0' || c == '1') {
                        return Err(ParseError {
                            message: format!(
                                "Binary literal must contain only 0 and 1: b'{}'",
                                string_val
                            ),
                        });
                    }

                    if string_val.len() % 8 != 0 {
                        return Err(ParseError {
                            message: format!(
                                "Binary literal must have length divisible by 8: b'{}'",
                                string_val
                            ),
                        });
                    }

                    let mut bytes = Vec::new();
                    for i in (0..string_val.len()).step_by(8) {
                        let binary_byte = &string_val[i..i + 8];
                        match u8::from_str_radix(binary_byte, 2) {
                            Ok(byte) => bytes.push(byte),
                            Err(_) => {
                                return Err(ParseError {
                                    message: format!("Invalid binary literal: b'{}'", string_val),
                                });
                            }
                        }
                    }

                    // Convert bytes to string
                    let result = arcstr::ArcStr::from(String::from_utf8_lossy(&bytes).as_ref());
                    return Ok(Some(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Varchar(result),
                    )));
                }
            }
        }

        // Check for function call (identifier followed by '(')
        if matches!(self.peek(), Token::LParen) {
            // This is handled by parse_function_call, return None
            // We need to rewind
            self.position -= 1;
            return Ok(None);
        }

        // Check for qualified column reference (table.column or schema.table.column)
        // Keywords are allowed as column names when qualified (e.g., ss.year)
        if matches!(self.peek(), Token::Symbol('.')) {
            self.advance(); // consume first '.'
            let (second, second_quoted) = match self.peek() {
                Token::Identifier(id) => {
                    let name = id.clone();
                    self.advance();
                    (name, false)
                }
                Token::DelimitedIdentifier(id) => {
                    let name = id.clone();
                    self.advance();
                    (name, true)
                }
                Token::Keyword { keyword: kw, .. } => {
                    // Allow keywords as column names when qualified (e.g., table.year)
                    // SQL:1999 normalizes unquoted identifiers to lowercase
                    let name = kw.to_string().to_lowercase();
                    self.advance();
                    (name, false)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected identifier after '.'".to_string(),
                    });
                }
            };

            // Check for pseudo-variable (OLD.column or NEW.column)
            let first_upper = first.to_uppercase();
            if first_upper == "OLD" || first_upper == "NEW" {
                let pseudo_table = if first_upper == "OLD" {
                    vibesql_ast::PseudoTable::Old
                } else {
                    vibesql_ast::PseudoTable::New
                };
                return Ok(Some(vibesql_ast::Expression::PseudoVariable {
                    pseudo_table,
                    column: second,
                }));
            }

            // Check for three-part name (schema.table.column)
            if matches!(self.peek(), Token::Symbol('.')) {
                self.advance(); // consume second '.'
                let (third, third_quoted) = match self.peek() {
                    Token::Identifier(id) => {
                        let name = id.clone();
                        self.advance();
                        (name, false)
                    }
                    Token::DelimitedIdentifier(id) => {
                        let name = id.clone();
                        self.advance();
                        (name, true)
                    }
                    Token::Keyword { keyword: kw, .. } => {
                        let name = kw.to_string().to_lowercase();
                        self.advance();
                        (name, false)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                        });
                    }
                };

                // schema.table.column
                Ok(Some(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::fully_qualified(
                        &first,
                        first_quoted,
                        &second,
                        second_quoted,
                        &third,
                        third_quoted,
                    ),
                )))
            } else {
                // table.column (two-part name)
                Ok(Some(vibesql_ast::Expression::ColumnRef(
                    vibesql_ast::ColumnIdentifier::qualified(
                        &first,
                        first_quoted,
                        &second,
                        second_quoted,
                    ),
                )))
            }
        } else {
            // Simple column reference
            Ok(Some(vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                &first,
                first_quoted,
            ))))
        }
    }
}
