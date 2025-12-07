use super::*;

impl Parser {
    /// Parse identifier-based expressions (column references, qualified names)
    pub(super) fn parse_identifier_expression(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        // Extract identifier string from token (handles regular identifiers and keywords-as-identifiers)
        let first = match self.peek() {
            Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                let name = id.clone();
                self.advance();
                name
            }
            Token::Keyword(kw) if kw.can_be_identifier() => {
                // Allow certain keywords (MONTH, YEAR, DAY, etc.) to be used as column names
                let name = format!("{}", kw); // Uses Display impl, returns uppercase
                self.advance();
                name
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

        // Check for qualified column reference (table.column)
        // Keywords are allowed as column names when qualified (e.g., ss.year)
        if matches!(self.peek(), Token::Symbol('.')) {
            self.advance(); // consume '.'
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let column = col.clone();
                    self.advance();
                    column
                }
                Token::Keyword(kw) => {
                    // Allow keywords as column names when qualified (e.g., table.year)
                    let column = kw.to_string();
                    self.advance();
                    column
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name after '.'".to_string(),
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
                Ok(Some(vibesql_ast::Expression::PseudoVariable { pseudo_table, column }))
            } else {
                Ok(Some(vibesql_ast::Expression::ColumnRef { table: Some(first), column }))
            }
        } else {
            // Simple column reference
            Ok(Some(vibesql_ast::Expression::ColumnRef { table: None, column: first }))
        }
    }
}
