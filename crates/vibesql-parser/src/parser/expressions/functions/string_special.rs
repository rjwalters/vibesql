//! Special SQL:1999 string function syntax parsing
//!
//! This module handles special SQL:1999 function syntax that differs from
//! standard function call syntax:
//! - POSITION(substring IN string [USING unit])
//! - TRIM([position] [removal_char FROM] string)
//! - SUBSTRING(string FROM start [FOR length] [USING unit])
//! - EXTRACT(field FROM expr)

use super::super::*;

impl Parser {
    /// Parse POSITION(substring IN string [USING unit])
    /// SQL:1999 standard syntax
    pub(super) fn parse_position_function(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        // Parse substring at primary level (literals, identifiers, function calls)
        // to avoid IN operator consumption at comparison level
        let substring = self.parse_primary_expression()?;

        // Expect IN keyword
        self.expect_keyword(Keyword::In)?;

        // Parse string expression at primary level
        let string = self.parse_primary_expression()?;

        // Parse optional USING clause
        let character_unit = if matches!(self.peek(), Token::Keyword(Keyword::Using)) {
            self.advance(); // consume USING
            Some(self.parse_character_unit()?)
        } else {
            None
        };

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::Expression::Position {
            substring: Box::new(substring),
            string: Box::new(string),
            character_unit,
        })
    }

    /// Parse EXTRACT(field FROM expr)
    /// SQL:1999 standard syntax for extracting date/time components
    ///
    /// Forms:
    /// - EXTRACT(YEAR FROM date_column)
    /// - EXTRACT(MONTH FROM timestamp_column)
    /// - EXTRACT(DAY FROM '2024-01-15')
    /// - EXTRACT(HOUR FROM current_timestamp)
    pub(super) fn parse_extract_function(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        // Parse the field (date/time unit) - reuse the interval unit parser
        let field = self.parse_interval_unit()?;

        // Expect FROM keyword
        self.expect_keyword(Keyword::From)?;

        // Parse the expression at primary level to avoid operator conflicts
        let expr = self.parse_primary_expression()?;

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::Expression::Extract { field, expr: Box::new(expr) })
    }

    /// Parse TRIM([position] [removal_char FROM] string)
    /// SQL:1999 standard syntax
    ///
    /// Forms:
    /// - TRIM(string)                              -- remove spaces from both sides
    /// - TRIM('x' FROM string)                     -- remove 'x' from both sides
    /// - TRIM(BOTH 'x' FROM string)                -- remove 'x' from both sides
    /// - TRIM(LEADING 'x' FROM string)             -- remove 'x' from start
    /// - TRIM(TRAILING 'x' FROM string)            -- remove 'x' from end
    /// - TRIM(FROM string)                         -- remove spaces from both sides (explicit FROM)
    /// - TRIM(BOTH FROM string)                    -- remove spaces from both sides (explicit BOTH)
    /// - TRIM(LEADING FROM string)                 -- remove spaces from start (explicit LEADING)
    /// - TRIM(TRAILING FROM string)                -- remove spaces from end (explicit TRAILING)
    pub(super) fn parse_trim_function(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        let mut position: Option<vibesql_ast::TrimPosition> = None;
        let removal_char: Option<Box<vibesql_ast::Expression>>;

        // Check for BOTH/LEADING/TRAILING keywords
        match self.peek() {
            Token::Keyword(Keyword::Both) => {
                self.advance();
                position = Some(vibesql_ast::TrimPosition::Both);
            }
            Token::Keyword(Keyword::Leading) => {
                self.advance();
                position = Some(vibesql_ast::TrimPosition::Leading);
            }
            Token::Keyword(Keyword::Trailing) => {
                self.advance();
                position = Some(vibesql_ast::TrimPosition::Trailing);
            }
            _ => {}
        }

        // Check if FROM comes immediately (no removal char specified)
        // This handles: TRIM(FROM 'foo'), TRIM(BOTH FROM 'foo'), etc.
        if matches!(self.peek(), Token::Keyword(Keyword::From)) {
            self.advance(); // consume FROM
            removal_char = None; // Default to space
            let string = self.parse_primary_expression()?;

            self.expect_token(Token::RParen)?;

            return Ok(vibesql_ast::Expression::Trim {
                position,
                removal_char,
                string: Box::new(string),
            });
        }

        // Try to parse the first expression (could be removal_char or string)
        let first_expr = self.parse_primary_expression()?;

        // Check if this is followed by FROM keyword
        if matches!(self.peek(), Token::Keyword(Keyword::From)) {
            self.advance(); // consume FROM
                            // first_expr is the removal_char, now parse the string
            removal_char = Some(Box::new(first_expr));
            let string = self.parse_primary_expression()?;

            self.expect_token(Token::RParen)?;

            Ok(vibesql_ast::Expression::Trim { position, removal_char, string: Box::new(string) })
        } else {
            // No FROM keyword, so first_expr is the string
            // removal_char defaults to None (which means space)
            self.expect_token(Token::RParen)?;

            Ok(vibesql_ast::Expression::Trim {
                position,
                removal_char: None,
                string: Box::new(first_expr),
            })
        }
    }

    /// Parse SUBSTRING(string FROM start [FOR length] [USING unit])
    /// SQL:1999 standard syntax - alternative to comma syntax
    pub(super) fn parse_substring_function(
        &mut self,
        function_name: String,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        // Parse the string expression
        let string_expr = self.parse_expression()?;

        // Check which syntax: comma or FROM keyword
        let (start_expr, length_expr) = if self.try_consume_keyword(Keyword::From) {
            // FROM/FOR syntax: SUBSTRING(string FROM start [FOR length])
            let start = self.parse_expression()?;
            let length = if self.try_consume_keyword(Keyword::For) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            (start, length)
        } else {
            // Comma syntax: SUBSTRING(string, start [, length])
            self.expect_token(Token::Comma)?;
            let start = self.parse_expression()?;
            let length = if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume comma
                Some(self.parse_expression()?)
            } else {
                None
            };
            (start, length)
        };

        // Parse optional USING clause
        let character_unit = if matches!(self.peek(), Token::Keyword(Keyword::Using)) {
            self.advance(); // consume USING
            Some(self.parse_character_unit()?)
        } else {
            None
        };

        self.expect_token(Token::RParen)?;

        // For SUBSTRING, we represent it as a function call with 2 or 3 arguments
        // The executor will handle the semantics
        let mut args = vec![string_expr, start_expr];
        if let Some(length) = length_expr {
            args.push(length);
        }

        Ok(vibesql_ast::Expression::Function { name: function_name, args, character_unit })
    }

    /// Parse CHARACTERS or OCTETS keyword for USING clause
    /// SQL:1999 Section 6.29: String value functions
    pub(super) fn parse_character_unit(
        &mut self,
    ) -> Result<vibesql_ast::CharacterUnit, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Characters) => {
                self.advance();
                Ok(vibesql_ast::CharacterUnit::Characters)
            }
            Token::Keyword(Keyword::Octets) => {
                self.advance();
                Ok(vibesql_ast::CharacterUnit::Octets)
            }
            _ => Err(ParseError {
                message: format!(
                    "Expected CHARACTERS or OCTETS after USING, found {:?}",
                    self.peek()
                ),
            }),
        }
    }
}
