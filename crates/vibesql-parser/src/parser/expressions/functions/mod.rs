//! Function parsing module
//!
//! This module handles parsing of all SQL function types:
//! - Regular functions (e.g., UPPER, LOWER, CONCAT)
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - Window functions (with OVER clause)
//! - Special SQL:1999 syntax (POSITION, TRIM, SUBSTRING)

use super::*;

mod string_special;
mod window;

impl Parser {
    /// Parse function call expressions (including window functions)
    pub(super) fn parse_function_call(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        // Try to match either an identifier or specific keywords that can be function names
        let function_name = match self.peek() {
            Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                let name = id.clone();
                self.advance();
                // Check if followed by '('
                if !matches!(self.peek(), Token::LParen) {
                    // Not a function call - rewind
                    self.position -= 1;
                    return Ok(None);
                }
                name
            }
            // Allow LEFT, RIGHT, REPLACE, SCHEMA, GROUPING, and GROUPING_ID keywords as function names
            // These are reserved keywords but can also be functions
            Token::Keyword(Keyword::Left)
            | Token::Keyword(Keyword::Right)
            | Token::Keyword(Keyword::Replace)
            | Token::Keyword(Keyword::Schema)
            | Token::Keyword(Keyword::Grouping)
            | Token::Keyword(Keyword::GroupingId) => {
                // Peek ahead to see if this is followed by '('
                // Don't consume the keyword unless we're sure it's a function
                let keyword_name = match self.peek() {
                    Token::Keyword(Keyword::Left) => "LEFT",
                    Token::Keyword(Keyword::Right) => "RIGHT",
                    Token::Keyword(Keyword::Replace) => "REPLACE",
                    Token::Keyword(Keyword::Schema) => "SCHEMA",
                    Token::Keyword(Keyword::Grouping) => "GROUPING",
                    Token::Keyword(Keyword::GroupingId) => "GROUPING_ID",
                    _ => unreachable!(),
                };

                // Look ahead to next token
                if self.position + 1 < self.tokens.len() {
                    if matches!(self.tokens[self.position + 1], Token::LParen) {
                        // Yes, it's a function call
                        self.advance(); // consume keyword
                        keyword_name.to_string()
                    } else {
                        // Not a function call, don't consume
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        self.advance(); // consume '('
        let first = function_name;

        // Special case for VALUES(column) - MySQL ON DUPLICATE KEY UPDATE
        // Returns the value that would have been inserted
        if first.to_uppercase() == "VALUES" {
            // Expect a single column name as argument
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let column_name = col.clone();
                    self.advance();
                    column_name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in VALUES() function".to_string(),
                    })
                }
            };
            self.expect_token(Token::RParen)?;
            return Ok(Some(vibesql_ast::Expression::DuplicateKeyValue { column }));
        }

        // Special case for POSITION(substring IN string [USING unit])
        // SQL:1999 standard syntax
        if first.to_uppercase() == "POSITION" {
            return Ok(Some(self.parse_position_function()?));
        }

        // Special case for TRIM([position] [removal_char FROM] string)
        // SQL:1999 standard syntax
        if first.to_uppercase() == "TRIM" {
            return Ok(Some(self.parse_trim_function()?));
        }

        // Special case for SUBSTRING(string FROM start [FOR length] [USING unit])
        // SQL:1999 standard syntax - alternative to comma syntax
        if first.to_uppercase() == "SUBSTRING" {
            return Ok(Some(self.parse_substring_function(first)?));
        }

        // Special case for EXTRACT(field FROM expr)
        // SQL:1999 standard syntax for date/time extraction
        if first.to_uppercase() == "EXTRACT" {
            return Ok(Some(self.parse_extract_function()?));
        }

        // Check if this is an aggregate function
        let function_name_upper = first.to_uppercase();
        let is_aggregate =
            matches!(function_name_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX");

        // Parse optional DISTINCT or ALL for aggregate functions
        let distinct = if is_aggregate {
            if matches!(self.peek(), Token::Keyword(Keyword::Distinct)) {
                self.advance(); // consume DISTINCT
                true
            } else if matches!(self.peek(), Token::Keyword(Keyword::All)) {
                self.advance(); // consume ALL
                false
            } else {
                false // default is ALL
            }
        } else {
            false
        };

        // Parse function arguments
        let mut args = Vec::new();
        let mut character_unit = None;

        // Check for empty argument list or '*'
        if matches!(self.peek(), Token::RParen) {
            // No arguments: func()
            self.advance();
        } else if matches!(self.peek(), Token::Symbol('*')) {
            // Special case for COUNT(*)
            self.advance(); // consume '*'
            self.expect_token(Token::RParen)?;
            // Represent * as a special wildcard expression
            args.push(vibesql_ast::Expression::ColumnRef { table: None, column: "*".to_string() });
        } else {
            // Parse comma-separated argument list
            loop {
                let arg = self.parse_expression()?;
                args.push(arg);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            // Parse optional USING clause for string functions BEFORE closing paren
            if matches!(function_name_upper.as_str(), "CHARACTER_LENGTH" | "CHAR_LENGTH")
                && matches!(self.peek(), Token::Keyword(Keyword::Using))
            {
                self.advance(); // consume USING
                character_unit = Some(self.parse_character_unit()?);
            }

            self.expect_token(Token::RParen)?;
        }

        // Check for OVER clause (window function)
        if matches!(self.peek(), Token::Keyword(Keyword::Over)) {
            self.advance(); // consume OVER

            // Parse window specification
            let window_spec = self.parse_window_spec()?;

            // Determine window function type based on function name
            let function_spec = self.classify_window_function(&first, args);

            return Ok(Some(vibesql_ast::Expression::WindowFunction {
                function: function_spec,
                over: window_spec,
            }));
        }

        // Return appropriate expression type
        if is_aggregate {
            Ok(Some(vibesql_ast::Expression::AggregateFunction { name: first, distinct, args }))
        } else {
            Ok(Some(vibesql_ast::Expression::Function { name: first, args, character_unit }))
        }
    }
}
