use super::*;

impl Parser {
    /// Peek at current token without consuming.
    pub(super) fn peek(&self) -> &Token {
        if self.position < self.tokens.len() {
            &self.tokens[self.position]
        } else {
            &Token::Eof
        }
    }

    /// Peek at next token (position + 1) without consuming.
    pub(super) fn peek_next(&self) -> &Token {
        if self.position + 1 < self.tokens.len() {
            &self.tokens[self.position + 1]
        } else {
            &Token::Eof
        }
    }

    /// Peek at token at offset (position + offset) without consuming.
    pub(super) fn peek_at_offset(&self, offset: usize) -> &Token {
        if self.position + offset < self.tokens.len() {
            &self.tokens[self.position + offset]
        } else {
            &Token::Eof
        }
    }

    /// Advance to next token.
    pub(super) fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    /// Check if current token is a specific keyword.
    pub(super) fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek(), Token::Keyword(k) if k == &keyword)
    }

    /// Check if next token is a specific keyword.
    pub(super) fn peek_next_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_next(), Token::Keyword(k) if k == &keyword)
    }

    /// Expect and consume a specific keyword.
    pub(super) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        if self.peek_keyword(keyword) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected keyword {:?}, found {:?}", keyword, self.peek()),
            })
        }
    }

    /// Consume a specific keyword.
    pub(super) fn consume_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        self.expect_keyword(keyword)
    }

    /// Expect a specific token.
    pub(super) fn expect_token(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.peek() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError { message: format!("Expected {:?}, found {:?}", expected, self.peek()) })
        }
    }

    /// Parse an identifier token (regular or delimited).
    pub(super) fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::Identifier(sym) | Token::DelimitedIdentifier(sym) => {
                let identifier = self.resolve(*sym);
                self.advance();
                Ok(identifier)
            }
            Token::Keyword(kw) => {
                Err(ParseError {
                    message: format!(
                        "Expected identifier, found reserved keyword '{}'. Use delimited identifiers (e.g., \"{}\") to use keywords as names, or choose a different identifier.",
                        kw, kw
                    ),
                })
            }
            _ => Err(ParseError {
                message: format!("Expected identifier, found {:?}", self.peek())
            }),
        }
    }

    /// Try to consume a keyword, returning true if successful.
    pub(super) fn try_consume_keyword(&mut self, keyword: Keyword) -> bool {
        if self.peek_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Try to consume a specific token, returning true if successful.
    pub(super) fn try_consume(&mut self, token: &Token) -> bool {
        if self.peek() == token {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Parse a signed number (optional minus sign followed by number)
    pub(super) fn parse_signed_number(&mut self) -> Result<String, ParseError> {
        let mut num_str = String::new();

        // Check for optional minus sign
        if self.try_consume(&Token::Symbol('-')) {
            num_str.push('-');
        }

        // Parse the number
        match self.peek() {
            Token::Number(sym) => {
                num_str.push_str(self.resolve_str(*sym));
                self.advance();
                Ok(num_str)
            }
            _ => Err(ParseError { message: "Expected number".to_string() }),
        }
    }

    /// Parse a qualified identifier (schema.table or just table)
    pub(super) fn parse_qualified_identifier(&mut self) -> Result<String, ParseError> {
        // Parse first identifier
        let first_part = match self.peek() {
            Token::Identifier(sym) | Token::DelimitedIdentifier(sym) => {
                let identifier = self.resolve(*sym);
                self.advance();
                identifier
            }
            Token::Keyword(keyword) => {
                let identifier = keyword.to_string();
                self.advance();
                identifier
            }
            _ => return Err(ParseError { message: "Expected identifier".to_string() }),
        };

        // Check if there's a dot followed by another identifier
        if self.peek() == &Token::Symbol('.') {
            self.advance(); // consume the dot
            let second_part = match self.peek() {
                Token::Identifier(sym) | Token::DelimitedIdentifier(sym) => {
                    let identifier = self.resolve(*sym);
                    self.advance();
                    identifier
                }
                Token::Keyword(keyword) => {
                    let identifier = keyword.to_string();
                    self.advance();
                    identifier
                }
                _ => {
                    return Err(ParseError { message: "Expected identifier after '.'".to_string() })
                }
            };
            Ok(format!("{}.{}", first_part, second_part))
        } else {
            Ok(first_part)
        }
    }

    /// Parse an integer literal and return its value
    pub(super) fn parse_integer_literal(&mut self) -> Result<i64, ParseError> {
        match self.peek() {
            Token::Number(sym) => {
                let num_str = self.resolve_str(*sym).to_string();
                self.advance();
                num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Expected integer, found '{}'", num_str),
                })
            }
            _ => Err(ParseError {
                message: format!("Expected integer literal, found {:?}", self.peek()),
            }),
        }
    }

    /// Consume tokens until semicolon or EOF is reached.
    /// Used for minimal stub implementations that skip optional clauses.
    #[allow(dead_code)]
    pub(super) fn consume_until_semicolon_or_eof(&mut self) {
        while !matches!(self.peek(), Token::Semicolon | Token::Eof) {
            self.advance();
        }
    }

    /// Parse a comma-separated list of identifiers
    ///
    /// This is a common pattern used in GRANT, REVOKE, and other statements
    /// that need to parse lists of user names, role names, or column names.
    pub(super) fn parse_identifier_list(&mut self) -> Result<Vec<String>, ParseError> {
        self.parse_comma_separated_list(|p| p.parse_identifier())
    }
}
