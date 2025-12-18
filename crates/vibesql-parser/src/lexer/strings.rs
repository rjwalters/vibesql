use super::{Lexer, LexerError};
use crate::token::Token;

impl<'a> Lexer<'a> {
    /// Tokenize a hex blob literal (x'1234' or X'ABCD').
    /// Returns the decoded bytes as a BlobLiteral token.
    /// SQLite allows whitespace within the hex string.
    pub(super) fn tokenize_blob_literal(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip 'x' or 'X'
        self.advance(); // Skip opening single quote

        let mut hex_chars = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == '\'' {
                self.advance();
                break;
            } else if ch.is_ascii_hexdigit() {
                hex_chars.push(ch);
                self.advance();
            } else if ch.is_ascii_whitespace() {
                // SQLite allows whitespace within hex literals
                self.advance();
            } else {
                return Err(LexerError {
                    message: format!("Invalid character '{}' in hex blob literal", ch),
                    position: self.position(),
                    near_token: Some(format!("x'{}", hex_chars)),
                });
            }
        }

        // Check for even number of hex digits
        if hex_chars.len() % 2 != 0 {
            return Err(LexerError {
                message: "Hex blob literal must have even number of digits".to_string(),
                position: self.position(),
                near_token: Some(format!("x'{}'", hex_chars)),
            });
        }

        // Decode hex string to bytes
        let mut bytes = Vec::with_capacity(hex_chars.len() / 2);
        let mut chars = hex_chars.chars().peekable();
        while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
            let byte = (hi.to_digit(16).unwrap() << 4 | lo.to_digit(16).unwrap()) as u8;
            bytes.push(byte);
        }

        Ok(Token::BlobLiteral(bytes))
    }

    /// Tokenize a string literal enclosed in single quotes.
    /// Supports SQL-standard escaped quotes (e.g., 'O''Reilly' becomes "O'Reilly")
    pub(super) fn tokenize_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.current_char();
        self.advance();

        let mut string_content = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == quote {
                // Check if this is an escaped quote (two consecutive quotes)
                self.advance();
                if !self.is_eof() && self.current_char() == quote {
                    // Escaped quote - add a single quote to the result and continue
                    string_content.push(quote);
                    self.advance();
                } else {
                    // End of string
                    return Ok(Token::String(string_content));
                }
            } else {
                string_content.push(ch);
                self.advance();
            }
        }

        // For unterminated strings, SQLite shows the partial string
        Err(LexerError {
            message: "Unterminated string literal".to_string(),
            position: self.position(),
            near_token: Some(format!("'{}", string_content)),
        })
    }
}
