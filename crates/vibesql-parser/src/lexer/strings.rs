use super::{Lexer, LexerError};
use crate::token::Token;

impl<'a> Lexer<'a> {
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

        Err(LexerError {
            message: "Unterminated string literal".to_string(),
            position: self.position(),
        })
    }
}
