use super::{keywords, Lexer, LexerError};
use crate::token::Token;

impl<'a> Lexer<'a> {
    /// Tokenize an identifier or keyword.
    pub(super) fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, LexerError> {
        let start = self.position();
        let mut needs_uppercase = false;

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                // Track if we have lowercase letters that need conversion
                if ch.is_ascii_lowercase() {
                    needs_uppercase = true;
                }
                self.advance();
            } else {
                break;
            }
        }

        // Get the identifier text directly from the input slice
        let text = self.slice_from(start);

        // Optimization: only allocate/uppercase if needed
        if needs_uppercase {
            let upper_text = text.to_ascii_uppercase();
            // Use perfect hash map for O(1) keyword lookup
            match keywords::map_keyword(&upper_text) {
                Some(keyword) => Ok(Token::Keyword(keyword)),
                None => Ok(Token::Identifier(self.intern(upper_text))),
            }
        } else {
            // Text is already uppercase - try keyword lookup on the slice directly
            match keywords::map_keyword(text) {
                Some(keyword) => Ok(Token::Keyword(keyword)),
                None => Ok(Token::Identifier(self.intern(text))),
            }
        }
    }

    /// Tokenize a delimited identifier enclosed in double quotes.
    /// Delimited identifiers are case-sensitive and can contain reserved words.
    /// Supports SQL-standard escaped quotes (e.g., "O""Reilly" becomes O"Reilly)
    pub(super) fn tokenize_delimited_identifier(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip opening quote

        let mut identifier = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == '"' {
                self.advance();
                // Check for escaped quote ("")
                if !self.is_eof() && self.current_char() == '"' {
                    // Escaped quote - add a single quote to the identifier
                    identifier.push('"');
                    self.advance();
                } else {
                    // End of delimited identifier
                    // Reject empty delimited identifiers
                    if identifier.is_empty() {
                        return Err(LexerError {
                            message: "Empty delimited identifier is not allowed".to_string(),
                            position: self.position(),
                        });
                    }
                    return Ok(Token::DelimitedIdentifier(self.intern(identifier)));
                }
            } else {
                identifier.push(ch);
                self.advance();
            }
        }

        Err(LexerError {
            message: "Unterminated delimited identifier".to_string(),
            position: self.position(),
        })
    }

    /// Tokenize a backtick-delimited identifier (MySQL-style).
    /// Backtick identifiers are case-sensitive and can contain reserved words.
    /// Supports doubled backticks as escape (e.g., `O``Reilly` becomes O`Reilly)
    pub(super) fn tokenize_backtick_identifier(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip opening backtick

        let mut identifier = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == '`' {
                self.advance();
                // Check for escaped backtick (``)
                if !self.is_eof() && self.current_char() == '`' {
                    // Escaped backtick - add a single backtick to the identifier
                    identifier.push('`');
                    self.advance();
                } else {
                    // End of delimited identifier
                    // Reject empty delimited identifiers
                    if identifier.is_empty() {
                        return Err(LexerError {
                            message: "Empty delimited identifier is not allowed".to_string(),
                            position: self.position(),
                        });
                    }
                    return Ok(Token::DelimitedIdentifier(self.intern(identifier)));
                }
            } else {
                identifier.push(ch);
                self.advance();
            }
        }

        Err(LexerError {
            message: "Unterminated delimited identifier".to_string(),
            position: self.position(),
        })
    }
}
