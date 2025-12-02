use super::{keywords, Lexer, LexerError};
use crate::token::Token;

/// Stack buffer size for uppercase conversion.
/// Most SQL keywords are short (SELECT=6, CURRENT_TIMESTAMP=17).
/// 32 bytes covers all standard SQL keywords with room to spare.
const STACK_BUF_SIZE: usize = 32;

impl<'a> Lexer<'a> {
    /// Tokenize an identifier or keyword.
    ///
    /// This function is optimized to avoid heap allocations when possible:
    /// - For identifiers <= 32 bytes that need uppercase conversion, uses a stack buffer
    /// - Only allocates when the token is confirmed to be an identifier (not a keyword)
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

        if needs_uppercase {
            // Fast path: use stack buffer for short identifiers to avoid heap allocation
            // when checking for keywords (which would discard the allocation anyway)
            if text.len() <= STACK_BUF_SIZE {
                let mut buf = [0u8; STACK_BUF_SIZE];
                for (i, b) in text.bytes().enumerate() {
                    buf[i] = b.to_ascii_uppercase();
                }
                // SAFETY: Converting ASCII lowercase to uppercase produces valid UTF-8.
                // The input `text` is valid UTF-8 (from slice_from), and to_ascii_uppercase
                // only modifies ASCII bytes (0x61-0x7A → 0x41-0x5A), preserving UTF-8 validity.
                let upper = unsafe { std::str::from_utf8_unchecked(&buf[..text.len()]) };

                // Try keyword lookup first - no allocation yet
                if let Some(keyword) = keywords::map_keyword(upper) {
                    return Ok(Token::Keyword(keyword));
                }

                // Not a keyword - now allocate for the identifier
                // We need to allocate anyway since identifiers are stored as String
                Ok(Token::Identifier(upper.to_string()))
            } else {
                // Long identifier - fall back to heap allocation
                let upper_text = text.to_ascii_uppercase();
                match keywords::map_keyword(&upper_text) {
                    Some(keyword) => Ok(Token::Keyword(keyword)),
                    None => Ok(Token::Identifier(upper_text)),
                }
            }
        } else {
            // Text is already uppercase - try keyword lookup on the slice directly
            match keywords::map_keyword(text) {
                Some(keyword) => Ok(Token::Keyword(keyword)),
                None => Ok(Token::Identifier(text.to_string())),
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
                    return Ok(Token::DelimitedIdentifier(identifier));
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
                    return Ok(Token::DelimitedIdentifier(identifier));
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
