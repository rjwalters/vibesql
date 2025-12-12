use super::{keywords, Lexer, LexerError};
use crate::token::Token;

/// Stack buffer size for case conversion.
/// Most SQL keywords are short (SELECT=6, CURRENT_TIMESTAMP=17).
/// 32 bytes covers all standard SQL keywords with room to spare.
const STACK_BUF_SIZE: usize = 32;

impl<'a> Lexer<'a> {
    /// Tokenize an identifier or keyword.
    ///
    /// This function is optimized to avoid heap allocations when possible:
    /// - For identifiers <= 32 bytes that need case conversion, uses a stack buffer
    /// - Only allocates when the token is confirmed to be an identifier (not a keyword)
    ///
    /// SQL:1999 case-sensitivity:
    /// - Keywords are matched case-insensitively (converted to uppercase for lookup)
    /// - Unquoted identifiers are normalized to LOWERCASE (per SQL:1999 standard)
    pub(super) fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, LexerError> {
        let start = self.position();

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        // Get the identifier text directly from the input slice
        let text = self.slice_from(start);

        // Keywords are case-insensitive (checked with uppercase)
        // Identifiers are normalized to lowercase per SQL:1999
        if text.len() <= STACK_BUF_SIZE {
            let mut upper_buf = [0u8; STACK_BUF_SIZE];
            for (i, b) in text.bytes().enumerate() {
                upper_buf[i] = b.to_ascii_uppercase();
            }
            // SAFETY: Converting ASCII to uppercase/lowercase produces valid UTF-8.
            let upper = unsafe { std::str::from_utf8_unchecked(&upper_buf[..text.len()]) };

            // Try keyword lookup first (case-insensitive)
            if let Some(keyword) = keywords::map_keyword(upper) {
                return Ok(Token::Keyword(keyword));
            }

            // Not a keyword - normalize identifier to lowercase per SQL:1999
            Ok(Token::Identifier(text.to_ascii_lowercase()))
        } else {
            // Long identifier - fall back to heap allocation
            let upper_text = text.to_ascii_uppercase();
            match keywords::map_keyword(&upper_text) {
                Some(keyword) => Ok(Token::Keyword(keyword)),
                // Normalize identifier to lowercase per SQL:1999
                None => Ok(Token::Identifier(text.to_ascii_lowercase())),
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
