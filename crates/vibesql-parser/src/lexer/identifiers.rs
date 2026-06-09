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
    /// **Case handling**: Keywords are matched case-insensitively (using uppercase),
    /// but identifiers preserve their original case from the SQL text. This matches
    /// SQLite's behavior where `SELECT col FROM t` returns "col" as the column name
    /// (preserving the case from the query for display purposes).
    ///
    /// **Non-ASCII handling**: SQLite treats any byte >= 0x80 as an identifier
    /// character (its `IdChar` macro), so identifiers like `tՑ` or `t1Ցam` are
    /// valid. We match that by accepting any non-ASCII char as an identifier
    /// continuation character. `advance()` steps by `char::len_utf8()`, so slice
    /// boundaries always stay on UTF-8 char boundaries.
    pub(super) fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, LexerError> {
        let start = self.position();

        while !self.is_eof() {
            let ch = self.current_char();
            // Note: at EOF current_char() returns '\0' (ASCII), but the loop
            // already guards with !self.is_eof(), so termination is unaffected.
            if ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
                self.advance();
            } else {
                break;
            }
        }

        // Get the identifier text directly from the input slice
        let text = self.slice_from(start);

        // Keywords are case-insensitive (checked with uppercase)
        // Identifiers preserve their original case for display
        if text.len() <= STACK_BUF_SIZE {
            let mut upper_buf = [0u8; STACK_BUF_SIZE];
            for (i, b) in text.bytes().enumerate() {
                upper_buf[i] = b.to_ascii_uppercase();
            }
            // SAFETY: `to_ascii_uppercase` maps ASCII bytes to ASCII bytes and
            // leaves non-ASCII bytes (>= 0x80) unchanged, so the buffer is a
            // byte-for-byte copy of valid UTF-8 except for ASCII case changes,
            // which preserves UTF-8 validity. (Keywords are all pure ASCII, so
            // an identifier containing non-ASCII bytes can never falsely match
            // a keyword.)
            let upper = unsafe { std::str::from_utf8_unchecked(&upper_buf[..text.len()]) };

            // Try keyword lookup first (case-insensitive)
            if let Some(keyword) = keywords::map_keyword(upper) {
                return Ok(Token::Keyword { keyword, original: text.to_string() });
            }

            // Not a keyword - preserve original case from SQL text
            Ok(Token::Identifier(text.to_string()))
        } else {
            // Long identifier - fall back to heap allocation
            let upper_text = text.to_ascii_uppercase();
            match keywords::map_keyword(&upper_text) {
                Some(keyword) => Ok(Token::Keyword { keyword, original: text.to_string() }),
                // Not a keyword - preserve original case from SQL text
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
                            near_token: Some("\"\"".to_string()),
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
            near_token: Some(format!("\"{}", identifier)),
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
                            near_token: Some("``".to_string()),
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
            near_token: Some(format!("`{}", identifier)),
        })
    }

    /// Tokenize a bracket-delimited identifier (SQLite-style).
    /// Bracket identifiers are case-sensitive and can contain reserved words.
    /// Supports doubled brackets as escape (e.g., [O]]Reilly] becomes O]Reilly)
    pub(super) fn tokenize_bracket_identifier(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip opening bracket

        let mut identifier = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == ']' {
                self.advance();
                // Check for escaped bracket (]])
                if !self.is_eof() && self.current_char() == ']' {
                    // Escaped bracket - add a single bracket to the identifier
                    identifier.push(']');
                    self.advance();
                } else {
                    // End of delimited identifier
                    // Reject empty delimited identifiers
                    if identifier.is_empty() {
                        return Err(LexerError {
                            message: "Empty delimited identifier is not allowed".to_string(),
                            position: self.position(),
                            near_token: Some("[]".to_string()),
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
            near_token: Some(format!("[{}", identifier)),
        })
    }
}
