//! SQL Lexer module - tokenizes SQL text into a stream of tokens.
//!
//! The lexer is organized into focused submodules:
//! - `keywords`: SQL keyword recognition and mapping
//! - `numbers`: Numeric literal parsing (integers, decimals, scientific notation)
//! - `strings`: String literal parsing with escape handling
//! - `identifiers`: Regular and delimited identifier handling
//! - `operators`: Multi-character operator recognition

use std::fmt;

use crate::token::Token;

mod identifiers;
mod keywords;
mod numbers;
mod operators;
mod strings;

/// Lexer error returned when tokenization fails.
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lexer error at position {}: {}", self.position, self.message)
    }
}

/// SQL Lexer - converts SQL text into tokens.
///
/// Uses direct &str access for zero-copy tokenization.
/// Tracks byte position for efficient slicing.
pub struct Lexer<'a> {
    input: &'a str,
    byte_pos: usize,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer from SQL input.
    #[inline]
    pub fn new(input: &'a str) -> Self {
        Lexer { input, byte_pos: 0 }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        // Pre-allocate based on estimated token count (~1 token per 6 bytes)
        let estimated_tokens = (self.input.len() / 6).max(4);
        let mut tokens = Vec::with_capacity(estimated_tokens);

        loop {
            self.skip_whitespace_and_comments();

            if self.is_eof() {
                tokens.push(Token::Eof);
                break;
            }

            let token = self.next_token()?;
            tokens.push(token);
        }

        Ok(tokens)
    }

    /// Get the next token.
    fn next_token(&mut self) -> Result<Token, LexerError> {
        let ch = self.current_char();

        match ch {
            ';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            '(' => {
                self.advance();
                Ok(Token::LParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RParen)
            }
            '=' | '<' | '>' | '!' | '|' => self.tokenize_operator(ch),
            '@' => {
                // Check for @@ (session variable) or @ (user variable)
                if self.peek_byte(1) == Some(b'@') {
                    self.tokenize_session_variable()
                } else {
                    self.tokenize_user_variable()
                }
            }
            '.' => {
                // Check if this is the start of a decimal number (e.g., .2, .5E+10)
                if self.peek_byte(1).map(|b| b.is_ascii_digit()).unwrap_or(false) {
                    self.tokenize_number()
                } else {
                    self.advance();
                    Ok(Token::Symbol('.'))
                }
            }
            '+' | '-' | '*' | '/' => {
                let symbol = ch;
                self.advance();
                Ok(Token::Symbol(symbol))
            }
            '\'' => self.tokenize_string(),
            '"' => self.tokenize_delimited_identifier(),
            '`' => self.tokenize_backtick_identifier(),
            '0'..='9' => self.tokenize_number(),
            'a'..='z' | 'A'..='Z' | '_' => self.tokenize_identifier_or_keyword(),
            '?' => {
                self.advance();
                Ok(Token::Placeholder)
            }
            '$' => {
                // Check if followed by digits for numbered placeholder ($1, $2, etc.)
                if self.peek_byte(1).map(|b| b.is_ascii_digit()).unwrap_or(false) {
                    self.tokenize_numbered_placeholder()
                } else {
                    Err(LexerError {
                        message: "Expected digit after '$' for numbered placeholder".to_string(),
                        position: self.position(),
                    })
                }
            }
            ':' => {
                // Check if followed by alphabetic character or underscore for named placeholder
                if self.peek_byte(1).map(|b| b.is_ascii_alphabetic() || b == b'_').unwrap_or(false)
                {
                    self.tokenize_named_placeholder()
                } else {
                    // Just a colon symbol (could be used in other contexts)
                    self.advance();
                    Ok(Token::Symbol(':'))
                }
            }
            _ => Err(LexerError {
                message: format!("Unexpected character: '{}'", ch),
                position: self.byte_pos,
            }),
        }
    }

    /// Skip whitespace characters.
    #[inline]
    fn skip_whitespace(&mut self) {
        while let Some(b) = self.peek_byte(0) {
            if b.is_ascii_whitespace() {
                self.byte_pos += 1;
            } else {
                break;
            }
        }
    }

    /// Skip whitespace and SQL comments.
    /// SQL supports line comments starting with -- until end of line.
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            self.skip_whitespace();

            if self.is_eof() {
                break;
            }

            // Check for -- line comment
            if self.peek_byte(0) == Some(b'-') && self.peek_byte(1) == Some(b'-') {
                // Skip until end of line
                while let Some(b) = self.peek_byte(0) {
                    self.byte_pos += 1;
                    if b == b'\n' {
                        break;
                    }
                }
                // Continue loop to skip the newline and any following whitespace/comments
                continue;
            }

            // No more whitespace or comments
            break;
        }
    }

    /// Get current character without advancing.
    #[inline]
    pub(super) fn current_char(&self) -> char {
        if self.byte_pos >= self.input.len() {
            '\0'
        } else {
            // Fast path for ASCII (most common case in SQL)
            let b = self.input.as_bytes()[self.byte_pos];
            if b.is_ascii() {
                b as char
            } else {
                // Slow path for multi-byte UTF-8
                self.input[self.byte_pos..].chars().next().unwrap_or('\0')
            }
        }
    }

    /// Peek ahead n bytes without advancing (for ASCII characters).
    #[inline]
    pub(super) fn peek_byte(&self, n: usize) -> Option<u8> {
        let peek_pos = self.byte_pos + n;
        if peek_pos < self.input.len() {
            Some(self.input.as_bytes()[peek_pos])
        } else {
            None
        }
    }

    /// Advance to next character.
    #[inline]
    pub(super) fn advance(&mut self) {
        if self.byte_pos < self.input.len() {
            // Fast path for ASCII
            let b = self.input.as_bytes()[self.byte_pos];
            if b.is_ascii() {
                self.byte_pos += 1;
            } else {
                // Slow path for multi-byte UTF-8
                if let Some(ch) = self.input[self.byte_pos..].chars().next() {
                    self.byte_pos += ch.len_utf8();
                }
            }
        }
    }

    /// Check if we've reached end of input.
    #[inline]
    pub(super) fn is_eof(&self) -> bool {
        self.byte_pos >= self.input.len()
    }

    /// Get the current byte position (for error reporting).
    #[inline]
    pub(super) fn position(&self) -> usize {
        self.byte_pos
    }

    /// Get a slice of the input from start to current position.
    #[inline]
    pub(super) fn slice_from(&self, start: usize) -> &'a str {
        &self.input[start..self.byte_pos]
    }

    /// Tokenize a session variable (@@variable, @@session.variable, @@global.variable).
    fn tokenize_session_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip first @
        self.advance(); // Skip second @

        let start = self.byte_pos;

        // Read the variable name (which may include scope prefix like 'global' or 'session')
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                self.advance();
            } else {
                break;
            }
        }

        if self.byte_pos == start {
            return Err(LexerError {
                message: "Expected variable name after @@".to_string(),
                position: self.byte_pos,
            });
        }

        let var_name = self.slice_from(start).to_string();
        Ok(Token::SessionVariable(var_name))
    }

    /// Tokenize a user variable (@variable).
    fn tokenize_user_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip @

        let start = self.byte_pos;

        // Read the variable name
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        if self.byte_pos == start {
            return Err(LexerError {
                message: "Expected variable name after @".to_string(),
                position: self.byte_pos,
            });
        }

        let var_name = self.slice_from(start).to_string();
        Ok(Token::UserVariable(var_name))
    }

    /// Tokenize a numbered placeholder ($1, $2, etc.).
    /// PostgreSQL-style: 1-indexed ($1 = first parameter).
    fn tokenize_numbered_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume '$'

        let start_pos = self.position();
        let mut num_str = String::new();

        // Read all digits
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_digit() {
                num_str.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if num_str.is_empty() {
            return Err(LexerError {
                message: "Expected digit after '$' for numbered placeholder".to_string(),
                position: start_pos,
            });
        }

        let index: usize = num_str.parse().map_err(|_| LexerError {
            message: format!("Invalid numbered placeholder: ${}", num_str),
            position: start_pos,
        })?;

        // PostgreSQL requires $1 or higher (no $0)
        if index == 0 {
            return Err(LexerError {
                message: "Numbered placeholder must be $1 or higher (no $0)".to_string(),
                position: start_pos,
            });
        }

        Ok(Token::NumberedPlaceholder(index))
    }

    /// Tokenize a named placeholder (:name, :user_id, etc.).
    fn tokenize_named_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume ':'

        let mut name = String::new();

        // Read the identifier (alphanumeric or underscore)
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                name.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if name.is_empty() {
            return Err(LexerError {
                message: "Expected identifier after ':' for named placeholder".to_string(),
                position: self.position(),
            });
        }

        Ok(Token::NamedPlaceholder(name))
    }
}
