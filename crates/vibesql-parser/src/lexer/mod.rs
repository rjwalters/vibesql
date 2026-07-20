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

/// Byte range span in the source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Start byte offset (inclusive)
    pub start: usize,
    /// End byte offset (exclusive)
    pub end: usize,
}

impl Span {
    /// Create a new span.
    pub fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }

    /// Extract the text covered by this span from the source.
    pub fn extract<'a>(&self, source: &'a str) -> &'a str {
        &source[self.start..self.end]
    }
}

/// Lexer error returned when tokenization fails.
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub position: usize,
    /// The token text that caused the error (for SQLite-compatible error messages)
    pub near_token: Option<String>,
}

impl LexerError {
    /// Construct an error for a token the tokenizer could not form into a valid
    /// lexeme. SQLite reports these as `unrecognized token: "X"` (distinct from
    /// the grammar-level `near "X": syntax error`), so we carry the full
    /// offending run in a pre-formatted `message` and leave `near_token` unset.
    pub fn unrecognized_token(token: &str, position: usize) -> Self {
        LexerError::preformatted(format!("unrecognized token: \"{}\"", token), position)
    }

    /// Construct an error whose `message` is already a fully-formed,
    /// SQLite-compatible diagnostic. `near_token` is left unset so `Display`
    /// emits the message verbatim rather than wrapping it as a syntax error.
    pub fn preformatted(message: String, position: usize) -> Self {
        LexerError { message, position, near_token: None }
    }
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref token) = self.near_token {
            // SQLite-compatible grammar error: near "TOKEN": syntax error
            write!(f, "near \"{}\": syntax error", token)
        } else {
            // A pre-formatted, SQLite-compatible diagnostic (e.g.
            // `unrecognized token: "X"`, `hex literal too big: X`) — emitted
            // verbatim so it matches SQLite exactly.
            write!(f, "{}", self.message)
        }
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

    /// Returns the original source input.
    pub fn input(&self) -> &'a str {
        self.input
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

    /// Tokenize the entire input, returning tokens with their byte spans.
    ///
    /// This is useful when you need to extract the original source text
    /// for a token (e.g., for preserving original identifier case).
    pub fn tokenize_with_spans(&mut self) -> Result<Vec<(Token, Span)>, LexerError> {
        let estimated_tokens = (self.input.len() / 6).max(4);
        let mut tokens = Vec::with_capacity(estimated_tokens);

        loop {
            self.skip_whitespace_and_comments();

            let start = self.byte_pos;

            if self.is_eof() {
                tokens.push((Token::Eof, Span::new(start, start)));
                break;
            }

            let token = self.next_token()?;
            let end = self.byte_pos;
            tokens.push((token, Span::new(start, end)));
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
            '=' | '<' | '>' | '!' | '|' | '&' | '~' => self.tokenize_operator(ch),
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
            '+' | '*' | '/' | '%' => {
                let symbol = ch;
                self.advance();
                Ok(Token::Symbol(symbol))
            }
            '-' => {
                // Could be -> or ->> (JSON extract operators) or just -
                self.tokenize_operator(ch)
            }
            '\'' => self.tokenize_string(),
            '"' => self.tokenize_delimited_identifier(),
            '`' => self.tokenize_backtick_identifier(),
            '[' => self.tokenize_bracket_identifier(),
            '0'..='9' => self.tokenize_number(),
            'x' | 'X' => {
                // Check if this is a hex blob literal (x'...' or X'...')
                if self.peek_byte(1) == Some(b'\'') {
                    self.tokenize_blob_literal()
                } else {
                    self.tokenize_identifier_or_keyword()
                }
            }
            'a'..='w' | 'y'..='z' | 'A'..='W' | 'Y'..='Z' | '_' => {
                self.tokenize_identifier_or_keyword()
            }
            '?' => {
                // SQLite-style numbered placeholder (?1, ?2, ...) when digits
                // follow; otherwise the anonymous `?` placeholder.
                if self.peek_byte(1).map(|b| b.is_ascii_digit()).unwrap_or(false) {
                    self.tokenize_question_numbered_placeholder()
                } else {
                    self.advance();
                    Ok(Token::Placeholder)
                }
            }
            '$' => {
                // Check what follows the $
                let next = self.peek_byte(1);
                if next.map(|b| b.is_ascii_digit()).unwrap_or(false) {
                    // PostgreSQL-style numbered placeholder ($1, $2, etc.)
                    self.tokenize_numbered_placeholder()
                } else if next
                    .map(|b| b.is_ascii_alphabetic() || b == b'_' || !b.is_ascii())
                    .unwrap_or(false)
                {
                    // SQLite/TCL-style named placeholder ($name, $x1, etc.).
                    // SQLite's IdChar macro treats any byte >= 0x80 as an
                    // identifier char, so non-ASCII starts a named variable
                    // (e.g. `$Ց`). The first byte of a multi-byte UTF-8 char
                    // is always >= 0x80, so the byte-level check is correct.
                    self.tokenize_dollar_named_placeholder()
                } else if next == Some(b':') {
                    // TCL global variable syntax ($::name) - treat as named placeholder
                    self.tokenize_tcl_global_placeholder()
                } else {
                    let token = self.extract_error_token();
                    Err(LexerError {
                        message: "Expected digit or identifier after '$' for placeholder"
                            .to_string(),
                        position: self.position(),
                        near_token: Some(token),
                    })
                }
            }
            ':' => {
                // Check if followed by alphabetic character, underscore, or a
                // non-ASCII byte (SQLite's IdChar accepts bytes >= 0x80) for a
                // named placeholder (e.g. `:name`, `:Ց`)
                if self
                    .peek_byte(1)
                    .map(|b| b.is_ascii_alphabetic() || b == b'_' || !b.is_ascii())
                    .unwrap_or(false)
                {
                    self.tokenize_named_placeholder()
                } else {
                    // Just a colon symbol (could be used in other contexts)
                    self.advance();
                    Ok(Token::Symbol(':'))
                }
            }
            // SQLite treats any non-ASCII character as an identifier character
            // (its IdChar macro accepts all bytes >= 0x80), so a leading
            // multi-byte UTF-8 char starts an identifier (e.g. `SELECT Ցam`).
            ch if !ch.is_ascii() => self.tokenize_identifier_or_keyword(),
            _ => {
                // SQLite-compatible error: extract a token-like span for "near" message
                let token = self.extract_error_token();
                Err(LexerError {
                    message: format!("Unexpected character: '{}'", ch),
                    position: self.byte_pos,
                    near_token: Some(token),
                })
            }
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

    /// Extract a token-like span from current position for error messages.
    /// SQLite shows the problematic token in error messages like: near "#1": syntax error
    /// This reads ahead to capture a reasonable token span (alphanumeric, symbols, etc.)
    fn extract_error_token(&self) -> String {
        let start = self.byte_pos;
        let mut end = start;
        let bytes = self.input.as_bytes();

        // Read ahead to capture a token-like span
        while end < bytes.len() {
            let b = bytes[end];
            // Continue for alphanumeric, symbols that could be part of a token
            if b.is_ascii_alphanumeric() || b == b'_' || b == b'#' || b == b'$' || b == b'@' {
                end += 1;
            } else if end == start {
                // Include at least one character. Advance by the character's
                // full UTF-8 width (not one byte) so the slice below never
                // lands inside a multi-byte character.
                end += self.input[start..].chars().next().map_or(1, char::len_utf8);
                break;
            } else {
                break;
            }
        }

        self.input[start..end].to_string()
    }

    /// Tokenize a session variable (@@variable, @@session.variable, @@global.variable).
    fn tokenize_session_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip first @
        self.advance(); // Skip second @

        let start = self.byte_pos;

        // Read the variable name (which may include scope prefix like 'global'
        // or 'session'). Deliberately ASCII-only: `@@` is a MySQL extension
        // (SQLite rejects it entirely) and MySQL system variable names are
        // ASCII, so SQLite's IdChar non-ASCII rule does not apply here.
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
                near_token: Some("@@".to_string()),
            });
        }

        let var_name = self.slice_from(start).to_string();
        Ok(Token::SessionVariable(var_name))
    }

    /// Tokenize a user variable (@variable).
    fn tokenize_user_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip @

        let start = self.byte_pos;

        // Read the variable name. Like SQLite's IdChar, any non-ASCII char is
        // part of the name (e.g. `@tՑ`).
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
                self.advance();
            } else {
                break;
            }
        }

        if self.byte_pos == start {
            return Err(LexerError {
                message: "Expected variable name after @".to_string(),
                position: self.byte_pos,
                near_token: Some("@".to_string()),
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
                near_token: Some("$".to_string()),
            });
        }

        let index: usize = num_str.parse().map_err(|_| LexerError {
            message: format!("Invalid numbered placeholder: ${}", num_str),
            position: start_pos,
            near_token: Some(format!("${}", num_str)),
        })?;

        // PostgreSQL requires $1 or higher (no $0)
        if index == 0 {
            return Err(LexerError {
                message: "Numbered placeholder must be $1 or higher (no $0)".to_string(),
                position: start_pos,
                near_token: Some("$0".to_string()),
            });
        }

        Ok(Token::NumberedPlaceholder(index))
    }

    /// Tokenize a SQLite-style numbered placeholder (?1, ?2, etc.).
    /// 1-indexed like `$N`; `?0` is rejected (SQLite: "variable number must
    /// be between ?1 and ?NNN"). Only called when a digit follows the `?`.
    fn tokenize_question_numbered_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume '?'

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

        let index: usize = num_str.parse().map_err(|_| LexerError {
            message: format!("Invalid numbered placeholder: ?{}", num_str),
            position: start_pos,
            near_token: Some(format!("?{}", num_str)),
        })?;

        // SQLite requires ?1 or higher (no ?0)
        if index == 0 {
            return Err(LexerError {
                message: "variable number must be between ?1 and ?NNN".to_string(),
                position: start_pos,
                near_token: Some(format!("?{}", num_str)),
            });
        }

        Ok(Token::NumberedPlaceholder(index))
    }

    /// Tokenize a named placeholder (:name, :user_id, etc.).
    fn tokenize_named_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume ':'

        let mut name = String::new();

        // Read the identifier (alphanumeric, underscore, or non-ASCII —
        // SQLite's IdChar accepts any byte >= 0x80, e.g. `:tՑ`)
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
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
                near_token: Some(":".to_string()),
            });
        }

        Ok(Token::NamedPlaceholder(name))
    }

    /// Tokenize a dollar-prefixed named placeholder ($name, $x1, $user_id, etc.).
    /// SQLite/TCL style - same as :name but with $ prefix.
    fn tokenize_dollar_named_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume '$'

        let mut name = String::new();

        // Read the identifier (alphanumeric, underscore, or non-ASCII —
        // SQLite's IdChar accepts any byte >= 0x80, e.g. `$tՑ`)
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
                name.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if name.is_empty() {
            return Err(LexerError {
                message: "Expected identifier after '$' for named placeholder".to_string(),
                position: self.position(),
                near_token: Some("$".to_string()),
            });
        }

        // Use NamedPlaceholder since it's functionally equivalent to :name
        Ok(Token::NamedPlaceholder(name))
    }

    /// Tokenize a TCL global variable placeholder ($::name, $::namespace::var, etc.).
    /// TCL uses $::name for global namespace variables. We treat these as named placeholders.
    fn tokenize_tcl_global_placeholder(&mut self) -> Result<Token, LexerError> {
        self.advance(); // consume '$'

        let mut name = String::new();

        // Consume the :: prefix and any subsequent ::namespace:: parts.
        // Non-ASCII chars are part of the name (SQLite's IdChar accepts any
        // byte >= 0x80, e.g. `$::Ց`).
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == ':' || ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
                name.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if name.is_empty() || name == ":" || name == "::" {
            return Err(LexerError {
                message: "Expected identifier after '$::' for TCL global placeholder".to_string(),
                position: self.position(),
                near_token: Some(format!("${}", name)),
            });
        }

        // Use NamedPlaceholder - the name includes :: prefix for uniqueness
        Ok(Token::NamedPlaceholder(name))
    }
}

#[cfg(test)]
mod extract_error_token_tests {
    use super::Lexer;

    /// `extract_error_token` is the only place in the lexer that does its own
    /// byte arithmetic before slicing. Via the public API it can no longer be
    /// reached with a non-ASCII start char (non-ASCII now lexes as an
    /// identifier), so exercise the char-boundary hardening directly.
    #[test]
    fn test_extract_error_token_multibyte_start_does_not_panic() {
        // 2-byte UTF-8 (U+0551) at the error position
        let lexer = Lexer::new("Ցam");
        assert_eq!(lexer.extract_error_token(), "Ց");

        // 4-byte UTF-8 as the only (and final) character in the input
        let lexer = Lexer::new("😀");
        assert_eq!(lexer.extract_error_token(), "😀");
    }

    #[test]
    fn test_extract_error_token_ascii_unchanged() {
        let lexer = Lexer::new("#1 rest");
        assert_eq!(lexer.extract_error_token(), "#1");

        // Single non-token ASCII char still captured
        let lexer = Lexer::new("^");
        assert_eq!(lexer.extract_error_token(), "^");
    }
}
