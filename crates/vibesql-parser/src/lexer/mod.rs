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
pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    /// Create a new lexer from SQL input.
    pub fn new(input: &str) -> Self {
        Lexer { input: input.chars().collect(), position: 0 }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();

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
                if self.peek(1) == Some('@') {
                    self.tokenize_session_variable()
                } else {
                    self.tokenize_user_variable()
                }
            }
            '.' => {
                // Check if this is the start of a decimal number (e.g., .2, .5E+10)
                if !self.is_eof() && self.peek(1).map(|c| c.is_ascii_digit()).unwrap_or(false) {
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
            _ => Err(LexerError {
                message: format!("Unexpected character: '{}'", ch),
                position: self.position,
            }),
        }
    }

    /// Skip whitespace characters.
    fn skip_whitespace(&mut self) {
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_whitespace() {
                self.advance();
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
            if self.current_char() == '-' && self.peek(1) == Some('-') {
                // Skip until end of line
                while !self.is_eof() && self.current_char() != '\n' {
                    self.advance();
                }
                // Continue loop to skip the newline and any following whitespace/comments
                continue;
            }

            // No more whitespace or comments
            break;
        }
    }

    /// Get current character without advancing.
    pub(super) fn current_char(&self) -> char {
        if self.is_eof() {
            '\0'
        } else {
            self.input[self.position]
        }
    }

    /// Peek ahead n characters without advancing.
    pub(super) fn peek(&self, n: usize) -> Option<char> {
        let peek_pos = self.position + n;
        if peek_pos < self.input.len() {
            Some(self.input[peek_pos])
        } else {
            None
        }
    }

    /// Advance to next character.
    pub(super) fn advance(&mut self) {
        if !self.is_eof() {
            self.position += 1;
        }
    }

    /// Check if we've reached end of input.
    pub(super) fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Tokenize a session variable (@@variable, @@session.variable, @@global.variable).
    fn tokenize_session_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip first @
        self.advance(); // Skip second @

        let mut var_name = String::new();

        // Read the variable name (which may include scope prefix like 'global' or 'session')
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                var_name.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if var_name.is_empty() {
            return Err(LexerError {
                message: "Expected variable name after @@".to_string(),
                position: self.position,
            });
        }

        Ok(Token::SessionVariable(var_name))
    }

    /// Tokenize a user variable (@variable).
    fn tokenize_user_variable(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip @

        let mut var_name = String::new();

        // Read the variable name
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_alphanumeric() || ch == '_' {
                var_name.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if var_name.is_empty() {
            return Err(LexerError {
                message: "Expected variable name after @".to_string(),
                position: self.position,
            });
        }

        Ok(Token::UserVariable(var_name))
    }
}
