use super::{Lexer, LexerError};
use crate::token::{MultiCharOperator, Token};

impl<'a> Lexer<'a> {
    /// Tokenize comparison and logical operators.
    /// Handles multi-character operators like <=, >=, !=, <>, ||
    /// Also handles pgvector-compatible distance operators: <->, <#>, <=>
    pub(super) fn tokenize_operator(&mut self, ch: char) -> Result<Token, LexerError> {
        match ch {
            '<' => {
                self.advance();
                if self.is_eof() {
                    return Ok(Token::Symbol('<'));
                }

                let next_ch = self.current_char();
                match next_ch {
                    '<' => {
                        // << (left shift)
                        self.advance();
                        Ok(Token::Operator(MultiCharOperator::LeftShift))
                    }
                    '-' => {
                        // Could be <-> (cosine distance)
                        if self.peek_byte(1) == Some(b'>') {
                            self.advance(); // consume '-'
                            self.advance(); // consume '>'
                            Ok(Token::Operator(MultiCharOperator::CosineDistance))
                        } else {
                            // Just '<' followed by '-' (separate tokens)
                            Ok(Token::Symbol('<'))
                        }
                    }
                    '#' => {
                        // Could be <#> (negative inner product)
                        if self.peek_byte(1) == Some(b'>') {
                            self.advance(); // consume '#'
                            self.advance(); // consume '>'
                            Ok(Token::Operator(MultiCharOperator::NegativeInnerProduct))
                        } else {
                            // Just '<' followed by '#' - return '<' as symbol
                            Ok(Token::Symbol('<'))
                        }
                    }
                    '=' => {
                        // Could be <=> (L2 distance) or <= (less than or equal)
                        if self.peek_byte(1) == Some(b'>') {
                            self.advance(); // consume '='
                            self.advance(); // consume '>'
                            Ok(Token::Operator(MultiCharOperator::L2Distance))
                        } else {
                            self.advance(); // consume '='
                            Ok(Token::Operator(MultiCharOperator::LessEqual))
                        }
                    }
                    '>' => {
                        self.advance();
                        Ok(Token::Operator(MultiCharOperator::NotEqualAlt))
                    }
                    _ => Ok(Token::Symbol('<')),
                }
            }
            '=' | '>' | '!' => {
                self.advance();
                if !self.is_eof() {
                    let next_ch = self.current_char();
                    match (ch, next_ch) {
                        ('>', '=') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::GreaterEqual))
                        }
                        ('>', '>') => {
                            // >> (right shift)
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::RightShift))
                        }
                        ('!', '=') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::NotEqual))
                        }
                        ('=', '=') => {
                            // SQLite compatibility: == is a synonym for =
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::DoubleEqual))
                        }
                        _ => Ok(Token::Symbol(ch)),
                    }
                } else {
                    Ok(Token::Symbol(ch))
                }
            }
            '|' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '|' {
                    self.advance();
                    Ok(Token::Operator(MultiCharOperator::Concat))
                } else {
                    // Single | is bitwise OR in SQLite
                    Ok(Token::Symbol('|'))
                }
            }
            '&' => {
                // & is bitwise AND in SQLite
                self.advance();
                Ok(Token::Symbol('&'))
            }
            '~' => {
                // ~ is bitwise NOT in SQLite
                self.advance();
                Ok(Token::Symbol('~'))
            }
            _ => {
                self.advance();
                Ok(Token::Symbol(ch))
            }
        }
    }
}
