use super::{Lexer, LexerError};
use crate::token::{MultiCharOperator, Token};

impl<'a> Lexer<'a> {
    /// Tokenize comparison and logical operators.
    /// Handles multi-character operators like <=, >=, !=, <>, ||
    pub(super) fn tokenize_operator(&mut self, ch: char) -> Result<Token, LexerError> {
        match ch {
            '=' | '<' | '>' | '!' => {
                self.advance();
                if !self.is_eof() {
                    let next_ch = self.current_char();
                    match (ch, next_ch) {
                        ('<', '=') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::LessEqual))
                        }
                        ('>', '=') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::GreaterEqual))
                        }
                        ('!', '=') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::NotEqual))
                        }
                        ('<', '>') => {
                            self.advance();
                            Ok(Token::Operator(MultiCharOperator::NotEqualAlt))
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
                    Err(LexerError {
                        message: "Unexpected character: '|' (did you mean '||'?)".to_string(),
                        position: self.position() - 1,
                    })
                }
            }
            _ => {
                self.advance();
                Ok(Token::Symbol(ch))
            }
        }
    }
}
