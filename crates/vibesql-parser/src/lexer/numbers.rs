use super::{Lexer, LexerError};
use crate::token::Token;

impl<'a> Lexer<'a> {
    /// Tokenize a number literal.
    /// Supports integers, decimals, and scientific notation (E-notation).
    /// Examples: 42, 3.14, 2.5E+10, 1.2E-5, .2E+2
    pub(super) fn tokenize_number(&mut self) -> Result<Token, LexerError> {
        let start = self.position();
        let mut has_dot = false;

        // Handle leading decimal point (e.g., .5)
        if !self.is_eof() && self.current_char() == '.' {
            has_dot = true;
            self.advance();
        }

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_digit() {
                self.advance();
            } else if ch == '.' && !has_dot {
                has_dot = true;
                self.advance();
            } else {
                break;
            }
        }

        // Check for scientific notation (E-notation)
        if !self.is_eof() {
            let ch = self.current_char();
            if ch == 'E' || ch == 'e' {
                self.advance();

                // Optional sign (+/-)
                if !self.is_eof() {
                    let sign = self.current_char();
                    if sign == '+' || sign == '-' {
                        self.advance();
                    }
                }

                // Exponent digits (required)
                let exp_start = self.position();
                while !self.is_eof() && self.current_char().is_ascii_digit() {
                    self.advance();
                }

                // Verify we got at least one exponent digit
                if self.position() == exp_start {
                    return Err(LexerError {
                        message: "Invalid scientific notation: expected digits after 'E'"
                            .to_string(),
                        position: self.position(),
                    });
                }
            }
        }

        let number = self.slice_from(start).to_string();
        Ok(Token::Number(number))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scientific_notation() {
        // Test various E-notation formats
        let test_cases = vec![
            ("2.5E+10", "2.5E+10"),
            ("1.2E-5", "1.2E-5"),
            (".2E+2", ".2E+2"),
            ("2E10", "2E10"),
            ("2.E-5", "2.E-5"),
            ("2.5e10", "2.5e10"), // lowercase e
            (".5E2", ".5E2"),
            ("123.456E-78", "123.456E-78"),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens =
                lexer.tokenize().unwrap_or_else(|_| panic!("Failed to tokenize: {}", input));

            // Should have exactly 2 tokens: the number and EOF
            assert_eq!(tokens.len(), 2, "Input: {}", input);

            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }

            assert_eq!(tokens[1], Token::Eof);
        }
    }

    #[test]
    fn test_decimal_point_start() {
        // Test numbers starting with decimal point
        let test_cases = vec![(".5", ".5"), (".123", ".123"), (".2E+2", ".2E+2")];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens =
                lexer.tokenize().unwrap_or_else(|_| panic!("Failed to tokenize: {}", input));

            assert_eq!(tokens.len(), 2, "Input: {}", input);

            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }
        }
    }

    #[test]
    fn test_invalid_scientific_notation() {
        // Test invalid E-notation should fail
        let invalid_cases = vec![
            "2E",  // No exponent digits
            "2E+", // No exponent digits after sign
            "2E-", // No exponent digits after sign
        ];

        for input in invalid_cases {
            let mut lexer = Lexer::new(input);
            let result = lexer.tokenize();
            assert!(result.is_err(), "Expected error for input: {}", input);
        }
    }
}
