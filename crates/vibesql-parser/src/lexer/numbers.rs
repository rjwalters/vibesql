use super::{Lexer, LexerError};
use crate::token::Token;

impl<'a> Lexer<'a> {
    /// Tokenize a number literal.
    /// Supports integers, decimals, scientific notation (E-notation), hex literals,
    /// and underscore-separated digits (e.g., 1_000_000).
    /// Examples: 42, 3.14, 2.5E+10, 1.2E-5, .2E+2, 0x1A, 0xFF, 10_000_000_000
    pub(super) fn tokenize_number(&mut self) -> Result<Token, LexerError> {
        let start = self.position();
        let mut has_dot = false;
        let mut has_underscore = false;

        // Handle leading decimal point (e.g., .5)
        if !self.is_eof() && self.current_char() == '.' {
            has_dot = true;
            self.advance();
        }

        // Check for hex literal (0x... or 0X...)
        if !has_dot && !self.is_eof() && self.current_char() == '0' {
            self.advance();
            if !self.is_eof() && (self.current_char() == 'x' || self.current_char() == 'X') {
                return self.tokenize_hex_literal();
            }
            // Not a hex literal, continue with normal number parsing
            // The '0' is already consumed, so we continue from here
        }

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_digit() {
                self.advance();
            } else if ch == '_' && !has_dot {
                // SQLite 3.46+ supports underscores as digit separators in integer literals
                // e.g., 1_000_000_000, 10_000
                // Only allowed between digits, not in decimal/float parts
                has_underscore = true;
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
                    let partial = self.slice_from(start);
                    return Err(LexerError {
                        message: "Invalid scientific notation: expected digits after 'E'"
                            .to_string(),
                        position: self.position(),
                        near_token: Some(partial.to_string()),
                    });
                }
            }
        }

        let number = self.slice_from(start).to_string();
        // Strip underscores from the token value so downstream sees a plain number
        if has_underscore {
            let stripped = number.replace('_', "");
            Ok(Token::Number(stripped))
        } else {
            Ok(Token::Number(number))
        }
    }

    /// Tokenize a hexadecimal literal (0x... or 0X...).
    /// The '0x' or '0X' prefix has already been consumed when this is called.
    /// Returns the decimal representation of the hex value.
    fn tokenize_hex_literal(&mut self) -> Result<Token, LexerError> {
        // Consume the 'x' or 'X'
        self.advance();

        let hex_start = self.position();

        // Collect hex digits
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_hexdigit() {
                self.advance();
            } else {
                break;
            }
        }

        let hex_str = self.slice_from(hex_start);

        if hex_str.is_empty() {
            return Err(LexerError {
                message: "Invalid hexadecimal literal: expected hex digits after '0x'".to_string(),
                position: self.position(),
                near_token: Some("0x".to_string()),
            });
        }

        // Parse the hex string to a number and convert to decimal string
        // SQLite treats hex literals as 64-bit signed integers
        match i64::from_str_radix(hex_str, 16) {
            Ok(value) => Ok(Token::Number(value.to_string())),
            Err(_) => {
                // For very large hex values, try parsing as u64 first
                match u64::from_str_radix(hex_str, 16) {
                    Ok(value) => {
                        // SQLite interprets large hex values as signed (two's complement)
                        Ok(Token::Number((value as i64).to_string()))
                    }
                    Err(_) => Err(LexerError {
                        message: format!("Hexadecimal literal '0x{}' is too large", hex_str),
                        position: self.position(),
                        near_token: Some(format!("0x{}", hex_str)),
                    }),
                }
            }
        }
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

    #[test]
    fn test_hex_literals() {
        // Test hexadecimal literal parsing (0x... or 0X...)
        // SQLite converts hex to decimal representation
        let test_cases = vec![
            ("0x10", "16"),
            ("0xFF", "255"),
            ("0xff", "255"), // lowercase hex digits
            ("0X10", "16"),  // uppercase X
            ("0x0", "0"),
            ("0x7FFFFFFF", "2147483647"),                  // max i32
            ("0x7FFFFFFFFFFFFFFF", "9223372036854775807"), // max i64
        ];

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
    fn test_hex_literals_twos_complement() {
        // Large hex values are interpreted as signed (two's complement)
        let test_cases = vec![
            ("0xFFFFFFFFFFFFFFFF", "-1"),                   // all bits set = -1
            ("0x8000000000000000", "-9223372036854775808"), // min i64
        ];

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
    fn test_invalid_hex_literals() {
        // Test invalid hex literals should fail
        let invalid_cases = vec![
            "0x", // No hex digits after 0x
            "0X", // No hex digits after 0X
        ];

        for input in invalid_cases {
            let mut lexer = Lexer::new(input);
            let result = lexer.tokenize();
            assert!(result.is_err(), "Expected error for input: {}", input);
        }
    }

    #[test]
    fn test_underscore_separated_integers() {
        // SQLite 3.46+ supports underscores as digit separators in integer literals
        let test_cases = vec![
            ("1_000", "1000"),
            ("1_000_000", "1000000"),
            ("10_000_000_000", "10000000000"),
            ("1_2_3", "123"),
            ("100_00", "10000"),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens =
                lexer.tokenize().unwrap_or_else(|_| panic!("Failed to tokenize: {}", input));

            assert_eq!(tokens.len(), 2, "Input: {}", input);

            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }

            assert_eq!(tokens[1], Token::Eof);
        }
    }
}
