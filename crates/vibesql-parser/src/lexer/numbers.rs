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
            } else if ch == '_' {
                // SQLite 3.46+ supports underscores as digit separators in
                // numeric literals (e.g. 1_000_000, 1.1_1). An underscore is
                // only valid when surrounded on both sides by digits — this
                // holds in both the integer and fractional parts. When it is
                // not between two digits (e.g. `1_.5`) we stop here and let the
                // trailing-character check below reject the malformed token.
                if self.underscore_between_digits(start) {
                    has_underscore = true;
                    self.advance();
                } else {
                    break;
                }
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

                // Exponent digits (required). Underscore separators are also
                // permitted here as long as they sit between two digits
                // (e.g. `1e1_0`).
                let exp_start = self.position();
                while !self.is_eof() {
                    let ch = self.current_char();
                    if ch.is_ascii_digit() {
                        self.advance();
                    } else if ch == '_' && self.underscore_between_digits(start) {
                        has_underscore = true;
                        self.advance();
                    } else {
                        break;
                    }
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

        // A numeric literal that runs directly into an identifier character
        // (letter or underscore) is a malformed token, not a value followed by
        // an alias. SQLite rejects these: `123a456`, `1FROM`, `1.5x` all raise
        // "unrecognized token". Previously the lexer split them into a number
        // plus an identifier, silently changing the meaning.
        self.reject_trailing_identifier_char(start)?;

        let number = self.slice_from(start).to_string();
        // Strip underscores from the token value so downstream sees a plain number
        if has_underscore {
            let stripped = number.replace('_', "");
            Ok(Token::Number(stripped))
        } else {
            Ok(Token::Number(number))
        }
    }

    /// Whether the underscore at the current position sits directly between two
    /// digits. `start` is the byte offset where the number literal began, used
    /// to guard against reading before the token.
    #[inline]
    fn underscore_between_digits(&self, start: usize) -> bool {
        let prev_is_digit =
            self.position() > start && self.input.as_bytes()[self.position() - 1].is_ascii_digit();
        let next_is_digit = self.peek_byte(1).map(|b| b.is_ascii_digit()).unwrap_or(false);
        prev_is_digit && next_is_digit
    }

    /// Reject a numeric literal that is immediately followed by an identifier
    /// character (an ASCII/Unicode letter or an underscore). SQLite treats
    /// `123a456` and friends as a single unrecognized token rather than a
    /// number adjacent to an identifier.
    fn reject_trailing_identifier_char(&self, start: usize) -> Result<(), LexerError> {
        if self.is_eof() {
            return Ok(());
        }
        let ch = self.current_char();
        if ch.is_alphabetic() || ch == '_' {
            // Capture the full offending run for a helpful error message.
            let bad = self.slice_from(start);
            return Err(LexerError {
                message: format!("unrecognized token: \"{}\"", bad),
                position: self.position(),
                near_token: Some(bad.to_string()),
            });
        }
        Ok(())
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

        // A hex literal running directly into an identifier character (e.g.
        // `0x1Ag`) is a malformed token in SQLite, not a value plus alias.
        self.reject_trailing_identifier_char(hex_start)?;

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
    fn test_underscore_in_fraction_and_exponent() {
        // SQLite 3.46+ allows underscore separators after the decimal point and
        // inside the exponent, as long as each underscore sits between digits.
        let test_cases = vec![
            ("1.1_1", "1.11"),
            ("1_000.5", "1000.5"),
            ("1.000_5", "1.0005"),
            ("1e1_0", "1e10"),
            ("1.5e1_0", "1.5e10"),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens = lexer
                .tokenize()
                .unwrap_or_else(|e| panic!("Failed to tokenize {}: {:?}", input, e));
            assert_eq!(tokens.len(), 2, "Input: {}", input);
            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }
        }
    }

    #[test]
    fn test_underscore_not_between_digits_errors() {
        // Underscore not surrounded by digits is a malformed token in SQLite.
        for input in ["1_.5", "1._5"] {
            let mut lexer = Lexer::new(input);
            assert!(lexer.tokenize().is_err(), "Expected error for input: {}", input);
        }
    }

    #[test]
    fn test_number_followed_by_identifier_char_errors() {
        // A number running into an identifier character is a single malformed
        // token, not `<number> AS <alias>`.
        for input in ["123a456", "1FROM", "1.5x", "0x1Ag", "42abc"] {
            let mut lexer = Lexer::new(input);
            assert!(lexer.tokenize().is_err(), "Expected error for input: {}", input);
        }
    }

    #[test]
    fn test_number_followed_by_operator_ok() {
        // Numbers adjacent to operators / punctuation must still tokenize.
        for input in ["1+2", "3.14*2", "5,6", "(7)", "8;"] {
            let mut lexer = Lexer::new(input);
            assert!(lexer.tokenize().is_ok(), "Expected ok for input: {}", input);
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
