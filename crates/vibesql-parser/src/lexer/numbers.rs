use super::{Lexer, LexerError, PrevTokenKind};
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

        // Handle leading decimal point (e.g., .5). When present, the digit run
        // consumed below is the fractional part.
        if !self.is_eof() && self.current_char() == '.' {
            has_dot = true;
            self.advance();
        }

        // Check for hex literal (0x... or 0X...).
        if !has_dot
            && !self.is_eof()
            && self.current_char() == '0'
            && matches!(self.peek_byte(1), Some(b'x') | Some(b'X'))
        {
            self.advance(); // '0'
            self.advance(); // 'x' / 'X'
            return self.tokenize_hex_literal(start);
        }

        // Integer part (or fractional part, when a leading dot was consumed).
        // Digits and underscores are consumed greedily and underscore placement
        // is validated *after* the whole token is scanned — this mirrors
        // SQLite's tokenizer, which absorbs the entire numeric-looking run and
        // only then flags a misplaced separator, reporting the full run as one
        // unrecognized token (e.g. `1_.4` -> `unrecognized token: "1_.4"`).
        self.consume_digits_and_underscores(&mut has_underscore);

        // Fractional part.
        if !has_dot && !self.is_eof() && self.current_char() == '.' {
            self.advance();
            self.consume_digits_and_underscores(&mut has_underscore);
        }

        // Scientific notation (E-notation). SQLite only treats `e`/`E` as an
        // exponent introducer when it is directly followed by a digit, or by a
        // sign and then a digit. Otherwise the `e` is just a trailing identifier
        // character, which makes the whole token unrecognized (`1e_4`, `2E+`).
        if !self.is_eof() {
            let ch = self.current_char();
            if ch == 'e' || ch == 'E' {
                let exp_ok = match self.peek_byte(1) {
                    Some(b) if b.is_ascii_digit() => true,
                    Some(b'+') | Some(b'-') => {
                        self.peek_byte(2).map(|b| b.is_ascii_digit()).unwrap_or(false)
                    }
                    _ => false,
                };
                if exp_ok {
                    self.advance(); // 'e' / 'E'
                    if matches!(self.current_char(), '+' | '-') {
                        self.advance();
                    }
                    self.consume_digits_and_underscores(&mut has_underscore);
                }
            }
        }

        // A numeric literal that runs directly into an identifier character
        // (letter, digit, or underscore) is a single malformed token, not a
        // value followed by an alias. SQLite absorbs the whole trailing run and
        // reports it as one unrecognized token (`123a456`, `1FROM`, `1.5x`),
        // rather than splitting it into a number plus an identifier.
        let mut illegal_trailing = false;
        while !self.is_eof() && Self::is_identifier_char(self.current_char()) {
            illegal_trailing = true;
            self.advance();
        }

        let token = self.slice_from(start);

        // Validate underscore placement across the fully scanned token: every
        // `_` must sit directly between two digits. A misplaced separator makes
        // the entire token unrecognized, exactly as SQLite reports it.
        let bad_underscore = has_underscore && !Self::underscores_all_between(token, false);

        if illegal_trailing || bad_underscore {
            return Err(LexerError::unrecognized_token(token, self.position()));
        }

        // Strip underscores from the token value so downstream sees a plain number.
        if has_underscore {
            Ok(Token::Number(token.replace('_', "")))
        } else {
            Ok(Token::Number(token.to_string()))
        }
    }

    /// Consume a run of ASCII digits and underscore separators from the current
    /// position, advancing the lexer. Underscores are consumed unconditionally
    /// (their placement is validated once the whole token is known). Sets
    /// `has_underscore` if any underscore was consumed.
    #[inline]
    fn consume_digits_and_underscores(&mut self, has_underscore: &mut bool) {
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_digit() {
                self.advance();
            } else if ch == '_' {
                *has_underscore = true;
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Whether every `_` in `token` sits directly between two digits. A digit
    /// separator adjacent to a non-digit (`1_`, `1_.4`, `12__34`, `1_e4`) is
    /// invalid and makes the whole numeric token unrecognized. When `hex` is
    /// true the neighbours are validated as hex digits (SQLite 3.46 allows
    /// separators between hex digits, e.g. `0xFF_FF`).
    fn underscores_all_between(token: &str, hex: bool) -> bool {
        let is_digit = |b: u8| if hex { b.is_ascii_hexdigit() } else { b.is_ascii_digit() };
        let bytes = token.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'_' {
                let prev_digit = i > 0 && is_digit(bytes[i - 1]);
                let next_digit = i + 1 < bytes.len() && is_digit(bytes[i + 1]);
                if !(prev_digit && next_digit) {
                    return false;
                }
            }
        }
        true
    }

    /// Whether `ch` is an identifier continuation character. Matches SQLite's
    /// notion of IdChar closely enough for tokenizing numbers: a numeric literal
    /// abutting one of these is a single unrecognized token.
    #[inline]
    fn is_identifier_char(ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }

    /// Tokenize a hexadecimal literal (0x... or 0X...).
    /// The '0x' or '0X' prefix has already been consumed when this is called.
    /// `start` is the byte offset of the leading '0' so a malformed token can be
    /// reported in full. Returns the decimal representation of the hex value.
    fn tokenize_hex_literal(&mut self, start: usize) -> Result<Token, LexerError> {
        let hex_start = self.position();

        // Collect hex digits (and underscore separators, SQLite 3.46+).
        let mut has_underscore = false;
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_hexdigit() {
                self.advance();
            } else if ch == '_' {
                has_underscore = true;
                self.advance();
            } else {
                break;
            }
        }

        // Any trailing identifier character makes the whole `0x...` run a single
        // unrecognized token (e.g. `0x1Ag`), not a value plus alias.
        let mut illegal_trailing = false;
        while !self.is_eof() && Self::is_identifier_char(self.current_char()) {
            illegal_trailing = true;
            self.advance();
        }

        let full = self.slice_from(start);
        let hex_body = self.slice_from(hex_start);

        if hex_body.is_empty() {
            return Err(LexerError {
                message: "Invalid hexadecimal literal: expected hex digits after '0x'".to_string(),
                position: self.position(),
                near_token: Some("0x".to_string()),
            });
        }

        if illegal_trailing || (has_underscore && !Self::underscores_all_between(hex_body, true)) {
            return Err(LexerError::unrecognized_token(full, self.position()));
        }

        let hex_str = if has_underscore { hex_body.replace('_', "") } else { hex_body.to_string() };

        // Parse the hex string to a number and convert to decimal string.
        // SQLite treats hex literals as 64-bit signed integers.
        match i64::from_str_radix(&hex_str, 16) {
            Ok(value) => Ok(Token::Number(value.to_string())),
            Err(_) => {
                // For very large hex values, try parsing as u64 first.
                match u64::from_str_radix(&hex_str, 16) {
                    Ok(value) => {
                        // SQLite interprets large hex values as signed (two's
                        // complement): `0x8000000000000000` is the minimum i64
                        // (-9223372036854775808) and is valid on its own. But a
                        // *unary minus* applied to it (`-0x8000000000000000`)
                        // negates i64::MIN, which overflows, and SQLite rejects
                        // it at parse time with `hex literal too big: -<literal>`
                        // (hexlit.test hexlist-402). Only this exact value is
                        // affected; every other hex literal negates cleanly. We
                        // detect the unary-minus context from the two preceding
                        // tokens: a `-` that is not itself preceded by a value.
                        if value == 0x8000_0000_0000_0000
                            && self.prev_kind == PrevTokenKind::Minus
                            && self.prev_prev_kind != PrevTokenKind::ValueTerminator
                        {
                            return Err(LexerError::preformatted(
                                format!("hex literal too big: -{}", full),
                                self.position(),
                            ));
                        }
                        // SQLite interprets large hex values as signed (two's complement).
                        Ok(Token::Number((value as i64).to_string()))
                    }
                    // A hex literal that needs more than 64 bits is rejected.
                    // SQLite reports the original literal text: `hex literal
                    // too big: 0x10000000000000000`.
                    Err(_) => Err(LexerError::preformatted(
                        format!("hex literal too big: {}", full),
                        self.position(),
                    )),
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
    fn test_negated_min_hex_literal_rejected() {
        // SQLite rejects a unary-minus applied to the minimum-i64 hex literal
        // (negating i64::MIN overflows): `hex literal too big: -<literal>`.
        // The original literal text (including leading zeros) is echoed back.
        let reject_cases = vec![
            ("SELECT -0x8000000000000000", "hex literal too big: -0x8000000000000000"),
            ("SELECT -0x08000000000000000", "hex literal too big: -0x08000000000000000"),
            ("SELECT abs(-0x8000000000000000)", "hex literal too big: -0x8000000000000000"),
            ("SELECT -0x8000000000000000+0", "hex literal too big: -0x8000000000000000"),
            ("SELECT x = -0x8000000000000000", "hex literal too big: -0x8000000000000000"),
        ];
        for (input, expected_msg) in reject_cases {
            let mut lexer = Lexer::new(input);
            let err = lexer.tokenize().expect_err(&format!("expected rejection for: {}", input));
            assert_eq!(err.message, expected_msg, "Input: {}", input);
        }
    }

    #[test]
    fn test_min_hex_literal_valid_without_unary_minus() {
        // The same literal is valid on its own (yields i64::MIN) and when the
        // preceding `-` is *binary* subtraction rather than unary negation.
        let ok_cases = vec![
            "SELECT 0x8000000000000000",
            "SELECT 5 - 0x8000000000000000",
            "SELECT 0 - 0x8000000000000000",
            "SELECT (0x8000000000000000)",
        ];
        for input in ok_cases {
            let mut lexer = Lexer::new(input);
            assert!(lexer.tokenize().is_ok(), "expected success for: {}", input);
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
    fn test_unrecognized_token_reports_full_run() {
        // SQLite's tokenizer absorbs the entire malformed numeric run and
        // reports it as one `unrecognized token: "X"`. These pairs mirror the
        // SQLite `literal.test` section-4 expectations exactly.
        let cases = vec![
            ("123a456", "123a456"),
            ("1_", "1_"),
            ("1_.4", "1_.4"),
            ("1e_4", "1e_4"),
            ("1_e4", "1_e4"),
            ("1.4_e4", "1.4_e4"),
            ("1.4e+_4", "1.4e"),
            ("1.4e-_4", "1.4e"),
            ("1.4e4_", "1.4e4_"),
            ("1.4e_4", "1.4e_4"),
            ("12__34", "12__34"),
            ("1234_", "1234_"),
            ("12._34", "12._34"),
            ("12_.34", "12_.34"),
            ("12.34_", "12.34_"),
            ("1.0e1_______2", "1.0e1_______2"),
        ];
        for (input, expected_token) in cases {
            let mut lexer = Lexer::new(input);
            let err = lexer.tokenize().expect_err(&format!("expected error for {input}"));
            assert_eq!(
                err.to_string(),
                format!("unrecognized token: \"{expected_token}\""),
                "input: {input}"
            );
        }
    }

    #[test]
    fn test_hex_unrecognized_token_reports_full_run() {
        let mut lexer = Lexer::new("0x1Ag");
        let err = lexer.tokenize().expect_err("expected error");
        assert_eq!(err.to_string(), "unrecognized token: \"0x1Ag\"");
    }

    #[test]
    fn test_hex_underscore_separators() {
        // SQLite 3.46+ allows underscores between hex digits in hex literals.
        let cases = vec![("0xFF_FF", "65535"), ("0XFF_EF", "65519"), ("0x1_0", "16")];
        for (input, expected) in cases {
            let mut lexer = Lexer::new(input);
            let tokens = lexer.tokenize().unwrap_or_else(|e| panic!("failed {input}: {e:?}"));
            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "input: {input}"),
                other => panic!("expected Number for {input}, got {other:?}"),
            }
        }
        // Misplaced separators in hex are unrecognized tokens, matching SQLite.
        for input in ["0xFF__EF", "0xFFEF_", "0x_FF"] {
            let mut lexer = Lexer::new(input);
            assert!(lexer.tokenize().is_err(), "expected error for {input}");
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
