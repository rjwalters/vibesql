/// Determine the number of bytes in a UTF-8 character based on its leading byte
#[inline]
fn utf8_char_len(byte: u8) -> usize {
    if byte & 0x80 == 0 {
        1 // ASCII: 0xxxxxxx
    } else if byte & 0xE0 == 0xC0 {
        2 // 2-byte: 110xxxxx
    } else if byte & 0xF0 == 0xE0 {
        3 // 3-byte: 1110xxxx
    } else if byte & 0xF8 == 0xF0 {
        4 // 4-byte: 11110xxx
    } else {
        1 // Invalid UTF-8 or continuation byte, advance by 1
    }
}

/// SQL LIKE pattern matching
/// Supports wildcards:
/// - % matches any sequence of characters (including empty)
/// - _ matches exactly one character
///
/// When case_sensitive is false (default SQLite behavior):
///   - ASCII letters are matched case-insensitively (A-Z = a-z)
///   - Unicode characters are matched case-sensitively
///
/// When case_sensitive is true (PRAGMA case_sensitive_like=ON):
///   - All characters are matched exactly (byte-for-byte)
///
/// The optional escape_char allows treating % and _ as literal characters
/// when preceded by the escape character. E.g., LIKE 'a\_b' ESCAPE '\'
/// would match the literal string 'a_b'.
pub(crate) fn like_match(
    text: &str,
    pattern: &str,
    case_sensitive: bool,
    escape_char: Option<char>,
) -> bool {
    // If escape character is provided, preprocess the pattern to handle escapes
    match escape_char {
        Some(esc) => {
            // Build a processed pattern with escape markers
            let pattern_bytes = pattern.as_bytes();

            // Encode the escape character as UTF-8 bytes
            let mut esc_buf = [0u8; 4];
            let esc_str = esc.encode_utf8(&mut esc_buf);
            let esc_bytes = esc_str.as_bytes();
            let esc_len = esc_bytes.len();

            let mut i = 0;
            let mut processed_pattern: Vec<PatternElement> =
                Vec::with_capacity(pattern_bytes.len());

            while i < pattern_bytes.len() {
                // Check if pattern starts with the escape character (comparing full UTF-8 sequence)
                if i + esc_len <= pattern_bytes.len() && &pattern_bytes[i..i + esc_len] == esc_bytes
                {
                    // Found escape character, next character is escaped
                    i += esc_len;
                    if i < pattern_bytes.len() {
                        // Get the full UTF-8 character being escaped
                        let next_char_len = utf8_char_len(pattern_bytes[i]);
                        let end = (i + next_char_len).min(pattern_bytes.len());
                        let escaped_bytes = pattern_bytes[i..end].to_vec();
                        processed_pattern.push(PatternElement::LiteralBytes(escaped_bytes));
                        i = end;
                    } else {
                        // Escape character at end of pattern with nothing to escape.
                        // Per SQLite documentation: "If there is no character following
                        // the escape character in the LIKE pattern, then the pattern is
                        // invalid and the LIKE expression evaluates to false."
                        return false;
                    }
                } else if pattern_bytes[i] == b'%' {
                    processed_pattern.push(PatternElement::AnySequence);
                    i += 1;
                } else if pattern_bytes[i] == b'_' {
                    processed_pattern.push(PatternElement::AnyChar);
                    i += 1;
                } else {
                    // Regular character - get full UTF-8 sequence
                    let char_len = utf8_char_len(pattern_bytes[i]);
                    let end = (i + char_len).min(pattern_bytes.len());
                    processed_pattern
                        .push(PatternElement::LiteralBytes(pattern_bytes[i..end].to_vec()));
                    i = end;
                }
            }

            like_match_with_elements(text.as_bytes(), &processed_pattern, 0, 0, case_sensitive)
        }
        None => like_match_recursive(text.as_bytes(), pattern.as_bytes(), 0, 0, case_sensitive),
    }
}

/// Pattern elements for LIKE matching with escape support
enum PatternElement {
    /// A literal sequence of bytes to match (supports multi-byte UTF-8 characters)
    LiteralBytes(Vec<u8>),
    AnySequence, // %
    AnyChar,     // _
}

/// Recursive LIKE matching with preprocessed pattern elements
fn like_match_with_elements(
    text: &[u8],
    pattern: &[PatternElement],
    text_pos: usize,
    pattern_pos: usize,
    case_sensitive: bool,
) -> bool {
    // If we've consumed the entire pattern
    if pattern_pos >= pattern.len() {
        // Match succeeds if we've also consumed all of text
        return text_pos >= text.len();
    }

    match &pattern[pattern_pos] {
        PatternElement::AnySequence => {
            // % matches zero or more characters
            for skip in 0..=(text.len() - text_pos) {
                if like_match_with_elements(
                    text,
                    pattern,
                    text_pos + skip,
                    pattern_pos + 1,
                    case_sensitive,
                ) {
                    return true;
                }
            }
            false
        }
        PatternElement::AnyChar => {
            // _ matches exactly one Unicode character (which may be multiple bytes)
            if text_pos >= text.len() {
                return false;
            }
            // Advance by the number of bytes in this UTF-8 character
            let char_len = utf8_char_len(text[text_pos]);
            like_match_with_elements(
                text,
                pattern,
                text_pos + char_len,
                pattern_pos + 1,
                case_sensitive,
            )
        }
        PatternElement::LiteralBytes(pattern_bytes) => {
            // Check if we have enough text remaining
            if text_pos + pattern_bytes.len() > text.len() {
                return false;
            }

            let text_slice = &text[text_pos..text_pos + pattern_bytes.len()];

            let matches = if case_sensitive {
                // Exact byte match
                text_slice == pattern_bytes.as_slice()
            } else {
                // Case-insensitive: compare byte by byte, but ASCII letters are case-insensitive
                // For multi-byte UTF-8 characters, they must match exactly (SQLite behavior)
                if pattern_bytes.len() == 1 {
                    let pattern_char = pattern_bytes[0];
                    let text_char = text_slice[0];
                    if pattern_char.is_ascii_alphabetic() && text_char.is_ascii_alphabetic() {
                        pattern_char.eq_ignore_ascii_case(&text_char)
                    } else {
                        text_char == pattern_char
                    }
                } else {
                    // Multi-byte characters: exact match (Unicode is case-sensitive in SQLite LIKE)
                    text_slice == pattern_bytes.as_slice()
                }
            };

            if !matches {
                return false;
            }
            like_match_with_elements(
                text,
                pattern,
                text_pos + pattern_bytes.len(),
                pattern_pos + 1,
                case_sensitive,
            )
        }
    }
}

/// SQLite GLOB pattern matching
/// Supports Unix-style wildcards:
/// - * matches any sequence of characters (including empty)
/// - ? matches exactly one character
/// - [...] matches any character in the brackets
/// - [^...] or [!...] matches any character NOT in the brackets
///
/// GLOB is case-sensitive (unlike LIKE which is case-insensitive in SQLite)
pub(crate) fn glob_match(text: &str, pattern: &str) -> bool {
    glob_match_recursive(text.as_bytes(), pattern.as_bytes(), 0, 0)
}

/// Recursive helper for GLOB pattern matching
fn glob_match_recursive(text: &[u8], pattern: &[u8], text_pos: usize, pattern_pos: usize) -> bool {
    // If we've consumed the entire pattern
    if pattern_pos >= pattern.len() {
        // Match succeeds if we've also consumed all of text
        return text_pos >= text.len();
    }

    let pattern_char = pattern[pattern_pos];

    match pattern_char {
        b'*' => {
            // * matches zero or more characters
            // Try matching with * consuming 0 chars, 1 char, 2 chars, etc.
            for skip in 0..=(text.len() - text_pos) {
                if glob_match_recursive(text, pattern, text_pos + skip, pattern_pos + 1) {
                    return true;
                }
            }
            false
        }
        b'?' => {
            // ? matches exactly one Unicode character (which may be multiple bytes)
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Advance by the number of bytes in this UTF-8 character
            let char_len = utf8_char_len(text[text_pos]);
            glob_match_recursive(text, pattern, text_pos + char_len, pattern_pos + 1)
        }
        b'[' => {
            // Character class [...] or [^...] or [!...]
            if text_pos >= text.len() {
                return false;
            }

            let text_char = text[text_pos];
            let mut pos = pattern_pos + 1;

            // Check for negation
            let negated = if pos < pattern.len() && (pattern[pos] == b'^' || pattern[pos] == b'!') {
                pos += 1;
                true
            } else {
                false
            };

            let mut matched = false;
            let mut prev_char: Option<u8> = None;

            // Issue #4910: Handle ] as the first character in a character class.
            // Per POSIX and SQLite, if ] appears immediately after [ (or [^ or [!),
            // it's treated as a literal character to match, not as the closing bracket.
            // For example: []abc] matches ], a, b, or c
            //              [^]abc] matches any character except ], a, b, or c
            let mut first_char_in_class = true;

            // Parse the character class until we find the closing ]
            while pos < pattern.len() {
                let ch = pattern[pos];

                // Check if this is the closing ] (but not if it's the first char)
                if ch == b']' && !first_char_in_class {
                    break;
                }

                first_char_in_class = false;

                // Check for range (e.g., a-z)
                if ch == b'-'
                    && prev_char.is_some()
                    && pos + 1 < pattern.len()
                    && pattern[pos + 1] != b']'
                {
                    let range_end = pattern[pos + 1];
                    let range_start = prev_char.unwrap();
                    if text_char >= range_start && text_char <= range_end {
                        matched = true;
                    }
                    pos += 2; // Skip - and the end character
                    prev_char = Some(range_end);
                } else {
                    // Single character
                    if text_char == ch {
                        matched = true;
                    }
                    prev_char = Some(ch);
                    pos += 1;
                }
            }

            // Skip the closing ]
            if pos < pattern.len() && pattern[pos] == b']' {
                pos += 1;
            } else {
                // Malformed pattern, treat [ as literal
                return if text_char == b'[' {
                    glob_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
                } else {
                    false
                };
            }

            let class_matches = if negated { !matched } else { matched };
            if class_matches {
                glob_match_recursive(text, pattern, text_pos + 1, pos)
            } else {
                false
            }
        }
        _ => {
            // Regular character must match exactly (case-sensitive)
            if text_pos >= text.len() {
                // No character left in text
                return false;
            }
            if text[text_pos] != pattern_char {
                // Characters don't match
                return false;
            }
            // Characters match, continue
            glob_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
        }
    }
}

/// Recursive helper for LIKE pattern matching
///
/// When case_sensitive is false (default):
///   SQLite LIKE is case-insensitive for ASCII letters (A-Z = a-z)
/// When case_sensitive is true:
///   All characters are matched exactly
fn like_match_recursive(
    text: &[u8],
    pattern: &[u8],
    text_pos: usize,
    pattern_pos: usize,
    case_sensitive: bool,
) -> bool {
    // If we've consumed the entire pattern
    if pattern_pos >= pattern.len() {
        // Match succeeds if we've also consumed all of text
        return text_pos >= text.len();
    }

    let pattern_char = pattern[pattern_pos];

    match pattern_char {
        b'%' => {
            // % matches zero or more characters
            // Try matching with % consuming 0 chars, 1 char, 2 chars, etc.
            for skip in 0..=(text.len() - text_pos) {
                if like_match_recursive(
                    text,
                    pattern,
                    text_pos + skip,
                    pattern_pos + 1,
                    case_sensitive,
                ) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // _ matches exactly one Unicode character (which may be multiple bytes)
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Advance by the number of bytes in this UTF-8 character
            let char_len = utf8_char_len(text[text_pos]);
            like_match_recursive(
                text,
                pattern,
                text_pos + char_len,
                pattern_pos + 1,
                case_sensitive,
            )
        }
        _ => {
            // Regular character comparison
            if text_pos >= text.len() {
                // No character left in text
                return false;
            }
            let text_char = text[text_pos];

            let matches = if case_sensitive {
                // Case-sensitive: exact byte match
                text_char == pattern_char
            } else {
                // Case-insensitive for ASCII letters only (SQLite default)
                if pattern_char.is_ascii_alphabetic() && text_char.is_ascii_alphabetic() {
                    pattern_char.eq_ignore_ascii_case(&text_char)
                } else {
                    text_char == pattern_char
                }
            };

            if !matches {
                return false;
            }
            // Characters match, continue
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1, case_sensitive)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_match_basic() {
        // Basic patterns without escape
        assert!(like_match("hello", "hello", false, None));
        assert!(like_match("hello", "h%", false, None));
        assert!(like_match("hello", "%o", false, None));
        assert!(like_match("hello", "h_llo", false, None));
        assert!(!like_match("hello", "h__llo", false, None));
    }

    #[test]
    fn test_like_match_ascii_escape() {
        // ASCII escape character (backslash)
        assert!(like_match("a_c", r"a\_c", false, Some('\\')));
        assert!(like_match("a%c", r"a\%c", false, Some('\\')));
        assert!(!like_match("abc", r"a\_c", false, Some('\\'))); // _ is literal, not wildcard
        assert!(!like_match("abc", r"a\%c", false, Some('\\'))); // % is literal, not wildcard

        // Escape the escape character itself
        assert!(like_match(r"a\c", r"a\\c", false, Some('\\')));
    }

    #[test]
    fn test_like_match_multibyte_unicode_escape() {
        // U+1234 (ሴ) is a 3-byte UTF-8 character: 0xE1 0x88 0xB4
        let escape_char = '\u{1234}'; // ሴ

        // Pattern: "aሴ_c" with escape ሴ means: match 'a', literal '_', 'c'
        let pattern = "a\u{1234}_c";
        assert!(like_match("a_c", pattern, false, Some(escape_char)));
        assert!(!like_match("abc", pattern, false, Some(escape_char)));

        // Pattern: "aሴ%c" with escape ሴ means: match 'a', literal '%', 'c'
        let pattern = "a\u{1234}%c";
        assert!(like_match("a%c", pattern, false, Some(escape_char)));
        assert!(!like_match("abc", pattern, false, Some(escape_char)));

        // Escape the escape character itself: "aሴሴc" means: match 'a', literal 'ሴ', 'c'
        let pattern = "a\u{1234}\u{1234}c";
        assert!(like_match("a\u{1234}c", pattern, false, Some(escape_char)));
    }

    #[test]
    fn test_like_match_multibyte_unicode_escape_2byte() {
        // U+00E9 (é) is a 2-byte UTF-8 character
        let escape_char = '\u{00E9}';

        let pattern = "a\u{00E9}_c";
        assert!(like_match("a_c", pattern, false, Some(escape_char)));
        assert!(!like_match("abc", pattern, false, Some(escape_char)));
    }

    #[test]
    fn test_like_match_multibyte_unicode_escape_4byte() {
        // U+1F600 (😀) is a 4-byte UTF-8 character
        let escape_char = '\u{1F600}';

        let pattern = "a\u{1F600}_c";
        assert!(like_match("a_c", pattern, false, Some(escape_char)));
        assert!(!like_match("abc", pattern, false, Some(escape_char)));
    }

    #[test]
    fn test_like_match_escape_at_end() {
        // Escape character at end of pattern should return false
        assert!(!like_match("a", "a\\", false, Some('\\')));
        assert!(!like_match("a", "a\u{1234}", false, Some('\u{1234}')));
    }

    #[test]
    fn test_like_match_escape_multibyte_char() {
        // Escaping a multi-byte Unicode character
        let escape_char = '\\';

        // Pattern: a\_c where _ is literal (escaping underscore works with any char)
        assert!(like_match("a_c", r"a\_c", false, Some(escape_char)));

        // Pattern with multi-byte char after escape
        // \é should match literal é
        assert!(like_match("aéc", "a\\\u{00E9}c", false, Some(escape_char)));
    }

    #[test]
    fn test_like_match_hex_blob_escape() {
        // Test case from the issue: x'e188b4' is the UTF-8 encoding of U+1234
        // SELECT 'a_c' LIKE 'a' || x'e188b4' || '_c' ESCAPE x'e188b4';
        // This should match because:
        // - escape char is ሴ (U+1234)
        // - pattern is 'aሴ_c' which means 'a' + escaped '_' + 'c'
        // - text is 'a_c' which has a literal underscore
        let escape_char = '\u{1234}';
        let pattern = "a\u{1234}_c";
        assert!(like_match("a_c", pattern, false, Some(escape_char)));
    }

    #[test]
    fn test_glob_match_basic() {
        // Basic patterns
        assert!(glob_match("hello", "hello"));
        assert!(glob_match("hello", "h*"));
        assert!(glob_match("hello", "*o"));
        assert!(glob_match("hello", "h?llo"));
        assert!(!glob_match("hello", "h??llo"));
    }

    #[test]
    fn test_glob_match_character_class() {
        // Basic character class
        assert!(glob_match("abc", "a[bcd]c"));
        assert!(!glob_match("aec", "a[bcd]c"));

        // Range in character class
        assert!(glob_match("abc", "a[a-z]c"));
        assert!(!glob_match("a1c", "a[a-z]c"));

        // Negated character class
        assert!(!glob_match("abc", "a[^b]c"));
        assert!(glob_match("adc", "a[^b]c"));
        assert!(!glob_match("abc", "a[!b]c"));
        assert!(glob_match("adc", "a[!b]c"));
    }

    #[test]
    fn test_glob_match_bracket_first_char() {
        // Issue #4910: ] as first character in character class
        // Per POSIX and SQLite, ] immediately after [ is treated as a literal

        // []b] matches ] or b
        assert!(glob_match("abc", "a[]b]c")); // matches 'b' in the class
        assert!(glob_match("a]c", "a[]b]c")); // matches ']' in the class
        assert!(!glob_match("adc", "a[]b]c")); // 'd' not in class

        // [^]b] matches anything except ] or b
        assert!(glob_match("axc", "a[^]b]c")); // 'x' not in negated class
        assert!(!glob_match("abc", "a[^]b]c")); // 'b' is in negated class
        assert!(!glob_match("a]c", "a[^]b]c")); // ']' is in negated class

        // [!]b] is same as [^]b] (alternate negation syntax)
        assert!(glob_match("axc", "a[!]b]c"));
        assert!(!glob_match("abc", "a[!]b]c"));
        assert!(!glob_match("a]c", "a[!]b]c"));

        // Uppercase variants (GLOB is case-sensitive)
        assert!(glob_match("ABC", "A[]B]C")); // matches 'B' in the class
        assert!(glob_match("A]C", "A[]B]C")); // matches ']' in the class
        assert!(glob_match("AxC", "A[^]B]C")); // 'x' not in negated class
        assert!(!glob_match("ABC", "A[^]B]C")); // 'B' is in negated class
    }
}
