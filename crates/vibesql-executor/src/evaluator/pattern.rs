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
pub(crate) fn like_match(text: &str, pattern: &str, case_sensitive: bool, escape_char: Option<char>) -> bool {
    // If escape character is provided, preprocess the pattern to handle escapes
    match escape_char {
        Some(esc) => {
            // Build a processed pattern with escape markers
            let pattern_bytes = pattern.as_bytes();
            let esc_byte = esc as u8;
            let mut i = 0;
            let mut processed_pattern: Vec<PatternElement> = Vec::with_capacity(pattern_bytes.len());

            while i < pattern_bytes.len() {
                if pattern_bytes[i] == esc_byte && i + 1 < pattern_bytes.len() {
                    // Next character is escaped - treat as literal
                    let next_char = pattern_bytes[i + 1];
                    processed_pattern.push(PatternElement::Literal(next_char));
                    i += 2;
                } else if pattern_bytes[i] == b'%' {
                    processed_pattern.push(PatternElement::AnySequence);
                    i += 1;
                } else if pattern_bytes[i] == b'_' {
                    processed_pattern.push(PatternElement::AnyChar);
                    i += 1;
                } else {
                    processed_pattern.push(PatternElement::Literal(pattern_bytes[i]));
                    i += 1;
                }
            }

            like_match_with_elements(text.as_bytes(), &processed_pattern, 0, 0, case_sensitive)
        }
        None => {
            like_match_recursive(text.as_bytes(), pattern.as_bytes(), 0, 0, case_sensitive)
        }
    }
}

/// Pattern elements for LIKE matching with escape support
enum PatternElement {
    Literal(u8),
    AnySequence,  // %
    AnyChar,      // _
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
                if like_match_with_elements(text, pattern, text_pos + skip, pattern_pos + 1, case_sensitive) {
                    return true;
                }
            }
            false
        }
        PatternElement::AnyChar => {
            // _ matches exactly one character
            if text_pos >= text.len() {
                return false;
            }
            like_match_with_elements(text, pattern, text_pos + 1, pattern_pos + 1, case_sensitive)
        }
        PatternElement::Literal(pattern_char) => {
            if text_pos >= text.len() {
                return false;
            }
            let text_char = text[text_pos];

            let matches = if case_sensitive {
                text_char == *pattern_char
            } else {
                if pattern_char.is_ascii_alphabetic() && text_char.is_ascii_alphabetic() {
                    pattern_char.eq_ignore_ascii_case(&text_char)
                } else {
                    text_char == *pattern_char
                }
            };

            if !matches {
                return false;
            }
            like_match_with_elements(text, pattern, text_pos + 1, pattern_pos + 1, case_sensitive)
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
            // ? matches exactly one character
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Skip one character in text and one in pattern
            glob_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
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

            // Parse the character class until we find ]
            while pos < pattern.len() && pattern[pos] != b']' {
                let ch = pattern[pos];

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
                if like_match_recursive(text, pattern, text_pos + skip, pattern_pos + 1, case_sensitive) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // _ matches exactly one character
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Skip one character in text and one in pattern
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1, case_sensitive)
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
