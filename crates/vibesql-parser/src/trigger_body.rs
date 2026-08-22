//! Statement splitting for `BEGIN ... END` trigger bodies.
//!
//! A SQLite trigger body is a sequence of statements separated by `;`. Both
//! the create-time validator (`Parser::validate_trigger_body`) and the
//! fire-time executor (`TriggerFirer::parse_trigger_sql`) need to break the
//! body into its component statements before parsing each one.
//!
//! A naive `body.split(';')` is wrong: it splits on a `;` that appears inside
//! a string literal (e.g. `INSERT INTO log VALUES('a;b')`), a quoted
//! identifier (`"a;b"`, `[a;b]`, `` `a;b` ``), a blob literal, or a `--`
//! comment — producing two malformed fragments where SQLite (and a correct
//! parser) sees one statement.
//!
//! This module reuses the crate's lexer to find the *top-level* semicolons.
//! The lexer already handles SQLite string-literal escaping (`''`), all of
//! VibeSQL's quoted-identifier forms, blob literals, and `--` comments, so a
//! `;` inside any of those is never tokenized as [`Token::Semicolon`]. We
//! split the original source on the byte offsets of the top-level semicolon
//! tokens, which preserves each statement's exact text.
//!
//! This is the single source of truth shared by the parser-side validator and
//! the executor's fire-time path so the two cannot drift.

use crate::{lexer::Lexer, token::Token};

/// Split a `BEGIN ... END` trigger body into its component statements.
///
/// `raw_sql` is the full triggered-action SQL as stored on the trigger,
/// including the `BEGIN`/`END` wrapper (the wrapper is stripped here,
/// case-insensitively). The returned statements are trimmed; empty fragments
/// and `--` comment-only fragments are dropped, mirroring the prior behavior
/// of both call sites.
///
/// Semicolons inside string literals, quoted identifiers, blob literals, and
/// comments are not treated as statement separators. If the body cannot be
/// tokenized (an unterminated string, an unexpected character, etc.), this
/// falls back to the historical naive `split(';')` so we never reject a body
/// that the lexer happens not to handle but the downstream parser might.
pub fn split_trigger_body_statements(raw_sql: &str) -> Vec<String> {
    let body = strip_begin_end(raw_sql);

    let statements = match split_top_level_semicolons(body) {
        Some(parts) => parts,
        // Lexing failed: preserve the prior naive behavior rather than
        // dropping the body entirely.
        None => body.split(';').map(str::to_string).collect(),
    };

    statements
        .into_iter()
        .filter_map(|stmt| {
            let trimmed = stmt.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

/// Strip the leading `BEGIN` keyword and trailing `END` keyword (case
/// insensitive) from a trigger body, returning the inner statement list.
///
/// Mirrors the stripping both call sites previously did inline.
fn strip_begin_end(raw_sql: &str) -> &str {
    let body = raw_sql.trim();

    // Strip a leading BEGIN only when it is the keyword (followed by
    // whitespace or end-of-input), not merely a prefix of an identifier.
    let body = match body.get(..5) {
        Some(prefix) if prefix.eq_ignore_ascii_case("BEGIN") => {
            let rest = &body[5..];
            if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace()) {
                rest.trim()
            } else {
                body
            }
        }
        _ => body,
    };

    // Strip a trailing END keyword similarly.
    let len = body.len();
    if len >= 3 && body[len - 3..].eq_ignore_ascii_case("END") {
        let before = &body[..len - 3];
        if before.is_empty() || before.ends_with(|c: char| c.is_whitespace()) {
            return before.trim();
        }
    }

    body
}

/// Split `body` on top-level semicolons using the lexer to skip semicolons
/// inside string/identifier/blob literals and comments.
///
/// Returns `None` if the body cannot be tokenized, so the caller can fall
/// back to the naive split.
fn split_top_level_semicolons(body: &str) -> Option<Vec<String>> {
    let mut lexer = Lexer::new(body);
    let tokens = lexer.tokenize_with_spans().ok()?;

    let mut statements = Vec::new();
    let mut stmt_start = 0usize;

    for (token, span) in &tokens {
        match token {
            Token::Semicolon => {
                // Statement is the source text up to (not including) the
                // semicolon. Slicing the original source preserves the exact
                // contents of any string literal that contained a `;`.
                statements.push(body[stmt_start..span.start].to_string());
                stmt_start = span.end;
            }
            // Trailing text after the last semicolon (no terminating `;`).
            Token::Eof if stmt_start < span.start => {
                statements.push(body[stmt_start..span.start].to_string());
            }
            _ => {}
        }
    }

    Some(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_multi_statement_body() {
        let body = "BEGIN INSERT INTO a VALUES (1); INSERT INTO b VALUES (2); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO a VALUES (1)", "INSERT INTO b VALUES (2)"]);
    }

    #[test]
    fn does_not_split_on_semicolon_inside_string_literal() {
        let body = "BEGIN INSERT INTO log VALUES ('a;b'); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO log VALUES ('a;b')"]);
    }

    #[test]
    fn keeps_full_string_with_multiple_semicolons() {
        let body = "BEGIN INSERT INTO log VALUES ('a;b;c;d'); INSERT INTO log VALUES ('x'); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(
            stmts,
            vec!["INSERT INTO log VALUES ('a;b;c;d')", "INSERT INTO log VALUES ('x')"]
        );
    }

    #[test]
    fn handles_escaped_quote_inside_semicolon_string() {
        // SQLite escaping: '' is an escaped single quote inside the string.
        // The `;` after the escaped quote must stay inside the literal.
        let body = "BEGIN INSERT INTO log VALUES ('o''reilly;jr'); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO log VALUES ('o''reilly;jr')"]);
    }

    #[test]
    fn does_not_split_on_semicolon_inside_double_quoted_identifier() {
        let body = "BEGIN INSERT INTO \"weird;col\" VALUES (1); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO \"weird;col\" VALUES (1)"]);
    }

    #[test]
    fn does_not_split_on_semicolon_inside_bracket_identifier() {
        let body = "BEGIN INSERT INTO [weird;col] VALUES (1); END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO [weird;col] VALUES (1)"]);
    }

    #[test]
    fn handles_body_without_trailing_semicolon() {
        let body = "BEGIN INSERT INTO log VALUES ('a;b') END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO log VALUES ('a;b')"]);
    }

    #[test]
    fn drops_empty_and_comment_only_fragments() {
        let body = "BEGIN INSERT INTO a VALUES (1); ; -- a comment\n END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO a VALUES (1)"]);
    }

    #[test]
    fn empty_body_yields_no_statements() {
        assert!(split_trigger_body_statements("BEGIN END").is_empty());
        assert!(split_trigger_body_statements("BEGIN  END").is_empty());
    }

    #[test]
    fn case_end_inside_statement_does_not_split_or_truncate_body() {
        // #5439: the END of a CASE...END expression must not be treated as the
        // body terminator. Both body statements must come through whole.
        let body = "BEGIN \
                    SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END; \
                    INSERT INTO base(id, v) VALUES(NEW.id, NEW.v); \
                    END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(
            stmts,
            vec![
                "SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END",
                "INSERT INTO base(id, v) VALUES(NEW.id, NEW.v)"
            ]
        );
    }

    #[test]
    fn nested_case_end_does_not_truncate_body() {
        let body = "BEGIN \
                    UPDATE t SET v = CASE WHEN NEW.v > 0 \
                      THEN CASE WHEN NEW.v > 10 THEN 'big' ELSE 'small' END \
                      ELSE 'neg' END; \
                    INSERT INTO log VALUES('done'); \
                    END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert!(stmts[0].to_uppercase().starts_with("UPDATE T"));
        assert_eq!(stmts[1], "INSERT INTO log VALUES('done')");
    }

    #[test]
    fn case_end_as_last_statement_without_trailing_semicolon() {
        let body = "BEGIN SELECT CASE WHEN NEW.v > 0 THEN 1 ELSE 0 END END";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["SELECT CASE WHEN NEW.v > 0 THEN 1 ELSE 0 END"]);
    }

    #[test]
    fn does_not_strip_begin_inside_identifier() {
        // A body that does not start with the BEGIN keyword should be left as
        // is (defensive: callers always pass a wrapped body, but guard anyway).
        let body = "INSERT INTO beginnings VALUES (1)";
        let stmts = split_trigger_body_statements(body);
        assert_eq!(stmts, vec!["INSERT INTO beginnings VALUES (1)"]);
    }
}
