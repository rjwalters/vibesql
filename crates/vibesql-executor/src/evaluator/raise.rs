//! Helpers for the `RAISE()` trigger-program expression (SQLite).
//!
//! `RAISE(ABORT|FAIL|ROLLBACK, msg)` reports `msg` coerced to text. SQLite's
//! text coercion of the message differs from VibeSQL's default `Display` in
//! one place that matters for compatibility: a `NULL` message is rendered as
//! the empty string (SQLite shows `Runtime error:  (19)` for
//! `RAISE(ABORT, NULL)`), whereas `SqlValue`'s `Display` renders `NULL` as the
//! literal text `NULL`. Every other storage class uses the standard display
//! form, matching SQLite (e.g. an integer `42` becomes `"42"`).

use vibesql_types::SqlValue;

/// Render a `RAISE()` error-message value as SQLite would.
///
/// `NULL` becomes the empty string; all other values use their normal text
/// representation.
pub(crate) fn render_raise_message(value: SqlValue) -> String {
    match value {
        SqlValue::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_string_message_verbatim() {
        assert_eq!(
            render_raise_message(SqlValue::Varchar("value too big".into())),
            "value too big"
        );
    }

    #[test]
    fn coerces_integer_message_to_text() {
        assert_eq!(render_raise_message(SqlValue::Integer(42)), "42");
    }

    #[test]
    fn renders_null_message_as_empty_string() {
        // SQLite shows an empty message for RAISE(ABORT, NULL).
        assert_eq!(render_raise_message(SqlValue::Null), "");
    }
}
