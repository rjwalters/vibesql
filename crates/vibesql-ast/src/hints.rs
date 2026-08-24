//! Query-comment hints — optimizer directives recognized from `/* ... */`
//! SQL comments.
//!
//! ## Recognized syntax
//!
//! A `/* ... */` block comment is recognized as a hint only when its entire
//! trimmed body is, case-insensitively, exactly one of the keywords in
//! [`QueryHint::parse`] — this is a closed allowlist, not a free-form
//! directive language. An incidental comment like
//! `/* uses COLUMNAR storage internally */` is *never* mistaken for a hint
//! (its trimmed body is not an exact match) and continues to be discarded
//! like any other comment, unchanged from #6544's lexing behavior.
//!
//! ## Scope: "leading hint" only
//!
//! A recognized hint comment only attaches to a `SELECT` statement's
//! [`SelectStmt::hints`](crate::SelectStmt::hints) when it appears in the
//! token gap immediately following that statement's leading `SELECT`
//! keyword — e.g. `SELECT /* COLUMNAR */ * FROM t` — **and** `SELECT` is
//! the very first token of the statement. This intentionally scopes out
//! `WITH cte AS (...) SELECT /* COLUMNAR */ ...` and
//! `INSERT INTO t SELECT /* COLUMNAR */ ...` for v1: the comment still
//! lexes exactly as before, it is simply not captured as a hint on those
//! shapes. A hint anywhere else in a statement (after `FROM`, inside a
//! `WHERE` clause, trailing the statement, inside a subquery, etc.) is
//! likewise never attached — it is treated exactly like an ordinary
//! comment.
//!
//! ## Precedence for multiple hints
//!
//! It is legal to write more than one recognized hint comment back to back
//! in the leading position (e.g.
//! `SELECT /* COLUMNAR */ /* ROW_ORIENTED */ * FROM t`).
//! [`SelectStmt::hints`](crate::SelectStmt::hints) preserves all of them in
//! source order. Every reader of the field in this codebase applies
//! **last-one-wins** precedence — the rightmost (most recently written)
//! hint overrides any earlier ones — via `.hints.last()`. This doc is the
//! single source of truth for that rule: do not add a reader with
//! different precedence without updating it here.

/// A recognized optimizer hint captured from a `/* ... */` SQL comment.
///
/// See the [module docs](self) for the recognized syntax and the
/// scope/precedence rules that govern when and how a hint attaches to a
/// [`SelectStmt`](crate::SelectStmt).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryHint {
    /// `/* COLUMNAR */` — request columnar (vectorized/SIMD) execution.
    Columnar,
    /// `/* ROW_ORIENTED */` — request traditional row-oriented execution.
    RowOriented,
}

impl QueryHint {
    /// Recognize `text` (the body of a `/* ... */` comment, not yet
    /// trimmed) as a query hint. Matching is case-insensitive over the
    /// *entire* trimmed body — a substring match is deliberately not
    /// enough (see [module docs](self)) — so `"  columnar  "` matches but
    /// `"columnar storage"` does not.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_uppercase().as_str() {
            "COLUMNAR" => Some(QueryHint::Columnar),
            "ROW_ORIENTED" => Some(QueryHint::RowOriented),
            _ => None,
        }
    }

    /// The canonical uppercase spelling of this hint, as it appears inside
    /// `/* ... */` in SQL text and in `StrategyReason::QueryHint` messages.
    pub fn as_str(self) -> &'static str {
        match self {
            QueryHint::Columnar => "COLUMNAR",
            QueryHint::RowOriented => "ROW_ORIENTED",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_recognized_keywords_case_insensitively() {
        assert_eq!(QueryHint::parse("COLUMNAR"), Some(QueryHint::Columnar));
        assert_eq!(QueryHint::parse("columnar"), Some(QueryHint::Columnar));
        assert_eq!(QueryHint::parse("  Columnar  "), Some(QueryHint::Columnar));
        assert_eq!(QueryHint::parse("ROW_ORIENTED"), Some(QueryHint::RowOriented));
        assert_eq!(QueryHint::parse("row_oriented"), Some(QueryHint::RowOriented));
    }

    #[test]
    fn rejects_substring_and_unrecognized_text() {
        // A comment that merely *contains* a hint keyword is not a match —
        // matching is over the entire trimmed body.
        assert_eq!(QueryHint::parse("uses COLUMNAR storage"), None);
        assert_eq!(QueryHint::parse("just a note"), None);
        assert_eq!(QueryHint::parse(""), None);
    }

    #[test]
    fn as_str_round_trips_through_parse() {
        for hint in [QueryHint::Columnar, QueryHint::RowOriented] {
            assert_eq!(QueryHint::parse(hint.as_str()), Some(hint));
        }
    }
}
