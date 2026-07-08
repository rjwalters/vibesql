//! Allow-list of table-valued functions (TVFs) recognized in FROM position.
//!
//! SQLite's JSON1 extension exposes two table-valued functions that appear in
//! the FROM clause rather than as scalar expressions: `json_each` and
//! `json_tree` (see ADR-0005). When the parser encounters an identifier
//! immediately followed by `(` in FROM position, it produces a
//! [`vibesql_ast::FromClause::TableFunction`] **only** for names on this
//! allow-list. Any other `ident(` in FROM remains a parse error, preserving
//! the pre-existing behavior for everything else.
//!
//! The comparison is case-insensitive; the AST stores the normalized lowercase
//! name.

/// The set of table-valued function names recognized in FROM position.
///
/// Kept lowercase; callers compare case-insensitively via
/// [`is_table_valued_function`].
pub(crate) const TABLE_VALUED_FUNCTIONS: [&str; 2] = ["json_each", "json_tree"];

/// Returns `true` if `name` (compared case-insensitively) is an allow-listed
/// table-valued function that may appear in FROM position.
pub(crate) fn is_table_valued_function(name: &str) -> bool {
    TABLE_VALUED_FUNCTIONS.iter().any(|tvf| name.eq_ignore_ascii_case(tvf))
}

/// Returns the normalized (lowercase) TVF name if `name` is allow-listed,
/// otherwise `None`.
pub(crate) fn normalized_table_valued_function(name: &str) -> Option<String> {
    TABLE_VALUED_FUNCTIONS
        .iter()
        .find(|tvf| name.eq_ignore_ascii_case(tvf))
        .map(|tvf| (*tvf).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_allow_listed_names_case_insensitively() {
        assert!(is_table_valued_function("json_each"));
        assert!(is_table_valued_function("JSON_EACH"));
        assert!(is_table_valued_function("Json_Tree"));
    }

    #[test]
    fn rejects_non_allow_listed_names() {
        assert!(!is_table_valued_function("foo"));
        assert!(!is_table_valued_function("json_extract"));
        assert!(!is_table_valued_function("generate_series"));
    }

    #[test]
    fn normalizes_to_lowercase() {
        assert_eq!(normalized_table_valued_function("JSON_EACH").as_deref(), Some("json_each"));
        assert_eq!(normalized_table_valued_function("Json_Tree").as_deref(), Some("json_tree"));
        assert_eq!(normalized_table_valued_function("foo"), None);
    }
}
