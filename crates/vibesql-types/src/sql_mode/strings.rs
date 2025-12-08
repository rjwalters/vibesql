/// String comparison and collation behavior
///
/// This module defines how string comparisons and collations work across
/// different SQL modes.
/// Collation type for string comparisons
///
/// Different SQL dialects support different collation strategies for comparing
/// strings and determining sort order.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Collation {
    /// Case-sensitive, byte-by-byte comparison
    ///
    /// Used by both MySQL (BINARY) and SQLite (BINARY).
    /// Compares strings byte-by-byte without any transformations.
    Binary,

    /// Case-insensitive comparison (SQLite NOCASE)
    ///
    /// SQLite-specific collation that treats ASCII uppercase letters
    /// as equal to their lowercase counterparts.
    NoCase,

    /// Ignore trailing spaces (SQLite RTRIM)
    ///
    /// SQLite-specific collation that ignores trailing whitespace
    /// when comparing strings.
    Rtrim,

    /// MySQL utf8mb4_bin - case-sensitive binary UTF-8 comparison
    ///
    /// Binary comparison for UTF-8 encoded strings.
    /// Character values are compared directly without case folding.
    Utf8Binary,

    /// MySQL utf8mb4_general_ci - case-insensitive general UTF-8 comparison
    ///
    /// Case-insensitive comparison for UTF-8 encoded strings.
    /// This is the most common default collation for MySQL.
    Utf8GeneralCi,
}

/// Trait defining string comparison and collation behavior for SQL modes
///
/// Different SQL dialects have different defaults and supported options
/// for string comparison:
///
/// - **MySQL**: Defaults to case-insensitive (`utf8mb4_general_ci`)
/// - **SQLite**: Defaults to case-sensitive (Binary collation)
#[allow(dead_code)]
pub trait StringBehavior {
    /// Returns whether string comparisons are case-sensitive by default
    ///
    /// # Examples
    ///
    /// ```text
    /// assert!(!SqlMode::MySQL.default_string_comparison_case_sensitive());
    /// assert!(SqlMode::SQLite.default_string_comparison_case_sensitive());
    /// ```
    fn default_string_comparison_case_sensitive(&self) -> bool;

    /// Returns the default collation for this SQL mode
    ///
    /// # Examples
    ///
    /// ```text
    /// assert_eq!(SqlMode::MySQL.default_collation(), Collation::Utf8GeneralCi);
    /// assert_eq!(SqlMode::SQLite.default_collation(), Collation::Binary);
    /// ```
    fn default_collation(&self) -> Collation;

    /// Returns the list of collations supported by this SQL mode
    ///
    /// # Examples
    ///
    /// ```text
    /// let mysql_collations = SqlMode::MySQL.supported_collations();
    /// assert!(mysql_collations.contains(&Collation::Utf8Binary));
    /// assert!(mysql_collations.contains(&Collation::Utf8GeneralCi));
    /// ```
    fn supported_collations(&self) -> &[Collation];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collation_equality() {
        assert_eq!(Collation::Binary, Collation::Binary);
        assert_ne!(Collation::Binary, Collation::NoCase);
    }

    #[test]
    fn test_collation_debug() {
        let collation = Collation::Utf8GeneralCi;
        let debug_str = format!("{:?}", collation);
        assert!(debug_str.contains("Utf8GeneralCi"));
    }
}
