//! SQL Identifier types with proper case handling per SQL:1999.
//!
//! This module provides identifier types that correctly handle case sensitivity
//! according to SQL:1999 standard:
//!
//! - **Unquoted identifiers**: Case-insensitive, folded to lowercase for canonical form
//! - **Quoted (delimited) identifiers**: Case-sensitive, preserve exact case
//!
//! # Example
//!
//! ```
//! use vibesql_catalog::TableIdentifier;
//!
//! // Unquoted identifiers are case-insensitive
//! let id1 = TableIdentifier::new("MyTable", false);
//! let id2 = TableIdentifier::new("mytable", false);
//! let id3 = TableIdentifier::new("MYTABLE", false);
//! assert_eq!(id1, id2);
//! assert_eq!(id2, id3);
//!
//! // Quoted identifiers are case-sensitive
//! let quoted = TableIdentifier::new("MyTable", true);
//! assert_ne!(id1, quoted); // Different canonical forms
//! ```

use std::fmt;
use std::hash::{Hash, Hasher};

/// A SQL identifier with proper case handling per SQL:1999.
///
/// This type separates three concerns:
/// - **Canonical form**: Used for HashMap keys and equality comparison
/// - **Display form**: Preserves user's original input for error messages
/// - **Quoted flag**: Whether the identifier was delimited (quoted)
///
/// ## SQL:1999 Compliance
///
/// Per SQL:1999, identifier handling depends on whether the identifier was quoted:
///
/// | Input | Quoted | Canonical | Matches `mytable`? | Matches `"MyTable"`? |
/// |-------|--------|-----------|--------------------|-----------------------|
/// | `MyTable` | No | `mytable` | Yes | No |
/// | `"MyTable"` | Yes | `MyTable` | No | Yes |
/// | `MYTABLE` | No | `mytable` | Yes | No |
/// | `"mytable"` | Yes | `mytable` | Yes (same canonical) | No |
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    /// Canonical form for HashMap keys and comparison.
    /// - If quoted: exact case preserved (case-sensitive)
    /// - If unquoted: lowercase folded (case-insensitive)
    canonical: String,

    /// Display form preserving user's original input.
    /// Used for error messages and user-facing output.
    display: String,

    /// Whether the identifier was quoted (delimited) in the original SQL.
    /// Quoted identifiers use double quotes: `"MyTable"`
    quoted: bool,
}

impl TableIdentifier {
    /// Create a new table identifier.
    ///
    /// # Arguments
    ///
    /// * `name` - The identifier name as written by the user
    /// * `quoted` - Whether the identifier was quoted (delimited) in SQL
    ///
    /// # SQL:1999 Behavior
    ///
    /// - If `quoted` is `false`: The canonical form is lowercase-folded for
    ///   case-insensitive comparison. This matches SQL:1999 behavior for
    ///   unquoted (regular) identifiers.
    ///
    /// - If `quoted` is `true`: The canonical form preserves exact case for
    ///   case-sensitive comparison. This matches SQL:1999 behavior for
    ///   delimited identifiers.
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_catalog::TableIdentifier;
    ///
    /// // Unquoted: case-insensitive
    /// let unquoted = TableIdentifier::new("MyTable", false);
    /// assert_eq!(unquoted.canonical(), "mytable");
    /// assert_eq!(unquoted.display(), "MyTable");
    ///
    /// // Quoted: case-sensitive
    /// let quoted = TableIdentifier::new("MyTable", true);
    /// assert_eq!(quoted.canonical(), "MyTable");
    /// assert_eq!(quoted.display(), "MyTable");
    /// ```
    pub fn new(name: &str, quoted: bool) -> Self {
        let canonical = if quoted {
            // Quoted identifiers preserve exact case (SQL:1999 delimited identifiers)
            name.to_string()
        } else {
            // Unquoted identifiers fold to lowercase (SQL:1999 regular identifiers)
            name.to_ascii_lowercase()
        };

        Self { canonical, display: name.to_string(), quoted }
    }

    /// Create an identifier from a canonical name (for internal use).
    ///
    /// This is used when loading from persistence where we only have the
    /// canonical form. The display form is set to match canonical.
    pub fn from_canonical(canonical: String, quoted: bool) -> Self {
        Self { display: canonical.clone(), canonical, quoted }
    }

    /// Get the canonical form of the identifier.
    ///
    /// This is used for HashMap keys and equality comparison.
    /// - Unquoted identifiers: lowercase
    /// - Quoted identifiers: exact case preserved
    #[inline]
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    /// Get the display form of the identifier.
    ///
    /// This preserves the user's original input and should be used
    /// in error messages and user-facing output.
    #[inline]
    pub fn display(&self) -> &str {
        &self.display
    }

    /// Check if this identifier was quoted (delimited) in the original SQL.
    #[inline]
    pub fn is_quoted(&self) -> bool {
        self.quoted
    }

    /// Get the canonical form as an owned String.
    ///
    /// Useful for HashMap operations that need owned keys.
    #[inline]
    pub fn into_canonical(self) -> String {
        self.canonical
    }

    /// Create an identifier that matches any case variation.
    ///
    /// This creates an unquoted identifier from the given name,
    /// which will match any case variation of that name.
    pub fn unquoted(name: &str) -> Self {
        Self::new(name, false)
    }

    /// Create an identifier that matches only the exact case.
    ///
    /// This creates a quoted identifier from the given name,
    /// which will only match the exact same case.
    pub fn quoted(name: &str) -> Self {
        Self::new(name, true)
    }
}

impl PartialEq for TableIdentifier {
    /// Two identifiers are equal if their canonical forms match.
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Eq for TableIdentifier {}

impl Hash for TableIdentifier {
    /// Hash based on canonical form for consistent HashMap behavior.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical.hash(state);
    }
}

impl fmt::Display for TableIdentifier {
    /// Display uses the original user input form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display)
    }
}

impl From<&str> for TableIdentifier {
    /// Convert from a string, assuming unquoted (case-insensitive).
    fn from(s: &str) -> Self {
        Self::new(s, false)
    }
}

impl From<String> for TableIdentifier {
    /// Convert from a String, assuming unquoted (case-insensitive).
    fn from(s: String) -> Self {
        Self::new(&s, false)
    }
}

/// A general SQL identifier (for columns, indexes, views, etc.)
///
/// This is a type alias for now, but can be specialized later if needed.
pub type Identifier = TableIdentifier;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_unquoted_case_insensitive() {
        let id1 = TableIdentifier::new("MyTable", false);
        let id2 = TableIdentifier::new("mytable", false);
        let id3 = TableIdentifier::new("MYTABLE", false);
        let id4 = TableIdentifier::new("myTABLE", false);

        // All unquoted variations should be equal
        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
        assert_eq!(id3, id4);

        // Canonical should be lowercase
        assert_eq!(id1.canonical(), "mytable");
        assert_eq!(id2.canonical(), "mytable");
        assert_eq!(id3.canonical(), "mytable");

        // Display preserves original
        assert_eq!(id1.display(), "MyTable");
        assert_eq!(id2.display(), "mytable");
        assert_eq!(id3.display(), "MYTABLE");
    }

    #[test]
    fn test_quoted_case_sensitive() {
        let id1 = TableIdentifier::new("MyTable", true);
        let id2 = TableIdentifier::new("mytable", true);
        let id3 = TableIdentifier::new("MYTABLE", true);

        // Quoted identifiers with different cases should NOT be equal
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        // Canonical preserves exact case
        assert_eq!(id1.canonical(), "MyTable");
        assert_eq!(id2.canonical(), "mytable");
        assert_eq!(id3.canonical(), "MYTABLE");
    }

    #[test]
    fn test_quoted_vs_unquoted() {
        // Unquoted "MyTable" should NOT equal quoted "MyTable"
        // because they have different canonical forms
        let unquoted = TableIdentifier::new("MyTable", false);
        let quoted = TableIdentifier::new("MyTable", true);

        assert_ne!(unquoted, quoted);
        assert_eq!(unquoted.canonical(), "mytable");
        assert_eq!(quoted.canonical(), "MyTable");
    }

    #[test]
    fn test_quoted_lowercase_matches_unquoted() {
        // Special case: quoted "mytable" SHOULD equal unquoted "MyTable"
        // because they have the same canonical form
        let unquoted = TableIdentifier::new("MyTable", false);
        let quoted_lower = TableIdentifier::new("mytable", true);

        assert_eq!(unquoted, quoted_lower);
        assert_eq!(unquoted.canonical(), "mytable");
        assert_eq!(quoted_lower.canonical(), "mytable");
    }

    #[test]
    fn test_hashmap_lookup() {
        let mut map: HashMap<TableIdentifier, i32> = HashMap::new();

        // Insert with unquoted identifier
        let key = TableIdentifier::new("users", false);
        map.insert(key, 42);

        // Should find with different case (unquoted)
        let lookup1 = TableIdentifier::new("USERS", false);
        let lookup2 = TableIdentifier::new("Users", false);
        let lookup3 = TableIdentifier::new("users", false);

        assert_eq!(map.get(&lookup1), Some(&42));
        assert_eq!(map.get(&lookup2), Some(&42));
        assert_eq!(map.get(&lookup3), Some(&42));

        // Should NOT find with quoted different case
        let quoted_upper = TableIdentifier::new("USERS", true);
        assert_eq!(map.get(&quoted_upper), None);
    }

    #[test]
    fn test_hashmap_quoted_keys() {
        let mut map: HashMap<TableIdentifier, i32> = HashMap::new();

        // Insert with quoted identifier
        let key = TableIdentifier::new("MyTable", true);
        map.insert(key, 42);

        // Should only find with exact case (quoted)
        let exact = TableIdentifier::new("MyTable", true);
        let wrong_case = TableIdentifier::new("mytable", true);
        let unquoted = TableIdentifier::new("MyTable", false);

        assert_eq!(map.get(&exact), Some(&42));
        assert_eq!(map.get(&wrong_case), None);
        assert_eq!(map.get(&unquoted), None);
    }

    #[test]
    fn test_display_trait() {
        let id = TableIdentifier::new("MyTable", false);
        assert_eq!(format!("{}", id), "MyTable");

        let quoted = TableIdentifier::new("MyTable", true);
        assert_eq!(format!("{}", quoted), "MyTable");
    }

    #[test]
    fn test_from_traits() {
        let id1: TableIdentifier = "MyTable".into();
        let id2: TableIdentifier = String::from("MyTable").into();

        assert_eq!(id1, id2);
        assert_eq!(id1.canonical(), "mytable"); // From assumes unquoted
    }

    #[test]
    fn test_helper_constructors() {
        let unquoted = TableIdentifier::unquoted("MyTable");
        let quoted = TableIdentifier::quoted("MyTable");

        assert!(!unquoted.is_quoted());
        assert!(quoted.is_quoted());

        assert_eq!(unquoted.canonical(), "mytable");
        assert_eq!(quoted.canonical(), "MyTable");
    }

    #[test]
    fn test_from_canonical() {
        let id = TableIdentifier::from_canonical("mytable".to_string(), false);
        assert_eq!(id.canonical(), "mytable");
        assert_eq!(id.display(), "mytable");
        assert!(!id.is_quoted());

        let quoted_id = TableIdentifier::from_canonical("MyTable".to_string(), true);
        assert_eq!(quoted_id.canonical(), "MyTable");
        assert_eq!(quoted_id.display(), "MyTable");
        assert!(quoted_id.is_quoted());
    }

    #[test]
    fn test_into_canonical() {
        let id = TableIdentifier::new("MyTable", false);
        let canonical: String = id.into_canonical();
        assert_eq!(canonical, "mytable");
    }

    #[test]
    fn test_sql_examples_from_issue() {
        // Examples from the issue description

        // CREATE TABLE MyTable (id INT);
        // INSERT INTO mytable VALUES (1);  -- Should work
        // INSERT INTO MYTABLE VALUES (2);  -- Should work
        let created = TableIdentifier::new("MyTable", false);
        let lookup1 = TableIdentifier::new("mytable", false);
        let lookup2 = TableIdentifier::new("MYTABLE", false);

        assert_eq!(created, lookup1);
        assert_eq!(created, lookup2);

        // CREATE TABLE "MyTable" (id INT);  -- Different table!
        let quoted_created = TableIdentifier::new("MyTable", true);
        assert_ne!(created, quoted_created);

        // SELECT * FROM "MyTable";  -- Only finds quoted table
        let quoted_lookup = TableIdentifier::new("MyTable", true);
        assert_eq!(quoted_created, quoted_lookup);
        assert_ne!(created, quoted_lookup);

        // SELECT * FROM MyTable;  -- Only finds unquoted table
        let unquoted_lookup = TableIdentifier::new("MyTable", false);
        assert_eq!(created, unquoted_lookup);
        assert_ne!(quoted_created, unquoted_lookup);
    }

    #[test]
    fn test_create_duplicate_detection() {
        // CREATE TABLE test (id INT);
        // CREATE TABLE TEST (id INT);  -- Should ERROR: table already exists
        let first = TableIdentifier::new("test", false);
        let second = TableIdentifier::new("TEST", false);
        assert_eq!(first, second); // Same table, should conflict

        // CREATE TABLE "TEST" (id INT);  -- Should OK: different table
        let quoted = TableIdentifier::new("TEST", true);
        assert_ne!(first, quoted); // Different table, no conflict
    }
}
