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
///
/// ## Compound Identifiers
///
/// TableIdentifier supports schema-qualified table names where each component
/// (schema and table) can be independently quoted or unquoted:
///
/// | SQL | Schema Part | Table Part | Canonical Form |
/// |-----|-------------|------------|----------------|
/// | `myApp.users` | unquoted | unquoted | `myapp.users` |
/// | `"myApp".users` | quoted | unquoted | `myApp.users` |
/// | `myapp."Users"` | unquoted | quoted | `myapp.Users` |
/// | `"myApp"."Users"` | quoted | quoted | `myApp.Users` |
#[derive(Debug, Clone)]
pub struct TableIdentifier {
    // Optional schema part
    schema_canonical: Option<String>,
    schema_display: Option<String>,
    schema_quoted: bool,

    // Table part (always present)
    table_canonical: String,
    table_display: String,
    table_quoted: bool,

    /// Canonical form for HashMap keys and comparison.
    /// - If quoted: exact case preserved (case-sensitive)
    /// - If unquoted: lowercase folded (case-insensitive)
    /// - For compound identifiers: "schema.table" using canonical forms
    canonical: String,

    /// Display form preserving user's original input.
    /// Used for error messages and user-facing output.
    display: String,

    /// Whether the identifier was quoted (delimited) in the original SQL.
    /// Quoted identifiers use double quotes: `"MyTable"`
    /// For compound identifiers, this is true if the table part was quoted.
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
    ///   case-insensitive comparison.
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
        let table_canonical = if quoted {
            // Quoted identifiers preserve exact case (SQL:1999 delimited identifiers)
            name.to_string()
        } else {
            // Unquoted identifiers fold to lowercase for case-insensitive comparison
            name.to_ascii_lowercase()
        };

        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: table_canonical.clone(),
            table_display: name.to_string(),
            table_quoted: quoted,
            canonical: table_canonical,
            display: name.to_string(),
            quoted,
        }
    }

    /// Create an identifier from a canonical name (for internal use).
    ///
    /// This is used when loading from persistence where we only have the
    /// canonical form. The display form is set to match canonical.
    pub fn from_canonical(canonical: String, quoted: bool) -> Self {
        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: canonical.clone(),
            table_display: canonical.clone(),
            table_quoted: quoted,
            canonical: canonical.clone(),
            display: canonical,
            quoted,
        }
    }

    /// Create a qualified (schema.table) identifier.
    ///
    /// Each component (schema and table) has independent quoted/unquoted semantics.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - The schema name as written by the user
    /// * `schema_quoted` - Whether the schema was quoted in SQL
    /// * `table_name` - The table name as written by the user
    /// * `table_quoted` - Whether the table was quoted in SQL
    ///
    /// # SQL:1999 Behavior
    ///
    /// Each identifier component is independent:
    /// - Unquoted: canonical form is lowercase (case-insensitive)
    /// - Quoted: canonical form preserves case (case-sensitive)
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_catalog::TableIdentifier;
    ///
    /// // "myApp".users → myApp.users (schema case-sensitive, table case-insensitive)
    /// let id = TableIdentifier::qualified("myApp", true, "users", false);
    /// assert_eq!(id.canonical(), "myApp.users");
    ///
    /// // myapp."Users" → myapp.Users (schema case-insensitive, table case-sensitive)
    /// let id = TableIdentifier::qualified("myapp", false, "Users", true);
    /// assert_eq!(id.canonical(), "myapp.Users");
    /// ```
    pub fn qualified(
        schema_name: &str,
        schema_quoted: bool,
        table_name: &str,
        table_quoted: bool,
    ) -> Self {
        let schema_canonical = if schema_quoted {
            schema_name.to_string()
        } else {
            schema_name.to_ascii_lowercase()
        };

        let table_canonical = if table_quoted {
            table_name.to_string()
        } else {
            table_name.to_ascii_lowercase()
        };

        let canonical = format!("{}.{}", schema_canonical, table_canonical);
        let display = format!("{}.{}", schema_name, table_name);

        Self {
            schema_canonical: Some(schema_canonical),
            schema_display: Some(schema_name.to_string()),
            schema_quoted,
            table_canonical,
            table_display: table_name.to_string(),
            table_quoted,
            canonical,
            display,
            quoted: table_quoted,
        }
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

    /// Check if this is a qualified (schema.table) identifier.
    #[inline]
    pub fn is_qualified(&self) -> bool {
        self.schema_canonical.is_some()
    }

    /// Get the schema part canonical form (if this is a qualified identifier).
    #[inline]
    pub fn schema_canonical(&self) -> Option<&str> {
        self.schema_canonical.as_deref()
    }

    /// Get the schema part display form (if this is a qualified identifier).
    #[inline]
    pub fn schema_display(&self) -> Option<&str> {
        self.schema_display.as_deref()
    }

    /// Check if the schema part was quoted (if this is a qualified identifier).
    #[inline]
    pub fn is_schema_quoted(&self) -> bool {
        self.schema_quoted
    }

    /// Get the table part canonical form.
    ///
    /// For simple identifiers, this is the same as `canonical()`.
    /// For qualified identifiers, this is just the table name portion.
    #[inline]
    pub fn table_canonical(&self) -> &str {
        &self.table_canonical
    }

    /// Get the table part display form.
    ///
    /// For simple identifiers, this is the same as `display()`.
    /// For qualified identifiers, this is just the table name portion.
    #[inline]
    pub fn table_display(&self) -> &str {
        &self.table_display
    }

    /// Check if the table part was quoted.
    ///
    /// For simple identifiers, this is the same as `is_quoted()`.
    /// For qualified identifiers, this indicates the quoting of the table part only.
    #[inline]
    pub fn is_table_quoted(&self) -> bool {
        self.table_quoted
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

        // Quoted "users" (lowercase) SHOULD find the value because it has the same canonical form
        // (unquoted "users" → "users", quoted "users" → "users")
        let quoted_lower = TableIdentifier::new("users", true);
        assert_eq!(map.get(&quoted_lower), Some(&42));

        // But quoted "USERS" (uppercase) should NOT find it
        let quoted_upper = TableIdentifier::new("USERS", true);
        assert_eq!(map.get(&quoted_upper), None);
    }

    #[test]
    fn test_hashmap_quoted_keys() {
        let mut map: HashMap<TableIdentifier, i32> = HashMap::new();

        // Insert with quoted identifier "MyTable" (preserves case)
        let key = TableIdentifier::new("MyTable", true);
        map.insert(key, 42);

        // Should only find with exact case (quoted)
        let exact = TableIdentifier::new("MyTable", true);
        let wrong_case = TableIdentifier::new("mytable", true);
        let unquoted = TableIdentifier::new("MyTable", false); // becomes "mytable"

        assert_eq!(map.get(&exact), Some(&42));
        assert_eq!(map.get(&wrong_case), None); // "mytable" != "MyTable"
        assert_eq!(map.get(&unquoted), None); // "mytable" != "MyTable"
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
        assert_eq!(first.canonical(), "test"); // Both normalize to lowercase

        // CREATE TABLE "TEST" (id INT);  -- Different table from unquoted!
        let quoted = TableIdentifier::new("TEST", true);
        assert_ne!(first, quoted); // Different table, no conflict (quoted "TEST" != "test")

        // CREATE TABLE "test" (id INT);  -- Same canonical as unquoted test
        let quoted_lower = TableIdentifier::new("test", true);
        assert_eq!(first, quoted_lower); // Same canonical form
    }

    #[test]
    fn test_qualified_identifier_unquoted_both() {
        // myApp.users → myapp.users
        let id = TableIdentifier::qualified("myApp", false, "users", false);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myapp.users");
        assert_eq!(id.display(), "myApp.users");

        assert_eq!(id.schema_canonical(), Some("myapp"));
        assert_eq!(id.schema_display(), Some("myApp"));
        assert!(!id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "users");
        assert_eq!(id.table_display(), "users");
        assert!(!id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_quoted_schema() {
        // "myApp".users → myApp.users
        let id = TableIdentifier::qualified("myApp", true, "users", false);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myApp.users");
        assert_eq!(id.display(), "myApp.users");

        assert_eq!(id.schema_canonical(), Some("myApp"));
        assert_eq!(id.schema_display(), Some("myApp"));
        assert!(id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "users");
        assert_eq!(id.table_display(), "users");
        assert!(!id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_quoted_table() {
        // myapp."Users" → myapp.Users
        let id = TableIdentifier::qualified("myapp", false, "Users", true);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myapp.Users");
        assert_eq!(id.display(), "myapp.Users");

        assert_eq!(id.schema_canonical(), Some("myapp"));
        assert_eq!(id.schema_display(), Some("myapp"));
        assert!(!id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "Users");
        assert_eq!(id.table_display(), "Users");
        assert!(id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_quoted_both() {
        // "myApp"."Users" → myApp.Users
        let id = TableIdentifier::qualified("myApp", true, "Users", true);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myApp.Users");
        assert_eq!(id.display(), "myApp.Users");

        assert_eq!(id.schema_canonical(), Some("myApp"));
        assert_eq!(id.schema_display(), Some("myApp"));
        assert!(id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "Users");
        assert_eq!(id.table_display(), "Users");
        assert!(id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_equality() {
        // Schema case differs, but both unquoted → should match
        let id1 = TableIdentifier::qualified("myApp", false, "users", false);
        let id2 = TableIdentifier::qualified("MYAPP", false, "USERS", false);
        assert_eq!(id1, id2);
        assert_eq!(id1.canonical(), "myapp.users");
        assert_eq!(id2.canonical(), "myapp.users");

        // Schema quoted with different case → should NOT match
        let id3 = TableIdentifier::qualified("myApp", true, "users", false);
        let id4 = TableIdentifier::qualified("MYAPP", true, "users", false);
        assert_ne!(id3, id4);
        assert_eq!(id3.canonical(), "myApp.users");
        assert_eq!(id4.canonical(), "MYAPP.users");

        // Table quoted with different case → should NOT match
        let id5 = TableIdentifier::qualified("myapp", false, "Users", true);
        let id6 = TableIdentifier::qualified("myapp", false, "USERS", true);
        assert_ne!(id5, id6);
        assert_eq!(id5.canonical(), "myapp.Users");
        assert_eq!(id6.canonical(), "myapp.USERS");
    }

    #[test]
    fn test_qualified_vs_simple_identifier() {
        // Qualified and simple identifiers with same table name should NOT be equal
        let simple = TableIdentifier::new("users", false);
        let qualified = TableIdentifier::qualified("myapp", false, "users", false);

        assert_ne!(simple, qualified);
        assert_eq!(simple.canonical(), "users");
        assert_eq!(qualified.canonical(), "myapp.users");

        assert!(!simple.is_qualified());
        assert!(qualified.is_qualified());
    }

    #[test]
    fn test_qualified_identifier_hashmap() {
        let mut map: HashMap<TableIdentifier, i32> = HashMap::new();

        // Insert with quoted schema, unquoted table
        let key = TableIdentifier::qualified("myApp", true, "users", false);
        map.insert(key, 42);

        // Should find with exact schema case (case-sensitive), any table case (case-insensitive)
        let lookup1 = TableIdentifier::qualified("myApp", true, "USERS", false);
        assert_eq!(map.get(&lookup1), Some(&42));

        // Should NOT find with different schema case
        let lookup2 = TableIdentifier::qualified("MYAPP", true, "users", false);
        assert_eq!(map.get(&lookup2), None);

        // Should NOT find with unquoted schema (even if lowercase matches)
        let lookup3 = TableIdentifier::qualified("myApp", false, "users", false);
        assert_eq!(map.get(&lookup3), None);
    }
}
