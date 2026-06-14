//! SQL Identifier types with SQLite-compatible case handling.
//!
//! This module provides identifier types that case-fold identifiers the way
//! SQLite does. Unlike the SQL:1999 standard (where double-quoted "delimited"
//! identifiers are case-sensitive), SQLite ASCII case-folds *all* identifiers
//! for equality and lookup, **even when quoted** (issue #5553):
//!
//! - Canonical form is always lowercase, regardless of quoting.
//! - The `quoted` flag is retained only to preserve the original spelling in
//!   the display form (for error messages, `sqlite_master`, column headers).
//!
//! # Example
//!
//! ```
//! use vibesql_ast::TableIdentifier;
//!
//! // Unquoted identifiers are case-insensitive
//! let id1 = TableIdentifier::new("MyTable", false);
//! let id2 = TableIdentifier::new("mytable", false);
//! let id3 = TableIdentifier::new("MYTABLE", false);
//! assert_eq!(id1, id2);
//! assert_eq!(id2, id3);
//!
//! // SQLite case-folds even quoted identifiers, so "MyTable" == MyTable
//! let quoted = TableIdentifier::new("MyTable", true);
//! assert_eq!(id1, quoted); // Same (case-folded) canonical form
//! assert_eq!(quoted.canonical(), "mytable");
//! assert_eq!(quoted.display(), "MyTable"); // original spelling preserved
//! ```

use std::fmt;
use std::hash::{Hash, Hasher};

/// A SQL identifier with SQLite-compatible case handling.
///
/// This type separates three concerns:
/// - **Canonical form**: Used for HashMap keys and equality comparison
///   (always ASCII case-folded, like SQLite — see issue #5553)
/// - **Display form**: Preserves user's original input for error messages
/// - **Quoted flag**: Whether the identifier was delimited (quoted)
///
/// ## SQLite Case-Folding
///
/// SQLite ASCII case-folds identifiers for equality/lookup regardless of
/// whether they were quoted (issue #5553):
///
/// | Input | Quoted | Canonical | Matches `mytable`? | Matches `"MyTable"`? |
/// |-------|--------|-----------|--------------------|-----------------------|
/// | `MyTable` | No | `mytable` | Yes | Yes |
/// | `"MyTable"` | Yes | `mytable` | Yes | Yes |
/// | `MYTABLE` | No | `mytable` | Yes | Yes |
/// | `"mytable"` | Yes | `mytable` | Yes | Yes |
///
/// ## Compound Identifiers
///
/// TableIdentifier supports schema-qualified table names where each component
/// (schema and table) is independently folded to lowercase for the canonical
/// form, while the original spelling is preserved in the display form:
///
/// | SQL | Canonical Form | Display Form |
/// |-----|----------------|--------------|
/// | `myApp.users` | `myapp.users` | `myApp.users` |
/// | `"myApp".users` | `myapp.users` | `myApp.users` |
/// | `myapp."Users"` | `myapp.users` | `myapp.Users` |
/// | `"myApp"."Users"` | `myapp.users` | `myApp.Users` |
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
    /// Always ASCII case-folded (lowercase), regardless of quoting, matching
    /// SQLite (issue #5553). For compound identifiers: "schema.table".
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
    /// # SQLite Behavior
    ///
    /// The canonical form is always lowercase-folded for case-insensitive
    /// comparison, regardless of the `quoted` flag. The `quoted` flag only
    /// affects the display form (original spelling preserved for echo).
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::TableIdentifier;
    ///
    /// // Unquoted: case-insensitive
    /// let unquoted = TableIdentifier::new("MyTable", false);
    /// assert_eq!(unquoted.canonical(), "mytable");
    /// assert_eq!(unquoted.display(), "MyTable");
    ///
    /// // Quoted: SQLite still case-folds the canonical form
    /// let quoted = TableIdentifier::new("MyTable", true);
    /// assert_eq!(quoted.canonical(), "mytable");
    /// assert_eq!(quoted.display(), "MyTable");
    /// ```
    pub fn new(name: &str, quoted: bool) -> Self {
        // SQLite semantics (issue #5553): identifiers are ASCII case-folded for
        // equality/lookup *regardless* of quoting. Unlike SQL:1999 delimited
        // identifiers, a double-quoted `"TBL1"` collides with `tbl1`. The
        // `quoted` flag is retained only to preserve the original spelling for
        // display/echo (error messages, sqlite_master, column headers).
        let table_canonical = name.to_ascii_lowercase();

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
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        // Fold the stored name to lowercase for the canonical/lookup key while
        // preserving the persisted spelling as the display form.
        let folded = canonical.to_ascii_lowercase();
        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: folded.clone(),
            table_display: canonical.clone(),
            table_quoted: quoted,
            canonical: folded,
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
    /// # SQLite Behavior
    ///
    /// Every component is ASCII case-folded for the canonical form regardless
    /// of quoting; the original spelling is kept in the display form.
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::TableIdentifier;
    ///
    /// // "myApp".users → canonical myapp.users (display preserved)
    /// let id = TableIdentifier::qualified("myApp", true, "users", false);
    /// assert_eq!(id.canonical(), "myapp.users");
    /// assert_eq!(id.display(), "myApp.users");
    ///
    /// // myapp."Users" → canonical myapp.users (display preserved)
    /// let id = TableIdentifier::qualified("myapp", false, "Users", true);
    /// assert_eq!(id.canonical(), "myapp.users");
    /// assert_eq!(id.display(), "myapp.Users");
    /// ```
    pub fn qualified(
        schema_name: &str,
        schema_quoted: bool,
        table_name: &str,
        table_quoted: bool,
    ) -> Self {
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        let schema_canonical = schema_name.to_ascii_lowercase();
        let table_canonical = table_name.to_ascii_lowercase();

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
    /// This is used for HashMap keys and equality comparison. SQLite
    /// case-folds identifiers regardless of quoting, so this is always the
    /// lowercase form.
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

    /// Create a quoted identifier from the given name.
    ///
    /// Note: under SQLite semantics the canonical form is still case-folded,
    /// so this matches case-insensitively; the `quoted` flag only preserves
    /// the original spelling for display/echo.
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

/// A SQL column identifier with SQLite-compatible case handling.
///
/// This type handles column references with optional table and schema qualifiers.
/// Like SQLite, every component is ASCII case-folded for the canonical form
/// regardless of quoting; the original spelling is kept for display/echo.
///
/// ## Supported Forms
///
/// | SQL Form | Description |
/// |----------|-------------|
/// | `id` | Unqualified column reference |
/// | `users.id` | Table-qualified column reference |
/// | `myschema.users.id` | Fully-qualified column reference |
///
/// ## SQLite Case-Folding
///
/// SQLite case-folds identifiers regardless of quoting (issue #5553):
///
/// | Input | Quoted | Canonical | Matches `mycolumn`? |
/// |-------|--------|-----------|---------------------|
/// | `MyColumn` | No | `mycolumn` | Yes |
/// | `"MyColumn"` | Yes | `mycolumn` | Yes |
/// | `MYCOLUMN` | No | `mycolumn` | Yes |
///
/// ## Example
///
/// ```
/// use vibesql_ast::ColumnIdentifier;
///
/// // Unquoted identifiers are case-insensitive
/// let c1 = ColumnIdentifier::simple("MyColumn", false);
/// let c2 = ColumnIdentifier::simple("mycolumn", false);
/// assert_eq!(c1, c2);
///
/// // SQLite case-folds even quoted identifiers
/// let quoted = ColumnIdentifier::simple("MyColumn", true);
/// assert_eq!(c1, quoted);
///
/// // Table-qualified column
/// let qualified = ColumnIdentifier::qualified("users", false, "id", false);
/// assert_eq!(qualified.canonical(), "users.id");
/// ```
#[derive(Debug, Clone)]
pub struct ColumnIdentifier {
    // Optional schema part
    schema_canonical: Option<String>,
    schema_display: Option<String>,
    schema_quoted: bool,

    // Optional table part
    table_canonical: Option<String>,
    table_display: Option<String>,
    table_quoted: bool,

    // Column part (always present)
    column_canonical: String,
    column_display: String,
    column_quoted: bool,

    /// Canonical form for HashMap keys and comparison.
    /// Format: "schema.table.column" or "table.column" or "column"
    canonical: String,

    /// Display form preserving user's original input.
    /// Used for error messages and user-facing output.
    display: String,
}

impl ColumnIdentifier {
    /// Create a simple (unqualified) column identifier.
    ///
    /// # Arguments
    ///
    /// * `column` - The column name as written by the user
    /// * `quoted` - Whether the identifier was quoted (delimited) in SQL
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::simple("MyColumn", false);
    /// assert_eq!(c.canonical(), "mycolumn");
    /// assert_eq!(c.display(), "MyColumn");
    /// ```
    pub fn simple(column: &str, quoted: bool) -> Self {
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        let column_canonical = column.to_ascii_lowercase();

        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: None,
            table_display: None,
            table_quoted: false,
            column_canonical: column_canonical.clone(),
            column_display: column.to_string(),
            column_quoted: quoted,
            canonical: column_canonical,
            display: column.to_string(),
        }
    }

    /// Create a table-qualified column identifier.
    ///
    /// # Arguments
    ///
    /// * `table` - The table name as written by the user
    /// * `table_quoted` - Whether the table was quoted in SQL
    /// * `column` - The column name as written by the user
    /// * `column_quoted` - Whether the column was quoted in SQL
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::qualified("Users", false, "ID", false);
    /// assert_eq!(c.canonical(), "users.id");
    /// assert_eq!(c.table_canonical(), Some("users"));
    /// assert_eq!(c.column_canonical(), "id");
    /// ```
    pub fn qualified(table: &str, table_quoted: bool, column: &str, column_quoted: bool) -> Self {
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        let table_canonical = table.to_ascii_lowercase();
        let column_canonical = column.to_ascii_lowercase();

        let canonical = format!("{}.{}", table_canonical, column_canonical);
        let display = format!("{}.{}", table, column);

        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: Some(table_canonical),
            table_display: Some(table.to_string()),
            table_quoted,
            column_canonical,
            column_display: column.to_string(),
            column_quoted,
            canonical,
            display,
        }
    }

    /// Create a fully-qualified (schema.table.column) identifier.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name as written by the user
    /// * `schema_quoted` - Whether the schema was quoted in SQL
    /// * `table` - The table name as written by the user
    /// * `table_quoted` - Whether the table was quoted in SQL
    /// * `column` - The column name as written by the user
    /// * `column_quoted` - Whether the column was quoted in SQL
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::fully_qualified(
    ///     "myApp", true,   // quoted schema
    ///     "users", false,  // unquoted table
    ///     "ID", false      // unquoted column
    /// );
    /// assert_eq!(c.canonical(), "myapp.users.id");
    /// ```
    pub fn fully_qualified(
        schema: &str,
        schema_quoted: bool,
        table: &str,
        table_quoted: bool,
        column: &str,
        column_quoted: bool,
    ) -> Self {
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        let schema_canonical = schema.to_ascii_lowercase();
        let table_canonical = table.to_ascii_lowercase();
        let column_canonical = column.to_ascii_lowercase();

        let canonical = format!("{}.{}.{}", schema_canonical, table_canonical, column_canonical);
        let display = format!("{}.{}.{}", schema, table, column);

        Self {
            schema_canonical: Some(schema_canonical),
            schema_display: Some(schema.to_string()),
            schema_quoted,
            table_canonical: Some(table_canonical),
            table_display: Some(table.to_string()),
            table_quoted,
            column_canonical,
            column_display: column.to_string(),
            column_quoted,
            canonical,
            display,
        }
    }

    /// Create an unquoted column identifier (convenience constructor).
    ///
    /// This creates a simple, case-insensitive column reference.
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::unquoted("MyColumn");
    /// assert_eq!(c.canonical(), "mycolumn");
    /// ```
    pub fn unquoted(column: &str) -> Self {
        Self::simple(column, false)
    }

    /// Create a quoted column identifier (convenience constructor).
    ///
    /// Note: under SQLite semantics the canonical form is still case-folded.
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::quoted("MyColumn");
    /// assert_eq!(c.canonical(), "mycolumn");
    /// assert_eq!(c.display(), "MyColumn");
    /// ```
    pub fn quoted(column: &str) -> Self {
        Self::simple(column, true)
    }

    /// Create a table.column reference with unquoted identifiers.
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_ast::ColumnIdentifier;
    ///
    /// let c = ColumnIdentifier::table_column("users", "id");
    /// assert_eq!(c.canonical(), "users.id");
    /// ```
    pub fn table_column(table: &str, column: &str) -> Self {
        Self::qualified(table, false, column, false)
    }

    /// Create an identifier from a canonical name (for internal use).
    ///
    /// This is used when loading from persistence where we only have the
    /// canonical form. The display form is set to match canonical.
    pub fn from_canonical(canonical: String, quoted: bool) -> Self {
        // SQLite case-folds identifiers regardless of quoting (issue #5553).
        let folded = canonical.to_ascii_lowercase();
        Self {
            schema_canonical: None,
            schema_display: None,
            schema_quoted: false,
            table_canonical: None,
            table_display: None,
            table_quoted: false,
            column_canonical: folded.clone(),
            column_display: canonical.clone(),
            column_quoted: quoted,
            canonical: folded,
            display: canonical,
        }
    }

    /// Get the canonical form of the identifier.
    ///
    /// This is used for HashMap keys and equality comparison.
    /// Format: "schema.table.column" or "table.column" or "column"
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

    /// Get the column name in canonical form.
    #[inline]
    pub fn column_canonical(&self) -> &str {
        &self.column_canonical
    }

    /// Get the column name in display form.
    #[inline]
    pub fn column_display(&self) -> &str {
        &self.column_display
    }

    /// Check if the column was quoted.
    #[inline]
    pub fn is_column_quoted(&self) -> bool {
        self.column_quoted
    }

    /// Get the table name in canonical form (if qualified).
    #[inline]
    pub fn table_canonical(&self) -> Option<&str> {
        self.table_canonical.as_deref()
    }

    /// Get the table name in display form (if qualified).
    #[inline]
    pub fn table_display(&self) -> Option<&str> {
        self.table_display.as_deref()
    }

    /// Check if the table was quoted (if qualified).
    #[inline]
    pub fn is_table_quoted(&self) -> bool {
        self.table_quoted
    }

    /// Get the schema name in canonical form (if fully qualified).
    #[inline]
    pub fn schema_canonical(&self) -> Option<&str> {
        self.schema_canonical.as_deref()
    }

    /// Get the schema name in display form (if fully qualified).
    #[inline]
    pub fn schema_display(&self) -> Option<&str> {
        self.schema_display.as_deref()
    }

    /// Check if the schema was quoted (if fully qualified).
    #[inline]
    pub fn is_schema_quoted(&self) -> bool {
        self.schema_quoted
    }

    /// Check if this is a table-qualified column (has table but not schema).
    #[inline]
    pub fn is_qualified(&self) -> bool {
        self.table_canonical.is_some()
    }

    /// Check if this is a fully-qualified column (has schema.table.column).
    #[inline]
    pub fn is_fully_qualified(&self) -> bool {
        self.schema_canonical.is_some()
    }

    /// Check if this column reference is ambiguous (no table qualifier).
    #[inline]
    pub fn is_ambiguous(&self) -> bool {
        self.table_canonical.is_none()
    }

    /// Get the canonical form as an owned String.
    #[inline]
    pub fn into_canonical(self) -> String {
        self.canonical
    }

    /// Check if this column matches another by canonical column name only.
    ///
    /// This ignores table and schema qualifiers, useful for finding columns
    /// by name across different tables.
    pub fn matches_column_name(&self, name: &str, case_sensitive: bool) -> bool {
        if case_sensitive {
            self.column_canonical == name
        } else {
            self.column_canonical == name.to_ascii_lowercase()
        }
    }

    /// Resolve an ambiguous column against a table.
    ///
    /// If this column is unqualified, creates a new column identifier
    /// qualified with the given table. If already qualified, returns self.
    pub fn resolve_against(&self, table: &TableIdentifier) -> Self {
        if self.is_qualified() {
            return self.clone();
        }

        if table.is_qualified() {
            // Table has schema qualification
            Self::fully_qualified(
                table.schema_display().unwrap_or_default(),
                table.is_schema_quoted(),
                table.table_display(),
                table.is_table_quoted(),
                &self.column_display,
                self.column_quoted,
            )
        } else {
            // Simple table name
            Self::qualified(
                table.table_display(),
                table.is_table_quoted(),
                &self.column_display,
                self.column_quoted,
            )
        }
    }

    /// Check if this column reference matches another.
    ///
    /// Matching rules:
    /// - An unqualified column matches if the column names match
    /// - A qualified column matches only if both table and column match
    /// - A fully-qualified column matches only if schema, table, and column match
    pub fn matches(&self, other: &ColumnIdentifier) -> bool {
        // Both must have same qualification level for exact match
        if self.is_fully_qualified() != other.is_fully_qualified() {
            // But an unqualified can match a qualified if column name matches
            if self.is_ambiguous() {
                return self.column_canonical == other.column_canonical;
            }
            if other.is_ambiguous() {
                return self.column_canonical == other.column_canonical;
            }
            return false;
        }

        if self.is_qualified() != other.is_qualified() {
            // Unqualified matches qualified if column name matches
            if self.is_ambiguous() {
                return self.column_canonical == other.column_canonical;
            }
            if other.is_ambiguous() {
                return self.column_canonical == other.column_canonical;
            }
            return false;
        }

        // Same qualification level - compare canonical forms
        self.canonical == other.canonical
    }

    /// Create a column identifier by stripping qualifiers.
    ///
    /// Returns a new unqualified column identifier with just the column name.
    pub fn unqualify(&self) -> Self {
        Self::simple(&self.column_display, self.column_quoted)
    }

    /// Create a column identifier with a different table qualifier.
    ///
    /// Useful when remapping columns during query planning.
    pub fn with_table(&self, table: &str, table_quoted: bool) -> Self {
        if let Some(schema_display) = &self.schema_display {
            Self::fully_qualified(
                schema_display,
                self.schema_quoted,
                table,
                table_quoted,
                &self.column_display,
                self.column_quoted,
            )
        } else {
            Self::qualified(table, table_quoted, &self.column_display, self.column_quoted)
        }
    }
}

impl PartialEq for ColumnIdentifier {
    /// Two column identifiers are equal if their canonical forms match.
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Eq for ColumnIdentifier {}

impl Hash for ColumnIdentifier {
    /// Hash based on canonical form for consistent HashMap behavior.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical.hash(state);
    }
}

impl fmt::Display for ColumnIdentifier {
    /// Display uses the original user input form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display)
    }
}

impl From<&str> for ColumnIdentifier {
    /// Convert from a string, assuming unquoted (case-insensitive).
    fn from(s: &str) -> Self {
        Self::simple(s, false)
    }
}

impl From<String> for ColumnIdentifier {
    /// Convert from a String, assuming unquoted (case-insensitive).
    fn from(s: String) -> Self {
        Self::simple(&s, false)
    }
}

/// A general SQL identifier (for indexes, views, etc.)
///
/// This is a type alias for now, but can be specialized later if needed.
pub type Identifier = TableIdentifier;

/// A SQL function identifier with proper case handling.
///
/// Similar to TableIdentifier and ColumnIdentifier, this type separates:
/// - **Canonical form**: Lowercase for comparison and lookup
/// - **Display form**: Preserves user's original case for error messages
///
/// ## Example
///
/// ```
/// use vibesql_ast::FunctionIdentifier;
///
/// // User wrote SUBSTR in their query
/// let func = FunctionIdentifier::new("SUBSTR");
/// assert_eq!(func.canonical(), "substr");  // Lowercase for comparison
/// assert_eq!(func.display(), "SUBSTR");    // Original case for errors
/// ```
#[derive(Debug, Clone)]
pub struct FunctionIdentifier {
    /// Canonical form (lowercase) for comparison and lookup
    canonical: String,
    /// Display form preserving user's original input
    display: String,
}

impl FunctionIdentifier {
    /// Create a new function identifier preserving original case.
    ///
    /// The canonical form is lowercased for case-insensitive comparison,
    /// while the display form preserves the original case for error messages.
    pub fn new(name: &str) -> Self {
        Self { canonical: name.to_lowercase(), display: name.to_string() }
    }

    /// Get the canonical (lowercase) form for comparison.
    pub fn canonical(&self) -> &str {
        &self.canonical
    }

    /// Get the display form (original case) for error messages.
    pub fn display(&self) -> &str {
        &self.display
    }

    /// Check if this function matches the given name (case-insensitive).
    pub fn matches(&self, name: &str) -> bool {
        self.canonical == name.to_lowercase()
    }

    /// Get the canonical form as str (alias for canonical()).
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Return the canonical (lowercase) form as a new String.
    /// Provides compatibility with code expecting String methods.
    pub fn to_lowercase(&self) -> String {
        self.canonical.clone()
    }

    /// Return the canonical form uppercased.
    /// Provides compatibility with code expecting String methods.
    pub fn to_uppercase(&self) -> String {
        self.canonical.to_uppercase()
    }

    /// Case-insensitive comparison with a string slice.
    /// Provides compatibility with code expecting String methods.
    pub fn eq_ignore_ascii_case(&self, other: &str) -> bool {
        self.canonical == other.to_ascii_lowercase()
    }
}

impl PartialEq for FunctionIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl PartialEq<str> for FunctionIdentifier {
    fn eq(&self, other: &str) -> bool {
        self.canonical == other.to_lowercase()
    }
}

impl PartialEq<&str> for FunctionIdentifier {
    fn eq(&self, other: &&str) -> bool {
        self.canonical == other.to_lowercase()
    }
}

impl Eq for FunctionIdentifier {}

impl Hash for FunctionIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical.hash(state);
    }
}

impl fmt::Display for FunctionIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display)
    }
}

impl From<&str> for FunctionIdentifier {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for FunctionIdentifier {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

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
    fn test_quoted_case_folds_like_sqlite() {
        // SQLite case-folds even quoted identifiers (issue #5553).
        let id1 = TableIdentifier::new("MyTable", true);
        let id2 = TableIdentifier::new("mytable", true);
        let id3 = TableIdentifier::new("MYTABLE", true);

        // All quoted variations are equal (case-folded canonical)
        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
        assert_eq!(id1, id3);

        // Canonical is always lowercase
        assert_eq!(id1.canonical(), "mytable");
        assert_eq!(id2.canonical(), "mytable");
        assert_eq!(id3.canonical(), "mytable");

        // Display preserves the original spelling for echo
        assert_eq!(id1.display(), "MyTable");
        assert_eq!(id3.display(), "MYTABLE");
    }

    #[test]
    fn test_quoted_vs_unquoted() {
        // Under SQLite semantics, quoted "MyTable" DOES equal unquoted "MyTable"
        // because both case-fold to the same canonical form.
        let unquoted = TableIdentifier::new("MyTable", false);
        let quoted = TableIdentifier::new("MyTable", true);

        assert_eq!(unquoted, quoted);
        assert_eq!(unquoted.canonical(), "mytable");
        assert_eq!(quoted.canonical(), "mytable");
    }

    #[test]
    fn test_quoted_lowercase_matches_unquoted() {
        // quoted "mytable" equals unquoted "MyTable" (same case-folded canonical)
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

        // SQLite case-folds quoted identifiers too, so BOTH quoted variants find it
        let quoted_lower = TableIdentifier::new("users", true);
        assert_eq!(map.get(&quoted_lower), Some(&42));

        let quoted_upper = TableIdentifier::new("USERS", true);
        assert_eq!(map.get(&quoted_upper), Some(&42));
    }

    #[test]
    fn test_hashmap_quoted_keys() {
        let mut map: HashMap<TableIdentifier, i32> = HashMap::new();

        // Insert with quoted identifier "MyTable" (case-folded to "mytable")
        let key = TableIdentifier::new("MyTable", true);
        map.insert(key, 42);

        // SQLite case-folds: all case/quoting variants resolve to the same key
        let exact = TableIdentifier::new("MyTable", true);
        let wrong_case = TableIdentifier::new("mytable", true);
        let unquoted = TableIdentifier::new("MyTable", false); // also "mytable"

        assert_eq!(map.get(&exact), Some(&42));
        assert_eq!(map.get(&wrong_case), Some(&42));
        assert_eq!(map.get(&unquoted), Some(&42));
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

        // SQLite case-folds both; the quoted flag only affects display.
        assert_eq!(unquoted.canonical(), "mytable");
        assert_eq!(quoted.canonical(), "mytable");
        assert_eq!(quoted.display(), "MyTable");
    }

    #[test]
    fn test_from_canonical() {
        let id = TableIdentifier::from_canonical("mytable".to_string(), false);
        assert_eq!(id.canonical(), "mytable");
        assert_eq!(id.display(), "mytable");
        assert!(!id.is_quoted());

        // from_canonical case-folds for lookup while preserving the spelling.
        let quoted_id = TableIdentifier::from_canonical("MyTable".to_string(), true);
        assert_eq!(quoted_id.canonical(), "mytable");
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
        // SQLite case-folding semantics (issue #5553).

        // CREATE TABLE MyTable (id INT);
        // INSERT INTO mytable / MYTABLE both resolve to MyTable.
        let created = TableIdentifier::new("MyTable", false);
        let lookup1 = TableIdentifier::new("mytable", false);
        let lookup2 = TableIdentifier::new("MYTABLE", false);

        assert_eq!(created, lookup1);
        assert_eq!(created, lookup2);

        // CREATE TABLE "MyTable" -- SQLite case-folds, so this COLLIDES with MyTable.
        let quoted_created = TableIdentifier::new("MyTable", true);
        assert_eq!(created, quoted_created);

        // SELECT * FROM "MyTable" / "MYTABLE" / MyTable all resolve to the same table.
        let quoted_lookup = TableIdentifier::new("MYTABLE", true);
        assert_eq!(quoted_created, quoted_lookup);
        assert_eq!(created, quoted_lookup);

        let unquoted_lookup = TableIdentifier::new("MyTable", false);
        assert_eq!(created, unquoted_lookup);
        assert_eq!(quoted_created, unquoted_lookup);
    }

    #[test]
    fn test_create_duplicate_detection() {
        // CREATE TABLE test; CREATE TABLE TEST -- collide (case-folded).
        let first = TableIdentifier::new("test", false);
        let second = TableIdentifier::new("TEST", false);
        assert_eq!(first, second);
        assert_eq!(first.canonical(), "test");

        // CREATE TABLE "TEST" -- SQLite case-folds, so this ALSO collides with test.
        let quoted = TableIdentifier::new("TEST", true);
        assert_eq!(first, quoted);
        assert_eq!(quoted.canonical(), "test");

        // CREATE TABLE "test" -- same canonical, collides too.
        let quoted_lower = TableIdentifier::new("test", true);
        assert_eq!(first, quoted_lower);
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
        // "myApp".users → canonical myapp.users (display preserved)
        let id = TableIdentifier::qualified("myApp", true, "users", false);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myapp.users");
        assert_eq!(id.display(), "myApp.users");

        assert_eq!(id.schema_canonical(), Some("myapp"));
        assert_eq!(id.schema_display(), Some("myApp"));
        assert!(id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "users");
        assert_eq!(id.table_display(), "users");
        assert!(!id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_quoted_table() {
        // myapp."Users" → canonical myapp.users (display preserved)
        let id = TableIdentifier::qualified("myapp", false, "Users", true);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myapp.users");
        assert_eq!(id.display(), "myapp.Users");

        assert_eq!(id.schema_canonical(), Some("myapp"));
        assert_eq!(id.schema_display(), Some("myapp"));
        assert!(!id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "users");
        assert_eq!(id.table_display(), "Users");
        assert!(id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_quoted_both() {
        // "myApp"."Users" → canonical myapp.users (display preserved)
        let id = TableIdentifier::qualified("myApp", true, "Users", true);

        assert!(id.is_qualified());
        assert_eq!(id.canonical(), "myapp.users");
        assert_eq!(id.display(), "myApp.Users");

        assert_eq!(id.schema_canonical(), Some("myapp"));
        assert_eq!(id.schema_display(), Some("myApp"));
        assert!(id.is_schema_quoted());

        assert_eq!(id.table_canonical(), "users");
        assert_eq!(id.table_display(), "Users");
        assert!(id.is_table_quoted());
    }

    #[test]
    fn test_qualified_identifier_equality() {
        // Schema case differs, both unquoted → match
        let id1 = TableIdentifier::qualified("myApp", false, "users", false);
        let id2 = TableIdentifier::qualified("MYAPP", false, "USERS", false);
        assert_eq!(id1, id2);
        assert_eq!(id1.canonical(), "myapp.users");
        assert_eq!(id2.canonical(), "myapp.users");

        // SQLite case-folds quoted parts too → these now match.
        let id3 = TableIdentifier::qualified("myApp", true, "users", false);
        let id4 = TableIdentifier::qualified("MYAPP", true, "users", false);
        assert_eq!(id3, id4);
        assert_eq!(id3.canonical(), "myapp.users");
        assert_eq!(id4.canonical(), "myapp.users");

        // Quoted table with different case also case-folds → match.
        let id5 = TableIdentifier::qualified("myapp", false, "Users", true);
        let id6 = TableIdentifier::qualified("myapp", false, "USERS", true);
        assert_eq!(id5, id6);
        assert_eq!(id5.canonical(), "myapp.users");
        assert_eq!(id6.canonical(), "myapp.users");
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

        // Insert with quoted schema, unquoted table (canonical myapp.users)
        let key = TableIdentifier::qualified("myApp", true, "users", false);
        map.insert(key, 42);

        // SQLite case-folds everything, so any case/quoting variation finds it.
        let lookup1 = TableIdentifier::qualified("myApp", true, "USERS", false);
        assert_eq!(map.get(&lookup1), Some(&42));

        let lookup2 = TableIdentifier::qualified("MYAPP", true, "users", false);
        assert_eq!(map.get(&lookup2), Some(&42));

        let lookup3 = TableIdentifier::qualified("myApp", false, "users", false);
        assert_eq!(map.get(&lookup3), Some(&42));
    }

    // ==================== ColumnIdentifier Tests ====================

    #[test]
    fn test_column_unquoted_case_insensitive() {
        let c1 = ColumnIdentifier::simple("MyColumn", false);
        let c2 = ColumnIdentifier::simple("mycolumn", false);
        let c3 = ColumnIdentifier::simple("MYCOLUMN", false);

        // All unquoted variations should be equal
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);

        // Canonical should be lowercase
        assert_eq!(c1.canonical(), "mycolumn");
        assert_eq!(c2.canonical(), "mycolumn");
        assert_eq!(c3.canonical(), "mycolumn");

        // Display preserves original
        assert_eq!(c1.display(), "MyColumn");
        assert_eq!(c2.display(), "mycolumn");
        assert_eq!(c3.display(), "MYCOLUMN");
    }

    #[test]
    fn test_column_quoted_case_folds_like_sqlite() {
        // SQLite case-folds even quoted column identifiers (issue #5553).
        let c1 = ColumnIdentifier::simple("MyColumn", true);
        let c2 = ColumnIdentifier::simple("mycolumn", true);
        let c3 = ColumnIdentifier::simple("MYCOLUMN", true);

        // All quoted variations are equal (case-folded canonical)
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);
        assert_eq!(c1, c3);

        // Canonical is always lowercase; display preserves the spelling.
        assert_eq!(c1.canonical(), "mycolumn");
        assert_eq!(c3.canonical(), "mycolumn");
        assert_eq!(c1.display(), "MyColumn");
    }

    #[test]
    fn test_column_qualified() {
        let c = ColumnIdentifier::qualified("Users", false, "ID", false);
        assert_eq!(c.canonical(), "users.id");
        assert_eq!(c.table_canonical(), Some("users"));
        assert_eq!(c.column_canonical(), "id");
        assert!(c.is_qualified());
        assert!(!c.is_fully_qualified());
        assert!(!c.is_ambiguous());
    }

    #[test]
    fn test_column_fully_qualified() {
        let c = ColumnIdentifier::fully_qualified(
            "myApp", true, // quoted schema
            "users", false, // unquoted table
            "ID", false, // unquoted column
        );
        assert_eq!(c.canonical(), "myapp.users.id");
        assert_eq!(c.schema_canonical(), Some("myapp"));
        assert_eq!(c.table_canonical(), Some("users"));
        assert_eq!(c.column_canonical(), "id");
        assert!(c.is_qualified());
        assert!(c.is_fully_qualified());
        assert!(!c.is_ambiguous());
    }

    #[test]
    fn test_column_convenience_constructors() {
        let unquoted = ColumnIdentifier::unquoted("MyColumn");
        assert_eq!(unquoted.canonical(), "mycolumn");
        assert!(!unquoted.is_column_quoted());

        let quoted = ColumnIdentifier::quoted("MyColumn");
        assert_eq!(quoted.canonical(), "mycolumn");
        assert_eq!(quoted.display(), "MyColumn");
        assert!(quoted.is_column_quoted());

        let table_col = ColumnIdentifier::table_column("users", "id");
        assert_eq!(table_col.canonical(), "users.id");
        assert!(table_col.is_qualified());
    }

    #[test]
    fn test_column_hashmap_lookup() {
        let mut map: HashMap<ColumnIdentifier, i32> = HashMap::new();

        // Insert with unquoted identifier
        let key = ColumnIdentifier::simple("userId", false);
        map.insert(key, 42);

        // Should find with different case (unquoted)
        let lookup1 = ColumnIdentifier::simple("USERID", false);
        let lookup2 = ColumnIdentifier::simple("UserId", false);
        let lookup3 = ColumnIdentifier::simple("userid", false);

        assert_eq!(map.get(&lookup1), Some(&42));
        assert_eq!(map.get(&lookup2), Some(&42));
        assert_eq!(map.get(&lookup3), Some(&42));

        // SQLite case-folds quoted identifiers too, so both quoted variants find it.
        let quoted_lower = ColumnIdentifier::simple("userid", true);
        assert_eq!(map.get(&quoted_lower), Some(&42));

        let quoted_upper = ColumnIdentifier::simple("USERID", true);
        assert_eq!(map.get(&quoted_upper), Some(&42));
    }

    #[test]
    fn test_column_matches_column_name() {
        let c = ColumnIdentifier::qualified("users", false, "id", false);

        // Case-insensitive match
        assert!(c.matches_column_name("id", false));
        assert!(c.matches_column_name("ID", false));
        assert!(c.matches_column_name("Id", false));

        // Case-sensitive match
        assert!(c.matches_column_name("id", true));
        assert!(!c.matches_column_name("ID", true));
        assert!(!c.matches_column_name("Id", true));
    }

    #[test]
    fn test_column_matches() {
        // Same qualified columns
        let c1 = ColumnIdentifier::qualified("users", false, "id", false);
        let c2 = ColumnIdentifier::qualified("USERS", false, "ID", false);
        assert!(c1.matches(&c2));

        // Unqualified matches qualified by column name
        let unqualified = ColumnIdentifier::simple("id", false);
        assert!(unqualified.matches(&c1));
        assert!(c1.matches(&unqualified));

        // Different tables should not match when both qualified
        let c3 = ColumnIdentifier::qualified("orders", false, "id", false);
        assert!(!c1.matches(&c3));

        // But unqualified still matches
        assert!(unqualified.matches(&c3));
    }

    #[test]
    fn test_column_resolve_against() {
        let col = ColumnIdentifier::simple("id", false);
        let table = TableIdentifier::new("users", false);

        let resolved = col.resolve_against(&table);
        assert_eq!(resolved.canonical(), "users.id");
        assert!(resolved.is_qualified());

        // Already qualified column should not change
        let qualified = ColumnIdentifier::qualified("orders", false, "id", false);
        let resolved2 = qualified.resolve_against(&table);
        assert_eq!(resolved2.canonical(), "orders.id");
    }

    #[test]
    fn test_column_resolve_against_qualified_table() {
        let col = ColumnIdentifier::simple("id", false);
        let table = TableIdentifier::qualified("myapp", false, "users", false);

        let resolved = col.resolve_against(&table);
        assert_eq!(resolved.canonical(), "myapp.users.id");
        assert!(resolved.is_fully_qualified());
    }

    #[test]
    fn test_column_unqualify() {
        let qualified = ColumnIdentifier::qualified("users", false, "ID", true);
        let unqualified = qualified.unqualify();

        assert!(!unqualified.is_qualified());
        // SQLite case-folds even quoted columns; spelling preserved in display.
        assert_eq!(unqualified.column_canonical(), "id");
        assert_eq!(unqualified.column_display(), "ID");
        assert!(unqualified.is_column_quoted());
    }

    #[test]
    fn test_column_with_table() {
        let col = ColumnIdentifier::simple("id", false);
        let with_table = col.with_table("users", false);

        assert_eq!(with_table.canonical(), "users.id");
        assert_eq!(with_table.table_canonical(), Some("users"));
    }

    #[test]
    fn test_column_display_trait() {
        let c = ColumnIdentifier::qualified("Users", false, "ID", false);
        assert_eq!(format!("{}", c), "Users.ID");

        let simple = ColumnIdentifier::simple("MyColumn", false);
        assert_eq!(format!("{}", simple), "MyColumn");
    }

    #[test]
    fn test_column_from_traits() {
        let c1: ColumnIdentifier = "MyColumn".into();
        let c2: ColumnIdentifier = String::from("MyColumn").into();

        assert_eq!(c1, c2);
        assert_eq!(c1.canonical(), "mycolumn"); // From assumes unquoted
    }

    #[test]
    fn test_column_from_canonical() {
        let c = ColumnIdentifier::from_canonical("mycolumn".to_string(), false);
        assert_eq!(c.canonical(), "mycolumn");
        assert_eq!(c.display(), "mycolumn");
        assert!(!c.is_column_quoted());

        let quoted = ColumnIdentifier::from_canonical("MyColumn".to_string(), true);
        assert_eq!(quoted.canonical(), "mycolumn");
        assert_eq!(quoted.display(), "MyColumn");
        assert!(quoted.is_column_quoted());
    }

    #[test]
    fn test_column_into_canonical() {
        let c = ColumnIdentifier::simple("MyColumn", false);
        let canonical: String = c.into_canonical();
        assert_eq!(canonical, "mycolumn");
    }

    #[test]
    fn test_column_qualified_equality() {
        // Table case differs, but both unquoted → should match
        let c1 = ColumnIdentifier::qualified("Users", false, "id", false);
        let c2 = ColumnIdentifier::qualified("USERS", false, "ID", false);
        assert_eq!(c1, c2);
        assert_eq!(c1.canonical(), "users.id");

        // SQLite case-folds quoted table parts too → these now match.
        let c3 = ColumnIdentifier::qualified("Users", true, "id", false);
        let c4 = ColumnIdentifier::qualified("USERS", true, "id", false);
        assert_eq!(c3, c4);

        // Quoted column with different case also case-folds → match.
        let c5 = ColumnIdentifier::qualified("users", false, "Id", true);
        let c6 = ColumnIdentifier::qualified("users", false, "ID", true);
        assert_eq!(c5, c6);
    }

    #[test]
    fn test_column_qualified_hashmap() {
        let mut map: HashMap<ColumnIdentifier, i32> = HashMap::new();

        // Insert with quoted table, unquoted column (canonical users.id)
        let key = ColumnIdentifier::qualified("Users", true, "id", false);
        map.insert(key, 42);

        // SQLite case-folds everything → any case/quoting variation finds it.
        let lookup1 = ColumnIdentifier::qualified("Users", true, "ID", false);
        assert_eq!(map.get(&lookup1), Some(&42));

        let lookup2 = ColumnIdentifier::qualified("USERS", true, "id", false);
        assert_eq!(map.get(&lookup2), Some(&42));

        let lookup3 = ColumnIdentifier::qualified("Users", false, "id", false);
        assert_eq!(map.get(&lookup3), Some(&42));
    }

    #[test]
    fn test_column_issue_examples() {
        // Examples from issue #4527

        // Unquoted case-insensitive
        let c1 = ColumnIdentifier::simple("MyColumn", false);
        let c2 = ColumnIdentifier::simple("mycolumn", false);
        let c3 = ColumnIdentifier::simple("MYCOLUMN", false);
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);
        assert_eq!(c1.canonical(), "mycolumn");

        // Quoted identifiers also case-fold (SQLite, issue #5553)
        let q1 = ColumnIdentifier::simple("MyColumn", true);
        let q2 = ColumnIdentifier::simple("mycolumn", true);
        assert_eq!(q1, q2);
        assert_eq!(q1.canonical(), "mycolumn");

        // Qualified column
        let qc = ColumnIdentifier::qualified("Users", false, "ID", false);
        assert_eq!(qc.canonical(), "users.id");
        assert_eq!(qc.table_canonical(), Some("users"));
        assert_eq!(qc.column_canonical(), "id");

        // Fully qualified
        let fq = ColumnIdentifier::fully_qualified(
            "myApp", true, // quoted schema
            "users", false, // unquoted table
            "ID", false, // unquoted column
        );
        assert_eq!(fq.canonical(), "myapp.users.id");
    }

    #[test]
    fn test_column_ambiguous_predicates() {
        let simple = ColumnIdentifier::simple("id", false);
        assert!(simple.is_ambiguous());
        assert!(!simple.is_qualified());
        assert!(!simple.is_fully_qualified());

        let qualified = ColumnIdentifier::qualified("users", false, "id", false);
        assert!(!qualified.is_ambiguous());
        assert!(qualified.is_qualified());
        assert!(!qualified.is_fully_qualified());

        let fully = ColumnIdentifier::fully_qualified("myapp", false, "users", false, "id", false);
        assert!(!fully.is_ambiguous());
        assert!(fully.is_qualified());
        assert!(fully.is_fully_qualified());
    }
}
