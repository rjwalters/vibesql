// ============================================================================
// Database Session Management (SQL Mode, Session Variables, Security)
// ============================================================================

use super::core::Database;

impl Database {
    // ============================================================================
    // Security and Role Management
    // ============================================================================

    /// Set the current session role for privilege checks
    pub fn set_role(&mut self, role: Option<String>) {
        self.lifecycle.set_role(role);
    }

    /// Get the current session role (defaults to "PUBLIC" if not set)
    pub fn get_current_role(&self) -> String {
        self.lifecycle.current_role().map(|s| s.to_string()).unwrap_or_else(|| "PUBLIC".to_string())
    }

    /// Check if security enforcement is enabled
    pub fn is_security_enabled(&self) -> bool {
        self.lifecycle.is_security_enabled()
    }

    /// Disable security checks (for testing)
    pub fn disable_security(&mut self) {
        self.lifecycle.disable_security();
    }

    /// Enable security checks
    pub fn enable_security(&mut self) {
        self.lifecycle.enable_security();
    }

    // ============================================================================
    // Session Variables
    // ============================================================================

    /// Set a session variable (MySQL-style @variable)
    pub fn set_session_variable(&mut self, name: &str, value: vibesql_types::SqlValue) {
        self.metadata.set_session_variable(name, value);
    }

    /// Get a session variable value
    pub fn get_session_variable(&self, name: &str) -> Option<&vibesql_types::SqlValue> {
        self.metadata.get_session_variable(name)
    }

    /// Clear all session variables
    pub fn clear_session_variables(&mut self) {
        self.metadata.clear_session_variables();
    }

    // ============================================================================
    // SQL Mode
    // ============================================================================

    /// Get the current SQL compatibility mode
    pub fn sql_mode(&self) -> vibesql_types::SqlMode {
        self.sql_mode.clone()
    }

    // ============================================================================
    // PRAGMA Settings (SQLite compatibility)
    // ============================================================================

    /// Get the full_column_names PRAGMA setting
    ///
    /// When ON, column names in result sets use "table.column" format
    pub fn full_column_names(&self) -> bool {
        match self.get_session_variable("FULL_COLUMN_NAMES") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF
        }
    }

    /// Set the full_column_names PRAGMA setting
    pub fn set_full_column_names(&mut self, value: bool) {
        self.set_session_variable(
            "FULL_COLUMN_NAMES",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the short_column_names PRAGMA setting
    ///
    /// When ON (default), column names use just the column name (e.g., "f1")
    /// When OFF, column names may include expression text
    pub fn short_column_names(&self) -> bool {
        match self.get_session_variable("SHORT_COLUMN_NAMES") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => true, // Default: ON
        }
    }

    /// Set the short_column_names PRAGMA setting
    pub fn set_short_column_names(&mut self, value: bool) {
        self.set_session_variable(
            "SHORT_COLUMN_NAMES",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the case_sensitive_like PRAGMA setting
    ///
    /// When OFF (default), LIKE comparisons are case-insensitive for ASCII letters (A-Z = a-z).
    /// When ON, LIKE comparisons are case-sensitive (strict byte-for-byte matching).
    ///
    /// This matches SQLite's default behavior where LIKE is case-insensitive for ASCII.
    pub fn case_sensitive_like(&self) -> bool {
        match self.get_session_variable("CASE_SENSITIVE_LIKE") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (case-insensitive LIKE)
        }
    }

    /// Set the case_sensitive_like PRAGMA setting
    pub fn set_case_sensitive_like(&mut self, value: bool) {
        self.set_session_variable(
            "CASE_SENSITIVE_LIKE",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the reverse_unordered_selects PRAGMA setting
    ///
    /// When ON, the order of output rows from SELECT statements that do not have
    /// an ORDER BY clause is reversed. This is useful for testing to ensure that
    /// applications do not depend on an implicit row ordering.
    pub fn reverse_unordered_selects(&self) -> bool {
        match self.get_session_variable("REVERSE_UNORDERED_SELECTS") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF
        }
    }

    /// Set the reverse_unordered_selects PRAGMA setting
    pub fn set_reverse_unordered_selects(&mut self, value: bool) {
        self.set_session_variable(
            "REVERSE_UNORDERED_SELECTS",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the recursive_triggers PRAGMA setting (SQLite compatibility).
    ///
    /// When ON, a trigger's body may fire further triggers — including,
    /// indirectly, itself — up to `MAX_TRIGGER_RECURSION_DEPTH`. When OFF
    /// (VibeSQL default, matching SQLite's `pragma.c` default of 0), a trigger
    /// that is already executing is not re-fired by DML performed within its own
    /// body (directly- or mutually-recursive trigger firing is suppressed). This
    /// is the historical SQLite behavior that pre-recursion tests such as
    /// `trigger1.test`, `triggerC.test`, and `trigger3.test` rely on (see #5535,
    /// #5840).
    ///
    /// Suppression is *per trigger*: a nested DML statement still fires any
    /// trigger that is not already on the execution stack — only a re-entry into
    /// a trigger that is currently running is skipped. The depth cap (#5479) is
    /// orthogonal and still applies when recursion is enabled.
    pub fn recursive_triggers(&self) -> bool {
        match self.get_session_variable("RECURSIVE_TRIGGERS") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (matches SQLite's pragma.c default of 0)
        }
    }

    /// Set the recursive_triggers PRAGMA setting (SQLite compatibility).
    pub fn set_recursive_triggers(&mut self, value: bool) {
        self.set_session_variable(
            "RECURSIVE_TRIGGERS",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the per-connection trigger recursion-depth limit, if one has been set
    /// via `sqlite3_limit(db, SQLITE_LIMIT_TRIGGER_DEPTH, N)` (#5536).
    ///
    /// Returns `None` when the connection has not lowered the limit, in which
    /// case the executor falls back to its compile-time
    /// `MAX_TRIGGER_RECURSION_DEPTH` cap. A returned `Some(n)` is the raw value
    /// requested by the caller; the executor clamps it into the stack-safe range
    /// `[1, MAX_TRIGGER_RECURSION_DEPTH]` (the cap that keeps native recursion
    /// from overflowing the stack lives in `vibesql-executor`, so the clamp is
    /// applied there to avoid a storage -> executor dependency).
    ///
    /// This mirrors SQLite's `db->aLimit[SQLITE_LIMIT_TRIGGER_DEPTH]`, a
    /// per-connection runtime value layered on top of the compile-time
    /// `SQLITE_MAX_TRIGGER_DEPTH`.
    pub fn trigger_depth_limit(&self) -> Option<i64> {
        match self.get_session_variable("TRIGGER_DEPTH_LIMIT") {
            Some(vibesql_types::SqlValue::Integer(n)) => Some(*n),
            _ => None,
        }
    }

    /// Set the per-connection trigger recursion-depth limit (#5536).
    ///
    /// Stores the raw requested value; clamping into the stack-safe range is the
    /// executor's responsibility (see [`Self::trigger_depth_limit`]).
    pub fn set_trigger_depth_limit(&mut self, value: i64) {
        self.set_session_variable("TRIGGER_DEPTH_LIMIT", vibesql_types::SqlValue::Integer(value));
    }

    // ============================================================================
    // Foreign Key Enforcement (SQLite Compatibility)
    // ============================================================================

    /// Get the foreign_keys PRAGMA setting
    ///
    /// When OFF, foreign key constraints are not enforced.
    /// SQLite defaults to OFF; VibeSQL defaults to OFF for compatibility.
    pub fn foreign_keys_enabled(&self) -> bool {
        match self.get_session_variable("FOREIGN_KEYS") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (SQLite compatibility)
        }
    }

    /// Set the foreign_keys PRAGMA setting
    pub fn set_foreign_keys_enabled(&mut self, value: bool) {
        self.set_session_variable(
            "FOREIGN_KEYS",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the writable_schema PRAGMA setting (SQLite compatibility).
    ///
    /// When ON, `UPDATE sqlite_master/sqlite_schema SET sql = ...` is allowed
    /// to rewrite the stored `CREATE TABLE` source text of schema objects (see
    /// `vibesql-executor::sqlite_schema::execute_sqlite_schema_update`).
    /// Defaults to OFF, matching SQLite; when OFF, all writes to the schema
    /// tables are rejected with "table sqlite_master may not be modified".
    pub fn writable_schema(&self) -> bool {
        match self.get_session_variable("WRITABLE_SCHEMA") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (SQLite compatibility)
        }
    }

    /// Set the writable_schema PRAGMA setting (SQLite compatibility).
    pub fn set_writable_schema(&mut self, value: bool) {
        self.set_session_variable(
            "WRITABLE_SCHEMA",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the defer_foreign_keys PRAGMA setting (SQLite compatibility).
    ///
    /// When ON, enforcement of all foreign key constraints is delayed until
    /// the outermost transaction is committed. Per SQLite, this pragma
    /// defaults to OFF and is automatically reset to OFF at every COMMIT or
    /// ROLLBACK (see `fkey6-1.10.1`).
    ///
    /// **Phase C1 of #5085**: this method only stores/returns the flag. The
    /// runtime change (queueing FK violations until commit) lands in Phase C2.
    pub fn defer_foreign_keys(&self) -> bool {
        match self.get_session_variable("DEFER_FOREIGN_KEYS") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (SQLite compatibility)
        }
    }

    /// Set the defer_foreign_keys PRAGMA setting (SQLite compatibility).
    pub fn set_defer_foreign_keys(&mut self, value: bool) {
        self.set_session_variable(
            "DEFER_FOREIGN_KEYS",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    /// Get the ignore_check_constraints PRAGMA setting (SQLite compatibility).
    ///
    /// When ON, CHECK constraints are not enforced by INSERT/UPDATE (they are
    /// silently skipped rather than raising "CHECK constraint failed"). This
    /// is intended only for loading a schema/data set that is already known to
    /// violate a CHECK constraint (e.g. to repair it afterwards) — SQLite's own
    /// docs discourage general use. Defaults to OFF, matching SQLite (`check.test`
    /// check-4.8/4.8.1).
    pub fn ignore_check_constraints(&self) -> bool {
        match self.get_session_variable("IGNORE_CHECK_CONSTRAINTS") {
            Some(vibesql_types::SqlValue::Integer(n)) => *n != 0,
            _ => false, // Default: OFF (SQLite compatibility)
        }
    }

    /// Set the ignore_check_constraints PRAGMA setting (SQLite compatibility).
    pub fn set_ignore_check_constraints(&mut self, value: bool) {
        self.set_session_variable(
            "IGNORE_CHECK_CONSTRAINTS",
            vibesql_types::SqlValue::Integer(if value { 1 } else { 0 }),
        );
    }

    // ============================================================================
    // SQLite stat1 Storage (SQLite Compatibility)
    // ============================================================================

    /// Insert a sqlite_stat1 entry
    ///
    /// This allows manual insertion of statistics for query optimizer tuning,
    /// matching SQLite's behavior where users can INSERT INTO sqlite_stat1.
    pub fn insert_sqlite_stat1(
        &mut self,
        table_name: String,
        index_name: Option<String>,
        stat: String,
    ) {
        self.metadata.insert_sqlite_stat1(table_name, index_name, stat);
    }

    /// Get a sqlite_stat1 entry
    pub fn get_sqlite_stat1(&self, table_name: &str, index_name: Option<&str>) -> Option<&String> {
        self.metadata.get_sqlite_stat1(table_name, index_name)
    }

    /// Get all sqlite_stat1 entries
    pub fn get_all_sqlite_stat1(
        &self,
    ) -> &std::collections::HashMap<(String, Option<String>), String> {
        self.metadata.get_all_sqlite_stat1()
    }

    /// Delete a sqlite_stat1 entry
    pub fn delete_sqlite_stat1(&mut self, table_name: &str, index_name: Option<&str>) {
        self.metadata.delete_sqlite_stat1(table_name, index_name);
    }

    /// Clear all sqlite_stat1 entries
    pub fn clear_sqlite_stat1(&mut self) {
        self.metadata.clear_sqlite_stat1();
    }

    // ============================================================================
    // Reserved Rowids (SQLite REPLACE semantics)
    // ============================================================================

    /// Reserve a rowid for a table during REPLACE operations
    ///
    /// During REPLACE INTO, SQLite allocates the rowid for the new row BEFORE
    /// firing BEFORE DELETE triggers. Any INSERT within those triggers that
    /// tries to allocate the same rowid will fail with a UNIQUE constraint
    /// violation on rowid.
    ///
    /// # Arguments
    /// * `table_name` - The table name (case-insensitive)
    /// * `rowid` - The rowid to reserve
    /// * `is_explicit` - True if the rowid comes from an explicit INTEGER PRIMARY KEY value, false
    ///   if it's auto-allocated. This affects how conflicts are handled in AFTER DELETE triggers.
    pub fn reserve_rowid(&mut self, table_name: &str, rowid: u64, is_explicit: bool) {
        self.reserved_rowids.insert(table_name.to_lowercase(), (rowid, is_explicit));
    }

    /// Release a reserved rowid after REPLACE completes
    pub fn release_reserved_rowid(&mut self, table_name: &str) {
        self.reserved_rowids.remove(&table_name.to_lowercase());
    }

    /// Check if a rowid is reserved for a table and get the reservation details
    ///
    /// Returns Some((rowid, is_explicit)) if a rowid is reserved, None otherwise.
    pub fn get_reserved_rowid_info(&self, table_name: &str) -> Option<(u64, bool)> {
        self.reserved_rowids.get(&table_name.to_lowercase()).copied()
    }

    /// Check if a rowid is reserved for a table
    pub fn is_rowid_reserved(&self, table_name: &str, rowid: u64) -> bool {
        self.reserved_rowids
            .get(&table_name.to_lowercase())
            .map(|(r, _)| *r == rowid)
            .unwrap_or(false)
    }

    /// Get the reserved rowid for a table, if any
    pub fn get_reserved_rowid(&self, table_name: &str) -> Option<u64> {
        self.reserved_rowids.get(&table_name.to_lowercase()).map(|(r, _)| *r)
    }

    // ============================================================================
    // SQL Mode
    // ============================================================================

    /// Set the SQL compatibility mode at runtime
    ///
    /// This allows changing the SQL dialect (MySQL, SQLite, etc.) during a session.
    /// The `@@sql_mode` session variable is automatically updated to reflect the change.
    ///
    /// # Example
    /// ```rust
    /// use vibesql_storage::Database;
    /// use vibesql_types::{MySqlModeFlags, SqlMode};
    ///
    /// let mut db = Database::new();
    /// // Default is MySQL (for SQLLogicTest compatibility)
    /// assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    ///
    /// db.set_sql_mode(SqlMode::SQLite);
    /// assert!(matches!(db.sql_mode(), SqlMode::SQLite));
    /// ```
    pub fn set_sql_mode(&mut self, mode: vibesql_types::SqlMode) {
        self.sql_mode = mode.clone();

        // Update the @@sql_mode session variable to reflect the new mode
        let mode_string = match &mode {
            vibesql_types::SqlMode::MySQL { flags } => {
                // Build MySQL mode string from flags
                let mut modes = Vec::new();
                if flags.strict_mode {
                    modes.push("STRICT_TRANS_TABLES");
                }
                if flags.pipes_as_concat {
                    modes.push("PIPES_AS_CONCAT");
                }
                if flags.ansi_quotes {
                    modes.push("ANSI_QUOTES");
                }
                // Add common MySQL defaults if no specific flags are set
                if modes.is_empty() {
                    "NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
                        .to_string()
                } else {
                    modes.join(",")
                }
            }
            vibesql_types::SqlMode::SQLite => "SQLITE".to_string(),
        };

        self.metadata.set_session_variable(
            "SQL_MODE",
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(mode_string.as_str())),
        );
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

    use super::*;

    #[test]
    fn test_set_sql_mode_changes_mode() {
        let mut db = Database::new();

        // Default is MySQL (for SQLLogicTest compatibility - dolthub corpus was regenerated against
        // MySQL 8.x)
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Change to SQLite
        db.set_sql_mode(SqlMode::SQLite);
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Change back to MySQL
        db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_set_sql_mode_updates_session_variable() {
        let mut db = Database::new();

        // Set to MySQL mode
        db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });

        // Check session variable reflects the change
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            // Default MySQL flags should include common MySQL defaults
            assert!(
                mode_str.contains("NO_ZERO_IN_DATE") || mode_str.contains("NO_ENGINE_SUBSTITUTION")
            );
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_set_sql_mode_mysql_with_flags() {
        let mut db = Database::new();

        // Set MySQL with specific flags
        db.set_sql_mode(SqlMode::MySQL {
            flags: MySqlModeFlags {
                pipes_as_concat: true,
                ansi_quotes: true,
                strict_mode: true,
                ..Default::default()
            },
        });

        // Check session variable contains the flags
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            assert!(mode_str.contains("STRICT_TRANS_TABLES"));
            assert!(mode_str.contains("PIPES_AS_CONCAT"));
            assert!(mode_str.contains("ANSI_QUOTES"));
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_set_sql_mode_mysql_default_flags() {
        let mut db = Database::new();

        // Set MySQL with default flags (all false)
        db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });

        // Check session variable has default MySQL modes
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            // Default should include common MySQL defaults
            assert!(
                mode_str.contains("NO_ZERO_IN_DATE") || mode_str.contains("NO_ENGINE_SUBSTITUTION")
            );
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_sql_mode_affects_subsequent_queries() {
        let mut db = Database::new();

        // Start in MySQL mode (default)
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Switch to SQLite
        db.set_sql_mode(SqlMode::SQLite);

        // Verify the mode changed
        let mode = db.sql_mode();
        assert!(matches!(mode, SqlMode::SQLite));
    }
}
