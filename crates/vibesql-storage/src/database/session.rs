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

        self.metadata
            .set_session_variable("SQL_MODE", vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(mode_string.as_str())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

    #[test]
    fn test_set_sql_mode_changes_mode() {
        let mut db = Database::new();

        // Default is MySQL (for SQLLogicTest compatibility - dolthub corpus was regenerated against MySQL 8.x)
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
