// ============================================================================
// Schema Metadata Management
// ============================================================================

use std::collections::HashMap;

use vibesql_types::SqlValue;

/// Manages schema metadata and session state
#[derive(Debug, Clone)]
pub struct Metadata {
    /// Session variables (MySQL-style @variables)
    /// Key: normalized variable name (uppercase)
    /// Value: variable value
    session_variables: HashMap<String, SqlValue>,
    /// Cache for parsed procedure and function bodies (Phase 6 performance)
    /// Key: routine name (procedure or function)
    /// Value: cached procedure body
    routine_body_cache: HashMap<String, vibesql_catalog::ProcedureBody>,
    /// Storage for sqlite_stat1 entries (SQLite compatibility)
    /// Key: (table_name, index_name) where index_name is None for table-level stats
    /// Value: stat string (space-separated statistics)
    sqlite_stat1: HashMap<(String, Option<String>), String>,
}

impl Metadata {
    /// Create a new metadata manager
    pub fn new() -> Self {
        let mut session_variables = HashMap::new();

        // Initialize default session variables (MySQL compatibility)
        // sql_mode starts with common modes including ONLY_FULL_GROUP_BY
        // The tests will remove ONLY_FULL_GROUP_BY to allow non-standard GROUP BY queries
        session_variables.insert(
            "SQL_MODE".to_string(),
            SqlValue::Varchar(arcstr::ArcStr::from("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")),
        );

        // version - simple version string
        session_variables.insert(
            "VERSION".to_string(),
            SqlValue::Varchar(arcstr::ArcStr::from("8.0.0-vibesql")),
        );

        // character_set_client - default to utf8mb4
        session_variables.insert(
            "CHARACTER_SET_CLIENT".to_string(),
            SqlValue::Varchar(arcstr::ArcStr::from("utf8mb4")),
        );

        // SQLite PRAGMA defaults for column naming:
        // - short_column_names=ON (default): Use just column name ("f1")
        // - full_column_names=OFF (default): Don't use table.column format
        // When full_column_names=ON, use "table.column" format
        // When both are OFF, use expression text as column name
        session_variables.insert("SHORT_COLUMN_NAMES".to_string(), SqlValue::Integer(1));
        session_variables.insert("FULL_COLUMN_NAMES".to_string(), SqlValue::Integer(0));

        Metadata {
            session_variables,
            routine_body_cache: HashMap::new(),
            sqlite_stat1: HashMap::new(),
        }
    }

    // ============================================================================
    // Session Variables
    // ============================================================================

    /// Set a session variable (MySQL-style @variable)
    pub fn set_session_variable(&mut self, name: &str, value: SqlValue) {
        let normalized_name = name.to_uppercase();
        self.session_variables.insert(normalized_name, value);
    }

    /// Get a session variable value
    pub fn get_session_variable(&self, name: &str) -> Option<&SqlValue> {
        let normalized_name = name.to_uppercase();
        self.session_variables.get(&normalized_name)
    }

    /// Clear all session variables
    pub fn clear_session_variables(&mut self) {
        self.session_variables.clear();
    }

    // ============================================================================
    // Procedure/Function Body Cache Methods (Phase 6 Performance)
    // ============================================================================

    /// Cache a procedure body
    pub fn cache_procedure_body(&mut self, name: String, body: vibesql_catalog::ProcedureBody) {
        self.routine_body_cache.insert(name, body);
    }

    /// Get cached procedure body
    pub fn get_cached_procedure_body(&self, name: &str) -> Option<&vibesql_catalog::ProcedureBody> {
        self.routine_body_cache.get(name)
    }

    /// Invalidate cached procedure body (call when procedure is dropped or replaced)
    pub fn invalidate_procedure_cache(&mut self, name: &str) {
        self.routine_body_cache.remove(name);
    }

    /// Clear all cached procedure/function bodies
    pub fn clear_routine_cache(&mut self) {
        self.routine_body_cache.clear();
    }

    // ============================================================================
    // SQLite stat1 Storage (SQLite Compatibility)
    // ============================================================================

    /// Insert a sqlite_stat1 entry
    pub fn insert_sqlite_stat1(
        &mut self,
        table_name: String,
        index_name: Option<String>,
        stat: String,
    ) {
        self.sqlite_stat1.insert((table_name, index_name), stat);
    }

    /// Get a sqlite_stat1 entry
    pub fn get_sqlite_stat1(
        &self,
        table_name: &str,
        index_name: Option<&str>,
    ) -> Option<&String> {
        self.sqlite_stat1
            .get(&(table_name.to_string(), index_name.map(|s| s.to_string())))
    }

    /// Get all sqlite_stat1 entries
    pub fn get_all_sqlite_stat1(&self) -> &HashMap<(String, Option<String>), String> {
        &self.sqlite_stat1
    }

    /// Delete a sqlite_stat1 entry
    pub fn delete_sqlite_stat1(&mut self, table_name: &str, index_name: Option<&str>) {
        self.sqlite_stat1.remove(&(table_name.to_string(), index_name.map(|s| s.to_string())));
    }

    /// Clear all sqlite_stat1 entries
    pub fn clear_sqlite_stat1(&mut self) {
        self.sqlite_stat1.clear();
    }

    /// Check if sqlite_stat1 has any entries
    pub fn sqlite_stat1_is_empty(&self) -> bool {
        self.sqlite_stat1.is_empty()
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}
