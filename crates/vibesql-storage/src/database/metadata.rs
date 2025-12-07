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
        session_variables
            .insert("VERSION".to_string(), SqlValue::Varchar(arcstr::ArcStr::from("8.0.0-vibesql")));

        // character_set_client - default to utf8mb4
        session_variables
            .insert("CHARACTER_SET_CLIENT".to_string(), SqlValue::Varchar(arcstr::ArcStr::from("utf8mb4")));

        Metadata { session_variables, routine_body_cache: HashMap::new() }
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
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}
