//! Database catalog store - manages all schemas and their objects.
//!
//! This module provides the main `Catalog` struct and is organized into
//! submodules by responsibility:
//!
//! - `tables` - Table creation, modification, deletion operations
//! - `schemas` - Schema management operations
//! - `privileges` - Privilege and grant management
//! - `session` - Session configuration (SQL:1999)
//! - `advanced` - Advanced SQL objects (types, domains, sequences, views, triggers, etc.)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use indexmap::IndexMap;

use crate::{
    advanced_objects::{
        Assertion, CharacterSet, Collation, Function, Procedure, Sequence, Translation,
    },
    domain::DomainDefinition,
    index::IndexMetadata,
    privilege::PrivilegeGrant,
    schema::Schema,
    trigger::TriggerDefinition,
    type_definition::TypeDefinition,
    view::ViewDefinition,
};

// Submodules - each handles a specific area of catalog operations
mod advanced;
mod indexes;
mod privileges;
mod schemas;
mod session;
mod tables;

// Re-export types from submodules
pub use advanced::ViewDropBehavior;

/// Global counter for generating unique session IDs.
/// This ensures each Catalog instance gets a unique session ID,
/// even if created in the same process.
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Database catalog - manages all schemas and their objects.
#[derive(Debug, Clone)]
pub struct Catalog {
    /// Session ID for temp table isolation.
    /// Each Catalog instance gets a unique session ID, and temp tables
    /// are stored in a session-specific schema (e.g., "temp_12345").
    /// This ensures temp tables are isolated between database connections.
    pub(crate) session_id: u64,
    /// The name of this session's temp schema (e.g., "temp_12345").
    /// Computed once on Catalog creation from the session_id.
    pub(crate) temp_schema_name: String,
    pub(crate) schemas: HashMap<String, Schema>,
    pub(crate) current_schema: String,
    pub(crate) privilege_grants: Vec<PrivilegeGrant>,
    pub(crate) roles: HashSet<String>,
    // Advanced SQL:1999 objects
    pub(crate) domains: HashMap<String, DomainDefinition>,
    pub(crate) sequences: HashMap<String, Sequence>,
    pub(crate) type_definitions: HashMap<String, TypeDefinition>,
    pub(crate) collations: HashMap<String, Collation>,
    pub(crate) character_sets: HashMap<String, CharacterSet>,
    pub(crate) translations: HashMap<String, Translation>,
    pub(crate) views: HashMap<String, ViewDefinition>,
    pub(crate) triggers: HashMap<String, TriggerDefinition>,
    pub(crate) assertions: HashMap<String, Assertion>,
    pub(crate) functions: HashMap<String, Function>,
    pub(crate) procedures: HashMap<String, Procedure>,
    // Index metadata (maps qualified name "table.index" -> IndexMetadata)
    // Uses IndexMap to preserve creation order for sqlite_master compatibility
    pub(crate) indexes: IndexMap<String, IndexMetadata>,
    // Session state (SQL:1999 session configuration)
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_charset: String,
    pub(crate) current_collation: Option<String>,
    pub(crate) current_timezone: String,
    // Configuration for case-sensitive identifier lookups
    /// When true, identifier lookups are case-sensitive (SQL standard).
    /// When false (default), identifier lookups are case-insensitive (MySQL compatible).
    pub(crate) case_sensitive_identifiers: bool,
}

impl Catalog {
    /// Create a new empty catalog with a unique session ID.
    ///
    /// Each catalog instance gets a unique session ID, which is used to create
    /// a session-specific temp schema for temporary table isolation.
    /// This ensures temporary tables are isolated between database connections,
    /// matching SQLite's behavior where temp tables are connection-local.
    pub fn new() -> Self {
        // Generate unique session ID
        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        let temp_schema_name = format!("{}_{}", crate::TEMP_SCHEMA, session_id);

        let mut catalog = Catalog {
            session_id,
            temp_schema_name: temp_schema_name.clone(),
            schemas: HashMap::new(),
            current_schema: crate::DEFAULT_SCHEMA.to_string(),
            privilege_grants: Vec::new(),
            roles: HashSet::new(),
            domains: HashMap::new(),
            sequences: HashMap::new(),
            type_definitions: HashMap::new(),
            collations: HashMap::new(),
            character_sets: HashMap::new(),
            translations: HashMap::new(),
            views: HashMap::new(),
            triggers: HashMap::new(),
            assertions: HashMap::new(),
            functions: HashMap::new(),
            procedures: HashMap::new(),
            indexes: IndexMap::new(),
            // Session defaults (SQL:1999)
            current_catalog: None,
            current_charset: "UTF8".to_string(),
            current_collation: None,
            current_timezone: "UTC".to_string(),
            // Default to case-insensitive identifiers (SQLite-compatible)
            // The parser preserves original case from SQL text. We use case-insensitive
            // mode so lookups work regardless of case in queries.
            case_sensitive_identifiers: false,
        };

        // Create the default schema (SQLite uses "main")
        catalog.schemas.insert(
            crate::DEFAULT_SCHEMA.to_string(),
            Schema::new(crate::DEFAULT_SCHEMA.to_string()),
        );

        // Create the session-specific temp schema for temporary tables
        // Each session gets its own temp schema (e.g., "temp_1", "temp_2", etc.)
        // This provides session isolation for temp tables, matching SQLite semantics
        catalog.schemas.insert(
            temp_schema_name.clone(),
            Schema::new(temp_schema_name),
        );

        catalog
    }

    /// Get this session's temp schema name.
    ///
    /// Returns the session-specific temp schema name (e.g., "temp_12345").
    /// Temporary tables created in this session will be stored in this schema.
    #[inline]
    pub fn temp_schema_name(&self) -> &str {
        &self.temp_schema_name
    }

    /// Get this session's ID.
    ///
    /// Each Catalog instance has a unique session ID used for temp table isolation.
    #[inline]
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Check if a schema name is a temp schema (matches "temp_*" pattern).
    ///
    /// This is used to identify temp schemas for special handling (e.g., not persisting them).
    #[inline]
    pub fn is_temp_schema(schema_name: &str) -> bool {
        schema_name.starts_with(crate::TEMP_SCHEMA) && schema_name.len() > crate::TEMP_SCHEMA.len()
            && schema_name.as_bytes().get(crate::TEMP_SCHEMA.len()) == Some(&b'_')
    }

    /// Set whether identifier lookups should be case-sensitive
    pub fn set_case_sensitive_identifiers(&mut self, case_sensitive: bool) {
        self.case_sensitive_identifiers = case_sensitive;
    }

    /// Check if identifier lookups are case-sensitive
    pub fn is_case_sensitive_identifiers(&self) -> bool {
        self.case_sensitive_identifiers
    }

    /// Normalize an identifier for lookup (applies case folding if case-insensitive mode)
    fn normalize_identifier(&self, identifier: &str) -> String {
        if self.case_sensitive_identifiers {
            identifier.to_string()
        } else {
            identifier.to_lowercase()
        }
    }

    /// Resolve a schema name to the actual internal schema name.
    ///
    /// SQLite Compatibility: The "temp" schema name is mapped to this session's
    /// temp schema (e.g., "temp_123"). This allows users to write `temp.tablename`
    /// syntax while internally temp tables are stored in session-isolated schemas.
    pub(crate) fn resolve_schema_name<'a>(&'a self, schema_name: &'a str) -> &'a str {
        if schema_name.eq_ignore_ascii_case(crate::TEMP_SCHEMA) {
            &self.temp_schema_name
        } else {
            schema_name
        }
    }

    /// Get a schema by name with case-insensitive lookup (if configured).
    ///
    /// SQLite Compatibility: References to the "temp" schema are automatically
    /// redirected to this session's temp schema (e.g., "temp_123"). This allows
    /// users to write `SELECT * FROM temp.t1` while internally temp tables are
    /// stored in session-isolated schemas.
    pub(crate) fn get_schema_case_insensitive(&self, schema_name: &str) -> Option<&crate::Schema> {
        // SQLite compatibility: "temp" schema references map to session's temp schema
        // This enables `temp.tablename` syntax while maintaining session isolation
        let effective_schema_name = if schema_name.eq_ignore_ascii_case(crate::TEMP_SCHEMA) {
            &self.temp_schema_name
        } else {
            schema_name
        };

        if self.case_sensitive_identifiers {
            // Case-sensitive: direct lookup
            self.schemas.get(effective_schema_name)
        } else {
            // Case-insensitive: find schema by comparing normalized names
            let normalized_name = effective_schema_name.to_uppercase();
            self.schemas
                .iter()
                .find(|(key, _)| key.to_uppercase() == normalized_name)
                .map(|(_, schema)| schema)
        }
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
