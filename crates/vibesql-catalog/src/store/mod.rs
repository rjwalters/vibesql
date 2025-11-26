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

/// Database catalog - manages all schemas and their objects.
#[derive(Debug, Clone)]
pub struct Catalog {
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
    pub(crate) indexes: HashMap<String, IndexMetadata>,
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
    /// Create a new empty catalog.
    pub fn new() -> Self {
        let mut catalog = Catalog {
            schemas: HashMap::new(),
            current_schema: "public".to_string(),
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
            indexes: HashMap::new(),
            // Session defaults (SQL:1999)
            current_catalog: None,
            current_charset: "UTF8".to_string(),
            current_collation: None,
            current_timezone: "UTC".to_string(),
            // Default to case-sensitive identifiers (SQL:1999 compliant)
            // The parser already normalizes unquoted identifiers to uppercase
            // and preserves case for delimited identifiers, so we must use
            // case-sensitive lookups to respect the parser's normalization
            case_sensitive_identifiers: true,
        };

        // Create the default "public" schema
        catalog.schemas.insert("public".to_string(), Schema::new("public".to_string()));

        catalog
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
            identifier.to_uppercase()
        }
    }

    /// Get a schema by name with case-insensitive lookup (if configured)
    pub(crate) fn get_schema_case_insensitive(&self, schema_name: &str) -> Option<&crate::Schema> {
        if self.case_sensitive_identifiers {
            // Case-sensitive: direct lookup
            self.schemas.get(schema_name)
        } else {
            // Case-insensitive: find schema by comparing normalized names
            let normalized_name = schema_name.to_uppercase();
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
