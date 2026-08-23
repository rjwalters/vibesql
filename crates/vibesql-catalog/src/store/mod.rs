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

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

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
mod attachments;
mod indexes;
mod privileges;
mod schemas;
mod session;
mod tables;

// Re-export types from submodules
pub use advanced::ViewDropBehavior;
pub use attachments::{AttachedDatabase, MAX_ATTACHED_DATABASES};

/// Global counter for generating unique session IDs.
/// This ensures each Catalog instance gets a unique session ID,
/// even if created in the same process.
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Database catalog - manages all schemas and their objects.
///
/// `Clone` is implemented manually (below) rather than derived: the
/// `restrict_unqualified_resolution_to_schema` field uses `RwLock` for
/// interior mutability (see its field doc), and `RwLock` does not implement
/// `Clone` — a manual impl deep-clones its current value into a fresh lock
/// instead, which also correctly gives each clone independent, unshared
/// restriction state (as opposed to `Arc<RwLock<_>>`, which would let two
/// unrelated `Catalog` clones interfere with each other's transient
/// resolution-restriction toggling).
#[derive(Debug)]
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
    /// Session-scoped registry of ATTACHed databases in attachment order
    /// (SQLite `ATTACH DATABASE`, Phase 1 — #6310). Each entry has a matching
    /// schema in `schemas`. Never persisted; see `store/attachments.rs`.
    pub(crate) attached_databases: Vec<attachments::AttachedDatabase>,
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
    /// When set, unqualified table-name resolution is restricted to exactly
    /// this internal schema name, bypassing the ordinary
    /// temp -> current -> attached search order entirely. This models
    /// SQLite's rule that an unqualified table name inside a trigger body
    /// resolves *only* within the schema that owns the trigger, never
    /// falling back to another schema: a `main` trigger cannot see a
    /// same-named TEMP table (trigger1-3.2..3.5), a `temp` trigger keeps its
    /// normal temp-first resolution (the value here is the session's own
    /// temp schema name), and — the fix for #6477 — a trigger owned by an
    /// ATTACHed schema resolves its body's unqualified names only within
    /// that attachment, never `main` or any other attachment. It is toggled
    /// on only for the duration of a trigger body's execution and restored
    /// afterward (nested trigger bodies restore the prior value on unwind).
    /// Default `None` (full search order) everywhere else.
    ///
    /// `RwLock`-wrapped (rather than a plain field mutated via `&mut self`,
    /// or `RefCell`) because a view body is re-executed from deep inside an
    /// already in-flight, read-only `SelectExecutor` (which holds only
    /// `&Database`, see `select/scan/table.rs`) — there is no `&mut Database`
    /// available at that point to toggle the restriction the way
    /// trigger-body execution does. Interior mutability lets
    /// `Catalog::set_restrict_unqualified_resolution_to_schema` be called
    /// from a `&self` (or `&mut self`, which still auto-derefs) context
    /// either way (#6485). `RwLock` rather than `RefCell` specifically: a
    /// `&Database` also crosses real thread boundaries in parallel query
    /// execution (rayon scans/joins/window functions), which requires
    /// `Database: Sync` — `RefCell` is not `Sync` (even for reads, since its
    /// internal borrow-count isn't atomic) and would poison that bound for
    /// the whole `Catalog`/`Database` tree; `RwLock` is `Sync`. Reads (the
    /// hot path, in `store/tables.rs`) use `.read()`; only the rare
    /// set/restore around trigger and view-body execution takes `.write()`.
    pub(crate) restrict_unqualified_resolution_to_schema: RwLock<Option<String>>,
    /// Monotonic creation-order sequence for schema objects (tables, indexes,
    /// views, triggers), keyed by `"{schema}\u{1}{name}"` (both lowercased).
    ///
    /// SQLite lists objects in `sqlite_master` in the order their rows were
    /// inserted into the schema table — i.e. object *creation* order, with a
    /// table's indexes appearing right after the table when they were created
    /// next (see pragma.test 23.1). VibeSQL stores tables and indexes in
    /// separate collections, so this side-map records a global creation ordinal
    /// used only to order `sqlite_master`/`sqlite_schema` output. It IS
    /// persisted (binary catalog format v17+, see
    /// `vibesql_storage::persistence::binary::catalog`) so the ordering survives
    /// a reload; an object with no recorded ordinal (e.g. a v16-and-earlier file,
    /// or an object created via a path that doesn't record one) falls back to
    /// the historical "tables first, then indexes" emission order, so nothing
    /// regresses.
    pub(crate) creation_seq: HashMap<String, u64>,
    /// Next value to hand out from [`Catalog::record_creation_seq`].
    pub(crate) next_creation_seq: u64,
    /// Sticky flag: has this session's temp database ever been touched by
    /// creating a temp table, view, or trigger?
    ///
    /// Real sqlite3 (verified against 3.51.0) lazily attaches the `temp`
    /// database on first use and then keeps it attached — and reported by
    /// `PRAGMA database_list` — for the rest of the connection's lifetime,
    /// even after every temp object created in it has since been dropped
    /// (`CREATE TEMP TABLE t1(...); DROP TABLE temp.t1;` still reports a
    /// `temp` row afterward). A plain "does the temp schema currently have
    /// any objects" check (as `has_temp_objects`'s doc used to describe)
    /// would flip back to false after a drop and disagree with sqlite3 — see
    /// e_createtable-1.3..1.6 (#6406), which create then drop temp objects
    /// across a test group and still expect `X(temp)` to be present (as an
    /// empty list) in every subsequent `table_list` snapshot. Set once, in
    /// [`Catalog::record_creation_seq`], and never cleared; never persisted
    /// (temp objects themselves are session-only and never persisted either).
    pub(crate) temp_touched: bool,
}

impl Clone for Catalog {
    fn clone(&self) -> Self {
        Catalog {
            session_id: self.session_id,
            temp_schema_name: self.temp_schema_name.clone(),
            schemas: self.schemas.clone(),
            attached_databases: self.attached_databases.clone(),
            current_schema: self.current_schema.clone(),
            privilege_grants: self.privilege_grants.clone(),
            roles: self.roles.clone(),
            domains: self.domains.clone(),
            sequences: self.sequences.clone(),
            type_definitions: self.type_definitions.clone(),
            collations: self.collations.clone(),
            character_sets: self.character_sets.clone(),
            translations: self.translations.clone(),
            views: self.views.clone(),
            triggers: self.triggers.clone(),
            assertions: self.assertions.clone(),
            functions: self.functions.clone(),
            procedures: self.procedures.clone(),
            indexes: self.indexes.clone(),
            current_catalog: self.current_catalog.clone(),
            current_charset: self.current_charset.clone(),
            current_collation: self.current_collation.clone(),
            current_timezone: self.current_timezone.clone(),
            case_sensitive_identifiers: self.case_sensitive_identifiers,
            // `RwLock` doesn't implement `Clone`; deep-clone its current
            // value into a fresh, independent lock (see the field doc and
            // the struct-level doc comment above for why this must not be
            // shared, e.g. via `Arc<RwLock<_>>`, across clones).
            restrict_unqualified_resolution_to_schema: RwLock::new(
                self.restrict_unqualified_resolution_to_schema
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            ),
            creation_seq: self.creation_seq.clone(),
            next_creation_seq: self.next_creation_seq,
            temp_touched: self.temp_touched,
        }
    }
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
            attached_databases: Vec::new(),
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
            // Full search order is active by default; only a trigger body's
            // execution restricts it (see field docs).
            restrict_unqualified_resolution_to_schema: RwLock::new(None),
            creation_seq: HashMap::new(),
            next_creation_seq: 0,
            temp_touched: false,
        };

        // Create the default schema (SQLite uses "main")
        catalog.schemas.insert(
            crate::DEFAULT_SCHEMA.to_string(),
            Schema::new(crate::DEFAULT_SCHEMA.to_string()),
        );

        // Create the session-specific temp schema for temporary tables
        // Each session gets its own temp schema (e.g., "temp_1", "temp_2", etc.)
        // This provides session isolation for temp tables, matching SQLite semantics
        catalog.schemas.insert(temp_schema_name.clone(), Schema::new(temp_schema_name));

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

    /// Returns true if this session has ever materialized a temporary object
    /// (temp table, temp view, or temp trigger) — even if it has since been
    /// dropped.
    ///
    /// SQLite lazily attaches the `temp` database the first time a temp object
    /// is created, then keeps it attached (and reported by `PRAGMA
    /// database_list`) for the rest of the connection's lifetime regardless of
    /// whether temp objects still exist (verified against sqlite3 3.51.0:
    /// `CREATE TEMP TABLE t1(...); DROP TABLE temp.t1;` still reports a `temp`
    /// row afterward — see #6406). This reads the sticky `temp_touched`
    /// session flag (set once and never cleared, see its field doc), not a
    /// live "does the temp schema currently have contents" check.
    pub fn has_temp_objects(&self) -> bool {
        self.temp_touched
    }

    /// Check if a schema name is a temp schema (matches "temp_*" pattern).
    ///
    /// This is used to identify temp schemas for special handling (e.g., not persisting them).
    #[inline]
    pub fn is_temp_schema(schema_name: &str) -> bool {
        schema_name.starts_with(crate::TEMP_SCHEMA)
            && schema_name.len() > crate::TEMP_SCHEMA.len()
            && schema_name.as_bytes().get(crate::TEMP_SCHEMA.len()) == Some(&b'_')
    }

    /// Build the `creation_seq` map key for a schema object.
    fn creation_seq_key(schema: &str, name: &str) -> String {
        format!("{}\u{1}{}", schema.to_lowercase(), name.to_lowercase())
    }

    /// Record (or refresh) the creation ordinal for a schema object.
    ///
    /// Called from the table/index/view/trigger create chokepoints so that
    /// `sqlite_master` can list objects in creation order. Re-creating an object
    /// with the same name overwrites its ordinal with a fresh (later) value,
    /// matching SQLite where a dropped-and-recreated object moves to the end of
    /// the schema table.
    ///
    /// This is also the single chokepoint every temp table/view/trigger
    /// creation passes through, so it doubles as the setter for the sticky
    /// `temp_touched` flag (see its field doc and [`Catalog::has_temp_objects`]).
    /// `schema` is the session-specific temp schema name (e.g. "temp_12345")
    /// for temp tables, or the literal "temp" for temp views/triggers.
    pub fn record_creation_seq(&mut self, schema: &str, name: &str) {
        if !self.temp_touched
            && (schema.eq_ignore_ascii_case(crate::TEMP_SCHEMA) || Self::is_temp_schema(schema))
        {
            self.temp_touched = true;
        }
        let seq = self.next_creation_seq;
        self.next_creation_seq += 1;
        self.creation_seq.insert(Self::creation_seq_key(schema, name), seq);
    }

    /// Look up the creation ordinal for a schema object, if one was recorded.
    ///
    /// Returns `None` for objects created via a path that did not record an
    /// ordinal (e.g. a rename, or an old-format reload that predates the
    /// persisted creation-order section); the `sqlite_master` generator falls
    /// back to its historical "tables first, then indexes" emission order for
    /// those.
    pub fn creation_seq(&self, schema: &str, name: &str) -> Option<u64> {
        self.creation_seq.get(&Self::creation_seq_key(schema, name)).copied()
    }

    /// Iterate the recorded creation ordinals as `(opaque_key, seq)` pairs.
    ///
    /// The key is the internal `creation_seq` map key and is opaque to callers;
    /// it is only meaningful when handed back to [`Catalog::restore_creation_seq`].
    /// Used by binary persistence to round-trip creation order across a reload so
    /// `sqlite_master` keeps SQLite's object-creation ordering (pragma.test 23.1)
    /// even though the reader re-registers tables and indexes in separate passes.
    pub fn creation_seq_entries(&self) -> impl Iterator<Item = (&str, u64)> {
        self.creation_seq.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Restore a creation ordinal from persistence, keeping `next_creation_seq`
    /// past every restored value so objects created after the reload still sort
    /// last. `key` must be one previously produced by
    /// [`Catalog::creation_seq_entries`].
    pub fn restore_creation_seq(&mut self, key: String, seq: u64) {
        self.creation_seq.insert(key, seq);
        if seq >= self.next_creation_seq {
            self.next_creation_seq = seq + 1;
        }
    }

    /// Set whether identifier lookups should be case-sensitive
    pub fn set_case_sensitive_identifiers(&mut self, case_sensitive: bool) {
        self.case_sensitive_identifiers = case_sensitive;
    }

    /// Check if identifier lookups are case-sensitive
    pub fn is_case_sensitive_identifiers(&self) -> bool {
        self.case_sensitive_identifiers
    }

    /// Restrict (or unrestrict) unqualified table-name resolution to a single
    /// named internal schema.
    ///
    /// `Some(schema)` makes unqualified lookups resolve *only* within
    /// `schema`, bypassing the temp -> current -> attached search order
    /// entirely. Used to scope a trigger body's unqualified names to the
    /// trigger's own schema (#6477) — pass `"main"` for a `main` trigger
    /// (matching the earlier main-only "suppress temp shadowing" behavior),
    /// the session's temp schema name for a `temp` trigger (preserving
    /// temp-first resolution), or an attached schema's name for a trigger
    /// owned by that attachment. `None` restores the ordinary search order.
    /// Returns the previous value so the caller can restore it (correct
    /// nesting of trigger bodies).
    pub fn set_restrict_unqualified_resolution_to_schema(
        &self,
        schema: Option<String>,
    ) -> Option<String> {
        // Recover from a poisoned lock (a prior panic while the lock was
        // held) rather than propagating the panic here: this is transient
        // bookkeeping, not data whose integrity we need to protect by
        // refusing further access.
        let mut guard = self
            .restrict_unqualified_resolution_to_schema
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::replace(&mut *guard, schema)
    }

    /// The schema unqualified table-name resolution is currently restricted
    /// to, if any (see [`Self::set_restrict_unqualified_resolution_to_schema`]).
    /// Returns an owned copy (rather than `Option<&str>`) since the value is
    /// held behind an `RwLock`, whose guard cannot outlive this call.
    pub fn unqualified_resolution_restricted_to(&self) -> Option<String> {
        self.restriction_read_guard().clone()
    }

    /// Read guard over the current unqualified-resolution restriction,
    /// recovering from lock poisoning the same way the setter/getter do.
    /// Internal helper shared by `store/tables.rs`'s several restriction
    /// checks so each call site doesn't repeat the poison-recovery
    /// boilerplate.
    pub(crate) fn restriction_read_guard(&self) -> std::sync::RwLockReadGuard<'_, Option<String>> {
        self.restrict_unqualified_resolution_to_schema
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Resolve a schema name written by the user to the **canonical** internal
    /// schema name it refers to.
    ///
    /// Schema qualifiers are kept verbatim as written (e.g. the parser stores
    /// `CREATE TRIGGER MAIN.tr`'s qualifier as `"MAIN"`) precisely because
    /// schema comparisons downstream are case-insensitive. Anything that uses
    /// a schema name as a **lookup key** rather than a comparison operand must
    /// canonicalize it first — notably the storage layer, whose `tables` map is
    /// keyed by `"<canonical schema>.<table>"` (see #6477's storage mirror).
    ///
    /// `"temp"` (in any case) resolves to this session's temp schema name;
    /// every other name is matched case-insensitively against the known
    /// schemas unless case-sensitive identifiers are enabled. A name matching
    /// no known schema is returned unchanged (after temp resolution) so the
    /// caller still reports the user's own spelling.
    pub fn canonical_schema_name(&self, schema_name: &str) -> String {
        let effective_schema_name = if schema_name.eq_ignore_ascii_case(crate::TEMP_SCHEMA) {
            self.temp_schema_name.clone()
        } else {
            schema_name.to_string()
        };

        if self.case_sensitive_identifiers {
            return effective_schema_name;
        }

        self.schemas
            .keys()
            .find(|key| key.eq_ignore_ascii_case(&effective_schema_name))
            .cloned()
            .unwrap_or(effective_schema_name)
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

    /// Mutable counterpart to `get_schema_case_insensitive`.
    pub(crate) fn get_schema_case_insensitive_mut(
        &mut self,
        schema_name: &str,
    ) -> Option<&mut crate::Schema> {
        let effective_schema_name = if schema_name.eq_ignore_ascii_case(crate::TEMP_SCHEMA) {
            self.temp_schema_name.clone()
        } else {
            schema_name.to_string()
        };

        if self.case_sensitive_identifiers {
            self.schemas.get_mut(&effective_schema_name)
        } else {
            let normalized_name = effective_schema_name.to_uppercase();
            self.schemas
                .iter_mut()
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
