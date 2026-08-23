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
    cell::RefCell,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
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

thread_local! {
    /// Current unqualified-resolution schema restriction (trigger-body /
    /// view-body execution, #6477/#6485) — thread-local rather than a field
    /// on `Catalog` so concurrent query execution across OS threads (e.g.
    /// `SharedDatabase`'s concurrent readers, `crates/vibesql-executor/src/readonly.rs`,
    /// which the HTTP server drives one-per-request against a single shared
    /// `Database`) can never race on it.
    ///
    /// This replaced a `RwLock<Option<String>>` *field* on `Catalog`, which
    /// was `Sync` at the type level but still shared, catalog-global mutable
    /// state: two concurrent view-body executions on the same `Database`
    /// interleaved their set-then-restore pairs and could leave the
    /// restriction permanently stuck on one schema, corrupting every
    /// subsequent unrelated query on that database (PR #6506 review — the
    /// reviewer reproduced `errors=119999` / `stuck_restriction=Some("main")`
    /// with 8 threads). Making the restriction thread-local removes the
    /// sharing entirely: a single query's execution runs synchronously to
    /// completion on the thread that set the restriction (the executor has no
    /// `.await` points inside a query, so no other query can interleave its
    /// own set/restore in between), so scoping by thread confines the
    /// restriction to exactly the call stack that established it — no shared
    /// mutable state, and therefore no lock.
    ///
    /// Always establish it through
    /// [`Catalog::scoped_unqualified_resolution_restriction`], whose guard
    /// restores the previous value on drop. Because query threads are pooled
    /// and long-lived, a restriction leaked by a skipped restore (an early
    /// `?` return or a panic) would otherwise silently mis-resolve every
    /// later, unrelated query that happens to land on the same thread —
    /// the thread-scoped version of the very bug this replaced.
    static RESTRICT_UNQUALIFIED_RESOLUTION_TO_SCHEMA: RefCell<Option<String>> =
        const { RefCell::new(None) };
}

/// RAII guard for a scoped unqualified-resolution restriction.
///
/// Created by [`Catalog::scoped_unqualified_resolution_restriction`]. The
/// restriction is active for exactly as long as the guard is alive, and the
/// previously-active restriction is restored when it drops — including on an
/// early `?` return or a panic unwinding through the scope. Nested guards
/// therefore behave like a proper stack (an inner trigger/view body restores
/// the outer body's restriction, not `None`).
///
/// Deliberately **not** `Send`: the value it restores lives in the
/// thread-local of the thread that created it, so dropping it on a different
/// thread would write the restore into the wrong thread's state and leave the
/// originating thread stuck.
#[derive(Debug)]
#[must_use = "the restriction is only active while the guard is alive; binding it to `_` drops it \
              immediately"]
pub struct UnqualifiedResolutionRestrictionGuard {
    /// The restriction that was active when this guard was created, restored
    /// on drop.
    previous: Option<String>,
    _not_send: PhantomData<*const ()>,
}

impl Drop for UnqualifiedResolutionRestrictionGuard {
    fn drop(&mut self) {
        // `try_with` rather than `with`: during thread teardown the
        // thread-local may already be destroyed, and `with` would panic
        // there. A destroyed thread-local has nothing left to corrupt, so
        // silently skipping the restore is correct.
        let _ = RESTRICT_UNQUALIFIED_RESOLUTION_TO_SCHEMA
            .try_with(|cell| *cell.borrow_mut() = self.previous.take());
    }
}

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
    ///
    /// Note this is thread-local state, not per-`Catalog` state — `&self` is
    /// accepted (rather than `&mut self`) purely so this can be called from
    /// the read-only view-body-at-query-time path, which only holds
    /// `&Database`. See the `RESTRICT_UNQUALIFIED_RESOLUTION_TO_SCHEMA`
    /// thread-local for why it is not a field on `Catalog`.
    ///
    /// Prefer [`Self::scoped_unqualified_resolution_restriction`], which
    /// restores the previous value automatically; this raw setter exists for
    /// the trigger-body path, whose restore is interleaved with other
    /// bookkeeping it must undo in the same order.
    pub fn set_restrict_unqualified_resolution_to_schema(
        &self,
        schema: Option<String>,
    ) -> Option<String> {
        RESTRICT_UNQUALIFIED_RESOLUTION_TO_SCHEMA
            .with(|cell| std::mem::replace(&mut *cell.borrow_mut(), schema))
    }

    /// Restrict unqualified table-name resolution for the lifetime of the
    /// returned guard, restoring the previously-active restriction when it
    /// drops.
    ///
    /// This is the safe form of
    /// [`Self::set_restrict_unqualified_resolution_to_schema`]: the restore
    /// cannot be skipped by an early `?` return or a panic, which matters
    /// because query threads are pooled — a leaked restriction would
    /// mis-resolve later, unrelated queries on the same thread (#6506).
    pub fn scoped_unqualified_resolution_restriction(
        &self,
        schema: Option<String>,
    ) -> UnqualifiedResolutionRestrictionGuard {
        let previous = self.set_restrict_unqualified_resolution_to_schema(schema);
        UnqualifiedResolutionRestrictionGuard { previous, _not_send: PhantomData }
    }

    /// The schema unqualified table-name resolution is currently restricted
    /// to, if any (see [`Self::set_restrict_unqualified_resolution_to_schema`]).
    pub fn unqualified_resolution_restricted_to(&self) -> Option<String> {
        RESTRICT_UNQUALIFIED_RESOLUTION_TO_SCHEMA.with(|cell| cell.borrow().clone())
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
