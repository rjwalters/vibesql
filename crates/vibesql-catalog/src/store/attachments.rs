//! Attached-database registry (SQLite `ATTACH DATABASE` support).
//!
//! Phase 1 (#6310) implements *session-scoped* attachments: `ATTACH` creates
//! an ordinary catalog schema plus a registry entry recording the attachment
//! order and the declared file path. The *registry entry itself* is never
//! persisted (a fresh session always starts with no attachments, matching
//! SQLite's per-connection ATTACH semantics), and WAL emission is always
//! suppressed for tables inside an attached schema.
//!
//! Phase 2 (#6362) adds file-backed attachment *contents*: `ATTACH
//! 'file.vbsql' AS aux` where the file exists loads its tables into `aux`
//! (`SqlExecutor::load_attached_schema_from_file` in vibesql-cli), and each
//! attached schema's tables are written back to their own file on
//! `\save`/exit/`DETACH` (`Database::save_attached_schema_sql_dump` in
//! vibesql-storage) — independent of the *registry entry*, which still never
//! persists.
//!
//! Attachment names are stored ASCII-lowercased, matching how SQLite compares
//! database names case-insensitively and how VibeSQL canonicalizes unquoted
//! identifiers.

use crate::{errors::CatalogError, schema::Schema};

/// SQLite's default limit on the number of attached databases.
pub const MAX_ATTACHED_DATABASES: usize = 10;

/// One attached database: its schema name (lowercased) and declared file path.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachedDatabase {
    /// Canonical (ASCII-lowercased) schema name the database is attached as.
    pub name: String,
    /// The filename as written in the ATTACH statement (e.g. `:memory:`).
    pub path: String,
}

impl super::Catalog {
    /// Attach a database under `name` (Phase 1: creates an empty
    /// session-scoped schema and records the attachment).
    ///
    /// The name is canonicalized to ASCII lowercase. Fails with
    /// [`CatalogError::SchemaAlreadyExists`] when the name collides with
    /// `main`, `temp`, an existing schema, or an existing attachment — callers
    /// (the CLI executor) map this to SQLite's
    /// `database <name> is already in use`. Fails with
    /// [`CatalogError::TooManyAttachedDatabases`] beyond
    /// [`MAX_ATTACHED_DATABASES`].
    pub fn attach_database(&mut self, name: &str, path: &str) -> Result<(), CatalogError> {
        let canonical = name.to_ascii_lowercase();

        // Reserved names: `main`, the `temp` alias, and the internal
        // session temp schemas (`temp_<session>`).
        if canonical == crate::DEFAULT_SCHEMA
            || canonical == crate::TEMP_SCHEMA
            || Self::is_temp_schema(&canonical)
            || self.schemas.contains_key(&canonical)
            || self.is_attached_schema(&canonical)
        {
            return Err(CatalogError::SchemaAlreadyExists(name.to_string()));
        }

        if self.attached_databases.len() >= MAX_ATTACHED_DATABASES {
            return Err(CatalogError::TooManyAttachedDatabases);
        }

        self.schemas.insert(canonical.clone(), Schema::new(canonical.clone()));
        self.attached_databases.push(AttachedDatabase { name: canonical, path: path.to_string() });
        Ok(())
    }

    /// Detach a previously attached database, removing its schema and every
    /// schema object registered under it (views, triggers, indexes).
    ///
    /// The caller is responsible for dropping the schema's *tables* first
    /// (storage holds their row data); [`Catalog::attached_table_names`] lists
    /// them. Fails with [`CatalogError::SchemaNotFound`] when `name` is not an
    /// attachment — callers map this to SQLite's `no such database: <name>`.
    pub fn detach_database(&mut self, name: &str) -> Result<(), CatalogError> {
        let canonical = name.to_ascii_lowercase();
        let Some(pos) = self.attached_databases.iter().position(|a| a.name == canonical) else {
            return Err(CatalogError::SchemaNotFound(name.to_string()));
        };

        self.attached_databases.remove(pos);
        self.schemas.remove(&canonical);

        // Remove schema objects that live in catalog-global maps but belong to
        // the detached schema. Views are tagged with their schema
        // (`schema: Some("aux")`, since #6490); the `name` prefix check below
        // is a defensive fallback for any view whose qualified name was
        // stored verbatim by a pre-#6490 build (before views round-tripped
        // through a persisted snapshot get re-tagged on load).
        let name_prefix = format!("{canonical}.");
        self.views.retain(|_, v| {
            !v.schema.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(&canonical))
                && !v.name.to_ascii_lowercase().starts_with(&name_prefix)
        });
        self.triggers.retain(|_, t| {
            !t.schema.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(&canonical))
        });
        self.indexes.retain(|_, idx| !idx.schema().eq_ignore_ascii_case(&canonical));

        Ok(())
    }

    /// Whether `name` (compared case-insensitively) is an attached database
    /// schema.
    pub fn is_attached_schema(&self, name: &str) -> bool {
        self.attached_databases.iter().any(|a| a.name.eq_ignore_ascii_case(name))
    }

    /// The attached databases in attachment order (the unqualified name
    /// resolution order and `PRAGMA database_list` order).
    pub fn attached_databases(&self) -> &[AttachedDatabase] {
        &self.attached_databases
    }

    /// List the table names currently registered in an attached schema
    /// (unqualified). Empty when `name` is not attached.
    pub fn attached_table_names(&self, name: &str) -> Vec<String> {
        let canonical = name.to_ascii_lowercase();
        if !self.is_attached_schema(&canonical) {
            return Vec::new();
        }
        self.schemas.get(&canonical).map(|s| s.list_tables()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{super::Catalog, MAX_ATTACHED_DATABASES};
    use crate::errors::CatalogError;

    #[test]
    fn attach_creates_schema_and_registry_entry() {
        let mut catalog = Catalog::new();
        catalog.attach_database("aux", ":memory:").unwrap();
        assert!(catalog.schema_exists("aux"));
        assert!(catalog.is_attached_schema("aux"));
        assert!(catalog.is_attached_schema("AUX")); // case-insensitive
        assert_eq!(catalog.attached_databases().len(), 1);
        assert_eq!(catalog.attached_databases()[0].name, "aux");
        assert_eq!(catalog.attached_databases()[0].path, ":memory:");
    }

    #[test]
    fn attach_name_is_case_folded() {
        let mut catalog = Catalog::new();
        catalog.attach_database("AuxDB", "file1").unwrap();
        assert!(catalog.schema_exists("auxdb"));
        // A different casing of the same name collides.
        assert!(matches!(
            catalog.attach_database("AUXDB", "file2"),
            Err(CatalogError::SchemaAlreadyExists(_))
        ));
    }

    #[test]
    fn attach_rejects_reserved_and_duplicate_names() {
        let mut catalog = Catalog::new();
        for reserved in ["main", "MAIN", "temp", "Temp"] {
            assert!(
                matches!(
                    catalog.attach_database(reserved, ":memory:"),
                    Err(CatalogError::SchemaAlreadyExists(_))
                ),
                "expected reserved-name rejection for {reserved}"
            );
        }
        catalog.attach_database("aux", ":memory:").unwrap();
        assert!(matches!(
            catalog.attach_database("aux", "other"),
            Err(CatalogError::SchemaAlreadyExists(_))
        ));
        // A CREATE SCHEMA name is also in use.
        catalog.create_schema("reports".to_string()).unwrap();
        assert!(matches!(
            catalog.attach_database("reports", ":memory:"),
            Err(CatalogError::SchemaAlreadyExists(_))
        ));
    }

    #[test]
    fn attach_enforces_max_limit() {
        let mut catalog = Catalog::new();
        for i in 0..MAX_ATTACHED_DATABASES {
            catalog.attach_database(&format!("db{i}"), ":memory:").unwrap();
        }
        assert!(matches!(
            catalog.attach_database("one_too_many", ":memory:"),
            Err(CatalogError::TooManyAttachedDatabases)
        ));
    }

    #[test]
    fn detach_removes_schema_and_allows_reattach() {
        let mut catalog = Catalog::new();
        catalog.attach_database("aux", ":memory:").unwrap();
        catalog.detach_database("AUX").unwrap(); // case-insensitive
        assert!(!catalog.schema_exists("aux"));
        assert!(!catalog.is_attached_schema("aux"));
        assert!(catalog.attached_databases().is_empty());
        // Re-attach after detach works.
        catalog.attach_database("aux", "elsewhere").unwrap();
        assert_eq!(catalog.attached_databases()[0].path, "elsewhere");
    }

    #[test]
    fn detach_unknown_name_errors() {
        let mut catalog = Catalog::new();
        assert!(matches!(catalog.detach_database("nosuch"), Err(CatalogError::SchemaNotFound(_))));
        // A plain (CREATE SCHEMA) schema is not detachable.
        catalog.create_schema("reports".to_string()).unwrap();
        assert!(matches!(catalog.detach_database("reports"), Err(CatalogError::SchemaNotFound(_))));
    }

    #[test]
    fn attachment_order_is_preserved() {
        let mut catalog = Catalog::new();
        catalog.attach_database("bbb", "1").unwrap();
        catalog.attach_database("aaa", "2").unwrap();
        catalog.attach_database("ccc", "3").unwrap();
        let names: Vec<&str> =
            catalog.attached_databases().iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["bbb", "aaa", "ccc"]);
        catalog.detach_database("aaa").unwrap();
        let names: Vec<&str> =
            catalog.attached_databases().iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, vec!["bbb", "ccc"]);
    }
}
