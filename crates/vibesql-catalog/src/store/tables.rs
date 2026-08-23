//! Table management operations for the catalog.
//!
//! This module handles all table-related operations including creation,
//! modification, deletion, and queries.

use std::collections::HashSet;

use crate::{errors::CatalogError, table::TableSchema, TableIdentifier};

impl super::Catalog {
    /// Check for circular foreign key dependencies that would be created by adding this table.
    ///
    /// Uses depth-first search to detect cycles in the foreign key dependency graph.
    /// Note: Self-referential tables (table references itself) are allowed.
    /// Returns an error if a circular dependency involving multiple tables is detected.
    ///
    /// Performance (#6344): a new cycle must pass through the new table, so the
    /// DFS starts there and resolves each visited table's FK edges lazily via
    /// `get_table`. FK-free tables early-exit without touching the catalog at
    /// all. This replaces the previous implementation, which materialized the
    /// full FK graph over every table in the catalog on each CREATE TABLE,
    /// making N consecutive creates O(N^2).
    fn check_circular_foreign_keys(&self, new_table: &TableSchema) -> Result<(), CatalogError> {
        let new_table_name = self.normalize_identifier(&new_table.name);

        // The new table's FK dependencies, excluding self-references (a table
        // referencing itself is allowed).
        let mut new_dependencies: HashSet<String> = HashSet::new();
        for fk in &new_table.foreign_keys {
            let parent_table = self.normalize_identifier(&fk.parent_table);
            if parent_table != new_table_name {
                new_dependencies.insert(parent_table);
            }
        }

        // Early exit: any new cycle must include the new table, which requires
        // at least one outgoing (non-self) FK edge from it. Without one, no
        // cycle involving this table is possible.
        if new_dependencies.is_empty() {
            return Ok(());
        }

        // Lazy DFS from the new table over its reachable FK closure.
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        if self.has_cycle_dfs(
            &new_table_name,
            &new_table_name,
            &new_dependencies,
            &mut visited,
            &mut rec_stack,
        ) {
            return Err(CatalogError::CircularForeignKey {
                table_name: new_table.name.clone(),
                message: "Circular foreign key dependency detected between multiple tables. \
                Circular foreign key relationships are not allowed during table creation. \
                Consider using ALTER TABLE to add foreign keys after all tables are created, \
                or ensure foreign keys only reference tables that don't create dependency cycles."
                    .to_string(),
            });
        }

        Ok(())
    }

    /// Helper function for cycle detection using depth-first search.
    ///
    /// FK edges are resolved lazily: each visited node's `TableSchema` is
    /// looked up via `get_table` only when the DFS reaches it, so the cost is
    /// bounded by the FK closure reachable from the new table rather than the
    /// total number of tables in the catalog. The new table (which is not in
    /// the catalog yet) supplies its dependencies via `new_dependencies` and
    /// shadows any existing catalog entry with the same normalized name.
    fn has_cycle_dfs(
        &self,
        node: &str,
        new_table_name: &str,
        new_dependencies: &HashSet<String>,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        if rec_stack.contains(node) {
            // Found a back edge - there's a cycle
            return true;
        }

        if visited.contains(node) {
            // Already processed this node
            return false;
        }

        visited.insert(node.to_string());
        rec_stack.insert(node.to_string());

        // Resolve this node's outgoing FK edges lazily, excluding
        // self-references (they're allowed). A table name that doesn't resolve
        // (FK to a not-yet-existing table) is a leaf, as before.
        let dependencies: Vec<String> = if node == new_table_name {
            new_dependencies.iter().cloned().collect()
        } else if let Some(table_schema) = self.get_table(node) {
            let normalized_name = self.normalize_identifier(&table_schema.name);
            table_schema
                .foreign_keys
                .iter()
                .map(|fk| self.normalize_identifier(&fk.parent_table))
                .filter(|parent_table| *parent_table != normalized_name)
                .collect()
        } else {
            Vec::new()
        };

        // Visit all dependencies
        for dep in &dependencies {
            if self.has_cycle_dfs(dep, new_table_name, new_dependencies, visited, rec_stack) {
                return true;
            }
        }

        rec_stack.remove(node);
        false
    }

    /// Create a table schema with SQL:1999 identifier semantics.
    ///
    /// The `identifier` parameter determines case-sensitivity and schema:
    /// - Quoted identifiers: stored with exact case
    /// - Unquoted identifiers: stored with lowercase canonical form
    /// - Qualified identifiers: table created in specified schema
    /// - Unqualified identifiers: table created in current schema
    pub fn create_table_with_identifier(
        &mut self,
        schema: TableSchema,
        identifier: TableIdentifier,
    ) -> Result<(), CatalogError> {
        // Check for circular foreign key dependencies
        self.check_circular_foreign_keys(&schema)?;

        // Determine target schema and table identifier
        let (target_schema_name, table_identifier) = if identifier.is_qualified() {
            // Qualified identifier: use specified schema, extract table part
            let schema_name = identifier.schema_canonical().unwrap().to_string();
            let table_id =
                TableIdentifier::new(identifier.table_display(), identifier.is_table_quoted());
            (schema_name, table_id)
        } else {
            // Unqualified identifier: use current schema
            (self.current_schema.clone(), identifier)
        };

        let object_name = schema.name.clone();
        let target_schema = self
            .schemas
            .get_mut(&target_schema_name)
            .ok_or_else(|| CatalogError::SchemaNotFound(target_schema_name.clone()))?;

        target_schema.create_table_with_identifier(schema, table_identifier)?;
        self.record_creation_seq(&target_schema_name, &object_name);
        Ok(())
    }

    /// Create a table schema in the current schema.
    /// Legacy method - uses global case_sensitive_identifiers setting
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        // Check for circular foreign key dependencies
        self.check_circular_foreign_keys(&schema)?;

        let case_sensitive = self.case_sensitive_identifiers;
        let object_name = schema.name.clone();
        let target_schema_name = self.current_schema.clone();
        let current_schema = self
            .schemas
            .get_mut(&self.current_schema)
            .ok_or_else(|| CatalogError::SchemaNotFound(self.current_schema.clone()))?;

        current_schema.create_table_with_case_mode(schema, case_sensitive)?;
        self.record_creation_seq(&target_schema_name, &object_name);
        Ok(())
    }

    /// Create a table schema in a specific schema.
    pub fn create_table_in_schema(
        &mut self,
        schema_name: &str,
        schema: TableSchema,
    ) -> Result<(), CatalogError> {
        // Check for circular foreign key dependencies
        self.check_circular_foreign_keys(&schema)?;

        let case_sensitive = self.case_sensitive_identifiers;
        let object_name = schema.name.clone();
        let target_schema = self
            .schemas
            .get_mut(schema_name)
            .ok_or_else(|| CatalogError::SchemaNotFound(schema_name.to_string()))?;

        target_schema.create_table_with_case_mode(schema, case_sensitive)?;
        self.record_creation_seq(schema_name, &object_name);
        Ok(())
    }

    /// Get a table schema using SQL:1999 identifier semantics.
    ///
    /// The `identifier` parameter determines case-sensitivity based on whether
    /// the identifier was quoted in the original SQL.
    ///
    /// For qualified identifiers (schema.table), looks up in the specified schema.
    /// For unqualified identifiers, follows SQLite semantics:
    /// 1. First check the temp schema (temporary tables shadow main tables)
    /// 2. Then check the current schema (main)
    ///
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `temp.tablename` syntax.
    pub fn get_table_by_identifier(&self, identifier: &TableIdentifier) -> Option<&TableSchema> {
        if identifier.is_qualified() {
            // For qualified identifiers, look up in the specified schema
            // Resolve "temp" to session's temp schema for SQLite compatibility
            let schema_canonical = identifier.schema_canonical().unwrap_or(&self.current_schema);
            let resolved_schema = self.resolve_schema_name(schema_canonical);
            self.schemas.get(resolved_schema).and_then(|schema| {
                // Create a simple identifier with just the table part for lookup
                let table_id = TableIdentifier::new(
                    identifier.table_canonical(),
                    identifier.is_table_quoted(),
                );
                schema.get_table_by_identifier(&table_id)
            })
        } else {
            // For unqualified identifiers: if resolution is restricted to a
            // single schema (trigger-body execution, #6477), look up ONLY
            // there — no fallback to any other schema.
            if let Some(restrict_schema) = self.restriction_read_guard().as_ref() {
                return self
                    .get_schema_case_insensitive(restrict_schema)
                    .and_then(|schema| schema.get_table_by_identifier(identifier));
            }

            // Otherwise, check session-specific temp schema first (SQLite
            // semantics): temp tables shadow tables in the main schema.
            if let Some(temp_schema) = self.schemas.get(&self.temp_schema_name) {
                if let Some(table) = temp_schema.get_table_by_identifier(identifier) {
                    return Some(table);
                }
            }

            // Then check current schema
            if let Some(table) = self
                .schemas
                .get(&self.current_schema)
                .and_then(|schema| schema.get_table_by_identifier(identifier))
            {
                return Some(table);
            }

            // Finally, check attached databases in attachment order (SQLite
            // searches temp, then main, then each ATTACHed database).
            for attached in &self.attached_databases {
                if let Some(table) = self
                    .schemas
                    .get(&attached.name)
                    .and_then(|schema| schema.get_table_by_identifier(identifier))
                {
                    return Some(table);
                }
            }
            None
        }
    }

    /// Get a table schema by name (supports qualified names like "schema.table").
    /// Legacy method - uses global case_sensitive_identifiers setting
    ///
    /// For unqualified names, follows SQLite semantics:
    /// 1. First check the temp schema (temporary tables shadow main tables)
    /// 2. Then check the current schema (main)
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        // Parse qualified name: schema.table or just table
        if let Some((schema_name, table_name)) = name.split_once('.') {
            let normalized_table = self.normalize_identifier(table_name);
            // Find schema with case-insensitive lookup
            self.get_schema_case_insensitive(schema_name).and_then(|schema| {
                schema.get_table(&normalized_table, self.case_sensitive_identifiers)
            })
        } else {
            // Unqualified name: check session-specific temp schema first (SQLite semantics)
            // Temp tables shadow tables in the main schema.
            let normalized_table = self.normalize_identifier(name);

            // If resolution is restricted to a single schema (trigger-body
            // execution, #6477), look up ONLY there — no fallback to any
            // other schema.
            if let Some(restrict_schema) = self.restriction_read_guard().as_ref() {
                return self.get_schema_case_insensitive(restrict_schema).and_then(|schema| {
                    schema.get_table(&normalized_table, self.case_sensitive_identifiers)
                });
            }

            // First check session's temp schema
            if let Some(temp_schema) = self.schemas.get(&self.temp_schema_name) {
                if let Some(table) =
                    temp_schema.get_table(&normalized_table, self.case_sensitive_identifiers)
                {
                    return Some(table);
                }
            }

            // Then check current schema
            if let Some(table) = self.schemas.get(&self.current_schema).and_then(|schema| {
                schema.get_table(&normalized_table, self.case_sensitive_identifiers)
            }) {
                return Some(table);
            }

            // Finally, check attached databases in attachment order (SQLite
            // searches temp, then main, then each ATTACHed database).
            for attached in &self.attached_databases {
                if let Some(table) = self.schemas.get(&attached.name).and_then(|schema| {
                    schema.get_table(&normalized_table, self.case_sensitive_identifiers)
                }) {
                    return Some(table);
                }
            }
            None
        }
    }

    /// Discard the verbatim `CREATE TABLE` source text (`TableSchema::sql_source`)
    /// stored on the catalog copy of `name`'s schema. Used after ALTER TABLE so
    /// that `sqlite_master.sql` reflects the mutated schema via reconstruction
    /// instead of stale original text. No-op when the table is not found. Mirrors
    /// `get_table`'s name-resolution order (temp schema, then current schema).
    /// See issue #5619.
    pub fn invalidate_table_sql_source(&mut self, name: &str) {
        let case_sensitive = self.case_sensitive_identifiers;

        if let Some((schema_name, table_name)) = name.split_once('.') {
            let normalized_table = self.normalize_identifier(table_name);
            if let Some(schema) = self.get_schema_case_insensitive_mut(schema_name) {
                if let Some(table) = schema.get_table_mut(&normalized_table, case_sensitive) {
                    table.invalidate_sql_source();
                }
            }
            return;
        }

        let normalized_table = self.normalize_identifier(name);

        // Temp schema shadows the current schema (SQLite semantics).
        let temp_schema_name = self.temp_schema_name.clone();
        if let Some(temp_schema) = self.schemas.get_mut(&temp_schema_name) {
            if let Some(table) = temp_schema.get_table_mut(&normalized_table, case_sensitive) {
                table.invalidate_sql_source();
                return;
            }
        }

        let current_schema = self.current_schema.clone();
        if let Some(schema) = self.schemas.get_mut(&current_schema) {
            if let Some(table) = schema.get_table_mut(&normalized_table, case_sensitive) {
                table.invalidate_sql_source();
            }
        }
    }

    /// Overwrite the catalog copy of `name`'s schema with `new_schema`.
    ///
    /// VibeSQL stores a table's schema twice — once in the storage `Table`
    /// (mutated directly by ALTER TABLE column operations) and once here in the
    /// catalog (the copy read by `sqlite_master`, `PRAGMA table_info`, and
    /// column resolution for DML). The two copies start out identical at CREATE
    /// time, but ALTER ADD/DROP/RENAME/MODIFY COLUMN only edited the storage
    /// copy, leaving the catalog stale. This pushes the freshly mutated storage
    /// schema back into the catalog so both copies stay consistent (issue #5625).
    ///
    /// No-op when the table is not found. Mirrors `invalidate_table_sql_source`'s
    /// name-resolution order (qualified schema, then temp schema, then current
    /// schema). The lookup key (canonical, possibly case-folded name) is left
    /// unchanged; only the stored `TableSchema` value is replaced.
    pub fn replace_table_schema(&mut self, name: &str, new_schema: TableSchema) {
        let case_sensitive = self.case_sensitive_identifiers;

        if let Some((schema_name, table_name)) = name.split_once('.') {
            let normalized_table = self.normalize_identifier(table_name);
            if let Some(schema) = self.get_schema_case_insensitive_mut(schema_name) {
                if let Some(table) = schema.get_table_mut(&normalized_table, case_sensitive) {
                    *table = new_schema;
                }
            }
            return;
        }

        let normalized_table = self.normalize_identifier(name);

        // Temp schema shadows the current schema (SQLite semantics).
        let temp_schema_name = self.temp_schema_name.clone();
        if let Some(temp_schema) = self.schemas.get_mut(&temp_schema_name) {
            if let Some(table) = temp_schema.get_table_mut(&normalized_table, case_sensitive) {
                *table = new_schema;
                return;
            }
        }

        let current_schema = self.current_schema.clone();
        if let Some(schema) = self.schemas.get_mut(&current_schema) {
            if let Some(table) = schema.get_table_mut(&normalized_table, case_sensitive) {
                *table = new_schema;
            }
        }
    }

    /// Drop a table schema (supports qualified names like "schema.table").
    /// Respects the `case_sensitive_identifiers` setting.
    ///
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `DROP TABLE temp.tablename` syntax.
    ///
    /// Note: Triggers are automatically dropped when the associated table is dropped.
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        // Parse qualified name: schema.table or just table.
        //
        // For an *unqualified* name, follow SQLite name-resolution order: the
        // session's temp schema shadows the current (main) schema. Without this,
        // `DROP TABLE t` for a temp table `t` would only look in `main` and fail
        // with "no such table" even though the temp table exists (it is the
        // schema that `get_table`/`table_exists` resolve to). This also matches
        // sqlite3 3.51.0, where an unqualified DROP removes the temp table first,
        // leaving a same-named main table intact. See #5596.
        let unqualified_schema: Option<String> =
            if name.contains('.') { None } else { self.resolve_table_schema_name(name) };
        let (schema_name_for_lookup, table_name, original_table_name) =
            if let Some((schema_part, table_part)) = name.split_once('.') {
                (schema_part, table_part, table_part)
            } else {
                // Use the temp-shadows-main resolved schema when the table exists;
                // otherwise fall back to the current schema so the lookup below
                // still produces a "table not found" error against `main`.
                (unqualified_schema.as_deref().unwrap_or(self.current_schema.as_str()), name, name)
            };

        let normalized_table = self.normalize_identifier(table_name);

        // Resolve "temp" to session's temp schema for SQLite compatibility
        let resolved_schema_name = self.resolve_schema_name(schema_name_for_lookup);

        // Find schema with case-insensitive lookup, then get mutable reference
        let schema_key = if self.case_sensitive_identifiers {
            // Case-sensitive: direct lookup
            if self.schemas.contains_key(resolved_schema_name) {
                resolved_schema_name.to_string()
            } else {
                return Err(CatalogError::SchemaNotFound(schema_name_for_lookup.to_string()));
            }
        } else {
            // Case-insensitive: find schema key by comparing normalized names
            let normalized_name = resolved_schema_name.to_lowercase();
            self.schemas
                .keys()
                .find(|key| key.to_lowercase() == normalized_name)
                .cloned()
                .ok_or_else(|| CatalogError::SchemaNotFound(schema_name_for_lookup.to_string()))?
        };

        // Drop all triggers associated with this table
        // Per SQL standard (R-37808-62273): triggers are automatically dropped when the table is
        // dropped Note: We need to normalize trigger table_name for comparison in
        // case-insensitive mode
        let case_sensitive = self.case_sensitive_identifiers;
        // Collect the schema-scoped storage keys (not the bare trigger names):
        // triggers are keyed per schema, so a bare name no longer identifies a
        // single map entry.
        let trigger_keys: Vec<String> = self
            .triggers
            .iter()
            .filter(|(_, trigger)| {
                let trigger_table = if case_sensitive {
                    trigger.table_name.clone()
                } else {
                    trigger.table_name.to_lowercase()
                };
                trigger_table == normalized_table
            })
            .map(|(key, _)| key.clone())
            .collect();

        for trigger_key in trigger_keys {
            self.triggers.remove(&trigger_key);
        }

        let schema = self
            .schemas
            .get_mut(&schema_key)
            .ok_or(CatalogError::SchemaNotFound(schema_key.clone()))?;

        // For error messages, we want to use the original input name, not the normalized one
        schema.drop_table(&normalized_table, case_sensitive).map_err(|e| match e {
            CatalogError::TableNotFound { .. } => {
                CatalogError::TableNotFound { table_name: original_table_name.to_string() }
            }
            other => other,
        })
    }

    /// List all table names in the current schema.
    pub fn list_tables(&self) -> Vec<String> {
        self.schemas
            .get(&self.current_schema)
            .map(|schema| schema.list_tables())
            .unwrap_or_default()
    }

    /// List all table names in a specific schema.
    ///
    /// The `temp` alias is resolved to this session's temp schema, so
    /// `list_tables_in_schema("temp")` returns the session's temp tables. Used
    /// by `sqlite_temp_master` introspection. See issue #5513.
    pub fn list_tables_in_schema(&self, schema_name: &str) -> Vec<String> {
        let resolved = self.resolve_schema_name(schema_name);
        self.schemas.get(resolved).map(|schema| schema.list_tables()).unwrap_or_default()
    }

    /// List all table names with qualified names (schema.table).
    pub fn list_all_tables(&self) -> Vec<String> {
        let mut result = Vec::new();
        for (schema_name, schema) in &self.schemas {
            for table_name in schema.list_tables() {
                result.push(format!("{}.{}", schema_name, table_name));
            }
        }
        result
    }

    /// Check if table exists (supports qualified names).
    pub fn table_exists(&self, name: &str) -> bool {
        self.get_table(name).is_some()
    }

    /// Resolve the internal schema name that an unqualified table resolves to.
    ///
    /// Follows SQLite name-resolution order for unqualified identifiers: the
    /// session's temp schema shadows the current (main) schema. If a qualified
    /// `schema.table` name is supplied, the supplied schema is resolved
    /// (mapping the literal `temp` alias to the session temp schema) without
    /// shadowing.
    ///
    /// Returns the internal schema name (e.g. `main` or `temp_123`) where the
    /// table actually lives, or `None` if no such table exists.
    ///
    /// This is the canonical way for DDL that must pre-qualify an unqualified
    /// table name (e.g. CREATE INDEX) to discover the correct schema instead
    /// of assuming `main`. See issue #5505.
    pub fn resolve_table_schema_name(&self, name: &str) -> Option<String> {
        if let Some((schema_part, table_part)) = name.split_once('.') {
            // Qualified: resolve the supplied schema (temp alias -> session temp),
            // and confirm the table exists there.
            let resolved_schema = self.resolve_schema_name(schema_part);
            let normalized_table = self.normalize_identifier(table_part);
            return self.get_schema_case_insensitive(resolved_schema).and_then(|schema| {
                schema
                    .get_table(&normalized_table, self.case_sensitive_identifiers)
                    .map(|_| resolved_schema.to_string())
            });
        }

        let normalized_table = self.normalize_identifier(name);

        // If resolution is restricted to a single schema (trigger-body
        // execution, #6477), look up ONLY there — no fallback to any other
        // schema.
        if let Some(restrict_schema) = self.restriction_read_guard().as_ref() {
            return self.get_schema_case_insensitive(restrict_schema).and_then(|schema| {
                schema
                    .get_table(&normalized_table, self.case_sensitive_identifiers)
                    .map(|_| restrict_schema.clone())
            });
        }

        // Temp schema shadows main for unqualified names (SQLite semantics).
        if let Some(temp_schema) = self.schemas.get(&self.temp_schema_name) {
            if temp_schema.get_table(&normalized_table, self.case_sensitive_identifiers).is_some() {
                return Some(self.temp_schema_name.clone());
            }
        }

        if let Some(found) = self.schemas.get(&self.current_schema).and_then(|schema| {
            schema
                .get_table(&normalized_table, self.case_sensitive_identifiers)
                .map(|_| self.current_schema.clone())
        }) {
            return Some(found);
        }

        // Finally, check attached databases in attachment order (SQLite
        // searches temp, then main, then each ATTACHed database — #6310).
        for attached in &self.attached_databases {
            if self.schemas.get(&attached.name).is_some_and(|schema| {
                schema.get_table(&normalized_table, self.case_sensitive_identifiers).is_some()
            }) {
                return Some(attached.name.clone());
            }
        }
        None
    }

    /// Check if table exists using SQL:1999 identifier semantics.
    ///
    /// Uses the `quoted` flag in the identifier to determine case-sensitivity:
    /// - Quoted identifiers are case-sensitive (match exact canonical form)
    /// - Unquoted identifiers are case-insensitive (lowercase canonical form)
    ///
    /// For qualified identifiers, looks up in the specified schema.
    /// For unqualified identifiers, checks session-specific temp schema first (SQLite semantics).
    ///
    /// SQLite Compatibility: The "temp" schema name is mapped to the session's
    /// temp schema, allowing `temp.tablename` syntax.
    pub fn table_exists_by_identifier(&self, identifier: &TableIdentifier) -> bool {
        if identifier.is_qualified() {
            // For qualified identifiers, look up in the specified schema
            // Resolve "temp" to session's temp schema for SQLite compatibility
            let schema_canonical = identifier.schema_canonical().unwrap_or(&self.current_schema);
            let resolved_schema = self.resolve_schema_name(schema_canonical);
            if let Some(schema) = self.schemas.get(resolved_schema) {
                // Create a simple identifier with just the table part for lookup
                let table_id = TableIdentifier::new(
                    identifier.table_canonical(),
                    identifier.is_table_quoted(),
                );
                return schema.table_exists_by_identifier(&table_id);
            }
            false
        } else {
            // For unqualified identifiers: if resolution is restricted to a
            // single schema (trigger-body execution, #6477), look up ONLY
            // there — no fallback to any other schema.
            if let Some(restrict_schema) = self.restriction_read_guard().as_ref() {
                return self
                    .get_schema_case_insensitive(restrict_schema)
                    .is_some_and(|schema| schema.table_exists_by_identifier(identifier));
            }

            // Otherwise, check session's temp schema first (SQLite semantics).
            if let Some(temp_schema) = self.schemas.get(&self.temp_schema_name) {
                if temp_schema.table_exists_by_identifier(identifier) {
                    return true;
                }
            }

            // Then check current schema
            if self
                .schemas
                .get(&self.current_schema)
                .is_some_and(|schema| schema.table_exists_by_identifier(identifier))
            {
                return true;
            }

            // Finally, check attached databases in attachment order (SQLite
            // searches temp, then main, then each ATTACHed database — #6310).
            self.attached_databases.iter().any(|attached| {
                self.schemas
                    .get(&attached.name)
                    .is_some_and(|schema| schema.table_exists_by_identifier(identifier))
            })
        }
    }

    /// Get the TableIdentifier for a table by its canonical name.
    ///
    /// Returns the identifier that was used when the table was created,
    /// which includes the `quoted` flag for SQL:1999 case-sensitivity semantics.
    ///
    /// The canonical_name should be the canonical form of the table name
    /// (as returned by `list_tables()`).
    pub fn get_table_identifier(&self, canonical_name: &str) -> Option<&TableIdentifier> {
        self.schemas
            .get(&self.current_schema)
            .and_then(|schema| schema.get_table_identifier(canonical_name))
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
    use vibesql_types::DataType;

    use super::*;
    use crate::{column::ColumnSchema, trigger::TriggerDefinition};

    #[test]
    fn test_drop_table_deletes_triggers_case_insensitive() {
        // Test that triggers are correctly deleted when table is dropped,
        // even with mixed-case table references (Issue #1481)

        // Create catalog with case-insensitive mode
        let mut catalog = crate::Catalog::new();
        catalog.set_case_sensitive_identifiers(false);
        assert!(!catalog.case_sensitive_identifiers);

        // Create table with lowercase name
        let column = ColumnSchema::new("x".to_string(), DataType::Integer, true);
        let table_schema = TableSchema::new("t1".to_string(), vec![column]);
        catalog.create_table(table_schema).unwrap();

        // Create trigger with UPPERCASE table reference
        let trigger = TriggerDefinition {
            name: "Tr1".to_string(),
            schema: None,
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None), // None = no column list
            table_name: "T1".to_string(),      // Different case than table creation
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
            sql_definition: None,
        };
        catalog.create_trigger(trigger).unwrap();

        // Verify trigger exists
        assert!(catalog.get_trigger("Tr1").is_some());

        // Drop table with lowercase name
        catalog.drop_table("t1").unwrap();

        // Verify trigger was automatically deleted despite case mismatch
        assert!(
            catalog.get_trigger("Tr1").is_none(),
            "Trigger should be automatically deleted when table is dropped, \
                 regardless of case used in CREATE TRIGGER vs DROP TABLE"
        );
    }

    #[test]
    fn test_resolve_table_schema_name_temp_shadows_main() {
        // #5505: an unqualified name resolves to the temp schema when a temp
        // table of that name exists, even if a main table also exists.
        let mut catalog = crate::Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let col = ColumnSchema::new("a".to_string(), DataType::Integer, true);

        // Only main.t exists -> resolves to main.
        catalog.create_table(TableSchema::new("t".to_string(), vec![col.clone()])).unwrap();
        assert_eq!(
            catalog.resolve_table_schema_name("t").as_deref(),
            Some(catalog.current_schema.as_str())
        );

        // Add a shadowing temp.t -> unqualified now resolves to the temp schema.
        let temp_schema = catalog.temp_schema_name().to_string();
        catalog
            .create_table_in_schema(&temp_schema, TableSchema::new("t".to_string(), vec![col]))
            .unwrap();
        assert_eq!(catalog.resolve_table_schema_name("t").as_deref(), Some(temp_schema.as_str()));

        // A qualified `main.t` still resolves to main (no shadowing for qualified).
        assert_eq!(
            catalog.resolve_table_schema_name(&format!("{}.t", catalog.current_schema)).as_deref(),
            Some(catalog.current_schema.as_str())
        );

        // An unknown table resolves to None.
        assert_eq!(catalog.resolve_table_schema_name("missing"), None);
    }

    #[test]
    fn test_drop_table_deletes_triggers_case_sensitive() {
        // Test that triggers work correctly in case-sensitive mode

        // Create catalog with case-sensitive mode
        let mut catalog = crate::Catalog::new();
        catalog.case_sensitive_identifiers = true;

        // Create table with lowercase name
        let column = ColumnSchema::new("x".to_string(), DataType::Integer, true);
        let table_schema = TableSchema::new("t1".to_string(), vec![column]);
        catalog.create_table(table_schema).unwrap();

        // Create trigger with exact case match
        let trigger = TriggerDefinition {
            name: "Tr1".to_string(),
            schema: None,
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t1".to_string(), // Exact match required in case-sensitive mode
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
            sql_definition: None,
        };
        catalog.create_trigger(trigger).unwrap();

        // Drop table
        catalog.drop_table("t1").unwrap();

        // Verify trigger was deleted
        assert!(catalog.get_trigger("Tr1").is_none());
    }

    #[test]
    fn test_drop_table_deletes_multiple_triggers() {
        // Test that all triggers for a table are deleted

        let mut catalog = crate::Catalog::new();

        // Create table
        let column = ColumnSchema::new("x".to_string(), DataType::Integer, true);
        let table_schema = TableSchema::new("t1".to_string(), vec![column]);
        catalog.create_table(table_schema).unwrap();

        // Create multiple triggers on the same table
        for i in 1..=3 {
            let trigger = TriggerDefinition {
                name: format!("tr{}", i),
                schema: None,
                timing: TriggerTiming::After,
                event: TriggerEvent::Update(None),
                table_name: "t1".to_string(),
                granularity: TriggerGranularity::Row,
                when_condition: None,
                triggered_action: TriggerAction::RawSql("".to_string()),
                enabled: true,
                sql_definition: None,
            };
            catalog.create_trigger(trigger).unwrap();
        }

        // Verify all triggers exist
        assert!(catalog.get_trigger("tr1").is_some());
        assert!(catalog.get_trigger("tr2").is_some());
        assert!(catalog.get_trigger("tr3").is_some());

        // Drop table
        catalog.drop_table("t1").unwrap();

        // Verify all triggers were deleted
        assert!(catalog.get_trigger("tr1").is_none());
        assert!(catalog.get_trigger("tr2").is_none());
        assert!(catalog.get_trigger("tr3").is_none());
    }

    #[test]
    fn test_drop_table_preserves_other_table_triggers() {
        // Test that dropping a table doesn't delete triggers for other tables

        let mut catalog = crate::Catalog::new();

        // Create two tables
        let column = ColumnSchema::new("x".to_string(), DataType::Integer, true);
        let table1 = TableSchema::new("t1".to_string(), vec![column.clone()]);
        let table2 = TableSchema::new("t2".to_string(), vec![column]);
        catalog.create_table(table1).unwrap();
        catalog.create_table(table2).unwrap();

        // Create triggers on both tables
        let trigger1 = TriggerDefinition {
            name: "tr1".to_string(),
            schema: None,
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t1".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
            sql_definition: None,
        };
        let trigger2 = TriggerDefinition {
            name: "tr2".to_string(),
            schema: None,
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t2".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
            sql_definition: None,
        };
        catalog.create_trigger(trigger1).unwrap();
        catalog.create_trigger(trigger2).unwrap();

        // Drop first table
        catalog.drop_table("t1").unwrap();

        // Verify only t1's trigger was deleted
        assert!(catalog.get_trigger("tr1").is_none());
        assert!(catalog.get_trigger("tr2").is_some());
    }

    // ---- Circular foreign key detection (#6344 lazy DFS rewrite) ----

    /// Build a foreign key constraint referencing `parent` (structure beyond
    /// `parent_table` is irrelevant to cycle detection).
    fn fk_to(parent: &str) -> crate::ForeignKeyConstraint {
        crate::ForeignKeyConstraint {
            name: None,
            column_names: vec!["a".to_string()],
            column_indices: vec![0],
            parent_table: parent.to_string(),
            parent_column_names: vec!["a".to_string()],
            parent_column_indices: vec![0],
            on_delete: crate::ReferentialAction::NoAction,
            on_update: crate::ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        }
    }

    /// Build a single-column table schema with FKs to each listed parent.
    fn table_with_fks(name: &str, parents: &[&str]) -> TableSchema {
        let column = ColumnSchema::new("a".to_string(), DataType::Integer, true);
        let mut schema = TableSchema::new(name.to_string(), vec![column]);
        for parent in parents {
            schema.foreign_keys.push(fk_to(parent));
        }
        schema
    }

    #[test]
    fn test_fk_cycle_two_tables_rejected() {
        let mut catalog = crate::Catalog::new();

        // t1 -> t2 (t2 doesn't exist yet; forward reference is allowed)
        catalog.create_table(table_with_fks("t1", &["t2"])).unwrap();

        // t2 -> t1 completes the cycle and must be rejected
        let err = catalog.create_table(table_with_fks("t2", &["t1"])).unwrap_err();
        assert!(
            matches!(err, CatalogError::CircularForeignKey { ref table_name, .. } if table_name == "t2"),
            "expected CircularForeignKey for t2, got: {err:?}"
        );
        // The rejected table must not have been created
        assert!(catalog.get_table("t2").is_none());
    }

    #[test]
    fn test_fk_cycle_three_tables_rejected() {
        let mut catalog = crate::Catalog::new();

        catalog.create_table(table_with_fks("t1", &["t2"])).unwrap();
        catalog.create_table(table_with_fks("t2", &["t3"])).unwrap();

        // t3 -> t1 closes the 3-table cycle t1 -> t2 -> t3 -> t1
        let err = catalog.create_table(table_with_fks("t3", &["t1"])).unwrap_err();
        assert!(
            matches!(err, CatalogError::CircularForeignKey { ref table_name, .. } if table_name == "t3"),
            "expected CircularForeignKey for t3, got: {err:?}"
        );
    }

    #[test]
    fn test_fk_self_reference_allowed() {
        let mut catalog = crate::Catalog::new();

        // employees.manager_id -> employees.id style self-reference is allowed
        catalog.create_table(table_with_fks("employees", &["employees"])).unwrap();
        assert!(catalog.get_table("employees").is_some());
    }

    #[test]
    fn test_fk_diamond_dependency_allowed() {
        let mut catalog = crate::Catalog::new();

        // Diamond: a -> b, a -> c, b -> d, c -> d (a DAG, not a cycle)
        catalog.create_table(table_with_fks("d", &[])).unwrap();
        catalog.create_table(table_with_fks("b", &["d"])).unwrap();
        catalog.create_table(table_with_fks("c", &["d"])).unwrap();
        catalog.create_table(table_with_fks("a", &["b", "c"])).unwrap();
        assert!(catalog.get_table("a").is_some());
    }

    #[test]
    fn test_fk_to_nonexistent_table_allowed() {
        let mut catalog = crate::Catalog::new();

        // FK to a table that doesn't exist yet: treated as a leaf (unchanged
        // pre-existing behavior; FK existence is enforced elsewhere).
        catalog.create_table(table_with_fks("child", &["missing_parent"])).unwrap();
        assert!(catalog.get_table("child").is_some());
    }

    #[test]
    fn test_fk_cycle_case_insensitive_rejected() {
        let mut catalog = crate::Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        // Mixed-case references must still normalize onto the same nodes
        catalog.create_table(table_with_fks("t1", &["T2"])).unwrap();
        let err = catalog.create_table(table_with_fks("T2", &["t1"])).unwrap_err();
        assert!(
            matches!(err, CatalogError::CircularForeignKey { .. }),
            "expected CircularForeignKey, got: {err:?}"
        );
    }

    #[test]
    fn test_fk_case_sensitive_no_false_cycle() {
        let mut catalog = crate::Catalog::new();
        catalog.case_sensitive_identifiers = true;

        // In case-sensitive mode "T1" and "t1" are distinct names, so this is
        // not a cycle (the "T1" reference doesn't resolve to table t1).
        catalog.create_table(table_with_fks("t1", &["T2"])).unwrap();
        catalog.create_table(table_with_fks("t2", &["T1"])).unwrap();
        assert!(catalog.get_table("t2").is_some());
    }

    #[test]
    fn test_fk_free_bulk_creates_after_fk_tables_exist() {
        // Regression guard for #6344: FK-free CREATE TABLE takes the early
        // exit and never scans the catalog, even when FK-bearing tables exist.
        let mut catalog = crate::Catalog::new();
        catalog.create_table(table_with_fks("parent", &[])).unwrap();
        catalog.create_table(table_with_fks("child", &["parent"])).unwrap();

        for i in 0..2000 {
            catalog.create_table(table_with_fks(&format!("tbl{i}"), &[])).unwrap();
        }
        assert!(catalog.get_table("tbl1999").is_some());
    }

    #[test]
    fn test_fk_cycle_still_detected_after_many_unrelated_tables() {
        // The lazy DFS must still find cycles when the catalog is large:
        // correctness must not have been traded for the early exit.
        let mut catalog = crate::Catalog::new();
        for i in 0..500 {
            catalog.create_table(table_with_fks(&format!("noise{i}"), &[])).unwrap();
        }

        catalog.create_table(table_with_fks("x", &["y"])).unwrap();
        let err = catalog.create_table(table_with_fks("y", &["x"])).unwrap_err();
        assert!(
            matches!(err, CatalogError::CircularForeignKey { .. }),
            "expected CircularForeignKey, got: {err:?}"
        );
    }
}
