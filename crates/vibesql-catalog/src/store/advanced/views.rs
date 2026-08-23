//! View management methods and dependency tracking.

use crate::{errors::CatalogError, view::ViewDefinition};

/// Drop behavior for views
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ViewDropBehavior {
    /// CASCADE: drop dependent views recursively
    Cascade,
    /// RESTRICT: fail if dependents exist
    Restrict,
    /// Silent: drop the view, ignore dependents (SQLite-compatible)
    Silent,
}

/// Compute the schema-scoped storage key for a view.
///
/// SQLite scopes view names *per schema*, exactly like tables and triggers:
/// `main.v1` and `aux.v1` (an attached-database view) are two distinct views
/// that may coexist. The catalog therefore keys views by `(schema, name)`
/// rather than by bare `name`, so a `main` view and a `temp`/attached-schema
/// view sharing a name no longer collide (issue #6490 — before this, `CREATE
/// VIEW <alias>.<name>` against an attached alias could not even be
/// represented correctly: the flat by-name map had no way to distinguish it
/// from a same-named `main` view).
///
/// The schema component is normalized case-insensitively, with `None` and
/// `main` collapsing to the default (`main`) schema. The name component
/// respects `case_sensitive_identifiers` the same way the pre-existing bare-name
/// key did. A control character separates the two parts so a schema/name that
/// happens to contain a `.` cannot forge a different key.
fn view_storage_key(schema: Option<&str>, name: &str, case_sensitive_identifiers: bool) -> String {
    let schema =
        schema.map(|s| s.to_ascii_lowercase()).unwrap_or_else(|| crate::DEFAULT_SCHEMA.to_string());
    let name_key = if case_sensitive_identifiers { name.to_string() } else { name.to_uppercase() };
    format!("{schema}\u{1f}{name_key}")
}

impl super::super::Catalog {
    // ============================================================================
    // View Management Methods
    // ============================================================================

    /// Create a VIEW.
    ///
    /// The collision check is scoped to the view's own schema (see
    /// [`view_storage_key`]): creating `aux.v1` when a `main.v1` already
    /// exists succeeds, matching SQLite's per-schema view namespace.
    pub fn create_view(&mut self, view: ViewDefinition) -> Result<(), CatalogError> {
        let name = view.name.clone();
        let key = view_storage_key(view.schema.as_deref(), &name, self.case_sensitive_identifiers);

        // Check if view already exists in this schema
        if self.views.contains_key(&key) {
            return Err(CatalogError::ViewAlreadyExists(name));
        }

        let view_schema = view.schema.clone().unwrap_or_else(|| crate::DEFAULT_SCHEMA.to_string());
        self.views.insert(key, view);
        self.record_creation_seq(&view_schema, &name);
        Ok(())
    }

    /// Get a VIEW definition by (possibly schema-qualified) name.
    ///
    /// - A dotted `schema.name` is split and resolved directly in that schema (`temp`/`main`/an
    ///   attached alias — mirrors [`Catalog::get_table`]'s qualified-name handling).
    /// - An unqualified name is resolved in SQLite's unqualified search order: the `temp` schema
    ///   first (temp views shadow main), then `main`, then each attached database in attachment
    ///   order. A final linear fallback (any schema, first match) covers views created without
    ///   going through [`Catalog::create_view`]'s normal schema tagging.
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        if let Some((schema_part, name_part)) = name.split_once('.') {
            return self.get_view_in_schema(name_part, Some(schema_part));
        }

        if let Some(view) = self.get_view_in_schema(name, Some("temp")) {
            return Some(view);
        }
        if let Some(view) = self.get_view_in_schema(name, None) {
            return Some(view);
        }
        for attached in &self.attached_databases {
            if let Some(view) = self.get_view_in_schema(name, Some(&attached.name)) {
                return Some(view);
            }
        }
        self.views.values().find(|v| v.name == name)
    }

    /// Get a VIEW definition scoped to a specific schema.
    ///
    /// `schema` is the logical schema label a view carries
    /// ([`ViewDefinition::schema`]): `None`/`Some("main")` for the main
    /// schema, `Some("temp")` for the temp schema, or an attached schema
    /// name. This never falls through to another schema, so `main.v1` and
    /// `aux.v1` are distinguishable.
    pub fn get_view_in_schema(&self, name: &str, schema: Option<&str>) -> Option<&ViewDefinition> {
        self.views.get(&view_storage_key(schema, name, self.case_sensitive_identifiers))
    }

    /// Returns true if a view of `name` exists in the given schema.
    pub fn view_exists_in_schema(&self, name: &str, schema: Option<&str>) -> bool {
        self.views.contains_key(&view_storage_key(schema, name, self.case_sensitive_identifiers))
    }

    /// Get a mutable reference to a VIEW definition by (possibly
    /// schema-qualified) name.
    ///
    /// Used by `ALTER TABLE ... RENAME COLUMN` to rewrite a view's stored
    /// `sql_definition` text and its parsed `query` AST in place when a source
    /// column it references is renamed (mirrors SQLite re-resolving dependent
    /// views). Name resolution follows the same rules as
    /// [`get_view`](Self::get_view).
    pub fn get_view_mut(&mut self, name: &str) -> Option<&mut ViewDefinition> {
        let case_sensitive = self.case_sensitive_identifiers;
        if let Some((schema_part, name_part)) = name.split_once('.') {
            let key = view_storage_key(Some(schema_part), name_part, case_sensitive);
            return self.views.get_mut(&key);
        }

        let temp_key = view_storage_key(Some("temp"), name, case_sensitive);
        if self.views.contains_key(&temp_key) {
            return self.views.get_mut(&temp_key);
        }
        let main_key = view_storage_key(None, name, case_sensitive);
        if self.views.contains_key(&main_key) {
            return self.views.get_mut(&main_key);
        }
        self.views.values_mut().find(|v| v.name == name)
    }

    /// List all VIEW names (returns original names, not normalized keys).
    ///
    /// Since views are keyed per schema, the same bare name may appear more
    /// than once (e.g. `main.v1` and `aux.v1`). Callers that need to
    /// distinguish same-named views in different schemas should use
    /// [`Catalog::iter_views`] instead.
    pub fn list_views(&self) -> Vec<String> {
        self.views.values().map(|v| v.name.clone()).collect()
    }

    /// Iterate over every view definition in the catalog, regardless of
    /// schema. Preferred over `list_views()` + `get_view()` by callers that
    /// must see *every* view unambiguously, since a name-only `get_view` can
    /// only return one of several same-named views living in different
    /// schemas (view analogue of the trigger fix in issue #6296).
    pub fn iter_views(&self) -> impl Iterator<Item = &ViewDefinition> {
        self.views.values()
    }

    /// Drop a VIEW with specified behavior, resolved by (possibly
    /// schema-qualified) name using the same rules as
    /// [`get_view`](Self::get_view).
    ///
    /// - `Cascade`: Drop dependent views recursively
    /// - `Restrict`: Fail if dependents exist
    /// - `Silent`: Drop the view, ignore dependents (SQLite-compatible)
    pub fn drop_view_with_behavior(
        &mut self,
        name: &str,
        behavior: ViewDropBehavior,
    ) -> Result<(), CatalogError> {
        let key = self
            .resolve_view_key(name)
            .ok_or_else(|| CatalogError::ViewNotFound(name.to_string()))?;

        match behavior {
            ViewDropBehavior::Cascade => {
                // Find all views that depend on this view or table.
                let dependent_views = self.find_dependent_views(&key);
                for dependent_key in dependent_views {
                    // Recursively drop dependent views (they might have their own dependents)
                    self.drop_view_key_with_behavior(&dependent_key, ViewDropBehavior::Cascade)?;
                }
            }
            ViewDropBehavior::Restrict => {
                // Check for dependent views and fail if any exist
                let dependent_views = self.find_dependent_views(&key);
                if !dependent_views.is_empty() {
                    let dependent_names = dependent_views
                        .iter()
                        .filter_map(|k| self.views.get(k).map(|v| v.name.clone()))
                        .collect();
                    return Err(CatalogError::ViewInUse {
                        view_name: name.to_string(),
                        dependent_views: dependent_names,
                    });
                }
            }
            ViewDropBehavior::Silent => {
                // SQLite-compatible behavior: just drop the view, don't check dependents
                // Dependent views will fail at query time if they reference this view
            }
        }

        self.views.remove(&key);
        Ok(())
    }

    /// Drop a VIEW (legacy method, defaults to RESTRICT behavior)
    ///
    /// This maintains backward compatibility with existing code.
    /// Use `drop_view_with_behavior` for explicit control.
    pub fn drop_view(&mut self, name: &str, cascade: bool) -> Result<(), CatalogError> {
        let behavior = if cascade { ViewDropBehavior::Cascade } else { ViewDropBehavior::Restrict };
        self.drop_view_with_behavior(name, behavior)
    }

    /// Drop a VIEW addressed by its already-resolved internal storage key
    /// (used for the recursive CASCADE case, where the dependent view's key
    /// is already known and must not be re-resolved via the unqualified
    /// search order — two different schemas may hold a same-named dependent
    /// view).
    fn drop_view_key_with_behavior(
        &mut self,
        key: &str,
        behavior: ViewDropBehavior,
    ) -> Result<(), CatalogError> {
        if !self.views.contains_key(key) {
            return Err(CatalogError::ViewNotFound(key.to_string()));
        }
        if behavior == ViewDropBehavior::Cascade {
            for dependent_key in self.find_dependent_views(key) {
                self.drop_view_key_with_behavior(&dependent_key, ViewDropBehavior::Cascade)?;
            }
        }
        self.views.remove(key);
        Ok(())
    }

    /// Resolve a (possibly schema-qualified) view name to its internal
    /// storage key, using the same rules as [`get_view`](Self::get_view).
    fn resolve_view_key(&self, name: &str) -> Option<String> {
        let case_sensitive = self.case_sensitive_identifiers;
        if let Some((schema_part, name_part)) = name.split_once('.') {
            let key = view_storage_key(Some(schema_part), name_part, case_sensitive);
            return self.views.contains_key(&key).then_some(key);
        }

        let temp_key = view_storage_key(Some("temp"), name, case_sensitive);
        if self.views.contains_key(&temp_key) {
            return Some(temp_key);
        }
        let main_key = view_storage_key(None, name, case_sensitive);
        if self.views.contains_key(&main_key) {
            return Some(main_key);
        }
        for attached in &self.attached_databases {
            let key = view_storage_key(Some(&attached.name), name, case_sensitive);
            if self.views.contains_key(&key) {
                return Some(key);
            }
        }
        self.views.iter().find(|(_, v)| v.name == name).map(|(k, _)| k.clone())
    }

    /// Find all views (by internal storage key) that depend on the view
    /// stored under `target_key`.
    fn find_dependent_views(&self, target_key: &str) -> Vec<String> {
        let mut dependent_views = Vec::new();

        let Some(target_view) = self.views.get(target_key) else {
            return dependent_views;
        };
        let target_name = target_view.name.clone();

        for (view_key, view_def) in &self.views {
            if view_key == target_key {
                // Skip the view itself
                continue;
            }

            // Check if this view's query references the target
            if self.select_references_table(&view_def.query, &target_name) {
                dependent_views.push(view_key.clone());
            }
        }

        dependent_views
    }

    /// Check if a SELECT statement references a specific table or view
    fn select_references_table(&self, select: &vibesql_ast::SelectStmt, table_name: &str) -> bool {
        // Check the FROM clause
        if let Some(ref from) = select.from {
            if self.does_from_clause_reference_table(from, table_name) {
                return true;
            }
        }

        // Check CTEs (WITH clause)
        if let Some(ref ctes) = select.with_clause {
            for cte in ctes {
                if self.select_references_table(&cte.query, table_name) {
                    return true;
                }
            }
        }

        // Check set operations (UNION, INTERSECT, EXCEPT)
        if let Some(ref set_op) = select.set_operation {
            if self.select_references_table(&set_op.right, table_name) {
                return true;
            }
        }

        false
    }

    /// Check if a FROM clause references a specific table or view
    fn does_from_clause_reference_table(
        &self,
        from: &vibesql_ast::FromClause,
        table_name: &str,
    ) -> bool {
        use vibesql_ast::FromClause;
        match from {
            FromClause::Table { name, .. } => {
                // Respect case sensitivity setting when comparing table names
                if self.case_sensitive_identifiers {
                    name == table_name
                } else {
                    name.to_uppercase() == table_name.to_uppercase()
                }
            }
            FromClause::Join { left, right, .. } => {
                self.does_from_clause_reference_table(left, table_name)
                    || self.does_from_clause_reference_table(right, table_name)
            }
            FromClause::Subquery { query, .. } => self.select_references_table(query, table_name),
            // VALUES clauses don't reference any tables
            FromClause::Values { .. } => false,
            // Table-valued functions (json_each/json_tree) don't reference a
            // named base table or view.
            FromClause::TableFunction { .. } => false,
        }
    }
}
