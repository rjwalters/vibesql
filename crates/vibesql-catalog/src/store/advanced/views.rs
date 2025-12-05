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

impl super::super::Catalog {
    // ============================================================================
    // View Management Methods
    // ============================================================================

    /// Create a VIEW
    pub fn create_view(&mut self, view: ViewDefinition) -> Result<(), CatalogError> {
        let name = view.name.clone();

        // Normalize the key for case-insensitive storage
        let key = if self.case_sensitive_identifiers { name.clone() } else { name.to_uppercase() };

        // Check if view already exists
        if self.views.contains_key(&key) {
            return Err(CatalogError::ViewAlreadyExists(name));
        }

        self.views.insert(key, view);
        Ok(())
    }

    /// Get a VIEW definition by name with optional case-insensitive lookup
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        // Normalize the key for lookup
        let key =
            if self.case_sensitive_identifiers { name.to_string() } else { name.to_uppercase() };

        self.views.get(&key)
    }

    /// List all VIEW names
    pub fn list_views(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    /// Drop a VIEW with specified behavior
    ///
    /// - `Cascade`: Drop dependent views recursively
    /// - `Restrict`: Fail if dependents exist
    /// - `Silent`: Drop the view, ignore dependents (SQLite-compatible)
    pub fn drop_view_with_behavior(
        &mut self,
        name: &str,
        behavior: ViewDropBehavior,
    ) -> Result<(), CatalogError> {
        // Normalize the key for case-insensitive lookup
        let key =
            if self.case_sensitive_identifiers { name.to_string() } else { name.to_uppercase() };

        // Check if view exists
        if !self.views.contains_key(&key) {
            return Err(CatalogError::ViewNotFound(name.to_string()));
        }

        match behavior {
            ViewDropBehavior::Cascade => {
                // Find all views that depend on this view or table
                // Use the original name for dependency checking (not the normalized key)
                let dependent_views = self.find_dependent_views(name);
                let views_to_drop = dependent_views.clone();
                for dependent_view in views_to_drop {
                    // Recursively drop dependent views (they might have their own dependents)
                    self.drop_view_with_behavior(&dependent_view, ViewDropBehavior::Cascade)?;
                }
            }
            ViewDropBehavior::Restrict => {
                // Check for dependent views and fail if any exist
                let dependent_views = self.find_dependent_views(name);
                if !dependent_views.is_empty() {
                    return Err(CatalogError::ViewInUse {
                        view_name: name.to_string(),
                        dependent_views,
                    });
                }
            }
            ViewDropBehavior::Silent => {
                // SQLite-compatible behavior: just drop the view, don't check dependents
                // Dependent views will fail at query time if they reference this view
            }
        }

        // Finally, drop the view itself using the normalized key
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

    /// Find all views that depend on a given view or table
    fn find_dependent_views(&self, target_name: &str) -> Vec<String> {
        let mut dependent_views = Vec::new();

        // Normalize target for key comparison
        let target_key = if self.case_sensitive_identifiers {
            target_name.to_string()
        } else {
            target_name.to_uppercase()
        };

        for (view_name, view_def) in &self.views {
            if view_name == &target_key {
                // Skip the view itself
                continue;
            }

            // Check if this view's query references the target
            if self.select_references_table(&view_def.query, target_name) {
                dependent_views.push(view_name.clone());
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
        }
    }
}
