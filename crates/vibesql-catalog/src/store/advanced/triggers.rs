//! Trigger management methods.

use crate::{errors::CatalogError, trigger::TriggerDefinition};

impl super::super::Catalog {
    // ============================================================================
    // Trigger Management Methods
    // ============================================================================

    /// Create a TRIGGER
    pub fn create_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let name = trigger.name.clone();
        if self.triggers.contains_key(&name) {
            return Err(CatalogError::TriggerAlreadyExists(name));
        }
        self.triggers.insert(name, trigger);
        Ok(())
    }

    /// Get a TRIGGER definition by name
    pub fn get_trigger(&self, name: &str) -> Option<&TriggerDefinition> {
        self.triggers.get(name)
    }

    /// Update a TRIGGER (for ALTER TRIGGER operations)
    pub fn update_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let name = trigger.name.clone();
        if !self.triggers.contains_key(&name) {
            return Err(CatalogError::TriggerNotFound(name));
        }
        self.triggers.insert(name, trigger);
        Ok(())
    }

    /// Drop a TRIGGER
    pub fn drop_trigger(&mut self, name: &str) -> Result<(), CatalogError> {
        self.triggers
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TriggerNotFound(name.to_string()))
    }

    /// Get all triggers for a table with a specific event
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check for triggers
    /// * `event` - Optional trigger event to filter by (Insert, Update, Delete)
    ///
    /// # Returns
    /// Iterator over trigger definitions matching the criteria
    pub fn get_triggers_for_table<'a>(
        &'a self,
        table_name: &'a str,
        event: Option<vibesql_ast::TriggerEvent>,
    ) -> impl Iterator<Item = &'a TriggerDefinition> + 'a {
        self.triggers.values().filter(move |trigger| {
            // Case-insensitive table name matching (SQLite-compatible)
            trigger.table_name.eq_ignore_ascii_case(table_name)
                // Match by event *kind* (INSERT / UPDATE / DELETE), not by exact
                // value. An `UPDATE OF c, d` trigger has event
                // `Update(Some([c, d]))`, but the executor dispatches the generic
                // `Update(None)` event for every UPDATE; comparing for equality
                // would never match column-list triggers, so they would never
                // fire (issue #5577). The column-list firing restriction is
                // applied later by `should_fire_update_of`.
                && event.as_ref().is_none_or(|e| event_kind_matches(&trigger.event, e))
        })
    }

    /// List all trigger names
    pub fn list_triggers(&self) -> Vec<String> {
        self.triggers.keys().cloned().collect()
    }

    /// Cheap O(1) check: does the catalog hold *any* trigger at all?
    ///
    /// Used by the executor's hot-path trigger guard to short-circuit before
    /// the (allocating) per-table cascade walk: a database with zero triggers
    /// can never fire a `RAISE()` regardless of foreign-key state.
    pub fn has_any_triggers(&self) -> bool {
        !self.triggers.is_empty()
    }
}

/// Compare two trigger events by *kind* (INSERT / UPDATE / DELETE), ignoring an
/// UPDATE's optional `OF <columns>` list.
///
/// The executor dispatches the generic `Update(None)` event for every UPDATE,
/// while a column-list trigger is stored as `Update(Some([...]))`. Matching by
/// kind lets such triggers be discovered; the per-row column-list restriction is
/// enforced separately by the executor's `should_fire_update_of`.
fn event_kind_matches(a: &vibesql_ast::TriggerEvent, b: &vibesql_ast::TriggerEvent) -> bool {
    use vibesql_ast::TriggerEvent::*;
    matches!(
        (a, b),
        (Insert, Insert) | (Update(_), Update(_)) | (Delete, Delete)
    )
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};

    use crate::{store::Catalog, trigger::TriggerDefinition};

    fn sample_trigger(name: &str, table: &str) -> TriggerDefinition {
        TriggerDefinition::new(
            name.to_string(),
            TriggerTiming::Before,
            TriggerEvent::Insert,
            table.to_string(),
            TriggerGranularity::Row,
            None,
            TriggerAction::RawSql("SELECT 1".to_string()),
        )
    }

    #[test]
    fn has_any_triggers_tracks_trigger_collection() {
        let mut catalog = Catalog::new();

        // Empty catalog: the executor hot-path guard must short-circuit here.
        assert!(!catalog.has_any_triggers());

        catalog.create_trigger(sample_trigger("t1", "users")).unwrap();
        assert!(catalog.has_any_triggers());

        // Still true with multiple triggers on different tables.
        catalog
            .create_trigger(sample_trigger("t2", "orders"))
            .unwrap();
        assert!(catalog.has_any_triggers());

        // Dropping all triggers returns to the O(1) false fast path.
        catalog.drop_trigger("t1").unwrap();
        catalog.drop_trigger("t2").unwrap();
        assert!(!catalog.has_any_triggers());
    }
}
