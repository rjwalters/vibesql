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
                && event.as_ref().is_none_or(|e| trigger.event == *e)
        })
    }

    /// List all trigger names
    pub fn list_triggers(&self) -> Vec<String> {
        self.triggers.keys().cloned().collect()
    }
}
