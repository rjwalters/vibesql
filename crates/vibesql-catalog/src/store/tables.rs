//! Table management operations for the catalog.
//!
//! This module handles all table-related operations including creation,
//! modification, deletion, and queries.

#![allow(clippy::only_used_in_recursion)]

use crate::{errors::CatalogError, table::TableSchema};
use std::collections::{HashMap, HashSet};

impl super::Catalog {
    /// Check for circular foreign key dependencies that would be created by adding this table.
    ///
    /// Uses depth-first search to detect cycles in the foreign key dependency graph.
    /// Note: Self-referential tables (table references itself) are allowed.
    /// Returns an error if a circular dependency involving multiple tables is detected.
    fn check_circular_foreign_keys(&self, new_table: &TableSchema) -> Result<(), CatalogError> {
        // Build a dependency graph: table_name -> set of tables it depends on (via FK)
        // Excludes self-references (table referencing itself is allowed)
        let mut graph: HashMap<String, HashSet<String>> = HashMap::new();

        // Add existing tables and their FK dependencies
        for table_name in self.list_all_tables() {
            if let Some(table_schema) = self.get_table(&table_name) {
                let normalized_name = if self.case_sensitive_identifiers {
                    table_schema.name.clone()
                } else {
                    table_schema.name.to_uppercase()
                };

                let mut dependencies = HashSet::new();
                for fk in &table_schema.foreign_keys {
                    let parent_table = if self.case_sensitive_identifiers {
                        fk.parent_table.clone()
                    } else {
                        fk.parent_table.to_uppercase()
                    };
                    // Skip self-references - they're allowed
                    if parent_table != normalized_name {
                        dependencies.insert(parent_table);
                    }
                }
                graph.insert(normalized_name, dependencies);
            }
        }

        // Add the new table's dependencies
        let new_table_name = if self.case_sensitive_identifiers {
            new_table.name.clone()
        } else {
            new_table.name.to_uppercase()
        };

        let mut new_dependencies = HashSet::new();
        for fk in &new_table.foreign_keys {
            let parent_table = if self.case_sensitive_identifiers {
                fk.parent_table.clone()
            } else {
                fk.parent_table.to_uppercase()
            };
            // Skip self-references - they're allowed
            if parent_table != new_table_name {
                new_dependencies.insert(parent_table);
            }
        }
        graph.insert(new_table_name.clone(), new_dependencies);

        // Perform DFS from the new table to detect cycles (excluding self-references)
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        if self.has_cycle_dfs(&new_table_name, &graph, &mut visited, &mut rec_stack) {
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

    /// Helper function for cycle detection using depth-first search
    fn has_cycle_dfs(
        &self,
        node: &str,
        graph: &HashMap<String, HashSet<String>>,
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

        // Visit all dependencies
        if let Some(dependencies) = graph.get(node) {
            for dep in dependencies {
                if self.has_cycle_dfs(dep, graph, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(node);
        false
    }

    /// Create a table schema in the current schema.
    pub fn create_table(&mut self, schema: TableSchema) -> Result<(), CatalogError> {
        // Check for circular foreign key dependencies
        self.check_circular_foreign_keys(&schema)?;

        let case_sensitive = self.case_sensitive_identifiers;
        let current_schema = self
            .schemas
            .get_mut(&self.current_schema)
            .ok_or_else(|| CatalogError::SchemaNotFound(self.current_schema.clone()))?;

        current_schema.create_table_with_case_mode(schema, case_sensitive)
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
        let target_schema = self
            .schemas
            .get_mut(schema_name)
            .ok_or_else(|| CatalogError::SchemaNotFound(schema_name.to_string()))?;

        target_schema.create_table_with_case_mode(schema, case_sensitive)
    }

    /// Get a table schema by name (supports qualified names like "schema.table").
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        // Parse qualified name: schema.table or just table
        if let Some((schema_name, table_name)) = name.split_once('.') {
            let normalized_table = self.normalize_identifier(table_name);
            // Find schema with case-insensitive lookup
            self.get_schema_case_insensitive(schema_name).and_then(|schema| {
                schema.get_table(&normalized_table, self.case_sensitive_identifiers)
            })
        } else {
            // Use current schema for unqualified names
            let normalized_table = self.normalize_identifier(name);
            self.schemas.get(&self.current_schema).and_then(|schema| {
                schema.get_table(&normalized_table, self.case_sensitive_identifiers)
            })
        }
    }

    /// Drop a table schema (supports qualified names like "schema.table").
    /// Respects the `case_sensitive_identifiers` setting.
    ///
    /// Note: Triggers are automatically dropped when the associated table is dropped.
    pub fn drop_table(&mut self, name: &str) -> Result<(), CatalogError> {
        // Parse qualified name: schema.table or just table
        let (schema_name_for_lookup, table_name, original_table_name) =
            if let Some((schema_part, table_part)) = name.split_once('.') {
                (schema_part, table_part, table_part)
            } else {
                (self.current_schema.as_str(), name, name)
            };

        let normalized_table = self.normalize_identifier(table_name);

        // Find schema with case-insensitive lookup, then get mutable reference
        let schema_key = if self.case_sensitive_identifiers {
            // Case-sensitive: direct lookup
            if self.schemas.contains_key(schema_name_for_lookup) {
                schema_name_for_lookup.to_string()
            } else {
                return Err(CatalogError::SchemaNotFound(schema_name_for_lookup.to_string()));
            }
        } else {
            // Case-insensitive: find schema key by comparing normalized names
            let normalized_name = schema_name_for_lookup.to_uppercase();
            self.schemas
                .keys()
                .find(|key| key.to_uppercase() == normalized_name)
                .cloned()
                .ok_or_else(|| CatalogError::SchemaNotFound(schema_name_for_lookup.to_string()))?
        };

        // Drop all triggers associated with this table
        // Per SQL standard (R-37808-62273): triggers are automatically dropped when the table is dropped
        // Note: We need to normalize trigger table_name for comparison in case-insensitive mode
        let case_sensitive = self.case_sensitive_identifiers;
        let trigger_names: Vec<String> = self
            .triggers
            .values()
            .filter(|trigger| {
                let trigger_table = if case_sensitive {
                    trigger.table_name.clone()
                } else {
                    trigger.table_name.to_uppercase()
                };
                trigger_table == normalized_table
            })
            .map(|trigger| trigger.name.clone())
            .collect();

        for trigger_name in trigger_names {
            self.triggers.remove(&trigger_name);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnSchema;
    use crate::trigger::TriggerDefinition;
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
    use vibesql_types::DataType;

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
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None), // None = no column list
            table_name: "T1".to_string(),      // Different case than table creation
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
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
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t1".to_string(), // Exact match required in case-sensitive mode
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
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
                timing: TriggerTiming::After,
                event: TriggerEvent::Update(None),
                table_name: "t1".to_string(),
                granularity: TriggerGranularity::Row,
                when_condition: None,
                triggered_action: TriggerAction::RawSql("".to_string()),
                enabled: true,
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
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t1".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
        };
        let trigger2 = TriggerDefinition {
            name: "tr2".to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Update(None),
            table_name: "t2".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("".to_string()),
            enabled: true,
        };
        catalog.create_trigger(trigger1).unwrap();
        catalog.create_trigger(trigger2).unwrap();

        // Drop first table
        catalog.drop_table("t1").unwrap();

        // Verify only t1's trigger was deleted
        assert!(catalog.get_trigger("tr1").is_none());
        assert!(catalog.get_trigger("tr2").is_some());
    }
}
