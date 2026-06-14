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
    ///
    /// This is the schema-unaware variant: it matches every trigger on a table of
    /// the given (bare) name regardless of which schema the trigger or the target
    /// table belongs to. Callers that know the schema the DML actually resolved to
    /// should prefer [`Catalog::get_triggers_for_table_in_schema`] so a `main`
    /// trigger does not fire on a same-named `temp` table and vice versa
    /// (triggerD-3.1/3.2).
    pub fn get_triggers_for_table<'a>(
        &'a self,
        table_name: &'a str,
        event: Option<vibesql_ast::TriggerEvent>,
    ) -> impl Iterator<Item = &'a TriggerDefinition> + 'a {
        self.get_triggers_for_table_in_schema(table_name, event, None)
    }

    /// Get all triggers for a table with a specific event, restricted to the
    /// schema the target table resolved to for the current statement.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to check for triggers
    /// * `event` - Optional trigger event to filter by (Insert, Update, Delete)
    /// * `dml_schema` - Internal schema name (e.g. `main` or `temp_<id>`) that the
    ///   DML target table resolved to, as returned by
    ///   [`Catalog::resolve_table_schema_name`]. When `None`, no schema filtering
    ///   is applied (legacy schema-unaware behavior).
    ///
    /// # Schema-aware firing (triggerD-3.1/3.2)
    /// When a `temp` table shadows a same-named `main` table, a trigger must only
    /// fire for operations on the table it is actually bound to. SQLite binds a
    /// trigger to the table its name resolves to *from the trigger's own schema*:
    /// a `main` trigger binds to `main.<table>` (temp is invisible to it), while a
    /// `temp` trigger binds to `temp.<table>` if it exists, otherwise to
    /// `main.<table>` (temp shadows main for temp-schema name resolution). A
    /// trigger fires only when that bound schema equals the schema the DML target
    /// table resolved to.
    ///
    /// # Returns
    /// Iterator over trigger definitions matching the criteria
    pub fn get_triggers_for_table_in_schema<'a>(
        &'a self,
        table_name: &'a str,
        event: Option<vibesql_ast::TriggerEvent>,
        dml_schema: Option<&'a str>,
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
                // Schema-aware firing: when the caller knows the schema the DML
                // target resolved to, require the trigger's bound schema to match.
                // When either side cannot be resolved, fall back to the legacy
                // name-only match so no existing single-schema behavior regresses.
                && match dml_schema {
                    Some(dml) => self
                        .trigger_bound_schema(trigger)
                        .is_none_or(|bound| bound.eq_ignore_ascii_case(dml)),
                    None => true,
                }
        })
    }

    /// Resolve the internal schema name a trigger is bound to.
    ///
    /// SQLite binds a trigger to the table its (unqualified) `table_name`
    /// resolves to *from the trigger's own schema*:
    /// - A `temp` trigger sees temp-then-main (temp shadows main), so it binds to
    ///   `temp.<table>` when a temp table of that name exists, else `main.<table>`.
    /// - A `main` trigger sees only the main schema (temp is invisible), so it
    ///   binds to `main.<table>`.
    ///
    /// Returns the internal schema name (`main` or `temp_<id>`) the trigger is
    /// bound to, or `None` if the target table cannot be resolved (e.g. it was
    /// dropped); callers treat `None` as "do not schema-filter".
    fn trigger_bound_schema(&self, trigger: &TriggerDefinition) -> Option<String> {
        if trigger.is_temp() {
            // Temp trigger: temp-then-main name resolution.
            self.resolve_table_schema_name(&trigger.table_name)
        } else {
            // Main trigger: only the main schema is visible. Qualify explicitly so
            // temp shadowing does not apply.
            self.resolve_table_schema_name(&format!(
                "{}.{}",
                crate::DEFAULT_SCHEMA,
                trigger.table_name
            ))
        }
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
    use vibesql_types::DataType;

    use crate::{
        column::ColumnSchema, store::Catalog, table::TableSchema, trigger::TriggerDefinition,
    };

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

    fn names_in_schema(
        catalog: &Catalog,
        table: &str,
        dml_schema: Option<&str>,
    ) -> Vec<String> {
        catalog
            .get_triggers_for_table_in_schema(table, Some(TriggerEvent::Insert), dml_schema)
            .map(|t| t.name.clone())
            .collect()
    }

    /// Schema-aware firing (triggerD-3.1/3.2): when a temp table shadows a
    /// same-named main table, a `main` trigger fires only for the main schema and
    /// a `temp` trigger fires only for the temp schema.
    #[test]
    fn get_triggers_for_table_in_schema_respects_trigger_schema() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let col = || ColumnSchema::new("x".to_string(), DataType::Integer, true);
        // t300 exists in BOTH main and temp.
        catalog.create_table(TableSchema::new("t300".to_string(), vec![col()])).unwrap();
        let temp_schema = catalog.temp_schema_name().to_string();
        catalog
            .create_table_in_schema(&temp_schema, TableSchema::new("t300".to_string(), vec![col()]))
            .unwrap();

        // main.r300 bound to main; temp.r301 bound to temp.
        catalog
            .create_trigger(sample_trigger("r300", "t300").with_schema(Some("main".to_string())))
            .unwrap();
        catalog
            .create_trigger(sample_trigger("r301", "t300").with_schema(Some("temp".to_string())))
            .unwrap();

        // Firing on main.t300 -> only the main trigger.
        let main_schema = "main";
        assert_eq!(names_in_schema(&catalog, "t300", Some(main_schema)), vec!["r300".to_string()]);

        // Firing on temp.t300 -> only the temp trigger.
        assert_eq!(names_in_schema(&catalog, "t300", Some(&temp_schema)), vec!["r301".to_string()]);

        // Schema-unaware (None) -> both fire (legacy behavior preserved).
        let mut both = names_in_schema(&catalog, "t300", None);
        both.sort();
        assert_eq!(both, vec!["r300".to_string(), "r301".to_string()]);
    }

    /// A temp trigger on a table that exists only in main binds to (and fires
    /// for) the main schema (temp-then-main name resolution).
    #[test]
    fn temp_trigger_binds_to_main_when_no_temp_table() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let col = ColumnSchema::new("x".to_string(), DataType::Integer, true);
        catalog.create_table(TableSchema::new("solo".to_string(), vec![col])).unwrap();

        catalog
            .create_trigger(sample_trigger("tr", "solo").with_schema(Some("temp".to_string())))
            .unwrap();

        // The temp trigger fires for the main insert (it bound to main.solo).
        assert_eq!(names_in_schema(&catalog, "solo", Some("main")), vec!["tr".to_string()]);
    }
}
