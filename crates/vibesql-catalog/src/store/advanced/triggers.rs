//! Trigger management methods.

use crate::{errors::CatalogError, trigger::TriggerDefinition};

/// Compute the schema-scoped storage key for a trigger.
///
/// SQLite scopes trigger names *per schema*, exactly like tables: `main.tr` and
/// `temp.tr` are two distinct triggers that may coexist. The catalog therefore
/// keys triggers by `(schema, name)` rather than by bare `name`, so a `main`
/// trigger and a `temp`/`aux` trigger sharing a name no longer collide
/// (e_update-2.3.1 / e_delete-2.3.2).
///
/// The schema component is normalized case-insensitively, with `None` and
/// `main` collapsing to the default (`main`) schema. The name component
/// preserves the (already parser-normalized) identifier spelling so this keeps
/// the previous exact-name collision semantics *within* a schema. A control
/// character separates the two parts so a schema/name that happens to contain a
/// `.` cannot forge a different key.
fn trigger_storage_key(schema: Option<&str>, name: &str) -> String {
    let schema =
        schema.map(|s| s.to_ascii_lowercase()).unwrap_or_else(|| crate::DEFAULT_SCHEMA.to_string());
    format!("{schema}\u{1f}{name}")
}

impl super::super::Catalog {
    // ============================================================================
    // Trigger Management Methods
    // ============================================================================

    /// Create a TRIGGER.
    ///
    /// The collision check is scoped to the trigger's own schema (see
    /// [`trigger_storage_key`]): creating `temp.tr1` when a `main.tr1` already
    /// exists succeeds, matching SQLite's per-schema trigger namespace.
    pub fn create_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let key = trigger_storage_key(trigger.schema.as_deref(), &trigger.name);
        if self.triggers.contains_key(&key) {
            return Err(CatalogError::TriggerAlreadyExists(trigger.name));
        }
        let trigger_schema =
            trigger.schema.clone().unwrap_or_else(|| crate::DEFAULT_SCHEMA.to_string());
        let trigger_name = trigger.name.clone();
        self.triggers.insert(key, trigger);
        self.record_creation_seq(&trigger_schema, &trigger_name);
        Ok(())
    }

    /// Get a TRIGGER definition by (unqualified) name.
    ///
    /// Triggers are stored per schema, so an unqualified name can match more than
    /// one entry. This resolves them in SQLite's unqualified search order —
    /// `temp` first, then `main`, then any other (attached) schema — returning
    /// the first match. Callers that know the schema should prefer
    /// [`Catalog::get_trigger_in_schema`].
    pub fn get_trigger(&self, name: &str) -> Option<&TriggerDefinition> {
        if let Some(trigger) = self.triggers.get(&trigger_storage_key(Some("temp"), name)) {
            return Some(trigger);
        }
        if let Some(trigger) = self.triggers.get(&trigger_storage_key(None, name)) {
            return Some(trigger);
        }
        self.triggers.values().find(|t| t.name == name)
    }

    /// Get a TRIGGER definition scoped to a specific schema.
    ///
    /// `schema` is the logical schema label a trigger carries
    /// ([`TriggerDefinition::schema`]): `None`/`Some("main")` for the main schema,
    /// `Some("temp")` for the temp schema, or an attached schema name. This never
    /// falls through to another schema, so `main.tr` and `temp.tr` are
    /// distinguishable.
    pub fn get_trigger_in_schema(
        &self,
        name: &str,
        schema: Option<&str>,
    ) -> Option<&TriggerDefinition> {
        self.triggers.get(&trigger_storage_key(schema, name))
    }

    /// Returns true if a trigger of `name` exists in the given schema.
    ///
    /// Used by the executor's `CREATE TRIGGER` path to enforce SQLite's
    /// per-schema "trigger already exists" rule without colliding across schemas.
    pub fn trigger_exists_in_schema(&self, name: &str, schema: Option<&str>) -> bool {
        self.triggers.contains_key(&trigger_storage_key(schema, name))
    }

    /// Iterate over every trigger definition in the catalog, regardless of
    /// schema. Preferred over `list_triggers()` + `get_trigger()` by callers
    /// (e.g. persistence) that must see *every* trigger unambiguously, since a
    /// name-only `get_trigger` can only return one of several same-named triggers
    /// living in different schemas.
    pub fn iter_triggers(&self) -> impl Iterator<Item = &TriggerDefinition> {
        self.triggers.values()
    }

    /// Update a TRIGGER (for ALTER TRIGGER operations)
    pub fn update_trigger(&mut self, trigger: TriggerDefinition) -> Result<(), CatalogError> {
        let key = trigger_storage_key(trigger.schema.as_deref(), &trigger.name);
        if !self.triggers.contains_key(&key) {
            return Err(CatalogError::TriggerNotFound(trigger.name));
        }
        self.triggers.insert(key, trigger);
        Ok(())
    }

    /// Drop a TRIGGER by (unqualified) name.
    ///
    /// Resolves the target in SQLite's unqualified search order (`temp`, then
    /// `main`, then any other schema) and removes the first match.
    pub fn drop_trigger(&mut self, name: &str) -> Result<(), CatalogError> {
        let key = if self.triggers.contains_key(&trigger_storage_key(Some("temp"), name)) {
            trigger_storage_key(Some("temp"), name)
        } else if self.triggers.contains_key(&trigger_storage_key(None, name)) {
            trigger_storage_key(None, name)
        } else {
            match self.triggers.iter().find(|(_, t)| t.name == name) {
                Some((k, _)) => k.clone(),
                None => return Err(CatalogError::TriggerNotFound(name.to_string())),
            }
        };
        self.triggers
            .remove(&key)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TriggerNotFound(name.to_string()))
    }

    /// Drop a TRIGGER scoped to a specific schema (`DROP TRIGGER main.tr1` /
    /// `temp.tr1` / `<attached>.tr1` — #6310). Never falls through to another
    /// schema, unlike the unqualified [`Catalog::drop_trigger`].
    ///
    /// `schema` follows the same convention as
    /// [`Catalog::get_trigger_in_schema`]: `None`/`Some("main")` for the main
    /// schema, `Some("temp")` for the temp schema, or an attached schema name.
    pub fn drop_trigger_in_schema(
        &mut self,
        name: &str,
        schema: Option<&str>,
    ) -> Result<(), CatalogError> {
        self.triggers
            .remove(&trigger_storage_key(schema, name))
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

    /// Drop all triggers defined *on* a table (called when dropping the table).
    ///
    /// SQLite drops every trigger whose `ON <table>` target is the dropped table
    /// (verified against sqlite3 3.51.0):
    ///
    /// ```sql
    /// CREATE TEMP TABLE t(a);
    /// CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;
    /// DROP TABLE t;   -- tr is gone
    /// ```
    ///
    /// A trigger merely *referencing* the table from its body (but defined ON a
    /// different table) is NOT dropped, and a view referencing the table is NOT
    /// dropped either — those are out of scope here and handled by their own
    /// lifecycle rules.
    ///
    /// Schema-aware (mirrors [`Catalog::drop_table_indexes`], #5513): the dropped
    /// table's owning schema is resolved with the same temp-shadows-main order as
    /// table lookup, and a trigger is removed only when it is *bound* to that same
    /// schema. This keeps a `main` trigger on `main.t` from being dropped when a
    /// same-named `temp.t` is dropped, and vice versa (triggerD-3.1/3.2 binding).
    ///
    /// Must be called *before* the table is removed from the catalog so trigger
    /// binding can still resolve the target table's schema. `table_name` may be
    /// schema-qualified (e.g. `temp.t`) to target a specific schema.
    ///
    /// Returns the names of the dropped triggers.
    pub fn drop_table_triggers(&mut self, table_name: &str) -> Vec<String> {
        // Resolve which schema the table being dropped lives in, mirroring
        // `drop_table_indexes`. If the table is already gone, fall back to a
        // name-only match across schemas.
        let resolved_schema = self.resolve_table_schema_name(table_name);

        let (bare_table_name, schema_filter) =
            if let Some((schema_part, table_part)) = table_name.split_once('.') {
                (table_part.to_string(), Some(self.resolve_schema_name(schema_part).to_string()))
            } else {
                (table_name.to_string(), resolved_schema)
            };

        let keys_to_remove: Vec<String> = self
            .triggers
            .iter()
            .filter(|(_, trigger)| {
                // Only triggers defined ON the dropped table (case-insensitive,
                // SQLite-compatible).
                if !trigger.table_name.eq_ignore_ascii_case(&bare_table_name) {
                    return false;
                }
                match &schema_filter {
                    // Drop only triggers bound to the same internal schema as the
                    // dropped table. When the trigger's binding cannot be resolved
                    // (e.g. its target was already gone), drop it — its only
                    // anchor was the table now being removed.
                    Some(schema) => self
                        .trigger_bound_schema(trigger)
                        .is_none_or(|bound| bound.eq_ignore_ascii_case(schema)),
                    // No resolvable owning schema for the dropped table: match by
                    // table name only (mirrors the index-drop fallback).
                    None => true,
                }
            })
            .map(|(key, _)| key.clone())
            .collect();

        keys_to_remove
            .into_iter()
            .filter_map(|key| self.triggers.remove(&key).map(|trigger| trigger.name))
            .collect()
    }

    /// Drop all INSTEAD OF triggers defined *on* a view (called when dropping the
    /// view).
    ///
    /// SQLite drops every trigger whose `ON <view>` target is the dropped view —
    /// an INSTEAD OF trigger cannot outlive the view it is attached to (verified
    /// against sqlite3 3.51.0):
    ///
    /// ```sql
    /// CREATE VIEW v AS SELECT a FROM base;
    /// CREATE TRIGGER v_ins INSTEAD OF INSERT ON v BEGIN INSERT INTO base VALUES(NEW.a); END;
    /// DROP VIEW v;   -- v_ins is gone; recreating v + v_ins succeeds
    /// ```
    ///
    /// This is the view analogue of [`Catalog::drop_table_triggers`] (#5597). It
    /// is kept separate because views live in a single flat map (not the
    /// per-schema table maps), so the table-based [`trigger_bound_schema`]
    /// resolution does not apply to a view target. Instead, schema-awareness is
    /// derived directly from the `temp`/`main` tag both the view and the trigger
    /// carry: a temp trigger on a temp view, and a main trigger on a main view.
    /// This keeps a `main` INSTEAD OF trigger on a `main` view from being dropped
    /// when a same-named `temp` view is dropped, and vice versa (temp shadows
    /// main).
    ///
    /// Must be called *before* the view is removed from the catalog. `view_name`
    /// is the bare (unqualified) view name; `view_is_temp` indicates whether the
    /// dropped view lives in the temp schema (`ViewDefinition::is_temp`).
    ///
    /// Returns the names of the dropped triggers.
    pub fn drop_view_triggers(&mut self, view_name: &str, view_is_temp: bool) -> Vec<String> {
        // A bare/qualified view name: only the last component is the view name
        // (qualified DROP VIEW resolves to the supplied schema, but the catalog
        // stores views by bare name).
        let bare_view_name = view_name.rsplit_once('.').map_or(view_name, |(_, n)| n);

        let keys_to_remove: Vec<String> = self
            .triggers
            .iter()
            .filter(|(_, trigger)| {
                // Only INSTEAD OF triggers defined ON the dropped view
                // (case-insensitive, SQLite-compatible). INSTEAD OF is the only
                // timing valid on a view, but checking it keeps this strictly
                // view-scoped and never disturbs table triggers.
                trigger.timing == vibesql_ast::TriggerTiming::InsteadOf
                    && trigger.table_name.eq_ignore_ascii_case(bare_view_name)
                    // Temp-shadows-main: drop only triggers whose schema matches
                    // the dropped view's schema (temp trigger <-> temp view,
                    // main trigger <-> main view).
                    && trigger.is_temp() == view_is_temp
            })
            .map(|(key, _)| key.clone())
            .collect();

        keys_to_remove
            .into_iter()
            .filter_map(|key| self.triggers.remove(&key).map(|trigger| trigger.name))
            .collect()
    }

    /// List all trigger names.
    ///
    /// Returns the trigger identifiers (not the internal schema-scoped storage
    /// keys). With per-schema keying a name may appear more than once (same name
    /// in `main` and `temp`); callers that need to disambiguate should use
    /// [`Catalog::iter_triggers`] or [`Catalog::get_trigger_in_schema`].
    pub fn list_triggers(&self) -> Vec<String> {
        self.triggers.values().map(|t| t.name.clone()).collect()
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
    matches!((a, b), (Insert, Insert) | (Update(_), Update(_)) | (Delete, Delete))
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
    use vibesql_types::DataType;

    use crate::{
        column::ColumnSchema, errors::CatalogError, store::Catalog, table::TableSchema,
        trigger::TriggerDefinition,
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

    /// An INSTEAD OF INSERT trigger on `view` (the only timing valid on a view).
    fn instead_of_trigger(name: &str, view: &str) -> TriggerDefinition {
        TriggerDefinition::new(
            name.to_string(),
            TriggerTiming::InsteadOf,
            TriggerEvent::Insert,
            view.to_string(),
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
        catalog.create_trigger(sample_trigger("t2", "orders")).unwrap();
        assert!(catalog.has_any_triggers());

        // Dropping all triggers returns to the O(1) false fast path.
        catalog.drop_trigger("t1").unwrap();
        catalog.drop_trigger("t2").unwrap();
        assert!(!catalog.has_any_triggers());
    }

    /// A `main` trigger and a `temp` trigger sharing a name coexist without a
    /// spurious "already exists" collision — triggers are keyed per schema
    /// (e_update-2.3.1 / e_delete-2.3.2).
    #[test]
    fn triggers_are_scoped_per_schema() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        // main.tr1 first, then temp.tr1 — no collision across schemas.
        catalog
            .create_trigger(sample_trigger("tr1", "t").with_schema(Some("main".to_string())))
            .unwrap();
        catalog
            .create_trigger(sample_trigger("tr1", "t").with_schema(Some("temp".to_string())))
            .unwrap();

        // Both are retrievable by their own schema.
        assert!(catalog.get_trigger_in_schema("tr1", Some("main")).is_some());
        assert!(catalog.get_trigger_in_schema("tr1", Some("temp")).is_some());
        assert!(catalog.trigger_exists_in_schema("tr1", Some("temp")));

        // A same-schema duplicate still collides.
        assert!(matches!(
            catalog
                .create_trigger(sample_trigger("tr1", "t").with_schema(Some("main".to_string()))),
            Err(CatalogError::TriggerAlreadyExists(_))
        ));

        // None (default) and Some("main") address the same schema slot.
        assert!(catalog.trigger_exists_in_schema("tr1", None));

        // Unqualified DROP resolves temp-first (SQLite search order): drops
        // temp.tr1, leaving main.tr1.
        catalog.drop_trigger("tr1").unwrap();
        assert!(catalog.get_trigger_in_schema("tr1", Some("temp")).is_none());
        assert!(catalog.get_trigger_in_schema("tr1", Some("main")).is_some());
        catalog.drop_trigger("tr1").unwrap();
        assert!(!catalog.has_any_triggers());
    }

    /// A default-schema trigger (`schema = None`) is addressable as `main` and
    /// does not collide with a `temp` namesake — the `None`/`main` collapse.
    #[test]
    fn default_schema_trigger_collapses_to_main() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        catalog.create_trigger(sample_trigger("tr", "t")).unwrap(); // schema None
        catalog
            .create_trigger(sample_trigger("tr", "t").with_schema(Some("temp".to_string())))
            .unwrap();

        // list/iter see both entries.
        assert_eq!(catalog.iter_triggers().count(), 2);
        // A None-schema create now collides (same slot as the first).
        assert!(catalog.create_trigger(sample_trigger("tr", "t")).is_err());
    }

    fn names_in_schema(catalog: &Catalog, table: &str, dml_schema: Option<&str>) -> Vec<String> {
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

    /// `drop_table_triggers` removes triggers defined ON the dropped table and
    /// leaves triggers that are on a different table (even if they reference the
    /// dropped one in their body) untouched — matching sqlite3 3.51.0.
    #[test]
    fn drop_table_triggers_removes_only_triggers_on_the_table() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let col = || ColumnSchema::new("x".to_string(), DataType::Integer, true);
        catalog.create_table(TableSchema::new("t".to_string(), vec![col()])).unwrap();
        catalog.create_table(TableSchema::new("other".to_string(), vec![col()])).unwrap();

        // tr is ON t (should be dropped); tr2 is ON other (should survive even
        // though sqlite3 would also let it reference t in its body).
        catalog.create_trigger(sample_trigger("tr", "t")).unwrap();
        catalog.create_trigger(sample_trigger("tr2", "other")).unwrap();

        let dropped = catalog.drop_table_triggers("t");
        assert_eq!(dropped, vec!["tr".to_string()]);
        assert!(catalog.get_trigger("tr").is_none());
        assert!(catalog.get_trigger("tr2").is_some());
    }

    /// Dropping a `temp` table removes its temp trigger but leaves a same-named
    /// `main` table's trigger intact (schema-aware binding, mirrors the
    /// index-drop schema isolation).
    #[test]
    fn drop_table_triggers_is_schema_aware() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let col = || ColumnSchema::new("x".to_string(), DataType::Integer, true);
        // t exists in BOTH main and temp.
        catalog.create_table(TableSchema::new("t".to_string(), vec![col()])).unwrap();
        let temp_schema = catalog.temp_schema_name().to_string();
        catalog
            .create_table_in_schema(&temp_schema, TableSchema::new("t".to_string(), vec![col()]))
            .unwrap();

        // main trigger bound to main.t; temp trigger bound to temp.t.
        catalog
            .create_trigger(sample_trigger("rmain", "t").with_schema(Some("main".to_string())))
            .unwrap();
        catalog
            .create_trigger(sample_trigger("rtemp", "t").with_schema(Some("temp".to_string())))
            .unwrap();

        // Dropping the temp table removes only the temp-bound trigger.
        let dropped = catalog.drop_table_triggers("temp.t");
        assert_eq!(dropped, vec!["rtemp".to_string()]);
        assert!(catalog.get_trigger("rtemp").is_none());
        assert!(catalog.get_trigger("rmain").is_some());

        // Dropping the main table now removes the main-bound trigger.
        let dropped_main = catalog.drop_table_triggers("main.t");
        assert_eq!(dropped_main, vec!["rmain".to_string()]);
        assert!(catalog.get_trigger("rmain").is_none());
    }

    /// `drop_view_triggers` removes the INSTEAD OF triggers defined ON the
    /// dropped view and leaves triggers on a *different* view untouched —
    /// matching sqlite3 3.51.0 (DROP VIEW cascade-drops its INSTEAD OF triggers).
    #[test]
    fn drop_view_triggers_removes_only_triggers_on_the_view() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        // v_ins is ON v (should be dropped); v2_ins is ON v2 (should survive).
        catalog.create_trigger(instead_of_trigger("v_ins", "v")).unwrap();
        catalog.create_trigger(instead_of_trigger("v2_ins", "v2")).unwrap();

        let dropped = catalog.drop_view_triggers("v", false);
        assert_eq!(dropped, vec!["v_ins".to_string()]);
        assert!(catalog.get_trigger("v_ins").is_none());
        assert!(catalog.get_trigger("v2_ins").is_some());
    }

    /// A table trigger (non-INSTEAD OF) sharing a view's name is never disturbed
    /// by `drop_view_triggers` — only INSTEAD OF triggers ON the view are dropped.
    #[test]
    fn drop_view_triggers_ignores_table_triggers() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        // A BEFORE INSERT (table) trigger that happens to share the dropped view's
        // name must be left alone; only INSTEAD OF triggers belong to views.
        catalog.create_trigger(sample_trigger("tbl_tr", "v")).unwrap();
        catalog.create_trigger(instead_of_trigger("v_ins", "v")).unwrap();

        let dropped = catalog.drop_view_triggers("v", false);
        assert_eq!(dropped, vec!["v_ins".to_string()]);
        assert!(catalog.get_trigger("v_ins").is_none());
        assert!(catalog.get_trigger("tbl_tr").is_some());
    }

    /// Dropping a `temp` view removes its temp INSTEAD OF trigger but leaves a
    /// same-named `main` view's trigger intact (temp shadows main), and vice
    /// versa — mirroring the table-trigger schema isolation.
    #[test]
    fn drop_view_triggers_is_schema_aware() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        // main trigger on main view v; temp trigger on temp view v.
        catalog.create_trigger(instead_of_trigger("vmain", "v")).unwrap();
        catalog
            .create_trigger(instead_of_trigger("vtemp", "v").with_schema(Some("temp".to_string())))
            .unwrap();

        // Dropping the temp view removes only the temp-bound trigger.
        let dropped = catalog.drop_view_triggers("v", true);
        assert_eq!(dropped, vec!["vtemp".to_string()]);
        assert!(catalog.get_trigger("vtemp").is_none());
        assert!(catalog.get_trigger("vmain").is_some());

        // Dropping the main view now removes the main-bound trigger.
        let dropped_main = catalog.drop_view_triggers("v", false);
        assert_eq!(dropped_main, vec!["vmain".to_string()]);
        assert!(catalog.get_trigger("vmain").is_none());
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
