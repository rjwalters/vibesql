//! Index management operations for the catalog
//!
//! This module provides methods for creating, dropping, and querying
//! index metadata in the database catalog.

use crate::{index::IndexMetadata, Catalog, CatalogError};

impl Catalog {
    /// Add an index to the catalog
    pub fn add_index(&mut self, index: IndexMetadata) -> Result<(), CatalogError> {
        let qualified_name = index.qualified_name();

        // Check if index already exists
        if self.indexes.contains_key(&qualified_name) {
            return Err(CatalogError::IndexAlreadyExists {
                index_name: index.name.clone(),
                table_name: index.table_name.clone(),
            });
        }

        // Verify table exists. Use the temp-shadow-aware resolver so an index
        // on an unqualified TEMP table (which lives in the session temp schema,
        // not `main`) is accepted. Previously this only checked the current
        // (main) schema, causing CREATE INDEX on a temp table to fail with
        // `TableNotFound`. See issue #5505.
        let table = match self.get_table(&index.table_name) {
            Some(table) => table,
            None => {
                return Err(CatalogError::TableNotFound { table_name: index.table_name.clone() });
            }
        };

        // Verify all column-based index columns exist in the table
        // Expression indexes skip this validation (they reference expressions, not specific
        // columns)

        for col in &index.columns {
            // Only validate column existence for non-expression index columns
            if let Some(column_name) = col.column_name() {
                // Use case-insensitive column comparison (SQLite behavior)
                // Column names from parser may be uppercase (keywords like TYPE)
                // while stored column names preserve original case from CREATE TABLE
                let column_name_lower = column_name.to_lowercase();
                if !table.columns.iter().any(|c| c.name.to_lowercase() == column_name_lower) {
                    return Err(CatalogError::ColumnNotFound {
                        column_name: column_name.to_string(),
                        table_name: index.table_name.clone(),
                    });
                }
            }
            // Expression index columns are allowed without column validation
            // (they may reference multiple columns or use literals)
        }

        let index_schema = index.schema.clone();
        let index_name = index.name.clone();
        self.indexes.insert(qualified_name, index);
        self.record_creation_seq(&index_schema, &index_name);
        Ok(())
    }

    /// Remove an index from the catalog.
    ///
    /// `table_name` may carry a schema (`schema.table`) — used by DROP INDEX so
    /// a temp index and a same-named main index on a same-named table can be
    /// dropped independently. When unqualified, the index is matched across all
    /// schemas (index names are unique within a database in practice).
    pub fn drop_index(
        &mut self,
        table_name: &str,
        index_name: &str,
    ) -> Result<IndexMetadata, CatalogError> {
        let key = self.resolve_index_key(table_name, index_name).ok_or_else(|| {
            CatalogError::IndexNotFound {
                index_name: index_name.to_string(),
                table_name: table_name.to_string(),
            }
        })?;

        self.indexes.shift_remove(&key).ok_or_else(|| CatalogError::IndexNotFound {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
        })
    }

    /// Get an index by table and index name.
    ///
    /// `table_name` may be schema-qualified (`schema.table`) to disambiguate a
    /// temp index from a same-named main index; otherwise the first match
    /// across schemas is returned.
    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<&IndexMetadata> {
        let key = self.resolve_index_key(table_name, index_name)?;
        self.indexes.get(&key)
    }

    /// Resolve the catalog key (`schema.table.index`) for the given table/index
    /// pair. Accepts a schema-qualified `table_name` for exact targeting, or an
    /// unqualified one, in which case the first matching index (by table + name,
    /// case-insensitively) is used.
    fn resolve_index_key(&self, table_name: &str, index_name: &str) -> Option<String> {
        if let Some((schema_part, table_part)) = table_name.split_once('.') {
            let resolved_schema = self.resolve_schema_name(schema_part);
            // Try the exact key first.
            let exact = format!("{}.{}.{}", resolved_schema, table_part, index_name);
            if self.indexes.contains_key(&exact) {
                return Some(exact);
            }
            // Fall back to a case-insensitive search constrained to that schema.
            let resolved_schema_lc = resolved_schema.to_lowercase();
            let table_lc = table_part.to_lowercase();
            let index_lc = index_name.to_lowercase();
            return self
                .indexes
                .iter()
                .find(|(_, idx)| {
                    idx.schema.to_lowercase() == resolved_schema_lc
                        && idx.table_name.to_lowercase() == table_lc
                        && idx.name.to_lowercase() == index_lc
                })
                .map(|(k, _)| k.clone());
        }

        // Unqualified: match by table + index name across all schemas.
        let table_lc = table_name.to_lowercase();
        let index_lc = index_name.to_lowercase();
        self.indexes
            .iter()
            .find(|(_, idx)| {
                idx.table_name.to_lowercase() == table_lc && idx.name.to_lowercase() == index_lc
            })
            .map(|(k, _)| k.clone())
    }

    /// Get all indexes for a specific table.
    ///
    /// Matches by bare table name across all schemas. For schema-scoped views,
    /// prefer [`Catalog::get_schema_indexes`].
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<&IndexMetadata> {
        self.indexes.values().filter(|index| index.table_name == table_name).collect()
    }

    /// Get all indexes owned by a specific schema (e.g. `main` or `temp_123`).
    ///
    /// Drives the per-schema split between `sqlite_master` (main objects) and
    /// `sqlite_temp_master` (temp objects). See issue #5513.
    pub fn get_schema_indexes(&self, schema_name: &str) -> Vec<&IndexMetadata> {
        let resolved = self.resolve_schema_name(schema_name);
        self.indexes.values().filter(|index| index.schema == resolved).collect()
    }

    /// Check whether an index with `index_name` exists in `schema_name`.
    ///
    /// SQLite places tables, indexes, and views in a single object namespace per
    /// schema, so CREATE TABLE must reject a name already taken by an index in
    /// the same schema (`there is already an index named X`). This is the
    /// table-side counterpart to the index-side `there is already a table named
    /// X` check.
    ///
    /// The lookup is schema-aware (so a temp index does not collide with a main
    /// table and vice versa — issue #5513) and case-insensitive (SQLite folds
    /// identifiers regardless of quoting — issue #5553). `schema_name` may be a
    /// user-facing alias (e.g. `temp`); it is resolved to the internal schema
    /// name before comparison.
    pub fn index_name_exists_in_schema(&self, schema_name: &str, index_name: &str) -> bool {
        let resolved_schema = self.resolve_schema_name(schema_name).to_lowercase();
        let index_lc = index_name.to_lowercase();
        self.indexes.values().any(|idx| {
            idx.schema.to_lowercase() == resolved_schema && idx.name.to_lowercase() == index_lc
        })
    }

    /// Look up an index by its name alone (across all tables in this catalog).
    ///
    /// The storage layer keys its index manager by name only (without a table
    /// qualifier), so query-time code that has just an index name often needs
    /// to consult the catalog for properties such as `is_partial()` or the
    /// expression list. Returns the first match — index names are unique
    /// within a database in SQLite-compatible mode.
    pub fn find_index_by_name(&self, index_name: &str) -> Option<&IndexMetadata> {
        // Index names are case-insensitive in SQLite. We match the stored
        // catalog name case-insensitively so callers that pass either the
        // original-case or lowercased form succeed.
        let target = index_name.to_lowercase();
        self.indexes.values().find(|index| index.name.to_lowercase() == target)
    }

    /// Attach (or clear) a partial-index WHERE clause on an existing index.
    ///
    /// Used by persistence/recovery paths that recreate indexes through the
    /// no-WHERE-clause path and then need to graft the partial predicate on
    /// afterwards. Returns `true` if an index with the given name was found
    /// and updated.
    pub fn set_index_where_clause(
        &mut self,
        index_name: &str,
        where_clause: Option<vibesql_ast::Expression>,
    ) -> bool {
        let target = index_name.to_lowercase();
        if let Some(meta) = self.indexes.values_mut().find(|m| m.name.to_lowercase() == target) {
            meta.where_clause = where_clause.map(Box::new);
            true
        } else {
            false
        }
    }

    /// Propagate `ALTER TABLE <table> RENAME COLUMN old TO new` into the
    /// metadata of every index on `table_name` (matched case-insensitively,
    /// across schemas — index metadata rides with its table).
    ///
    /// Rewrites plain column-name entries, column references inside
    /// expression-index ASTs, and partial-index WHERE predicates. Without
    /// this, `sqlite_master` keeps rendering the old column name and the next
    /// binary checkpoint persists index metadata that no longer resolves
    /// against the renamed table, making the database unopenable (issue
    /// #5877).
    ///
    /// Returns the number of indexes whose metadata changed.
    pub fn rename_column_in_table_indexes(
        &mut self,
        table_name: &str,
        old_column: &str,
        new_column: &str,
    ) -> usize {
        use vibesql_ast::rename::rename_column_in_expression;

        use crate::index::IndexedColumn;

        let table_lc = table_name.to_lowercase();
        let mut updated = 0;
        for index in self.indexes.values_mut().filter(|i| i.table_name.to_lowercase() == table_lc) {
            let mut changed = false;
            for col in index.columns.iter_mut() {
                match col {
                    IndexedColumn::Column { column_name, .. } => {
                        if column_name.eq_ignore_ascii_case(old_column) {
                            *column_name = new_column.to_string();
                            changed = true;
                        }
                    }
                    IndexedColumn::Expression { expr, .. } => {
                        changed |= rename_column_in_expression(expr, old_column, new_column);
                    }
                }
            }
            if let Some(where_expr) = index.where_clause.as_deref_mut() {
                changed |= rename_column_in_expression(where_expr, old_column, new_column);
            }
            if changed {
                updated += 1;
            }
        }
        updated
    }

    /// List all indexes in the catalog
    pub fn list_all_indexes(&self) -> Vec<&IndexMetadata> {
        self.indexes.values().collect()
    }

    /// Drop all indexes associated with a table (called when dropping a table).
    ///
    /// Schema-aware (#5513): the dropped table's owning schema is resolved using
    /// the same temp-shadows-main order as table lookup, so dropping a TEMP
    /// table removes only that temp table's indexes and leaves a same-named main
    /// table's indexes intact (and vice versa). `table_name` may be
    /// schema-qualified to target a specific schema.
    pub fn drop_table_indexes(&mut self, table_name: &str) -> Vec<IndexMetadata> {
        // Resolve which schema the table being dropped lives in. If the table is
        // already gone from the catalog (callers sometimes drop indexes after
        // the table), fall back to matching by bare table name across schemas.
        let resolved_schema = self.resolve_table_schema_name(table_name);

        let (bare_table_name, schema_filter) =
            if let Some((schema_part, table_part)) = table_name.split_once('.') {
                (table_part.to_string(), Some(self.resolve_schema_name(schema_part).to_string()))
            } else {
                (table_name.to_string(), resolved_schema)
            };

        let bare_table_lc = bare_table_name.to_lowercase();

        let indexes_to_remove: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, index)| {
                if index.table_name.to_lowercase() != bare_table_lc {
                    return false;
                }
                match &schema_filter {
                    Some(schema) => index.schema.eq_ignore_ascii_case(schema),
                    // No resolvable owning schema: match by table name only
                    // (preserves the pre-#5513 fallback behaviour).
                    None => true,
                }
            })
            .map(|(qualified_name, _)| qualified_name.clone())
            .collect();

        indexes_to_remove
            .into_iter()
            .filter_map(|qualified_name| self.indexes.shift_remove(&qualified_name))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::{IndexType, IndexedColumn, SortOrder},
        ColumnSchema, TableSchema,
    };

    fn create_test_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        // Use case-insensitive identifiers to match create_table() behavior
        catalog.set_case_sensitive_identifiers(false);

        use vibesql_types::DataType;

        // Create a test table
        let table = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    true,
                ),
                ColumnSchema::new(
                    "email".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true,
                ),
            ],
        );

        catalog.schemas.get_mut(&catalog.current_schema).unwrap().create_table(table).unwrap();

        catalog
    }

    #[test]
    fn test_add_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        assert!(catalog.add_index(index).is_ok());
        assert!(catalog.get_index("users", "idx_name").is_some());
    }

    #[test]
    fn test_add_duplicate_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        catalog.add_index(index.clone()).unwrap();
        let result = catalog.add_index(index);

        assert!(matches!(result, Err(CatalogError::IndexAlreadyExists { .. })));
    }

    #[test]
    fn test_add_index_nonexistent_table() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "nonexistent".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        let result = catalog.add_index(index);
        assert!(matches!(result, Err(CatalogError::TableNotFound { .. })));
    }

    #[test]
    fn test_add_index_nonexistent_column() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_age".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("age".to_string(), SortOrder::Ascending)], /* Column doesn't exist */
            false,
        );

        let result = catalog.add_index(index);
        assert!(matches!(result, Err(CatalogError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_drop_index() {
        let mut catalog = create_test_catalog();

        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        catalog.add_index(index).unwrap();
        assert!(catalog.drop_index("users", "idx_name").is_ok());
        assert!(catalog.get_index("users", "idx_name").is_none());
    }

    #[test]
    fn test_get_table_indexes() {
        let mut catalog = create_test_catalog();

        let index1 = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        let index2 = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true,
        );

        catalog.add_index(index1).unwrap();
        catalog.add_index(index2).unwrap();

        let indexes = catalog.get_table_indexes("users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn test_drop_table_indexes() {
        let mut catalog = create_test_catalog();

        let index1 = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        );

        let index2 = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true,
        );

        catalog.add_index(index1).unwrap();
        catalog.add_index(index2).unwrap();

        let dropped = catalog.drop_table_indexes("users");
        assert_eq!(dropped.len(), 2);
        assert!(catalog.get_table_indexes("users").is_empty());
    }

    #[test]
    fn test_add_expression_index() {
        let mut catalog = create_test_catalog();

        // Expression indexes can be added without column validation
        // Use a simple binary expression for testing: 1 + 1
        let expr = vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Plus,
            left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        };
        let index = IndexMetadata::new(
            "idx_expr".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_expression(expr, SortOrder::Ascending)],
            false,
        );

        // Expression index should be added successfully
        assert!(catalog.add_index(index).is_ok());
        assert!(catalog.get_index("users", "idx_expr").is_some());
    }

    #[test]
    fn test_temp_and_main_index_coexist() {
        // #5513: a temp-schema index and a main-schema index can share a name,
        // even on a same-named table, without colliding in the catalog.
        let mut catalog = create_test_catalog();

        // Create a shadowing temp.users table so both schemas hold `users`.
        let temp_schema = catalog.temp_schema_name().to_string();
        let temp_table = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            )],
        );
        catalog.create_table_in_schema(&temp_schema, temp_table).unwrap();

        let main_idx = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        ); // defaults to schema "main"
        let temp_idx = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
            false,
        )
        .with_schema(temp_schema.clone());

        catalog.add_index(main_idx).unwrap();
        // Same name, same table name, different schema -> must NOT collide.
        catalog.add_index(temp_idx).expect("temp index should coexist with main index");

        // Both registered, keyed independently by schema.
        let main_only = catalog.get_schema_indexes("main");
        let temp_only = catalog.get_schema_indexes(&temp_schema);
        assert_eq!(main_only.len(), 1);
        assert_eq!(temp_only.len(), 1);
        assert_eq!(main_only[0].schema(), "main");
        assert_eq!(temp_only[0].schema(), temp_schema);

        // get_index can disambiguate via a schema-qualified table name.
        assert_eq!(catalog.get_index("main.users", "idx_name").unwrap().schema(), "main");
        assert_eq!(
            catalog.get_index(&format!("{}.users", temp_schema), "idx_name").unwrap().schema(),
            temp_schema
        );
    }

    #[test]
    fn test_drop_table_indexes_is_schema_scoped() {
        // #5513: dropping a temp table removes only the temp-schema index,
        // leaving the same-named main-schema index intact.
        let mut catalog = create_test_catalog();
        let temp_schema = catalog.temp_schema_name().to_string();
        let temp_table = TableSchema::new(
            "users".to_string(),
            vec![ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            )],
        );
        catalog.create_table_in_schema(&temp_schema, temp_table).unwrap();

        catalog
            .add_index(IndexMetadata::new(
                "idx_name".to_string(),
                "users".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
                false,
            ))
            .unwrap();
        catalog
            .add_index(
                IndexMetadata::new(
                    "idx_name".to_string(),
                    "users".to_string(),
                    IndexType::BTree,
                    vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
                    false,
                )
                .with_schema(temp_schema.clone()),
            )
            .unwrap();

        // Drop the temp `users` table's indexes (temp shadows main for the bare
        // name) -> only the temp index goes.
        let dropped = catalog.drop_table_indexes("users");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].schema(), temp_schema);

        // Main index survives.
        assert_eq!(catalog.get_schema_indexes("main").len(), 1);
        assert!(catalog.get_schema_indexes(&temp_schema).is_empty());
    }

    #[test]
    fn test_index_name_exists_in_schema_is_schema_aware_and_case_insensitive() {
        // #5613: index/table namespace collision check must be schema-aware and
        // case-insensitive (#5553), and must not leak a temp index into main.
        let mut catalog = create_test_catalog();
        let temp_schema = catalog.temp_schema_name().to_string();

        catalog
            .add_index(IndexMetadata::new(
                "idx_name".to_string(),
                "users".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("name".to_string(), SortOrder::Ascending)],
                false,
            ))
            .unwrap(); // defaults to schema "main"

        // Present in main, in either case.
        assert!(catalog.index_name_exists_in_schema("main", "idx_name"));
        assert!(catalog.index_name_exists_in_schema("main", "IDX_NAME"));
        // Absent from a different (temp) schema -> a temp table named `idx_name`
        // must NOT see this main index as a collision.
        assert!(!catalog.index_name_exists_in_schema(&temp_schema, "idx_name"));
        // Unknown name -> no collision.
        assert!(!catalog.index_name_exists_in_schema("main", "nope"));
    }

    #[test]
    fn test_add_mixed_column_expression_index() {
        let mut catalog = create_test_catalog();

        // Index with both column and expression
        // Use a simple literal expression for testing
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        let index = IndexMetadata::new(
            "idx_mixed".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn::new_column("name".to_string(), SortOrder::Ascending),
                IndexedColumn::new_expression(expr, SortOrder::Descending),
            ],
            false,
        );

        // Mixed index should be added successfully (column validation for 'name', skip for
        // expression)
        assert!(catalog.add_index(index).is_ok());

        let retrieved = catalog.get_index("users", "idx_mixed").unwrap();
        assert!(retrieved.has_expression_columns());
        assert!(!retrieved.is_expression_index()); // Not purely expression
    }
}
