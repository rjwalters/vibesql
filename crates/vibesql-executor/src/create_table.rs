//! CREATE TABLE statement execution

use vibesql_ast::{CreateTableStmt, IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableIdentifier, TableSchema};
use vibesql_storage::Database;
use vibesql_types::DataType;

use crate::{
    constraint_validator::ConstraintValidator, errors::ExecutorError,
    privilege_checker::PrivilegeChecker, SelectExecutor,
};

/// Executor for CREATE TABLE statements
pub struct CreateTableExecutor;

impl CreateTableExecutor {
    /// Execute a CREATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The CREATE TABLE statement AST node
    /// * `database` - The database to create the table in
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{ColumnDef, CreateTableStmt};
    /// use vibesql_executor::CreateTableExecutor;
    /// use vibesql_storage::Database;
    /// use vibesql_types::DataType;
    ///
    /// let mut db = Database::new();
    /// let stmt = CreateTableStmt { temporary: false,
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     columns: vec![
    ///         ColumnDef {
    ///             name: "id".to_string(),
    ///             data_type: DataType::Integer,
    ///             nullable: false,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///             generated_expr: None, is_exact_integer_type: false,
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///             generated_expr: None, is_exact_integer_type: false,
    ///         },
    ///     ],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    ///     quoted: false,
    ///     name_source: None,
    ///     as_query: None, without_rowid: false,
    /// };
    ///
    /// let result = CreateTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Parse qualified table name (schema.table or just table)
        // For TEMP tables, use the session-specific temp schema (SQLite compatibility)
        let (schema_name, table_name, identifier) = if stmt.temporary {
            // Temporary table - use session-specific temp schema
            // Each session gets its own temp schema (e.g., "temp_1", "temp_2")
            // for isolation between database connections
            let temp_schema = database.catalog.temp_schema_name();
            let id = TableIdentifier::qualified(temp_schema, false, &stmt.table_name, stmt.quoted);
            (temp_schema.to_string(), stmt.table_name.clone(), id)
        } else if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
            // Schema-qualified table name - use qualified identifier
            // Note: We use stmt.quoted for both parts since the parser combined them
            // In a future iteration, CREATE TABLE could also store schema/table quoted status separately
            let id = TableIdentifier::qualified(schema_part, stmt.quoted, table_part, stmt.quoted);
            (schema_part.to_string(), table_part.to_string(), id)
        } else {
            // Simple table name - use current schema
            let id = TableIdentifier::new(&stmt.table_name, stmt.quoted);
            (database.catalog.get_current_schema().to_string(), stmt.table_name.clone(), id)
        };

        // Check CREATE privilege on the schema
        PrivilegeChecker::check_create(database, &schema_name)?;

        // Handle CREATE TABLE AS SELECT syntax
        if let Some(query) = &stmt.as_query {
            return Self::execute_create_as_select(
                database,
                &table_name,
                &schema_name,
                identifier,
                stmt.if_not_exists,
                stmt.name_source.as_deref(),
                query,
            );
        }

        // Check if table already exists in the target schema using SQL:1999 identifier semantics
        // For CREATE TABLE, we only check the target schema (not temp schema)
        // Temp tables can shadow main tables, but we allow creating in main even if temp exists
        // Use table_exists_by_identifier which keys on the canonical (case-folded)
        // form. SQLite case-folds identifiers regardless of quoting (issue #5553),
        // so `CREATE TABLE "TBL1"` collides with an existing `tbl1`.
        if database.catalog.table_exists_by_identifier(&identifier) {
            if stmt.if_not_exists {
                // IF NOT EXISTS - silently return success without creating the table
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            // A bare `CREATE TABLE` over an existing name is an error. SQLite
            // echoes the table name *exactly as written in the source*,
            // preserving its quoting form and casing — `table "tbl1" already
            // exists` / `table [tbl1] already exists` (sqlite3 3.51.0). The
            // parser captures that verbatim spelling in `name_source`; fall back
            // to the schema-qualified normalized name when it is unavailable
            // (e.g. programmatically-built AST). Mirrors the CREATE TRIGGER
            // mechanism from #5538.
            let echoed = stmt
                .name_source
                .clone()
                .unwrap_or_else(|| format!("{}.{}", schema_name, identifier.display()));
            return Err(ExecutorError::TableAlreadyExists(echoed));
        }

        // Check for AUTO_INCREMENT constraints
        // MySQL allows only one AUTO_INCREMENT column per table
        let auto_increment_columns: Vec<&str> = stmt
            .columns
            .iter()
            .filter(|col_def| {
                col_def
                    .constraints
                    .iter()
                    .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement))
            })
            .map(|col_def| col_def.name.as_str())
            .collect();

        if auto_increment_columns.len() > 1 {
            return Err(ExecutorError::ConstraintViolation(
                "Only one AUTO_INCREMENT column allowed per table".to_string(),
            ));
        }

        // Convert AST ColumnDef → Catalog ColumnSchema
        let mut columns: Vec<ColumnSchema> = stmt
            .columns
            .iter()
            .map(|col_def| {
                // For AUTO_INCREMENT columns, set default to NEXT VALUE FOR sequence
                let default_value = if col_def
                    .constraints
                    .iter()
                    .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement))
                {
                    // Create sequence name: {table_name}_{column_name}_seq
                    let sequence_name = format!("{}_{}_seq", table_name, col_def.name);
                    Some(vibesql_ast::Expression::NextValue { sequence_name })
                } else {
                    col_def.default_value.as_ref().map(|expr| (**expr).clone())
                };

                // Extract column-level collation from constraints
                let collation = col_def.constraints.iter().find_map(|c| {
                    if let vibesql_ast::ColumnConstraintKind::Collate(coll) = &c.kind {
                        Some(coll.clone())
                    } else {
                        None
                    }
                });

                ColumnSchema {
                    name: col_def.name.clone(),
                    data_type: col_def.data_type.clone(),
                    nullable: col_def.nullable,
                    default_value,
                    generated_expr: col_def.generated_expr.as_ref().map(|expr| (**expr).clone()),
                    collation,
                    is_exact_integer_type: col_def.is_exact_integer_type,
                }
            })
            .collect();

        // Process constraints using the constraint validator
        let constraint_result =
            ConstraintValidator::process_constraints(&stmt.columns, &stmt.table_constraints)?;

        // Apply constraint results to columns (updates nullability)
        ConstraintValidator::apply_to_columns(&mut columns, &constraint_result);

        // Create TableSchema with unqualified name
        let mut table_schema = TableSchema::new(table_name.clone(), columns);

        // Apply WITHOUT ROWID flag from AST (SQLite compatibility)
        table_schema.without_rowid = stmt.without_rowid;

        // Apply constraint results to schema (sets PK, unique, and check constraints)
        ConstraintValidator::apply_to_schema(&mut table_schema, &constraint_result);

        // WITHOUT ROWID tables must have a PRIMARY KEY (SQLite requirement, Issue #4953)
        if stmt.without_rowid && table_schema.primary_key.is_none() {
            return Err(ExecutorError::ConstraintViolation(
                "WITHOUT ROWID tables must have a PRIMARY KEY".to_string(),
            ));
        }

        // Detect INTEGER PRIMARY KEY for SQLite rowid aliasing (Issue #4536)
        // In SQLite, a single-column PRIMARY KEY with exactly "INTEGER" type is an alias for rowid.
        // The column's value IS the rowid, and SELECT rowid returns this column's value.
        // IMPORTANT: Only exact "INTEGER" qualifies - "INT" does NOT (even though both parse to DataType::Integer)
        if let Some(pk_cols) = &table_schema.primary_key {
            if pk_cols.len() == 1 {
                if let Some(col_idx) = table_schema.get_column_index(&pk_cols[0]) {
                    let col = &table_schema.columns[col_idx];
                    // Only exact "INTEGER" type qualifies for rowid aliasing, not "INT"
                    if matches!(col.data_type, DataType::Integer) && col.is_exact_integer_type {
                        table_schema.set_rowid_alias_column(Some(col_idx));
                    }
                }
            }
        }

        // Check for STORAGE table option and apply storage format
        for option in &stmt.table_options {
            if let vibesql_ast::TableOption::Storage(format) = option {
                table_schema.set_storage_format(*format);
            }
        }

        // Process foreign key constraints from table_constraints
        for constraint in &stmt.table_constraints {
            if let vibesql_ast::TableConstraintKind::ForeignKey {
                columns: fk_columns,
                references_table,
                references_columns,
                on_delete,
                on_update,
                deferral,
            } = &constraint.kind
            {
                // Resolve column indices for FK columns
                let column_indices: Vec<usize> = fk_columns
                    .iter()
                    .map(|col_name| {
                        table_schema.get_column_index(col_name).ok_or_else(|| {
                            ExecutorError::ColumnNotFound {
                                column_name: col_name.to_string(),
                                table_name: table_name.clone(),
                                searched_tables: vec![table_name.clone()],
                                available_columns: table_schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Lookup parent table to get parent column indices
                // If the parent table doesn't exist yet, use placeholder indices.
                // SQLite stores FK metadata at CREATE TABLE time even if the
                // referenced table doesn't exist yet.
                let parent_schema = database.catalog.get_table(references_table);

                let parent_column_indices: Vec<usize> = references_columns
                    .iter()
                    .map(|col_name| {
                        parent_schema
                            .as_ref()
                            .and_then(|s| s.get_column_index(col_name))
                            .unwrap_or(0)
                    })
                    .collect();

                // Convert ReferentialAction from AST to catalog type
                let convert_action = |action: &Option<vibesql_ast::ReferentialAction>| match action
                    .as_ref()
                    .unwrap_or(&vibesql_ast::ReferentialAction::NoAction)
                {
                    vibesql_ast::ReferentialAction::Cascade => {
                        vibesql_catalog::ReferentialAction::Cascade
                    }
                    vibesql_ast::ReferentialAction::SetNull => {
                        vibesql_catalog::ReferentialAction::SetNull
                    }
                    vibesql_ast::ReferentialAction::SetDefault => {
                        vibesql_catalog::ReferentialAction::SetDefault
                    }
                    vibesql_ast::ReferentialAction::Restrict => {
                        vibesql_catalog::ReferentialAction::Restrict
                    }
                    vibesql_ast::ReferentialAction::NoAction => {
                        vibesql_catalog::ReferentialAction::NoAction
                    }
                };

                // If no parent columns specified, pad with empty strings to match
                // the FK column count. SQLite stores empty "to" in PRAGMA foreign_key_list
                // when no column list is given after REFERENCES.
                let effective_parent_names = if references_columns.is_empty() {
                    vec![String::new(); fk_columns.len()]
                } else {
                    references_columns.clone()
                };
                let effective_parent_indices = if parent_column_indices.is_empty() {
                    vec![0; fk_columns.len()]
                } else {
                    parent_column_indices
                };

                let (is_deferrable, initially_deferred) = deferral
                    .map(|d| (d.is_deferrable, d.initially_deferred))
                    .unwrap_or((false, false));

                let fk = vibesql_catalog::ForeignKeyConstraint {
                    name: constraint.name.clone(),
                    column_names: fk_columns.clone(),
                    column_indices,
                    parent_table: references_table.clone(),
                    parent_column_names: effective_parent_names,
                    parent_column_indices: effective_parent_indices,
                    on_delete: convert_action(on_delete),
                    on_update: convert_action(on_update),
                    is_deferrable,
                    initially_deferred,
                };

                table_schema.add_foreign_key(fk)?;
            }
        }

        // Process column-level REFERENCES constraints
        // These are parsed as ColumnConstraintKind::References but need to be
        // converted into ForeignKeyConstraint entries in the schema.
        // Note: In SQLite, FK constraints are stored as metadata at CREATE TABLE time
        // even if the referenced table doesn't exist yet. Validation only happens
        // at INSERT/UPDATE/DELETE time when PRAGMA foreign_keys=ON.
        // SQLite assigns FK IDs in reverse column order for column-level constraints,
        // so we collect them first and then add in reverse order.
        let mut column_level_fks = Vec::new();
        for col_def in &stmt.columns {
            for constraint in &col_def.constraints {
                if let vibesql_ast::ColumnConstraintKind::References {
                    table: ref_table,
                    column: ref_column,
                    on_delete,
                    on_update,
                    deferral,
                } = &constraint.kind
                {
                    let col_idx =
                        table_schema.get_column_index(&col_def.name).ok_or_else(|| {
                            ExecutorError::ColumnNotFound {
                                column_name: col_def.name.clone(),
                                table_name: table_name.clone(),
                                searched_tables: vec![table_name.clone()],
                                available_columns: table_schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect(),
                            }
                        })?;

                    // Try to lookup parent table to resolve column references.
                    // If the parent table doesn't exist yet, store the FK with
                    // the column name but use placeholder index 0.
                    let parent_schema = database.catalog.get_table(ref_table);

                    let (parent_col_name, parent_col_idx) = if let Some(col) = ref_column {
                        // Explicit column reference
                        let idx = parent_schema
                            .as_ref()
                            .and_then(|s| s.get_column_index(col))
                            .unwrap_or(0);
                        (col.clone(), idx)
                    } else {
                        // No column specified - store empty string to match SQLite behavior.
                        // PRAGMA foreign_key_list shows empty "to" column for implicit PK refs.
                        // The actual PK column is resolved at enforcement time.
                        (String::new(), 0)
                    };

                    let convert_action =
                        |action: &Option<vibesql_ast::ReferentialAction>| match action
                            .as_ref()
                            .unwrap_or(&vibesql_ast::ReferentialAction::NoAction)
                        {
                            vibesql_ast::ReferentialAction::Cascade => {
                                vibesql_catalog::ReferentialAction::Cascade
                            }
                            vibesql_ast::ReferentialAction::SetNull => {
                                vibesql_catalog::ReferentialAction::SetNull
                            }
                            vibesql_ast::ReferentialAction::SetDefault => {
                                vibesql_catalog::ReferentialAction::SetDefault
                            }
                            vibesql_ast::ReferentialAction::Restrict => {
                                vibesql_catalog::ReferentialAction::Restrict
                            }
                            vibesql_ast::ReferentialAction::NoAction => {
                                vibesql_catalog::ReferentialAction::NoAction
                            }
                        };

                    let (is_deferrable, initially_deferred) = deferral
                        .map(|d| (d.is_deferrable, d.initially_deferred))
                        .unwrap_or((false, false));

                    let fk = vibesql_catalog::ForeignKeyConstraint {
                        name: constraint.name.clone(),
                        column_names: vec![col_def.name.clone()],
                        column_indices: vec![col_idx],
                        parent_table: ref_table.clone(),
                        parent_column_names: vec![parent_col_name],
                        parent_column_indices: vec![parent_col_idx],
                        on_delete: convert_action(on_delete),
                        on_update: convert_action(on_update),
                        is_deferrable,
                        initially_deferred,
                    };

                    column_level_fks.push(fk);
                }
            }
        }
        // Add column-level FKs in reverse order to match SQLite's FK ID assignment
        for fk in column_level_fks.into_iter().rev() {
            table_schema.add_foreign_key(fk)?;
        }

        // If creating in a non-current schema, temporarily switch to it
        let original_schema = database.catalog.get_current_schema().to_string();
        let needs_schema_switch = schema_name != original_schema;

        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&schema_name)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Create internal sequences for AUTO_INCREMENT columns
        for auto_inc_col in &auto_increment_columns {
            let sequence_name = format!("{}_{}_seq", table_name, auto_inc_col);
            database
                .catalog
                .create_sequence(
                    sequence_name.clone(),
                    Some(1), // start_with: 1
                    1,       // increment_by: 1
                    Some(1), // min_value: 1
                    None,    // max_value: unlimited
                    false,   // cycle: false
                )
                .map_err(|e| {
                    ExecutorError::StorageError(format!(
                        "Failed to create sequence for AUTO_INCREMENT: {:?}",
                        e
                    ))
                })?;
        }

        // Create table using Database API with TableIdentifier (handles both catalog and storage)
        // Note: identifier was created at the start of this function with proper quoted semantics
        let result = database
            .create_table_with_identifier(table_schema.clone(), identifier.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()));

        // Check if table creation succeeded before creating indexes
        result?;

        // Auto-create indexes for PRIMARY KEY and UNIQUE constraints
        Self::create_implicit_indexes(database, &table_name, &table_schema)?;

        // Restore original schema if we switched
        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&original_schema)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Return success message
        Ok(format!("Table '{}' created successfully in schema '{}'", table_name, schema_name))
    }

    /// Create implicit indexes for PRIMARY KEY and UNIQUE constraints
    ///
    /// Production databases automatically create B-tree indexes for these constraints
    /// to enable efficient query optimization. This function replicates that behavior.
    fn create_implicit_indexes(
        database: &mut Database,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> Result<(), ExecutorError> {
        // Counter for SQLite-compatible auto-index naming: sqlite_autoindex_<table>_<n>
        let mut autoindex_counter = 1;

        // Auto-create PRIMARY KEY index
        // Skip autoindex for INTEGER PRIMARY KEY - it's an alias for rowid
        // and doesn't need a separate B-tree index (matches SQLite behavior)
        if let Some(pk_cols) = &table_schema.primary_key {
            if table_schema.rowid_alias_column.is_none() {
                let index_name = format!("sqlite_autoindex_{}_{}", table_name, autoindex_counter);
                autoindex_counter += 1;

                // Create IndexColumn specs for the PRIMARY KEY columns
                let index_columns: Vec<IndexColumn> = pk_cols
                    .iter()
                    .map(|col_name| IndexColumn::Column {
                        column_name: col_name.to_string(),
                        direction: OrderDirection::Asc,
                        prefix_length: None,
                    })
                    .collect();

                // Add to catalog first
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.to_string(),
                    vibesql_catalog::IndexType::BTree,
                    index_columns
                        .iter()
                        .map(|col| {
                            vibesql_catalog::IndexedColumn::new_column(
                                col.expect_column_name().to_string(),
                                vibesql_catalog::SortOrder::Ascending,
                            )
                        })
                        .collect(),
                    true, // unique
                );
                database
                    .catalog
                    .add_index(index_metadata)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                // Create the actual B-tree index
                database
                    .create_index(index_name, table_name.to_string(), true, index_columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }
        }

        // Auto-create UNIQUE constraint indexes
        for unique_cols in &table_schema.unique_constraints {
            let index_name = format!("sqlite_autoindex_{}_{}", table_name, autoindex_counter);
            autoindex_counter += 1;

            // Create IndexColumn specs for the UNIQUE columns
            let index_columns: Vec<IndexColumn> = unique_cols
                .iter()
                .map(|col_name| IndexColumn::Column {
                    column_name: col_name.to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                })
                .collect();

            // Add to catalog first
            let index_metadata = vibesql_catalog::IndexMetadata::new(
                index_name.clone(),
                table_name.to_string(),
                vibesql_catalog::IndexType::BTree,
                index_columns
                    .iter()
                    .map(|col| {
                        vibesql_catalog::IndexedColumn::new_column(
                            col.expect_column_name().to_string(),
                            vibesql_catalog::SortOrder::Ascending,
                        )
                    })
                    .collect(),
                true, // unique
            );
            database
                .catalog
                .add_index(index_metadata)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Create the actual B-tree index
            database
                .create_index(index_name, table_name.to_string(), true, index_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(())
    }

    /// Execute CREATE TABLE ... AS SELECT
    ///
    /// Creates a new table with schema derived from the SELECT result,
    /// and populates it with the query results.
    fn execute_create_as_select(
        database: &mut Database,
        table_name: &str,
        schema_name: &str,
        identifier: TableIdentifier,
        if_not_exists: bool,
        name_source: Option<&str>,
        query: &vibesql_ast::SelectStmt,
    ) -> Result<String, ExecutorError> {
        // Check if table already exists
        if database.catalog.table_exists_by_identifier(&identifier) {
            if if_not_exists {
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            // Echo the source quoting form when available (see the bare CREATE
            // TABLE path above for rationale); fall back to the schema-qualified
            // normalized name for programmatically-built ASTs.
            let echoed = name_source
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}.{}", schema_name, identifier.display()));
            return Err(ExecutorError::TableAlreadyExists(echoed));
        }

        // Execute the SELECT query to get results
        let rows = SelectExecutor::new(database).execute(query)?;

        // Derive column names from the SELECT list (expanding wildcards if needed)
        let column_names =
            Self::derive_column_names_from_select_list(&query.select_list, &query.from, database)?;

        // Derive column schema from the first row (if any) or default to BLOB
        let columns: Vec<ColumnSchema> = column_names
            .iter()
            .enumerate()
            .map(|(idx, col_name)| {
                // Try to infer data type from the first row if available
                let data_type = if !rows.is_empty() && idx < rows[0].values.len() {
                    Self::infer_data_type(&rows[0].values[idx])
                } else {
                    // No rows or column - default to BLOB affinity
                    DataType::BinaryLargeObject
                };

                ColumnSchema {
                    name: col_name.to_string(),
                    data_type,
                    nullable: true, // Default to nullable for CTAS
                    default_value: None,
                    generated_expr: None,
                    collation: None,              // CTAS doesn't preserve collation
                    is_exact_integer_type: false, // CTAS doesn't preserve exact type
                }
            })
            .collect();

        // Create the table schema
        let table_schema = TableSchema::new(table_name.to_string(), columns);

        // Create the table
        database
            .create_table_with_identifier(table_schema, identifier)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Insert the result rows into the new table
        let row_count = rows.len();
        for row in rows {
            database
                .insert_row(table_name, row)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(format!(
            "Table '{}' created successfully in schema '{}' with {} rows",
            table_name, schema_name, row_count
        ))
    }

    /// Derive column names from a SELECT list, expanding wildcards using the database schema
    fn derive_column_names_from_select_list(
        select_list: &[vibesql_ast::SelectItem],
        from: &Option<vibesql_ast::FromClause>,
        database: &Database,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut names = Vec::new();
        let mut counter = 0;

        for item in select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. } => {
                    // Expand wildcard using the FROM clause tables
                    let table_names = Self::get_table_names_from_from(from)?;
                    for table_name in table_names {
                        if let Some(schema) = database.catalog.get_table(&table_name) {
                            for col in &schema.columns {
                                names.push(col.name.clone());
                            }
                        } else {
                            return Err(ExecutorError::TableNotFound(table_name));
                        }
                    }
                }
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Expand table.* using the specific table's schema
                    if let Some(schema) = database.catalog.get_table(qualifier) {
                        for col in &schema.columns {
                            names.push(col.name.clone());
                        }
                    } else {
                        return Err(ExecutorError::TableNotFound(qualifier.clone()));
                    }
                }
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    let name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        // Try to derive from expression
                        Self::derive_column_name_from_expr(expr, &mut counter)
                    };
                    names.push(name);
                }
            }
        }

        Ok(names)
    }

    /// Extract table names from a FROM clause
    fn get_table_names_from_from(
        from: &Option<vibesql_ast::FromClause>,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut names = Vec::new();

        match from {
            None => {
                // No FROM clause - can't expand wildcard
                return Err(ExecutorError::UnsupportedFeature(
                    "CREATE TABLE AS SELECT * requires a FROM clause".to_string(),
                ));
            }
            Some(vibesql_ast::FromClause::Table { name, .. }) => {
                names.push(name.clone());
            }
            Some(vibesql_ast::FromClause::Join { left, right, .. }) => {
                // Recursively get tables from join
                names.extend(Self::get_table_names_from_from(&Some(*left.clone()))?);
                names.extend(Self::get_table_names_from_from(&Some(*right.clone()))?);
            }
            Some(vibesql_ast::FromClause::Subquery { alias, .. }) => {
                // For derived tables (subqueries), we can't easily expand *
                // because we'd need to recursively process the subquery
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CREATE TABLE AS SELECT * from subquery '{}' not supported - please specify columns explicitly",
                    alias
                )));
            }
            Some(vibesql_ast::FromClause::Values { alias, .. }) => {
                // VALUES clause - can't determine column names from schema
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CREATE TABLE AS SELECT * from VALUES '{}' not supported - please specify columns explicitly",
                    alias
                )));
            }
        }

        Ok(names)
    }

    /// Derive a column name from an expression
    fn derive_column_name_from_expr(expr: &vibesql_ast::Expression, counter: &mut usize) -> String {
        match expr {
            vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
            vibesql_ast::Expression::Literal(_) => {
                *counter += 1;
                format!("column{}", counter)
            }
            vibesql_ast::Expression::Function { name, .. } => {
                // Use the function name as the column name
                name.to_string().to_lowercase()
            }
            _ => {
                *counter += 1;
                format!("column{}", counter)
            }
        }
    }

    /// Infer DataType from an SqlValue
    fn infer_data_type(value: &vibesql_types::SqlValue) -> DataType {
        use vibesql_types::SqlValue;
        match value {
            SqlValue::Null => DataType::BinaryLargeObject,
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Integer(_) => DataType::Integer,
            SqlValue::Bigint(_) => DataType::Bigint,
            SqlValue::Smallint(_) => DataType::Smallint,
            SqlValue::Unsigned(_) => DataType::Unsigned,
            SqlValue::Float(_) | SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) | SqlValue::Numeric(_) => DataType::DoublePrecision,
            SqlValue::Character(_) => DataType::Character { length: 255 },
            SqlValue::Varchar(_) => DataType::Varchar { max_length: None },
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Interval(_) => DataType::Interval {
                start_field: vibesql_types::IntervalField::Day,
                end_field: None,
            },
            SqlValue::Vector(v) => DataType::Vector { dimensions: v.len() as u32 },
            SqlValue::Blob(_) => DataType::BinaryLargeObject,
        }
    }
}
