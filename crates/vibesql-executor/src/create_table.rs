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
    ///             generated_expr: None,
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///             generated_expr: None,
    ///         },
    ///     ],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    ///     quoted: false,
    ///     as_query: None,
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
        // For TEMP tables, force the schema to "temp" (SQLite compatibility)
        let (schema_name, table_name, identifier) = if stmt.temporary {
            // Temporary table - always use temp schema
            let id = TableIdentifier::qualified(
                vibesql_catalog::TEMP_SCHEMA,
                false,
                &stmt.table_name,
                stmt.quoted,
            );
            (vibesql_catalog::TEMP_SCHEMA.to_string(), stmt.table_name.clone(), id)
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
                query,
            );
        }

        // Check if table already exists in the target schema using SQL:1999 identifier semantics
        // For CREATE TABLE, we only check the target schema (not temp schema)
        // Temp tables can shadow main tables, but we allow creating in main even if temp exists
        // Use table_exists_by_identifier which respects quoted/unquoted semantics:
        // - Quoted identifiers: case-sensitive (exact match)
        // - Unquoted identifiers: case-insensitive (lowercase canonical)
        if database.catalog.table_exists_by_identifier(&identifier) {
            if stmt.if_not_exists {
                // IF NOT EXISTS - silently return success without creating the table
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            return Err(ExecutorError::TableAlreadyExists(format!(
                "{}.{}",
                schema_name,
                identifier.display()
            )));
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

        // Apply constraint results to schema (sets PK, unique, and check constraints)
        ConstraintValidator::apply_to_schema(&mut table_schema, &constraint_result);

        // Detect INTEGER PRIMARY KEY for SQLite rowid aliasing (Issue #4536)
        // In SQLite, a single-column PRIMARY KEY with INTEGER type is an alias for rowid.
        // The column's value IS the rowid, and SELECT rowid returns this column's value.
        if let Some(pk_cols) = &table_schema.primary_key {
            if pk_cols.len() == 1 {
                if let Some(col_idx) = table_schema.get_column_index(&pk_cols[0]) {
                    let col_type = &table_schema.columns[col_idx].data_type;
                    // Only INTEGER (not BIGINT, INT, etc.) qualifies for rowid aliasing
                    if matches!(col_type, DataType::Integer) {
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
                let parent_schema = database
                    .catalog
                    .get_table(references_table)
                    .ok_or_else(|| ExecutorError::TableNotFound(references_table.clone()))?;

                let parent_column_indices: Vec<usize> = references_columns
                    .iter()
                    .map(|col_name| {
                        parent_schema.get_column_index(col_name).ok_or_else(|| {
                            ExecutorError::ColumnNotFound {
                                column_name: col_name.to_string(),
                                table_name: references_table.clone(),
                                searched_tables: vec![references_table.clone()],
                                available_columns: parent_schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

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

                let fk = vibesql_catalog::ForeignKeyConstraint {
                    name: constraint.name.clone(),
                    column_names: fk_columns.clone(),
                    column_indices,
                    parent_table: references_table.clone(),
                    parent_column_names: references_columns.clone(),
                    parent_column_indices,
                    on_delete: convert_action(on_delete),
                    on_update: convert_action(on_update),
                };

                table_schema.add_foreign_key(fk)?;
            }
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
            return Err(ExecutorError::TableAlreadyExists(format!(
                "{}.{}",
                schema_name,
                identifier.display()
            )));
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
                    collation: None, // CTAS doesn't preserve collation
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
