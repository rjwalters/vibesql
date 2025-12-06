//! CREATE TABLE statement execution

use vibesql_ast::{CreateTableStmt, IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Database;

use crate::{
    constraint_validator::ConstraintValidator, errors::ExecutorError,
    privilege_checker::PrivilegeChecker,
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
    /// let stmt = CreateTableStmt {
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
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///         },
    ///     ],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
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
        let (schema_name, table_name) =
            if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
                (schema_part.to_string(), table_part.to_string())
            } else {
                (database.catalog.get_current_schema().to_string(), stmt.table_name.clone())
            };

        // Check CREATE privilege on the schema
        PrivilegeChecker::check_create(database, &schema_name)?;

        // Check if table already exists in the target schema
        let qualified_name = format!("{}.{}", schema_name, table_name);
        if database.catalog.table_exists(&qualified_name) {
            if stmt.if_not_exists {
                // IF NOT EXISTS - silently return success without creating the table
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            return Err(ExecutorError::TableAlreadyExists(qualified_name));
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
        let mut columns: Vec<ColumnSchema> =
            stmt.columns
                .iter()
                .map(|col_def| {
                    // For AUTO_INCREMENT columns, set default to NEXT VALUE FOR sequence
                    let default_value =
                        if col_def.constraints.iter().any(|c| {
                            matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)
                        }) {
                            // Create sequence name: {table_name}_{column_name}_seq
                            let sequence_name = format!("{}_{}_seq", table_name, col_def.name);
                            Some(vibesql_ast::Expression::NextValue { sequence_name })
                        } else {
                            col_def.default_value.as_ref().map(|expr| (**expr).clone())
                        };

                    ColumnSchema {
                        name: col_def.name.clone(),
                        data_type: col_def.data_type.clone(),
                        nullable: col_def.nullable,
                        default_value,
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
                                column_name: col_name.clone(),
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
                                column_name: col_name.clone(),
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

        // Create table using Database API (handles both catalog and storage)
        let result = database
            .create_table(table_schema.clone())
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
        // Auto-create PRIMARY KEY index
        if let Some(pk_cols) = &table_schema.primary_key {
            let index_name = format!("pk_{}", table_name);

            // Create IndexColumn specs for the PRIMARY KEY columns
            let index_columns: Vec<IndexColumn> = pk_cols
                .iter()
                .map(|col_name| IndexColumn {
                    column_name: col_name.clone(),
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
                    .map(|col| vibesql_catalog::IndexedColumn {
                        column_name: col.column_name.clone(),
                        order: vibesql_catalog::SortOrder::Ascending,
                        prefix_length: None,
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

        // Auto-create UNIQUE constraint indexes
        for unique_cols in &table_schema.unique_constraints {
            let index_name = format!("uq_{}_{}", table_name, unique_cols.join("_"));

            // Create IndexColumn specs for the UNIQUE columns
            let index_columns: Vec<IndexColumn> = unique_cols
                .iter()
                .map(|col_name| IndexColumn {
                    column_name: col_name.clone(),
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
                    .map(|col| vibesql_catalog::IndexedColumn {
                        column_name: col.column_name.clone(),
                        order: vibesql_catalog::SortOrder::Ascending,
                        prefix_length: None,
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
}
