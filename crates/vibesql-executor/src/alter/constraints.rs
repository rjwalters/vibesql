//! Constraint operation executors for ALTER TABLE

use vibesql_ast::*;
use vibesql_catalog::ForeignKeyConstraint;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute ADD CONSTRAINT
pub(super) fn execute_add_constraint(
    stmt: &AddConstraintStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    match &stmt.constraint.kind {
        TableConstraintKind::PrimaryKey { columns } => {
            // Extract column names from IndexColumn structs
            let column_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();

            // Verify all columns exist
            for col_name in &column_names {
                if !table.schema.has_column(col_name) {
                    return Err(ExecutorError::ColumnNotFound {
                        column_name: col_name.clone(),
                        table_name: stmt.table_name.clone(),
                        searched_tables: vec![stmt.table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    });
                }
            }

            // Check if primary key already exists
            if table.schema.primary_key.is_some() {
                return Err(ExecutorError::ConstraintViolation(
                    "Table already has a PRIMARY KEY constraint".to_string(),
                ));
            }

            // Add primary key to table schema
            table.schema_mut().primary_key = Some(column_names);

            // Rebuild table indexes to create the primary key index
            table.rebuild_indexes();

            // Update catalog with modified schema by dropping and recreating
            let updated_schema = table.schema.clone();
            database.catalog.drop_table(&stmt.table_name)?;
            database.catalog.create_table(updated_schema)?;

            Ok(format!("PRIMARY KEY constraint added to table '{}'", stmt.table_name))
        }
        TableConstraintKind::Unique { columns } => {
            // Extract column names from IndexColumn structs
            let column_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();
            table.schema_mut().add_unique_constraint(column_names)?;

            // Rebuild table indexes to create the unique constraint index
            table.rebuild_indexes();

            // Update catalog with modified schema by dropping and recreating
            let updated_schema = table.schema.clone();
            database.catalog.drop_table(&stmt.table_name)?;
            database.catalog.create_table(updated_schema)?;

            Ok(format!("UNIQUE constraint added to table '{}'", stmt.table_name))
        }
        TableConstraintKind::Check { expr } => {
            let constraint_name = stmt
                .constraint
                .name
                .clone()
                .unwrap_or_else(|| format!("check_{}", table.schema.check_constraints.len()));

            table.schema_mut().add_check_constraint(constraint_name.clone(), *expr.clone())?;

            Ok(format!(
                "CHECK constraint '{}' added to table '{}'",
                constraint_name, stmt.table_name
            ))
        }
        TableConstraintKind::ForeignKey {
            columns,
            references_table,
            references_columns,
            on_delete,
            on_update,
        } => {
            // Convert AST ReferentialAction to catalog ReferentialAction
            let convert_action = |action: &Option<vibesql_ast::ReferentialAction>| match action {
                Some(vibesql_ast::ReferentialAction::Cascade) => {
                    vibesql_catalog::ReferentialAction::Cascade
                }
                Some(vibesql_ast::ReferentialAction::SetNull) => {
                    vibesql_catalog::ReferentialAction::SetNull
                }
                Some(vibesql_ast::ReferentialAction::SetDefault) => {
                    vibesql_catalog::ReferentialAction::SetDefault
                }
                Some(vibesql_ast::ReferentialAction::Restrict) => {
                    vibesql_catalog::ReferentialAction::Restrict
                }
                Some(vibesql_ast::ReferentialAction::NoAction) | None => {
                    vibesql_catalog::ReferentialAction::NoAction
                }
            };

            // Get column indices
            let mut column_indices = Vec::new();
            for col_name in columns {
                let idx = table.schema.get_column_index(col_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: col_name.clone(),
                        table_name: stmt.table_name.clone(),
                        searched_tables: vec![stmt.table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;
                column_indices.push(idx);
            }

            // Verify referenced table exists
            let ref_table = database
                .get_table(references_table)
                .ok_or_else(|| ExecutorError::TableNotFound(references_table.clone()))?;

            // Get referenced column indices
            let mut parent_column_indices = Vec::new();
            for col_name in references_columns {
                let idx = ref_table.schema.get_column_index(col_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: col_name.clone(),
                        table_name: references_table.clone(),
                        searched_tables: vec![references_table.clone()],
                        available_columns: ref_table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;
                parent_column_indices.push(idx);
            }

            let fk = ForeignKeyConstraint {
                name: stmt.constraint.name.clone(),
                column_names: columns.clone(),
                column_indices,
                parent_table: references_table.clone(),
                parent_column_names: references_columns.clone(),
                parent_column_indices,
                on_delete: convert_action(on_delete),
                on_update: convert_action(on_update),
            };

            let table = database
                .get_table_mut(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
            table.schema_mut().add_foreign_key(fk)?;

            // Update catalog with modified schema by dropping and recreating
            let updated_schema = table.schema.clone();
            database.catalog.drop_table(&stmt.table_name)?;
            database.catalog.create_table(updated_schema)?;

            Ok(format!("FOREIGN KEY constraint added to table '{}'", stmt.table_name))
        }
        TableConstraintKind::Fulltext { index_name: _, columns: _ } => {
            // TODO: Implement FULLTEXT index creation
            // For now, just return a message indicating it's not yet implemented
            Err(ExecutorError::UnsupportedFeature(
                "FULLTEXT index support is not yet implemented".to_string(),
            ))
        }
    }
}

/// Execute DROP CONSTRAINT
pub(super) fn execute_drop_constraint(
    stmt: &DropConstraintStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Try to drop from each constraint type
    // First try check constraints
    if table.schema_mut().drop_check_constraint(&stmt.constraint_name).is_ok() {
        // Update catalog with modified schema
        let updated_schema = table.schema.clone();
        database.catalog.drop_table(&stmt.table_name)?;
        database.catalog.create_table(updated_schema)?;

        return Ok(format!(
            "CHECK constraint '{}' dropped from table '{}'",
            stmt.constraint_name, stmt.table_name
        ));
    }

    // Try foreign keys
    if table.schema_mut().drop_foreign_key(&stmt.constraint_name).is_ok() {
        // Update catalog with modified schema
        let updated_schema = table.schema.clone();
        database.catalog.drop_table(&stmt.table_name)?;
        database.catalog.create_table(updated_schema)?;

        return Ok(format!(
            "FOREIGN KEY constraint '{}' dropped from table '{}'",
            stmt.constraint_name, stmt.table_name
        ));
    }

    // Constraint not found
    Err(ExecutorError::ConstraintNotFound {
        constraint_name: stmt.constraint_name.clone(),
        table_name: stmt.table_name.clone(),
    })
}
