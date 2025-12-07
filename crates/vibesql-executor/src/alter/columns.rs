//! Column operation executors for ALTER TABLE

use vibesql_ast::*;
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::validation::{convert_value, evaluate_simple_default, is_type_conversion_safe};
use crate::errors::ExecutorError;

/// Execute ADD COLUMN
pub(super) fn execute_add_column(
    stmt: &AddColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Check if column already exists
    if table.schema.has_column(&stmt.column_def.name) {
        return Err(ExecutorError::ColumnAlreadyExists(stmt.column_def.name.clone()));
    }

    // Add column to schema
    let mut new_column = ColumnSchema::new(
        stmt.column_def.name.clone(),
        stmt.column_def.data_type.clone(),
        stmt.column_def.nullable,
    );

    // Set the default value if provided
    if let Some(ref default_expr) = stmt.column_def.default_value {
        new_column.set_default(*default_expr.clone());
    }

    table.schema_mut().add_column(new_column)?;

    // Add default value (or NULL) to all existing rows
    let default_value = if let Some(ref default_expr) = stmt.column_def.default_value {
        // Evaluate the default expression for simple cases (literals)
        evaluate_simple_default(default_expr)?
    } else {
        SqlValue::Null
    };

    for row in table.rows_mut() {
        row.add_value(default_value.clone());
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' added to table '{}'", stmt.column_def.name, stmt.table_name))
}

/// Execute DROP COLUMN
pub(super) fn execute_drop_column(
    stmt: &DropColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Check if column exists
    if !stmt.if_exists && !table.schema.has_column(&stmt.column_name) {
        return Err(ExecutorError::ColumnNotFound {
            column_name: stmt.column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        });
    }

    // Check if column is part of constraints
    if table.schema.is_column_in_primary_key(&stmt.column_name) {
        return Err(ExecutorError::CannotDropColumn("Column is part of PRIMARY KEY".to_string()));
    }

    // Check if this is the last column in the table
    if table.schema.columns.len() <= 1 {
        return Err(ExecutorError::CannotDropColumn(
            "Cannot drop the last column in a table. Use DROP TABLE instead.".to_string(),
        ));
    }

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Remove column from schema
    table.schema_mut().remove_column(col_index)?;

    // Remove column data from all rows
    for row in table.rows_mut() {
        let _ = row.remove_value(col_index);
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' dropped from table '{}'", stmt.column_name, stmt.table_name))
}

/// Execute ALTER COLUMN
pub(super) fn execute_alter_column(
    stmt: &AlterColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    match stmt {
        AlterColumnStmt::SetDefault { table_name, column_name, default } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Set the default value in the schema
            table.schema_mut().set_column_default(col_index, default.clone())?;

            Ok(format!("Default set for column '{}' in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::DropDefault { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Drop the default value from the schema
            table.schema_mut().drop_column_default(col_index)?;

            Ok(format!("Default dropped for column '{}' in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::SetNotNull { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Check if any existing rows have NULL in this column
            for row in table.scan() {
                if let SqlValue::Null = &row.values[col_index] {
                    return Err(ExecutorError::ConstraintViolation(
                        "Cannot set NOT NULL: column contains NULL values".to_string(),
                    ));
                }
            }

            // Set column as NOT NULL
            table.schema_mut().set_column_nullable(col_index, false)?;

            Ok(format!("Column '{}' set to NOT NULL in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::DropNotNull { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Set column as nullable
            table.schema_mut().set_column_nullable(col_index, true)?;

            Ok(format!("Column '{}' set to nullable in table '{}'", column_name, table_name))
        }
    }
}

/// Execute MODIFY COLUMN
pub(super) fn execute_modify_column(
    stmt: &ModifyColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    let old_type = &table.schema.columns[col_index].data_type;
    let new_type = &stmt.new_column_def.data_type;

    // Type conversion validation - be strict about compatibility
    let is_compatible = is_type_conversion_safe(old_type, new_type);

    if !is_compatible {
        return Err(ExecutorError::TypeMismatch {
            left: SqlValue::Null, // Placeholder
            op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
            right: SqlValue::Null,
        });
    }

    // Convert existing data
    for row in table.rows_mut() {
        if let Some(value) = row.values.get_mut(col_index) {
            *value = convert_value(value.clone(), new_type)?;
        }
    }

    // Update schema
    table.schema_mut().columns[col_index].data_type = new_type.clone();
    table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

    // Update default value if provided
    if let Some(ref default_expr) = stmt.new_column_def.default_value {
        table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' modified in table '{}'", stmt.column_name, stmt.table_name))
}

/// Execute CHANGE COLUMN (rename + modify)
pub(super) fn execute_change_column(
    stmt: &ChangeColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.old_column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.old_column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    let old_type = &table.schema.columns[col_index].data_type;
    let new_type = &stmt.new_column_def.data_type;

    // Type conversion validation
    let is_compatible = is_type_conversion_safe(old_type, new_type);

    if !is_compatible {
        return Err(ExecutorError::TypeMismatch {
            left: SqlValue::Null,
            op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
            right: SqlValue::Null,
        });
    }

    // Convert existing data
    for row in table.rows_mut() {
        if let Some(value) = row.values.get_mut(col_index) {
            *value = convert_value(value.clone(), new_type)?;
        }
    }

    // Update schema - rename and modify
    table.schema_mut().columns[col_index].name = stmt.new_column_def.name.clone();
    table.schema_mut().columns[col_index].data_type = new_type.clone();
    table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

    // Update default value if provided
    if let Some(ref default_expr) = stmt.new_column_def.default_value {
        table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!(
        "Column '{}' changed to '{}' in table '{}'",
        stmt.old_column_name, stmt.new_column_def.name, stmt.table_name
    ))
}
