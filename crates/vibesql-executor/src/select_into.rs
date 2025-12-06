//! SELECT INTO executor - SQL:1999 Feature E111
//!
//! Implements SELECT INTO statements which create a new table and insert query results.
//! Feature E111 requires exactly one row to be returned.

use vibesql_ast::{ColumnDef, CreateTableStmt, SelectItem, SelectStmt};
use vibesql_storage::Database;
use vibesql_types::DataType;

use crate::{errors::ExecutorError, select::SelectExecutor};

pub struct SelectIntoExecutor;

impl SelectIntoExecutor {
    /// Execute a SELECT INTO statement
    ///
    /// This creates a new table with the specified name and inserts the query results.
    /// Per SQL:1999 Feature E111, exactly one row must be returned.
    pub fn execute(
        stmt: &SelectStmt,
        target_table: &str,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Validate that this is a SELECT INTO
        if stmt.into_table.is_none() {
            return Err(ExecutorError::Other(
                "execute_select_into called on non-SELECT INTO statement".to_string(),
            ));
        }

        // Execute the SELECT query
        let executor = SelectExecutor::new(database);
        let rows = executor.execute(stmt)?;

        // SQL:1999 Feature E111 requires exactly one row
        if rows.is_empty() {
            return Err(ExecutorError::UnsupportedFeature(
                "SELECT INTO returned no rows (expected exactly 1 row for Feature E111)"
                    .to_string(),
            ));
        }
        if rows.len() > 1 {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "SELECT INTO returned {} rows (expected exactly 1 row for Feature E111)",
                rows.len()
            )));
        }

        // Derive column definitions from the SELECT list and result row
        let column_defs = Self::derive_column_definitions(&stmt.select_list, &rows[0], database)?;

        // Create the target table
        let create_stmt = CreateTableStmt {
            if_not_exists: false,
            table_name: target_table.to_string(),
            columns: column_defs,
            table_constraints: vec![],
            table_options: vec![],
        };

        crate::CreateTableExecutor::execute(&create_stmt, database)?;

        // Insert the row
        database
            .insert_row(target_table, rows[0].clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(format!("SELECT INTO: Created table '{}' with 1 row", target_table))
    }

    /// Derive column definitions from SELECT list and result row
    fn derive_column_definitions(
        select_list: &[SelectItem],
        result_row: &vibesql_storage::Row,
        database: &Database,
    ) -> Result<Vec<ColumnDef>, ExecutorError> {
        let mut columns = Vec::new();

        for (idx, item) in select_list.iter().enumerate() {
            match item {
                SelectItem::Wildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * is not supported in SELECT INTO statements".to_string(),
                    ));
                }
                SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT table.* is not supported in SELECT INTO statements".to_string(),
                    ));
                }
                SelectItem::Expression { expr, alias } => {
                    // Column name: use alias if present, otherwise derive from expression
                    let column_name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        Self::derive_column_name(expr, database)?
                    };

                    // Infer data type from the result value
                    let data_type = Self::infer_data_type(&result_row.values[idx]);

                    columns.push(ColumnDef {
                        name: column_name,
                        data_type,
                        nullable: true, // Allow NULL by default
                        constraints: vec![],
                        default_value: None,
                        comment: None,
                    });
                }
            }
        }

        Ok(columns)
    }

    /// Derive a column name from an expression
    fn derive_column_name(
        expr: &vibesql_ast::Expression,
        _database: &Database,
    ) -> Result<String, ExecutorError> {
        match expr {
            vibesql_ast::Expression::ColumnRef { column, .. } => Ok(column.clone()),
            vibesql_ast::Expression::Literal(_) => Ok("column".to_string()),
            vibesql_ast::Expression::BinaryOp { .. } => Ok("expr".to_string()),
            vibesql_ast::Expression::UnaryOp { .. } => Ok("expr".to_string()),
            vibesql_ast::Expression::Function { name, .. } => Ok(name.to_lowercase()),
            _ => Ok("column".to_string()),
        }
    }

    /// Infer SQL data type from a runtime value
    fn infer_data_type(value: &vibesql_types::SqlValue) -> DataType {
        match value {
            vibesql_types::SqlValue::Null => DataType::Varchar { max_length: Some(255) }, /* Default for */
            // NULL
            vibesql_types::SqlValue::Integer(_) => DataType::Integer,
            vibesql_types::SqlValue::Bigint(_) => DataType::Bigint,
            vibesql_types::SqlValue::Unsigned(_) => DataType::Unsigned,
            vibesql_types::SqlValue::Smallint(_) => DataType::Smallint,
            vibesql_types::SqlValue::Numeric(_) => DataType::Numeric { precision: 38, scale: 0 },
            vibesql_types::SqlValue::Float(_) => DataType::Float { precision: 53 },
            vibesql_types::SqlValue::Real(_) => DataType::Real,
            vibesql_types::SqlValue::Double(_) => DataType::DoublePrecision,
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                DataType::Varchar { max_length: Some(s.len().max(255)) }
            }
            vibesql_types::SqlValue::Boolean(_) => DataType::Boolean,
            vibesql_types::SqlValue::Date(_) => DataType::Date,
            vibesql_types::SqlValue::Time(_) => DataType::Time { with_timezone: false },
            vibesql_types::SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            vibesql_types::SqlValue::Interval(_) => DataType::Interval {
                start_field: vibesql_types::IntervalField::Year,
                end_field: Some(vibesql_types::IntervalField::Month),
            },
            vibesql_types::SqlValue::Vector(v) => DataType::Vector { dimensions: v.len() as u32 },
        }
    }
}
