//! Value update logic for UPDATE operations

use std::collections::HashSet;

use vibesql_ast::Assignment;

use crate::{
    errors::ExecutorError, evaluator::ExpressionEvaluator, insert::validation::coerce_value,
};

/// Applies assignment expressions to rows
pub struct ValueUpdater<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    evaluator: &'a ExpressionEvaluator<'a>,
}

impl<'a> ValueUpdater<'a> {
    /// Create a new value updater
    pub fn new(
        schema: &'a vibesql_catalog::TableSchema,
        evaluator: &'a ExpressionEvaluator<'a>,
        _table_name: &'a str, // Kept for API compatibility
    ) -> Self {
        Self { schema, evaluator }
    }

    /// Apply assignments to a row
    ///
    /// Returns the updated row and a set of changed column indices.
    /// Expressions are evaluated against the original row (two-phase semantics).
    pub fn apply_assignments(
        &self,
        original_row: &vibesql_storage::Row,
        assignments: &[Assignment],
    ) -> Result<(vibesql_storage::Row, HashSet<usize>), ExecutorError> {
        let mut new_row = original_row.clone();
        let mut changed_columns = HashSet::new();

        // Apply each assignment
        for assignment in assignments {
            // Check if this is a rowid assignment (SQLite compatibility)
            let col_name_lower = assignment.column.to_lowercase();
            let is_rowid =
                col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";

            if is_rowid {
                // Handle rowid update
                // If the table has an INTEGER PRIMARY KEY (rowid alias), update that column
                // Otherwise, update the row's internal row_id field
                if let Some(ipk_col_idx) = self.schema.rowid_alias_column {
                    // The INTEGER PRIMARY KEY column IS the rowid - update it
                    let new_value = self.evaluator.eval(&assignment.value, original_row)?;
                    new_row
                        .set(ipk_col_idx, new_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(ipk_col_idx);
                } else {
                    // No INTEGER PRIMARY KEY - update the virtual rowid
                    // Evaluate the new rowid value
                    let new_value = self.evaluator.eval(&assignment.value, original_row)?;
                    // Convert to u64 for row_id
                    let new_rowid = match new_value {
                        vibesql_types::SqlValue::Integer(i) => i as u64,
                        vibesql_types::SqlValue::Bigint(i) => i as u64,
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "ROWID must be an integer, got {:?}",
                                other
                            )));
                        }
                    };
                    new_row.row_id = Some(new_rowid);
                    // Note: We don't add anything to changed_columns since row_id
                    // is not a real column. The storage layer will use the updated row_id.
                }
                continue;
            }

            // Find column index
            // Use SQLite-compatible "no such column: X" error format
            let col_index = self.schema.get_column_index(&assignment.column).ok_or_else(|| {
                ExecutorError::NoSuchColumn { column_ref: assignment.column.clone() }
            })?;

            // Evaluate new value expression
            // Handle DEFAULT specially before evaluating other expressions
            let new_value = match &assignment.value {
                vibesql_ast::Expression::Default => {
                    // Use column's default value, or NULL if no default is defined
                    let column = &self.schema.columns[col_index];
                    if let Some(default_expr) = &column.default_value {
                        // Evaluate the default expression (currently only supports literals)
                        match default_expr {
                            vibesql_ast::Expression::Literal(lit) => lit.clone(),
                            _ => {
                                return Err(ExecutorError::UnsupportedExpression(format!(
                                    "Complex default expressions not yet supported for column '{}'",
                                    column.name
                                )))
                            }
                        }
                    } else {
                        // No default value defined, use NULL
                        vibesql_types::SqlValue::Null
                    }
                }
                _ => {
                    // Evaluate other expressions against ORIGINAL row
                    self.evaluator.eval(&assignment.value, original_row)?
                }
            };

            // Apply type affinity coercion (SQLite compatibility)
            // This ensures UPDATE applies the same type conversion as INSERT
            // e.g., UPDATE t SET r='5' on a REAL column stores 5.0, not '5'
            let column = &self.schema.columns[col_index];
            let coerced_value = coerce_value(new_value, &column.data_type)?;

            // Update column in new row
            new_row
                .set(col_index, coerced_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Track that this column changed
            changed_columns.insert(col_index);
        }

        Ok((new_row, changed_columns))
    }
}
