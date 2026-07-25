//! Value update logic for UPDATE operations

use std::collections::HashSet;

use vibesql_ast::Assignment;

use crate::{
    errors::ExecutorError,
    evaluator::ExpressionEvaluator,
    insert::validation::coerce_value,
    insert::{coerce_rowid_affinity, RowidAffinity},
};

/// Apply SQLite's rowid (INTEGER) affinity to a value assigned to the rowid —
/// either the INTEGER PRIMARY KEY alias column or the virtual `rowid`
/// pseudo-column — by an UPDATE statement.
///
/// Shares the INSERT-path coercion rules (`coerce_rowid_affinity`): integers
/// pass through, lossless-integer TEXT/REAL values are coerced, everything
/// else raises `datatype mismatch`. Unlike INSERT, where NULL means
/// "auto-assign the next rowid", sqlite3 3.51.0 rejects `UPDATE t SET
/// rowid=NULL` (and `SET <ipk>=NULL`) with `datatype mismatch`
/// (trigger1-15.1).
fn coerce_update_rowid_value(value: &vibesql_types::SqlValue) -> Result<i64, ExecutorError> {
    match coerce_rowid_affinity(value)? {
        RowidAffinity::Value(i) => Ok(i),
        RowidAffinity::Auto => {
            Err(ExecutorError::SqliteCompatError("datatype mismatch".to_string()))
        }
    }
}

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
            // Tuple assignment `SET (a, b, ...) = (row-value | subquery)`:
            // evaluate the RHS to one value per target column (positionally),
            // then coerce and store each. Evaluated against the ORIGINAL row
            // (two-phase semantics), like single-column assignments.
            if assignment.is_tuple() {
                let col_indices = assignment
                    .columns
                    .iter()
                    .map(|name| {
                        self.schema
                            .get_column_index(name)
                            .ok_or_else(|| ExecutorError::NoSuchColumn { column_ref: name.clone() })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let values = self.evaluator.eval_row_value(
                    &assignment.value,
                    original_row,
                    col_indices.len(),
                )?;
                for (col_index, new_value) in col_indices.into_iter().zip(values) {
                    let column = &self.schema.columns[col_index];
                    let coerced_value = if let Some(st) = self.schema.strict_type_of(col_index) {
                        crate::strict::enforce_strict_type(
                            new_value,
                            st,
                            &self.schema.name,
                            &column.name,
                        )?
                    } else {
                        coerce_value(new_value, &column.data_type)?
                    };
                    new_row
                        .set(col_index, coerced_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(col_index);
                }
                continue;
            }

            // Check if this is a rowid assignment (SQLite compatibility)
            let col_name_lower = assignment.column.to_lowercase();
            let is_rowid =
                col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";

            // A *real* column literally named `rowid`/`oid`/`_rowid_` shadows the
            // system rowid alias (triggerD-1.3/1.4, ticket
            // [34d2ae1c6d08b5271ba5e5592936d4a1d913ffe3]). `UPDATE t SET rowid=..`
            // on `t(rowid, ...)` must write that ordinary column, not relocate the
            // row's internal rowid. Only treat the assignment as a system-rowid
            // update when no ordinary column shadows the name — this preserves the
            // virtual-rowid relocation behavior (#5517) for tables that have *no*
            // such column. The INTEGER PRIMARY KEY alias is itself a real column
            // (`rowid_alias_column`), so it also flows through the normal
            // column-update path below and writes the PK column as before.
            let shadowing_column = self.schema.get_column_index(&assignment.column);

            if is_rowid && shadowing_column.is_none() {
                // Handle rowid update
                // If the table has an INTEGER PRIMARY KEY (rowid alias), update that column
                // Otherwise, update the row's internal row_id field
                if let Some(ipk_col_idx) = self.schema.rowid_alias_column {
                    // The INTEGER PRIMARY KEY column IS the rowid - update it.
                    // Apply SQLite's rowid (INTEGER) affinity: lossless TEXT/REAL
                    // integers coerce; anything else — including NULL, which is
                    // valid on INSERT (auto-assign) but not on UPDATE — raises
                    // `datatype mismatch` (trigger1-15.1, sqlite3 3.51.0).
                    let new_value = self.evaluator.eval(&assignment.value, original_row)?;
                    let coerced = coerce_update_rowid_value(&new_value)?;
                    new_row
                        .set(ipk_col_idx, vibesql_types::SqlValue::Integer(coerced))
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(ipk_col_idx);
                } else {
                    // No INTEGER PRIMARY KEY - update the virtual rowid.
                    // Same rowid affinity rules as the alias path: sqlite3 raises
                    // `datatype mismatch` for NULL / BLOB / non-numeric TEXT /
                    // fractional REAL, and coerces lossless TEXT/REAL integers.
                    let new_value = self.evaluator.eval(&assignment.value, original_row)?;
                    let new_rowid = coerce_update_rowid_value(&new_value)? as u64;
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
            // e.g., UPDATE t SET r='5' on a REAL column stores 5.0, not '5'.
            // STRICT tables (issue #5837) apply the rigid strict-datatype rules
            // instead, matching the INSERT strict gate.
            //
            // The INTEGER PRIMARY KEY rowid-alias column applies SQLite's rowid
            // affinity instead: the stored value must be a (losslessly coerced)
            // integer, and anything else — including NULL, which INSERT would
            // auto-assign — raises `datatype mismatch` (trigger1-15.1).
            let column = &self.schema.columns[col_index];
            let coerced_value = if self.schema.rowid_alias_column == Some(col_index) {
                vibesql_types::SqlValue::Integer(coerce_update_rowid_value(&new_value)?)
            } else if let Some(st) = self.schema.strict_type_of(col_index) {
                crate::strict::enforce_strict_type(new_value, st, &self.schema.name, &column.name)?
            } else {
                coerce_value(new_value, &column.data_type)?
            };

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
